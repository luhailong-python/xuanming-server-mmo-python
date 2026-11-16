"""单连接、双向的节点间 RPC0 调用与派发。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass
from enum import IntEnum
from itertools import count
import logging
from math import isfinite

from google.protobuf.message import DecodeError, Message

from .framing import DEFAULT_RPC_FRAME_SIZE, FrameError, encode_rpc_frame, read_rpc_frame, write_rpc_frame

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RpcReply:
    """回复交给连接后触发回调，连接提前断开时也收尾。"""

    body: bytes
    after_reply: Callable[[], None]


RpcHandler = Callable[[bytes], Awaitable[bytes | RpcReply]]


class RpcMessageType(IntEnum):
    REQUEST = 0
    RESPONSE = 1
    RPC_ERROR = 4
    HEARTBEAT = 5


class RpcErrorCode(IntEnum):
    WRONG_PROTO = 1
    NO_SERVICE = 2
    NO_METHOD = 3
    INVALID_REQUEST = 4
    HANDLER_FAILED = 7
    REPLY_DROPPED = 8
    KILL_SWITCH_DENIED = 9


class RpcError(Exception):
    """对端应答了 RPC_ERROR。处理器也可用此异常指定回包错误码。"""

    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(f"RPC0 error {code}")


class RpcDisconnected(ConnectionError):
    """连接关闭。已发送请求的执行结果未知。"""


class RpcNotSent(RpcDisconnected):
    """调用确定没有提交到 TCP 写缓冲。"""


class RpcInvalidResponse(ValueError):
    """应答的消息号与在途调用不一致。"""


class RpcBusy(RuntimeError):
    """本端在途调用达到上限。"""


_Reply = bytes | RpcError | RpcDisconnected | RpcInvalidResponse


@dataclass(frozen=True, slots=True)
class RpcLinkHeartbeat:
    """节点链路默认每 5 秒空闲发送、15 秒静默断开。"""

    interval: float = 5.0
    idle_timeout: float = 15.0
    check_period: float = 1.0

    def __post_init__(self) -> None:
        if not (
            0 < self.check_period <= self.interval
            and self.interval + 2 * self.check_period < self.idle_timeout
            and isfinite(self.idle_timeout)
        ):
            raise ValueError("invalid RPC0 link heartbeat intervals")


class RpcChannel:
    """启动读循环，同时处理本端请求、对端请求及各自的回复。"""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        envelope_type: type[Message],
        handlers: Mapping[int, RpcHandler] | None = None,
        *,
        known_ids: Collection[int] | None = None,
        max_pending: int = 1024,
        max_inbound: int = 1024,
        max_frame_size: int = DEFAULT_RPC_FRAME_SIZE,
        heartbeat: RpcLinkHeartbeat | None = None,
        deny: Callable[[int], bool] | None = None,
    ) -> None:
        if min(max_pending, max_inbound) < 1:
            raise ValueError("RPC0 concurrency limits must be positive")
        self._reader = reader
        self._writer = writer
        self._envelope_type = envelope_type
        self._handlers = dict(handlers or {})
        self._known_ids = frozenset(self._handlers if known_ids is None else known_ids)
        self._max_pending = max_pending
        self._max_inbound = max_inbound
        self._max_frame_size = max_frame_size
        self._call_ids = count(1)
        self._pending: dict[int, tuple[int, asyncio.Future[_Reply]]] = {}
        self._inbound: set[asyncio.Task[None]] = set()
        self._write_lock = asyncio.Lock()
        self._closed = False
        self._loop = asyncio.get_running_loop()
        self._heartbeat = heartbeat
        self._deny = deny
        self._heartbeat_timer: asyncio.TimerHandle | None = None
        self._last_check = self._last_sent = self._last_received = self._loop.time()
        self._read_activity = False
        self._write_activity = False
        if heartbeat is not None:
            probe = envelope_type(type=RpcMessageType.HEARTBEAT).SerializeToString()
            self._heartbeat_frame = encode_rpc_frame(probe, max_size=max_frame_size)
            self._heartbeat_timer = self._loop.call_later(heartbeat.check_period, self._check_heartbeat, heartbeat)
        self._receiver = self._loop.create_task(self._receive(), name="rpc0-receiver")

    async def call(self, message_id: int, body: bytes, *, timeout: float = 5.0,
                   can_send: Callable[[], bool] | None = None) -> bytes:
        """发送需要应答的请求。超时或断连时，调用可能已经执行。"""
        self._ensure_open()
        if len(self._pending) >= self._max_pending:
            raise RpcBusy("too many outstanding RPC0 calls")
        if timeout <= 0:
            raise ValueError("RPC0 timeout must be positive")
        call_id = next(self._call_ids)
        reply = self._loop.create_future()
        self._pending[call_id] = (message_id, reply)
        try:
            async with asyncio.timeout(timeout):
                await self._write(self._envelope_type(id=call_id, message_id=message_id, request=body), can_send=can_send)
                result = await reply
            if isinstance(result, Exception):
                raise result
            return result
        finally:
            self._pending.pop(call_id, None)

    async def send(self, message_id: int, body: bytes) -> None:
        """发送单向请求，线上不分配调用号。"""
        await self._write(self._envelope_type(message_id=message_id, request=body))

    async def send_heartbeat(self) -> None:
        await self._write(self._envelope_type(type=RpcMessageType.HEARTBEAT))

    async def wait_closed(self) -> None:
        await asyncio.shield(self._receiver)

    @property
    def closed(self) -> bool:
        return self._closed

    def abort(self) -> None:
        self._stop()

    async def aclose(self) -> None:
        self._stop()
        if asyncio.current_task() not in self._inbound:
            await self.wait_closed()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RpcNotSent("RPC0 channel is closed")

    async def _write(self, envelope: Message, *, can_send: Callable[[], bool] | None = None) -> None:
        self._ensure_open()
        async with self._write_lock:
            self._ensure_open()
            if can_send is not None and not can_send():
                raise RpcNotSent("RPC0 call was superseded before sending")
            try:
                self._write_activity = True
                await write_rpc_frame(
                    self._writer, envelope.SerializeToString(), max_size=self._max_frame_size,
                )
            except OSError as exc:
                self._stop()
                raise RpcDisconnected("RPC0 write failed") from exc

    def _stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._heartbeat_timer is not None:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None
        for _, future in self._pending.values():
            if not future.done():
                future.set_result(RpcDisconnected("RPC0 channel closed before response"))
        for task in self._inbound:
            if task is not asyncio.current_task():
                task.cancel()
        self._writer.transport.abort()

    def _note_read(self) -> None:
        self._read_activity = True

    def _check_heartbeat(self, config: RpcLinkHeartbeat) -> None:
        now = self._loop.time()
        late = now - self._last_check > 2 * config.check_period
        self._last_check = now
        if self._read_activity:
            self._read_activity = False
            self._last_received = now
        if self._write_activity:
            self._write_activity = False
            self._last_sent = now

        # 本端事件循环卡顿时留一个节拍处理积压的网络输入。
        if not late and now - self._last_received >= config.idle_timeout:
            self._stop()
            return
        if now - self._last_sent >= config.interval and not self._writer.transport.get_write_buffer_size():
            try:
                self._writer.write(self._heartbeat_frame)
            except OSError:
                self._stop()
                return
            self._last_sent = now
        self._heartbeat_timer = self._loop.call_later(config.check_period, self._check_heartbeat, config)

    async def _receive(self) -> None:
        on_read = self._note_read if self._heartbeat is not None else None
        try:
            while not self._closed:
                payload = await read_rpc_frame(self._reader, max_size=self._max_frame_size, on_read=on_read)
                if payload is None:
                    break
                envelope = self._envelope_type.FromString(payload)
                if envelope.type == RpcMessageType.REQUEST:
                    await self._dispatch(envelope)
                elif envelope.type in (RpcMessageType.RESPONSE, RpcMessageType.RPC_ERROR):
                    self._finish_call(envelope)
                # HEARTBEAT 及未知类型均不派发，也不回包。
        except (OSError, FrameError, DecodeError) as exc:
            _LOG.debug("RPC0 link ended: %s", exc)
        finally:
            self._stop()
            await asyncio.gather(*self._inbound, return_exceptions=True)
            try:
                await self._writer.wait_closed()
            except OSError:
                pass

    def _finish_call(self, reply: Message) -> None:
        pending = self._pending.pop(reply.id, None)
        if pending is None:
            return
        message_id, future = pending
        if future.done():
            return
        if reply.message_id != message_id:
            result = RpcInvalidResponse(f"expected RPC0 message {message_id}, got {reply.message_id}")
        elif reply.type == RpcMessageType.RPC_ERROR:
            result = RpcError(reply.error)
        else:
            result = reply.response
        future.set_result(result)

    async def _dispatch(self, request: Message) -> None:
        handler = self._handlers.get(request.message_id)
        if request.message_id not in self._known_ids:
            await self._reply(request, error=RpcErrorCode.INVALID_REQUEST)
        elif self._deny is not None and self._deny(request.message_id):
            await self._reply(request, error=RpcErrorCode.KILL_SWITCH_DENIED)
        elif handler is None or len(self._inbound) >= self._max_inbound:
            await self._reply(request, error=RpcErrorCode.NO_SERVICE)
        else:
            task = asyncio.create_task(self._handle(request, handler), name=f"rpc0-method-{request.message_id}")
            self._inbound.add(task)
            task.add_done_callback(self._inbound.discard)

    async def _handle(self, request: Message, handler: RpcHandler) -> None:
        try:
            try:
                result = await handler(request.request)
                if isinstance(result, RpcReply):
                    try:
                        await self._reply(request, body=result.body)
                    finally:
                        try:
                            result.after_reply()
                        except Exception:
                            _LOG.exception("RPC0 post-reply callback failed for message %d", request.message_id)
                else:
                    await self._reply(request, body=result)
            except RpcError as exc:
                code = exc.code if 0 < exc.code < 1 << 31 else RpcErrorCode.HANDLER_FAILED
                await self._reply(request, error=code)
            except RpcDisconnected:
                raise
            except Exception:
                _LOG.exception("RPC0 handler failed for message %d", request.message_id)
                await self._reply(request, error=RpcErrorCode.HANDLER_FAILED)
        except RpcDisconnected:
            pass
        except FrameError:
            self._stop()

    async def _reply(self, request: Message, *, body: bytes = b"", error: int = 0) -> None:
        if request.id:
            await self._write(self._envelope_type(
                type=RpcMessageType.RPC_ERROR if error else RpcMessageType.RESPONSE,
                id=request.id, message_id=request.message_id, response=body, error=error,
            ))
