"""玄冥服务端纯 Python 机器人客户端（Portal HTTP / Gate TCP&WS / Battle TCP）。"""

from __future__ import annotations

import asyncio
import base64
from collections import deque
from dataclasses import dataclass
from itertools import count
import logging
from typing import Any

import aiohttp
from google.protobuf.message import Message

from ..protocol import Protocol, Route, RpcKind
from ..transport import (
    DEFAULT_GATE_FRAME_SIZE,
    FrameError,
    Packet,
    WebSocketPacketReader,
    WebSocketPacketWriter,
    read_packet,
    write_packet,
)

logger = logging.getLogger(__name__)


class RobotError(RuntimeError):
    """机器人网络、协议或业务校验失败的基础异常。"""


class RobotRpcError(RobotError):
    """服务端在 ``MessageContent.error_message`` 中返回非零错误码。"""

    def __init__(
        self,
        service: str,
        method: str,
        message_id: int,
        error_id: int,
        parameters: tuple[str, ...] = (),
    ) -> None:
        self.service = service
        self.method = method
        self.message_id = message_id
        self.error_id = error_id
        self.parameters = parameters
        detail = f" params={list(parameters)}" if parameters else ""
        super().__init__(
            f"RPC {service}.{method} (msg={message_id}) failed with error_id={error_id}{detail}"
        )


@dataclass(frozen=True, slots=True)
class NetworkProbeResult:
    """Battle 地域探测完成后的签名回执与解析指标。"""

    rtt_ms: int
    sample_count: int
    measured_at_ms: int
    expires_at_ms: int
    signed_receipt: Message
    receipt: Message


def decode_gate_ticket(assign_response: dict[str, Any]) -> tuple[bytes, bytes]:
    """从 Portal ``assign-gate`` / ``queue-status`` JSON 响应解码 Gate 票据与签名。"""
    payload_b64 = assign_response.get("token_payload", "")
    signature_b64 = assign_response.get("token_signature", "")
    if not payload_b64 or not signature_b64:
        raise RobotError("assign-gate response missing token_payload or token_signature")
    return base64.b64decode(payload_b64), base64.b64decode(signature_b64)


class PortalHttpClient:
    """Portal HTTP 入口异步客户端，复用单个 ``aiohttp.ClientSession``。"""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> PortalHttpClient:
        self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            )
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """关闭内部持有的 HTTP 会话（外部传入的会话由调用方管理）。"""
        if self._session is not None and self._owns_session:
            await self._session.close()
            self._session = None

    async def _request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        session = self._ensure_session()
        url = f"{self.base_url}{path}"
        try:
            async with session.request(method, url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.read()
                    # 日志与异常不携带响应正文，避免泄漏票据或敏感字段。
                    raise RobotError(f"Portal {method} {path} returned HTTP {resp.status} ({len(body)} bytes)")
                data = await resp.json(content_type=None)
        except aiohttp.ClientError as exc:
            raise RobotError(f"Portal {method} {path} network error: {exc}") from exc
        if not isinstance(data, dict):
            raise RobotError(f"Portal {method} {path} returned non-object JSON")
        return data

    async def server_list(self) -> dict[str, Any]:
        """获取区服列表（``GET /api/server-list``）。"""
        return await self._request_json("GET", "/api/server-list")

    async def login(
        self,
        *,
        zone_id: int,
        account: str = "",
        password: str = "",
        auth_type: str = "",
        auth_token: str = "",
    ) -> dict[str, Any]:
        """提交 HTTP 登录鉴权（``POST /api/login``）。"""
        body: dict[str, Any] = {
            "zone_id": zone_id,
            "account": account,
            "password": password,
            "auth_type": auth_type,
            "auth_token": auth_token,
        }
        return await self._request_json("POST", "/api/login", body)

    async def refresh_token(self, refresh_token: str) -> dict[str, Any]:
        """刷新登录令牌（``POST /api/refresh-token``）。"""
        return await self._request_json("POST", "/api/refresh-token", {"refresh_token": refresh_token})

    async def assign_gate(
        self,
        *,
        zone_id: int,
        access_token: str = "",
        device_id: str = "",
        queue_token: str = "",
    ) -> dict[str, Any]:
        """申请 Gate 节点地址与签发票据（``POST /api/assign-gate``）。"""
        body: dict[str, Any] = {
            "zone_id": zone_id,
            "access_token": access_token,
            "device_id": device_id,
            "queue_token": queue_token,
        }
        return await self._request_json("POST", "/api/assign-gate", body)

    async def queue_status(self, *, queue_token: str, zone_id: int = 0) -> dict[str, Any]:
        """轮询排队状态并获取放行票据（``POST /api/queue-status``）。"""
        return await self._request_json(
            "POST",
            "/api/queue-status",
            {"queue_token": queue_token, "zone_id": zone_id},
        )

    async def submit_real_name(
        self,
        *,
        zone_id: int,
        access_token: str,
        name: str,
        id_number: str,
    ) -> dict[str, Any]:
        """提交实名认证信息（``POST /api/real-name``）。"""
        return await self._request_json(
            "POST",
            "/api/real-name",
            {
                "zone_id": zone_id,
                "access_token": access_token,
                "name": name,
                "id_number": id_number,
            },
        )

    async def request_account_deletion(
        self,
        *,
        zone_id: int,
        account: str = "",
        password: str = "",
        auth_type: str = "",
        auth_token: str = "",
        authorization_code: str = "",
    ) -> dict[str, Any]:
        """发起账号注销申请（``POST /api/account/delete``）。"""
        return await self._request_json(
            "POST",
            "/api/account/delete",
            {
                "zone_id": zone_id,
                "account": account,
                "password": password,
                "auth_type": auth_type,
                "auth_token": auth_token,
                "authorization_code": authorization_code,
            },
        )

    async def cancel_account_deletion(
        self,
        *,
        zone_id: int,
        access_token: str,
    ) -> dict[str, Any]:
        """撤销冷静期内的账号注销申请（``POST /api/account/delete/cancel``）。"""
        return await self._request_json(
            "POST",
            "/api/account/delete/cancel",
            {"zone_id": zone_id, "access_token": access_token},
        )


def _build_route_index(protocol: Protocol) -> dict[tuple[str, str], Route]:
    """按完整服务名与短服务名建立 ``(service, method) -> Route`` 快速索引。"""
    index: dict[tuple[str, str], Route] = {}
    short_counts: dict[tuple[str, str], int] = {}
    for route in protocol.routes.values():
        index[(route.service, route.method)] = route
        short_service = route.service.rsplit(".", 1)[-1]
        key = (short_service, route.method)
        short_counts[key] = short_counts.get(key, 0) + 1
    for route in protocol.routes.values():
        short_service = route.service.rsplit(".", 1)[-1]
        key = (short_service, route.method)
        if short_counts[key] == 1 and key not in index:
            index[key] = route
    return index


class GateRobotClient:
    """Gate 客户端，同时支持 muduo TCP 帧与 WebSocket 二进制帧收发。"""

    def __init__(
        self,
        protocol: Protocol | None = None,
        *,
        max_frame_size: int = DEFAULT_GATE_FRAME_SIZE,
    ) -> None:
        self.protocol = protocol or Protocol()
        self._max_frame_size = max_frame_size
        self._routes = _build_route_index(self.protocol)
        self._reader: Any = None
        self._writer: Any = None
        self._websocket: Any = None
        self._write_lock = asyncio.Lock()
        self._reader_task: asyncio.Task | None = None
        self._closed_error: Exception | None = None
        self._call_ids = count(1)
        self._heartbeat_seqs = count(1)
        self._pending_verify: asyncio.Future | None = None
        self._pending_heartbeats: dict[int, asyncio.Future] = {}
        self._pending_rpcs: dict[int, tuple[int, asyncio.Future]] = {}
        self.pushes: asyncio.Queue[Message] = asyncio.Queue()
        self._unclaimed_pushes: deque[Message] = deque()
        self.account: str = ""
        self.player_id: int = 0
        self.heartbeat_interval_ms: int = 0
        self.heartbeat_timeout_ms: int = 0

    @property
    def connected(self) -> bool:
        return self._writer is not None and self._reader_task is not None and not self._reader_task.done()

    async def __aenter__(self) -> GateRobotClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect_tcp(self, host: str, port: int, *, timeout: float = 10.0) -> GateRobotClient:
        """建立到 Gate TCP 监听端口的连接并启动后台读循环。"""
        if self._writer is not None:
            raise RobotError("GateRobotClient is already connected")
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_connection(host, port)
        self._reader = reader
        self._writer = writer
        self._websocket = None
        self._closed_error = None
        self._reader_task = asyncio.create_task(self._read_loop(), name="robot-gate-reader")
        logger.debug("Connected to Gate TCP %s:%d", host, port)
        return self

    async def connect_websocket(self, url: str, *, timeout: float = 10.0) -> GateRobotClient:
        """建立到 Gate WebSocket 监听地址的连接并启动后台读循环。"""
        if self._writer is not None:
            raise RobotError("GateRobotClient is already connected")
        from websockets.asyncio.client import connect

        async with asyncio.timeout(timeout):
            ws = await connect(
                url,
                max_size=self._max_frame_size + 4,
                compression=None,
                open_timeout=timeout,
            )
        self._websocket = ws
        self._reader = WebSocketPacketReader(ws, max_size=self._max_frame_size)
        self._writer = WebSocketPacketWriter(ws)
        self._closed_error = None
        self._reader_task = asyncio.create_task(self._read_loop(), name="robot-gate-ws-reader")
        logger.debug("Connected to Gate WebSocket %s", url)
        return self

    async def close(self) -> None:
        """关闭连接、停止后台读任务并唤醒所有挂起调用。"""
        task = self._reader_task
        self._reader_task = None
        if task is not None and not task.done():
            task.cancel()
        writer = self._writer
        ws = self._websocket
        self._writer = None
        self._reader = None
        self._websocket = None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                logger.debug("Gate WebSocket close raised", exc_info=True)
        elif writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                logger.debug("Gate TCP writer wait_closed raised", exc_info=True)
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        self._fail_all_pending(RobotError("Gate connection closed"))

    async def _send_message(self, message: Message) -> None:
        if self._writer is None:
            raise RobotError("GateRobotClient is not connected")
        if self._closed_error is not None:
            raise RobotError(f"Gate connection already closed: {self._closed_error}") from self._closed_error
        async with self._write_lock:
            if self._writer is None:
                raise RobotError("GateRobotClient is not connected")
            await write_packet(self._writer, Packet.from_message(message))

    async def _read_loop(self) -> None:
        error: Exception | None = None
        try:
            while True:
                packet = await read_packet(self._reader, max_size=self._max_frame_size)
                if packet is None:
                    error = RobotError("Gate closed connection (EOF)")
                    break
                self._dispatch_packet(packet)
        except asyncio.CancelledError:
            error = RobotError("Gate reader cancelled")
            raise
        except (FrameError, OSError, ValueError) as exc:
            error = exc
            logger.debug("Gate reader stopped with error: %s", exc)
        finally:
            self._closed_error = error or RobotError("Gate connection closed")
            self._fail_all_pending(self._closed_error)

    def _dispatch_packet(self, packet: Packet) -> None:
        if packet.type_name == "ClientTokenVerifyResponse":
            response = self.protocol.parse(packet.type_name, packet.payload)
            future = self._pending_verify
            self._pending_verify = None
            if future is not None and not future.done():
                future.set_result(response)
            return
        if packet.type_name == "ClientHeartbeatResponse":
            response = self.protocol.parse(packet.type_name, packet.payload)
            future = self._pending_heartbeats.pop(response.seq, None)
            if future is not None and not future.done():
                future.set_result(response)
            return
        if packet.type_name == "MessageContent":
            content = self.protocol.parse("MessageContent", packet.payload)
            if content.id and content.id in self._pending_rpcs:
                expected_mid, future = self._pending_rpcs.pop(content.id)
                if not future.done():
                    if content.message_id != expected_mid:
                        future.set_exception(
                            RobotError(
                                f"Mismatched RPC response message_id={content.message_id}, expected {expected_mid}"
                            )
                        )
                    else:
                        future.set_result(content)
                return
            self.pushes.put_nowait(content)
            return
        logger.debug("Ignored unexpected Gate packet type=%s", packet.type_name)

    def _fail_all_pending(self, error: Exception) -> None:
        if self._pending_verify is not None and not self._pending_verify.done():
            self._pending_verify.set_exception(error)
        self._pending_verify = None
        for future in list(self._pending_heartbeats.values()):
            if not future.done():
                future.set_exception(error)
        self._pending_heartbeats.clear()
        for _, future in list(self._pending_rpcs.values()):
            if not future.done():
                future.set_exception(error)
        self._pending_rpcs.clear()

    def _resolve_route(self, service: str, method: str) -> Route:
        route = self._routes.get((service, method))
        if route is None:
            raise RobotError(f"Unknown RPC route {service}.{method}")
        return route

    async def verify_ticket(
        self,
        payload: bytes,
        signature: bytes,
        *,
        timeout: float = 5.0,
    ) -> Message:
        """发送 ``ClientTokenVerifyRequest`` 完成 Gate 连接验签。"""
        if self._pending_verify is not None:
            raise RobotError("Ticket verification already in progress")
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending_verify = future
        request = self.protocol.message("ClientTokenVerifyRequest", payload=payload, signature=signature)
        try:
            await self._send_message(request)
            async with asyncio.timeout(timeout):
                response = await future
        finally:
            if self._pending_verify is future:
                self._pending_verify = None
        if not response.success:
            raise RobotError(f"Gate ticket verification rejected: {response.error or 'unknown'}")
        self.heartbeat_interval_ms = response.heartbeat_interval_ms
        self.heartbeat_timeout_ms = response.heartbeat_timeout_ms
        return response

    async def heartbeat(self, seq: int | None = None, *, timeout: float = 5.0) -> Message:
        """发送 ``ClientHeartbeatRequest`` 并等待对应序号的 ``ClientHeartbeatResponse``。"""
        seq_num = next(self._heartbeat_seqs) if seq is None else seq
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending_heartbeats[seq_num] = future
        try:
            await self._send_message(self.protocol.message("ClientHeartbeatRequest", seq=seq_num))
            async with asyncio.timeout(timeout):
                return await future
        finally:
            self._pending_heartbeats.pop(seq_num, None)

    async def rpc(
        self,
        service: str,
        method: str,
        request_msg: Message | dict[str, Any] | None = None,
        response_name: str | None = None,
        *,
        timeout: float = 5.0,
    ) -> Message | None:
        """发起客户端 RPC 并按自增 ``request_id`` 关联应答；单向消息返回 ``None``。"""
        route = self._resolve_route(service, method)
        if request_msg is None:
            msg = self.protocol.message(route.request_type)
        elif isinstance(request_msg, dict):
            msg = self.protocol.message(route.request_type, **request_msg)
        else:
            msg = request_msg

        if route.kind == RpcKind.ONE_WAY:
            envelope = self.protocol.message(
                "ClientRequest",
                id=0,
                message_id=route.message_id,
                body=msg.SerializeToString(),
            )
            await self._send_message(envelope)
            return None

        call_id = next(self._call_ids)
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending_rpcs[call_id] = (route.message_id, future)
        envelope = self.protocol.message(
            "ClientRequest",
            id=call_id,
            message_id=route.message_id,
            body=msg.SerializeToString(),
        )
        try:
            await self._send_message(envelope)
            async with asyncio.timeout(timeout):
                content = await future
        finally:
            self._pending_rpcs.pop(call_id, None)

        if content.error_message.id:
            raise RobotRpcError(
                route.service,
                route.method,
                route.message_id,
                content.error_message.id,
                tuple(content.error_message.parameters),
            )
        target_type = response_name or route.response_type
        return self.protocol.parse(target_type, content.serialized_message)

    async def wait_push(self, message_id: int | str, *, timeout: float = 5.0) -> Message:
        """等待指定消息号（或消息名）的服务端推送 ``MessageContent`` 帧。"""
        target_id = self.protocol.message_ids[message_id] if isinstance(message_id, str) else int(message_id)
        for idx, item in enumerate(self._unclaimed_pushes):
            if item.message_id == target_id:
                del self._unclaimed_pushes[idx]
                return item
        async with asyncio.timeout(timeout):
            while True:
                item = await self.pushes.get()
                if item.message_id == target_id:
                    return item
                self._unclaimed_pushes.append(item)

    async def login_and_enter(
        self,
        account: str,
        access_token: str = "",
        *,
        password: str = "",
        auth_type: str = "",
        class_id: int = 1,
        gender: int = 1,
        role_name: str = "",
        wait_enter_push: bool = True,
        timeout: float = 5.0,
    ) -> int:
        """执行 ``Login -> (空列表时 CreatePlayer) -> EnterGame`` 全流程并返回 ``player_id``。"""
        login_fields: dict[str, Any] = {"account": account}
        if auth_type:
            login_fields["auth_type"] = auth_type
            if password:
                login_fields["password"] = password
            if access_token:
                login_fields["auth_token"] = access_token
        elif password:
            login_fields["password"] = password
        elif access_token:
            login_fields["auth_type"] = "access_token"
            login_fields["auth_token"] = access_token

        login_resp = await self.rpc(
            "loginpb.ClientPlayerLogin",
            "Login",
            login_fields,
            "loginpb.LoginResponse",
            timeout=timeout,
        )
        if login_resp.error_message.id:
            raise RobotRpcError(
                "loginpb.ClientPlayerLogin",
                "Login",
                self.protocol.message_ids["ClientPlayerLoginLogin"],
                login_resp.error_message.id,
                tuple(login_resp.error_message.parameters),
            )

        players = list(login_resp.players)
        if not players:
            create_resp = await self.rpc(
                "loginpb.ClientPlayerLogin",
                "CreatePlayer",
                {
                    "class_id": class_id,
                    "gender": gender,
                    "name": (role_name or account)[:12],
                },
                "loginpb.CreatePlayerResponse",
                timeout=timeout,
            )
            if create_resp.error_message.id:
                raise RobotRpcError(
                    "loginpb.ClientPlayerLogin",
                    "CreatePlayer",
                    self.protocol.message_ids["ClientPlayerLoginCreatePlayer"],
                    create_resp.error_message.id,
                    tuple(create_resp.error_message.parameters),
                )
            players = list(create_resp.players)
        if not players:
            raise RobotError("Login and CreatePlayer returned no available players")

        player_id = players[0].player.player_id
        enter_resp = await self.rpc(
            "loginpb.ClientPlayerLogin",
            "EnterGame",
            {"player_id": player_id, "request_id": f"robot-enter-{player_id}"},
            "loginpb.EnterGameResponse",
            timeout=timeout,
        )
        if enter_resp.error_message.id:
            raise RobotRpcError(
                "loginpb.ClientPlayerLogin",
                "EnterGame",
                self.protocol.message_ids["ClientPlayerLoginEnterGame"],
                enter_resp.error_message.id,
                tuple(enter_resp.error_message.parameters),
            )
        if enter_resp.player_id != player_id:
            raise RobotError(f"EnterGame returned mismatched player_id={enter_resp.player_id}, expected {player_id}")

        if wait_enter_push:
            await self.wait_push("SceneSceneClientPlayerNotifyEnterScene", timeout=timeout)

        self.account = account
        self.player_id = player_id
        return player_id

    async def leave_game(self, *, timeout: float = 5.0) -> None:
        """发送 ``LeaveGame`` 主动退出当前场景。"""
        await self.rpc(
            "loginpb.ClientPlayerLogin",
            "LeaveGame",
            {},
            "loginpb.LoginEmptyResponse",
            timeout=timeout,
        )


class BattleRobotClient:
    """Battle 直连 TCP 客户端，支持票据验证、RPC 调用与地域延迟探测。"""

    def __init__(
        self,
        protocol: Protocol | None = None,
        *,
        max_frame_size: int = DEFAULT_GATE_FRAME_SIZE,
    ) -> None:
        self.protocol = protocol or Protocol()
        self._max_frame_size = max_frame_size
        self._routes = _build_route_index(self.protocol)
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._call_ids = count(1)
        self._heartbeat_seqs = count(1)
        self.battle_id: int = 0
        self.pushes: deque[Message] = deque()

    async def __aenter__(self) -> BattleRobotClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect(self, host: str, port: int, *, timeout: float = 10.0) -> BattleRobotClient:
        """建立到 Battle 直连 TCP 端口的连接。"""
        if self._writer is not None:
            raise RobotError("BattleRobotClient is already connected")
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_connection(host, port)
        self._reader = reader
        self._writer = writer
        logger.debug("Connected to Battle TCP %s:%d", host, port)
        return self

    async def close(self) -> None:
        """关闭 Battle TCP 连接。"""
        writer = self._writer
        self._writer = None
        self._reader = None
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                logger.debug("Battle TCP writer wait_closed raised", exc_info=True)

    async def _send(self, message: Message) -> None:
        if self._writer is None:
            raise RobotError("BattleRobotClient is not connected")
        await write_packet(self._writer, Packet.from_message(message))

    async def _read(self, *, timeout: float = 5.0) -> Packet:
        if self._reader is None:
            raise RobotError("BattleRobotClient is not connected")
        async with asyncio.timeout(timeout):
            packet = await read_packet(self._reader, max_size=self._max_frame_size)
        if packet is None:
            raise RobotError("Battle closed connection (EOF)")
        return packet

    async def enter_battle(
        self,
        ticket_payload: bytes,
        ticket_sig: bytes,
        *,
        timeout: float = 5.0,
    ) -> Message:
        """发送 ``BattleTokenVerifyRequest`` 完成战斗直连票据验签。"""
        request = self.protocol.message(
            "BattleTokenVerifyRequest",
            payload=ticket_payload,
            signature=ticket_sig,
        )
        await self._send(request)
        packet = await self._read(timeout=timeout)
        if packet.type_name != "BattleTokenVerifyResponse":
            raise RobotError(f"Unexpected Battle verify response packet {packet.type_name!r}")
        response = self.protocol.parse("BattleTokenVerifyResponse", packet.payload)
        if not response.success:
            raise RobotError(f"Battle ticket verification rejected: {response.error or 'unknown'}")
        self.battle_id = response.battle_id
        return response

    async def heartbeat(self, seq: int | None = None, *, timeout: float = 5.0) -> Message:
        """向 Battle 连接发送一次心跳并校验回显。"""
        seq_num = next(self._heartbeat_seqs) if seq is None else seq
        await self._send(self.protocol.message("ClientHeartbeatRequest", seq=seq_num))
        async with asyncio.timeout(timeout):
            while True:
                packet = await self._read(timeout=timeout)
                if packet.type_name == "ClientHeartbeatResponse":
                    resp = self.protocol.parse(packet.type_name, packet.payload)
                    if resp.seq == seq_num:
                        return resp
                elif packet.type_name == "MessageContent":
                    self.pushes.append(self.protocol.parse("MessageContent", packet.payload))

    async def rpc(
        self,
        service: str,
        method: str,
        request_msg: Message | dict[str, Any] | None = None,
        response_name: str | None = None,
        *,
        timeout: float = 5.0,
    ) -> Message:
        """在已验签的 Battle 连接上调用战斗 RPC（如 ``GetBattleState`` / ``SubmitBattleAction``）。"""
        route = self._routes.get((service, method))
        if route is None:
            raise RobotError(f"Unknown Battle RPC route {service}.{method}")
        if request_msg is None:
            msg = self.protocol.message(route.request_type)
        elif isinstance(request_msg, dict):
            msg = self.protocol.message(route.request_type, **request_msg)
        else:
            msg = request_msg
        call_id = next(self._call_ids)
        await self._send(
            self.protocol.message(
                "ClientRequest",
                id=call_id,
                message_id=route.message_id,
                body=msg.SerializeToString(),
            )
        )
        async with asyncio.timeout(timeout):
            while True:
                packet = await self._read(timeout=timeout)
                if packet.type_name != "MessageContent":
                    continue
                content = self.protocol.parse("MessageContent", packet.payload)
                if content.id == call_id and content.message_id == route.message_id:
                    if content.error_message.id:
                        raise RobotRpcError(
                            route.service,
                            route.method,
                            route.message_id,
                            content.error_message.id,
                            tuple(content.error_message.parameters),
                        )
                    return self.protocol.parse(response_name or route.response_type, content.serialized_message)
                self.pushes.append(content)

    def _coerce_probe_grant(self, token: Any) -> Message:
        if isinstance(token, Message):
            return token
        if isinstance(token, dict):
            return self.protocol.message("match.SignedNetworkProbe", **token)
        if isinstance(token, tuple) and len(token) == 2:
            return self.protocol.message("match.SignedNetworkProbe", payload=token[0], signature=token[1])
        if isinstance(token, (bytes, bytearray)):
            return self.protocol.parse("match.SignedNetworkProbe", bytes(token))
        raise RobotError(f"Unsupported probe grant token type: {type(token).__name__}")

    async def probe_latency(self, token: Any, *, timeout: float = 5.0) -> NetworkProbeResult:
        """执行 Battle 地域延迟探测握手（Start -> 3x Challenge/Echo -> Complete）。"""
        grant = self._coerce_probe_grant(token)
        start_msg = self.protocol.message("match.NetworkProbeStart", grant=grant)
        async with asyncio.timeout(timeout):
            await self._send(start_msg)
            while True:
                packet = await self._read(timeout=timeout)
                if packet.type_name == "match.NetworkProbeChallenge":
                    challenge = self.protocol.parse(packet.type_name, packet.payload)
                    echo = self.protocol.message(
                        "match.NetworkProbeEcho",
                        nonce=challenge.nonce,
                        sample_index=challenge.sample_index,
                    )
                    await self._send(echo)
                elif packet.type_name == "match.NetworkProbeComplete":
                    complete = self.protocol.parse(packet.type_name, packet.payload)
                    signed = complete.receipt
                    if not signed.payload or not signed.signature:
                        raise RobotError("NetworkProbeComplete carries empty signed receipt")
                    receipt = self.protocol.parse("match.NetworkProbeReceipt", signed.payload)
                    return NetworkProbeResult(
                        rtt_ms=receipt.rtt_ms,
                        sample_count=receipt.sample_count,
                        measured_at_ms=receipt.measured_at_ms,
                        expires_at_ms=receipt.expires_at_ms,
                        signed_receipt=signed,
                        receipt=receipt,
                    )
                else:
                    raise RobotError(f"Unexpected packet during network probe: {packet.type_name!r}")
