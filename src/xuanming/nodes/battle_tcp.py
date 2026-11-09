import asyncio
import hmac
import logging
import secrets
import time
from collections import deque

from google.protobuf.message import DecodeError

from ..transport import DEFAULT_GATE_FRAME_SIZE, FrameError, Packet, encode_packet, read_packet, write_packet
from ..transport.client_limits import illegal_packet_threshold
from .battle import CLIENT_REQUESTS, BattleError

logger = logging.getLogger(__name__)

_MAX_FRAME_SIZE = DEFAULT_GATE_FRAME_SIZE
_MAX_REQUEST_SIZE = 1024
_MAX_BUFFERED_OUTPUT = 2 * 1024 * 1024
_SHUTDOWN_DRAIN_SECONDS = 5
_PROBE_START = "match.NetworkProbeStart"
_PROBE_ECHO = "match.NetworkProbeEcho"
_PROBE_GRANT_DOMAIN = b"xuanming.match.probe.grant.v1\0"
_PROBE_RECEIPT_DOMAIN = b"xuanming.match.probe.receipt.v1\0"


class BattleClients:
    def __init__(self, protocol, battle, *, probe_secret: bytes = b"") -> None:
        self.protocol = protocol
        self.battle = battle
        self._probe_secret = probe_secret
        self._writers = set()
        self._draining_writers = set()
        self._connections = set()
        self._stopping = False
        self._illegal_threshold = illegal_packet_threshold()
        self._limits = {}
        if battle.tables is not None:
            limits = battle.tables.tables["MessageLimiter"]
            for message_id in CLIENT_REQUESTS:
                row = limits.get(message_id)
                if row is not None:
                    self._limits[message_id] = (row.max_requests, row.time_window)

    async def serve(self, reader, writer) -> None:
        if self._stopping or len(self._writers) >= self.battle.max_connections:
            writer.close()
            return
        self._writers.add(writer)
        self._connections.add(asyncio.current_task())
        ticket = None
        send_task = None
        queue: asyncio.Queue[bytes | None] = asyncio.Queue(128)
        queued_bytes = 0
        closing = False
        illegal = 0
        requests: dict[int, deque[float]] = {}
        loop = asyncio.get_running_loop()

        def enqueue(message):
            nonlocal queued_bytes
            try:
                frame = None if message is None else encode_packet(Packet.from_message(message))
                if frame is not None and (queued_bytes + writer.transport.get_write_buffer_size() + len(frame)
                                          > _MAX_BUFFERED_OUTPUT):
                    logger.warning("Battle 直连输出超过缓冲限制")
                    writer.close()
                    return False
                queue.put_nowait(frame)
                if frame is not None:
                    queued_bytes += len(frame)
                return True
            except (asyncio.QueueFull, FrameError):
                writer.close()
                return False

        def close():
            nonlocal closing
            if closing:
                return
            closing = True
            self._draining_writers.add(writer)
            # Dispatch may close the room from inside an RPC. Queue its response first.
            loop.call_soon(enqueue, None)

        def is_live():
            return not closing and not writer.is_closing()

        def emit(message_id, message):
            if closing or writer.is_closing():
                return
            enqueue(self.protocol.content(message_id, body=message.SerializeToString()))

        async def send():
            nonlocal queued_bytes
            try:
                while (frame := await queue.get()) is not None:
                    queued_bytes -= len(frame)
                    writer.write(frame)
                    async with asyncio.timeout(5):
                        await writer.drain()
            finally:
                writer.close()

        try:
            async with asyncio.timeout(10):
                packet = await read_packet(reader, max_size=_MAX_FRAME_SIZE)
                if packet is not None and packet.type_name == _PROBE_START:
                    if self._probe_secret:
                        await self._probe(packet, reader, writer)
                    return
                if packet is None or packet.type_name != "BattleTokenVerifyRequest":
                    return
                verify = self.protocol.parse(packet.type_name, packet.payload)
                try:
                    ticket = self.battle.authorize(verify.payload, verify.signature)
                    self.battle.attach(ticket, emit, close, is_live=is_live)
                    response = self.protocol.message(
                        "BattleTokenVerifyResponse", success=True, battle_id=ticket.battle_id,
                    )
                except BattleError:
                    response = self.protocol.message("BattleTokenVerifyResponse", error="invalid battle token")
                await write_packet(writer, Packet.from_message(response))
                if not response.success:
                    return
            send_task = asyncio.create_task(send())
            if self.battle.owns_connection(ticket, close):
                self.battle.on_verified(ticket)
            while not closing and self.battle.owns_connection(ticket, close):
                packet = await read_packet(reader, max_size=_MAX_FRAME_SIZE)
                if packet is None or not self.battle.owns_connection(ticket, close):
                    return
                if packet.type_name == "BattleTokenVerifyRequest":
                    if not enqueue(self.protocol.message("BattleTokenVerifyResponse", success=True, battle_id=ticket.battle_id)):
                        return
                    continue
                if packet.type_name != "ClientRequest":
                    return
                request = self.protocol.parse(packet.type_name, packet.payload)
                content = self._dispatch(ticket, request, len(packet.payload), requests)
                if content.error_message.id:
                    illegal += 1
                if not enqueue(content) or self._illegal_threshold and illegal >= self._illegal_threshold:
                    return
        except (OSError, FrameError, DecodeError, TimeoutError):
            logger.debug("Battle 连接结束", exc_info=True)
        finally:
            if ticket is not None:
                self.battle.detach(ticket, close)
            if send_task is not None:
                if not closing:
                    send_task.cancel()
                try:
                    async with asyncio.timeout(5):
                        await asyncio.gather(send_task, return_exceptions=True)
                except TimeoutError:
                    logger.debug("Battle 连接排空超时")
            writer.close()
            self._writers.discard(writer)
            self._draining_writers.discard(writer)
            self._connections.discard(asyncio.current_task())

    async def _probe(self, packet, reader, writer) -> None:
        signed = self.protocol.parse(_PROBE_START, packet.payload).grant
        expected = hmac.digest(self._probe_secret, _PROBE_GRANT_DOMAIN + signed.payload, "sha256")
        if not hmac.compare_digest(signed.signature, expected):
            return
        grant = self.protocol.parse("match.NetworkProbeGrant", signed.payload)
        if (not all((grant.player_id, grant.plan_id, grant.session_binding, grant.physical_region))
                or (grant.physical_region, grant.target_node_id, grant.target_instance_id)
                != (self.battle.physical_region, self.battle.node_id, self.battle.instance_id)
                or grant.expires_at_ms <= time.time_ns() // 1_000_000):
            return

        rtt_ms = 1
        for index in range(3):
            nonce = secrets.token_bytes(32)
            challenge = self.protocol.message("match.NetworkProbeChallenge", nonce=nonce, sample_index=index)
            sent = time.monotonic_ns()
            async with asyncio.timeout(3):
                await write_packet(writer, Packet.from_message(challenge))
                packet = await read_packet(reader, max_size=_MAX_FRAME_SIZE)
            elapsed = time.monotonic_ns() - sent
            measured_at_ms = time.time_ns() // 1_000_000
            if (packet is None or packet.type_name != _PROBE_ECHO or elapsed > 3_000_000_000
                    or grant.expires_at_ms <= measured_at_ms):
                return
            echo = self.protocol.parse(_PROBE_ECHO, packet.payload)
            if echo.sample_index != index or not hmac.compare_digest(echo.nonce, nonce):
                return
            rtt_ms = max(rtt_ms, (elapsed + 999_999) // 1_000_000)

        receipt = self.protocol.message("match.NetworkProbeReceipt", grant=grant, rtt_ms=rtt_ms, sample_count=3,
                                        measured_at_ms=measured_at_ms, expires_at_ms=grant.expires_at_ms)
        payload = receipt.SerializeToString(deterministic=True)
        signature = hmac.digest(self._probe_secret, _PROBE_RECEIPT_DOMAIN + payload, "sha256")
        complete = self.protocol.message("match.NetworkProbeComplete", receipt={"payload": payload, "signature": signature})
        await write_packet(writer, Packet.from_message(complete))

    def _dispatch(self, ticket, request, size: int, requests: dict[int, deque[float]]):
        message_id, call_id = request.message_id, request.id
        kind = CLIENT_REQUESTS.get(message_id)
        if size > _MAX_REQUEST_SIZE:
            return self.protocol.content(message_id, call_id, error=1010)
        if kind is None:
            return self.protocol.content(message_id, call_id, error=1005)
        count, window = self._limits.get(message_id, (3, 1))
        now = time.monotonic()
        timestamps = requests.setdefault(message_id, deque())
        while timestamps and now - timestamps[0] > window:
            timestamps.popleft()
        if len(timestamps) >= count:
            return self.protocol.content(message_id, call_id, error=1008)
        timestamps.append(now)
        try:
            message = self.protocol.parse(kind, request.body)
            response = self.battle.dispatch(ticket, message_id, message)
        except (BattleError, DecodeError):
            return self.protocol.content(message_id, call_id, error=1005)
        return self.protocol.content(message_id, call_id, body=response.SerializeToString())

    async def close(self) -> None:
        self._stopping = True
        for writer in self._writers - self._draining_writers:
            writer.close()
        if self._connections:
            _, pending = await asyncio.wait(self._connections, timeout=_SHUTDOWN_DRAIN_SECONDS)
            if pending:
                for writer in self._writers:
                    writer.transport.abort()
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
