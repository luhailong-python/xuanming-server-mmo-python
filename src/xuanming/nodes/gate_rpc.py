"""Gate 内部 RPC0 入口和原版 GM 管理签名。"""

from __future__ import annotations

import asyncio
import hmac
import ipaddress
import logging
import os
import re
import socket
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial

import grpc
from google.protobuf.message import Message

from ..cluster import SCENE
from ..node_link import GATE_BROADCAST_METHODS, _identity, _parse, _session_on_gate
from ..transport import RpcChannel, RpcError, RpcErrorCode, RpcLinkHeartbeat, RpcReply

logger = logging.getLogger(__name__)

_HEARTBEAT = RpcLinkHeartbeat()
_REGISTRATION_FAILED = 1018
_DISCOVERY_REFRESH = 1.0
_GM_METHOD = "Gate.GmGracefulShutdown"
_MAX_NONCES = 1024
_MAX_I64 = (1 << 63) - 1
_ASCII_SPACE = " \t\n\r\v\f"
_UNIX_DIGITS = re.compile(r"[0-9]{1,20}")
_LOWER_HEX = re.compile(r"[0-9a-f]{64}")
_SIGNED_DIGITS = re.compile(r"[+-]?[0-9]{1,20}")


def _gm_skew() -> int:
    raw = os.getenv("GATE_GM_AUTH_SKEW_SECONDS", "").lstrip(_ASCII_SPACE)
    if not _SIGNED_DIGITS.fullmatch(raw):
        return 300
    seconds = int(raw)
    return min(900, max(30, seconds)) if -1 - _MAX_I64 <= seconds <= _MAX_I64 else 300


class GmRequestVerifier:
    def __init__(self, secret: bytes | None = None, *, clock: Callable[[], float] = time.time) -> None:
        self.secret, self.clock = secret, clock
        self._seen: OrderedDict[str, int] = OrderedDict()

    def verify(self, method: str, node_id: int, field: str, reason: str) -> tuple[str | None, str]:
        secret = self.secret if self.secret is not None else os.getenv("GATE_GM_ADMIN_SECRET", "").encode()
        if not secret.strip(_ASCII_SPACE.encode()):
            return None, "secret_not_configured"
        parts = field.split("|", 4)
        if len(parts) != 4:
            return None, "malformed_envelope"
        operator, timestamp_raw, nonce, signature = parts
        if (
            not 1 <= len(operator.encode()) <= 64 or not 1 <= len(nonce.encode()) <= 128
            or "\n" in operator or "\r" in operator or "\n" in nonce or "\r" in nonce
            or not _UNIX_DIGITS.fullmatch(timestamp_raw) or not _LOWER_HEX.fullmatch(signature)
        ):
            return None, "malformed_envelope"
        timestamp = int(timestamp_raw)
        if timestamp > _MAX_I64:
            return None, "malformed_envelope"
        now, skew = int(self.clock()), _gm_skew()
        if abs(timestamp - now) > skew:
            return None, "timestamp_out_of_window"
        canonical = "\n".join((method, str(node_id), operator, timestamp_raw, nonce, reason)).encode()
        if not hmac.compare_digest(hmac.digest(secret, canonical, "sha256").hex(), signature):
            return None, "signature_mismatch"
        while self._seen and next(iter(self._seen.values())) <= now:
            self._seen.popitem(last=False)
        if nonce in self._seen or len(self._seen) >= _MAX_NONCES:
            return None, "replayed_nonce"
        # 时间窗两端均包含，保留到最晚可接受的那一秒结束。
        self._seen[nonce] = now + 2 * skew + 1
        return operator, "ok"


@dataclass(slots=True)
class _ScenePeer:
    channel: RpcChannel
    address: str
    node: Message | None = None
    checked_at: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _ip(text: str):
    address = ipaddress.ip_address(text.split("%", 1)[0])
    return getattr(address, "ipv4_mapped", None) or address


async def _source_matches(host: str, address: str) -> bool:
    if not host or not address:
        return False
    source = _ip(address)
    try:
        return _ip(host) == source
    except ValueError:
        records = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
        return any(_ip(item[4][0]) == source for item in records)


class GateRpcService:
    def __init__(
        self, gate, directory, on_shutdown: Callable[[], None], *, gm_secret: bytes | None = None,
        max_connections: int = 128, handshake_timeout: float = 3.0,
    ) -> None:
        if max_connections < 1 or handshake_timeout <= 0:
            raise ValueError("Gate RPC0 connection limits must be positive")
        self.gate, self.directory, self.on_shutdown = gate, directory, on_shutdown
        self.protocol = gate.protocol
        self.gm_auth = GmRequestVerifier(gm_secret)
        self.max_connections, self.handshake_timeout = max_connections, handshake_timeout
        self._envelope = self.protocol.message_class("GameRpcMessage")
        self._channels: set[RpcChannel] = set()
        self._scenes: dict[int, _ScenePeer] = {}
        self._closing = self._shutdown_requested = False

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._closing or self._shutdown_requested or len(self._channels) >= self.max_connections:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            return

        async def handshake(body: bytes) -> bytes:
            return await self._handshake(peer, _parse(self.protocol, "NodeHandshakeRequest", body))

        async def push(body: bytes) -> bytes:
            source = await self._authorize(peer)
            request = _parse(self.protocol, "NodeRouteMessageRequest", body)
            if not _session_on_gate(request.header.session_id, self.gate.info.node_id):
                raise RpcError(RpcErrorCode.INVALID_REQUEST)
            if request.HasField("message_content"):
                self.gate.push(request, scene_uuid=source.node_uuid)
            return b""

        async def broadcast(method: str, body: bytes) -> bytes:
            await self._authorize(peer)
            self.gate.broadcast(method, _parse(self.protocol, method + "Request", body))
            return b""

        ids = self.protocol.message_ids
        handlers = {
            ids["GateNodeHandshake"]: handshake,
            ids["GateSendMessageToPlayer"]: push,
            ids["GateGmGracefulShutdown"]: self._shutdown,
            **{ids["Gate" + method]: partial(broadcast, method) for method in GATE_BROADCAST_METHODS},
        }
        channel = RpcChannel(reader, writer, self._envelope, handlers, heartbeat=_HEARTBEAT, max_inbound=32,
                             deny=lambda mid: bool(getattr(self.gate, "kill_switch", None)
                                                   and self.gate.kill_switch.check_id(mid)))
        address = writer.get_extra_info("peername")
        peer = _ScenePeer(channel, address[0] if address else "")
        self._channels.add(channel)

        def expire_handshake() -> None:
            if peer.node is None and not self._shutdown_requested:
                channel.abort()

        timer = asyncio.get_running_loop().call_later(self.handshake_timeout, expire_handshake)
        try:
            await channel.wait_closed()
        finally:
            timer.cancel()
            if peer.node is not None and self._scenes.get(peer.node.node_id) is peer:
                del self._scenes[peer.node.node_id]
            self._channels.discard(channel)
            await channel.aclose()

    async def _lookup(self, node):
        authority = await self.directory.lookup(SCENE, self.gate.info.zone_id, node.node_id)
        if (authority is None or _identity(authority) != _identity(node)
                or not 0 < authority.endpoint.port < 65536 or not authority.endpoint.ip):
            return None
        return authority

    async def _handshake(self, peer: _ScenePeer, request) -> bytes:
        node = request.self_node
        reply = self.protocol.message("NodeHandshakeResponse", peer_node=self.gate.info)
        reply.error_message.id = _REGISTRATION_FAILED
        if (
            request.expected_peer_uuid != self.gate.info.node_uuid or not self.gate.info.node_uuid
            or node.node_type != SCENE or node.zone_id != self.gate.info.zone_id
            or not node.node_uuid or not 0 < node.node_id < 1 << 17
        ):
            return reply.SerializeToString()
        async with peer.lock:
            if peer.node is not None and _identity(peer.node) != _identity(node):
                return reply.SerializeToString()
            try:
                async with asyncio.timeout(self.handshake_timeout):
                    authority = await self._lookup(node)
                    verified = (authority is not None and node.endpoint == authority.endpoint
                                and await _source_matches(authority.endpoint.ip, peer.address))
            except (OSError, ValueError, RuntimeError, grpc.RpcError):
                verified = False
            if not verified:
                return reply.SerializeToString()
            if self._closing or self._shutdown_requested or peer.channel.closed:
                raise RpcError(RpcErrorCode.NO_SERVICE)
            previous = self._scenes.get(node.node_id)
            peer.node = type(authority).FromString(authority.SerializeToString())
            peer.checked_at = asyncio.get_running_loop().time()
            self._scenes[node.node_id] = peer
            if previous is not None and previous is not peer:
                previous.channel.abort()
            reply.error_message.id = 0
        return reply.SerializeToString()

    async def _authorize(self, peer: _ScenePeer) -> Message:
        async with peer.lock:
            node = peer.node
            if self._closing or self._shutdown_requested or node is None or self._scenes.get(node.node_id) is not peer:
                raise RpcError(RpcErrorCode.NO_SERVICE)
            now = asyncio.get_running_loop().time()
            if now - peer.checked_at >= _DISCOVERY_REFRESH:
                try:
                    authority = await self._lookup(node)
                except (OSError, ValueError, RuntimeError, grpc.RpcError) as exc:
                    raise RpcError(RpcErrorCode.NO_SERVICE) from exc
                if (self._closing or self._shutdown_requested or authority is None or authority.endpoint != node.endpoint
                        or self._scenes.get(node.node_id) is not peer):
                    raise RpcError(RpcErrorCode.NO_SERVICE)
                peer.checked_at = now
            return node

    async def _shutdown(self, body: bytes) -> RpcReply:
        request = _parse(self.protocol, "GmGracefulShutdownRequest", body)
        operator, result = self.gm_auth.verify(
            _GM_METHOD, self.gate.info.node_id, getattr(request, "operator"), request.reason,
        )
        if operator is None:
            logger.warning("GM graceful shutdown rejected: %s", result)
            raise RpcError(RpcErrorCode.HANDLER_FAILED)
        if self._closing or self._shutdown_requested:
            raise RpcError(RpcErrorCode.NO_SERVICE)
        self._shutdown_requested = True
        sessions = [session for session in self.gate.sessions.values() if not session.writer.is_closing()]
        for session in sessions:
            session.writer.transport.abort()
        logger.info("GM graceful shutdown requested by %r, reason=%r, sessions=%d", operator, request.reason[:128], len(sessions))
        response = self.protocol.message("GmGracefulShutdownResponse", affected_count=len(sessions))
        return RpcReply(response.SerializeToString(), self.on_shutdown)

    async def close(self) -> None:
        self._closing = True
        await asyncio.gather(*(channel.aclose() for channel in tuple(self._channels)))
        self._channels.clear()
        self._scenes.clear()
