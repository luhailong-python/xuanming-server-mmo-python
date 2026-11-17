"""Direct Gate calls to the original Go business services."""

import asyncio
import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field

import grpc
from google.protobuf.message import DecodeError

from ..grpc_runtime import GrpcClient, session_metadata
from ..protocol import RpcKind
from .gate_login_renewal import RENEW_PATH

logger = logging.getLogger(__name__)

_LOGIN = "loginpb.ClientPlayerLogin"
_MATCH = "match.MatchService"
# The native direct Gate connects only Login and Match among the public Go services.
_TARGET_NAMES = {
    _LOGIN: "LoginNodeService",
    _MATCH: "MatchNodeService",
}
_DEADLINES = {_LOGIN: 12, _MATCH: 7}
_UNAVAILABLE = 1003
_PARSE_ERROR = 1014


@dataclass(slots=True, eq=False)
class _Peer:
    address: str
    track: str
    client: GrpcClient | None = None
    in_flight: int = 0
    retired: bool = False


@dataclass(slots=True)
class _Scope:
    expires: float = 0
    available: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    peers: dict[tuple[str, str], _Peer] = field(default_factory=dict)


class DirectForwarder:
    def __init__(self, protocol, directory, secret, *, client_factory=GrpcClient,
                 discovery_ttl: float = 1) -> None:
        self.protocol = protocol
        self.directory = directory
        self.secret = secret
        self._client_factory = client_factory
        self._discovery_ttl = discovery_ttl
        node_types = protocol.pool.FindEnumTypeByName("common.base.eNodeType").values_by_name
        self._target_types = {service: node_types[name].number for service, name in _TARGET_NAMES.items()}
        self._scopes: dict[tuple[int, int | None], _Scope] = defaultdict(_Scope)
        self._retired: set[_Peer] = set()
        self._closed = False

    def validate_request(self, route, request):
        return self.protocol.parse(route.request_type, request.body)

    async def renew_login(self, gate, session):
        peer = await self._select(gate.info, _LOGIN)
        if peer is None:
            raise RuntimeError("Login 节点不可用")
        try:
            details = gate.details(session, 0).SerializeToString()
            metadata = session_metadata(details, RENEW_PATH, self.secret)
            return await peer.client.call(RENEW_PATH, b"", metadata=metadata)
        finally:
            peer.in_flight -= 1
            if peer.retired and not peer.in_flight:
                await self._close_peer(peer)

    async def request_migration(self, gate, session):
        route = self.protocol.routes[self.protocol.message_ids["ClientPlayerLoginRequestGateMigration"]]
        peer = await self._select(gate.info, _LOGIN)
        if peer is None:
            raise RuntimeError("Login 节点不可用")
        try:
            details = gate.details(session, 0).SerializeToString()
            return await peer.client.call(route.path, b"", metadata=session_metadata(details, route.path, self.secret))
        finally:
            peer.in_flight -= 1
            if peer.retired and not peer.in_flight:
                await self._close_peer(peer)

    async def __call__(self, gate, session, route, request, seq):
        def error(code):
            return self.protocol.content(request.message_id, request.id, error=code) if request.id else None

        service = route.service
        disconnect = service == _LOGIN and route.method == "Disconnect" and not request.id
        if (self._closed or not route.client_protocol
                or (not disconnect and (route.kind != RpcKind.REQUEST or not request.id))):
            return error(_UNAVAILABLE)
        try:
            typed = self.validate_request(route, request)
        except DecodeError:
            return error(_PARSE_ERROR)
        if service not in self._target_types:
            return error(_UNAVAILABLE)

        peer = None
        try:
            peer = await self._select(gate.info, service)
            if peer is None:
                return error(_UNAVAILABLE)
            # The native direct path correlates replies on the call itself.
            details = gate.details(session, 0).SerializeToString()
            metadata = session_metadata(details, route.path, self.secret)
            if service == _LOGIN and route.method == "Login":
                session.login_started = True
            data = await peer.client.call(route.path, typed.SerializeToString(), metadata=metadata)
            response = self.protocol.parse(route.response_type, data)
            if disconnect:
                return None
            return self.protocol.content(request.message_id, request.id, body=response.SerializeToString())
        except (grpc.RpcError, OSError, ValueError, DecodeError, RuntimeError):
            logger.debug("Gate 直连请求失败 message_id=%s", request.message_id, exc_info=True)
            return error(_UNAVAILABLE)
        finally:
            if peer is not None:
                peer.in_flight -= 1
                if peer.retired and not peer.in_flight:
                    await self._close_peer(peer)

    async def _select(self, gate, service: str) -> _Peer | None:
        node_type = self._target_types[service]
        zone = None if service == _MATCH else gate.zone_id
        scope = self._scopes[node_type, zone]
        async with scope.lock:
            if scope.expires <= time.monotonic():
                await self._refresh(scope, node_type, zone)
            if self._closed or not scope.available:
                return None
            candidates = list(scope.peers.values())
            if service != _LOGIN:
                caller_track = gate.release_track or "stable"
                preferred = [peer for peer in candidates if peer.track == caller_track]
                if not preferred and caller_track == "canary":
                    preferred = [peer for peer in candidates if peer.track == "stable"]
                candidates = preferred
            if not candidates:
                return None
            peer = random.choice(candidates)
            if peer.client is None:
                peer.client = self._client_factory(
                    peer.address, timeout=_DEADLINES[service], enable_retries=False,
                )
            peer.in_flight += 1
            return peer

    async def _refresh(self, scope: _Scope, node_type: int, zone: int | None) -> None:
        try:
            nodes = await self.directory.candidates(node_type, zone)
        except (grpc.RpcError, OSError, ValueError, RuntimeError):
            scope.available = False
            scope.expires = time.monotonic() + self._discovery_ttl
            raise
        if self._closed:
            return
        previous = scope.peers
        peers = {}
        for node in nodes:
            endpoint = node.grpc_endpoint
            if (node.node_type != node_type or (zone is not None and node.zone_id != zone)
                    or not node.node_uuid or node.protocol_type != 1
                    or not endpoint.ip or not 0 < endpoint.port <= 65535):
                continue
            ip = endpoint.ip
            address = f"[{ip}]:{endpoint.port}" if ":" in ip else f"{ip}:{endpoint.port}"
            key = node.node_uuid, address
            track = node.release_track or "stable"
            peer = previous.get(key)
            if peer is None:
                peer = _Peer(address, track)
            else:
                peer.track = track
            peers[key] = peer
        scope.peers = peers
        scope.expires = time.monotonic() + self._discovery_ttl
        scope.available = True
        removed = [previous[key] for key in previous.keys() - peers.keys()]
        for peer in removed:
            peer.retired = True
        self._retired.update(removed)
        for peer in removed:
            if not peer.in_flight:
                await self._close_peer(peer)

    async def _close_peer(self, peer: _Peer) -> None:
        self._retired.discard(peer)
        client, peer.client = peer.client, None
        if client is not None:
            await client.close()

    async def close(self) -> None:
        self._closed = True
        scopes, self._scopes = self._scopes, defaultdict(_Scope)
        peers = {peer for scope in scopes.values() for peer in scope.peers.values()}
        await asyncio.gather(*(self._close_peer(peer) for peer in peers | self._retired))
