"""Gate 拨号、Scene 接受及同一条 RPC0 链路上的反向推送。"""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from functools import partial
from inspect import isawaitable
import logging

from google.protobuf.message import DecodeError, Message

from .cluster import GATE, SCENE
from .protocol import Protocol
from .transport import FrameError, RpcChannel, RpcDisconnected, RpcError, RpcErrorCode, RpcLinkHeartbeat

_LOG = logging.getLogger(__name__)
_HEARTBEAT = RpcLinkHeartbeat()
_HANDSHAKE_TIMEOUT = 3.0
_SCENE_CALL_TIMEOUT = 12.0
_DISCOVERY_REFRESH = 1.0
_SESSION_SEQUENCE_BITS = 17
_INVALID_SESSION = (1 << 32) - 1
_FAILED_NODE_REGISTRATION = 1018
_MAX_QUEUED_PUSH_BYTES = 2 * 1024 * 1024
GATE_BROADCAST_METHODS = ("BroadcastToPlayers", "BroadcastToScene", "BroadcastToAll")


class SceneNodeNotFound(RpcDisconnected):
    """权威发现中没有目标节点。"""


class SceneLinkNotReady(RpcDisconnected):
    """连接或握手未就绪，业务帧尚未发送。"""


def _identity(node: Message) -> tuple[int, int, int, str]:
    return node.node_type, node.zone_id, node.node_id, node.node_uuid


def _session_on_gate(session_id: int, gate_id: int) -> bool:
    return 0 < session_id < _INVALID_SESSION and session_id >> _SESSION_SEQUENCE_BITS == gate_id


def _parse(protocol: Protocol, name: str, data: bytes) -> Message:
    try:
        return protocol.parse(name, data)
    except DecodeError as exc:
        raise RpcError(RpcErrorCode.INVALID_REQUEST) from exc


@dataclass(slots=True)
class _SceneConnection:
    info: Message
    channel: RpcChannel
    checked_at: float


class GateLinks:
    """按区和节点号发现 Scene，校验实例身份后才转发业务请求。"""

    def __init__(self, protocol: Protocol, gate_info: Message, directory, on_push, *,
                 source_aware: bool = False, on_broadcast=None) -> None:
        self._protocol = protocol
        self._info = gate_info
        self._directory = directory
        self._on_push = on_push
        self._on_broadcast = on_broadcast
        self._source_aware = source_aware
        self._envelope = protocol.message_class("GameRpcMessage")
        self._handshake_id = protocol.message_ids["SceneNodeHandshake"]
        self._push_id = protocol.message_ids["GateSendMessageToPlayer"]
        self._links: dict[int, _SceneConnection] = {}
        self._locks: dict[int, asyncio.Lock] = {}
        self._channels: set[RpcChannel] = set()
        self._closing = False

    async def call(
        self,
        node_id: int,
        message_id: int,
        protobuf: Message,
        *,
        one_way: bool = False,
        expected_uuid: str | None = None,
        timeout: float = _SCENE_CALL_TIMEOUT,
    ) -> bytes:
        route = self._protocol.routes.get(message_id)
        if route is None or route.service != "Scene" or route.request_type != protobuf.DESCRIPTOR.full_name:
            raise ValueError("Scene RPC 的消息号与请求类型不匹配")
        if message_id == self._handshake_id or not 0 < node_id < (1 << 17):
            raise ValueError("Scene RPC 的节点号或方法无效")
        channel = await self._link(node_id, expected_uuid)
        body = protobuf.SerializeToString()
        if one_way:
            await channel.send(message_id, body)
            return b""
        return await channel.call(message_id, body, timeout=timeout)

    async def identity(self, node_id: int) -> str:
        if not 0 < node_id < (1 << 17):
            raise ValueError("Scene 节点号无效")
        await self._link(node_id, None)
        return self._links[node_id].info.node_uuid

    async def prepare_entry(self, node_id: int) -> _SceneConnection:
        if not 0 < node_id < (1 << 17):
            raise ValueError("Scene 节点号无效")
        try:
            await self._link(node_id, None)
        except SceneNodeNotFound:
            raise
        except Exception as exc:
            raise SceneLinkNotReady("Scene 节点链路未就绪") from exc
        return self._links[node_id]

    async def discover_entry(self, node_id: int) -> None:
        """登录绑定尚未到达，只确认权威节点存在，不拨号或发包。"""
        await self._discover(node_id)

    async def send_entry(self, link: _SceneConnection, request: Message, *, timeout: float, can_send) -> None:
        await link.channel.call(
            self._protocol.message_ids["ScenePlayerEnterGameNode"], request.SerializeToString(),
            timeout=timeout, can_send=can_send,
        )

    async def _discover(self, node_id: int) -> Message:
        if not 0 < node_id < (1 << 17):
            raise ValueError("Scene 节点号无效")
        remote = await self._directory.lookup(SCENE, self._info.zone_id, node_id)
        if (remote is None or not remote.node_uuid
                or _identity(remote)[:3] != (SCENE, self._info.zone_id, node_id)
                or not remote.endpoint.ip or not 0 < remote.endpoint.port < 65536):
            raise SceneNodeNotFound("Scene 节点未登记或注册地址无效")
        return remote

    async def _link(self, node_id: int, expected_uuid: str | None) -> RpcChannel:
        if self._closing:
            raise RpcDisconnected("Gate 节点链路已关闭")
        lock = self._locks.setdefault(node_id, asyncio.Lock())
        async with lock:
            if self._closing:
                raise RpcDisconnected("Gate 节点链路已关闭")
            loop = asyncio.get_running_loop()
            previous = self._links.get(node_id)
            if (
                previous is not None and not previous.channel.closed
                and loop.time() - previous.checked_at < _DISCOVERY_REFRESH
                and (not expected_uuid or expected_uuid == previous.info.node_uuid)
            ):
                return previous.channel
            try:
                remote = await self._discover(node_id)
            except SceneNodeNotFound:
                await self._forget(node_id)
                raise
            if self._closing:
                raise RpcDisconnected("Gate 节点链路已关闭")
            if previous is not None and (
                previous.channel.closed or _identity(previous.info) != _identity(remote)
                or previous.info.endpoint != remote.endpoint
            ):
                await self._forget(node_id)
                previous = None
            if expected_uuid and expected_uuid != remote.node_uuid:
                raise RpcDisconnected("Scene 实例已更换")
            if previous is not None:
                previous.checked_at = loop.time()
                return previous.channel
            snapshot = type(remote)()
            snapshot.CopyFrom(remote)
            channel = await self._connect(snapshot)
            self._links[node_id] = _SceneConnection(snapshot, channel, loop.time())
            return channel

    async def _forget(self, node_id: int) -> None:
        previous = self._links.pop(node_id, None)
        if previous is not None:
            await previous.channel.aclose()
            self._channels.discard(previous.channel)

    async def _connect(self, remote: Message) -> RpcChannel:
        ready = False
        push_lock = asyncio.Lock()

        async def push(body: bytes) -> bytes:
            request = _parse(self._protocol, "NodeRouteMessageRequest", body)
            if not request.HasField("message_content") or not _session_on_gate(request.header.session_id, self._info.node_id):
                raise RpcError(RpcErrorCode.INVALID_REQUEST)
            async with push_lock:
                if not ready or channel.closed:
                    raise RpcError(RpcErrorCode.NO_SERVICE)
                result = (self._on_push(request, scene_uuid=remote.node_uuid) if self._source_aware
                          else self._on_push(request))
                if isawaitable(result):
                    await result
            return b""

        async def broadcast(method: str, body: bytes) -> bytes:
            request = _parse(self._protocol, method + "Request", body)
            async with push_lock:
                if not ready or channel.closed:
                    raise RpcError(RpcErrorCode.NO_SERVICE)
                result = self._on_broadcast(method, request)
                if isawaitable(result):
                    await result
            return b""

        handlers = {self._push_id: push}
        if self._on_broadcast is not None:
            handlers.update({self._protocol.message_ids["Gate" + method]: partial(broadcast, method)
                             for method in GATE_BROADCAST_METHODS})
        async with asyncio.timeout(_HANDSHAKE_TIMEOUT):
            reader, writer = await asyncio.open_connection(remote.endpoint.ip, remote.endpoint.port)
        channel = RpcChannel(reader, writer, self._envelope, handlers, heartbeat=_HEARTBEAT)
        self._channels.add(channel)
        try:
            if self._closing:
                raise RpcDisconnected("Gate 节点链路已关闭")
            request = self._protocol.message(
                "NodeHandshakeRequest", self_node=self._info, expected_peer_uuid=remote.node_uuid,
            )
            body = await channel.call(self._handshake_id, request.SerializeToString(), timeout=_HANDSHAKE_TIMEOUT)
            reply = self._protocol.parse("NodeHandshakeResponse", body)
            if reply.error_message.id or _identity(reply.peer_node) != _identity(remote):
                raise RpcDisconnected("Scene 实例握手校验失败")
            if self._closing or channel.closed:
                raise RpcDisconnected("Scene 节点链路已关闭")
            ready = True
            return channel
        finally:
            if not ready:
                await channel.aclose()
                self._channels.discard(channel)

    async def close(self) -> None:
        self._closing = True
        await asyncio.gather(*(channel.aclose() for channel in self._channels))
        self._channels.clear()
        self._links.clear()


@dataclass(slots=True)
class _SessionLane:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0


@dataclass(slots=True)
class _GatePeer:
    channel: RpcChannel
    info: Message | None = None
    queued_bytes: int = 0
    outgoing: asyncio.Queue[tuple[int, bytes]] = field(default_factory=lambda: asyncio.Queue(1024))
    lanes: dict[int, _SessionLane] = field(default_factory=dict)
    handshake: asyncio.Lock = field(default_factory=asyncio.Lock)


class SceneLinks:
    """接收入站 Gate 握手，按实例 UUID 保存可反向调用的连接。"""

    def __init__(self, protocol: Protocol, scene_info: Message, directory=None) -> None:
        self._protocol = protocol
        self._info = scene_info
        self._directory = directory
        self._envelope = protocol.message_class("GameRpcMessage")
        self._push_id = protocol.message_ids["GateSendMessageToPlayer"]
        self._broadcast_players_id = protocol.message_ids["GateBroadcastToPlayers"]
        self._peers: dict[str, _GatePeer] = {}
        self._channels: set[RpcChannel] = set()
        self._scene = None
        self._admin = None
        self._closing = False

    def bind(self, scene, *, admin=None) -> None:
        self._scene = scene
        self._admin = admin

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._scene is None or self._closing:
            writer.close()
            await writer.wait_closed()
            return

        async def handshake(body: bytes) -> bytes:
            request = _parse(self._protocol, "NodeHandshakeRequest", body)
            async with peer.handshake:
                return await self._handshake(peer, request)

        async def enter(body: bytes) -> bytes:
            request = _parse(self._protocol, "PlayerEnterGameNodeRequest", body)
            async with self._session_lane(peer, request.session_id) as gate:
                return (await self._scene.enter(request, gate.node_uuid)).SerializeToString()

        async def process(body: bytes) -> bytes:
            request = _parse(self._protocol, "ProcessClientPlayerMessageRequest", body)
            async with self._session_lane(peer, request.session_id) as gate:
                return (await self._scene.process(request, gate.node_uuid)).SerializeToString()

        async def create(body: bytes) -> bytes:
            self._gate(peer)
            return self._scene.create(_parse(self._protocol, "CreateSceneRequest", body)).SerializeToString()

        async def destroy(body: bytes) -> bytes:
            self._gate(peer)
            return self._scene.destroy(_parse(self._protocol, "DestroySceneRequest", body)).SerializeToString()

        async def administrative(method, body: bytes):
            return await method(body, on_authorized=timer.cancel)

        ids = self._protocol.message_ids
        handlers = {
            ids["SceneNodeHandshake"]: handshake,
            ids["ScenePlayerEnterGameNode"]: enter,
            ids["SceneProcessClientPlayerMessage"]: process,
            ids["SceneCreateScene"]: create,
            ids["SceneDestroyScene"]: destroy,
        }
        if self._admin is not None:
            handlers.update({
                ids["SceneSceneGmGracefulShutdown"]: partial(administrative, self._admin.shutdown),
                ids["SceneSceneGmDescribeEntity"]: partial(administrative, self._admin.describe),
            })
        channel = RpcChannel(reader, writer, self._envelope, handlers, heartbeat=_HEARTBEAT)
        peer = _GatePeer(channel)
        self._channels.add(channel)

        def refuse_idle_handshake() -> None:
            if peer.info is None:
                channel.abort()

        timer = asyncio.get_running_loop().call_later(_HANDSHAKE_TIMEOUT, refuse_idle_handshake)
        sender = asyncio.create_task(self._send_pushes(peer), name="scene-gate-pushes")
        try:
            await channel.wait_closed()
        finally:
            timer.cancel()
            self._channels.discard(channel)
            if peer.info is not None and self._peers.get(peer.info.node_uuid) is peer:
                del self._peers[peer.info.node_uuid]
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)
            await channel.aclose()

    async def _handshake(self, peer: _GatePeer, request: Message) -> bytes:
        reply = self._protocol.message("NodeHandshakeResponse", peer_node=self._info)
        gate = request.self_node
        if (
            request.expected_peer_uuid != self._info.node_uuid or not request.expected_peer_uuid
            or gate.node_type != GATE or gate.zone_id != self._info.zone_id
            or not 0 < gate.node_id < (1 << (32 - _SESSION_SEQUENCE_BITS)) or not gate.node_uuid
            or peer.info is not None and _identity(peer.info) != _identity(gate)
        ):
            reply.error_message.id = _FAILED_NODE_REGISTRATION
            return reply.SerializeToString()
        if self._directory is not None:
            authority = await self._directory.lookup(GATE, gate.zone_id, gate.node_id)
            if authority is None or _identity(authority) != _identity(gate):
                reply.error_message.id = _FAILED_NODE_REGISTRATION
                return reply.SerializeToString()
        if self._closing or peer.channel.closed:
            raise RpcError(RpcErrorCode.NO_SERVICE)
        previous = self._peers.get(gate.node_uuid)
        peer.info = gate
        self._peers[gate.node_uuid] = peer
        if previous is not None and previous is not peer:
            previous.channel.abort()
        reply.error_message.id = 0
        return reply.SerializeToString()

    def _gate(self, peer: _GatePeer, session_id: int | None = None) -> Message:
        gate = peer.info
        if gate is None or self._peers.get(gate.node_uuid) is not peer:
            raise RpcError(RpcErrorCode.NO_SERVICE)
        if session_id is not None and not _session_on_gate(session_id, gate.node_id):
            raise RpcError(RpcErrorCode.INVALID_REQUEST)
        return gate

    @asynccontextmanager
    async def _session_lane(self, peer: _GatePeer, session_id: int):
        self._gate(peer, session_id)
        lane = peer.lanes.get(session_id)
        if lane is None:
            lane = peer.lanes[session_id] = _SessionLane()
        lane.users += 1
        try:
            async with lane.lock:
                yield self._gate(peer, session_id)
        finally:
            lane.users -= 1
            if not lane.users:
                del peer.lanes[session_id]

    def emit(self, player, content: Message) -> None:
        peer = self._peers.get(player.gate_uuid)
        if peer is None or peer.channel.closed:
            return
        request = self._protocol.message(
            "NodeRouteMessageRequest", message_content=content,
            header={"session_id": player.session_id, "target_player_id": player.player_id},
        )
        self._queue_push(peer, self._push_id, request.SerializeToString())

    def emit_many(self, players, content: Message) -> None:
        by_gate: dict[str, dict[int, int]] = {}
        for player in players:
            peer = self._peers.get(player.gate_uuid)
            if (peer is not None and peer.info is not None and not peer.channel.closed and player.player_id
                    and _session_on_gate(player.session_id, peer.info.node_id)):
                by_gate.setdefault(player.gate_uuid, {}).setdefault(player.session_id, player.player_id)
        for gate_uuid, targets in by_gate.items():
            request = self._broadcast_to_players(content, targets)
            self._queue_push(self._peers[gate_uuid], self._broadcast_players_id, request.SerializeToString())

    def _broadcast_to_players(self, content: Message, targets: dict[int, int]) -> Message:
        sessions = sorted(targets)
        size = (sessions[-1] - sessions[0] + 8) // 8
        request = self._protocol.message(
            "BroadcastToPlayersRequest", message_content=content, player_list=[targets[session] for session in sessions],
        )
        # The gate prefix fences the span to the native 17-bit session sequence.
        if len(sessions) >= 32 and size + 6 < len(sessions) * 3:
            base = sessions[0]
            bitmap = bytearray(size)
            for session in sessions:
                index, bit = divmod(session - base, 8)
                bitmap[index] |= 1 << bit
            request.session_bitmap_base = base
            request.session_bitmap = bytes(bitmap)
        else:
            request.session_list.extend(sessions)
        return request

    @staticmethod
    def _queue_push(peer: _GatePeer, message_id: int, body: bytes) -> None:
        if peer.channel.closed:
            return
        if peer.outgoing.full() or peer.queued_bytes + len(body) > _MAX_QUEUED_PUSH_BYTES:
            _LOG.warning("Gate 回推积压超过上限，关闭节点链路 gate_id=%d", peer.info.node_id)
            peer.channel.abort()
            return
        peer.outgoing.put_nowait((message_id, body))
        peer.queued_bytes += len(body)

    async def _send_pushes(self, peer: _GatePeer) -> None:
        try:
            while True:
                message_id, body = await peer.outgoing.get()
                await peer.channel.send(message_id, body)
                peer.queued_bytes -= len(body)
        except (FrameError, RpcDisconnected):
            peer.channel.abort()

    async def close(self) -> None:
        self._closing = True
        await asyncio.gather(*(channel.aclose() for channel in self._channels))
