import asyncio
import hmac
import logging
import os
import secrets
import time
from collections import deque
from dataclasses import dataclass, field
from functools import cache
from itertools import count

import grpc
from google.protobuf.message import DecodeError

from ..data.config_tables import ConfigTables
from ..grpc_runtime import session_metadata
from ..node_link import GATE_BROADCAST_METHODS, SceneLinkNotReady, SceneNodeNotFound
from ..protocol import RouteNode, RpcKind
from ..transport import (
    FrameError, Packet, RpcError, WebSocketPacketReader, WebSocketPacketWriter,
    encode_packet, read_packet, write_packet,
)
from ..transport.client_limits import illegal_packet_threshold as _illegal_packet_threshold
from ..transport.client_output import ClientOutputQueue
from .gate_chat import WorldChatFanout
from .gate_drain_migration import GateDrainMigration
from .gate_entry import FORCE_CLOSE_DELAY, PendingSceneEntry, retry_entry
from .gate_login_renewal import LoginRenewal, RENEW_ID, RENEW_PATH
from .gate_scene_lane import SceneLane

logger = logging.getLogger(__name__)
HEARTBEAT_INTERVAL = 10
HEARTBEAT_TIMEOUT = 30
ROUTER_LANE_DEPTH = 32
ROUTER_LANE_MAX_WAIT = 5.0
SESSION_MASK = (1 << 17) - 1
ROUTER_PATH = "/client_rpc_router.ClientRpcRouter/Forward"
UNAVAILABLE = 1003
INVALID_PARAMETER = 1005
FEATURE_UNAVAILABLE = 1006
RATE_LIMIT_EXCEEDED = 1008
MESSAGE_SIZE_EXCEEDED = 1010
REQUEST_PARSE_ERROR = 1014
LOGIN_IN_PROGRESS = 2005
LOGIN_SESSION_NOT_FOUND = 2028
PYTHON_EXTENSION_ID_MIN = 10_000
LOGIN_SERVICE = "loginpb.ClientPlayerLogin"
_GM_CLIENT_MESSAGES = (
    "SceneCurrencyClientPlayerGmAddCurrency", "SceneCurrencyClientPlayerGmDeductCurrency",
    "SceneCurrencyClientPlayerGmBlockCurrency", "SceneCurrencyClientPlayerGmUnblockCurrency",
    "SceneAttributeClientPlayerGmSetPlayerLevel", "ScenePetClientPlayerGmGrantPet",
    "SceneBagClientPlayerGmGrantItem",
)


@cache
def _native_message_limits():
    return {row.id: (row.max_requests, row.time_window) for row in ConfigTables()["MessageLimiter"].rows()}


def issue_gate_ticket(protocol, node_id: int, zone_id: int, secret: bytes, *, ttl: int = 600, player_id: int = 0):
    payload = protocol.message(
        "GateTokenPayload", gate_node_id=node_id, zone_id=zone_id,
        expire_timestamp=int(time.time()) + ttl, player_id=player_id,
        hmac_session_key=secrets.token_bytes(32),
    ).SerializeToString()
    return protocol.message(
        "ClientTokenVerifyRequest", payload=payload,
        signature=hmac.new(secret, payload, "sha256").hexdigest().encode(),
    )


@dataclass(slots=True)
class RouterLaneItem:
    sequence: int
    request: object
    admitted_identity: tuple | None
    details: object
    admitted_at: float
    leave_barrier: bool = False


@dataclass(slots=True)
class Session:
    session_id: int
    incarnation: int
    writer: asyncio.StreamWriter
    outbound: ClientOutputQueue = field(default_factory=ClientOutputQueue)
    requests: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(32))
    message_timestamps: dict[int, deque[float]] = field(default_factory=dict)
    illegal_packets: int = 0
    player_id: int = 0
    version: int = 0
    login_generation: int = 0
    owner_revision: int = 0
    account: str = ""
    ticket_player_id: int = 0
    ticket_target_zone_id: int = 0
    scene_id: int = 0
    scene_node_id: int = 0
    scene_instance_id: str = ""
    owner_epoch: int = 0
    left_player_id: int = 0
    left_generation: int = 0
    enter_type: int = 0
    pending_route: PendingSceneEntry | None = None
    last_scene_route: object = None
    entry_failed: bool = False
    routing: asyncio.Lock = field(default_factory=asyncio.Lock)
    verified: bool = False
    login_started: bool = False
    logged_in: bool = False
    enter_requested: bool = False
    enter_player_id: int = 0
    redirected: bool = False
    lease_expired: bool = False
    lane_busy: bool = False
    queued_leave: int = 0
    direct_tasks: set = field(default_factory=set)
    direct_sent: set = field(default_factory=set)
    direct_exit: object = None
    scene_lane: object = None
    last_enter_at: float | None = None

    def close(self) -> None:
        self.writer.close()

    @property
    def buffered_output_bytes(self) -> int:
        transport = getattr(self.writer, "transport", None)
        return self.outbound.buffered_bytes + (transport.get_write_buffer_size() if transport is not None else 0)

    def emit(self, message) -> None:
        if self.writer.is_closing():
            return
        try:
            self.outbound.put_nowait(message)
        except (asyncio.QueueFull, FrameError):
            self.close()


class RouterForwarder:
    def __init__(self, protocol, client, secret) -> None:
        self.protocol = protocol
        self.client = client
        self.secret = secret

    async def ready_for_leave(self) -> bool:
        """A timeout here is definitely unsent: no business call exists yet."""
        try:
            async with asyncio.timeout(self.client.timeout):
                await self.client.channel.channel_ready()
            return True
        except TimeoutError:
            return False

    async def __call__(self, gate, session, route, request, seq, *, details=None):
        forward = self.protocol.message(
            "client_rpc_router.ForwardRequest", request=request, zone_id=gate.info.zone_id,
            release_track=gate.info.release_track,
        )
        subject = (details if details is not None else gate.details(session, seq)).SerializeToString()
        metadata = session_metadata(subject, route.path, self.secret)
        call = self.client.request(ROUTER_PATH, forward.SerializeToString(), metadata=metadata)
        data = await call
        echo = dict(await call.initial_metadata()).get("x-session-detail-bin")
        if echo != metadata[0][1]:
            raise PermissionError("RPC 路由响应会话校验失败")
        return self.protocol.parse("MessageContent", data)

    async def renew_login(self, gate, session):
        request = self.protocol.message("ClientRequest", message_id=RENEW_ID)
        forward = self.protocol.message("client_rpc_router.ForwardRequest", request=request,
                                        zone_id=gate.info.zone_id, release_track=gate.info.release_track)
        subject = gate.details(session, 0).SerializeToString()
        metadata = session_metadata(subject, RENEW_PATH, self.secret)
        call = self.client.request(ROUTER_PATH, forward.SerializeToString(), metadata=metadata)
        data = await call
        if dict(await call.initial_metadata()).get("x-session-detail-bin") != metadata[0][1]:
            raise ValueError("RPC 路由响应会话校验失败")
        result = self.protocol.parse("MessageContent", data)
        if result.error_message.id:
            raise ValueError("Login 续租失败")
        return result.serialized_message

    async def request_migration(self, gate, session):
        route = self.protocol.routes[self.protocol.message_ids["ClientPlayerLoginRequestGateMigration"]]
        request = self.protocol.message("ClientRequest", message_id=route.message_id)
        forward = self.protocol.message("client_rpc_router.ForwardRequest", request=request,
                                        zone_id=gate.info.zone_id, release_track=gate.info.release_track)
        metadata = session_metadata(gate.details(session, 0).SerializeToString(), route.path, self.secret)
        call = self.client.request(ROUTER_PATH, forward.SerializeToString(), metadata=metadata)
        data = await call
        if dict(await call.initial_metadata()).get("x-session-detail-bin") != metadata[0][1]:
            raise ValueError("RPC 路由响应会话校验失败")
        result = self.protocol.parse("MessageContent", data)
        if result.error_message.id or result.message_id != route.message_id:
            raise ValueError("Gate 迁移请求失败")
        return result.serialized_message


class GateNode:
    def __init__(self, protocol, info, ticket_secret: bytes, forward, *, max_connections: int = 1024):
        if not 0 < info.node_id < (1 << 15) or len(ticket_secret) < 32:
            raise ValueError("Gate 节点号或票据密钥无效")
        self.protocol = protocol
        self.info = info
        self.ticket_secret = ticket_secret
        self.forward = forward
        self.max_connections = max_connections
        self.scene = None
        self.spectate = None
        self.world_chat = WorldChatFanout(protocol, self._broadcast_all)
        self._message_limits = _native_message_limits()
        self._gm_client_ids = frozenset(protocol.message_ids[name] for name in _GM_CLIENT_MESSAGES)
        self._gm_enabled = os.getenv("GATE_RUN_MODE", "").strip().lower() in (
            "dev", "development", "local", "test", "testing",
        )
        self._disconnect_id = protocol.message_ids["ClientPlayerLoginDisconnect"]
        self._illegal_threshold = _illegal_packet_threshold()
        self.sessions: dict[int, Session] = {}
        self._generation = count(1)
        self._calls = count(1)
        self._handlers: set[asyncio.Task] = set()
        self._logout_exits: set[asyncio.Task] = set()
        self._detached_direct: set[asyncio.Task] = set()
        sender = getattr(forward, "renew_login", None)
        self.login_renewal = LoginRenewal(self, sender) if sender is not None else None
        self.drain_migration = GateDrainMigration(self, getattr(forward, "request_migration", None))

    def issue_ticket(self, *, ttl: int = 600, player_id: int = 0):
        return issue_gate_ticket(
            self.protocol, self.info.node_id, self.info.zone_id, self.ticket_secret,
            ttl=ttl, player_id=player_id,
        )

    def details(self, session: Session, seq: int):
        return self.protocol.message(
            "SessionDetails", session_id=session.session_id, player_id=session.player_id,
            gate_node_id=self.info.node_id, gate_instance_id=self.info.node_uuid,
            ticket_player_id=session.ticket_player_id, ticket_target_zone_id=session.ticket_target_zone_id,
            session_incarnation=session.incarnation, gate_call_seq=seq,
            owner_revision=session.owner_revision, authenticated_account=session.account,
        )

    async def handle_client(self, reader, writer) -> None:
        if len(self.sessions) >= self.max_connections:
            writer.close()
            return
        incarnation = next(self._generation)
        session_id = self.info.node_id << 17 | incarnation & SESSION_MASK
        while not session_id & SESSION_MASK or session_id in self.sessions or session_id == 0xFFFFFFFF:
            incarnation = next(self._generation)
            session_id = self.info.node_id << 17 | incarnation & SESSION_MASK
        session = Session(session_id, incarnation, writer)
        session.scene_lane = SceneLane(self, session)
        self.sessions[session_id] = session
        task = asyncio.current_task()
        self._handlers.add(task)
        io_tasks = []
        try:
            if not await self._verify(reader, session):
                return
            io_tasks = [
                asyncio.create_task(self._receive(reader, session)),
                asyncio.create_task(self._write(session)),
                asyncio.create_task(self._lane(session)),
                asyncio.create_task(session.scene_lane.run()),
            ]
            done, _ = await asyncio.wait(io_tasks, return_when=asyncio.FIRST_COMPLETED)
            for completed in done:
                completed.result()
        except (FrameError, DecodeError, TimeoutError, OSError):
            logger.debug("客户端连接结束", exc_info=True)
        finally:
            session.close()
            for pending in io_tasks:
                pending.cancel()
            if io_tasks:
                await asyncio.gather(*io_tasks, return_exceptions=True)
            unsent = tuple(task for task in session.direct_tasks if task not in session.direct_sent)
            sent = tuple(task for task in session.direct_tasks if task in session.direct_sent)
            for pending in unsent:
                pending.cancel()
            for pending in sent:
                self._detached_direct.add(pending)
                pending.add_done_callback(self._detached_direct.discard)
            if unsent:
                await asyncio.gather(*unsent, return_exceptions=True)
            self.sessions.pop(session_id, None)
            if self.login_renewal is not None:
                self.login_renewal.forget(session)
            self.drain_migration.forget(session)
            if self.spectate:
                self.spectate.close_session(session_id)
            try:
                if session.verified:
                    await self._disconnect(session)
            finally:
                self._handlers.discard(task)

    async def handle_websocket(self, websocket) -> None:
        reader = WebSocketPacketReader(websocket)
        writer = WebSocketPacketWriter(websocket)
        await self.handle_client(reader, writer)

    async def _verify(self, reader, session: Session) -> bool:
        async with asyncio.timeout(10):
            packet = await read_packet(reader)
            if packet is None or packet.type_name != "ClientTokenVerifyRequest":
                return False
            request = self.protocol.parse(packet.type_name, packet.payload)
            expected = hmac.new(self.ticket_secret, request.payload, "sha256").hexdigest().encode()
            payload = self.protocol.parse("GateTokenPayload", request.payload)
            valid = (
                hmac.compare_digest(expected, request.signature)
                and payload.gate_node_id == self.info.node_id
                and payload.target_zone_id in (0, self.info.zone_id)
                and payload.expire_timestamp > int(time.time())
            )
            reply = self.protocol.message("ClientTokenVerifyResponse", success=valid)
            if valid:
                session.ticket_player_id = payload.player_id
                session.ticket_target_zone_id = payload.target_zone_id
                reply.heartbeat_interval_ms = HEARTBEAT_INTERVAL * 1000
                reply.heartbeat_timeout_ms = HEARTBEAT_TIMEOUT * 1000
            else:
                reply.error = "invalid gate token"
            await write_packet(session.writer, Packet.from_message(reply))
            session.verified = valid
            return valid

    async def _receive(self, reader, session: Session) -> None:
        heartbeat_at = None
        heartbeats = 0
        while True:
            async with asyncio.timeout(HEARTBEAT_TIMEOUT):
                packet = await read_packet(reader)
            if packet is None:
                return
            if packet.type_name == "ClientHeartbeatRequest":
                request = self.protocol.parse(packet.type_name, packet.payload)
                now = time.monotonic()
                if heartbeat_at is None or now - heartbeat_at >= HEARTBEAT_INTERVAL:
                    heartbeat_at, heartbeats = now, 0
                if heartbeats < 3:
                    heartbeats += 1
                    session.emit(self.protocol.message("ClientHeartbeatResponse", seq=request.seq))
                elif self._illegal_packet(session):
                    return
            elif packet.type_name == "ClientRequest":
                request = self.protocol.parse(packet.type_name, packet.payload)
                if not self._admit_request(session, request, len(packet.payload), time.monotonic()):
                    if session.writer.is_closing():
                        return
                    continue
                route = self.protocol.routes.get(request.message_id)
                direct_rpc = (getattr(self.forward, "direct_mode", False) and route is not None
                              and route.node_type not in (RouteNode.SCENE, RouteNode.GATE, RouteNode.BATTLE))
                if session.redirected and not direct_rpc:
                    session.emit(self.protocol.content(request.message_id, request.id, error=UNAVAILABLE))
                    continue
                if (session.queued_leave and route is not None
                        and route.node_type in (RouteNode.SCENE, RouteNode.GATE)):
                    session.emit(self.protocol.content(request.message_id, request.id, error=UNAVAILABLE))
                    continue
                if route is not None and route.node_type == RouteNode.SCENE:
                    if bool(request.id) != (route.kind == RpcKind.REQUEST):
                        reply = self._error(request, INVALID_PARAMETER)
                        if reply is not None:
                            session.emit(reply)
                    else:
                        await session.scene_lane.admit_wait(request, route.kind == RpcKind.ONE_WAY)
                    continue
                if route is not None and route.node_type == RouteNode.GATE:
                    reply = await self._dispatch(session, next(self._calls), request)
                    if reply is not None:
                        session.emit(reply)
                    continue
                if getattr(self.forward, "direct_mode", False):
                    self._admit_direct(session, request, route)
                    continue
                self._admit_router(session, request, time.monotonic())
            else:
                return

    def _illegal_packet(self, session: Session) -> bool:
        session.illegal_packets += 1
        if self._illegal_threshold and session.illegal_packets >= self._illegal_threshold:
            session.writer.transport.abort()
            return True
        return False

    def _admit_request(self, session: Session, request, size: int, now: float) -> bool:
        message_id = request.message_id
        route = self.protocol.routes.get(message_id)
        if (route is None or not route.client_protocol or message_id == self._disconnect_id
                or route.kind not in (RpcKind.REQUEST, RpcKind.ONE_WAY)):
            self._illegal_packet(session)
            return False

        error = 0
        if size > 1024:
            error = MESSAGE_SIZE_EXCEEDED
        # Python extension IDs keep their existing lane bound unless explicitly configured.
        elif message_id < PYTHON_EXTENSION_ID_MIN or message_id in self._message_limits:
            limit, window = self._message_limits.get(message_id, (3, 1))
            timestamps = session.message_timestamps.setdefault(message_id, deque())
            while timestamps and now - timestamps[0] > window:
                timestamps.popleft()
            if len(timestamps) >= limit:
                error = RATE_LIMIT_EXCEEDED
            else:
                timestamps.append(now)
        if not error and not self._gm_enabled and message_id in self._gm_client_ids:
            error = FEATURE_UNAVAILABLE
        if not error:
            return True
        session.emit(self.protocol.content(message_id, request.id, error=error))
        self._illegal_packet(session)
        return False

    async def _write(self, session: Session) -> None:
        while True:
            message = await session.outbound.get()
            async with asyncio.timeout(5):
                await write_packet(session.writer, Packet.from_message(message))

    def _admit_router(self, session: Session, request, now: float) -> None:
        if session.requests.qsize() + int(session.lane_busy) >= ROUTER_LANE_DEPTH:
            session.emit(self.protocol.content(request.message_id, request.id, error=RATE_LIMIT_EXCEEDED))
            return
        seq = next(self._calls)
        details = self.details(session, seq)
        if session.queued_leave:
            # A request received behind LeaveGame must not acquire the old
            # online account, even if it is dispatched after another login.
            details.authenticated_account = ""
            details.owner_revision = 0
        identity = (details.player_id, details.authenticated_account, details.owner_revision) if details.authenticated_account else None
        is_leave = request.message_id == self.protocol.message_ids["ClientPlayerLoginLeaveGame"]
        session.requests.put_nowait(RouterLaneItem(seq, request, identity, details, now, is_leave))
        if is_leave:
            session.queued_leave += 1

    def _admit_direct(self, session: Session, request, route) -> None:
        """Prepare a direct call in receive order; its network wait is concurrent."""
        def reject(code):
            session.emit(self.protocol.content(request.message_id, request.id, error=code))

        if (route is None or bool(request.id) != (route.kind == RpcKind.REQUEST)
                or route.node_type == RouteNode.BATTLE):
            reject(UNAVAILABLE if route and route.node_type == RouteNode.BATTLE else INVALID_PARAMETER)
            return
        switch = getattr(self, "kill_switch", None)
        if switch is not None and switch.check_id(request.message_id):
            reject(UNAVAILABLE)
            return

        previous_player = session.player_id
        if route.service == LOGIN_SERVICE and route.method == "EnterGame":
            # Native direct admission applies this transition before parsing
            # the RPC body when there is no currently bound role.
            try:
                target = self.protocol.parse(route.request_type, request.body).player_id
            except DecodeError:
                if not session.player_id:
                    session.enter_requested = True
                    session.last_enter_at = time.monotonic()
                reject(REQUEST_PARSE_ERROR)
                return
            if session.player_id not in (0, target):
                reject(LOGIN_IN_PROGRESS)
                return
            session.enter_requested = True
            session.enter_player_id = target
            session.last_enter_at = time.monotonic()
        elif route.service == LOGIN_SERVICE and route.method == "LeaveGame":
            self._direct_leave(session)

        try:
            self.forward.validate_request(route, request)
        except DecodeError:
            reject(REQUEST_PARSE_ERROR)
            return
        if (route.service == LOGIN_SERVICE and route.method in ("CreatePlayer", "EnterGame", "LeaveGame")
                and not session.logged_in):
            reject(LOGIN_SESSION_NOT_FOUND)
            return
        details = self.details(session, 0)
        # Native ResetSceneRoute preserves the old role until a new binding;
        # Python retains it separately to fence delayed scene events.
        if not details.player_id and session.left_player_id:
            details.player_id = session.left_player_id
        if route.service == LOGIN_SERVICE and route.method == "LeaveGame" and previous_player:
            details.player_id = previous_player
        task = asyncio.create_task(self._direct_reply(session, route, request, details))
        session.direct_tasks.add(task)
        task.add_done_callback(session.direct_tasks.discard)
        task.add_done_callback(session.direct_sent.discard)

    def _direct_leave(self, session: Session) -> None:
        node, instance, player = session.scene_node_id, session.scene_instance_id, session.player_id
        if node and self.scene is not None:
            exit_body = self.protocol.message("GameNodeExitGameRequest", reason=1).SerializeToString()
            payload = self.protocol.message("ProcessClientPlayerMessageRequest", session_id=session.session_id,
                                            player_id=player, message_content=self.protocol.content(
                                                self.protocol.message_ids["ScenePlayerExitGame"], body=exit_body))

            async def send_exit():
                try:
                    # Serialize after any previous entry and wait for Scene to
                    # finish this logout before permitting the next entry.
                    async with session.routing:
                        raw = await self.scene.call(node, self.protocol.message_ids["SceneProcessClientPlayerMessage"],
                                                    payload, expected_uuid=instance or None)
                        reply = self.protocol.parse("ProcessClientPlayerMessageResponse", raw)
                        # C++ accepts an outer request/reply for this RPC but
                        # returns an empty body for the inner one-way method.
                        if (reply.session_id not in (0, session.session_id)
                                or reply.HasField("message_content") and reply.message_content.error_message.id):
                            raise ValueError("场景登出响应身份不匹配")
                except (OSError, ValueError, RpcError, grpc.RpcError, RuntimeError, TimeoutError, DecodeError):
                    logger.warning("无法发送直连登出场景通知", exc_info=True)
                    # The exit may have arrived. Do not let a new generation
                    # reuse this session while its effect is uncertain.
                    session.entry_failed = True
                    session.close()

            task = asyncio.create_task(send_exit())
            session.direct_exit = task
            self._logout_exits.add(task)
            task.add_done_callback(self._logout_exits.discard)
        if self.spectate:
            self.spectate.close_session(session.session_id)
        if player:
            if player != session.left_player_id:
                session.left_generation = 0
            session.left_player_id = player
            session.left_generation = max(session.left_generation, session.login_generation)
        session.player_id = session.scene_node_id = session.scene_id = session.owner_revision = 0
        session.scene_instance_id = ""
        session.owner_epoch = 0
        session.account = ""
        self._clear_scene_entry(session)
        session.last_scene_route = None
        session.enter_requested = False
        session.enter_player_id = 0

    async def _direct_reply(self, session: Session, route, request, details) -> None:
        try:
            def on_send():
                session.direct_sent.add(asyncio.current_task())

            result = await self.forward(self, session, route, request, 0, details=details, on_send=on_send)
            if self.sessions.get(session.session_id) is not session or session.writer.is_closing():
                return
            if result is not None and (result.id != request.id or result.message_id != request.message_id):
                result = self.protocol.content(request.message_id, request.id, error=UNAVAILABLE)
            if (route.service == LOGIN_SERVICE and route.method == "Login" and result is not None
                    and not result.error_message.id
                    and not self.protocol.parse(route.response_type, result.serialized_message).error_message.id):
                session.logged_in = True
                if self.login_renewal is not None:
                    self.login_renewal.login_succeeded(session)
            if result is not None:
                session.emit(result)
        except (OSError, ValueError, RpcError, DecodeError, grpc.RpcError, RuntimeError):
            if self.sessions.get(session.session_id) is session and not session.writer.is_closing():
                session.emit(self.protocol.content(request.message_id, request.id, error=UNAVAILABLE))

    async def _lane(self, session: Session) -> None:
        while True:
            item = await session.requests.get()
            request = item.request
            session.lane_busy = True
            try:
                identity = session.player_id, session.account, session.owner_revision
                if (session.redirected or session.entry_failed
                        or time.monotonic() - item.admitted_at > ROUTER_LANE_MAX_WAIT):
                    reply = self.protocol.content(request.message_id, request.id, error=UNAVAILABLE)
                elif item.admitted_identity is not None and item.admitted_identity != identity:
                    reply = self.protocol.content(request.message_id, request.id, error=LOGIN_SESSION_NOT_FOUND)
                else:
                    reply = await self._dispatch(session, item.sequence, request, router_details=item.details,
                                                 lane_item=item)
                if reply is not None:
                    session.emit(reply)
            finally:
                if item.leave_barrier:
                    session.queued_leave = max(0, session.queued_leave - 1)
                session.lane_busy = False
                session.requests.task_done()

    async def _dispatch(self, session: Session, seq: int, request, *, router_details=None, lane_item=None):
        route = self.protocol.routes.get(request.message_id)
        if (route is None or not route.client_protocol or request.message_id == self._disconnect_id
                or route.kind not in (RpcKind.REQUEST, RpcKind.ONE_WAY)):
            return self._error(request, INVALID_PARAMETER)
        if bool(request.id) != (route.kind == RpcKind.REQUEST):
            return self._error(request, INVALID_PARAMETER)
        switch = getattr(self, "kill_switch", None)
        if switch is not None and switch.check_id(request.message_id):
            return self._error(request, UNAVAILABLE)
        if route.node_type == RouteNode.GATE:
            return self.spectate.request(session, route, request) if self.spectate else self._error(request, UNAVAILABLE)
        if route.node_type == RouteNode.BATTLE:
            return self._error(request, UNAVAILABLE)
        leaving = False
        try:
            if route.node_type == RouteNode.SCENE:
                if not session.player_id or not session.scene_node_id:
                    return self._error(request, UNAVAILABLE)
                return await self._scene_request(session, request, one_way=route.kind == RpcKind.ONE_WAY)
            validate_direct = getattr(self.forward, "validate_request", None)
            if validate_direct is not None:
                try:
                    validate_direct(route, request)
                except DecodeError:
                    return self._error(request, REQUEST_PARSE_ERROR)
            if route.service == LOGIN_SERVICE:
                if route.method in ("CreatePlayer", "EnterGame", "LeaveGame") and not session.logged_in:
                    return self._error(request, LOGIN_SESSION_NOT_FOUND)
                if route.method == "Login" and validate_direct is None:
                    session.login_started = True
                elif route.method == "EnterGame":
                    try:
                        target = self.protocol.parse(route.request_type, request.body).player_id
                    except DecodeError:
                        # Router mode validates malformed bodies downstream if
                        # there is no bound role; a bound role cannot be
                        # safely compared and must be rejected at Gate.
                        if session.player_id:
                            return self._error(request, REQUEST_PARSE_ERROR)
                        target = 0
                    if session.player_id not in (0, target):
                        return self._error(request, LOGIN_IN_PROGRESS)
                    session.enter_requested = True
                    session.enter_player_id = target
                    session.last_enter_at = time.monotonic()
                elif route.method == "LeaveGame":
                    if isinstance(self.forward, RouterForwarder) and not await self.forward.ready_for_leave():
                        return self._error(request, UNAVAILABLE)
                    async with session.routing:
                        await self.exit_scene(session, 1)
                        if self.spectate:
                            self.spectate.close_session(session.session_id)
                        session.left_player_id = session.player_id
                        session.left_generation = session.login_generation
                        session.scene_node_id = session.scene_id = session.owner_revision = 0
                        session.scene_instance_id = ""
                        session.owner_epoch = 0
                        session.account = ""
                        self._clear_scene_entry(session)
                        session.last_scene_route = None
                        session.enter_requested = False
                        session.enter_player_id = 0
                    leaving = True
                    if lane_item is not None and lane_item.leave_barrier:
                        session.queued_leave = max(0, session.queued_leave - 1)
                        lane_item.leave_barrier = False
            if isinstance(self.forward, RouterForwarder) and router_details is not None:
                result = await self.forward(self, session, route, request, seq, details=router_details)
            else:
                result = await self.forward(self, session, route, request, seq)
            if result is not None and (result.id != request.id or result.message_id != request.message_id):
                return self._error(request, UNAVAILABLE)
            if route.service == LOGIN_SERVICE and route.method == "Login":
                if (result is not None and not result.error_message.id
                        and not self.protocol.parse(route.response_type, result.serialized_message).error_message.id):
                    session.logged_in = True
                    if self.login_renewal is not None:
                        self.login_renewal.login_succeeded(session)
            return result
        except (OSError, ValueError, RpcError, DecodeError, grpc.RpcError, RuntimeError):
            logger.debug("客户端请求失败 message_id=%s", request.message_id, exc_info=True)
            return self._error(request, UNAVAILABLE)
        finally:
            if leaving:
                session.player_id = 0

    def _error(self, request, error: int):
        if request.id:
            return self.protocol.content(request.message_id, request.id, error=error)
        return None

    async def _scene_request(self, session, request, *, one_way=False, previous_route=False,
                             on_sent=None, can_send=None, lane_response=False):
        target = self._scene_identity(session)
        payload = self.protocol.message(
            "ProcessClientPlayerMessageRequest", session_id=session.session_id, player_id=session.player_id,
            message_content=self.protocol.content(request.message_id, request.id, body=request.body),
            received_on_previous_route=previous_route,
        )
        options = {}
        if on_sent is not None:
            options["on_sent"] = on_sent
        if can_send is not None:
            options["can_send"] = can_send
        data = await self.scene.call(
            target[0], self.protocol.message_ids["SceneProcessClientPlayerMessage"],
            payload, one_way=one_way, expected_uuid=target[3] or None, **options,
        )
        if not one_way:
            result = self.protocol.parse("ProcessClientPlayerMessageResponse", data)
            if lane_response:
                if result.session_id not in (0, session.session_id):
                    raise ValueError("场景响应会话不匹配")
                return result
            if (target != self._scene_identity(session)
                    or result.session_id != session.session_id
                    or result.message_content.message_id != request.message_id
                    or result.message_content.id != request.id):
                raise ValueError("场景响应身份不匹配")
            return result.message_content
        return None

    @staticmethod
    def _scene_identity(session: Session) -> tuple:
        return (session.scene_node_id, session.scene_id, session.login_generation,
                session.scene_instance_id, session.owner_epoch, session.owner_revision)

    @staticmethod
    def _scene_route(session: Session) -> tuple:
        return session.scene_node_id, session.scene_id, session.scene_instance_id

    @staticmethod
    def _client_identity(session: Session) -> tuple:
        return session.player_id, session.account, session.login_generation, session.owner_revision

    async def exit_scene(self, session: Session, reason: int) -> None:
        if not session.scene_node_id:
            return
        request = self.protocol.message(
            "ClientRequest", message_id=self.protocol.message_ids["ScenePlayerExitGame"],
            body=self.protocol.message("GameNodeExitGameRequest", reason=reason).SerializeToString(),
        )
        await self._scene_request(session, request, one_way=reason != 1)

    async def _disconnect(self, session: Session) -> None:
        self._clear_scene_entry(session)
        try:
            await self.exit_scene(session, 3 if session.lease_expired else 2)
        except (OSError, RpcError, grpc.RpcError, ValueError, RuntimeError):
            logger.warning("无法通知 Scene 会话断开 session=%s", session.session_id, exc_info=True)
        if not (session.login_started or session.player_id):
            return
        try:
            message_id = self._disconnect_id
            request = self.protocol.message(
                "ClientRequest", message_id=message_id,
                body=self.protocol.message(
                    "loginpb.LoginNodeDisconnectRequest", session_id=session.session_id,
                ).SerializeToString(),
            )
            await self.forward(self, session, self.protocol.routes[message_id], request, 0)
        except (OSError, grpc.RpcError, DecodeError, ValueError, RuntimeError):
            logger.warning("无法通知 Login 会话断开 session=%s", session.session_id, exc_info=True)

    def push(self, request, *, scene_uuid: str = "") -> None:
        session = self.sessions.get(request.header.session_id)
        player_id = request.header.target_player_id
        if (session and scene_uuid and session.scene_instance_id and scene_uuid != session.scene_instance_id
                and request.message_content.message_id == self.protocol.message_ids["SceneClientPlayerCommonKickPlayer"]):
            return
        if session and session.verified and (not player_id or session.player_id == player_id):
            session.emit(request.message_content)

    async def bind(self, event) -> None:
        session = self.sessions.get(event.session_id)
        if (
            session is None or not session.verified or session.writer.is_closing()
            or not session.enter_requested or session.redirected or session.entry_failed
            or session.enter_player_id not in (0, event.player_id)
            or not event.player_id or not event.owner_revision or not event.authenticated_account
            or event.session_incarnation != session.incarnation
            or event.session_version < session.version
            or event.player_id == session.player_id and event.login_generation < session.login_generation
            or event.player_id == session.left_player_id and event.login_generation <= session.left_generation
            or event.player_id == session.player_id and event.owner_revision < session.owner_revision
            or session.ticket_player_id not in (0, event.player_id)
            or session.player_id not in (0, event.player_id)
        ):
            return
        previous_binding = self._entry_binding(session)
        session.player_id = event.player_id
        session.owner_revision = event.owner_revision
        session.account = event.authenticated_account
        session.version = event.session_version
        session.login_generation = event.login_generation
        binding_changed = previous_binding != self._entry_binding(session)
        if binding_changed or not session.scene_id:
            session.enter_type = event.enter_gs_type
        pending = session.pending_route
        if pending is not None and pending.binding != self._entry_binding(session):
            await self.route(pending.event, 0, background=True, from_bind=True, started=pending.started)
        elif pending is None and binding_changed and session.enter_type and session.last_scene_route is not None:
            await self.route(session.last_scene_route, 0, background=True, from_bind=True)

    @staticmethod
    def _entry_binding(session: Session) -> tuple:
        return session.player_id, session.version, session.login_generation, session.owner_revision, session.account

    @staticmethod
    def _clear_scene_entry(session: Session) -> None:
        entry, session.pending_route = session.pending_route, None
        if entry is not None:
            entry.cancel_if_unsent()

    async def route(self, event, enter_type: int, *, background: bool = False,
                    from_bind: bool = False, started: float | None = None) -> None:
        session = self.sessions.get(event.session_id)
        if (session is None or not session.verified or not session.enter_requested or session.redirected or session.entry_failed
                or session.writer.is_closing() or session.enter_player_id not in (0, event.player_id)
                or event.player_id == session.left_player_id and event.login_generation <= session.left_generation):
            return
        if session.player_id and (event.player_id != session.player_id or event.owner_revision != session.owner_revision
                                  or event.owner_epoch < session.owner_epoch):
            return
        previous = session.pending_route
        if previous is not None and not from_bind and previous.event.player_id == event.player_id:
            if event.owner_epoch < previous.event.owner_epoch or (
                event.owner_epoch == previous.event.owner_epoch
                and event.login_generation < previous.event.login_generation
            ):
                return
        snapshot = type(event)()
        snapshot.CopyFrom(event)
        loop = asyncio.get_running_loop()
        entry = PendingSceneEntry(snapshot, enter_type, loop.time() if started is None else started,
                                  self._entry_binding(session) if session.player_id else None)
        self._clear_scene_entry(session)
        session.pending_route = entry
        session.last_scene_route = snapshot
        entry.task = asyncio.create_task(self._run_scene_entry(session, entry), name=f"scene-entry-{session.session_id}")
        if not background and entry.binding is not None:
            try:
                await asyncio.shield(entry.task)
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():
                    raise

    async def _run_scene_entry(self, session: Session, entry: PendingSceneEntry) -> None:
        def current() -> bool:
            binding = self._entry_binding(session) if session.player_id else None
            return (self.sessions.get(session.session_id) is session and session.pending_route is entry
                    and entry.binding == binding and session.enter_requested
                    and not session.redirected and not session.entry_failed and not session.writer.is_closing())

        async def attempt(budget: float) -> None:
            event = entry.event
            if not event.scene_id or not event.target_node_id:
                raise ValueError("场景入场目标无效")
            loop = asyncio.get_running_loop()
            deadline = loop.time() + budget
            # Do not hold routing while waiting: the pending direct logout
            # takes that same lock to order against older entry attempts.
            while True:
                barrier = session.direct_exit
                if barrier is not None:
                    await asyncio.shield(barrier)
                await session.routing.acquire()
                if barrier is session.direct_exit:
                    break
                session.routing.release()
            try:
                if not current():
                    return
                try:
                    async with asyncio.timeout_at(deadline):
                        if entry.binding is None:
                            await self.scene.discover_entry(event.target_node_id)
                            return
                        link = await self.scene.prepare_entry(event.target_node_id)
                except TimeoutError as exc:
                    raise SceneLinkNotReady("Scene 连接超出入场预算") from exc
                except grpc.RpcError as exc:
                    raise SceneNodeNotFound("Scene 权威发现尚未就绪") from exc
                if not current():
                    return
                if loop.time() >= deadline:
                    raise SceneLinkNotReady("Scene 入场预算已耗尽")
                enter_type = entry.enter_type or session.enter_type
                if (not enter_type and session.scene_id == event.scene_id and session.scene_node_id == event.target_node_id
                        and session.scene_instance_id == link.info.node_uuid and session.owner_epoch == event.owner_epoch):
                    return
                request = self.protocol.message(
                    "PlayerEnterGameNodeRequest", player_id=event.player_id, session_id=event.session_id,
                    scene_id=event.scene_id, home_zone_id=event.home_zone_id, owner_epoch=event.owner_epoch,
                    login_generation=max(event.login_generation, session.login_generation),
                    owner_revision=event.owner_revision, enter_gs_type=enter_type,
                )
                entry.attempted_send = True
                await self.scene.send_entry(link, request, timeout=min(deadline - loop.time(), 12), can_send=current)
                if not current():
                    return
                session.scene_node_id = event.target_node_id
                session.scene_id = event.scene_id
                session.scene_instance_id = link.info.node_uuid
                session.owner_epoch = request.owner_epoch
                session.login_generation = request.login_generation
                session.enter_type = 0
            finally:
                session.routing.release()

        async def give_up() -> None:
            logger.warning("场景入场失败 session=%s scene=%s target=%s attempts=%d first=%s last=%s",
                           session.session_id, entry.event.scene_id, entry.event.target_node_id,
                           entry.attempts, entry.first_failure, entry.last_failure)
            session.entry_failed = True
            tip = self.protocol.message("TipInfoMessage", id=3023)
            kick = self.protocol.message("GameKickPlayerRequest", reason={"id": 3023})
            for name, message in (("SceneClientPlayerCommonSendTipToClient", tip),
                                  ("SceneClientPlayerCommonKickPlayer", kick)):
                session.emit(self.protocol.content(self.protocol.message_ids[name], body=message.SerializeToString()))
            await asyncio.sleep(FORCE_CLOSE_DELAY)
            if self.sessions.get(session.session_id) is session:
                session.close()

        try:
            await retry_entry(entry, current, attempt, give_up)
        finally:
            if session.pending_route is entry:
                session.pending_route = None

    async def command(self, payload: bytes) -> None:
        try:
            command = self.protocol.parse("contracts.kafka.GateCommand", payload)
            if (command.target_gate_id != self.info.node_id
                    or command.target_instance_id != self.info.node_uuid):
                return
            ids = self.protocol.event_ids
            event_id = command.event_id
            if not command.payload and event_id != ids.get("ContractsKafkaGateDrainMigrationEvent"):
                return
            if event_id == ids["ContractsKafkaBindSessionEvent"]:
                await self.bind(self.protocol.parse("contracts.kafka.BindSessionEvent", command.payload))
            elif event_id == ids["ContractsKafkaRoutePlayerEvent"]:
                event = self.protocol.parse("contracts.kafka.RoutePlayerEvent", command.payload)
                await self.route(event, command.enter_gs_type, background=True)
            elif event_id == ids["ContractsKafkaPlayerLeaseExpiredEvent"]:
                event = self.protocol.parse("contracts.kafka.PlayerLeaseExpiredEvent", command.payload)
                session = self.sessions.get(event.session_id)
                if session and (not event.player_id or not session.player_id or session.player_id == event.player_id):
                    session.lease_expired = True
                    session.writer.transport.abort()
            elif event_id == ids["ContractsKafkaRedirectToGateEvent"]:
                self._redirect(self.protocol.parse("contracts.kafka.RedirectToGateEvent", command.payload))
            elif event_id == ids.get("ContractsKafkaGateDrainMigrationEvent"):
                self.drain_migration.lease(self.protocol.parse("contracts.kafka.GateDrainMigrationEvent", command.payload))
            elif event_id == ids["ContractsKafkaPushToPlayerEvent"]:
                event = self.protocol.parse("contracts.kafka.PushToPlayerEvent", command.payload)
                if event.HasField("message_content"):
                    self._deliver_to_session(event.session_id, event.player_id, event.message_content)
            elif event_id == ids["ContractsKafkaKickPlayerEvent"]:
                event = self.protocol.parse("contracts.kafka.KickPlayerEvent", command.payload)
                session = self.sessions.get(event.session_id)
                if session and (
                    not event.player_id or (
                        session.player_id not in (0, (1 << 64) - 1)
                        and session.player_id == event.player_id
                        and (not event.login_generation or session.login_generation < event.login_generation)
                    )
                ):
                    # The native Gate writes the reason before a graceful shutdown.
                    # Write synchronously into the transport so close() can flush it;
                    # enqueuing it would race with cancellation of the writer task.
                    kick = self.protocol.message("GameKickPlayerRequest", reason={"id": 2017})
                    content = self.protocol.content(
                        self.protocol.message_ids["SceneClientPlayerCommonKickPlayer"],
                        body=kick.SerializeToString(),
                    )
                    try:
                        session.writer.write(encode_packet(Packet.from_message(content)))
                    finally:
                        session.close()
            elif event_id in (
                ids["ContractsKafkaBroadcastToPlayersEvent"], ids["ContractsKafkaBroadcastToSceneEvent"],
                ids["ContractsKafkaBroadcastToAllEvent"],
            ):
                self._broadcast(event_id, command.payload)
        except (DecodeError, OSError, ValueError, RpcError, grpc.RpcError, RuntimeError):
            logger.warning("Gate 命令处理失败", exc_info=True)

    def _redirect(self, event) -> None:
        session = self.sessions.get(event.session_id)
        if (session is None or not session.verified or not session.enter_requested or session.entry_failed
                or session.writer.is_closing()
                or not event.player_id or session.player_id not in (0, event.player_id)
                or session.enter_player_id not in (0, event.player_id)
                or session.ticket_player_id not in (0, event.player_id)
                or not event.target_gate_ip or not 0 < event.target_gate_port < 65536
                or not event.token_payload or not event.token_signature or event.token_deadline <= int(time.time())):
            return
        token = self.protocol.parse("GateTokenPayload", event.token_payload)
        if token.player_id not in (0, event.player_id) or not token.gate_node_id:
            return
        session.redirected = True
        self.drain_migration.redirected(session)
        self._clear_scene_entry(session)
        session.last_scene_route = None
        if session.scene_lane is not None:
            session.scene_lane.redirect()
        while not session.requests.empty():
            queued = session.requests.get_nowait()
            session.requests.task_done()
            if isinstance(queued, RouterLaneItem):
                session.emit(self.protocol.content(queued.request.message_id, queued.request.id, error=UNAVAILABLE))
        session.queued_leave = 0
        notify = self.protocol.message(
            "RedirectToGateNotify", target_ip=event.target_gate_ip, target_port=event.target_gate_port,
            token_payload=event.token_payload, token_signature=event.token_signature, token_deadline=event.token_deadline,
        )
        session.emit(self.protocol.content(
            self.protocol.message_ids["SceneClientPlayerCommonRedirectToGate"], body=notify.SerializeToString(),
        ))

    def _drain_redirect(self, session, response, expected_player: int) -> bool:
        if (not response.target_gate_ip or not 0 < response.target_gate_port < 65536
                or not response.token_payload or not response.token_signature
                or response.token_deadline <= int(time.time()) or session.writer.is_closing()):
            return False
        try:
            token = self.protocol.parse("GateTokenPayload", response.token_payload)
        except DecodeError:
            return False
        if (not token.gate_node_id or token.gate_node_id == self.info.node_id
                or token.player_id != expected_player or token.target_zone_id != 0
                or token.zone_id != self.info.zone_id or token.expire_timestamp <= int(time.time())):
            return False
        notify = self.protocol.message("RedirectToGateNotify", target_ip=response.target_gate_ip,
                                       target_port=response.target_gate_port, token_payload=response.token_payload,
                                       token_signature=response.token_signature, token_deadline=response.token_deadline)
        session.redirected = True
        if session.scene_lane is not None:
            session.scene_lane.redirect()
        session.emit(self.protocol.content(self.protocol.message_ids["SceneClientPlayerCommonRedirectToGate"],
                                           body=notify.SerializeToString()))
        if session.writer.is_closing():
            return False
        self.drain_migration.redirected(session)
        return True

    def _broadcast(self, event_id, payload) -> None:
        for method in GATE_BROADCAST_METHODS:
            if event_id == self.protocol.event_ids[f"ContractsKafka{method}Event"]:
                self.broadcast(method, self.protocol.parse(f"contracts.kafka.{method}Event", payload))
                return

    def _deliver_to_session(self, session_id: int, player_id: int, content) -> None:
        session = self.sessions.get(session_id)
        if session and session.verified and (not player_id or session.player_id == player_id):
            session.emit(content)

    def broadcast(self, method: str, event) -> None:
        if not event.HasField("message_content"):
            return
        if method == "BroadcastToPlayers":
            if event.session_bitmap:
                if len(event.session_bitmap) > 1 << 14:
                    return
                targets = [
                    event.session_bitmap_base + index * 8 + bit
                    for index, byte in enumerate(event.session_bitmap)
                    for bit in range(8) if byte & (1 << bit)
                ]
                if targets and targets[-1] >= 1 << 32:
                    return
            else:
                targets = list(event.session_list)
                if targets != sorted(set(targets)):
                    return
            if event.player_list and len(event.player_list) != len(targets):
                return
            for index, session_id in enumerate(targets):
                self._deliver_to_session(
                    session_id, event.player_list[index] if event.player_list else 0, event.message_content,
                )
        elif method == "BroadcastToScene" and event.scene_id:
            self._broadcast_all(event.message_content, scene_id=event.scene_id)
        elif method == "BroadcastToAll":
            self._broadcast_all(event.message_content)

    def _broadcast_all(self, content, *, scene_id: int | None = None) -> None:
        for session in self.sessions.values():
            if session.player_id and session.verified and (scene_id is None or scene_id == session.scene_id):
                session.emit(content)

    async def close(self) -> None:
        await self.drain_migration.close()
        if self.login_renewal is not None:
            await self.login_renewal.close()
        for session in tuple(self.sessions.values()):
            session.close()
        if self._handlers:
            await asyncio.gather(*tuple(self._handlers), return_exceptions=True)
        if self._logout_exits:
            done, pending = await asyncio.wait(tuple(self._logout_exits), timeout=2)
            for task in pending:
                task.cancel()
            await asyncio.gather(*done, *pending, return_exceptions=True)
        if self._detached_direct:
            done, pending = await asyncio.wait(tuple(self._detached_direct), timeout=2)
            for task in pending:
                task.cancel()
            await asyncio.gather(*done, *pending, return_exceptions=True)
