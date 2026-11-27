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
HANDSHAKE_TIMEOUT = 10.0
LOGIN_DEADLINE_AFTER_VERIFY = 60.0
PRE_AUTH_MAX_MESSAGE_BYTES = 4096
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
LOGIN_MINOR_PLAY_TIME_OVER = 2049
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
    ticket_account_digest: bytes = b""
    ticket_key: bytes = b""
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
    verified_at: float | None = None
    login_started: bool = False
    logged_in: bool = False
    enter_requested: bool = False
    enter_player_id: int = 0
    redirected: bool = False
    lease_expired: bool = False
    exit_scene_immediately_on_close: bool = False
    scene_route_reset_pending: bool = False
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
    def __init__(
        self,
        protocol,
        info,
        ticket_secret: bytes,
        forward,
        *,
        max_connections: int = 1024,
        max_pre_auth_connections: int | None = None,
        handshake_timeout: float = HANDSHAKE_TIMEOUT,
        login_deadline_after_verify: float = LOGIN_DEADLINE_AFTER_VERIFY,
        pre_auth_max_message_bytes: int = PRE_AUTH_MAX_MESSAGE_BYTES,
    ):
        if not 0 < info.node_id < (1 << 15) or len(ticket_secret) < 32:
            raise ValueError("Gate 节点号或票据密钥无效")
        self.protocol = protocol
        self.info = info
        self.ticket_secret = ticket_secret
        self.forward = forward
        self.max_connections = max_connections
        self.max_pre_auth_connections = (
            max_connections if max_pre_auth_connections is None else max(0, int(max_pre_auth_connections))
        )
        self.handshake_timeout = float(handshake_timeout)
        self.login_deadline_after_verify = float(login_deadline_after_verify)
        self.pre_auth_max_message_bytes = int(pre_auth_max_message_bytes)
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
        self.pre_auth_connections: dict[int, float] = {}
        self._active_tickets: dict[bytes, int] = {}
        self.admission_counters: dict[str, int] = {
            "accepted": 0,
            "rejected_capacity": 0,
            "rejected_preauth_budget": 0,
            "rejected_ticket_in_use": 0,
            "handshake_timeouts": 0,
            "login_timeouts": 0,
            "heartbeat_before_verify": 0,
            "preauth_oversize": 0,
        }
        # 对齐 C++ client_request_lane / client_scene_lane / scene_entry_dispatch 单源计数
        self.router_lane_counters: dict[str, int] = {
            "admitted": 0,
            "sent": 0,
            "replied": 0,
            "failed": 0,
            "expired": 0,
            "expired_in_queue": 0,
            "no_router": 0,
            "call_not_built": 0,
            "rejected_full": 0,
            "rejected_redirected": 0,
            "late_dropped": 0,
            "incarnation_mismatch": 0,
            "dropped_on_close": 0,
            "dropped_on_redirect": 0,
            "rejected_not_logged_in": 0,
            "rejected_stale_identity": 0,
            "rejected_before_send": 0,
        }
        self._router_lane_in_flight = 0
        self._router_lane_max_depth_seen = 0
        self.scene_lane_counters: dict[str, int] = {
            "admitted": 0,
            "sent": 0,
            "replied": 0,
            "failed": 0,
            "no_content": 0,
            "dropped_uncorrelated": 0,
            "held": 0,
            "retry_verdicts": 0,
            "expired_on_hold": 0,
            "expired_in_queue": 0,
            "no_scene": 0,
            "rejected_full": 0,
            "dropped_on_redirect": 0,
            "one_way_direct": 0,
            "one_way_queued": 0,
            "one_way_sent": 0,
            "one_way_dropped": 0,
            "sent_on_new_route": 0,
            "inflight_lost": 0,
            "late_dropped": 0,
        }
        self._scene_lane_in_flight = 0
        self._scene_lane_max_depth_seen = 0
        self.scene_entry_counters: dict[str, int] = {
            "deferred": 0,
            "recovered": 0,
            "gave_up": 0,
            "superseded": 0,
            "session_closed": 0,
            "player_changed": 0,
        }
        self._generation = count(1)
        self._calls = count(1)
        self._handlers: set[asyncio.Task] = set()
        self._logout_exits: set[asyncio.Task] = set()
        self._detached_direct: set[asyncio.Task] = set()
        sender = getattr(forward, "renew_login", None)
        self.login_renewal = LoginRenewal(self, sender) if sender is not None else None
        self.drain_migration = GateDrainMigration(self, getattr(forward, "request_migration", None))

    @property
    def pre_auth_count(self) -> int:
        """当前处于未验签池中的连接数（对照 C++ gate_unauthenticated_connections）。"""
        return len(self.pre_auth_connections)

    @property
    def total_connections(self) -> int:
        """当前总连接数 = 已验签会话数 + 未验签连接数（总量上限按此口径校验）。"""
        return len(self.sessions) + len(self.pre_auth_connections)

    def collect_lane_metrics(self) -> dict[str, object]:
        """返回与 C++ gate_router_lane_* / gate_scene_lane_* / gate_scene_entry_* 同源的计数与瞬时量快照。"""
        holding_sessions = sum(
            1
            for s in self.sessions.values()
            if s.scene_lane is not None and s.scene_lane.queue and s.scene_lane.queue[0].retry_at
        )
        pending_entries = sum(1 for s in self.sessions.values() if s.pending_route is not None)
        return {
            "router_lane": {
                "events": dict(self.router_lane_counters),
                "in_flight": self._router_lane_in_flight,
                "max_depth_seen": self._router_lane_max_depth_seen,
            },
            "scene_lane": {
                "events": dict(self.scene_lane_counters),
                "in_flight": self._scene_lane_in_flight,
                "holding_sessions": holding_sessions,
                "max_depth_seen": self._scene_lane_max_depth_seen,
            },
            "scene_entry": {
                "events": dict(self.scene_entry_counters),
                "pending_sessions": pending_entries,
            },
        }

    def format_lane_summary(self) -> dict[str, str]:
        """按与 C++ VisitLaneCounters / VisitSceneLaneCounters / VisitSceneEntryCounters 相同的键序生成汇总行。"""
        snap = self.collect_lane_metrics()
        rl = snap["router_lane"]
        sl = snap["scene_lane"]
        se = snap["scene_entry"]
        return {
            "router_lane": (
                "[lane] summary "
                + " ".join(f"{k}={v}" for k, v in rl["events"].items())
                + f" in_flight={rl['in_flight']} max_depth_seen={rl['max_depth_seen']}"
            ),
            "scene_lane": (
                "[scene_lane] summary "
                + " ".join(f"{k}={v}" for k, v in sl["events"].items())
                + f" in_flight={sl['in_flight']} holding_sessions={sl['holding_sessions']}"
                + f" max_depth_seen={sl['max_depth_seen']}"
            ),
            "scene_entry": (
                "[SceneEntry] "
                + " ".join(f"{k}={v}" for k, v in se["events"].items())
                + f" pending={se['pending_sessions']}"
            ),
        }

    def issue_ticket(self, *, ttl: int = 600, player_id: int = 0):
        return issue_gate_ticket(
            self.protocol, self.info.node_id, self.info.zone_id, self.ticket_secret,
            ttl=ttl, player_id=player_id,
        )

    def details(self, session: Session, seq: int):
        msg = self.protocol.message(
            "SessionDetails", session_id=session.session_id, player_id=session.player_id,
            gate_node_id=self.info.node_id, gate_instance_id=self.info.node_uuid,
            ticket_player_id=session.ticket_player_id, ticket_target_zone_id=session.ticket_target_zone_id,
            session_incarnation=session.incarnation, gate_call_seq=seq,
            owner_revision=session.owner_revision, authenticated_account=session.account,
        )
        if session.ticket_account_digest and "ticket_account_digest" in msg.DESCRIPTOR.fields_by_name:
            msg.ticket_account_digest = session.ticket_account_digest
        return msg

    async def handle_client(self, reader, writer) -> None:
        if self.total_connections >= self.max_connections:
            self.admission_counters["rejected_capacity"] += 1
            writer.close()
            return
        if len(self.pre_auth_connections) >= self.max_pre_auth_connections:
            self.admission_counters["rejected_preauth_budget"] += 1
            writer.close()
            return
        incarnation = next(self._generation)
        self.pre_auth_connections[incarnation] = time.monotonic()
        self.admission_counters["accepted"] += 1
        session_id = 0
        session = Session(0, incarnation, writer)
        task = asyncio.current_task()
        self._handlers.add(task)
        io_tasks = []
        try:
            if not await self._verify(reader, session):
                return
            session_id = self.info.node_id << 17 | incarnation & SESSION_MASK
            while not session_id & SESSION_MASK or session_id in self.sessions or session_id == 0xFFFFFFFF:
                incarnation = next(self._generation)
                session_id = self.info.node_id << 17 | incarnation & SESSION_MASK
            session.session_id = session_id
            session.scene_lane = SceneLane(self, session)
            self.pre_auth_connections.pop(session.incarnation, None)
            self.sessions[session_id] = session
            if session.ticket_key:
                self._active_tickets[session.ticket_key] = session_id
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
            self.pre_auth_connections.pop(session.incarnation, None)
            if session.ticket_key and self._active_tickets.get(session.ticket_key) == session_id:
                self._active_tickets.pop(session.ticket_key, None)
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
            if session_id:
                self.sessions.pop(session_id, None)
            if self.login_renewal is not None:
                self.login_renewal.forget(session)
            self.drain_migration.forget(session)
            if self.spectate and session_id:
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
        try:
            async with asyncio.timeout(self.handshake_timeout):
                packet = await read_packet(reader)
        except TimeoutError:
            self.admission_counters["handshake_timeouts"] += 1
            return False
        if packet is None:
            return False
        if len(packet.payload) > self.pre_auth_max_message_bytes:
            self.admission_counters["preauth_oversize"] += 1
            return False
        if packet.type_name == "ClientHeartbeatRequest":
            self.admission_counters["heartbeat_before_verify"] += 1
            return False
        if packet.type_name != "ClientTokenVerifyRequest":
            return False
        request = self.protocol.parse(packet.type_name, packet.payload)
        expected = hmac.new(self.ticket_secret, request.payload, "sha256").hexdigest().encode()
        payload = self.protocol.parse("GateTokenPayload", request.payload)
        valid = (
            hmac.compare_digest(expected, request.signature)
            and payload.gate_node_id == self.info.node_id
            and payload.zone_id in (0, self.info.zone_id)
            and payload.target_zone_id in (0, self.info.zone_id)
            and payload.expire_timestamp > int(time.time())
        )
        ticket_key = bytes(request.signature) if valid else b""
        if valid and ticket_key and ticket_key in self._active_tickets:
            self.admission_counters["rejected_ticket_in_use"] += 1
            reply = self.protocol.message("ClientTokenVerifyResponse", success=False, error="gate token already in use")
            await write_packet(session.writer, Packet.from_message(reply))
            return False
        reply = self.protocol.message("ClientTokenVerifyResponse", success=valid)
        if valid:
            session.ticket_player_id = payload.player_id
            session.ticket_target_zone_id = payload.target_zone_id
            session.ticket_account_digest = bytes(getattr(payload, "account_digest", b""))
            session.ticket_key = ticket_key
            session.verified_at = time.monotonic()
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
            read_timeout = float(HEARTBEAT_TIMEOUT)
            if not session.logged_in and session.verified_at is not None and self.login_deadline_after_verify > 0:
                login_left = self.login_deadline_after_verify - (time.monotonic() - session.verified_at)
                if login_left <= 0:
                    self.admission_counters["login_timeouts"] += 1
                    return
                read_timeout = min(read_timeout, login_left)
            try:
                async with asyncio.timeout(read_timeout):
                    packet = await read_packet(reader)
            except TimeoutError:
                if (
                    not session.logged_in
                    and session.verified_at is not None
                    and self.login_deadline_after_verify > 0
                    and (time.monotonic() - session.verified_at) >= self.login_deadline_after_verify
                ):
                    self.admission_counters["login_timeouts"] += 1
                return
            if packet is None:
                return
            if (
                not session.logged_in
                and session.verified_at is not None
                and self.login_deadline_after_verify > 0
                and (time.monotonic() - session.verified_at) >= self.login_deadline_after_verify
            ):
                self.admission_counters["login_timeouts"] += 1
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
                    if route is not None and route.node_type == RouteNode.SCENE:
                        self.scene_lane_counters["dropped_on_redirect"] += 1
                    else:
                        self.router_lane_counters["rejected_redirected"] += 1
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
                        one_way = route.kind == RpcKind.ONE_WAY
                        lane = session.scene_lane
                        self.scene_lane_counters["admitted"] += 1
                        if one_way:
                            if not lane.queue or lane.queue[0].retry_at:
                                self.scene_lane_counters["one_way_direct"] += 1
                            else:
                                self.scene_lane_counters["one_way_queued"] += 1
                        await lane.admit_wait(request, one_way)
                        depth = len(lane.queue) + int(lane.inflight is not None)
                        if depth > self._scene_lane_max_depth_seen:
                            self._scene_lane_max_depth_seen = depth
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
        depth = session.requests.qsize() + int(session.lane_busy)
        if depth >= ROUTER_LANE_DEPTH:
            self.router_lane_counters["rejected_full"] += 1
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
        self.router_lane_counters["admitted"] += 1
        new_depth = depth + 1
        if new_depth > self._router_lane_max_depth_seen:
            self._router_lane_max_depth_seen = new_depth
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
        session.scene_route_reset_pending = True
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
        try:
            while True:
                item = await session.requests.get()
                request = item.request
                session.lane_busy = True
                try:
                    identity = session.player_id, session.account, session.owner_revision
                    if session.redirected:
                        self.router_lane_counters["rejected_redirected"] += 1
                        reply = self.protocol.content(request.message_id, request.id, error=UNAVAILABLE)
                    elif session.entry_failed:
                        self.router_lane_counters["rejected_before_send"] += 1
                        reply = self.protocol.content(request.message_id, request.id, error=UNAVAILABLE)
                    elif time.monotonic() - item.admitted_at > ROUTER_LANE_MAX_WAIT:
                        self.router_lane_counters["expired_in_queue"] += 1
                        reply = self.protocol.content(request.message_id, request.id, error=UNAVAILABLE)
                    elif item.admitted_identity is not None and item.admitted_identity != identity:
                        self.router_lane_counters["rejected_stale_identity"] += 1
                        reply = self.protocol.content(request.message_id, request.id, error=LOGIN_SESSION_NOT_FOUND)
                    else:
                        self._router_lane_in_flight += 1
                        self.router_lane_counters["sent"] += 1
                        try:
                            reply = await self._dispatch(
                                session, item.sequence, request, router_details=item.details, lane_item=item
                            )
                        finally:
                            self._router_lane_in_flight = max(0, self._router_lane_in_flight - 1)
                        if reply is not None and getattr(reply, "error_message", None) and reply.error_message.id == UNAVAILABLE:
                            self.router_lane_counters["failed"] += 1
                        elif reply is not None:
                            self.router_lane_counters["replied"] += 1
                    if reply is not None:
                        session.emit(reply)
                finally:
                    if item.leave_barrier:
                        session.queued_leave = max(0, session.queued_leave - 1)
                    session.lane_busy = False
                    session.requests.task_done()
        except asyncio.CancelledError:
            while not session.requests.empty():
                session.requests.get_nowait()
                session.requests.task_done()
                self.router_lane_counters["dropped_on_close"] += 1
            raise

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
                    self.router_lane_counters["rejected_not_logged_in"] += 1
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
                        session.scene_route_reset_pending = True
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
                             on_sent=None, can_send=None, lane_response=False, empty_exit_response=False):
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
        self._scene_lane_in_flight += 1
        self.scene_lane_counters["sent"] += 1
        if one_way:
            self.scene_lane_counters["one_way_sent"] += 1
        if previous_route:
            self.scene_lane_counters["sent_on_new_route"] += 1
        try:
            data = await self.scene.call(
                target[0], self.protocol.message_ids["SceneProcessClientPlayerMessage"],
                payload, one_way=one_way, expected_uuid=target[3] or None, **options,
            )
        except Exception:
            self.scene_lane_counters["failed"] += 1
            raise
        finally:
            self._scene_lane_in_flight = max(0, self._scene_lane_in_flight - 1)
        if not one_way:
            result = self.protocol.parse("ProcessClientPlayerMessageResponse", data)
            if lane_response:
                if result.session_id not in (0, session.session_id):
                    self.scene_lane_counters["dropped_uncorrelated"] += 1
                    raise ValueError("场景响应会话不匹配")
                if result.HasField("message_content"):
                    self.scene_lane_counters["replied"] += 1
                elif result.dispatch_verdict == 1:
                    self.scene_lane_counters["retry_verdicts"] += 1
                    self.scene_lane_counters["held"] += 1
                else:
                    self.scene_lane_counters["no_content"] += 1
                return result
            # ExitGame has an Empty inner response. C++ Scene consequently sends
            # an empty outer response, even when this RPC0 call expects a reply.
            # Keep this exception local to logout; ordinary client responses
            # still require the full echoed identity below.
            if empty_exit_response and not result.HasField("message_content"):
                if (target != self._scene_identity(session)
                        or result.session_id not in (0, session.session_id)
                        or result.dispatch_verdict):
                    raise ValueError("场景退出响应身份不匹配")
                return None
            if (target != self._scene_identity(session)
                    or result.session_id != session.session_id
                    or result.message_content.message_id != request.message_id
                    or result.message_content.id != request.id
                    or empty_exit_response and (result.dispatch_verdict or result.message_content.error_message.id)):
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
        await self._scene_request(session, request, one_way=reason != 1, empty_exit_response=reason == 1)

    async def _disconnect(self, session: Session) -> None:
        if session.pending_route is not None:
            self.scene_entry_counters["session_closed"] += 1
        self._clear_scene_entry(session)
        try:
            exit_reason = 1 if session.exit_scene_immediately_on_close else (3 if session.lease_expired else 2)
            await self.exit_scene(session, exit_reason)
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
        self._deliver_to_session(request.header.session_id, player_id, request.message_content)

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
        session.scene_route_reset_pending = False
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
        if previous is not None and not from_bind:
            if previous.event.player_id != event.player_id:
                self.scene_entry_counters["player_changed"] += 1
            else:
                self.scene_entry_counters["superseded"] += 1
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
            self.scene_entry_counters["gave_up"] += 1
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

        deferred_counted = False

        async def tracked_attempt(budget: float) -> None:
            nonlocal deferred_counted
            try:
                await attempt(budget)
            except Exception:
                if not deferred_counted:
                    deferred_counted = True
                    self.scene_entry_counters["deferred"] += 1
                raise
            else:
                if deferred_counted:
                    self.scene_entry_counters["recovered"] += 1

        try:
            await retry_entry(entry, current, tracked_attempt, give_up)
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
            if not command.payload:
                return
            if event_id == ids["ContractsKafkaBindSessionEvent"]:
                await self.bind(self.protocol.parse("contracts.kafka.BindSessionEvent", command.payload))
            elif event_id == ids["ContractsKafkaRoutePlayerEvent"]:
                event = self.protocol.parse("contracts.kafka.RoutePlayerEvent", command.payload)
                await self.route(event, command.enter_gs_type, background=True)
            elif event_id == ids["ContractsKafkaPlayerLeaseExpiredEvent"]:
                event = self.protocol.parse("contracts.kafka.PlayerLeaseExpiredEvent", command.payload)
                session = self.sessions.get(event.session_id)
                event_inc = int(getattr(event, "session_incarnation", 0))
                if (
                    session
                    and (not event_inc or event_inc == session.incarnation)
                    and (not event.player_id or not session.player_id or session.player_id == event.player_id)
                ):
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
                    reason_tip = int(getattr(event, "reason_tip", 0))
                    if reason_tip == LOGIN_MINOR_PLAY_TIME_OVER:
                        session.exit_scene_immediately_on_close = True
                    if session.redirected:
                        session.close()
                        return
                    # The native Gate writes the reason before a graceful shutdown.
                    # Write synchronously into the transport so close() can flush it;
                    # enqueuing it would race with cancellation of the writer task.
                    kick = self.protocol.message("GameKickPlayerRequest", reason={"id": reason_tip or 2017})
                    content = self.protocol.content(
                        self.protocol.message_ids["SceneClientPlayerCommonKickPlayer"],
                        body=kick.SerializeToString(),
                    )
                    frame = encode_packet(Packet.from_message(content))
                    try:
                        session.writer.write(frame)
                        if isinstance(session.writer, WebSocketPacketWriter):
                            async with asyncio.timeout(2):
                                await session.writer.drain()
                    except TimeoutError:
                        pass
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
        if session.pending_route is not None:
            self.scene_entry_counters["superseded"] += 1
        self._clear_scene_entry(session)
        session.last_scene_route = None
        if session.scene_lane is not None:
            self.scene_lane_counters["dropped_on_redirect"] += len(session.scene_lane.queue)
            session.scene_lane.redirect()
        while not session.requests.empty():
            queued = session.requests.get_nowait()
            session.requests.task_done()
            self.router_lane_counters["dropped_on_redirect"] += 1
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
        if session.pending_route is not None:
            self.scene_entry_counters["superseded"] += 1
        self._clear_scene_entry(session)
        session.last_scene_route = None
        if session.scene_lane is not None:
            self.scene_lane_counters["dropped_on_redirect"] += len(session.scene_lane.queue)
            session.scene_lane.redirect()
        while not session.requests.empty():
            queued = session.requests.get_nowait()
            session.requests.task_done()
            self.router_lane_counters["dropped_on_redirect"] += 1
            if isinstance(queued, RouterLaneItem):
                session.emit(self.protocol.content(queued.request.message_id, queued.request.id, error=UNAVAILABLE))
        session.queued_leave = 0
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

    @staticmethod
    def _bound_player_id(session: Session) -> int:
        """返回会话当前有效绑定的玩家 ID；处于 LeaveGame 选角态或未绑定时返回 0（对齐 C++ BoundPlayerIdOf）。"""
        if (session.scene_route_reset_pending and not session.account) or session.player_id in (0, (1 << 64) - 1):
            return 0
        return session.player_id

    def find_active_session_by_player(self, player_id: int) -> Session | None:
        """按玩家 ID 查找当前未关闭的在线会话（供单进程本地模式公开查询，不直接遍历 sessions）。"""
        pid = int(player_id)
        if pid <= 0:
            return None
        for session in self.sessions.values():
            if session.player_id == pid and not session.writer.is_closing():
                return session
        return None

    def make_error_content(self, request, code: int):
        """构造统一错误应答信封（公开接口，避免跨模块直调私有 _error）。"""
        return self._error(request, code)

    @staticmethod
    def reset_session_on_leave(session: Session) -> None:
        """LeaveGame 返回选角态时由 Gate 统一清空会话绑定字段，禁止外部模块直接覆写内部属性。"""
        session.player_id = session.scene_id = session.scene_node_id = session.owner_revision = 0
        session.account = ""
        session.pending_route = None

    def close_superseded_session(self, session_id: int, incarnation: int, current: Session) -> None:
        """顶号成功后关闭被替换的旧会话（公开边界，不暴露 sessions 字典给外部直改）。"""
        previous = self.sessions.get(session_id)
        if previous is not None and previous is not current and previous.incarnation == incarnation:
            previous.close()

    def _deliver_to_session(self, session_id: int, player_id: int, content) -> None:
        session = self.sessions.get(session_id)
        if session is None or not session.verified:
            return
        bound_id = self._bound_player_id(session)
        if not player_id or (bound_id and bound_id == player_id):
            session.emit(content)

    @staticmethod
    def _clone_content_from_bytes(message_type, raw: bytes):
        copied = message_type()
        copied.ParseFromString(raw)
        return copied

    def broadcast(self, method: str, event, *, scene_node_id: int = 0, scene_uuid: str = "") -> None:
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
            msg_type = type(event.message_content)
            raw = event.message_content.SerializeToString()
            for index, session_id in enumerate(targets):
                self._deliver_to_session(
                    session_id,
                    event.player_list[index] if event.player_list else 0,
                    self._clone_content_from_bytes(msg_type, raw),
                )
        elif method == "BroadcastToScene" and event.scene_id:
            self.broadcast_content(event.message_content, scene_id=event.scene_id,
                                   scene_node_id=scene_node_id, scene_uuid=scene_uuid)
        elif method == "BroadcastToAll":
            self.broadcast_content(event.message_content)

    def broadcast_content(self, content, *, scene_id: int | None = None,
                          scene_node_id: int = 0, scene_uuid: str = "") -> None:
        """向匹配条件的在线会话广播隔离副本（公开边界；先冻结字节再逐会话克隆，禁止共享可变 Protobuf）。"""
        msg_type = type(content)
        raw = content.SerializeToString()
        for session in self.sessions.values():
            if (self._bound_player_id(session) and session.verified
                    and (scene_id is None or scene_id == session.scene_id)
                    and (not scene_node_id or session.scene_node_id == scene_node_id
                         and session.scene_instance_id == scene_uuid)):
                session.emit(self._clone_content_from_bytes(msg_type, raw))

    def _broadcast_all(self, content, *, scene_id: int | None = None,
                       scene_node_id: int = 0, scene_uuid: str = "") -> None:
        self.broadcast_content(content, scene_id=scene_id,
                               scene_node_id=scene_node_id, scene_uuid=scene_uuid)

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
