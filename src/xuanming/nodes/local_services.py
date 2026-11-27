"""单机本地模式进程内全微服务聚合枢纽（LocalServiceHub）。

在 ``xuanming run local``（以及 Windows ``启动玄冥服务器.bat``）单进程模式下，
复用与集群模式完全一致的 10 个正式微服务实现（Friend / Chat / Mail / Guild /
Team / Match / Rank / Payment / Trade / ContentGuard）及其 ``Memory*Store``，
通过进程内真 Ed25519 签名上下文直接分发客户端 RPC，实现零外部中间件依赖下的
全系统平替运行。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import secrets
import time
from collections.abc import Callable, Sequence
from itertools import count

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from google.protobuf.message import DecodeError, Message

from ..chat.model import (
    PlayerProfile,
    REVIEW_MASKED,
    REVIEW_PASS,
    REVIEW_REJECT,
    ReviewResult,
)
from ..chat.service import ChatService
from ..chat.store import MemoryChatStore
from ..content_guard.engine import Engine as ContentGuardEngine, Verdict as GuardVerdict
from ..content_guard.service import ContentGuardService
from ..data.config_tables import ConfigTables
from ..friend.model import OnlineStatus, PlayerBrief, ScanPageResult
from ..friend.service import FriendService
from ..friend.store import MemoryFriendStore
from ..grpc_runtime import GrpcUnavailable, session_metadata, sign_service_call
from ..guild.rank_store import MemoryGuildRankStore
from ..guild.service import GuildService
from ..guild.store import MemoryGuildStore
from ..login.caller_auth import CallerVerifier
from ..mail.service import MailService
from ..mail.store import MemoryMailStore
from ..match.config import MatchConfig
from ..match.gather import BattleNodeEntry, GatherPipeline, PlayerPresence
from ..match.match_service import MatchGrpcService
from ..match.ranked import RankedManager
from ..match.spectate import SpectateManager
from ..match.store import MemoryMatchStore
from ..match.team_service import TeamGrpcService
from ..match.tournament import TournamentManager
from ..payment.service import (
    MallNotFoundError,
    PaymentCore,
    PaymentGrpcService,
    PaymentOptions,
)
from ..payment.store import MemoryPaymentStore
from ..protocol import Protocol
from ..rank.service import RankService
from ..rank.store import MemoryRankStore
from ..trade.service import TradeService
from ..trade.store import MemoryTradeStore

logger = logging.getLogger(__name__)

_LOGIN_SERVICE = "loginpb.ClientPlayerLogin"
_TIP_UNAVAILABLE = 1003
_TIP_INVALID_PARAMETER = 1005
_MAIL_PERSONAL_PATH = "/mailpb.MailAdmin/SendPersonalMail"
_LOCAL_BATTLE_ENDPOINT = "local://battle"
_LOCAL_SCENE_ENDPOINT = "local://scene"


class _LocalGrpcAbort(Exception):
    """进程内 gRPC 上下文 ``context.abort`` 抛出的受控中断。"""

    def __init__(self, code: grpc.StatusCode, details: str = "") -> None:
        super().__init__(f"{code}: {details}")
        self.code = code
        self.details = details


class _LocalGrpcContext:
    """最小完整 ``grpc.aio.ServicerContext`` 进程内适配实现。"""

    def __init__(self, metadata: tuple[tuple[str, str | bytes], ...] = (), *, timeout_sec: float = 10.0) -> None:
        self._metadata = tuple(metadata)
        self._deadline = time.monotonic() + timeout_sec
        self.initial_metadata: list[tuple[str, str | bytes]] = []

    def invocation_metadata(self) -> tuple[tuple[str, str | bytes], ...]:
        return self._metadata

    async def send_initial_metadata(self, metadata) -> None:
        self.initial_metadata.extend(metadata)

    async def abort(self, code: grpc.StatusCode, details: str = "") -> None:
        raise _LocalGrpcAbort(code, details)

    def cancelled(self) -> bool:
        return False

    def time_remaining(self) -> float:
        return max(0.0, self._deadline - time.monotonic())


class _LocalDirectoryBridge:
    """为进程内微服务提供统一的角色护栏、档案、在线状态与公会归属查询。"""

    def __init__(self, protocol: Protocol, *, zone_id: int = 1) -> None:
        self.protocol = protocol
        self.zone_id = int(zone_id) or 1
        self.local_login = None
        self.gate = None
        self.scene = None
        self.battle = None
        self.guild_store: MemoryGuildStore | None = None
        self._id_gen = count(1)

    def _find_session(self, player_id: int):
        gate = self.gate
        if gate is None or not player_id:
            return None
        return gate.find_active_session_by_player(int(player_id))

    def _account_for(self, player_id: int) -> str:
        session = self._find_session(player_id)
        if session is not None and session.account:
            return str(session.account)
        if self.local_login is not None:
            account = self.local_login.account_of(player_id)
            if account:
                return account
        return ""

    def _player_display(self, player_id: int) -> PlayerBrief:
        name = ""
        level = 1
        class_id = 1
        gender = 1
        appearance_id = ""
        if self.local_login is not None:
            role = self.local_login.role_of(player_id)
            if role is not None:
                name = str(role.name or name)
                class_id = int(role.class_id or class_id)
                gender = int(role.gender or gender)
                appearance_id = str(role.appearance_id or appearance_id)
        if self.scene is not None:
            try:
                brief = self.scene.snapshot_player_brief(player_id)
                if brief is not None:
                    name = str(brief["name"] or name)
                    level = int(brief["level"] or level)
                    class_id = int(brief["class_id"] or class_id)
                    gender = int(brief["gender"] or gender)
                    appearance_id = str(brief["appearance_id"] or appearance_id)
            except Exception:
                logger.debug("local directory snapshot brief failed player_id=%s", player_id, exc_info=True)
        return PlayerBrief(
            name=name or f"player-{player_id}",
            level=level,
            class_id=class_id,
            gender=gender,
            appearance_id=appearance_id,
        )

    # -- RoleFenceDirectory / FriendDataServiceClient ------------------------------------------

    async def get_role_fence_state(self, player_id: int) -> Message | None:
        pid = int(player_id)
        if pid <= 0:
            return None
        account = self._account_for(pid)
        if not account:
            return None
        return self.protocol.message(
            "rolefence.RoleFenceState",
            player_id=pid,
            account=account,
            owner_revision=1,
            freeze_token=0,
            version=1,
            home_zone=self.zone_id,
            freeze_final=False,
        )

    async def allocate_request_id(self) -> int:
        return next(self._id_gen)

    async def batch_get_player_display(self, player_ids: Sequence[int]) -> dict[int, PlayerBrief]:
        return {int(pid): self._player_display(int(pid)) for pid in player_ids if int(pid) > 0}

    async def batch_get_player_name(self, player_ids: Sequence[int]) -> dict[int, str]:
        return {int(pid): self._player_display(int(pid)).name for pid in player_ids if int(pid) > 0}

    async def batch_get_names(self, player_ids: Sequence[int]) -> dict[int, str]:
        return await self.batch_get_player_name(player_ids)

    async def batch_get_player_home_zone(self, player_ids: Sequence[int]) -> dict[int, int]:
        return {int(pid): self.zone_id for pid in player_ids if int(pid) > 0}

    async def get_home_zone(self, player_id: int) -> int:
        return self.zone_id if int(player_id) > 0 else 0

    async def home_zone(self, player_id: int) -> int:
        return await self.get_home_zone(player_id)

    # -- Chat ActorVerifier / ProfileReader ----------------------------------------------------

    async def verify_actor(self, identity) -> None:
        pid = int(identity.player_id)
        account = self._account_for(pid)
        if not account or account != str(identity.account):
            raise PermissionError(f"local actor identity mismatch player_id={pid}")

    async def get_player_profile(self, player_id: int) -> PlayerProfile:
        brief = self._player_display(int(player_id))
        return PlayerProfile(level=max(1, brief.level), account_created_ms=1_700_000_000_000)

    # -- OnlineStatusResolver / PresenceProvider / GuildDirectory ------------------------------

    async def batch_online(self, player_ids: Sequence[int], *, strict: bool = False) -> dict[int, bool]:
        return {int(pid): self._find_session(int(pid)) is not None for pid in player_ids if int(pid) > 0}

    # -- Friend SessionStore -------------------------------------------------------------------

    async def batch_online_status(self, player_ids: Sequence[int]) -> dict[int, OnlineStatus]:
        now_ms = time.time_ns() // 1_000_000
        return {
            int(pid): OnlineStatus(
                online=self._find_session(int(pid)) is not None,
                last_active_ms=now_ms,
            )
            for pid in player_ids
            if int(pid) > 0
        }

    async def fill_online_status(self, player_ids: Sequence[int]) -> dict[int, OnlineStatus]:
        return await self.batch_online_status(player_ids)

    async def load_session(self, player_id: int) -> Message | None:
        return None

    async def scan_session_page(self, cursor, *, count: int = 100) -> ScanPageResult:
        return ScanPageResult(keys=(), current=cursor, next=None, restarted=False)

    async def mget_sessions_raw(self, keys: Sequence[str]) -> list[bytes | None]:
        return [None for _ in keys]

    async def get_presence(self, player_id: int) -> PlayerPresence:
        pid = int(player_id)
        session = self._find_session(pid)
        brief = self._player_display(pid)
        scene_info = self.scene.info if self.scene is not None else None
        gate_info = self.gate.info if self.gate is not None else None
        return PlayerPresence(
            player_id=pid,
            online=session is not None,
            in_battle=False,
            zone_id=self.zone_id,
            scene_node_id=int(scene_info.node_id) if scene_info is not None else 0,
            scene_instance_id=str(scene_info.node_uuid) if scene_info is not None else "",
            scene_grpc_endpoint=_LOCAL_SCENE_ENDPOINT if scene_info is not None else "",
            session_id=int(session.session_id) if session is not None else 0,
            gate_node_id=int(gate_info.node_id) if gate_info is not None else 0,
            gate_instance_id=str(gate_info.node_uuid) if gate_info is not None else "",
            name=brief.name,
            level=brief.level,
            class_id=brief.class_id,
            gender=brief.gender,
            appearance_id=brief.appearance_id,
        )

    async def guild_of(self, player_id: int) -> int:
        if self.guild_store is None:
            return 0
        try:
            view = await self.guild_store.get_player_guild(int(player_id))
            return int(view.guild.guild_id) if view is not None and view.guild is not None else 0
        except Exception:
            return 0


class _LocalContentGuardAdapter:
    """将进程内 ``ContentGuardEngine`` 适配为 Chat / Guild / Trade 的文本审查接口。"""

    def __init__(self, engine: ContentGuardEngine) -> None:
        self._engine = engine

    async def check_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
        async_platform: bool = False,
    ):
        result = self._engine.check(scene, text)
        if result.verdict == GuardVerdict.REJECT:
            return ReviewResult(verdict=REVIEW_REJECT, masked=result.masked_text, category=int(result.category))
        if result.verdict == GuardVerdict.MASKED:
            return ReviewResult(verdict=REVIEW_MASKED, masked=result.masked_text, category=int(result.category))
        return ReviewResult(verdict=REVIEW_PASS, masked=text, category=int(result.category))

    async def check_platform_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
    ) -> ReviewResult:
        return await self.check_text(scene, player_id, text, account=account)


class _LocalGuildContentGuardAdapter:
    """将进程内 ``ContentGuardEngine`` 适配为 Guild/Trade 期望返回 ``int`` verdict 的接口。"""

    def __init__(self, engine: ContentGuardEngine) -> None:
        self._engine = engine

    async def check_text(self, scene: int, player_id: int, text: str) -> int:
        return int(self._engine.check(scene, text).verdict)


class _LocalGatePusher:
    """将进程内微服务的下推通知直接路由到 ``GateNode`` 对应在线会话。"""

    def __init__(self, protocol: Protocol, bridge: _LocalDirectoryBridge) -> None:
        self.protocol = protocol
        self.bridge = bridge

    def _emit_to_player(self, player_id: int, message_id: int, payload: Message) -> None:
        gate = self.bridge.gate
        if gate is None:
            return
        session = self.bridge._find_session(int(player_id))
        if session is None:
            return
        request = self.protocol.message(
            "NodeRouteMessageRequest",
            header={"session_id": session.session_id, "target_player_id": int(player_id)},
            message_content=self.protocol.content(message_id, body=payload.SerializeToString()),
        )
        gate.push(request)

    # -- Chat WorldPusher ----------------------------------------------------------------------

    async def push_world(self, message: Message) -> None:
        gate = self.bridge.gate
        if gate is None:
            return
        msg_id = self.protocol.message_ids.get("ClientPlayerChatNotifyWorldChat")
        if msg_id is None:
            return
        gate.broadcast_content(self.protocol.content(msg_id, body=message.SerializeToString()))

    async def push_world_retract(self, notify: Message) -> None:
        gate = self.bridge.gate
        if gate is None:
            return
        msg_id = self.protocol.message_ids.get("ClientPlayerChatNotifyWorldChatRetracted")
        if msg_id is None:
            return
        gate.broadcast_content(self.protocol.content(msg_id, body=notify.SerializeToString()))

    # -- FriendPusher --------------------------------------------------------------------------

    async def push_friend_event(self, to_player_id: int, reason: int, by_player_id: int) -> None:
        msg_id = self.protocol.message_ids.get("ClientPlayerFriendNotifyFriendEvent")
        if msg_id is None:
            return
        notify = self.protocol.message(
            "friendpb.FriendEventS2C",
            reason=int(reason),
            by_player_id=int(by_player_id),
            ts_ms=time.time_ns() // 1_000_000,
        )
        self._emit_to_player(to_player_id, msg_id, notify)

    # -- MailNotifier --------------------------------------------------------------------------

    async def personal(self, player_id: int, mail_id: int) -> None:
        msg_id = self.protocol.message_ids.get("ClientPlayerMailNotifyMailChanged")
        if msg_id is None:
            return
        notify = self.protocol.message("mailpb.MailChangedEvent", mail_id=int(mail_id))
        self._emit_to_player(player_id, msg_id, notify)

    # -- GuildNotifier -------------------------------------------------------------------------

    async def notify(
        self,
        kind: int,
        guild_id: int,
        actor_player_id: int,
        target_player_id: int,
        recipients: Sequence[int],
    ) -> None:
        msg_id = self.protocol.message_ids.get("GuildServiceNotifyGuildEvent")
        if msg_id is None:
            return
        payload = self.protocol.message(
            "guildpb.GuildEventNotify",
            kind=int(kind),
            guild_id=int(guild_id),
            actor_player_id=int(actor_player_id),
            target_player_id=int(target_player_id),
        )
        for recipient in recipients:
            self._emit_to_player(int(recipient), msg_id, payload)

    # -- TeamNotifier --------------------------------------------------------------------------

    async def push_to_player(self, player_id: int, message_id: int, payload_msg: Message) -> None:
        self._emit_to_player(int(player_id), int(message_id), payload_msg)

    async def refresh_scene(self, player_id: int) -> None:
        scene = self.bridge.scene
        if scene is not None:
            scene.hint_team_refresh(int(player_id))


class _LocalBattleNodeResolver:
    """将进程内 ``BattleNode`` 暴露为 ``Match`` 集结流水线的可分配战斗节点。"""

    def __init__(self, bridge: _LocalDirectoryBridge) -> None:
        self.bridge = bridge

    def list_nodes(self, *, release_track: str | None = None) -> list[BattleNodeEntry]:
        battle = self.bridge.battle
        if battle is None or not battle.admitting:
            return []
        return [
            BattleNodeEntry(
                node_id=int(battle.node_id),
                node_uuid=str(battle.instance_id),
                grpc_endpoint=_LOCAL_BATTLE_ENDPOINT,
                physical_region=int(battle.physical_region or 1),
                client_host=str(battle.host or "127.0.0.1"),
                client_port=int(battle.port),
                release_track=release_track or "stable",
                admitting=bool(battle.admitting),
            )
        ]

    def mark_not_allocatable(self, node_uuid: str, *, cooldown_sec: float = 5.0) -> None:
        return None


class _LocalEndpointCaller:
    """进程内 ``EndpointCaller``，直连 ``BattleNode`` 与 ``SceneNode`` 内部方法。"""

    def __init__(self, protocol: Protocol, bridge: _LocalDirectoryBridge) -> None:
        self.protocol = protocol
        self.bridge = bridge

    async def call(
        self,
        endpoint: str,
        full_method: str,
        body: bytes,
        *,
        metadata: tuple = (),
        timeout: float | None = None,
    ) -> bytes:
        parts = full_method.lstrip("/").split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"invalid local endpoint method: {full_method}")
        service_name, method_name = parts
        descriptor = self.protocol.pool.FindServiceByName(service_name)
        method_desc = descriptor.methods_by_name[method_name]
        request = self.protocol.parse(method_desc.input_type.full_name, body)

        if service_name == "BattleNode":
            battle = self.bridge.battle
            if battle is None:
                raise GrpcUnavailable("local battle node not ready")
            mapping = {
                "CreateBattle": battle.create,
                "QueryBattleCreation": battle.query_creation,
                "DestroyBattle": battle.destroy,
                "AddObserver": battle.add_observer,
                "RemoveObserver": battle.remove_observer,
                "QueryObserver": battle.query_observer,
                "IssueBattleTicket": battle.issue_ticket,
                "AttachRelayGate": battle.attach_relay_gate,
                "PromoteSpectateBroadcast": battle.promote_spectate_broadcast,
            }
            handler = mapping.get(method_name)
            if handler is None:
                raise GrpcUnavailable(f"unsupported local battle method: {method_name}")
            result = handler(request)
            if inspect.isawaitable(result):
                result = await result
            return result.SerializeToString()

        if service_name == "scene_node.SceneNodeGrpc":
            scene = self.bridge.scene
            if scene is None:
                raise GrpcUnavailable("local scene node not ready")
            mapping = {
                "PrepareBattle": scene.battle.prepare,
                "CancelBattlePrepare": scene.battle.cancel,
            }
            handler = mapping.get(method_name)
            if handler is None:
                raise GrpcUnavailable(f"unsupported local scene method: {method_name}")
            result = handler(request)
            if inspect.isawaitable(result):
                result = await result
            return result.SerializeToString()

        raise GrpcUnavailable(f"unsupported local endpoint: {endpoint} {full_method}")

    async def close(self) -> None:
        return None


class _LocalMallClient:
    """单机模式下供 ``PaymentCore`` 使用的轻量内存商城适配。"""

    async def order_view(self, order_id: int, *, timeout_sec: float = 2.0):
        raise MallNotFoundError(f"local order not found: {order_id}")

    async def record_payment(self, *args, **kwargs):
        raise MallNotFoundError("local mall payment recording disabled")

    async def pending_refunds(self, *args, **kwargs):
        return ()

    async def report_refund(self, *args, **kwargs):
        return None

    async def close_refund(self, *args, **kwargs):
        return None

    async def push_revision(self, *args, **kwargs):
        return None

    async def close(self) -> None:
        return None


class _LocalMailSender:
    """实现 ``MailSender`` 协议（可调用 + ``close()``），供 ``SceneMailOutbox`` 直投本地 ``MailService``。"""

    def __init__(self, hub: LocalServiceHub) -> None:
        self._hub = hub

    async def __call__(self, player_id: int, entry) -> Message:
        return await self._hub.send_personal_mail(player_id, entry)

    async def close(self) -> None:
        return None


class LocalServiceHub:
    """单进程本地模式下托管全部 10 个客户端/内部微服务的统一进程内调度枢纽。

    采用惰性装配：构造阶段零耗时，首次收到非登录客户端 RPC 或内部调用时一次性
    实例化全部 10 个正式微服务及 ``Memory*Store``。
    """

    def __init__(
        self,
        protocol: Protocol,
        *,
        tables: ConfigTables | None = None,
        zone_id: int = 1,
    ) -> None:
        self.protocol = protocol
        self._tables = tables
        self.zone_id = int(zone_id) or 1
        self._internal_secret = secrets.token_bytes(32)
        self._mail_battle_secret = secrets.token_bytes(32)
        self.bridge = _LocalDirectoryBridge(protocol, zone_id=self.zone_id)
        self.pusher = _LocalGatePusher(protocol, self.bridge)
        self._endpoint_caller = _LocalEndpointCaller(protocol, self.bridge)
        self._battle_nodes = _LocalBattleNodeResolver(self.bridge)
        self._services: dict[str, dict[str, Callable]] = {}
        self._initialized = False
        self._signer: Ed25519PrivateKey | None = None
        self.chat = None
        self.mail_sender = _LocalMailSender(self)

    @property
    def internal_secret(self) -> bytes:
        return self._internal_secret

    @property
    def tables(self) -> ConfigTables:
        if self._tables is None:
            self._tables = ConfigTables()
        return self._tables

    def ensure_ready(self) -> None:
        """确保 10 个进程内微服务已完成初始化（幂等）。"""
        if self._initialized:
            return
        self._initialized = True
        protocol = self.protocol
        tables = self.tables

        # 进程内同样走正式 Ed25519 签名与 CallerVerifier 验签链路，不绕过鉴权边界。
        self._signer = Ed25519PrivateKey.generate()
        self._verifier = CallerVerifier([self._signer.public_key()], allowed_callers=("gate",))

        # 1. ContentGuard
        self.content_guard_engine = ContentGuardEngine()
        self.content_guard = ContentGuardService(protocol, self.content_guard_engine)
        chat_reviewer = _LocalContentGuardAdapter(self.content_guard_engine)
        guild_guard = _LocalGuildContentGuardAdapter(self.content_guard_engine)

        # 2. Friend
        self.friend_store = MemoryFriendStore(protocol, self.bridge)
        self.friend = FriendService(
            protocol,
            self.friend_store,
            self._verifier,
            coordinator_secret=self._internal_secret,
            sessions=self.bridge,
            pusher=self.pusher,
        )

        # 3. Chat
        self.chat_store = MemoryChatStore(
            protocol,
            self.bridge,
            reviewer=chat_reviewer,
            profile_reader=self.bridge,
        )
        self.chat = ChatService(
            protocol,
            self.chat_store,
            self._verifier,
            world_pusher=self.pusher,
            platform_reviewer=chat_reviewer,
        )

        # 4. Guild
        self.guild_store = MemoryGuildStore(protocol, role_directory=self.bridge)
        self.bridge.guild_store = self.guild_store
        self.guild_rank_store = MemoryGuildRankStore()
        self.guild = GuildService(
            protocol,
            self.guild_store,
            rank_store=self.guild_rank_store,
            caller_verifier=self._verifier,
            coordinator_secret=self._internal_secret,
            home_zones=self.bridge,
            player_names=self.bridge,
            online_resolver=self.bridge,
            content_guard=guild_guard,
            notifier=self.pusher,
        )

        # 5. Mail
        item_table = self.tables["Item"]
        self.mail_store = MemoryMailStore(
            protocol,
            item_exists=lambda item_id: item_table.get(int(item_id)) is not None,
            guilds=self.bridge,
            role_directory=self.bridge,
        )
        self.mail = MailService(
            protocol,
            self.mail_store,
            caller_verifier=self._verifier,
            producer_secrets={"battle": self._mail_battle_secret, "scene": self._internal_secret},
            coordinator_secret=self._internal_secret,
            notifier=self.pusher,
        )

        # 6. Team + Match (+ Ranked + Tournament + Spectate)
        self.match_store = MemoryMatchStore(protocol)
        self.match_config = MatchConfig(
            zone_id=self.zone_id,
            battle_outcome_secret=self._internal_secret,
            scene_secret=self._internal_secret,
            coordinator_secret=self._internal_secret,
            player_inbox_secret=self._internal_secret,
            guild_secret=self._internal_secret,
            ops_secret=self._internal_secret,
            rank_secret=self._internal_secret,
            spectate_secret=secrets.token_bytes(32),
        )
        self.gather = GatherPipeline(
            protocol,
            self.match_store,
            self.match_config,
            self._endpoint_caller,
            self.bridge,
            self._battle_nodes,
        )
        self.spectate = SpectateManager(
            protocol,
            self.match_store,
            self.match_config,
            self._endpoint_caller,
            self.bridge,
        )
        self.ranked = RankedManager(protocol, self.match_store, tables=self.tables)
        self.tournament = TournamentManager(protocol, tables=self.tables)
        self.team = TeamGrpcService(
            protocol,
            self.match_store,
            self.match_config,
            gate_verifier=self._verifier,
            presence=self.bridge,
            battle_nodes=self._battle_nodes,
            gather=self.gather,
            notifier=self.pusher,
            ranked=self.ranked,
            inline_gather=True,
        )
        self.match = MatchGrpcService(
            protocol,
            self.match_store,
            self.match_config,
            gate_verifier=self._verifier,
            presence=self.bridge,
            battle_nodes=self._battle_nodes,
            gather=self.gather,
            spectate=self.spectate,
            ranked=self.ranked,
            tournament=self.tournament,
            inline_gather=True,
        )

        # 7. Rank
        self.rank_store = MemoryRankStore(
            protocol,
            item_exists=lambda item_id: item_table.get(int(item_id)) is not None,
        )
        self.rank = RankService(
            protocol,
            self.rank_store,
            producer_secrets={"scene": self._internal_secret, "match": self._internal_secret},
            caller_verifier=self._verifier,
            name_reader=self.bridge,
        )

        # 8. Payment
        self.payment_store = MemoryPaymentStore()
        self.payment_core = PaymentCore(
            self.payment_store,
            _LocalMallClient(),
            channels=(),
            platforms={},
            options=PaymentOptions(),
        )
        self.payment = PaymentGrpcService(
            protocol,
            self.payment_core,
            caller_verifier=self._verifier,
        )

        # 9. Trade (聚宝斋)
        self.trade_store = MemoryTradeStore(protocol, role_directory=self.bridge)
        self.trade = TradeService(
            protocol,
            self.trade_store,
            caller_verifier=self._verifier,
            coordinator_secret=self._internal_secret,
            home_zones=self.bridge,
            content_guard=guild_guard,
        )

        for service in (
            self.content_guard,
            self.friend,
            self.chat,
            self.mail,
            self.guild,
            self.team,
            self.match,
            self.rank,
            self.payment,
            self.trade,
        ):
            for svc_name, methods in service.services().items():
                self._services.setdefault(svc_name, {}).update(methods)

    def bind_runtime(self, *, local_login=None, gate=None, scene=None, battle=None) -> None:
        """在 ``Runtime`` 各节点启动就绪后绑定进程内引用。"""
        if local_login is not None:
            self.bridge.local_login = local_login
        if gate is not None:
            self.bridge.gate = gate
        if scene is not None:
            self.bridge.scene = scene
        if battle is not None:
            self.bridge.battle = battle

    @property
    def registered_services(self) -> tuple[str, ...]:
        self.ensure_ready()
        return tuple(sorted(self._services))

    async def dispatch_client(self, gate, session, route, request, seq):
        """按与 ``ClientRpcRouterService`` 相同的会话签名语义调度进程内微服务。"""
        self.ensure_ready()
        methods = self._services.get(route.service)
        if methods is None:
            return gate._error(request, _TIP_UNAVAILABLE)
        handler = methods.get(route.method)
        if handler is None:
            return gate._error(request, _TIP_UNAVAILABLE)
        try:
            payload = self.protocol.parse(route.request_type, request.body)
        except DecodeError:
            return gate._error(request, _TIP_INVALID_PARAMETER)
        subject = gate.details(session, seq).SerializeToString()
        metadata = session_metadata(subject, route.path, self._signer) + (("x-release-track", "stable"),)
        context = _LocalGrpcContext(metadata)
        try:
            result = handler(payload, context, route.path)
            if inspect.isawaitable(result):
                result = await result
        except _LocalGrpcAbort as exc:
            tip = _TIP_INVALID_PARAMETER if exc.code == grpc.StatusCode.INVALID_ARGUMENT else _TIP_UNAVAILABLE
            logger.debug("local hub rpc aborted path=%s code=%s details=%s", route.path, exc.code, exc.details)
            return gate._error(request, tip)
        except Exception:
            logger.warning("local hub rpc failed path=%s", route.path, exc_info=True)
            return gate._error(request, _TIP_UNAVAILABLE)
        return self.protocol.content(
            request.message_id,
            request.id,
            body=b"" if result is None else result.SerializeToString(),
        )

    async def call_raw(self, path: str, body: bytes, *, metadata: tuple = (), timeout: float | None = None) -> bytes:
        """兼容 ``GrpcClient.call(path, body)`` 的进程内原始字节调用入口。"""
        self.ensure_ready()
        parts = path.lstrip("/").split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"invalid local rpc path: {path}")
        service_name, method_name = parts
        handler = self._services.get(service_name, {}).get(method_name)
        if handler is None:
            raise GrpcUnavailable(f"local service method unavailable: {path}")
        descriptor = self.protocol.pool.FindServiceByName(service_name)
        method_desc = descriptor.methods_by_name[method_name]
        request = self.protocol.parse(method_desc.input_type.full_name, body)
        context = _LocalGrpcContext(metadata, timeout_sec=10.0 if timeout is None else timeout)
        try:
            result = handler(request, context, path)
            if inspect.isawaitable(result):
                result = await result
        except _LocalGrpcAbort as exc:
            raise GrpcUnavailable(f"{exc.code}: {exc.details}") from exc
        return b"" if result is None else result.SerializeToString()

    async def send_personal_mail(self, player_id: int, entry) -> Message:
        """供本地 ``SceneMailOutbox`` 直投进程内 ``MailService``。"""
        self.ensure_ready()
        request = self.protocol.message(
            "mailpb.SendMailRequest",
            sender_kind=5,
            request_id=entry.request_id,
            recipient_player_id=int(player_id),
            title=entry.title,
            body=entry.body,
        )
        for item in entry.items:
            request.attachments.items.add(config_id=item.config_id, count=item.count)
        sign_service_call(_MAIL_PERSONAL_PATH, request, "battle", time.time_ns() // 1_000_000, self._mail_battle_secret)
        raw = await self.call_raw(_MAIL_PERSONAL_PATH, request.SerializeToString())
        return self.protocol.parse("mailpb.SendMailResponse", raw)

    async def close(self) -> None:
        if not self._initialized:
            return
        closers = []
        for svc in (self.chat, self._endpoint_caller):
            if svc is None:
                continue
            close_fn = getattr(svc, "close", None)
            if callable(close_fn):
                res = close_fn()
                if inspect.isawaitable(res):
                    closers.append(res)
        if closers:
            await asyncio.gather(*closers, return_exceptions=True)


class LocalGatewayForwarder:
    """本地单进程 Gate 前置转发器：登录交由 ``LocalLogin``，其余客户端服务交由 ``LocalServiceHub``。"""

    def __init__(self, login, hub: LocalServiceHub) -> None:
        self.login = login
        self.hub = hub

    async def __call__(self, gate, session, route, request, seq):
        if route.service == _LOGIN_SERVICE:
            return await self.login(gate, session, route, request, seq)
        return await self.hub.dispatch_client(gate, session, route, request, seq)
