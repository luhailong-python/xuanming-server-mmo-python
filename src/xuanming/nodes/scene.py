import asyncio
import inspect
import logging
import math
import time
from collections import defaultdict
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import count
from weakref import WeakValueDictionary

from google.protobuf.message import DecodeError

from ..data import PlayerRowKind, PlayerSnapshot, StoreRejected, StoreUnavailable
from ..data.navigation import navigation_contract
from ..protocol import RouteNode, RpcKind
from .scene_afk import FRAME_MASK, FrameActivity
from .scene_actor_sync import SceneActorSync
from .scene_aoi import SceneAoi
from .scene_aoi_load import AoiLoadMonitor, PRESSURE_LEVELS
from .scene_channels import SceneChannels
from .scene_combat_engagement import SceneCombatEngagement
from .scene_connection_hold import (
    HOLD_WINDOW, SWEEP_INTERVAL, HoldDecision, SceneConnectionHolds, decide_hold, session_bound,
)
from .scene_player import actor_profile_fields, register_player_handlers
from .scene_planned_drain import RelocationPacer

logger = logging.getLogger(__name__)
Position = tuple[float, float, float]
ZERO: Position = (0., 0., 0.)
MOVE_SPEED = 10.0
MOVE_CORRECTION_EPSILON = 0.5
TELEPORT_SETTLE_SECONDS = 3.0
TELEPORT_SETTLE_RADIUS = 3.0
DEFAULT_SPAWN: Position = (180., 200., 0.)
SAVE_INTERVAL = 30
SHUTDOWN_SAVE_BUDGET = 15.0
SHUTDOWN_SAVE_RETRY = 0.25
STALE_SAVE_RESULTS = {"SAVE_PLAYER_STALE_OWNER", "SAVE_PLAYER_NOT_HOLDER", "SAVE_PLAYER_UNCLAIMED"}
_SPECTATE_CHAT_PREF_ALLOW = 1
_SPECTATE_CHAT_PREF_FORBID = 2
_INVALID_PARAMETER = 1005
_FEATURE_UNAVAILABLE = 1006
_SAVE_SEQ_MAX = (1 << 64) - 1

# 与 C++ player_handoff_gate 一致：默认不允许跨场景重放。
_HANDOFF_SERVICES = frozenset({
    "SceneBagClientPlayer", "SceneAttributeClientPlayer", "ScenePetClientPlayer",
    "SceneMissionClientPlayer", "SceneCurrencyClientPlayer", "SceneActivityClientPlayer",
    "SceneBattleResultClientPlayer",
})
_HANDOFF_METHODS = frozenset({
    ("SceneSkillClientPlayer", "ListSkills"), ("SceneSkillClientPlayer", "UpgradeSkill"),
    ("SceneSceneClientPlayer", "GetSyncDigest"), ("SceneSceneClientPlayer", "SceneInfoC2S"),
    ("SceneSceneClientPlayer", "GetSpectateChatSetting"),
    ("SceneSceneClientPlayer", "SetSpectateChatSetting"),
})


def _replayable_on_another_scene(route) -> bool:
    return bool(route and route.client_protocol and route.node_type == RouteNode.SCENE
                and (route.service in _HANDOFF_SERVICES or (route.service, route.method) in _HANDOFF_METHODS))


def _may_retry_handoff(route) -> bool:
    return bool(_replayable_on_another_scene(route) and route.kind == RpcKind.REQUEST)


def xyz(value) -> Position:
    return value.x, value.y, value.z


def vector(value: Position) -> dict:
    return dict(zip(("x", "y", "z"), value))


@dataclass(slots=True)
class Transform:
    position: Position = ZERO
    rotation: Position = ZERO

    def wire(self) -> dict:
        return {
            "location": vector(self.position), "rotation": vector(self.rotation),
            "scale": vector((1., 1., 1.)),
        }


@dataclass(slots=True)
class Motion:
    velocity: Position = ZERO
    teleport_target: Position | None = None
    teleport_until: float = 0.


@dataclass(slots=True)
class PendingSave:
    rows: dict[int, bytes]
    force: bool
    periodic: bool
    release: bool
    on_committed: Callable[[PlayerSnapshot], None] | None = None


@dataclass(slots=True)
class Player:
    entity: int
    player_id: int
    session_id: int
    gate_uuid: str
    generation: int
    owner_revision: int
    snapshot: PlayerSnapshot
    transform: Transform
    home_zone_id: int = 0
    admission: object = None
    motion: Motion = field(default_factory=Motion)
    disconnect_at: float | None = None
    disconnect_save_due: bool = False
    save_at: float = field(default_factory=lambda: time.monotonic() + SAVE_INTERVAL)
    pending_save: PendingSave | None = None
    seen_battles: set[int] = field(default_factory=set)
    battle_expired_id: int = 0
    asset_anomaly: dict | None = None
    asset_wake_after: float = 0.
    activity: FrameActivity = field(default_factory=FrameActivity)

    @property
    def role_identity(self):
        return self.admission.identity if self.admission is not None else None


@dataclass(slots=True)
class Npc:
    entity: int
    world: "World"
    transform: Transform
    attributes: object = None
    derived: object = None
    level: int = 1


class World:
    """事件循环持有实体和 AOI 空间索引。"""

    def __init__(self, info, nav=None, *, pressure: float = 0.0) -> None:
        self.info = info
        self.nav = nav
        self.players: dict[int, Player] = {}
        self.npcs: dict[int, Npc] = {}
        self.actors: dict[int, Player | Npc] = {}
        self.pending_entrants = 0
        self.draining = False
        self.aoi = SceneAoi(pressure=pressure)
        self.aoi_reentry: set[int] = set()

    def add(self, player: Player) -> None:
        self.players[player.player_id] = player
        self.actors[player.entity] = player

    def remove(self, player: Player) -> None:
        del self.players[player.player_id]
        del self.actors[player.entity]
        self.aoi_reentry.discard(player.entity)

    def add_npc(self, npc: Npc) -> None:
        self.npcs[npc.entity] = npc
        self.actors[npc.entity] = npc

    def remove_npc(self, npc: Npc) -> None:
        del self.npcs[npc.entity]
        del self.actors[npc.entity]

    def nearby(self, actor: Player | Npc) -> set[int]:
        return self.aoi.nearby(actor.entity)


class SceneNode:
    def __init__(self, protocol, info, store, rows, emit, *, emit_many=None,
                 disconnect_grace: float = HOLD_WINDOW, tick_interval: float = 0.05) -> None:
        self.protocol = protocol
        self.info = info
        self.store = store
        self.rows = rows
        self.emit = emit
        self.emit_many = emit_many
        self.disconnect_grace = disconnect_grace
        self.tick_interval = tick_interval if math.isfinite(tick_interval) and tick_interval > 0 else 0.05
        self.aoi_load = AoiLoadMonitor()
        self._aoi_pressure_level = 0
        self.aoi_trimmed_entries_total = 0
        self.worlds: dict[int, World] = {}
        self._placement: dict[int, tuple[int, int]] = {}
        self.locations: dict[int, World] = {}
        self.sessions: dict[tuple[str, int], Player] = {}
        self._pending_sessions: set[tuple[str, int]] = set()
        self._entry_tasks: set[asyncio.Task] = set()
        self._entries_drained = asyncio.Event()
        self._entries_drained.set()
        self._maintenance: dict[int, asyncio.Task] = {}
        self._ids = count(1)
        self._locks: WeakValueDictionary[int, asyncio.Lock] = WeakValueDictionary()
        self._actor_tasks: set[asyncio.Task] = set()
        self._closing = False
        self._closed = False
        self._admission_closed = False
        self._planned_drain_pacer = None
        self.current_frame = 0
        self.battle = None
        self.travel = None
        self.combat_engagement = SceneCombatEngagement(self)
        self.inbox = None
        self.mail_outbox = None
        self.landing = None
        self.team = None
        self.connection_holds = SceneConnectionHolds(on_hosting=self._hosting_started, hold_seconds=disconnect_grace)
        self.navigation = {}
        self.actor_sync = SceneActorSync(self, emit_many)
        self.channels = SceneChannels(self)
        self.buffs = None
        self.skill_casts = None
        self.initialize_player = None
        self.tables = None
        # 对齐 C++ enter_fence / release_fence / save_seq / player_lifecycle 单源计数（日志与指标出口同源）
        self.enter_fence_counters: dict[str, int] = {
            "admit_new": 0,
            "admit_reentry": 0,
            "admit_relocate": 0,
            "reject_incomplete": 0,
            "reject_closing_or_draining": 0,
            "reject_sealed": 0,
            "reject_hosted_invalid": 0,
            "reject_stale_generation": 0,
            "reject_stale_epoch": 0,
            "reject_revision_mismatch": 0,
            "reject_handoff_busy": 0,
            "reject_hold_not_resumable": 0,
            "bind_rejected": 0,
            "session_update_rejected": 0,
            "stale_gate_instance_rejected": 0,
        }
        self.release_fence_counters: dict[str, int] = {
            "accepted": 0,
            "not_found": 0,
            "rejected_stale_epoch": 0,
            "rejected_stale_session": 0,
            "session_ended": 0,
            "session_ended_kept_held": 0,
        }
        self.exit_persist_counters: dict[str, int] = {
            "exit_superseded": 0,
            "exit_intent_missing": 0,
            "exit_resave": 0,
            "exit_resave_capped": 0,
            "exit_exhausted_rekick": 0,
            "exit_fastpath_deferred": 0,
            "exit_client_msg_rejected": 0,
            "exit_deposed_on_reentry": 0,
        }
        self.connection_lost_hold_counters: dict[str, int] = {
            "held": 0,
            "resumed": 0,
            "expired": 0,
            "superseded_by_exit": 0,
            "exit_now": 0,
            "hosted_arrived": 0,
            "hosted_relocating": 0,
        }
        self.save_seq_counters: dict[str, int] = {
            "observe_beyond_bound": 0,
            "personal_floor_issued": 0,
            "exhausted": 0,
        }
        self._save_seq_floor: dict[int, int] = {}
        self._mutating: set[tuple[str, str]] = {
            ("SceneSceneClientPlayer", "SetSpectateChatSetting"),
        }
        self._handlers = {
            "SceneMovementClientPlayer": {
                "MoveStart": self._movement, "MoveStop": self._movement, "MoveSync": self._movement,
                "TeleportRequest": self._teleport_request,
            },
            "SceneSceneClientPlayer": {
                "SceneInfoC2S": self._scene_info,
                "GetSpectateChatSetting": self._get_spectate_chat_setting,
                "SetSpectateChatSetting": self._set_spectate_chat_setting,
            },
        }
        register_player_handlers(self)

    def collect_lifecycle_metrics(self) -> dict[str, dict[str, int]]:
        """返回与 C++ /metrics 同源的入场/释放/退出/断线保留/存盘序号计数快照。"""
        return {
            "enter_fence": dict(self.enter_fence_counters),
            "release_fence": dict(self.release_fence_counters),
            "exit_persist": dict(self.exit_persist_counters),
            "connection_lost_hold": dict(self.connection_lost_hold_counters),
            "save_seq": dict(self.save_seq_counters),
        }

    def format_lifecycle_summary(self) -> dict[str, str]:
        """按与 C++ Visit* 相同的键顺序格式化结构化汇总行，避免日志与指标漂移。"""
        tags = (
            ("enter_fence", "[EnterFence]", self.enter_fence_counters),
            ("release_fence", "[ReleaseFence]", self.release_fence_counters),
            ("exit_persist", "[ExitPersist]", self.exit_persist_counters),
            ("connection_lost_hold", "[ConnectionLostHold]", self.connection_lost_hold_counters),
            ("save_seq", "[SaveSeq]", self.save_seq_counters),
        )
        return {
            name: f"{prefix} " + " ".join(f"{k}={v}" for k, v in counters.items())
            for name, prefix, counters in tags
        }

    def register(self, service: str, method: str, handler, *, mutating: bool = False) -> None:
        route = next((
            route for route in self.protocol.routes.values()
            if route.service == service and route.method == method
            and route.node_type == RouteNode.SCENE
            and route.client_protocol and route.kind in (RpcKind.REQUEST, RpcKind.ONE_WAY)
        ), None)
        if route is None:
            raise ValueError(f"未知场景客户端入口 {service}/{method}")
        methods = self._handlers.setdefault(service, {})
        if method in methods:
            raise ValueError(f"场景入口已经注册 {service}/{method}")
        methods[method] = handler
        if mutating:
            self._mutating.add((service, method))

    def create(self, request):
        if self._closing or self._admission_closed:
            raise ValueError("场景或配表 ID 无效")
        scene_id = request.scene_id
        generation = request.placement_generation
        newest, destroyed = self._placement.get(scene_id, (0, 0))
        if not scene_id or destroyed and generation <= destroyed:
            return self.protocol.message("CreateSceneResponse")
        if generation:
            self._placement[scene_id] = max(generation, newest), destroyed
        info = self.protocol.message(
            "SceneInfoComp", scene_id=request.scene_id, scene_config_id=request.config_id,
            mirror_config_id=request.mirror_config_id, dungeon_config_id=request.dungeon_config_id,
            creators=dict.fromkeys(request.creator_ids, True),
        )
        world = self.worlds.setdefault(
            scene_id, World(info, self.navigation.get(request.config_id), pressure=self.aoi_load.pressure_factor),
        )
        return self.protocol.message("CreateSceneResponse", scene_info=world.info)

    def destroy(self, request):
        generation = request.placement_generation
        newest, destroyed = self._placement.get(request.scene_id, (0, 0))
        if generation:
            if newest > generation:
                return self.protocol.message("Empty")
            self._placement[request.scene_id] = newest, max(destroyed, generation)
        world = self.worlds.get(request.scene_id)
        if world is not None:
            if world.players or world.pending_entrants:
                world.draining = True
                if self.travel:
                    self.travel.drain(world)
            else:
                for npc in tuple(world.npcs.values()):
                    self.despawn_npc(npc)
                del self.worlds[request.scene_id]
        return self.protocol.message("Empty")

    def _actor_id(self) -> int:
        sequence = next(self._ids)
        if not self.info.node_id or sequence >= 1 << 32:
            raise RuntimeError("场景实体网络编号无法分配")
        return self.info.node_id << 32 | sequence

    def spawn_npc(self, world: World, transform: Transform, attributes=None, *, derived=None, level=1) -> Npc:
        """Attach a server-created NPC to an active world and assign its wire ID."""
        if (self._closing or world.draining or self.worlds.get(world.info.scene_id) is not world
                or not all(math.isfinite(value) for value in transform.position + transform.rotation)):
            raise ValueError("NPC 场景或坐标无效")
        npc = Npc(self._actor_id(), world, Transform(transform.position, transform.rotation), attributes, derived, level)
        world.add_npc(npc)
        self._enter_aoi(world, npc)
        return npc

    def despawn_npc(self, npc: Npc) -> None:
        world = npc.world
        if world.npcs.get(npc.entity) is not npc:
            return
        self.combat_engagement.on_leave(npc)
        self._visibility(world, world.aoi.leave(npc.entity), departed=npc.entity)
        world.remove_npc(npc)
        self.actor_sync.discard(npc)
        if self.buffs is not None:
            self.buffs.discard(npc)

    async def enter(self, request, gate_uuid: str, *, hosted: bool = False):
        player_id = request.player_id
        if self._closing or self._admission_closed or not all((player_id, request.owner_revision)) or not session_bound(request.session_id):
            key = "reject_closing_or_draining" if (self._closing or self._admission_closed) else "reject_incomplete"
            self.enter_fence_counters[key] += 1
            raise ValueError("入场身份不完整")
        if self.travel and not all((request.owner_epoch, request.login_generation)):
            self.enter_fence_counters["reject_incomplete"] += 1
            raise ValueError("入场身份不完整")
        async with self._lock(player_id):
            if self._closing or self._admission_closed:
                self.enter_fence_counters["reject_closing_or_draining"] += 1
                raise ValueError("场景正在关闭")
            if self.travel and self.travel.sealed(player_id):
                self.enter_fence_counters["reject_sealed"] += 1
                raise ValueError("角色原入场世代正在封存")
            world = self.worlds.get(request.scene_id)
            if world is None or world.draining:
                self.enter_fence_counters["reject_closing_or_draining"] += 1
                raise ValueError("场景尚未创建")
            key = gate_uuid, request.session_id
            with self._reserve_entry(world, key, player_id):
                previous = self.locations.get(player_id)
                old = previous.players[player_id] if previous is not None else None
                published = old is not None and self.travel and self.travel.published(player_id)
                replacement_admission = None
                if old is not None:
                    if hosted and old.disconnect_at is None:
                        self.enter_fence_counters["reject_hosted_invalid"] += 1
                        raise ValueError("在线角色不能接收托管落点")
                    if hosted and (self._releasing(old) or not self.connection_holds.held(old)):
                        self.enter_fence_counters["reject_hosted_invalid"] += 1
                        raise ValueError("退出中的角色不能接收托管落点")
                    same_session = (old.session_id == request.session_id if hosted
                                    else (old.gate_uuid, old.session_id) == key)
                    # 原版 enter_fence：同一绑定会话允许迟到请求，先校验再处理交接。
                    if (hosted and (not same_session or request.login_generation != old.generation)
                            or not hosted and not same_session and request.login_generation <= old.generation):
                        self.enter_fence_counters["reject_stale_generation"] += 1
                        raise ValueError("过期的场景入场请求")
                if published and request.owner_epoch > old.snapshot.owner_epoch:
                    # 原版 RoleAdmission 在废黜旧实体前完成；新账号修订由权威服务验证。
                    replacement_admission = await self.travel.admit(request)
                    try:
                        await self.travel.confirm(replacement_admission)
                    except BaseException:
                        await self.travel.retire(replacement_admission)
                        raise
                    self.travel.superseded(old)
                    self.exit_persist_counters["exit_deposed_on_reentry"] += 1
                    self._evict(previous, old)
                    try:
                        await self.travel.retire(old.admission, old.snapshot, transferred=True)
                    except Exception:
                        logger.exception("关闭已被新代次接管的旧入场资格失败 player_id=%s", player_id)
                    old = None
                if old is not None:
                    if self.battle and self.battle.frozen(player_id) and previous is not world:
                        self.enter_fence_counters["reject_handoff_busy"] += 1
                        raise ValueError("角色备战期间不可切换场景")
                    if request.owner_epoch and request.owner_epoch < old.snapshot.owner_epoch:
                        self.enter_fence_counters["reject_stale_epoch"] += 1
                        raise ValueError("过期的场景入场请求")
                    if request.owner_revision != old.owner_revision:
                        self.enter_fence_counters["reject_revision_mismatch"] += 1
                        raise ValueError("过期的场景入场请求")
                    same_epoch = request.owner_epoch in (0, old.snapshot.owner_epoch)
                    rebound = not same_session or old.generation < request.login_generation
                    if published and (rebound or not same_epoch):
                        self.enter_fence_counters["reject_handoff_busy"] += 1
                        raise ValueError("玩家原入场世代正在交接")
                    if not hosted and self.connection_holds.held(old) and not self.connection_holds.resumable(
                        old, request.session_id, request.login_generation,
                    ):
                        self.enter_fence_counters["reject_hold_not_resumable"] += 1
                        raise ValueError("已断开的会话不能接回角色")
                    if same_epoch and not self._releasing(old):
                        was_held = self.connection_holds.held(old)
                        if self.travel and (rebound or previous is not world and not published):
                            await self.flush_player_rows(old)
                            former = old.admission
                            try:
                                admitted = await self.travel.admit(request)
                                try:
                                    await self.travel.confirm(admitted)
                                except BaseException:
                                    await self.travel.retire(admitted)
                                    raise
                            except BaseException:
                                # A failed registration can revoke the old fence as
                                # well. Hosted players have no client to re-enter, so
                                # restore their old admission before keeping them in
                                # the original scene. If that cannot be confirmed,
                                # evict rather than retain an unfenced writer.
                                restored = False
                                if hosted and not asyncio.current_task().cancelling():
                                    try:
                                        restore = type(request)()
                                        restore.CopyFrom(request)
                                        restore.scene_id = previous.info.scene_id
                                        restore.owner_epoch = old.snapshot.owner_epoch
                                        restore.session_id = old.session_id
                                        restore.login_generation = old.generation
                                        admitted = await self.travel.admit(restore)
                                        try:
                                            await self.travel.confirm(admitted)
                                        except BaseException:
                                            await self.travel.retire(admitted)
                                            raise
                                        old.admission = admitted
                                        restored = True
                                        if former:
                                            try:
                                                await self.travel.retire(former, transferred=True)
                                            except Exception:
                                                logger.exception("关闭恢复前的托管入场世代失败 player_id=%s", player_id)
                                    except Exception:
                                        logger.exception("恢复托管角色原场景入场资格失败 player_id=%s", player_id)
                                    except BaseException:
                                        self._evict(previous, old)
                                        raise
                                if not restored:
                                    self._evict(previous, old)
                                raise
                            old.admission = admitted
                            if former:
                                try:
                                    await self.travel.retire(former, transferred=True)
                                except Exception:
                                    logger.exception("关闭旧角色入场世代待重试 player_id=%s", player_id)
                        if not hosted and (rebound or old.disconnect_at is not None):
                            self.connection_holds.forget(old)
                            if was_held:
                                self.connection_lost_hold_counters["resumed"] += 1
                            old_key = old.gate_uuid, old.session_id
                            if self.sessions.get(old_key) is old:
                                del self.sessions[old_key]
                            old.gate_uuid, old.session_id = key
                            old.disconnect_at = None
                            old.disconnect_save_due = False
                            old.motion = Motion()
                            self.actor_sync.mark(old, "velocity")
                            self.sessions[key] = old
                        old.generation = max(old.generation, request.login_generation)
                        if previous is not world:
                            self.enter_fence_counters["admit_relocate"] += 1
                            self._relocate(previous, world, old)
                        else:
                            self.enter_fence_counters["admit_reentry"] += 1
                            if rebound or request.enter_gs_type in (2, 3):
                                self._announce_neighbors(world, old)
                        if rebound and self.battle:
                            await self.battle.restore(old)
                        if rebound and self.inbox:
                            self.inbox.landed(old)
                        if self.landing:
                            self.landing.wake(old)
                        if self.team:
                            self.team.entered(old)
                        return self.protocol.message("Empty")
                    await self._remove(previous, old, release_credential=False)
                player = await self._load_player(request, gate_uuid, world, admission=replacement_admission)
                if self._closing or self._admission_closed:
                    # Shutdown can start while load/claim is awaiting I/O. Do
                    # not publish a late actor after the shutdown snapshot.
                    try:
                        await self.flush_player_rows(player)
                        if not player.snapshot.released:
                            player.snapshot = await self.store.release(player.snapshot)
                        if self.travel and player.admission:
                            await self.travel.retire(player.admission, player.snapshot)
                    except Exception:
                        logger.exception("停机期间撤销入场失败 player_id=%s", player_id)
                    self.enter_fence_counters["reject_closing_or_draining"] += 1
                    raise ValueError("场景正在关闭")
                if hosted:
                    player.disconnect_at = time.monotonic() + self.disconnect_grace
                    if not self.connection_holds.start(player):
                        if self.travel and player.admission:
                            await self.travel.retire(player.admission)
                        self.enter_fence_counters["reject_hosted_invalid"] += 1
                        raise ValueError("无法建立托管断线保留")
                    self.connection_lost_hold_counters["hosted_arrived"] += 1
                self.enter_fence_counters["admit_new"] += 1
                world.add(player)
                self.locations[player_id] = world
                if not hosted:
                    self.sessions[key] = player
                self._announce(world, player)
                self._enter_aoi(world, player)
                if self.battle and not hosted:
                    self.battle.notify_reconnect(player)
                if self.inbox:
                    self.inbox.landed(player)
                if self.landing:
                    self.landing.wake(player)
                if self.team:
                    self.team.entered(player)
        return self.protocol.message("Empty")

    async def _load_player(self, request, gate_uuid, world, *, admission=None):
        if admission is None and self.travel:
            admission = await self.travel.admit(request)
        try:
            snapshot = await self.store.load_and_claim(
                request.player_id, self.info.node_uuid.encode(), request.owner_epoch,
            )
            if admission:
                await self.travel.confirm(admission)
            saved = self.rows.assemble(snapshot)
            original = saved.SerializeToString(deterministic=True)
            position = self._resolve_entry_position(world, xyz(saved.transform.location))
            if position != xyz(saved.transform.location):
                saved.transform.location.x, saved.transform.location.y, saved.transform.location.z = position
            transform = Transform(position, xyz(saved.transform.rotation))
            player = Player(
                self._actor_id(), request.player_id, request.session_id,
                gate_uuid, request.login_generation, request.owner_revision, snapshot, transform,
                home_zone_id=request.home_zone_id or self.info.zone_id, admission=admission,
                activity=FrameActivity(self.current_frame),
            )
            if self.initialize_player:
                self.initialize_player(saved)
            if original != saved.SerializeToString(deterministic=True):
                await self.commit_player_data(player, saved)
            if self.battle:
                await self.battle.restore(player, notify=False)
            proxy_dir = getattr(self, "craft_proxy_directory", None)
            if proxy_dir is not None:
                from .scene_skill_craft import drain_pending_proxy_reputation
                if await drain_pending_proxy_reputation(self, player):
                    saved = self.rows.assemble(player.snapshot)
                if saved.HasField("craft_component") and saved.craft_component.proxy_stall_open:
                    await proxy_dir.sync_player(request.player_id, saved)
            if admission:
                await self.travel.confirm(admission)
            return player
        except BaseException:
            if admission:
                try:
                    await self.travel.retire(admission)
                except Exception:
                    logger.exception("角色载入失败后撤销入场资格失败 player_id=%s", request.player_id)
            raise

    @contextmanager
    def _reserve_entry(self, world, key, player_id):
        bound = self.sessions.get(key)
        if key in self._pending_sessions or bound is not None and bound.player_id != player_id:
            self.enter_fence_counters["bind_rejected"] += 1
            raise ValueError("会话已绑定其他角色或正在入场")
        self._pending_sessions.add(key)
        task = asyncio.current_task()
        self._entry_tasks.add(task)
        self._entries_drained.clear()
        world.pending_entrants += 1
        try:
            yield
        finally:
            world.pending_entrants -= 1
            self._pending_sessions.remove(key)
            self._entry_tasks.discard(task)
            if not self._pending_sessions:
                self._entries_drained.set()

    def _announce(self, world, player) -> None:
        self.actor_sync.mark(player, "transform")
        self._push(player, "SceneSceneClientPlayerNotifyEnterScene",
                   self.protocol.message("EnterSceneS2C", scene_info=world.info))
        self._push(player, "SceneSceneClientPlayerNotifyActorCreate", self._actor(player))
        if self.skill_casts is not None:
            self.skill_casts.sync_self_state_after_entry(player)

    def _announce_neighbors(self, world, player) -> None:
        self._announce(world, player)
        if targets := world.aoi.visible_targets(player.entity):
            self._push_visibility(world, [player], targets, shown=True)

    def _relocate(self, previous, world, player) -> None:
        if self.team:
            self.team.moved_scene(player)
        self._visibility(previous, previous.aoi.leave(player.entity), departed=player.entity)
        previous.remove(player)
        player.transform.position = self._resolve_entry_position(
            world, player.transform.position, force_spawn=previous.info.scene_config_id != world.info.scene_config_id,
        )
        player.motion = Motion()
        self.actor_sync.mark(player, "velocity")
        world.add(player)
        self.locations[player.player_id] = world
        self._announce(world, player)
        self._enter_aoi(world, player)

    def _spawn_position(self, world) -> Position:
        row = self.tables["BaseScene"].get(world.info.scene_config_id) if self.tables else None
        configured = (row.spawn_x, row.spawn_y, row.spawn_z) if row is not None else ZERO
        spawn = configured if configured != ZERO and all(map(math.isfinite, configured)) else DEFAULT_SPAWN
        if world.nav:
            return world.nav.snap(spawn) or spawn
        return spawn

    def _missing_required_navigation(self, world: World) -> bool:
        if world.nav is not None or self.tables is None:
            return False
        row = self.tables["BaseScene"].get(world.info.scene_config_id)
        return bool(row and navigation_contract(row.nav_bin_file) != "legacy")

    def _resolve_entry_position(self, world, position, *, force_spawn=False) -> Position:
        if force_spawn or position == ZERO or not all(map(math.isfinite, position)):
            return self._spawn_position(world)
        if world.nav:
            return world.nav.snap(position) or self._spawn_position(world)
        return position

    def local_scene_entry(self, world, player, route, request):
        requested = request.scene_info
        target = self.worlds.get(requested.scene_id)
        if not requested.scene_id and requested.scene_config_id:
            target = next((candidate for candidate in self.worlds.values()
                           if candidate.info.scene_config_id == requested.scene_config_id
                           and not candidate.info.dungeon_config_id and not candidate.info.mirror_config_id), None)
        error = 0
        if self.battle and self.battle.frozen(player.player_id):
            error = 3023
        elif not requested.scene_id and not requested.scene_config_id:
            error = 3005
        elif target is None or target.draining:
            error = 3007
        elif target is world:
            error = 3008
        elif target.info.creators and not target.info.creators.get(player.player_id, False):
            error = 3022
        else:
            self._relocate(world, target, player)
        response = self.protocol.message("EnterSceneC2SResponse")
        if error:
            response.error_message.id = error
        return response

    def local_zone_travel(self, world, player, route, request):
        # Standalone mode has no zone coordinator, but still returns the
        # same validation errors as the clustered entry before declining it.
        if not request.target_zone_id or request.target_zone_id == self.info.zone_id:
            code = 3024
        elif request.scene_config_id and self.tables and not any(
            row.scene_id == request.scene_config_id for row in self.tables["World"].rows()
        ):
            code = 3007
        elif self.battle and self.battle.frozen(player.player_id):
            code = 3025
        else:
            code = 3027
        return self.protocol.message("TravelToZoneResponse", error_message={"id": code})

    async def process(self, request, gate_uuid: str):
        player = self.sessions.get((gate_uuid, request.session_id))
        content = request.message_content
        route = self.protocol.routes.get(content.message_id)
        reply = self.protocol.message("ProcessClientPlayerMessageResponse", session_id=request.session_id)
        leaving = content.message_id == self.protocol.message_ids["ScenePlayerExitGame"]
        if player is None:
            if leaving:
                reply.message_content.CopyFrom(self.protocol.content(content.message_id, content.id))
                return reply
            if _may_retry_handoff(route):
                reply.dispatch_verdict = 1
                return reply
            raise ValueError("场景会话身份不匹配")
        # 老版 Gate 未带 player_id；带值时必须与已认证会话绑定的玩家一致。
        if request.player_id and player.player_id != request.player_id:
            raise ValueError("场景会话身份不匹配")
        async with self._lock(player.player_id):
            if self.sessions.get((gate_uuid, request.session_id)) is not player:
                if leaving:
                    reply.message_content.CopyFrom(self.protocol.content(content.message_id, content.id))
                    return reply
                raise ValueError("场景会话已被替换")
            world = self.locations[player.player_id]
            handler = None
            if route and route.node_type == RouteNode.SCENE and route.client_protocol:
                handler = self._handlers.get(route.service, {}).get(route.method)
            if (leaving or handler is not None) and not self._releasing(player) and player.disconnect_at is None:
                # 原版在解析和业务校验前记录活动；未知 RPC 与旧会话不会到这里。
                player.activity.record(self.current_frame)
            if leaving:
                reason = self.protocol.parse("GameNodeExitGameRequest", content.serialized_message).reason
                # 在旧场景路由下收到的退出不能作用于新场景。
                if request.received_on_previous_route:
                    return reply
                if reason == 2:
                    await self._connection_lost(world, player)
                else:
                    await self._remove(world, player)
                reply.message_content.CopyFrom(self.protocol.content(content.message_id, content.id))
                return reply
            error, body = 0, b""
            if handler is None or player.disconnect_at is not None or self._closing:
                error = 1003
            elif route.kind not in (RpcKind.ONE_WAY, RpcKind.REQUEST):
                error = 1005
            else:
                try:
                    message = self.protocol.parse(route.request_type, content.serialized_message)
                    # 退出优先；只有明确仍在跨节点交接的玩家才允许重投。
                    if self._releasing(player):
                        self.exit_persist_counters["exit_client_msg_rejected"] += 1
                        if route.kind == RpcKind.REQUEST:
                            response = self.protocol.message(route.response_type)
                            tip = response.DESCRIPTOR.fields_by_name.get("error_message")
                            if tip and tip.message_type and tip.message_type.full_name == "TipInfoMessage":
                                response.error_message.id = 1006
                            reply.message_content.CopyFrom(self.protocol.content(
                                content.message_id, content.id, body=response.SerializeToString()))
                        return reply
                    handoff_frozen = getattr(self.travel, "handoff_frozen", None)
                    if handoff_frozen is not None and handoff_frozen(player.player_id) and _may_retry_handoff(route):
                        reply.dispatch_verdict = 1
                        return reply
                    if request.received_on_previous_route and not _replayable_on_another_scene(route):
                        return reply
                    switch = getattr(self, "kill_switch", None)
                    if switch is not None and switch.check_id(content.message_id):
                        reply.message_content.CopyFrom(self.protocol.content(content.message_id, content.id, error=1003))
                        return reply
                    if (route.service, route.method) in self._mutating:
                        await self.flush_player_rows(player)
                    response = handler(world, player, route, message)
                    if inspect.isawaitable(response):
                        response = await response
                    if response is not None:
                        body = response.SerializeToString()
                except (DecodeError, ValueError):
                    error = 1005
                except (StoreRejected, StoreUnavailable):
                    error = 1003
            reply.message_content.CopyFrom(
                self.protocol.content(content.message_id, content.id, body=body, error=error),
            )
        return reply

    def _hosting_started(self, player: Player) -> None:
        if self.team is not None:
            self.team.hosting(player)

    def _stop_motion(self, world: World, player: Player) -> None:
        if player.motion.velocity != ZERO:
            self.set_velocity(player, ZERO)
            self._broadcast_move(world, player)

    async def _connection_lost(self, world: World, player: Player) -> None:
        held = self.connection_holds.held(player)
        decision = decide_hold(
            exiting=self._releasing(player) or player.disconnect_at is not None and not held,
            handoff_in_flight=bool(self.travel and self.travel.frozen(player.player_id)),
            evacuating=self._closing or world.draining,
            bound=bool(player.player_id and session_bound(player.session_id)),
            already_held=held,
        )
        if decision is HoldDecision.EXIT_NOW:
            self.connection_lost_hold_counters["exit_now"] += 1
            await self._remove(world, player)
        elif decision is HoldDecision.HOLD and self.connection_holds.start(player):
            self.connection_lost_hold_counters["held"] += 1
            self._stop_motion(world, player)
            key = player.gate_uuid, player.session_id
            if self.sessions.get(key) is player:
                del self.sessions[key]
            now = time.monotonic()
            player.disconnect_at = now + self.disconnect_grace
            player.disconnect_save_due = True
            player.save_at = now
            if self.travel:
                self.travel.superseded(player)

    async def release(self, request):
        player_id = request.player_id
        if request.session_ended:
            self.release_fence_counters["session_ended"] += 1
        async with self._lock(player_id):
            world = self.locations.get(player_id)
            if world is not None:
                player = world.players[player_id]
                covers_epoch = request.expected_owner_epoch in (0, player.snapshot.owner_epoch)
                generation = request.covered_login_generation
                covers_session = not request.session_ended or not generation or (
                    generation > player.generation or generation == player.generation
                    and request.covered_session_id in (0, player.session_id)
                )
                if not covers_epoch:
                    self.release_fence_counters["rejected_stale_epoch"] += 1
                elif not covers_session:
                    self.release_fence_counters["rejected_stale_session"] += 1
                elif (
                    request.session_ended
                    and bool(getattr(request, "keep_held", False))
                    and self.connection_holds.held(player)
                ):
                    self.release_fence_counters["session_ended_kept_held"] += 1
                else:
                    self.release_fence_counters["accepted"] += 1
                    if self.connection_holds.held(player):
                        self.connection_lost_hold_counters["superseded_by_exit"] += 1
                    await self._remove(world, player, release_credential=request.session_ended)
            else:
                self.release_fence_counters["not_found"] += 1
                if self.travel and request.session_ended:
                    await self.travel.release_credential(player_id, request.expected_owner_epoch)
        return self.protocol.message("Empty")

    async def lease_expired(self, player_id: int, session_id: int, gate_uuid: str, scene_id: int):
        async with self._lock(player_id):
            world = self.locations.get(player_id)
            if world is None or world.info.scene_id != scene_id:
                return
            player = world.players[player_id]
            if player.session_id == session_id and player.gate_uuid == gate_uuid:
                await self._remove(world, player)

    def _lock(self, player_id: int) -> asyncio.Lock:
        lock = self._locks.get(player_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[player_id] = lock
        return lock

    def player_lock(self, player_id: int) -> asyncio.Lock:
        """返回指定玩家的串行执行锁（公开边界，替代跨模块直接访问 _lock）。"""
        return self._lock(int(player_id))

    def schedule_player_task(self, player_id: int, coro_factory) -> asyncio.Task | None:
        """在目标玩家独立的串行执行锁内异步投递任务，避免跨玩家嵌套持锁死锁。"""
        if self._closing or self._closed:
            return None
        pid = int(player_id)

        async def _runner() -> None:
            try:
                async with self._lock(pid):
                    await coro_factory()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("玩家异步 Actor 任务执行失败 player_id=%s", pid, exc_info=True)

        task = asyncio.create_task(_runner(), name=f"scene-player-actor-{pid}")
        self._actor_tasks.add(task)
        task.add_done_callback(self._actor_tasks.discard)
        return task

    def snapshot_player_brief(self, player_id: int) -> dict[str, object] | None:
        """从不可变 PlayerSnapshot 提取在线玩家只读展示投影，避免外部模块直接穿透 locations/rows。"""
        pid = int(player_id)
        world = self.locations.get(pid)
        if world is None:
            return None
        player = world.players.get(pid)
        if player is None:
            return None
        data = self.rows.assemble(player.snapshot)
        return {
            "name": str(data.profile_component.name or ""),
            "level": int(data.level_component.level or 0),
            "class_id": int(getattr(data.uint32_pb_component, "class", 0) or 0),
            "gender": int(data.profile_component.gender or 0),
            "appearance_id": str(data.profile_component.appearance_id or ""),
        }

    def hint_team_refresh(self, player_id: int) -> None:
        """通知组队同步子系统刷新指定玩家队伍视图（公开边界）。"""
        if self.team is not None:
            self.team.hint(int(player_id))

    def mutable(self, player: Player) -> bool:
        # 断线会话已解绑，保留中的写权仍可处理结算和资产事务。
        return (
            not self._closing and (player.disconnect_at is None or self.connection_holds.held(player))
            and not self._releasing(player)
            and player.snapshot.owner_epoch > 0 and not player.snapshot.released
            and not (self.travel and self.travel.frozen(player.player_id))
        )

    @staticmethod
    def _releasing(player: Player) -> bool:
        return player.snapshot.released or bool(player.pending_save and player.pending_save.release)

    async def flush_player_rows(self, player: Player) -> None:
        pending = player.pending_save
        if pending is None:
            return
        try:
            player.snapshot = await self.store.commit(
                player.snapshot, pending.rows, force=pending.force,
                periodic=pending.periodic, release=pending.release,
            )
        except StoreRejected:
            player.pending_save = None
            raise
        else:
            player.pending_save = None
            if pending.on_committed is not None:
                try:
                    result = pending.on_committed(player.snapshot)
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    logger.exception("执行存档后的通知失败 player_id=%s", player.player_id)

    async def commit_player_rows(
        self, player: Player, rows, *, force: bool = True, on_committed=None,
    ) -> PlayerSnapshot:
        if player.pending_save is not None:
            raise StoreUnavailable("玩家有待确认存档，请先确认原件")
        if player.snapshot.released:
            raise StoreRejected("SAVE_PLAYER_NOT_HOLDER")
        observed_seq = int(getattr(player.snapshot, "save_seq", 0))
        if observed_seq >= _SAVE_SEQ_MAX:
            self.save_seq_counters["exhausted"] += 1
            raise StoreRejected("SAVE_SEQ_EXHAUSTED")
        floor = self._save_seq_floor.get(player.player_id, 0)
        if observed_seq > 0 and floor > 0 and observed_seq < floor:
            self.save_seq_counters["personal_floor_issued"] += 1
        elif observed_seq > floor:
            self._save_seq_floor[player.player_id] = observed_seq
        player.pending_save = PendingSave(dict(rows), force, False, False, on_committed)
        await self.flush_player_rows(player)
        return player.snapshot

    async def commit_player_data(self, player: Player, data, *, on_committed=None) -> PlayerSnapshot:
        before = self.rows.project(self.rows.assemble(player.snapshot))
        after = self.rows.project(data)
        rows = dict(player.snapshot.rows)
        rows.update({kind: value for kind, value in after.items() if value != before[kind]})
        self._merge_seen(player, rows)
        return await self.commit_player_rows(player, rows, on_committed=on_committed)

    def _merge_seen(self, player: Player, rows: dict[int, bytes]) -> None:
        if not player.seen_battles:
            return
        profile = self.protocol.parse("player_database", rows.get(PlayerRowKind.PROFILE, b""))
        changed = False
        for entry in profile.battle_results.entries:
            if not entry.seen and entry.battle_id in player.seen_battles:
                entry.seen = changed = True
        if changed:
            rows[PlayerRowKind.PROFILE] = profile.SerializeToString(deterministic=True)

    async def _remove(self, world: World, player: Player, *, release_credential: bool = True) -> None:
        self.connection_holds.forget(player)
        player.disconnect_at = float("inf")
        self._stop_motion(world, player)
        if self.travel and (self.travel.published(player.player_id) or self.travel.sealed(player.player_id)):
            return
        try:
            await self.flush_player_rows(player)
            if not player.snapshot.released:
                player.pending_save = PendingSave(self._position_rows(player), False, False, True)
                await self.flush_player_rows(player)
            if self.travel and player.admission:
                await self.travel.retire(player.admission, player.snapshot, release=release_credential)
        except StoreRejected as error:
            if error.result not in STALE_SAVE_RESULTS:
                player.disconnect_at = time.monotonic() + 5
                raise
            logger.warning("角色写权已过期，清理本地实体 player_id=%s", player.player_id)
            if self.travel and player.admission:
                try:
                    await self.travel.retire(player.admission, transferred=True)
                except Exception:
                    logger.exception("关闭失去写权的角色入场资格失败 player_id=%s", player.player_id)
        except BaseException:
            player.disconnect_at = time.monotonic() + 5
            raise
        self._evict(world, player)

    def _position_rows(self, player: Player) -> dict[int, bytes]:
        saved = self.rows.assemble(player.snapshot)
        for target, position in (
            (saved.transform.location, player.transform.position),
            (saved.transform.rotation, player.transform.rotation),
        ):
            if xyz(target) != position:
                target.x, target.y, target.z = position
        rows = dict(player.snapshot.rows)
        rows[PlayerRowKind.LOCATION] = self.rows.project(saved)[PlayerRowKind.LOCATION]
        self._merge_seen(player, rows)
        return rows

    async def _periodic_save(self, player: Player) -> None:
        if player.pending_save is None:
            player.pending_save = PendingSave(self._position_rows(player), False, True, False)
        await self.flush_player_rows(player)

    def _evict(self, world: World, player: Player) -> None:
        self.connection_holds.forget(player)
        if self.travel and hasattr(self.travel, "forget_hosted_presence"):
            self.travel.forget_hosted_presence(player.player_id)
        if self.team:
            self.team.departed(player)
        self.actor_sync.discard(player)
        self.combat_engagement.on_leave(player)
        self._visibility(world, world.aoi.leave(player.entity), departed=player.entity)
        world.remove(player)
        self.locations.pop(player.player_id)
        key = player.gate_uuid, player.session_id
        if self.sessions.get(key) is player:
            del self.sessions[key]
        if self.battle:
            self.battle.departed(player)
        if self.inbox:
            self.inbox.departed(player)
        if self.mail_outbox:
            self.mail_outbox.departed(player)
        if self.buffs is not None:
            self.buffs.discard(player)

    async def tick(self, delta: float) -> None:
        if self._closing:
            return
        step_started = time.monotonic()
        for world in self.worlds.values():
            for player in world.players.values():
                was_afk = player.activity.afk
                player.activity.update(self.current_frame, frozen=bool(self.travel and self.travel.frozen(player.player_id)))
                if player.activity.afk and not was_afk and not (self.team and self.team.driven(player.player_id)):
                    self._stop_motion(world, player)
                if player.activity.afk != was_afk:
                    logger.info("Scene player activity changed player_id=%d afk=%s frame=%d",
                                player.player_id, player.activity.afk, self.current_frame)
        now = time.monotonic()
        self.connection_holds.sweep(now)
        if self.battle:
            self.battle.reap()
        if self.inbox:
            self.inbox.tick()
        if self.mail_outbox:
            self.mail_outbox.tick()
        if self.skill_casts is not None:
            await self.skill_casts.tick(delta)
        self.combat_engagement.update(delta)
        for world in tuple(self.worlds.values()):
            if world.draining and self.travel:
                self.travel.drain(world)
            for player in tuple(world.players.values()):
                if world.players.get(player.player_id) is not player:
                    continue
                held = self.connection_holds.held(player)
                if held and self.travel and hasattr(self.travel, "refresh_hosted_presence"):
                    self.travel.refresh_hosted_presence(player)
                if held:
                    evacuating_hosted = bool(world.draining and self.travel
                                             and self.connection_holds.placement_eligible(player))
                    due = ((world.draining and not evacuating_hosted)
                           or self.connection_holds.expired(player, now) or now >= player.save_at)
                else:
                    due = now >= (player.disconnect_at if player.disconnect_at is not None else player.save_at)
                can_save = (self.mutable(player) if player.disconnect_at is None else not (
                    self.travel and (self.travel.published(player.player_id) or self.travel.sealed(player.player_id))
                ))
                if can_save and due and len(self._maintenance) < 16 and player.player_id not in self._maintenance:
                    self._maintenance[player.player_id] = asyncio.create_task(self._maintain(world, player))
                if player.disconnect_at is not None:
                    continue
                if (player.motion.velocity != ZERO and not player.activity.afk and self.mutable(player)
                        and not (self.battle and self.battle.frozen(player.player_id))
                        and not (self.team and self.team.driven(player.player_id))):
                    position = tuple(a + b * delta for a, b in zip(
                        player.transform.position, player.motion.velocity,
                    ))
                    if world.nav:
                        walkable, clamped = world.nav.validate_move(player.transform.position, position)
                        position = clamped or player.transform.position
                        if not walkable:
                            self.set_velocity(player, ZERO)
                    elif self._missing_required_navigation(world):
                        position = player.transform.position
                        self.set_velocity(player, ZERO)
                    self._move_position(world, player, position)
                    if player.motion.velocity == ZERO:
                        self._broadcast_move(world, player)
        if self.team:
            self.team.tick(delta)
        for world in self.worlds.values():
            for entity in sorted(world.aoi_reentry):
                self._visibility(world, world.aoi.move(entity, world.actors[entity].transform.position))
            world.aoi_reentry.clear()
        self.actor_sync.tick()
        self.current_frame = (self.current_frame + 1) & FRAME_MASK
        if self.aoi_load.observe(time.monotonic() - step_started, self.tick_interval):
            level = self.aoi_load.level
            if level != self._aoi_pressure_level:
                previous = self._aoi_pressure_level
                pressure = self.aoi_load.pressure_factor
                trimmed = 0
                for world in tuple(self.worlds.values()):
                    changes, count = world.aoi.apply_pressure(pressure)
                    self._visibility(world, changes)
                    trimmed += count
                self._aoi_pressure_level = level
                self.aoi_trimmed_entries_total += trimmed
                logger.info(
                    "Scene AOI pressure level changed: %d -> %d of %d, utilization=%.3f, trimmed_entries=%d",
                    previous, level, PRESSURE_LEVELS, self.aoi_load.smoothed_utilization, trimmed,
                )

    async def _maintain(self, world, player) -> None:
        try:
            async with self._lock(player.player_id):
                if (world.players.get(player.player_id) is not player
                        or self.travel and (self.travel.published(player.player_id) or self.travel.sealed(player.player_id))):
                    return
                now = time.monotonic()
                held = self.connection_holds.held(player)
                evacuating_hosted = bool(held and world.draining and self.travel
                                         and self.connection_holds.placement_eligible(player))
                if held and ((world.draining and not evacuating_hosted) or self.connection_holds.expired(player, now)):
                    if self.connection_holds.expired(player, now):
                        self.connection_lost_hold_counters["expired"] += 1
                    await self._remove(world, player)
                elif player.disconnect_at is not None and not held:
                    if now >= player.disconnect_at:
                        await self._remove(world, player)
                elif now >= player.save_at and self.mutable(player):
                    player.save_at = now + SAVE_INTERVAL
                    try:
                        if held and player.disconnect_save_due:
                            await self.flush_player_rows(player)
                            await self.commit_player_rows(player, self._position_rows(player), force=False)
                            player.disconnect_save_due = False
                        else:
                            await self._periodic_save(player)
                    except StoreUnavailable:
                        if held:
                            player.save_at = now + SWEEP_INTERVAL
                        raise
                    except StoreRejected as error:
                        if error.result not in STALE_SAVE_RESULTS:
                            raise
                        self._evict(world, player)
        except Exception:
            logger.exception("角色存盘失败，保留存盘上下文等待重试")
        finally:
            self._maintenance.pop(player.player_id, None)

    def begin_planned_drain(self) -> None:
        """Stop admitting new work while existing worlds and handoffs continue running."""
        if self._planned_drain_pacer is None:
            self._planned_drain_pacer = RelocationPacer.from_env()
        self._admission_closed = True

    def evacuate_remaining(self) -> None:
        """Fall back to the normal, fenced SceneManager handoff for each world."""
        for world in tuple(self.worlds.values()):
            world.draining = True
            if self.travel:
                self.travel.drain(world)

    async def _shutdown_player(self, world: World, player: Player) -> None:
        while world.players.get(player.player_id) is player:
            try:
                async with self._lock(player.player_id):
                    if world.players.get(player.player_id) is not player:
                        return
                    # Keep hosted/session state intact while a fenced handoff
                    # or trade seal is still resolving.
                    if not (self.travel and (self.travel.frozen(player.player_id)
                                              or self.travel.published(player.player_id))):
                        await self._remove(world, player)
            except Exception:
                logger.warning("停机角色存盘失败，将重试 player_id=%s", player.player_id, exc_info=True)
            if world.players.get(player.player_id) is player:
                await asyncio.sleep(SHUTDOWN_SAVE_RETRY)

    async def close(self) -> None:
        if self._closed:
            return
        self._closing = True
        deadline = asyncio.get_running_loop().time() + SHUTDOWN_SAVE_BUDGET
        saves = {}
        try:
            async with asyncio.timeout_at(deadline):
                if self._actor_tasks:
                    pending_actor = tuple(self._actor_tasks)
                    await asyncio.gather(*pending_actor, return_exceptions=True)
                if self.skill_casts is not None:
                    await self.skill_casts.close()
                await self.channels.close()
                if self.inbox:
                    await self.inbox.close()
                if self.mail_outbox:
                    await self.mail_outbox.close()
                if self.battle:
                    await self.battle.close()
                while True:
                    for world in tuple(self.worlds.values()):
                        for player in tuple(world.players.values()):
                            key = id(player)
                            if key not in saves or saves[key].done():
                                saves[key] = asyncio.create_task(self._shutdown_player(world, player))
                    if (self._entries_drained.is_set() and not self.locations
                            and all(task.done() for task in saves.values())
                            and not self._maintenance):
                        break
                    await asyncio.sleep(.01)
        except TimeoutError:
            logger.error("Scene 停机任务超过总预算")
        finally:
            for world in tuple(self.worlds.values()):
                for player in tuple(world.players.values()):
                    logger.error("停机角色尚未释放 player_id=%s", player.player_id)
            pending = tuple(self._entry_tasks) + tuple(saves.values())
            for task in pending:
                if not task.done():
                    task.cancel()
            try:
                async with asyncio.timeout(0.1):
                    await asyncio.gather(*pending, return_exceptions=True)
            except TimeoutError:
                logger.error("停机等待入场任务取消超时")
            if self.travel:
                try:
                    async with asyncio.timeout(0.1):
                        await self.travel.stop()
                except TimeoutError:
                    logger.error("停机等待交接任务取消超时")
            try:
                async with asyncio.timeout(0.1):
                    await self.connection_holds.close()
            except TimeoutError:
                logger.error("停机等待断线保留清理超时")
            self._closed = True

    def _scene_info(self, world, player, route, request):
        self.channels.query(world, player, request)

    def _get_spectate_chat_setting(self, _world, player, _route, _request):
        """589 GetSpectateChatSetting：读取玩家个人「局外人观战交流」偏好及解析后的生效值。"""
        profile_row = self.protocol.parse("player_database", player.snapshot.rows.get(PlayerRowKind.PROFILE, b""))
        pref = int(profile_row.profile_component.spectate_chat_pref)
        return self.protocol.message(
            "GetSpectateChatSettingResponse",
            preference=pref,
            effective_allow=(pref != _SPECTATE_CHAT_PREF_FORBID),
        )

    async def _set_spectate_chat_setting(self, _world, player, _route, request):
        """590 SetSpectateChatSetting：校验并持久化玩家个人「局外人观战交流」偏好（仅控交流，不禁止观战）。"""
        resp = self.protocol.message("SetSpectateChatSettingResponse")
        pref = int(request.preference)
        if pref not in (_SPECTATE_CHAT_PREF_ALLOW, _SPECTATE_CHAT_PREF_FORBID):
            resp.error_message.id = _INVALID_PARAMETER
            return resp
        if not self.mutable(player):
            resp.error_message.id = _FEATURE_UNAVAILABLE
            return resp
        saved = self.rows.assemble(player.snapshot)
        if int(saved.profile_component.spectate_chat_pref) != pref:
            saved.profile_component.spectate_chat_pref = pref
            await self.commit_player_data(player, saved)
        resp.preference = pref
        resp.effective_allow = (pref != _SPECTATE_CHAT_PREF_FORBID)
        return resp

    def _teleport_request(self, _world, _player, _route, _request):
        # 与原版一致，缺少传送规则时明确返回业务不可用。
        return self.protocol.message("TeleportRequestC2SResponse", error_message={"id": 1006})

    def _movement(self, world, player, route, request) -> None:
        if self.team and self.team.driven(player.player_id):
            return
        motion = player.motion
        if not self.mutable(player) or self.battle and self.battle.frozen(player.player_id):
            if route.method == "MoveStop":
                self._stop_motion(world, player)
            return
        seq = request.input_seq
        rotation = xyz(request.rotation)
        velocity = xyz(request.velocity) if route.method != "MoveStop" else ZERO
        field = {"MoveStart": "start_location", "MoveStop": "end_location", "MoveSync": "location"}[route.method]
        reported = xyz(getattr(request, field))
        if not all(map(math.isfinite, rotation + velocity + reported)):
            raise ValueError("移动参数无效")
        if motion.teleport_target is not None:
            if time.monotonic() < motion.teleport_until and math.dist(reported[:2], motion.teleport_target[:2]) > TELEPORT_SETTLE_RADIUS:
                return
            motion.teleport_target = None
        speed = math.hypot(*velocity)
        if speed > MOVE_SPEED:
            velocity = tuple(v * (MOVE_SPEED / speed) for v in velocity)
        position = reported
        if world.nav:
            _, position = world.nav.validate_move(player.transform.position, reported)
            if position is None:
                guided = world.nav.snap(reported) if world.nav.mask is None else None
                position = guided or self._spawn_position(world)
        missing_navigation = self._missing_required_navigation(world)
        if missing_navigation:
            position, velocity = player.transform.position, ZERO
        previous_velocity = ZERO if missing_navigation else motion.velocity
        player.transform.rotation = rotation
        self.set_velocity(player, velocity)
        self._move_position(world, player, position)
        self.actor_sync.mark(player, "transform", "velocity")
        if math.dist(position[:2], reported[:2]) > MOVE_CORRECTION_EPSILON:
            self._push(player, "SceneMovementClientPlayerNotifyMoveAck", self.protocol.message(
                "MoveAckS2C", input_seq=seq, server_location=vector(position),
                server_velocity=vector(previous_velocity),
                server_time_ms=time.time_ns() // 1_000_000,
            ))
        self._broadcast_move(world, player)

    def _move_position(self, world, player, position, *, teleport=False) -> None:
        if position == player.transform.position and not teleport:
            return
        player.transform.position = position
        if teleport:
            self._visibility(world, world.aoi.reset(player.entity, position))
            world.aoi_reentry.add(player.entity)
        elif player.entity not in world.aoi_reentry:
            self._visibility(world, world.aoi.move(player.entity, position))
        self.actor_sync.mark(player, "transform")

    def set_velocity(self, player, velocity) -> None:
        if player.motion.velocity != velocity:
            player.motion.velocity = velocity
            self.actor_sync.mark(player, "velocity")

    def _actor_move(self, player, server_time_ms):
        velocity = player.motion.velocity
        return self.protocol.message(
            "ActorMoveS2C", entity=player.entity, transform=player.transform.wire(),
            velocity=vector(velocity), server_time_ms=server_time_ms, move_state=int(velocity != ZERO),
        )

    def _broadcast_move(self, world, player, *, include_self=False) -> None:
        message = self._actor_move(player, time.time_ns() // 1_000_000)
        if include_self and player.disconnect_at is None:
            self._push(player, "SceneMovementClientPlayerNotifyActorMove", message)
        self._push_many(self.watchers(world, player), "SceneMovementClientPlayerNotifyActorMove", message)

    def teleport(self, world, player, position, rotation, *, reason) -> bool:
        if self._missing_required_navigation(world):
            return False
        if world.nav is not None and world.nav.mask is not None:
            destination = world.nav.snap(position)
            if destination is None or not world.nav.find_path(player.transform.position, destination):
                return False
            position = destination
        player.transform.rotation = rotation
        self.set_velocity(player, ZERO)
        self.actor_sync.mark(player, "transform", "velocity")
        player.motion.teleport_target = position
        player.motion.teleport_until = time.monotonic() + TELEPORT_SETTLE_SECONDS
        self._move_position(world, player, position, teleport=True)
        if player.disconnect_at is None:
            self._push(player, "SceneMovementClientPlayerNotifyTeleport", self.protocol.message(
                "TeleportS2C", entity=player.entity, transform=player.transform.wire(), reason=reason,
            ))
        self._broadcast_move(world, player)
        return True

    def _actor(self, player: Player | Npc):
        if isinstance(player, Npc):
            return self.protocol.message(
                "ActorCreateS2C", entity=player.entity, actor_type=2,
                transform={"location": vector(player.transform.position)},
            )
        return self.protocol.message(
            "ActorCreateS2C", entity=player.entity, transform=player.transform.wire(),
            actor_type=1, guid=player.player_id, **actor_profile_fields(self, player),
        )

    def _enter_aoi(self, world, actor) -> None:
        team_id = self.team.team_id(actor.player_id) if self.team and isinstance(actor, Player) else 0
        self._visibility(world, world.aoi.enter(
            actor.entity, actor.transform.position, team_id=team_id, stealthed=self._stealthed(actor),
        ))

    def _visibility(self, world, changes, *, departed=0) -> None:
        hidden, shown = defaultdict(list), defaultdict(list)
        departed_watchers = []
        for change in changes:
            watcher = world.actors[change.watcher]
            if not isinstance(watcher, Player) or watcher.disconnect_at is not None:
                continue
            if change.shown:
                shown[change.watcher].append(change.target)
            elif change.target == departed:
                departed_watchers.append(watcher)
            else:
                hidden[change.watcher].append(change.target)
        self._push_visibility_groups(world, hidden, shown=False)
        if departed_watchers:
            self._push_many(departed_watchers, "SceneSceneClientPlayerNotifyActorDestroy",
                            self.protocol.message("ActorDestroyS2C", entity=departed))
        self._push_visibility_groups(world, shown, shown=True)

    def _push_visibility_groups(self, world, by_watcher, *, shown) -> None:
        groups = defaultdict(list)
        for watcher, targets in by_watcher.items():
            groups[tuple(targets)].append(world.actors[watcher])
        for targets, watchers in groups.items():
            self._push_visibility(world, watchers, targets, shown=shown)

    def _push_visibility(self, world, watchers, targets, *, shown) -> None:
        if shown:
            message = self.protocol.message("ActorListCreateS2C", actor_list=[self._actor(world.actors[n]) for n in targets])
            self._push_many(watchers, "SceneSceneClientPlayerNotifyActorListCreate", message)
            advancing = [actor for target in targets if isinstance((actor := world.actors[target]), Player)
                         and actor.motion.velocity != ZERO
                         and ((self.team and self.team.driven(actor.player_id)) or
                              (not actor.activity.afk and not (self.travel and self.travel.frozen(actor.player_id))
                               and not (self.battle and self.battle.frozen(actor.player_id))))]
            if advancing:
                now = time.time_ns() // 1_000_000
                moves = [self._actor_move(actor, now) for actor in advancing]
                if len(moves) == 1:
                    self._push_many(watchers, "SceneMovementClientPlayerNotifyActorMove", moves[0])
                else:
                    self._push_many(watchers, "SceneMovementClientPlayerNotifyActorMoveList",
                                    self.protocol.message("ActorMoveListS2C", moves=moves))
        else:
            message = self.protocol.message("ActorListDestroyS2C", entity=targets)
            self._push_many(watchers, "SceneSceneClientPlayerNotifyActorListDestroy", message)

    def _stealthed(self, actor: Player | Npc) -> bool:
        return self.buffs is not None and self.buffs.stealthed(actor)

    def watchers(self, world: World, actor: Player | Npc):
        for entity in world.aoi.visible_watchers(actor.entity):
            other = world.actors[entity]
            if isinstance(other, Player) and other.disconnect_at is None:
                yield other

    def refresh_actor_visibility(self, actor: Player | Npc) -> None:
        world = actor.world if isinstance(actor, Npc) else self.locations.get(actor.player_id)
        if world is None or world.actors.get(actor.entity) is not actor:
            return
        self._visibility(world, world.aoi.set_stealth(actor.entity, self._stealthed(actor)))

    def _push_many(self, players, name, message) -> None:
        players = tuple(players)
        if not players:
            return
        content = self.protocol.content(self.protocol.message_ids[name], body=message.SerializeToString())
        if self.emit_many is not None and len(players) > 1:
            self.emit_many(players, content)
        else:
            for player in players:
                if player.disconnect_at is None:
                    self.emit(player, content)

    def _push(self, player, name, message) -> None:
        if player.disconnect_at is None:
            self.emit(player, self.protocol.content(
                self.protocol.message_ids[name], body=message.SerializeToString(),
            ))
