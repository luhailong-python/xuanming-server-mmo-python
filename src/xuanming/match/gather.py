"""回合制战斗 Gather 开局管线与按池撮合器（Matchmaker）。

完整对齐 Go 侧 gather 补偿矩阵：
1. 逐参与者 SceneNodeGrpc.PrepareBattle 冻结并收快照（记录真实备战端点）；
2. 校验配表指纹一致性；
3. BattleNode.QueryBattleCreation 预检（确认 supported_outcome_protocol_version == 1 且未拒收新房）；
4. 持久化确定性 CreateBattleRequest（不合法原件拒绝落库、绝不发 Create）；
5. 调用 BattleNode.CreateBattle：
   - 收到 battle_not_allocatable 或业务拒绝 -> 短时冷却节点 + DestroyBattle + CancelBattlePrepare 回滚 + 无辜者回队；
   - 发房前失败 -> CancelBattlePrepare 回滚 + abort_formation_pre_persist；
   - 落库后 Create 结果未知（传输/超时）-> 标记 UNKNOWN，绝不盲目解冻或释票。
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from typing import Protocol as TypingProtocol

import grpc
from google.protobuf.message import Message

from ..grpc_runtime import GrpcClient
from ..protocol import Protocol
from .config import MatchConfig
from .errors import (
    BattleNotAllocatableError,
    ERR_UNAVAILABLE,
    PrepareRejectedError,
    StoreUnavailableError,
    UnterminableOriginalError,
)
from .model import (
    BATTLE_NOT_ALLOCATABLE_MESSAGE,
    BATTLE_NODE_TYPE,
    MAX_TEAM_SIZE,
    MODE_1V1,
    MODE_5V5,
    MODE_PVE_SOLO,
    MODE_PVE_TEAM,
    MODE_PVP_TEAM,
    MODE_SEASON_TOURNAMENT,
    BattleNodeEntry,
    PlayerPresence,
    preferred_region,
    record_replay_for,
    spectate_visibility_for,
)
from .store import MatchStore

logger = logging.getLogger(__name__)

PREPARE_BATTLE_PATH = "/scene_node.SceneNodeGrpc/PrepareBattle"
CANCEL_BATTLE_PREPARE_PATH = "/scene_node.SceneNodeGrpc/CancelBattlePrepare"
QUERY_BATTLE_CREATION_PATH = "/BattleNode/QueryBattleCreation"
CREATE_BATTLE_PATH = "/BattleNode/CreateBattle"
DESTROY_BATTLE_PATH = "/BattleNode/DestroyBattle"


class EndpointCaller(TypingProtocol):
    async def call(self, endpoint: str, path: str, data: bytes, *, timeout: float = 5.0) -> bytes: ...
    async def close(self) -> None: ...


class GrpcEndpointPool:
    """按端点缓存 GrpcClient 连接，退出时统一关闭。"""

    def __init__(self, *, default_timeout: float = 5.0) -> None:
        self._timeout = default_timeout
        self._clients: dict[str, GrpcClient] = {}

    async def call(self, endpoint: str, path: str, data: bytes, *, timeout: float | None = None) -> bytes:
        client = self._clients.get(endpoint)
        if client is None:
            client = self._clients[endpoint] = GrpcClient(endpoint, timeout=self._timeout, enable_retries=False)
        return await client.call(path, data, timeout=self._timeout if timeout is None else timeout)

    async def close(self) -> None:
        clients, self._clients = tuple(self._clients.values()), {}
        await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)


class BattleNodeResolver(TypingProtocol):
    async def list_nodes(self, *, release_track: str | None = None) -> list[BattleNodeEntry]: ...
    def mark_not_allocatable(self, node_uuid: str, *, cooldown_sec: float = 5.0) -> None: ...


class StaticBattleNodeResolver:
    """内存/静态 Battle 节点发现器，带 battle_not_allocatable 短时熔断冷却。"""

    def __init__(self, nodes: list[BattleNodeEntry] | None = None) -> None:
        self._nodes: list[BattleNodeEntry] = list(nodes or [])
        self._cooldown_until: dict[str, float] = {}

    def set_nodes(self, nodes: list[BattleNodeEntry]) -> None:
        self._nodes = list(nodes)

    def mark_not_allocatable(self, node_uuid: str, *, cooldown_sec: float = 5.0) -> None:
        if node_uuid:
            self._cooldown_until[node_uuid] = time.monotonic() + cooldown_sec

    async def list_nodes(self, *, release_track: str | None = None) -> list[BattleNodeEntry]:
        now = time.monotonic()
        out: list[BattleNodeEntry] = []
        for n in self._nodes:
            if not n.admitting or not n.grpc_endpoint or not n.node_uuid:
                continue
            if self._cooldown_until.get(n.node_uuid, 0.0) > now:
                continue
            track = n.release_track or "stable"
            if release_track is not None and track != release_track:
                continue
            out.append(n)
        return out


class EtcdBattleNodeResolver:
    """基于 EtcdDirectory 的 Battle 节点实时发现器（支持 release_track 过滤与冷却）。"""

    def __init__(self, directory) -> None:
        self._directory = directory
        self._cooldown_until: dict[str, float] = {}

    def mark_not_allocatable(self, node_uuid: str, *, cooldown_sec: float = 5.0) -> None:
        if node_uuid:
            self._cooldown_until[node_uuid] = time.monotonic() + cooldown_sec

    async def list_nodes(self, *, release_track: str | None = None) -> list[BattleNodeEntry]:
        raw_nodes = await self._directory.candidates(BATTLE_NODE_TYPE, None)
        now = time.monotonic()
        out: list[BattleNodeEntry] = []
        for info in raw_nodes:
            if not info.node_uuid or not info.grpc_endpoint.ip or not info.grpc_endpoint.port:
                continue
            if self._cooldown_until.get(info.node_uuid, 0.0) > now:
                continue
            track = info.release_track or "stable"
            if release_track is not None and track != release_track:
                continue
            if getattr(info, "admission", 0) == 2:  # NODE_ADMISSION_CLOSED
                continue
            host = f"[{info.grpc_endpoint.ip}]" if ":" in info.grpc_endpoint.ip else info.grpc_endpoint.ip
            c_ep = info.client_endpoint if info.HasField("client_endpoint") and info.client_endpoint.ip else info.endpoint
            out.append(
                BattleNodeEntry(
                    node_id=int(info.node_id),
                    node_uuid=str(info.node_uuid),
                    grpc_endpoint=f"{host}:{int(info.grpc_endpoint.port)}",
                    physical_region=int(info.physical_region) or 1,
                    client_host=str(c_ep.ip),
                    client_port=int(c_ep.port),
                    client_ws_url=str(getattr(info, "client_ws_url", "")),
                    release_track=track,
                    admitting=True,
                )
            )
        return out


class PresenceProvider(TypingProtocol):
    async def get_presence(self, player_id: int) -> PlayerPresence: ...


class MemoryPresenceProvider:
    """测试与本地运行用的内存玩家在线/场景位置目录。"""

    def __init__(self) -> None:
        self._players: dict[int, PlayerPresence] = {}

    def set_presence(self, presence: PlayerPresence) -> None:
        self._players[presence.player_id] = presence

    async def get_presence(self, player_id: int) -> PlayerPresence:
        return self._players.get(player_id, PlayerPresence(player_id=player_id, online=False))


def select_battle_node(
    nodes: list[BattleNodeEntry],
    *,
    preferred_regions: list[int] = (),
    release_track: str = "stable",
) -> BattleNodeEntry | None:
    """按发布轨道与首选机房挑选一台可用 Battle 节点。"""
    eligible = [n for n in nodes if (n.release_track or "stable") == release_track and n.admitting]
    if not eligible:
        return None
    for region in preferred_regions:
        if not region:
            continue
        in_region = [n for n in eligible if n.physical_region == region]
        if in_region:
            in_region.sort(key=lambda n: n.node_id)
            return in_region[0]
    eligible.sort(key=lambda n: (n.physical_region, n.node_id))
    return eligible[0]


def _check_table_fingerprints(mode: str, prepared: list[tuple[int, str, Message, str]]) -> tuple[str, int, bool]:
    """比对全员战斗配表指纹（off / warn / enforce）。返回 (统一指纹, 肇事者 player_id, 是否放行)。"""
    fps = [fp for _, _, _, fp in prepared if fp]
    if not fps:
        return "", 0, True
    counts = Counter(fps)
    if len(counts) == 1 and len(fps) == len(prepared):
        return fps[0], 0, True
    majority_fp = counts.most_common(1)[0][0]
    if mode == "off":
        return "", 0, True
    if mode == "warn":
        logger.warning("match gather table fingerprint mismatch under warn mode fps=%s", dict(counts))
        return "", 0, True
    # enforce 模式下找出首个与多数派不一致的玩家
    offender = next((pid for pid, _, _, fp in prepared if fp != majority_fp), prepared[0][0])
    return "", offender, False


def _is_battle_not_allocatable(exc: grpc.RpcError) -> bool:
    return exc.code() == grpc.StatusCode.UNAVAILABLE and BATTLE_NOT_ALLOCATABLE_MESSAGE in (exc.details() or "")


class GatherPipeline:
    """推进单局成局（MatchFormationRecord）的完整 Gather 开局管线。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MatchStore,
        config: MatchConfig,
        rpc: EndpointCaller,
        presence: PresenceProvider,
        battle_nodes: BattleNodeResolver,
        *,
        on_tournament_profiles: Callable[[list[tuple[int, str, int, int, str]]], None] | None = None,
        on_tournament_captains: Callable[[int, int], set[int]] | None = None,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.config = config
        self.rpc = rpc
        self.presence = presence
        self.battle_nodes = battle_nodes
        self.on_tournament_profiles = on_tournament_profiles
        self.on_tournament_captains = on_tournament_captains

    async def _cancel_prepared(self, battle_id: int, prepared: list[tuple[int, str]]) -> None:
        """发房前失败补偿：逐人向原备战端点发 CancelBattlePrepare（幂等）。"""
        for player_id, endpoint in prepared:
            if not endpoint:
                continue
            req = self.protocol.message("CancelBattlePrepareRequest", player_id=player_id, battle_id=battle_id)
            try:
                await self.rpc.call(endpoint, CANCEL_BATTLE_PREPARE_PATH, req.SerializeToString(deterministic=True), timeout=3.0)
            except Exception:
                logger.warning(
                    "match gather cancel prepare failed battle_id=%d player_id=%d endpoint=%s",
                    battle_id, player_id, endpoint, exc_info=True,
                )

    async def _destroy_battle(self, formation: Message, reason: str) -> bool:
        """向原目标 Battle 实例发 DestroyBattle 立墓碑；仅确认成功返回 True。"""
        req = self.protocol.message(
            "DestroyBattleRequest",
            battle_id=int(formation.battle_id),
            reason=reason,
            placement_operation_id=formation.operation_id,
            target_instance_id=formation.target_instance_id,
        )
        try:
            await self.rpc.call(
                formation.target_endpoint,
                DESTROY_BATTLE_PATH,
                req.SerializeToString(deterministic=True),
                timeout=3.0,
            )
            return True
        except Exception:
            logger.warning(
                "match gather destroy battle failed battle_id=%d endpoint=%s reason=%s",
                int(formation.battle_id), formation.target_endpoint, reason, exc_info=True,
            )
            return False

    async def run_formation(
        self,
        formation: Message,
        *,
        ai_defender_snapshots: tuple[Message, ...] = (),
        boss_weekly_affix_id: int = 0,
    ) -> bool:
        """同步执行一局成局的 Prepare -> Preflight -> SaveIntent -> CreateBattle 管线。"""
        battle_id = int(formation.battle_id)
        now_ms = await self.store.now_ms()
        deadline_ms = int(formation.deadline_ms)
        prepare_deadline_ms = min(
            max(int(formation.created_ms), now_ms) + self.config.prepare_deadline_sec * 1000,
            deadline_ms - 1000 if deadline_ms > now_ms + 2000 else now_ms + self.config.prepare_deadline_sec * 1000,
        )
        prepared_endpoints: list[tuple[int, str]] = []
        prepared_members: list[tuple[int, str, Message, str]] = []

        async def fail_pre_persist(reason: str, culprit: int = 0) -> bool:
            logger.info(
                "match gather pre-persist rollback battle_id=%d reason=%s culprit=%d",
                battle_id, reason, culprit,
            )
            await self._cancel_prepared(battle_id, prepared_endpoints)
            await self.store.abort_formation_pre_persist(battle_id, culprit)
            return False

        # 解码成局时冻结的可选上下文（锦标赛 V2/V3、自定义切磋规则、排位上下文）。
        raw_ranked = bytes(getattr(formation, "ranked_context", b""))
        raw_tournament = bytes(getattr(formation, "tournament_context", b""))
        raw_custom_duel = bytes(getattr(formation, "custom_duel_rules", b""))
        is_wuxing_tower = bool(getattr(formation, "is_wuxing_tower", False))
        ranked_ctx = None
        tournament_ctx = None
        custom_duel_rules = None
        try:
            if raw_ranked:
                ranked_ctx = self.protocol.parse("RankedMatchContext", raw_ranked)
            if raw_tournament:
                tournament_ctx = self.protocol.parse("TournamentBattleContext", raw_tournament)
            if raw_custom_duel:
                custom_duel_rules = self.protocol.parse("CustomDuelRules", raw_custom_duel)
        except Exception:
            logger.error("match gather invalid frozen context battle_id=%d", battle_id, exc_info=True)
            return await fail_pre_persist("invalid_frozen_context", 0)

        tournament_captains: set[int] = set()
        if tournament_ctx is not None and self.on_tournament_captains is not None:
            try:
                tournament_captains = set(
                    self.on_tournament_captains(int(tournament_ctx.squad_a_id), int(tournament_ctx.squad_b_id))
                )
            except Exception:
                logger.warning("match gather tournament captain lookup failed battle_id=%d", battle_id, exc_info=True)

        # 1. 逐参与者 PrepareBattle 收快照
        for member in formation.members:
            pid = int(member.player_id)
            pres = await self.presence.get_presence(pid)
            if not pres.scene_grpc_endpoint:
                return await fail_pre_persist("no_location", pid)
            endpoint = pres.scene_grpc_endpoint
            await self.store.mark_formation_prepared(battle_id, pid, endpoint)
            req = self.protocol.message(
                "PrepareBattleRequest",
                player_id=pid,
                battle_id=battle_id,
                battle_node_id=int(formation.target_node_id),
                deadline_ms=deadline_ms,
                prepare_deadline_ms=prepare_deadline_ms,
                battle_config_id=int(formation.config_id),
                match_mode=int(formation.mode),
                is_wuxing_tower=is_wuxing_tower,
            )
            if formation.activity_context:
                req.activity_context.CopyFrom(
                    self.protocol.parse("BattleActivityContext", formation.activity_context)
                )
            if ranked_ctx is not None:
                req.ranked_context.CopyFrom(ranked_ctx)
            if custom_duel_rules is not None:
                req.custom_duel_rules.CopyFrom(custom_duel_rules)
            try:
                raw_resp = await self.rpc.call(
                    endpoint, PREPARE_BATTLE_PATH, req.SerializeToString(deterministic=True), timeout=3.0
                )
                resp = self.protocol.parse("PrepareBattleResponse", raw_resp)
            except Exception:
                logger.warning("match gather prepare rpc failed battle_id=%d player_id=%d", battle_id, pid, exc_info=True)
                # 未知结果不定罪，避免无辜者被踢出队
                prepared_endpoints.append((pid, endpoint))
                return await fail_pre_persist("prepare_rpc_error", 0)
            err_id = int(resp.error_message.id)
            if err_id != 0:
                culprit = 0 if err_id == ERR_UNAVAILABLE else pid
                return await fail_pre_persist(f"prepare_rejected_{err_id}", culprit)
            prepared_endpoints.append((pid, endpoint))
            snap = self.protocol.message("BattlePlayerSnapshot")
            snap.CopyFrom(resp.snapshot)
            snap.team_index = int(member.team_index)
            if pid in tournament_captains:
                snap.is_team_captain = True
            prepared_members.append((pid, endpoint, snap, resp.table_fingerprint or snap.table_fingerprint))

        # 锦标赛 V3 模块 F：建房阶段顺手回填参战成员真实角色外观快照（最佳努力，不阻塞建房）。
        if int(formation.mode) == MODE_SEASON_TOURNAMENT and self.on_tournament_profiles is not None:
            try:
                profiles = [
                    (
                        pid,
                        str(getattr(snap, "player_name", "")),
                        int(getattr(snap, "class_id", 0)),
                        int(getattr(snap, "gender", 0)),
                        str(getattr(snap, "appearance_id", "")),
                    )
                    for pid, _, snap, _ in prepared_members
                ]
                self.on_tournament_profiles(profiles)
            except Exception:
                logger.warning("match gather tournament profile backfill failed battle_id=%d", battle_id, exc_info=True)

        # 2. 校验配表指纹一致性
        table_fp, offender, fp_ok = _check_table_fingerprints(self.config.table_fingerprint_mode, prepared_members)
        if not fp_ok:
            return await fail_pre_persist("fingerprint_mismatch", offender)

        # 3. BattleNode.QueryBattleCreation 预检（确认 v1 终局协议与可分配状态）
        preflight_req = self.protocol.message(
            "QueryBattleCreationRequest",
            battle_id=battle_id,
            placement_operation_id=formation.operation_id,
            target_instance_id=formation.target_instance_id,
        )
        try:
            raw_pf = await self.rpc.call(
                formation.target_endpoint,
                QUERY_BATTLE_CREATION_PATH,
                preflight_req.SerializeToString(deterministic=True),
                timeout=3.0,
            )
            pf_resp = self.protocol.parse("QueryBattleCreationResponse", raw_pf)
        except Exception:
            logger.warning("match gather preflight failed battle_id=%d endpoint=%s", battle_id, formation.target_endpoint, exc_info=True)
            return await fail_pre_persist("preflight_rpc_error", 0)

        if (
            pf_resp.target_instance_id != formation.target_instance_id
            or int(pf_resp.supported_outcome_protocol_version) != 1
            or bool(pf_resp.rejects_new_battles)
        ):
            if pf_resp.rejects_new_battles:
                self.battle_nodes.mark_not_allocatable(formation.target_instance_id)
            return await fail_pre_persist("preflight_rejected", 0)

        # 4. 构造并持久化确定性 CreateBattleRequest
        now_ms = await self.store.now_ms()
        created_at_ms = max(now_ms, int(formation.created_ms))
        has_activity = bool(formation.activity_context)
        allow_public_spectate = bool(custom_duel_rules.allow_public_spectate) if custom_duel_rules is not None else False
        create_req = self.protocol.message(
            "CreateBattleRequest",
            battle_id=battle_id,
            battle_config_id=int(formation.config_id),
            seed=secrets.randbits(64) or 1,
            match_mode=int(formation.mode),
            created_at_ms=created_at_ms,
            deadline_ms=deadline_ms,
            table_fingerprint=table_fp,
            required_physical_region=int(formation.physical_region),
            placement_operation_id=formation.operation_id,
            target_instance_id=formation.target_instance_id,
            admit_before_ms=prepare_deadline_ms,
            outcome_protocol_version=1,
            spectate_visibility=spectate_visibility_for(
                int(formation.mode),
                has_activity,
                allow_public_spectate=allow_public_spectate,
            ),
            record_replay=record_replay_for(int(formation.mode)),
        )
        if has_activity:
            create_req.activity_context.CopyFrom(
                self.protocol.parse("BattleActivityContext", bytes(formation.activity_context))
            )
        if ranked_ctx is not None:
            create_req.ranked_context.CopyFrom(ranked_ctx)
        if tournament_ctx is not None:
            create_req.tournament_context.CopyFrom(tournament_ctx)
        if custom_duel_rules is not None:
            create_req.custom_duel_rules.CopyFrom(custom_duel_rules)
        effective_boss_affix = int(boss_weekly_affix_id) or int(getattr(formation, "boss_weekly_affix_id", 0))
        if effective_boss_affix:
            create_req.boss_weekly_affix_id = effective_boss_affix
        for _, _, snap, _ in prepared_members:
            create_req.players.add().CopyFrom(snap)
        for ai_snap in ai_defender_snapshots:
            dst = create_req.players.add()
            dst.CopyFrom(ai_snap)
            dst.team_index = 1
            dst.ai_controlled = True

        create_bytes = create_req.SerializeToString(deterministic=True)
        try:
            formation = await self.store.save_formation_intent(battle_id, create_bytes, formation.target_instance_id)
        except (UnterminableOriginalError, StoreUnavailableError) as exc:
            logger.error("match gather intent persist rejected battle_id=%d err=%s", battle_id, exc)
            return await fail_pre_persist("intent_unterminable", 0)

        # 5. 调用 BattleNode.CreateBattle
        try:
            raw_create = await self.rpc.call(
                formation.target_endpoint, CREATE_BATTLE_PATH, create_bytes, timeout=5.0
            )
            create_resp = self.protocol.parse("CreateBattleResponse", raw_create)
        except grpc.RpcError as exc:
            if _is_battle_not_allocatable(exc):
                logger.warning(
                    "match gather battle_not_allocatable battle_id=%d instance=%s",
                    battle_id, formation.target_instance_id,
                )
                self.battle_nodes.mark_not_allocatable(formation.target_instance_id)
                await self._destroy_battle(formation, BATTLE_NOT_ALLOCATABLE_MESSAGE)
                await self._cancel_prepared(battle_id, prepared_endpoints)
                await self.store.abort_formation_pre_persist(battle_id, 0)
                return False
            # 落库后 Create 传输失败：结果未知，标记 UNKNOWN，禁止盲目取消或释票
            logger.error("match gather create battle outcome unknown battle_id=%d code=%s", battle_id, exc.code())
            await self.store.mark_formation_unknown(battle_id)
            return False
        except Exception:
            logger.error("match gather create battle unexpected error battle_id=%d", battle_id, exc_info=True)
            await self.store.mark_formation_unknown(battle_id)
            return False

        if int(create_resp.error_message.id) != 0:
            logger.warning(
                "match gather create battle rejected battle_id=%d err_id=%d",
                battle_id, int(create_resp.error_message.id),
            )
            await self._destroy_battle(formation, f"create_rejected_{int(create_resp.error_message.id)}")
            await self._cancel_prepared(battle_id, prepared_endpoints)
            await self.store.abort_formation_pre_persist(battle_id, 0)
            return False

        adopted = int(create_resp.adopted_outcome_protocol_version) or 1
        await self.store.mark_formation_created(battle_id, adopted)
        logger.info(
            "match gather battle created battle_id=%d mode=%d node_id=%d room_count=%d",
            battle_id, int(formation.mode), int(formation.target_node_id), int(create_resp.room_count),
        )
        return True


class Matchmaker:
    """按池撮合循环：将排队单排票与整队补位单凑局并派发给 GatherPipeline。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MatchStore,
        config: MatchConfig,
        gather: GatherPipeline,
        battle_nodes: BattleNodeResolver,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.config = config
        self.gather = gather
        self.battle_nodes = battle_nodes

    def _required_count(self, mode: int, team_size: int) -> tuple[int, int]:
        """返回 (每侧人数, 总人数)。"""
        if mode == MODE_PVE_SOLO:
            return 1, 1
        if mode == MODE_PVE_TEAM:
            n = max(1, min(team_size or 1, MAX_TEAM_SIZE))
            return n, n
        if mode == MODE_1V1:
            return 1, 2
        if mode == MODE_5V5:
            return MAX_TEAM_SIZE, MAX_TEAM_SIZE * 2
        if mode == MODE_PVP_TEAM:
            n = max(1, min(team_size or 1, MAX_TEAM_SIZE))
            return n, n * 2
        return 0, 0

    def _group_release_track(self, tickets: list[Message]) -> str:
        """全员探测轨道均为 canary 才放 canary battle，其余一律放 stable（灰度隔离契约）。"""
        tracks: list[str] = []
        for tk in tickets:
            track = "stable"
            if tk.network_profile:
                try:
                    prof = self.protocol.parse("match.MatchNetworkProfile", bytes(tk.network_profile))
                    track = prof.release_track or "stable"
                except Exception:
                    track = "stable"
            tracks.append(track)
        return "canary" if (tracks and all(t == "canary" for t in tracks)) else "stable"

    async def run_once(self) -> list[int]:
        """执行一轮撮合，返回本轮触发 Gather 的 battle_id 列表。"""
        tickets = await self.store.list_queued_tickets(limit=200)
        pools: dict[tuple[int, int, int], list[Message]] = {}
        for tk in tickets:
            key = (int(tk.mode), int(tk.config_id), int(tk.team_size))
            pools.setdefault(key, []).append(tk)

        formed_ids: list[int] = []
        now_ms = await self.store.now_ms()
        for (mode, config_id, team_size), group in pools.items():
            per_side, total = self._required_count(mode, team_size)
            if total <= 0 or len(group) < total:
                continue
            idx = 0
            while idx + total <= len(group):
                batch = group[idx : idx + total]
                idx += total
                track = self._group_release_track(batch)
                nodes = await self.battle_nodes.list_nodes(release_track=track)
                prefs = [int(tk.pool_region) for tk in batch if int(tk.pool_region)]
                if not prefs:
                    for tk in batch:
                        if tk.network_profile:
                            try:
                                prof = self.protocol.parse("match.MatchNetworkProfile", bytes(tk.network_profile))
                                r = preferred_region(prof, now_ms, self.config.hard_latency_ms)
                                if r:
                                    prefs.append(r)
                            except Exception:
                                pass
                node = select_battle_node(nodes, preferred_regions=prefs, release_track=track)
                if node is None:
                    continue
                members: list[tuple[int, str, int, bool]] = []
                for pos, tk in enumerate(batch):
                    side = 0 if mode in (MODE_PVE_SOLO, MODE_PVE_TEAM) else (0 if pos < per_side else 1)
                    members.append((int(tk.player_id), tk.ticket, side, False))
                try:
                    formation = await self.store.claim_formation(
                        mode=mode,
                        config_id=config_id,
                        members=members,
                        physical_region=node.physical_region,
                        target_node_id=node.node_id,
                        target_instance_id=node.node_uuid,
                        target_endpoint=node.grpc_endpoint,
                        release_track=node.release_track or "stable",
                        battle_deadline_ms=self.config.battle_deadline_sec * 1000,
                        requeue_on_failure=True,
                    )
                except StoreUnavailableError:
                    continue
                formed_ids.append(int(formation.battle_id))
                await self.gather.run_formation(formation)
        return formed_ids

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("matchmaker loop round failed", exc_info=True)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.config.matcher_interval_sec)
            except TimeoutError:
                pass
