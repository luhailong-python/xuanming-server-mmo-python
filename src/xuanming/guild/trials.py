"""guild 同道历练（Guild Trials）全链路编排：MatchInternal HMAC 客户端、SQL 大厅/启动/取消协议、战斗结算与后台对账。"""

from __future__ import annotations

import asyncio
import hmac
import logging
import secrets
import time
from collections.abc import Awaitable, Callable, Sequence

from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import service_digest, sign_service_call, verify_service_call
from ..protocol import Protocol
from .asset_loop import GuildAssetOpLoop
from .errors import (
    ERR_GUILD_ACTIVITY_JOIN_TOO_RECENT,
    ERR_GUILD_ACTIVITY_LEVEL_TOO_LOW,
    ERR_GUILD_ACTIVITY_NOT_OPEN,
    ERR_GUILD_TRIAL_BATTLE_ABORTED,
    ERR_GUILD_TRIAL_INVITE_COOLDOWN,
    ERR_GUILD_TRIAL_INVITE_DECLINED,
    ERR_GUILD_TRIAL_INVITE_EXPIRED,
    ERR_GUILD_TRIAL_SERVICE_BUSY,
    ERR_GUILD_TRIAL_TEAM_INVALID,
    GuildError,
    GuildNotFoundError,
    GuildWriteConflictError,
    TrialActivityPoisonError,
    TrialInputInvalidError,
    TrialMatchUnavailableError,
)
from .model import (
    ACTIVITY_TYPE_TRIAL,
    ASSET_STREAM_GUILD_CREDIT,
    ACTIVITY_BATTLE_REJECT_INTERNAL,
    ACTIVITY_BATTLE_REJECT_INVALID_ARGUMENT,
    ACTIVITY_BATTLE_REJECT_MEMBER_IN_BATTLE,
    ACTIVITY_BATTLE_REJECT_MEMBER_NOT_READY,
    ACTIVITY_BATTLE_REJECT_MEMBER_OFFLINE,
    ACTIVITY_BATTLE_REJECT_NONE,
    ActivityRuleRow,
    BATTLE_ACTIVITY_KIND_GUILD_TRIAL,
    BATTLE_OUTCOME_SIDE_A_WIN,
    GuildData,
    MATCH_INTERNAL_CALLER,
    MAX_TRIAL_LOBBY_ROSTER,
    MIN_TRIAL_LOBBY_ROSTER,
    OP_KIND_ACTIVITY_REWARD,
    OUTCOME_DECISION_ABORT,
    OUTCOME_DECISION_COMPLETE,
    PendingAssetOp,
    RoleOriginal,
    TRIAL_LAUNCH_STATE_PREPARED,
    TRIAL_LOBBY_STATE_ENDED,
    TRIAL_LOBBY_STATE_LAUNCHED,
    TRIAL_LOBBY_STATE_LAUNCHING,
    TRIAL_LOBBY_STATE_PENDING,
    TRIAL_OCCUPANCY_MARGIN_MS,
    TRIAL_OCCUPANCY_REJECT_DENIED,
    TRIAL_OCCUPANCY_REJECT_NONE,
    TRIAL_OCCUPANCY_REJECT_OCCUPIED,
    TRIAL_OCCUPANCY_REJECT_RETRY,
    TRIAL_OCCUPANCY_REJECT_ROLE,
    TRIAL_LAUNCH_WINDOW_MS,
    TX_GUILD_ACTIVITY_REWARD,
    TrialBattleKey,
    TrialLaunchRecord,
    TrialLobbyRecord,
    TrialRewardOp,
    TrialSettleResult,
    day_key,
    guild_period_key,
    joined_long_enough,
    pick_activity_for_write,
)
from .store import GuildNotifier, GuildStoreCore, OnlineStatusResolver

logger = logging.getLogger(__name__)

_MATCH_INTERNAL_PREFIX = "/match.MatchInternal/"
_PATH_ACQUIRE_OCCUPANCY = _MATCH_INTERNAL_PREFIX + "AcquireTrialOccupancy"
_PATH_RELEASE_OCCUPANCY = _MATCH_INTERNAL_PREFIX + "ReleaseTrialOccupancy"
_PATH_START_BATTLE = _MATCH_INTERNAL_PREFIX + "StartActivityBattle"
_PATH_CANCEL_BATTLE = _MATCH_INTERNAL_PREFIX + "CancelActivityBattle"
_PATH_GET_COMPLETION = _MATCH_INTERNAL_PREFIX + "GetBattleCompletion"

_MATCH_REPLY_CALLER = "match"
_FORMATION_STATE_TERMINAL = 5
_CHANGE_ACTIVITY_CHANGED = 12

DEFAULT_MATCH_TIMEOUT_SEC = 1.5
DEFAULT_TRIAL_SWEEP_INTERVAL_SEC = 2.0
DEFAULT_BATTLE_OVERDUE_MS = 300_000
DEFAULT_BATTLE_ABANDON_MS = 3_600_000


def is_terminal_cancel_receipt(receipt: Message) -> bool:
    """判定经 match 验签的 CancelActivityBattleResponse 是否证明无活跃对局在运行。"""
    battle_id = int(receipt.battle_id)
    if bool(receipt.request_cancelled):
        return battle_id == 0
    if battle_id == 0:
        return False
    form_state = int(receipt.formation_state)
    decision = int(receipt.terminal_decision)
    if decision == OUTCOME_DECISION_ABORT:
        return True
    if form_state == _FORMATION_STATE_TERMINAL and decision != OUTCOME_DECISION_COMPLETE:
        return True
    return False


def normalize_trial_roster(
    initiator_id: int,
    member_player_ids: Sequence[int],
    size_min: int,
    size_max: int,
) -> list[int]:
    """规范化历练名单：发起者置首、去重、校验零值与队伍人数区间。"""
    if initiator_id <= 0:
        raise GuildError("invalid initiator", tip_id=ERR_GUILD_TRIAL_TEAM_INVALID)
    lo = max(MIN_TRIAL_LOBBY_ROSTER, int(size_min) or MIN_TRIAL_LOBBY_ROSTER)
    hi = min(MAX_TRIAL_LOBBY_ROSTER, int(size_max) or MAX_TRIAL_LOBBY_ROSTER)
    roster: list[int] = [initiator_id]
    seen: set[int] = {initiator_id}
    for raw_pid in member_player_ids:
        pid = int(raw_pid)
        if pid <= 0:
            raise GuildError("zero player_id in trial roster", tip_id=ERR_GUILD_TRIAL_TEAM_INVALID)
        if pid in seen:
            continue
        seen.add(pid)
        roster.append(pid)
    if not (lo <= len(roster) <= hi):
        raise GuildError(
            f"trial roster size {len(roster)} out of range [{lo}, {hi}]",
            tip_id=ERR_GUILD_TRIAL_TEAM_INVALID,
        )
    return roster


def build_start_activity_battle_request(protocol: Protocol, launch_orig: Message) -> Message:
    """由永久 GuildTrialLaunchOriginal 确定性重建 StartActivityBattleRequest（用于发战与取消对账）。"""
    req = protocol.message(
        "match.StartActivityBattleRequest",
        request_id=int(launch_orig.request_id),
        battle_config_id=int(launch_orig.dungeon_id),
        member_player_ids=[int(r.player_id) for r in launch_orig.roster],
    )
    req.activity_context.kind = BATTLE_ACTIVITY_KIND_GUILD_TRIAL
    req.activity_context.guild_id = int(launch_orig.guild_id)
    req.activity_context.activity_id = int(launch_orig.activity_id)
    req.activity_context.period_key = int(launch_orig.period_key)
    req.activity_context.guild_period_key = int(launch_orig.guild_period_key)
    req.activity_context.initiator_player_id = int(launch_orig.initiator_player_id)
    req.activity_context.launch_request_id = int(launch_orig.request_id)
    return req


def build_trial_reward_payload(protocol: Protocol, reward_items: Sequence[tuple[int, int]]) -> bytes:
    """将活动奖励条目编码为确定性 AssetBundle 载荷（无物品奖励时返回空串）。"""
    if not reward_items:
        return b""
    bundle = protocol.message("AssetBundle")
    for item_id, count in reward_items:
        bundle.items.add(config_id=item_id, count=count)
    return bundle.SerializeToString(deterministic=True)


def _map_start_battle_reject_tip(reject: int) -> int:
    if reject in (
        ACTIVITY_BATTLE_REJECT_INVALID_ARGUMENT,
        ACTIVITY_BATTLE_REJECT_MEMBER_OFFLINE,
        ACTIVITY_BATTLE_REJECT_MEMBER_IN_BATTLE,
        ACTIVITY_BATTLE_REJECT_MEMBER_NOT_READY,
    ):
        return ERR_GUILD_TRIAL_TEAM_INVALID
    return ERR_GUILD_TRIAL_SERVICE_BUSY


class MatchInternalClient:
    """经 HMAC-SHA256 双向签名的 match.MatchInternal 客户端（caller='guild'，验签 'match' 回执）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        secret: bytes,
        *,
        caller: str = MATCH_INTERNAL_CALLER,
        timeout_sec: float = DEFAULT_MATCH_TIMEOUT_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        if len(secret) < 32:
            raise ValueError("MMORPG_MATCH_INTERNAL_SECRET_GUILD must be at least 32 bytes")
        self._protocol = protocol
        self._call = call
        self._secret = secret
        self._caller = caller
        self._timeout_sec = timeout_sec
        self._clock_ms = clock_ms

    async def acquire_trial_occupancy(
        self, owner_id: int, roster: Sequence[RoleOriginal], ttl_ms: int
    ) -> tuple[int, int]:
        req = self._protocol.message(
            "match.AcquireTrialOccupancyRequest",
            owner_id=owner_id,
            ttl_ms=ttl_ms,
        )
        for role in roster:
            req.roster.add(
                player_id=role.player_id,
                account=role.account,
                owner_revision=role.owner_revision,
            )
        sign_service_call(_PATH_ACQUIRE_OCCUPANCY, req, self._caller, self._clock_ms(), self._secret)
        try:
            raw = await self._call(
                _PATH_ACQUIRE_OCCUPANCY,
                req.SerializeToString(deterministic=True),
                timeout=self._timeout_sec,
            )
        except Exception as exc:
            raise TrialMatchUnavailableError(f"AcquireTrialOccupancy failed owner={owner_id}: {exc}") from exc
        resp = self._protocol.parse("match.AcquireTrialOccupancyResponse", raw)
        return int(resp.reject), int(resp.offender_player_id)

    async def release_trial_occupancy(self, owner_id: int, player_ids: Sequence[int]) -> int:
        req = self._protocol.message(
            "match.ReleaseTrialOccupancyRequest",
            owner_id=owner_id,
            player_ids=[int(p) for p in player_ids],
        )
        sign_service_call(_PATH_RELEASE_OCCUPANCY, req, self._caller, self._clock_ms(), self._secret)
        try:
            raw = await self._call(
                _PATH_RELEASE_OCCUPANCY,
                req.SerializeToString(deterministic=True),
                timeout=self._timeout_sec,
            )
        except Exception as exc:
            raise TrialMatchUnavailableError(f"ReleaseTrialOccupancy failed owner={owner_id}: {exc}") from exc
        resp = self._protocol.parse("match.ReleaseTrialOccupancyResponse", raw)
        return int(resp.reject)

    async def start_activity_battle(self, request: Message) -> tuple[int, int, int, int]:
        signed = self._protocol.message("match.StartActivityBattleRequest")
        signed.CopyFrom(request)
        sign_service_call(_PATH_START_BATTLE, signed, self._caller, self._clock_ms(), self._secret)
        try:
            raw = await self._call(
                _PATH_START_BATTLE,
                signed.SerializeToString(deterministic=True),
                timeout=self._timeout_sec,
            )
        except Exception as exc:
            raise TrialMatchUnavailableError(
                f"StartActivityBattle failed request_id={int(request.request_id)}: {exc}"
            ) from exc
        resp = self._protocol.parse("match.StartActivityBattleResponse", raw)
        return int(resp.reject), int(resp.offender_player_id), int(resp.battle_id), int(resp.formation_state)

    async def cancel_activity_battle(self, original: Message) -> Message:
        """发送 CancelActivityBattle 并强制校验 'match' 签名与 request_digest（fail-closed）。"""
        req = self._protocol.message("match.CancelActivityBattleRequest")
        req.original.CopyFrom(original)
        req.original.ClearField("auth")
        expected_digest = service_digest(req)
        now_ms = self._clock_ms()
        sign_service_call(_PATH_CANCEL_BATTLE, req, self._caller, now_ms, self._secret)
        try:
            raw = await self._call(
                _PATH_CANCEL_BATTLE,
                req.SerializeToString(deterministic=True),
                timeout=self._timeout_sec,
            )
        except Exception as exc:
            raise TrialMatchUnavailableError(
                f"CancelActivityBattle failed request_id={int(original.request_id)}: {exc}"
            ) from exc
        resp = self._protocol.parse("match.CancelActivityBattleResponse", raw)
        verify_now = self._clock_ms()
        if not verify_service_call(_PATH_CANCEL_BATTLE + "/reply", resp, _MATCH_REPLY_CALLER, verify_now, self._secret):
            raise TrialMatchUnavailableError("CancelActivityBattle reply HMAC verification failed")
        if not hmac.compare_digest(str(resp.request_digest), expected_digest):
            raise TrialMatchUnavailableError("CancelActivityBattle reply request_digest mismatch")
        if bool(resp.request_cancelled) and int(resp.battle_id) != 0:
            raise TrialMatchUnavailableError("CancelActivityBattle reply contradictory cancelled+battle_id")
        if int(resp.formation_state) > _FORMATION_STATE_TERMINAL:
            raise TrialMatchUnavailableError(f"CancelActivityBattle reply invalid formation_state={resp.formation_state}")
        return resp

    async def get_battle_completion(self, battle_id: int) -> tuple[int, int, bytes]:
        req = self._protocol.message("match.GetBattleCompletionRequest", battle_id=battle_id)
        sign_service_call(_PATH_GET_COMPLETION, req, self._caller, self._clock_ms(), self._secret)
        try:
            raw = await self._call(
                _PATH_GET_COMPLETION,
                req.SerializeToString(deterministic=True),
                timeout=self._timeout_sec,
            )
        except Exception as exc:
            raise TrialMatchUnavailableError(f"GetBattleCompletion failed battle_id={battle_id}: {exc}") from exc
        resp = self._protocol.parse("match.GetBattleCompletionResponse", raw)
        return int(resp.reject), int(resp.decision), bytes(resp.raw_payload)


class GuildTrialCoordinator:
    """同道历练协调器：大厅创建/邀请响应/发战取消/战斗结算与后台兜底循环。"""

    def __init__(
        self,
        protocol: Protocol,
        store: GuildStoreCore,
        match_client: MatchInternalClient,
        *,
        online_resolver: OnlineStatusResolver,
        notifier: GuildNotifier,
        asset_loop: GuildAssetOpLoop | None = None,
        asset_op_lease_ms: int = 10_000,
        sweep_interval_sec: float = DEFAULT_TRIAL_SWEEP_INTERVAL_SEC,
        battle_overdue_ms: int = DEFAULT_BATTLE_OVERDUE_MS,
        battle_abandon_ms: int = DEFAULT_BATTLE_ABANDON_MS,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.match_client = match_client
        self._online = online_resolver
        self._notifier = notifier
        self._asset_loop = asset_loop
        self._asset_op_lease_ms = asset_op_lease_ms
        self._sweep_interval_sec = sweep_interval_sec
        self._battle_overdue_ms = battle_overdue_ms
        self._battle_abandon_ms = battle_abandon_ms
        self._clock_ms = clock_ms

    # -- 内部辅助 --------------------------------------------------------------------------------

    async def _resolve_roster_identities(
        self, roster: Sequence[int], actor: RoleOriginal | None = None
    ) -> list[RoleOriginal]:
        fences = await self.store.get_role_fences_local(roster)
        out: list[RoleOriginal] = []
        for pid in roster:
            if actor is not None and pid == actor.player_id:
                out.append(actor)
            elif pid in fences:
                f = fences[pid]
                out.append(RoleOriginal(player_id=pid, account=f.account, owner_revision=f.owner_revision))
            else:
                out.append(RoleOriginal(player_id=pid, account="", owner_revision=0))
        return out

    async def _release_occupancy_best_effort(self, owner_id: int, player_ids: Sequence[int]) -> None:
        if owner_id == 0 or not player_ids:
            return
        try:
            reject = await self.match_client.release_trial_occupancy(owner_id, player_ids)
            if reject != TRIAL_OCCUPANCY_REJECT_NONE:
                logger.warning("guild trial release occupancy rejected owner=%d reject=%d", owner_id, reject)
        except Exception as exc:
            logger.warning("guild trial release occupancy failed owner=%d err=%s", owner_id, exc)

    async def _notify_activity(self, guild_id: int, actor_id: int, recipients: Sequence[int]) -> None:
        if not recipients:
            return
        try:
            await self._notifier.notify(_CHANGE_ACTIVITY_CHANGED, guild_id, actor_id, 0, list(recipients))
        except Exception as exc:
            logger.warning("guild trial notify failed guild=%d err=%s", guild_id, exc)

    async def _validate_trial_roster_and_rule(
        self,
        guild: GuildData,
        activity_id: int,
        roster: Sequence[int],
        now_ms: int,
    ) -> tuple[ActivityRuleRow | None, int]:
        """校验活动开放、帮会等级、全体成员资格/入会时长与严格在线状态。返回 (rule, tip_id)。"""
        rule = pick_activity_for_write(self.store.catalog.activities, ACTIVITY_TYPE_TRIAL, activity_id, now_ms)
        if rule is None or rule.activity_type != ACTIVITY_TYPE_TRIAL:
            return None, ERR_GUILD_ACTIVITY_NOT_OPEN
        if guild.level < rule.min_guild_level:
            return None, ERR_GUILD_ACTIVITY_LEVEL_TOO_LOW

        member_joins = await self.store.get_trial_roster_members(guild.guild_id, roster)
        if len(member_joins) != len(roster):
            return None, ERR_GUILD_TRIAL_TEAM_INVALID
        min_hours = self.store.catalog.rule.activity_join_min_hours
        for pid in roster:
            join_ms = member_joins.get(pid)
            if join_ms is None:
                return None, ERR_GUILD_TRIAL_TEAM_INVALID
            if not joined_long_enough(join_ms, now_ms, min_hours):
                return None, ERR_GUILD_ACTIVITY_JOIN_TOO_RECENT if pid == roster[0] else ERR_GUILD_TRIAL_TEAM_INVALID

        try:
            online_map = await self._online.batch_online(roster, strict=True)
        except Exception as exc:
            logger.warning("guild trial strict online check failed guild=%d err=%s", guild.guild_id, exc)
            return None, ERR_GUILD_TRIAL_SERVICE_BUSY
        if not all(online_map.get(pid, False) for pid in roster):
            return None, ERR_GUILD_TRIAL_TEAM_INVALID
        return rule, 0

    # -- 玩家侧 RPC 流程 -------------------------------------------------------------------------

    async def start_trial(
        self,
        actor: RoleOriginal,
        guild: GuildData,
        activity_id: int,
        member_player_ids: Sequence[int],
    ) -> tuple[int, tuple[str, ...]]:
        """发起同道历练组队邀请：校验资格 -> 抢占 match 占用 -> 写入 SQL 大厅。"""
        now_ms = self._clock_ms()
        rule = pick_activity_for_write(self.store.catalog.activities, ACTIVITY_TYPE_TRIAL, activity_id, now_ms)
        if rule is None or rule.activity_type != ACTIVITY_TYPE_TRIAL:
            return ERR_GUILD_ACTIVITY_NOT_OPEN, ()
        if guild.level < rule.min_guild_level:
            return ERR_GUILD_ACTIVITY_LEVEL_TOO_LOW, ()

        try:
            roster = normalize_trial_roster(actor.player_id, member_player_ids, rule.team_size_min, rule.team_size_max)
        except GuildError as exc:
            return exc.tip_id, exc.parameters

        rule, tip_id = await self._validate_trial_roster_and_rule(guild, rule.activity_id, roster, now_ms)
        if tip_id != 0 or rule is None:
            return tip_id, ()

        pre_status, extra = await self.store.precheck_trial_lobby_create(roster, actor.player_id, now_ms)
        if pre_status == "cooldown":
            return ERR_GUILD_TRIAL_INVITE_COOLDOWN, (str(max(1, (extra + 999) // 1000)),)
        if pre_status == "busy":
            return ERR_GUILD_TRIAL_TEAM_INVALID, ()

        ttl_ms = max(1_000, self.store.catalog.rule.trial_invite_ttl_seconds * 1000)
        cooldown_ms = max(0, self.store.catalog.rule.trial_invite_cooldown_seconds * 1000)
        lobby_id = await self.store.allocate_op_id()
        identities = await self._resolve_roster_identities(roster, actor=actor)

        try:
            reject, _offender = await self.match_client.acquire_trial_occupancy(
                lobby_id, identities, ttl_ms + TRIAL_OCCUPANCY_MARGIN_MS
            )
        except TrialMatchUnavailableError as exc:
            logger.warning("guild trial start occupancy unavailable guild=%d err=%s", guild.guild_id, exc)
            return ERR_GUILD_TRIAL_SERVICE_BUSY, ()

        if reject in (TRIAL_OCCUPANCY_REJECT_OCCUPIED, TRIAL_OCCUPANCY_REJECT_ROLE):
            return ERR_GUILD_TRIAL_TEAM_INVALID, ()
        if reject != TRIAL_OCCUPANCY_REJECT_NONE:
            return ERR_GUILD_TRIAL_SERVICE_BUSY, ()

        try:
            status, _lobby_rec, create_extra = await self.store.create_trial_lobby(
                actor,
                lobby_id=lobby_id,
                guild_id=guild.guild_id,
                activity_id=rule.activity_id,
                roster=roster,
                ttl_ms=ttl_ms,
                cooldown_ms=cooldown_ms,
                now_ms=now_ms,
            )
        except (GuildNotFoundError, GuildWriteConflictError) as exc:
            await self._release_occupancy_best_effort(lobby_id, roster)
            logger.warning("guild trial create lobby failed guild=%d lobby=%d err=%s", guild.guild_id, lobby_id, exc)
            return ERR_GUILD_TRIAL_SERVICE_BUSY, ()
        except Exception:
            await self._release_occupancy_best_effort(lobby_id, roster)
            raise

        if status == "cooldown":
            await self._release_occupancy_best_effort(lobby_id, roster)
            return ERR_GUILD_TRIAL_INVITE_COOLDOWN, (str(max(1, (create_extra + 999) // 1000)),)
        if status == "busy":
            await self._release_occupancy_best_effort(lobby_id, roster)
            return ERR_GUILD_TRIAL_TEAM_INVALID, ()

        await self._notify_activity(guild.guild_id, actor.player_id, roster)
        return 0, ()

    async def respond_invite(
        self,
        actor: RoleOriginal,
        guild: GuildData,
        lobby_id: int,
        accept: bool,
    ) -> tuple[int, tuple[str, ...]]:
        """响应历练邀请：同意续租并在齐人后发战；拒绝则立即关闭大厅并释放占用。"""
        now_ms = self._clock_ms()
        pre = await self.store.get_trial_lobby(lobby_id)
        if pre is None or pre.guild_id != guild.guild_id:
            return ERR_GUILD_TRIAL_INVITE_EXPIRED, ()
        orig = self.protocol.parse("guildpb.GuildTrialLobbyOriginal", pre.original_proto)
        roster_ids = [int(r.player_id) for r in orig.roster]
        if actor.player_id not in roster_ids:
            return ERR_GUILD_TRIAL_INVITE_EXPIRED, ()

        if accept and pre.state == TRIAL_LOBBY_STATE_PENDING and now_ms < pre.expires_ms:
            remaining = max(1_000, pre.expires_ms - now_ms)
            identities = [
                RoleOriginal(
                    player_id=int(r.player_id),
                    account=str(r.account),
                    owner_revision=int(r.owner_revision),
                )
                for r in orig.roster
            ]
            try:
                reject, _ = await self.match_client.acquire_trial_occupancy(
                    lobby_id, identities, remaining + TRIAL_OCCUPANCY_MARGIN_MS
                )
                if reject in (TRIAL_OCCUPANCY_REJECT_OCCUPIED, TRIAL_OCCUPANCY_REJECT_ROLE):
                    return ERR_GUILD_TRIAL_TEAM_INVALID, ()
                if reject != TRIAL_OCCUPANCY_REJECT_NONE:
                    return ERR_GUILD_TRIAL_SERVICE_BUSY, ()
            except TrialMatchUnavailableError as exc:
                logger.warning("guild trial accept occupancy renew failed lobby=%d err=%s", lobby_id, exc)
                return ERR_GUILD_TRIAL_SERVICE_BUSY, ()

        status, _already, updated = await self.store.respond_trial_lobby(
            actor,
            lobby_id=lobby_id,
            accept=accept,
            launch_window_ms=TRIAL_LAUNCH_WINDOW_MS,
            now_ms=now_ms,
        )

        if status == "gone":
            await self._release_occupancy_best_effort(lobby_id, roster_ids)
            return ERR_GUILD_TRIAL_INVITE_EXPIRED, ()
        if status == "not_pending":
            return ERR_GUILD_TRIAL_INVITE_EXPIRED, ()
        if status == "declined":
            await self._release_occupancy_best_effort(lobby_id, roster_ids)
            await self._notify_activity(guild.guild_id, actor.player_id, roster_ids)
            return 0, ()
        if status == "accepted":
            await self._notify_activity(guild.guild_id, actor.player_id, roster_ids)
            return 0, ()

        assert status == "launch" and updated is not None
        return await self._launch_trial(actor, guild, updated, now_ms)

    async def _launch_trial(
        self,
        actor: RoleOriginal,
        guild: GuildData,
        lobby: TrialLobbyRecord,
        now_ms: int,
    ) -> tuple[int, tuple[str, ...]]:
        """齐人发战：重验名单/在线 -> 持久化 launch -> StartActivityBattle -> 失败走签名取消。"""
        orig = self.protocol.parse("guildpb.GuildTrialLobbyOriginal", lobby.original_proto)
        roster_ids = [int(r.player_id) for r in orig.roster]
        request_id = lobby.lobby_id

        rule, tip_id = await self._validate_trial_roster_and_rule(guild, lobby.activity_id, roster_ids, now_ms)
        if tip_id != 0 or rule is None:
            await self.store.finish_trial_lobby(
                lobby.lobby_id,
                expected_state=TRIAL_LOBBY_STATE_LAUNCHING,
                new_state=TRIAL_LOBBY_STATE_ENDED,
                end_tip_id=tip_id or ERR_GUILD_ACTIVITY_NOT_OPEN,
                now_ms=now_ms,
            )
            await self._release_occupancy_best_effort(lobby.lobby_id, roster_ids)
            await self._notify_activity(guild.guild_id, actor.player_id, roster_ids)
            return tip_id or ERR_GUILD_ACTIVITY_NOT_OPEN, ()

        p_key = day_key(now_ms)
        g_key = guild_period_key(rule, now_ms)
        battle_key = TrialBattleKey(
            battle_id=0,
            guild_id=guild.guild_id,
            activity_id=lobby.activity_id,
            period_key=p_key,
            guild_period_key=g_key,
            initiator_player_id=lobby.initiator_player_id,
            launch_request_id=request_id,
        )

        try:
            launch_orig = await self.store.prepare_trial_launch(
                request_id=request_id,
                battle=battle_key,
                dungeon_id=rule.dungeon_id,
                now_ms=now_ms,
            )
        except Exception as exc:
            logger.warning("guild trial prepare_trial_launch failed lobby=%d err=%s", request_id, exc)
            await self.store.finish_trial_lobby(
                lobby.lobby_id,
                expected_state=TRIAL_LOBBY_STATE_LAUNCHING,
                new_state=TRIAL_LOBBY_STATE_ENDED,
                end_tip_id=ERR_GUILD_TRIAL_SERVICE_BUSY,
                now_ms=now_ms,
            )
            await self._release_occupancy_best_effort(lobby.lobby_id, roster_ids)
            await self._notify_activity(guild.guild_id, actor.player_id, roster_ids)
            return ERR_GUILD_TRIAL_SERVICE_BUSY, ()

        start_req = build_start_activity_battle_request(self.protocol, launch_orig)

        reject = ACTIVITY_BATTLE_REJECT_INTERNAL
        battle_id = 0
        try:
            reject, _offender, battle_id, _form_state = await self.match_client.start_activity_battle(start_req)
        except TrialMatchUnavailableError as exc:
            logger.warning("guild trial StartActivityBattle failed request=%d err=%s", request_id, exc)
            reject = ACTIVITY_BATTLE_REJECT_INTERNAL
            battle_id = 0

        if reject == ACTIVITY_BATTLE_REJECT_NONE and battle_id != 0:
            registered_key = TrialBattleKey(
                battle_id=battle_id,
                guild_id=guild.guild_id,
                activity_id=lobby.activity_id,
                period_key=p_key,
                guild_period_key=g_key,
                initiator_player_id=lobby.initiator_player_id,
                launch_request_id=request_id,
            )
            await self.store.register_trial_battle(registered_key, now_ms)
            await self.store.finish_trial_lobby(
                lobby.lobby_id,
                expected_state=TRIAL_LOBBY_STATE_LAUNCHING,
                new_state=TRIAL_LOBBY_STATE_LAUNCHED,
                battle_id=battle_id,
                now_ms=now_ms,
            )
            await self._notify_activity(guild.guild_id, actor.player_id, roster_ids)
            return 0, ()

        # 发战被拒或超时：记录实时提示并立即尝试经 match 签名取消释放；失败则留给后台 recover_launches_once
        end_tip = _map_start_battle_reject_tip(reject)
        await self.store.record_trial_launch_end_tip(request_id, end_tip)
        await self.store.expedite_trial_launch_check(request_id, now_ms)
        launched_anyway = await self._reconcile_prepared_launch(launch_orig, start_req, now_ms)
        if launched_anyway:
            await self._notify_activity(guild.guild_id, actor.player_id, roster_ids)
            return 0, ()
        await self._notify_activity(guild.guild_id, actor.player_id, roster_ids)
        return end_tip, ()

    async def _reconcile_prepared_launch(
        self, launch_orig: Message, start_req: Message, now_ms: int
    ) -> bool:
        """向 match 请求签名取消：若收到终态取消回执则释放占用与 launch；若对局已真实拉起则补登记并返回 True。"""
        request_id = int(launch_orig.request_id)
        try:
            cancel_resp = await self.match_client.cancel_activity_battle(start_req)
        except TrialMatchUnavailableError as exc:
            logger.warning("guild trial cancel_activity_battle failed request=%d err=%s", request_id, exc)
            return False

        if is_terminal_cancel_receipt(cancel_resp):
            released, roster_ids = await self.store.finish_cancelled_trial_launch(request_id, cancel_resp, now_ms)
            if released:
                await self._release_occupancy_best_effort(request_id, roster_ids)
            return False

        battle_id = int(cancel_resp.battle_id)
        if battle_id != 0:
            # match 证明对局已实际启动且未终止：补登记对局与 LAUNCHED 大厅状态
            key = TrialBattleKey(
                battle_id=battle_id,
                guild_id=int(launch_orig.guild_id),
                activity_id=int(launch_orig.activity_id),
                period_key=int(launch_orig.period_key),
                guild_period_key=int(launch_orig.guild_period_key),
                initiator_player_id=int(launch_orig.initiator_player_id),
                launch_request_id=request_id,
            )
            await self.store.register_trial_battle(key, now_ms)
            await self.store.finish_trial_lobby(
                request_id,
                expected_state=TRIAL_LOBBY_STATE_LAUNCHING,
                new_state=TRIAL_LOBBY_STATE_LAUNCHED,
                battle_id=battle_id,
                now_ms=now_ms,
            )
            return True
        return False

    # -- 战斗结果结算（Kafka BattleResultEvent 或 GetBattleCompletion 兜底共用） -------------------

    async def handle_battle_result_event(self, event: Message) -> TrialSettleResult | None:
        """处理 contracts.kafka.BattleResultEvent（仅消费 BATTLE_ACTIVITY_KIND_GUILD_TRIAL）。"""
        if not event.HasField("activity_context"):
            return None
        ctx = event.activity_context
        if int(ctx.kind) != BATTLE_ACTIVITY_KIND_GUILD_TRIAL:
            return None

        battle_id = int(event.battle_id)
        guild_id = int(ctx.guild_id)
        activity_id = int(ctx.activity_id)
        period_k = int(ctx.period_key)
        guild_period_k = int(ctx.guild_period_key)
        initiator_id = int(ctx.initiator_player_id)
        launch_req_id = int(ctx.launch_request_id)
        finished_at_ms = int(event.finished_at_ms)
        now_ms = self._clock_ms()

        key = TrialBattleKey(
            battle_id=battle_id,
            guild_id=guild_id,
            activity_id=activity_id,
            period_key=period_k,
            guild_period_key=guild_period_k,
            initiator_player_id=initiator_id,
            launch_request_id=launch_req_id,
        )

        # 提取队伍 0 参战玩家并剔除逃跑者（阵亡玩家仍保留领奖资格）
        fled = {int(pid) for pid in event.fled_player_ids}
        candidates: list[int] = []
        seen: set[int] = set()
        for team in event.teams:
            if int(team.team_index) != 0:
                continue
            for raw_pid in team.player_ids:
                pid = int(raw_pid)
                if pid > 0 and pid not in fled and pid not in seen:
                    seen.add(pid)
                    candidates.append(pid)

        win = int(event.outcome) == BATTLE_OUTCOME_SIDE_A_WIN
        rule = next(
            (
                r
                for r in self.store.catalog.activities
                if r.activity_id == activity_id and r.activity_type == ACTIVITY_TYPE_TRIAL
            ),
            None,
        )

        reward_items = self.store.catalog.reward_items(rule.reward_id) if (win and rule is not None) else ()
        reward_payload = build_trial_reward_payload(self.protocol, reward_items) if reward_items else b""
        reward_ops: list[TrialRewardOp] = []
        if win and reward_payload:
            for pid in sorted(candidates):
                op_id = await self.store.allocate_op_id()
                lease_token = secrets.randbits(64) | 1
                reward_ops.append(TrialRewardOp(player_id=pid, op_id=op_id, lease_token=lease_token))

        lease_until_ms = now_ms + self._asset_op_lease_ms
        try:
            res = await self.store.settle_trial_battle(
                battle=key,
                row=rule,
                win=win,
                candidates=candidates,
                reward_payload=reward_payload,
                reward_ops=reward_ops,
                lease_until_ms=lease_until_ms,
                finished_at_ms=finished_at_ms,
                now_ms=now_ms,
            )
        except (TrialInputInvalidError, TrialActivityPoisonError) as exc:
            logger.error("guild trial battle settle poison battle=%d err=%s", battle_id, exc)
            _outcome, rel_req, rel_players = await self.store.mark_trial_battle_poison(key, finished_at_ms, now_ms)
            if rel_req != 0:
                await self._release_occupancy_best_effort(rel_req, rel_players)
            return TrialSettleResult(
                status="poison",
                release_request_id=rel_req,
                release_players=rel_players,
            )

        if res.release_request_id != 0:
            await self._release_occupancy_best_effort(res.release_request_id, res.release_players)

        # 即时投递已入队的奖励指令（本事务已为其预置租约）
        if self._asset_loop is not None and res.enqueued and reward_payload:
            bundle = self.protocol.parse("AssetBundle", reward_payload)
            for enq in res.enqueued:
                pending = PendingAssetOp(
                    op_id=enq.op_id,
                    player_id=enq.player_id,
                    guild_id=guild_id,
                    stream=ASSET_STREAM_GUILD_CREDIT,
                    seq=enq.seq,
                    stream_epoch=enq.stream_epoch,
                    correlation_id=enq.op_id,
                    tx_type=TX_GUILD_ACTIVITY_REWARD,
                    kind=OP_KIND_ACTIVITY_REWARD,
                    bundle=bundle,
                    attempts=0,
                    deadline_ms=0,
                    last_reason=0,
                    lease_token=enq.lease_token,
                    created_ms=now_ms,
                    last_send_ms=now_ms,
                )
                try:
                    await self._asset_loop.process_one(pending)
                except Exception as exc:
                    logger.warning("guild trial reward sync delivery failed op_id=%d err=%s", enq.op_id, exc)

        if res.status == "settled" and (res.rewarded or res.funds_granted or res.release_players):
            recipients = sorted(set(res.rewarded) | set(res.release_players))
            await self._notify_activity(guild_id, initiator_id, recipients)
        return res

    # -- 视图查询 --------------------------------------------------------------------------------

    async def get_lobby_view_for_player(
        self, player_id: int, guild_id: int, activity_id: int, now_ms: int
    ) -> Message | None:
        """读取玩家当前可见的历练大厅视图；已超时但尚未被后台扫描关闭的 PENDING 视图按 ENDED 展示。"""
        lob = await self.store.get_player_trial_lobby(player_id)
        if lob is None or lob.guild_id != guild_id or lob.activity_id != activity_id:
            return None
        try:
            view = self.protocol.parse("guildpb.GuildTrialLobbyView", lob.view_proto)
        except DecodeError:
            return None
        if int(view.state) == TRIAL_LOBBY_STATE_PENDING and int(view.expire_at_ms) <= now_ms:
            view.state = TRIAL_LOBBY_STATE_ENDED
            view.end_tip_id = ERR_GUILD_TRIAL_INVITE_EXPIRED
        return view

    # -- 后台对账与清理循环 ----------------------------------------------------------------------

    async def expire_pending_lobbies_once(self, limit: int = 50) -> int:
        now_ms = self._clock_ms()
        released = await self.store.expire_pending_trial_lobbies(now_ms, limit=limit)
        for lobby_id, pids in released:
            await self._release_occupancy_best_effort(lobby_id, pids)
        return len(released)

    async def recover_launches_once(self, limit: int = 20) -> int:
        now_ms = self._clock_ms()
        claimed = await self.store.claim_due_trial_launches(now_ms, limit=limit)
        processed = 0
        for rec in claimed:
            try:
                launch_orig = self.protocol.parse("guildpb.GuildTrialLaunchOriginal", rec.original_proto)
            except DecodeError:
                logger.error("guild trial launch corrupt original_proto request=%d", rec.request_id)
                continue
            start_req = build_start_activity_battle_request(self.protocol, launch_orig)
            await self._reconcile_prepared_launch(launch_orig, start_req, now_ms)
            processed += 1
        return processed

    async def convert_owed_rewards_once(self, limit: int = 50) -> int:
        now_ms = self._clock_ms()
        rows = await self.store.list_trial_rewards_owed(limit=limit)
        converted = 0
        for owed in rows:
            op_id = await self.store.allocate_op_id()
            status, op_row = await self.store.convert_owed_reward(owed, op_id, now_ms)
            if status == "converted" and op_row is not None:
                converted += 1
                if self._asset_loop is not None:
                    try:
                        await self._asset_loop.deliver_now(op_row.op_id)
                    except Exception as exc:
                        logger.warning("guild trial owed reward delivery failed op_id=%d err=%s", op_row.op_id, exc)
        return converted

    async def sweep_overdue_battles_once(
        self,
        *,
        overdue_ms: int | None = None,
        abandon_ms: int | None = None,
        limit: int = 100,
    ) -> int:
        """向 match.GetBattleCompletion 补查超时未结算的对局（Kafka 丢消息兜底）。"""
        now_ms = self._clock_ms()
        ov_ms = self._battle_overdue_ms if overdue_ms is None else overdue_ms
        ab_ms = self._battle_abandon_ms if abandon_ms is None else abandon_ms
        rows = await self.store.list_overdue_trial_battles(now_ms - ov_ms, limit=limit)
        handled = 0
        for rec in rows:
            try:
                reject, decision, raw_payload = await self.match_client.get_battle_completion(rec.battle_id)
            except TrialMatchUnavailableError as exc:
                logger.warning("guild trial GetBattleCompletion failed battle=%d err=%s", rec.battle_id, exc)
                continue
            if reject != TRIAL_OCCUPANCY_REJECT_NONE:
                continue
            if decision == OUTCOME_DECISION_COMPLETE and raw_payload:
                try:
                    ev = self.protocol.parse("contracts.kafka.BattleResultEvent", raw_payload)
                except DecodeError:
                    logger.error("guild trial completion raw_payload corrupt battle=%d", rec.battle_id)
                    continue
                await self.handle_battle_result_event(ev)
                handled += 1
            elif decision == OUTCOME_DECISION_ABORT or (now_ms - rec.created_ms) >= ab_ms:
                expired = await self.store.expire_trial_battle(rec.battle_id, rec.guild_id, now_ms)
                if expired:
                    handled += 1
                if rec.launch_request_id != 0:
                    launch = await self.store.get_trial_launch(rec.launch_request_id)
                    if launch is not None:
                        orig = self.protocol.parse("guildpb.GuildTrialLaunchOriginal", launch.original_proto)
                        pids = [int(r.player_id) for r in orig.roster]
                        cancel_receipt = self.protocol.message(
                            "match.CancelActivityBattleResponse",
                            battle_id=rec.battle_id,
                            formation_state=_FORMATION_STATE_TERMINAL,
                            terminal_decision=OUTCOME_DECISION_ABORT,
                        )
                        released, _ = await self.store.finish_cancelled_trial_launch(
                            rec.launch_request_id, cancel_receipt, now_ms
                        )
                        if released:
                            await self._release_occupancy_best_effort(rec.launch_request_id, pids)
        return handled

    async def tick(self) -> None:
        await self.expire_pending_lobbies_once()
        await self.recover_launches_once()
        await self.convert_owed_rewards_once()
        await self.sweep_overdue_battles_once()

    async def run(self, *, stop_event: asyncio.Event | None = None) -> None:
        while stop_event is None or not stop_event.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("guild trial background tick failed", exc_info=True)
            if stop_event is None:
                await asyncio.sleep(self._sweep_interval_sec)
            else:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self._sweep_interval_sec)
                except asyncio.TimeoutError:
                    pass
