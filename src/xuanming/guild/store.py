"""guild 核心存储引擎（GuildStoreCore）、外部服务适配与确定性内存双胞胎（MemoryGuildStore）。"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import logging
import secrets
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from typing import Protocol as TypingProtocol, TypeVar

from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import sign_service_call
from ..protocol import Protocol
from .errors import (
    AssetOpLeaseLostError,
    AssetOpPoisonRowError,
    ERR_GUILD_TRIAL_BATTLE_ABORTED,
    ERR_GUILD_TRIAL_INVITE_DECLINED,
    ERR_GUILD_TRIAL_INVITE_EXPIRED,
    ERR_GUILD_TRIAL_SERVICE_BUSY,
    GuildActivityAlreadyClaimedError,
    GuildActivityJoinTooRecentError,
    GuildActivityLevelTooLowError,
    GuildActivityOverflowError,
    GuildActivityThresholdNotReachedError,
    GuildAlreadyInGuildError,
    GuildApplicationLimitError,
    GuildApplicationNotFoundError,
    GuildApplicationQueueFullError,
    GuildAssetPendingError,
    GuildClientRequestReplayedError,
    GuildConfigMissingError,
    GuildContributionInsufficientError,
    GuildDonateLimitError,
    GuildFullError,
    GuildFundsInsufficientError,
    GuildLeaderCantLeaveError,
    GuildLeaderMismatchError,
    GuildMaxLevelError,
    GuildNameTakenError,
    GuildNoPermissionError,
    GuildNotFoundError,
    GuildNotInGuildError,
    GuildOfficerLimitError,
    GuildRankTooLowError,
    GuildRenameCooldownError,
    GuildRenameSameNameError,
    GuildShopLevelTooLowError,
    GuildShopLimitError,
    GuildTargetNotMemberError,
    GuildTextCheckUnavailableError,
    GuildWriteConflictError,
    GuildZoneMismatchError,
    InvalidRoleFenceStateError,
    RoleFenceStaleError,
    RoleFenceUnavailableError,
    RoleNotFoundError,
    StoreUnavailableError,
    TrialActivityPoisonError,
    TrialInputInvalidError,
)
from .model import (
    ACTIVITY_TYPE_LANTERN,
    ACTIVITY_TYPE_REUNION,
    ACTIVITY_TYPE_TRIAL,
    ASSET_OP_FRESH_ATTEMPT_LIMIT,
    ASSET_OP_MAX_PENDING,
    ASSET_OP_MAX_SPAN,
    ASSET_STREAM_GUILD_CREDIT,
    ASSET_STREAM_GUILD_DEBIT,
    ActivityProgress,
    ActivityRewardStatus,
    ActivityRuleRow,
    ActivityTxResult,
    ApplicantRow,
    ApplicationRow,
    ApplyResult,
    AssetOpRow,
    COUNTER_KIND_ACTIVITY,
    COUNTER_KIND_DONATE,
    COUNTER_KIND_SHOP,
    DisbandResult,
    FinalizedAssetOp,
    GuildData,
    GuildTableCatalog,
    MAX_APPLIED_OPS_PAGE_LIMIT,
    MAX_APPLIED_OPS_PLAYER_IDS,
    MAX_RANK_VERSION,
    MAX_TRIAL_LOBBY_ROSTER,
    MIN_TRIAL_LOBBY_ROSTER,
    MS_PER_DAY,
    MemberData,
    MemberWriteResult,
    MergedZones,
    OP_KIND_ACTIVITY_REWARD,
    OP_KIND_DONATE,
    OP_KIND_SHOP,
    OP_STATUS_ABANDONED,
    OP_STATUS_ABORTED,
    OP_STATUS_APPLIED,
    OP_STATUS_APPLIED_PARTIAL,
    OP_STATUS_PENDING,
    OP_STATUS_REJECTED,
    PendingAssetOp,
    PrepareFreezeOutcome,
    REJECTED_REWARD_WINDOW_MS,
    ROLE_BLOCKER_ASSET_OP,
    ROLE_BLOCKER_GUILD_LEADER,
    ROLE_BLOCKER_GUILD_MEMBER,
    ROLE_BLOCKER_TRIAL_LAUNCH_ROLE,
    ROLE_BLOCKER_TRIAL_LOBBY_SLOT,
    ROLE_BLOCKER_TRIAL_REWARD_OWED,
    ROLE_BLOCKER_WAR_CREDIT_OWED,
    ROLE_FENCE_CALLER,
    ROLE_LEADER,
    ROLE_MEMBER,
    ROLE_OFFICER,
    ReviewResult,
    RoleFenceRow,
    RoleOriginal,
    RolePurgeResult,
    CreditFenceVerdict,
    WarCreditOwed,
    WarCreditOwedCursor,
    WarOwedConvertStatus,
    classify_credit_fence,
    TEXT_VERDICT_MASKED,
    TEXT_VERDICT_PASS,
    TRIAL_BATTLE_STATE_EXPIRED,
    TRIAL_BATTLE_STATE_SETTLED,
    TRIAL_BATTLE_STATE_STARTED,
    TRIAL_LAUNCH_ROLE_ACTIVE,
    TRIAL_LAUNCH_ROLE_RELEASED,
    TRIAL_LAUNCH_STATE_LAUNCHED,
    TRIAL_LAUNCH_STATE_PREPARED,
    TRIAL_LAUNCH_STATE_TERMINAL,
    TRIAL_LOBBY_STATE_ENDED,
    TRIAL_LOBBY_STATE_LAUNCHED,
    TRIAL_LOBBY_STATE_LAUNCHING,
    TRIAL_LOBBY_STATE_PENDING,
    TRIAL_OCCUPANCY_MARGIN_MS,
    TRIAL_SETTLE_RESULT_CONFIG_MISSING,
    TRIAL_SETTLE_RESULT_LOSS,
    TRIAL_SETTLE_RESULT_POISON,
    TRIAL_SETTLE_RESULT_UNSPECIFIED,
    TRIAL_SETTLE_RESULT_WIN,
    TX_GUILD_ACTIVITY_REWARD,
    TX_GUILD_DONATE,
    TX_GUILD_SHOP,
    TrialBattleKey,
    TrialBattleRecord,
    TrialEnqueuedReward,
    TrialLaunchRecord,
    TrialLaunchRoleRecord,
    TrialLobbyRecord,
    TrialLobbySlotRecord,
    TrialOwedRewardRecord,
    TrialRewardOp,
    TrialSettleResult,
    UINT64_MAX,
    UpgradeResult,
    better_successor_key,
    can_assign_role,
    can_kick,
    can_review_applications,
    can_set_announcement,
    can_transfer_leader,
    can_upgrade_guild,
    check_role_fence,
    day_key,
    demoted_leader_role,
    guild_name_norm,
    guild_period_key,
    is_assignable_role,
    is_removal_token,
    joined_long_enough,
    lantern_outcome,
    reunion_outcome,
    role_fence_from_proto,
    role_rank,
    RANK_LEADER,
    trial_funds_counted,
    valid_role_fence_state,
)

logger = logging.getLogger(__name__)

_DATA_SERVICE_PREFIX = "/data_service.DataService/"
_CONTENT_GUARD_PREFIX = "/content_guard.ContentGuard/"
_ROLE_FENCE_TIMEOUT_SEC = 2.0
_ID_ALLOC_TIMEOUT_SEC = 2.0
_HOME_ZONE_TIMEOUT_SEC = 1.5
_NAME_LOOKUP_TIMEOUT_SEC = 0.8
_CONTENT_GUARD_TIMEOUT_SEC = 1.0

T = TypeVar("T")

# 人工终结审计列长度上限（与 Go asset_store.go resolvedByMaxRunes / resolveReasonMaxRunes 对齐）
RESOLVED_BY_MAX_RUNES = 64
RESOLVE_REASON_MAX_RUNES = 191


@dataclass(frozen=True, slots=True)
class QuietWitness:
    """人工终结前最后一次读行时的静默见证（与 Go assetop.QuietWitness 对齐：租约、发送时刻与 attempts）。"""

    lease_until_ms: int
    last_send_ms: int
    attempts: int


@dataclass(frozen=True, slots=True)
class ManualResolution:
    """帮会资产指令人工终结请求（与 Go assetop.ManualResolution 对齐）。"""

    op_id: int
    final_status: int
    operator: str
    reason: str


def _validate_manual_resolution(res: ManualResolution) -> None:
    if res.final_status not in (OP_STATUS_APPLIED, OP_STATUS_REJECTED, OP_STATUS_ABORTED, OP_STATUS_ABANDONED):
        raise ValueError(f"manual resolve op {res.op_id}: status {res.final_status} is not terminal")
    if not res.operator.strip() or len(res.operator) > RESOLVED_BY_MAX_RUNES:
        raise ValueError(f"manual resolve op {res.op_id}: operator must be 1..{RESOLVED_BY_MAX_RUNES} chars")
    if len(res.reason) > RESOLVE_REASON_MAX_RUNES:
        raise ValueError(f"manual resolve op {res.op_id}: reason exceeds {RESOLVE_REASON_MAX_RUNES} chars")


class _TxCommitThen(Exception):
    """在事务块内提交已完成写入并返回指定业务异常的内部控制流信号（用于惰性清理过期申请）。"""

    def __init__(self, inner: Exception) -> None:
        super().__init__(str(inner))
        self.inner = inner


class _TxRollbackReturn(Exception):
    """在事务块内显式回滚并返回结果的内部控制流信号。"""

    def __init__(self, value) -> None:
        super().__init__("rollback_return")
        self.value = value


class _DuplicateKeyError(Exception):
    """唯一键或主键冲突（对应 MySQL 1062）。"""

    def __init__(self, message: str = "", key_name: str = "") -> None:
        super().__init__(message)
        self.key_name = key_name


# -- 外部依赖协议与 gRPC 适配器 -----------------------------------------------------------------


class RoleFenceDirectory(TypingProtocol):
    async def get_role_fence_state(self, player_id: int) -> Message | None: ...


class HomeZoneDirectory(TypingProtocol):
    async def get_home_zone(self, player_id: int) -> int: ...


class MergedZoneDirectory(TypingProtocol):
    async def get_merged_zones(self) -> MergedZones: ...


class PlayerNameResolver(TypingProtocol):
    async def batch_get_names(self, player_ids: Sequence[int]) -> dict[int, str]: ...


class OnlineStatusResolver(TypingProtocol):
    async def batch_online(self, player_ids: Sequence[int], *, strict: bool = False) -> dict[int, bool]: ...


class ContentGuardChecker(TypingProtocol):
    async def check_text(self, scene: int, player_id: int, text: str) -> int: ...


class GuildNotifier(TypingProtocol):
    async def notify(
        self, kind: int, guild_id: int, actor_player_id: int, target_player_id: int, recipients: Sequence[int]
    ) -> None: ...


class IdSegmentAllocator:
    """经 data_service.DataService/AllocateIdSegment 批量领取递增号段（biz_tag=guild / guild_asset_op）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        biz_tag: str,
        *,
        step: int = 1000,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._biz_tag = biz_tag
        self._step = step
        self._lock = asyncio.Lock()
        self._next = 0
        self._hi = 0

    async def __call__(self) -> int:
        async with self._lock:
            if self._next < self._hi:
                value = self._next
                self._next += 1
                return value
            req = self._protocol.message("data_service.AllocateIdSegmentRequest", biz_tag=self._biz_tag, step=self._step)
            try:
                raw = await self._call(
                    _DATA_SERVICE_PREFIX + "AllocateIdSegment",
                    req.SerializeToString(deterministic=True),
                    timeout=_ID_ALLOC_TIMEOUT_SEC,
                )
            except Exception as exc:
                raise StoreUnavailableError(f"AllocateIdSegment failed tag={self._biz_tag}: {exc}") from exc
            resp = self._protocol.parse("data_service.AllocateIdSegmentResponse", raw)
            if resp.error_code != 0 or not 0 < resp.lo < resp.hi:
                raise StoreUnavailableError(
                    f"AllocateIdSegment rejected tag={self._biz_tag} code={resp.error_code} lo={resp.lo} hi={resp.hi}"
                )
            self._next = int(resp.lo) + 1
            self._hi = int(resp.hi)
            return int(resp.lo)


class GrpcRoleFenceDirectory:
    """经 data_service.DataService/GetRoleFenceState 读取权威护栏状态（caller="guild"）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        role_fence_secret: bytes,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._secret = role_fence_secret
        self._clock_ms = clock_ms

    async def get_role_fence_state(self, player_id: int) -> Message | None:
        path = _DATA_SERVICE_PREFIX + "GetRoleFenceState"
        req = self._protocol.message("data_service.GetRoleFenceStateRequest", player_id=player_id)
        sign_service_call(path, req, ROLE_FENCE_CALLER, self._clock_ms(), self._secret)
        try:
            raw = await self._call(path, req.SerializeToString(deterministic=True), timeout=_ROLE_FENCE_TIMEOUT_SEC)
        except Exception as exc:
            raise RoleFenceUnavailableError(f"GetRoleFenceState failed player={player_id}: {exc}") from exc
        resp = self._protocol.parse("data_service.GetRoleFenceStateResponse", raw)
        if resp.retry:
            raise RoleFenceUnavailableError(f"GetRoleFenceState requested retry player={player_id}")
        if not resp.found:
            return None
        return resp.state


class GrpcHomeZoneDirectory:
    """经 data_service.DataService/GetPlayerHomeZone 查询玩家归属区。"""

    def __init__(self, protocol: Protocol, call: Callable[..., Awaitable[bytes]]) -> None:
        self._protocol = protocol
        self._call = call

    async def get_home_zone(self, player_id: int) -> int:
        req = self._protocol.message("data_service.GetPlayerHomeZoneRequest", player_id=player_id)
        raw = await self._call(
            _DATA_SERVICE_PREFIX + "GetPlayerHomeZone",
            req.SerializeToString(deterministic=True),
            timeout=_HOME_ZONE_TIMEOUT_SEC,
        )
        resp = self._protocol.parse("data_service.GetPlayerHomeZoneResponse", raw)
        return int(resp.home_zone_id)


class GrpcMergedZoneDirectory:
    """经 data_service.DataService/GetMergedZones 加载合服映射。"""

    def __init__(self, protocol: Protocol, call: Callable[..., Awaitable[bytes]]) -> None:
        self._protocol = protocol
        self._call = call

    async def get_merged_zones(self) -> MergedZones:
        req = self._protocol.message("data_service.GetMergedZonesRequest")
        raw = await self._call(
            _DATA_SERVICE_PREFIX + "GetMergedZones",
            req.SerializeToString(deterministic=True),
            timeout=_HOME_ZONE_TIMEOUT_SEC,
        )
        resp = self._protocol.parse("data_service.GetMergedZonesResponse", raw)
        return MergedZones({int(k): int(v) for k, v in resp.merged_into.items()})


class GrpcPlayerNameResolver:
    """经 data_service.DataService/BatchGetPlayerName 批量读取展示名（fail-open）。"""

    def __init__(self, protocol: Protocol, call: Callable[..., Awaitable[bytes]]) -> None:
        self._protocol = protocol
        self._call = call

    async def batch_get_names(self, player_ids: Sequence[int]) -> dict[int, str]:
        unique = sorted({int(pid) for pid in player_ids if int(pid) > 0})
        if not unique:
            return {}
        req = self._protocol.message("data_service.BatchGetPlayerNameRequest", player_ids=unique)
        try:
            raw = await self._call(
                _DATA_SERVICE_PREFIX + "BatchGetPlayerName",
                req.SerializeToString(deterministic=True),
                timeout=_NAME_LOOKUP_TIMEOUT_SEC,
            )
            resp = self._protocol.parse("data_service.BatchGetPlayerNameResponse", raw)
            return {int(k): str(v) for k, v in resp.names.items()}
        except Exception as exc:
            logger.warning("guild batch player name lookup failed count=%d err=%s", len(unique), exc)
            return {}


class GrpcContentGuardChecker:
    """经 content_guard.ContentGuard/CheckText 校验帮会名与公告（fail-closed）。"""

    def __init__(self, protocol: Protocol, call: Callable[..., Awaitable[bytes]]) -> None:
        self._protocol = protocol
        self._call = call

    async def check_text(self, scene: int, player_id: int, text: str) -> int:
        req = self._protocol.message(
            "content_guard.CheckTextRequest",
            scene=scene,
            text=text,
            player_id=player_id,
        )
        try:
            raw = await self._call(
                _CONTENT_GUARD_PREFIX + "CheckText",
                req.SerializeToString(deterministic=True),
                timeout=_CONTENT_GUARD_TIMEOUT_SEC,
            )
            resp = self._protocol.parse("content_guard.CheckTextResponse", raw)
            return int(resp.verdict)
        except Exception as exc:
            raise GuildTextCheckUnavailableError(f"content guard check failed: {exc}") from exc


class NullPlayerNameResolver:
    async def batch_get_names(self, player_ids: Sequence[int]) -> dict[int, str]:
        return {}


class NullOnlineStatusResolver:
    async def batch_online(self, player_ids: Sequence[int], *, strict: bool = False) -> dict[int, bool]:
        return {}


class NullContentGuardChecker:
    async def check_text(self, scene: int, player_id: int, text: str) -> int:
        return TEXT_VERDICT_PASS


class NullGuildNotifier:
    async def notify(
        self, kind: int, guild_id: int, actor_player_id: int, target_player_id: int, recipients: Sequence[int]
    ) -> None:
        return None


def default_id_generator(start: int = 1) -> Callable[[], Awaitable[int]]:
    """进程内单调递增发号器（用于单测或 MemoryGuildStore）。"""
    current = start - 1

    async def _next() -> int:
        nonlocal current
        current += 1
        return current

    return _next


def _random_hold_id() -> int:
    return (secrets.randbits(63) or 1)


def _new_lease_token() -> int:
    return secrets.randbits(64) | 1


# -- 单事务原语协议（_MemoryTx 与 _SqlTx 共同实现） ---------------------------------------------


@dataclass(frozen=True, slots=True)
class _GuildLockRow:
    guild_id: int
    name: str
    name_norm: str
    leader_id: int
    level: int
    max_members: int
    zone_id: int
    funds: int
    score: int
    score_version: int
    renamed_ms: int


class GuildTx(TypingProtocol):
    """单事务内底层存储原语协议（SqlGuildStore 与 MemoryGuildStore 共享同一业务核心）。"""

    # role_fence
    async def get_role_fence(self, player_id: int) -> RoleFenceRow | None: ...
    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None: ...
    async def apply_role_fence(self, state: RoleFenceRow, now_ms: int) -> bool: ...
    async def role_fence_pending_blocker(self, state: RoleFenceRow, *, dry_run: bool) -> str: ...

    # guild_player_state
    async def ensure_player_state(self, player_id: int, now_ms: int) -> None: ...
    async def lock_player_state(self, player_id: int) -> bool: ...
    async def delete_player_state(self, player_id: int) -> None: ...

    # guild
    async def load_guild(self, guild_id: int) -> GuildData | None: ...
    async def lock_guild(self, guild_id: int) -> _GuildLockRow | None: ...
    async def get_guild_level(self, guild_id: int) -> int | None: ...
    async def insert_guild(self, guild: GuildData, name_norm: str) -> None: ...
    async def update_guild_announcement(self, guild_id: int, announcement: str) -> None: ...
    async def update_guild_name(self, guild_id: int, name: str, name_norm: str) -> None: ...
    async def deduct_guild_funds_and_stamp_rename(self, guild_id: int, cost_funds: int, now_ms: int) -> bool: ...
    async def update_guild_leader(self, guild_id: int, new_leader_id: int) -> None: ...
    async def update_guild_score(self, guild_id: int, score: int, new_version: int) -> None: ...
    async def upgrade_guild_row(
        self, guild_id: int, cur_level: int, new_level: int, cost_funds: int, next_max_members: int
    ) -> bool: ...
    async def set_guild_funds(self, guild_id: int, new_funds: int) -> None: ...
    async def credit_guild_funds(self, guild_id: int, delta: int) -> bool: ...
    async def delete_guild(self, guild_id: int) -> bool: ...
    async def list_guild_zones(self) -> list[int]: ...
    async def list_rank_rows(self) -> list[tuple[int, int, int, int]]: ...

    # guild_name_hold
    async def claim_name_hold(self, guild_id: int, name_norm: str, now_ms: int) -> bool: ...
    async def upsert_name_hold(self, guild_id: int, old_norm: str, held_until_ms: int, hold_id: int) -> None: ...

    # guild_member
    async def get_player_guild_id(self, player_id: int) -> int: ...
    async def get_member_role(self, guild_id: int, player_id: int) -> int | None: ...
    async def get_member_contribution(self, guild_id: int, player_id: int) -> tuple[int, int] | None: ...
    async def lock_member(self, guild_id: int, player_id: int) -> MemberData | None: ...
    async def list_member_ids(self, guild_id: int) -> list[int]: ...
    async def list_members(self, guild_id: int) -> list[MemberData]: ...
    async def count_members(self, guild_id: int) -> int: ...
    async def count_members_by_role(self, guild_id: int, role: int) -> int: ...
    async def insert_member(self, guild_id: int, member: MemberData) -> None: ...
    async def update_member_role(self, guild_id: int, player_id: int, role: int) -> None: ...
    async def set_member_contribution(self, guild_id: int, player_id: int, total: int, balance: int) -> None: ...
    async def credit_member_contribution(self, guild_id: int, player_id: int, delta: int) -> bool: ...
    async def refund_member_contribution(self, guild_id: int, player_id: int, delta: int) -> bool: ...
    async def debit_member_contribution(self, guild_id: int, player_id: int, cost: int) -> bool: ...
    async def delete_member(self, guild_id: int, player_id: int) -> bool: ...
    async def get_trial_roster_members(self, guild_id: int, player_ids: Sequence[int]) -> dict[int, int]: ...

    # guild_application
    async def lock_application_expire(self, guild_id: int, player_id: int) -> int | None: ...
    async def insert_application(self, guild_id: int, player_id: int, apply_ms: int, expire_ms: int) -> bool: ...
    async def refresh_application(self, guild_id: int, player_id: int, apply_ms: int, expire_ms: int) -> None: ...
    async def delete_application(self, guild_id: int, player_id: int) -> bool: ...
    async def delete_expired_application(self, guild_id: int, player_id: int, now_ms: int) -> bool: ...
    async def delete_live_application(self, guild_id: int, player_id: int, now_ms: int) -> bool: ...
    async def delete_applications_of_player(self, player_id: int) -> None: ...
    async def delete_applications_of_guild(self, guild_id: int) -> None: ...
    async def purge_expired_applications_of_player(self, player_id: int, now_ms: int) -> None: ...
    async def count_pending_applications_of_player(self, player_id: int, now_ms: int) -> int: ...
    async def count_live_applications_of_guild(self, guild_id: int, now_ms: int) -> int: ...
    async def list_my_applications(self, player_id: int, now_ms: int, limit: int) -> list[ApplicationRow]: ...
    async def list_applicants(self, guild_id: int, now_ms: int, limit: int) -> list[ApplicantRow]: ...

    # guild_player_op_seq / guild_asset_op / guild_daily_counter / guild_client_request
    async def ensure_and_lock_seq(self, player_id: int, stream: int, now_ms: int) -> tuple[int, int]: ...
    async def lock_seq_guard(self, player_id: int, stream: int) -> bool: ...
    async def allocate_seq(self, player_id: int, stream: int, now_ms: int) -> tuple[int, int]: ...
    async def insert_asset_op(self, op: AssetOpRow) -> None: ...
    async def get_asset_op(self, op_id: int) -> AssetOpRow | None: ...
    async def lock_asset_op(self, op_id: int) -> AssetOpRow | None: ...
    async def list_due_asset_ops(
        self, now_ms: int, limit: int, owned_shards: frozenset[int] | None = None
    ) -> list[int]: ...
    async def claim_asset_op(
        self, op_id: int, now_ms: int, lease_until_ms: int, lease_token: int
    ) -> AssetOpRow | None: ...
    async def mark_asset_op_poison(self, op_id: int, lease_token: int, poison_until_ms: int, now_ms: int) -> None: ...
    async def reschedule_asset_op(
        self,
        op_id: int,
        lease_token: int,
        next_attempt_ms: int,
        outcome: int,
        reason: int,
        durable: bool,
        local: bool,
        now_ms: int,
    ) -> bool: ...
    async def finalize_asset_op_cas(
        self,
        op_id: int,
        final_status: int,
        last_outcome: int,
        last_reason: int,
        reason_tip_id: int,
        now_ms: int,
    ) -> bool: ...
    async def abandon_asset_op_cas(self, op_id: int, lease_token: int, now_ms: int) -> bool: ...
    async def resolve_asset_op_manual_cas(
        self,
        op_id: int,
        final_status: int,
        operator: str,
        reason: str,
        now_ms: int,
        witness: QuietWitness | None = None,
    ) -> bool: ...
    async def list_pending_ops(self, player_id: int, stream: int, limit: int) -> list[AssetOpRow]: ...
    async def list_recent_ops(self, player_id: int, stream: int, limit: int) -> list[AssetOpRow]: ...
    async def accelerate_donation_deadlines(self, guild_id: int, player_ids: Sequence[int], now_ms: int) -> None: ...
    async def get_client_request_op_id(self, player_id: int, client_request_id: int) -> int | None: ...
    async def insert_client_request(self, player_id: int, client_request_id: int, op_id: int, now_ms: int) -> None: ...
    async def upsert_counter_with_limit(
        self, player_id: int, counter_kind: int, ref_id: int, period_key_val: int, delta: int, limit: int, now_ms: int
    ) -> bool: ...
    async def refund_counter(
        self, player_id: int, counter_kind: int, ref_id: int, period_key_val: int, delta: int, now_ms: int
    ) -> None: ...
    async def get_counter_usage(
        self, player_id: int, counter_kind: int, period_keys: Sequence[int]
    ) -> dict[tuple[int, int], int]: ...
    async def get_activity_counter_value(self, player_id: int, activity_id: int, period_key_val: int) -> int: ...
    async def list_applied_asset_ops_since(
        self,
        player_ids: Sequence[int],
        since_ms: int,
        after_op_id: int,
        limit: int,
        zones: Sequence[int],
    ) -> tuple[list[AssetOpRow], int]: ...

    # guild_activity_progress / guild_trial_* / purge
    async def get_activity_progress(
        self, guild_id: int, keys: Sequence[tuple[int, int]]
    ) -> dict[tuple[int, int], ActivityProgress]: ...
    async def upsert_and_lock_activity_progress(
        self, guild_id: int, activity_id: int, period_key_val: int, *, increment: bool, now_ms: int
    ) -> ActivityProgress: ...
    async def latch_activity_progress(
        self,
        guild_id: int,
        activity_id: int,
        period_key_val: int,
        threshold_reached_ms: int,
        funds_granted: bool,
        now_ms: int,
    ) -> None: ...
    async def set_trial_progress_count(
        self, guild_id: int, activity_id: int, period_key_val: int, count: int, now_ms: int
    ) -> None: ...
    async def delete_activity_progress_of_guild(self, guild_id: int) -> None: ...
    async def delete_started_trial_battles_of_guild(self, guild_id: int) -> None: ...
    async def get_activity_reward_statuses(self, player_id: int, now_ms: int) -> dict[int, ActivityRewardStatus]: ...
    async def purge_player_personal_tables(self, player_id: int) -> None: ...

    # guild_trial_lobby / slot / launch / launch_role / battle / reward_owed
    async def get_trial_lobby(self, lobby_id: int) -> TrialLobbyRecord | None: ...
    async def lock_trial_lobby(self, lobby_id: int) -> TrialLobbyRecord | None: ...
    async def insert_trial_lobby(self, rec: TrialLobbyRecord) -> None: ...
    async def update_trial_lobby(
        self,
        lobby_id: int,
        state: int,
        view_proto: bytes,
        consent_proto: bytes,
        launch_deadline_ms: int,
        battle_id: int,
        updated_ms: int,
    ) -> None: ...
    async def list_expired_pending_lobbies(self, now_ms: int, limit: int) -> list[TrialLobbyRecord]: ...
    async def get_trial_lobby_slot(self, player_id: int) -> TrialLobbySlotRecord | None: ...
    async def lock_trial_lobby_slot(self, player_id: int) -> TrialLobbySlotRecord | None: ...
    async def upsert_trial_lobby_slot(self, player_id: int, lobby_id: int, cooldown_until_ms: int) -> None: ...
    async def get_trial_launch(self, request_id: int) -> TrialLaunchRecord | None: ...
    async def lock_trial_launch(self, request_id: int) -> TrialLaunchRecord | None: ...
    async def get_trial_launch_by_battle(self, battle_id: int) -> TrialLaunchRecord | None: ...
    async def insert_trial_launch(self, rec: TrialLaunchRecord) -> None: ...
    async def update_trial_launch(
        self,
        request_id: int,
        battle_id: int,
        state: int,
        updated_ms: int,
        cancel_receipt: bytes | None = None,
    ) -> None: ...
    async def record_trial_launch_end_tip(self, request_id: int, tip_bytes: bytes) -> None: ...
    async def expedite_trial_launch_check(self, request_id: int, now_ms: int) -> None: ...
    async def list_due_trial_launches(self, now_ms: int, limit: int) -> list[TrialLaunchRecord]: ...
    async def claim_trial_launch(self, request_id: int, prev_next_check_ms: int, new_next_check_ms: int) -> bool: ...
    async def insert_trial_launch_role(self, rec: TrialLaunchRoleRecord) -> None: ...
    async def lock_trial_launch_role(self, player_id: int, request_id: int) -> TrialLaunchRoleRecord | None: ...
    async def has_active_trial_launch_role(self, player_id: int, exclude_request_id: int = 0) -> bool: ...
    async def update_trial_launch_role_state(self, player_id: int, request_id: int, state: int) -> None: ...
    async def get_trial_battle(self, battle_id: int) -> TrialBattleRecord | None: ...
    async def lock_trial_battle(self, battle_id: int) -> TrialBattleRecord | None: ...
    async def insert_trial_battle(self, rec: TrialBattleRecord) -> None: ...
    async def update_trial_battle(
        self, battle_id: int, state: int, settle_result: int, rewarded_count: int, settled_ms: int
    ) -> None: ...
    async def list_overdue_trial_battles(self, created_before_ms: int, limit: int) -> list[TrialBattleRecord]: ...
    async def insert_trial_reward_owed(self, rec: TrialOwedRewardRecord) -> None: ...
    async def delete_trial_reward_owed(self, player_id: int, battle_id: int) -> bool: ...
    async def list_trial_rewards_owed(self, limit: int) -> list[TrialOwedRewardRecord]: ...
    async def count_trial_rewards_owed_by_player(self, player_id: int) -> dict[int, int]: ...
    async def increment_trial_reward_owed_attempts(self, player_id: int, battle_id: int) -> None: ...
    async def insert_war_credit_owed(self, rec: WarCreditOwed) -> None: ...
    async def delete_war_credit_owed(self, player_id: int, op_id: int) -> bool: ...
    async def war_credit_owed_exists(self, player_id: int, op_id: int) -> bool: ...
    async def list_war_credit_owed(self, after: WarCreditOwedCursor, limit: int) -> list[WarCreditOwed]: ...
    async def count_war_credit_owed(self, limit: int) -> int: ...


# -- 核心领域存储引擎（SqlGuildStore 与 MemoryGuildStore 复用全部业务逻辑） ----------------------


class GuildStoreCore:
    """guild 共享事务编排层，不直接绑定 SQL 或内存细节。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        catalog: GuildTableCatalog,
        guild_ids: Callable[[], Awaitable[int]],
        op_ids: Callable[[], Awaitable[int]],
        role_directory: RoleFenceDirectory | None = None,
        merged_zones: MergedZones | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.catalog = catalog
        self._guild_ids = guild_ids
        self._op_ids = op_ids
        self._role_directory = role_directory
        self._merged_zones = merged_zones or MergedZones()
        self._clock_ms = clock_ms
        self.on_asset_finalized: Callable[[FinalizedAssetOp], Awaitable[None]] | None = None

    @property
    def merged_zones(self) -> MergedZones:
        return self._merged_zones

    def set_merged_zones(self, zones: MergedZones) -> None:
        self._merged_zones = zones

    async def _run_tx(self, fn: Callable[[GuildTx], Awaitable[T]], *, read_only: bool = False) -> T:
        raise NotImplementedError

    # -- 角色护栏入口预检与写事务自愈 ------------------------------------------------------------

    async def _heal_role_fence(self, player_id: int) -> bool:
        """从 data_service 读取权威护栏状态并合并到本地副本；找不到返回 False。"""
        if self._role_directory is None:
            raise RoleFenceUnavailableError("role fence directory not configured")
        state = await self._role_directory.get_role_fence_state(player_id)
        if state is None:
            return False
        row = role_fence_from_proto(state)
        if not valid_role_fence_state(row):
            raise RoleFenceUnavailableError(f"invalid authoritative role fence state player={player_id}")
        now_ms = self._clock_ms()
        await self._run_tx(lambda tx: tx.apply_role_fence(row, now_ms))
        return True

    async def verify_role_owner(self, original: RoleOriginal) -> str:
        """OwnershipInterceptor 读预检：返回 'ok' / 'reject' / 'unverified'（不拦冻结态）。"""
        for attempt in range(2):
            fence = await self._run_tx(lambda tx: tx.get_role_fence(original.player_id), read_only=True)
            if fence is not None:
                if fence.account == "":
                    return "reject"
                if fence.owner_revision > original.owner_revision or (
                    fence.owner_revision == original.owner_revision and fence.account != original.account
                ):
                    return "reject"
                if fence.owner_revision == original.owner_revision and fence.account == original.account:
                    return "ok"
            if attempt == 1 or self._role_directory is None:
                return "unverified" if self._role_directory is not None else "ok"
            try:
                found = await self._heal_role_fence(original.player_id)
            except RoleFenceUnavailableError:
                return "unverified"
            if not found:
                return "reject"
        return "unverified"

    async def _lock_and_check_roles(
        self,
        tx: GuildTx,
        actor: RoleOriginal | None,
        extra_targets: Sequence[int] = (),
    ) -> None:
        """写事务开头按 player_id 升序锁定全部涉及角色的 role_fence 副本并校验。"""
        if self._role_directory is None:
            return
        targets: set[int] = {int(pid) for pid in extra_targets if int(pid) > 0}
        if actor is not None and actor.player_id > 0:
            targets.add(actor.player_id)
        for pid in sorted(targets):
            fence = await tx.lock_role_fence(pid)
            if actor is not None and pid == actor.player_id:
                verdict = check_role_fence(fence, actor)
                if verdict == "stale":
                    raise RoleFenceStaleError(pid)
                if verdict == "reject":
                    raise GuildWriteConflictError("role ownership changed or deleted")
            else:
                if fence is None or fence.freeze_token != 0:
                    raise RoleFenceStaleError(pid)
                if fence.account == "":
                    raise GuildWriteConflictError("target role deleted")

    async def _run_write_tx(
        self,
        fn: Callable[[GuildTx], Awaitable[T]],
        *,
        actor: RoleOriginal | None = None,
        extra_targets: Sequence[int] = (),
    ) -> T:
        """带护栏自愈重试的写事务入口（与 Go GuildRepo.inTx 一致）。"""
        for attempt in range(2):
            try:
                async def _wrapped(tx: GuildTx) -> T:
                    await self._lock_and_check_roles(tx, actor, extra_targets)
                    return await fn(tx)

                return await self._run_tx(_wrapped)
            except RoleFenceStaleError as stale:
                if attempt == 1:
                    raise GuildWriteConflictError(f"role fence frozen or stale player={stale.player_id}") from stale
                try:
                    found = await self._heal_role_fence(stale.player_id)
                except RoleFenceUnavailableError as exc:
                    raise GuildWriteConflictError(f"role fence heal unavailable: {exc}") from exc
                if not found:
                    raise GuildWriteConflictError(f"role not found player={stale.player_id}") from stale
        raise GuildWriteConflictError("role fence retry exhausted")

    # -- 基础 CRUD / 信息 ------------------------------------------------------------------------

    async def get_guild(self, guild_id: int) -> GuildData | None:
        return await self._run_tx(lambda tx: tx.load_guild(guild_id), read_only=True)

    async def get_player_guild_id(self, player_id: int) -> int:
        return await self._run_tx(lambda tx: tx.get_player_guild_id(player_id), read_only=True)

    async def get_player_guild(self, player_id: int) -> GuildData | None:
        async def _op(tx: GuildTx) -> GuildData | None:
            gid = await tx.get_player_guild_id(player_id)
            if gid == 0:
                return None
            return await tx.load_guild(gid)

        return await self._run_tx(_op, read_only=True)

    async def count_live_applications(self, guild_id: int, now_ms: int) -> int:
        return await self._run_tx(lambda tx: tx.count_live_applications_of_guild(guild_id, now_ms), read_only=True)

    async def list_guild_zones(self) -> list[int]:
        return await self._run_tx(lambda tx: tx.list_guild_zones(), read_only=True)

    async def list_rank_rows(self) -> list[tuple[int, int, int, int]]:
        return await self._run_tx(lambda tx: tx.list_rank_rows(), read_only=True)

    async def create_guild(
        self,
        actor: RoleOriginal,
        *,
        name: str,
        name_norm: str,
        zone_id: int,
        now_ms: int,
    ) -> GuildData:
        level_cfg = self.catalog.levels.get(1)
        if level_cfg is None:
            raise GuildConfigMissingError("GuildLevel row 1 missing")
        existing = await self.get_player_guild_id(actor.player_id)
        if existing > 0:
            raise GuildAlreadyInGuildError("already in a guild")
        try:
            guild_id = await self._guild_ids()
        except Exception as exc:
            raise StoreUnavailableError(f"mint guild_id failed: {exc}") from exc
        if guild_id <= 0:
            raise StoreUnavailableError("minted guild_id is zero")

        await self._run_tx(lambda tx: tx.ensure_player_state(actor.player_id, now_ms))
        founding_member = MemberData(
            player_id=actor.player_id,
            role=ROLE_LEADER,
            join_time_ms=now_ms,
            last_active_ms=now_ms,
        )
        guild = GuildData(
            guild_id=guild_id,
            name=name,
            leader_id=actor.player_id,
            level=1,
            announcement="",
            create_time_ms=now_ms,
            max_members=level_cfg.max_members,
            zone_id=zone_id,
            score=0,
            funds=0,
            score_version=0,
            renamed_ms=0,
            name_norm=name_norm,
            members=(founding_member,),
        )

        async def _op(tx: GuildTx) -> GuildData:
            if not await tx.lock_player_state(actor.player_id):
                await tx.ensure_player_state(actor.player_id, now_ms)
                await tx.lock_player_state(actor.player_id)
            try:
                await tx.insert_member(guild_id, founding_member)
            except _DuplicateKeyError as exc:
                raise GuildAlreadyInGuildError("already in a guild") from exc
            await tx.delete_applications_of_player(actor.player_id)
            try:
                await tx.insert_guild(guild, name_norm)
            except _DuplicateKeyError as exc:
                raise GuildNameTakenError("guild name taken") from exc
            if not await tx.claim_name_hold(guild_id, name_norm, now_ms):
                raise GuildNameTakenError("guild name held by another guild")
            return guild

        return await self._run_write_tx(_op, actor=actor)

    async def set_announcement(self, actor: RoleOriginal, guild_id: int, announcement: str) -> GuildData:
        async def _op(tx: GuildTx) -> GuildData:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            member = await tx.lock_member(guild_id, actor.player_id)
            if member is None or not can_set_announcement(member.role):
                raise GuildNoPermissionError("no permission to update announcement")
            await tx.update_guild_announcement(guild_id, announcement)
            snap = await tx.load_guild(guild_id)
            if snap is None:
                raise GuildNotFoundError("guild not found")
            return snap

        return await self._run_write_tx(_op, actor=actor)

    async def rename_guild(
        self,
        actor: RoleOriginal,
        *,
        guild_id: int,
        new_name: str,
        new_norm: str,
        now_ms: int,
    ) -> tuple[GuildData, int]:
        cooldown_ms = self.catalog.rule.rename_cooldown_ms
        hold_ms = self.catalog.rule.name_hold_ms
        cost_funds = self.catalog.rule.rename_cost_funds

        async def _op(tx: GuildTx) -> tuple[GuildData, int]:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            member = await tx.lock_member(guild_id, actor.player_id)
            if member is None:
                raise GuildNotInGuildError("not a member of the guild")
            if locked.leader_id != actor.player_id or role_rank(member.role) != RANK_LEADER:
                raise GuildRankTooLowError("only leader can rename guild")
            if new_norm == locked.name_norm:
                raise GuildRenameSameNameError("same guild name")
            if locked.renamed_ms > 0 and cooldown_ms > 0 and now_ms < locked.renamed_ms + cooldown_ms:
                raise GuildRenameCooldownError(locked.renamed_ms + cooldown_ms)
            if locked.funds < cost_funds:
                raise GuildFundsInsufficientError("guild funds insufficient")

            try:
                await tx.update_guild_name(guild_id, new_name, new_norm)
            except _DuplicateKeyError as exc:
                raise GuildNameTakenError("guild name taken") from exc
            if not await tx.claim_name_hold(guild_id, new_norm, now_ms):
                raise GuildNameTakenError("guild name held by another guild")
            if hold_ms > 0 and locked.name_norm:
                await tx.upsert_name_hold(guild_id, locked.name_norm, now_ms + hold_ms, _random_hold_id())
            if not await tx.deduct_guild_funds_and_stamp_rename(guild_id, cost_funds, now_ms):
                raise GuildFundsInsufficientError("guild funds insufficient")
            snap = await tx.load_guild(guild_id)
            if snap is None:
                raise GuildNotFoundError("guild not found")
            next_allowed_ms = now_ms + cooldown_ms if cooldown_ms > 0 else 0
            return snap, next_allowed_ms

        return await self._run_write_tx(_op, actor=actor)

    async def leave_guild(
        self, actor: RoleOriginal, guild_id: int, now_ms: int
    ) -> MemberWriteResult:
        await self._run_tx(lambda tx: tx.ensure_player_state(actor.player_id, now_ms))

        async def _op(tx: GuildTx) -> MemberWriteResult:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            if not await tx.lock_player_state(actor.player_id):
                raise StoreUnavailableError("guild_player_state row missing")
            member = await tx.lock_member(guild_id, actor.player_id)
            if member is None:
                raise GuildNotInGuildError("not a member of the guild")
            if role_rank(member.role) == RANK_LEADER or locked.leader_id == actor.player_id:
                raise GuildLeaderCantLeaveError("leader cannot leave guild")
            if not await tx.delete_member(guild_id, actor.player_id):
                raise GuildNotInGuildError("not a member of the guild")
            await tx.delete_applications_of_player(actor.player_id)
            await tx.accelerate_donation_deadlines(guild_id, (actor.player_id,), now_ms)
            snap = await tx.load_guild(guild_id)
            if snap is None:
                raise GuildNotFoundError("guild not found")
            return MemberWriteResult(guild=snap, changed=True)

        return await self._run_write_tx(_op, actor=actor)

    async def disband_guild(
        self, actor: RoleOriginal, guild_id: int, now_ms: int
    ) -> DisbandResult:
        preread = await self._run_tx(lambda tx: tx.list_member_ids(guild_id), read_only=True)
        for pid in sorted(set(preread)):
            await self._run_tx(lambda tx, p=pid: tx.ensure_player_state(p, now_ms))

        async def _op(tx: GuildTx) -> DisbandResult:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            if locked.leader_id != actor.player_id:
                role = await tx.get_member_role(guild_id, actor.player_id)
                if role is None:
                    raise GuildNotInGuildError("not a member of the guild")
                raise GuildRankTooLowError("not guild leader")
            member_ids = await self._disband_locked_guild(tx, guild_id, now_ms)
            return DisbandResult(zone_id=locked.zone_id, member_ids=tuple(member_ids))

        return await self._run_write_tx(_op, actor=actor)

    async def _disband_locked_guild(self, tx: GuildTx, guild_id: int, now_ms: int) -> list[int]:
        member_ids = sorted(await tx.list_member_ids(guild_id))
        for pid in member_ids:
            if not await tx.lock_player_state(pid):
                await tx.ensure_player_state(pid, now_ms)
                await tx.lock_player_state(pid)
        for pid in member_ids:
            member = await tx.lock_member(guild_id, pid)
            if member is None:
                raise StoreUnavailableError(f"member {pid} vanished under guild lock")
        for pid in member_ids:
            if not await tx.delete_member(guild_id, pid):
                raise StoreUnavailableError(f"failed to delete member {pid} of guild {guild_id}")
        await tx.delete_applications_of_guild(guild_id)
        for pid in member_ids:
            await tx.delete_applications_of_player(pid)
        await tx.accelerate_donation_deadlines(guild_id, member_ids, now_ms)
        await tx.delete_activity_progress_of_guild(guild_id)
        await tx.delete_started_trial_battles_of_guild(guild_id)
        if not await tx.delete_guild(guild_id):
            raise GuildNotFoundError("guild already disbanded")
        return member_ids

    # -- 成员管理 --------------------------------------------------------------------------------

    async def _lock_member_pair(
        self, tx: GuildTx, guild_id: int, actor_id: int, target_id: int
    ) -> tuple[MemberData, MemberData]:
        low, high = min(actor_id, target_id), max(actor_id, target_id)
        low_m = await tx.lock_member(guild_id, low)
        high_m = await tx.lock_member(guild_id, high)
        actor_m, target_m = (low_m, high_m) if actor_id == low else (high_m, low_m)
        if actor_m is None:
            raise GuildNotInGuildError("operator is not a member of the guild")
        if target_m is None:
            raise GuildTargetNotMemberError("target is not a member of the guild")
        return actor_m, target_m

    async def set_member_role(
        self, actor: RoleOriginal, guild_id: int, target_id: int, new_role: int
    ) -> MemberWriteResult:
        if not is_assignable_role(new_role):
            raise GuildNoPermissionError("role not assignable")

        async def _op(tx: GuildTx) -> MemberWriteResult:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            actor_m, target_m = await self._lock_member_pair(tx, guild_id, actor.player_id, target_id)
            if not can_assign_role(actor_m.role) or role_rank(target_m.role) == RANK_LEADER:
                raise GuildRankTooLowError("rank too low")
            if target_m.role == new_role:
                snap = await tx.load_guild(guild_id)
                assert snap is not None
                return MemberWriteResult(guild=snap, changed=False)
            if new_role == ROLE_OFFICER:
                lvl = self.catalog.levels.get(locked.level)
                if lvl is None:
                    raise GuildConfigMissingError(f"GuildLevel row {locked.level} missing")
                officers = await tx.count_members_by_role(guild_id, ROLE_OFFICER)
                if officers >= lvl.max_officers:
                    raise GuildOfficerLimitError("officer limit reached")
            await tx.update_member_role(guild_id, target_id, new_role)
            snap = await tx.load_guild(guild_id)
            assert snap is not None
            return MemberWriteResult(guild=snap, changed=True)

        return await self._run_write_tx(_op, actor=actor)

    async def kick_member(
        self, actor: RoleOriginal, guild_id: int, target_id: int, now_ms: int
    ) -> MemberWriteResult:
        # 预读目标存在性，避免为随机无效 ID 创建孤儿 player_state 行
        target_role = await self._run_tx(lambda tx: tx.get_member_role(guild_id, target_id), read_only=True)
        if target_role is None:
            g = await self.get_guild(guild_id)
            if g is None:
                raise GuildNotFoundError("guild not found")
            if g.member(actor.player_id) is None:
                raise GuildNotInGuildError("not a member of the guild")
            raise GuildTargetNotMemberError("target is not a member")
        await self._run_tx(lambda tx: tx.ensure_player_state(target_id, now_ms))

        async def _op(tx: GuildTx) -> MemberWriteResult:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            if not await tx.lock_player_state(target_id):
                raise StoreUnavailableError("target player state missing")
            actor_m, target_m = await self._lock_member_pair(tx, guild_id, actor.player_id, target_id)
            if not can_kick(actor_m.role, target_m.role):
                raise GuildRankTooLowError("rank too low to kick target")
            if not await tx.delete_member(guild_id, target_id):
                raise GuildTargetNotMemberError("target not member")
            await tx.delete_applications_of_player(target_id)
            await tx.accelerate_donation_deadlines(guild_id, (target_id,), now_ms)
            snap = await tx.load_guild(guild_id)
            assert snap is not None
            return MemberWriteResult(guild=snap, changed=True)

        return await self._run_write_tx(_op, actor=actor)

    async def transfer_leader(
        self, actor: RoleOriginal, guild_id: int, target_id: int
    ) -> MemberWriteResult:
        async def _op(tx: GuildTx) -> MemberWriteResult:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            actor_m, target_m = await self._lock_member_pair(tx, guild_id, actor.player_id, target_id)
            if not can_transfer_leader(actor_m.role):
                raise GuildRankTooLowError("only leader can transfer leadership")
            if locked.leader_id != actor.player_id:
                raise GuildLeaderMismatchError("guild leader_id disagrees with member role")
            lvl = self.catalog.levels.get(locked.level)
            if lvl is None:
                raise GuildConfigMissingError(f"GuildLevel row {locked.level} missing")
            officers = await tx.count_members_by_role(guild_id, ROLE_OFFICER)
            if target_m.role == ROLE_OFFICER and officers > 0:
                officers -= 1
            old_leader_role = demoted_leader_role(officers, lvl.max_officers)
            await tx.update_guild_leader(guild_id, target_id)
            await tx.update_member_role(guild_id, target_id, ROLE_LEADER)
            await tx.update_member_role(guild_id, actor.player_id, old_leader_role)
            if await tx.count_members_by_role(guild_id, ROLE_LEADER) != 1:
                raise GuildLeaderMismatchError("leader count invariant violated")
            snap = await tx.load_guild(guild_id)
            assert snap is not None
            return MemberWriteResult(guild=snap, changed=True)

        return await self._run_write_tx(_op, actor=actor, extra_targets=(target_id,))

    # -- 入帮申请 --------------------------------------------------------------------------------

    async def apply_to_guild(
        self, actor: RoleOriginal, guild_id: int, required_zone: int, now_ms: int
    ) -> ApplyResult:
        rule = self.catalog.rule
        await self._run_tx(lambda tx: tx.ensure_player_state(actor.player_id, now_ms))

        async def _op(tx: GuildTx) -> ApplyResult:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            if required_zone != 0 and not self._merged_zones.same_zone(locked.zone_id, required_zone):
                raise GuildZoneMismatchError("guild belongs to another zone")
            if not await tx.lock_player_state(actor.player_id):
                raise StoreUnavailableError("player state missing")
            if await tx.get_player_guild_id(actor.player_id) != 0:
                raise GuildAlreadyInGuildError("already in a guild")
            await tx.purge_expired_applications_of_player(actor.player_id, now_ms)
            existing_exp = await tx.lock_application_expire(guild_id, actor.player_id)
            expire_ms = now_ms + rule.application_ttl_ms
            if existing_exp is not None:
                await tx.refresh_application(guild_id, actor.player_id, now_ms, expire_ms)
                return ApplyResult(inserted=False, reviewer_ids=())
            if await tx.count_members(guild_id) >= locked.max_members:
                raise GuildFullError("guild is full")
            if await tx.count_pending_applications_of_player(actor.player_id, now_ms) >= rule.max_pending_applications_per_player:
                raise GuildApplicationLimitError("pending application limit reached")
            if await tx.count_live_applications_of_guild(guild_id, now_ms) >= rule.max_pending_applications_per_guild:
                raise GuildApplicationQueueFullError("guild application queue full")
            if not await tx.insert_application(guild_id, actor.player_id, now_ms, expire_ms):
                raise GuildWriteConflictError("concurrent live application conflict")
            members = await tx.list_members(guild_id)
            reviewers = tuple(m.player_id for m in members if can_review_applications(m.role))
            return ApplyResult(inserted=True, reviewer_ids=reviewers)

        return await self._run_write_tx(_op, actor=actor)

    async def cancel_application(self, actor: RoleOriginal, guild_id: int, now_ms: int) -> None:
        async def _op(tx: GuildTx) -> None:
            exp = await tx.lock_application_expire(guild_id, actor.player_id)
            if exp is None:
                raise GuildApplicationNotFoundError("application not found")
            if await tx.delete_live_application(guild_id, actor.player_id, now_ms):
                return
            await tx.delete_expired_application(guild_id, actor.player_id, now_ms)
            raise _TxCommitThen(GuildApplicationNotFoundError("application expired"))

        await self._run_write_tx(_op, actor=actor)

    async def list_my_applications(self, player_id: int, now_ms: int, limit: int) -> list[ApplicationRow]:
        return await self._run_tx(lambda tx: tx.list_my_applications(player_id, now_ms, limit), read_only=True)

    async def list_guild_applications(
        self, actor_id: int, guild_id: int, now_ms: int, limit: int
    ) -> list[ApplicantRow]:
        async def _op(tx: GuildTx) -> list[ApplicantRow]:
            role = await tx.get_member_role(guild_id, actor_id)
            if role is None:
                raise GuildNotInGuildError("not a member of the guild")
            if not can_review_applications(role):
                raise GuildRankTooLowError("officer rank required")
            return await tx.list_applicants(guild_id, now_ms, limit)

        return await self._run_tx(_op, read_only=True)

    async def review_application(
        self,
        actor: RoleOriginal,
        *,
        guild_id: int,
        applicant_id: int,
        approve: bool,
        applicant_zone: int,
        now_ms: int,
    ) -> ReviewResult:
        if approve:
            await self._run_tx(lambda tx: tx.ensure_player_state(applicant_id, now_ms))

        async def _op(tx: GuildTx) -> ReviewResult:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            if approve:
                if not await tx.lock_player_state(applicant_id):
                    raise StoreUnavailableError("applicant player state missing")
            actor_m = await tx.lock_member(guild_id, actor.player_id)
            if actor_m is None:
                raise GuildNotInGuildError("not a member of the guild")
            if not can_review_applications(actor_m.role):
                raise GuildRankTooLowError("officer rank required")
            exp = await tx.lock_application_expire(guild_id, applicant_id)
            if exp is None:
                raise GuildApplicationNotFoundError("application not found")
            if exp <= now_ms:
                await tx.delete_application(guild_id, applicant_id)
                raise _TxCommitThen(GuildApplicationNotFoundError("application expired"))
            if not approve:
                await tx.delete_application(guild_id, applicant_id)
                snap = await tx.load_guild(guild_id)
                assert snap is not None
                return ReviewResult(guild=snap, approved=False)
            if not self._merged_zones.same_zone(locked.zone_id, applicant_zone):
                raise GuildZoneMismatchError("applicant is in another zone")
            if await tx.count_members(guild_id) >= locked.max_members:
                raise GuildFullError("guild is full")
            try:
                await tx.insert_member(
                    guild_id,
                    MemberData(
                        player_id=applicant_id,
                        role=ROLE_MEMBER,
                        join_time_ms=now_ms,
                        last_active_ms=now_ms,
                    ),
                )
            except _DuplicateKeyError:
                await tx.delete_application(guild_id, applicant_id)
                raise _TxCommitThen(GuildApplicationNotFoundError("applicant already in another guild"))
            await tx.delete_applications_of_player(applicant_id)
            snap = await tx.load_guild(guild_id)
            assert snap is not None
            return ReviewResult(guild=snap, approved=True)

        extra = (applicant_id,) if approve else ()
        return await self._run_write_tx(_op, actor=actor, extra_targets=extra)

    # -- 排行榜分数持久化 ------------------------------------------------------------------------

    async def update_guild_score(self, guild_id: int, score: int) -> tuple[int, int]:
        """事务内持久化权威分数并递增 score_version，返回 ``(authoritative_zone_id, new_version)``。"""

        async def _op(tx: GuildTx) -> tuple[int, int]:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            if locked.score_version >= MAX_RANK_VERSION:
                raise StoreUnavailableError(f"score version exhausted guild={guild_id}")
            new_version = locked.score_version + 1
            await tx.update_guild_score(guild_id, score, new_version)
            return locked.zone_id, new_version

        return await self._run_tx(_op)

    # -- 帮会经济（升级 / 捐献 / 商店） ----------------------------------------------------------

    async def upgrade_guild(
        self, actor: RoleOriginal, guild_id: int, expected_level: int
    ) -> UpgradeResult:
        async def _op(tx: GuildTx) -> UpgradeResult:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            member = await tx.lock_member(guild_id, actor.player_id)
            if member is None:
                raise GuildNotInGuildError("not a member of the guild")
            if not can_upgrade_guild(member.role):
                raise GuildRankTooLowError("officer rank required to upgrade guild")
            if expected_level != 0 and expected_level != locked.level:
                return UpgradeResult(changed=False, new_level=locked.level, member_ids=())
            cur_cfg = self.catalog.levels.get(locked.level)
            if cur_cfg is None:
                raise GuildConfigMissingError(f"GuildLevel row {locked.level} missing")
            if cur_cfg.upgrade_cost_funds == 0:
                raise GuildMaxLevelError("guild already at max level")
            next_cfg = self.catalog.levels.get(locked.level + 1)
            if next_cfg is None:
                raise GuildConfigMissingError(f"GuildLevel row {locked.level + 1} missing")
            if locked.funds < cur_cfg.upgrade_cost_funds:
                raise GuildFundsInsufficientError("guild funds insufficient")
            ok = await tx.upgrade_guild_row(
                guild_id, locked.level, locked.level + 1, cur_cfg.upgrade_cost_funds, next_cfg.max_members
            )
            if not ok:
                raise GuildFundsInsufficientError("guild funds insufficient")
            members = tuple(await tx.list_member_ids(guild_id))
            return UpgradeResult(changed=True, new_level=locked.level + 1, member_ids=members)

        return await self._run_write_tx(_op, actor=actor)

    async def get_member_contribution(self, guild_id: int, player_id: int) -> tuple[int, int] | None:
        return await self._run_tx(lambda tx: tx.get_member_contribution(guild_id, player_id), read_only=True)

    async def get_counter_usage(
        self, player_id: int, counter_kind: int, period_keys: Sequence[int]
    ) -> dict[tuple[int, int], int]:
        return await self._run_tx(
            lambda tx: tx.get_counter_usage(player_id, counter_kind, period_keys), read_only=True
        )

    async def list_pending_ops(self, player_id: int, stream: int, limit: int) -> list[AssetOpRow]:
        return await self._run_tx(lambda tx: tx.list_pending_ops(player_id, stream, limit), read_only=True)

    async def list_recent_ops(self, player_id: int, stream: int, limit: int) -> list[AssetOpRow]:
        return await self._run_tx(lambda tx: tx.list_recent_ops(player_id, stream, limit), read_only=True)

    async def get_client_request(self, player_id: int, client_request_id: int) -> tuple[bool, int, AssetOpRow | None]:
        async def _op(tx: GuildTx) -> tuple[bool, int, AssetOpRow | None]:
            op_id = await tx.get_client_request_op_id(player_id, client_request_id)
            if op_id is None:
                return False, 0, None
            row = await tx.get_asset_op(op_id)
            return True, op_id, row

        return await self._run_tx(_op, read_only=True)

    async def reserve_donation(
        self,
        actor: RoleOriginal,
        *,
        guild_id: int,
        donate_id: int,
        min_guild_level: int,
        contribution_gain: int,
        funds_gain: int,
        daily_limit: int,
        period_key_val: int,
        deadline_ms: int,
        lease_until_ms: int,
        payload: bytes,
        client_request_id: int,
        now_ms: int,
    ) -> AssetOpRow:
        op_id = await self._op_ids()
        if op_id <= 0:
            raise StoreUnavailableError("minted op_id is zero")
        lease_token = _new_lease_token()

        async def _op(tx: GuildTx) -> AssetOpRow:
            level = await tx.get_guild_level(guild_id)
            if level is None:
                raise GuildNotFoundError("guild not found")
            if level < min_guild_level:
                raise GuildShopLevelTooLowError("guild level too low")
            member = await tx.lock_member(guild_id, actor.player_id)
            if member is None:
                raise GuildNotInGuildError("not a member of the guild")
            if client_request_id != 0:
                existing_op = await tx.get_client_request_op_id(actor.player_id, client_request_id)
                if existing_op is not None:
                    raise GuildClientRequestReplayedError(existing_op)
            seq, epoch = await tx.allocate_seq(actor.player_id, ASSET_STREAM_GUILD_DEBIT, now_ms)
            op = AssetOpRow(
                op_id=op_id,
                player_id=actor.player_id,
                stream=ASSET_STREAM_GUILD_DEBIT,
                seq=seq,
                guild_id=guild_id,
                kind=OP_KIND_DONATE,
                status=OP_STATUS_PENDING,
                durable=0,
                attempts=0,
                next_attempt_ms=lease_until_ms,
                deadline_ms=deadline_ms,
                payload=payload,
                ref_id=donate_id,
                ref_count=1,
                period_key=period_key_val,
                contribution_delta=contribution_gain,
                funds_delta=funds_gain,
                reason_tip_id=0,
                created_ms=now_ms,
                updated_ms=now_ms,
                lease_until_ms=lease_until_ms,
                lease_token=lease_token,
                tx_type=TX_GUILD_DONATE,
                last_outcome=0,
                last_reason=0,
                stream_epoch=epoch,
                last_send_ms=now_ms,
            )
            await tx.insert_asset_op(op)
            if not await tx.upsert_counter_with_limit(
                actor.player_id, COUNTER_KIND_DONATE, donate_id, period_key_val, 1, daily_limit, now_ms
            ):
                raise GuildDonateLimitError("daily donate limit reached")
            if client_request_id != 0:
                try:
                    await tx.insert_client_request(actor.player_id, client_request_id, op_id, now_ms)
                except _DuplicateKeyError as exc:
                    raise GuildClientRequestReplayedError(op_id) from exc
            return op

        return await self._run_write_tx(_op, actor=actor)

    async def reserve_shop_order(
        self,
        actor: RoleOriginal,
        *,
        guild_id: int,
        goods_id: int,
        count: int,
        required_guild_level: int,
        cost: int,
        limit_count: int,
        period_key_val: int,
        lease_until_ms: int,
        payload: bytes,
        client_request_id: int,
        now_ms: int,
    ) -> tuple[AssetOpRow, int]:
        op_id = await self._op_ids()
        if op_id <= 0:
            raise StoreUnavailableError("minted op_id is zero")
        lease_token = _new_lease_token()

        async def _op(tx: GuildTx) -> tuple[AssetOpRow, int]:
            level = await tx.get_guild_level(guild_id)
            if level is None:
                raise GuildNotFoundError("guild not found")
            if level < required_guild_level:
                raise GuildShopLevelTooLowError("guild level too low")
            member = await tx.lock_member(guild_id, actor.player_id)
            if member is None:
                raise GuildNotInGuildError("not a member of the guild")
            if client_request_id != 0:
                existing_op = await tx.get_client_request_op_id(actor.player_id, client_request_id)
                if existing_op is not None:
                    raise GuildClientRequestReplayedError(existing_op)
            if member.contribution_balance < cost:
                raise GuildContributionInsufficientError("contribution insufficient")
            seq, epoch = await tx.allocate_seq(actor.player_id, ASSET_STREAM_GUILD_CREDIT, now_ms)
            op = AssetOpRow(
                op_id=op_id,
                player_id=actor.player_id,
                stream=ASSET_STREAM_GUILD_CREDIT,
                seq=seq,
                guild_id=guild_id,
                kind=OP_KIND_SHOP,
                status=OP_STATUS_PENDING,
                durable=0,
                attempts=0,
                next_attempt_ms=lease_until_ms,
                deadline_ms=0,
                payload=payload,
                ref_id=goods_id,
                ref_count=count,
                period_key=period_key_val,
                contribution_delta=cost,
                funds_delta=0,
                reason_tip_id=0,
                created_ms=now_ms,
                updated_ms=now_ms,
                lease_until_ms=lease_until_ms,
                lease_token=lease_token,
                tx_type=TX_GUILD_SHOP,
                last_outcome=0,
                last_reason=0,
                stream_epoch=epoch,
                last_send_ms=now_ms,
            )
            await tx.insert_asset_op(op)
            if limit_count > 0:
                if not await tx.upsert_counter_with_limit(
                    actor.player_id, COUNTER_KIND_SHOP, goods_id, period_key_val, count, limit_count, now_ms
                ):
                    raise GuildShopLimitError("shop purchase limit reached")
            if not await tx.debit_member_contribution(guild_id, actor.player_id, cost):
                raise GuildContributionInsufficientError("contribution insufficient")
            if client_request_id != 0:
                try:
                    await tx.insert_client_request(actor.player_id, client_request_id, op_id, now_ms)
                except _DuplicateKeyError as exc:
                    raise GuildClientRequestReplayedError(op_id) from exc
            return op, member.contribution_balance - cost

        return await self._run_write_tx(_op, actor=actor)

    # -- 帮会活动（元宵灯会 / 中秋团圆） --------------------------------------------------------

    async def get_activity_progress(
        self, guild_id: int, keys: Sequence[tuple[int, int]]
    ) -> dict[tuple[int, int], ActivityProgress]:
        return await self._run_tx(lambda tx: tx.get_activity_progress(guild_id, keys), read_only=True)

    async def get_activity_reward_statuses(self, player_id: int, now_ms: int) -> dict[int, ActivityRewardStatus]:
        return await self._run_tx(lambda tx: tx.get_activity_reward_statuses(player_id, now_ms), read_only=True)

    async def participate_activity(
        self,
        actor: RoleOriginal,
        *,
        guild_id: int,
        row: ActivityRuleRow,
        observed_reunion: bool,
        online_for_error: int,
        threshold_for_error: int,
        reward_payload: bytes,
        lease_until_ms: int,
        now_ms: int,
    ) -> ActivityTxResult:
        join_min_hours = self.catalog.rule.activity_join_min_hours
        d_key = day_key(now_ms)
        g_key = guild_period_key(row, now_ms)
        op_id = 0
        lease_token = 0
        if reward_payload:
            op_id = await self._op_ids()
            if op_id <= 0:
                raise StoreUnavailableError("minted op_id is zero")
            lease_token = _new_lease_token()

        async def _op(tx: GuildTx) -> ActivityTxResult:
            locked = await tx.lock_guild(guild_id)
            if locked is None:
                raise GuildNotFoundError("guild not found")
            if locked.level < row.min_guild_level:
                raise GuildActivityLevelTooLowError(row.min_guild_level)
            member = await tx.lock_member(guild_id, actor.player_id)
            if member is None:
                raise GuildNotInGuildError("not a member of the guild")
            if not joined_long_enough(member.join_time_ms, now_ms, join_min_hours):
                raise GuildActivityJoinTooRecentError(join_min_hours)
            gain = row.personal_contribution
            if member.contribution_total > UINT64_MAX - gain or member.contribution_balance > UINT64_MAX - gain:
                raise GuildActivityOverflowError("member contribution overflow")

            seq = 0
            epoch = 0
            enqueued = False
            created_op: AssetOpRow | None = None
            if reward_payload:
                seq, epoch = await tx.allocate_seq(actor.player_id, ASSET_STREAM_GUILD_CREDIT, now_ms)
                op = AssetOpRow(
                    op_id=op_id,
                    player_id=actor.player_id,
                    stream=ASSET_STREAM_GUILD_CREDIT,
                    seq=seq,
                    guild_id=guild_id,
                    kind=OP_KIND_ACTIVITY_REWARD,
                    status=OP_STATUS_PENDING,
                    durable=0,
                    attempts=0,
                    next_attempt_ms=lease_until_ms,
                    deadline_ms=0,
                    payload=reward_payload,
                    ref_id=row.activity_id,
                    ref_count=1,
                    period_key=d_key,
                    contribution_delta=0,
                    funds_delta=0,
                    reason_tip_id=0,
                    created_ms=now_ms,
                    updated_ms=now_ms,
                    lease_until_ms=lease_until_ms,
                    lease_token=lease_token,
                    tx_type=TX_GUILD_ACTIVITY_REWARD,
                    last_outcome=0,
                    last_reason=0,
                    stream_epoch=epoch,
                    last_send_ms=now_ms,
                )
                await tx.insert_asset_op(op)
                created_op = op
                enqueued = True
            else:
                await tx.ensure_and_lock_seq(actor.player_id, ASSET_STREAM_GUILD_CREDIT, now_ms)

            if not await tx.upsert_counter_with_limit(
                actor.player_id, COUNTER_KIND_ACTIVITY, row.activity_id, d_key, 1, row.daily_limit, now_ms
            ):
                raise GuildActivityAlreadyClaimedError("guild activity daily limit reached")

            progress = await tx.upsert_and_lock_activity_progress(
                guild_id,
                row.activity_id,
                g_key,
                increment=(row.activity_type == ACTIVITY_TYPE_LANTERN),
                now_ms=now_ms,
            )

            if row.activity_type == ACTIVITY_TYPE_LANTERN:
                reached_now, grant_funds = lantern_outcome(row, progress)
            elif row.activity_type == ACTIVITY_TYPE_REUNION:
                ok, reached_now, grant_funds = reunion_outcome(row, progress, observed_reunion)
                if not ok:
                    raise GuildActivityThresholdNotReachedError(online_for_error, threshold_for_error)
            else:
                raise GuildConfigMissingError(f"unsupported activity type {row.activity_type}")

            new_reached_ms = progress.threshold_reached_ms or (now_ms if reached_now else 0)
            new_funds_granted = progress.funds_granted or grant_funds
            if reached_now or grant_funds:
                await tx.latch_activity_progress(
                    guild_id, row.activity_id, g_key, new_reached_ms, new_funds_granted, now_ms
                )
                progress = ActivityProgress(
                    count=progress.count,
                    threshold_reached_ms=new_reached_ms,
                    funds_granted=new_funds_granted,
                )
            if grant_funds:
                if locked.funds > UINT64_MAX - row.guild_funds:
                    raise GuildActivityOverflowError("guild funds overflow")
                await tx.set_guild_funds(guild_id, locked.funds + row.guild_funds)
            if gain > 0:
                await tx.set_member_contribution(
                    guild_id,
                    actor.player_id,
                    member.contribution_total + gain,
                    member.contribution_balance + gain,
                )
            return ActivityTxResult(
                enqueued=enqueued,
                seq=seq,
                stream_epoch=epoch,
                day_key_val=d_key,
                guild_period_key_val=g_key,
                progress=progress,
                reached_now=reached_now,
                funds_granted=grant_funds,
                op=created_op,
            )

        return await self._run_write_tx(_op, actor=actor)

    # -- AssetOp 生命周期（领取 / 重排 / 终结对侧账 / 自动放弃） ---------------------------------

    def op_to_pending(self, op: AssetOpRow) -> PendingAssetOp:
        """将持久化 AssetOpRow 解码为待投递 PendingAssetOp；空包或畸形包抛 AssetOpPoisonRowError。"""
        if not op.payload:
            raise AssetOpPoisonRowError(f"guild asset op {op.op_id}: empty payload")
        try:
            bundle = self.protocol.parse("AssetBundle", op.payload)
        except DecodeError as exc:
            raise AssetOpPoisonRowError(f"guild asset op {op.op_id}: invalid payload: {exc}") from exc
        return PendingAssetOp(
            op_id=op.op_id,
            player_id=op.player_id,
            guild_id=op.guild_id,
            stream=op.stream,
            seq=op.seq,
            stream_epoch=op.stream_epoch,
            correlation_id=op.op_id,
            tx_type=op.tx_type,
            kind=op.kind,
            bundle=bundle,
            attempts=op.attempts,
            deadline_ms=op.deadline_ms,
            last_reason=op.last_reason,
            lease_token=op.lease_token,
            created_ms=op.created_ms,
            last_send_ms=op.last_send_ms,
        )

    async def get_asset_op(self, op_id: int) -> AssetOpRow | None:
        return await self._run_tx(lambda tx: tx.get_asset_op(op_id), read_only=True)

    async def list_due_asset_ops(
        self, now_ms: int, limit: int, *, owned_shards: frozenset[int] | None = None
    ) -> list[int]:
        if limit <= 0:
            return []
        return await self._run_tx(
            lambda tx: tx.list_due_asset_ops(now_ms, limit, owned_shards), read_only=True
        )

    async def claim_asset_op(
        self,
        op_id: int,
        now_ms: int,
        lease_until_ms: int,
        poison_until_ms: int,
        lease_token: int,
    ) -> PendingAssetOp | None:
        row = await self._run_tx(lambda tx: tx.claim_asset_op(op_id, now_ms, lease_until_ms, lease_token))
        if row is None:
            return None
        try:
            return self.op_to_pending(row)
        except AssetOpPoisonRowError:
            await self._run_tx(lambda tx: tx.mark_asset_op_poison(op_id, lease_token, poison_until_ms, now_ms))
            raise

    async def reschedule_asset_op(
        self,
        op: PendingAssetOp,
        next_attempt_ms: int,
        outcome: int,
        reason: int,
        durable: bool,
        local: bool,
        now_ms: int,
    ) -> None:
        ok = await self._run_tx(
            lambda tx: tx.reschedule_asset_op(
                op.op_id, op.lease_token, next_attempt_ms, outcome, reason, durable, local, now_ms
            )
        )
        if not ok:
            raise AssetOpLeaseLostError(f"guild asset op {op.op_id} lease lost on reschedule")

    @staticmethod
    def _refunds_counterparty(final_status: int) -> bool:
        return final_status in (OP_STATUS_REJECTED, OP_STATUS_ABORTED, OP_STATUS_ABANDONED)

    @classmethod
    def _counter_refund(cls, row: AssetOpRow, final_status: int) -> tuple[int, int] | None:
        if not cls._refunds_counterparty(final_status) or row.period_key == 0:
            return None
        if row.kind == OP_KIND_DONATE:
            return COUNTER_KIND_DONATE, 1
        if row.kind == OP_KIND_SHOP and row.ref_count > 0:
            return COUNTER_KIND_SHOP, row.ref_count
        return None

    async def _terminate_asset_op(
        self,
        op_id: int,
        final_status: int,
        *,
        cas_fn: Callable[[GuildTx], Awaitable[bool]],
        now_ms: int,
    ) -> bool:
        row = await self.get_asset_op(op_id)
        if row is None:
            return False

        refund_spec = self._counter_refund(row, final_status)
        refunded = self._refunds_counterparty(final_status)

        async def _op(tx: GuildTx) -> bool:
            # 按全库锁序：guild -> guild_member -> guild_player_op_seq -> guild_asset_op -> guild_daily_counter
            guild_ok = False
            member_ok = False
            if row.kind == OP_KIND_DONATE and final_status == OP_STATUS_APPLIED:
                locked_g = await tx.lock_guild(row.guild_id)
                guild_ok = locked_g is not None
                if guild_ok:
                    member_ok = await tx.lock_member(row.guild_id, row.player_id) is not None
            elif row.kind == OP_KIND_SHOP and refunded:
                member_ok = await tx.lock_member(row.guild_id, row.player_id) is not None

            if refund_spec is not None:
                await tx.lock_seq_guard(row.player_id, row.stream)

            locked_op = await tx.lock_asset_op(op_id)
            if locked_op is None:
                return False
            if not await cas_fn(tx):
                return False

            # APPLIED_PARTIAL 绝不做对侧账（人工补偿线索留存）
            if final_status == OP_STATUS_APPLIED_PARTIAL:
                return True

            if row.kind == OP_KIND_DONATE and final_status == OP_STATUS_APPLIED:
                if not guild_ok:
                    logger.info(
                        "guild asset orphan kind=donate what=guild_gone op_id=%d guild=%d player=%d",
                        op_id,
                        row.guild_id,
                        row.player_id,
                    )
                else:
                    if row.funds_delta > 0:
                        await tx.credit_guild_funds(row.guild_id, row.funds_delta)
                    if not member_ok:
                        logger.info(
                            "guild asset orphan kind=donate what=member_gone op_id=%d guild=%d player=%d",
                            op_id,
                            row.guild_id,
                            row.player_id,
                        )
                    elif row.contribution_delta > 0:
                        await tx.credit_member_contribution(row.guild_id, row.player_id, row.contribution_delta)
            elif row.kind == OP_KIND_SHOP and refunded:
                if not member_ok:
                    logger.info(
                        "guild asset orphan kind=shop what=refund_member_gone op_id=%d guild=%d player=%d",
                        op_id,
                        row.guild_id,
                        row.player_id,
                    )
                elif row.contribution_delta > 0:
                    await tx.refund_member_contribution(row.guild_id, row.player_id, row.contribution_delta)

            if refund_spec is not None:
                c_kind, delta = refund_spec
                await tx.refund_counter(row.player_id, c_kind, row.ref_id, row.period_key, delta, now_ms)
            return True

        finalized = await self._run_tx(_op)
        if finalized and self.on_asset_finalized is not None:
            try:
                await self.on_asset_finalized(
                    FinalizedAssetOp(
                        op_id=op_id,
                        player_id=row.player_id,
                        guild_id=row.guild_id,
                        kind=row.kind,
                        status=final_status,
                    )
                )
            except Exception as exc:
                logger.error("guild asset on_finalized callback failed op_id=%d err=%s", op_id, exc)
        return finalized

    async def finalize_asset_op(
        self,
        op: PendingAssetOp,
        final_status: int,
        last_outcome: int,
        last_reason: int,
        now_ms: int,
    ) -> bool:
        if final_status not in (
            OP_STATUS_APPLIED,
            OP_STATUS_REJECTED,
            OP_STATUS_ABORTED,
            OP_STATUS_APPLIED_PARTIAL,
        ):
            raise ValueError(f"invalid terminal status for finalize: {final_status}")
        reason_tip = last_reason if final_status == OP_STATUS_REJECTED else 0
        return await self._terminate_asset_op(
            op.op_id,
            final_status,
            cas_fn=lambda tx: tx.finalize_asset_op_cas(
                op.op_id, final_status, last_outcome, last_reason, reason_tip, now_ms
            ),
            now_ms=now_ms,
        )

    async def abandon_asset_op(self, op: PendingAssetOp, now_ms: int) -> None:
        if op.lease_token == 0:
            raise ValueError(f"abandon asset op {op.op_id}: lease token is zero")
        done = await self._terminate_asset_op(
            op.op_id,
            OP_STATUS_ABANDONED,
            cas_fn=lambda tx: tx.abandon_asset_op_cas(op.op_id, op.lease_token, now_ms),
            now_ms=now_ms,
        )
        if not done:
            raise AssetOpLeaseLostError(f"guild asset op {op.op_id} abandon CAS missed")

    async def resolve_manually(self, resolution: ManualResolution, now_ms: int) -> bool:
        """人工终结未决资产指令并同步对侧账（对齐 Go GuildAssetStore.ResolveManually）。"""
        _validate_manual_resolution(resolution)
        return await self._terminate_asset_op(
            resolution.op_id,
            resolution.final_status,
            cas_fn=lambda tx: tx.resolve_asset_op_manual_cas(
                resolution.op_id,
                resolution.final_status,
                resolution.operator,
                resolution.reason,
                now_ms,
                None,
            ),
            now_ms=now_ms,
        )

    async def resolve_manually_if_quiet(
        self,
        resolution: ManualResolution,
        witness: QuietWitness,
        now_ms: int,
    ) -> bool:
        """静默条件人工终结（对齐 Go GuildAssetStore.ResolveManuallyIfQuiet）：须租约、last_send_ms 与 attempts 全一致。"""
        _validate_manual_resolution(resolution)
        return await self._terminate_asset_op(
            resolution.op_id,
            resolution.final_status,
            cas_fn=lambda tx: tx.resolve_asset_op_manual_cas(
                resolution.op_id,
                resolution.final_status,
                resolution.operator,
                resolution.reason,
                now_ms,
                witness,
            ),
            now_ms=now_ms,
        )

    # -- 同道历练（guild_trial_lobby / slot / launch / launch_role / battle / reward_owed） -------

    async def get_trial_roster_members(self, guild_id: int, player_ids: Sequence[int]) -> dict[int, int]:
        return await self._run_tx(lambda tx: tx.get_trial_roster_members(guild_id, player_ids), read_only=True)

    async def allocate_op_id(self) -> int:
        return await self._op_ids()

    async def get_role_fences_local(self, player_ids: Sequence[int]) -> dict[int, RoleFenceRow]:
        async def _op(tx: GuildTx) -> dict[int, RoleFenceRow]:
            out: dict[int, RoleFenceRow] = {}
            for pid in sorted({int(p) for p in player_ids if int(p) > 0}):
                row = await tx.get_role_fence(pid)
                if row is not None:
                    out[pid] = row
            return out

        return await self._run_tx(_op, read_only=True)

    async def get_activity_counter_value(self, player_id: int, activity_id: int, period_key_val: int) -> int:
        return await self._run_tx(
            lambda tx: tx.get_activity_counter_value(player_id, activity_id, period_key_val),
            read_only=True,
        )

    async def get_trial_lobby(self, lobby_id: int) -> TrialLobbyRecord | None:
        return await self._run_tx(lambda tx: tx.get_trial_lobby(lobby_id), read_only=True)

    async def get_player_trial_lobby(self, player_id: int) -> TrialLobbyRecord | None:
        async def _op(tx: GuildTx) -> TrialLobbyRecord | None:
            slot = await tx.get_trial_lobby_slot(player_id)
            if slot is None or slot.lobby_id == 0:
                return None
            return await tx.get_trial_lobby(slot.lobby_id)

        return await self._run_tx(_op, read_only=True)

    async def precheck_trial_lobby_create(
        self, player_ids: Sequence[int], initiator_id: int, now_ms: int
    ) -> tuple[str, int]:
        """事务外预检冷却与忙碌槽位：返回 ('ok', 0) / ('cooldown', remaining_ms) / ('busy', player_id)。"""

        async def _op(tx: GuildTx) -> tuple[str, int]:
            for pid in sorted(set(player_ids)):
                slot = await tx.get_trial_lobby_slot(pid)
                if slot is None:
                    continue
                if pid == initiator_id and slot.cooldown_until_ms > now_ms:
                    return "cooldown", slot.cooldown_until_ms - now_ms
                if slot.lobby_id != 0:
                    lob = await tx.get_trial_lobby(slot.lobby_id)
                    if lob is not None and lob.state != TRIAL_LOBBY_STATE_ENDED:
                        return "busy", pid
            return "ok", 0

        return await self._run_tx(_op, read_only=True)

    async def create_trial_lobby(
        self,
        actor: RoleOriginal,
        *,
        lobby_id: int,
        guild_id: int,
        activity_id: int,
        roster: Sequence[int],
        ttl_ms: int,
        cooldown_ms: int,
        now_ms: int,
    ) -> tuple[str, TrialLobbyRecord | None, int]:
        """创建 SQL 历练大厅与占位槽：返回 (status, lobby_rec, extra_value)。
        status: 'created' | 'cooldown' (extra=remaining_ms) | 'busy' (extra=player_id)。
        """
        sorted_ids = sorted(set(int(p) for p in roster))
        if len(sorted_ids) < MIN_TRIAL_LOBBY_ROSTER or len(sorted_ids) > MAX_TRIAL_LOBBY_ROSTER:
            raise GuildWriteConflictError("invalid trial lobby roster size")
        if actor.player_id != int(roster[0]):
            raise GuildWriteConflictError("initiator mismatch")
        expires_ms = now_ms + ttl_ms

        async def _op(tx: GuildTx) -> tuple[str, TrialLobbyRecord | None, int]:
            locked_g = await tx.lock_guild(guild_id)
            if locked_g is None:
                raise GuildNotFoundError("guild not found")

            original = self.protocol.message(
                "guildpb.GuildTrialLobbyOriginal",
                lobby_id=lobby_id,
                guild_id=guild_id,
                activity_id=activity_id,
                initiator_player_id=actor.player_id,
                created_ms=now_ms,
                expires_ms=expires_ms,
            )
            initiator_identity = None
            for pid in sorted_ids:
                member = await tx.lock_member(guild_id, pid)
                if member is None:
                    raise GuildWriteConflictError(f"player {pid} not member of guild {guild_id}")
                fence = await tx.lock_role_fence(pid)
                if fence is not None:
                    acc, rev = fence.account, fence.owner_revision
                elif pid == actor.player_id:
                    acc, rev = actor.account, actor.owner_revision
                else:
                    acc, rev = f"role_{pid}", 1
                if pid == actor.player_id and fence is not None:
                    if fence.account != actor.account or fence.owner_revision != actor.owner_revision:
                        raise GuildWriteConflictError("initiator role identity changed")
                role_msg = original.roster.add(player_id=pid, account=acc, owner_revision=rev)
                if pid == actor.player_id:
                    initiator_identity = role_msg

                slot = await tx.lock_trial_lobby_slot(pid)
                if slot is not None:
                    if pid == actor.player_id and slot.cooldown_until_ms > now_ms:
                        return "cooldown", None, slot.cooldown_until_ms - now_ms
                    if slot.lobby_id != 0:
                        existing_lob = await tx.get_trial_lobby(slot.lobby_id)
                        if existing_lob is None or existing_lob.state != TRIAL_LOBBY_STATE_ENDED:
                            return "busy", None, pid
                if await tx.has_active_trial_launch_role(pid):
                    return "busy", None, pid

            ordered_members = [actor.player_id] + [pid for pid in roster if int(pid) != actor.player_id]
            view = self.protocol.message(
                "guildpb.GuildTrialLobbyView",
                lobby_id=lobby_id,
                initiator_player_id=actor.player_id,
                member_player_ids=ordered_members,
                accepted_player_ids=[actor.player_id],
                state=TRIAL_LOBBY_STATE_PENDING,
                expire_at_ms=expires_ms,
            )
            consents = self.protocol.message("guildpb.GuildTrialLobbyConsents")
            c_item = consents.consents.add(lobby_id=lobby_id)
            assert initiator_identity is not None
            c_item.identity.CopyFrom(initiator_identity)

            rec = TrialLobbyRecord(
                lobby_id=lobby_id,
                guild_id=guild_id,
                activity_id=activity_id,
                initiator_player_id=actor.player_id,
                state=TRIAL_LOBBY_STATE_PENDING,
                view_proto=view.SerializeToString(deterministic=True),
                original_proto=original.SerializeToString(deterministic=True),
                consent_proto=consents.SerializeToString(deterministic=True),
                expires_ms=expires_ms,
                launch_deadline_ms=0,
                battle_id=0,
                created_ms=now_ms,
                updated_ms=now_ms,
            )
            await tx.insert_trial_lobby(rec)
            for pid in sorted_ids:
                cd_until = now_ms + cooldown_ms if pid == actor.player_id else 0
                await tx.upsert_trial_lobby_slot(pid, lobby_id, cd_until)
            return "created", rec, 0

        extra = [pid for pid in sorted_ids if pid != actor.player_id]
        return await self._run_write_tx(_op, actor=actor, extra_targets=extra)

    async def respond_trial_lobby(
        self,
        actor: RoleOriginal,
        *,
        lobby_id: int,
        accept: bool,
        launch_window_ms: int,
        now_ms: int,
    ) -> tuple[str, bool, TrialLobbyRecord | None]:
        """响应历练邀请：返回 (status, already_accepted, updated_lobby)。
        status: 'gone' | 'not_pending' | 'declined' | 'accepted' | 'launch'。
        """
        pre = await self.get_trial_lobby(lobby_id)
        if pre is None:
            return "gone", False, None
        pre_orig = self.protocol.parse("guildpb.GuildTrialLobbyOriginal", pre.original_proto)
        roster_ids = [int(r.player_id) for r in pre_orig.roster]
        if actor.player_id not in roster_ids:
            return "gone", False, None

        async def _op(tx: GuildTx) -> tuple[str, bool, TrialLobbyRecord | None]:
            await tx.lock_guild(pre.guild_id)
            row = await tx.lock_trial_lobby(lobby_id)
            if row is None:
                return "gone", False, None
            orig = self.protocol.parse("guildpb.GuildTrialLobbyOriginal", row.original_proto)
            view = self.protocol.parse("guildpb.GuildTrialLobbyView", row.view_proto)
            consents = self.protocol.parse("guildpb.GuildTrialLobbyConsents", row.consent_proto)

            roles_by_pid = {int(r.player_id): r for r in orig.roster}
            my_role = roles_by_pid.get(actor.player_id)
            if my_role is None:
                return "gone", False, row
            if self._role_directory is not None:
                if my_role.account != actor.account or int(my_role.owner_revision) != actor.owner_revision:
                    raise GuildWriteConflictError("responder role identity mismatch")

            consented_pids = {int(c.identity.player_id) for c in consents.consents}
            mine = actor.player_id in consented_pids
            if row.state != TRIAL_LOBBY_STATE_PENDING:
                return "not_pending", mine, row

            if now_ms >= row.expires_ms:
                view.state = TRIAL_LOBBY_STATE_ENDED
                view.end_tip_id = ERR_GUILD_TRIAL_INVITE_EXPIRED
                view_bytes = view.SerializeToString(deterministic=True)
                await tx.update_trial_lobby(
                    lobby_id, TRIAL_LOBBY_STATE_ENDED, view_bytes, row.consent_proto, 0, 0, now_ms
                )
                return "gone", mine, replace(row, state=TRIAL_LOBBY_STATE_ENDED, view_proto=view_bytes, updated_ms=now_ms)

            if not accept:
                view.state = TRIAL_LOBBY_STATE_ENDED
                view.end_tip_id = ERR_GUILD_TRIAL_INVITE_DECLINED
                del view.end_parameters[:]
                view.end_parameters.append(str(actor.player_id))
                view_bytes = view.SerializeToString(deterministic=True)
                await tx.update_trial_lobby(
                    lobby_id, TRIAL_LOBBY_STATE_ENDED, view_bytes, row.consent_proto, 0, 0, now_ms
                )
                return "declined", mine, replace(row, state=TRIAL_LOBBY_STATE_ENDED, view_proto=view_bytes, updated_ms=now_ms)

            if mine:
                return "accepted", True, row

            # 写入本人同意并保持 player_id 升序
            consented_pids.add(actor.player_id)
            new_consents = self.protocol.message("guildpb.GuildTrialLobbyConsents")
            for pid in sorted(consented_pids):
                item = new_consents.consents.add(lobby_id=lobby_id)
                item.identity.CopyFrom(roles_by_pid[pid])
            del view.accepted_player_ids[:]
            for pid in view.member_player_ids:
                if int(pid) in consented_pids:
                    view.accepted_player_ids.append(int(pid))

            all_ready = len(consented_pids) == len(roles_by_pid)
            new_state = TRIAL_LOBBY_STATE_LAUNCHING if all_ready else TRIAL_LOBBY_STATE_PENDING
            launch_deadline = now_ms + launch_window_ms if all_ready else 0
            view.state = new_state
            view_bytes = view.SerializeToString(deterministic=True)
            consent_bytes = new_consents.SerializeToString(deterministic=True)
            await tx.update_trial_lobby(
                lobby_id, new_state, view_bytes, consent_bytes, launch_deadline, 0, now_ms
            )
            updated = replace(
                row,
                state=new_state,
                view_proto=view_bytes,
                consent_proto=consent_bytes,
                launch_deadline_ms=launch_deadline,
                updated_ms=now_ms,
            )
            return ("launch" if all_ready else "accepted"), False, updated

        extra = [pid for pid in roster_ids if pid != actor.player_id]
        return await self._run_write_tx(_op, actor=actor, extra_targets=extra)

    async def finish_trial_lobby(
        self,
        lobby_id: int,
        *,
        expected_state: int,
        new_state: int,
        battle_id: int = 0,
        end_tip_id: int = 0,
        end_params: Sequence[str] = (),
        now_ms: int,
    ) -> tuple[bool, TrialLobbyRecord | None]:
        """推进大厅状态（LAUNCHING -> LAUNCHED 或 ENDED）；存在活跃 launch 时拒绝无证明直接结束。"""
        pre = await self.get_trial_lobby(lobby_id)
        if pre is None:
            return False, None

        async def _op(tx: GuildTx) -> tuple[bool, TrialLobbyRecord | None]:
            await tx.lock_guild(pre.guild_id)
            row = await tx.lock_trial_lobby(lobby_id)
            if row is None or row.state != expected_state:
                return False, row
            if new_state == TRIAL_LOBBY_STATE_ENDED:
                existing_launch = await tx.lock_trial_launch(lobby_id)
                if existing_launch is not None and existing_launch.state != TRIAL_LAUNCH_STATE_TERMINAL:
                    return False, row
            view = self.protocol.parse("guildpb.GuildTrialLobbyView", row.view_proto)
            view.state = new_state
            view.battle_id = battle_id
            if new_state == TRIAL_LOBBY_STATE_ENDED:
                view.end_tip_id = end_tip_id
                del view.end_parameters[:]
                view.end_parameters.extend(end_params)
            view_bytes = view.SerializeToString(deterministic=True)
            await tx.update_trial_lobby(
                lobby_id, new_state, view_bytes, row.consent_proto, row.launch_deadline_ms, battle_id, now_ms
            )
            return True, replace(row, state=new_state, view_proto=view_bytes, battle_id=battle_id, updated_ms=now_ms)

        return await self._run_tx(_op)

    async def expire_pending_trial_lobbies(self, now_ms: int, limit: int = 50) -> list[tuple[int, tuple[int, ...]]]:
        """关闭已过期的 PENDING 大厅，返回需释放占用的 (lobby_id, player_ids) 列表。"""
        expired = await self._run_tx(lambda tx: tx.list_expired_pending_lobbies(now_ms, limit), read_only=True)
        released: list[tuple[int, tuple[int, ...]]] = []
        for lob in expired:
            orig = self.protocol.parse("guildpb.GuildTrialLobbyOriginal", lob.original_proto)
            pids = tuple(int(r.player_id) for r in orig.roster)
            moved, _ = await self.finish_trial_lobby(
                lob.lobby_id,
                expected_state=TRIAL_LOBBY_STATE_PENDING,
                new_state=TRIAL_LOBBY_STATE_ENDED,
                end_tip_id=ERR_GUILD_TRIAL_INVITE_EXPIRED,
                now_ms=now_ms,
            )
            if moved:
                released.append((lob.lobby_id, pids))
        return released

    async def prepare_trial_launch(
        self,
        *,
        request_id: int,
        battle: TrialBattleKey,
        dungeon_id: int,
        now_ms: int,
    ) -> Message:
        """提交永久 launch 原件与 launch_role 占位；重复调用幂等返回原件。"""
        pre_lobby = await self.get_trial_lobby(request_id)
        if pre_lobby is None:
            raise GuildWriteConflictError(f"trial lobby {request_id} not found")
        pre_orig = self.protocol.parse("guildpb.GuildTrialLobbyOriginal", pre_lobby.original_proto)
        roster_ids = [int(r.player_id) for r in pre_orig.roster]

        async def _op(tx: GuildTx) -> Message:
            locked_g = await tx.lock_guild(battle.guild_id)
            if locked_g is None:
                raise GuildNotFoundError("guild not found")
            lobby = await tx.lock_trial_lobby(request_id)
            if lobby is None or lobby.state == TRIAL_LOBBY_STATE_ENDED:
                raise GuildWriteConflictError("trial lobby ended")
            lobby_orig = self.protocol.parse("guildpb.GuildTrialLobbyOriginal", lobby.original_proto)
            lobby_consents = self.protocol.parse("guildpb.GuildTrialLobbyConsents", lobby.consent_proto)
            if len(lobby_consents.consents) != len(lobby_orig.roster):
                raise GuildWriteConflictError("trial lobby consents incomplete")

            for role in lobby_orig.roster:
                pid = int(role.player_id)
                if await tx.lock_member(battle.guild_id, pid) is None:
                    raise GuildWriteConflictError(f"member {pid} left guild")

            proposed = self.protocol.message(
                "guildpb.GuildTrialLaunchOriginal",
                request_id=request_id,
                guild_id=battle.guild_id,
                activity_id=battle.activity_id,
                period_key=battle.period_key,
                guild_period_key=battle.guild_period_key,
                initiator_player_id=battle.initiator_player_id,
                dungeon_id=dungeon_id,
                created_ms=now_ms,
            )
            for role in sorted(lobby_orig.roster, key=lambda r: int(r.player_id)):
                proposed.roster.add(
                    player_id=int(role.player_id),
                    account=str(role.account),
                    owner_revision=int(role.owner_revision),
                )

            existing = await tx.lock_trial_launch(request_id)
            if existing is not None:
                saved = self.protocol.parse("guildpb.GuildTrialLaunchOriginal", existing.original_proto)
                if existing.state == TRIAL_LAUNCH_STATE_TERMINAL:
                    raise GuildWriteConflictError("trial launch already terminal")
                proposed.created_ms = saved.created_ms
                if proposed.SerializeToString(deterministic=True) != existing.original_proto:
                    raise GuildWriteConflictError("trial launch original mismatch")
                return saved

            if lobby.state != TRIAL_LOBBY_STATE_LAUNCHING:
                raise GuildWriteConflictError("trial lobby not launching")
            for pid in roster_ids:
                if await tx.has_active_trial_launch_role(pid, exclude_request_id=request_id):
                    raise GuildWriteConflictError(f"player {pid} has active trial launch role")

            raw = proposed.SerializeToString(deterministic=True)
            digest = hashlib.sha256(raw).digest()
            first_check_ms = max(lobby.launch_deadline_ms, now_ms) + TRIAL_OCCUPANCY_MARGIN_MS
            await tx.insert_trial_launch(
                TrialLaunchRecord(
                    request_id=request_id,
                    battle_id=0,
                    state=TRIAL_LAUNCH_STATE_PREPARED,
                    original_proto=raw,
                    request_digest=digest,
                    created_ms=now_ms,
                    updated_ms=now_ms,
                    cancel_receipt=b"",
                    next_check_ms=first_check_ms,
                    check_attempts=0,
                    realtime_end_tip=b"",
                )
            )
            for role in proposed.roster:
                await tx.insert_trial_launch_role(
                    TrialLaunchRoleRecord(
                        player_id=int(role.player_id),
                        request_id=request_id,
                        account=str(role.account),
                        owner_revision=int(role.owner_revision),
                        state=TRIAL_LAUNCH_ROLE_ACTIVE,
                    )
                )
            return proposed

        return await self._run_write_tx(_op, extra_targets=roster_ids)

    async def get_trial_launch(self, request_id: int) -> TrialLaunchRecord | None:
        return await self._run_tx(lambda tx: tx.get_trial_launch(request_id), read_only=True)

    async def record_trial_launch_end_tip(self, request_id: int, tip_id: int, params: Sequence[str] = ()) -> None:
        if request_id == 0 or tip_id == 0:
            return
        tip = self.protocol.message("TipInfoMessage", id=tip_id, parameters=list(params))
        raw = tip.SerializeToString(deterministic=True)
        await self._run_tx(lambda tx: tx.record_trial_launch_end_tip(request_id, raw))

    async def expedite_trial_launch_check(self, request_id: int, now_ms: int) -> None:
        if request_id == 0:
            return
        await self._run_tx(lambda tx: tx.expedite_trial_launch_check(request_id, now_ms))

    async def claim_due_trial_launches(self, now_ms: int, limit: int = 20) -> list[TrialLaunchRecord]:
        due = await self._run_tx(lambda tx: tx.list_due_trial_launches(now_ms, limit), read_only=True)
        claimed: list[TrialLaunchRecord] = []
        for rec in due:
            delay_ms = min(10_000 * (1 << min(rec.check_attempts, 4)), 120_000)
            ok = await self._run_tx(
                lambda tx, r=rec, nxt=now_ms + delay_ms: tx.claim_trial_launch(r.request_id, r.next_check_ms, nxt)
            )
            if ok:
                claimed.append(rec)
        return claimed

    async def _close_trial_lobby_tx(
        self,
        tx: GuildTx,
        lobby_id: int,
        *,
        end_tip_id: int,
        end_params: Sequence[str],
        now_ms: int,
    ) -> None:
        lobby = await tx.lock_trial_lobby(lobby_id)
        if lobby is None or lobby.state == TRIAL_LOBBY_STATE_ENDED:
            return
        view = self.protocol.parse("guildpb.GuildTrialLobbyView", lobby.view_proto)
        view.state = TRIAL_LOBBY_STATE_ENDED
        if end_tip_id != 0:
            view.end_tip_id = end_tip_id
            del view.end_parameters[:]
            view.end_parameters.extend(end_params)
        await tx.update_trial_lobby(
            lobby_id,
            TRIAL_LOBBY_STATE_ENDED,
            view.SerializeToString(deterministic=True),
            lobby.consent_proto,
            lobby.launch_deadline_ms,
            lobby.battle_id,
            now_ms,
        )

    async def finish_cancelled_trial_launch(
        self,
        request_id: int,
        receipt: Message,
        now_ms: int,
    ) -> tuple[bool, tuple[int, ...]]:
        """凭 match 签名取消终态回执释放 launch、launch_role 与大厅。返回 (released, roster_ids)。"""
        pre = await self.get_trial_launch(request_id)
        if pre is None:
            return False, ()
        orig = self.protocol.parse("guildpb.GuildTrialLaunchOriginal", pre.original_proto)
        roster_ids = tuple(int(r.player_id) for r in orig.roster)
        receipt_bytes = receipt.SerializeToString(deterministic=True)

        async def _op(tx: GuildTx) -> bool:
            await tx.lock_guild(int(orig.guild_id))
            rec = await tx.lock_trial_launch(request_id)
            if rec is None:
                return False
            if rec.state == TRIAL_LAUNCH_STATE_TERMINAL:
                return False
            battle_id = int(receipt.battle_id) or rec.battle_id
            await tx.update_trial_launch(
                request_id, battle_id, TRIAL_LAUNCH_STATE_TERMINAL, now_ms, cancel_receipt=receipt_bytes
            )
            for pid in roster_ids:
                await tx.update_trial_launch_role_state(pid, request_id, TRIAL_LAUNCH_ROLE_RELEASED)

            end_tip_id = ERR_GUILD_TRIAL_SERVICE_BUSY
            end_params: list[str] = []
            if int(receipt.battle_id) != 0:
                end_tip_id = ERR_GUILD_TRIAL_BATTLE_ABORTED
            elif rec.realtime_end_tip:
                try:
                    parsed_tip = self.protocol.parse("TipInfoMessage", rec.realtime_end_tip)
                    if int(parsed_tip.id) != 0:
                        end_tip_id = int(parsed_tip.id)
                        end_params = [str(p) for p in parsed_tip.parameters]
                except DecodeError:
                    pass
            await self._close_trial_lobby_tx(
                tx, request_id, end_tip_id=end_tip_id, end_params=end_params, now_ms=now_ms
            )
            return True

        released = await self._run_tx(_op)
        return released, roster_ids

    async def register_trial_battle(self, battle: TrialBattleKey, now_ms: int) -> None:
        """登记对局行（STARTED）并将对应 launch 推进为 LAUNCHED。"""
        if battle.battle_id == 0 or battle.guild_id == 0:
            raise TrialInputInvalidError("trial battle_id and guild_id must be non-zero")

        async def _op(tx: GuildTx) -> None:
            locked_g = await tx.lock_guild(battle.guild_id)
            if locked_g is None:
                raise GuildNotFoundError("guild not found")
            existing = await tx.get_trial_battle(battle.battle_id)
            if existing is not None:
                if existing.guild_id != battle.guild_id:
                    raise GuildWriteConflictError("battle registered to foreign guild")
                if existing.state == TRIAL_BATTLE_STATE_SETTLED:
                    return
            if battle.launch_request_id != 0:
                launch = await tx.lock_trial_launch(battle.launch_request_id)
                if launch is not None and launch.state == TRIAL_LAUNCH_STATE_PREPARED:
                    await tx.update_trial_launch(
                        battle.launch_request_id, battle.battle_id, TRIAL_LAUNCH_STATE_LAUNCHED, now_ms
                    )
            if existing is None:
                await tx.insert_trial_battle(
                    TrialBattleRecord(
                        battle_id=battle.battle_id,
                        guild_id=battle.guild_id,
                        activity_id=battle.activity_id,
                        period_key=battle.period_key,
                        guild_period_key=battle.guild_period_key,
                        initiator_player_id=battle.initiator_player_id,
                        state=TRIAL_BATTLE_STATE_STARTED,
                        settle_result=TRIAL_SETTLE_RESULT_UNSPECIFIED,
                        rewarded_count=0,
                        created_ms=now_ms,
                        settled_ms=0,
                        launch_request_id=battle.launch_request_id,
                    )
                )

        await self._run_tx(_op)

    async def get_trial_battle(self, battle_id: int) -> TrialBattleRecord | None:
        return await self._run_tx(lambda tx: tx.get_trial_battle(battle_id), read_only=True)

    async def list_overdue_trial_battles(self, created_before_ms: int, limit: int = 200) -> list[TrialBattleRecord]:
        return await self._run_tx(
            lambda tx: tx.list_overdue_trial_battles(created_before_ms, limit), read_only=True
        )

    async def expire_trial_battle(self, battle_id: int, guild_id: int, now_ms: int) -> bool:
        async def _op(tx: GuildTx) -> bool:
            if await tx.lock_guild(guild_id) is None:
                return False
            row = await tx.lock_trial_battle(battle_id)
            if row is None or row.guild_id != guild_id or row.state != TRIAL_BATTLE_STATE_STARTED:
                return False
            await tx.update_trial_battle(battle_id, TRIAL_BATTLE_STATE_EXPIRED, row.settle_result, 0, now_ms)
            return True

        return await self._run_tx(_op)

    async def mark_trial_battle_poison(
        self, battle: TrialBattleKey, finished_at_ms: int, now_ms: int
    ) -> tuple[str, int, tuple[int, ...]]:
        """将确定性失败的对局标记为 SETTLED/POISON，返回 (outcome, release_req_id, release_players)。"""
        created_ms = finished_at_ms or now_ms

        async def _op(tx: GuildTx) -> tuple[str, int, tuple[int, ...]]:
            launch_rec: TrialLaunchRecord | None = None
            req_id = battle.launch_request_id
            if req_id == 0 and battle.battle_id != 0:
                launch_rec = await tx.get_trial_launch_by_battle(battle.battle_id)
                if launch_rec is not None:
                    req_id = launch_rec.request_id
            elif req_id != 0:
                launch_rec = await tx.get_trial_launch(req_id)

            roster_ids: tuple[int, ...] = ()
            if launch_rec is not None:
                orig = self.protocol.parse("guildpb.GuildTrialLaunchOriginal", launch_rec.original_proto)
                roster_ids = tuple(int(r.player_id) for r in orig.roster)

            locked_g = await tx.lock_guild(battle.guild_id)
            if locked_g is None:
                if launch_rec is not None and req_id != 0:
                    await tx.update_trial_launch(req_id, battle.battle_id, TRIAL_LAUNCH_STATE_TERMINAL, now_ms)
                    for pid in roster_ids:
                        await tx.update_trial_launch_role_state(pid, req_id, TRIAL_LAUNCH_ROLE_RELEASED)
                    await self._close_trial_lobby_tx(tx, req_id, end_tip_id=0, end_params=(), now_ms=now_ms)
                return "guild_gone", req_id, roster_ids

            existing = await tx.lock_trial_battle(battle.battle_id)
            if existing is not None:
                if existing.guild_id != battle.guild_id:
                    return "mismatch", 0, ()
                if existing.state == TRIAL_BATTLE_STATE_SETTLED:
                    return "already_settled", 0, ()
                await tx.update_trial_battle(
                    battle.battle_id, TRIAL_BATTLE_STATE_SETTLED, TRIAL_SETTLE_RESULT_POISON, 0, now_ms
                )
            elif battle.activity_id != 0 and battle.period_key != 0 and battle.guild_period_key != 0:
                await tx.insert_trial_battle(
                    TrialBattleRecord(
                        battle_id=battle.battle_id,
                        guild_id=battle.guild_id,
                        activity_id=battle.activity_id,
                        period_key=battle.period_key,
                        guild_period_key=battle.guild_period_key,
                        initiator_player_id=battle.initiator_player_id,
                        state=TRIAL_BATTLE_STATE_SETTLED,
                        settle_result=TRIAL_SETTLE_RESULT_POISON,
                        rewarded_count=0,
                        created_ms=created_ms,
                        settled_ms=now_ms,
                        launch_request_id=req_id,
                    )
                )
            else:
                return "missing", 0, ()

            if launch_rec is not None and req_id != 0:
                await tx.update_trial_launch(req_id, battle.battle_id, TRIAL_LAUNCH_STATE_TERMINAL, now_ms)
                for pid in roster_ids:
                    await tx.update_trial_launch_role_state(pid, req_id, TRIAL_LAUNCH_ROLE_RELEASED)
                await self._close_trial_lobby_tx(tx, req_id, end_tip_id=0, end_params=(), now_ms=now_ms)
            return "marked", req_id, roster_ids

        return await self._run_tx(_op)

    async def settle_trial_battle(
        self,
        *,
        battle: TrialBattleKey,
        row: ActivityRuleRow | None,
        win: bool,
        candidates: Sequence[int],
        reward_payload: bytes,
        reward_ops: Sequence[TrialRewardOp],
        lease_until_ms: int,
        finished_at_ms: int,
        now_ms: int,
    ) -> TrialSettleResult:
        """同道历练权威结算事务：单事务完成幂等判重、成员帮贡、帮会资金（含日上限）、物品入队/溢出 owed、对局/launch/大厅终结。"""
        if (
            battle.battle_id == 0
            or battle.guild_id == 0
            or battle.activity_id == 0
            or battle.period_key == 0
            or battle.guild_period_key == 0
        ):
            raise TrialInputInvalidError("incomplete trial battle key")
        sorted_candidates = sorted({int(pid) for pid in candidates})
        if sorted_candidates and sorted_candidates[0] == 0:
            raise TrialInputInvalidError("candidate player_id 0")

        if row is None:
            settle_result = TRIAL_SETTLE_RESULT_CONFIG_MISSING
        elif win:
            settle_result = TRIAL_SETTLE_RESULT_WIN
        else:
            settle_result = TRIAL_SETTLE_RESULT_LOSS

        should_grant = win and row is not None and len(sorted_candidates) > 0
        ops_by_pid = {op.player_id: op for op in reward_ops}
        if should_grant and reward_payload:
            for pid in sorted_candidates:
                if pid not in ops_by_pid:
                    raise TrialInputInvalidError(f"missing reward op for candidate {pid}")

        async def _op(tx: GuildTx) -> TrialSettleResult:
            launch_rec: TrialLaunchRecord | None = None
            req_id = battle.launch_request_id
            if req_id == 0:
                launch_rec = await tx.get_trial_launch_by_battle(battle.battle_id)
                if launch_rec is not None:
                    req_id = launch_rec.request_id
            else:
                launch_rec = await tx.lock_trial_launch(req_id)

            roster_ids: tuple[int, ...] = ()
            if launch_rec is not None:
                orig = self.protocol.parse("guildpb.GuildTrialLaunchOriginal", launch_rec.original_proto)
                roster_ids = tuple(int(r.player_id) for r in orig.roster)
                roster_set = set(roster_ids)
                for pid in sorted_candidates:
                    if pid not in roster_set:
                        raise GuildWriteConflictError(f"candidate {pid} not in original launch roster")

            locked_g = await tx.lock_guild(battle.guild_id)
            if locked_g is None:
                if launch_rec is not None and req_id != 0:
                    await tx.update_trial_launch(req_id, battle.battle_id, TRIAL_LAUNCH_STATE_TERMINAL, now_ms)
                    for pid in roster_ids:
                        await tx.update_trial_launch_role_state(pid, req_id, TRIAL_LAUNCH_ROLE_RELEASED)
                    await self._close_trial_lobby_tx(tx, req_id, end_tip_id=0, end_params=(), now_ms=now_ms)
                return TrialSettleResult(
                    status="guild_gone",
                    release_request_id=req_id,
                    release_players=roster_ids,
                )

            existing = await tx.lock_trial_battle(battle.battle_id)
            if existing is not None:
                if existing.guild_id != battle.guild_id:
                    return TrialSettleResult(status="context_mismatch")
                if existing.state == TRIAL_BATTLE_STATE_SETTLED:
                    return TrialSettleResult(status="duplicate")

            rewarded_members: list[MemberData] = []
            enqueued: list[TrialEnqueuedReward] = []
            owed: list[int] = []
            funds_granted = False
            counted_wins = 0

            if should_grant and row is not None:
                # 周期键必须晚于清理窗口下界（now - 7 天），避免与清理碰同一行
                oldest_day = day_key(max(0, now_ms - 7 * MS_PER_DAY))
                if battle.period_key <= oldest_day:
                    raise TrialActivityPoisonError(
                        f"trial result period_key {battle.period_key} too old at {now_ms}"
                    )

                # M -> Q -> 读 C -> O -> C -> P -> G/M
                members: list[MemberData] = []
                for pid in sorted_candidates:
                    m = await tx.lock_member(battle.guild_id, pid)
                    if m is not None:
                        members.append(m)

                for m in members:
                    await tx.ensure_and_lock_seq(m.player_id, ASSET_STREAM_GUILD_CREDIT, now_ms)

                for m in members:
                    used = await tx.get_activity_counter_value(m.player_id, battle.activity_id, battle.period_key)
                    if used < row.daily_limit:
                        rewarded_members.append(m)

                gain = row.personal_contribution
                for m in rewarded_members:
                    if m.contribution_total > UINT64_MAX - gain or m.contribution_balance > UINT64_MAX - gain:
                        raise TrialActivityPoisonError(f"member {m.player_id} contribution overflow")

                if reward_payload:
                    for m in rewarded_members:
                        rw = ops_by_pid[m.player_id]
                        try:
                            seq, epoch = await tx.allocate_seq(m.player_id, ASSET_STREAM_GUILD_CREDIT, now_ms)
                        except GuildAssetPendingError:
                            owed.append(m.player_id)
                            continue
                        await tx.insert_asset_op(
                            AssetOpRow(
                                op_id=rw.op_id,
                                player_id=m.player_id,
                                stream=ASSET_STREAM_GUILD_CREDIT,
                                seq=seq,
                                guild_id=battle.guild_id,
                                kind=OP_KIND_ACTIVITY_REWARD,
                                status=OP_STATUS_PENDING,
                                durable=0,
                                attempts=0,
                                next_attempt_ms=lease_until_ms,
                                deadline_ms=0,
                                payload=reward_payload,
                                ref_id=battle.activity_id,
                                ref_count=1,
                                period_key=battle.period_key,
                                contribution_delta=0,
                                funds_delta=0,
                                reason_tip_id=0,
                                created_ms=now_ms,
                                updated_ms=now_ms,
                                lease_until_ms=lease_until_ms,
                                lease_token=rw.lease_token,
                                tx_type=TX_GUILD_ACTIVITY_REWARD,
                                last_outcome=0,
                                last_reason=0,
                                stream_epoch=epoch,
                                last_send_ms=now_ms,
                            )
                        )
                        enqueued.append(
                            TrialEnqueuedReward(
                                player_id=m.player_id,
                                op_id=rw.op_id,
                                seq=seq,
                                stream_epoch=epoch,
                                lease_token=rw.lease_token,
                            )
                        )

                for m in rewarded_members:
                    if not await tx.upsert_counter_with_limit(
                        m.player_id,
                        COUNTER_KIND_ACTIVITY,
                        battle.activity_id,
                        battle.period_key,
                        1,
                        row.daily_limit,
                        now_ms,
                    ):
                        raise GuildWriteConflictError(f"trial counter changed concurrently player={m.player_id}")

                if rewarded_members:
                    if row.guild_funds > 0:
                        prog = await tx.upsert_and_lock_activity_progress(
                            battle.guild_id,
                            battle.activity_id,
                            battle.guild_period_key,
                            increment=False,
                            now_ms=now_ms,
                        )
                        counted_wins = prog.count
                        if trial_funds_counted(row, prog.count):
                            if locked_g.funds > UINT64_MAX - row.guild_funds:
                                raise TrialActivityPoisonError(f"guild {battle.guild_id} funds overflow")
                            await tx.set_guild_funds(battle.guild_id, locked_g.funds + row.guild_funds)
                            counted_wins = prog.count + 1
                            await tx.set_trial_progress_count(
                                battle.guild_id, battle.activity_id, battle.guild_period_key, counted_wins, now_ms
                            )
                            funds_granted = True
                    if gain > 0:
                        for m in rewarded_members:
                            await tx.set_member_contribution(
                                battle.guild_id,
                                m.player_id,
                                m.contribution_total + gain,
                                m.contribution_balance + gain,
                            )

            created_ms = finished_at_ms or now_ms
            if existing is None:
                await tx.insert_trial_battle(
                    TrialBattleRecord(
                        battle_id=battle.battle_id,
                        guild_id=battle.guild_id,
                        activity_id=battle.activity_id,
                        period_key=battle.period_key,
                        guild_period_key=battle.guild_period_key,
                        initiator_player_id=battle.initiator_player_id,
                        state=TRIAL_BATTLE_STATE_SETTLED,
                        settle_result=settle_result,
                        rewarded_count=len(rewarded_members),
                        created_ms=created_ms,
                        settled_ms=now_ms,
                        launch_request_id=req_id,
                    )
                )
            else:
                await tx.update_trial_battle(
                    battle.battle_id,
                    TRIAL_BATTLE_STATE_SETTLED,
                    settle_result,
                    len(rewarded_members),
                    now_ms,
                )

            for owed_pid in owed:
                await tx.insert_trial_reward_owed(
                    TrialOwedRewardRecord(
                        player_id=owed_pid,
                        battle_id=battle.battle_id,
                        guild_id=battle.guild_id,
                        activity_id=battle.activity_id,
                        period_key=battle.period_key,
                        payload=reward_payload,
                        attempts=0,
                        created_ms=now_ms,
                    )
                )

            if launch_rec is not None and req_id != 0:
                await tx.update_trial_launch(req_id, battle.battle_id, TRIAL_LAUNCH_STATE_TERMINAL, now_ms)
                for pid in roster_ids:
                    await tx.update_trial_launch_role_state(pid, req_id, TRIAL_LAUNCH_ROLE_RELEASED)
                await self._close_trial_lobby_tx(tx, req_id, end_tip_id=0, end_params=(), now_ms=now_ms)

            return TrialSettleResult(
                status="settled",
                result=settle_result,
                rewarded=tuple(m.player_id for m in rewarded_members),
                enqueued=tuple(enqueued),
                owed=tuple(owed),
                funds_granted=funds_granted,
                counted_wins=counted_wins,
                release_request_id=req_id,
                release_players=roster_ids,
            )

        return await self._run_tx(_op)

    async def list_trial_rewards_owed(self, limit: int = 50) -> list[TrialOwedRewardRecord]:
        return await self._run_tx(lambda tx: tx.list_trial_rewards_owed(limit), read_only=True)

    async def count_trial_rewards_owed_by_player(self, player_id: int) -> dict[int, int]:
        return await self._run_tx(lambda tx: tx.count_trial_rewards_owed_by_player(player_id), read_only=True)

    async def convert_owed_reward(
        self, owed: TrialOwedRewardRecord, op_id: int, now_ms: int
    ) -> tuple[str, AssetOpRow | None]:
        """将待入队奖励转为一行 PENDING 资产指令并删除 owed 行；返回 ('converted'|'still_full'|'gone', op)。"""

        async def _op(tx: GuildTx) -> tuple[str, AssetOpRow | None]:
            try:
                seq, epoch = await tx.allocate_seq(owed.player_id, ASSET_STREAM_GUILD_CREDIT, now_ms)
            except GuildAssetPendingError:
                raise _TxRollbackReturn(("still_full", None))
            if not await tx.delete_trial_reward_owed(owed.player_id, owed.battle_id):
                raise _TxRollbackReturn(("gone", None))
            row = AssetOpRow(
                op_id=op_id,
                player_id=owed.player_id,
                stream=ASSET_STREAM_GUILD_CREDIT,
                seq=seq,
                guild_id=owed.guild_id,
                kind=OP_KIND_ACTIVITY_REWARD,
                status=OP_STATUS_PENDING,
                durable=0,
                attempts=0,
                next_attempt_ms=now_ms,
                deadline_ms=0,
                payload=owed.payload,
                ref_id=owed.activity_id,
                ref_count=1,
                period_key=owed.period_key,
                contribution_delta=0,
                funds_delta=0,
                reason_tip_id=0,
                created_ms=now_ms,
                updated_ms=now_ms,
                lease_until_ms=0,
                lease_token=0,
                tx_type=TX_GUILD_ACTIVITY_REWARD,
                last_outcome=0,
                last_reason=0,
                stream_epoch=epoch,
                last_send_ms=now_ms,
            )
            await tx.insert_asset_op(row)
            return "converted", row

        status, op_row = await self._run_tx(_op)
        if status == "still_full":
            await self._run_tx(lambda tx: tx.increment_trial_reward_owed_attempts(owed.player_id, owed.battle_id))
        return status, op_row

    # -- 帮战待入账行 guild_war_credit_owed（r2-gw#5 / #6）----------------------------------------

    async def credit_or_owe_war_op(
        self,
        credit: WarCreditOwed,
        now_ms: int,
        *,
        lease_until_ms: int = 0,
        lease_token: int = 0,
    ) -> tuple[AssetOpRow | None, WarCreditOwed | None, int]:
        """帮战入账分流：正常插 GUILD_CREDIT 指令；冻结或本流未决已满则记 guild_war_credit_owed；墓碑则放弃。"""
        if credit.player_id <= 0 or credit.op_id <= 0 or not credit.payload or now_ms <= 0:
            raise ValueError("war credit requires non-zero player_id, op_id, payload, and now_ms")
        healed = False
        fence_enabled = self._role_directory is not None

        while True:
            need_heal_pid = 0

            async def _op(tx: GuildTx) -> tuple[AssetOpRow | None, WarCreditOwed | None, int]:
                nonlocal need_heal_pid
                fence = await tx.lock_role_fence(credit.player_id)
                # 独立内存夹具未装配目录时，若已显式写入本地护栏行仍遵守护栏状态。
                effective_enabled = fence_enabled or (fence is not None)
                verdict, need_heal = classify_credit_fence(
                    fence, healed=healed, fence_enabled=effective_enabled,
                )
                if need_heal and fence_enabled:
                    need_heal_pid = credit.player_id
                    raise _TxRollbackReturn((None, None, CreditFenceVerdict.OPEN))
                if need_heal and not fence_enabled:
                    # 无外部目录可自愈：本地已有冻结令牌直接按 FROZEN 分流。
                    verdict = CreditFenceVerdict.FROZEN if (fence is not None and fence.freeze_token != 0) else CreditFenceVerdict.OPEN
                if verdict == CreditFenceVerdict.REMOVED:
                    return None, None, CreditFenceVerdict.REMOVED
                stamped = replace(credit, created_ms=now_ms)
                if verdict == CreditFenceVerdict.FROZEN:
                    await tx.insert_war_credit_owed(stamped)
                    return None, stamped, CreditFenceVerdict.FROZEN
                try:
                    seq, epoch = await tx.allocate_seq(credit.player_id, ASSET_STREAM_GUILD_CREDIT, now_ms)
                except GuildAssetPendingError:
                    await tx.insert_war_credit_owed(stamped)
                    return None, stamped, CreditFenceVerdict.OPEN
                row = AssetOpRow(
                    op_id=credit.op_id,
                    player_id=credit.player_id,
                    stream=ASSET_STREAM_GUILD_CREDIT,
                    seq=seq,
                    guild_id=credit.guild_id,
                    kind=credit.kind,
                    status=OP_STATUS_PENDING,
                    durable=0,
                    attempts=0,
                    next_attempt_ms=now_ms,
                    deadline_ms=0,
                    payload=credit.payload,
                    ref_id=credit.ref_id,
                    ref_count=1,
                    period_key=credit.period_key,
                    contribution_delta=0,
                    funds_delta=0,
                    reason_tip_id=0,
                    created_ms=now_ms,
                    updated_ms=now_ms,
                    lease_until_ms=lease_until_ms,
                    lease_token=lease_token,
                    tx_type=credit.tx_type,
                    last_outcome=0,
                    last_reason=0,
                    stream_epoch=epoch,
                    last_send_ms=0,
                )
                await tx.insert_asset_op(row)
                return row, None, CreditFenceVerdict.OPEN

            result = await self._run_tx(_op)
            if need_heal_pid:
                healed = True
                await self._heal_role_fence(need_heal_pid)
                continue
            return result

    async def list_war_credit_owed(
        self, after: WarCreditOwedCursor = WarCreditOwedCursor(), limit: int = 64
    ) -> list[WarCreditOwed]:
        if limit <= 0:
            return []
        return await self._run_tx(lambda tx: tx.list_war_credit_owed(after, limit), read_only=True)

    async def count_war_credit_owed(self, limit: int = 10_000) -> int:
        """普通读帮战待入账积压行数，至多数到 limit（对齐 Go WarRepo.CountWarCreditOwed）。"""
        if limit <= 0:
            return 0
        return await self._run_tx(lambda tx: tx.count_war_credit_owed(limit), read_only=True)

    async def convert_war_credit_owed(self, owed: WarCreditOwed, now_ms: int) -> tuple[int, AssetOpRow | None]:
        """将一行 guild_war_credit_owed 原子转为 guild_asset_op（保留原 op_id 幂等防重）并点删待入账行。"""
        if owed.player_id <= 0 or owed.op_id <= 0 or not owed.payload or now_ms <= 0:
            raise ValueError("war credit owed requires non-zero player_id, op_id, payload, and now_ms")
        healed = False
        fence_enabled = self._role_directory is not None

        while True:
            need_heal_pid = 0

            async def _op(tx: GuildTx) -> tuple[int, AssetOpRow | None]:
                nonlocal need_heal_pid
                fence = await tx.lock_role_fence(owed.player_id)
                effective_enabled = fence_enabled or (fence is not None)
                verdict, need_heal = classify_credit_fence(
                    fence, healed=healed, fence_enabled=effective_enabled,
                )
                if need_heal and fence_enabled:
                    need_heal_pid = owed.player_id
                    raise _TxRollbackReturn((0, None))
                if need_heal and not fence_enabled:
                    verdict = CreditFenceVerdict.FROZEN if (fence is not None and fence.freeze_token != 0) else CreditFenceVerdict.OPEN
                if verdict == CreditFenceVerdict.REMOVED:
                    removed = await tx.delete_war_credit_owed(owed.player_id, owed.op_id)
                    if not removed:
                        raise _TxRollbackReturn((WarOwedConvertStatus.GONE, None))
                    return WarOwedConvertStatus.ABANDONED, None
                if verdict == CreditFenceVerdict.FROZEN:
                    raise _TxRollbackReturn((WarOwedConvertStatus.FROZEN, None))
                try:
                    seq, epoch = await tx.allocate_seq(owed.player_id, ASSET_STREAM_GUILD_CREDIT, now_ms)
                except GuildAssetPendingError:
                    raise _TxRollbackReturn((WarOwedConvertStatus.STILL_FULL, None))
                if not await tx.war_credit_owed_exists(owed.player_id, owed.op_id):
                    raise _TxRollbackReturn((WarOwedConvertStatus.GONE, None))
                row = AssetOpRow(
                    op_id=owed.op_id,
                    player_id=owed.player_id,
                    stream=ASSET_STREAM_GUILD_CREDIT,
                    seq=seq,
                    guild_id=owed.guild_id,
                    kind=owed.kind,
                    status=OP_STATUS_PENDING,
                    durable=0,
                    attempts=0,
                    next_attempt_ms=now_ms,
                    deadline_ms=0,
                    payload=owed.payload,
                    ref_id=owed.ref_id,
                    ref_count=1,
                    period_key=owed.period_key,
                    contribution_delta=0,
                    funds_delta=0,
                    reason_tip_id=0,
                    created_ms=now_ms,
                    updated_ms=now_ms,
                    lease_until_ms=0,
                    lease_token=0,
                    tx_type=owed.tx_type,
                    last_outcome=0,
                    last_reason=0,
                    stream_epoch=epoch,
                    last_send_ms=0,
                )
                await tx.insert_asset_op(row)
                if not await tx.delete_war_credit_owed(owed.player_id, owed.op_id):
                    raise _TxRollbackReturn((WarOwedConvertStatus.GONE, None))
                return WarOwedConvertStatus.CONVERTED, row

            status, op_row = await self._run_tx(_op)
            if need_heal_pid:
                healed = True
                await self._heal_role_fence(need_heal_pid)
                continue
            return status, op_row

    # -- GuildInternal.ListAppliedAssetOpsSince --------------------------------------------------

    async def list_applied_asset_ops_since(
        self,
        *,
        zone_id: int,
        player_ids: Sequence[int],
        since_ms: int,
        after_op_id: int,
        limit: int,
    ) -> tuple[list[AssetOpRow], int]:
        if not 1 <= len(player_ids) <= MAX_APPLIED_OPS_PLAYER_IDS:
            raise ValueError("player_ids count out of range")
        if since_ms <= 0 or not 1 <= limit <= MAX_APPLIED_OPS_PAGE_LIMIT:
            raise ValueError("invalid since_ms or limit")
        zones = self._merged_zones.zone_members(zone_id) if zone_id != 0 else ()
        return await self._run_tx(
            lambda tx: tx.list_applied_asset_ops_since(player_ids, since_ms, after_op_id, limit, zones),
            read_only=True,
        )

    # -- RoleFenceParticipant --------------------------------------------------------------------

    async def prepare_role_freeze(self, state_msg: Message, *, dry_run: bool) -> PrepareFreezeOutcome:
        state = role_fence_from_proto(state_msg)
        if not valid_role_fence_state(state):
            raise InvalidRoleFenceStateError("invalid RoleFenceState")
        if not dry_run and (state.freeze_token == 0 or state.account == ""):
            raise InvalidRoleFenceStateError("PrepareRoleFreeze requires active account and non-zero freeze_token")
        now_ms = self._clock_ms()

        async def _op(tx: GuildTx) -> PrepareFreezeOutcome:
            cur = await tx.lock_role_fence(state.player_id)
            if cur is not None and cur.version > state.version:
                raise _TxRollbackReturn(PrepareFreezeOutcome(stale=True))
            if not dry_run:
                await tx.apply_role_fence(state, now_ms)
            blocker = await tx.role_fence_pending_blocker(state, dry_run=dry_run)
            if blocker:
                raise _TxRollbackReturn(PrepareFreezeOutcome(clean=False, blocker=blocker))
            if dry_run:
                raise _TxRollbackReturn(PrepareFreezeOutcome(clean=True))
            return PrepareFreezeOutcome(clean=True)

        return await self._run_tx(_op)

    async def apply_role_fence(self, state_msg: Message) -> None:
        state = role_fence_from_proto(state_msg)
        if not valid_role_fence_state(state):
            raise InvalidRoleFenceStateError("invalid RoleFenceState")
        now_ms = self._clock_ms()
        await self._run_tx(lambda tx: tx.apply_role_fence(state, now_ms))

    async def purge_removed_role(self, state_msg: Message) -> RolePurgeResult | None:
        """删角收尾清数据：墓碑版本合并后若为成员则退帮/自动让位/单人解散，返回提交结果；未达墓碑返回 None。"""
        state = role_fence_from_proto(state_msg)
        if not valid_role_fence_state(state) or state.account != "":
            raise InvalidRoleFenceStateError("PurgeRemovedRole requires tombstone RoleFenceState (empty account)")
        now_ms = self._clock_ms()
        await self._run_tx(lambda tx: tx.apply_role_fence(state, now_ms))

        async def _op(tx: GuildTx) -> RolePurgeResult | None:
            fence = await tx.lock_role_fence(state.player_id)
            if fence is None or fence.account != "":
                return None

            player_id = state.player_id
            guild_id = await tx.get_player_guild_id(player_id)
            locked: _GuildLockRow | None = None
            if guild_id != 0:
                locked = await tx.lock_guild(guild_id)
                if locked is None:
                    guild_id = 0

            await tx.lock_player_state(player_id)

            res = RolePurgeResult(player_id=player_id)
            if guild_id != 0 and locked is not None:
                role = await tx.get_member_role(guild_id, player_id)
                if role is not None:
                    if role_rank(role) != RANK_LEADER and locked.leader_id != player_id:
                        member = await tx.lock_member(guild_id, player_id)
                        if member is None:
                            raise StoreUnavailableError("member vanished under guild lock")
                        await tx.delete_member(guild_id, player_id)
                        remaining = tuple(sorted(await tx.list_member_ids(guild_id)))
                        res = RolePurgeResult(
                            player_id=player_id,
                            guild_id=guild_id,
                            zone_id=locked.zone_id,
                            members=remaining,
                        )
                    else:
                        candidates = [m for m in await tx.list_members(guild_id) if m.player_id != player_id]
                        if not candidates:
                            await tx.ensure_player_state(player_id, now_ms)
                            await self._disband_locked_guild(tx, guild_id, now_ms)
                            res = RolePurgeResult(
                                player_id=player_id,
                                guild_id=guild_id,
                                zone_id=locked.zone_id,
                                disbanded=True,
                            )
                        else:
                            successor = min(candidates, key=better_successor_key).player_id
                            await self._lock_member_pair(tx, guild_id, player_id, successor)
                            await tx.update_guild_leader(guild_id, successor)
                            await tx.update_member_role(guild_id, successor, ROLE_LEADER)
                            await tx.delete_member(guild_id, player_id)
                            if await tx.count_members_by_role(guild_id, ROLE_LEADER) != 1:
                                raise GuildLeaderMismatchError("expected 1 leader after handover")
                            remaining = tuple(sorted(await tx.list_member_ids(guild_id)))
                            res = RolePurgeResult(
                                player_id=player_id,
                                guild_id=guild_id,
                                zone_id=locked.zone_id,
                                successor=successor,
                                members=remaining,
                            )

            await tx.delete_player_state(player_id)
            await tx.delete_applications_of_player(player_id)
            await tx.purge_player_personal_tables(player_id)
            return res

        return await self._run_tx(_op)


# -- 确定性内存双胞胎（MemoryGuildStore） -------------------------------------------------------


@dataclass
class _MemoryState:
    role_fences: dict[int, RoleFenceRow]
    guilds: dict[int, GuildData]
    player_states: dict[int, int]
    members_by_player: dict[int, tuple[int, MemberData]]
    applications: dict[tuple[int, int], tuple[int, int]]  # (guild_id, player_id) -> (apply_ms, expire_ms)
    seqs: dict[tuple[int, int], tuple[int, int, int]]  # (player_id, stream) -> (next_seq, epoch, updated_ms)
    asset_ops: dict[int, AssetOpRow]
    daily_counters: dict[tuple[int, int, int, int], tuple[int, int]]  # (pid, kind, ref, period) -> (used, updated_ms)
    activity_progress: dict[tuple[int, int, int], ActivityProgress]  # (gid, act_id, period) -> progress
    trial_lobbies: dict[int, TrialLobbyRecord]  # lobby_id -> record
    trial_lobby_slots: dict[int, TrialLobbySlotRecord]  # player_id -> slot
    trial_launches: dict[int, TrialLaunchRecord]  # request_id -> record
    trial_launch_roles: dict[tuple[int, int], TrialLaunchRoleRecord]  # (player_id, request_id) -> record
    trial_battles: dict[int, TrialBattleRecord]  # battle_id -> record
    trial_reward_owed: dict[tuple[int, int], TrialOwedRewardRecord]  # (player_id, battle_id) -> record
    war_credit_owed: dict[tuple[int, int], WarCreditOwed]  # (player_id, op_id) -> record
    client_requests: dict[tuple[int, int], tuple[int, int]]  # (player_id, req_id) -> (op_id, created_ms)
    name_holds: dict[str, tuple[int, int, int]]  # name_norm -> (hold_id, guild_id, held_until_ms)

    @classmethod
    def empty(cls) -> "_MemoryState":
        return cls(
            role_fences={},
            guilds={},
            player_states={},
            members_by_player={},
            applications={},
            seqs={},
            asset_ops={},
            daily_counters={},
            activity_progress={},
            trial_lobbies={},
            trial_lobby_slots={},
            trial_launches={},
            trial_launch_roles={},
            trial_battles={},
            trial_reward_owed={},
            war_credit_owed={},
            client_requests={},
            name_holds={},
        )


class _MemoryTx:
    """基于深拷贝快照的内存事务句柄（异常即整体回滚）。"""

    def __init__(self, state: _MemoryState) -> None:
        self._s = state

    # role_fence
    async def get_role_fence(self, player_id: int) -> RoleFenceRow | None:
        return self._s.role_fences.get(player_id)

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        return self._s.role_fences.get(player_id)

    async def apply_role_fence(self, state: RoleFenceRow, now_ms: int) -> bool:
        cur = self._s.role_fences.get(state.player_id)
        if cur is not None and cur.version >= state.version:
            return False
        self._s.role_fences[state.player_id] = replace(state, updated_ms=now_ms)
        return True

    async def role_fence_pending_blocker(self, state: RoleFenceRow, *, dry_run: bool) -> str:
        pid = state.player_id
        pair = self._s.members_by_player.get(pid)
        if pair is not None:
            _, member = pair
            if not is_removal_token(state.freeze_token):
                return ROLE_BLOCKER_GUILD_MEMBER
            if dry_run and role_rank(member.role) == RANK_LEADER:
                return ROLE_BLOCKER_GUILD_LEADER
        for op in self._s.asset_ops.values():
            if op.player_id == pid and op.status not in (
                OP_STATUS_APPLIED,
                OP_STATUS_REJECTED,
                OP_STATUS_ABORTED,
                OP_STATUS_ABANDONED,
            ):
                return ROLE_BLOCKER_ASSET_OP
        for owed_pid, _ in self._s.trial_reward_owed:
            if owed_pid == pid:
                return ROLE_BLOCKER_TRIAL_REWARD_OWED
        for (l_pid, req_id), role_rec in self._s.trial_launch_roles.items():
            if l_pid != pid:
                continue
            launch = self._s.trial_launches.get(req_id)
            if role_rec.state != TRIAL_LAUNCH_ROLE_RELEASED or launch is None or launch.state != TRIAL_LAUNCH_STATE_TERMINAL:
                return ROLE_BLOCKER_TRIAL_LAUNCH_ROLE
        slot = self._s.trial_lobby_slots.get(pid)
        if slot is not None and slot.lobby_id != 0:
            lob = self._s.trial_lobbies.get(slot.lobby_id)
            if lob is None or lob.state != TRIAL_LOBBY_STATE_ENDED:
                return ROLE_BLOCKER_TRIAL_LOBBY_SLOT
        for owed_pid, _ in self._s.war_credit_owed:
            if owed_pid == pid:
                return ROLE_BLOCKER_WAR_CREDIT_OWED
        return ""

    # guild_player_state
    async def ensure_player_state(self, player_id: int, now_ms: int) -> None:
        self._s.player_states.setdefault(player_id, now_ms)

    async def lock_player_state(self, player_id: int) -> bool:
        return player_id in self._s.player_states

    async def delete_player_state(self, player_id: int) -> None:
        self._s.player_states.pop(player_id, None)

    # guild
    async def load_guild(self, guild_id: int) -> GuildData | None:
        g = self._s.guilds.get(guild_id)
        if g is None:
            return None
        members = sorted(
            (m for gid, m in self._s.members_by_player.values() if gid == guild_id),
            key=lambda m: m.player_id,
        )
        return replace(g, members=tuple(members))

    async def lock_guild(self, guild_id: int) -> _GuildLockRow | None:
        g = self._s.guilds.get(guild_id)
        if g is None:
            return None
        return _GuildLockRow(
            guild_id=g.guild_id,
            name=g.name,
            name_norm=g.name_norm,
            leader_id=g.leader_id,
            level=g.level,
            max_members=g.max_members,
            zone_id=g.zone_id,
            funds=g.funds,
            score=g.score,
            score_version=g.score_version,
            renamed_ms=g.renamed_ms,
        )

    async def get_guild_level(self, guild_id: int) -> int | None:
        g = self._s.guilds.get(guild_id)
        return None if g is None else g.level

    async def insert_guild(self, guild: GuildData, name_norm: str) -> None:
        if guild.guild_id in self._s.guilds:
            raise _DuplicateKeyError("duplicate guild_id", "PRIMARY")
        for other in self._s.guilds.values():
            if other.name_norm == name_norm:
                raise _DuplicateKeyError("duplicate name_norm", "uk_guild")
        self._s.guilds[guild.guild_id] = replace(guild, name_norm=name_norm, members=())

    async def update_guild_announcement(self, guild_id: int, announcement: str) -> None:
        g = self._s.guilds[guild_id]
        self._s.guilds[guild_id] = replace(g, announcement=announcement)

    async def update_guild_name(self, guild_id: int, name: str, name_norm: str) -> None:
        for oid, other in self._s.guilds.items():
            if oid != guild_id and other.name_norm == name_norm:
                raise _DuplicateKeyError("duplicate name_norm", "uk_guild")
        g = self._s.guilds[guild_id]
        self._s.guilds[guild_id] = replace(g, name=name, name_norm=name_norm)

    async def deduct_guild_funds_and_stamp_rename(self, guild_id: int, cost_funds: int, now_ms: int) -> bool:
        g = self._s.guilds.get(guild_id)
        if g is None or g.funds < cost_funds:
            return False
        self._s.guilds[guild_id] = replace(g, funds=g.funds - cost_funds, renamed_ms=now_ms)
        return True

    async def update_guild_leader(self, guild_id: int, new_leader_id: int) -> None:
        g = self._s.guilds[guild_id]
        self._s.guilds[guild_id] = replace(g, leader_id=new_leader_id)

    async def update_guild_score(self, guild_id: int, score: int, new_version: int) -> None:
        g = self._s.guilds[guild_id]
        self._s.guilds[guild_id] = replace(g, score=score, score_version=new_version)

    async def upgrade_guild_row(
        self, guild_id: int, cur_level: int, new_level: int, cost_funds: int, next_max_members: int
    ) -> bool:
        g = self._s.guilds.get(guild_id)
        if g is None or g.level != cur_level or g.funds < cost_funds:
            return False
        self._s.guilds[guild_id] = replace(
            g, level=new_level, funds=g.funds - cost_funds, max_members=next_max_members
        )
        return True

    async def set_guild_funds(self, guild_id: int, new_funds: int) -> None:
        g = self._s.guilds[guild_id]
        self._s.guilds[guild_id] = replace(g, funds=new_funds)

    async def credit_guild_funds(self, guild_id: int, delta: int) -> bool:
        g = self._s.guilds.get(guild_id)
        if g is None:
            return False
        self._s.guilds[guild_id] = replace(g, funds=min(UINT64_MAX, g.funds + delta))
        return True

    async def delete_guild(self, guild_id: int) -> bool:
        return self._s.guilds.pop(guild_id, None) is not None

    async def list_guild_zones(self) -> list[int]:
        return sorted({g.zone_id for g in self._s.guilds.values() if g.zone_id > 0})

    async def list_rank_rows(self) -> list[tuple[int, int, int, int]]:
        return [(g.guild_id, g.zone_id, g.score, g.score_version) for g in self._s.guilds.values()]

    # guild_name_hold
    async def claim_name_hold(self, guild_id: int, name_norm: str, now_ms: int) -> bool:
        hold = self._s.name_holds.get(name_norm)
        if hold is None:
            return True
        _, holder_gid, held_until_ms = hold
        if holder_gid != guild_id and held_until_ms > now_ms:
            return False
        del self._s.name_holds[name_norm]
        return True

    async def upsert_name_hold(self, guild_id: int, old_norm: str, held_until_ms: int, hold_id: int) -> None:
        existing = self._s.name_holds.get(old_norm)
        hid = existing[0] if existing is not None else hold_id
        self._s.name_holds[old_norm] = (hid, guild_id, held_until_ms)

    # guild_member
    async def get_player_guild_id(self, player_id: int) -> int:
        pair = self._s.members_by_player.get(player_id)
        return 0 if pair is None else pair[0]

    async def get_member_role(self, guild_id: int, player_id: int) -> int | None:
        pair = self._s.members_by_player.get(player_id)
        if pair is None or pair[0] != guild_id:
            return None
        return pair[1].role

    async def get_member_contribution(self, guild_id: int, player_id: int) -> tuple[int, int] | None:
        pair = self._s.members_by_player.get(player_id)
        if pair is None or pair[0] != guild_id:
            return None
        return pair[1].contribution_total, pair[1].contribution_balance

    async def lock_member(self, guild_id: int, player_id: int) -> MemberData | None:
        pair = self._s.members_by_player.get(player_id)
        if pair is None or pair[0] != guild_id:
            return None
        return pair[1]

    async def list_member_ids(self, guild_id: int) -> list[int]:
        return sorted(m.player_id for gid, m in self._s.members_by_player.values() if gid == guild_id)

    async def list_members(self, guild_id: int) -> list[MemberData]:
        return sorted(
            (m for gid, m in self._s.members_by_player.values() if gid == guild_id),
            key=lambda m: m.player_id,
        )

    async def count_members(self, guild_id: int) -> int:
        return sum(1 for gid, _ in self._s.members_by_player.values() if gid == guild_id)

    async def count_members_by_role(self, guild_id: int, role: int) -> int:
        return sum(1 for gid, m in self._s.members_by_player.values() if gid == guild_id and m.role == role)

    async def insert_member(self, guild_id: int, member: MemberData) -> None:
        if member.player_id in self._s.members_by_player:
            raise _DuplicateKeyError("duplicate player_id", "uk_guild_member")
        self._s.members_by_player[member.player_id] = (guild_id, member)

    async def update_member_role(self, guild_id: int, player_id: int, role: int) -> None:
        gid, m = self._s.members_by_player[player_id]
        assert gid == guild_id
        self._s.members_by_player[player_id] = (gid, replace(m, role=role))

    async def set_member_contribution(self, guild_id: int, player_id: int, total: int, balance: int) -> None:
        gid, m = self._s.members_by_player[player_id]
        assert gid == guild_id
        self._s.members_by_player[player_id] = (
            gid,
            replace(m, contribution_total=total, contribution_balance=balance),
        )

    async def credit_member_contribution(self, guild_id: int, player_id: int, delta: int) -> bool:
        pair = self._s.members_by_player.get(player_id)
        if pair is None or pair[0] != guild_id:
            return False
        gid, m = pair
        self._s.members_by_player[player_id] = (
            gid,
            replace(
                m,
                contribution_total=min(UINT64_MAX, m.contribution_total + delta),
                contribution_balance=min(UINT64_MAX, m.contribution_balance + delta),
            ),
        )
        return True

    async def refund_member_contribution(self, guild_id: int, player_id: int, delta: int) -> bool:
        pair = self._s.members_by_player.get(player_id)
        if pair is None or pair[0] != guild_id:
            return False
        gid, m = pair
        self._s.members_by_player[player_id] = (
            gid,
            replace(m, contribution_balance=min(UINT64_MAX, m.contribution_balance + delta)),
        )
        return True

    async def debit_member_contribution(self, guild_id: int, player_id: int, cost: int) -> bool:
        pair = self._s.members_by_player.get(player_id)
        if pair is None or pair[0] != guild_id or pair[1].contribution_balance < cost:
            return False
        gid, m = pair
        self._s.members_by_player[player_id] = (
            gid,
            replace(m, contribution_balance=m.contribution_balance - cost),
        )
        return True

    async def delete_member(self, guild_id: int, player_id: int) -> bool:
        pair = self._s.members_by_player.get(player_id)
        if pair is None or pair[0] != guild_id:
            return False
        del self._s.members_by_player[player_id]
        return True

    async def get_trial_roster_members(self, guild_id: int, player_ids: Sequence[int]) -> dict[int, int]:
        out: dict[int, int] = {}
        for pid in player_ids:
            pair = self._s.members_by_player.get(int(pid))
            if pair is not None and pair[0] == guild_id:
                out[int(pid)] = pair[1].join_time_ms
        return out

    # guild_application
    async def lock_application_expire(self, guild_id: int, player_id: int) -> int | None:
        row = self._s.applications.get((guild_id, player_id))
        return None if row is None else row[1]

    async def insert_application(self, guild_id: int, player_id: int, apply_ms: int, expire_ms: int) -> bool:
        if (guild_id, player_id) in self._s.applications:
            return False
        self._s.applications[(guild_id, player_id)] = (apply_ms, expire_ms)
        return True

    async def refresh_application(self, guild_id: int, player_id: int, apply_ms: int, expire_ms: int) -> None:
        if (guild_id, player_id) in self._s.applications:
            self._s.applications[(guild_id, player_id)] = (apply_ms, expire_ms)

    async def delete_application(self, guild_id: int, player_id: int) -> bool:
        return self._s.applications.pop((guild_id, player_id), None) is not None

    async def delete_expired_application(self, guild_id: int, player_id: int, now_ms: int) -> bool:
        row = self._s.applications.get((guild_id, player_id))
        if row is not None and row[1] <= now_ms:
            del self._s.applications[(guild_id, player_id)]
            return True
        return False

    async def delete_live_application(self, guild_id: int, player_id: int, now_ms: int) -> bool:
        row = self._s.applications.get((guild_id, player_id))
        if row is not None and row[1] > now_ms:
            del self._s.applications[(guild_id, player_id)]
            return True
        return False

    async def delete_applications_of_player(self, player_id: int) -> None:
        for key in [k for k in self._s.applications if k[1] == player_id]:
            del self._s.applications[key]

    async def delete_applications_of_guild(self, guild_id: int) -> None:
        for key in [k for k in self._s.applications if k[0] == guild_id]:
            del self._s.applications[key]

    async def purge_expired_applications_of_player(self, player_id: int, now_ms: int) -> None:
        for key in [k for k, (_, exp) in self._s.applications.items() if k[1] == player_id and exp <= now_ms]:
            del self._s.applications[key]

    async def count_pending_applications_of_player(self, player_id: int, now_ms: int) -> int:
        return sum(1 for (_, pid), (_, exp) in self._s.applications.items() if pid == player_id and exp > now_ms)

    async def count_live_applications_of_guild(self, guild_id: int, now_ms: int) -> int:
        return sum(
            1
            for (gid, pid), (_, exp) in self._s.applications.items()
            if gid == guild_id and exp > now_ms and pid not in self._s.members_by_player
        )

    async def list_my_applications(self, player_id: int, now_ms: int, limit: int) -> list[ApplicationRow]:
        rows = [
            ApplicationRow(guild_id=gid, apply_ms=apply_ms, expire_ms=expire_ms)
            for (gid, pid), (apply_ms, expire_ms) in self._s.applications.items()
            if pid == player_id and expire_ms > now_ms
        ]
        rows.sort(key=lambda r: (-r.apply_ms, r.guild_id))
        return rows[:limit]

    async def list_applicants(self, guild_id: int, now_ms: int, limit: int) -> list[ApplicantRow]:
        rows = [
            ApplicantRow(player_id=pid, apply_ms=apply_ms, expire_ms=expire_ms)
            for (gid, pid), (apply_ms, expire_ms) in self._s.applications.items()
            if gid == guild_id and expire_ms > now_ms and pid not in self._s.members_by_player
        ]
        rows.sort(key=lambda r: (r.apply_ms, r.player_id))
        return rows[:limit]

    # asset ops & counters
    async def ensure_and_lock_seq(self, player_id: int, stream: int, now_ms: int) -> tuple[int, int]:
        key = (player_id, stream)
        if key not in self._s.seqs:
            self._s.seqs[key] = (1, now_ms, now_ms)
        next_seq, epoch, _ = self._s.seqs[key]
        return next_seq, epoch

    async def lock_seq_guard(self, player_id: int, stream: int) -> bool:
        return (player_id, stream) in self._s.seqs

    async def allocate_seq(self, player_id: int, stream: int, now_ms: int) -> tuple[int, int]:
        next_seq, epoch = await self.ensure_and_lock_seq(player_id, stream, now_ms)
        pending_seqs = [
            op.seq
            for op in self._s.asset_ops.values()
            if op.player_id == player_id
            and op.stream == stream
            and op.stream_epoch == epoch
            and op.status == OP_STATUS_PENDING
        ]
        if len(pending_seqs) >= ASSET_OP_MAX_PENDING:
            raise GuildAssetPendingError("too many pending asset ops")
        if pending_seqs and (next_seq - min(pending_seqs)) >= ASSET_OP_MAX_SPAN:
            raise GuildAssetPendingError("pending asset op seq span exceeded")
        self._s.seqs[(player_id, stream)] = (next_seq + 1, epoch, now_ms)
        return next_seq, epoch

    async def insert_asset_op(self, op: AssetOpRow) -> None:
        if op.op_id in self._s.asset_ops:
            raise _DuplicateKeyError("duplicate op_id", "PRIMARY")
        self._s.asset_ops[op.op_id] = op

    async def get_asset_op(self, op_id: int) -> AssetOpRow | None:
        return self._s.asset_ops.get(op_id)

    async def lock_asset_op(self, op_id: int) -> AssetOpRow | None:
        return self._s.asset_ops.get(op_id)

    async def list_due_asset_ops(
        self, now_ms: int, limit: int, owned_shards: frozenset[int] | None = None
    ) -> list[int]:
        if limit <= 0:
            return []
        eligible = [
            op
            for op in self._s.asset_ops.values()
            if op.status == OP_STATUS_PENDING
            and op.next_attempt_ms <= now_ms
            and op.lease_until_ms <= now_ms
            and (owned_shards is None or (op.player_id % 64) in owned_shards)
        ]
        fresh = [op for op in eligible if op.attempts < ASSET_OP_FRESH_ATTEMPT_LIMIT]
        aged = [op for op in eligible if op.attempts >= ASSET_OP_FRESH_ATTEMPT_LIMIT]
        fresh.sort(key=lambda o: (o.next_attempt_ms, o.op_id))
        aged.sort(key=lambda o: (o.next_attempt_ms, o.op_id))
        half = max(1, limit // 2)
        out: list[int] = [o.op_id for o in fresh[:half]]
        rem = limit - len(out)
        if rem > 0:
            out.extend(o.op_id for o in aged[:rem])
        rem = limit - len(out)
        if rem > 0:
            picked = set(out)
            for o in fresh[half:]:
                if o.op_id not in picked:
                    out.append(o.op_id)
                    if len(out) >= limit:
                        break
        return out

    async def claim_asset_op(
        self, op_id: int, now_ms: int, lease_until_ms: int, lease_token: int
    ) -> AssetOpRow | None:
        op = self._s.asset_ops.get(op_id)
        if op is None or op.status != OP_STATUS_PENDING or op.lease_until_ms > now_ms:
            return None
        updated = replace(
            op,
            lease_until_ms=lease_until_ms,
            lease_token=lease_token,
            updated_ms=now_ms,
        )
        self._s.asset_ops[op_id] = updated
        return updated

    async def mark_asset_op_poison(self, op_id: int, lease_token: int, poison_until_ms: int, now_ms: int) -> None:
        op = self._s.asset_ops.get(op_id)
        if op is None or op.status != OP_STATUS_PENDING or op.lease_token != lease_token:
            return
        self._s.asset_ops[op_id] = replace(
            op,
            next_attempt_ms=poison_until_ms,
            lease_until_ms=0,
            lease_token=0,
            updated_ms=now_ms,
        )

    async def reschedule_asset_op(
        self,
        op_id: int,
        lease_token: int,
        next_attempt_ms: int,
        outcome: int,
        reason: int,
        durable: bool,
        local: bool,
        now_ms: int,
    ) -> bool:
        op = self._s.asset_ops.get(op_id)
        if op is None or op.status != OP_STATUS_PENDING or op.lease_token != lease_token:
            return False
        self._s.asset_ops[op_id] = replace(
            op,
            attempts=op.attempts + 1,
            next_attempt_ms=next_attempt_ms,
            last_outcome=outcome,
            last_reason=reason,
            durable=1 if durable else 0,
            lease_until_ms=0,
            lease_token=0,
            last_send_ms=op.last_send_ms if local else now_ms,
            updated_ms=now_ms,
        )
        return True

    async def finalize_asset_op_cas(
        self,
        op_id: int,
        final_status: int,
        last_outcome: int,
        last_reason: int,
        reason_tip_id: int,
        now_ms: int,
    ) -> bool:
        op = self._s.asset_ops.get(op_id)
        if op is None or op.status != OP_STATUS_PENDING:
            return False
        self._s.asset_ops[op_id] = replace(
            op,
            status=final_status,
            durable=1,
            last_outcome=last_outcome,
            last_reason=last_reason,
            reason_tip_id=reason_tip_id,
            next_attempt_ms=now_ms,
            lease_until_ms=0,
            lease_token=0,
            updated_ms=now_ms,
        )
        return True

    async def abandon_asset_op_cas(self, op_id: int, lease_token: int, now_ms: int) -> bool:
        op = self._s.asset_ops.get(op_id)
        if op is None or op.status != OP_STATUS_PENDING or op.lease_token != lease_token:
            return False
        self._s.asset_ops[op_id] = replace(
            op,
            status=OP_STATUS_ABANDONED,
            lease_until_ms=0,
            lease_token=0,
            updated_ms=now_ms,
        )
        return True

    async def resolve_asset_op_manual_cas(
        self,
        op_id: int,
        final_status: int,
        operator: str,
        reason: str,
        now_ms: int,
        witness: QuietWitness | None = None,
    ) -> bool:
        op = self._s.asset_ops.get(op_id)
        if op is None or op.status != OP_STATUS_PENDING:
            return False
        if witness is not None and (
            op.lease_until_ms != witness.lease_until_ms
            or op.last_send_ms != witness.last_send_ms
            or op.attempts != witness.attempts
        ):
            return False
        self._s.asset_ops[op_id] = replace(
            op,
            status=final_status,
            resolved_by=operator,
            resolve_reason=reason,
            lease_until_ms=0,
            lease_token=0,
            next_attempt_ms=now_ms,
            updated_ms=now_ms,
        )
        return True

    async def list_pending_ops(self, player_id: int, stream: int, limit: int) -> list[AssetOpRow]:
        rows = [
            op
            for op in self._s.asset_ops.values()
            if op.player_id == player_id and op.stream == stream and op.status == OP_STATUS_PENDING
        ]
        rows.sort(key=lambda o: (o.stream_epoch, o.seq))
        return rows[:limit]

    async def list_recent_ops(self, player_id: int, stream: int, limit: int) -> list[AssetOpRow]:
        rows = [op for op in self._s.asset_ops.values() if op.player_id == player_id and op.stream == stream]
        rows.sort(key=lambda o: (o.stream_epoch, o.seq), reverse=True)
        return rows[:limit]

    async def accelerate_donation_deadlines(self, guild_id: int, player_ids: Sequence[int], now_ms: int) -> None:
        pids = set(player_ids)
        for op_id, op in list(self._s.asset_ops.items()):
            if (
                op.player_id in pids
                and op.stream == ASSET_STREAM_GUILD_DEBIT
                and op.status == OP_STATUS_PENDING
                and op.guild_id == guild_id
                and op.kind == OP_KIND_DONATE
                and op.deadline_ms > now_ms
            ):
                self._s.asset_ops[op_id] = replace(
                    op,
                    deadline_ms=now_ms,
                    next_attempt_ms=min(op.next_attempt_ms, now_ms),
                    updated_ms=now_ms,
                )

    async def get_client_request_op_id(self, player_id: int, client_request_id: int) -> int | None:
        rec = self._s.client_requests.get((player_id, client_request_id))
        return None if rec is None else rec[0]

    async def insert_client_request(self, player_id: int, client_request_id: int, op_id: int, now_ms: int) -> None:
        key = (player_id, client_request_id)
        if key in self._s.client_requests:
            raise _DuplicateKeyError("duplicate client_request", "PRIMARY")
        self._s.client_requests[key] = (op_id, now_ms)

    async def upsert_counter_with_limit(
        self, player_id: int, counter_kind: int, ref_id: int, period_key_val: int, delta: int, limit: int, now_ms: int
    ) -> bool:
        key = (player_id, counter_kind, ref_id, period_key_val)
        cur = self._s.daily_counters.get(key)
        if cur is None:
            if delta > limit:
                return False
            self._s.daily_counters[key] = (delta, now_ms)
            return True
        used, _ = cur
        if used + delta > limit:
            return False
        self._s.daily_counters[key] = (used + delta, now_ms)
        return True

    async def refund_counter(
        self, player_id: int, counter_kind: int, ref_id: int, period_key_val: int, delta: int, now_ms: int
    ) -> None:
        key = (player_id, counter_kind, ref_id, period_key_val)
        cur = self._s.daily_counters.get(key)
        if cur is None:
            return
        used, _ = cur
        self._s.daily_counters[key] = (max(0, used - delta), now_ms)

    async def get_counter_usage(
        self, player_id: int, counter_kind: int, period_keys: Sequence[int]
    ) -> dict[tuple[int, int], int]:
        pks = set(period_keys)
        return {
            (ref_id, pk): used
            for (pid, kind, ref_id, pk), (used, _) in self._s.daily_counters.items()
            if pid == player_id and kind == counter_kind and pk in pks
        }

    async def get_activity_counter_value(self, player_id: int, activity_id: int, period_key_val: int) -> int:
        cur = self._s.daily_counters.get((player_id, COUNTER_KIND_ACTIVITY, activity_id, period_key_val))
        return 0 if cur is None else cur[0]

    async def list_applied_asset_ops_since(
        self,
        player_ids: Sequence[int],
        since_ms: int,
        after_op_id: int,
        limit: int,
        zones: Sequence[int],
    ) -> tuple[list[AssetOpRow], int]:
        pids = set(player_ids)
        zone_set = set(zones)
        matched: list[AssetOpRow] = []
        for op in self._s.asset_ops.values():
            if op.status not in (OP_STATUS_APPLIED, OP_STATUS_APPLIED_PARTIAL):
                continue
            if op.next_attempt_ms <= since_ms or op.op_id <= after_op_id or op.player_id not in pids:
                continue
            if zone_set:
                g = self._s.guilds.get(op.guild_id)
                if g is not None and g.zone_id not in zone_set:
                    continue
            matched.append(op)
        matched.sort(key=lambda o: o.op_id)
        if len(matched) > limit:
            page = matched[:limit]
            return page, page[-1].op_id
        return matched, 0

    # activities & purge
    async def get_activity_progress(
        self, guild_id: int, keys: Sequence[tuple[int, int]]
    ) -> dict[tuple[int, int], ActivityProgress]:
        out: dict[tuple[int, int], ActivityProgress] = {}
        for act_id, pk in keys:
            val = self._s.activity_progress.get((guild_id, act_id, pk))
            if val is not None:
                out[(act_id, pk)] = val
        return out

    async def upsert_and_lock_activity_progress(
        self, guild_id: int, activity_id: int, period_key_val: int, *, increment: bool, now_ms: int
    ) -> ActivityProgress:
        key = (guild_id, activity_id, period_key_val)
        cur = self._s.activity_progress.get(key)
        if cur is None:
            nxt = ActivityProgress(count=1 if increment else 0, threshold_reached_ms=0, funds_granted=False)
        else:
            nxt = replace(cur, count=cur.count + (1 if increment else 0))
        self._s.activity_progress[key] = nxt
        return nxt

    async def latch_activity_progress(
        self,
        guild_id: int,
        activity_id: int,
        period_key_val: int,
        threshold_reached_ms: int,
        funds_granted: bool,
        now_ms: int,
    ) -> None:
        key = (guild_id, activity_id, period_key_val)
        cur = self._s.activity_progress[key]
        self._s.activity_progress[key] = replace(
            cur, threshold_reached_ms=threshold_reached_ms, funds_granted=funds_granted
        )

    async def set_trial_progress_count(
        self, guild_id: int, activity_id: int, period_key_val: int, count: int, now_ms: int
    ) -> None:
        key = (guild_id, activity_id, period_key_val)
        cur = self._s.activity_progress.get(key)
        if cur is None:
            self._s.activity_progress[key] = ActivityProgress(count=count, threshold_reached_ms=0, funds_granted=False)
        else:
            self._s.activity_progress[key] = replace(cur, count=count)

    async def delete_activity_progress_of_guild(self, guild_id: int) -> None:
        for key in [k for k in self._s.activity_progress if k[0] == guild_id]:
            del self._s.activity_progress[key]

    async def delete_started_trial_battles_of_guild(self, guild_id: int) -> None:
        for bid in [
            k
            for k, rec in self._s.trial_battles.items()
            if rec.guild_id == guild_id and rec.state == TRIAL_BATTLE_STATE_STARTED
        ]:
            del self._s.trial_battles[bid]

    async def get_activity_reward_statuses(self, player_id: int, now_ms: int) -> dict[int, ActivityRewardStatus]:
        pending_count: dict[int, int] = {}
        pending_reason: dict[int, int] = {}
        last_reject: dict[int, int] = {}
        cutoff = max(0, now_ms - REJECTED_REWARD_WINDOW_MS)
        credit_ops = [
            op for op in self._s.asset_ops.values() if op.player_id == player_id and op.stream == ASSET_STREAM_GUILD_CREDIT
        ]
        for op in sorted(credit_ops, key=lambda o: (o.stream_epoch, o.seq)):
            if op.status == OP_STATUS_PENDING and op.kind == OP_KIND_ACTIVITY_REWARD:
                pending_count[op.ref_id] = pending_count.get(op.ref_id, 0) + 1
                if op.ref_id not in pending_reason and op.last_reason != 0:
                    pending_reason[op.ref_id] = op.last_reason
        for op in sorted(credit_ops, key=lambda o: (o.stream_epoch, o.seq), reverse=True)[:4]:
            if (
                op.status == OP_STATUS_REJECTED
                and op.kind == OP_KIND_ACTIVITY_REWARD
                and op.next_attempt_ms >= cutoff
                and op.ref_id not in last_reject
            ):
                last_reject[op.ref_id] = op.reason_tip_id
        all_ids = set(pending_count) | set(pending_reason) | set(last_reject)
        return {
            aid: ActivityRewardStatus(
                pending_count=pending_count.get(aid, 0),
                pending_reason_tip_id=pending_reason.get(aid, 0),
                last_reject_tip_id=last_reject.get(aid, 0),
            )
            for aid in all_ids
        }

    async def purge_player_personal_tables(self, player_id: int) -> None:
        for key in [k for k in self._s.seqs if k[0] == player_id]:
            del self._s.seqs[key]
        for key in [k for k in self._s.daily_counters if k[0] == player_id]:
            del self._s.daily_counters[key]
        for key in [k for k in self._s.client_requests if k[0] == player_id]:
            del self._s.client_requests[key]
        # 注销删角同步清理帮战待入账行（对齐 Go role_fence_purge.go purgeWarRowsOfPlayer）。
        for key in [k for k in self._s.war_credit_owed if k[0] == player_id]:
            del self._s.war_credit_owed[key]

    # guild_trial_lobby / slot / launch / launch_role / battle / reward_owed
    async def get_trial_lobby(self, lobby_id: int) -> TrialLobbyRecord | None:
        return self._s.trial_lobbies.get(lobby_id)

    async def lock_trial_lobby(self, lobby_id: int) -> TrialLobbyRecord | None:
        return self._s.trial_lobbies.get(lobby_id)

    async def insert_trial_lobby(self, rec: TrialLobbyRecord) -> None:
        if rec.lobby_id in self._s.trial_lobbies:
            raise _DuplicateKeyError("duplicate lobby_id", "PRIMARY")
        self._s.trial_lobbies[rec.lobby_id] = rec

    async def update_trial_lobby(
        self,
        lobby_id: int,
        state: int,
        view_proto: bytes,
        consent_proto: bytes,
        launch_deadline_ms: int,
        battle_id: int,
        updated_ms: int,
    ) -> None:
        cur = self._s.trial_lobbies[lobby_id]
        self._s.trial_lobbies[lobby_id] = replace(
            cur,
            state=state,
            view_proto=view_proto,
            consent_proto=consent_proto,
            launch_deadline_ms=launch_deadline_ms,
            battle_id=battle_id,
            updated_ms=updated_ms,
        )

    async def list_expired_pending_lobbies(self, now_ms: int, limit: int) -> list[TrialLobbyRecord]:
        rows = [
            lob
            for lob in self._s.trial_lobbies.values()
            if lob.state == TRIAL_LOBBY_STATE_PENDING and lob.expires_ms <= now_ms
        ]
        rows.sort(key=lambda r: (r.expires_ms, r.lobby_id))
        return rows[:limit]

    async def get_trial_lobby_slot(self, player_id: int) -> TrialLobbySlotRecord | None:
        return self._s.trial_lobby_slots.get(player_id)

    async def lock_trial_lobby_slot(self, player_id: int) -> TrialLobbySlotRecord | None:
        return self._s.trial_lobby_slots.get(player_id)

    async def upsert_trial_lobby_slot(self, player_id: int, lobby_id: int, cooldown_until_ms: int) -> None:
        self._s.trial_lobby_slots[player_id] = TrialLobbySlotRecord(
            player_id=player_id,
            lobby_id=lobby_id,
            cooldown_until_ms=cooldown_until_ms,
        )

    async def get_trial_launch(self, request_id: int) -> TrialLaunchRecord | None:
        return self._s.trial_launches.get(request_id)

    async def lock_trial_launch(self, request_id: int) -> TrialLaunchRecord | None:
        return self._s.trial_launches.get(request_id)

    async def get_trial_launch_by_battle(self, battle_id: int) -> TrialLaunchRecord | None:
        for rec in self._s.trial_launches.values():
            if rec.battle_id == battle_id:
                return rec
        return None

    async def insert_trial_launch(self, rec: TrialLaunchRecord) -> None:
        if rec.request_id in self._s.trial_launches:
            raise _DuplicateKeyError("duplicate request_id", "PRIMARY")
        self._s.trial_launches[rec.request_id] = rec

    async def update_trial_launch(
        self,
        request_id: int,
        battle_id: int,
        state: int,
        updated_ms: int,
        cancel_receipt: bytes | None = None,
    ) -> None:
        cur = self._s.trial_launches[request_id]
        self._s.trial_launches[request_id] = replace(
            cur,
            battle_id=battle_id,
            state=state,
            updated_ms=updated_ms,
            cancel_receipt=cur.cancel_receipt if cancel_receipt is None else cancel_receipt,
        )

    async def record_trial_launch_end_tip(self, request_id: int, tip_bytes: bytes) -> None:
        cur = self._s.trial_launches.get(request_id)
        if cur is not None and not cur.realtime_end_tip:
            self._s.trial_launches[request_id] = replace(cur, realtime_end_tip=tip_bytes)

    async def expedite_trial_launch_check(self, request_id: int, now_ms: int) -> None:
        cur = self._s.trial_launches.get(request_id)
        if cur is not None and cur.state == TRIAL_LAUNCH_STATE_PREPARED and cur.next_check_ms > now_ms:
            self._s.trial_launches[request_id] = replace(cur, next_check_ms=now_ms)

    async def list_due_trial_launches(self, now_ms: int, limit: int) -> list[TrialLaunchRecord]:
        rows = [
            rec
            for rec in self._s.trial_launches.values()
            if rec.state == TRIAL_LAUNCH_STATE_PREPARED and rec.next_check_ms <= now_ms
        ]
        rows.sort(key=lambda r: (r.next_check_ms, r.request_id))
        return rows[:limit]

    async def claim_trial_launch(self, request_id: int, prev_next_check_ms: int, new_next_check_ms: int) -> bool:
        cur = self._s.trial_launches.get(request_id)
        if (
            cur is None
            or cur.state != TRIAL_LAUNCH_STATE_PREPARED
            or cur.next_check_ms != prev_next_check_ms
        ):
            return False
        self._s.trial_launches[request_id] = replace(
            cur,
            next_check_ms=new_next_check_ms,
            check_attempts=cur.check_attempts + 1,
        )
        return True

    async def insert_trial_launch_role(self, rec: TrialLaunchRoleRecord) -> None:
        key = (rec.player_id, rec.request_id)
        if key in self._s.trial_launch_roles:
            raise _DuplicateKeyError("duplicate launch_role", "PRIMARY")
        self._s.trial_launch_roles[key] = rec

    async def lock_trial_launch_role(self, player_id: int, request_id: int) -> TrialLaunchRoleRecord | None:
        return self._s.trial_launch_roles.get((player_id, request_id))

    async def has_active_trial_launch_role(self, player_id: int, exclude_request_id: int = 0) -> bool:
        for (pid, req_id), role_rec in self._s.trial_launch_roles.items():
            if pid != player_id or req_id == exclude_request_id:
                continue
            launch = self._s.trial_launches.get(req_id)
            if role_rec.state != TRIAL_LAUNCH_ROLE_RELEASED or launch is None or launch.state != TRIAL_LAUNCH_STATE_TERMINAL:
                return True
        return False

    async def update_trial_launch_role_state(self, player_id: int, request_id: int, state: int) -> None:
        key = (player_id, request_id)
        cur = self._s.trial_launch_roles.get(key)
        if cur is not None:
            self._s.trial_launch_roles[key] = replace(cur, state=state)

    async def get_trial_battle(self, battle_id: int) -> TrialBattleRecord | None:
        return self._s.trial_battles.get(battle_id)

    async def lock_trial_battle(self, battle_id: int) -> TrialBattleRecord | None:
        return self._s.trial_battles.get(battle_id)

    async def insert_trial_battle(self, rec: TrialBattleRecord) -> None:
        if rec.battle_id in self._s.trial_battles:
            raise _DuplicateKeyError("duplicate battle_id", "PRIMARY")
        self._s.trial_battles[rec.battle_id] = rec

    async def update_trial_battle(
        self, battle_id: int, state: int, settle_result: int, rewarded_count: int, settled_ms: int
    ) -> None:
        cur = self._s.trial_battles[battle_id]
        self._s.trial_battles[battle_id] = replace(
            cur,
            state=state,
            settle_result=settle_result,
            rewarded_count=rewarded_count,
            settled_ms=settled_ms,
        )

    async def list_overdue_trial_battles(self, created_before_ms: int, limit: int) -> list[TrialBattleRecord]:
        rows = [
            rec
            for rec in self._s.trial_battles.values()
            if rec.state == TRIAL_BATTLE_STATE_STARTED and rec.created_ms <= created_before_ms
        ]
        rows.sort(key=lambda r: (r.created_ms, r.battle_id))
        return rows[:limit]

    async def insert_trial_reward_owed(self, rec: TrialOwedRewardRecord) -> None:
        self._s.trial_reward_owed.setdefault((rec.player_id, rec.battle_id), rec)

    async def delete_trial_reward_owed(self, player_id: int, battle_id: int) -> bool:
        return self._s.trial_reward_owed.pop((player_id, battle_id), None) is not None

    async def list_trial_rewards_owed(self, limit: int) -> list[TrialOwedRewardRecord]:
        rows = sorted(self._s.trial_reward_owed.values(), key=lambda r: (r.created_ms, r.battle_id, r.player_id))
        return rows[:limit]

    async def count_trial_rewards_owed_by_player(self, player_id: int) -> dict[int, int]:
        counts: dict[int, int] = {}
        for rec in self._s.trial_reward_owed.values():
            if rec.player_id == player_id:
                counts[rec.activity_id] = counts.get(rec.activity_id, 0) + 1
        return counts

    async def increment_trial_reward_owed_attempts(self, player_id: int, battle_id: int) -> None:
        key = (player_id, battle_id)
        cur = self._s.trial_reward_owed.get(key)
        if cur is not None:
            self._s.trial_reward_owed[key] = replace(cur, attempts=cur.attempts + 1)

    async def insert_war_credit_owed(self, rec: WarCreditOwed) -> None:
        key = (rec.player_id, rec.op_id)
        if key in self._s.war_credit_owed:
            raise GuildWriteConflictError(f"duplicate guild_war_credit_owed key {key}")
        self._s.war_credit_owed[key] = rec

    async def delete_war_credit_owed(self, player_id: int, op_id: int) -> bool:
        return self._s.war_credit_owed.pop((player_id, op_id), None) is not None

    async def war_credit_owed_exists(self, player_id: int, op_id: int) -> bool:
        return (player_id, op_id) in self._s.war_credit_owed

    async def list_war_credit_owed(self, after: WarCreditOwedCursor, limit: int) -> list[WarCreditOwed]:
        cursor = (after.created_ms, after.player_id, after.op_id)
        rows = [
            rec
            for rec in self._s.war_credit_owed.values()
            if (rec.created_ms, rec.player_id, rec.op_id) > cursor
        ]
        rows.sort(key=lambda r: (r.created_ms, r.player_id, r.op_id))
        return rows[:limit]

    async def count_war_credit_owed(self, limit: int) -> int:
        return min(len(self._s.war_credit_owed), max(0, int(limit)))


class MemoryGuildStore(GuildStoreCore):
    """确定性内存帮会存储双胞胎（供单测与无库烟测使用，提供完整事务回滚语义）。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        catalog: GuildTableCatalog | None = None,
        guild_ids: Callable[[], Awaitable[int]] | None = None,
        op_ids: Callable[[], Awaitable[int]] | None = None,
        role_directory: RoleFenceDirectory | None = None,
        merged_zones: MergedZones | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        super().__init__(
            protocol,
            catalog=catalog or GuildTableCatalog.from_config_tables(),
            guild_ids=guild_ids or default_id_generator(10001),
            op_ids=op_ids or default_id_generator(50001),
            role_directory=role_directory,
            merged_zones=merged_zones,
            clock_ms=clock_ms,
        )
        self._lock = asyncio.Lock()
        self.state = _MemoryState.empty()

    async def _run_tx(self, fn: Callable[[GuildTx], Awaitable[T]], *, read_only: bool = False) -> T:
        async with self._lock:
            if read_only:
                return await fn(_MemoryTx(self.state))
            snapshot = copy.deepcopy(self.state)
            try:
                result = await fn(_MemoryTx(snapshot))
            except _TxCommitThen as commit_then:
                self.state = snapshot
                raise commit_then.inner
            except _TxRollbackReturn as rollback_ret:
                return rollback_ret.value
            self.state = snapshot
            return result

    # 测试辅助方法：直接预置资金、帮贡、已应用资产流水等
    def set_guild_funds_for_test(self, guild_id: int, funds: int) -> None:
        g = self.state.guilds[guild_id]
        self.state.guilds[guild_id] = replace(g, funds=funds)

    def set_member_contribution_for_test(
        self, guild_id: int, player_id: int, total: int, balance: int
    ) -> None:
        gid, m = self.state.members_by_player[player_id]
        assert gid == guild_id
        self.state.members_by_player[player_id] = (
            gid,
            replace(m, contribution_total=total, contribution_balance=balance),
        )

    def seed_asset_op_for_test(self, op: AssetOpRow) -> None:
        self.state.asset_ops[op.op_id] = op
