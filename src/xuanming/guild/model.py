"""guild 领域常量、不可变数据模型、游戏日/周、活动纯规则与配置表校验（零 I/O）。"""

from __future__ import annotations

import unicodedata
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from google.protobuf.message import Message

from ..data import ConfigTables
from .errors import (
    ERR_GUILD_ACTIVITY_ALREADY_CLAIMED,
    ERR_GUILD_ACTIVITY_JOIN_TOO_RECENT,
    ERR_GUILD_ACTIVITY_LEVEL_TOO_LOW,
    ERR_GUILD_ACTIVITY_NOT_OPEN,
    ERR_GUILD_ACTIVITY_THRESHOLD_NOT_REACHED,
)

DATABASE_NAME = "mmorpg_guild"
GUILD_NODE_TYPE = 15  # common.base.eNodeType.GuildNodeService

# 持久化职位编码（0=成员 / 1=长老 / 3=帮主，2 刻意跳过；权限比较一律走 role_rank）
ROLE_MEMBER = 0
ROLE_OFFICER = 1
ROLE_LEADER = 3

RANK_NONE = 0
RANK_MEMBER = 1
RANK_OFFICER = 2
RANK_LEADER = 3

DEFAULT_INIT_LEVEL = 1
GUILD_RULE_ROW_ID = 1
MAX_GUILD_MEMBERS_CAP = 100
MAX_GUILD_NAME_RUNES = 24
MAX_GUILD_NAME_NORM_RUNES = 48
MAX_ANNOUNCEMENT_BYTES = 600
MAX_RANK_PAGE_SIZE = 50
DEFAULT_RANK_PAGE_SIZE = 20
MAX_SHOP_BUY_COUNT = 20
MY_APPLICATIONS_LIMIT = 10
MAX_APPLIED_OPS_PLAYER_IDS = 100
MAX_APPLIED_OPS_PAGE_LIMIT = 500
MAX_REWARD_ITEM_KINDS = 16
MAX_REWARD_TOTAL_ITEMS = 4096
MAX_ACCOUNT_BYTES = 191

MS_PER_HOUR = 3_600_000
MS_PER_DAY = 86_400_000
RETENTION_SAFETY_MS = 3_600_000
RANK_TOMBSTONE_TTL_MS = 24 * MS_PER_HOUR
MAX_RANK_VERSION = 1 << 53
UINT64_MAX = (1 << 64) - 1
UINT32_MAX = (1 << 32) - 1
INT64_MAX = (1 << 63) - 1
REMOVAL_TOKEN_BIT = 1 << 63

RETENTION_REJECTED_PREFIX = "since_ms older than terminal retention; cutoff_ms="

# 资产通道与账本常量（对应 proto/common/asset 与 guild_db.proto）
ASSET_OP_CALLER = "guild"
MATCH_INTERNAL_CALLER = "guild"
ASSET_STREAM_GUILD_DEBIT = 1
ASSET_STREAM_GUILD_CREDIT = 2
TX_GUILD_DONATE = 24
TX_GUILD_SHOP = 25
TX_GUILD_ACTIVITY_REWARD = 26
TX_GUILD_BOUNTY_ESCROW = 41
TX_GUILD_WAR_REWARD = 42
TX_GUILD_BOUNTY_PAYOUT = 43
ASSET_OP_FRESH_ATTEMPT_LIMIT = 3

OP_STATUS_UNSPECIFIED = 0
OP_STATUS_PENDING = 1
OP_STATUS_APPLIED = 2
OP_STATUS_REJECTED = 3
OP_STATUS_ABORTED = 4
OP_STATUS_APPLIED_PARTIAL = 5
OP_STATUS_ABANDONED = 6

ORDER_STATUS_UNSPECIFIED = 0
ORDER_STATUS_PENDING = 1
ORDER_STATUS_APPLIED = 2
ORDER_STATUS_REJECTED = 3
ORDER_STATUS_ABORTED = 4
ORDER_STATUS_APPLIED_PARTIAL = 5

OP_KIND_UNSPECIFIED = 0
OP_KIND_DONATE = 1
OP_KIND_SHOP = 2
OP_KIND_ACTIVITY_REWARD = 3

COUNTER_KIND_UNSPECIFIED = 0
COUNTER_KIND_DONATE = 1
COUNTER_KIND_SHOP = 2
COUNTER_KIND_ACTIVITY = 3

ASSET_OP_MAX_PENDING = 16
ASSET_OP_MAX_SPAN = 512
RECENT_RESULT_WINDOW_MS = 600_000
RECENT_RESULT_LIMIT = 5
RECENT_SCAN_LIMIT = 20
REJECTED_REWARD_WINDOW_MS = 86_400_000

# 帮会活动类型与状态（与 guild.proto 枚举数值一致）
ACTIVITY_TYPE_LANTERN = 1
ACTIVITY_TYPE_REUNION = 2
ACTIVITY_TYPE_TRIAL = 3
ALL_ACTIVITY_TYPES = (ACTIVITY_TYPE_LANTERN, ACTIVITY_TYPE_REUNION, ACTIVITY_TYPE_TRIAL)

ACTIVITY_STATE_DISABLED = 1
ACTIVITY_STATE_UPCOMING = 2
ACTIVITY_STATE_OPEN = 3
ACTIVITY_STATE_ENDED = 4

# 同道历练状态与跨服务常量（与 guild.proto / guild_db.proto / match_internal.proto 一致）
TRIAL_LOBBY_STATE_PENDING = 1
TRIAL_LOBBY_STATE_LAUNCHING = 2
TRIAL_LOBBY_STATE_LAUNCHED = 3
TRIAL_LOBBY_STATE_ENDED = 4

TRIAL_LAUNCH_STATE_PREPARED = 1
TRIAL_LAUNCH_STATE_LAUNCHED = 2
TRIAL_LAUNCH_STATE_TERMINAL = 3

TRIAL_LAUNCH_ROLE_ACTIVE = 1
TRIAL_LAUNCH_ROLE_RELEASED = 2

TRIAL_BATTLE_STATE_STARTED = 1
TRIAL_BATTLE_STATE_SETTLED = 2
TRIAL_BATTLE_STATE_EXPIRED = 3

TRIAL_SETTLE_RESULT_UNSPECIFIED = 0
TRIAL_SETTLE_RESULT_WIN = 1
TRIAL_SETTLE_RESULT_LOSS = 2
TRIAL_SETTLE_RESULT_GUILD_GONE = 3
TRIAL_SETTLE_RESULT_CONFIG_MISSING = 4
TRIAL_SETTLE_RESULT_POISON = 5

BATTLE_ACTIVITY_KIND_GUILD_TRIAL = 1
BATTLE_ACTIVITY_KIND_GUILD_WAR = 3
BATTLE_OUTCOME_SIDE_A_WIN = 1
BATTLE_OUTCOME_SIDE_B_WIN = 2
BATTLE_OUTCOME_DRAW = 3
MATCH_MODE_PVE_TEAM = 5
MATCH_MODE_GUILD_WAR_PVP = 9
MATCH_MODE_GUILD_WAR_SIEGE = 10
MATCH_MODE_JIANGHU_DUEL = 12

OUTCOME_DECISION_UNKNOWN = 0
OUTCOME_DECISION_COMPLETE = 1
OUTCOME_DECISION_ABORT = 2

TRIAL_OCCUPANCY_REJECT_NONE = 0
TRIAL_OCCUPANCY_REJECT_OCCUPIED = 1
TRIAL_OCCUPANCY_REJECT_ROLE = 2
TRIAL_OCCUPANCY_REJECT_DENIED = 3
TRIAL_OCCUPANCY_REJECT_RETRY = 4

ACTIVITY_BATTLE_REJECT_NONE = 0
ACTIVITY_BATTLE_REJECT_INVALID_ARGUMENT = 1
ACTIVITY_BATTLE_REJECT_MEMBER_OFFLINE = 2
ACTIVITY_BATTLE_REJECT_MEMBER_IN_BATTLE = 3
ACTIVITY_BATTLE_REJECT_MEMBER_NOT_READY = 4
ACTIVITY_BATTLE_REJECT_INTERNAL = 5

MAX_TRIAL_LOBBY_ROSTER = 5
MIN_TRIAL_LOBBY_ROSTER = 2
TRIAL_OCCUPANCY_MARGIN_MS = 30_000
TRIAL_LAUNCH_WINDOW_MS = 5_000

# 限购周期枚举
LIMIT_PERIOD_NONE = 0
LIMIT_PERIOD_DAILY = 1
LIMIT_PERIOD_WEEKLY = 2
LIMIT_PERIOD_LIFETIME = 3
# 终身限购固定周期键（与 0=不限、6位周键 >=100000、8位日键 >=19700101 完全不相交，永不被清理任务删除）
LIFETIME_PERIOD_KEY = 1

# 角色护栏常量
ROLE_FENCE_CALLER = "guild"
ROLE_BLOCKER_GUILD_MEMBER = "guild_member"
ROLE_BLOCKER_GUILD_LEADER = "guild_leader"
ROLE_BLOCKER_ASSET_OP = "guild_asset_op"
ROLE_BLOCKER_TRIAL_REWARD_OWED = "guild_trial_reward_owed"
ROLE_BLOCKER_TRIAL_LAUNCH_ROLE = "guild_trial_launch_role"
ROLE_BLOCKER_TRIAL_LOBBY_SLOT = "guild_trial_lobby_slot"

# 内容安全场景常量（对应 content_guard.TextScene）
TEXT_SCENE_GUILD_NAME = 2
TEXT_SCENE_GUILD_ANNOUNCEMENT = 4
TEXT_VERDICT_PASS = 1
TEXT_VERDICT_MASKED = 2
TEXT_VERDICT_REJECT = 3

# 游戏日固定时区 UTC+8，每日 05:00 切日
GAME_TZ = timezone(timedelta(hours=8))
GAME_DAY_RESET_HOUR = 5


# -- 职位与权限纯函数 ---------------------------------------------------------------------------


def role_rank(role: int) -> int:
    """将持久化职位编码映射到连续权限档位；未知编码 fail-closed 返回 0。"""
    if role == ROLE_MEMBER:
        return RANK_MEMBER
    if role == ROLE_OFFICER:
        return RANK_OFFICER
    if role == ROLE_LEADER:
        return RANK_LEADER
    return RANK_NONE


def is_assignable_role(role: int) -> bool:
    """SetGuildMemberRole 仅允许 0(成员) 与 1(长老)，帮主只能经转让产生。"""
    return role in (ROLE_MEMBER, ROLE_OFFICER)


def can_assign_role(actor_role: int) -> bool:
    return role_rank(actor_role) == RANK_LEADER


def can_transfer_leader(actor_role: int) -> bool:
    return role_rank(actor_role) == RANK_LEADER


def can_review_applications(actor_role: int) -> bool:
    return role_rank(actor_role) >= RANK_OFFICER


def can_set_announcement(actor_role: int) -> bool:
    return role_rank(actor_role) >= RANK_OFFICER


def can_upgrade_guild(actor_role: int) -> bool:
    return role_rank(actor_role) >= RANK_OFFICER


def can_kick(actor_role: int, target_role: int) -> bool:
    actor_r, target_r = role_rank(actor_role), role_rank(target_role)
    return actor_r >= RANK_OFFICER and target_r != RANK_NONE and actor_r > target_r


def demoted_leader_role(officers_after_target: int, max_officers: int) -> int:
    return ROLE_OFFICER if officers_after_target < max_officers else ROLE_MEMBER


# -- 帮名规范化与输入校验 -----------------------------------------------------------------------


def guild_name_norm(display: str) -> str | None:
    """返回帮名全局唯一键：NFKC -> trim -> lower；空串或超 48 字符返回 None。"""
    norm = unicodedata.normalize("NFKC", display).strip().lower()
    if not norm or len(norm) > MAX_GUILD_NAME_NORM_RUNES:
        return None
    return norm


def normalize_guild_name(raw: str) -> tuple[str, str] | None:
    """去首尾空白并校验长度与控制字符，返回 ``(display_name, name_norm)``。"""
    name = raw.strip()
    if not name or len(name) > MAX_GUILD_NAME_RUNES:
        return None
    for ch in name:
        if unicodedata.category(ch) == "Cc":
            return None
    norm = guild_name_norm(name)
    if norm is None:
        return None
    return name, norm


def announcement_valid(text: str) -> bool:
    return len(text.encode("utf-8")) <= MAX_ANNOUNCEMENT_BYTES


# -- 游戏日 / 游戏周（UTC+8 05:00 切点） --------------------------------------------------------


def _shifted_game_dt(now_ms: int) -> datetime:
    dt = datetime.fromtimestamp(now_ms / 1000.0, tz=GAME_TZ)
    return dt - timedelta(hours=GAME_DAY_RESET_HOUR)


def day_key(now_ms: int) -> int:
    s = _shifted_game_dt(now_ms)
    return s.year * 10000 + s.month * 100 + s.day


def week_key(now_ms: int) -> int:
    iso = _shifted_game_dt(now_ms).isocalendar()
    return iso.year * 100 + iso.week


def next_daily_reset_ms(now_ms: int) -> int:
    s = _shifted_game_dt(now_ms)
    nxt = datetime(s.year, s.month, s.day, GAME_DAY_RESET_HOUR, 0, 0, tzinfo=GAME_TZ) + timedelta(days=1)
    return int(nxt.timestamp() * 1000)


def next_weekly_reset_ms(now_ms: int) -> int:
    s = _shifted_game_dt(now_ms)
    days_since_monday = s.weekday()  # Monday=0 .. Sunday=6
    nxt = datetime(s.year, s.month, s.day, GAME_DAY_RESET_HOUR, 0, 0, tzinfo=GAME_TZ) + timedelta(
        days=7 - days_since_monday
    )
    return int(nxt.timestamp() * 1000)


def period_key(limit_period: int, now_ms: int) -> tuple[int, bool]:
    if limit_period == LIMIT_PERIOD_NONE:
        return 0, True
    if limit_period == LIMIT_PERIOD_DAILY:
        return day_key(now_ms), True
    if limit_period == LIMIT_PERIOD_WEEKLY:
        return week_key(now_ms), True
    if limit_period == LIMIT_PERIOD_LIFETIME:
        return LIFETIME_PERIOD_KEY, True
    return 0, False


# -- 角色护栏辅助 -------------------------------------------------------------------------------


def removal_token(player_id: int) -> int:
    return REMOVAL_TOKEN_BIT | int(player_id)


def is_removal_token(token: int) -> bool:
    return (int(token) & REMOVAL_TOKEN_BIT) != 0


@dataclass(frozen=True, slots=True)
class RoleOriginal:
    """Gate 验签原件提取的在线角色原身份。"""

    player_id: int
    account: str
    owner_revision: int


@dataclass(frozen=True, slots=True)
class RoleFenceRow:
    """本库 role_fence 副本的一行快照。"""

    player_id: int
    account: str
    owner_revision: int
    freeze_token: int
    freeze_final: bool
    version: int
    home_zone: int
    updated_ms: int = 0


def valid_role_fence_state(state: RoleFenceRow | Message | None) -> bool:
    if state is None:
        return False
    player_id = int(state.player_id)
    account = str(state.account)
    owner_revision = int(state.owner_revision)
    freeze_token = int(state.freeze_token)
    freeze_final = bool(state.freeze_final)
    version = int(state.version)
    home_zone = int(state.home_zone)
    if player_id == 0 or version == 0:
        return False
    if len(account.encode("utf-8")) > MAX_ACCOUNT_BYTES:
        return False
    if freeze_final and freeze_token == 0:
        return False
    if account == "":
        # 墓碑态：account 为空时允许 owner_revision=0 或正数
        return True
    if owner_revision == 0 or home_zone == 0:
        return False
    return True


def check_role_fence(fence: RoleFenceRow | None, original: RoleOriginal) -> str:
    """比对本地护栏副本与会话原身份：返回 'ok' / 'stale' / 'reject'。"""
    if fence is None:
        return "stale"
    if fence.account == "":
        return "reject"
    if fence.owner_revision < original.owner_revision:
        return "stale"
    if fence.owner_revision > original.owner_revision or fence.account != original.account:
        return "reject"
    if fence.freeze_token != 0:
        return "stale"
    return "ok"


def role_fence_from_proto(msg: Message) -> RoleFenceRow:
    return RoleFenceRow(
        player_id=int(msg.player_id),
        account=str(msg.account),
        owner_revision=int(msg.owner_revision),
        freeze_token=int(msg.freeze_token),
        freeze_final=bool(msg.freeze_final),
        version=int(msg.version),
        home_zone=int(msg.home_zone),
    )


# -- 核心领域数据结构 ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MemberData:
    player_id: int
    role: int
    join_time_ms: int
    last_active_ms: int
    contribution_total: int = 0
    contribution_balance: int = 0


@dataclass(frozen=True, slots=True)
class GuildData:
    guild_id: int
    name: str
    leader_id: int
    level: int
    announcement: str
    create_time_ms: int
    max_members: int
    zone_id: int
    score: int = 0
    funds: int = 0
    score_version: int = 0
    renamed_ms: int = 0
    name_norm: str = ""
    members: tuple[MemberData, ...] = ()

    def member(self, player_id: int) -> MemberData | None:
        for item in self.members:
            if item.player_id == player_id:
                return item
        return None


@dataclass(frozen=True, slots=True)
class ApplicationRow:
    guild_id: int
    apply_ms: int
    expire_ms: int


@dataclass(frozen=True, slots=True)
class ApplicantRow:
    player_id: int
    apply_ms: int
    expire_ms: int


@dataclass(frozen=True, slots=True)
class RankEntry:
    guild_id: int
    score: int
    rank: int  # 1-based; 0 = 未上榜


@dataclass(frozen=True, slots=True)
class AssetOpRow:
    op_id: int
    player_id: int
    stream: int
    seq: int
    guild_id: int
    kind: int
    status: int
    durable: int
    attempts: int
    next_attempt_ms: int
    deadline_ms: int
    payload: bytes
    ref_id: int
    ref_count: int
    period_key: int
    contribution_delta: int
    funds_delta: int
    reason_tip_id: int
    created_ms: int
    updated_ms: int
    lease_until_ms: int
    lease_token: int
    tx_type: int
    last_outcome: int
    last_reason: int
    stream_epoch: int
    resolved_by: str = ""
    resolve_reason: str = ""
    last_send_ms: int = 0


@dataclass(frozen=True, slots=True)
class PendingAssetOp:
    """已领取或刚入队的待投递资产指令视图（供 GuildAssetOpLoop 处理）。"""

    op_id: int
    player_id: int
    guild_id: int
    stream: int
    seq: int
    stream_epoch: int
    correlation_id: int
    tx_type: int
    kind: int
    bundle: Message
    attempts: int
    deadline_ms: int
    last_reason: int
    lease_token: int
    created_ms: int = 0
    last_send_ms: int = 0


@dataclass(frozen=True, slots=True)
class FinalizedAssetOp:
    """一次本次 CAS 命中的资产指令终结摘要（供推送回调使用）。"""

    op_id: int
    player_id: int
    guild_id: int
    kind: int
    status: int


@dataclass(frozen=True, slots=True)
class TrialLobbyRecord:
    """guild_trial_lobby 一行快照。"""

    lobby_id: int
    guild_id: int
    activity_id: int
    initiator_player_id: int
    state: int
    view_proto: bytes
    original_proto: bytes
    consent_proto: bytes
    expires_ms: int
    launch_deadline_ms: int
    battle_id: int
    created_ms: int
    updated_ms: int


@dataclass(frozen=True, slots=True)
class TrialLobbySlotRecord:
    """guild_trial_lobby_slot 一行快照。"""

    player_id: int
    lobby_id: int
    cooldown_until_ms: int


@dataclass(frozen=True, slots=True)
class TrialLaunchRecord:
    """guild_trial_launch 一行快照。"""

    request_id: int
    battle_id: int
    state: int
    original_proto: bytes
    request_digest: bytes
    created_ms: int
    updated_ms: int
    cancel_receipt: bytes = b""
    next_check_ms: int = 0
    check_attempts: int = 0
    realtime_end_tip: bytes = b""


@dataclass(frozen=True, slots=True)
class TrialLaunchRoleRecord:
    """guild_trial_launch_role 一行快照。"""

    player_id: int
    request_id: int
    account: str
    owner_revision: int
    state: int


@dataclass(frozen=True, slots=True)
class TrialBattleKey:
    """同道历练对局上下文键。"""

    battle_id: int
    guild_id: int
    activity_id: int
    period_key: int
    guild_period_key: int
    initiator_player_id: int
    launch_request_id: int = 0


@dataclass(frozen=True, slots=True)
class TrialBattleRecord:
    """guild_trial_battle 一行快照。"""

    battle_id: int
    guild_id: int
    activity_id: int
    period_key: int
    guild_period_key: int
    initiator_player_id: int
    state: int
    settle_result: int
    rewarded_count: int
    created_ms: int
    settled_ms: int
    launch_request_id: int = 0


@dataclass(frozen=True, slots=True)
class TrialOwedRewardRecord:
    """guild_trial_reward_owed 一行快照（未决窗满时溢出暂存）。"""

    player_id: int
    battle_id: int
    guild_id: int
    activity_id: int
    period_key: int
    payload: bytes
    attempts: int
    created_ms: int


@dataclass(frozen=True, slots=True)
class TrialRewardOp:
    """结算前为单个候选人预分配的物品指令号与租约令牌。"""

    player_id: int
    op_id: int
    lease_token: int


@dataclass(frozen=True, slots=True)
class TrialEnqueuedReward:
    """结算事务内成功入队的单个物品指令。"""

    player_id: int
    op_id: int
    seq: int
    stream_epoch: int
    lease_token: int


@dataclass(frozen=True, slots=True)
class TrialSettleResult:
    """同道历练结算事务结果。"""

    status: str  # "settled" | "duplicate" | "guild_gone" | "context_mismatch"
    result: int = TRIAL_SETTLE_RESULT_UNSPECIFIED
    rewarded: tuple[int, ...] = ()
    enqueued: tuple[TrialEnqueuedReward, ...] = ()
    owed: tuple[int, ...] = ()
    funds_granted: bool = False
    counted_wins: int = 0
    release_request_id: int = 0
    release_players: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class ActivityProgress:
    count: int = 0
    threshold_reached_ms: int = 0
    funds_granted: bool = False

    @property
    def latched(self) -> bool:
        return self.threshold_reached_ms != 0


@dataclass(frozen=True, slots=True)
class ActivityRewardStatus:
    pending_count: int = 0
    pending_reason_tip_id: int = 0
    last_reject_tip_id: int = 0


@dataclass(frozen=True, slots=True)
class ActivityTxResult:
    enqueued: bool
    seq: int
    stream_epoch: int
    day_key_val: int
    guild_period_key_val: int
    progress: ActivityProgress
    reached_now: bool
    funds_granted: bool
    op: AssetOpRow | None = None


@dataclass(frozen=True, slots=True)
class DisbandResult:
    zone_id: int
    member_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class UpgradeResult:
    changed: bool
    new_level: int
    member_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class ApplyResult:
    inserted: bool
    reviewer_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class ReviewResult:
    guild: GuildData
    approved: bool


@dataclass(frozen=True, slots=True)
class MemberWriteResult:
    guild: GuildData
    changed: bool


@dataclass(frozen=True, slots=True)
class RolePurgeResult:
    player_id: int
    guild_id: int = 0
    zone_id: int = 0
    disbanded: bool = False
    successor: int = 0
    members: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class PrepareFreezeOutcome:
    clean: bool = False
    blocker: str = ""
    stale: bool = False
    retry: bool = False


@dataclass(frozen=True, slots=True)
class MergedZones:
    """合服映射快照：src -> dst，解析最终区并计算合服组成员。"""

    merges: Mapping[int, int] = field(default_factory=dict)

    def final_zone(self, zone_id: int) -> int:
        if zone_id <= 0 or not self.merges:
            return zone_id
        cur = zone_id
        seen: set[int] = set()
        while cur in self.merges and cur not in seen:
            seen.add(cur)
            nxt = self.merges[cur]
            if nxt <= 0:
                break
            cur = nxt
        return cur

    def same_zone(self, a: int, b: int) -> bool:
        return self.final_zone(a) == self.final_zone(b)

    def zone_members(self, zone_id: int) -> tuple[int, ...]:
        if zone_id <= 0:
            return ()
        target = self.final_zone(zone_id)
        group = {target}
        for src in self.merges:
            if self.final_zone(src) == target:
                group.add(src)
        return tuple(sorted(group))


def better_successor_key(member: MemberData) -> tuple[int, int, int, int]:
    """删角自动转让顺位：职位档位降序 -> 累计帮贡降序 -> 入帮时间升序 -> player_id 升序。"""
    return (
        -role_rank(member.role),
        -int(member.contribution_total),
        int(member.join_time_ms),
        int(member.player_id),
    )


def order_status_view(op_status: int, last_reason: int, reason_tip_id: int) -> tuple[int, int]:
    """将库内 GuildAssetOpStatus 显式映射为客户端 GuildAssetOrderStatus 与原因 tip。"""
    if op_status == OP_STATUS_PENDING:
        return ORDER_STATUS_PENDING, last_reason
    if op_status == OP_STATUS_APPLIED:
        return ORDER_STATUS_APPLIED, reason_tip_id
    if op_status == OP_STATUS_REJECTED:
        return ORDER_STATUS_REJECTED, reason_tip_id
    if op_status in (OP_STATUS_ABORTED, OP_STATUS_ABANDONED):
        return ORDER_STATUS_ABORTED, reason_tip_id
    if op_status == OP_STATUS_APPLIED_PARTIAL:
        return ORDER_STATUS_APPLIED_PARTIAL, reason_tip_id
    return ORDER_STATUS_UNSPECIFIED, 0


def is_terminal_op_status(op_status: int) -> bool:
    return op_status in (
        OP_STATUS_APPLIED,
        OP_STATUS_REJECTED,
        OP_STATUS_ABORTED,
        OP_STATUS_APPLIED_PARTIAL,
        OP_STATUS_ABANDONED,
    )


def provable_cutoff_ms(now_ms: int, terminal_retention_days: int, cleanup_watermark_ms: int) -> int:
    """计算 ListAppliedAssetOpsSince 可证明的最小 since_ms。"""
    retention_ms = terminal_retention_days * MS_PER_DAY
    if now_ms + RETENTION_SAFETY_MS <= retention_ms:
        cutoff = 0
    else:
        cutoff = now_ms + RETENTION_SAFETY_MS - retention_ms
    if cleanup_watermark_ms > 0:
        cutoff = max(cutoff, cleanup_watermark_ms + RETENTION_SAFETY_MS)
    return cutoff


# -- 活动纯规则（与 Go internal/activity/rules.go 逐项一致） ------------------------------------


@dataclass(frozen=True, slots=True)
class ActivityRuleRow:
    activity_id: int
    activity_type: int
    name: str
    enabled: bool
    start_at_ms: int
    end_at_ms: int
    min_guild_level: int
    personal_contribution: int
    guild_funds: int
    guild_threshold: int
    reward_id: int
    daily_limit: int
    dungeon_id: int = 0
    team_size_min: int = 0
    team_size_max: int = 0


def activity_state_of(row: ActivityRuleRow | None, now_ms: int) -> int:
    if row is None or not row.enabled:
        return ACTIVITY_STATE_DISABLED
    if row.start_at_ms == 0 and row.end_at_ms == 0:
        return ACTIVITY_STATE_OPEN
    if now_ms < row.start_at_ms:
        return ACTIVITY_STATE_UPCOMING
    if now_ms >= row.end_at_ms:
        return ACTIVITY_STATE_ENDED
    return ACTIVITY_STATE_OPEN


def guild_period_key(row: ActivityRuleRow | None, now_ms: int) -> int:
    if row is None or row.activity_type == ACTIVITY_TYPE_TRIAL:
        return day_key(now_ms)
    if row.start_at_ms == 0 and row.end_at_ms == 0:
        return day_key(now_ms)
    return day_key(row.start_at_ms)


def select_for_activity_type(rows: Iterable[ActivityRuleRow], activity_type: int, now_ms: int) -> ActivityRuleRow | None:
    open_row: ActivityRuleRow | None = None
    upcoming_row: ActivityRuleRow | None = None
    lowest_row: ActivityRuleRow | None = None
    for row in rows:
        if row.activity_type != activity_type:
            continue
        state = activity_state_of(row, now_ms)
        if state == ACTIVITY_STATE_OPEN:
            if open_row is None or row.activity_id < open_row.activity_id:
                open_row = row
        elif state == ACTIVITY_STATE_UPCOMING:
            if (
                upcoming_row is None
                or row.start_at_ms < upcoming_row.start_at_ms
                or (row.start_at_ms == upcoming_row.start_at_ms and row.activity_id < upcoming_row.activity_id)
            ):
                upcoming_row = row
        if lowest_row is None or row.activity_id < lowest_row.activity_id:
            lowest_row = row
    if open_row is not None:
        return open_row
    if upcoming_row is not None:
        return upcoming_row
    return lowest_row


def select_visible_activities(rows: Iterable[ActivityRuleRow], now_ms: int) -> list[ActivityRuleRow]:
    row_list = tuple(rows)
    out: list[ActivityRuleRow] = []
    for typ in ALL_ACTIVITY_TYPES:
        picked = select_for_activity_type(row_list, typ, now_ms)
        if picked is not None:
            out.append(picked)
    return out


def pick_activity_for_write(
    rows: Iterable[ActivityRuleRow], activity_type: int, req_id: int, now_ms: int
) -> ActivityRuleRow | None:
    picked = select_for_activity_type(rows, activity_type, now_ms)
    if picked is None or picked.activity_id != req_id or activity_state_of(picked, now_ms) != ACTIVITY_STATE_OPEN:
        return None
    return picked


def joined_long_enough(join_time_ms: int, now_ms: int, min_hours: int) -> bool:
    if min_hours == 0:
        return True
    if join_time_ms == 0 or join_time_ms > now_ms:
        return False
    return (now_ms - join_time_ms) >= min_hours * MS_PER_HOUR


def reunion_threshold(row: ActivityRuleRow, min_online_rule: int) -> int:
    return max(row.guild_threshold, min_online_rule)


def reunion_observed(online: int, threshold: int) -> bool:
    return threshold > 0 and online >= threshold


def lantern_outcome(row: ActivityRuleRow, progress: ActivityProgress) -> tuple[bool, bool]:
    """返回 ``(reached_now, grant_funds)``。"""
    reached = row.guild_threshold > 0 and progress.count >= row.guild_threshold
    return reached and not progress.latched, reached and not progress.funds_granted and row.guild_funds > 0


def reunion_outcome(row: ActivityRuleRow, progress: ActivityProgress, observed: bool) -> tuple[bool, bool, bool]:
    """返回 ``(ok, reached_now, grant_funds)``。"""
    if not progress.latched and not observed:
        return False, False, False
    return True, not progress.latched, not progress.funds_granted and row.guild_funds > 0


def trial_funds_counted(row: ActivityRuleRow, counted_wins: int) -> bool:
    """历练胜利是否计入帮会资金：配表资金 > 0 且当日已计资金胜场未达上限。"""
    return row.guild_funds > 0 and counted_wins < row.guild_threshold


def activity_blocked(
    *,
    state: int,
    guild_level: int,
    min_level: int,
    joined: bool,
    join_min_hours: int,
    used: int,
    daily_limit: int,
    activity_type: int,
    threshold_reached: bool,
    progress: int,
    threshold: int,
) -> tuple[int, tuple[str, ...]]:
    """按固定优先级返回首个不满足项的 ``(tip_id, parameters)``。"""
    if state != ACTIVITY_STATE_OPEN:
        return ERR_GUILD_ACTIVITY_NOT_OPEN, ()
    if guild_level < min_level:
        return ERR_GUILD_ACTIVITY_LEVEL_TOO_LOW, (str(min_level),)
    if not joined:
        return ERR_GUILD_ACTIVITY_JOIN_TOO_RECENT, (str(join_min_hours),)
    if used >= daily_limit:
        return ERR_GUILD_ACTIVITY_ALREADY_CLAIMED, ()
    if activity_type == ACTIVITY_TYPE_REUNION and not threshold_reached and not reunion_observed(progress, threshold):
        return ERR_GUILD_ACTIVITY_THRESHOLD_NOT_REACHED, (str(progress), str(threshold))
    return 0, ()


# -- 配表快照包装（GuildRule / GuildLevel / GuildDonate / GuildShop / GuildActivity / Reward / Item） --


@dataclass(frozen=True, slots=True)
class GuildRuleConfig:
    application_expire_hours: int = 72
    max_pending_applications_per_player: int = 3
    max_pending_applications_per_guild: int = 50
    asset_op_deadline_seconds: int = 600
    asset_op_retry_base_ms: int = 1000
    reunion_min_online_members: int = 3
    activity_join_min_hours: int = 0
    rename_cooldown_seconds: int = 7 * 86400
    name_hold_seconds: int = 7 * 86400
    rename_cost_funds: int = 1000
    trial_invite_ttl_seconds: int = 30
    trial_invite_cooldown_seconds: int = 10

    @property
    def application_ttl_ms(self) -> int:
        return self.application_expire_hours * MS_PER_HOUR

    @property
    def rename_cooldown_ms(self) -> int:
        return self.rename_cooldown_seconds * 1000

    @property
    def name_hold_ms(self) -> int:
        return self.name_hold_seconds * 1000


@dataclass(frozen=True, slots=True)
class GuildLevelConfig:
    level: int
    max_members: int
    max_officers: int
    upgrade_cost_funds: int


@dataclass(frozen=True, slots=True)
class GuildDonateConfig:
    donate_id: int
    name: str
    currency_type: int
    cost_amount: int
    contribution_gain: int
    funds_gain: int
    daily_limit: int
    min_guild_level: int


@dataclass(frozen=True, slots=True)
class GuildShopConfig:
    goods_id: int
    name: str
    category: int
    item_id: int
    item_count: int
    cost_contribution: int
    required_guild_level: int
    limit_period: int
    limit_count: int


def max_shop_buy_count(goods: GuildShopConfig, max_stack: int) -> int:
    if goods.item_count <= 0 or max_stack <= 0:
        return 0
    by_stack = max_stack // goods.item_count
    if by_stack <= 0:
        return 0
    return min(MAX_SHOP_BUY_COUNT, by_stack)


class GuildTableCatalog:
    """只读帮会配表视图，从 ConfigTables 加载并做启动期自洽校验。"""

    def __init__(
        self,
        *,
        rule: GuildRuleConfig,
        levels: Mapping[int, GuildLevelConfig],
        donates: Mapping[int, GuildDonateConfig],
        shops: Mapping[int, GuildShopConfig],
        activities: tuple[ActivityRuleRow, ...],
        rewards: Mapping[int, tuple[tuple[int, int], ...]],
        item_max_stacks: Mapping[int, int],
    ) -> None:
        self.rule = rule
        self.levels = dict(levels)
        self.donates = dict(donates)
        self.shops = dict(shops)
        self.activities = tuple(activities)
        self.rewards = {k: tuple(v) for k, v in rewards.items()}
        self.item_max_stacks = dict(item_max_stacks)
        self.validate()

    @classmethod
    def from_config_tables(cls, tables: ConfigTables | None = None) -> "GuildTableCatalog":
        tables = ConfigTables() if tables is None else tables
        rule_row = tables["GuildRule"].get(GUILD_RULE_ROW_ID)
        if rule_row is None:
            raise ValueError(f"GuildRule table missing id={GUILD_RULE_ROW_ID}")
        rule = GuildRuleConfig(
            application_expire_hours=int(getattr(rule_row, "application_expire_hours", 72)),
            max_pending_applications_per_player=int(getattr(rule_row, "max_pending_applications_per_player", 3)),
            max_pending_applications_per_guild=int(getattr(rule_row, "max_pending_applications_per_guild", 50)),
            asset_op_deadline_seconds=int(getattr(rule_row, "asset_op_deadline_seconds", 600)),
            asset_op_retry_base_ms=int(getattr(rule_row, "asset_op_retry_base_ms", 1000)),
            reunion_min_online_members=int(getattr(rule_row, "reunion_min_online_members", 3)),
            activity_join_min_hours=int(getattr(rule_row, "activity_join_min_hours", 0)),
            rename_cooldown_seconds=int(getattr(rule_row, "rename_cooldown_seconds", 7 * 86400)),
            name_hold_seconds=int(getattr(rule_row, "name_hold_seconds", 7 * 86400)),
            rename_cost_funds=int(getattr(rule_row, "rename_cost_funds", 1000)),
            trial_invite_ttl_seconds=int(getattr(rule_row, "trial_invite_ttl_seconds", 30)) or 30,
            trial_invite_cooldown_seconds=int(getattr(rule_row, "trial_invite_cooldown_seconds", 10)),
        )

        levels: dict[int, GuildLevelConfig] = {}
        for row in tables["GuildLevel"].rows():
            lvl = int(row.id)
            levels[lvl] = GuildLevelConfig(
                level=lvl,
                max_members=int(row.max_members),
                max_officers=int(row.max_officers),
                upgrade_cost_funds=int(row.upgrade_cost_funds),
            )

        donates: dict[int, GuildDonateConfig] = {}
        for row in tables["GuildDonate"].rows():
            did = int(row.id)
            donates[did] = GuildDonateConfig(
                donate_id=did,
                name=str(row.name),
                currency_type=int(row.currency_type),
                cost_amount=int(row.cost_amount),
                contribution_gain=int(row.contribution_gain),
                funds_gain=int(row.funds_gain),
                daily_limit=int(row.daily_limit),
                min_guild_level=int(row.min_guild_level),
            )

        shops: dict[int, GuildShopConfig] = {}
        for row in tables["GuildShop"].rows():
            gid = int(row.id)
            shops[gid] = GuildShopConfig(
                goods_id=gid,
                name=str(row.name),
                category=int(row.category),
                item_id=int(row.item_id),
                item_count=int(row.item_count),
                cost_contribution=int(row.cost_contribution),
                required_guild_level=int(row.required_guild_level),
                limit_period=int(row.limit_period),
                limit_count=int(row.limit_count),
            )

        activities: list[ActivityRuleRow] = []
        for row in tables["GuildActivity"].rows():
            activities.append(
                ActivityRuleRow(
                    activity_id=int(row.id),
                    activity_type=int(row.type),
                    name=str(row.name),
                    enabled=bool(row.enabled),
                    start_at_ms=int(getattr(row, "start_at_ms", 0)),
                    end_at_ms=int(getattr(row, "end_at_ms", 0)),
                    min_guild_level=int(row.min_guild_level),
                    personal_contribution=int(row.personal_contribution),
                    guild_funds=int(row.guild_funds),
                    guild_threshold=int(row.guild_threshold),
                    reward_id=int(getattr(row, "reward_id", 0)),
                    daily_limit=int(row.daily_limit),
                    dungeon_id=int(getattr(row, "dungeon_id", 0)),
                    team_size_min=int(getattr(row, "team_size_min", 0)),
                    team_size_max=int(getattr(row, "team_size_max", 0)),
                )
            )

        rewards: dict[int, tuple[tuple[int, int], ...]] = {}
        for row in tables["Reward"].rows():
            merged: dict[int, int] = {}
            for slot in getattr(row, "reward", ()):
                cnt = int(slot.reward_count)
                if cnt == 0:
                    continue
                item_id = int(slot.reward_item)
                merged[item_id] = merged.get(item_id, 0) + cnt
            rewards[int(row.id)] = tuple(sorted(merged.items()))

        item_max_stacks: dict[int, int] = {}
        for row in tables["Item"].rows():
            stack = int(getattr(row, "max_stack_size", 0)) or 1
            item_max_stacks[int(row.id)] = stack

        return cls(
            rule=rule,
            levels=levels,
            donates=donates,
            shops=shops,
            activities=tuple(activities),
            rewards=rewards,
            item_max_stacks=item_max_stacks,
        )

    def validate(self) -> None:
        if not 1 <= self.rule.application_expire_hours <= 720:
            raise ValueError(f"GuildRule.application_expire_hours out of range: {self.rule.application_expire_hours}")
        if not 1 <= self.rule.max_pending_applications_per_player <= 10:
            raise ValueError("GuildRule.max_pending_applications_per_player out of [1,10]")
        if not 1 <= self.rule.max_pending_applications_per_guild <= 500:
            raise ValueError("GuildRule.max_pending_applications_per_guild out of [1,500]")
        if not self.levels:
            raise ValueError("GuildLevel table is empty")
        prev_members = 0
        prev_officers = 0
        sorted_levels = [self.levels[k] for k in sorted(self.levels)]
        for idx, row in enumerate(sorted_levels):
            expected_level = idx + 1
            if row.level != expected_level:
                raise ValueError(f"GuildLevel ids must be contiguous from 1, got {row.level} expected {expected_level}")
            if not 2 <= row.max_members <= MAX_GUILD_MEMBERS_CAP:
                raise ValueError(f"GuildLevel[{row.level}].max_members invalid: {row.max_members}")
            if row.max_officers >= row.max_members:
                raise ValueError(f"GuildLevel[{row.level}].max_officers >= max_members")
            if row.max_members < prev_members or row.max_officers < prev_officers:
                raise ValueError(f"GuildLevel[{row.level}] caps must be non-decreasing")
            is_last = idx == len(sorted_levels) - 1
            if (row.upgrade_cost_funds == 0) != is_last:
                raise ValueError(f"GuildLevel[{row.level}].upgrade_cost_funds zero iff max level")
            prev_members = row.max_members
            prev_officers = row.max_officers

    def sorted_donates(self) -> list[GuildDonateConfig]:
        return [self.donates[k] for k in sorted(self.donates)]

    def sorted_shops(self) -> list[GuildShopConfig]:
        return sorted(self.shops.values(), key=lambda g: (g.category, g.goods_id))

    def reward_items(self, reward_id: int) -> tuple[tuple[int, int], ...]:
        if reward_id == 0:
            return ()
        items = self.rewards.get(reward_id)
        if items is None:
            raise ValueError(f"Reward table missing id={reward_id}")
        if len(items) > MAX_REWARD_ITEM_KINDS:
            raise ValueError(f"Reward[{reward_id}] exceeds {MAX_REWARD_ITEM_KINDS} item kinds")
        total = sum(cnt for _, cnt in items)
        if total > MAX_REWARD_TOTAL_ITEMS:
            raise ValueError(f"Reward[{reward_id}] total items {total} exceeds {MAX_REWARD_TOTAL_ITEMS}")
        return items
