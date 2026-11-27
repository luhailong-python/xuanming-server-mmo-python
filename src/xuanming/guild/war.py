"""帮会全天帮战 V2 核心逻辑（docs/design/guild-play-v2.md）。

涵盖：
- 配置表驱动（GuildWarRule / GuildWarMilestone）；
- 三座灵脉（八仙岛/碧渊水宫/月汐湾）每帮限占 1 座、旗势与 15 分钟护盾、每小时金脉 1.5x 轮换与周期分红；
- 5 槽守旗阵 AI 托管快照（60s 刷新冷却、长老/帮主置顶与下阵）；
- 无匹配池 0 秒开战（一键快速、真人直打、守旗踢馆、追杀结单、野外现场），可选委托 MatchInternal.StartGuildWarBattle；
- 幂等终局结算（660 材料口径、30 分钟防互刷递减、连胜悬赏、破旗首功、宿敌 +20%、仇敌录与追杀/反杀撕单）；
- 每周战功 4 档里程碑宝箱与灵脉分红（换帮不重置个人已领位图/次数）。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from functools import lru_cache

from google.protobuf.message import Message

from ..data.config_tables import ConfigTableError, ConfigTables
from ..protocol import Protocol
from .errors import (
    ERR_GUILD_FUNDS_INSUFFICIENT,
    ERR_GUILD_NOT_IN_GUILD,
    ERR_GUILD_NO_PERMISSION,
    ERR_GUILD_RANK_TOO_LOW,
    ERR_GUILD_WAR_BOUNTY_ALREADY_ACTIVE,
    ERR_GUILD_WAR_BOUNTY_INVALID_TARGET,
    ERR_GUILD_WAR_BOUNTY_LIMIT,
    ERR_GUILD_WAR_BOUNTY_NOT_FOUND,
    ERR_GUILD_WAR_DIVIDEND_EMPTY,
    ERR_GUILD_WAR_FEUD_ALREADY_ACTIVE,
    ERR_GUILD_WAR_FEUD_COOLDOWN,
    ERR_GUILD_WAR_FEUD_INVALID_TARGET,
    ERR_GUILD_WAR_FEUD_NOT_FOUND,
    ERR_GUILD_WAR_GARRISON_COOLDOWN,
    ERR_GUILD_WAR_GARRISON_FULL,
    ERR_GUILD_WAR_LEYLINE_NOT_FOUND,
    ERR_GUILD_WAR_MILESTONE_CLAIMED,
    ERR_GUILD_WAR_MILESTONE_LOCKED,
    ERR_GUILD_WAR_PEACE_COOLDOWN,
    ERR_GUILD_WAR_SAME_GUILD,
    ERR_GUILD_WAR_SELF_PEACE_MODE,
    ERR_GUILD_WAR_SERVICE_BUSY,
    ERR_GUILD_WAR_TARGET_IN_BATTLE,
    ERR_GUILD_WAR_TARGET_OFFLINE,
    ERR_GUILD_WAR_TARGET_PEACE_MODE,
    ERR_GUILD_WAR_TARGET_PROTECTED,
    ERR_GUILD_WAR_TRACK_COOLDOWN,
    ERR_INVALID_PARAMETER,
)
from .model import (
    BATTLE_ACTIVITY_KIND_GUILD_WAR,
    BATTLE_OUTCOME_DRAW,
    BATTLE_OUTCOME_SIDE_A_WIN,
    BATTLE_OUTCOME_SIDE_B_WIN,
    CreditFenceVerdict,
    GuildData,
    MATCH_MODE_GUILD_WAR_PVP,
    MATCH_MODE_GUILD_WAR_SIEGE,
    MATCH_MODE_JIANGHU_DUEL,
    MS_PER_DAY,
    OP_KIND_BOUNTY_PAYOUT,
    OP_KIND_WAR_REWARD,
    TX_GUILD_BOUNTY_PAYOUT,
    TX_GUILD_WAR_REWARD,
    WarCreditOwed,
    WarCreditOwedCursor,
    WarOwedConvertStatus,
    MS_PER_HOUR,
    ORDER_STATUS_APPLIED,
    RANK_OFFICER,
    RoleOriginal,
    day_key,
    next_weekly_reset_ms,
    role_rank,
    week_key,
)
from .store import GuildStoreCore, OnlineStatusResolver, PlayerNameResolver

logger = logging.getLogger(__name__)

# -- 帮战枚举常量（与 proto/guild/guild.proto 严格一致）----------------------------------------------

TARGET_KIND_QUICK_AUTO = 1
TARGET_KIND_LIVE_PLAYER = 2
TARGET_KIND_LEYLINE_GARRISON = 3
TARGET_KIND_BOUNTY = 4
TARGET_KIND_FIELD_PLAYER = 5

BATTLE_KIND_PANEL_PVP = 1
BATTLE_KIND_GARRISON_SIEGE = 2
BATTLE_KIND_FIELD_ROAD = 3
BATTLE_KIND_LEYLINE_PVP = 4
BATTLE_KIND_BOUNTY_HUNT = 5

BOUNTY_TIER_NORMAL_SILVER = 1
BOUNTY_TIER_HEAVY_SILVER = 2
BOUNTY_TIER_GUILD_PUBLIC = 3

BOUNTY_STATUS_ESCROW_PENDING = 1
BOUNTY_STATUS_ACTIVE = 2
BOUNTY_STATUS_COMPLETED = 3
BOUNTY_STATUS_TORN_BY_TARGET = 4
BOUNTY_STATUS_EXPIRED = 5

ALERT_LEYLINE_UNDER_SIEGE = 1
ALERT_LEYLINE_CAPTURED = 2
ALERT_PURSUER_ENTERED_MAP = 3
ALERT_BOUNTY_RESOLVED = 4
ALERT_STREAK_BOUNTY = 5
ALERT_FEUD_CHALLENGE = 6
ALERT_FEUD_STARTED = 7
ALERT_FEUD_ENDED = 8

# 宿敌约战战书状态（GuildWarFeudStatus）与规则常量（对齐 Go war/rules.go）
FEUD_STATUS_PENDING = 1
FEUD_STATUS_ACTIVE = 2
FEUD_STATUS_FINISHED = 3
FEUD_STATUS_DECLINED = 4
FEUD_STATUS_EXPIRED = 5

FEUD_BONUS_BPS = 2500
FEUD_ACCEPT_WINDOW_SEC = 900
FEUD_DURATION_SEC = 7200
FEUD_COOLDOWN_SEC = 86400
FEUD_FREE_DIMINISH_FIGHTS = 3
STREAK_BROADCAST_MIN = 5
MAX_GUILD_JIANGHU_WALL_RECORDS = 20

BOUNTY_TAG_NONE = 0
BOUNTY_TAG_STREAK_END = 1
BOUNTY_TAG_HUNT_WIN = 2
BOUNTY_TAG_COUNTER_KILL = 3
BOUNTY_TAG_COUNTER_TEAR = 4
BOUNTY_TAG_BANNER_BREAK = 5

LEYLINES: tuple[tuple[int, str, int], ...] = (
    (1, "八仙岛", 2),
    (2, "碧渊水宫", 5),
    (3, "月汐湾", 3),
)
LEYLINE_BY_ID: dict[int, tuple[str, int]] = {lid: (name, scene_cfg) for lid, name, scene_cfg in LEYLINES}

_DIVIDEND_FUNDS_PER_TICK = 600
_RECENT_LOG_LIMIT = 30
_ENEMY_RETENTION_MS = MS_PER_DAY
_INSPECT_MAX_TARGETS = 30
_RIVALRY_CLASH_THRESHOLD = 5
_SIEGE_ALERT_COOLDOWN_MS = 60_000
_STREAK_MIN_DISTINCT = 2
_PUBLIC_BOUNTY_REWARD_SILVER = 4500


# -- 不可变配置模型 ----------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GuildWarRuleConfig:
    """GuildWarRule 配表行（默认 id=1）。"""

    id: int = 1
    battle_config_id: int = 1
    min_level: int = 15
    peace_cooldown_sec: int = 60
    post_battle_protect_sec: int = 10
    garrison_max_slots: int = 5
    garrison_refresh_cooldown_sec: int = 60
    leyline_max_banner_hp: int = 1000
    leyline_capture_init_hp: int = 600
    leyline_shield_sec: int = 900
    pvp_win_banner_damage: int = 100
    pvp_defend_banner_heal: int = 60
    siege_win_banner_damage: int = 35
    gold_leyline_multiplier_bps: int = 15000
    rivalry_bonus_bps: int = 2000
    diminish_window_sec: int = 1800
    dividend_interval_sec: int = 3600
    dividend_guild_weekly_cap: int = 24
    dividend_player_weekly_cap: int = 12
    dividend_min_personal_merit: int = 15
    bounty_normal_silver: int = 5000
    bounty_heavy_silver: int = 15000
    bounty_public_funds: int = 2000
    bounty_public_daily_limit: int = 2
    bounty_tax_bps: int = 1000
    bounty_ttl_sec: int = 86400
    bounty_counter_kill_tear: int = 2
    bounty_protect_sec: int = 1800
    bounty_daily_payout_limit: int = 5
    track_cooldown_sec: int = 15
    frenzy_start_round: int = 12


@dataclass(frozen=True, slots=True)
class GuildWarMilestoneConfig:
    """GuildWarMilestone 配表行（档位 1..4）。"""

    milestone_id: int
    required_guild_merit: int
    reward_contribution: int
    reward_silver: int
    reward_items: tuple[tuple[int, int], ...]
    reward_preview: str


@lru_cache(maxsize=1)
def _default_config_tables() -> ConfigTables | None:
    try:
        return ConfigTables()
    except (ConfigTableError, FileNotFoundError, OSError):
        return None


def load_guild_war_tables(
    tables: ConfigTables | None = None,
) -> tuple[GuildWarRuleConfig, dict[int, GuildWarMilestoneConfig]]:
    """从配置表加载 GuildWarRule 与 GuildWarMilestone；缺表时使用首测基准常量兜底。"""
    source = tables if tables is not None else _default_config_tables()
    rule = GuildWarRuleConfig()
    milestones: dict[int, GuildWarMilestoneConfig] = {}

    if source is not None and "GuildWarRule" in source.tables:
        row = source["GuildWarRule"].get(1)
        if row is not None:
            rule = GuildWarRuleConfig(
                id=int(getattr(row, "id", 1)) or 1,
                battle_config_id=int(getattr(row, "battle_config_id", 1)) or 1,
                min_level=int(getattr(row, "min_level", 15)),
                peace_cooldown_sec=int(getattr(row, "peace_cooldown_sec", 60)),
                post_battle_protect_sec=int(getattr(row, "post_battle_protect_sec", 10)),
                garrison_max_slots=max(1, int(getattr(row, "garrison_max_slots", 5))),
                garrison_refresh_cooldown_sec=int(getattr(row, "garrison_refresh_cooldown_sec", 60)),
                leyline_max_banner_hp=max(1, int(getattr(row, "leyline_max_banner_hp", 1000))),
                leyline_capture_init_hp=max(1, int(getattr(row, "leyline_capture_init_hp", 600))),
                leyline_shield_sec=int(getattr(row, "leyline_shield_sec", 900)),
                pvp_win_banner_damage=int(getattr(row, "pvp_win_banner_damage", 100)),
                pvp_defend_banner_heal=int(getattr(row, "pvp_defend_banner_heal", 60)),
                siege_win_banner_damage=int(getattr(row, "siege_win_banner_damage", 35)),
                gold_leyline_multiplier_bps=int(getattr(row, "gold_leyline_multiplier_bps", 15000)),
                rivalry_bonus_bps=int(getattr(row, "rivalry_bonus_bps", 2000)),
                diminish_window_sec=int(getattr(row, "diminish_window_sec", 1800)),
                dividend_interval_sec=max(60, int(getattr(row, "dividend_interval_sec", 3600))),
                dividend_guild_weekly_cap=int(getattr(row, "dividend_guild_weekly_cap", 24)),
                dividend_player_weekly_cap=int(getattr(row, "dividend_player_weekly_cap", 12)),
                dividend_min_personal_merit=int(getattr(row, "dividend_min_personal_merit", 15)),
                bounty_normal_silver=int(getattr(row, "bounty_normal_silver", 5000)),
                bounty_heavy_silver=int(getattr(row, "bounty_heavy_silver", 15000)),
                bounty_public_funds=int(getattr(row, "bounty_public_funds", 2000)),
                bounty_public_daily_limit=int(getattr(row, "bounty_public_daily_limit", 2)),
                bounty_tax_bps=int(getattr(row, "bounty_tax_bps", 1000)),
                bounty_ttl_sec=int(getattr(row, "bounty_ttl_sec", 86400)),
                bounty_counter_kill_tear=max(1, int(getattr(row, "bounty_counter_kill_tear", 2))),
                bounty_protect_sec=int(getattr(row, "bounty_protect_sec", 1800)),
                bounty_daily_payout_limit=int(getattr(row, "bounty_daily_payout_limit", 5)),
                track_cooldown_sec=int(getattr(row, "track_cooldown_sec", 15)),
                frenzy_start_round=int(getattr(row, "frenzy_start_round", 12)),
            )

    if source is not None and "GuildWarMilestone" in source.tables:
        for row in source["GuildWarMilestone"].rows():
            mid = int(getattr(row, "id", 0))
            if mid <= 0:
                continue
            items: list[tuple[int, int]] = []
            for idx in (1, 2):
                item_id = int(getattr(row, f"reward_item{idx}_id", 0))
                cnt = int(getattr(row, f"reward_item{idx}_count", 0))
                if item_id > 0 and cnt > 0:
                    items.append((item_id, cnt))
            milestones[mid] = GuildWarMilestoneConfig(
                milestone_id=mid,
                required_guild_merit=int(getattr(row, "required_guild_merit", 0)),
                reward_contribution=int(getattr(row, "reward_contribution", 0)),
                reward_silver=int(getattr(row, "reward_silver", 0)),
                reward_items=tuple(items),
                reward_preview=str(getattr(row, "reward_preview", "")),
            )

    if not milestones:
        for mid, req, contrib, silver, items, preview in (
            (1, 300, 80, 5000, ((3403, 2),), "重铸砂×2 + 银两×5000 + 帮贡×80"),
            (2, 1200, 120, 8000, ((3406, 4), (3403, 3)), "悟道残页×4 + 重铸砂×3 + 银两×8000"),
            (3, 3000, 180, 12000, ((3406, 6), (20, 20)), "悟道残页×6 + 灵兽口粮×20 + 银两×12000"),
            (4, 6000, 300, 20000, ((3403, 5), (3406, 8)), "重铸砂×5 + 悟道残页×8 + 帮贡×300"),
        ):
            milestones[mid] = GuildWarMilestoneConfig(mid, req, contrib, silver, items, preview)

    return rule, milestones


# -- 内部状态模型（与 guild_db.proto 表 13..19 严格对齐）---------------------------------------------


@dataclass(slots=True)
class _LeylineState:
    zone_id: int
    leyline_id: int
    owner_guild_id: int = 0
    banner_hp: int = 1000
    shield_until_ms: int = 0
    captured_at_ms: int = 0
    last_dividend_at_ms: int = 0
    dividend_week_key: int = 0
    weekly_dividend_count: int = 0
    last_siege_alert_ms: int = 0
    active_battle_ids: set[int] = field(default_factory=set)
    updated_ms: int = 0


@dataclass(slots=True)
class _GarrisonSlot:
    slot_index: int
    player_id: int
    player_name: str
    level: int = 30
    appearance_id: str = ""
    pet_name: str = ""
    pet_table_id: int = 0
    updated_ms: int = 0


@dataclass(slots=True)
class _GuildWeeklyState:
    guild_id: int
    week_key: int
    guild_merit: int = 0
    dividend_pool_count: int = 0
    member_merit: dict[int, int] = field(default_factory=dict)
    updated_ms: int = 0


@dataclass(slots=True)
class _PlayerWarState:
    player_id: int
    zone_id: int = 1
    peace_mode: bool = False
    peace_ready_at_ms: int = 0
    open_challenge: bool = False
    protect_until_ms: int = 0
    streak_count: int = 0
    streak_distinct_opponents: set[int] = field(default_factory=set)
    streak_last_opponent_id: int = 0
    weekly_week_key: int = 0
    weekly_merit: int = 0
    milestone_claimed_mask: int = 0  # bit 0..3 对应档位 1..4，换帮不重置
    dividend_claimed_count: int = 0  # 本周已领分红份数，换帮不重置
    bounty_protect_until_ms: int = 0
    last_garrison_refresh_ms: int = 0
    last_track_ms: int = 0
    bounty_payout_day_key: int = 0
    bounty_payouts_today: int = 0
    in_battle_id: int = 0
    updated_ms: int = 0


@dataclass(slots=True)
class _BountyState:
    bounty_id: int
    zone_id: int
    issuer_player_id: int
    issuer_guild_id: int
    target_player_id: int
    target_guild_id: int
    tier: int
    escrow_silver: int
    public_board: bool
    status: int = BOUNTY_STATUS_ACTIVE
    escrow_guild_funds: int = 0
    counter_kills: int = 0
    last_counter_attacker_id: int = 0
    hunter_player_id: int = 0
    created_at_ms: int = 0
    expires_at_ms: int = 0
    resolved_at_ms: int = 0
    last_scene_config_id: int = 0
    last_channel_no: int = 0


_TERMINAL_BOUNTY_STATUSES = frozenset({
    BOUNTY_STATUS_COMPLETED,
    BOUNTY_STATUS_TORN_BY_TARGET,
    BOUNTY_STATUS_EXPIRED,
    6,  # GUILD_WAR_BOUNTY_STATUS_ESCROW_FAILED
})
WAR_BOUNTY_TERMINAL_RETENTION_MS = 30 * MS_PER_DAY


@dataclass(frozen=True, slots=True)
class BannerDelta:
    """灵脉旗势变化与事务内快照一致性比对基线（r2-gw#3，对齐 Go war.BannerDelta）。"""

    hp_before: int = 0
    owner_before: int = 0
    shield_until_before: int = 0
    attacker_owns_other: bool = False
    hp_after: int = 0
    owner_after: int = 0
    shield_until_after: int = 0
    broke_banner: bool = False
    captured_by_winner: bool = False
    break_bonus_funds: int = 0
    break_bonus_silver: int = 0

    def basis_holds(self, cur: _LeylineState | None, attacker_owns_other: bool) -> bool:
        """校验锁内当前灵脉行是否仍与事务外计算时一致（防并发结算错扣旗势或兼并第 2 座）。"""
        return (
            cur is not None
            and cur.owner_guild_id == self.owner_before
            and cur.banner_hp == self.hp_before
            and cur.shield_until_ms == self.shield_until_before
            and bool(attacker_owns_other) == self.attacker_owns_other
        )


def find_leyline_basis(
    rows: Sequence[_LeylineState],
    leyline_id: int,
    attacker_guild_id: int,
) -> tuple[_LeylineState | None, bool]:
    """从同区灵脉行中取出目标灵脉并判攻方是否已占领其余灵脉（r2-gw#3）。"""
    cur = None
    owns_other = False
    for lr in rows:
        if lr.leyline_id == leyline_id:
            cur = lr
        elif attacker_guild_id > 0 and lr.owner_guild_id == attacker_guild_id:
            owns_other = True
    return cur, owns_other


def client_war_target_hints(target_kind: int, req_leyline_id: int, req_bounty_id: int) -> tuple[int, int]:
    """按目标种类收窄允许采信的客户端字段（r2-gw#1）：仅守旗阵采信 leyline_id，仅追杀榜采信 bounty_id。"""
    if target_kind == TARGET_KIND_LEYLINE_GARRISON:
        return req_leyline_id, 0
    if target_kind == TARGET_KIND_BOUNTY:
        return 0, req_bounty_id
    return 0, 0


def bounty_huntable(b: _BountyState | None, zone_id: int, hunter_guild_id: int, now_ms: int) -> bool:
    """判定追杀令此刻能否由 hunter_guild_id 在 zone_id 接单/结单/展示（r2-gw#1 / #8）。"""
    return (
        b is not None
        and b.zone_id == zone_id
        and b.status == BOUNTY_STATUS_ACTIVE
        and b.expires_at_ms > now_ms
        and (bool(b.public_board) or b.issuer_guild_id == hunter_guild_id)
    )


def field_target_bounty(
    b: _BountyState | None, zone_id: int, viewer_guild_id: int, now_ms: int
) -> tuple[int, int, int]:
    """野外点选面板可见追杀令（r2-gw#8）：仅发单帮可见的单对别帮返回 (0, 0, 0)。"""
    if not bounty_huntable(b, zone_id, viewer_guild_id, now_ms) or b is None:
        return 0, 0, 0
    return b.bounty_id, b.escrow_silver, b.tier


def bounty_expiry_refund(b: _BountyState) -> tuple[int, int]:
    """ACTIVE 追杀令到期原路退回本金（r2-gw#2）：公费档退预扣帮会资金，银两档退扣税后托管银两。"""
    if b.tier == BOUNTY_TIER_GUILD_PUBLIC:
        return 0, b.escrow_guild_funds
    return b.escrow_silver, 0


@dataclass(slots=True)
class WarCreditOwedStats:
    """一轮 guild_war_credit_owed 后台转换统计（r2-gw#5 / #6）。"""

    scanned: int = 0
    converted: int = 0
    still_full: int = 0
    frozen: int = 0
    abandoned: int = 0
    gone: int = 0
    failed: int = 0


@dataclass(slots=True)
class _SideReward:
    contribution: int = 0
    silver: int = 0
    funds: int = 0
    merit: int = 0
    material_units: int = 0
    material_tradable: int = 0
    material_pvp_loss_ineligible: bool = False
    jackpot_contribution: int = 0
    jackpot_silver: int = 0


@dataclass(slots=True)
class _WarBattleState:
    battle_id: int
    zone_id: int
    war_battle_kind: int
    match_mode: int
    leyline_id: int
    bounty_id: int
    attacker_guild_id: int
    defender_guild_id: int
    attacker_guild_name: str
    defender_guild_name: str
    attacker_ids: tuple[int, ...]
    defender_ids: tuple[int, ...]
    attacker_names: tuple[str, ...]
    defender_names: tuple[str, ...]
    week_key: int
    gold_leyline_bonus: bool
    rivalry_bonus: bool
    started_at_ms: int
    feud_bonus: bool = False
    settled: bool = False
    outcome: int = 0
    rounds: int = 0
    banner_hp_before: int = 0
    banner_hp_after: int = 0
    banner_shielded: bool = False
    bounty_outcome_tag: int = BOUNTY_TAG_NONE
    diminish_percent: int = 100
    attacker_reward: _SideReward = field(default_factory=_SideReward)
    defender_reward: _SideReward = field(default_factory=_SideReward)
    streak_after: dict[int, int] = field(default_factory=dict)
    finished_at_ms: int = 0


@dataclass(slots=True)
class _EnemyRecord:
    player_id: int
    player_name: str
    guild_id: int
    guild_name: str
    victim_name: str
    battle_kind: int
    occurred_at_ms: int


@dataclass(slots=True)
class _FeudState:
    feud_id: int
    zone_id: int
    issuer_guild_id: int
    issuer_guild_name: str
    target_guild_id: int
    target_guild_name: str
    status: int = FEUD_STATUS_PENDING
    issuer_wins: int = 0
    target_wins: int = 0
    issuer_merit: int = 0
    target_merit: int = 0
    created_at_ms: int = 0
    accept_deadline_ms: int = 0
    started_at_ms: int = 0
    ends_at_ms: int = 0
    winner_guild_id: int = 0
    updated_ms: int = 0


@dataclass(slots=True)
class _GuildJianghuRivalSummary:
    rival_guild_id: int
    rival_guild_name: str = ""
    wins: int = 0
    losses: int = 0
    draws: int = 0
    last_battle_ms: int = 0


@dataclass(slots=True)
class _GuildJianghuGrudgeRec:
    battle_id: int
    rival_guild_id: int
    rival_guild_name: str
    outcome_for_guild: int
    writ_tier: int
    team_size: int
    total_rounds: int
    our_captain_name: str
    rival_captain_name: str
    finished_ms: int


def _diminish_percent_for_count(prior_count: int, *, threshold: int = 0, feud_active: bool = False) -> int:
    """30 分钟窗口内同对手/同守旗阵防互刷递减；2 小时宿敌约战期间前 3 场免递减、第 4 场 20%、第 5 场起 0%。"""
    if feud_active:
        if prior_count < FEUD_FREE_DIMINISH_FIGHTS:
            return 100
        if prior_count == FEUD_FREE_DIMINISH_FIGHTS:
            return 20
        return 0
    over = prior_count - threshold
    if over <= 0:
        return 100
    if over == 1:
        return 50
    if over == 2:
        return 20
    return 0


def resolve_feud_winner(
    issuer_guild_id: int,
    target_guild_id: int,
    issuer_wins: int,
    target_wins: int,
    issuer_merit: int,
    target_merit: int,
) -> int:
    """约战收官胜负判定：先比胜场，同胜场比约战战功，仍平局返回 0。"""
    if issuer_wins > target_wins:
        return issuer_guild_id
    if target_wins > issuer_wins:
        return target_guild_id
    if issuer_merit > target_merit:
        return issuer_guild_id
    if target_merit > issuer_merit:
        return target_guild_id
    return 0


def format_feud_summary(rec: _FeudState | None) -> str:
    """生成约战收官战报碑摘要（纯服务端拼接，零玩家自由文本）。"""
    if rec is None or rec.status != FEUD_STATUS_FINISHED:
        return ""
    if rec.winner_guild_id == rec.issuer_guild_id and rec.winner_guild_id != 0:
        return f"约战大捷：{rec.issuer_guild_name} {rec.issuer_wins}:{rec.target_wins} 胜 {rec.target_guild_name}"
    if rec.winner_guild_id == rec.target_guild_id and rec.winner_guild_id != 0:
        return f"约战大捷：{rec.target_guild_name} {rec.target_wins}:{rec.issuer_wins} 胜 {rec.issuer_guild_name}"
    return f"势均力敌：{rec.issuer_guild_name} {rec.issuer_wins}:{rec.target_wins} 平 {rec.target_guild_name}"


def should_broadcast_streak_alert(new_streak: int, ended_loser_streak: int) -> bool:
    """达成 5/8 连胜或终结 >=5 连胜时推送非阻断捷报。"""
    return new_streak in (5, 8) or ended_loser_streak >= STREAK_BROADCAST_MIN


def _streak_jackpot(streak: int) -> tuple[int, int]:
    """终结敌方连胜悬赏奖励：(bonus_contribution, bonus_silver)。"""
    if streak >= 8:
        return 350, 10000
    if streak >= 5:
        return 200, 6000
    if streak >= 3:
        return 100, 3000
    return 0, 0


def _gold_leyline_for(now_ms: int) -> tuple[int, int]:
    """返回 (当小时 1.5x 金脉 ID 1..3, 下次轮换时刻 ms)。"""
    hour_index = max(0, int(now_ms)) // MS_PER_HOUR
    return (hour_index % 3) + 1, (hour_index + 1) * MS_PER_HOUR


class GuildWarManager:
    """全天帮战 V2 权威业务协调器（供 GuildService 挂载全部 10 个帮战 RPC 与终局结算调用）。"""

    def __init__(
        self,
        protocol: Protocol,
        store: GuildStoreCore,
        *,
        tables: ConfigTables | None = None,
        player_names: PlayerNameResolver | None = None,
        online_resolver: OnlineStatusResolver | None = None,
        match_war_starter: Callable[[Message], Awaitable[Message]] | None = None,
        player_locator: Callable[[int], Awaitable[tuple[bool, bool, int, int, int]]] | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.rule, self.milestones = load_guild_war_tables(tables)
        self._player_names = player_names
        self._online = online_resolver
        self._match_war_starter = match_war_starter
        self._player_locator = player_locator
        self._clock_ms = clock_ms

        self._lock = asyncio.Lock()
        self._next_battle_id = 700_000
        self._next_bounty_id = 900_000
        self._next_feud_id = 950_000
        self._leylines: dict[tuple[int, int], _LeylineState] = {}
        self._garrisons: dict[int, list[_GarrisonSlot]] = {}
        self._weekly: dict[tuple[int, int], _GuildWeeklyState] = {}
        self._first_wins: dict[int, tuple[tuple[str, ...], int]] = {}
        self._players: dict[int, _PlayerWarState] = {}
        self._bounties: dict[int, _BountyState] = {}
        self._public_bounty_day: dict[tuple[int, int], int] = {}
        self._battles: dict[int, _WarBattleState] = {}
        self._guild_logs: dict[int, deque[int]] = {}
        self._rivalries: dict[tuple[int, int, int], tuple[int, bool]] = {}
        self._enemies: dict[int, list[_EnemyRecord]] = {}
        self._pair_clashes: dict[tuple[int, int], deque[int]] = {}
        self._siege_clashes: dict[tuple[int, int], deque[int]] = {}
        self._feuds: dict[int, _FeudState] = {}
        self._jianghu_rivals: dict[int, dict[int, _GuildJianghuRivalSummary]] = {}
        self._jianghu_records: dict[int, list[_GuildJianghuGrudgeRec]] = {}
        self._jianghu_seen: set[tuple[int, int]] = set()
        self._idempotency: dict[tuple[str, int, int], Message] = {}
        self.alerts: list[Message] = []  # 记录已触发警报，便于推送/观测

    # -- 辅助方法 ------------------------------------------------------------------------------------

    def _tip(self, tip_id: int, *params: str) -> Message:
        return self.protocol.message("TipInfoMessage", id=tip_id, parameters=list(params))

    def _now(self) -> int:
        return max(0, int(self._clock_ms()))

    def _ensure_player(self, player_id: int, zone_id: int, now_ms: int) -> _PlayerWarState:
        wk = week_key(now_ms)
        dk = day_key(now_ms)
        st = self._players.get(player_id)
        if st is None:
            st = _PlayerWarState(
                player_id=player_id,
                zone_id=zone_id,
                weekly_week_key=wk,
                bounty_payout_day_key=dk,
                updated_ms=now_ms,
            )
            self._players[player_id] = st
            return st
        st.zone_id = zone_id or st.zone_id
        if st.weekly_week_key != wk:
            st.weekly_week_key = wk
            st.weekly_merit = 0
            st.milestone_claimed_mask = 0
            st.dividend_claimed_count = 0
        if st.bounty_payout_day_key != dk:
            st.bounty_payout_day_key = dk
            st.bounty_payouts_today = 0
        return st

    def _ensure_leyline(self, zone_id: int, leyline_id: int, now_ms: int) -> _LeylineState:
        key = (zone_id, leyline_id)
        st = self._leylines.get(key)
        if st is None:
            st = _LeylineState(
                zone_id=zone_id,
                leyline_id=leyline_id,
                banner_hp=self.rule.leyline_max_banner_hp,
                dividend_week_key=week_key(now_ms),
                updated_ms=now_ms,
            )
            self._leylines[key] = st
        return st

    def _ensure_weekly(self, guild_id: int, wk: int, now_ms: int) -> _GuildWeeklyState:
        key = (guild_id, wk)
        st = self._weekly.get(key)
        if st is None:
            st = _GuildWeeklyState(guild_id=guild_id, week_key=wk, updated_ms=now_ms)
            self._weekly[key] = st
        return st

    def _rivalry_key(self, g1: int, g2: int, wk: int) -> tuple[int, int, int]:
        return (min(g1, g2), max(g1, g2), wk)

    def _is_rival(self, g1: int, g2: int, wk: int) -> bool:
        if g1 <= 0 or g2 <= 0 or g1 == g2:
            return False
        clashes, bounty_marked = self._rivalries.get(self._rivalry_key(g1, g2, wk), (0, False))
        return bounty_marked or clashes >= _RIVALRY_CLASH_THRESHOLD

    def _active_bounty_for_target(self, target_player_id: int, now_ms: int) -> _BountyState | None:
        for b in self._bounties.values():
            if b.target_player_id == target_player_id and b.status == BOUNTY_STATUS_ACTIVE:
                if b.expires_at_ms > now_ms:
                    return b
        return None

    async def _resolve_names(self, player_ids: Sequence[int]) -> dict[int, str]:
        ids = [int(pid) for pid in dict.fromkeys(player_ids) if int(pid) > 0]
        if not ids:
            return {}
        if self._player_names is not None:
            try:
                return await self._player_names.batch_get_names(ids)
            except Exception:
                logger.warning("guild war player name lookup failed", exc_info=True)
        return {}

    async def _housekeep_locked(self, zone_id: int, now_ms: int) -> None:
        """惰性清理过期追杀令并推进灵脉每小时周期分红（保证多调用幂等不重发）。"""
        wk = week_key(now_ms)
        for b in self._bounties.values():
            if b.status == BOUNTY_STATUS_ACTIVE and b.expires_at_ms <= now_ms:
                b.status = BOUNTY_STATUS_EXPIRED
                b.resolved_at_ms = now_ms

        interval_ms = self.rule.dividend_interval_sec * 1000
        for lid, _, _ in LEYLINES:
            ll = self._ensure_leyline(zone_id, lid, now_ms)
            if ll.dividend_week_key != wk:
                ll.dividend_week_key = wk
                ll.weekly_dividend_count = 0
            if ll.owner_guild_id == 0:
                continue
            if ll.last_dividend_at_ms == 0:
                ll.last_dividend_at_ms = ll.captured_at_ms or now_ms
                continue
            if now_ms < ll.last_dividend_at_ms + interval_ms:
                continue
            ticks = (now_ms - ll.last_dividend_at_ms) // interval_ms
            ll.last_dividend_at_ms += ticks * interval_ms
            room = max(0, self.rule.dividend_guild_weekly_cap - ll.weekly_dividend_count)
            grant = min(int(ticks), room)
            if grant <= 0:
                continue
            ll.weekly_dividend_count += grant
            ll.updated_ms = now_ms
            weekly = self._ensure_weekly(ll.owner_guild_id, wk, now_ms)
            weekly.dividend_pool_count = min(
                self.rule.dividend_guild_weekly_cap,
                weekly.dividend_pool_count + grant,
            )
            weekly.updated_ms = now_ms
            await self._credit_guild_funds(ll.owner_guild_id, grant * _DIVIDEND_FUNDS_PER_TICK)

    async def _credit_guild_funds(self, guild_id: int, delta: int) -> None:
        if guild_id <= 0 or delta <= 0:
            return
        try:
            async def work(tx):
                await tx.credit_guild_funds(guild_id, int(delta))

            await self.store._run_tx(work)
        except Exception:
            logger.debug("guild war credit funds skipped guild=%d delta=%d", guild_id, delta, exc_info=True)

    async def _deduct_guild_funds(self, guild_id: int, cost: int) -> bool:
        if guild_id <= 0 or cost <= 0:
            return True

        async def work(tx) -> bool:
            lock_row = await tx.lock_guild(guild_id)
            if lock_row is None or int(lock_row.funds) < cost:
                return False
            await tx.set_guild_funds(guild_id, int(lock_row.funds) - cost)
            return True

        return await self.store._run_tx(work)

    async def _credit_member_contribution(self, guild_id: int, player_id: int, delta: int) -> None:
        if guild_id <= 0 or player_id <= 0 or delta <= 0:
            return
        try:
            async def work(tx):
                await tx.credit_member_contribution(guild_id, player_id, int(delta))

            await self.store._run_tx(work)
        except Exception:
            logger.debug(
                "guild war credit contribution skipped guild=%d player=%d delta=%d",
                guild_id,
                player_id,
                delta,
                exc_info=True,
            )

    def _emit_alert(self, kind: int, **kwargs) -> None:
        msg = self.protocol.message("guildpb.GuildWarAlertS2C", kind=kind, **kwargs)
        self.alerts.append(msg)
        if len(self.alerts) > 200:
            del self.alerts[:100]

    def _guild_zone(self, guild: GuildData) -> int:
        return self.store.merged_zones.final_zone(guild.zone_id) or guild.zone_id or 1

    # -- 成员退帮/踢出/解散钩子 ----------------------------------------------------------------------

    async def on_member_removed(self, guild_id: int, player_id: int) -> None:
        """成员退帮或被踢出时移除其守旗槽位，并清除挂牌迎战状态。"""
        async with self._lock:
            slots = self._garrisons.get(guild_id)
            if slots:
                kept = [s for s in slots if s.player_id != player_id]
                for idx, s in enumerate(kept, start=1):
                    s.slot_index = idx
                self._garrisons[guild_id] = kept
            st = self._players.get(player_id)
            if st is not None:
                st.open_challenge = False

    async def on_guild_disbanded(self, guild_id: int) -> None:
        """帮会解散时清空其守旗阵与已占灵脉归属（回归无主守卫）。"""
        async with self._lock:
            self._garrisons.pop(guild_id, None)
            for ll in self._leylines.values():
                if ll.owner_guild_id == guild_id:
                    ll.owner_guild_id = 0
                    ll.banner_hp = self.rule.leyline_max_banner_hp
                    ll.shield_until_ms = 0

    # -- 视图投影构建 --------------------------------------------------------------------------------

    def _stance_view(self, st: _PlayerWarState) -> Message:
        next_refresh = (
            st.last_garrison_refresh_ms + self.rule.garrison_refresh_cooldown_sec * 1000
            if st.last_garrison_refresh_ms > 0
            else 0
        )
        next_track = (
            st.last_track_ms + self.rule.track_cooldown_sec * 1000
            if st.last_track_ms > 0
            else 0
        )
        return self.protocol.message(
            "guildpb.GuildWarStanceView",
            peace_mode=st.peace_mode,
            peace_ready_at_ms=st.peace_ready_at_ms,
            open_challenge=st.open_challenge,
            protect_until_ms=st.protect_until_ms,
            my_streak=st.streak_count,
            my_weekly_merit=st.weekly_merit,
            my_bounty_payouts_today=st.bounty_payouts_today,
            my_bounty_payout_limit=self.rule.bounty_daily_payout_limit,
            bounty_protect_until_ms=st.bounty_protect_until_ms,
            next_garrison_refresh_ms=next_refresh,
            next_track_at_ms=next_track,
        )

    def _garrison_views(self, guild_id: int) -> list[Message]:
        return [
            self.protocol.message(
                "guildpb.GuildWarGarrisonSlotView",
                slot_index=s.slot_index,
                player_id=s.player_id,
                player_name=s.player_name,
                level=s.level,
                appearance_id=s.appearance_id,
                pet_name=s.pet_name,
                pet_table_id=s.pet_table_id,
                updated_ms=s.updated_ms,
            )
            for s in self._garrisons.get(guild_id, ())
        ]

    def _milestone_view(self, cfg: GuildWarMilestoneConfig, guild_merit: int, st: _PlayerWarState) -> Message:
        bit = 1 << (cfg.milestone_id - 1)
        claimed = bool(st.milestone_claimed_mask & bit)
        claimable = (
            not claimed
            and guild_merit >= cfg.required_guild_merit
            and st.weekly_merit >= self.rule.dividend_min_personal_merit
        )
        view = self.protocol.message(
            "guildpb.GuildWarMilestoneView",
            milestone_id=cfg.milestone_id,
            required_merit=cfg.required_guild_merit,
            guild_merit=guild_merit,
            claimed=claimed,
            claimable=claimable,
            reward_preview=cfg.reward_preview,
            reward_silver=cfg.reward_silver,
            reward_contribution=cfg.reward_contribution,
        )
        for item_id, count in cfg.reward_items:
            view.reward_items.add(item_id=item_id, count=count)
        return view

    def _dividend_view(self, weekly: _GuildWeeklyState, st: _PlayerWarState) -> Message:
        cap = self.rule.dividend_player_weekly_cap
        if st.weekly_merit >= self.rule.dividend_min_personal_merit:
            claimable = max(0, min(weekly.dividend_pool_count, cap) - st.dividend_claimed_count)
        else:
            claimable = 0
        return self.protocol.message(
            "guildpb.GuildWarDividendView",
            claimable_count=claimable,
            claimed_this_week=st.dividend_claimed_count,
            weekly_cap=cap,
            guild_produced_this_week=weekly.dividend_pool_count,
            guild_weekly_cap=self.rule.dividend_guild_weekly_cap,
            min_personal_merit=self.rule.dividend_min_personal_merit,
        )

    def _bounty_view(
        self,
        b: _BountyState,
        *,
        target_name: str,
        target_guild_name: str,
        issuer_guild_name: str,
        caller_state: _PlayerWarState,
        target_online: bool,
        now_ms: int,
    ) -> Message:
        target_st = self._players.get(b.target_player_id)
        can_direct = (
            b.status == BOUNTY_STATUS_ACTIVE
            and not caller_state.peace_mode
            and target_st is not None
            and not target_st.peace_mode
            and target_st.open_challenge
            and target_st.in_battle_id == 0
            and now_ms >= target_st.protect_until_ms
        )
        next_track = (
            caller_state.last_track_ms + self.rule.track_cooldown_sec * 1000
            if caller_state.last_track_ms > 0
            else 0
        )
        return self.protocol.message(
            "guildpb.GuildWarBountyView",
            bounty_id=b.bounty_id,
            target_player_id=b.target_player_id,
            target_name=target_name,
            target_guild_id=b.target_guild_id,
            target_guild_name=target_guild_name,
            issuer_guild_id=b.issuer_guild_id,
            issuer_guild_name=issuer_guild_name,
            tier=b.tier,
            reward_silver=b.escrow_silver,
            public_board=b.public_board,
            status=b.status,
            counter_kills=b.counter_kills,
            counter_kills_to_tear=self.rule.bounty_counter_kill_tear,
            expires_at_ms=b.expires_at_ms,
            last_scene_config_id=b.last_scene_config_id,
            last_channel_no=b.last_channel_no,
            target_in_safe_zone=False,
            target_online=target_online,
            next_track_at_ms=next_track,
            can_direct_attack=can_direct,
        )

    # -- 10 个客户端 RPC 处理器 ----------------------------------------------------------------------

    async def get_overview(self, actor: RoleOriginal) -> Message:
        """485 GetGuildWarOverview：返回三灵脉、本人姿态、里程碑、分红、悬赏、活人目标、守旗阵、仇敌与追杀榜。"""
        now_ms = self._now()
        guild = await self.store.get_player_guild(actor.player_id)
        if guild is None:
            return self.protocol.message(
                "guildpb.GetGuildWarOverviewResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
            )
        zone_id = self._guild_zone(guild)
        wk = week_key(now_ms)
        dk = day_key(now_ms)
        gold_lid, gold_ends_ms = _gold_leyline_for(now_ms)

        async with self._lock:
            await self._housekeep_locked(zone_id, now_ms)
            me = self._ensure_player(actor.player_id, zone_id, now_ms)
            weekly = self._ensure_weekly(guild.guild_id, wk, now_ms)
            leylines_snap = [self._ensure_leyline(zone_id, lid, now_ms) for lid, _, _ in LEYLINES]
            occupied_lid = next((ll.leyline_id for ll in leylines_snap if ll.owner_guild_id == guild.guild_id), 0)
            rival_ids = sorted(
                g_other
                for (g_lo, g_hi, r_wk), (clashes, b_mark) in self._rivalries.items()
                if r_wk == wk and (b_mark or clashes >= _RIVALRY_CLASH_THRESHOLD)
                for g_other in ((g_hi,) if g_lo == guild.guild_id else (g_lo,) if g_hi == guild.guild_id else ())
            )
            mvp_ids = [
                pid
                for pid, _ in sorted(weekly.member_merit.items(), key=lambda kv: (-kv[1], kv[0]))[:2]
            ]
            first_win_names, first_win_at = self._first_wins.get(guild.guild_id, ((), 0))
            open_players = [
                st
                for st in self._players.values()
                if st.zone_id == zone_id and st.player_id != actor.player_id and st.open_challenge and not st.peace_mode
            ]
            streak_players = [
                st
                for st in self._players.values()
                if st.zone_id == zone_id
                and st.streak_count >= 3
                and len(st.streak_distinct_opponents) >= _STREAK_MIN_DISTINCT
            ]
            visible_bounties = [
                b
                for b in self._bounties.values()
                if b.zone_id == zone_id
                and b.status == BOUNTY_STATUS_ACTIVE
                and (b.issuer_guild_id == guild.guild_id or b.public_board)
            ]
            raw_enemies = [
                e for e in self._enemies.get(guild.guild_id, ()) if now_ms - e.occurred_at_ms <= _ENEMY_RETENTION_MS
            ]
            self._enemies[guild.guild_id] = raw_enemies
            log_ids = list(self._guild_logs.get(guild.guild_id, ()))
            recent_battles = [self._battles[bid] for bid in log_ids if bid in self._battles]
            garrison_views = self._garrison_views(guild.guild_id)
            public_used = self._public_bounty_day.get((guild.guild_id, dk), 0)
            me_stance = self._stance_view(me)
            active_bid = me.in_battle_id
            dividend_view = self._dividend_view(weekly, me)
            milestone_views = [
                self._milestone_view(self.milestones[mid], weekly.guild_merit, me)
                for mid in sorted(self.milestones)
            ]
            active_feud, just_finished = self._active_or_pending_feud_locked(guild.guild_id, now_ms)
            if just_finished is not None:
                self._emit_feud_alert_locked(ALERT_FEUD_ENDED, just_finished)
            active_feud_view = self._feud_view(active_feud) if active_feud is not None else None
            last_feud = self._last_finished_feud_locked(guild.guild_id)
            last_feud_summary = format_feud_summary(last_feud)
            last_issued_at = self._last_feud_issued_at_locked(guild.guild_id)
            next_feud_at = 0
            if last_issued_at > 0:
                cand = last_issued_at + FEUD_COOLDOWN_SEC * 1000
                if cand > now_ms:
                    next_feud_at = cand

        # 在锁外批量查询帮会名、玩家名与帮贡余额
        guild_ids_needed = {ll.owner_guild_id for ll in leylines_snap if ll.owner_guild_id}
        guild_ids_needed.update(rival_ids)
        guild_ids_needed.update(b.target_guild_id for b in visible_bounties if b.target_guild_id)
        guild_ids_needed.update(b.issuer_guild_id for b in visible_bounties if b.issuer_guild_id)
        guild_name_map: dict[int, str] = {guild.guild_id: guild.name}
        for gid in guild_ids_needed:
            if gid not in guild_name_map:
                g = await self.store.get_guild(gid)
                if g is not None:
                    guild_name_map[gid] = g.name

        player_ids_needed = set(mvp_ids)
        player_ids_needed.update(st.player_id for st in open_players)
        player_ids_needed.update(st.player_id for st in streak_players)
        player_ids_needed.update(b.target_player_id for b in visible_bounties)
        name_map = await self._resolve_names(list(player_ids_needed))

        player_guild_map: dict[int, GuildData | None] = {}
        for pid in {st.player_id for st in open_players} | {st.player_id for st in streak_players}:
            pg = await self.store.get_player_guild(pid)
            player_guild_map[pid] = pg
            if pg is not None:
                guild_name_map[pg.guild_id] = pg.name

        online_map: dict[int, bool] = {}
        if self._online is not None and visible_bounties:
            try:
                online_map = await self._online.batch_online([b.target_player_id for b in visible_bounties])
            except Exception:
                online_map = {}

        contrib = await self.store.get_member_contribution(guild.guild_id, actor.player_id)
        contrib_balance = int(contrib[1]) if contrib is not None else 0

        resp = self.protocol.message(
            "guildpb.GetGuildWarOverviewResponse",
            server_time_ms=now_ms,
            week_key=wk,
            next_weekly_reset_ms=next_weekly_reset_ms(now_ms),
            gold_leyline_id=gold_lid,
            gold_leyline_ends_at_ms=gold_ends_ms,
            my_stance=me_stance,
            dividend=dividend_view,
            active_battle_id=active_bid,
            contribution_balance=contrib_balance,
            public_bounty_used_today=public_used,
            public_bounty_daily_limit=self.rule.bounty_public_daily_limit,
            last_feud_summary=last_feud_summary,
            next_feud_issue_at_ms=next_feud_at,
        )
        if active_feud_view is not None:
            resp.active_feud.CopyFrom(active_feud_view)
        resp.banner_honor.occupied_leyline_id = occupied_lid
        resp.banner_honor.weekly_mvps.extend(name_map.get(pid, f"player-{pid}") for pid in mvp_ids)
        resp.banner_honor.first_win_names.extend(first_win_names)
        resp.banner_honor.first_win_at_ms = first_win_at
        resp.banner_honor.rival_guild_ids.extend(rival_ids)
        resp.banner_honor.rival_guild_names.extend(guild_name_map.get(gid, f"guild-{gid}") for gid in rival_ids)

        for ll in leylines_snap:
            lname, scene_cfg = LEYLINE_BY_ID[ll.leyline_id]
            featured_bid = min(ll.active_battle_ids) if ll.active_battle_ids else 0
            resp.leylines.add(
                leyline_id=ll.leyline_id,
                name=lname,
                scene_config_id=scene_cfg,
                owner_guild_id=ll.owner_guild_id,
                owner_guild_name=guild_name_map.get(ll.owner_guild_id, ""),
                banner_hp=ll.banner_hp,
                banner_max_hp=self.rule.leyline_max_banner_hp,
                shield_until_ms=ll.shield_until_ms,
                active_battle_count=len(ll.active_battle_ids),
                under_siege=bool(ll.active_battle_ids),
                featured_watch_battle_id=featured_bid,
                is_gold_leyline=(ll.leyline_id == gold_lid),
                garrison_count=len(self._garrisons.get(ll.owner_guild_id, ())) if ll.owner_guild_id else 0,
                recommended_scene_id=scene_cfg,
                recommended_channel_no=1,
            )

        for mv in milestone_views:
            resp.milestones.add().CopyFrom(mv)
        for gv in garrison_views:
            resp.garrison_slots.add().CopyFrom(gv)

        for st in streak_players:
            pg = player_guild_map.get(st.player_id)
            bonus_c, bonus_s = _streak_jackpot(st.streak_count)
            resp.streak_bounties.add(
                player_id=st.player_id,
                player_name=name_map.get(st.player_id, f"player-{st.player_id}"),
                guild_id=pg.guild_id if pg else 0,
                guild_name=pg.name if pg else "",
                streak=st.streak_count,
                bonus_contribution=bonus_c,
                bonus_silver=bonus_s,
                in_battle=st.in_battle_id != 0,
                battle_id=st.in_battle_id,
            )

        for st in open_players:
            pg = player_guild_map.get(st.player_id)
            if pg is None or pg.guild_id == guild.guild_id:
                continue
            ab = self._active_bounty_for_target(st.player_id, now_ms)
            resp.live_targets.add(
                player_id=st.player_id,
                player_name=name_map.get(st.player_id, f"player-{st.player_id}"),
                guild_id=pg.guild_id,
                guild_name=pg.name,
                level=30,
                party_size=1,
                streak=st.streak_count,
                in_battle=st.in_battle_id != 0,
                battle_id=st.in_battle_id,
                leyline_id=0,
                is_rival_guild=self._is_rival(guild.guild_id, pg.guild_id, wk),
                active_bounty_id=ab.bounty_id if ab else 0,
            )

        for enemy in raw_enemies:
            has_bounty = self._active_bounty_for_target(enemy.player_id, now_ms) is not None
            est = self._players.get(enemy.player_id)
            protected = est is not None and now_ms < est.bounty_protect_until_ms
            blocked_tip = (
                ERR_GUILD_WAR_BOUNTY_ALREADY_ACTIVE
                if has_bounty
                else ERR_GUILD_WAR_TARGET_PROTECTED if protected else 0
            )
            resp.enemies.add(
                player_id=enemy.player_id,
                player_name=enemy.player_name,
                guild_id=enemy.guild_id,
                guild_name=enemy.guild_name,
                victim_name=enemy.victim_name,
                battle_kind=enemy.battle_kind,
                occurred_at_ms=enemy.occurred_at_ms,
                can_post_bounty=(blocked_tip == 0),
                blocked_tip_id=blocked_tip,
            )

        for b in visible_bounties:
            resp.bounties.add().CopyFrom(
                self._bounty_view(
                    b,
                    target_name=name_map.get(b.target_player_id, f"player-{b.target_player_id}"),
                    target_guild_name=guild_name_map.get(b.target_guild_id, ""),
                    issuer_guild_name=guild_name_map.get(b.issuer_guild_id, ""),
                    caller_state=me,
                    target_online=online_map.get(b.target_player_id, True),
                    now_ms=now_ms,
                )
            )

        for btl in reversed(recent_battles):
            resp.recent_logs.add(
                battle_id=btl.battle_id,
                war_battle_kind=btl.war_battle_kind,
                leyline_id=btl.leyline_id,
                attacker_guild_name=btl.attacker_guild_name,
                defender_guild_name=btl.defender_guild_name,
                attacker_summary=",".join(btl.attacker_names),
                defender_summary=",".join(btl.defender_names),
                outcome=btl.outcome,
                rounds=btl.rounds,
                banner_hp_delta=int(btl.banner_hp_after) - int(btl.banner_hp_before),
                bounty_outcome_tag=btl.bounty_outcome_tag,
                self_participated=(actor.player_id in btl.attacker_ids or actor.player_id in btl.defender_ids),
                finished_at_ms=btl.finished_at_ms,
            )

        return resp

    async def set_stance(self, actor: RoleOriginal, request: Message) -> Message:
        """486 SetGuildWarStance：切换和平免战（战意->和平需 60s 脱战冷却）与界面挂牌迎战。"""
        now_ms = self._now()
        guild = await self.store.get_player_guild(actor.player_id)
        if guild is None:
            return self.protocol.message(
                "guildpb.SetGuildWarStanceResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                server_time_ms=now_ms,
            )
        zone_id = self._guild_zone(guild)
        async with self._lock:
            st = self._ensure_player(actor.player_id, zone_id, now_ms)
            if request.HasField("set_peace_mode"):
                want_peace = bool(request.set_peace_mode)
                if want_peace:
                    if st.in_battle_id != 0 or (not st.peace_mode and now_ms < st.peace_ready_at_ms):
                        return self.protocol.message(
                            "guildpb.SetGuildWarStanceResponse",
                            error_message=self._tip(ERR_GUILD_WAR_PEACE_COOLDOWN, "peace mode cooldown active"),
                            my_stance=self._stance_view(st),
                            server_time_ms=now_ms,
                        )
                    st.peace_mode = True
                    st.open_challenge = False
                else:
                    st.peace_mode = False
                st.updated_ms = now_ms

            if request.HasField("set_open_challenge"):
                want_open = bool(request.set_open_challenge)
                if want_open and st.peace_mode:
                    return self.protocol.message(
                        "guildpb.SetGuildWarStanceResponse",
                        error_message=self._tip(ERR_GUILD_WAR_SELF_PEACE_MODE, "cannot open challenge in peace mode"),
                        my_stance=self._stance_view(st),
                        server_time_ms=now_ms,
                    )
                st.open_challenge = want_open
                st.updated_ms = now_ms

            return self.protocol.message(
                "guildpb.SetGuildWarStanceResponse",
                my_stance=self._stance_view(st),
                server_time_ms=now_ms,
            )

    async def register_garrison(self, actor: RoleOriginal, request: Message) -> Message:
        """487 RegisterGuildWarGarrison：1=上阵/刷新（60s 冷却）、2=下阵、3=帮主或长老置顶槽位。"""
        now_ms = self._now()
        action = int(request.action)
        target_pid = int(request.target_player_id) or actor.player_id
        if action not in (1, 2, 3):
            return self.protocol.message(
                "guildpb.RegisterGuildWarGarrisonResponse",
                error_message=self._tip(ERR_INVALID_PARAMETER, "invalid garrison action"),
            )
        guild = await self.store.get_player_guild(actor.player_id)
        if guild is None or (me_member := guild.member(actor.player_id)) is None:
            return self.protocol.message(
                "guildpb.RegisterGuildWarGarrisonResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
            )
        zone_id = self._guild_zone(guild)
        names = await self._resolve_names([actor.player_id, target_pid])

        async with self._lock:
            st = self._ensure_player(actor.player_id, zone_id, now_ms)
            slots = self._garrisons.setdefault(guild.guild_id, [])

            if action == 1:
                if (
                    st.last_garrison_refresh_ms > 0
                    and now_ms - st.last_garrison_refresh_ms < self.rule.garrison_refresh_cooldown_sec * 1000
                ):
                    return self.protocol.message(
                        "guildpb.RegisterGuildWarGarrisonResponse",
                        error_message=self._tip(ERR_GUILD_WAR_GARRISON_COOLDOWN, "garrison refresh cooldown"),
                        garrison_slots=self._garrison_views(guild.guild_id),
                        next_refresh_at_ms=st.last_garrison_refresh_ms
                        + self.rule.garrison_refresh_cooldown_sec * 1000,
                    )
                existing = next((s for s in slots if s.player_id == actor.player_id), None)
                if existing is not None:
                    existing.player_name = names.get(actor.player_id, existing.player_name)
                    existing.updated_ms = now_ms
                else:
                    if len(slots) >= self.rule.garrison_max_slots:
                        return self.protocol.message(
                            "guildpb.RegisterGuildWarGarrisonResponse",
                            error_message=self._tip(ERR_GUILD_WAR_GARRISON_FULL, "garrison slots full"),
                            garrison_slots=self._garrison_views(guild.guild_id),
                        )
                    slots.append(
                        _GarrisonSlot(
                            slot_index=len(slots) + 1,
                            player_id=actor.player_id,
                            player_name=names.get(actor.player_id, f"player-{actor.player_id}"),
                            updated_ms=now_ms,
                        )
                    )
                st.last_garrison_refresh_ms = now_ms
                next_refresh = now_ms + self.rule.garrison_refresh_cooldown_sec * 1000

            elif action == 2:
                if target_pid != actor.player_id and role_rank(me_member.role) < RANK_OFFICER:
                    return self.protocol.message(
                        "guildpb.RegisterGuildWarGarrisonResponse",
                        error_message=self._tip(ERR_GUILD_NO_PERMISSION, "officer permission required"),
                    )
                slots[:] = [s for s in slots if s.player_id != target_pid]
                for idx, s in enumerate(slots, start=1):
                    s.slot_index = idx
                next_refresh = (
                    st.last_garrison_refresh_ms + self.rule.garrison_refresh_cooldown_sec * 1000
                    if st.last_garrison_refresh_ms > 0
                    else 0
                )

            else:  # action == 3 置顶槽位
                if role_rank(me_member.role) < RANK_OFFICER:
                    return self.protocol.message(
                        "guildpb.RegisterGuildWarGarrisonResponse",
                        error_message=self._tip(ERR_GUILD_NO_PERMISSION, "officer permission required"),
                    )
                found = next((s for s in slots if s.player_id == target_pid), None)
                if found is None:
                    return self.protocol.message(
                        "guildpb.RegisterGuildWarGarrisonResponse",
                        error_message=self._tip(ERR_INVALID_PARAMETER, "target not in garrison"),
                    )
                slots.remove(found)
                slots.insert(0, found)
                for idx, s in enumerate(slots, start=1):
                    s.slot_index = idx
                next_refresh = (
                    st.last_garrison_refresh_ms + self.rule.garrison_refresh_cooldown_sec * 1000
                    if st.last_garrison_refresh_ms > 0
                    else 0
                )

            return self.protocol.message(
                "guildpb.RegisterGuildWarGarrisonResponse",
                garrison_slots=self._garrison_views(guild.guild_id),
                next_refresh_at_ms=next_refresh,
            )

    async def start_battle(self, actor: RoleOriginal, request: Message) -> Message:
        """488 StartGuildWarBattle：无匹配池 0 秒直打真人、踢守旗阵、执行追杀或一键快速开战。"""
        now_ms = self._now()
        req_id = int(request.client_request_id)
        if req_id != 0 and ("start", actor.player_id, req_id) in self._idempotency:
            return self._idempotency[("start", actor.player_id, req_id)]

        caller_guild = await self.store.get_player_guild(actor.player_id)
        if caller_guild is None:
            return self.protocol.message(
                "guildpb.StartGuildWarBattleResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
            )
        zone_id = self._guild_zone(caller_guild)
        wk = week_key(now_ms)
        gold_lid, _ = _gold_leyline_for(now_ms)
        target_kind = int(request.target_kind)
        req_leyline_id = int(request.leyline_id)
        req_target_pid = int(request.target_player_id)
        req_bounty_id = int(request.bounty_id)

        def fail(tip_id: int, msg: str = "", *, offender: int = 0, offender_bid: int = 0) -> Message:
            return self.protocol.message(
                "guildpb.StartGuildWarBattleResponse",
                error_message=self._tip(tip_id, msg or f"err:{tip_id}"),
                offender_player_id=offender,
                offender_battle_id=offender_bid,
            )

        if target_kind not in (
            TARGET_KIND_QUICK_AUTO,
            TARGET_KIND_LIVE_PLAYER,
            TARGET_KIND_LEYLINE_GARRISON,
            TARGET_KIND_BOUNTY,
            TARGET_KIND_FIELD_PLAYER,
        ):
            return fail(ERR_INVALID_PARAMETER, "invalid target_kind")

        # 预先读取目标玩家帮会（避免在锁内做异步存储查询）
        explicit_target_guild: GuildData | None = None
        if req_target_pid > 0:
            explicit_target_guild = await self.store.get_player_guild(req_target_pid)

        # 收集候选在线活人的帮会映射（仅 QUICK_AUTO 需要）
        quick_candidates: list[tuple[_PlayerWarState, GuildData]] = []
        if target_kind == TARGET_KIND_QUICK_AUTO:
            snapshot_open = [
                st
                for st in list(self._players.values())
                if st.zone_id == zone_id
                and st.player_id != actor.player_id
                and st.open_challenge
                and not st.peace_mode
                and st.in_battle_id == 0
                and now_ms >= st.protect_until_ms
            ]
            for st in snapshot_open:
                tg = await self.store.get_player_guild(st.player_id)
                if tg is not None and tg.guild_id != caller_guild.guild_id and self._guild_zone(tg) == zone_id:
                    quick_candidates.append((st, tg))

        # 追杀令目标帮会预读
        bounty_target_guild: GuildData | None = None
        if target_kind == TARGET_KIND_BOUNTY and req_bounty_id in self._bounties:
            b_pre = self._bounties[req_bounty_id]
            bounty_target_guild = await self.store.get_player_guild(b_pre.target_player_id)

        # 灵脉占领帮预读
        leyline_guilds: dict[int, GuildData] = {}
        for lid, _, _ in LEYLINES:
            ll_pre = self._leylines.get((zone_id, lid))
            if ll_pre is not None and ll_pre.owner_guild_id > 0:
                g = await self.store.get_guild(ll_pre.owner_guild_id)
                if g is not None:
                    leyline_guilds[ll_pre.owner_guild_id] = g

        names_cache = await self._resolve_names(
            [actor.player_id]
            + ([req_target_pid] if req_target_pid else [])
            + [st.player_id for st, _ in quick_candidates]
            + ([self._bounties[req_bounty_id].target_player_id] if req_bounty_id in self._bounties else [])
        )

        # 核心原子决策与占位
        match_req_to_send: Message | None = None
        pending_plan: dict | None = None

        async with self._lock:
            await self._housekeep_locked(zone_id, now_ms)
            me = self._ensure_player(actor.player_id, zone_id, now_ms)
            if me.peace_mode:
                return fail(ERR_GUILD_WAR_SELF_PEACE_MODE, "caller is in peace mode")
            if me.in_battle_id != 0:
                return fail(
                    ERR_GUILD_WAR_TARGET_IN_BATTLE,
                    "caller already in battle",
                    offender=actor.player_id,
                    offender_bid=me.in_battle_id,
                )

            war_battle_kind = 0
            match_mode = 0
            leyline_id = 0
            bounty_id = 0
            defender_guild_id = 0
            defender_guild_name = ""
            defender_ids: list[int] = []
            ai_defenders: list[_GarrisonSlot] = []

            if target_kind in (TARGET_KIND_LIVE_PLAYER, TARGET_KIND_FIELD_PLAYER):
                if req_target_pid <= 0 or req_target_pid == actor.player_id:
                    return fail(ERR_INVALID_PARAMETER, "invalid target_player_id")
                if explicit_target_guild is None or self._guild_zone(explicit_target_guild) != zone_id:
                    return fail(ERR_GUILD_NOT_IN_GUILD, "target not in a guild in same zone")
                if explicit_target_guild.guild_id == caller_guild.guild_id:
                    return fail(ERR_GUILD_WAR_SAME_GUILD, "cannot attack same guild member")
                tgt = self._ensure_player(req_target_pid, zone_id, now_ms)
                if tgt.peace_mode:
                    return fail(ERR_GUILD_WAR_TARGET_PEACE_MODE, "target in peace mode")
                if now_ms < tgt.protect_until_ms:
                    return fail(ERR_GUILD_WAR_TARGET_PROTECTED, "target under post-battle protection")
                if tgt.in_battle_id != 0:
                    return fail(
                        ERR_GUILD_WAR_TARGET_IN_BATTLE,
                        "target already in battle",
                        offender=req_target_pid,
                        offender_bid=tgt.in_battle_id,
                    )
                if target_kind == TARGET_KIND_LIVE_PLAYER and not tgt.open_challenge:
                    return fail(ERR_GUILD_WAR_TARGET_OFFLINE, "target is not open for panel challenge")
                match_mode = MATCH_MODE_GUILD_WAR_PVP
                defender_ids = [req_target_pid]
                defender_guild_id = explicit_target_guild.guild_id
                defender_guild_name = explicit_target_guild.name
                if target_kind == TARGET_KIND_LIVE_PLAYER:
                    # r2-gw#1：直打活人不采信客户端 leyline_id。
                    leyline_id = 0
                    war_battle_kind = BATTLE_KIND_PANEL_PVP
                else:
                    leyline_id = req_leyline_id if req_leyline_id in LEYLINE_BY_ID else 0
                    if leyline_id != 0:
                        war_battle_kind = BATTLE_KIND_LEYLINE_PVP
                    else:
                        war_battle_kind = BATTLE_KIND_FIELD_ROAD

            elif target_kind == TARGET_KIND_LEYLINE_GARRISON:
                if req_leyline_id not in LEYLINE_BY_ID:
                    return fail(ERR_GUILD_WAR_LEYLINE_NOT_FOUND, "leyline not found")
                ll = self._ensure_leyline(zone_id, req_leyline_id, now_ms)
                if ll.owner_guild_id == caller_guild.guild_id:
                    return fail(ERR_GUILD_WAR_SAME_GUILD, "cannot siege own leyline")
                leyline_id = req_leyline_id
                war_battle_kind = BATTLE_KIND_GARRISON_SIEGE
                match_mode = MATCH_MODE_GUILD_WAR_SIEGE
                defender_guild_id = ll.owner_guild_id
                if defender_guild_id > 0:
                    dg = leyline_guilds.get(defender_guild_id)
                    defender_guild_name = dg.name if dg is not None else f"guild-{defender_guild_id}"
                    ai_defenders = list(self._garrisons.get(defender_guild_id, ()))
                else:
                    defender_guild_name = "灵脉守卫"

            elif target_kind == TARGET_KIND_BOUNTY:
                b = self._bounties.get(req_bounty_id)
                if b is None or b.zone_id != zone_id or b.status != BOUNTY_STATUS_ACTIVE:
                    return fail(ERR_GUILD_WAR_BOUNTY_NOT_FOUND, "bounty not found or inactive")
                if b.issuer_guild_id != caller_guild.guild_id and not b.public_board:
                    return fail(ERR_GUILD_WAR_BOUNTY_NOT_FOUND, "bounty not visible to caller guild")
                if b.target_guild_id == caller_guild.guild_id or b.target_player_id == actor.player_id:
                    return fail(ERR_GUILD_WAR_SAME_GUILD, "cannot hunt same guild member")
                tgt = self._ensure_player(b.target_player_id, zone_id, now_ms)
                if tgt.peace_mode:
                    return fail(ERR_GUILD_WAR_TARGET_PEACE_MODE, "bounty target in peace mode")
                if now_ms < tgt.protect_until_ms:
                    return fail(ERR_GUILD_WAR_TARGET_PROTECTED, "bounty target protected")
                if tgt.in_battle_id != 0:
                    return fail(
                        ERR_GUILD_WAR_TARGET_IN_BATTLE,
                        "bounty target in battle",
                        offender=b.target_player_id,
                        offender_bid=tgt.in_battle_id,
                    )
                bounty_id = b.bounty_id
                war_battle_kind = BATTLE_KIND_BOUNTY_HUNT
                match_mode = MATCH_MODE_GUILD_WAR_PVP
                defender_ids = [b.target_player_id]
                defender_guild_id = bounty_target_guild.guild_id if bounty_target_guild else b.target_guild_id
                defender_guild_name = bounty_target_guild.name if bounty_target_guild else f"guild-{defender_guild_id}"

            else:  # TARGET_KIND_QUICK_AUTO：优先活人，无活人 0 秒打守旗阵
                picked_live: tuple[_PlayerWarState, GuildData] | None = None
                for st, tg in quick_candidates:
                    cur = self._players.get(st.player_id)
                    if (
                        cur is not None
                        and cur.open_challenge
                        and not cur.peace_mode
                        and cur.in_battle_id == 0
                        and now_ms >= cur.protect_until_ms
                    ):
                        picked_live = (cur, tg)
                        break
                if picked_live is not None:
                    tgt, tg = picked_live
                    war_battle_kind = BATTLE_KIND_PANEL_PVP
                    match_mode = MATCH_MODE_GUILD_WAR_PVP
                    defender_ids = [tgt.player_id]
                    defender_guild_id = tg.guild_id
                    defender_guild_name = tg.name
                    # r2-gw#1：一键开战命中活人时不采信客户端 leyline_id。
                    leyline_id = 0
                else:
                    candidate_lids = []
                    if req_leyline_id in LEYLINE_BY_ID:
                        candidate_lids.append(req_leyline_id)
                    if gold_lid not in candidate_lids:
                        candidate_lids.append(gold_lid)
                    for lid, _, _ in LEYLINES:
                        if lid not in candidate_lids:
                            candidate_lids.append(lid)
                    chosen_ll = next(
                        (
                            self._ensure_leyline(zone_id, lid, now_ms)
                            for lid in candidate_lids
                            if self._ensure_leyline(zone_id, lid, now_ms).owner_guild_id != caller_guild.guild_id
                        ),
                        None,
                    )
                    if chosen_ll is None:
                        return fail(ERR_GUILD_WAR_LEYLINE_NOT_FOUND, "no attackable leyline")
                    leyline_id = chosen_ll.leyline_id
                    war_battle_kind = BATTLE_KIND_GARRISON_SIEGE
                    match_mode = MATCH_MODE_GUILD_WAR_SIEGE
                    defender_guild_id = chosen_ll.owner_guild_id
                    if defender_guild_id > 0:
                        dg = leyline_guilds.get(defender_guild_id)
                        defender_guild_name = dg.name if dg is not None else f"guild-{defender_guild_id}"
                        ai_defenders = list(self._garrisons.get(defender_guild_id, ()))
                    else:
                        defender_guild_name = "灵脉守卫"

            # 主动进攻立刻解除本人战后保护
            me.protect_until_ms = 0
            gold_bonus = leyline_id != 0 and leyline_id == gold_lid
            rivalry_bonus = self._is_rival(caller_guild.guild_id, defender_guild_id, wk)
            if bounty_id == 0 and defender_ids:
                ab = self._active_bounty_for_target(defender_ids[0], now_ms)
                if ab is not None and (ab.issuer_guild_id == caller_guild.guild_id or ab.public_board):
                    bounty_id = ab.bounty_id

            if self._match_war_starter is not None:
                match_req = self.protocol.message(
                    "match.StartGuildWarBattleRequest",
                    battle_config_id=self.rule.battle_config_id,
                    match_mode=match_mode,
                    attacker_player_ids=[actor.player_id],
                    defender_player_ids=defender_ids,
                    request_id=req_id or (self._next_battle_id + 1),
                )
                match_req.activity_context.kind = BATTLE_ACTIVITY_KIND_GUILD_WAR
                match_req.activity_context.guild_id = caller_guild.guild_id
                match_req.activity_context.initiator_player_id = actor.player_id
                gw_ctx = match_req.activity_context.guild_war
                gw_ctx.zone_id = zone_id
                gw_ctx.attacker_guild_id = caller_guild.guild_id
                gw_ctx.defender_guild_id = defender_guild_id
                gw_ctx.leyline_id = leyline_id
                gw_ctx.war_battle_kind = war_battle_kind
                gw_ctx.bounty_id = bounty_id
                gw_ctx.week_key = wk
                gw_ctx.attacker_player_ids.append(actor.player_id)
                gw_ctx.defender_player_ids.extend(defender_ids or [s.player_id for s in ai_defenders])
                gw_ctx.gold_leyline_bonus = gold_bonus
                gw_ctx.rivalry_bonus = rivalry_bonus
                gw_ctx.started_at_ms = now_ms
                for slot in ai_defenders:
                    snap = match_req.ai_defender_snapshots.add()
                    snap.player_id = slot.player_id
                    snap.player_name = slot.player_name
                    snap.level = slot.level
                    snap.team_index = 1
                    snap.ai_controlled = True
                match_req_to_send = match_req
                pending_plan = dict(
                    war_battle_kind=war_battle_kind,
                    match_mode=match_mode,
                    leyline_id=leyline_id,
                    bounty_id=bounty_id,
                    defender_guild_id=defender_guild_id,
                    defender_guild_name=defender_guild_name,
                    defender_ids=tuple(defender_ids),
                    ai_defenders=tuple(ai_defenders),
                    gold_bonus=gold_bonus,
                    rivalry_bonus=rivalry_bonus,
                )
            else:
                self._next_battle_id += 1
                battle_id = self._next_battle_id
                self._register_started_battle_locked(
                    battle_id=battle_id,
                    zone_id=zone_id,
                    caller_guild=caller_guild,
                    attacker_ids=(actor.player_id,),
                    defender_ids=tuple(defender_ids),
                    ai_defenders=tuple(ai_defenders),
                    defender_guild_id=defender_guild_id,
                    defender_guild_name=defender_guild_name,
                    war_battle_kind=war_battle_kind,
                    match_mode=match_mode,
                    leyline_id=leyline_id,
                    bounty_id=bounty_id,
                    wk=wk,
                    gold_bonus=gold_bonus,
                    rivalry_bonus=rivalry_bonus,
                    names_cache=names_cache,
                    now_ms=now_ms,
                )
                resp = self.protocol.message(
                    "guildpb.StartGuildWarBattleResponse",
                    battle_id=battle_id,
                    war_battle_kind=war_battle_kind,
                )
                if req_id != 0:
                    self._idempotency[("start", actor.player_id, req_id)] = resp
                return resp

        # 外部委托 MatchInternal.StartGuildWarBattle（不在锁内发起 RPC）
        assert match_req_to_send is not None and pending_plan is not None
        try:
            mresp = await self._match_war_starter(match_req_to_send)
        except Exception:
            logger.warning("guild war MatchInternal.StartGuildWarBattle failed", exc_info=True)
            return fail(ERR_GUILD_WAR_SERVICE_BUSY, "match service unavailable")

        rej = int(mresp.reject)
        if rej != 0:
            if rej == 2:
                return fail(ERR_GUILD_WAR_TARGET_OFFLINE, "member offline", offender=int(mresp.offender_player_id))
            if rej == 3:
                return fail(
                    ERR_GUILD_WAR_TARGET_IN_BATTLE,
                    "member in battle",
                    offender=int(mresp.offender_player_id),
                    offender_bid=int(mresp.offender_battle_id),
                )
            return fail(ERR_GUILD_WAR_SERVICE_BUSY, f"match rejected={rej}", offender=int(mresp.offender_player_id))

        battle_id = int(mresp.battle_id)
        async with self._lock:
            self._register_started_battle_locked(
                battle_id=battle_id,
                zone_id=zone_id,
                caller_guild=caller_guild,
                attacker_ids=(actor.player_id,),
                defender_ids=pending_plan["defender_ids"],
                ai_defenders=pending_plan["ai_defenders"],
                defender_guild_id=pending_plan["defender_guild_id"],
                defender_guild_name=pending_plan["defender_guild_name"],
                war_battle_kind=pending_plan["war_battle_kind"],
                match_mode=pending_plan["match_mode"],
                leyline_id=pending_plan["leyline_id"],
                bounty_id=pending_plan["bounty_id"],
                wk=wk,
                gold_bonus=pending_plan["gold_bonus"],
                rivalry_bonus=pending_plan["rivalry_bonus"],
                names_cache=names_cache,
                now_ms=now_ms,
            )
            resp = self.protocol.message(
                "guildpb.StartGuildWarBattleResponse",
                battle_id=battle_id,
                war_battle_kind=pending_plan["war_battle_kind"],
            )
            if req_id != 0:
                self._idempotency[("start", actor.player_id, req_id)] = resp
            return resp

    def _register_started_battle_locked(
        self,
        *,
        battle_id: int,
        zone_id: int,
        caller_guild: GuildData,
        attacker_ids: tuple[int, ...],
        defender_ids: tuple[int, ...],
        ai_defenders: tuple[_GarrisonSlot, ...],
        defender_guild_id: int,
        defender_guild_name: str,
        war_battle_kind: int,
        match_mode: int,
        leyline_id: int,
        bounty_id: int,
        wk: int,
        gold_bonus: bool,
        rivalry_bonus: bool,
        names_cache: dict[int, str],
        now_ms: int,
    ) -> None:
        if battle_id in self._battles:
            return
        for pid in attacker_ids:
            self._ensure_player(pid, zone_id, now_ms).in_battle_id = battle_id
        for pid in defender_ids:
            self._ensure_player(pid, zone_id, now_ms).in_battle_id = battle_id

        if leyline_id in LEYLINE_BY_ID:
            ll = self._ensure_leyline(zone_id, leyline_id, now_ms)
            ll.active_battle_ids.add(battle_id)
            if (
                war_battle_kind == BATTLE_KIND_GARRISON_SIEGE
                and ll.owner_guild_id > 0
                and now_ms - ll.last_siege_alert_ms >= _SIEGE_ALERT_COOLDOWN_MS
            ):
                ll.last_siege_alert_ms = now_ms
                pct = (ll.banner_hp * 100) // max(1, self.rule.leyline_max_banner_hp)
                self._emit_alert(
                    ALERT_LEYLINE_UNDER_SIEGE,
                    leyline_id=leyline_id,
                    scene_config_id=LEYLINE_BY_ID[leyline_id][1],
                    channel_no=1,
                    attacker_guild_name=caller_guild.name,
                    defender_guild_name=defender_guild_name,
                    banner_hp_percent=pct,
                    battle_id=battle_id,
                )

        atk_names = tuple(names_cache.get(pid, f"player-{pid}") for pid in attacker_ids)
        if defender_ids:
            def_names = tuple(names_cache.get(pid, f"player-{pid}") for pid in defender_ids)
        elif ai_defenders:
            def_names = tuple(s.player_name for s in ai_defenders)
        else:
            def_names = ("灵脉守卫",)

        self._battles[battle_id] = _WarBattleState(
            battle_id=battle_id,
            zone_id=zone_id,
            war_battle_kind=war_battle_kind,
            match_mode=match_mode,
            leyline_id=leyline_id,
            bounty_id=bounty_id,
            attacker_guild_id=caller_guild.guild_id,
            defender_guild_id=defender_guild_id,
            attacker_guild_name=caller_guild.name,
            defender_guild_name=defender_guild_name,
            attacker_ids=attacker_ids,
            defender_ids=defender_ids,
            attacker_names=atk_names,
            defender_names=def_names,
            week_key=wk,
            gold_leyline_bonus=gold_bonus,
            rivalry_bonus=rivalry_bonus,
            started_at_ms=now_ms,
        )

    async def settle_battle(
        self,
        battle_id: int,
        *,
        outcome: int,
        rounds: int = 5,
        manual_rounds: int = 3,
        has_effective_contribution: bool = True,
        finished_at_ms: int | None = None,
    ) -> _WarBattleState | None:
        """幂等结算一场全天帮战（同一 battle_id 重放直接返回首次结果，绝不双发奖或双扣旗势）。"""
        now_ms = int(finished_at_ms) if finished_at_ms is not None else self._now()
        credits_to_apply: list[tuple[int, int, int]] = []  # (guild_id, player_id, contrib_delta)
        funds_to_apply: list[tuple[int, int]] = []  # (guild_id, funds_delta)

        async with self._lock:
            btl = self._battles.get(int(battle_id))
            if btl is None:
                return None
            if btl.settled:
                return btl

            wk = btl.week_key
            dk = day_key(now_ms)
            is_pvp = btl.match_mode == MATCH_MODE_GUILD_WAR_PVP
            attacker_win = outcome == BATTLE_OUTCOME_SIDE_A_WIN
            defender_win = outcome == BATTLE_OUTCOME_SIDE_B_WIN
            qualified_loss = rounds >= 3 and manual_rounds >= 2 and has_effective_contribution

            # 释放参战占位与写入脱战冷却/战后保护
            for pid in btl.attacker_ids:
                st = self._ensure_player(pid, btl.zone_id, now_ms)
                if st.in_battle_id == battle_id:
                    st.in_battle_id = 0
                st.peace_ready_at_ms = now_ms + self.rule.peace_cooldown_sec * 1000
                if defender_win:
                    st.protect_until_ms = now_ms + self.rule.post_battle_protect_sec * 1000
            for pid in btl.defender_ids:
                st = self._ensure_player(pid, btl.zone_id, now_ms)
                if st.in_battle_id == battle_id:
                    st.in_battle_id = 0
                st.peace_ready_at_ms = now_ms + self.rule.peace_cooldown_sec * 1000
                if attacker_win:
                    st.protect_until_ms = now_ms + self.rule.post_battle_protect_sec * 1000

            # 检查两帮是否正处于 2 小时宿敌约战窗口内（前 3 场免递减 + 收益 +25% + 计入比分）
            active_feud = self._active_feud_between_locked(btl.attacker_guild_id, btl.defender_guild_id, now_ms)
            feud_active = active_feud is not None
            btl.feud_bonus = feud_active

            # 30 分钟窗口递减计算
            window_ms = self.rule.diminish_window_sec * 1000
            cutoff = now_ms - window_ms
            if is_pvp and btl.attacker_ids and btl.defender_ids:
                pair = (
                    min(btl.attacker_ids[0], btl.defender_ids[0]),
                    max(btl.attacker_ids[0], btl.defender_ids[0]),
                )
                hist = self._pair_clashes.setdefault(pair, deque())
                while hist and hist[0] <= cutoff:
                    hist.popleft()
                prior_cnt = len(hist)
                hist.append(now_ms)
                dim_pct = _diminish_percent_for_count(prior_cnt, threshold=0, feud_active=feud_active)
            elif not is_pvp and btl.attacker_ids and btl.defender_guild_id > 0:
                skey = (btl.attacker_ids[0], btl.defender_guild_id)
                hist = self._siege_clashes.setdefault(skey, deque())
                while hist and hist[0] <= cutoff:
                    hist.popleft()
                prior_cnt = len(hist)
                hist.append(now_ms)
                dim_pct = _diminish_percent_for_count(prior_cnt, threshold=6, feud_active=feud_active)
            else:
                dim_pct = 100

            btl.diminish_percent = dim_pct
            btl.outcome = int(outcome)
            btl.rounds = int(rounds)
            btl.finished_at_ms = now_ms

            def scale_bonus(val: int, *, apply_mult: bool) -> int:
                v = (val * dim_pct) // 100
                if not apply_mult or v <= 0:
                    return v
                if btl.gold_leyline_bonus:
                    v = (v * self.rule.gold_leyline_multiplier_bps) // 10000
                if btl.rivalry_bonus or feud_active:
                    bonus_bps = max(self.rule.rivalry_bonus_bps, FEUD_BONUS_BPS) if feud_active else self.rule.rivalry_bonus_bps
                    v = (v * (10000 + bonus_bps)) // 10000
                return v

            # 基础第 1/2 层战利品
            atk_r = btl.attacker_reward
            def_r = btl.defender_reward
            if is_pvp:
                if attacker_win:
                    atk_r.contribution = scale_bonus(80, apply_mult=True)
                    atk_r.silver = scale_bonus(3000, apply_mult=False)
                    atk_r.funds = scale_bonus(400, apply_mult=False)
                    atk_r.merit = scale_bonus(30, apply_mult=True)
                    atk_r.material_units = 15
                    atk_r.material_tradable = 1
                    if qualified_loss:
                        def_r.contribution = scale_bonus(35, apply_mult=True)
                        def_r.silver = scale_bonus(1200, apply_mult=False)
                        def_r.funds = scale_bonus(150, apply_mult=False)
                        def_r.merit = scale_bonus(12, apply_mult=True)
                        def_r.material_units = 10
                        def_r.material_tradable = 1
                    else:
                        def_r.material_pvp_loss_ineligible = True
                elif defender_win:
                    def_r.contribution = scale_bonus(80, apply_mult=True)
                    def_r.silver = scale_bonus(3000, apply_mult=False)
                    def_r.funds = scale_bonus(400, apply_mult=False)
                    def_r.merit = scale_bonus(30, apply_mult=True)
                    def_r.material_units = 15
                    def_r.material_tradable = 1
                    if qualified_loss:
                        atk_r.contribution = scale_bonus(35, apply_mult=True)
                        atk_r.silver = scale_bonus(1200, apply_mult=False)
                        atk_r.funds = scale_bonus(150, apply_mult=False)
                        atk_r.merit = scale_bonus(12, apply_mult=True)
                        atk_r.material_units = 10
                        atk_r.material_tradable = 1
                    else:
                        atk_r.material_pvp_loss_ineligible = True
                else:  # DRAW
                    for r in (atk_r, def_r):
                        if qualified_loss:
                            r.contribution = scale_bonus(35, apply_mult=True)
                            r.silver = scale_bonus(1200, apply_mult=False)
                            r.funds = scale_bonus(150, apply_mult=False)
                            r.merit = scale_bonus(12, apply_mult=True)
                            r.material_units = 10
                            r.material_tradable = 1
                        else:
                            r.material_pvp_loss_ineligible = True
            else:  # GARRISON_SIEGE（守旗 AI 托管不发个人收益）
                if attacker_win:
                    atk_r.contribution = scale_bonus(40, apply_mult=True)
                    atk_r.silver = scale_bonus(1500, apply_mult=False)
                    atk_r.funds = scale_bonus(150, apply_mult=False)
                    atk_r.merit = scale_bonus(12, apply_mult=True)
                    atk_r.material_units = 10
                    atk_r.material_tradable = 1

            # 连胜与终结悬赏（仅真人 PVP）
            if is_pvp and btl.attacker_ids and btl.defender_ids:
                atk_st = self._ensure_player(btl.attacker_ids[0], btl.zone_id, now_ms)
                def_st = self._ensure_player(btl.defender_ids[0], btl.zone_id, now_ms)
                if attacker_win:
                    if def_st.streak_count >= 3 and len(def_st.streak_distinct_opponents) >= _STREAK_MIN_DISTINCT:
                        jc, js = _streak_jackpot(def_st.streak_count)
                        atk_r.jackpot_contribution += jc
                        atk_r.jackpot_silver += js
                        atk_r.funds += 200
                        atk_r.merit += 20
                        btl.bounty_outcome_tag = BOUNTY_TAG_STREAK_END
                        self._emit_alert(
                            ALERT_STREAK_BOUNTY,
                            actor_name=btl.attacker_names[0],
                            target_name=btl.defender_names[0],
                            streak_count=def_st.streak_count,
                            reward_silver=js,
                            battle_id=battle_id,
                        )
                    def_st.streak_count = 0
                    def_st.streak_distinct_opponents.clear()
                    def_st.streak_last_opponent_id = 0
                    if dim_pct == 100:
                        atk_st.streak_count += 1
                        atk_st.streak_distinct_opponents.add(def_st.player_id)
                        atk_st.streak_last_opponent_id = def_st.player_id
                elif defender_win:
                    if atk_st.streak_count >= 3 and len(atk_st.streak_distinct_opponents) >= _STREAK_MIN_DISTINCT:
                        jc, js = _streak_jackpot(atk_st.streak_count)
                        def_r.jackpot_contribution += jc
                        def_r.jackpot_silver += js
                        def_r.funds += 200
                        def_r.merit += 20
                        btl.bounty_outcome_tag = BOUNTY_TAG_STREAK_END
                    atk_st.streak_count = 0
                    atk_st.streak_distinct_opponents.clear()
                    atk_st.streak_last_opponent_id = 0
                    if dim_pct == 100:
                        def_st.streak_count += 1
                        def_st.streak_distinct_opponents.add(atk_st.player_id)
                        def_st.streak_last_opponent_id = atk_st.player_id
                btl.streak_after[atk_st.player_id] = atk_st.streak_count
                btl.streak_after[def_st.player_id] = def_st.streak_count

            # 灵脉旗势、15 分钟护盾与「每帮限占 1 座」硬约束
            if btl.leyline_id in LEYLINE_BY_ID:
                ll = self._ensure_leyline(btl.zone_id, btl.leyline_id, now_ms)
                ll.active_battle_ids.discard(battle_id)
                btl.banner_hp_before = ll.banner_hp
                btl.banner_shielded = now_ms < ll.shield_until_ms
                if not btl.banner_shielded:
                    if attacker_win:
                        dmg = self.rule.pvp_win_banner_damage if is_pvp else self.rule.siege_win_banner_damage
                        ll.banner_hp = max(0, ll.banner_hp - dmg)
                        if ll.banner_hp == 0:
                            # 破旗首功奖励
                            atk_r.jackpot_contribution += 120
                            atk_r.jackpot_silver += 5000
                            atk_r.funds += 800
                            atk_r.merit += 50
                            btl.bounty_outcome_tag = BOUNTY_TAG_BANNER_BREAK
                            already_owns = any(
                                other.owner_guild_id == btl.attacker_guild_id
                                for (z, lid), other in self._leylines.items()
                                if z == btl.zone_id and lid != ll.leyline_id
                            )
                            if already_owns:
                                # 硬约束：已占灵脉的帮会不兼并第 2 座，灵脉回归无主守卫
                                ll.owner_guild_id = 0
                                ll.banner_hp = self.rule.leyline_max_banner_hp
                                ll.shield_until_ms = 0
                            else:
                                ll.owner_guild_id = btl.attacker_guild_id
                                ll.banner_hp = self.rule.leyline_capture_init_hp
                                ll.shield_until_ms = now_ms + self.rule.leyline_shield_sec * 1000
                                ll.captured_at_ms = now_ms
                                ll.last_dividend_at_ms = now_ms
                            self._emit_alert(
                                ALERT_LEYLINE_CAPTURED,
                                leyline_id=ll.leyline_id,
                                scene_config_id=LEYLINE_BY_ID[ll.leyline_id][1],
                                attacker_guild_name=btl.attacker_guild_name,
                                defender_guild_name=btl.defender_guild_name,
                                battle_id=battle_id,
                            )
                    elif defender_win and is_pvp and ll.owner_guild_id == btl.defender_guild_id:
                        ll.banner_hp = min(self.rule.leyline_max_banner_hp, ll.banner_hp + self.rule.pvp_defend_banner_heal)
                btl.banner_hp_after = ll.banner_hp
                ll.updated_ms = now_ms

            # 追杀令销单 / 反杀 / 反杀 2 人撕单（r2-gw#1 / #2：行锁内复核同区 + 接单资格 + 未过期）
            bounty_silver_refunds: list[tuple[int, int, int, int]] = []  # (player_id, guild_id, tier, silver, bounty_id)
            if btl.bounty_id != 0 and btl.bounty_id in self._bounties:
                b = self._bounties[btl.bounty_id]
                if bounty_huntable(b, btl.zone_id, btl.attacker_guild_id, now_ms):
                    if attacker_win and btl.attacker_ids:
                        hunter = self._ensure_player(btl.attacker_ids[0], btl.zone_id, now_ms)
                        target_st = self._ensure_player(b.target_player_id, btl.zone_id, now_ms)
                        b.status = BOUNTY_STATUS_COMPLETED
                        b.hunter_player_id = hunter.player_id
                        b.resolved_at_ms = now_ms
                        target_st.bounty_protect_until_ms = now_ms + self.rule.bounty_protect_sec * 1000
                        atk_r.jackpot_contribution += 100
                        atk_r.funds += 200
                        atk_r.merit += 25
                        payout = 0
                        if hunter.bounty_payouts_today < self.rule.bounty_daily_payout_limit:
                            hunter.bounty_payouts_today += 1
                            payout = b.escrow_silver
                            atk_r.jackpot_silver += payout
                        btl.bounty_outcome_tag = BOUNTY_TAG_HUNT_WIN
                        self._emit_alert(
                            ALERT_BOUNTY_RESOLVED,
                            bounty_id=b.bounty_id,
                            actor_name=btl.attacker_names[0],
                            target_name=btl.defender_names[0] if btl.defender_names else "",
                            reward_silver=payout,
                            battle_id=battle_id,
                        )
                    elif defender_win and btl.attacker_ids and btl.defender_ids:
                        target_st = self._ensure_player(b.target_player_id, btl.zone_id, now_ms)
                        attacker_pid = btl.attacker_ids[0]
                        if attacker_pid != b.last_counter_attacker_id:
                            b.counter_kills += 1
                            b.last_counter_attacker_id = attacker_pid
                        def_r.jackpot_contribution += 80
                        def_r.funds += 150
                        def_r.merit += 20
                        silver_bonus = 3000
                        if b.counter_kills >= self.rule.bounty_counter_kill_tear:
                            b.status = BOUNTY_STATUS_TORN_BY_TARGET
                            b.resolved_at_ms = now_ms
                            target_half = b.escrow_silver // 2
                            silver_bonus += target_half
                            btl.bounty_outcome_tag = BOUNTY_TAG_COUNTER_TEAR
                            # r2-gw#2：撕单退发布者另一半（公费档退一半实付帮会资金，银两档退另一半托管银两）。
                            if b.tier == BOUNTY_TIER_GUILD_PUBLIC:
                                issuer_funds = b.escrow_guild_funds - (b.escrow_guild_funds // 2)
                                if issuer_funds > 0:
                                    funds_to_apply.append((b.issuer_guild_id, issuer_funds))
                            else:
                                issuer_silver = b.escrow_silver - target_half
                                if issuer_silver > 0:
                                    bounty_silver_refunds.append(
                                        (b.issuer_player_id, b.issuer_guild_id, b.tier, issuer_silver)
                                    )
                        else:
                            btl.bounty_outcome_tag = BOUNTY_TAG_COUNTER_KILL
                        if target_st.bounty_payouts_today < self.rule.bounty_daily_payout_limit:
                            target_st.bounty_payouts_today += 1
                            def_r.jackpot_silver += silver_bonus
                        self._emit_alert(
                            ALERT_BOUNTY_RESOLVED,
                            bounty_id=b.bounty_id,
                            actor_name=btl.defender_names[0],
                            target_name=btl.attacker_names[0],
                            reward_silver=def_r.jackpot_silver,
                            battle_id=battle_id,
                        )

            # 宿敌计数更新
            if btl.attacker_guild_id > 0 and btl.defender_guild_id > 0 and btl.attacker_guild_id != btl.defender_guild_id:
                rk = self._rivalry_key(btl.attacker_guild_id, btl.defender_guild_id, wk)
                clashes, b_mark = self._rivalries.get(rk, (0, False))
                self._rivalries[rk] = (clashes + 1, b_mark)

            # 仇敌录登记（被异帮击败时记入败方帮会近 24 小时仇敌录）
            if is_pvp and btl.attacker_ids and btl.defender_ids:
                if attacker_win and btl.defender_guild_id > 0:
                    self._record_enemy_locked(
                        btl.defender_guild_id,
                        _EnemyRecord(
                            player_id=btl.attacker_ids[0],
                            player_name=btl.attacker_names[0],
                            guild_id=btl.attacker_guild_id,
                            guild_name=btl.attacker_guild_name,
                            victim_name=btl.defender_names[0],
                            battle_kind=btl.war_battle_kind,
                            occurred_at_ms=now_ms,
                        ),
                    )
                elif defender_win and btl.attacker_guild_id > 0:
                    self._record_enemy_locked(
                        btl.attacker_guild_id,
                        _EnemyRecord(
                            player_id=btl.defender_ids[0],
                            player_name=btl.defender_names[0],
                            guild_id=btl.defender_guild_id,
                            guild_name=btl.defender_guild_name,
                            victim_name=btl.attacker_names[0],
                            battle_kind=btl.war_battle_kind,
                            occurred_at_ms=now_ms,
                        ),
                    )

            # 累加双方周战功、个人周战功、建帮首胜史记与待入账帮贡/资金
            for gid, pids, names, rew, won in (
                (btl.attacker_guild_id, btl.attacker_ids, btl.attacker_names, atk_r, attacker_win),
                (btl.defender_guild_id, btl.defender_ids, btl.defender_names, def_r, defender_win),
            ):
                if gid <= 0:
                    continue
                if won and gid not in self._first_wins:
                    self._first_wins[gid] = (names[:2], now_ms)
                gw = self._ensure_weekly(gid, wk, now_ms)
                if rew.merit > 0:
                    gw.guild_merit += rew.merit
                    gw.updated_ms = now_ms
                    for pid in pids:
                        st = self._ensure_player(pid, btl.zone_id, now_ms)
                        st.weekly_merit += rew.merit
                        gw.member_merit[pid] = gw.member_merit.get(pid, 0) + rew.merit
                total_c = rew.contribution + rew.jackpot_contribution
                if total_c > 0:
                    for pid in pids:
                        credits_to_apply.append((gid, pid, total_c))
                if rew.funds > 0:
                    funds_to_apply.append((gid, rew.funds))
                logs = self._guild_logs.setdefault(gid, deque(maxlen=_RECENT_LOG_LIMIT))
                logs.append(battle_id)

            # 计入 2 小时宿敌约战比分与双方战功（到点自动收官并播报战果）
            if feud_active and active_feud is not None:
                finished = self._record_feud_battle_locked(
                    active_feud,
                    btl.attacker_guild_id,
                    btl.defender_guild_id,
                    attacker_win,
                    defender_win,
                    atk_r.merit * len(btl.attacker_ids),
                    def_r.merit * len(btl.defender_ids),
                    now_ms,
                )
                if finished:
                    self._emit_feud_alert_locked(ALERT_FEUD_ENDED, active_feud)

            btl.settled = True

        # 在锁外写回帮会资金与个人帮贡（帮会已解散则静默跳过），并经分流通道处理撕单银两退本
        for gid, f_delta in funds_to_apply:
            await self._credit_guild_funds(gid, f_delta)
        for gid, pid, c_delta in credits_to_apply:
            await self._credit_member_contribution(gid, pid, c_delta)
        for issuer_pid, issuer_gid, b_tier, refund_silver in bounty_silver_refunds:
            await self.credit_war_asset(
                player_id=issuer_pid,
                guild_id=issuer_gid,
                silver=refund_silver,
                kind=OP_KIND_BOUNTY_PAYOUT,
                tx_type=TX_GUILD_BOUNTY_PAYOUT,
                ref_id=b_tier,
                source_id=btl.bounty_id or battle_id,
                now_ms=now_ms,
            )
        return btl

    def _record_enemy_locked(self, victim_guild_id: int, rec: _EnemyRecord) -> None:
        lst = self._enemies.setdefault(victim_guild_id, [])
        lst[:] = [e for e in lst if e.player_id != rec.player_id]
        lst.insert(0, rec)
        if len(lst) > 30:
            del lst[30:]

    async def claim_milestone(self, actor: RoleOriginal, request: Message) -> Message:
        """489 ClaimGuildWarMilestone：领取本周全帮战功里程碑宝箱（换帮不重领已领档位）。"""
        now_ms = self._now()
        req_id = int(request.client_request_id)
        if req_id != 0 and ("milestone", actor.player_id, req_id) in self._idempotency:
            return self._idempotency[("milestone", actor.player_id, req_id)]

        mid = int(request.milestone_id)
        cfg = self.milestones.get(mid)
        if cfg is None:
            return self.protocol.message(
                "guildpb.ClaimGuildWarMilestoneResponse",
                error_message=self._tip(ERR_INVALID_PARAMETER, "invalid milestone_id"),
            )
        guild = await self.store.get_player_guild(actor.player_id)
        if guild is None:
            return self.protocol.message(
                "guildpb.ClaimGuildWarMilestoneResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
            )
        zone_id = self._guild_zone(guild)
        wk = week_key(now_ms)
        bit = 1 << (mid - 1)

        async with self._lock:
            st = self._ensure_player(actor.player_id, zone_id, now_ms)
            weekly = self._ensure_weekly(guild.guild_id, wk, now_ms)
            if st.milestone_claimed_mask & bit:
                return self.protocol.message(
                    "guildpb.ClaimGuildWarMilestoneResponse",
                    error_message=self._tip(ERR_GUILD_WAR_MILESTONE_CLAIMED, "milestone already claimed"),
                    milestone=self._milestone_view(cfg, weekly.guild_merit, st),
                )
            if (
                weekly.guild_merit < cfg.required_guild_merit
                or st.weekly_merit < self.rule.dividend_min_personal_merit
            ):
                return self.protocol.message(
                    "guildpb.ClaimGuildWarMilestoneResponse",
                    error_message=self._tip(ERR_GUILD_WAR_MILESTONE_LOCKED, "milestone merit requirement not met"),
                    milestone=self._milestone_view(cfg, weekly.guild_merit, st),
                )
            st.milestone_claimed_mask |= bit
            st.updated_ms = now_ms
            mv = self._milestone_view(cfg, weekly.guild_merit, st)

        if cfg.reward_contribution > 0:
            await self._credit_member_contribution(guild.guild_id, actor.player_id, cfg.reward_contribution)

        resp = self.protocol.message(
            "guildpb.ClaimGuildWarMilestoneResponse",
            milestone=mv,
            asset_order_status=ORDER_STATUS_APPLIED,
        )
        if req_id != 0:
            self._idempotency[("milestone", actor.player_id, req_id)] = resp
        return resp

    async def claim_dividend(self, actor: RoleOriginal, request: Message) -> Message:
        """490 ClaimGuildWarDividend：一键领取累计灵脉周期分红（周上限 12 份，换帮不重置）。"""
        now_ms = self._now()
        req_id = int(request.client_request_id)
        if req_id != 0 and ("dividend", actor.player_id, req_id) in self._idempotency:
            return self._idempotency[("dividend", actor.player_id, req_id)]

        guild = await self.store.get_player_guild(actor.player_id)
        if guild is None:
            return self.protocol.message(
                "guildpb.ClaimGuildWarDividendResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
            )
        zone_id = self._guild_zone(guild)
        wk = week_key(now_ms)

        async with self._lock:
            await self._housekeep_locked(zone_id, now_ms)
            st = self._ensure_player(actor.player_id, zone_id, now_ms)
            weekly = self._ensure_weekly(guild.guild_id, wk, now_ms)
            cap = self.rule.dividend_player_weekly_cap
            if st.weekly_merit < self.rule.dividend_min_personal_merit:
                return self.protocol.message(
                    "guildpb.ClaimGuildWarDividendResponse",
                    error_message=self._tip(ERR_GUILD_WAR_DIVIDEND_EMPTY, "personal merit below 30"),
                    dividend=self._dividend_view(weekly, st),
                )
            available = max(0, min(weekly.dividend_pool_count, cap) - st.dividend_claimed_count)
            if available <= 0:
                return self.protocol.message(
                    "guildpb.ClaimGuildWarDividendResponse",
                    error_message=self._tip(ERR_GUILD_WAR_DIVIDEND_EMPTY, "no claimable dividend"),
                    dividend=self._dividend_view(weekly, st),
                )
            st.dividend_claimed_count += available
            st.updated_ms = now_ms
            resp = self.protocol.message(
                "guildpb.ClaimGuildWarDividendResponse",
                claimed_count=available,
                dividend=self._dividend_view(weekly, st),
                asset_order_status=ORDER_STATUS_APPLIED,
            )
            if req_id != 0:
                self._idempotency[("dividend", actor.player_id, req_id)] = resp
            return resp

    async def post_bounty(self, actor: RoleOriginal, request: Message) -> Message:
        """491 PostGuildWarBounty：按配表固定小额档位发布追杀令（含 10% 销毁税、每日公费限 2、单目标限 1）。"""
        now_ms = self._now()
        req_id = int(request.client_request_id)
        if req_id != 0 and ("bounty", actor.player_id, req_id) in self._idempotency:
            return self._idempotency[("bounty", actor.player_id, req_id)]

        target_pid = int(request.target_player_id)
        tier = int(request.tier)
        public_board = bool(request.public_board)

        def fail(tip_id: int, msg: str) -> Message:
            return self.protocol.message(
                "guildpb.PostGuildWarBountyResponse",
                error_message=self._tip(tip_id, msg),
            )

        if target_pid <= 0 or target_pid == actor.player_id:
            return fail(ERR_GUILD_WAR_BOUNTY_INVALID_TARGET, "invalid bounty target")
        if tier not in (BOUNTY_TIER_NORMAL_SILVER, BOUNTY_TIER_HEAVY_SILVER, BOUNTY_TIER_GUILD_PUBLIC):
            return fail(ERR_INVALID_PARAMETER, "invalid bounty tier")

        issuer_guild = await self.store.get_player_guild(actor.player_id)
        if issuer_guild is None or (me_member := issuer_guild.member(actor.player_id)) is None:
            return fail(ERR_GUILD_NOT_IN_GUILD, "not in any guild")
        target_guild = await self.store.get_player_guild(target_pid)
        zone_id = self._guild_zone(issuer_guild)
        if target_guild is None or self._guild_zone(target_guild) != zone_id:
            return fail(ERR_GUILD_WAR_BOUNTY_INVALID_TARGET, "target must belong to another guild in same zone")
        if target_guild.guild_id == issuer_guild.guild_id:
            return fail(ERR_GUILD_WAR_SAME_GUILD, "cannot post bounty on same guild member")

        if tier == BOUNTY_TIER_GUILD_PUBLIC and role_rank(me_member.role) < RANK_OFFICER:
            return fail(ERR_GUILD_NO_PERMISSION, "public bounty requires officer permission")

        names = await self._resolve_names([target_pid])
        wk = week_key(now_ms)
        dk = day_key(now_ms)

        async with self._lock:
            await self._housekeep_locked(zone_id, now_ms)
            me = self._ensure_player(actor.player_id, zone_id, now_ms)
            tgt = self._ensure_player(target_pid, zone_id, now_ms)
            if self._active_bounty_for_target(target_pid, now_ms) is not None:
                return fail(ERR_GUILD_WAR_BOUNTY_ALREADY_ACTIVE, "target already has an active bounty")
            if now_ms < tgt.bounty_protect_until_ms:
                return fail(ERR_GUILD_WAR_TARGET_PROTECTED, "target under post-bounty protection")

            if tier == BOUNTY_TIER_GUILD_PUBLIC:
                used = self._public_bounty_day.get((issuer_guild.guild_id, dk), 0)
                if used >= self.rule.bounty_public_daily_limit:
                    return fail(ERR_GUILD_WAR_BOUNTY_LIMIT, "daily guild public bounty limit reached")
                if not await self._deduct_guild_funds(issuer_guild.guild_id, self.rule.bounty_public_funds):
                    return fail(ERR_GUILD_FUNDS_INSUFFICIENT, "insufficient guild funds for public bounty")
                self._public_bounty_day[(issuer_guild.guild_id, dk)] = used + 1
                escrow = _PUBLIC_BOUNTY_REWARD_SILVER
            else:
                gross = (
                    self.rule.bounty_normal_silver
                    if tier == BOUNTY_TIER_NORMAL_SILVER
                    else self.rule.bounty_heavy_silver
                )
                escrow = gross - (gross * self.rule.bounty_tax_bps) // 10000

            self._next_bounty_id += 1
            b = _BountyState(
                bounty_id=self._next_bounty_id,
                zone_id=zone_id,
                issuer_player_id=actor.player_id,
                issuer_guild_id=issuer_guild.guild_id,
                target_player_id=target_pid,
                target_guild_id=target_guild.guild_id,
                tier=tier,
                escrow_silver=escrow,
                public_board=public_board,
                status=BOUNTY_STATUS_ACTIVE,
                escrow_guild_funds=self.rule.bounty_public_funds if tier == BOUNTY_TIER_GUILD_PUBLIC else 0,
                created_at_ms=now_ms,
                expires_at_ms=now_ms + self.rule.bounty_ttl_sec * 1000,
            )
            self._bounties[b.bounty_id] = b
            # 互挂追杀令直接结为本周宿敌
            rk = self._rivalry_key(issuer_guild.guild_id, target_guild.guild_id, wk)
            clashes, _ = self._rivalries.get(rk, (0, False))
            self._rivalries[rk] = (clashes, True)

            view = self._bounty_view(
                b,
                target_name=names.get(target_pid, f"player-{target_pid}"),
                target_guild_name=target_guild.name,
                issuer_guild_name=issuer_guild.name,
                caller_state=me,
                target_online=True,
                now_ms=now_ms,
            )
            resp = self.protocol.message("guildpb.PostGuildWarBountyResponse", bounty=view)
            if req_id != 0:
                self._idempotency[("bounty", actor.player_id, req_id)] = resp
            return resp

    async def track_bounty(self, actor: RoleOriginal, request: Message) -> Message:
        """492 TrackGuildWarBounty：15 秒冷却追踪目标所在野外图与分线（绝不暴露精确坐标或账号）。"""
        now_ms = self._now()
        bid = int(request.bounty_id)
        guild = await self.store.get_player_guild(actor.player_id)
        if guild is None:
            return self.protocol.message(
                "guildpb.TrackGuildWarBountyResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
            )
        zone_id = self._guild_zone(guild)
        cooldown_ms = self.rule.track_cooldown_sec * 1000

        async with self._lock:
            await self._housekeep_locked(zone_id, now_ms)
            me = self._ensure_player(actor.player_id, zone_id, now_ms)
            b = self._bounties.get(bid)
            if (
                b is None
                or b.zone_id != zone_id
                or b.status != BOUNTY_STATUS_ACTIVE
                or (b.issuer_guild_id != guild.guild_id and not b.public_board)
            ):
                return self.protocol.message(
                    "guildpb.TrackGuildWarBountyResponse",
                    error_message=self._tip(ERR_GUILD_WAR_BOUNTY_NOT_FOUND, "bounty not found"),
                )
            if me.last_track_ms > 0 and now_ms - me.last_track_ms < cooldown_ms:
                return self.protocol.message(
                    "guildpb.TrackGuildWarBountyResponse",
                    error_message=self._tip(ERR_GUILD_WAR_TRACK_COOLDOWN, "track cooldown active"),
                    next_track_at_ms=me.last_track_ms + cooldown_ms,
                )
            me.last_track_ms = now_ms
            next_track = now_ms + cooldown_ms
            target_pid = b.target_player_id

        # 锁外查询定位（安全区或离线时 scene_id 恒为 0）
        online = True
        in_safe_zone = False
        scene_cfg = LEYLINES[0][2]
        channel_no = 1
        scene_id = scene_cfg
        if self._player_locator is not None:
            online, in_safe_zone, scene_cfg, channel_no, scene_id = await self._player_locator(target_pid)
        elif self._online is not None:
            try:
                om = await self._online.batch_online([target_pid])
                online = bool(om.get(target_pid, False))
            except Exception:
                online = False
        if not online or in_safe_zone:
            scene_id = 0
            if not online:
                scene_cfg = 0
                channel_no = 0
        else:
            async with self._lock:
                if bid in self._bounties:
                    self._bounties[bid].last_scene_config_id = scene_cfg
                    self._bounties[bid].last_channel_no = channel_no

        return self.protocol.message(
            "guildpb.TrackGuildWarBountyResponse",
            target_online=online,
            in_safe_zone=in_safe_zone,
            scene_config_id=scene_cfg,
            channel_no=channel_no,
            next_track_at_ms=next_track,
            scene_id=scene_id,
        )

    async def get_battle_result(self, actor: RoleOriginal, request: Message) -> Message:
        """493 GetGuildWarBattleResult：回查单局帮战权威结算详情与一键追杀候选。"""
        bid = int(request.battle_id)
        if bid <= 0:
            return self.protocol.message(
                "guildpb.GetGuildWarBattleResultResponse",
                error_message=self._tip(ERR_INVALID_PARAMETER, "battle_id required"),
            )
        async with self._lock:
            btl = self._battles.get(bid)
            if btl is None or not btl.settled:
                return self.protocol.message(
                    "guildpb.GetGuildWarBattleResultResponse",
                    settled=False,
                    battle_id=bid,
                    war_battle_kind=btl.war_battle_kind if btl else 0,
                )
            is_defender = actor.player_id in btl.defender_ids
            rew = btl.defender_reward if is_defender else btl.attacker_reward
            lost_to_human = (
                btl.match_mode == MATCH_MODE_GUILD_WAR_PVP
                and (
                    (not is_defender and btl.outcome == BATTLE_OUTCOME_SIDE_B_WIN and bool(btl.defender_ids))
                    or (is_defender and btl.outcome == BATTLE_OUTCOME_SIDE_A_WIN and bool(btl.attacker_ids))
                )
            )
            cand_pid = 0
            cand_name = ""
            if lost_to_human:
                cand_pid = btl.attacker_ids[0] if is_defender else btl.defender_ids[0]
                cand_name = btl.attacker_names[0] if is_defender else btl.defender_names[0]
            can_post = lost_to_human and cand_pid > 0 and self._active_bounty_for_target(cand_pid, self._now()) is None
            return self.protocol.message(
                "guildpb.GetGuildWarBattleResultResponse",
                settled=True,
                battle_id=btl.battle_id,
                war_battle_kind=btl.war_battle_kind,
                outcome=btl.outcome,
                rounds=btl.rounds,
                material_units_added=rew.material_units,
                material_tradable_added=rew.material_tradable,
                material_pvp_loss_ineligible=rew.material_pvp_loss_ineligible,
                contribution_gain=rew.contribution,
                silver_gain=rew.silver,
                guild_funds_gain=rew.funds,
                merit_gain=rew.merit,
                streak_after=btl.streak_after.get(actor.player_id, 0),
                bounty_jackpot_contribution=rew.jackpot_contribution,
                bounty_jackpot_silver=rew.jackpot_silver,
                bounty_outcome_tag=btl.bounty_outcome_tag,
                diminish_percent=btl.diminish_percent,
                leyline_id=btl.leyline_id,
                banner_hp_before=btl.banner_hp_before,
                banner_hp_after=btl.banner_hp_after,
                banner_shielded=btl.banner_shielded,
                can_post_bounty=can_post,
                bounty_candidate_player_id=cand_pid,
                bounty_candidate_name=cand_name,
                gold_leyline_bonus=btl.gold_leyline_bonus,
                rivalry_bonus=btl.rivalry_bonus,
                feud_bonus=btl.feud_bonus,
            )

    async def inspect_field_targets(self, actor: RoleOriginal, request: Message) -> Message:
        """499 InspectFieldWarTargets：批量查询野外同屏玩家（最多 30 人）的帮战头顶状态与可点战标记。"""
        now_ms = self._now()
        raw_ids = [int(pid) for pid in dict.fromkeys(request.player_ids) if int(pid) > 0]
        if len(raw_ids) > _INSPECT_MAX_TARGETS:
            return self.protocol.message(
                "guildpb.InspectFieldWarTargetsResponse",
                error_message=self._tip(ERR_INVALID_PARAMETER, "too many player_ids (max 30)"),
            )
        caller_guild = await self.store.get_player_guild(actor.player_id)
        caller_gid = caller_guild.guild_id if caller_guild is not None else 0
        caller_zone = self._guild_zone(caller_guild) if caller_guild is not None else 1
        wk = week_key(now_ms)

        target_guilds: dict[int, GuildData | None] = {}
        for pid in raw_ids:
            target_guilds[pid] = await self.store.get_player_guild(pid)
        names = await self._resolve_names(raw_ids)

        resp = self.protocol.message("guildpb.InspectFieldWarTargetsResponse")
        async with self._lock:
            await self._housekeep_locked(caller_zone, now_ms)
            me = self._ensure_player(actor.player_id, caller_zone, now_ms)
            for pid in raw_ids:
                tg = target_guilds.get(pid)
                tg_id = tg.guild_id if tg is not None else 0
                tg_zone = self._guild_zone(tg) if tg is not None else 0
                st = self._players.get(pid)
                peace = st.peace_mode if st is not None else False
                protect_until = st.protect_until_ms if st is not None else 0
                streak = st.streak_count if st is not None else 0
                bid = st.in_battle_id if st is not None else 0
                ab = self._active_bounty_for_target(pid, now_ms)
                b_id, b_silver, b_tier = field_target_bounty(ab, caller_zone, caller_gid, now_ms)
                can_attack = (
                    caller_gid > 0
                    and tg_id > 0
                    and tg_id != caller_gid
                    and tg_zone == caller_zone
                    and not me.peace_mode
                    and not peace
                    and now_ms >= protect_until
                    and me.in_battle_id == 0
                    and bid == 0
                )
                resp.targets.add(
                    player_id=pid,
                    player_name=names.get(pid, f"player-{pid}"),
                    guild_id=tg_id,
                    guild_name=tg.name if tg is not None else "",
                    peace_mode=peace,
                    protect_until_ms=protect_until,
                    streak=streak,
                    in_battle=bid != 0,
                    battle_id=bid,
                    is_rival_guild=self._is_rival(caller_gid, tg_id, wk),
                    active_bounty_id=b_id,
                    active_bounty_reward_silver=b_silver,
                    active_bounty_tier=b_tier,
                    can_attack_now=can_attack,
                )
        return resp

    # -- 帮战 V2.5 · 宿敌约战战书（567 IssueGuildWarFeud / 568 RespondGuildWarFeud）-----------------

    def _feud_view(self, rec: _FeudState) -> Message:
        return self.protocol.message(
            "guildpb.GuildWarFeudView",
            feud_id=rec.feud_id,
            issuer_guild_id=rec.issuer_guild_id,
            issuer_guild_name=rec.issuer_guild_name,
            target_guild_id=rec.target_guild_id,
            target_guild_name=rec.target_guild_name,
            status=rec.status,
            issuer_wins=rec.issuer_wins,
            target_wins=rec.target_wins,
            issuer_merit=rec.issuer_merit,
            target_merit=rec.target_merit,
            created_at_ms=rec.created_at_ms,
            accept_deadline_ms=rec.accept_deadline_ms,
            started_at_ms=rec.started_at_ms,
            ends_at_ms=rec.ends_at_ms,
            winner_guild_id=rec.winner_guild_id,
        )

    def _emit_feud_alert_locked(self, kind: int, rec: _FeudState) -> None:
        self._emit_alert(
            kind,
            attacker_guild_name=rec.issuer_guild_name,
            defender_guild_name=rec.target_guild_name,
            feud_id=rec.feud_id,
            issuer_wins=rec.issuer_wins,
            target_wins=rec.target_wins,
        )

    def _active_or_pending_feud_locked(
        self, guild_id: int, now_ms: int
    ) -> tuple[_FeudState | None, _FeudState | None]:
        """返回 (生效中或待应答约战, 本次惰性收官的约战)；同步收口超时战书。"""
        candidates = [
            f
            for f in self._feuds.values()
            if (f.issuer_guild_id == guild_id or f.target_guild_id == guild_id)
            and f.status in (FEUD_STATUS_PENDING, FEUD_STATUS_ACTIVE)
        ]
        if not candidates:
            return None, None
        candidates.sort(key=lambda f: (f.created_at_ms, f.feud_id), reverse=True)
        rec = candidates[0]
        if rec.status == FEUD_STATUS_PENDING and rec.accept_deadline_ms > 0 and now_ms >= rec.accept_deadline_ms:
            rec.status = FEUD_STATUS_EXPIRED
            rec.updated_ms = now_ms
            return None, None
        if rec.status == FEUD_STATUS_ACTIVE and rec.ends_at_ms > 0 and now_ms >= rec.ends_at_ms:
            rec.status = FEUD_STATUS_FINISHED
            rec.winner_guild_id = resolve_feud_winner(
                rec.issuer_guild_id,
                rec.target_guild_id,
                rec.issuer_wins,
                rec.target_wins,
                rec.issuer_merit,
                rec.target_merit,
            )
            rec.updated_ms = now_ms
            return None, rec
        return rec, None

    def _last_finished_feud_locked(self, guild_id: int) -> _FeudState | None:
        finished = [
            f
            for f in self._feuds.values()
            if (f.issuer_guild_id == guild_id or f.target_guild_id == guild_id)
            and f.status == FEUD_STATUS_FINISHED
        ]
        if not finished:
            return None
        finished.sort(key=lambda f: (f.ends_at_ms, f.feud_id), reverse=True)
        return finished[0]

    def _last_feud_issued_at_locked(self, issuer_guild_id: int) -> int:
        times = [
            f.created_at_ms
            for f in self._feuds.values()
            if f.issuer_guild_id == issuer_guild_id
            and f.status in (FEUD_STATUS_PENDING, FEUD_STATUS_ACTIVE, FEUD_STATUS_FINISHED)
        ]
        return max(times) if times else 0

    def _active_feud_between_locked(self, g1: int, g2: int, now_ms: int) -> _FeudState | None:
        if g1 <= 0 or g2 <= 0 or g1 == g2:
            return None
        for rec in self._feuds.values():
            if rec.status != FEUD_STATUS_ACTIVE:
                continue
            if {rec.issuer_guild_id, rec.target_guild_id} == {g1, g2}:
                if rec.ends_at_ms > 0 and now_ms >= rec.ends_at_ms:
                    return None
                return rec
        return None

    def _record_feud_battle_locked(
        self,
        rec: _FeudState,
        atk_guild_id: int,
        def_guild_id: int,
        atk_won: bool,
        def_won: bool,
        atk_merit: int,
        def_merit: int,
        now_ms: int,
    ) -> bool:
        if rec.status != FEUD_STATUS_ACTIVE:
            return False
        if atk_guild_id == rec.issuer_guild_id and def_guild_id == rec.target_guild_id:
            if atk_won:
                rec.issuer_wins += 1
            elif def_won:
                rec.target_wins += 1
            rec.issuer_merit += atk_merit
            rec.target_merit += def_merit
        elif atk_guild_id == rec.target_guild_id and def_guild_id == rec.issuer_guild_id:
            if atk_won:
                rec.target_wins += 1
            elif def_won:
                rec.issuer_wins += 1
            rec.target_merit += atk_merit
            rec.issuer_merit += def_merit
        finished = rec.ends_at_ms > 0 and now_ms >= rec.ends_at_ms
        if finished:
            rec.status = FEUD_STATUS_FINISHED
            rec.winner_guild_id = resolve_feud_winner(
                rec.issuer_guild_id,
                rec.target_guild_id,
                rec.issuer_wins,
                rec.target_wins,
                rec.issuer_merit,
                rec.target_merit,
            )
        rec.updated_ms = now_ms
        return finished

    @staticmethod
    def _is_officer_or_leader(guild: GuildData, player_id: int) -> bool:
        if guild.leader_id == player_id:
            return True
        member = guild.member(player_id)
        return member is not None and role_rank(member.role) >= RANK_OFFICER

    async def issue_feud(self, actor: RoleOriginal, request: Message) -> Message:
        """567 IssueGuildWarFeud：帮主/长老向异帮下发 2 小时宿敌约战战书（15 分钟应战窗，24h 冷却）。"""
        now_ms = self._now()
        guild = await self.store.get_player_guild(actor.player_id)
        if guild is None or guild.member(actor.player_id) is None:
            return self.protocol.message(
                "guildpb.IssueGuildWarFeudResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
            )
        if not self._is_officer_or_leader(guild, actor.player_id):
            return self.protocol.message(
                "guildpb.IssueGuildWarFeudResponse",
                error_message=self._tip(ERR_GUILD_RANK_TOO_LOW, "officer or leader required"),
            )
        target_guild_id = int(request.target_guild_id)
        if target_guild_id <= 0 or target_guild_id == guild.guild_id:
            return self.protocol.message(
                "guildpb.IssueGuildWarFeudResponse",
                error_message=self._tip(ERR_GUILD_WAR_FEUD_INVALID_TARGET, "invalid target guild"),
            )
        target_guild = await self.store.get_guild(target_guild_id)
        if target_guild is None:
            return self.protocol.message(
                "guildpb.IssueGuildWarFeudResponse",
                error_message=self._tip(ERR_GUILD_WAR_FEUD_INVALID_TARGET, "target guild not found"),
            )
        zone_id = self._guild_zone(guild)
        async with self._lock:
            a_live, a_fin = self._active_or_pending_feud_locked(guild.guild_id, now_ms)
            if a_fin is not None:
                self._emit_feud_alert_locked(ALERT_FEUD_ENDED, a_fin)
            b_live, b_fin = self._active_or_pending_feud_locked(target_guild_id, now_ms)
            if b_fin is not None:
                self._emit_feud_alert_locked(ALERT_FEUD_ENDED, b_fin)
            if a_live is not None or b_live is not None:
                return self.protocol.message(
                    "guildpb.IssueGuildWarFeudResponse",
                    error_message=self._tip(ERR_GUILD_WAR_FEUD_ALREADY_ACTIVE, "guild already has pending or active feud"),
                )
            last_issued = self._last_feud_issued_at_locked(guild.guild_id)
            if last_issued > 0 and now_ms < last_issued + FEUD_COOLDOWN_SEC * 1000:
                return self.protocol.message(
                    "guildpb.IssueGuildWarFeudResponse",
                    error_message=self._tip(ERR_GUILD_WAR_FEUD_COOLDOWN, "feud issue cooldown active"),
                )
            self._next_feud_id += 1
            rec = _FeudState(
                feud_id=self._next_feud_id,
                zone_id=zone_id,
                issuer_guild_id=guild.guild_id,
                issuer_guild_name=guild.name,
                target_guild_id=target_guild.guild_id,
                target_guild_name=target_guild.name,
                status=FEUD_STATUS_PENDING,
                created_at_ms=now_ms,
                accept_deadline_ms=now_ms + FEUD_ACCEPT_WINDOW_SEC * 1000,
                updated_ms=now_ms,
            )
            self._feuds[rec.feud_id] = rec
            self._emit_feud_alert_locked(ALERT_FEUD_CHALLENGE, rec)
            view = self._feud_view(rec)
        return self.protocol.message(
            "guildpb.IssueGuildWarFeudResponse",
            feud=view,
            next_issue_at_ms=now_ms + FEUD_COOLDOWN_SEC * 1000,
        )

    async def respond_feud(self, actor: RoleOriginal, request: Message) -> Message:
        """568 RespondGuildWarFeud：目标帮会管理应战/拒战，或发起方管理撤回待应答战书。"""
        now_ms = self._now()
        guild = await self.store.get_player_guild(actor.player_id)
        if guild is None or guild.member(actor.player_id) is None:
            return self.protocol.message(
                "guildpb.RespondGuildWarFeudResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
            )
        if not self._is_officer_or_leader(guild, actor.player_id):
            return self.protocol.message(
                "guildpb.RespondGuildWarFeudResponse",
                error_message=self._tip(ERR_GUILD_RANK_TOO_LOW, "officer or leader required"),
            )
        feud_id = int(request.feud_id)
        accept = bool(request.accept)
        async with self._lock:
            rec = self._feuds.get(feud_id)
            if rec is None or rec.status != FEUD_STATUS_PENDING:
                return self.protocol.message(
                    "guildpb.RespondGuildWarFeudResponse",
                    error_message=self._tip(ERR_GUILD_WAR_FEUD_NOT_FOUND, "feud not found or resolved"),
                )
            if rec.accept_deadline_ms > 0 and now_ms >= rec.accept_deadline_ms:
                rec.status = FEUD_STATUS_EXPIRED
                rec.updated_ms = now_ms
                return self.protocol.message(
                    "guildpb.RespondGuildWarFeudResponse",
                    error_message=self._tip(ERR_GUILD_WAR_FEUD_NOT_FOUND, "feud expired"),
                )
            if accept:
                if guild.guild_id != rec.target_guild_id:
                    return self.protocol.message(
                        "guildpb.RespondGuildWarFeudResponse",
                        error_message=self._tip(ERR_GUILD_WAR_FEUD_NOT_FOUND, "only target guild can accept"),
                    )
                rec.status = FEUD_STATUS_ACTIVE
                rec.started_at_ms = now_ms
                rec.ends_at_ms = now_ms + FEUD_DURATION_SEC * 1000
                rec.updated_ms = now_ms
                self._emit_feud_alert_locked(ALERT_FEUD_STARTED, rec)
            else:
                if guild.guild_id not in (rec.target_guild_id, rec.issuer_guild_id):
                    return self.protocol.message(
                        "guildpb.RespondGuildWarFeudResponse",
                        error_message=self._tip(ERR_GUILD_WAR_FEUD_NOT_FOUND, "not feud participant guild"),
                    )
                rec.status = FEUD_STATUS_DECLINED
                rec.updated_ms = now_ms
            view = self._feud_view(rec)
        return self.protocol.message("guildpb.RespondGuildWarFeudResponse", feud=view)

    # -- 江湖恩怨·帮派生死状恩怨墙（547 GetGuildJianghuGrudgeWall）----------------------------------

    async def record_jianghu_duel_result(self, event: Message) -> bool:
        """幂等记入两帮纯同帮小队天墉生死状的帮派恩怨汇总与近 20 场恩怨战绩。"""
        if event is None or int(getattr(event, "battle_id", 0)) == 0:
            return False
        if int(getattr(event, "match_mode", 0)) != MATCH_MODE_JIANGHU_DUEL:
            return False
        total_rounds = int(getattr(event, "total_rounds", 0))
        outcome = int(getattr(event, "outcome", 0))
        if total_rounds == 0 or outcome not in (BATTLE_OUTCOME_SIDE_A_WIN, BATTLE_OUTCOME_SIDE_B_WIN, BATTLE_OUTCOME_DRAW):
            return False
        if not event.HasField("jianghu_duel_context"):
            return False
        duel = event.jianghu_duel_context
        gid_a = int(duel.side_a_guild_id)
        gid_b = int(duel.side_b_guild_id)
        if gid_a <= 0 or gid_b <= 0 or gid_a == gid_b:
            return False

        if outcome == BATTLE_OUTCOME_SIDE_A_WIN:
            out_a, out_b = 1, 2
        elif outcome == BATTLE_OUTCOME_SIDE_B_WIN:
            out_a, out_b = 2, 1
        else:
            out_a, out_b = 3, 3

        battle_id = int(event.battle_id)
        finished_ms = int(getattr(event, "finished_at_ms", 0)) or battle_id
        writ_tier = int(duel.writ_tier)
        team_size = int(duel.team_size)

        sides = (
            (gid_a, gid_b, str(duel.side_b_guild_name), out_a, str(duel.challenger_captain_name), str(duel.target_captain_name)),
            (gid_b, gid_a, str(duel.side_a_guild_name), out_b, str(duel.target_captain_name), str(duel.challenger_captain_name)),
        )
        async with self._lock:
            for gid, _, _, _, _, _ in sides:
                if (gid, battle_id) in self._jianghu_seen:
                    return False
            for gid, rival_id, rival_name, out_for_guild, our_cap, rival_cap in sides:
                self._jianghu_seen.add((gid, battle_id))
                rivals_map = self._jianghu_rivals.setdefault(gid, {})
                summary = rivals_map.get(rival_id)
                if summary is None:
                    summary = _GuildJianghuRivalSummary(rival_guild_id=rival_id, rival_guild_name=rival_name)
                    rivals_map[rival_id] = summary
                else:
                    summary.rival_guild_name = rival_name
                if out_for_guild == 1:
                    summary.wins += 1
                elif out_for_guild == 2:
                    summary.losses += 1
                else:
                    summary.draws += 1
                summary.last_battle_ms = max(summary.last_battle_ms, finished_ms)

                recs = self._jianghu_records.setdefault(gid, [])
                recs.append(
                    _GuildJianghuGrudgeRec(
                        battle_id=battle_id,
                        rival_guild_id=rival_id,
                        rival_guild_name=rival_name,
                        outcome_for_guild=out_for_guild,
                        writ_tier=writ_tier,
                        team_size=team_size,
                        total_rounds=total_rounds,
                        our_captain_name=our_cap,
                        rival_captain_name=rival_cap,
                        finished_ms=finished_ms,
                    )
                )
                recs.sort(key=lambda r: (r.finished_ms, r.battle_id), reverse=True)
                if len(recs) > MAX_GUILD_JIANGHU_WALL_RECORDS:
                    del recs[MAX_GUILD_JIANGHU_WALL_RECORDS:]
        return True

    async def get_jianghu_grudge_wall(self, actor: RoleOriginal) -> Message:
        """547 GetGuildJianghuGrudgeWall：返回本帮生死状宿敌帮会汇总与最近 20 场战绩（仅本帮在册成员可读）。"""
        guild = await self.store.get_player_guild(actor.player_id)
        if guild is None or guild.member(actor.player_id) is None:
            return self.protocol.message(
                "guildpb.GetGuildJianghuGrudgeWallResponse",
                error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
            )
        gid = guild.guild_id
        async with self._lock:
            rivals = sorted(
                self._jianghu_rivals.get(gid, {}).values(),
                key=lambda s: (s.last_battle_ms, s.rival_guild_id),
                reverse=True,
            )[:MAX_GUILD_JIANGHU_WALL_RECORDS]
            records = list(self._jianghu_records.get(gid, ()))[:MAX_GUILD_JIANGHU_WALL_RECORDS]

        resp = self.protocol.message("guildpb.GetGuildJianghuGrudgeWallResponse", guild_id=gid)
        for s in rivals:
            resp.rivals.add(
                rival_guild_id=s.rival_guild_id,
                rival_guild_name=s.rival_guild_name,
                wins=s.wins,
                losses=s.losses,
                draws=s.draws,
                last_battle_ms=s.last_battle_ms,
            )
        for r in records:
            resp.recent_records.add(
                battle_id=r.battle_id,
                rival_guild_id=r.rival_guild_id,
                rival_guild_name=r.rival_guild_name,
                outcome_for_guild=r.outcome_for_guild,
                writ_tier=r.writ_tier,
                team_size=r.team_size,
                total_rounds=r.total_rounds,
                our_captain_name=r.our_captain_name,
                rival_captain_name=r.rival_captain_name,
                finished_ms=r.finished_ms,
            )
        return resp

    # -- 帮战入账分流、追杀令到期退本、终态清理与待入账转换（r2-gw#2 / #5 / #6 / #7）-----------

    def _encode_silver_bundle(self, silver: int) -> bytes:
        bundle = self.protocol.message("AssetBundle")
        bundle.currencies.add(currency_type=0, amount=int(silver))
        return bundle.SerializeToString()

    async def credit_war_asset(
        self,
        *,
        player_id: int,
        guild_id: int,
        silver: int,
        kind: int = OP_KIND_WAR_REWARD,
        tx_type: int = TX_GUILD_WAR_REWARD,
        ref_id: int = 0,
        period_key: int = 0,
        source_id: int = 0,
        now_ms: int | None = None,
        op_id: int | None = None,
    ) -> tuple[object | None, WarCreditOwed | None, int]:
        """将单笔帮战银两奖励/退本经 GUILD_CREDIT 分流落库（满窗或护栏冻结转入 guild_war_credit_owed，墓碑放弃）。"""
        if player_id <= 0 or silver <= 0:
            return None, None, CreditFenceVerdict.OPEN
        credit_fn = getattr(self.store, "credit_or_owe_war_op", None)
        if not callable(credit_fn):
            return None, None, CreditFenceVerdict.OPEN
        ts = self._now() if now_ms is None else int(now_ms)
        minted_op_id = int(op_id) if op_id else int(await self.store._op_ids())
        rec = WarCreditOwed(
            player_id=player_id,
            op_id=minted_op_id,
            guild_id=guild_id,
            kind=kind,
            tx_type=tx_type,
            ref_id=ref_id,
            period_key=period_key,
            payload=self._encode_silver_bundle(silver),
            source_id=source_id,
            created_ms=ts,
        )
        op_row, owed_row, verdict = await credit_fn(rec, ts)
        if owed_row is not None:
            logger.info(
                "Guild war credit op %d of player %d deferred to guild_war_credit_owed (source=%d verdict=%d)",
                owed_row.op_id, owed_row.player_id, source_id, verdict,
            )
        elif verdict == CreditFenceVerdict.REMOVED:
            logger.error(
                "Guild war credit for removed player %d abandoned (source=%d op_id=%d)",
                player_id, source_id, minted_op_id,
            )
        return op_row, owed_row, verdict

    async def expire_due_bounties(self, now_ms: int | None = None, *, limit: int = 200) -> dict[str, int]:
        """r2-gw#2：到期 ACTIVE 追杀令原路退本（公费退帮会资金；银两档经 GUILD_CREDIT 分流退发布者）。"""
        ts = self._now() if now_ms is None else int(now_ms)
        due: list[_BountyState] = []
        async with self._lock:
            candidates = [
                b for b in self._bounties.values()
                if b.status == BOUNTY_STATUS_ACTIVE and b.expires_at_ms <= ts
            ]
            candidates.sort(key=lambda b: (b.expires_at_ms, b.bounty_id))
            for b in candidates[:max(0, limit)]:
                b.status = BOUNTY_STATUS_EXPIRED
                b.resolved_at_ms = ts
                due.append(b)

        stats = {
            "expired": 0,
            "funds_refunded": 0,
            "ops_created": 0,
            "owed_count": 0,
            "abandoned_count": 0,
        }
        for b in due:
            stats["expired"] += 1
            silver, funds = bounty_expiry_refund(b)
            if funds > 0:
                await self._credit_guild_funds(b.issuer_guild_id, funds)
                stats["funds_refunded"] += funds
            if silver > 0:
                op_row, owed_row, verdict = await self.credit_war_asset(
                    player_id=b.issuer_player_id,
                    guild_id=b.issuer_guild_id,
                    silver=silver,
                    kind=OP_KIND_BOUNTY_PAYOUT,
                    tx_type=TX_GUILD_BOUNTY_PAYOUT,
                    ref_id=b.tier,
                    source_id=b.bounty_id,
                    now_ms=ts,
                )
                if op_row is not None:
                    stats["ops_created"] += 1
                elif owed_row is not None:
                    stats["owed_count"] += 1
                elif verdict == CreditFenceVerdict.REMOVED:
                    stats["abandoned_count"] += 1
        return stats

    async def purge_terminal_bounties(
        self,
        now_ms: int | None = None,
        *,
        retention_ms: int = WAR_BOUNTY_TERMINAL_RETENTION_MS,
        limit: int = 200,
    ) -> int:
        """r2-gw#7：按保留期批量清理终态追杀令（永不删除 ESCROW_PENDING / ACTIVE）。"""
        ts = self._now() if now_ms is None else int(now_ms)
        if limit <= 0 or ts <= retention_ms:
            return 0
        cutoff = ts - retention_ms
        async with self._lock:
            victim_ids = [
                b.bounty_id
                for b in self._bounties.values()
                if b.status in _TERMINAL_BOUNTY_STATUSES and b.expires_at_ms < cutoff
            ][:limit]
            for bid in victim_ids:
                self._bounties.pop(bid, None)
        if victim_ids:
            logger.info("Purged %d terminal bounties expired before %d", len(victim_ids), cutoff)
        return len(victim_ids)

    async def convert_war_credit_owed_round(
        self,
        after: WarCreditOwedCursor = WarCreditOwedCursor(),
        now_ms: int | None = None,
        *,
        page_size: int = 64,
        max_rows: int = 256,
    ) -> tuple[WarCreditOwedCursor, WarCreditOwedStats]:
        """r2-gw#5 / #6：按游标翻页将 guild_war_credit_owed 转换为 GUILD_CREDIT 资产指令。"""
        next_cursor = after
        stats = WarCreditOwedStats()
        list_fn = getattr(self.store, "list_war_credit_owed", None)
        convert_fn = getattr(self.store, "convert_war_credit_owed", None)
        if not callable(list_fn) or not callable(convert_fn):
            return next_cursor, stats
        ts = self._now() if now_ms is None else int(now_ms)
        waiting: set[int] = set()
        while stats.scanned < max_rows:
            page = await list_fn(next_cursor, page_size)
            for owed in page:
                stats.scanned += 1
                if owed.player_id not in waiting:
                    try:
                        status, _ = await convert_fn(owed, ts)
                    except Exception:
                        stats.failed += 1
                        logger.error(
                            "Convert owed war credit failed op_id=%d player_id=%d",
                            owed.op_id, owed.player_id, exc_info=True,
                        )
                    else:
                        if status == WarOwedConvertStatus.CONVERTED:
                            stats.converted += 1
                        elif status == WarOwedConvertStatus.STILL_FULL:
                            stats.still_full += 1
                            waiting.add(owed.player_id)
                        elif status == WarOwedConvertStatus.FROZEN:
                            stats.frozen += 1
                            waiting.add(owed.player_id)
                        elif status == WarOwedConvertStatus.ABANDONED:
                            stats.abandoned += 1
                            logger.error(
                                "Owed war credit op_id=%d source=%d abandoned: player %d removed",
                                owed.op_id, owed.source_id, owed.player_id,
                            )
                        elif status == WarOwedConvertStatus.GONE:
                            stats.gone += 1
                next_cursor = owed.cursor()
            if len(page) < page_size:
                next_cursor = WarCreditOwedCursor()
                break
        return next_cursor, stats
