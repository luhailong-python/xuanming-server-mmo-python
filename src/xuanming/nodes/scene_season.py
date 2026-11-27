"""赛季技能与单槽赛季印（方案 A）系统，写操作随角色存档提交。"""

from __future__ import annotations

import logging
import time

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import INVALID, UNAVAILABLE, balance, clone
from .battle_dropin_effects import (
    DEFAULT_FAIR_CHAR_PRESET_ID,
    DEFAULT_FAIR_PET_PRESET_ID,
    MAX_SAVED_BUILD_BOOKMARKS,
    REALM_BRANCH_ELITE,
    REALM_BRANCH_SPECIAL,
    REALM_EVENT_ENCOUNTER,
    REALM_EVENT_NONE,
    REALM_EVENT_REST,
    REALM_NODE_ELITE,
    REALM_NODE_EVENT,
    REALM_NODE_NEED_BRANCH,
    REALM_NODE_NORMAL,
    REALM_NODE_REST,
    WUXING_REALM_V2_MAX_NODES,
    WUXING_REALM_V2_VERSION,
    WUXING_TOWER_MAX_FLOORS,
    SeasonBattleRules,
    compute_active_resonance_element,
    dominant_boon_element,
    is_realm_combat_node_kind,
    is_valid_fair_char_preset_id,
    is_valid_fair_pet_preset_id,
    is_valid_formation_id,
    is_valid_tower_or_boon_id,
    next_upgrade_boon_id,
    realm_node_default_kind,
    realm_node_dungeon_id,
    roll_realm_boon_offer,
    roll_realm_encounter_boon,
    roll_tower_sigil_offer,
    tower_floor_dungeon_id,
    tower_floor_leyline_element,
    wuxing_week_key,
)
from .scene_mission import effective_level, now_ms
from .scene_stats import rank_limit

logger = logging.getLogger(__name__)

_SERVICE = "SceneSeasonClientPlayer"
_CLIENT_REQUEST_KIND_CATCHUP = 9
_MAX_UINT64 = (1 << 64) - 1
_MAX_CACHED_RECEIPTS = 256
_MAX_BOOKMARK_TITLE_BYTES = 48
_MAX_BOOKMARK_SKILLS = 12
_MAX_BOOKMARK_SIGILS = 10
_MAX_TOWER_ACTIVE_SIGILS = 9
_TOWER_LEADERBOARD_TOP_N = 20
_FEATURE_UNAVAILABLE = 1006

# 赛季规则硬安全边界（与 C++ season_rules.h 保持一致）。
HARD_STAT_CAP_BPS = 500
HARD_MAX_PROGRESS_POINTS = 1000
HARD_MAX_CATCHUP_PRICE_PER_POINT = 1_000_000_000
MAX_VALID_CURRENCY_TYPE = 3
MIN_CYCLE_DURATION_DAYS = 7
MAX_CYCLE_DURATION_DAYS = 180
_MS_PER_DAY = 24 * 3600 * 1000

# 赛季错误码（与 C++ season_error_tip.pb.h 严格对齐）。
SEASON_NOT_OPEN = 34000
SEASON_MISMATCH = 34001
SEASON_SEAL_INVALID = 34002
SEASON_SEAL_ALREADY_EQUIPPED = 34003
SEASON_SEAL_ALREADY_MAX = 34004
SEASON_CATCHUP_DISABLED = 34005
SEASON_CATCHUP_PRICE_CHANGED = 34006
SEASON_CATCHUP_CURRENCY_NOT_ENOUGH = 34007
SEASON_FROZEN = 34008
SEASON_IN_BATTLE = 34009
SEASON_SKILL_NO_UPGRADE = 34010
SEASON_UNAVAILABLE = 34011
SEASON_HONOR_ALREADY_UNLOCKED = 34012
SEASON_HONOR_TARGET_INVALID = 34013

# 严格对齐 proto MatchMode 枚举值：PVE_SOLO=4, PVE_TEAM=5; PVP: 5V5=1, 1V1=3, PVP_TEAM=7。
_PVE_MODES = frozenset({4, 5})
_PVP_MODES = frozenset({1, 3, 7})

# 正式排位败局/平局防挂机门槛（对齐 C++ PlayerEquipmentMaterialRewardSystem::kPvpMin*）。
PVP_MIN_ROUNDS_FOR_LOSS = 3
PVP_MIN_MANUAL_ROUNDS_FOR_LOSS = 2


def is_eligible_battle(settlement) -> bool:
    """按 C++ PlayerSeasonSystem::IsEligibleBattle 判定本场结算是否可累计赛季印进度点。"""
    boss_kind = int(getattr(settlement, "boss_trial_kind", 0))
    if (
        not settlement.battle_id
        or not settlement.player_id
        or settlement.fled
        or settlement.total_rounds <= 0
        or settlement.player_team_index > 1
        or (9 <= boss_kind <= 11)
        or settlement.outcome not in (1, 2, 3)
    ):
        return False
    player_won = (
        (settlement.outcome == 1 and settlement.player_team_index == 0)
        or (settlement.outcome == 2 and settlement.player_team_index == 1)
    )
    mode = int(settlement.match_mode)
    if mode in (4, 5):  # MATCH_MODE_PVE_SOLO, MATCH_MODE_PVE_TEAM
        return player_won
    if mode in (1, 3, 7):  # MATCH_MODE_5V5, MATCH_MODE_1V1, MATCH_MODE_PVP_TEAM
        if not bool(getattr(settlement, "competitive_ranked", False)):
            return True
        if not bool(getattr(settlement, "participated", False)):
            return False
        if player_won:
            return True
        return (
            settlement.total_rounds >= PVP_MIN_ROUNDS_FOR_LOSS
            and int(getattr(settlement, "manual_action_rounds", 0)) >= PVP_MIN_MANUAL_ROUNDS_FOR_LOSS
            and bool(getattr(settlement, "effective_combat_contribution", False))
        )
    return False


def valid_artifact_definition(
    effect_id: int,
    bonus_bps: int,
    cap_bps: int,
    per_round_limit: int,
    battle_limit: int,
    active_rounds: int,
    health_threshold_bps: int,
) -> bool:
    """校验法宝/赛季印战斗机制参数是否合法（对齐 C++ equipbattle::ValidArtifactDefinition）。"""
    if (
        not 1 <= effect_id <= 8
        or not 1 <= bonus_bps <= 10_000
        or cap_bps > 10_000
        or health_threshold_bps > 10_000
        or per_round_limit > 1
        or battle_limit > 3
        or active_rounds > 2
    ):
        return False
    if effect_id != 6 and per_round_limit == 0:
        return False
    if (effect_id == 3 or effect_id >= 6) and battle_limit == 0:
        return False
    if effect_id == 6 and (cap_bps == 0 or active_rounds == 0):
        return False
    return effect_id != 4 or health_threshold_bps != 0


def is_window_active(starts_at_ms: int, ends_at_ms: int, enabled: bool, now_ms_val: int) -> bool:
    """判断给定赛季时间窗在当前时刻是否开启。"""
    if not enabled:
        return False
    if ends_at_ms and ends_at_ms <= starts_at_ms:
        return False
    if now_ms_val < starts_at_ms:
        return False
    return ends_at_ms == 0 or now_ms_val < ends_at_ms


def windows_overlap(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    """判断两个半开赛季窗口是否存在时间重叠。"""
    a_before_b = end_a != 0 and end_a <= start_b
    b_before_a = end_b != 0 and end_b <= start_a
    return not a_before_b and not b_before_a


def validate_rule(row) -> str | None:
    """校验单条 SeasonRule 配置行，返回错误说明或 None。"""
    if not row.id:
        return "season_id must be non-zero"
    if row.ends_at_ms and row.ends_at_ms <= row.starts_at_ms:
        return "ends_at_ms must be 0 or greater than starts_at_ms"
    if not 1 <= row.seal_max_points <= HARD_MAX_PROGRESS_POINTS:
        return "seal_max_points out of range [1, 1000]"
    if row.max_health_bps_cap > HARD_STAT_CAP_BPS or row.defense_bps_cap > HARD_STAT_CAP_BPS:
        return "season stat cap exceeds hard limit 500 bps"
    if row.catchup_currency_type > MAX_VALID_CURRENCY_TYPE:
        return "catchup_currency_type invalid"
    if row.catchup_price_per_point > HARD_MAX_CATCHUP_PRICE_PER_POINT:
        return "catchup_price_per_point exceeds limit"
    if not row.default_seal_id:
        return "default_seal_id must be non-zero"
    cycle_days = int(getattr(row, "cycle_duration_days", 0))
    if cycle_days != 0 and not (MIN_CYCLE_DURATION_DAYS <= cycle_days <= MAX_CYCLE_DURATION_DAYS):
        return "cycle_duration_days must be 0 or in [7, 180]"
    return None


def valid_seal_row(seal) -> bool:
    """校验单条 SeasonSeal 配置行是否合法（含禁控印与表驱动共鸣硬上限）。"""
    return SeasonBattleRules.valid_seal_definition(
        int(seal.season_id),
        int(seal.id),
        int(seal.effect_id),
        int(seal.bonus_bps),
        int(seal.cap_bps),
        int(seal.per_round_limit),
        int(seal.battle_limit),
        int(seal.active_rounds),
        int(seal.health_threshold_bps),
        int(getattr(seal, "resonance_skill_id", 0)),
        int(getattr(seal, "resonance_kind", 0)),
        int(getattr(seal, "resonance_param", 0)),
    )


def validate_schedule(rules) -> str | None:
    """校验整张 SeasonRule 排期（固定单季窗 vs 无限期号自动循环）。"""
    any_cycle = False
    for r in rules:
        err = validate_rule(r)
        if err is not None:
            return err
        if int(getattr(r, "cycle_duration_days", 0)) != 0:
            any_cycle = True
    if not any_cycle:
        enabled = [r for r in rules if r.enabled]
        for i, first in enumerate(enabled):
            for second in enabled[i + 1:]:
                if windows_overlap(first.starts_at_ms, first.ends_at_ms, second.starts_at_ms, second.ends_at_ms):
                    return "overlapping enabled seasons in fixed-window mode"
        return None
    pool = []
    shared_duration = 0
    for r in rules:
        order = int(getattr(r, "cycle_order", 0))
        days = int(getattr(r, "cycle_duration_days", 0))
        if order == 0:
            if r.enabled or days != 0:
                return "non-pool rule cannot be enabled or set cycle_duration_days in cycle mode"
            continue
        if not r.enabled:
            return "cycle pool rule must be enabled"
        if not (MIN_CYCLE_DURATION_DAYS <= days <= MAX_CYCLE_DURATION_DAYS):
            return "cycle pool rule cycle_duration_days out of range [7, 180]"
        if shared_duration == 0:
            shared_duration = days
        elif shared_duration != days:
            return "cycle pool rules must share identical cycle_duration_days"
        pool.append(r)
    if not pool:
        return "cycle mode requires at least one rule with cycle_order > 0"
    pool.sort(key=lambda r: int(getattr(r, "cycle_order", 0)))
    for i, r in enumerate(pool):
        if int(getattr(r, "cycle_order", 0)) != i + 1:
            return "cycle_order must form contiguous 1..N sequence without duplicates"
    if not int(pool[0].starts_at_ms):
        return "cycle mode requires pool[0].starts_at_ms > 0 as anchor timestamp"
    return None


def resolve_active_season(tables, now_ms_val: int):
    """解析当前时刻活跃赛季：(rule, season_term, cycle_index, cycle_pool_size, term_start_ms, term_end_ms) 或 None。"""
    if "SeasonRule" not in tables.tables:
        return None
    rules = list(tables["SeasonRule"].rows())
    if not rules or validate_schedule(rules) is not None:
        return None
    any_cycle = any(int(getattr(r, "cycle_duration_days", 0)) != 0 for r in rules)
    if not any_cycle:
        for r in rules:
            if is_window_active(r.starts_at_ms, r.ends_at_ms, bool(r.enabled), now_ms_val):
                if find_valid_seal(tables, r.id, r.default_seal_id) is None:
                    return None
                return (r, 1, 1, 0, int(r.starts_at_ms), int(r.ends_at_ms))
        return None
    pool = sorted(
        (r for r in rules if int(getattr(r, "cycle_order", 0)) != 0),
        key=lambda r: int(getattr(r, "cycle_order", 0)),
    )
    anchor_ms = int(pool[0].starts_at_ms)
    if now_ms_val < anchor_ms:
        return None
    period_ms = int(getattr(pool[0], "cycle_duration_days", 0)) * _MS_PER_DAY
    elapsed_idx = (now_ms_val - anchor_ms) // period_ms
    pool_size = len(pool)
    picked = pool[elapsed_idx % pool_size]
    if find_valid_seal(tables, picked.id, picked.default_seal_id) is None:
        return None
    term = min(0xFFFF_FFFF, elapsed_idx + 1)
    cycle_idx = min(0xFFFF_FFFF, (elapsed_idx // pool_size) + 1)
    term_start = anchor_ms + elapsed_idx * period_ms
    term_end = term_start + period_ms
    return (picked, int(term), int(cycle_idx), pool_size, int(term_start), int(term_end))


def is_honor_bit_set(words, season_id: int) -> bool:
    if season_id <= 0:
        return False
    word_idx, bit = divmod(season_id - 1, 64)
    if word_idx >= len(words):
        return False
    return bool(int(words[word_idx]) & (1 << bit))


def set_honor_bit(words, season_id: int) -> bool:
    if season_id <= 0:
        return False
    word_idx, bit = divmod(season_id - 1, 64)
    while len(words) <= word_idx:
        words.append(0)
    mask = 1 << bit
    prev = int(words[word_idx])
    newly = (prev & mask) == 0
    words[word_idx] = prev | mask
    return newly


def is_season_honor_unlocked(tables, comp, season_id: int) -> bool:
    if season_id <= 0:
        return False
    if is_honor_bit_set(comp.unlocked_honor_words, season_id):
        return True
    if "SeasonRule" not in tables.tables:
        return False
    rule = tables["SeasonRule"].get(season_id)
    if rule is None or not rule.seal_max_points:
        return False
    rec = _find_record(comp, season_id)
    return rec is not None and int(rec.progress_points) >= int(rule.seal_max_points)


def sync_legacy_honors_to_bitmap(tables, comp) -> None:
    if "SeasonRule" not in tables.tables:
        return
    for rec in comp.records:
        sid = int(rec.season_id)
        if not sid:
            continue
        rule = tables["SeasonRule"].get(sid)
        if rule is not None and rule.seal_max_points and int(rec.progress_points) >= int(rule.seal_max_points):
            set_honor_bit(comp.unlocked_honor_words, sid)
            comp.retro_honor_progress.pop(sid, None)
            if int(comp.retro_honor_target_season_id) == sid:
                comp.retro_honor_target_season_id = 0


def read_effective_term_progress(rec, current_term: int, max_points: int) -> tuple[int, int, int]:
    if rec is None or max_points <= 0:
        return 0, 0, 0
    last_term = int(getattr(rec, "last_term", 0))
    if not (last_term == current_term or (last_term == 0 and current_term == 1)):
        return 0, 0, 0
    progress = min(int(rec.progress_points), max_points)
    battle_pts = min(int(rec.battle_points), progress)
    catchup_pts = min(int(rec.catchup_points), progress)
    return progress, battle_pts, catchup_pts


def sync_record_for_term(tables, comp, rule, current_term: int):
    sync_legacy_honors_to_bitmap(tables, comp)
    record = _find_or_add_record(comp, rule.id)
    last_term = int(getattr(record, "last_term", 0))
    if last_term == 0 and current_term == 1:
        record.last_term = 1
    elif last_term != current_term:
        record.progress_points = 0
        record.battle_points = 0
        record.catchup_points = 0
        record.last_term = current_term
    return record


def all_configured_season_ids_asc(tables) -> list[int]:
    if "SeasonRule" not in tables.tables:
        return []
    return sorted({int(r.id) for r in tables["SeasonRule"].rows() if r.id and r.seal_max_points})


def resolve_retro_honor_target(tables, comp, active_season_id: int) -> tuple[int, bool]:
    explicit_id = int(getattr(comp, "retro_honor_target_season_id", 0))
    if explicit_id and explicit_id != active_season_id and "SeasonRule" in tables.tables:
        rule = tables["SeasonRule"].get(explicit_id)
        if rule is not None and rule.seal_max_points and not is_season_honor_unlocked(tables, comp, explicit_id):
            return explicit_id, True
    for sid in all_configured_season_ids_asc(tables):
        if sid != active_season_id and not is_season_honor_unlocked(tables, comp, sid):
            return sid, False
    return 0, False


def scaled_bonus_bps(progress_points: int, max_points: int, cap_bps: int) -> int:
    """按进度点线性映射轻属性万分比，向下取整并受硬上限约束。"""
    if not max_points or not cap_bps or not progress_points:
        return 0
    capped_cap = min(cap_bps, HARD_STAT_CAP_BPS)
    clamped_points = min(progress_points, max_points)
    return (capped_cap * clamped_points) // max_points


def compute_catchup_plan(
    current_points: int,
    max_points: int,
    requested_points: int,
    price_per_point: int,
) -> tuple[int, int] | None:
    """计算追赶点数与总价；requested_points=0 表示补满剩余点数。"""
    if not max_points or current_points >= max_points or not price_per_point:
        return None
    remaining = max_points - current_points
    buy = remaining if requested_points == 0 else requested_points
    if not 1 <= buy <= remaining:
        return None
    total_price = price_per_point * buy
    if total_price > _MAX_UINT64 or total_price >= (1 << 63):
        return None
    return buy, total_price


def _find_record(comp, season_id: int):
    return next((rec for rec in comp.records if rec.season_id == season_id), None)


def _find_or_add_record(comp, season_id: int):
    existing = _find_record(comp, season_id)
    if existing is not None:
        return existing
    return comp.records.add(season_id=season_id)


def find_valid_seal(tables, season_id: int, seal_id: int):
    """按 (season_id, seal_id) 查找有效赛季印行。"""
    if not season_id or not seal_id or "SeasonSeal" not in tables.tables:
        return None
    row = tables["SeasonSeal"].get(seal_id)
    if row is None or row.season_id != season_id or not valid_seal_row(row):
        return None
    return row


def find_active_season_rule(tables, now_ms_val: int):
    """查找当前时刻唯一活跃且合法的 SeasonRule；重叠或配置非法时 fail-closed 返回 None。"""
    active = resolve_active_season(tables, now_ms_val)
    return active[0] if active is not None else None


def _truncate_utf8(text: str, max_bytes: int) -> str:
    """按 UTF-8 字节上限安全截断字符串，避免截断多字节字符。"""
    cleaned = text.strip()
    raw = cleaned.encode("utf-8")
    if len(raw) <= max_bytes:
        return cleaned
    return raw[:max_bytes].decode("utf-8", errors="ignore").strip()


def _copy_bookmark_to_view(src, dst) -> None:
    """将持久化配装收藏条目投影至客户端视图消息。"""
    dst.bookmark_id = src.bookmark_id
    dst.title = src.title
    dst.source_battle_id = src.source_battle_id
    dst.source_player_id = src.source_player_id
    dst.source_player_name = src.source_player_name
    dst.class_id = src.class_id
    dst.formation_id = src.formation_id
    dst.season_seal_id = src.season_seal_id
    dst.active_pet_table_id = src.active_pet_table_id
    dst.active_pet_name = src.active_pet_name
    dst.skill_table_ids.extend(src.skill_table_ids)
    dst.wuxing_sigil_ids.extend(src.wuxing_sigil_ids)
    dst.saved_at_ms = src.saved_at_ms


# 赛季全员对称周环境词缀常量（对齐 C++ season_rules.h）。
WEEKLY_AFFIX_NONE = 0
WEEKLY_AFFIX_SHIELD_OVERLOAD = 1
WEEKLY_AFFIX_SPIRIT_RECOVERY = 2
WEEKLY_AFFIX_OPENING_GUARD = 3
WEEKLY_AFFIX_SHARP_TEMPO = 4
WEEKLY_AFFIX_COUNT = 4
_MS_PER_WEEK = 7 * 24 * 3600 * 1000
_MONDAY_05_CST_OFFSET_MS = (3 * 24 - 3) * 3600 * 1000
_WEEKLY_AFFIX_TEXT = {
    WEEKLY_AFFIX_SHIELD_OVERLOAD: (
        "护盾过载周",
        "全员对称：本季常规对局中双方技能施加的护盾量额外 +15%。",
        "推荐搭配破盾赛季印与破甲/护盾技能抢占节奏。",
    ),
    WEEKLY_AFFIX_SPIRIT_RECOVERY: (
        "灵息充盈周",
        "全员对称：本季常规对局中双方执行普通防御额外回复 3% 最大法力。",
        "推荐搭配聚息援印或固阵守印打稳守蓄蓝反击。",
    ),
    WEEKLY_AFFIX_OPENING_GUARD: (
        "先机守势周",
        "全员对称：本季常规对局前 2 回合双方受到的直伤降低 8%。",
        "前两回合爆发更克制，推荐蓄势增益或固阵守印后发制人。",
    ),
    WEEKLY_AFFIX_SHARP_TEMPO: (
        "锋芒竞速周",
        "全员对称：本季常规对局中攻击无护盾目标的直伤提升 5%。",
        "先破盾再集火收益更高，推荐破阵锐印/玄印联动群攻技。",
    ),
}


def compute_season_week_index(starts_at_ms: int, now_ms_val: int) -> int:
    """计算当前权威赛季周次（1 起）。"""
    if starts_at_ms > 0:
        if now_ms_val <= starts_at_ms:
            return 1
        return ((now_ms_val - starts_at_ms) // _MS_PER_WEEK) + 1
    return ((now_ms_val + _MONDAY_05_CST_OFFSET_MS) // _MS_PER_WEEK) + 1


def weekly_affix_for_week(season_id: int, week_index: int) -> int:
    """由赛季编号与周次确定性轮换全员对称周词缀（1..4）；任一为 0 返回 0。"""
    if not season_id or not week_index:
        return WEEKLY_AFFIX_NONE
    return ((season_id - 1 + week_index - 1) % WEEKLY_AFFIX_COUNT) + 1


def is_seal_effect_recommended_for_affix(affix_id: int, effect_id: int) -> bool:
    if affix_id == WEEKLY_AFFIX_SHIELD_OVERLOAD:
        return effect_id in (1, 2, 5)
    if affix_id == WEEKLY_AFFIX_SPIRIT_RECOVERY:
        return effect_id in (7, 6, 4)
    if affix_id == WEEKLY_AFFIX_OPENING_GUARD:
        return effect_id in (6, 7, 5)
    if affix_id == WEEKLY_AFFIX_SHARP_TEMPO:
        return effect_id in (1, 2, 4)
    return False


def _rotate_tower_weekly_stats_if_needed(tower, week_key: int) -> bool:
    """跨周仅重置周统计位图与周最佳，绝不清空跨周在途轮次（对齐 C++ RotateTowerWeeklyStatsIfNeeded）。"""
    if int(tower.week_key) == week_key:
        return False
    tower.week_key = week_key
    tower.weekly_cleared_mask = 0
    tower.weekly_best_floor = 0
    tower.weekly_best_rounds = 0
    tower.weekly_best_floor_preset = 0
    tower.weekly_best_rounds_preset = 0
    return True


def _effective_run_week_key(tower) -> int:
    """返回本轮开局冻结周键（兼容 V1 旧存档无 run_week_key 时回退 week_key）。"""
    return int(tower.run_week_key) if int(tower.run_week_key) != 0 else int(tower.week_key)


def _compute_leaderboard_score(best_floor: int, best_rounds: int) -> int:
    """与 C++ ComputeLeaderboardScore 同口径的周榜排序分（单调递增，越大越靠前）。"""
    effective_rounds = 999_999 if best_rounds == 0 else min(int(best_rounds), 999_999)
    return int(best_floor) * 1_000_000 - effective_rounds


def realm_leaderboard_zset_key(week_key: int, use_free_preset: bool) -> str:
    """水平扩展周榜 Redis Cluster ZSET 键（带 {week_key} hash-tag 与 self/preset 隔离）。"""
    return f"wuxing:realm:lb:{{{int(week_key)}}}:{'preset' if use_free_preset else 'self'}"


def realm_leaderboard_meta_key(week_key: int, use_free_preset: bool) -> str:
    """水平扩展周榜 Redis Cluster 元信息 HASH 键（与 ZSET 共享 {week_key} hash-tag）。"""
    return f"wuxing:realm:lb_meta:{{{int(week_key)}}}:{'preset' if use_free_preset else 'self'}"


class SceneSeason:
    """Scene 赛季技能与单槽赛季印服务，管理状态快照、切换、快捷追赶、五行阵法/幻境 V2 与配装收藏。"""

    realm_leaderboard_zset_key = staticmethod(realm_leaderboard_zset_key)
    realm_leaderboard_meta_key = staticmethod(realm_leaderboard_meta_key)

    def __init__(self, scene, tables, assets=None, *, clock=now_ms) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        tx_types = self.protocol.pool.FindEnumTypeByName("TransactionType")
        self._debit_tx = tx_types.values_by_name["TX_CURRENCY_DEDUCT"].number
        self._catchup_receipts: dict[tuple[int, int], tuple[int, int]] = {}
        # 五行幻境周最速双榜内存快照：(week_key, use_free_preset) -> {player_id: (best_floor, best_rounds, player_name, formation_id, resonance_element, sigil_ids)}
        self._tower_leaderboards: dict[tuple[int, bool], dict[int, tuple[int, int, str, int, int, tuple[int, ...]]]] = {}

    def register(self) -> None:
        """向 SceneNode 注册赛季及五行玩法扩展客户端 RPC（含 515..521、569、585..586）。"""
        self.scene.register(_SERVICE, "GetSeasonState", self.get_state)
        self.scene.register(_SERVICE, "EquipSeasonSeal", self.equip_seal, mutating=True)
        self.scene.register(_SERVICE, "CatchupSeasonSeal", self.catchup, mutating=True)
        self.scene.register(_SERVICE, "EquipWuxingFormation", self.equip_wuxing_formation, mutating=True)
        self.scene.register(_SERVICE, "GetWuxingTowerState", self.get_wuxing_tower_state)
        self.scene.register(_SERVICE, "StartWuxingTowerRun", self.start_wuxing_tower_run, mutating=True)
        self.scene.register(_SERVICE, "PickWuxingTowerSigil", self.pick_wuxing_tower_sigil, mutating=True)
        self.scene.register(_SERVICE, "AbandonWuxingTowerRun", self.abandon_wuxing_tower_run, mutating=True)
        self.scene.register(_SERVICE, "SaveBattleBuildBookmark", self.save_battle_build_bookmark, mutating=True)
        self.scene.register(_SERVICE, "DeleteBattleBuildBookmark", self.delete_battle_build_bookmark, mutating=True)
        self.scene.register(_SERVICE, "SetRetroHonorTarget", self.set_retro_honor_target, mutating=True)
        self.scene.register(_SERVICE, "ChooseWuxingRealmBranch", self.choose_wuxing_realm_branch, mutating=True)
        self.scene.register(_SERVICE, "ResolveWuxingRealmEvent", self.resolve_wuxing_realm_event, mutating=True)

    def validate_tables(self) -> bool:
        """启动前校验 SeasonRule 与 SeasonSeal 配表一致性（含无限循环与共鸣校验）。"""
        rules = list(self.tables["SeasonRule"].rows()) if "SeasonRule" in self.tables.tables else []
        seals = list(self.tables["SeasonSeal"].rows()) if "SeasonSeal" in self.tables.tables else []
        if not rules:
            return not seals
        if validate_schedule(rules) is not None:
            return False
        rule_ids = {int(r.id) for r in rules}
        for rule in rules:
            if find_valid_seal(self.tables, rule.id, rule.default_seal_id) is None:
                return False
        skill_table = self.tables["Skill"] if "Skill" in self.tables.tables else None
        for seal in seals:
            if int(seal.season_id) not in rule_ids or not valid_seal_row(seal):
                return False
            res_kind = int(getattr(seal, "resonance_kind", 0))
            if res_kind != SeasonBattleRules.RESONANCE_NONE and skill_table is not None:
                linked = skill_table.get(int(getattr(seal, "resonance_skill_id", 0)))
                if linked is None or int(getattr(linked, "season_id", 0)) != int(seal.season_id):
                    return False
        return True

    def initialize_on_load(self, data) -> None:
        """角色载入时确保 season_component 存在，不删除历史赛季归档。"""
        data.season_component.SetInParent()

    def active_season_id(self, now_ms_val: int | None = None) -> int:
        """返回当前活跃赛季 ID，未开启或配置冲突返回 0。"""
        stamp = self.clock() if now_ms_val is None else now_ms_val
        rule = find_active_season_rule(self.tables, stamp)
        return rule.id if rule is not None else 0

    def _effective_seal_id(self, rule, record) -> int:
        if record is not None and record.equipped_seal_id:
            if find_valid_seal(self.tables, rule.id, record.equipped_seal_id) is not None:
                return record.equipped_seal_id
        return rule.default_seal_id

    def _writable_error(self, player) -> int:
        if not self.scene.mutable(player):
            return SEASON_FROZEN
        battle = getattr(self.scene, "battle", None)
        if battle is not None and battle.frozen(player.player_id):
            return SEASON_IN_BATTLE
        return 0

    def record_battle_progress_delta(
        self,
        data,
        *,
        match_mode: int,
        player_won: bool,
        fled: bool = False,
        total_rounds: int = 1,
        now_ms_val: int | None = None,
        competitive_ranked: bool = False,
        participated: bool = True,
        manual_action_rounds: int = 0,
        effective_combat_contribution: bool = False,
    ) -> dict:
        """按权威战斗结果累计当季赛季印进度点与满级后免费往季徽章追忆，返回增量摘要。"""
        empty = {
            "season_id": 0,
            "points_added": 0,
            "progress_after": 0,
            "max_points": 0,
            "honor_newly_unlocked": False,
            "retro_honor_season_id": 0,
            "retro_honor_points_added": 0,
            "retro_honor_progress_after": 0,
            "retro_honor_max_points": 0,
            "retro_honor_newly_unlocked": False,
        }
        if fled or total_rounds <= 0:
            return empty
        if competitive_ranked:
            if not participated:
                return empty
            if not player_won and (
                total_rounds < PVP_MIN_ROUNDS_FOR_LOSS
                or manual_action_rounds < PVP_MIN_MANUAL_ROUNDS_FOR_LOSS
                or not effective_combat_contribution
            ):
                return empty
        stamp = self.clock() if now_ms_val is None else now_ms_val
        active = resolve_active_season(self.tables, stamp)
        if active is None:
            return empty
        rule, season_term, _cycle_idx, _pool_size, _term_start, _term_end = active
        if match_mode in _PVE_MODES:
            gain = rule.pve_win_points if player_won else 0
        elif match_mode in _PVP_MODES:
            gain = rule.pvp_win_points if player_won else rule.pvp_other_points
        else:
            gain = 0
        if gain <= 0:
            return empty

        comp = data.season_component
        record = sync_record_for_term(self.tables, comp, rule, season_term)
        if not record.equipped_seal_id:
            record.equipped_seal_id = rule.default_seal_id
        max_points = int(rule.seal_max_points)
        before = min(int(record.progress_points), max_points)
        was_already_unlocked = is_season_honor_unlocked(self.tables, comp, rule.id)
        used = min(gain, max_points - before) if before < max_points else 0
        after = before + used
        current_newly_unlocked = False
        if used > 0:
            record.progress_points = after
            record.battle_points += used
            if after >= max_points:
                newly_set = set_honor_bit(comp.unlocked_honor_words, rule.id)
                current_newly_unlocked = newly_set and not was_already_unlocked
                comp.retro_honor_progress.pop(rule.id, None)
                if int(comp.retro_honor_target_season_id) == rule.id:
                    comp.retro_honor_target_season_id = 0

        retro_sid = 0
        retro_added = 0
        retro_after = 0
        retro_max = 0
        retro_unlocked = False
        overflow = gain - used
        if overflow > 0:
            target_sid, _ = resolve_retro_honor_target(self.tables, comp, rule.id)
            if target_sid:
                target_rule = self.tables["SeasonRule"].get(target_sid)
                if target_rule is not None and target_rule.seal_max_points:
                    target_max = int(target_rule.seal_max_points)
                    prev_retro = min(int(comp.retro_honor_progress.get(target_sid, 0)), target_max)
                    add = min(overflow, target_max - prev_retro) if prev_retro < target_max else 0
                    after_retro = prev_retro + add
                    if add > 0:
                        retro_sid = target_sid
                        retro_added = add
                        retro_after = after_retro
                        retro_max = target_max
                        if after_retro >= target_max:
                            retro_unlocked = set_honor_bit(comp.unlocked_honor_words, target_sid)
                            comp.retro_honor_progress.pop(target_sid, None)
                            if int(comp.retro_honor_target_season_id) == target_sid:
                                comp.retro_honor_target_season_id = 0
                        else:
                            comp.retro_honor_progress[target_sid] = after_retro

        return {
            "season_id": int(rule.id),
            "points_added": used,
            "progress_after": after,
            "max_points": max_points,
            "honor_newly_unlocked": current_newly_unlocked,
            "retro_honor_season_id": retro_sid,
            "retro_honor_points_added": retro_added,
            "retro_honor_progress_after": retro_after,
            "retro_honor_max_points": retro_max,
            "retro_honor_newly_unlocked": retro_unlocked,
        }

    def record_battle_progress(
        self,
        data,
        *,
        match_mode: int,
        player_won: bool,
        fled: bool = False,
        total_rounds: int = 1,
        now_ms_val: int | None = None,
        competitive_ranked: bool = False,
        participated: bool = True,
        manual_action_rounds: int = 0,
        effective_combat_contribution: bool = False,
    ) -> int:
        """按权威战斗结果累计当季赛季印进度点，返回实际新增本季点数。"""
        delta = self.record_battle_progress_delta(
            data,
            match_mode=match_mode,
            player_won=player_won,
            fled=fled,
            total_rounds=total_rounds,
            now_ms_val=now_ms_val,
            competitive_ranked=competitive_ranked,
            participated=participated,
            manual_action_rounds=manual_action_rounds,
            effective_combat_contribution=effective_combat_contribution,
        )
        return int(delta["points_added"])

    def collect_effects(self, data, effects, *, now_ms_val: int | None = None) -> None:
        """汇总当前活跃赛季印的轻属性与战斗机制至 EquipmentEffects。"""
        effects.ClearField("season_seal")
        effects.season_max_health_bps = 0
        effects.season_defense_bps = 0
        if not data.HasField("season_component"):
            return
        stamp = self.clock() if now_ms_val is None else now_ms_val
        active = resolve_active_season(self.tables, stamp)
        if active is None:
            return
        rule, season_term, *_ = active
        record = _find_record(data.season_component, rule.id)
        seal_id = self._effective_seal_id(rule, record)
        seal = find_valid_seal(self.tables, rule.id, seal_id)
        if seal is None:
            return
        progress, _, _ = read_effective_term_progress(record, season_term, int(rule.seal_max_points))
        effects.season_max_health_bps = scaled_bonus_bps(progress, rule.seal_max_points, rule.max_health_bps_cap)
        effects.season_defense_bps = scaled_bonus_bps(progress, rule.seal_max_points, rule.defense_bps_cap)
        dst = effects.season_seal
        dst.season_id = rule.id
        dst.seal_id = seal.id
        dst.effect_id = seal.effect_id
        dst.bonus_bps = seal.bonus_bps
        dst.cap_bps = seal.cap_bps
        dst.per_round_limit = seal.per_round_limit
        dst.battle_limit = seal.battle_limit
        dst.active_rounds = seal.active_rounds
        dst.health_threshold_bps = seal.health_threshold_bps
        dst.name = seal.name
        dst.resonance_skill_id = int(getattr(seal, "resonance_skill_id", 0))
        dst.resonance_kind = int(getattr(seal, "resonance_kind", 0))
        dst.resonance_param = int(getattr(seal, "resonance_param", 0))

    def _fill_honors(
        self,
        comp,
        active_season_id: int,
        active_term: int,
        retro_target_season_id: int,
        out,
    ) -> None:
        """填充全量已配置赛季的永久零数值纪念徽章与往季追忆状态。"""
        for sid in all_configured_season_ids_asc(self.tables):
            rule = self.tables["SeasonRule"].get(sid)
            if rule is None or not rule.seal_max_points:
                continue
            max_pts = int(rule.seal_max_points)
            unlocked = is_season_honor_unlocked(self.tables, comp, sid)
            is_current = sid == active_season_id
            retro_pts = 0 if unlocked else min(int(comp.retro_honor_progress.get(sid, 0)), max_pts)
            if unlocked:
                progress = max_pts
            elif is_current:
                progress, _, _ = read_effective_term_progress(_find_record(comp, sid), active_term, max_pts)
            else:
                progress = retro_pts
            out.honors.add(
                season_id=sid,
                season_name=rule.name,
                honor_title_name=getattr(rule, "honor_title_name", ""),
                honor_badge_icon_key=getattr(rule, "honor_badge_icon_key", ""),
                progress_points=progress,
                max_points=max_pts,
                unlocked=unlocked,
                is_current_season=is_current,
                is_retro_target=(not unlocked and sid == retro_target_season_id),
                retro_progress_points=retro_pts,
            )

    @staticmethod
    def _fill_weekly_affix(active, stamp: int, valid_seals: list, out) -> None:
        out.Clear()
        rule, season_term, _cycle_idx, pool_size, term_start_ms, _term_end_ms = active
        affix_seed = season_term if pool_size > 0 else int(rule.id)
        week_idx = compute_season_week_index(int(term_start_ms), stamp)
        affix_id = weekly_affix_for_week(affix_seed, week_idx)
        text = _WEEKLY_AFFIX_TEXT.get(affix_id)
        if text is None:
            return
        out.week_index = week_idx
        out.affix_id = affix_id
        out.name, out.description, out.strategy_tip = text
        for seal in valid_seals:
            if is_seal_effect_recommended_for_affix(affix_id, int(seal.effect_id)):
                out.recommended_seal_ids.append(int(seal.id))

    def _fill_state(self, player, data, stamp: int, out) -> int:
        out.Clear()
        out.server_now_ms = stamp
        if not data.HasField("level_component"):
            return SEASON_UNAVAILABLE
        # 无论当前是否存在进行中的赛季，始终回填玩家已装配五行阵法与公开配装收藏夹。
        comp = data.season_component
        fid = int(comp.equipped_formation_id)
        out.equipped_formation_id = fid if is_valid_formation_id(fid) else 0
        for bookmark in comp.saved_builds:
            _copy_bookmark_to_view(bookmark, out.saved_builds.add())

        active = resolve_active_season(self.tables, stamp)
        active_sid = int(active[0].id) if active is not None else 0
        retro_target_id, retro_explicit = resolve_retro_honor_target(self.tables, comp, active_sid)
        out.retro_honor_target_season_id = retro_target_id
        out.retro_honor_target_explicit = retro_explicit
        if active is None:
            self._fill_honors(comp, 0, 0, retro_target_id, out)
            out.available = False
            out.unavailable_reason = "当前暂无进行中的赛季"
            return 0

        rule, season_term, cycle_index, cycle_pool_size, term_start_ms, term_end_ms = active
        self._fill_honors(comp, rule.id, season_term, retro_target_id, out)
        record = _find_record(data.season_component, rule.id)
        equipped_id = self._effective_seal_id(rule, record)
        progress, battle_pts, catchup_pts = read_effective_term_progress(record, season_term, int(rule.seal_max_points))

        out.available = True
        out.season_id = rule.id
        out.season_name = rule.name
        out.season_desc = rule.desc
        out.starts_at_ms = term_start_ms
        out.ends_at_ms = term_end_ms
        out.season_term = season_term
        out.cycle_index = cycle_index
        out.cycle_pool_size = cycle_pool_size

        out.equipped_seal_id = equipped_id
        out.progress_points = progress
        out.max_points = rule.seal_max_points
        out.battle_points = battle_pts
        out.catchup_points = catchup_pts
        out.current_max_health_bps = scaled_bonus_bps(progress, rule.seal_max_points, rule.max_health_bps_cap)
        out.cap_max_health_bps = rule.max_health_bps_cap
        out.current_defense_bps = scaled_bonus_bps(progress, rule.seal_max_points, rule.defense_bps_cap)
        out.cap_defense_bps = rule.defense_bps_cap
        out.pve_win_points = rule.pve_win_points
        out.pvp_win_points = rule.pvp_win_points
        out.pvp_other_points = rule.pvp_other_points

        out.catchup_currency_type = rule.catchup_currency_type
        out.catchup_price_per_point = rule.catchup_price_per_point
        full_plan = compute_catchup_plan(progress, rule.seal_max_points, 0, rule.catchup_price_per_point)
        out.catchup_remaining_total_price = full_plan[1] if full_plan is not None else 0
        out.currency_balance = balance(data, rule.catchup_currency_type)
        out.revision = record.revision if record is not None else 0

        writable_err = self._writable_error(player)
        out.can_switch_seal = writable_err == 0
        out.can_catchup = writable_err == 0 and full_plan is not None
        if writable_err == SEASON_IN_BATTLE:
            out.action_unavailable_reason = "战斗中暂不能更换或追赶赛季印，请在结算后重试"
        elif writable_err:
            out.action_unavailable_reason = "当前角色状态暂时不能操作赛季印，请稍后再试"

        seals = [
            seal for seal in self.tables["SeasonSeal"].rows()
            if seal.season_id == rule.id and valid_seal_row(seal)
        ]
        seals.sort(key=lambda row: row.id)
        self._fill_weekly_affix(active, stamp, seals, out.weekly_affix)
        for seal in seals:
            out.seals.add(
                seal_id=seal.id,
                name=seal.name,
                description=seal.description,
                icon_key=seal.icon_key,
                effect_id=seal.effect_id,
                bonus_bps=seal.bonus_bps,
                cap_bps=seal.cap_bps,
                per_round_limit=seal.per_round_limit,
                battle_limit=seal.battle_limit,
                active_rounds=seal.active_rounds,
                health_threshold_bps=seal.health_threshold_bps,
                equipped=seal.id == equipped_id,
                resonance_skill_id=int(getattr(seal, "resonance_skill_id", 0)),
                resonance_kind=int(getattr(seal, "resonance_kind", 0)),
                resonance_param=int(getattr(seal, "resonance_param", 0)),
                resonance_desc=str(getattr(seal, "resonance_desc", "")),
            )

        level = effective_level(data)
        season_skills = [
            skill for skill in self.tables["Skill"].rows()
            if getattr(skill, "season_id", 0) == rule.id
        ]
        season_skills.sort(key=lambda row: row.id)
        for skill in season_skills:
            eff_rank = rank_limit(level, skill)
            out.season_skills.add(
                skill_table_id=skill.id,
                name=skill.name,
                description=skill.description,
                icon_key=skill.icon_key,
                category=skill.category,
                target_team=skill.target_team,
                max_targets=skill.max_targets,
                mana_percent=skill.mana_percent,
                cooldown_rounds=skill.cooldown_rounds,
                unlock_level=skill.unlock_level,
                max_rank=skill.max_rank,
                effective_rank=eff_rank,
                unlocked=eff_rank > 0,
                ability_kind=getattr(skill, "ability_kind", 0),
                caster_mask=getattr(skill, "caster_mask", 0),
                action_tags=getattr(skill, "action_tags", 0),
            )
        return 0

    def _finish(self, response, player, data, stamp: int, error: int = 0):
        if error:
            response.error_message.id = error
        state_err = self._fill_state(player, data, stamp, response.state)
        if state_err:
            response.ClearField("state")
            if not error:
                response.error_message.id = state_err
        return response

    def get_state(self, _world, player, _route, _request):
        """查询当前赛季状态快照（只读）。"""
        response = self.protocol.message("GetSeasonStateResponse")
        data = self.scene.rows.assemble(player.snapshot)
        return self._finish(response, player, data, self.clock())

    async def equip_seal(self, _world, player, _route, request):
        """战外免费切换当前激活赛季的赛季印。"""
        response = self.protocol.message("EquipSeasonSealResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            return self._finish(response, player, data, stamp, SEASON_UNAVAILABLE)

        active = resolve_active_season(self.tables, stamp)
        if active is None:
            return self._finish(response, player, data, stamp, SEASON_NOT_OPEN)
        rule, season_term, *_ = active
        if not request.season_id or request.season_id != rule.id:
            return self._finish(response, player, data, stamp, SEASON_MISMATCH)
        if find_valid_seal(self.tables, rule.id, request.seal_id) is None:
            return self._finish(response, player, data, stamp, SEASON_SEAL_INVALID)
        if writable_err := self._writable_error(player):
            return self._finish(response, player, data, stamp, writable_err)

        current_seal_id = self._effective_seal_id(rule, _find_record(data.season_component, rule.id))
        if current_seal_id == request.seal_id:
            return self._finish(response, player, data, stamp, SEASON_SEAL_ALREADY_EQUIPPED)

        record = sync_record_for_term(self.tables, data.season_component, rule, season_term)
        record.equipped_seal_id = request.seal_id
        if record.revision < _MAX_UINT64:
            record.revision += 1

        attributes = getattr(self.scene, "attributes", None)
        on_committed = (lambda _snapshot: attributes.notify(player)) if attributes is not None else None
        try:
            await self.scene.commit_player_data(player, data, on_committed=on_committed)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info(
            "Equipped season seal player_id=%s season_id=%s seal_id=%s",
            player.player_id, rule.id, request.seal_id,
        )
        return self._finish(response, player, data, stamp)

    def _remember_receipt(self, player_id: int, request_id: int, added_points: int, spent_currency: int) -> None:
        if len(self._catchup_receipts) >= _MAX_CACHED_RECEIPTS:
            oldest = next(iter(self._catchup_receipts))
            del self._catchup_receipts[oldest]
        self._catchup_receipts[(player_id, request_id)] = (added_points, spent_currency)

    def _replay_receipt(self, player_id: int, request, rule) -> tuple[int, int]:
        cached = self._catchup_receipts.get((player_id, request.client_request_id))
        if cached is not None:
            return cached
        spent = request.expected_total_price
        if request.points_to_buy:
            added = request.points_to_buy
        elif rule is not None and rule.catchup_price_per_point:
            added = spent // rule.catchup_price_per_point
        else:
            added = 0
        return added, spent

    async def catchup(self, _world, player, _route, request):
        """灵石/货币快捷追赶当前赛季印进度，带 client_request_id 幂等与报价防呆。"""
        response = self.protocol.message("CatchupSeasonSealResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id:
            return self._finish(response, player, data, stamp, INVALID)

        # 幂等查账排在一切状态校验之前，确保超时重放始终返回首次结果而不双扣。
        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_CATCHUP)
        if err:
            return self._finish(response, player, data, stamp, err)
        if prior is not None:
            rule = find_active_season_rule(self.tables, stamp)
            added, spent = self._replay_receipt(player.player_id, request, rule)
            response.replayed = True
            response.added_points = added
            response.spent_currency = spent
            return self._finish(response, player, data, stamp)

        if not data.HasField("level_component") or self.assets is None:
            return self._finish(response, player, data, stamp, SEASON_UNAVAILABLE)

        active = resolve_active_season(self.tables, stamp)
        if active is None:
            return self._finish(response, player, data, stamp, SEASON_NOT_OPEN)
        rule, season_term, *_ = active
        if not request.season_id or request.season_id != rule.id:
            return self._finish(response, player, data, stamp, SEASON_MISMATCH)
        if not rule.catchup_price_per_point:
            return self._finish(response, player, data, stamp, SEASON_CATCHUP_DISABLED)
        if writable_err := self._writable_error(player):
            return self._finish(response, player, data, stamp, writable_err)

        existing = _find_record(data.season_component, rule.id)
        current_points, _, _ = read_effective_term_progress(existing, season_term, int(rule.seal_max_points))
        if current_points >= rule.seal_max_points:
            return self._finish(response, player, data, stamp, SEASON_SEAL_ALREADY_MAX)

        plan = compute_catchup_plan(
            current_points, rule.seal_max_points, request.points_to_buy, rule.catchup_price_per_point,
        )
        if plan is None:
            return self._finish(response, player, data, stamp, SEASON_CATCHUP_PRICE_CHANGED)
        points_to_buy, total_price = plan

        if (
            request.expected_currency_type != rule.catchup_currency_type
            or request.expected_total_price != total_price
        ):
            return self._finish(response, player, data, stamp, SEASON_CATCHUP_PRICE_CHANGED)

        if balance(data, rule.catchup_currency_type) < total_price:
            return self._finish(response, player, data, stamp, SEASON_CATCHUP_CURRENCY_NOT_ENOUGH)

        before = clone(data) if self.assets.audit_enabled else None
        deduct_err = self.assets.deduct_currency(data, rule.catchup_currency_type, total_price)
        if deduct_err:
            return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, deduct_err)

        record = sync_record_for_term(self.tables, data.season_component, rule, season_term)
        if not record.equipped_seal_id:
            record.equipped_seal_id = rule.default_seal_id
        after_points = current_points + points_to_buy
        record.progress_points = after_points
        record.catchup_points += points_to_buy
        if after_points >= rule.seal_max_points:
            set_honor_bit(data.season_component.unlocked_honor_words, rule.id)
            data.season_component.retro_honor_progress.pop(rule.id, None)
            if int(data.season_component.retro_honor_target_season_id) == rule.id:
                data.season_component.retro_honor_target_season_id = 0
        if record.revision < _MAX_UINT64:
            record.revision += 1

        client_request.remember(ledger, request.client_request_id, _CLIENT_REQUEST_KIND_CATCHUP, fingerprint)
        audit = (
            self.assets.make_audit_callback(
                player, before, data, self._debit_tx, correlation=request.client_request_id,
            )
            if before is not None else None
        )
        attributes = getattr(self.scene, "attributes", None)

        async def _committed(snapshot):
            if audit is not None:
                await audit(snapshot)
            if attributes is not None:
                attributes.notify(player)

        callback = _committed if attributes is not None else audit
        try:
            await self.scene.commit_player_data(player, data, on_committed=callback)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)

        self._remember_receipt(player.player_id, request.client_request_id, points_to_buy, total_price)
        logger.info(
            "Season seal catchup succeeded player_id=%s season_id=%s added_points=%s progress=%s/%s spent=%s",
            player.player_id, rule.id, points_to_buy, record.progress_points, rule.seal_max_points, total_price,
        )
        response.added_points = points_to_buy
        response.spent_currency = total_price
        return self._finish(response, player, data, stamp)

    async def set_retro_honor_target(self, _world, player, _route, request):
        """569 设定/清除「往季徽章追忆」指定目标（0=自动顺延首个未解锁往季；纯免费零收费）。"""
        response = self.protocol.message("SetRetroHonorTargetResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            return self._finish(response, player, data, stamp, SEASON_UNAVAILABLE)
        if writable_err := self._writable_error(player):
            return self._finish(response, player, data, stamp, writable_err)

        comp = data.season_component
        sync_legacy_honors_to_bitmap(self.tables, comp)
        target_sid = int(request.season_id)
        if target_sid != 0:
            target_rule = self.tables["SeasonRule"].get(target_sid) if "SeasonRule" in self.tables.tables else None
            active_rule = find_active_season_rule(self.tables, stamp)
            if (
                target_rule is None
                or not target_rule.seal_max_points
                or (active_rule is not None and int(active_rule.id) == target_sid)
            ):
                return self._finish(response, player, data, stamp, SEASON_HONOR_TARGET_INVALID)
            if is_season_honor_unlocked(self.tables, comp, target_sid):
                return self._finish(response, player, data, stamp, SEASON_HONOR_ALREADY_UNLOCKED)
        comp.retro_honor_target_season_id = target_sid
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info("Retro honor target updated player_id=%s target_season_id=%s", player.player_id, target_sid)
        return self._finish(response, player, data, stamp)

    # ── 五行阵法 / 五行幻境周常爬塔 / 战报配装收藏夹（515..521） ──

    async def equip_wuxing_formation(self, _world, player, _route, request):
        """515 战外免费切换五行阵法（0..5，零数值付费）。"""
        response = self.protocol.message("EquipWuxingFormationResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            return self._finish(response, player, data, stamp, SEASON_UNAVAILABLE)
        fid = int(request.formation_id)
        if not is_valid_formation_id(fid):
            response.equipped_formation_id = int(data.season_component.equipped_formation_id)
            return self._finish(response, player, data, stamp, INVALID)
        if writable_err := self._writable_error(player):
            response.equipped_formation_id = int(data.season_component.equipped_formation_id)
            return self._finish(response, player, data, stamp, writable_err)

        data.season_component.equipped_formation_id = fid
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            latest = self.scene.rows.assemble(player.snapshot)
            response.equipped_formation_id = int(latest.season_component.equipped_formation_id)
            return self._finish(response, player, latest, stamp, UNAVAILABLE)
        response.equipped_formation_id = fid
        logger.info("Equipped wuxing formation player_id=%s formation_id=%s", player.player_id, fid)
        return self._finish(response, player, data, stamp)

    def try_equip_linked_formation(self, player, data, formation_id: int) -> int:
        """装备流派方案切换时联动装配免费五行阵法（1..5，对齐 C++ TryEquipLinkedFormation）。"""
        fid = int(formation_id)
        if fid == 0 or not is_valid_formation_id(fid):
            return INVALID
        if writable_err := self._writable_error(player):
            return writable_err
        if not data.HasField("level_component"):
            return SEASON_UNAVAILABLE
        if int(data.season_component.equipped_formation_id) == fid:
            return 0
        data.season_component.equipped_formation_id = fid
        return 0

    def _upsert_tower_leaderboard(
        self,
        week_key: int,
        player_id: int,
        player_name: str,
        best_floor: int,
        best_rounds: int,
        formation_id: int,
        use_free_preset: bool,
        resonance_element: int,
        sigil_ids: tuple[int, ...],
    ) -> None:
        """单调防倒退更新本周流派榜（本人/公平双榜隔离，新分必须严格高于旧分才覆盖）。"""
        if not player_id or not best_floor:
            return
        board = self._tower_leaderboards.setdefault((int(week_key), bool(use_free_preset)), {})
        new_score = _compute_leaderboard_score(best_floor, best_rounds)
        prev = board.get(player_id)
        if prev is not None and _compute_leaderboard_score(prev[0], prev[1]) >= new_score:
            return
        board[player_id] = (
            int(best_floor),
            int(best_rounds),
            player_name,
            int(formation_id),
            int(resonance_element),
            tuple(sigil_ids),
        )

    def _fill_tower_state(self, data, stamp: int, out) -> None:
        out.Clear()
        wk = wuxing_week_key(stamp)
        out.week_key = wk
        out.realm_version = WUXING_REALM_V2_VERSION
        out.max_floors = WUXING_REALM_V2_MAX_NODES

        floor = 1
        node_kind = REALM_NODE_NORMAL
        effective_version = WUXING_REALM_V2_VERSION
        run_week_key = wk
        if data.HasField("season_component") and data.season_component.HasField("wuxing_tower"):
            tower = data.season_component.wuxing_tower
            out.best_rounds_self = tower.best_rounds_self
            out.best_rounds_preset = tower.best_rounds_preset
            out.revision = tower.revision
            # 本周首通位图与周最佳按当前周键过滤，跨周在途轮次绝不清空。
            if int(tower.week_key) == wk:
                out.weekly_cleared_mask = tower.weekly_cleared_mask
                out.weekly_best_floor = tower.weekly_best_floor
                out.weekly_best_rounds = tower.weekly_best_rounds
                out.weekly_best_floor_preset = tower.weekly_best_floor_preset
                out.weekly_best_rounds_preset = tower.weekly_best_rounds_preset
            if tower.run_active:
                effective_version = (
                    int(tower.realm_version)
                    if int(tower.realm_version) >= WUXING_REALM_V2_VERSION
                    else 1
                )
                max_floors = (
                    WUXING_REALM_V2_MAX_NODES
                    if effective_version >= WUXING_REALM_V2_VERSION
                    else WUXING_TOWER_MAX_FLOORS
                )
                out.realm_version = effective_version
                out.max_floors = max_floors
                out.run_active = True
                out.run_id = tower.run_id
                run_week_key = _effective_run_week_key(tower) or wk
                out.run_week_key = run_week_key
                floor = int(tower.current_floor) if 1 <= int(tower.current_floor) <= max_floors else 1
                node_kind = (
                    int(tower.current_node_kind)
                    if effective_version >= WUXING_REALM_V2_VERSION
                    else REALM_NODE_NORMAL
                )
                out.use_free_preset = bool(tower.use_free_preset)
                out.character_preset_id = tower.character_preset_id
                out.pet_preset_id = tower.pet_preset_id
                out.current_node_kind = node_kind
                out.pending_event_kind = tower.pending_event_kind
                out.route_choice_mask = tower.route_choice_mask
                out.run_total_rounds = tower.run_total_rounds
                out.active_sigil_ids.extend(tower.active_sigil_ids)
                out.pending_offer_sigil_ids.extend(tower.pending_offer_sigil_ids)
                if effective_version >= WUXING_REALM_V2_VERSION:
                    out.active_resonance_element = compute_active_resonance_element(tower.active_sigil_ids)

        out.current_floor = floor
        out.current_floor_dungeon_id = (
            realm_node_dungeon_id(floor, node_kind)
            if effective_version >= WUXING_REALM_V2_VERSION
            else tower_floor_dungeon_id(floor)
        )
        out.current_floor_leyline_element = tower_floor_leyline_element(run_week_key, floor)

        for use_preset in (False, True):
            board = self._tower_leaderboards.get((wk, use_preset), {})
            ranked = sorted(
                board.items(),
                key=lambda kv: (-kv[1][0], kv[1][1] if kv[1][1] > 0 else (1 << 32), kv[0]),
            )[:_TOWER_LEADERBOARD_TOP_N]
            target_list = out.weekly_leaderboard_preset if use_preset else out.weekly_leaderboard
            for rank_idx, (pid, (best_floor, best_rounds, pname, fid, resonance, sigils)) in enumerate(ranked, start=1):
                entry = target_list.add(
                    rank=rank_idx,
                    player_id=pid,
                    player_name=pname,
                    best_floor=best_floor,
                    best_rounds=best_rounds,
                    formation_id=fid,
                    use_free_preset=use_preset,
                    resonance_element=resonance,
                )
                entry.sigil_ids.extend(sigils)

    def _finish_tower(self, response, data, stamp: int, error: int = 0):
        if error:
            response.error_message.id = error
        self._fill_tower_state(data, stamp, response.state)
        return response

    def prepare_wuxing_tower_context(
        self,
        data,
        battle_config_id: int,
        *,
        now_ms_val: int | None = None,
    ) -> tuple[int, object | None, int, int]:
        """构建并校验五行幻境 V2/V1 开战上下文与公平预设 ID（对齐 C++ PrepareWuxingTowerContext）。"""
        _ = now_ms_val
        if not data.HasField("season_component") or not data.season_component.HasField("wuxing_tower"):
            return _FEATURE_UNAVAILABLE, None, 0, 0
        tower = data.season_component.wuxing_tower
        is_v2 = int(tower.realm_version) >= WUXING_REALM_V2_VERSION
        max_floors = WUXING_REALM_V2_MAX_NODES if is_v2 else WUXING_TOWER_MAX_FLOORS
        if (
            not tower.run_active
            or tower.run_id == 0
            or not (1 <= int(tower.current_floor) <= max_floors)
            or bool(tower.pending_offer_sigil_ids)
            or int(tower.pending_event_kind) != REALM_EVENT_NONE
        ):
            return _FEATURE_UNAVAILABLE, None, 0, 0
        node_kind = int(tower.current_node_kind) if is_v2 else REALM_NODE_NORMAL
        if is_v2 and not is_realm_combat_node_kind(node_kind):
            return _FEATURE_UNAVAILABLE, None, 0, 0
        expected_dungeon = (
            realm_node_dungeon_id(int(tower.current_floor), node_kind)
            if is_v2
            else tower_floor_dungeon_id(int(tower.current_floor))
        )
        if int(battle_config_id) != expected_dungeon:
            return INVALID, None, 0, 0

        run_wk = _effective_run_week_key(tower)
        ctx = self.protocol.message(
            "WuxingTowerBattleContext",
            week_key=run_wk,
            run_id=tower.run_id,
            floor=tower.current_floor,
            leyline_element=tower_floor_leyline_element(run_wk, int(tower.current_floor)),
            realm_version=int(tower.realm_version) if is_v2 else 1,
            node_kind=node_kind,
            use_free_preset=bool(tower.use_free_preset),
        )
        for sid in tower.active_sigil_ids:
            if is_valid_tower_or_boon_id(int(sid)):
                ctx.active_sigil_ids.append(int(sid))
        if is_v2:
            ctx.active_resonance_element = compute_active_resonance_element(ctx.active_sigil_ids)
        char_id = 0
        pet_id = 0
        if tower.use_free_preset:
            char_id = (
                int(tower.character_preset_id)
                if is_valid_fair_char_preset_id(int(tower.character_preset_id))
                else DEFAULT_FAIR_CHAR_PRESET_ID
            )
            pet_id = (
                int(tower.pet_preset_id)
                if is_valid_fair_pet_preset_id(int(tower.pet_preset_id))
                else DEFAULT_FAIR_PET_PRESET_ID
            )
        return 0, ctx, char_id, pet_id

    def get_wuxing_tower_state(self, _world, player, _route, _request):
        """516 查询五行幻境 V2 状态与本周本人/公平最速双榜（只读）。"""
        response = self.protocol.message("GetWuxingTowerStateResponse")
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        return self._finish_tower(response, data, self.clock())

    async def start_wuxing_tower_run(self, _world, player, _route, request):
        """517 开启新一轮五行幻境 V2 6 关分岔探索（支持本人流派 / 公平 3x3 试炼）。"""
        response = self.protocol.message("StartWuxingTowerRunResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        if writable_err := self._writable_error(player):
            return self._finish_tower(response, data, stamp, writable_err)

        use_free_preset = bool(getattr(request, "use_free_preset", False))
        char_preset_id = 0
        pet_preset_id = 0
        if use_free_preset:
            req_char = int(getattr(request, "character_preset_id", 0))
            req_pet = int(getattr(request, "pet_preset_id", 0))
            char_preset_id = req_char if req_char != 0 else DEFAULT_FAIR_CHAR_PRESET_ID
            pet_preset_id = req_pet if req_pet != 0 else DEFAULT_FAIR_PET_PRESET_ID
            if not is_valid_fair_char_preset_id(char_preset_id) or not is_valid_fair_pet_preset_id(pet_preset_id):
                return self._finish_tower(response, data, stamp, INVALID)

        wk = wuxing_week_key(stamp)
        tower = data.season_component.wuxing_tower
        _rotate_tower_weekly_stats_if_needed(tower, wk)
        tower.realm_version = WUXING_REALM_V2_VERSION
        tower.run_week_key = wk
        tower.run_active = True
        if tower.run_id < _MAX_UINT64:
            tower.run_id += 1
        tower.current_floor = 1
        tower.current_node_kind = REALM_NODE_NORMAL
        tower.pending_event_kind = REALM_EVENT_NONE
        tower.event_reroll_count = 0
        tower.route_choice_mask = 0
        tower.use_free_preset = use_free_preset
        tower.character_preset_id = char_preset_id
        tower.pet_preset_id = pet_preset_id
        tower.run_total_rounds = 0
        tower.ClearField("active_sigil_ids")
        tower.ClearField("pending_offer_sigil_ids")
        if tower.revision < _MAX_UINT64:
            tower.revision += 1

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish_tower(response, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info(
            "Started wuxing realm v2 run player_id=%s week_key=%s run_id=%s use_free_preset=%s",
            player.player_id, wk, tower.run_id, use_free_preset,
        )
        return self._finish_tower(response, data, stamp)

    async def pick_wuxing_tower_sigil(self, _world, player, _route, request):
        """518 从三选一候选中确定选择一枚局内奇策/符印并推进至下一关（跨周在途局有效）。"""
        response = self.protocol.message("PickWuxingTowerSigilResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        if writable_err := self._writable_error(player):
            return self._finish_tower(response, data, stamp, writable_err)

        tower = data.season_component.wuxing_tower
        is_v2 = int(tower.realm_version) >= WUXING_REALM_V2_VERSION
        max_floors = WUXING_REALM_V2_MAX_NODES if is_v2 else WUXING_TOWER_MAX_FLOORS
        sigil_id = int(request.sigil_id)
        if (
            not tower.run_active
            or request.expected_run_id != tower.run_id
            or not tower.pending_offer_sigil_ids
            or sigil_id not in tower.pending_offer_sigil_ids
            or not is_valid_tower_or_boon_id(sigil_id)
            or sigil_id in tower.active_sigil_ids
        ):
            return self._finish_tower(response, data, stamp, INVALID)

        tower.active_sigil_ids.append(sigil_id)
        tower.ClearField("pending_offer_sigil_ids")
        if int(tower.current_floor) < max_floors:
            next_floor = int(tower.current_floor) + 1
            tower.current_floor = next_floor
            if is_v2:
                tower.current_node_kind = realm_node_default_kind(next_floor)
        if tower.revision < _MAX_UINT64:
            tower.revision += 1

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish_tower(response, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info(
            "Picked wuxing realm boon/sigil player_id=%s run_id=%s sigil_id=%s next_floor=%s active_count=%s",
            player.player_id, tower.run_id, sigil_id, tower.current_floor, len(tower.active_sigil_ids),
        )
        return self._finish_tower(response, data, stamp)

    async def abandon_wuxing_tower_run(self, _world, player, _route, request):
        """519 放弃当前在途五行幻境轮次并立即清空局内奇策/分岔状态（保留本周首通位图）。"""
        response = self.protocol.message("AbandonWuxingTowerRunResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        if writable_err := self._writable_error(player):
            return self._finish_tower(response, data, stamp, writable_err)

        tower = data.season_component.wuxing_tower
        if not tower.run_active or (request.expected_run_id and request.expected_run_id != tower.run_id):
            return self._finish_tower(response, data, stamp, INVALID)

        abandoned_run = tower.run_id
        tower.run_active = False
        tower.current_floor = 1
        tower.current_node_kind = REALM_NODE_NORMAL
        tower.pending_event_kind = REALM_EVENT_NONE
        tower.route_choice_mask = 0
        tower.event_reroll_count = 0
        tower.run_total_rounds = 0
        tower.ClearField("active_sigil_ids")
        tower.ClearField("pending_offer_sigil_ids")
        if tower.revision < _MAX_UINT64:
            tower.revision += 1

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish_tower(response, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info("Abandoned wuxing tower run player_id=%s run_id=%s", player.player_id, abandoned_run)
        return self._finish_tower(response, data, stamp)

    async def choose_wuxing_realm_branch(self, _world, player, _route, request):
        """585 在第 2/4 关二选一分岔节点选择行进路线（1=左路精英强敌，2=右路奇遇/休整）。"""
        response = self.protocol.message("ChooseWuxingRealmBranchResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        if writable_err := self._writable_error(player):
            return self._finish_tower(response, data, stamp, writable_err)

        tower = data.season_component.wuxing_tower
        branch = int(request.branch)
        if (
            not tower.run_active
            or int(tower.realm_version) < WUXING_REALM_V2_VERSION
            or int(request.expected_run_id) != int(tower.run_id)
            or int(tower.current_node_kind) != REALM_NODE_NEED_BRANCH
            or bool(tower.pending_offer_sigil_ids)
            or int(tower.pending_event_kind) != REALM_EVENT_NONE
            or branch not in (REALM_BRANCH_ELITE, REALM_BRANCH_SPECIAL)
        ):
            return self._finish_tower(response, data, stamp, INVALID)

        if branch == REALM_BRANCH_ELITE:
            tower.current_node_kind = REALM_NODE_ELITE
        else:
            is_node_2 = int(tower.current_floor) <= 2
            tower.route_choice_mask |= 1 if is_node_2 else 2
            tower.current_node_kind = REALM_NODE_EVENT if is_node_2 else REALM_NODE_REST
            tower.pending_event_kind = REALM_EVENT_ENCOUNTER if is_node_2 else REALM_EVENT_REST
        if tower.revision < _MAX_UINT64:
            tower.revision += 1

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish_tower(response, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info(
            "Chose wuxing realm branch player_id=%s run_id=%s floor=%s branch=%s node_kind=%s",
            player.player_id, tower.run_id, tower.current_floor, branch, tower.current_node_kind,
        )
        return self._finish_tower(response, data, stamp)

    async def resolve_wuxing_realm_event(self, _world, player, _route, request):
        """586 在奇遇事件（第2关右路）或秘境休整（第4关右路）节点执行二选一抉择。"""
        response = self.protocol.message("ResolveWuxingRealmEventResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        if writable_err := self._writable_error(player):
            return self._finish_tower(response, data, stamp, writable_err)

        tower = data.season_component.wuxing_tower
        event_kind = int(tower.pending_event_kind)
        option_index = int(request.option_index)
        if (
            not tower.run_active
            or int(tower.realm_version) < WUXING_REALM_V2_VERSION
            or int(request.expected_run_id) != int(tower.run_id)
            or bool(tower.pending_offer_sigil_ids)
            or event_kind not in (REALM_EVENT_ENCOUNTER, REALM_EVENT_REST)
            or option_index not in (1, 2)
        ):
            return self._finish_tower(response, data, stamp, INVALID)

        current_wk = wuxing_week_key(stamp)
        run_wk = _effective_run_week_key(tower) or current_wk
        floor = int(tower.current_floor)
        owned = [int(x) for x in tower.active_sigil_ids]

        def _mark_node_cleared_if_same_week() -> None:
            if run_wk == current_wk and 1 <= floor <= WUXING_REALM_V2_MAX_NODES:
                _rotate_tower_weekly_stats_if_needed(tower, current_wk)
                tower.weekly_cleared_mask |= 1 << (floor - 1)

        if event_kind == REALM_EVENT_ENCOUNTER:
            if option_index == 1:
                # 道缘寻策：免战确定性获赠 1 枚未持有灵品奇策，直接推进至下一关。
                boon_id = roll_realm_encounter_boon(run_wk, int(tower.run_id), owned)
                if boon_id != 0:
                    tower.active_sigil_ids.append(boon_id)
                    response.granted_boon_id = boon_id
                _mark_node_cleared_if_same_week()
                tower.pending_event_kind = REALM_EVENT_NONE
                next_floor = min(WUXING_REALM_V2_MAX_NODES, floor + 1)
                tower.current_floor = next_floor
                tower.current_node_kind = realm_node_default_kind(next_floor)
            else:
                # 五行悟道：弹出 3 选 1 奇策候选（选完推进至下一关）。
                _mark_node_cleared_if_same_week()
                tower.pending_event_kind = REALM_EVENT_NONE
                offers = roll_realm_boon_offer(
                    run_wk, int(tower.run_id), floor, REALM_NODE_NORMAL, int(tower.event_reroll_count), owned,
                )
                tower.ClearField("pending_offer_sigil_ids")
                tower.pending_offer_sigil_ids.extend(offers)
        else:
            # 第 4 关右路：秘境休整
            if option_index == 1:
                # 奇策淬炼升品：将指定或首枚可升品奇策升级为同五行更高品质奇策（全仙品兜底赠 1 枚未持有奇策）。
                target_idx = -1
                upgraded_id = 0
                req_target = int(request.target_boon_id)
                if req_target != 0:
                    for idx, existing_id in enumerate(tower.active_sigil_ids):
                        if int(existing_id) == req_target:
                            cand = next_upgrade_boon_id(req_target, owned)
                            if cand != 0:
                                target_idx = idx
                                upgraded_id = cand
                            break
                if target_idx < 0:
                    for idx, existing_id in enumerate(tower.active_sigil_ids):
                        cand = next_upgrade_boon_id(int(existing_id), owned)
                        if cand != 0:
                            target_idx = idx
                            upgraded_id = cand
                            break
                if target_idx >= 0 and upgraded_id != 0:
                    tower.active_sigil_ids[target_idx] = upgraded_id
                    response.granted_boon_id = upgraded_id
                else:
                    fallback_id = roll_realm_encounter_boon(run_wk, int(tower.run_id), owned)
                    if fallback_id != 0:
                        tower.active_sigil_ids.append(fallback_id)
                        response.granted_boon_id = fallback_id
                _mark_node_cleared_if_same_week()
                tower.pending_event_kind = REALM_EVENT_NONE
                next_floor = min(WUXING_REALM_V2_MAX_NODES, floor + 1)
                tower.current_floor = next_floor
                tower.current_node_kind = realm_node_default_kind(next_floor)
            else:
                # 同源寻策：偏向当前持有最多同系五行的 3 选 1 奇策，冲刺 3 同系共鸣。
                _mark_node_cleared_if_same_week()
                tower.pending_event_kind = REALM_EVENT_NONE
                tower.event_reroll_count = int(tower.event_reroll_count) + 1
                preferred_elem = dominant_boon_element(owned)
                offers = roll_realm_boon_offer(
                    run_wk, int(tower.run_id), floor, REALM_NODE_ELITE,
                    int(tower.event_reroll_count), owned, preferred_elem,
                )
                tower.ClearField("pending_offer_sigil_ids")
                tower.pending_offer_sigil_ids.extend(offers)

        if tower.revision < _MAX_UINT64:
            tower.revision += 1

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            response.granted_boon_id = 0
            return self._finish_tower(response, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info(
            "Resolved wuxing realm event player_id=%s run_id=%s event_kind=%s option=%s granted=%s",
            player.player_id, tower.run_id, event_kind, option_index, response.granted_boon_id,
        )
        return self._finish_tower(response, data, stamp)

    def settle_wuxing_tower_battle(
        self,
        data,
        settlement,
        *,
        now_ms_val: int | None = None,
        player_name: str = "",
    ) -> bool:
        """幂等推进五行幻境 V2/V1 战斗节点结算（跨周在途轮次允许完局且绝不污染新周榜）。"""
        if not settlement.HasField("tower_settlement") or int(settlement.tower_settlement.floor) == 0:
            return False
        player_won = (
            (settlement.outcome == 1 and settlement.player_team_index == 0)
            or (settlement.outcome == 2 and settlement.player_team_index == 1)
        )
        if not player_won or settlement.fled:
            return False
        echo = settlement.tower_settlement
        stamp = self.clock() if now_ms_val is None else now_ms_val
        tower = data.season_component.wuxing_tower
        is_v2 = int(tower.realm_version) >= WUXING_REALM_V2_VERSION
        max_floors = WUXING_REALM_V2_MAX_NODES if is_v2 else WUXING_TOWER_MAX_FLOORS
        run_week = _effective_run_week_key(tower)
        floor = int(echo.floor)
        if (
            not tower.run_active
            or run_week != int(echo.week_key)
            or int(tower.run_id) != int(echo.run_id)
            or int(tower.current_floor) != floor
            or bool(tower.pending_offer_sigil_ids)
            or int(tower.pending_event_kind) != REALM_EVENT_NONE
            or not (1 <= floor <= max_floors)
        ):
            return False

        current_wk = wuxing_week_key(stamp)
        same_week_run = run_week == current_wk
        tower.run_total_rounds = int(tower.run_total_rounds) + max(1, int(echo.total_rounds or settlement.total_rounds or 1))

        if same_week_run:
            _rotate_tower_weekly_stats_if_needed(tower, current_wk)
            tower.weekly_cleared_mask |= 1 << (floor - 1)
            if tower.use_free_preset:
                if floor > int(tower.weekly_best_floor_preset):
                    tower.weekly_best_floor_preset = floor
                    if floor == max_floors:
                        tower.weekly_best_rounds_preset = tower.run_total_rounds
                elif floor == max_floors and (
                    int(tower.weekly_best_rounds_preset) == 0 or int(tower.run_total_rounds) < int(tower.weekly_best_rounds_preset)
                ):
                    tower.weekly_best_rounds_preset = tower.run_total_rounds
            else:
                if floor > int(tower.weekly_best_floor):
                    tower.weekly_best_floor = floor
                    if floor == max_floors:
                        tower.weekly_best_rounds = tower.run_total_rounds
                elif floor == max_floors and (
                    int(tower.weekly_best_rounds) == 0 or int(tower.run_total_rounds) < int(tower.weekly_best_rounds)
                ):
                    tower.weekly_best_rounds = tower.run_total_rounds

        if floor >= max_floors:
            tower.run_active = False
            tower.ClearField("pending_offer_sigil_ids")
            tower.pending_event_kind = REALM_EVENT_NONE
            if tower.use_free_preset:
                if int(tower.best_rounds_preset) == 0 or int(tower.run_total_rounds) < int(tower.best_rounds_preset):
                    tower.best_rounds_preset = tower.run_total_rounds
            else:
                if int(tower.best_rounds_self) == 0 or int(tower.run_total_rounds) < int(tower.best_rounds_self):
                    tower.best_rounds_self = tower.run_total_rounds
        else:
            owned = [int(x) for x in tower.active_sigil_ids]
            offers = (
                roll_realm_boon_offer(
                    run_week, int(tower.run_id), floor, int(tower.current_node_kind),
                    int(tower.event_reroll_count), owned,
                )
                if is_v2
                else roll_tower_sigil_offer(run_week, int(tower.run_id), floor, owned)
            )
            tower.ClearField("pending_offer_sigil_ids")
            tower.pending_offer_sigil_ids.extend(offers)

        if tower.revision < _MAX_UINT64:
            tower.revision += 1

        # 本周最速双榜同步（对齐 C++ OnTowerSettlementApplied 单调防倒退守卫）。
        if same_week_run and int(tower.week_key) == current_wk:
            preset = bool(tower.use_free_preset)
            best_floor = int(tower.weekly_best_floor_preset if preset else tower.weekly_best_floor)
            best_rounds = int(tower.weekly_best_rounds_preset if preset else tower.weekly_best_rounds)
            if best_floor > 0 and floor >= best_floor:
                if not (floor == max_floors and best_rounds > 0 and int(tower.run_total_rounds) > best_rounds):
                    pname = player_name or (data.profile_component.name if data.HasField("profile_component") else "")
                    fid = int(data.season_component.equipped_formation_id)
                    resonance = compute_active_resonance_element(tower.active_sigil_ids)
                    self._upsert_tower_leaderboard(
                        current_wk, data.player_id, pname, best_floor, best_rounds,
                        fid, preset, resonance, tuple(int(x) for x in tower.active_sigil_ids),
                    )
        return True

    async def save_battle_build_bookmark(self, _world, player, _route, request):
        """520 保存或更新战报公开脱敏配装收藏（单角色硬上限 12 条，按 bookmark_id 幂等覆盖）。"""
        response = self.protocol.message("SaveBattleBuildBookmarkResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        comp = data.season_component

        def _finish_bm(error: int = 0, saved_entry=None):
            if error:
                response.error_message.id = error
            if saved_entry is not None:
                _copy_bookmark_to_view(saved_entry, response.saved)
            for item in comp.saved_builds:
                _copy_bookmark_to_view(item, response.all_bookmarks.add())
            return response

        if not data.HasField("level_component"):
            return _finish_bm(SEASON_UNAVAILABLE)
        if writable_err := self._writable_error(player):
            return _finish_bm(writable_err)
        if not request.HasField("bookmark"):
            return _finish_bm(INVALID)

        incoming = request.bookmark
        existing = (
            next((b for b in comp.saved_builds if b.bookmark_id == incoming.bookmark_id), None)
            if incoming.bookmark_id
            else None
        )
        if existing is None and len(comp.saved_builds) >= MAX_SAVED_BUILD_BOOKMARKS:
            return _finish_bm(INVALID)

        if existing is not None:
            target = existing
            bookmark_id = existing.bookmark_id
        else:
            bookmark_id = incoming.bookmark_id or (max((b.bookmark_id for b in comp.saved_builds), default=0) + 1)
            target = comp.saved_builds.add()

        target.Clear()
        target.bookmark_id = bookmark_id
        target.title = _truncate_utf8(incoming.title, _MAX_BOOKMARK_TITLE_BYTES) or "配装方案"
        target.source_battle_id = incoming.source_battle_id
        target.source_player_id = incoming.source_player_id
        target.source_player_name = _truncate_utf8(incoming.source_player_name, _MAX_BOOKMARK_TITLE_BYTES)
        target.class_id = incoming.class_id
        target.formation_id = incoming.formation_id if is_valid_formation_id(incoming.formation_id) else 0
        target.season_seal_id = incoming.season_seal_id
        target.active_pet_table_id = incoming.active_pet_table_id
        target.active_pet_name = _truncate_utf8(incoming.active_pet_name, _MAX_BOOKMARK_TITLE_BYTES)
        seen_skills: set[int] = set()
        for sid in incoming.skill_table_ids:
            if sid and sid not in seen_skills:
                seen_skills.add(sid)
                target.skill_table_ids.append(sid)
                if len(target.skill_table_ids) >= _MAX_BOOKMARK_SKILLS:
                    break
        seen_sigils: set[int] = set()
        for sg in incoming.wuxing_sigil_ids:
            if is_valid_tower_or_boon_id(sg) and sg not in seen_sigils:
                seen_sigils.add(sg)
                target.wuxing_sigil_ids.append(sg)
                if len(target.wuxing_sigil_ids) >= _MAX_BOOKMARK_SIGILS:
                    break
        target.saved_at_ms = stamp

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            latest = self.scene.rows.assemble(player.snapshot)
            comp = latest.season_component
            return _finish_bm(UNAVAILABLE)
        logger.info("Saved battle build bookmark player_id=%s bookmark_id=%s", player.player_id, bookmark_id)
        return _finish_bm(0, target)

    async def delete_battle_build_bookmark(self, _world, player, _route, request):
        """521 删除指定战报配装收藏条目。"""
        response = self.protocol.message("DeleteBattleBuildBookmarkResponse")
        data = self.scene.rows.assemble(player.snapshot)
        comp = data.season_component

        def _finish_del(error: int = 0):
            if error:
                response.error_message.id = error
            for item in comp.saved_builds:
                _copy_bookmark_to_view(item, response.all_bookmarks.add())
            return response

        if not data.HasField("level_component"):
            return _finish_del(SEASON_UNAVAILABLE)
        if writable_err := self._writable_error(player):
            return _finish_del(writable_err)
        if not request.bookmark_id:
            return _finish_del(INVALID)
        idx = next((i for i, b in enumerate(comp.saved_builds) if b.bookmark_id == request.bookmark_id), -1)
        if idx < 0:
            return _finish_del(INVALID)

        del comp.saved_builds[idx]
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            latest = self.scene.rows.assemble(player.snapshot)
            comp = latest.season_component
            return _finish_del(UNAVAILABLE)
        logger.info("Deleted battle build bookmark player_id=%s bookmark_id=%s", player.player_id, request.bookmark_id)
        return _finish_del(0)
