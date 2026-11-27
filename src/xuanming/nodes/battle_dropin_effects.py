"""与 C++ 战斗子系统对齐的五项纯 Python 战斗规则模块。

本模块集中封装与 C++ `cpp/libs/services/battle/system/` 逐项对齐的五类规则：
1. 装备与法宝战斗效果（`EquipmentBattleEffects`，对照 `equipment_battle_rules.h` / `equipment_battle_effects.cpp`）；
2. 宠物修炼与辅助效果投影（`PetAndSupportEffects`，对照 `pet_cultivation_rules.h` / `support_effect_rules.h`）；
3. 赛季印与赛季规则（`SeasonBattleRules`，对照 `season_rules.h`）；
4. 副本怪物战术 AI（`DungeonMonsterAiProfile` / `DungeonMonsterAiController` / `choose_monster_action`，对照 `dungeon_monster_ai.cpp` / `monster_utility_ai.h`）；
5. 五行试炼志战斗事实追踪器（`TrialFactTracker`，对照 `trial_journal_rules.h` / `turn_battle_engine.cpp`）。

所有状态均归属单场 `TurnEngine` 实例，不依赖全局可变单例，不引入第三方依赖，并在快照未开启新字段时保持零副作用。
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING

from google.protobuf.message import Message

from .battle_effects import FREEZE, SILENCE, STUN, U64_MAX, landed_damage, landed_heal, rank_factor
from .battle_skills import active as _active

if TYPE_CHECKING:
    from .battle import TurnEngine
    from .battle_tables import BattleBuff, BattleSkill

_LOG = logging.getLogger(__name__)

# 战斗单位与动作/事件常量（与 battle_data.proto 枚举值严格一致）
_ACTOR_PLAYER = 1
_ACTOR_MONSTER = 2
_ACTOR_PET = 3

_ACTION_ATTACK = 1
_ACTION_SKILL = 2
_ACTION_DEFEND = 3

_EVENT_HEAL = 4
_EVENT_BUFF_ADD = 5
_EVENT_BUFF_REMOVE = 6
_EVENT_MANA = 14
_EVENT_RESIST = 15
_EVENT_SHIELD = 20
_EVENT_ARTIFACT = 21
EVENT_BOSS_MECHANIC = 22
EVENT_WUXING_SYNERGY = 23
EVENT_SUMMON_PET = 24

_HIT_COUNTER = 2
_HIT_REFLECT = 3
_HIT_MIRROR = 5
_DAMAGE_ORIGIN_MIRROR = 6
_DAMAGE_MAGIC = 0
_DAMAGE_PHYSICAL = 1

_I64_MAX = (1 << 63) - 1
_ENGINE_ACTOR_FLAG = 1 << 63

# 行动超时与 PVP V2 / 投降 / 首领试炼 V2 / 论剑 V3~V5 变阵禁招常量（与 C++ turn_battle_constants.h 逐项对齐）
ACTION_TIMEOUT_MS = 25_000
BOSS_TRIAL_ACTION_TIMEOUT_MS = 18_000
TACTICAL_PREP_TIMEOUT_MS = 15_000
TACTICAL_PREP_DURATION_SECONDS = 15
TACTICAL_PREP_WITH_BAN_TIMEOUT_MS = 25_000
TACTICAL_PREP_WITH_BAN_DURATION_SECONDS = 25
PREP_SKILL_BAN_SLOTS_PER_TEAM = 3
PREP_SKILL_PROTECT_SLOTS_PER_TEAM = 1
PREP_PROTECT_HASH_SALT = 0x50524F54454354
MAX_BATTLE_PINGS_PER_ROUND = 2
PVP_SUMMON_PET_UNLOCK_ROUND = 2
MIN_WIN_STREAK_HIGHLIGHT = 3
CLUTCH_COMEBACK_MIN_ESCALATION_STACKS = 2
CLUTCH_COMEBACK_LOW_HP_DENOMINATOR = 4
HIGHLIGHT_CONTROL_MIN_COUNT = 2
ERR_SKILL_BANNED_IN_BATTLE = 7028
ERR_RATE_LIMIT_EXCEEDED = 1008
ACTION_TAG_PHYSICAL = 1
ACTION_TAG_MAGIC = 2
MATCH_MODE_PVE_SOLO = 4
MATCH_MODE_PVE_TEAM = 5
MATCH_MODE_PVP_CHALLENGE = 6
MATCH_MODE_POLARITY_5V5 = 8
MATCH_MODE_GUILD_WAR_PVP = 9
MATCH_MODE_GUILD_WAR_SIEGE = 10
MATCH_MODE_SEASON_TOURNAMENT = 11
MATCH_MODE_JIANGHU_DUEL = 12
JIANGHU_DUEL_DEFAULT_MAX_ROUNDS = 30

# 锦标赛 V2 & 五行幻境共享五行灵脉枚举（与 TournamentLeylineElement 逐项一致）
TOURNAMENT_LEYLINE_UNSPECIFIED = 0
TOURNAMENT_LEYLINE_METAL = 1
TOURNAMENT_LEYLINE_WOOD = 2
TOURNAMENT_LEYLINE_WATER = 3
TOURNAMENT_LEYLINE_FIRE = 4
TOURNAMENT_LEYLINE_EARTH = 5

PVP_PRIMARY_FOCUS_NUM = 120
PVP_PRIMARY_FOCUS_DEN = 100
PVP_ESCALATION_MIN_MAX_ROUNDS = 10
PVP_ESCALATION_LEAD_ROUNDS = 7
PVP_ESCALATION_DAMAGE_STEP_PCT = 10
PVP_ESCALATION_SUPPORT_STEP_PCT = 10
PVP_ESCALATION_SUPPORT_FLOOR_PCT = 20
PVP_SURRENDER_UNLOCK_ROUND = 5
PVP_SURRENDER_REJECT_COOLDOWN_ROUNDS = 3
PVP_SURRENDER_EXPIRE_COOLDOWN_ROUNDS = 2
PVP_SURRENDER_IDLE_EXCLUDE_ROUNDS = 2

# 投降错误码（与 match_error_tip.pb.h 对齐）
ERR_SURRENDER_NOT_ALLOWED = 16052
ERR_SURRENDER_TOO_EARLY = 16053
ERR_SURRENDER_COOLDOWN = 16054
ERR_SURRENDER_ALREADY_VOTED = 16055

# 首领试炼与特殊玩法类型（对应 DungeonTable.boss_trial_kind）
BOSS_TRIAL_KIND_NONE = 0
BOSS_TRIAL_KIND_BREAK_REJUV = 1
BOSS_TRIAL_KIND_BREAK_STANCE = 2
BOSS_TRIAL_KIND_BARRIER = 3
BOSS_TRIAL_KIND_VENOM = 4
BOSS_TRIAL_KIND_GUARDIAN_RESCUE = 5
BOSS_TRIAL_KIND_POLARITY_5V5 = 6
BOSS_TRIAL_KIND_FESTIVAL_BEAST = 7
BOSS_TRIAL_KIND_BREAK_DUAL_GUARD = 8
BOSS_TRIAL_KIND_PRACTICE_DUMMY = 9
BOSS_TRIAL_KIND_TACTICAL_PUZZLE = 10
BOSS_TRIAL_KIND_CHARACTER_TALE = 11
BOSS_TRIAL_KIND_WILD_WIND = 12
BOSS_TRIAL_KIND_WILD_BELL = 13
BOSS_TRIAL_KIND_WILD_TIDE = 14

BOSS_STANCE_NONE = 0
BOSS_STANCE_PHYSICAL = 1
BOSS_STANCE_MAGIC = 2

BOSS_TRIAL_MONSTER_GUARD_REJUV = 102
BOSS_TRIAL_MONSTER_GUARD_STANCE = 103
BOSS_TRIAL_MONSTER_SUPPORT_BREAKER = 109
BOSS_TRIAL_MONSTER_SUPPORT_DRUMMER = 110

BOSS_TRIAL_SPLASH_KEEP_PCT = 60
BOSS_EXPOSED_FULL_PCT = 35
BOSS_EXPOSED_PARTIAL_PCT = 20
BOSS_PHASE_TWO_VULN_PCT = 15
BOSS_MOMENTUM_MAX_STACKS = 3
BOSS_MOMENTUM_PER_STACK_PCT = 12
BOSS_HEAL_BACKLASH_PCT = 6
BOSS_DRUMMER_DEATH_SHOCK_PCT = 4
BOSS_DUAL_GUARD_HP_KEEP_PCT = 75

BOSS_MECHANIC_CLEAVE_PARRY_EXPOSE = 1
BOSS_MECHANIC_HEAL_BACKLASH = 2
BOSS_MECHANIC_STANCE_BREAK = 3
BOSS_MECHANIC_SUPPORT_SHOCK = 4
BOSS_MECHANIC_PHASE_TWO_ENRAGE = 5
BOSS_MECHANIC_MOMENTUM_GAIN = 6
BOSS_MECHANIC_BREAKER_SUNDER = 7
BOSS_MECHANIC_REJUV_CLEANSE = 8
BOSS_MECHANIC_ANTI_HEAL_DISHEARTEN = 9
BOSS_MECHANIC_WEEKLY_AFFIX_TRIGGER = 10

# 首领试炼周常五行词缀常量与纯规则（与 C++ boss_trial_weekly_affix_rules.h 逐项一致）
BOSS_AFFIX_NONE = 0
BOSS_AFFIX_METAL = 1
BOSS_AFFIX_WOOD = 2
BOSS_AFFIX_WATER = 3
BOSS_AFFIX_FIRE = 4
BOSS_AFFIX_EARTH = 5
BOSS_AFFIX_MAX_ID = 5

BOSS_AFFIX_METAL_EXPOSED_CRIT_BONUS_PCT = 15
BOSS_AFFIX_METAL_EXPOSED_PRIMARY_BONUS_PCT = 10
BOSS_AFFIX_METAL_ENEMY_OPENING_SHIELD_PCT = 8
BOSS_AFFIX_WOOD_DISPEL_MANA_REGEN_PCT = 12
BOSS_AFFIX_WOOD_TEAM_DAMAGE_BONUS_PCT = 10
BOSS_AFFIX_WOOD_BOSS_REGEN_HP_PCT = 4
BOSS_AFFIX_WATER_CHARGE_OR_PERIODIC_DAMAGE_BONUS_PCT = 20
BOSS_AFFIX_WATER_MANA_KEEP_PCT = 80
BOSS_AFFIX_FIRE_PET_OR_FOLLOWUP_DAMAGE_BONUS_PCT = 18
BOSS_AFFIX_FIRE_EXPOSED_BURN_MAX_HP_PCT = 3
BOSS_AFFIX_FIRE_EXPOSED_BURN_CAP = 1200
BOSS_AFFIX_FIRE_BOSS_PHASE_TWO_HP_PCT = 60
BOSS_AFFIX_EARTH_PARRY_TEAM_SHIELD_PCT = 8
BOSS_AFFIX_EARTH_PARRY_NEXT_ROUND_DAMAGE_BONUS_PCT = 12
BOSS_AFFIX_EARTH_BOSS_CLEAVE_MULTIPLIER = 1.12

_BOSS_AFFIX_DAY_MS = 86_400_000
_BOSS_AFFIX_WEEK_MS = 7 * _BOSS_AFFIX_DAY_MS
_BOSS_AFFIX_FIRST_MONDAY_05_UTC8_MS = 3 * _BOSS_AFFIX_DAY_MS + 21 * 3_600_000


def is_valid_boss_affix_override(affix_id: int) -> bool:
    """校验显式指定的首领试炼周常五行词缀是否合法（0..5）。"""
    return 0 <= affix_id <= BOSS_AFFIX_MAX_ID


def boss_affix_id_for_timestamp(now_ms_val: int) -> int:
    """按 UTC+8 周一 05:00 切周基准确定性计算当周首领试炼词缀（1..5，早于基准返回 0）。"""
    if now_ms_val < _BOSS_AFFIX_FIRST_MONDAY_05_UTC8_MS:
        return BOSS_AFFIX_NONE
    week_index = (now_ms_val - _BOSS_AFFIX_FIRST_MONDAY_05_UTC8_MS) // _BOSS_AFFIX_WEEK_MS
    return int((week_index % BOSS_AFFIX_MAX_ID) + 1)


def resolve_boss_weekly_affix(
    is_boss_trial: bool,
    has_tower_context: bool,
    created_at_ms: int,
    explicit_affix_id: int,
) -> int:
    """裁决本场战斗生效的首领试炼周常词缀（仅真实首领试炼且非爬塔生效）。"""
    if not is_boss_trial or has_tower_context:
        return BOSS_AFFIX_NONE
    if 1 <= explicit_affix_id <= BOSS_AFFIX_MAX_ID:
        return explicit_affix_id
    return boss_affix_id_for_timestamp(created_at_ms)


def is_wood_regen_round(round_index: int) -> bool:
    """木周首领回合末回血回合（第 2/5/8... 回合）。"""
    return round_index >= 2 and ((round_index - 2) % 3) == 0


def is_water_momentum_round(round_index: int) -> bool:
    """水周首领回合末蓄势回合（第 2/4/6... 偶数回合）。"""
    return round_index >= 2 and (round_index % 2) == 0


def discount_water_mana_cost(original_cost: int) -> int:
    """水周蓄力/毒火技能法力消耗 8 折（最低保留 1 点）。"""
    if original_cost <= 0:
        return 0
    discounted = (original_cost * BOSS_AFFIX_WATER_MANA_KEEP_PCT) // 100
    return discounted if discounted > 0 else 1


def is_health_at_or_below_pct(health: int, max_health: int, pct: int) -> bool:
    """溢出安全的气血比例门槛判定：`health / max_health <= pct / 100`。"""
    if max_health <= 0:
        return False
    threshold = (max_health // 100) * pct + ((max_health % 100) * pct) // 100
    return health <= threshold

TELEGRAPH_KIND_SLOW_CLEAVE = 1
TELEGRAPH_KIND_GUARD_HEAL = 2
TELEGRAPH_KIND_GUARD_STANCE_PHYS = 3
TELEGRAPH_KIND_GUARD_STANCE_MAGIC = 4
TELEGRAPH_KIND_GUARD_SHIELD = 5
TELEGRAPH_KIND_GUARD_BIND = 6
TELEGRAPH_KIND_BOSS_MAGIC_STRIKE = 7
TELEGRAPH_KIND_BOSS_VENOM_AOE = 8
TELEGRAPH_KIND_FESTIVAL_HEAVY_STRIKE = 9
TELEGRAPH_KIND_WILD_WIND_GUST = 10

TELEGRAPH_STATUS_UPCOMING = 1
TELEGRAPH_STATUS_EXECUTING = 2
TELEGRAPH_STATUS_EXECUTED = 3
TELEGRAPH_STATUS_CANCELLED_BY_CONTROL = 4
TELEGRAPH_STATUS_CANCELLED_TARGET_INVALID = 5

POLARITY_DEFAULT_MAX_ROUNDS = 16
POLARITY_DAMAGE_NUM = 115
POLARITY_DAMAGE_DEN = 100
RANKED_DEFAULT_MAX_ROUNDS = 20

# 五行试炼志 V3 自选试炼誓约位图（bit 0..3，对齐 trial_journal_rules.h）。
VOW_NONE = 0
VOW_BAN_CONTROL = 1 << 0
VOW_SINGLE_ARTIFACT = 1 << 1
VOW_NO_PET = 1 << 2
VOW_TIGHT_ROUNDS = 1 << 3
VOW_MASK_ALL = VOW_BAN_CONTROL | VOW_SINGLE_ARTIFACT | VOW_NO_PET | VOW_TIGHT_ROUNDS


def is_valid_vow_mask(mask: int) -> bool:
    """校验试炼誓约位掩码仅含合法位 0..3。"""
    return (int(mask) & ~VOW_MASK_ALL) == 0


def count_vows(mask: int) -> int:
    """统计试炼誓约位掩码启用条数（0..4）。"""
    return (int(mask) & VOW_MASK_ALL).bit_count()


def tighten_round_limit(base_rounds: int) -> int:
    """紧缩回合誓约：ceil(base_rounds * 0.7)，下限保护 6 回合。"""
    if base_rounds <= 0:
        return 0
    if base_rounds <= 6:
        return base_rounds
    scaled = (base_rounds * 7 + 9) // 10
    return max(6, scaled)


def is_wild_boss_trial_kind(kind: int) -> bool:
    """判断 boss_trial_kind 是否属于卷四·三境明雷野外首领试炼（12..14）。"""
    return BOSS_TRIAL_KIND_WILD_WIND <= kind <= BOSS_TRIAL_KIND_WILD_TIDE


def is_wild_bell_yang_round(round_index: int) -> bool:
    """碧渊水宫阳钟（物理 +15%/法术 -12%）与阴鼓每 2 回合交替。"""
    block = (round_index - 1 if round_index > 0 else 0) // 2
    return (block % 2) == 0


def is_wild_tide_ebb_round(round_index: int) -> bool:
    """月汐湾涨潮（承伤 -25%）与退潮（承伤 +20%）每 2 回合交替。"""
    block = (round_index - 1 if round_index > 0 else 0) // 2
    return (block % 2) == 1


def is_boss_trial_kind(kind: int) -> bool:
    """判断 boss_trial_kind 是否属于首领试炼（1..4、8 或 12..14）。"""
    return (
        (BOSS_TRIAL_KIND_BREAK_REJUV <= kind <= BOSS_TRIAL_KIND_VENOM)
        or kind == BOSS_TRIAL_KIND_BREAK_DUAL_GUARD
        or is_wild_boss_trial_kind(kind)
    )


def is_boss_trial_or_rescue_kind(kind: int) -> bool:
    """判断 boss_trial_kind 是否属于首领试炼或护卫救援。"""
    return is_boss_trial_kind(kind) or kind == BOSS_TRIAL_KIND_GUARDIAN_RESCUE


def is_boss_stance_trial_kind(kind: int) -> bool:
    """判断是否包含护势联防阵机制（破阵·护势 或 破阵·双卫同阵）。"""
    return kind in (BOSS_TRIAL_KIND_BREAK_STANCE, BOSS_TRIAL_KIND_BREAK_DUAL_GUARD)


def is_boss_rejuv_trial_kind(kind: int) -> bool:
    """判断是否包含回春护卫机制（破阵·回春 或 破阵·双卫同阵）。"""
    return kind in (BOSS_TRIAL_KIND_BREAK_REJUV, BOSS_TRIAL_KIND_BREAK_DUAL_GUARD)

# 赛季规则硬安全上限（对照 season_rules.h）
SEASON_HARD_STAT_CAP_BPS = 500
SEASON_HARD_MAX_PROGRESS_POINTS = 1000
SEASON_HARD_MAX_CATCHUP_PRICE = 1_000_000_000
SEASON_MAX_VALID_CURRENCY_TYPE = 2

# 试炼志常量（对照 trial_journal_rules.h）
MIN_ARTIFACT_EFFECT_ID = 1
MAX_ARTIFACT_EFFECT_ID = 8
MAX_ARTIFACT_SLOTS = 2
MAX_COOP_TEAMMATES = 4

# 装备来源万分比上限表（对照 equipbattle::MaximumSourceBps）
_MAXIMUM_SOURCE_BPS: dict[int, int] = {
    1: 3200,
    2: 3200,
    3: 800,
    4: 200,
    5: 3200,
    6: 3200,
    7: 800,
    8: 800,
    9: 600,
    10: 600,
    11: 1600,
    12: 1600,
    13: 1200,
}

# 旧版 BuffType 到异常分型（1..7）的映射
_BUFF_TYPE_TO_AILMENT_KIND: dict[int, int] = {
    30: 5,  # 眩晕/昏睡
    31: 1,  # 沉默/封灵
    50: 7,  # 中毒
    52: 6,  # 冰冻
}

# 怪物效用 AI 常量（对照 dungeon_monster_ai.h / monster_utility_ai.h）
UTILITY_SCALE = 10_000
MONSTER_FEATURE_COUNT = 6

FEATURE_TARGET_MISSING_HP = 0
FEATURE_TARGET_UNCONTROLLED = 1
FEATURE_TARGET_THREAT = 2
FEATURE_IS_SKILL = 3
FEATURE_PHASE_URGENCY = 4
FEATURE_TARGET_IS_PLAYER = 5


class BattleEffectOp(IntEnum):
    """配置化战斗效果操作码（与 C++ `BattleEffectOp` 逐值一致）。"""

    NONE = 0
    DAMAGE = 1
    HEAL = 2
    SHIELD = 3
    DISPEL_ALLY = 4
    DISPEL_ENEMY = 5
    MODIFIER = 6
    PERIODIC_DAMAGE = 7
    PERIODIC_HEAL = 8
    DAMAGE_REDUCE = 9
    REFLECT = 10
    DAMAGE_SHARE = 11
    MANA_SHIELD = 12
    CONTROL = 13
    CONSUME_PERIODIC = 14
    EXTEND_PERIODIC = 15
    DRAIN_MANA = 16
    RETREAT_PET = 17
    TEMPORARY_HEALTH = 18
    MIRROR_SKILL = 19
    PURSUIT_DAMAGE = 20
    APPLY_CHILDREN = 21
    CONDITIONAL_HEAL = 22
    CONDITIONAL_MANA = 23
    EVASION_CHARGE = 24
    GROWING_AURA = 25
    COPY_TARGET_SKILL = 26
    PREPARE_CAST = 27
    HIT_VULNERABILITY = 28
    CONTROL_IMMUNITY = 29


class BattleModifier(IntEnum):
    """配置化战斗属性/伤害修正项（与 C++ `BattleModifier` 逐值一致）。"""

    NONE = 0
    PHYSICAL_ATTACK = 1
    MAGIC_ATTACK = 2
    DEFENSE = 3
    SPEED = 4
    OUTGOING_DAMAGE = 5
    INCOMING_HEAL = 6
    OUTGOING_HEAL = 7
    CRIT_CHANCE = 8
    MANA_COST = 9
    MAGIC_REDUCTION = 10
    RETREAT_RESIST = 11
    CONTROL_RESIST = 12
    LATER_HIT_REDUCTION = 13
    NEXT_DIRECT_DAMAGE = 14
    ADAPTIVE_DAMAGE = 15
    INCOMING_DAMAGE = 16
    OUTGOING_SHIELD = 17
    OUTGOING_PHYSICAL_DAMAGE = 18
    OUTGOING_MAGIC_DAMAGE = 19
    SELF_GROWING_DEFENSE = 20
    NEXT_PHYSICAL_ATTACK = 21
    NEXT_MAGIC_ATTACK = 22
    FORBID_PHYSICAL = 23
    FORBID_MAGIC = 24
    SELF_GROWING_DIRECT_DAMAGE = 25
    DEFENSE_PENETRATION = 26
    PERIODIC_DAMAGE_BONUS = 27
    ALL_CONTROL_RESIST = 28


class SecondaryTargetPolicy(IntEnum):
    """多目标战法副目标筛选策略（与 C++ `SecondaryTargetPolicy` 逐值一致）。"""

    RANDOM = 0
    HIGHEST_PHYSICAL_ATTACK = 1
    HIGHEST_MAGIC_ATTACK = 2
    LOWEST_HEALTH_VALUE = 3
    LOWEST_HEALTH_RATIO = 4
    LOWEST_PHYSICAL_DEFENSE = 5
    LOWEST_MAGIC_DEFENSE = 6
    LOWEST_ROUND_SPEED = 7


class BattleStateKind(IntEnum):
    """配置化控制与异常分型（与 C++ `BattleStateKind` 逐值一致）。"""

    NONE = 0
    SEAL_PHYSICAL = 1
    SEAL_MAGIC = 2
    SEAL_ACTIVE = 3
    CONFUSION = 4
    SLEEP = 5
    FREEZE = 6
    POISON = 7
    BURN = 8


class BattleTrigger(IntEnum):
    """配置化被动触发时机（与 C++ `BattleTrigger` 逐值一致）。"""

    NONE = 0
    AFTER_BASIC = 1
    AFTER_SKILL = 2
    BEFORE_DIRECT_DAMAGE = 3
    AFTER_DIRECT_DAMAGE = 4
    AFTER_PERIODIC = 5
    ROUND_START = 6
    AFTER_DIRECT_ATTACK = 7
    AFTER_CONTROL_ATTEMPT = 8
    AFTER_SUPPORT_CAST = 9


class BossPhaseId(IntEnum):
    """副本首领 HFSM 阶段枚举（与 C++ `BossPhaseId` 逐值一致）。"""

    ROOT = 0
    COMBAT = 1
    OPENING = 2
    ENRAGE = 3
    DESPERATE = 4
    DEAD = 5


class SealConditionType(IntEnum):
    """五行试炼志战法印权威判定条件类型（与 C++ `SealConditionType` 逐值一致）。"""

    UNSPECIFIED = 0
    CLEAR = 1
    SPEED_CLEAR = 2
    FLAWLESS_SELF_AND_PET = 3
    NO_BOSS_GUARD_HEAL = 4
    BREAK_GUARD_STANCE = 5
    WIND_DEFEND = 6
    BELL_HARMONY = 7
    TIDE_FINISH = 8
    GUARD_RESCUED = 9
    POLARITY_WIN = 10
    ARTIFACT_SHIELD_BREAK = 11
    ANTI_CONTROL_OR_SEASON_SKILL = 12
    DUAL_GUARD_BREAK = 13
    PERFECT_CLEAVE_PARRY = 14
    SEASON_SKILL_CAST = 15
    SEASON_RESONANCE = 16


# ---------------------------------------------------------------------------
# 1. 装备与法宝纯规则函数（对照 equipment_battle_rules.h）
# ---------------------------------------------------------------------------


def maximum_source_bps(effect: int) -> int:
    """返回装备单项属性来源的万分比上限（未强化表上限）。"""
    return _MAXIMUM_SOURCE_BPS.get(effect, 0)


def maximum_effective_source_bps(effect: int) -> int:
    """返回装备单项属性含 +11 控辅克制强化受益后的有效上限（速度恒定 200）。"""
    if effect in (5, 6):
        return 3584  # 3200 * (1 + 2400 * 5000 / 10000 / 10000)
    if effect == 11:
        return 1696  # 1600 * (1 + 2400 * 2500 / 10000 / 10000)
    return maximum_source_bps(effect)


def floor_bps(value: int, bps: int) -> int:
    """按万分比向下取整计算额度（上限封顶 10000，无溢出）。"""
    if value <= 0 or bps <= 0:
        return 0
    bounded = min(bps, 10_000)
    return (value * bounded) // 10_000


def valid_artifact_definition(
    effect_id: int,
    bonus_bps: int,
    cap_bps: int,
    per_round_limit: int,
    battle_limit: int,
    active_rounds: int,
    health_threshold_bps: int,
) -> bool:
    """校验单条法宝（或赛季印）机制参数是否满足 C++ `ValidArtifactDefinition` 约束。"""
    if (
        effect_id < 1
        or effect_id > 8
        or bonus_bps <= 0
        or bonus_bps > 10_000
        or cap_bps < 0
        or cap_bps > 10_000
        or health_threshold_bps < 0
        or health_threshold_bps > 10_000
        or per_round_limit < 0
        or per_round_limit > 1
        or battle_limit < 0
        or battle_limit > 3
        or active_rounds < 0
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


def discount_mana(original: int, reduction_bps: float) -> int:
    """按减耗万分比折算技能法力消耗，最低保留 1 点（对照 `equipbattle::DiscountMana`）。"""
    if original <= 0:
        return 0
    if not math.isfinite(reduction_bps):
        reduction_bps = 0.0
    reduction = int(max(0.0, min(3000.0, reduction_bps)))
    return max(1, original - floor_bps(original, reduction))


def ailment_chance(
    base: float,
    hit_bps: float,
    typed_resist_bps: float,
    legacy_resist_bps: float,
    ignore_bps: float,
    *,
    poison: bool = False,
) -> float:
    """计算异常状态最终命中概率（对照 `equipbattle::AilmentChance`）。"""
    if not math.isfinite(base) or base <= 0.0:
        return 0.0
    resistance = max(0.0, typed_resist_bps + legacy_resist_bps - ignore_bps)
    upper = 0.95 if poison else 0.85
    return max(0.05, min(upper, base + (hit_bps - resistance) / 10_000.0))


def received_ratio(armor: int, defense: int, resistance_bps: float, level: int) -> float:
    """计算含装备抗性万分比后的承伤系数，保底 0.4（对照 `equipbattle::ReceivedRatio`）。"""
    clamped_level = max(1, min(85, level))
    coefficient = 360.0 + 120.0 * clamped_level
    resist_rate = max(0.0, min(1.0, resistance_bps / 10_000.0))
    return max(0.4, coefficient / (float(armor) + float(defense) + coefficient) * (1.0 - resist_rate))


def skill_mana_category(skill: BattleSkill, buffs: Mapping[int, BattleBuff]) -> int:
    """确定技能归属的减耗类别（1..12，0 表示不适用，对照 `equipbattle::ManaCategory`）。"""
    if skill.base_damage > 0 or skill.damage_per_level > 0 or skill.damage_formula is not None:
        if skill.target_team in (0, 1):
            return 1 if skill.damage_kind == _DAMAGE_PHYSICAL else 2
    if skill.heal_multiplier > 0 or skill.heal_per_level > 0:
        return 3
    for buff_id in skill.effects:
        buff = buffs.get(buff_id)
        if buff is None:
            continue
        ailment_kind = _BUFF_TYPE_TO_AILMENT_KIND.get(buff.buff_type)
        if ailment_kind is not None:
            return 11 if ailment_kind == 7 else ailment_kind + 4
    if skill.restore_mana_percent > 0:
        return 0
    return 12


# ---------------------------------------------------------------------------
# 2. 赛季印规则（对照 season_rules.h）
# ---------------------------------------------------------------------------


class SeasonBattleRules:
    """赛季技能与单槽赛季印的纯规则校验与局内生效器（对照 `season_rules.h`）。"""

    HARD_STAT_CAP_BPS = SEASON_HARD_STAT_CAP_BPS
    HARD_MAX_PROGRESS_POINTS = SEASON_HARD_MAX_PROGRESS_POINTS
    HARD_MAX_CATCHUP_PRICE = SEASON_HARD_MAX_CATCHUP_PRICE
    MAX_VALID_CURRENCY_TYPE = SEASON_MAX_VALID_CURRENCY_TYPE

    # 赛季周词缀轮换常量（与 C++ season_rules.h 逐项一致）
    WEEKLY_AFFIX_NONE = 0
    WEEKLY_AFFIX_SHIELD_OVERLOAD = 1
    WEEKLY_AFFIX_SPIRIT_RECOVERY = 2
    WEEKLY_AFFIX_OPENING_GUARD = 3
    WEEKLY_AFFIX_SHARP_TEMPO = 4
    WEEKLY_AFFIX_COUNT = 4
    WEEKLY_AFFIX_WEEK_MS = 7 * 86_400_000
    SHIELD_OVERLOAD_BONUS_PCT = 15
    SPIRIT_RECOVERY_DEFEND_MANA_BPS = 300
    OPENING_GUARD_DAMAGE_REDUCE_PCT = 8
    OPENING_GUARD_MAX_ROUND = 2
    SHARP_TEMPO_UNSHIELDED_BONUS_PCT = 5

    @classmethod
    def is_valid_weekly_affix_id(cls, affix_id: int) -> bool:
        """校验赛季周词缀 ID 是否在 1..4 范围内。"""
        return 1 <= affix_id <= cls.WEEKLY_AFFIX_COUNT

    @classmethod
    def compute_season_week_index(cls, starts_at_ms: int, now_ms: int) -> int:
        """按赛季开启时间计算当前周序号（从 1 开始）。"""
        if now_ms <= starts_at_ms:
            return 1
        return int((now_ms - starts_at_ms) // cls.WEEKLY_AFFIX_WEEK_MS) + 1

    @classmethod
    def weekly_affix_for_week(cls, season_id: int, week_index: int) -> int:
        """确定性计算指定赛季第 N 周的周词缀 ID（1..4）。"""
        if season_id == 0:
            return cls.WEEKLY_AFFIX_NONE
        zero_based = max(0, week_index - 1)
        return ((season_id - 1 + zero_based) % cls.WEEKLY_AFFIX_COUNT) + 1

    @classmethod
    def weekly_affix_at(cls, season_id: int, starts_at_ms: int, now_ms: int) -> int:
        if season_id == 0:
            return cls.WEEKLY_AFFIX_NONE
        return cls.weekly_affix_for_week(season_id, cls.compute_season_week_index(starts_at_ms, now_ms))

    @classmethod
    def shield_overload_bonus_pct(cls, affix_id: int) -> int:
        return cls.SHIELD_OVERLOAD_BONUS_PCT if affix_id == cls.WEEKLY_AFFIX_SHIELD_OVERLOAD else 0

    @classmethod
    def defend_bonus_mana_regen_bps(cls, affix_id: int) -> int:
        return cls.SPIRIT_RECOVERY_DEFEND_MANA_BPS if affix_id == cls.WEEKLY_AFFIX_SPIRIT_RECOVERY else 0

    @classmethod
    def opening_guard_damage_reduce_pct(cls, affix_id: int, round_index: int) -> int:
        if affix_id == cls.WEEKLY_AFFIX_OPENING_GUARD and 1 <= round_index <= cls.OPENING_GUARD_MAX_ROUND:
            return cls.OPENING_GUARD_DAMAGE_REDUCE_PCT
        return 0

    @classmethod
    def sharp_tempo_damage_bonus_pct(cls, affix_id: int, target_has_shield: bool) -> int:
        if affix_id == cls.WEEKLY_AFFIX_SHARP_TEMPO and not target_has_shield:
            return cls.SHARP_TEMPO_UNSHIELDED_BONUS_PCT
        return 0

    # 赛季玩法 V4：8 种表驱动「技印共鸣」原语（零面板属性、零速度控制，对齐 C++ season_rules.h）
    RESONANCE_NONE = 0
    RESONANCE_SHARP_MANA_REFUND = 1
    RESONANCE_BREAK_EXTEND_DEBUFF = 2
    RESONANCE_GUARD_SHIELD_BOOST = 3
    RESONANCE_DEFEND_MANA_DISCOUNT = 4
    RESONANCE_EMERGENCY_HEAL_REFUND = 5
    RESONANCE_FIRST_SHIELD_MANA = 6
    RESONANCE_GUARD_HEAL_BOOST = 7
    RESONANCE_DEFEND_SHIELD_BOOST = 8
    RESONANCE_EVENT_BASE = 100

    SEASON_1_ID = 1
    SEAL_POZHEN_RUIYIN = 101
    SEAL_POZHEN_XUANYIN = 102
    SEAL_GUZHEN_SHOUYIN = 103
    SEAL_JUXI_YUANYIN = 104

    SKILL_POZHEN_LIEFENG = 15001
    SKILL_POZHEN_LINGLEI = 15002
    SKILL_GUZHEN_LINGZHANG = 15003
    SKILL_POZHEN_XUSHI = 15004

    RESONANCE_EVENT_SHARP_MANA_REFUND = 101
    RESONANCE_EVENT_MYSTIC_EXTEND_DEBUFF = 102
    RESONANCE_EVENT_GUARD_SHIELD_BOOST = 103
    RESONANCE_EVENT_BREATH_HALF_MANA = 104
    RESONANCE_EVENT_EMERGENCY_HEAL_REFUND = 105
    RESONANCE_EVENT_FIRST_SHIELD_MANA = 106
    RESONANCE_EVENT_GUARD_HEAL_BOOST = 107
    RESONANCE_EVENT_DEFEND_SHIELD_BOOST = 108

    RESONANCE_SHARP_SHATTER_MANA_REFUND_PCT = 30
    RESONANCE_MYSTIC_DEBUFF_BONUS_ROUNDS = 1
    RESONANCE_GUARD_SHIELD_BONUS_PCT = 10

    @staticmethod
    def compute_mana_pct_ceil(paid_mana: int, pct: int) -> int:
        if paid_mana <= 0 or pct <= 0:
            return 0
        bounded = min(int(pct), 100)
        q, r = divmod(int(paid_mana), 100)
        return q * bounded + (r * bounded + 99) // 100

    @staticmethod
    def compute_mana_discounted(base_cost: int, discount_pct: int) -> int:
        if base_cost <= 0:
            return 0
        pct = max(0, int(discount_pct))
        keep_pct = 0 if pct >= 100 else 100 - pct
        q, r = divmod(int(base_cost), 100)
        return q * keep_pct + (r * keep_pct) // 100

    @classmethod
    def resonance_sharp_mana_refund(cls, paid_mana: int) -> int:
        return cls.compute_mana_pct_ceil(paid_mana, cls.RESONANCE_SHARP_SHATTER_MANA_REFUND_PCT)

    @classmethod
    def resonance_half_mana_cost(cls, base_cost: int) -> int:
        return cls.compute_mana_discounted(base_cost, 50)

    @classmethod
    def valid_seal_resonance(
        cls,
        effect_id: int,
        resonance_skill_id: int,
        resonance_kind: int,
        resonance_param: int,
    ) -> bool:
        if resonance_kind == cls.RESONANCE_NONE:
            return True
        if resonance_skill_id <= 0 or resonance_param <= 0:
            return False
        if resonance_kind == cls.RESONANCE_SHARP_MANA_REFUND:
            return effect_id in (1, 2) and resonance_param <= 40
        if resonance_kind == cls.RESONANCE_BREAK_EXTEND_DEBUFF:
            return effect_id in (1, 2) and resonance_param <= 1
        if resonance_kind == cls.RESONANCE_GUARD_SHIELD_BOOST:
            return effect_id == 6 and resonance_param <= 15
        if resonance_kind == cls.RESONANCE_DEFEND_MANA_DISCOUNT:
            return effect_id == 7 and resonance_param <= 50
        if resonance_kind == cls.RESONANCE_EMERGENCY_HEAL_REFUND:
            return effect_id == 4 and resonance_param <= 35
        if resonance_kind == cls.RESONANCE_FIRST_SHIELD_MANA:
            return effect_id == 5 and resonance_param <= 300
        if resonance_kind == cls.RESONANCE_GUARD_HEAL_BOOST:
            return effect_id == 6 and resonance_param <= 15
        if resonance_kind == cls.RESONANCE_DEFEND_SHIELD_BOOST:
            return effect_id == 7 and resonance_param <= 12
        return False

    @staticmethod
    def is_window_active(starts_at_ms: int, ends_at_ms: int, enabled: bool, now_ms: int) -> bool:
        """判定当前时间是否处于活跃赛季时间窗内。"""
        if not enabled:
            return False
        if ends_at_ms != 0 and ends_at_ms <= starts_at_ms:
            return False
        if now_ms < starts_at_ms:
            return False
        return ends_at_ms == 0 or now_ms < ends_at_ms

    @staticmethod
    def windows_overlap(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
        """判定两个赛季时间窗是否存在重叠。"""
        a_before_b = end_a != 0 and end_a <= start_b
        b_before_a = end_b != 0 and end_b <= start_a
        return not a_before_b and not b_before_a

    @classmethod
    def valid_seal_effect(
        cls,
        season_id: int,
        seal_id: int,
        effect_id: int,
        bonus_bps: int,
        cap_bps: int,
        per_round_limit: int,
        battle_limit: int,
        active_rounds: int,
        health_threshold_bps: int,
    ) -> bool:
        """校验单槽赛季印战斗机制参数是否合法（严禁 effect_id=3 控命与 8 控抗）。"""
        if season_id <= 0 or seal_id <= 0 or effect_id in (3, 8):
            return False
        return valid_artifact_definition(
            effect_id,
            bonus_bps,
            cap_bps,
            per_round_limit,
            battle_limit,
            active_rounds,
            health_threshold_bps,
        )

    @classmethod
    def valid_seal_definition(
        cls,
        season_id: int,
        seal_id: int,
        effect_id: int,
        bonus_bps: int,
        cap_bps: int,
        per_round_limit: int,
        battle_limit: int,
        active_rounds: int,
        health_threshold_bps: int,
        resonance_skill_id: int = 0,
        resonance_kind: int = 0,
        resonance_param: int = 0,
    ) -> bool:
        return cls.valid_seal_effect(
            season_id,
            seal_id,
            effect_id,
            bonus_bps,
            cap_bps,
            per_round_limit,
            battle_limit,
            active_rounds,
            health_threshold_bps,
        ) and cls.valid_seal_resonance(
            effect_id, resonance_skill_id, resonance_kind, resonance_param
        )

    @classmethod
    def scaled_bonus_bps(cls, progress_points: int, max_points: int, cap_bps: int) -> int:
        """按赛季进度线性折算轻属性万分比，严格夹取至 500 bps 硬上限。"""
        if max_points <= 0 or cap_bps <= 0 or progress_points <= 0:
            return 0
        capped_cap = min(cap_bps, cls.HARD_STAT_CAP_BPS)
        clamped_points = min(progress_points, max_points)
        return (capped_cap * clamped_points) // max_points

    @staticmethod
    def is_skill_active_in_season(skill_season_id: int, active_season_id: int) -> bool:
        """校验技能是否在当前赛季生效（0 为常驻永久技能）。"""
        return skill_season_id == 0 or (active_season_id != 0 and skill_season_id == active_season_id)

    @staticmethod
    def compute_catchup_plan(
        current_points: int,
        max_points: int,
        requested_points: int,
        price_per_point: int,
    ) -> tuple[int, int] | None:
        """计算赛季进度追赶点数与总价，非法返回 `None`。"""
        if max_points <= 0 or current_points >= max_points or price_per_point <= 0:
            return None
        remaining = max_points - current_points
        buy = remaining if requested_points == 0 else requested_points
        if buy <= 0 or buy > remaining:
            return None
        if price_per_point > U64_MAX // buy:
            return None
        return buy, price_per_point * buy

    @classmethod
    def apply_init_bonuses(cls, actor: Message) -> tuple[int, int]:
        """在角色入战初始化阶段应用赛季印轻属性加成（气血与防御，严格封顶 500 bps）。"""
        if not actor.HasField("equipment_effects"):
            return 0, 0
        effects = actor.equipment_effects
        hp_bps = min(max(0, effects.season_max_health_bps), cls.HARD_STAT_CAP_BPS)
        def_bps = min(max(0, effects.season_defense_bps), cls.HARD_STAT_CAP_BPS)
        hp_delta = floor_bps(actor.max_health, hp_bps) if hp_bps else 0
        def_delta = floor_bps(actor.defense, def_bps) if def_bps else 0
        if hp_delta:
            actor.max_health = min(U64_MAX, actor.max_health + hp_delta)
            if actor.attributes.health > 0:
                actor.attributes.health = min(actor.max_health, actor.attributes.health + hp_delta)
        if def_delta:
            actor.defense = min(U64_MAX, actor.defense + def_delta)
        return hp_delta, def_delta

    @staticmethod
    def seal_available(actor: Message, effect_id: int, round_index: int) -> bool:
        """判断玩家佩戴的赛季印当前回合是否可触发指定效果。"""
        if (
            actor.actor_type != _ACTOR_PLAYER
            or not _active(actor)
            or not actor.HasField("equipment_effects")
            or not actor.equipment_effects.HasField("season_seal")
        ):
            return False
        seal = actor.equipment_effects.season_seal
        if seal.effect_id != effect_id:
            return False
        if seal.active_rounds > 0 and round_index > seal.active_rounds:
            return False
        state = actor.season_seal_state
        if seal.battle_limit > 0 and state.used_count >= seal.battle_limit:
            return False
        if (
            seal.per_round_limit > 0
            and state.last_trigger_round == round_index
            and state.round_trigger_count >= seal.per_round_limit
        ):
            return False
        return True

    @staticmethod
    def use_seal(
        actor: Message,
        target_id: int,
        value: int,
        round_index: int,
        turn: Message,
        group: int,
    ) -> None:
        """提交一次赛季印触发次数并写入 `BATTLE_EVENT_ARTIFACT` 事件。"""
        if not actor.HasField("equipment_effects") or not actor.equipment_effects.HasField("season_seal"):
            return
        seal = actor.equipment_effects.season_seal
        state = actor.season_seal_state
        state.used_count += 1
        state.round_trigger_count = state.round_trigger_count + 1 if state.last_trigger_round == round_index else 1
        state.last_trigger_round = round_index
        turn.events.add(
            event_type=_EVENT_ARTIFACT,
            source_id=actor.actor_id,
            target_id=target_id,
            group_id=group,
            artifact_item_id=0,
            artifact_effect_id=seal.effect_id,
            season_seal_id=seal.seal_id,
            value=value,
            success=True,
        )


# ---------------------------------------------------------------------------
# 3. 宠物修炼与辅助效果（对照 pet_cultivation_rules.h / support_effect_rules.h）
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ModifierSelection:
    """同类辅助修正择优器：仅取最强正项与最强负项之和（对照 `supportrules::ModifierSelection`）。"""

    gain: float = 0.0
    loss: float = 0.0
    gain_index: int = -1
    loss_index: int = -1

    def consider(self, value: float, index: int = -1) -> None:
        """纳入一条候选修正值。"""
        if not math.isfinite(value):
            return
        if value > self.gain:
            self.gain = value
            self.gain_index = index
        if value < self.loss:
            self.loss = value
            self.loss_index = index

    def total(self) -> float:
        """返回最终生效净修正比例。"""
        return self.gain + self.loss

    def project(self, breakdown: Message) -> None:
        """将胜出项的生效贡献投影回 `SupportEffectBreakdown`。"""
        for index in (self.gain_index, self.loss_index):
            if 0 <= index < len(breakdown.sources):
                source = breakdown.sources[index]
                source.effective_bonus_bps = source.raw_bonus_bps
                source.ClearField("reason")


class PetAndSupportEffects:
    """宠物独立修炼加成与治疗/护盾辅助效果规则（对照 `pet_cultivation_rules.h` / `support_effect_rules.h`）。"""

    def __init__(self, engine: TurnEngine):
        self.engine = engine

    @staticmethod
    def valid_pet_cultivation(effects: Message) -> bool:
        """校验 `PetCultivationEffects` 快照全部万分比字段是否在合法区间内。"""
        scalar_fields = (
            effects.physical_damage_bps,
            effects.magic_damage_bps,
            effects.outgoing_heal_bps,
            effects.shield_capacity_bps,
            effects.poison_damage_bps,
            effects.retreat_hit_bps,
            effects.retreat_resist_bps,
        )
        if any(value > 10_000 for value in scalar_fields):
            return False
        for state_kind, value in effects.ailment_hit_bps.items():
            if not 1 <= state_kind <= 7 or value > 10_000:
                return False
        for state_kind, value in effects.ailment_resist_bps.items():
            if not 1 <= state_kind <= 7 or value > 10_000:
                return False
        return True

    def initialize_pet_cultivation(self, actor: Message, effects: Message) -> bool:
        """校验并冻结宠物修炼快照到 `BattleActorState.pet_cultivation`。"""
        if not self.valid_pet_cultivation(effects):
            _LOG.warning("rejected invalid pet cultivation snapshot actor_id=%s", actor.actor_id)
            return False
        actor.pet_cultivation.CopyFrom(effects)
        return True

    @staticmethod
    def _rate(bps: int) -> float:
        return min(max(0, bps), 10_000) / 10_000.0

    @classmethod
    def pet_damage_bonus(
        cls,
        actor: Message,
        damage_kind: int,
        *,
        is_direct: bool = True,
        hit_kind: int = 0,
    ) -> float:
        """计算宠物直接攻击的物伤/法伤增幅比例（反击不吃此加成）。"""
        if (
            actor.actor_type != _ACTOR_PET
            or not is_direct
            or hit_kind == _HIT_COUNTER
            or not actor.HasField("pet_cultivation")
        ):
            return 0.0
        if damage_kind == _DAMAGE_PHYSICAL:
            return cls._rate(actor.pet_cultivation.physical_damage_bps)
        if damage_kind == _DAMAGE_MAGIC:
            return cls._rate(actor.pet_cultivation.magic_damage_bps)
        return 0.0

    @classmethod
    def apply_pet_damage_bonus(
        cls,
        actor: Message,
        damage: float,
        damage_kind: int,
        *,
        is_direct: bool = True,
        hit_kind: int = 0,
    ) -> float:
        """将宠物修炼直伤加成乘算至伤害基数。"""
        bonus = cls.pet_damage_bonus(actor, damage_kind, is_direct=is_direct, hit_kind=hit_kind)
        return cls.apply_bonus(damage, bonus) if bonus else damage

    @classmethod
    def pet_heal_bonus(cls, actor: Message) -> float:
        """返回宠物治疗增幅比例。"""
        if actor.actor_type != _ACTOR_PET or not actor.HasField("pet_cultivation"):
            return 0.0
        return cls._rate(actor.pet_cultivation.outgoing_heal_bps)

    @classmethod
    def pet_shield_bonus(cls, actor: Message) -> float:
        """返回宠物护盾容量增幅比例。"""
        if actor.actor_type != _ACTOR_PET or not actor.HasField("pet_cultivation"):
            return 0.0
        return cls._rate(actor.pet_cultivation.shield_capacity_bps)

    @classmethod
    def pet_poison_bonus(cls, actor: Message) -> float:
        """返回宠物中毒周期伤害增幅比例。"""
        if actor.actor_type != _ACTOR_PET or not actor.HasField("pet_cultivation"):
            return 0.0
        return cls._rate(actor.pet_cultivation.poison_damage_bps)

    @staticmethod
    def pet_hit_bps(actor: Message, state_kind: int) -> float:
        """返回宠物对应异常分型的命中万分比。"""
        if actor.actor_type != _ACTOR_PET or not actor.HasField("pet_cultivation"):
            return 0.0
        return float(min(actor.pet_cultivation.ailment_hit_bps.get(state_kind, 0), 10_000))

    @staticmethod
    def pet_resist_bps(actor: Message, state_kind: int) -> float:
        """返回宠物对应异常分型的抵抗万分比。"""
        if actor.actor_type != _ACTOR_PET or not actor.HasField("pet_cultivation"):
            return 0.0
        return float(min(actor.pet_cultivation.ailment_resist_bps.get(state_kind, 0), 10_000))

    @classmethod
    def pet_retreat_hit(cls, actor: Message) -> float:
        """返回宠物劝退命中加值。"""
        if actor.actor_type != _ACTOR_PET or not actor.HasField("pet_cultivation"):
            return 0.0
        return cls._rate(actor.pet_cultivation.retreat_hit_bps)

    @classmethod
    def pet_retreat_resist(cls, actor: Message) -> float:
        """返回宠物劝退抵抗加值。"""
        if actor.actor_type != _ACTOR_PET or not actor.HasField("pet_cultivation"):
            return 0.0
        return cls._rate(actor.pet_cultivation.retreat_resist_bps)

    @staticmethod
    def support_base_value(
        flat: float,
        per_level: float,
        level: int,
        basis: float,
        ratio: float,
        rank: int,
    ) -> float:
        """计算治疗/护盾中立基础值（对照 `supportrules::BaseValue`）。"""
        value = max(0.0, flat + per_level * level + basis * ratio) * rank_factor(rank)
        return value if math.isfinite(value) else 0.0

    @staticmethod
    def apply_bonus(base: float, bonus: float) -> float:
        """将总加成比例应用到基础值（对照 `supportrules::ApplyBonus`）。"""
        return base * max(0.0, 1.0 + bonus)

    @staticmethod
    def whole(value: float) -> int:
        """将浮点结果一次向下取整为非负 uint64（对照 `supportrules::Whole`）。"""
        if not (value > 0.0) or not math.isfinite(value):
            return 0
        if value >= float(U64_MAX):
            return U64_MAX
        return int(value)

    @staticmethod
    def valid_equipment_sources(equipment: Message) -> bool:
        """校验装备快照中的辅助来源明细是否完整且自洽（对照 `supportrules::ValidEquipmentSources`）。"""
        if not equipment.support_sources_complete:
            return False
        seen: set[tuple[int, int]] = set()
        healing = 0.0
        shield = 0.0
        for source in equipment.support_sources:
            key = (source.item_id, source.affix_id)
            if (
                source.kind != 1
                or source.item_id == 0
                or source.affix_id == 0
                or key in seen
                or source.effect_kind not in (1, 2)
                or not math.isfinite(source.raw_bonus_bps)
                or not math.isfinite(source.effective_bonus_bps)
                or source.raw_bonus_bps < 0.0
                or source.effective_bonus_bps < 0.0
                or source.effective_bonus_bps > source.raw_bonus_bps
                or source.conditional
            ):
                return False
            seen.add(key)
            if source.effect_kind == 2:
                shield += source.effective_bonus_bps
            else:
                healing += source.effective_bonus_bps
        return healing == float(equipment.outgoing_heal_bps) and shield == float(equipment.shield_capacity_bps)

    def build_support_summary(self, actor: Message, round_index: int) -> Message:
        """构建只读 `SupportEffectSummary` 投影（对照 `supportrules::Begin` + `BuildSupportEffects`）。"""
        summary = self.engine.protocol.message("SupportEffectSummary", in_battle=True, round_index=round_index)
        has_equip = actor.HasField("equipment_effects")
        equipment = actor.equipment_effects if has_equip else None
        complete = bool(equipment.support_sources_complete) if equipment is not None else True

        for is_shield in (False, True):
            part = summary.shield if is_shield else summary.healing
            part.complete = complete
            part.equipment_cap_bps = maximum_source_bps(6 if is_shield else 5)
            equip_bps = (equipment.shield_capacity_bps if is_shield else equipment.outgoing_heal_bps) if equipment else 0
            pet_bps = (
                self.pet_shield_bonus(actor) * 10_000.0
                if is_shield
                else self.pet_heal_bonus(actor) * 10_000.0
            )
            passive_bps = 0.0
            if not is_shield:
                for skill_id, passive_rank in sorted(actor.skill_ranks.items()):
                    passive = self.engine.skills.rules.get(skill_id)
                    if passive is not None and passive.passive_stat == "outgoingHeal":
                        delta_bps = passive.passive_percent * rank_factor(passive_rank, 0.05) * 10_000.0
                        passive_bps += delta_bps
                        part.sources.add(
                            kind=2,
                            skill_table_id=skill_id,
                            raw_bonus_bps=delta_bps,
                            effective_bonus_bps=delta_bps,
                            effect_kind=1,
                        )
            part.total_bonus_bps = float(equip_bps) + passive_bps + pet_bps

            if equipment is not None:
                effect_kind = 2 if is_shield else 1
                max_eff_cap = maximum_effective_source_bps(6 if is_shield else 5)
                has_explicit_cap = False
                for source in equipment.support_sources:
                    if source.effect_kind != effect_kind:
                        continue
                    part.sources.add().CopyFrom(source)
                    if source.source_cap_bps > 0:
                        bounded_cap = min(source.source_cap_bps, max_eff_cap)
                        part.equipment_cap_bps = (
                            min(part.equipment_cap_bps, bounded_cap) if has_explicit_cap else bounded_cap
                        )
                        has_explicit_cap = True

                artifact_effect_id = 5 if is_shield else 4
                available = set(self.engine.equipment_effects.available_artifacts(actor, artifact_effect_id, round_index))
                for idx, artifact in enumerate(equipment.artifacts):
                    if artifact.effect_id != artifact_effect_id:
                        continue
                    cond_src = part.sources.add(
                        kind=4,
                        item_id=artifact.item_id,
                        name=artifact.name,
                        raw_bonus_bps=float(artifact.bonus_bps),
                        conditional=True,
                        effect_kind=effect_kind,
                    )
                    if idx not in available:
                        cond_src.reason = "当前次数/回合或角色状态不满足法宝条件，不计常驻总计"
                    elif is_shield:
                        cond_src.reason = "主目标首个独立容量盾；不计常驻总计"
                    else:
                        pct = artifact.health_threshold_bps / 100.0
                        cond_src.reason = f"主目标首段即时治疗且气血≤{pct:g}%；不计常驻总计"
        return summary


# ---------------------------------------------------------------------------
# 4. 装备与法宝局内效果管理器（对照 equipment_battle_effects.cpp）
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ActorCombatBonus:
    """可选的局内扩展战斗修正（用于伤害加成、额外减伤、吸血与开局护盾）。"""

    damage_bonus_bps: int = 0
    damage_reduction_bps: int = 0
    lifesteal_bps: int = 0
    battle_start_shield: int = 0


@dataclass(slots=True)
class SeasonSealRuntimeState:
    """赛季玩法 V3：每名参战玩家的赛季印局内共鸣窗口与本场功劳统计（纯引擎内状态）。"""

    guard_resonance_ready_round: int = 0
    guard_resonance_consumed: bool = False
    defend_resonance_ready_round: int = 0
    contribution_value: int = 0
    trigger_count: int = 0
    resonance_count: int = 0


class EquipmentBattleEffects:
    """管理单场战斗内装备词条、法宝触发次数、护盾吸收与吸血效果。"""

    def __init__(self, engine: TurnEngine):
        self.engine = engine
        self._extra_bonuses: dict[int, ActorCombatBonus] = {}
        self._next_shield_buff_id: int = 1 << 40
        self._season_seal_runtime: dict[int, SeasonSealRuntimeState] = {}
        self.pending_mystic_extend_caster_id: int = 0
        self.pending_mystic_extend_target_id: int = 0
        self.pending_mystic_extend_skill_id: int = 0
        self.pending_mystic_extend_bonus_rounds: int = 0
        self.active_next_round_boost_caster_id: int = 0
        self.active_next_round_boost_kind: int = 0
        self.active_next_round_boost_pct: int = 0
        self.active_next_round_boost_extra: int = 0

    def season_seal_runtime(self, actor_id: int) -> SeasonSealRuntimeState:
        rt = self._season_seal_runtime.get(actor_id)
        if rt is None:
            rt = SeasonSealRuntimeState()
            self._season_seal_runtime[actor_id] = rt
        return rt

    def find_season_seal_runtime(self, actor_id: int) -> SeasonSealRuntimeState | None:
        return self._season_seal_runtime.get(actor_id)

    def clear_season_seal_runtime(self, actor_id: int) -> None:
        self._season_seal_runtime.pop(actor_id, None)

    def record_season_seal_use(self, actor_id: int, value: int) -> None:
        rt = self.season_seal_runtime(actor_id)
        rt.trigger_count += 1
        rt.contribution_value = min(U64_MAX, rt.contribution_value + max(0, int(value)))

    def has_any_regular_shield(self, target: Message) -> bool:
        rules = self.engine.effects.rules
        for entry in target.buffs:
            if entry.remaining_capacity <= 0:
                continue
            row = rules.get(entry.buff_table_id)
            if row is None or row.hit_limit == 0:
                return True
        return False

    def defend_mana_discount_resonance_pct(self, actor: Message, skill_id: int, round_index: int) -> int:
        if (
            actor.actor_type != _ACTOR_PLAYER
            or not actor.HasField("equipment_effects")
            or not actor.equipment_effects.HasField("season_seal")
        ):
            return 0
        seal = actor.equipment_effects.season_seal
        if (
            int(getattr(seal, "resonance_kind", 0)) != SeasonBattleRules.RESONANCE_DEFEND_MANA_DISCOUNT
            or int(getattr(seal, "resonance_skill_id", 0)) != skill_id
            or int(getattr(seal, "resonance_param", 0)) <= 0
        ):
            return 0
        rt = self._season_seal_runtime.get(actor.actor_id)
        return int(seal.resonance_param) if (rt is not None and rt.defend_resonance_ready_round == round_index) else 0

    def is_defend_breath_resonance_ready(self, actor: Message, skill_id: int, round_index: int) -> bool:
        return self.defend_mana_discount_resonance_pct(actor, skill_id, round_index) > 0

    def emit_season_resonance(
        self,
        actor: Message,
        target_id: int,
        resonance_effect_id: int,
        value: int,
        turn: Message,
        group: int,
    ) -> None:
        if not actor.HasField("equipment_effects") or not actor.equipment_effects.HasField("season_seal"):
            return
        seal = actor.equipment_effects.season_seal
        rt = self.season_seal_runtime(actor.actor_id)
        rt.resonance_count += 1
        turn.events.add(
            event_type=_EVENT_ARTIFACT,
            source_id=actor.actor_id,
            target_id=target_id,
            group_id=group,
            artifact_item_id=0,
            artifact_effect_id=resonance_effect_id,
            season_seal_id=seal.seal_id,
            value=value,
            success=True,
        )
        if resonance_effect_id > SeasonBattleRules.RESONANCE_EVENT_BASE:
            self.engine.trial_tracker.record_season_resonance(
                actor, resonance_effect_id - SeasonBattleRules.RESONANCE_EVENT_BASE
            )

    def set_actor_bonus(
        self,
        actor_id: int,
        *,
        damage_bonus_bps: int = 0,
        damage_reduction_bps: int = 0,
        lifesteal_bps: int = 0,
        battle_start_shield: int = 0,
    ) -> None:
        """为指定单位注册额外的战斗修正（伤害加成/减伤/吸血/开局护盾）。"""
        self._extra_bonuses[actor_id] = ActorCombatBonus(
            damage_bonus_bps=max(0, min(10_000, damage_bonus_bps)),
            damage_reduction_bps=max(0, min(10_000, damage_reduction_bps)),
            lifesteal_bps=max(0, min(10_000, lifesteal_bps)),
            battle_start_shield=max(0, battle_start_shield),
        )
        actor = self.engine.actors.get(actor_id)
        if actor is not None and battle_start_shield > 0:
            self.grant_shield(actor, float(battle_start_shield))

    def actor_bonus(self, actor_id: int) -> ActorCombatBonus | None:
        """获取指定单位的额外战斗修正。"""
        return self._extra_bonuses.get(actor_id)

    @staticmethod
    def _bounded_map(values: Mapping[int, int], max_key: int, max_value: int) -> bool:
        return all(1 <= key <= max_key and 0 <= val <= max_value for key, val in values.items())

    @staticmethod
    def _artifact_state(actor: Message, item_id: int) -> Message | None:
        for state in actor.artifact_states:
            if state.item_id == item_id:
                return state
        return None

    def initialize_equipment(self, actor: Message, effects: Message) -> bool:
        """校验并初始化角色装备快照、法宝次数状态及赛季印轻属性（对照 `InitializeEquipment`）。"""
        if (
            effects.physical_attack_bps > maximum_effective_source_bps(1)
            or effects.magic_attack_bps > maximum_effective_source_bps(2)
            or effects.max_health_bps > maximum_effective_source_bps(3)
            or effects.speed_bps > maximum_effective_source_bps(4)
            or effects.outgoing_heal_bps > maximum_effective_source_bps(5)
            or effects.shield_capacity_bps > maximum_effective_source_bps(6)
            or effects.physical_crit_bps > maximum_effective_source_bps(7)
            or effects.magic_crit_bps > maximum_effective_source_bps(8)
            or effects.physical_resist_bps > maximum_effective_source_bps(9)
            or effects.magic_resist_bps > maximum_effective_source_bps(10)
            or not self._bounded_map(effects.ailment_hit_bps, 7, maximum_effective_source_bps(11))
            or not self._bounded_map(effects.ailment_resist_bps, 7, maximum_effective_source_bps(12))
            or not self._bounded_map(effects.mana_reduction_bps, 12, maximum_effective_source_bps(13))
            or len(effects.artifacts) > MAX_ARTIFACT_SLOTS
            or effects.season_max_health_bps > SeasonBattleRules.HARD_STAT_CAP_BPS
            or effects.season_defense_bps > SeasonBattleRules.HARD_STAT_CAP_BPS
        ):
            _LOG.warning("rejected equipment effects snapshot due to cap violation actor_id=%s", actor.actor_id)
            return False

        if effects.HasField("season_seal"):
            seal = effects.season_seal
            if not SeasonBattleRules.valid_seal_definition(
                seal.season_id,
                seal.seal_id,
                seal.effect_id,
                seal.bonus_bps,
                seal.cap_bps,
                seal.per_round_limit,
                seal.battle_limit,
                seal.active_rounds,
                seal.health_threshold_bps,
                int(getattr(seal, "resonance_skill_id", 0)),
                int(getattr(seal, "resonance_kind", 0)),
                int(getattr(seal, "resonance_param", 0)),
            ):
                _LOG.warning("rejected invalid season seal in equipment effects actor_id=%s", actor.actor_id)
                return False

        seen_items: set[int] = set()
        for source in effects.artifacts:
            if (
                source.item_id == 0
                or source.item_id >= _ENGINE_ACTOR_FLAG
                or source.item_id in seen_items
                or not valid_artifact_definition(
                    source.effect_id,
                    source.bonus_bps,
                    source.cap_bps,
                    source.per_round_limit,
                    source.battle_limit,
                    source.active_rounds,
                    source.health_threshold_bps,
                )
            ):
                _LOG.warning(
                    "rejected invalid artifact source actor_id=%s item_id=%s effect_id=%s",
                    actor.actor_id,
                    source.item_id,
                    source.effect_id,
                )
                return False
            seen_items.add(source.item_id)

        actor.equipment_effects.CopyFrom(effects)
        if not PetAndSupportEffects.valid_equipment_sources(actor.equipment_effects):
            actor.equipment_effects.support_sources_complete = False
            actor.equipment_effects.ClearField("support_sources")

        # 按 item_id 升序冻结法宝顺序并初始化局内触发状态
        sorted_artifacts = sorted(
            (type(src).FromString(src.SerializeToString()) for src in actor.equipment_effects.artifacts),
            key=lambda src: src.item_id,
        )
        actor.equipment_effects.ClearField("artifacts")
        actor.equipment_effects.artifacts.extend(sorted_artifacts)
        actor.ClearField("artifact_states")
        for source in actor.equipment_effects.artifacts:
            actor.artifact_states.add(item_id=source.item_id)
        actor.ClearField("season_seal_state")

        # 应用装备四项被动万分比与赛季印轻属性加成（全 0 时保持面板零变化）
        base_max_hp = actor.max_health
        hp_delta = floor_bps(base_max_hp, actor.equipment_effects.max_health_bps)
        if actor.equipment_effects.physical_attack_bps:
            actor.physical_attack = min(
                U64_MAX,
                actor.physical_attack + floor_bps(actor.physical_attack, actor.equipment_effects.physical_attack_bps),
            )
        if actor.equipment_effects.magic_attack_bps:
            actor.magic_attack = min(
                U64_MAX,
                actor.magic_attack + floor_bps(actor.magic_attack, actor.equipment_effects.magic_attack_bps),
            )
        if actor.equipment_effects.speed_bps:
            actor.attributes.speed = min(
                U64_MAX,
                actor.attributes.speed + floor_bps(actor.attributes.speed, actor.equipment_effects.speed_bps),
            )
        if hp_delta:
            actor.max_health = min(U64_MAX, actor.max_health + hp_delta)
            if actor.attributes.health > 0:
                actor.attributes.health = min(actor.max_health, actor.attributes.health + hp_delta)

        # 应用赛季印轻属性（气血 / 防御，均基于原始基础值计算）
        season_hp_bps = min(actor.equipment_effects.season_max_health_bps, SeasonBattleRules.HARD_STAT_CAP_BPS)
        season_def_bps = min(actor.equipment_effects.season_defense_bps, SeasonBattleRules.HARD_STAT_CAP_BPS)
        if season_hp_bps:
            season_hp_delta = floor_bps(base_max_hp, season_hp_bps)
            actor.max_health = min(U64_MAX, actor.max_health + season_hp_delta)
            if actor.attributes.health > 0:
                actor.attributes.health = min(actor.max_health, actor.attributes.health + season_hp_delta)
        if season_def_bps:
            actor.defense = min(U64_MAX, actor.defense + floor_bps(actor.defense, season_def_bps))

        actor.entry_max_health = actor.max_health
        actor.entry_max_mana = actor.max_mana
        return True

    def crit_bonus(self, actor: Message, damage_kind: int) -> float:
        """返回装备暴击万分比折算的暴击率增量。"""
        if not actor.HasField("equipment_effects"):
            return 0.0
        if damage_kind == _DAMAGE_PHYSICAL:
            return actor.equipment_effects.physical_crit_bps / 10_000.0
        if damage_kind == _DAMAGE_MAGIC:
            return actor.equipment_effects.magic_crit_bps / 10_000.0
        return 0.0

    def apply_equipment_resist(
        self,
        target: Message,
        damage: float,
        damage_kind: int,
        effective_defense: int,
    ) -> float:
        """按 C++ `DamageBeforeCriticalRoll` 口径叠加装备物理/法术抗性万分比。"""
        if damage <= 0.0 or not target.HasField("equipment_effects"):
            return damage
        equip_resist = (
            target.equipment_effects.physical_resist_bps
            if damage_kind == _DAMAGE_PHYSICAL
            else target.equipment_effects.magic_resist_bps
            if damage_kind == _DAMAGE_MAGIC
            else 0
        )
        if equip_resist <= 0:
            return damage
        resist_bonus = (
            target.combat.physical_resist
            if damage_kind == _DAMAGE_PHYSICAL
            else target.combat.magic_resist
            if damage_kind == _DAMAGE_MAGIC
            else 0
        )
        legacy_resist = min(U64_MAX, target.attributes.resistance + resist_bonus)
        orig_ratio = received_ratio(target.attributes.armor, effective_defense, float(legacy_resist) * 100.0, target.level)
        if orig_ratio <= 0.0:
            return damage
        new_ratio = received_ratio(
            target.attributes.armor,
            effective_defense,
            float(legacy_resist) * 100.0 + float(equip_resist),
            target.level,
        )
        return damage * (new_ratio / orig_ratio)

    def discount_skill_mana(self, actor: Message, skill: BattleSkill, original_cost: int) -> int:
        """按技能类别查询装备减耗万分比并返回折后法力消耗。"""
        if original_cost <= 0 or not actor.HasField("equipment_effects"):
            return original_cost
        reduction_map = actor.equipment_effects.mana_reduction_bps
        if not reduction_map:
            return original_cost
        category = skill_mana_category(skill, self.engine.effects.rules)
        reduction = reduction_map.get(category, 0)
        return discount_mana(original_cost, float(reduction)) if reduction else original_cost

    def available_artifacts(self, actor: Message, effect_id: int, round_index: int) -> list[int]:
        """返回当前回合可用且匹配效果编号的法宝索引列表（按 item_id 升序）。"""
        if actor.actor_type != _ACTOR_PLAYER or not _active(actor) or not actor.HasField("equipment_effects"):
            return []
        sources = actor.equipment_effects.artifacts
        available: list[int] = []
        for idx, source in enumerate(sources):
            state = self._artifact_state(actor, source.item_id)
            if (
                state is None
                or source.effect_id != effect_id
                or (source.active_rounds > 0 and round_index > source.active_rounds)
                or (source.battle_limit > 0 and state.used_count >= source.battle_limit)
                or (
                    source.per_round_limit > 0
                    and state.last_trigger_round == round_index
                    and state.round_trigger_count >= source.per_round_limit
                )
            ):
                continue
            available.append(idx)
        available.sort(key=lambda i: sources[i].item_id)
        return available

    def use_artifact(
        self,
        actor: Message,
        index: int,
        target_id: int,
        value: int,
        round_index: int,
        turn: Message,
        group: int,
    ) -> None:
        """扣减一件法宝的局内触发次数并写入 `BATTLE_EVENT_ARTIFACT` 事件。"""
        source = actor.equipment_effects.artifacts[index]
        state = self._artifact_state(actor, source.item_id)
        if state is None:
            return
        state.used_count += 1
        state.round_trigger_count = state.round_trigger_count + 1 if state.last_trigger_round == round_index else 1
        state.last_trigger_round = round_index
        turn.events.add(
            event_type=_EVENT_ARTIFACT,
            source_id=actor.actor_id,
            target_id=target_id,
            group_id=group,
            artifact_item_id=source.item_id,
            artifact_effect_id=source.effect_id,
            value=value,
            success=True,
        )

    def on_defend(self, actor: Message, round_index: int, turn: Message, group: int) -> int:
        """执行防御指令时的法宝/赛季印回蓝（`effect_id == 7`，对照 `EquipmentDefend`）。"""
        if not actor.HasField("equipment_effects"):
            return 0
        before = actor.attributes.mana
        missing = max(0, actor.max_mana - before)
        entry_max_mana = actor.entry_max_mana or actor.max_mana
        total = 0
        for idx in self.available_artifacts(actor, 7, round_index):
            source = actor.equipment_effects.artifacts[idx]
            gain = min(missing - total, floor_bps(entry_max_mana, source.bonus_bps))
            if gain <= 0:
                continue
            total += gain
            self.use_artifact(actor, idx, actor.actor_id, gain, round_index, turn, group)
        if SeasonBattleRules.seal_available(actor, 7, round_index) and missing > total:
            seal = actor.equipment_effects.season_seal
            gain = min(missing - total, floor_bps(entry_max_mana, seal.bonus_bps))
            if gain > 0:
                total += gain
                SeasonBattleRules.use_seal(actor, actor.actor_id, gain, round_index, turn, group)
                self.record_season_seal_use(actor.actor_id, gain)
                if int(getattr(seal, "resonance_kind", 0)) in (
                    SeasonBattleRules.RESONANCE_DEFEND_MANA_DISCOUNT,
                    SeasonBattleRules.RESONANCE_DEFEND_SHIELD_BOOST,
                ):
                    self.season_seal_runtime(actor.actor_id).defend_resonance_ready_round = round_index + 1
        if total <= 0:
            return 0
        actor.attributes.mana = before + total
        turn.events.add(
            event_type=_EVENT_MANA,
            source_id=actor.actor_id,
            target_id=actor.actor_id,
            group_id=group,
            value=total,
            mana_delta=min(total, _I64_MAX),
            target_health_after=actor.attributes.health,
            target_mana_after=actor.attributes.mana,
        )
        return total

    def reduce_damage(
        self,
        source: Message,
        target: Message,
        damage: int,
        round_index: int,
        turn: Message,
        group: int,
        *,
        direct_or_pursuit_or_mirror: bool = True,
    ) -> int:
        """执行开局减伤法宝/赛季印（`effect_id == 6`）与额外减伤修正（对照 `EquipmentReduceDamage`）。"""
        if damage <= 0 or source.team_index == target.team_index or not direct_or_pursuit_or_mirror:
            return damage
        reduction = 0
        extra = self._extra_bonuses.get(target.actor_id)
        if extra is not None and extra.damage_reduction_bps > 0:
            reduction += min(damage - reduction, floor_bps(damage, extra.damage_reduction_bps))
        if target.HasField("equipment_effects"):
            entry_max_hp = target.entry_max_health or target.max_health
            for idx in self.available_artifacts(target, 6, round_index):
                artifact = target.equipment_effects.artifacts[idx]
                part = min(
                    damage - reduction,
                    floor_bps(damage, artifact.bonus_bps),
                    floor_bps(entry_max_hp, artifact.cap_bps),
                )
                if part <= 0:
                    continue
                reduction += part
                self.use_artifact(target, idx, target.actor_id, part, round_index, turn, group)
            if SeasonBattleRules.seal_available(target, 6, round_index) and damage > reduction:
                seal = target.equipment_effects.season_seal
                part = min(
                    damage - reduction,
                    floor_bps(damage, seal.bonus_bps),
                    floor_bps(entry_max_hp, seal.cap_bps),
                )
                if part > 0:
                    reduction += part
                    SeasonBattleRules.use_seal(target, target.actor_id, part, round_index, turn, group)
                    self.record_season_seal_use(target.actor_id, part)
                    if int(getattr(seal, "resonance_kind", 0)) in (
                        SeasonBattleRules.RESONANCE_GUARD_SHIELD_BOOST,
                        SeasonBattleRules.RESONANCE_GUARD_HEAL_BOOST,
                    ):
                        rt = self.season_seal_runtime(target.actor_id)
                        if not rt.guard_resonance_consumed:
                            rt.guard_resonance_ready_round = round_index + 1
        return max(0, damage - reduction)

    def grant_shield(
        self,
        actor: Message,
        base_capacity: float,
        *,
        source: Message | None = None,
        buff_table_id: int = 0,
        remain_rounds: int = 0,
        round_index: int = 1,
        turn: Message | None = None,
        group: int = 0,
        commit_artifacts: bool = False,
        skill_id: int = 0,
        is_mirror: bool = False,
    ) -> int:
        """为目标附加独立容量护盾，自动结算施放方护盾容量词条/宠物修炼/法宝 5。"""
        caster = source if source is not None else actor
        capacity_float = base_capacity
        contributions: list[int] = []
        season_seal_shield_gain = 0
        if caster.actor_type == _ACTOR_PET:
            capacity_float = PetAndSupportEffects.apply_bonus(base_capacity, PetAndSupportEffects.pet_shield_bonus(caster))
        elif caster.actor_type == _ACTOR_PLAYER and caster.HasField("equipment_effects"):
            bonus = caster.equipment_effects.shield_capacity_bps / 10_000.0
            if commit_artifacts and turn is not None:
                for idx in self.available_artifacts(caster, 5, round_index):
                    bps = caster.equipment_effects.artifacts[idx].bonus_bps
                    after = base_capacity * (1.0 + bonus + bps / 10_000.0)
                    if math.floor(after) > math.floor(base_capacity * (1.0 + bonus)):
                        contributions.append(idx)
                    bonus += bps / 10_000.0
                if SeasonBattleRules.seal_available(caster, 5, round_index):
                    seal = caster.equipment_effects.season_seal
                    before_floor = math.floor(max(0.0, base_capacity * (1.0 + bonus)))
                    bonus += seal.bonus_bps / 10_000.0
                    after_floor = math.floor(max(0.0, base_capacity * (1.0 + bonus)))
                    if after_floor > before_floor:
                        season_seal_shield_gain = after_floor - before_floor
            capacity_float = PetAndSupportEffects.apply_bonus(base_capacity, bonus)
        if getattr(self.engine, "active_leyline", 0) == TOURNAMENT_LEYLINE_WOOD:
            capacity_float *= 110.0 / 100.0
        season_shield_pct = SeasonBattleRules.shield_overload_bonus_pct(
            getattr(self.engine, "active_season_weekly_affix_id", 0),
        )
        if season_shield_pct > 0:
            capacity_float *= (100.0 + season_shield_pct) / 100.0
        support_bonus_pct = getattr(self.engine, "wuxing_support_bonus_pct", lambda _a: 0)(caster)
        if support_bonus_pct > 0:
            capacity_float *= (100.0 + support_bonus_pct) / 100.0
        if getattr(self.engine, "escalation_stacks", 0) > 0:
            capacity_float *= self.engine.escalation_support_pct / 100.0
        next_round_shield_gain = 0
        if (
            not is_mirror
            and self.active_next_round_boost_caster_id == caster.actor_id
            and self.active_next_round_boost_kind in (
                SeasonBattleRules.RESONANCE_GUARD_SHIELD_BOOST,
                SeasonBattleRules.RESONANCE_DEFEND_SHIELD_BOOST,
            )
            and self.active_next_round_boost_pct > 0
        ):
            before_shield = PetAndSupportEffects.whole(capacity_float)
            capacity_float *= (100.0 + self.active_next_round_boost_pct) / 100.0
            after_shield = PetAndSupportEffects.whole(capacity_float)
            if after_shield > before_shield:
                next_round_shield_gain = after_shield - before_shield
        capacity = PetAndSupportEffects.whole(capacity_float)
        if capacity <= 0:
            return 0
        if next_round_shield_gain > 0:
            self.active_next_round_boost_extra = min(
                U64_MAX, self.active_next_round_boost_extra + next_round_shield_gain,
            )
        buff_id = self._next_shield_buff_id
        self._next_shield_buff_id += 1
        actor.buffs.add(
            buff_id=buff_id,
            buff_table_id=buff_table_id,
            caster_id=caster.actor_id,
            layer=1,
            remain_rounds=remain_rounds,
            remaining_capacity=capacity,
        )
        owner_id = (
            caster.actor_id if caster.actor_type == _ACTOR_PLAYER
            else (caster.owner_player_id if caster.actor_type == _ACTOR_PET else 0)
        )
        if owner_id and hasattr(self.engine, "total_shield_granted_by_player"):
            self.engine.total_shield_granted_by_player[owner_id] = min(
                U64_MAX, self.engine.total_shield_granted_by_player.get(owner_id, 0) + capacity,
            )
        if commit_artifacts and turn is not None:
            for idx in contributions:
                self.use_artifact(
                    caster,
                    idx,
                    actor.actor_id,
                    caster.equipment_effects.artifacts[idx].bonus_bps,
                    round_index,
                    turn,
                    group,
                )
            if season_seal_shield_gain > 0 and SeasonBattleRules.seal_available(caster, 5, round_index):
                seal = caster.equipment_effects.season_seal
                SeasonBattleRules.use_seal(caster, actor.actor_id, season_seal_shield_gain, round_index, turn, group)
                self.record_season_seal_use(caster.actor_id, season_seal_shield_gain)
                if (
                    int(getattr(seal, "resonance_kind", 0)) == SeasonBattleRules.RESONANCE_FIRST_SHIELD_MANA
                    and int(getattr(seal, "resonance_skill_id", 0)) == skill_id
                    and int(getattr(seal, "resonance_param", 0)) > 0
                ):
                    mana_before = caster.attributes.mana
                    missing_mana = max(0, caster.max_mana - mana_before)
                    entry_max_mana = caster.entry_max_mana or caster.max_mana
                    gain = min(missing_mana, floor_bps(entry_max_mana, int(seal.resonance_param)))
                    if gain > 0:
                        caster.attributes.mana = mana_before + gain
                        turn.events.add(
                            event_type=_EVENT_MANA,
                            source_id=caster.actor_id,
                            target_id=caster.actor_id,
                            group_id=group,
                            skill_table_id=skill_id,
                            value=gain,
                            mana_delta=min(gain, _I64_MAX),
                            target_health_after=caster.attributes.health,
                            target_mana_after=caster.attributes.mana,
                        )
                        self.emit_season_resonance(
                            caster,
                            caster.actor_id,
                            SeasonBattleRules.RESONANCE_EVENT_FIRST_SHIELD_MANA,
                            gain,
                            turn,
                            group,
                        )
        return capacity

    @staticmethod
    def _find_buff_index(actor: Message, buff_id: int) -> int:
        for idx, entry in enumerate(actor.buffs):
            if entry.buff_id == buff_id:
                return idx
        return -1

    def absorb_shields(
        self,
        source: Message,
        target: Message,
        damage: int,
        turn: Message,
        group: int,
    ) -> tuple[int, int]:
        """按顺序消耗目标护盾容量吸收伤害并产出 `BATTLE_EVENT_SHIELD` 事件。"""
        if damage <= 0:
            return 0, 0
        shield_ids = [entry.buff_id for entry in target.buffs if entry.remaining_capacity > 0]
        if not shield_ids:
            return damage, 0
        remaining = damage
        absorbed_total = 0
        for buff_id in shield_ids:
            if remaining <= 0:
                break
            idx = self._find_buff_index(target, buff_id)
            if idx < 0:
                continue
            entry = target.buffs[idx]
            part = min(remaining, entry.remaining_capacity)
            if part <= 0:
                continue
            remaining -= part
            absorbed_total += part
            entry.remaining_capacity -= part
            after = entry.remaining_capacity
            turn.events.add(
                event_type=_EVENT_SHIELD,
                source_id=source.actor_id,
                target_id=target.actor_id,
                group_id=group,
                buff_table_id=entry.buff_table_id,
                buff_instance_id=buff_id,
                absorbed=part,
                shield_after=after,
            )
            if after == 0:
                buff_table_id = entry.buff_table_id
                caster_id = entry.caster_id
                del target.buffs[idx]
                turn.events.add(
                    event_type=_EVENT_BUFF_REMOVE,
                    source_id=caster_id,
                    target_id=target.actor_id,
                    buff_table_id=buff_table_id,
                    buff_instance_id=buff_id,
                    group_id=group,
                )
        return remaining, absorbed_total

    def break_shields(
        self,
        source: Message,
        target: Message,
        damage: int,
        damage_kind: int,
        round_index: int,
        turn: Message,
        group: int,
        shield_ids: Sequence[int],
        *,
        skill_id: int = 0,
        paid_mana: int = 0,
    ) -> int:
        """执行破盾法宝/赛季印（`effect_id == 1` 物理削盾 / `2` 法术削盾，对照 `EquipmentBreakShields`）。"""
        if not _active(target) or damage <= 0 or not shield_ids or not source.HasField("equipment_effects"):
            return 0
        effect_id = 1 if damage_kind == _DAMAGE_PHYSICAL else 2 if damage_kind == _DAMAGE_MAGIC else 0
        if effect_id == 0:
            return 0
        accumulated = 0
        total_removed = 0

        def _strip_with(bonus_bps: int, *, artifact_item_id: int, season_seal_id: int) -> int:
            nonlocal accumulated
            before = floor_bps(damage, accumulated)
            accumulated += bonus_bps
            left = floor_bps(damage, accumulated) - before
            removed = 0
            for buff_id in shield_ids:
                if left <= 0:
                    break
                idx = self._find_buff_index(target, buff_id)
                if idx < 0:
                    continue
                entry = target.buffs[idx]
                part = min(left, entry.remaining_capacity)
                if part <= 0:
                    continue
                left -= part
                removed += part
                entry.remaining_capacity -= part
                after = entry.remaining_capacity
                turn.events.add(
                    event_type=_EVENT_SHIELD,
                    source_id=source.actor_id,
                    target_id=target.actor_id,
                    group_id=group,
                    buff_table_id=entry.buff_table_id,
                    buff_instance_id=buff_id,
                    absorbed=part,
                    shield_after=after,
                    artifact_item_id=artifact_item_id,
                    artifact_effect_id=effect_id,
                    season_seal_id=season_seal_id,
                )
                if after == 0:
                    buff_table_id = entry.buff_table_id
                    caster_id = entry.caster_id
                    del target.buffs[idx]
                    turn.events.add(
                        event_type=_EVENT_BUFF_REMOVE,
                        source_id=caster_id,
                        target_id=target.actor_id,
                        buff_table_id=buff_table_id,
                        buff_instance_id=buff_id,
                        group_id=group,
                    )
            return removed

        for idx in self.available_artifacts(source, effect_id, round_index):
            artifact = source.equipment_effects.artifacts[idx]
            removed = _strip_with(artifact.bonus_bps, artifact_item_id=artifact.item_id, season_seal_id=0)
            if removed > 0:
                total_removed += removed
                self.use_artifact(source, idx, target.actor_id, removed, round_index, turn, group)
                self.engine.trial_tracker.record_shield_break(source)

        if SeasonBattleRules.seal_available(source, effect_id, round_index):
            seal = source.equipment_effects.season_seal
            removed = _strip_with(seal.bonus_bps, artifact_item_id=0, season_seal_id=seal.seal_id)
            if removed > 0:
                total_removed += removed
                SeasonBattleRules.use_seal(source, target.actor_id, removed, round_index, turn, group)
                self.record_season_seal_use(source.actor_id, removed)
                self.engine.trial_tracker.record_shield_break(source)
                res_kind = int(getattr(seal, "resonance_kind", 0))
                res_skill = int(getattr(seal, "resonance_skill_id", 0))
                res_param = int(getattr(seal, "resonance_param", 0))
                if res_skill == skill_id and res_param > 0:
                    if (
                        res_kind == SeasonBattleRules.RESONANCE_SHARP_MANA_REFUND
                        and not self.has_any_regular_shield(target)
                    ):
                        mana_before = source.attributes.mana
                        missing_mana = max(0, source.max_mana - mana_before)
                        refund = min(missing_mana, SeasonBattleRules.compute_mana_pct_ceil(paid_mana, res_param))
                        if refund > 0:
                            source.attributes.mana = mana_before + refund
                            turn.events.add(
                                event_type=_EVENT_MANA,
                                source_id=source.actor_id,
                                target_id=source.actor_id,
                                group_id=group,
                                skill_table_id=skill_id,
                                value=refund,
                                mana_delta=min(refund, _I64_MAX),
                                target_health_after=source.attributes.health,
                                target_mana_after=source.attributes.mana,
                            )
                            self.emit_season_resonance(
                                source,
                                source.actor_id,
                                SeasonBattleRules.RESONANCE_EVENT_SHARP_MANA_REFUND,
                                refund,
                                turn,
                                group,
                            )
                    elif res_kind == SeasonBattleRules.RESONANCE_BREAK_EXTEND_DEBUFF:
                        self.pending_mystic_extend_caster_id = source.actor_id
                        self.pending_mystic_extend_target_id = target.actor_id
                        self.pending_mystic_extend_skill_id = skill_id
                        self.pending_mystic_extend_bonus_rounds = res_param

        return total_removed

    def amplify_heal_event(
        self,
        source: Message,
        target: Message,
        event: Message,
        round_index: int,
        turn: Message,
        group: int,
        *,
        is_primary_first_hit: bool = True,
    ) -> int:
        """为已结算的治疗事件叠加装备治疗词条、宠物修炼及急救法宝（`effect_id == 4`）。"""
        if event.value <= 0 or target.is_dead:
            return 0
        base_bonus = 0.0
        if source.actor_type == _ACTOR_PLAYER and source.HasField("equipment_effects"):
            base_bonus += source.equipment_effects.outgoing_heal_bps / 10_000.0
        elif source.actor_type == _ACTOR_PET:
            base_bonus += PetAndSupportEffects.pet_heal_bonus(source)

        extra_bonus = base_bonus
        contributions: list[tuple[int, int]] = []
        hp_before = max(0, target.attributes.health - event.value)
        if (
            is_primary_first_hit
            and source.actor_type == _ACTOR_PLAYER
            and source.HasField("equipment_effects")
            and target.max_health > 0
        ):
            for idx in self.available_artifacts(source, 4, round_index):
                artifact = source.equipment_effects.artifacts[idx]
                if hp_before * 10_000 > target.max_health * artifact.health_threshold_bps:
                    continue
                before_extra = floor_bps(event.value, int(round(extra_bonus * 10_000)))
                extra_bonus += artifact.bonus_bps / 10_000.0
                after_extra = floor_bps(event.value, int(round(extra_bonus * 10_000)))
                if after_extra > before_extra:
                    contributions.append((idx, after_extra - before_extra))

        if extra_bonus <= 0.0:
            return 0
        added = landed_heal(target, float(event.value) * extra_bonus)
        if added <= 0:
            return 0
        target.attributes.health += added
        event.value += added
        event.target_health_after = target.attributes.health
        for idx, part in contributions:
            self.use_artifact(source, idx, target.actor_id, min(part, added), round_index, turn, group)
        return added

    def apply_lifesteal(self, source: Message, dealt: int, turn: Message, group: int) -> int:
        """按已注册的吸血万分比对出手方进行气血回复并产出 `BATTLE_EVENT_HEAL`。"""
        if dealt <= 0 or not _active(source):
            return 0
        extra = self._extra_bonuses.get(source.actor_id)
        if extra is None or extra.lifesteal_bps <= 0:
            return 0
        healed = landed_heal(source, float(floor_bps(dealt, extra.lifesteal_bps)))
        if healed <= 0:
            return 0
        source.attributes.health += healed
        turn.events.add(
            event_type=_EVENT_HEAL,
            source_id=source.actor_id,
            target_id=source.actor_id,
            group_id=group,
            value=healed,
            target_health_after=source.attributes.health,
            target_mana_after=source.attributes.mana,
        )
        return healed

    def roll_ailment_with_equipment(
        self,
        caster: Message,
        target: Message,
        base_chance: float,
        ailment_kind: int,
        buff_table_id: int,
        round_index: int,
        turn: Message,
        group: int,
        *,
        is_primary: bool = True,
    ) -> bool:
        """执行含装备命中/抗性、法宝 3（控场回响）与法宝 8（清心防控制）的异常命中判定。"""
        if not 1 <= ailment_kind <= 7 or not math.isfinite(base_chance) or base_chance <= 0.0:
            return False
        hit = PetAndSupportEffects.pet_hit_bps(caster, ailment_kind)
        if caster.HasField("equipment_effects"):
            hit += float(caster.equipment_effects.ailment_hit_bps.get(ailment_kind, 0))

        resist = PetAndSupportEffects.pet_resist_bps(target, ailment_kind)
        if target.HasField("equipment_effects"):
            resist += float(target.equipment_effects.ailment_resist_bps.get(ailment_kind, 0))
        combat = target.combat
        if ailment_kind == 7:
            resist += 100.0 * combat.resist_poison
        elif ailment_kind == 6:
            resist += 100.0 * combat.resist_freeze
        elif ailment_kind == 5:
            resist += 100.0 * combat.resist_sleep
        elif ailment_kind == 4:
            resist += 100.0 * combat.resist_confusion
        else:
            resist += 100.0 * combat.resist_forget
        if ailment_kind in (4, 5):
            resist += 10_000.0 * self.engine.configured.modifier(target, BattleModifier.CONTROL_RESIST)
        if 1 <= ailment_kind <= 6:
            resist += 10_000.0 * self.engine.configured.modifier(target, BattleModifier.ALL_CONTROL_RESIST)
        if getattr(self.engine, "active_leyline", 0) == TOURNAMENT_LEYLINE_WATER:
            resist += 800.0
        wuxing_ctrl_resist = getattr(self.engine, "wuxing_control_resist_pct", lambda _a: 0)(target)
        if wuxing_ctrl_resist > 0:
            resist += 100.0 * wuxing_ctrl_resist

        consumed_echo = False
        if is_primary and caster.HasField("equipment_effects"):
            for source in caster.equipment_effects.artifacts:
                state = self._artifact_state(caster, source.item_id)
                if (
                    source.effect_id == 3
                    and state is not None
                    and state.pending_round == round_index
                    and state.pending_ailment_kind == ailment_kind
                ):
                    hit += float(source.bonus_bps)
                    state.ClearField("pending_ailment_kind")
                    state.ClearField("pending_round")
                    consumed_echo = True

        if ailment_kind <= 6 and target.HasField("equipment_effects"):
            for idx in self.available_artifacts(target, 8, round_index):
                bonus = target.equipment_effects.artifacts[idx].bonus_bps
                resist += float(bonus)
                self.use_artifact(target, idx, target.actor_id, bonus, round_index, turn, group)
                self.engine.trial_tracker.record_anti_control_or_season_skill(target)

        chance = ailment_chance(
            base_chance,
            hit,
            resist,
            100.0 * combat.resist_all_ailment,
            100.0 * caster.combat.ignore_ailment_resist,
            poison=(ailment_kind == 7),
        )
        success = chance > 0.0 and self.engine.rng.unit() < chance
        if not success:
            turn.events.add(
                event_type=_EVENT_RESIST,
                source_id=caster.actor_id,
                target_id=target.actor_id,
                buff_table_id=buff_table_id,
                group_id=group,
            )
            if is_primary and not consumed_echo and caster.HasField("equipment_effects"):
                for idx in self.available_artifacts(caster, 3, round_index):
                    source = caster.equipment_effects.artifacts[idx]
                    state = self._artifact_state(caster, source.item_id)
                    if state is None or state.pending_round != 0:
                        continue
                    state.pending_ailment_kind = ailment_kind
                    state.pending_round = round_index + 1
                    self.use_artifact(caster, idx, caster.actor_id, ailment_kind, round_index, turn, group)
        return success

    @staticmethod
    def clear_pending_echoes_on_death(target: Message) -> None:
        """单位阵亡时清除待用控场回响，但不重置已消耗次数（对照 `HandleDeath`）。"""
        for state in target.artifact_states:
            state.ClearField("pending_ailment_kind")
            state.ClearField("pending_round")


# ---------------------------------------------------------------------------
# 5. 副本怪物战术 AI（对照 dungeon_monster_ai.h / dungeon_monster_ai.cpp / monster_utility_ai.h）
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LinearUtilityModel:
    """定点整数线性效用打分模型（零浮点误差，跨语言逐位一致）。"""

    weights: tuple[int, ...] = (0,) * MONSTER_FEATURE_COUNT
    bias: int = 0

    def score(self, features: Sequence[int]) -> int:
        """计算特征向量的定点整数效用得分。"""
        total = self.bias * UTILITY_SCALE
        for weight, value in zip(self.weights, features):
            total += weight * value
        return total


@dataclass(frozen=True, slots=True)
class PairwiseTrainingSample:
    """成对排序偏好训练样本：`chosen` 优于 `rejected`。"""

    chosen: tuple[int, ...]
    rejected: tuple[int, ...]
    sample_weight: float = 1.0


@dataclass(frozen=True, slots=True)
class PairwiseTrainConfig:
    """成对逻辑回归训练超参数（与 C++ `PairwiseTrainConfig` 一致）。"""

    epochs: int = 200
    learning_rate: float = 0.25
    l2_regularization: float = 0.001
    max_abs_weight: float = 20.0


def train_pairwise_linear_model(
    samples: Sequence[PairwiseTrainingSample],
    config: PairwiseTrainConfig | None = None,
) -> LinearUtilityModel:
    """使用确定性全批量梯度下降训练并量化定点线性效用模型（对照 `TrainPairwiseLinearModel`）。"""
    cfg = config or PairwiseTrainConfig()
    if not samples or cfg.epochs <= 0:
        return LinearUtilityModel()
    feature_count = len(samples[0].chosen)
    inv_scale = 1.0 / float(UTILITY_SCALE)
    weights = [0.0] * feature_count

    for _ in range(cfg.epochs):
        grad = [0.0] * feature_count
        total_weight = 0.0
        for sample in samples:
            if sample.sample_weight <= 0.0:
                continue
            margin = 0.0
            for i in range(feature_count):
                diff = float(sample.chosen[i] - sample.rejected[i]) * inv_scale
                margin += weights[i] * diff
            if margin >= 0.0:
                exp_neg = math.exp(-margin)
                prob_wrong = exp_neg / (1.0 + exp_neg)
            else:
                prob_wrong = 1.0 / (1.0 + math.exp(margin))
            factor = sample.sample_weight * prob_wrong
            for i in range(feature_count):
                diff = float(sample.chosen[i] - sample.rejected[i]) * inv_scale
                grad[i] -= factor * diff
            total_weight += sample.sample_weight
        if total_weight <= 0.0:
            break
        for i in range(feature_count):
            g = (grad[i] / total_weight) + cfg.l2_regularization * weights[i]
            updated = weights[i] - cfg.learning_rate * g
            weights[i] = max(-cfg.max_abs_weight, min(cfg.max_abs_weight, updated))

    quantized = tuple(int(math.floor(w * UTILITY_SCALE + 0.5)) if w >= 0 else -int(math.floor(-w * UTILITY_SCALE + 0.5)) for w in weights)
    return LinearUtilityModel(weights=quantized)


@dataclass(frozen=True, slots=True)
class DungeonMonsterAiProfile:
    """副本怪物战术配置（HFSM 血线阈值、自保冷却、技能表与定点效用权重）。"""

    enrage_hp_permyriad: int = 6000
    desperate_hp_permyriad: int = 3000
    emergency_defend_hp_permyriad: int = 2500
    defend_cooldown_rounds: int = 3
    emergency_heal_hp_permyriad: int = 0
    monster_skill_ids: tuple[int, ...] = ()
    utility_model: LinearUtilityModel = field(default_factory=LinearUtilityModel)


_DEFAULT_TRAINING_SAMPLES: tuple[PairwiseTrainingSample, ...] = (
    PairwiseTrainingSample((8000, 10000, 8000, 10000, 5000, 10000), (1000, 0, 2000, 0, 0, 3000)),
    PairwiseTrainingSample((7500, 10000, 6000, 10000, 5000, 10000), (7500, 10000, 6000, 0, 5000, 10000)),
    PairwiseTrainingSample((9000, 10000, 5000, 0, 0, 10000), (1500, 10000, 5000, 0, 0, 10000)),
    PairwiseTrainingSample((4000, 10000, 9000, 10000, 5000, 10000), (4000, 0, 3000, 10000, 5000, 10000)),
    PairwiseTrainingSample((5000, 10000, 7000, 0, 0, 10000), (5000, 10000, 7000, 0, 0, 3000)),
    PairwiseTrainingSample((6500, 10000, 7500, 10000, 10000, 10000), (2000, 0, 4000, 0, 10000, 10000)),
    PairwiseTrainingSample((3000, 10000, 8500, 10000, 5000, 10000), (3000, 10000, 2000, 10000, 5000, 10000)),
    PairwiseTrainingSample((8500, 0, 6000, 10000, 10000, 10000), (1000, 10000, 2000, 0, 0, 3000)),
)


def build_trained_default_monster_ai_profile(
    *,
    monster_skill_ids: Sequence[int] = (),
    enrage_hp_permyriad: int = 6000,
    desperate_hp_permyriad: int = 3000,
    emergency_defend_hp_permyriad: int = 2500,
    defend_cooldown_rounds: int = 3,
    emergency_heal_hp_permyriad: int = 0,
) -> DungeonMonsterAiProfile:
    """构建内置默认副本怪物战术配置（对照 `BuildTrainedDefaultMonsterAiProfile`）。"""
    model = train_pairwise_linear_model(_DEFAULT_TRAINING_SAMPLES)
    return DungeonMonsterAiProfile(
        enrage_hp_permyriad=enrage_hp_permyriad,
        desperate_hp_permyriad=desperate_hp_permyriad,
        emergency_defend_hp_permyriad=emergency_defend_hp_permyriad,
        defend_cooldown_rounds=defend_cooldown_rounds,
        emergency_heal_hp_permyriad=emergency_heal_hp_permyriad,
        monster_skill_ids=tuple(monster_skill_ids),
        utility_model=model,
    )


class DungeonMonsterAiController:
    """单只副本怪物的运行期 HFSM + 行为树 + 效用 AI 控制器（纯确定性，零随机数）。"""

    def __init__(self, profile: DungeonMonsterAiProfile):
        self.profile = profile
        self._phase: BossPhaseId = BossPhaseId.OPENING
        self.combat_enter_count: int = 1
        self.combat_exit_count: int = 0
        self._defend_cooldown_ready_round: int = 0

    @property
    def current_phase(self) -> BossPhaseId:
        """返回当前 HFSM 叶阶段。"""
        return self._phase

    @property
    def in_combat(self) -> bool:
        """返回怪物是否仍处于 Combat 父状态。"""
        return self._phase in (BossPhaseId.OPENING, BossPhaseId.ENRAGE, BossPhaseId.DESPERATE)

    def notify_dead(self) -> None:
        """单位阵亡时切入 Dead 态并记录一次 Combat 退出。"""
        if self._phase != BossPhaseId.DEAD:
            if self.in_combat:
                self.combat_exit_count += 1
            self._phase = BossPhaseId.DEAD

    def _update_phase(self, self_hp_permyriad: int) -> None:
        if self._phase == BossPhaseId.OPENING:
            if self_hp_permyriad <= self.profile.desperate_hp_permyriad:
                self._phase = BossPhaseId.DESPERATE
            elif self_hp_permyriad <= self.profile.enrage_hp_permyriad:
                self._phase = BossPhaseId.ENRAGE
        elif self._phase == BossPhaseId.ENRAGE:
            if self_hp_permyriad <= self.profile.desperate_hp_permyriad:
                self._phase = BossPhaseId.DESPERATE

    def decide_action(self, engine: TurnEngine, actor: Message) -> Message | None:
        """推进 HFSM 阶段并执行反应式行为树，返回选定的合法行动（无候选返回 `None`）。"""
        if not _active(actor):
            self.notify_dead()
            return None

        if actor.max_health > 0:
            clamped_hp = min(actor.attributes.health, actor.max_health)
            self_hp_permyriad = (clamped_hp * UTILITY_SCALE) // actor.max_health
        else:
            self_hp_permyriad = 0

        self._update_phase(self_hp_permyriad)

        # 行为树分支 0（可选战术自愈）：低于急救血线且持有合法治疗技能时优先治疗最残血友方/自身
        if self.profile.emergency_heal_hp_permyriad > 0 and self_hp_permyriad <= self.profile.emergency_heal_hp_permyriad:
            heal_action = self._choose_heal_action(engine, actor)
            if heal_action is not None:
                return heal_action

        # 行为树分支 1：绝境防御自保（Desperate 阶段 && HP <= 阈值 && 防御冷却就绪）
        if (
            self._phase == BossPhaseId.DESPERATE
            and self_hp_permyriad <= self.profile.emergency_defend_hp_permyriad
            and engine.round_index >= self._defend_cooldown_ready_round
        ):
            self._defend_cooldown_ready_round = engine.round_index + self.profile.defend_cooldown_rounds
            return engine.protocol.message("BattleAction", action_type=_ACTION_DEFEND, target_id=0)

        # 行为树分支 2：定点效用打分（枚举全部合法攻击/技能候选并按确定性决胜键选优）
        enemies = [other for other in engine.actors.values() if engine._enemy(actor, other)]
        if not enemies:
            return None

        max_enemy_speed = max(1, max((engine._effective_stat(enemy, 13) for enemy in enemies), default=1))
        phase_urgency = (
            UTILITY_SCALE
            if self._phase == BossPhaseId.DESPERATE
            else (UTILITY_SCALE // 2 if self._phase == BossPhaseId.ENRAGE else 0)
        )

        best_key: tuple[int, int, int, int] | None = None
        best_action: Message | None = None

        for target in enemies:
            if target.max_health > 0:
                clamped_target_hp = min(target.attributes.health, target.max_health)
                missing_hp = ((target.max_health - clamped_target_hp) * UTILITY_SCALE) // target.max_health
            else:
                missing_hp = 0
            controlled = engine.effects.has(target, STUN, SILENCE, FREEZE)
            uncontrolled = 0 if controlled else UTILITY_SCALE
            threat = min(UTILITY_SCALE, (engine._effective_stat(target, 13) * UTILITY_SCALE) // max_enemy_speed)
            target_is_player = UTILITY_SCALE if target.actor_type == _ACTOR_PLAYER else 3000

            candidates: list[tuple[Message, int, int]] = [
                (
                    engine.protocol.message("BattleAction", action_type=_ACTION_ATTACK, target_id=target.actor_id),
                    0,
                    _ACTION_ATTACK,
                )
            ]
            for skill_id in actor.skill_table_ids:
                rule = engine.skills.rules.get(skill_id)
                if rule is not None and (rule.heal_multiplier > 0 or rule.heal_per_level > 0):
                    continue
                candidates.append(
                    (
                        engine.protocol.message(
                            "BattleAction",
                            action_type=_ACTION_SKILL,
                            skill_table_id=skill_id,
                            target_id=target.actor_id,
                        ),
                        UTILITY_SCALE,
                        skill_id,
                    )
                )

            for action, is_skill_flag, action_id in candidates:
                if engine.validate(actor, action) != 0:
                    continue
                features = (missing_hp, uncontrolled, threat, is_skill_flag, phase_urgency, target_is_player)
                score = self.profile.utility_model.score(features)
                # 确定性排序键：分数降序 -> 阵位升序 -> target_id 升序 -> action_id 升序
                rank_key = (-score, target.formation_slot, target.actor_id, action_id)
                if best_key is None or rank_key < best_key:
                    best_key = rank_key
                    best_action = action

        return best_action

    @staticmethod
    def _choose_heal_action(engine: TurnEngine, actor: Message) -> Message | None:
        allies = [
            other
            for other in engine.actors.values()
            if _active(other) and other.team_index == actor.team_index
        ]
        if not allies:
            return None
        allies.sort(
            key=lambda other: (
                (other.attributes.health * UTILITY_SCALE // other.max_health) if other.max_health else UTILITY_SCALE,
                other.formation_slot,
                other.actor_id,
            )
        )
        for skill_id in actor.skill_table_ids:
            rule = engine.skills.rules.get(skill_id)
            if rule is None or not (rule.heal_multiplier > 0 or rule.heal_per_level > 0):
                continue
            for ally in allies:
                action = engine.protocol.message(
                    "BattleAction",
                    action_type=_ACTION_SKILL,
                    skill_table_id=skill_id,
                    target_id=ally.actor_id,
                )
                if engine.validate(actor, action) == 0:
                    return action
        return None


def choose_monster_action(
    engine: TurnEngine,
    actor: Message,
    controller_or_profile: DungeonMonsterAiController | DungeonMonsterAiProfile | None = None,
) -> Message | None:
    """为指定怪物选择战术行动；未配置战术档案时返回 `None` 交由既有默认逻辑兜底。"""
    if actor.actor_type != _ACTOR_MONSTER:
        return None
    if isinstance(controller_or_profile, DungeonMonsterAiController):
        return controller_or_profile.decide_action(engine, actor)
    if isinstance(controller_or_profile, DungeonMonsterAiProfile):
        return DungeonMonsterAiController(controller_or_profile).decide_action(engine, actor)
    return engine.monster_ai.decide_for_actor(actor)


class MonsterAiRuntime:
    """管理单场战斗内全部副本怪物 AI 控制器的绑定与回合选招。"""

    def __init__(
        self,
        engine: TurnEngine,
        profiles: Mapping[int, DungeonMonsterAiProfile] | None = None,
    ):
        self.engine = engine
        self.profiles: dict[int, DungeonMonsterAiProfile] = dict(profiles or {})
        self.controllers: dict[int, DungeonMonsterAiController] = {}

    @property
    def active(self) -> bool:
        """是否存在已绑定的怪物战术控制器。"""
        return bool(self.controllers)

    def set_profiles(self, profiles: Mapping[int, DungeonMonsterAiProfile]) -> None:
        """替换怪物战术档案并立即重新绑定场上怪物。"""
        self.profiles = dict(profiles)
        self.bind_actors()

    def bind_actors(self) -> None:
        """按 monster_table_id（回退到 0 号默认档）为怪物绑定控制器并挂载合法技能。"""
        self.controllers.clear()
        if not self.profiles:
            return
        fallback = self.profiles.get(0)
        for actor in self.engine.actors.values():
            if actor.actor_type != _ACTOR_MONSTER:
                continue
            profile = self.profiles.get(actor.monster_table_id, fallback)
            if profile is None:
                continue
            valid_skills: list[int] = []
            for skill_id in sorted(set(profile.monster_skill_ids)):
                if skill_id <= 0:
                    continue
                rule = self.engine.skills.rules.get(skill_id)
                if rule is None or not rule.castable:
                    continue
                if rule.class_id == 0 and rule.mana_cost > actor.max_mana:
                    continue
                valid_skills.append(skill_id)
            for skill_id in valid_skills:
                if skill_id not in actor.skill_table_ids:
                    actor.skill_table_ids.append(skill_id)
            self.controllers[actor.actor_id] = DungeonMonsterAiController(profile)

    def phase_of(self, actor_id: int) -> BossPhaseId | None:
        """查询指定怪物的当前 HFSM 阶段。"""
        controller = self.controllers.get(actor_id)
        return controller.current_phase if controller is not None else None

    def notify_dead(self, actor_id: int) -> None:
        """通知指定怪物的控制器切入 Dead 态。"""
        controller = self.controllers.get(actor_id)
        if controller is not None:
            controller.notify_dead()

    def decide_for_actor(self, actor: Message) -> Message | None:
        """为单个怪物执行控制器决策。"""
        controller = self.controllers.get(actor.actor_id)
        if controller is None:
            return None
        return controller.decide_action(self.engine, actor)

    def fill_actions(self) -> None:
        """为本回合尚未决定行动的绑定怪物填入战术指令。"""
        if not self.controllers:
            return
        for actor in self.engine.actors.values():
            if actor.actor_type != _ACTOR_MONSTER:
                continue
            controller = self.controllers.get(actor.actor_id)
            if controller is None:
                continue
            if not _active(actor):
                controller.notify_dead()
                continue
            if actor.actor_id in self.engine.pending:
                continue
            decided = controller.decide_action(self.engine, actor)
            if decided is not None:
                self.engine.pending[actor.actor_id] = decided


# ---------------------------------------------------------------------------
# 6. 五行试炼志战斗事实追踪与判定（对照 trial_journal_rules.h / turn_battle_engine.cpp）
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TrialContext:
    """轻量试炼志开局冻结上下文（兼容 `TrialBattleContext` proto 与 Python dataclass）。"""

    journal_entry_id: int = 0
    trial_rule_version: int = 1
    season_id: int = 0
    guard_monster_table_id: int = 0
    guard_stance_buff_id: int = 0
    challenge_vow_mask: int = 0


def normalize_artifact_effect_ids(raw: Iterable[int]) -> list[int]:
    """过滤并按升序规范化开局法宝效果 ID（合法区间 1..8，至多 2 槽）。"""
    out = sorted(eid for eid in raw if MIN_ARTIFACT_EFFECT_ID <= eid <= MAX_ARTIFACT_EFFECT_ID)
    return out[:MAX_ARTIFACT_SLOTS]


def is_player_victory(outcome: int, player_team_index: int) -> bool:
    """判断指定阵营玩家是否取得有效胜局。"""
    return (outcome == 1 and player_team_index == 0) or (outcome == 2 and player_team_index == 1)


def is_valid_trial_settlement(settlement: Message) -> bool:
    """校验结算数据是否满足五行试炼志权威结算前提（对照 `IsValidTrialSettlement`）。"""
    if (
        not settlement.battle_id
        or not settlement.player_id
        or settlement.fled
        or not settlement.total_rounds
        or settlement.player_team_index > 1
        or settlement.outcome not in (1, 2, 3)
        or not settlement.HasField("trial_facts")
    ):
        return False
    facts = settlement.trial_facts
    return bool(facts.journal_entry_id and facts.trial_rule_version and 1 <= facts.initial_human_count <= 5)


def evaluate_seal_condition(condition_type: int, condition_param: int, settlement: Message) -> bool:
    """按 C++ `trialjournal::EvaluateSealCondition` 判定单条战法印是否达成。"""
    if not is_valid_trial_settlement(settlement):
        return False
    facts = settlement.trial_facts
    won = is_player_victory(settlement.outcome, settlement.player_team_index)
    try:
        cond = SealConditionType(condition_type)
    except ValueError:
        return False
    if cond == SealConditionType.CLEAR:
        return won
    if cond == SealConditionType.SPEED_CLEAR:
        return won and condition_param > 0 and settlement.total_rounds <= condition_param
    if cond == SealConditionType.FLAWLESS_SELF_AND_PET:
        return won and not facts.self_or_pet_ever_died
    if cond == SealConditionType.NO_BOSS_GUARD_HEAL:
        return won and facts.boss_guard_heal_total == 0
    if cond == SealConditionType.BREAK_GUARD_STANCE:
        return won and (
            facts.guard_stance_dispelled
            or (facts.team_dealt_physical_direct_damage and facts.team_dealt_magic_direct_damage)
        )
    if cond == SealConditionType.WIND_DEFEND:
        return won and not facts.boss_wind_hit_unprotected_death and (
            condition_param == 0 or settlement.total_rounds <= condition_param
        )
    if cond == SealConditionType.BELL_HARMONY:
        return won and facts.dealt_yang_bell_physical_damage and facts.dealt_yin_drum_magic_damage
    if cond == SealConditionType.TIDE_FINISH:
        return won and facts.finished_in_ebb_tide
    if cond == SealConditionType.GUARD_RESCUED:
        return won and facts.guard_survived
    if cond == SealConditionType.POLARITY_WIN:
        return won
    if cond == SealConditionType.ARTIFACT_SHIELD_BREAK:
        return (won or settlement.participated) and facts.self_triggered_shield_break
    if cond == SealConditionType.ANTI_CONTROL_OR_SEASON_SKILL:
        return (won or settlement.participated) and facts.self_triggered_anti_control_or_season_skill
    if cond == SealConditionType.DUAL_GUARD_BREAK:
        return (
            won
            and (
                facts.guard_stance_dispelled
                or (facts.team_dealt_physical_direct_damage and facts.team_dealt_magic_direct_damage)
            )
            and (facts.boss_guard_heal_total == 0 or facts.heal_interrupt_backlash_count > 0)
        )
    if cond == SealConditionType.PERFECT_CLEAVE_PARRY:
        return won and facts.all_self_cleaves_parried
    if cond == SealConditionType.SEASON_SKILL_CAST:
        return (won or settlement.participated) and bool(getattr(facts, "self_cast_season_skill", False))
    if cond == SealConditionType.SEASON_RESONANCE:
        if not (won or settlement.participated):
            return False
        mask = int(getattr(facts, "self_season_resonance_mask", 0))
        if condition_param == 0:
            return bool(getattr(facts, "self_triggered_season_resonance", False)) or mask != 0
        return (mask & int(condition_param)) != 0
    return False


class TrialFactTracker:
    """追踪单场战斗全部试炼志与参与事实，并在终局冻结至 `BattleSettlementData.trial_facts`。"""

    def __init__(self, engine: TurnEngine, trial_context: Message | TrialContext | None = None):
        self.engine = engine
        self.context = self._normalize_context(trial_context)
        self.total_rounds: int = 0
        self.boss_guard_heal_total: int = 0
        self.guard_stance_dispelled: bool = False
        self.team_dealt_physical_direct_damage: list[bool] = [False, False]
        self.team_dealt_magic_direct_damage: list[bool] = [False, False]
        self.ever_died_actors: set[int] = set()
        self.player_deaths: dict[int, int] = {}
        self.min_hp_bps: dict[int, int] = {}
        self.shield_break_players: set[int] = set()
        self.anti_control_or_season_skill_players: set[int] = set()
        self.cast_season_skill_players: set[int] = set()
        self.season_resonance_mask_by_player: dict[int, int] = {}
        self.manual_action_rounds: dict[int, set[int]] = {}
        self.effective_contribution_players: set[int] = set()

        self._initial_human_counts: dict[int, int] = {0: 0, 1: 0}
        self._teammates_by_player: dict[int, list[str]] = {}
        self._used_pet_table_id: dict[int, int] = {}
        self._used_artifact_effect_ids: dict[int, tuple[int, ...]] = {}
        self._used_season_seal_id: dict[int, int] = {}

        # 卷二扩展事件标记
        self.boss_wind_hit_unprotected_death: bool = False
        self.dealt_yang_bell_physical_damage: bool = False
        self.dealt_yin_drum_magic_damage: bool = False
        self.finished_in_ebb_tide: bool = False

        # 首领试炼 V2 局内战法事实
        self.cleave_parry_count: int = 0
        self.heal_interrupt_backlash_count: int = 0
        self.stance_break_count: int = 0
        self.self_cleaves_targeted_by_player: dict[int, int] = {}
        self.self_cleaves_parried_by_player: dict[int, int] = {}

    @staticmethod
    def _normalize_context(ctx: Message | TrialContext | None) -> TrialContext:
        if ctx is None:
            return TrialContext()
        if isinstance(ctx, TrialContext):
            return ctx
        return TrialContext(
            journal_entry_id=int(getattr(ctx, "journal_entry_id", 0)),
            trial_rule_version=int(getattr(ctx, "trial_rule_version", 0)),
            season_id=int(getattr(ctx, "season_id", 0)),
            guard_monster_table_id=int(getattr(ctx, "guard_monster_table_id", 0)),
            guard_stance_buff_id=int(getattr(ctx, "guard_stance_buff_id", 0)),
            challenge_vow_mask=int(getattr(ctx, "challenge_vow_mask", 0)),
        )

    @property
    def enabled(self) -> bool:
        """当前对局是否携带有效的试炼志上下文。"""
        return self.context.journal_entry_id > 0

    def initial_human_count(self, team_index: int = 0) -> int:
        """返回指定阵营入场真人玩家数。"""
        return self._initial_human_counts.get(team_index, 0)

    def used_pet_table_id(self, player_id: int) -> int:
        """返回玩家开局出战宠物表 ID。"""
        return self._used_pet_table_id.get(player_id, 0)

    def used_artifact_effect_ids(self, player_id: int) -> tuple[int, ...]:
        """返回玩家开局佩戴的法宝效果 ID 元组（升序、至多 2 个）。"""
        return self._used_artifact_effect_ids.get(player_id, ())

    def used_season_seal_id(self, player_id: int) -> int:
        """返回玩家开局生效的赛季印 ID。"""
        return self._used_season_seal_id.get(player_id, 0)

    def initialize(self, request: Message) -> None:
        """在开局时冻结阵容人数、队友名、初始血线、出战宠、法宝与赛季印。"""
        players_by_team: dict[int, list[Message]] = {0: [], 1: []}
        no_pet_vow = getattr(self.engine, "has_trial_vow", lambda _b: False)(VOW_NO_PET)
        for snapshot in request.players:
            players_by_team.setdefault(snapshot.team_index, []).append(snapshot)
            actor = self.engine.actors.get(snapshot.player_id)
            if snapshot.pets and not (snapshot.team_index == 0 and no_pet_vow):
                pet = snapshot.pets[0]
                self._used_pet_table_id[snapshot.player_id] = pet.combat_pet_table_id or pet.pet_table_id
            else:
                self._used_pet_table_id[snapshot.player_id] = 0
            if actor is not None and actor.HasField("equipment_effects"):
                eff = actor.equipment_effects
                self._used_artifact_effect_ids[snapshot.player_id] = tuple(
                    normalize_artifact_effect_ids(art.effect_id for art in eff.artifacts)
                )
                self._used_season_seal_id[snapshot.player_id] = (
                    eff.season_seal.seal_id if eff.HasField("season_seal") else 0
                )
            else:
                self._used_artifact_effect_ids[snapshot.player_id] = ()
                self._used_season_seal_id[snapshot.player_id] = 0
            self.player_deaths[snapshot.player_id] = 0
            if actor is not None:
                self._sample_hp(actor)

        for team_index, members in players_by_team.items():
            self._initial_human_counts[team_index] = len(members)
            ordered = sorted(members, key=lambda snap: snap.player_id)
            for member in ordered:
                names = [
                    other.player_name
                    for other in ordered
                    if other.player_id != member.player_id and other.player_name
                ]
                self._teammates_by_player[member.player_id] = names[:MAX_COOP_TEAMMATES]

    def _sample_hp(self, actor: Message) -> None:
        if actor.actor_type != _ACTOR_PLAYER:
            return
        bps = (min(actor.attributes.health, actor.max_health) * 10_000) // actor.max_health if actor.max_health else 0
        previous = self.min_hp_bps.get(actor.actor_id)
        if previous is None or bps < previous:
            self.min_hp_bps[actor.actor_id] = bps

    def _owner_player_id(self, actor: Message) -> int:
        if actor.actor_type == _ACTOR_PLAYER:
            return actor.actor_id
        if actor.actor_type == _ACTOR_PET:
            return actor.owner_player_id
        return 0

    def record_manual_action(self, player_id: int, round_index: int) -> None:
        """记录玩家在指定回合提交了有效手动指令。"""
        if player_id > 0:
            self.manual_action_rounds.setdefault(player_id, set()).add(round_index)

    def record_effective_contribution(self, actor: Message) -> None:
        """记录玩家或己方宠物产生了有效战斗贡献。"""
        owner_id = self._owner_player_id(actor)
        if owner_id > 0:
            self.effective_contribution_players.add(owner_id)

    def record_direct_damage(self, source: Message, target: Message, damage_kind: int, dealt: int) -> None:
        """记录敌我直接伤害（更新双系直伤标记、卷四阳钟阴鼓、有效贡献与最低血线）。"""
        if dealt <= 0 or source.team_index == target.team_index or source.team_index >= 2:
            return
        boss_kind = getattr(getattr(self.engine, "encounter", None), "boss_trial_kind", BOSS_TRIAL_KIND_NONE)
        if damage_kind == _DAMAGE_PHYSICAL:
            self.team_dealt_physical_direct_damage[source.team_index] = True
            if (
                source.team_index == 0
                and target.team_index == 1
                and boss_kind == BOSS_TRIAL_KIND_WILD_BELL
                and is_wild_bell_yang_round(self.engine.round_index)
            ):
                self.dealt_yang_bell_physical_damage = True
        elif damage_kind == _DAMAGE_MAGIC:
            self.team_dealt_magic_direct_damage[source.team_index] = True
            if (
                source.team_index == 0
                and target.team_index == 1
                and boss_kind == BOSS_TRIAL_KIND_WILD_BELL
                and not is_wild_bell_yang_round(self.engine.round_index)
            ):
                self.dealt_yin_drum_magic_damage = True
        self.record_effective_contribution(source)
        self._sample_hp(target)

    def record_monster_heal(self, source: Message, target: Message, healed: int) -> None:
        """记录敌方护卫对首领的有效治疗总量（对照 `RecordTrialMonsterHeal`）。"""
        if (
            healed <= 0
            or source.actor_type != _ACTOR_MONSTER
            or target.actor_type != _ACTOR_MONSTER
            or not target.is_boss
            or source.actor_id == target.actor_id
        ):
            return
        guard_id = self.context.guard_monster_table_id
        if guard_id == 0 or source.monster_table_id == guard_id:
            self.boss_guard_heal_total = min(U64_MAX, self.boss_guard_heal_total + healed)

    def record_guard_stance_dispelled(self, buff_table_id: int = 0) -> None:
        """记录己方成功驱散敌方护势状态。"""
        expected = self.context.guard_stance_buff_id
        if expected == 0 or buff_table_id == expected:
            self.guard_stance_dispelled = True

    def record_shield_break(self, source: Message) -> None:
        """记录玩家（或其出战宠）触发了有效削盾。"""
        owner_id = self._owner_player_id(source)
        if owner_id > 0:
            self.shield_break_players.add(owner_id)
            self.effective_contribution_players.add(owner_id)

    def record_anti_control_or_season_skill(self, actor: Message) -> None:
        """记录玩家触发清心防控制、净化或当季免费赛季技能。"""
        if actor.actor_type == _ACTOR_PLAYER:
            self.anti_control_or_season_skill_players.add(actor.actor_id)
            self.effective_contribution_players.add(actor.actor_id)

    def record_season_skill_cast(self, actor: Message) -> None:
        """记录玩家主动施放了当季免费赛季技能（同时计入 anti_control_or_season_skill）。"""
        if actor.actor_type == _ACTOR_PLAYER:
            self.cast_season_skill_players.add(actor.actor_id)
            self.anti_control_or_season_skill_players.add(actor.actor_id)
            self.effective_contribution_players.add(actor.actor_id)

    def record_season_resonance(self, actor: Message, resonance_kind: int) -> None:
        """记录玩家触发技印共鸣 R1..R8（bit = resonance_kind - 1）。"""
        if actor.actor_type == _ACTOR_PLAYER and 1 <= resonance_kind <= 31:
            mask = self.season_resonance_mask_by_player.get(actor.actor_id, 0) | (1 << (resonance_kind - 1))
            self.season_resonance_mask_by_player[actor.actor_id] = mask
            self.effective_contribution_players.add(actor.actor_id)

    def record_death(self, target: Message) -> None:
        """记录单位阵亡事实（复活不清除）并更新玩家死亡计数与最低血线。"""
        self.ever_died_actors.add(target.actor_id)
        if target.actor_type == _ACTOR_PLAYER:
            self.player_deaths[target.actor_id] = self.player_deaths.get(target.actor_id, 0) + 1
            self.min_hp_bps[target.actor_id] = 0

    def on_round_complete(self, resolved_round: int) -> None:
        """每回合结算完毕刷新已结算回合数与全员血线水位。"""
        self.total_rounds = max(self.total_rounds, resolved_round)
        for actor in self.engine.actors.values():
            if actor.actor_type == _ACTOR_PLAYER:
                self._sample_hp(actor)

    def populate_settlement(
        self,
        settlement: Message,
        player_id: int,
        *,
        journal_entry_id: int | None = None,
        trial_rule_version: int | None = None,
        season_id: int | None = None,
    ) -> Message | None:
        """将当前冻结的战斗事实写入 `BattleSettlementData.trial_facts`（对照 `BuildSettlement`）。"""
        entry_id = self.context.journal_entry_id if journal_entry_id is None else journal_entry_id
        rule_version = self.context.trial_rule_version if trial_rule_version is None else trial_rule_version
        effective_season = self.context.season_id if season_id is None else season_id
        if entry_id <= 0:
            return None

        actor = self.engine.actors.get(player_id)
        if actor is None:
            return None

        facts = settlement.trial_facts
        facts.Clear()
        facts.journal_entry_id = entry_id
        facts.trial_rule_version = rule_version or 1
        facts.season_id = effective_season
        facts.initial_human_count = self._initial_human_counts.get(actor.team_index, 1)

        self_or_pet_died = player_id in self.ever_died_actors
        guard_survived = False
        guard_id = self.context.guard_monster_table_id
        for other in self.engine.actors.values():
            if other.actor_type == _ACTOR_PET and other.owner_player_id == player_id and other.actor_id in self.ever_died_actors:
                self_or_pet_died = True
            if (
                other.actor_type == _ACTOR_MONSTER
                and other.team_index != actor.team_index
                and not other.is_boss
                and not other.is_dead
                and (guard_id == 0 or other.monster_table_id == guard_id)
            ):
                guard_survived = True

        my_team = actor.team_index
        facts.self_or_pet_ever_died = self_or_pet_died
        facts.boss_guard_heal_total = self.boss_guard_heal_total
        facts.guard_stance_dispelled = self.guard_stance_dispelled
        facts.team_dealt_physical_direct_damage = my_team < 2 and self.team_dealt_physical_direct_damage[my_team]
        facts.team_dealt_magic_direct_damage = my_team < 2 and self.team_dealt_magic_direct_damage[my_team]
        facts.guard_survived = guard_survived
        facts.self_triggered_shield_break = player_id in self.shield_break_players
        facts.self_triggered_anti_control_or_season_skill = player_id in self.anti_control_or_season_skill_players
        live_pet_table_id = 0
        for other in self.engine.actors.values():
            if other.actor_type == _ACTOR_PET and other.owner_player_id == player_id:
                live_pet_table_id = other.pet_table_id
                break
        if live_pet_table_id and not self.engine.uses_free_preset:
            facts.used_pet_table_id = live_pet_table_id
        elif not getattr(self.engine, "has_trial_vow", lambda _b: False)(VOW_NO_PET):
            facts.used_pet_table_id = self._used_pet_table_id.get(player_id, 0)
        facts.used_artifact_effect_ids.extend(self._used_artifact_effect_ids.get(player_id, ()))
        if actor.HasField("equipment_effects") and actor.equipment_effects.HasField("season_seal"):
            facts.used_season_seal_id = actor.equipment_effects.season_seal.seal_id
        else:
            facts.used_season_seal_id = self._used_season_seal_id.get(player_id, 0)
        facts.teammate_names.extend(self._teammates_by_player.get(player_id, ()))
        facts.boss_wind_hit_unprotected_death = self.boss_wind_hit_unprotected_death
        facts.dealt_yang_bell_physical_damage = self.dealt_yang_bell_physical_damage
        facts.dealt_yin_drum_magic_damage = self.dealt_yin_drum_magic_damage
        boss_kind = getattr(getattr(self.engine, "encounter", None), "boss_trial_kind", BOSS_TRIAL_KIND_NONE)
        facts.finished_in_ebb_tide = self.finished_in_ebb_tide or (
            boss_kind == BOSS_TRIAL_KIND_WILD_TIDE
            and self.engine.outcome == 1
            and is_wild_tide_ebb_round(self.engine.round_index)
        )
        facts.challenge_vow_mask = int(getattr(self.engine, "trial_vow_mask", 0))
        facts.cleave_parry_count = self.cleave_parry_count
        facts.heal_interrupt_backlash_count = self.heal_interrupt_backlash_count
        facts.stance_break_count = self.stance_break_count
        self_targeted = self.self_cleaves_targeted_by_player.get(player_id, 0)
        self_parried = self.self_cleaves_parried_by_player.get(player_id, 0)
        facts.self_parried_cleave_at_least_once = self_parried > 0
        facts.all_self_cleaves_parried = self_targeted > 0 and self_parried == self_targeted
        facts.self_cast_season_skill = player_id in self.cast_season_skill_players
        resonance_mask = self.season_resonance_mask_by_player.get(player_id, 0)
        facts.self_triggered_season_resonance = resonance_mask != 0
        facts.self_season_resonance_mask = resonance_mask

        settlement.manual_action_rounds = len(self.manual_action_rounds.get(player_id, ()))
        settlement.effective_combat_contribution = player_id in self.effective_contribution_players
        return facts


# ---------------------------------------------------------------------------
# 7. 首领试炼 V2 / 瑞兽迎春 / 物法易势 / 护卫救援局内机制控制器
# ---------------------------------------------------------------------------


class EncounterV2Controller:
    """承载首领试炼 V2、瑞兽迎春、护卫救援与物法易势 5V5 的确定性机制状态机。"""

    def __init__(
        self,
        engine: TurnEngine,
        request: Message,
        *,
        boss_trial_kind: int = BOSS_TRIAL_KIND_NONE,
        battle_difficulty: int = 0,
    ):
        self.engine = engine
        self.request = request
        self.boss_trial_kind = boss_trial_kind
        self.battle_difficulty = battle_difficulty
        self.initial_attacker_human_count = sum(1 for p in request.players if p.team_index == 0)
        self.protected_guard_actor_id: int = 0
        self.initial_polarity: int = 0
        self.active_polarity: int = 0
        self.boss_stance_type: int = BOSS_STANCE_NONE
        self.boss_stance_remain_rounds: int = 0
        self.last_chosen_stance_type: int = BOSS_STANCE_NONE
        self.boss_barrier_shield: int = 0
        self.boss_phase: int = 1 if is_boss_trial_kind(boss_trial_kind) else 0
        self.next_phase2_cleave_telegraph_round: int = 0
        self.boss_momentum_stacks: int = 0
        self.boss_disheartened_until_round: int = 0
        self.exposed_actors: dict[int, tuple[int, int]] = {}
        self.sunder_until_round: dict[int, int] = {}
        self.attacker_triggered_stance_counter_this_round: set[int] = set()
        self.round_player_direct_damage: dict[int, list[int]] = {}
        self.active_telegraphs: list[Message] = []
        self.last_resolved_telegraphs: list[Message] = []
        self.telegraph_state_version: int = 0
        self.boss_weekly_affix_id: int = resolve_boss_weekly_affix(
            is_boss_trial_kind(boss_trial_kind),
            engine.tower_context is not None,
            int(getattr(request, "created_at_ms", 0)),
            int(getattr(request, "boss_weekly_affix_id", 0)),
        )
        self.weekly_affix_wood_team_boost_until_round: int = 0
        self.weekly_affix_wood_mana_triggered_actors: set[int] = set()
        self.weekly_affix_earth_team_boost_until_round: int = 0

    @property
    def is_festival_beast(self) -> bool:
        return (
            self.boss_trial_kind == BOSS_TRIAL_KIND_FESTIVAL_BEAST
            and self.request.HasField("activity_context")
            and self.request.activity_context.kind == 2
            and self.request.activity_context.HasField("festival_beast")
        )

    @property
    def is_boss_trial(self) -> bool:
        return is_boss_trial_kind(self.boss_trial_kind)

    @property
    def has_active_damage_modifiers(self) -> bool:
        return self.active_polarity != 0 or self.is_boss_trial

    def _find_alive_boss(self) -> Message | None:
        for actor in self.engine.actors.values():
            if actor.actor_type == _ACTOR_MONSTER and actor.team_index == 1 and actor.is_boss and _active(actor):
                return actor
        return None

    def _collect_alive_enemies(self, actor: Message) -> list[int]:
        return sorted(
            other.actor_id
            for other in self.engine.actors.values()
            if _active(other) and other.team_index != actor.team_index
        )

    def _rand_index(self, bound: int) -> int:
        return 0 if bound <= 1 else int(self.engine.rng.next() % bound)

    def init_special_encounter_state(self) -> None:
        """开局初始化物法易势极性、护卫救援目标或首领试炼/瑞兽首轮预告。"""
        if (
            self.boss_trial_kind == BOSS_TRIAL_KIND_POLARITY_5V5
            or self.request.match_mode == MATCH_MODE_POLARITY_5V5
            or getattr(self.engine, "custom_duel_polarity_enabled", False)
        ):
            self.initial_polarity = _DAMAGE_PHYSICAL if self._rand_index(2) == 0 else _DAMAGE_MAGIC
            self.update_polarity_for_round()
            return
        if self.boss_trial_kind == BOSS_TRIAL_KIND_NONE:
            return
        if self.boss_trial_kind == BOSS_TRIAL_KIND_GUARDIAN_RESCUE:
            for actor in self.engine.actors.values():
                if actor.actor_type == _ACTOR_MONSTER and actor.team_index == 0 and not actor.is_boss:
                    self.protected_guard_actor_id = actor.actor_id
                    break
        self.refresh_encounter_telegraphs()

    def update_polarity_for_round(self) -> None:
        if (
            self.boss_trial_kind != BOSS_TRIAL_KIND_POLARITY_5V5
            and self.request.match_mode != MATCH_MODE_POLARITY_5V5
            and not getattr(self.engine, "custom_duel_polarity_enabled", False)
        ):
            self.active_polarity = 0
            return
        if self.initial_polarity == 0:
            self.initial_polarity = _DAMAGE_PHYSICAL
        block = max(0, self.engine.round_index - 1) // 2
        if block % 2 == 0:
            self.active_polarity = self.initial_polarity
        else:
            self.active_polarity = _DAMAGE_MAGIC if self.initial_polarity == _DAMAGE_PHYSICAL else _DAMAGE_PHYSICAL

    def next_polarity_switch_round(self) -> int:
        if self.active_polarity == 0:
            return 0
        block = max(0, self.engine.round_index - 1) // 2
        next_round = (block + 1) * 2 + 1
        return next_round if next_round <= self.engine.max_rounds else 0

    def is_actor_control_locked(self, actor: Message) -> bool:
        """判定单位是否处于硬控/封印状态（受控时直接取消蓄力或护卫机制）。"""
        if self.engine.effects.has(actor, STUN, SILENCE, FREEZE):
            return True
        for entry in actor.buffs:
            rule = self.engine.effects.rules.get(entry.buff_table_id)
            if rule is not None and rule.battle_op == 13 and rule.state_kind in (1, 2, 3, 4, 5, 6):
                return True
        return False

    def incoming_heal_modifier(self, actor: Message) -> float:
        """汇总目标身上的受疗修正比例（用于识别断脉咒压制回春）。"""
        total = 0.0
        for entry in actor.buffs:
            rule = self.engine.effects.rules.get(entry.buff_table_id)
            if rule is not None and rule.battle_op == 6 and rule.modifier_stat == 6:
                total += entry.stat_percent if entry.stat_percent != 0.0 else rule.ratio
        return total

    def is_slow_phase_actor(self, actor_id: int) -> bool:
        return any(
            entry.execute_round == self.engine.round_index and entry.source_actor_id == actor_id
            for entry in self.active_telegraphs
        )

    def emit_boss_mechanic_event(
        self, turn: Message, group: int, mechanic_kind: int, source_id: int, target_id: int, value: int,
    ) -> Message:
        return turn.events.add(
            event_type=EVENT_BOSS_MECHANIC,
            source_id=source_id,
            target_id=target_id,
            group_id=group,
            mechanic_kind=mechanic_kind,
            value=value,
        )

    def apply_boss_exposed(self, actor_id: int, pct: int, until_round: int) -> None:
        if not actor_id or not pct:
            return
        cur_pct, cur_until = self.exposed_actors.get(actor_id, (0, 0))
        new_pct = pct if pct > cur_pct else cur_pct
        self.exposed_actors[actor_id] = (new_pct, max(cur_until, until_round))

    def actor_has_boss_cleave_parry_protection(self, target: Message, damage_type: int) -> bool:
        """判定目标是否持有合法重劈化解保护（无敌/护盾/分摊/反震）。"""
        for entry in target.buffs:
            if entry.remaining_capacity > 0:
                return True
            rule = self.engine.effects.rules.get(entry.buff_table_id)
            if rule is None:
                continue
            if rule.buff_type == 34 or "Invincibility" in rule.tags:
                return True
            if rule.battle_op in (9, 10, 11, 12):
                flt = rule.damage_filter
                if flt == 0 or (flt == 1 and damage_type == _DAMAGE_PHYSICAL) or (flt == 2 and damage_type == _DAMAGE_MAGIC):
                    return True
        return False

    def effective_physical_attack_multiplier(self, actor: Message) -> float:
        if actor.is_boss and self.boss_disheartened_until_round >= self.engine.round_index:
            return 0.85
        return 1.0

    def effective_defense_multiplier(self, actor: Message) -> float:
        if self.sunder_until_round.get(actor.actor_id, 0) >= self.engine.round_index:
            return 0.80
        return 1.0

    def record_player_direct_damage(self, source: Message, target: Message, damage_kind: int, dealt: int) -> None:
        if dealt <= 0 or source.team_index != 0 or target.team_index != 1:
            return
        tally = self.round_player_direct_damage.setdefault(self.engine.round_index, [0, 0])
        if damage_kind == _DAMAGE_PHYSICAL:
            tally[0] = min(U64_MAX, tally[0] + dealt)
        elif damage_kind == _DAMAGE_MAGIC:
            tally[1] = min(U64_MAX, tally[1] + dealt)

    def choose_dynamic_stance_type(self, current_round: int) -> int:
        if current_round <= 1:
            return BOSS_STANCE_PHYSICAL
        if self.battle_difficulty < 2:
            return BOSS_STANCE_PHYSICAL if (((current_round - 1) // 3) % 2 == 0) else BOSS_STANCE_MAGIC
        phys_total = 0
        magic_total = 0
        start_r = max(1, current_round - 2)
        for r in range(start_r, current_round):
            tally = self.round_player_direct_damage.get(r)
            if tally is not None:
                phys_total = min(U64_MAX, phys_total + tally[0])
                magic_total = min(U64_MAX, magic_total + tally[1])
        if phys_total > magic_total:
            return BOSS_STANCE_PHYSICAL
        if magic_total > phys_total:
            return BOSS_STANCE_MAGIC
        return BOSS_STANCE_MAGIC if self.last_chosen_stance_type == BOSS_STANCE_PHYSICAL else BOSS_STANCE_PHYSICAL

    def apply_encounter_damage_modifiers(
        self,
        source: Message,
        target: Message,
        damage: int,
        damage_type: int,
        is_direct_primary: bool,
        turn: Message,
        group: int,
        *,
        is_multi_target_skill: bool = False,
        is_primary_target: bool = True,
    ) -> tuple[int, bool]:
        """按 C++ `ApplyEncounterDamageModifiers` 依次结算易势、溅射、联防、破绽/破釜易伤与灵障盾。"""
        splash_reduced = False
        if damage <= 0:
            return 0, False
        dmg_stance_polarity = (
            BOSS_STANCE_PHYSICAL if damage_type == _DAMAGE_PHYSICAL
            else (BOSS_STANCE_MAGIC if damage_type == _DAMAGE_MAGIC else BOSS_STANCE_NONE)
        )
        # 1. 物法易势 5V5 同系直伤 ×1.15
        if is_direct_primary and self.active_polarity != 0 and self.active_polarity == damage_type:
            damage = (damage * POLARITY_DAMAGE_NUM) // POLARITY_DAMAGE_DEN
        # 1.8 卷四·三境明雷野外首领机制（碧渊水宫·阳钟阴鼓 / 月汐湾·涨退潮，纯整数定点数倍率）。
        if (
            is_direct_primary
            and self.boss_trial_kind == BOSS_TRIAL_KIND_WILD_BELL
            and source.team_index == 0
            and target.team_index == 1
        ):
            yang_bell = is_wild_bell_yang_round(self.engine.round_index)
            if (yang_bell and damage_type == _DAMAGE_PHYSICAL) or (not yang_bell and damage_type == _DAMAGE_MAGIC):
                damage = (damage * 115) // 100
            elif damage_type in (_DAMAGE_PHYSICAL, _DAMAGE_MAGIC):
                damage = (damage * 88) // 100
        if (
            self.boss_trial_kind == BOSS_TRIAL_KIND_WILD_TIDE
            and source.team_index == 0
            and target.team_index == 1
            and target.is_boss
        ):
            damage = (
                (damage * 120) // 100
                if is_wild_tide_ebb_round(self.engine.round_index)
                else (damage * 75) // 100
            )
        # 2. 首领试炼 V2 多目标直伤副目标溅射保留 60%
        if (
            is_direct_primary
            and self.is_boss_trial
            and source.team_index == 0
            and target.team_index == 1
            and is_multi_target_skill
            and not is_primary_target
        ):
            damage = (damage * BOSS_TRIAL_SPLASH_KEEP_PCT) // 100
            splash_reduced = True
        # 3. 破阵·护势联防阵：同系直伤减免 + 轻反制叠蓄势（每攻方单位每回合至多 1 层）
        if (
            is_direct_primary
            and is_boss_stance_trial_kind(self.boss_trial_kind)
            and target.team_index == 1
            and self.boss_stance_type != BOSS_STANCE_NONE
            and self.boss_stance_remain_rounds > 0
            and self.boss_stance_type == dmg_stance_polarity
        ):
            keep_pct = (
                65 if self.boss_trial_kind == BOSS_TRIAL_KIND_BREAK_DUAL_GUARD
                else (60 if self.battle_difficulty >= 2 else 65)
            )
            damage = (damage * keep_pct) // 100
            if source.team_index == 0 and source.actor_id not in self.attacker_triggered_stance_counter_this_round:
                self.attacker_triggered_stance_counter_this_round.add(source.actor_id)
                if self.boss_momentum_stacks < BOSS_MOMENTUM_MAX_STACKS:
                    self.boss_momentum_stacks += 1
                    boss = self._find_alive_boss()
                    boss_id = boss.actor_id if boss is not None else target.actor_id
                    self.emit_boss_mechanic_event(
                        turn, group, BOSS_MECHANIC_MOMENTUM_GAIN, target.actor_id, boss_id, self.boss_momentum_stacks,
                    )
        # 4. 破绽 (+35%/+20%) + 二阶段决战姿态 (+15%) 加算易伤（含直伤与持续毒火）
        if self.is_boss_trial and source.team_index != target.team_index and target.team_index == 1:
            vuln_pct = 0
            exp = self.exposed_actors.get(target.actor_id)
            target_exposed = exp is not None and exp[1] >= self.engine.round_index
            if target_exposed:
                vuln_pct += exp[0]
            if target.is_boss and self.boss_phase >= 2:
                vuln_pct += BOSS_PHASE_TWO_VULN_PCT
            if vuln_pct > 0:
                damage = (damage * (100 + vuln_pct)) // 100
            # 首领试炼周常五行词缀攻方增伤（与破绽独立乘算）
            if source.team_index == 0 and damage > 0:
                if (
                    self.boss_weekly_affix_id == BOSS_AFFIX_METAL
                    and is_direct_primary
                    and is_primary_target
                    and target_exposed
                ):
                    damage = (damage * (100 + BOSS_AFFIX_METAL_EXPOSED_PRIMARY_BONUS_PCT)) // 100
                elif (
                    self.boss_weekly_affix_id == BOSS_AFFIX_WOOD
                    and self.weekly_affix_wood_team_boost_until_round >= self.engine.round_index
                ):
                    damage = (damage * (100 + BOSS_AFFIX_WOOD_TEAM_DAMAGE_BONUS_PCT)) // 100
                elif (
                    self.boss_weekly_affix_id == BOSS_AFFIX_FIRE
                    and is_direct_primary
                    and source.actor_type == _ACTOR_PET
                ):
                    damage = (damage * (100 + BOSS_AFFIX_FIRE_PET_OR_FOLLOWUP_DAMAGE_BONUS_PCT)) // 100
                elif (
                    self.boss_weekly_affix_id == BOSS_AFFIX_EARTH
                    and self.weekly_affix_earth_team_boost_until_round >= self.engine.round_index
                ):
                    damage = (damage * (100 + BOSS_AFFIX_EARTH_PARRY_NEXT_ROUND_DAMAGE_BONUS_PCT)) // 100
        # 5. 灵障首领：灵障护盾吸收
        if self.boss_trial_kind == BOSS_TRIAL_KIND_BARRIER and target.is_boss and self.boss_barrier_shield > 0 and damage > 0:
            absorbed = min(damage, self.boss_barrier_shield)
            self.boss_barrier_shield -= absorbed
            damage -= absorbed
            turn.events.add(
                event_type=_EVENT_SHIELD,
                source_id=source.actor_id,
                target_id=target.actor_id,
                group_id=group,
                absorbed=absorbed,
                shield_after=self.boss_barrier_shield,
            )
            if self.boss_barrier_shield == 0:
                self.engine.trial_tracker.record_shield_break(source)
                # 灵障碎障破绽：击破首领灵障护盾立即触发完整「破绽（+35%，至下回合末）」
                self.apply_boss_exposed(target.actor_id, BOSS_EXPOSED_FULL_PCT, self.engine.round_index + 1)
                self.engine.trial_tracker.stance_break_count += 1
                self.emit_boss_mechanic_event(
                    turn, group, BOSS_MECHANIC_STANCE_BREAK, source.actor_id, target.actor_id, BOSS_EXPOSED_FULL_PCT,
                )
        return damage, splash_reduced

    def apply_breaker_sunder_on_basic_hit(
        self, attacker: Message, target: Message, dealt: int, hit_kind: int, turn: Message, group: int,
    ) -> None:
        if (
            dealt > 0
            and hit_kind == 0
            and _active(target)
            and self.is_boss_trial
            and attacker.actor_type == _ACTOR_MONSTER
            and attacker.team_index == 1
            and attacker.monster_table_id == BOSS_TRIAL_MONSTER_SUPPORT_BREAKER
        ):
            self.sunder_until_round[target.actor_id] = self.engine.round_index + 2
            self.emit_boss_mechanic_event(turn, group, BOSS_MECHANIC_BREAKER_SUNDER, attacker.actor_id, target.actor_id, 20)

    def clear_sunder_on_ally_dispel(
        self, caster: Message, target: Message, turn: Message | None = None, group: int = 0,
    ) -> None:
        if self.sunder_until_round.pop(target.actor_id, None) is not None:
            self.engine.trial_tracker.record_effective_contribution(caster)
            if turn is not None:
                self.on_boss_weekly_affix_dispel_triggered(caster.actor_id, turn, group)

    def is_actor_exposed(self, actor_id: int) -> bool:
        """判定指定单位当前回合是否处于首领试炼破绽状态。"""
        if not actor_id:
            return False
        exp = self.exposed_actors.get(actor_id)
        return exp is not None and exp[0] > 0 and exp[1] >= self.engine.round_index

    def is_weekly_water_discount_skill(self, rule: BattleSkill) -> bool:
        """判定技能是否满足水周（水·蓄势聚灵）蓄力/毒火耗蓝减免与增伤条件。"""
        buff_rules = self.engine.effects.rules
        for effect_buff_id in rule.effects:
            buff_row = buff_rules.get(effect_buff_id)
            if buff_row is None:
                continue
            if (
                buff_row.battle_op in (
                    BattleEffectOp.PREPARE_CAST,
                    BattleEffectOp.PERIODIC_DAMAGE,
                    BattleEffectOp.CONSUME_PERIODIC,
                    BattleEffectOp.EXTEND_PERIODIC,
                )
                or buff_row.buff_type in (50, 51)
            ):
                return True
        return False

    def on_boss_weekly_affix_dispel_triggered(self, caster_actor_id: int, turn: Message, group: int) -> None:
        """木周（木·灵脉涤尘）：成功驱散/净化时，赋予全队增伤至下回合末并每人限一次回复 12% 法力。"""
        if self.boss_weekly_affix_id != BOSS_AFFIX_WOOD or not caster_actor_id:
            return
        caster = self.engine.actors.get(caster_actor_id)
        if caster is None or caster.team_index != 0 or not _active(caster):
            return
        self.weekly_affix_wood_team_boost_until_round = max(
            self.weekly_affix_wood_team_boost_until_round, self.engine.round_index + 1,
        )
        if caster.max_mana <= 0 or caster_actor_id in self.weekly_affix_wood_mana_triggered_actors:
            return
        self.weekly_affix_wood_mana_triggered_actors.add(caster_actor_id)
        raw_gain = max(1, (caster.max_mana * BOSS_AFFIX_WOOD_DISPEL_MANA_REGEN_PCT) // 100)
        cur_mana = caster.attributes.mana
        missing = max(0, caster.max_mana - cur_mana)
        gained = min(raw_gain, missing)
        if gained > 0:
            caster.attributes.mana = cur_mana + gained
            turn.events.add(
                event_type=_EVENT_MANA,
                source_id=caster_actor_id,
                target_id=caster_actor_id,
                group_id=group,
                value=gained,
                mana_delta=min(gained, _I64_MAX),
                target_health_after=caster.attributes.health,
                target_mana_after=caster.attributes.mana,
            )
        self.emit_boss_mechanic_event(turn, group, BOSS_MECHANIC_WEEKLY_AFFIX_TRIGGER, caster_actor_id, caster_actor_id, gained)

    def on_boss_weekly_affix_parry_triggered(self, boss_actor_id: int, turn: Message, group: int) -> None:
        """土周（土·守御反震）：成功化劲触发破绽时，赐予我方全体 8% 最大气血开场护盾及下回合全伤 +12%。"""
        if self.boss_weekly_affix_id != BOSS_AFFIX_EARTH:
            return
        self.weekly_affix_earth_team_boost_until_round = max(
            self.weekly_affix_earth_team_boost_until_round, self.engine.round_index + 1,
        )
        shielded_count = 0
        for actor in self.engine.actors.values():
            if actor.team_index != 0 or not _active(actor) or actor.max_health <= 0:
                continue
            gain = max(1, (actor.max_health * BOSS_AFFIX_EARTH_PARRY_TEAM_SHIELD_PCT) // 100)
            pool = min(U64_MAX, self.engine.opening_shield_by_actor.get(actor.actor_id, 0) + gain)
            self.engine.opening_shield_by_actor[actor.actor_id] = pool
            turn.events.add(
                event_type=_EVENT_SHIELD,
                source_id=actor.actor_id,
                target_id=actor.actor_id,
                group_id=group,
                shield_after=pool,
            )
            shielded_count += 1
        if shielded_count > 0:
            self.emit_boss_mechanic_event(turn, group, BOSS_MECHANIC_WEEKLY_AFFIX_TRIGGER, boss_actor_id, boss_actor_id, shielded_count)

    def trigger_rejuv_backlash(self, source_id: int, boss: Message, turn: Message, group: int) -> None:
        if not _active(boss):
            return
        raw_backlash = max(1, (boss.max_health * BOSS_HEAL_BACKLASH_PCT) // 100)
        dealt = min(boss.attributes.health, raw_backlash)
        if dealt > 0:
            boss.attributes.health -= dealt
        self.engine.trial_tracker.heal_interrupt_backlash_count += 1
        self.apply_boss_exposed(boss.actor_id, BOSS_EXPOSED_FULL_PCT, self.engine.round_index + 1)
        self.emit_boss_mechanic_event(turn, group, BOSS_MECHANIC_HEAL_BACKLASH, source_id, boss.actor_id, dealt)
        turn.events.add(
            event_type=3,
            source_id=source_id,
            target_id=boss.actor_id,
            group_id=group,
            value=dealt,
            target_health_after=boss.attributes.health,
            target_mana_after=boss.attributes.mana,
        )
        if boss.attributes.health == 0:
            self.engine._death(turn, group, boss, source_id)

    def handle_boss_trial_monster_death(self, dead_monster: Message, turn: Message, group: int) -> None:
        if not self.is_boss_trial or dead_monster.actor_type != _ACTOR_MONSTER or dead_monster.team_index != 1 or dead_monster.is_boss:
            return
        boss = self._find_alive_boss()

        # 1. 护势护卫阵亡：联防阵瓦解
        if (
            is_boss_stance_trial_kind(self.boss_trial_kind)
            and self.boss_stance_type != BOSS_STANCE_NONE
            and (
                dead_monster.monster_table_id == BOSS_TRIAL_MONSTER_GUARD_STANCE
                or (
                    self.boss_trial_kind == BOSS_TRIAL_KIND_BREAK_STANCE
                    and dead_monster.monster_table_id not in (BOSS_TRIAL_MONSTER_SUPPORT_BREAKER, BOSS_TRIAL_MONSTER_SUPPORT_DRUMMER)
                )
            )
        ):
            self.boss_stance_type = BOSS_STANCE_NONE
            self.boss_stance_remain_rounds = 0

        # 2. 回春护卫在已发出治疗预告后、兑现前阵亡 -> 立即触发断疗反噬
        cancelled_pending_heal = False
        kept: list[Message] = []
        for entry in self.active_telegraphs:
            if (
                entry.source_actor_id == dead_monster.actor_id
                and entry.intent_kind == TELEGRAPH_KIND_GUARD_HEAL
                and entry.execute_round >= self.engine.round_index
            ):
                resolved = type(entry)()
                resolved.CopyFrom(entry)
                resolved.status = TELEGRAPH_STATUS_CANCELLED_BY_CONTROL
                self.telegraph_state_version += 1
                resolved.state_version = self.telegraph_state_version
                resolved.label = "回春护卫阵亡，治疗中断触发灵力反噬"
                turn.resolved_telegraphs.add().CopyFrom(resolved)
                self.last_resolved_telegraphs.append(resolved)
                cancelled_pending_heal = True
            else:
                kept.append(entry)
        self.active_telegraphs = kept
        if cancelled_pending_heal and boss is not None and _active(boss):
            self.trigger_rejuv_backlash(dead_monster.actor_id, boss, turn, group)

        # 3. 策应卫阵亡：破军卫清 1 层蓄势；鼓噪卫清全部蓄势并对首领造成 4% 震荡反噬
        if dead_monster.monster_table_id == BOSS_TRIAL_MONSTER_SUPPORT_BREAKER and boss is not None and _active(boss):
            if self.boss_momentum_stacks > 0:
                self.boss_momentum_stacks -= 1
                self.emit_boss_mechanic_event(turn, group, BOSS_MECHANIC_SUPPORT_SHOCK, dead_monster.actor_id, boss.actor_id, 1)
        elif dead_monster.monster_table_id == BOSS_TRIAL_MONSTER_SUPPORT_DRUMMER and boss is not None and _active(boss):
            self.boss_momentum_stacks = 0
            raw_shock = max(1, (boss.max_health * BOSS_DRUMMER_DEATH_SHOCK_PCT) // 100)
            dealt = min(boss.attributes.health, raw_shock)
            if dealt > 0:
                boss.attributes.health -= dealt
            self.emit_boss_mechanic_event(turn, group, BOSS_MECHANIC_SUPPORT_SHOCK, dead_monster.actor_id, boss.actor_id, dealt)
            turn.events.add(
                event_type=3,
                source_id=dead_monster.actor_id,
                target_id=boss.actor_id,
                group_id=group,
                value=dealt,
                target_health_after=boss.attributes.health,
                target_mana_after=boss.attributes.mana,
            )
            if boss.attributes.health == 0:
                self.engine._death(turn, group, boss, dead_monster.actor_id)

    def process_boss_trial_round_end(self, turn: Message, group: int) -> None:
        if not self.is_boss_trial:
            return
        boss = None
        any_special_guard_alive = False
        for actor in self.engine.actors.values():
            if actor.actor_type != _ACTOR_MONSTER or actor.team_index != 1 or not _active(actor):
                continue
            if actor.is_boss:
                boss = actor
            elif actor.monster_table_id not in (BOSS_TRIAL_MONSTER_SUPPORT_BREAKER, BOSS_TRIAL_MONSTER_SUPPORT_DRUMMER):
                any_special_guard_alive = True
        if boss is None or not _active(boss):
            return

        # 1. 存活且未受控的鼓噪卫回合末为首领 +1 蓄势（上限 3 层）
        for actor in self.engine.actors.values():
            if (
                actor.actor_type == _ACTOR_MONSTER
                and actor.team_index == 1
                and actor.monster_table_id == BOSS_TRIAL_MONSTER_SUPPORT_DRUMMER
                and _active(actor)
                and not self.is_actor_control_locked(actor)
                and self.boss_momentum_stacks < BOSS_MOMENTUM_MAX_STACKS
            ):
                self.boss_momentum_stacks += 1
                self.emit_boss_mechanic_event(
                    turn, group, BOSS_MECHANIC_MOMENTUM_GAIN, actor.actor_id, boss.actor_id, self.boss_momentum_stacks,
                )

        # 2. 护势联防剩余回合扣减
        if self.boss_stance_remain_rounds > 0:
            self.boss_stance_remain_rounds -= 1
            if self.boss_stance_remain_rounds == 0:
                self.boss_stance_type = BOSS_STANCE_NONE

        # 3. 二阶段决战姿态（破釜）：全部特殊护卫阵亡或首领气血 <= 50%（火周提前至 <= 60%），仅触发一次
        phase_two_hp_pct = BOSS_AFFIX_FIRE_BOSS_PHASE_TWO_HP_PCT if self.boss_weekly_affix_id == BOSS_AFFIX_FIRE else 50
        if self.boss_phase < 2 and (not any_special_guard_alive or is_health_at_or_below_pct(boss.attributes.health, boss.max_health, phase_two_hp_pct)):
            self.boss_phase = 2
            has_next_round_cleave = any(
                e.source_actor_id == boss.actor_id and e.execute_round == self.engine.round_index + 1
                for e in self.active_telegraphs
            )
            self.next_phase2_cleave_telegraph_round = (
                self.engine.round_index + 2 if has_next_round_cleave else self.engine.round_index + 1
            )
            self.emit_boss_mechanic_event(turn, group, BOSS_MECHANIC_PHASE_TWO_ENRAGE, boss.actor_id, boss.actor_id, 2)

        # 4. 首领试炼周常五行词缀回合末首领压迫（木周 R2/5/8 回 4% 血；水周偶数回合 +1 层蓄势）
        if (
            self.boss_weekly_affix_id == BOSS_AFFIX_WOOD
            and is_wood_regen_round(self.engine.round_index)
            and boss.max_health > 0
            and boss.attributes.health < boss.max_health
        ):
            raw_regen = max(1, (boss.max_health * BOSS_AFFIX_WOOD_BOSS_REGEN_HP_PCT) // 100)
            healed = landed_heal(boss, float(raw_regen))
            if healed > 0:
                boss.attributes.health += healed
                turn.events.add(
                    event_type=_EVENT_HEAL,
                    source_id=boss.actor_id,
                    target_id=boss.actor_id,
                    group_id=group,
                    value=healed,
                    target_health_after=boss.attributes.health,
                    target_mana_after=boss.attributes.mana,
                )
                self.emit_boss_mechanic_event(turn, group, BOSS_MECHANIC_WEEKLY_AFFIX_TRIGGER, boss.actor_id, boss.actor_id, healed)
        elif (
            self.boss_weekly_affix_id == BOSS_AFFIX_WATER
            and is_water_momentum_round(self.engine.round_index)
            and self.boss_momentum_stacks < BOSS_MOMENTUM_MAX_STACKS
        ):
            self.boss_momentum_stacks += 1
            self.emit_boss_mechanic_event(
                turn, group, BOSS_MECHANIC_MOMENTUM_GAIN, boss.actor_id, boss.actor_id, self.boss_momentum_stacks,
            )

    def refresh_encounter_telegraphs(self) -> None:
        round_idx = self.engine.round_index
        kept: list[Message] = []
        for entry in self.active_telegraphs:
            if entry.execute_round >= round_idx:
                next_status = TELEGRAPH_STATUS_EXECUTING if entry.execute_round == round_idx else TELEGRAPH_STATUS_UPCOMING
                if entry.status != next_status:
                    entry.status = next_status
                    self.telegraph_state_version += 1
                    entry.state_version = self.telegraph_state_version
                kept.append(entry)
        self.active_telegraphs = kept

        if not self.is_festival_beast and not is_boss_trial_or_rescue_kind(self.boss_trial_kind):
            return

        boss = None
        rejuv_guard = None
        stance_guard = None
        other_guard = None
        for actor in self.engine.actors.values():
            if actor.actor_type != _ACTOR_MONSTER or actor.team_index != 1 or not _active(actor):
                continue
            if actor.is_boss:
                boss = actor
                continue
            mid = actor.monster_table_id
            if mid in (BOSS_TRIAL_MONSTER_SUPPORT_BREAKER, BOSS_TRIAL_MONSTER_SUPPORT_DRUMMER):
                continue
            if self.boss_trial_kind == BOSS_TRIAL_KIND_BREAK_DUAL_GUARD:
                if mid == BOSS_TRIAL_MONSTER_GUARD_REJUV and rejuv_guard is None:
                    rejuv_guard = actor
                elif mid == BOSS_TRIAL_MONSTER_GUARD_STANCE and stance_guard is None:
                    stance_guard = actor
                elif rejuv_guard is None:
                    rejuv_guard = actor
                elif stance_guard is None:
                    stance_guard = actor
            elif self.boss_trial_kind == BOSS_TRIAL_KIND_BREAK_REJUV:
                if rejuv_guard is None:
                    rejuv_guard = actor
            elif self.boss_trial_kind == BOSS_TRIAL_KIND_BREAK_STANCE:
                if stance_guard is None:
                    stance_guard = actor
            elif other_guard is None:
                other_guard = actor

        def has_entry(src_id: int, exec_round: int) -> bool:
            return any(e.source_actor_id == src_id and e.execute_round == exec_round for e in self.active_telegraphs)

        def pick_enemy_target(caster: Message) -> int:
            targets = self._collect_alive_enemies(caster)
            return targets[self._rand_index(len(targets))] if targets else 0

        def owner_of(aid: int) -> int:
            a = self.engine.actors.get(aid)
            if a is None:
                return 0
            return a.owner_player_id if a.actor_type == _ACTOR_PET else a.actor_id

        def pick_cleave_targets(caster: Message, dual_target: bool) -> list[int]:
            targets = self._collect_alive_enemies(caster)
            if not targets:
                return []
            first = pick_enemy_target(caster)
            chosen = [first] if first else []
            if not dual_target or len(targets) < 2 or not chosen:
                return chosen
            first_owner = owner_of(first)
            diff_owner = [tid for tid in targets if tid != first and first_owner and owner_of(tid) != first_owner]
            fallback = [tid for tid in targets if tid != first and tid not in diff_owner]
            pool = diff_owner or fallback
            if pool:
                chosen.append(pool[self._rand_index(len(pool))])
            return chosen

        def make_entry(**kwargs) -> Message:
            self.telegraph_state_version += 1
            return self.engine.protocol.message("BattleTelegraphEntry", state_version=self.telegraph_state_version, **kwargs)

        # 0. 瑞兽迎春 R2/5/8/11 预告 -> R3/6/9/12 慢速重击
        if self.is_festival_beast:
            fb = self.request.activity_context.festival_beast
            exec_round = round_idx + fb.telegraph_lead_rounds
            if (
                boss is not None
                and exec_round <= self.engine.max_rounds
                and exec_round >= fb.first_heavy_round
                and (exec_round - fb.first_heavy_round) % fb.cycle_rounds == 0
                and not has_entry(boss.actor_id, exec_round)
            ):
                tid = pick_enemy_target(boss)
                if tid:
                    self.active_telegraphs.append(
                        make_entry(
                            source_actor_id=boss.actor_id,
                            target_actor_id=tid,
                            target_actor_ids=[tid],
                            telegraph_round=round_idx,
                            execute_round=exec_round,
                            intent_kind=TELEGRAPH_KIND_FESTIVAL_HEAVY_STRIKE,
                            status=TELEGRAPH_STATUS_UPCOMING,
                            label="春禧重击蓄力：下回合慢速物理重击锁定目标，防御可减半、控场可打断",
                        )
                    )
            return

        # 1. 首领蓄力重击：一阶段 R1/4/7/10/13；二阶段按 next_phase2_cleave_telegraph_round 每 2 回合循环
        boss_due = (
            (self.next_phase2_cleave_telegraph_round != 0 and round_idx == self.next_phase2_cleave_telegraph_round)
            if self.boss_phase >= 2
            else (round_idx % 3 == 1)
        )
        if boss is not None and boss_due and round_idx + 1 <= self.engine.max_rounds and not has_entry(boss.actor_id, round_idx + 1):
            if self.boss_trial_kind == BOSS_TRIAL_KIND_BARRIER:
                tid = pick_enemy_target(boss)
                self.active_telegraphs.append(
                    make_entry(
                        source_actor_id=boss.actor_id,
                        target_actor_id=tid,
                        target_actor_ids=[tid] if tid else [],
                        telegraph_round=round_idx,
                        execute_round=round_idx + 1,
                        intent_kind=TELEGRAPH_KIND_BOSS_MAGIC_STRIKE,
                        status=TELEGRAPH_STATUS_UPCOMING,
                        label="灵障重击：下回合慢速法术重击，防御或护罩化解可触发首领破绽",
                    )
                )
            elif self.boss_trial_kind == BOSS_TRIAL_KIND_VENOM:
                self.active_telegraphs.append(
                    make_entry(
                        source_actor_id=boss.actor_id,
                        target_actor_id=0,
                        telegraph_round=round_idx,
                        execute_round=round_idx + 1,
                        intent_kind=TELEGRAPH_KIND_BOSS_VENOM_AOE,
                        status=TELEGRAPH_STATUS_UPCOMING,
                        label="玄毒冲击：下回合慢速群体法术伤害，防御可减半、控场打断触发破绽",
                    )
                )
            elif self.boss_trial_kind == BOSS_TRIAL_KIND_WILD_WIND:
                dual = self.initial_attacker_human_count >= 3
                targets = pick_cleave_targets(boss, dual)
                self.active_telegraphs.append(
                    make_entry(
                        source_actor_id=boss.actor_id,
                        target_actor_id=targets[0] if targets else 0,
                        target_actor_ids=targets,
                        telegraph_round=round_idx,
                        execute_round=round_idx + 1,
                        intent_kind=TELEGRAPH_KIND_WILD_WIND_GUST,
                        status=TELEGRAPH_STATUS_UPCOMING,
                        label="风眼引风：下回合慢速风袭锁定目标，防御或护盾化解可免折损并触发首领破绽",
                    )
                )
            else:
                dual = self.is_boss_trial and self.initial_attacker_human_count >= 3
                targets = pick_cleave_targets(boss, dual)
                self.active_telegraphs.append(
                    make_entry(
                        source_actor_id=boss.actor_id,
                        target_actor_id=targets[0] if targets else 0,
                        target_actor_ids=targets,
                        telegraph_round=round_idx,
                        execute_round=round_idx + 1,
                        intent_kind=TELEGRAPH_KIND_SLOW_CLEAVE,
                        status=TELEGRAPH_STATUS_UPCOMING,
                        label=(
                            "裂地双劈：下回合慢速重劈双目标，全员防御/护盾化解触发完整破绽"
                            if len(targets) >= 2
                            else "蓄力重劈：下回合慢速物理重劈，防御/护盾/反震化解触发首领破绽"
                        ),
                    )
                )
            if self.boss_phase >= 2:
                self.next_phase2_cleave_telegraph_round = round_idx + 2

        # 2. 敌方护卫周期机制预告（支持单卫与双卫同阵并行）
        if boss is None:
            return
        if rejuv_guard is not None and is_boss_rejuv_trial_kind(self.boss_trial_kind):
            trigger_mod = 1 if (self.boss_trial_kind == BOSS_TRIAL_KIND_BREAK_REJUV and self.battle_difficulty >= 2) else 2
            if round_idx % 3 == trigger_mod and round_idx + 1 <= self.engine.max_rounds and not has_entry(rejuv_guard.actor_id, round_idx + 1):
                self.active_telegraphs.append(
                    make_entry(
                        source_actor_id=rejuv_guard.actor_id,
                        target_actor_id=boss.actor_id,
                        target_actor_ids=[boss.actor_id],
                        telegraph_round=round_idx,
                        execute_round=round_idx + 1,
                        intent_kind=TELEGRAPH_KIND_GUARD_HEAL,
                        status=TELEGRAPH_STATUS_UPCOMING,
                        label=(
                            "回春祈愈（进阶）：下回合为首领回复10%血并净1减益；打断或击杀触发反噬破绽"
                            if self.battle_difficulty >= 2
                            else "回春祈愈：下回合为首领回复8%最大生命；控断或击杀触发反噬破绽"
                        ),
                    )
                )
        if stance_guard is not None and is_boss_stance_trial_kind(self.boss_trial_kind):
            if round_idx % 3 == 1 and not has_entry(stance_guard.actor_id, round_idx):
                chosen_type = self.choose_dynamic_stance_type(round_idx)
                is_phys = chosen_type == BOSS_STANCE_PHYSICAL
                self.active_telegraphs.append(
                    make_entry(
                        source_actor_id=stance_guard.actor_id,
                        target_actor_id=boss.actor_id,
                        target_actor_ids=[boss.actor_id],
                        telegraph_round=round_idx,
                        execute_round=round_idx,
                        intent_kind=TELEGRAPH_KIND_GUARD_STANCE_PHYS if is_phys else TELEGRAPH_KIND_GUARD_STANCE_MAGIC,
                        status=TELEGRAPH_STATUS_EXECUTING,
                        label=(
                            "物防联防阵：本回合展开全体物理减伤阵（可驱散崩势，物理直伤叠蓄势）"
                            if is_phys
                            else "法防联防阵：本回合展开全体法术减伤阵（可驱散崩势，法术直伤叠蓄势）"
                        ),
                    )
                )
        if other_guard is not None and self.boss_trial_kind == BOSS_TRIAL_KIND_BARRIER:
            if round_idx % 3 == 1 and not has_entry(other_guard.actor_id, round_idx):
                self.active_telegraphs.append(
                    make_entry(
                        source_actor_id=other_guard.actor_id,
                        target_actor_id=boss.actor_id,
                        target_actor_ids=[boss.actor_id],
                        telegraph_round=round_idx,
                        execute_round=round_idx,
                        intent_kind=TELEGRAPH_KIND_GUARD_SHIELD,
                        status=TELEGRAPH_STATUS_EXECUTING,
                        label="灵障护佑：本回合为首领附加灵障护盾",
                    )
                )
        elif other_guard is not None and self.boss_trial_kind == BOSS_TRIAL_KIND_VENOM:
            if round_idx % 3 == 1 and round_idx + 1 <= self.engine.max_rounds and not has_entry(other_guard.actor_id, round_idx + 1):
                tid = pick_enemy_target(other_guard)
                self.active_telegraphs.append(
                    make_entry(
                        source_actor_id=other_guard.actor_id,
                        target_actor_id=tid,
                        target_actor_ids=[tid] if tid else [],
                        telegraph_round=round_idx,
                        execute_round=round_idx + 1,
                        intent_kind=TELEGRAPH_KIND_GUARD_BIND,
                        status=TELEGRAPH_STATUS_UPCOMING,
                        label="缚灵锁脉：下回合封控目标并消耗法力",
                    )
                )

    def execute_slow_and_guardian_actions(self, turn: Message) -> None:
        if not self.active_telegraphs:
            return
        from .battle_tables import BattleSkill

        due = list(self.active_telegraphs)
        round_resolved: list[Message] = []

        def record_resolved(base: Message, status: int, label: str) -> None:
            res = type(base)()
            res.CopyFrom(base)
            res.status = status
            self.telegraph_state_version += 1
            res.state_version = self.telegraph_state_version
            if label:
                res.label = label
            turn.resolved_telegraphs.add().CopyFrom(res)
            round_resolved.append(res)

        def deal_boss_strike(source: Message, target: Message, dmg_type: int, scale: float, group: int) -> None:
            rule = BattleSkill(
                base_damage=10.0 * scale,
                physical=(dmg_type == _DAMAGE_PHYSICAL),
                damage_type=dmg_type,
                attack_multiplier=scale,
            )
            self.engine._damage(turn, group, source, target, rule, hit_kind=0, base=10.0 * scale)

        for entry in due:
            if entry.execute_round != self.engine.round_index:
                continue
            source = self.engine.actors.get(entry.source_actor_id)
            if source is None or not _active(source):
                if entry.intent_kind == TELEGRAPH_KIND_FESTIVAL_HEAVY_STRIKE:
                    record_resolved(entry, TELEGRAPH_STATUS_CANCELLED_BY_CONTROL, "首领已倒地，春禧重击取消")
                continue

            if self.is_actor_control_locked(source):
                if entry.intent_kind == TELEGRAPH_KIND_FESTIVAL_HEAVY_STRIKE:
                    record_resolved(entry, TELEGRAPH_STATUS_CANCELLED_BY_CONTROL, "春禧重击受控取消")
                elif self.is_boss_trial:
                    self.engine._group += 1
                    grp = self.engine._group
                    if entry.intent_kind in (
                        TELEGRAPH_KIND_SLOW_CLEAVE,
                        TELEGRAPH_KIND_BOSS_MAGIC_STRIKE,
                        TELEGRAPH_KIND_BOSS_VENOM_AOE,
                        TELEGRAPH_KIND_WILD_WIND_GUST,
                    ):
                        self.apply_boss_exposed(source.actor_id, BOSS_EXPOSED_FULL_PCT, self.engine.round_index + 1)
                        self.engine.trial_tracker.cleave_parry_count += 1
                        self.emit_boss_mechanic_event(
                            turn, grp, BOSS_MECHANIC_CLEAVE_PARRY_EXPOSE, source.actor_id, source.actor_id, BOSS_EXPOSED_FULL_PCT,
                        )
                        record_resolved(
                            entry,
                            TELEGRAPH_STATUS_CANCELLED_BY_CONTROL,
                            "风眼强袭受控取消，首领陷入破绽"
                            if entry.intent_kind == TELEGRAPH_KIND_WILD_WIND_GUST
                            else "重击受控取消，首领陷入破绽",
                        )
                    elif entry.intent_kind == TELEGRAPH_KIND_GUARD_HEAL:
                        boss = self._find_alive_boss()
                        if boss is not None:
                            self.trigger_rejuv_backlash(source.actor_id, boss, turn, grp)
                        record_resolved(entry, TELEGRAPH_STATUS_CANCELLED_BY_CONTROL, "回春被控打断，触发灵力反噬")
                    elif entry.intent_kind in (TELEGRAPH_KIND_GUARD_STANCE_PHYS, TELEGRAPH_KIND_GUARD_STANCE_MAGIC):
                        self.engine.trial_tracker.stance_break_count += 1
                        self.apply_boss_exposed(source.actor_id, BOSS_EXPOSED_FULL_PCT, self.engine.round_index + 1)
                        boss = self._find_alive_boss()
                        if boss is not None:
                            self.apply_boss_exposed(boss.actor_id, BOSS_EXPOSED_FULL_PCT, self.engine.round_index + 1)
                            self.emit_boss_mechanic_event(
                                turn, grp, BOSS_MECHANIC_STANCE_BREAK, source.actor_id, boss.actor_id, BOSS_EXPOSED_FULL_PCT,
                            )
                        record_resolved(entry, TELEGRAPH_STATUS_CANCELLED_BY_CONTROL, "切阵被控打断，触发崩势破绽")
                    elif entry.intent_kind in (TELEGRAPH_KIND_GUARD_SHIELD, TELEGRAPH_KIND_GUARD_BIND):
                        self.engine.trial_tracker.stance_break_count += 1
                        boss = self._find_alive_boss()
                        if boss is not None:
                            self.apply_boss_exposed(boss.actor_id, BOSS_EXPOSED_PARTIAL_PCT, self.engine.round_index + 1)
                            self.emit_boss_mechanic_event(
                                turn, grp, BOSS_MECHANIC_STANCE_BREAK, source.actor_id, boss.actor_id, BOSS_EXPOSED_PARTIAL_PCT,
                            )
                        record_resolved(
                            entry,
                            TELEGRAPH_STATUS_CANCELLED_BY_CONTROL,
                            "灵障护佑被控打断，首领陷入破绽"
                            if entry.intent_kind == TELEGRAPH_KIND_GUARD_SHIELD
                            else "缚灵锁脉被控打断，首领陷入破绽",
                        )
                continue

            kind = entry.intent_kind
            if kind == TELEGRAPH_KIND_FESTIVAL_HEAVY_STRIKE:
                target = self.engine.actors.get(entry.target_actor_id)
                if target is None or not _active(target):
                    record_resolved(entry, TELEGRAPH_STATUS_CANCELLED_TARGET_INVALID, "目标已失效，春禧重击取消")
                    continue
                self.engine._group += 1
                grp = self.engine._group
                self.engine._event(turn, grp, 1, source, target)
                scale = (
                    self.request.activity_context.festival_beast.heavy_damage_percent / 100.0
                    if self.is_festival_beast
                    else 1.8
                )
                deal_boss_strike(source, target, _DAMAGE_PHYSICAL, scale, grp)
                record_resolved(entry, TELEGRAPH_STATUS_EXECUTED, "春禧重击已发动")
            elif kind in (TELEGRAPH_KIND_SLOW_CLEAVE, TELEGRAPH_KIND_BOSS_MAGIC_STRIKE, TELEGRAPH_KIND_WILD_WIND_GUST):
                target_ids = list(entry.target_actor_ids) or ([entry.target_actor_id] if entry.target_actor_id else [])
                if not target_ids:
                    continue
                is_wind_gust = kind == TELEGRAPH_KIND_WILD_WIND_GUST
                is_magic = kind == TELEGRAPH_KIND_BOSS_MAGIC_STRIKE or is_wind_gust
                dmg_type = _DAMAGE_MAGIC if is_magic else _DAMAGE_PHYSICAL
                scale = (
                    1.75
                    if is_wind_gust
                    else ((2.0 if self.battle_difficulty >= 2 else 1.7) if is_magic else (2.1 if self.battle_difficulty >= 2 else 1.8))
                )
                if self.is_boss_trial:
                    if self.boss_momentum_stacks > 0:
                        scale *= 1.0 + (self.boss_momentum_stacks * BOSS_MOMENTUM_PER_STACK_PCT) / 100.0
                    if self.boss_phase >= 2:
                        scale *= 1.10
                    if self.boss_weekly_affix_id == BOSS_AFFIX_EARTH:
                        scale *= BOSS_AFFIX_EARTH_BOSS_CLEAVE_MULTIPLIER
                valid_targets = 0
                invalid_targets = 0
                parried_targets = 0
                for tid in target_ids:
                    target = self.engine.actors.get(tid)
                    owner_id = 0
                    if target is not None:
                        owner_id = (
                            target.owner_player_id if target.actor_type == _ACTOR_PET
                            else (target.actor_id if target.actor_type == _ACTOR_PLAYER else 0)
                        )
                    if owner_id and self.is_boss_trial:
                        self.engine.trial_tracker.self_cleaves_targeted_by_player[owner_id] = (
                            self.engine.trial_tracker.self_cleaves_targeted_by_player.get(owner_id, 0) + 1
                        )
                    if target is None or not _active(target):
                        invalid_targets += 1
                        continue
                    valid_targets += 1
                    was_defending = bool(target.is_defending)
                    had_protection = self.actor_has_boss_cleave_parry_protection(target, dmg_type)
                    ev_before = len(turn.events)
                    self.engine._group += 1
                    grp = self.engine._group
                    self.engine._event(turn, grp, 1, source, target)
                    deal_boss_strike(source, target, dmg_type, scale, grp)
                    triggered_shield_or_reflect = any(
                        (ev.event_type == _EVENT_SHIELD and ev.target_id == target.actor_id and ev.absorbed > 0)
                        or (ev.event_type == 3 and ev.hit_kind == _HIT_REFLECT and ev.source_id == target.actor_id)
                        for ev in turn.events[ev_before:]
                    )
                    if was_defending or had_protection or triggered_shield_or_reflect:
                        parried_targets += 1
                        if owner_id and self.is_boss_trial:
                            self.engine.trial_tracker.self_cleaves_parried_by_player[owner_id] = (
                                self.engine.trial_tracker.self_cleaves_parried_by_player.get(owner_id, 0) + 1
                            )
                    elif is_wind_gust and target.attributes.health == 0:
                        self.engine.trial_tracker.boss_wind_hit_unprotected_death = True
                if self.is_boss_trial:
                    self.boss_momentum_stacks = 0
                    if valid_targets == 0:
                        record_resolved(entry, TELEGRAPH_STATUS_CANCELLED_TARGET_INVALID, "目标已失效，重劈取消")
                    else:
                        self.engine._group += 1
                        grp = self.engine._group
                        if parried_targets == len(target_ids) and invalid_targets == 0:
                            self.apply_boss_exposed(source.actor_id, BOSS_EXPOSED_FULL_PCT, self.engine.round_index + 1)
                            self.engine.trial_tracker.cleave_parry_count += 1
                            self.emit_boss_mechanic_event(
                                turn, grp, BOSS_MECHANIC_CLEAVE_PARRY_EXPOSE, source.actor_id, source.actor_id, BOSS_EXPOSED_FULL_PCT,
                            )
                            self.on_boss_weekly_affix_parry_triggered(source.actor_id, turn, grp)
                        elif len(target_ids) >= 2 and parried_targets >= 1:
                            self.apply_boss_exposed(source.actor_id, BOSS_EXPOSED_PARTIAL_PCT, self.engine.round_index + 1)
                            self.engine.trial_tracker.cleave_parry_count += 1
                            self.emit_boss_mechanic_event(
                                turn, grp, BOSS_MECHANIC_CLEAVE_PARRY_EXPOSE, source.actor_id, source.actor_id, BOSS_EXPOSED_PARTIAL_PCT,
                            )
                            self.on_boss_weekly_affix_parry_triggered(source.actor_id, turn, grp)
                        record_resolved(entry, TELEGRAPH_STATUS_EXECUTED, "慢速重劈已发动")
            elif kind == TELEGRAPH_KIND_BOSS_VENOM_AOE:
                targets = self._collect_alive_enemies(source)
                if not targets:
                    continue
                self.engine._group += 1
                grp = self.engine._group
                scale = 1.05 if self.battle_difficulty >= 2 else 0.85
                if self.is_boss_trial:
                    if self.boss_momentum_stacks > 0:
                        scale *= 1.0 + (self.boss_momentum_stacks * BOSS_MOMENTUM_PER_STACK_PCT) / 100.0
                    if self.boss_phase >= 2:
                        scale *= 1.10
                    if self.boss_weekly_affix_id == BOSS_AFFIX_EARTH:
                        scale *= BOSS_AFFIX_EARTH_BOSS_CLEAVE_MULTIPLIER
                valid_targets = 0
                parried_targets = 0
                for tid in targets:
                    target = self.engine.actors.get(tid)
                    if target is None or not _active(target):
                        continue
                    valid_targets += 1
                    owner_id = (
                        target.owner_player_id if target.actor_type == _ACTOR_PET
                        else (target.actor_id if target.actor_type == _ACTOR_PLAYER else 0)
                    )
                    if owner_id and self.is_boss_trial:
                        self.engine.trial_tracker.self_cleaves_targeted_by_player[owner_id] = (
                            self.engine.trial_tracker.self_cleaves_targeted_by_player.get(owner_id, 0) + 1
                        )
                    was_defending = bool(target.is_defending)
                    had_protection = self.actor_has_boss_cleave_parry_protection(target, _DAMAGE_MAGIC)
                    ev_before = len(turn.events)
                    deal_boss_strike(source, target, _DAMAGE_MAGIC, scale, grp)
                    triggered_shield_or_reflect = any(
                        (ev.event_type == _EVENT_SHIELD and ev.target_id == target.actor_id and ev.absorbed > 0)
                        or (ev.event_type == 3 and ev.hit_kind == _HIT_REFLECT and ev.source_id == target.actor_id)
                        for ev in turn.events[ev_before:]
                    )
                    if was_defending or had_protection or triggered_shield_or_reflect:
                        parried_targets += 1
                        if owner_id and self.is_boss_trial:
                            self.engine.trial_tracker.self_cleaves_parried_by_player[owner_id] = (
                                self.engine.trial_tracker.self_cleaves_parried_by_player.get(owner_id, 0) + 1
                            )
                if self.is_boss_trial:
                    self.boss_momentum_stacks = 0
                    if valid_targets > 0 and parried_targets == valid_targets:
                        self.apply_boss_exposed(source.actor_id, BOSS_EXPOSED_FULL_PCT, self.engine.round_index + 1)
                        self.engine.trial_tracker.cleave_parry_count += 1
                        self.emit_boss_mechanic_event(
                            turn, grp, BOSS_MECHANIC_CLEAVE_PARRY_EXPOSE, source.actor_id, source.actor_id, BOSS_EXPOSED_FULL_PCT,
                        )
                        self.on_boss_weekly_affix_parry_triggered(source.actor_id, turn, grp)
                    elif valid_targets >= 2 and parried_targets * 2 >= valid_targets:
                        self.apply_boss_exposed(source.actor_id, BOSS_EXPOSED_PARTIAL_PCT, self.engine.round_index + 1)
                        self.engine.trial_tracker.cleave_parry_count += 1
                        self.emit_boss_mechanic_event(
                            turn, grp, BOSS_MECHANIC_CLEAVE_PARRY_EXPOSE, source.actor_id, source.actor_id, BOSS_EXPOSED_PARTIAL_PCT,
                        )
                        self.on_boss_weekly_affix_parry_triggered(source.actor_id, turn, grp)
                record_resolved(entry, TELEGRAPH_STATUS_EXECUTED, "玄毒冲击已发动")
            elif kind == TELEGRAPH_KIND_GUARD_HEAL:
                boss = self._find_alive_boss()
                if boss is None or not _active(boss):
                    continue
                inc_mod = self.incoming_heal_modifier(boss)
                anti_heal_suppressed = inc_mod < 0.0
                received = max(0.0, 1.0 + inc_mod)
                self.engine._group += 1
                grp = self.engine._group
                healed = 0
                if received > 0.0:
                    pct = 10 if self.battle_difficulty >= 2 else 8
                    raw_heal = max(1, (boss.max_health * pct) // 100)
                    healed = landed_heal(boss, float(raw_heal) * received)
                    boss.attributes.health += healed
                    self.engine.trial_tracker.record_monster_heal(source, boss, healed)
                    turn.events.add(
                        event_type=_EVENT_HEAL,
                        source_id=source.actor_id,
                        target_id=boss.actor_id,
                        group_id=grp,
                        value=healed,
                        target_health_after=boss.attributes.health,
                        target_mana_after=boss.attributes.mana,
                    )
                if anti_heal_suppressed:
                    self.boss_disheartened_until_round = self.engine.round_index + 1
                    self.emit_boss_mechanic_event(turn, grp, BOSS_MECHANIC_ANTI_HEAL_DISHEARTEN, source.actor_id, boss.actor_id, 15)
                elif self.battle_difficulty >= 2 and healed > 0:
                    for idx, b in enumerate(boss.buffs):
                        if self.engine.effects.is_debuff(b):
                            self.engine.effects._remove(boss, idx, turn, grp, 0)
                            self.emit_boss_mechanic_event(turn, grp, BOSS_MECHANIC_REJUV_CLEANSE, source.actor_id, boss.actor_id, 1)
                            break
                record_resolved(entry, TELEGRAPH_STATUS_EXECUTED, "回春祈愈已发动")
            elif kind in (TELEGRAPH_KIND_GUARD_STANCE_PHYS, TELEGRAPH_KIND_GUARD_STANCE_MAGIC):
                boss = self._find_alive_boss()
                if boss is None or not _active(boss):
                    continue
                self.engine._group += 1
                grp = self.engine._group
                self.boss_stance_type = BOSS_STANCE_PHYSICAL if kind == TELEGRAPH_KIND_GUARD_STANCE_PHYS else BOSS_STANCE_MAGIC
                self.boss_stance_remain_rounds = 3
                self.last_chosen_stance_type = self.boss_stance_type
                turn.events.add(
                    event_type=_EVENT_BUFF_ADD,
                    source_id=source.actor_id,
                    target_id=boss.actor_id,
                    group_id=grp,
                    value=self.boss_stance_type,
                )
                if (
                    self.boss_trial_kind == BOSS_TRIAL_KIND_BREAK_STANCE
                    and self.battle_difficulty >= 2
                    and self.boss_momentum_stacks < BOSS_MOMENTUM_MAX_STACKS
                ):
                    self.boss_momentum_stacks += 1
                    self.emit_boss_mechanic_event(
                        turn, grp, BOSS_MECHANIC_MOMENTUM_GAIN, source.actor_id, boss.actor_id, self.boss_momentum_stacks,
                    )
                record_resolved(
                    entry,
                    TELEGRAPH_STATUS_EXECUTED,
                    "物防联防阵已展开" if self.boss_stance_type == BOSS_STANCE_PHYSICAL else "法防联防阵已展开",
                )
            elif kind == TELEGRAPH_KIND_GUARD_SHIELD:
                boss = self._find_alive_boss()
                if boss is None or not _active(boss):
                    continue
                self.engine._group += 1
                grp = self.engine._group
                pct = 18 if self.battle_difficulty >= 2 else 12
                add_shield = max(1, (boss.max_health * pct) // 100)
                self.boss_barrier_shield = min(U64_MAX, self.boss_barrier_shield + add_shield)
                turn.events.add(
                    event_type=_EVENT_SHIELD,
                    source_id=source.actor_id,
                    target_id=boss.actor_id,
                    group_id=grp,
                    value=add_shield,
                    shield_after=self.boss_barrier_shield,
                )
                record_resolved(entry, TELEGRAPH_STATUS_EXECUTED, "灵障护盾已附加")
            elif kind == TELEGRAPH_KIND_GUARD_BIND:
                target = self.engine.actors.get(entry.target_actor_id)
                if target is None or not _active(target):
                    continue
                self.engine._group += 1
                grp = self.engine._group
                drain = max(1, (target.max_mana * 15) // 100) if target.max_mana else 0
                actual = min(target.attributes.mana, drain)
                if actual > 0:
                    target.attributes.mana -= actual
                    turn.events.add(
                        event_type=_EVENT_MANA,
                        source_id=source.actor_id,
                        target_id=target.actor_id,
                        group_id=grp,
                        value=actual,
                        mana_delta=-actual,
                        target_health_after=target.attributes.health,
                        target_mana_after=target.attributes.mana,
                    )
                record_resolved(entry, TELEGRAPH_STATUS_EXECUTED, "缚灵锁脉已发动")

        if round_resolved:
            self.last_resolved_telegraphs = round_resolved

    def try_dispel_boss_stance(self, source: Message, target: Message, turn: Message, group: int) -> None:
        if not self.is_boss_trial or source.team_index == target.team_index or target.team_index != 1:
            return
        dispelled_any = False
        if is_boss_stance_trial_kind(self.boss_trial_kind) and self.boss_stance_type != BOSS_STANCE_NONE:
            self.boss_stance_type = BOSS_STANCE_NONE
            self.boss_stance_remain_rounds = 0
            self.engine.trial_tracker.guard_stance_dispelled = True
            self.engine.trial_tracker.stance_break_count += 1
            dispelled_any = True
            for actor in self.engine.actors.values():
                if actor.actor_type != _ACTOR_MONSTER or actor.team_index != 1 or not _active(actor):
                    continue
                if (
                    actor.is_boss
                    or actor.monster_table_id == BOSS_TRIAL_MONSTER_GUARD_STANCE
                    or (
                        self.boss_trial_kind == BOSS_TRIAL_KIND_BREAK_STANCE
                        and actor.monster_table_id not in (BOSS_TRIAL_MONSTER_SUPPORT_BREAKER, BOSS_TRIAL_MONSTER_SUPPORT_DRUMMER)
                    )
                ):
                    self.apply_boss_exposed(actor.actor_id, BOSS_EXPOSED_FULL_PCT, self.engine.round_index + 1)
            self.emit_boss_mechanic_event(turn, group, BOSS_MECHANIC_STANCE_BREAK, source.actor_id, target.actor_id, BOSS_EXPOSED_FULL_PCT)
        if self.boss_momentum_stacks > 0:
            self.boss_momentum_stacks = 0
            dispelled_any = True
        if dispelled_any:
            self.engine.trial_tracker.record_effective_contribution(source)
            self.on_boss_weekly_affix_dispel_triggered(source.actor_id, turn, group)

    def advance_round_cleanup(self) -> None:
        round_idx = self.engine.round_index
        self.attacker_triggered_stance_counter_this_round.clear()
        self.exposed_actors = {aid: st for aid, st in self.exposed_actors.items() if st[1] >= round_idx}
        self.sunder_until_round = {aid: until for aid, until in self.sunder_until_round.items() if until >= round_idx}
        self.update_polarity_for_round()
        self.refresh_encounter_telegraphs()

    def fill_state_snapshot(self, state: Message) -> None:
        if self.active_polarity:
            state.active_polarity = self.active_polarity
            state.next_polarity_switch_round = self.next_polarity_switch_round()
        if self.protected_guard_actor_id:
            state.protected_guard_actor_id = self.protected_guard_actor_id
        if self.boss_trial_kind != BOSS_TRIAL_KIND_NONE:
            state.boss_trial_kind = self.boss_trial_kind
            state.battle_difficulty = self.battle_difficulty
        if self.is_boss_trial:
            state.boss_phase = self.boss_phase
            state.boss_momentum_stacks = self.boss_momentum_stacks
            if self.boss_weekly_affix_id != BOSS_AFFIX_NONE:
                state.boss_weekly_affix_id = self.boss_weekly_affix_id
            if self.boss_stance_type != BOSS_STANCE_NONE and self.boss_stance_remain_rounds > 0:
                state.boss_stance_type = self.boss_stance_type
                state.boss_stance_remain_rounds = self.boss_stance_remain_rounds
            boss = self._find_alive_boss()
            if boss is not None:
                exp = self.exposed_actors.get(boss.actor_id)
                if exp is not None and exp[1] >= self.engine.round_index:
                    state.boss_exposed_pct = exp[0]
                    state.boss_exposed_until_round = exp[1]
        for resolved in self.last_resolved_telegraphs:
            state.last_resolved_telegraphs.add().CopyFrom(resolved)
        if self.engine.outcome == 0:
            for entry in self.active_telegraphs:
                state.telegraphs.add().CopyFrom(entry)


# ---------------------------------------------------------------------------
# 8. 战法 V3 配置化技能与状态控制器（对照 configured_battle_effects.cpp）
# ---------------------------------------------------------------------------


class ConfiguredAbilitiesController:
    """承载 `battle_model == 1` 战法、策略副目标、蓄力释放、受击易伤、免控与专修禁攻规则。"""

    def __init__(self, engine: TurnEngine):
        self.engine = engine

    @staticmethod
    def subject_allowed(actor: Message, rule: BattleSkill) -> bool:
        """判定单位是否满足战法主体掩码与解锁等级。"""
        mask = 2 if actor.actor_type == _ACTOR_PET else 1
        return rule.battle_model == 1 and (rule.caster_mask & mask) != 0 and actor.level >= rule.unlock_level

    def configured_rank(self, actor: Message, rule: BattleSkill) -> int:
        if not self.subject_allowed(actor, rule):
            return 0
        return actor.skill_ranks.get(rule.id if hasattr(rule, "id") else 0, 0)

    def has_state(self, actor: Message, state_kind: int) -> bool:
        """判定单位是否带有指定 `BattleStateKind` 配置化控制状态。"""
        rules = self.engine.effects.rules
        for entry in actor.buffs:
            row = rules.get(entry.buff_table_id)
            if row is not None and row.battle_op == BattleEffectOp.CONTROL and row.state_kind == state_kind:
                return True
        return False

    def modifier(self, actor: Message, kind: int) -> float:
        """按最强正项与最强负项之和汇总配置化属性/伤害修正（对照 `ConfiguredModifier`）。"""
        selected = ModifierSelection()
        rules = self.engine.effects.rules
        for entry in actor.buffs:
            row = rules.get(entry.buff_table_id)
            if (
                row is None
                or row.battle_op not in (BattleEffectOp.MODIFIER, BattleEffectOp.GROWING_AURA)
                or row.modifier_stat != kind
            ):
                continue
            if row.require_living_caster:
                caster = self.engine.actors.get(entry.caster_id)
                if caster is None or not _active(caster):
                    continue
            stacks = entry.stacks if row.battle_op == BattleEffectOp.GROWING_AURA else 1
            value = row.ratio * stacks
            selected.consider(value)
        return selected.total()

    def is_physical_forbidden(self, actor: Message) -> bool:
        return self.modifier(actor, BattleModifier.FORBID_PHYSICAL) > 0.0

    def is_magic_forbidden(self, actor: Message) -> bool:
        return self.modifier(actor, BattleModifier.FORBID_MAGIC) > 0.0

    def effective_physical_attack(self, actor: Message) -> int:
        base = self.engine._effective_stat(actor, 10)
        mult = max(0.1, 1.0 + self.modifier(actor, BattleModifier.PHYSICAL_ATTACK))
        return PetAndSupportEffects.whole(float(base) * mult)

    def effective_magic_attack(self, actor: Message) -> int:
        mult = max(0.1, 1.0 + self.modifier(actor, BattleModifier.MAGIC_ATTACK))
        return PetAndSupportEffects.whole(float(actor.magic_attack) * mult)

    def effective_defense(self, actor: Message) -> int:
        base = self.engine._effective_stat(actor, 11)
        mult = max(0.1, 1.0 + self.modifier(actor, BattleModifier.DEFENSE))
        return PetAndSupportEffects.whole(float(base) * mult)

    def effective_speed(self, actor: Message) -> int:
        base = self.engine._effective_stat(actor, 13)
        mult = max(0.1, 1.0 + self.modifier(actor, BattleModifier.SPEED))
        speed_bonus_pct = getattr(self.engine, "wuxing_speed_bonus_pct", lambda _a: 0)(actor)
        if speed_bonus_pct > 0:
            mult *= (100.0 + speed_bonus_pct) / 100.0
        return PetAndSupportEffects.whole(float(base) * mult)

    def effective_mitigation_score(self, actor: Message, *, is_magic: bool) -> float:
        """计算物理/法术等效防御厚度分（1 - received），供最低防御副目标策略升序选脆皮。"""
        resist_bonus = actor.combat.magic_resist if is_magic else actor.combat.physical_resist
        equip_resist = (
            (actor.equipment_effects.magic_resist_bps if is_magic else actor.equipment_effects.physical_resist_bps)
            if actor.HasField("equipment_effects")
            else 0
        )
        legacy_resist = min(U64_MAX, actor.attributes.resistance + resist_bonus)
        received = received_ratio(
            actor.attributes.armor,
            self.effective_defense(actor),
            float(legacy_resist) * 100.0 + float(equip_resist),
            actor.level,
        )
        return 1.0 - max(0.0, min(1.0, received))

    def sync_charging_skill_id(self, actor: Message) -> None:
        """同步 `BattleActorState.charging_skill_id` 字段与当前明牌蓄力状态。"""
        rules = self.engine.effects.rules
        charging_id = 0
        for entry in actor.buffs:
            row = rules.get(entry.buff_table_id)
            if row is not None and row.battle_op == BattleEffectOp.PREPARE_CAST:
                charging_id = entry.source_skill_id
                break
        actor.charging_skill_id = charging_id

    def remove_buff(self, actor: Message, buff_id: int, turn: Message, group: int) -> bool:
        """按实例号移除 Buff、发出移除事件并在需要时刷新 `charging_skill_id`。"""
        for idx, entry in enumerate(actor.buffs):
            if entry.buff_id != buff_id:
                continue
            row = self.engine.effects.rules.get(entry.buff_table_id)
            caster_id = entry.caster_id
            table_id = entry.buff_table_id
            del actor.buffs[idx]
            if row is not None and row.battle_op == BattleEffectOp.PREPARE_CAST:
                self.sync_charging_skill_id(actor)
            turn.events.add(
                event_type=_EVENT_BUFF_REMOVE,
                source_id=caster_id,
                target_id=actor.actor_id,
                buff_table_id=table_id,
                buff_instance_id=buff_id,
                group_id=group,
            )
            return True
        return False

    def try_consume_control_immunity(
        self, caster: Message, target: Message, incoming_buff_id: int, turn: Message, group: int, hit: int = 0,
    ) -> bool:
        """若目标持有免控护阵（`ControlImmunity`），消耗 1 次次数并阻挡敌方硬控。"""
        rules = self.engine.effects.rules
        for idx, entry in enumerate(target.buffs):
            guard = rules.get(entry.buff_table_id)
            if guard is None or guard.battle_op != BattleEffectOp.CONTROL_IMMUNITY:
                continue
            guard_id = entry.buff_id
            if entry.remaining_hits <= 1:
                self.remove_buff(target, guard_id, turn, group)
            else:
                target.buffs[idx].remaining_hits = entry.remaining_hits - 1
            if target.actor_type == _ACTOR_PLAYER:
                self.engine.trial_tracker.record_anti_control_or_season_skill(target)
            turn.events.add(
                event_type=_EVENT_RESIST,
                source_id=caster.actor_id,
                target_id=target.actor_id,
                buff_table_id=incoming_buff_id,
                group_id=group,
                hit_index=hit,
            )
            return True
        return False

    def increment_hit_vulnerability(self, target: Message, round_index: int, group_id: int) -> None:
        """目标受直伤/追击时叠加「连击破绽」易伤层数（同行动 group 至多叠 1 层）。"""
        rules = self.engine.effects.rules
        for entry in target.buffs:
            row = rules.get(entry.buff_table_id)
            if row is None or row.battle_op != BattleEffectOp.HIT_VULNERABILITY:
                continue
            if entry.last_trigger_round == round_index and entry.secondary_value == group_id:
                continue
            cap = max(1, row.stack_limit)
            if entry.stacks >= cap:
                continue
            entry.stacks += 1
            entry.last_trigger_round = round_index
            entry.secondary_value = group_id

    def hit_vulnerability_bonus(self, target: Message) -> float:
        """计算目标当前全部受击叠层易伤总比例。"""
        rules = self.engine.effects.rules
        bonus = 0.0
        for entry in target.buffs:
            row = rules.get(entry.buff_table_id)
            if row is None or row.battle_op != BattleEffectOp.HIT_VULNERABILITY:
                continue
            if entry.stacks > 0 and math.isfinite(row.ratio):
                bonus += row.ratio * float(entry.stacks)
        return bonus

    def consume_one_shot_attack_modifiers(self, source: Message, damage_type: int, turn: Message, group: int) -> None:
        """一次性消耗攻击方身上的单次物理/法术攻击修正标记（如截锋令 / 授锋战印）。"""
        target_stat = (
            BattleModifier.NEXT_PHYSICAL_ATTACK
            if damage_type == _DAMAGE_PHYSICAL
            else BattleModifier.NEXT_MAGIC_ATTACK
        )
        rules = self.engine.effects.rules
        to_remove = [
            entry.buff_id
            for entry in source.buffs
            if (row := rules.get(entry.buff_table_id)) is not None
            and row.battle_op == BattleEffectOp.MODIFIER
            and row.modifier_stat == target_stat
        ]
        for buff_id in to_remove:
            self.remove_buff(source, buff_id, turn, group)

    def target_allowed(self, actor: Message, target: Message, rule: BattleSkill) -> bool:
        """校验目标是否符合配置化战法阵营、存活、禁复制与劝退限制。"""
        if target.fled or (target.is_dead and not rule.allow_dead_target):
            return False
        if rule.target_team == 3:
            return target.actor_id == actor.actor_id
        if (target.team_index == actor.team_index) != (rule.target_team == 2):
            return False
        if rule.health_cost_mode == 2 and actor.actor_id == target.actor_id:
            return False
        rules = self.engine.effects.rules
        for eff_id in rule.effects:
            eff = rules.get(eff_id)
            if eff is None:
                continue
            if eff.battle_op == BattleEffectOp.RETREAT_PET and target.actor_type != _ACTOR_PET:
                return False
            if eff.battle_op == BattleEffectOp.COPY_TARGET_SKILL and target.is_boss:
                return False
        return True

    def has_valid_target(self, caster: Message, rule: BattleSkill) -> bool:
        """判定当前战场是否存在至少一名满足该配置化战法目标条件的单位。"""
        return any(self.target_allowed(caster, cand, rule) for cand in self.engine.actors.values())

    def is_skill_copyable_for(self, caster: Message, rule: BattleSkill, skill_id: int = 0) -> bool:
        """判定指定技能是否可被 A41 玄镜映法镜像复制（对照 `IsSkillCopyableFor`）。"""
        if rule.battle_model != 1 or rule.ability_kind != 1:
            return False
        if skill_id and self.engine.is_skill_banned(skill_id):
            return False
        if rule.forbid_copy or rule.health_cost_mode != 0 or rule.max_battle_uses != 0:
            return False
        buff_rules = self.engine.effects.rules
        for eff_id in rule.effects:
            eff = buff_rules.get(eff_id)
            if eff is None:
                continue
            if eff.battle_op == BattleEffectOp.COPY_TARGET_SKILL:
                return False
            # 依赖已付气血/法力的转换技在零耗蓝镜像中无有效基数，结构上禁止复制。
            if eff.value_basis in (5, 6):
                return False
        return self.has_valid_target(caster, rule)

    def has_copyable_skill_for(self, caster: Message, target: Message) -> bool:
        """判定目标是否非首领且至少持有一项可复制主动技能。"""
        if target.is_boss:
            return False
        for sid in target.skill_table_ids:
            cand = self.engine.skills.rules.get(sid)
            if cand is not None and self.is_skill_copyable_for(caster, cand, sid):
                return True
        return False

    def select_copied_skill_id(self, caster: Message, target: Message) -> int | None:
        """优先复制目标本回合选定的可复制主动技能，否则按方案 A 从其已学池确定性抽取。"""
        planned_action = self.engine.pending.get(target.actor_id)
        if planned_action is not None and planned_action.action_type == _ACTION_SKILL:
            planned = self.engine.skills.rules.get(planned_action.skill_table_id)
            if planned is not None and self.is_skill_copyable_for(caster, planned, planned_action.skill_table_id):
                return planned_action.skill_table_id
        candidates = sorted({
            sid
            for sid in target.skill_table_ids
            if (cand := self.engine.skills.rules.get(sid)) is not None
            and self.is_skill_copyable_for(caster, cand, sid)
        })
        if not candidates:
            return None
        pick = 0 if len(candidates) == 1 else int(self.engine.rng.next() % len(candidates))
        return candidates[pick]

    def check_skill(self, actor: Message, action: Message, rule: BattleSkill) -> int:
        """校验配置化战法的禁招、封印/专修禁攻、蓄力防重、冷却、次数、耗蓝与目标合法性。"""
        if not self.subject_allowed(actor, rule) or not rule.castable:
            return 7004
        if self.engine.is_skill_banned(action.skill_table_id):
            return ERR_SKILL_BANNED_IN_BATTLE
        control_banned_fn = getattr(self.engine, "is_control_banned_for_actor", None)
        if (
            (control_banned_fn(actor) if callable(control_banned_fn) else getattr(self.engine, "ban_control_skills", False))
            and self.engine.is_control_skill(rule)
        ):
            return 7004
        if (
            self.has_state(actor, BattleStateKind.SEAL_ACTIVE)
            or ((self.has_state(actor, BattleStateKind.SEAL_PHYSICAL) or self.is_physical_forbidden(actor))
                and (rule.action_tags & ACTION_TAG_PHYSICAL))
            or ((self.has_state(actor, BattleStateKind.SEAL_MAGIC) or self.is_magic_forbidden(actor))
                and (rule.action_tags & ACTION_TAG_MAGIC))
        ):
            return 7004
        if actor.charging_skill_id != 0:
            for eff_id in rule.effects:
                eff = self.engine.effects.rules.get(eff_id)
                if eff is not None and eff.battle_op == BattleEffectOp.PREPARE_CAST:
                    return 7004
        if actor.skill_cooldown_rounds.get(action.skill_table_id, 0) > 0:
            return 7003
        if rule.max_battle_uses > 0 and actor.ability_use_counts.get(action.skill_table_id, 0) >= rule.max_battle_uses:
            return 7004
        if rule.health_cost_mode == 1 and actor.attributes.health <= 1:
            return 7004
        from .battle_skills import mana_cost as _mana_cost
        cost = _mana_cost(actor, rule)
        if actor.HasField("equipment_effects") and actor.equipment_effects.mana_reduction_bps:
            cost = self.engine.equipment_effects.discount_skill_mana(actor, rule, cost)
        if (
            actor.team_index == 0
            and self.engine.encounter.boss_weekly_affix_id == BOSS_AFFIX_WATER
            and self.engine.encounter.is_weekly_water_discount_skill(rule)
        ):
            cost = discount_water_mana_cost(cost)
        discount_pct = self.engine.equipment_effects.defend_mana_discount_resonance_pct(
            actor, action.skill_table_id, self.engine.round_index
        )
        if discount_pct > 0:
            cost = SeasonBattleRules.compute_mana_discounted(cost, discount_pct)
        if cost > actor.attributes.mana:
            return 7004
        if action.target_id != 0:
            target = self.engine.actors.get(action.target_id)
            return 0 if (target is not None and self.target_allowed(actor, target, rule)) else 7002
        return 0 if self.has_valid_target(actor, rule) else 7002

    def _compare_secondary(self, policy: int, lhs: Message, rhs: Message) -> int:
        if policy == SecondaryTargetPolicy.HIGHEST_PHYSICAL_ATTACK:
            a, b = self.effective_physical_attack(lhs), self.effective_physical_attack(rhs)
            return 1 if a > b else (-1 if a < b else 0)
        if policy == SecondaryTargetPolicy.HIGHEST_MAGIC_ATTACK:
            a, b = self.effective_magic_attack(lhs), self.effective_magic_attack(rhs)
            return 1 if a > b else (-1 if a < b else 0)
        if policy == SecondaryTargetPolicy.LOWEST_HEALTH_VALUE:
            a, b = lhs.attributes.health, rhs.attributes.health
            return 1 if a < b else (-1 if a > b else 0)
        if policy == SecondaryTargetPolicy.LOWEST_HEALTH_RATIO:
            a = (lhs.attributes.health / lhs.max_health) if lhs.max_health > 0 else 1.0
            b = (rhs.attributes.health / rhs.max_health) if rhs.max_health > 0 else 1.0
            if abs(a - b) <= 1e-12:
                return 0
            return 1 if a < b else -1
        if policy == SecondaryTargetPolicy.LOWEST_PHYSICAL_DEFENSE:
            a = self.effective_mitigation_score(lhs, is_magic=False)
            b = self.effective_mitigation_score(rhs, is_magic=False)
            if abs(a - b) <= 1e-9:
                return 0
            return 1 if a < b else -1
        if policy == SecondaryTargetPolicy.LOWEST_MAGIC_DEFENSE:
            a = self.effective_mitigation_score(lhs, is_magic=True)
            b = self.effective_mitigation_score(rhs, is_magic=True)
            if abs(a - b) <= 1e-9:
                return 0
            return 1 if a < b else -1
        if policy == SecondaryTargetPolicy.LOWEST_ROUND_SPEED:
            frozen = self.engine.round_start_speeds
            a = frozen.get(lhs.actor_id, self.effective_speed(lhs))
            b = frozen.get(rhs.actor_id, self.effective_speed(rhs))
            return 1 if a < b else (-1 if a > b else 0)
        return 0

    def select_targets(self, actor: Message, rule: BattleSkill, primary_id: int) -> list[Message]:
        """按策略模式选择配置化战法主目标与确定性副目标（并列用确定性 RNG 打破平局）。"""
        subject_max = (rule.pet_max_targets or rule.max_targets) if actor.actor_type == _ACTOR_PET else rule.max_targets
        count = max(1, subject_max)
        targets: list[Message] = []
        candidates: list[Message] = []
        for candidate in self.engine.actors.values():
            if not self.target_allowed(actor, candidate, rule):
                continue
            if candidate.actor_id == primary_id:
                targets.append(candidate)
            else:
                candidates.append(candidate)

        policy = rule.secondary_target_policy
        if policy == SecondaryTargetPolicy.RANDOM:
            while len(targets) < count and candidates:
                idx = 0 if len(candidates) == 1 else int(self.engine.rng.next() % len(candidates))
                targets.append(candidates.pop(idx))
            return targets

        while len(targets) < count and candidates:
            best_indices: list[int] = []
            for idx, cand in enumerate(candidates):
                if not best_indices:
                    best_indices.append(idx)
                    continue
                cmp = self._compare_secondary(policy, cand, candidates[best_indices[0]])
                if cmp > 0:
                    best_indices = [idx]
                elif cmp == 0:
                    best_indices.append(idx)
            if not best_indices:
                break
            chosen_pos = (
                best_indices[0]
                if len(best_indices) == 1
                else best_indices[int(self.engine.rng.next() % len(best_indices))]
            )
            targets.append(candidates.pop(chosen_pos))
        return targets

    def initialize_abilities_for_actor(self, actor: Message, turn: Message, group: int = 0) -> None:
        """为单个单位初始化配置化被动、触发器与开场光环（供开局与临阵唤灵复用）。"""
        for skill_id in sorted(actor.skill_ranks):
            rule = self.engine.skills.rules.get(skill_id)
            if rule is None or rule.battle_model != 1 or not self.subject_allowed(actor, rule):
                continue
            if skill_id not in actor.runtime_skills:
                actor.runtime_skills.append(skill_id)
            if not rule.init_effects:
                continue
            rank = max(1, actor.skill_ranks.get(skill_id, 1))
            if rule.ability_kind == 4:
                primary = actor.owner_player_id if actor.actor_type == _ACTOR_PET else actor.actor_id
                targets = self.select_targets(actor, rule, primary)
            else:
                targets = [actor]
            primary_id = targets[0].actor_id if targets else 0
            for target in targets:
                for effect_id in rule.init_effects:
                    self.apply_effect(
                        actor, target, effect_id, rule=rule, skill_id=skill_id, rank=rank,
                        primary_target_id=primary_id, turn=turn, group=group, hit=0,
                    )

    def initialize_abilities(self) -> None:
        """在阵容最终锁定后初始化全部配置化被动、触发器与开场光环（对照 `InitializeConfiguredAbilities`）。"""
        ignored = self.engine.protocol.message("TurnResultS2C")
        for actor in self.engine.actors.values():
            self.initialize_abilities_for_actor(actor, ignored, 0)

    def reset_volatile_growing_stacks(self, actor: Message) -> None:
        """单位阵亡时重置非 `keep_on_death` 成长光环层数（对照 `ResetVolatileGrowingStacks`）。"""
        rules = self.engine.effects.rules
        for entry in actor.buffs:
            row = rules.get(entry.buff_table_id)
            if (
                row is not None
                and row.battle_op == BattleEffectOp.GROWING_AURA
                and not row.keep_on_death
                and entry.stacks > 0
            ):
                entry.stacks = 0

    def apply_effect(
        self,
        caster: Message,
        target: Message,
        buff_id: int,
        *,
        rule: BattleSkill | None,
        skill_id: int,
        rank: int,
        primary_target_id: int,
        turn: Message,
        group: int,
        hit: int,
        is_multi_target: bool = False,
        primary_focus_bonus: bool = False,
        scale: float = 1.0,
        is_mirror: bool = False,
        depth: int = 0,
        paid_mana: int = 0,
    ) -> None:
        """执行单条配置化 Buff/技能子效果（对照 `ApplyConfiguredEffect` 核心分支）。"""
        row = self.engine.effects.rules.get(buff_id)
        if row is None or depth > 8:
            return
        op = row.battle_op
        if op == BattleEffectOp.NONE:
            self.engine.effects.add(target, buff_id, caster.actor_id, turn, group, hit, depth=depth)
            return
        receiver = caster if row.effect_target in (1, 3) else target
        if receiver.fled or receiver.is_dead:
            return
        hostile = receiver.team_index != caster.team_index
        if hostile and self.has_state(receiver, BattleStateKind.FREEZE):
            return

        if op == BattleEffectOp.COPY_TARGET_SKILL:
            if is_mirror:
                return
            if receiver.is_boss or self.engine.effects.immune(receiver, row):
                turn.events.add(
                    event_type=_EVENT_RESIST,
                    source_id=caster.actor_id,
                    target_id=receiver.actor_id,
                    buff_table_id=buff_id,
                    group_id=group,
                    hit_index=hit,
                )
                return
            copied_skill_id = self.select_copied_skill_id(caster, receiver)
            if copied_skill_id is None:
                turn.events.add(
                    event_type=_EVENT_RESIST,
                    source_id=caster.actor_id,
                    target_id=receiver.actor_id,
                    buff_table_id=buff_id,
                    group_id=group,
                    hit_index=hit,
                )
                return
            copied_rule = self.engine.skills.rules.get(copied_skill_id)
            if copied_rule is None:
                return
            if copied_rule.target_team in (2, 3):
                # 复制到友方辅助/治疗/护盾或自身技能时，自动切回己方阵营，避免反手资敌。
                copied_target_id = caster.actor_id if self.target_allowed(caster, caster, copied_rule) else 0
            else:
                copied_target_id = receiver.actor_id if self.target_allowed(caster, receiver, copied_rule) else 0
            mirror_scale = row.ratio if row.ratio > 0.0 else 1.0
            copied_action = self.engine.protocol.message(
                "BattleAction",
                action_type=_ACTION_SKILL,
                skill_table_id=copied_skill_id,
                target_id=copied_target_id,
            )
            self.execute_skill(
                caster,
                copied_action,
                copied_rule,
                turn,
                group,
                is_mirror=True,
                scale=mirror_scale,
                rank_override=rank,
            )
            return

        if op == BattleEffectOp.DAMAGE:
            if rule is None:
                return
            base = self.engine.skills._base_damage(caster, rule)
            if base <= 0.0 and (row.flat_value > 0 or row.per_level > 0):
                base = row.flat_value + row.per_level * caster.level
            if base <= 0.0:
                base = 10.0
            is_primary = receiver.actor_id == primary_target_id
            self.engine._damage(
                turn, group, caster, receiver, rule, skill_id=skill_id,
                hit_kind=(_HIT_MIRROR if is_mirror else 0), hit_index=hit,
                base=base, rank=rank, is_multi_target=is_multi_target,
                is_primary_target=is_primary, is_primary_focus=(primary_focus_bonus and is_primary),
                damage_scale=scale,
                damage_origin=(_DAMAGE_ORIGIN_MIRROR if is_mirror else 0),
                paid_mana=paid_mana,
            )
            return

        if op == BattleEffectOp.HEAL:
            raw = 0.0
            if rule is not None and (rule.heal_multiplier > 0 or rule.heal_per_level > 0):
                raw = self.engine.skills._heal_amount(caster, receiver, rule, rank)
            else:
                basis = (
                    float(caster.magic_attack)
                    if row.value_basis == 2
                    else float(receiver.max_health)
                    if row.value_basis == 3
                    else float(receiver.max_mana)
                    if row.value_basis == 4
                    else float(caster.physical_attack)
                    if row.value_basis == 1
                    else 0.0
                )
                raw = PetAndSupportEffects.support_base_value(row.flat_value, row.per_level, caster.level, basis, row.ratio, rank)
            if scale != 1.0:
                raw *= scale
            if getattr(self.engine, "active_leyline", 0) == TOURNAMENT_LEYLINE_WOOD:
                raw *= 110.0 / 100.0
            support_bonus_pct = getattr(self.engine, "wuxing_support_bonus_pct", lambda _a: 0)(caster)
            if support_bonus_pct > 0:
                raw *= (100.0 + support_bonus_pct) / 100.0
            if getattr(self.engine, "escalation_stacks", 0) > 0:
                raw *= self.engine.escalation_support_pct / 100.0
            eq = self.engine.equipment_effects
            season_seal_heal_gain = 0
            if (
                not is_mirror
                and hit == 0
                and receiver.actor_id == primary_target_id
                and receiver.max_health > 0
                and SeasonBattleRules.seal_available(caster, 4, self.engine.round_index)
            ):
                seal = caster.equipment_effects.season_seal
                if receiver.attributes.health * 10_000 <= receiver.max_health * seal.health_threshold_bps:
                    before_seal = landed_heal(receiver, raw)
                    raw_after_seal = raw * (1.0 + seal.bonus_bps / 10_000.0)
                    after_seal = landed_heal(receiver, raw_after_seal)
                    if after_seal > before_seal:
                        season_seal_heal_gain = after_seal - before_seal
                        raw = raw_after_seal
            before_r7 = 0
            apply_r7 = False
            if (
                not is_mirror
                and eq.active_next_round_boost_caster_id == caster.actor_id
                and eq.active_next_round_boost_kind == SeasonBattleRules.RESONANCE_GUARD_HEAL_BOOST
                and eq.active_next_round_boost_pct > 0
            ):
                before_r7 = landed_heal(receiver, raw)
                raw *= (100.0 + eq.active_next_round_boost_pct) / 100.0
                apply_r7 = True
            value = landed_heal(receiver, raw)
            if apply_r7 and value > before_r7:
                eq.active_next_round_boost_extra = min(
                    U64_MAX, eq.active_next_round_boost_extra + (value - before_r7),
                )
            if value > 0:
                receiver.attributes.health += value
                if season_seal_heal_gain > 0 and SeasonBattleRules.seal_available(caster, 4, self.engine.round_index):
                    seal = caster.equipment_effects.season_seal
                    SeasonBattleRules.use_seal(
                        caster, receiver.actor_id, season_seal_heal_gain, self.engine.round_index, turn, group
                    )
                    eq.record_season_seal_use(caster.actor_id, season_seal_heal_gain)
                    if (
                        int(getattr(seal, "resonance_kind", 0)) == SeasonBattleRules.RESONANCE_EMERGENCY_HEAL_REFUND
                        and int(getattr(seal, "resonance_skill_id", 0)) == skill_id
                        and int(getattr(seal, "resonance_param", 0)) > 0
                    ):
                        mana_before = caster.attributes.mana
                        missing_mana = max(0, caster.max_mana - mana_before)
                        refund = min(
                            missing_mana,
                            SeasonBattleRules.compute_mana_pct_ceil(paid_mana, int(seal.resonance_param)),
                        )
                        if refund > 0:
                            caster.attributes.mana = mana_before + refund
                            turn.events.add(
                                event_type=_EVENT_MANA,
                                source_id=caster.actor_id,
                                target_id=caster.actor_id,
                                group_id=group,
                                skill_table_id=skill_id,
                                value=refund,
                                mana_delta=min(refund, _I64_MAX),
                                target_health_after=caster.attributes.health,
                                target_mana_after=caster.attributes.mana,
                            )
                            eq.emit_season_resonance(
                                caster,
                                caster.actor_id,
                                SeasonBattleRules.RESONANCE_EVENT_EMERGENCY_HEAL_REFUND,
                                refund,
                                turn,
                                group,
                            )
                self.engine._event(
                    turn, group, _EVENT_HEAL, caster, receiver, skill_table_id=skill_id, hit_index=hit,
                    value=value, target_health_after=receiver.attributes.health, target_mana_after=receiver.attributes.mana,
                )
            return

        if op in (BattleEffectOp.SHIELD, BattleEffectOp.DAMAGE_REDUCE):
            basis = (
                float(caster.magic_attack)
                if row.value_basis == 2
                else float(receiver.max_health)
                if row.value_basis == 3
                else float(receiver.max_mana)
                if row.value_basis == 4
                else float(caster.physical_attack)
                if row.value_basis == 1
                else 0.0
            )
            raw = PetAndSupportEffects.support_base_value(row.flat_value, row.per_level, caster.level, basis, row.ratio, rank)
            if scale != 1.0:
                raw *= scale
            if raw <= 0.0:
                raw = max(1.0, float(receiver.max_health) * 0.1)
            self.engine.equipment_effects.grant_shield(
                receiver, raw, source=caster, buff_table_id=buff_id,
                remain_rounds=row.config_duration_rounds or row.duration_rounds or 2,
                round_index=self.engine.round_index, turn=turn, group=group, commit_artifacts=(hit == 0 and not is_mirror),
                skill_id=skill_id, is_mirror=is_mirror,
            )
            return

        if op == BattleEffectOp.DISPEL_ALLY:
            self.engine.encounter.clear_sunder_on_ally_dispel(caster, receiver, turn, group)
            removed = self.engine.effects.dispel(receiver, max(1, rule.dispel_count if rule else 1), turn, group, hit)
            if removed:
                self.engine.record_dispel_landed(caster.actor_id, removed)
                self.engine.encounter.on_boss_weekly_affix_dispel_triggered(caster.actor_id, turn, group)
            return

        if op == BattleEffectOp.DISPEL_ENEMY:
            self.engine.encounter.try_dispel_boss_stance(caster, receiver, turn, group)
            return

        # 持久化 Buff / 控制 / 蓄力 / 易伤 / 免控分支
        if hostile and op == BattleEffectOp.CONTROL and 1 <= row.state_kind <= 6:
            control_banned_fn = getattr(self.engine, "is_control_banned_for_actor", None)
            if control_banned_fn(caster) if callable(control_banned_fn) else getattr(self.engine, "ban_control_skills", False):
                turn.events.add(
                    event_type=_EVENT_RESIST,
                    source_id=caster.actor_id,
                    target_id=receiver.actor_id,
                    buff_table_id=buff_id,
                    group_id=group,
                    hit_index=hit,
                )
                return
            if self.try_consume_control_immunity(caster, receiver, buff_id, turn, group, hit):
                return
            base_chance = row.application_chance if row.application_chance > 0.0 else 1.0
            if not self.engine.equipment_effects.roll_ailment_with_equipment(
                caster, receiver, base_chance, row.state_kind, buff_id, self.engine.round_index, turn, group,
                is_primary=(receiver.actor_id == primary_target_id),
            ):
                return
        elif hostile and op == BattleEffectOp.MODIFIER and 0.0 < row.application_chance < 1.0:
            if self.engine.rng.unit() >= row.application_chance:
                turn.events.add(
                    event_type=_EVENT_RESIST, source_id=caster.actor_id, target_id=receiver.actor_id,
                    buff_table_id=buff_id, group_id=group, hit_index=hit,
                )
                return

        instance_id = self.engine.effects.next_buff_id()
        remain = row.config_duration_rounds if row.config_duration_rounds > 0 else (0 if row.infinite else row.duration_rounds)
        mystic_resonance_extended = False
        mystic_bonus_rounds = 0
        eq = self.engine.equipment_effects
        if (
            hostile
            and op == BattleEffectOp.MODIFIER
            and remain > 0
            and not is_mirror
            and eq.pending_mystic_extend_skill_id != 0
            and skill_id == eq.pending_mystic_extend_skill_id
            and caster.actor_id == eq.pending_mystic_extend_caster_id
            and receiver.actor_id == eq.pending_mystic_extend_target_id
            and eq.pending_mystic_extend_bonus_rounds > 0
        ):
            mystic_bonus_rounds = eq.pending_mystic_extend_bonus_rounds
            remain += mystic_bonus_rounds
            eq.pending_mystic_extend_caster_id = 0
            eq.pending_mystic_extend_target_id = 0
            eq.pending_mystic_extend_skill_id = 0
            eq.pending_mystic_extend_bonus_rounds = 0
            mystic_resonance_extended = True
        entry = receiver.buffs.add(
            buff_id=instance_id,
            buff_table_id=buff_id,
            caster_id=caster.actor_id,
            layer=1,
            remain_rounds=remain,
            applied_round=self.engine.round_index,
            source_skill_id=skill_id,
        )
        if row.hit_limit > 0:
            entry.remaining_hits = row.hit_limit
        if op == BattleEffectOp.MODIFIER:
            entry.stat_percent = row.ratio
        if op == BattleEffectOp.PREPARE_CAST:
            entry.secondary_value = primary_target_id
            self.sync_charging_skill_id(receiver)
        turn.events.add(
            event_type=_EVENT_BUFF_ADD,
            source_id=caster.actor_id,
            target_id=receiver.actor_id,
            buff_table_id=buff_id,
            buff_instance_id=instance_id,
            value=1,
            group_id=group,
            hit_index=hit,
        )
        if mystic_resonance_extended:
            eq.emit_season_resonance(
                caster,
                receiver.actor_id,
                SeasonBattleRules.RESONANCE_EVENT_MYSTIC_EXTEND_DEBUFF,
                mystic_bonus_rounds,
                turn,
                group,
            )
        if row.trigger_event == 0 and op not in (BattleEffectOp.PURSUIT_DAMAGE, BattleEffectOp.PREPARE_CAST):
            for child in row.sub_buffs:
                if not any(b.buff_id == instance_id for b in receiver.buffs):
                    break
                self.apply_effect(
                    caster, receiver, child, rule=rule, skill_id=skill_id, rank=rank,
                    primary_target_id=primary_target_id, turn=turn, group=group, hit=hit, depth=depth + 1,
                )

    def execute_skill(
        self,
        actor: Message,
        action: Message,
        rule: BattleSkill,
        turn: Message,
        group: int,
        *,
        is_mirror: bool = False,
        scale: float = 1.0,
        rank_override: int = 0,
    ) -> None:
        """执行 `battle_model == 1` 配置化主动战法（含镜像复制、副目标策略、蓄力入态与一次性攻击标记消耗）。"""
        skill_id = action.skill_table_id
        if not is_mirror and self.check_skill(actor, action, rule) != 0:
            if rule.target_team == 1 and not self.is_physical_forbidden(actor):
                self.engine._attack_target(turn, group, actor, action.target_id)
            return
        targets = self.select_targets(actor, rule, action.target_id)
        if not targets:
            return
        rank = max(1, rank_override if rank_override > 0 else actor.skill_ranks.get(skill_id, 1))
        if not is_mirror:
            if rule.cooldown_rounds > 0:
                actor.skill_cooldown_rounds[skill_id] = rule.cooldown_rounds + 1
            if rule.max_battle_uses > 0:
                actor.ability_use_counts[skill_id] = actor.ability_use_counts.get(skill_id, 0) + 1
        self.engine._event(
            turn, group, 2, actor, targets[0],
            skill_table_id=skill_id,
            hit_kind=(_HIT_MIRROR if is_mirror else 0),
        )
        if not is_mirror and actor.actor_type == _ACTOR_PLAYER and rule.season_id != 0:
            self.engine.trial_tracker.record_season_skill_cast(actor)
        paid_mana = 0
        eq = self.engine.equipment_effects
        if not is_mirror:
            from .battle_skills import mana_cost as _mana_cost
            cost = _mana_cost(actor, rule)
            if actor.HasField("equipment_effects") and actor.equipment_effects.mana_reduction_bps:
                cost = eq.discount_skill_mana(actor, rule, cost)
            if (
                actor.team_index == 0
                and self.engine.encounter.boss_weekly_affix_id == BOSS_AFFIX_WATER
                and self.engine.encounter.is_weekly_water_discount_skill(rule)
            ):
                cost = discount_water_mana_cost(cost)
            full_cost = cost
            discount_pct = eq.defend_mana_discount_resonance_pct(actor, skill_id, self.engine.round_index)
            consume_breath_resonance = discount_pct > 0
            if consume_breath_resonance:
                cost = SeasonBattleRules.compute_mana_discounted(cost, discount_pct)
            paid_mana = cost
            if cost > 0:
                cost = min(cost, actor.attributes.mana)
                paid_mana = cost
                actor.attributes.mana -= cost
                self.engine.skills._mana_event(turn, group, 0, actor, actor, skill_id, -cost)
            if consume_breath_resonance:
                rt = eq.season_seal_runtime(actor.actor_id)
                rt.defend_resonance_ready_round = 0
                saved = max(0, full_cost - paid_mana)
                if saved > 0:
                    rt.contribution_value = min(U64_MAX, rt.contribution_value + saved)
                    eq.emit_season_resonance(
                        actor,
                        actor.actor_id,
                        SeasonBattleRules.RESONANCE_EVENT_BREATH_HALF_MANA,
                        saved,
                        turn,
                        group,
                    )

        subject_max = (rule.pet_max_targets or rule.max_targets) if actor.actor_type == _ACTOR_PET else rule.max_targets
        is_multi = subject_max > 1
        primary_focus = (
            not is_mirror
            and self.engine.is_competitive_pvp_battle
            and actor.actor_type == _ACTOR_PLAYER
            and subject_max > 1
            and action.target_id != 0
            and targets[0].actor_id == action.target_id
            and len(targets) < subject_max
        )
        next_round_boost_active = False
        next_round_boost_kind = SeasonBattleRules.RESONANCE_NONE
        if (
            not is_mirror
            and actor.actor_type == _ACTOR_PLAYER
            and actor.HasField("equipment_effects")
            and actor.equipment_effects.HasField("season_seal")
        ):
            seal = actor.equipment_effects.season_seal
            res_kind = int(getattr(seal, "resonance_kind", 0))
            res_skill = int(getattr(seal, "resonance_skill_id", 0))
            res_param = int(getattr(seal, "resonance_param", 0))
            if res_skill == skill_id and res_param > 0:
                rt = eq.season_seal_runtime(actor.actor_id)
                if (
                    res_kind in (
                        SeasonBattleRules.RESONANCE_GUARD_SHIELD_BOOST,
                        SeasonBattleRules.RESONANCE_GUARD_HEAL_BOOST,
                    )
                    and not rt.guard_resonance_consumed
                    and rt.guard_resonance_ready_round == self.engine.round_index
                ):
                    rt.guard_resonance_consumed = True
                    rt.guard_resonance_ready_round = 0
                    next_round_boost_active = True
                    next_round_boost_kind = res_kind
                    eq.active_next_round_boost_caster_id = actor.actor_id
                    eq.active_next_round_boost_kind = res_kind
                    eq.active_next_round_boost_pct = res_param
                    eq.active_next_round_boost_extra = 0
                elif (
                    res_kind == SeasonBattleRules.RESONANCE_DEFEND_SHIELD_BOOST
                    and rt.defend_resonance_ready_round == self.engine.round_index
                ):
                    rt.defend_resonance_ready_round = 0
                    next_round_boost_active = True
                    next_round_boost_kind = res_kind
                    eq.active_next_round_boost_caster_id = actor.actor_id
                    eq.active_next_round_boost_kind = res_kind
                    eq.active_next_round_boost_pct = res_param
                    eq.active_next_round_boost_extra = 0
        effects_begin = len(turn.events)
        primary_target_id = targets[0].actor_id
        for idx, target in enumerate(targets):
            if target.fled or (target.is_dead and not rule.allow_dead_target):
                continue
            for eff_id in rule.effects:
                eff = self.engine.effects.rules.get(eff_id)
                if eff is None:
                    continue
                if idx > 0 and eff.effect_target == 1:
                    continue
                self.apply_effect(
                    actor, target, eff_id, rule=rule, skill_id=skill_id, rank=rank,
                    primary_target_id=primary_target_id, turn=turn, group=group, hit=idx,
                    is_multi_target=is_multi, primary_focus_bonus=primary_focus,
                    scale=scale, is_mirror=is_mirror, paid_mana=paid_mana,
                )
            eq.pending_mystic_extend_caster_id = 0
            eq.pending_mystic_extend_target_id = 0
            eq.pending_mystic_extend_skill_id = 0
            eq.pending_mystic_extend_bonus_rounds = 0
        if next_round_boost_active and eq.active_next_round_boost_caster_id == actor.actor_id:
            extra_val = eq.active_next_round_boost_extra
            eq.active_next_round_boost_caster_id = 0
            eq.active_next_round_boost_kind = SeasonBattleRules.RESONANCE_NONE
            eq.active_next_round_boost_pct = 0
            eq.active_next_round_boost_extra = 0
            if extra_val > 0 and next_round_boost_kind > 0:
                rt = eq.season_seal_runtime(actor.actor_id)
                rt.contribution_value = min(U64_MAX, rt.contribution_value + extra_val)
                eq.emit_season_resonance(
                    actor,
                    actor.actor_id,
                    SeasonBattleRules.RESONANCE_EVENT_BASE + next_round_boost_kind,
                    extra_val,
                    turn,
                    group,
                )

        if is_mirror:
            return
        hit_directly = False
        landed_direct_strike = False
        for ev in turn.events[effects_begin:]:
            if (
                ev.event_type == 3
                and ev.source_id == actor.actor_id
                and ev.skill_table_id == skill_id
                and ev.damage_origin == 0
            ):
                if ev.value > 0:
                    hit_directly = True
                if ev.value > 0 or ev.absorbed > 0:
                    landed_direct_strike = True
        if landed_direct_strike and _active(actor):
            self.consume_one_shot_attack_modifiers(actor, rule.damage_kind, turn, group)
        if hit_directly and _active(actor):
            self.trigger_buffs(actor, targets[0], BattleTrigger.AFTER_SKILL, rule.damage_kind, turn, group, skill_id=skill_id)
            self.trigger_buffs(actor, targets[0], BattleTrigger.AFTER_DIRECT_ATTACK, rule.damage_kind, turn, group, skill_id=skill_id)
        if _active(actor):
            if any(
                (eff := self.engine.effects.rules.get(eid)) is not None
                and eff.battle_op == BattleEffectOp.CONTROL
                and 1 <= eff.state_kind <= 6
                for eid in rule.effects
            ):
                self.trigger_buffs(actor, targets[0], BattleTrigger.AFTER_CONTROL_ATTEMPT, rule.damage_kind, turn, group, skill_id=skill_id)
            if any(
                (eff := self.engine.effects.rules.get(eid)) is not None
                and eff.battle_op in (BattleEffectOp.HEAL, BattleEffectOp.SHIELD, BattleEffectOp.DISPEL_ALLY)
                for eid in rule.effects
            ):
                self.trigger_buffs(actor, targets[0], BattleTrigger.AFTER_SUPPORT_CAST, rule.damage_kind, turn, group, skill_id=skill_id)

    def execute_prepared_cast_if_ready(self, actor: Message, turn: Message, group: int) -> bool:
        """若单位在上回合进入蓄力且本回合未被控制/封印打断，则自动释放蓄力战法（对照 `ExecutePreparedCastIfReady`）。"""
        rules = self.engine.effects.rules
        prepare_entry = None
        for entry in actor.buffs:
            row = rules.get(entry.buff_table_id)
            if row is not None and row.battle_op == BattleEffectOp.PREPARE_CAST and entry.applied_round < self.engine.round_index:
                prepare_entry = entry
                break
        if prepare_entry is None:
            return False

        saved_buff_id = prepare_entry.buff_id
        saved_table_id = prepare_entry.buff_table_id
        saved_skill_id = prepare_entry.source_skill_id
        saved_primary_id = prepare_entry.secondary_value
        prepare_row = rules.get(saved_table_id)
        skill_row = self.engine.skills.rules.get(saved_skill_id)

        blocked = (
            prepare_row is None
            or skill_row is None
            or self.engine.effects.has(actor, STUN, FREEZE)
            or self.has_state(actor, BattleStateKind.FREEZE)
            or self.has_state(actor, BattleStateKind.SLEEP)
            or self.has_state(actor, BattleStateKind.CONFUSION)
            or self.has_state(actor, BattleStateKind.SEAL_ACTIVE)
            or (
                (skill_row.action_tags & ACTION_TAG_PHYSICAL) != 0
                and (self.has_state(actor, BattleStateKind.SEAL_PHYSICAL) or self.is_physical_forbidden(actor))
            )
            or (
                (skill_row.action_tags & ACTION_TAG_MAGIC) != 0
                and (self.has_state(actor, BattleStateKind.SEAL_MAGIC) or self.is_magic_forbidden(actor))
            )
        )
        # 无论释放或被打断，蓄力状态均在本回合窗口消耗清除（打断不退蓝、不退冷却）。
        self.remove_buff(actor, saved_buff_id, turn, group)
        if blocked:
            return False

        targets = self.select_targets(actor, skill_row, saved_primary_id)
        if not targets:
            return True
        rank = max(1, actor.skill_ranks.get(saved_skill_id, 1))
        subject_max = (skill_row.pet_max_targets or skill_row.max_targets) if actor.actor_type == _ACTOR_PET else skill_row.max_targets
        is_multi = subject_max > 1
        primary_focus = (
            self.engine.is_competitive_pvp_battle
            and actor.actor_type == _ACTOR_PLAYER
            and subject_max > 1
            and saved_primary_id != 0
            and targets[0].actor_id == saved_primary_id
            and len(targets) < subject_max
        )
        self.engine._event(turn, group, 2, actor, targets[0], skill_table_id=saved_skill_id, hit_kind=0)
        effects_begin = len(turn.events)
        primary_target_id = targets[0].actor_id
        charge_scale = (
            (100.0 + BOSS_AFFIX_WATER_CHARGE_OR_PERIODIC_DAMAGE_BONUS_PCT) / 100.0
            if (
                actor.team_index == 0
                and self.engine.encounter.boss_weekly_affix_id == BOSS_AFFIX_WATER
            )
            else 1.0
        )
        for idx, target in enumerate(targets):
            if target.fled:
                continue
            for child_buff_id in prepare_row.sub_buffs:
                self.apply_effect(
                    actor, target, child_buff_id, rule=skill_row, skill_id=saved_skill_id, rank=rank,
                    primary_target_id=primary_target_id, turn=turn, group=group, hit=idx,
                    is_multi_target=is_multi, primary_focus_bonus=primary_focus, scale=charge_scale, depth=1,
                )

        hit_directly = any(
            ev.event_type == 3
            and ev.source_id == actor.actor_id
            and ev.skill_table_id == saved_skill_id
            and ev.value > 0
            and ev.damage_origin == 0
            for ev in turn.events[effects_begin:]
        )
        if hit_directly and _active(actor):
            self.consume_one_shot_attack_modifiers(actor, skill_row.damage_kind, turn, group)
            self.trigger_buffs(actor, targets[0], BattleTrigger.AFTER_SKILL, skill_row.damage_kind, turn, group, skill_id=saved_skill_id)
        return True

    def trigger_buffs(
        self, owner: Message, other: Message, trigger: int, damage_type: int, turn: Message, group: int,
        *, skill_id: int = 0,
    ) -> None:
        """触发普攻后/战法后的配置化监听被动（如 E05 截灵追击 / E06 封锋追击）。"""
        if not _active(owner) or not _active(other):
            return
        rules = self.engine.effects.rules
        ids = [entry.buff_id for entry in owner.buffs]
        for bid in ids:
            idx = next((i for i, e in enumerate(owner.buffs) if e.buff_id == bid), -1)
            if idx < 0 or not _active(owner):
                continue
            saved = owner.buffs[idx]
            row = rules.get(saved.buff_table_id)
            if row is None or row.battle_op == 0 or row.trigger_event != trigger:
                continue
            if row.damage_filter == 1 and damage_type != _DAMAGE_PHYSICAL:
                continue
            if row.damage_filter == 2 and damage_type != _DAMAGE_MAGIC:
                continue
            if (
                row.per_round_limit > 0
                and saved.last_trigger_round == self.engine.round_index
                and saved.trigger_count >= row.per_round_limit
            ):
                continue
            if 0.0 < row.trigger_chance < 1.0 and self.engine.rng.unit() >= row.trigger_chance:
                continue
            saved.trigger_count = (saved.trigger_count + 1) if saved.last_trigger_round == self.engine.round_index else 1
            saved.last_trigger_round = self.engine.round_index
            src_skill = self.engine.skills.rules.get(saved.source_skill_id)
            rank = max(1, owner.skill_ranks.get(saved.source_skill_id, 1))
            if row.battle_op == BattleEffectOp.GROWING_AURA:
                cap = max(1, row.stack_limit)
                if saved.stacks < cap:
                    saved.stacks += 1
            elif row.battle_op in (BattleEffectOp.APPLY_CHILDREN, BattleEffectOp.EVASION_CHARGE):
                for child in row.sub_buffs:
                    self.apply_effect(
                        owner, owner, child, rule=src_skill, skill_id=saved.source_skill_id, rank=rank,
                        primary_target_id=owner.actor_id, turn=turn, group=group, hit=0, depth=1,
                    )
                for child in row.target_sub_buffs:
                    self.apply_effect(
                        owner, other, child, rule=src_skill, skill_id=saved.source_skill_id, rank=rank,
                        primary_target_id=other.actor_id, turn=turn, group=group, hit=0, depth=1,
                    )
            cur_idx = next((i for i, e in enumerate(owner.buffs) if e.buff_id == bid), -1)
            if cur_idx >= 0 and owner.buffs[cur_idx].remaining_hits > 0:
                if owner.buffs[cur_idx].remaining_hits == 1:
                    self.remove_buff(owner, bid, turn, group)
                else:
                    owner.buffs[cur_idx].remaining_hits -= 1


# ---------------------------------------------------------------------------
# 9. 五行玩法扩展纯规则（对照 wuxing_expansion_rules.h）
# ---------------------------------------------------------------------------

WUXING_ELEMENT_NONE = 0
WUXING_ELEMENT_METAL = 1
WUXING_ELEMENT_WOOD = 2
WUXING_ELEMENT_WATER = 3
WUXING_ELEMENT_FIRE = 4
WUXING_ELEMENT_EARTH = 5

MAX_WUXING_FORMATION_ID = 5
WUXING_TOWER_MAX_FLOORS = 10
WUXING_TOWER_OFFER_COUNT = 3
MAX_SAVED_BUILD_BOOKMARKS = 12
WUXING_SYNERGY_INTERVAL_ROUNDS = 3
WUXING_SYNERGY_BONUS_PCT = 10
WUXING_FORMATION_COUNTER_BONUS_PCT = 5
WUXING_FIRE_CRIT_BURN_CAP_PER_HIT = 1500

_WUXING_DAY_MS = 86_400_000
_WUXING_WEEK_MS = 7 * _WUXING_DAY_MS
_WUXING_FIRST_MONDAY_05_UTC8_MS = 3 * _WUXING_DAY_MS + 21 * 3_600_000

_TOWER_FLOOR_DUNGEONS: dict[int, int] = {
    1: 101,
    2: 103,
    3: 105,
    4: 107,
    5: 103,
    6: 101,
    7: 105,
    8: 107,
    9: 103,
    10: 107,
}


def wuxing_week_key(now_ms_val: int) -> int:
    """按 UTC+8 周一 05:00 计算五行幻境周键（正整数单调递增）。"""
    if now_ms_val < _WUXING_FIRST_MONDAY_05_UTC8_MS:
        return 1
    return int((now_ms_val - _WUXING_FIRST_MONDAY_05_UTC8_MS) // _WUXING_WEEK_MS) + 1


def element_of_class(class_id: int) -> int:
    """角色门派/职业映射到五行属性（1..5，0=无）。"""
    return WUXING_ELEMENT_NONE if class_id == 0 else ((class_id - 1) % 5) + 1


def element_of_pet(pet_table_id: int) -> int:
    """灵宠表 ID 映射到五行属性（1..5，0=无）。"""
    return WUXING_ELEMENT_NONE if pet_table_id == 0 else ((pet_table_id - 1) % 5) + 1


def element_generates(producer: int, receiver: int) -> bool:
    """五行相生链：金(1)→水(3)→木(2)→火(4)→土(5)→金(1)。"""
    return (producer, receiver) in (
        (WUXING_ELEMENT_METAL, WUXING_ELEMENT_WATER),
        (WUXING_ELEMENT_WATER, WUXING_ELEMENT_WOOD),
        (WUXING_ELEMENT_WOOD, WUXING_ELEMENT_FIRE),
        (WUXING_ELEMENT_FIRE, WUXING_ELEMENT_EARTH),
        (WUXING_ELEMENT_EARTH, WUXING_ELEMENT_METAL),
    )


def are_elements_mutually_generating(first: int, second: int) -> bool:
    """判定两五行是否互为相生（人生宠或宠生人皆触发）。"""
    return element_generates(first, second) or element_generates(second, first)


def is_synergy_round(round_index: int) -> bool:
    """判定指定回合是否触发人宠五行相生合击（第 3/6/9... 回合）。"""
    return round_index > 0 and (round_index % WUXING_SYNERGY_INTERVAL_ROUNDS) == 0


@dataclass(frozen=True, slots=True)
class WuxingFormationDef:
    """五套免费道家阵法定义（与 C++ `WuxingFormationDef` 逐项一致）。"""

    id: int = 0
    element: int = WUXING_ELEMENT_NONE
    counters_id: int = 0
    damage_bonus_pct: int = 0
    damage_taken_reduce_pct: int = 0
    speed_bonus_pct: int = 0
    heal_shield_bonus_pct: int = 0
    control_resist_pct: int = 0
    round_mana_regen_pct: int = 0
    crit_bonus_pct: int = 0
    pet_damage_bonus_pct: int = 0
    opening_shield_pct: int = 0


_EMPTY_FORMATION = WuxingFormationDef()

WUXING_FORMATIONS: tuple[WuxingFormationDef, ...] = (
    # 1 天覆阵（金）：直伤 +5%、速度 +2%，克地载阵(2)
    WuxingFormationDef(1, WUXING_ELEMENT_METAL, 2, damage_bonus_pct=5, speed_bonus_pct=2),
    # 2 地载阵（土）：受击减伤 +6%、开局 4% 护盾，克风扬阵(3)
    WuxingFormationDef(2, WUXING_ELEMENT_EARTH, 3, damage_taken_reduce_pct=6, opening_shield_pct=4),
    # 3 风扬阵（木）：速度 +6%、治疗与护盾 +4%，克云垂阵(4)
    WuxingFormationDef(3, WUXING_ELEMENT_WOOD, 4, speed_bonus_pct=6, heal_shield_bonus_pct=4),
    # 4 云垂阵（水）：控制抗性 +8%、回合初回复 3% 法力，克虎翼阵(5)
    WuxingFormationDef(4, WUXING_ELEMENT_WATER, 5, control_resist_pct=8, round_mana_regen_pct=3),
    # 5 虎翼阵（火）：暴击率 +4%、出战灵宠伤害 +8%，克天覆阵(1)
    WuxingFormationDef(5, WUXING_ELEMENT_FIRE, 1, crit_bonus_pct=4, pet_damage_bonus_pct=8),
)


def is_valid_formation_id(formation_id: int) -> bool:
    """校验五行阵法 ID 是否合法（0..5）。"""
    return 0 <= formation_id <= MAX_WUXING_FORMATION_ID


def formation_of(formation_id: int) -> WuxingFormationDef:
    """按阵法 ID 返回定义，0 或越界返回空阵法。"""
    if 1 <= formation_id <= MAX_WUXING_FORMATION_ID:
        return WUXING_FORMATIONS[formation_id - 1]
    return _EMPTY_FORMATION


def formation_counter_bonus_pct(attacker_formation_id: int, defender_formation_id: int) -> int:
    """判定阵法克制环（1>2>3>4>5>1），克制方获得 +5% 直伤。"""
    if not (1 <= attacker_formation_id <= MAX_WUXING_FORMATION_ID and 1 <= defender_formation_id <= MAX_WUXING_FORMATION_ID):
        return 0
    return (
        WUXING_FORMATION_COUNTER_BONUS_PCT
        if WUXING_FORMATIONS[attacker_formation_id - 1].counters_id == defender_formation_id
        else 0
    )


@dataclass(frozen=True, slots=True)
class WuxingTowerSigilDef:
    """五行幻境 15 枚局内五行符印定义（与 C++ `WuxingTowerSigilDef` 逐项一致）。"""

    id: int = 0
    element: int = WUXING_ELEMENT_NONE
    rarity: int = 1
    damage_pct: int = 0
    crit_pct: int = 0
    crit_damage_pct: int = 0
    shield_break_bonus_pct: int = 0
    heal_shield_pct: int = 0
    pet_damage_pct: int = 0
    round_hp_regen_pct: int = 0
    round_mana_regen_pct: int = 0
    control_resist_pct: int = 0
    on_hit_mana_regen_pct: int = 0
    damage_taken_reduce_pct: int = 0
    first3_rounds_damage_pct: int = 0
    single_target_damage_pct: int = 0
    crit_burn_max_hp_pct: int = 0
    opening_shield_pct: int = 0
    reflect_pct: int = 0


WUXING_TOWER_SIGILS: tuple[WuxingTowerSigilDef, ...] = (
    # 金系（101..103）
    WuxingTowerSigilDef(101, WUXING_ELEMENT_METAL, 1, damage_pct=8),
    WuxingTowerSigilDef(102, WUXING_ELEMENT_METAL, 2, crit_pct=12, shield_break_bonus_pct=25),
    WuxingTowerSigilDef(103, WUXING_ELEMENT_METAL, 3, damage_pct=14, crit_damage_pct=20),
    # 木系（201..203）
    WuxingTowerSigilDef(201, WUXING_ELEMENT_WOOD, 1, heal_shield_pct=12),
    WuxingTowerSigilDef(202, WUXING_ELEMENT_WOOD, 2, pet_damage_pct=18),
    WuxingTowerSigilDef(203, WUXING_ELEMENT_WOOD, 3, pet_damage_pct=22, round_hp_regen_pct=5),
    # 水系（301..303）
    WuxingTowerSigilDef(301, WUXING_ELEMENT_WATER, 1, round_mana_regen_pct=8),
    WuxingTowerSigilDef(302, WUXING_ELEMENT_WATER, 2, control_resist_pct=15, on_hit_mana_regen_pct=4),
    WuxingTowerSigilDef(303, WUXING_ELEMENT_WATER, 3, round_mana_regen_pct=10, control_resist_pct=20, damage_taken_reduce_pct=8),
    # 火系（401..403）
    WuxingTowerSigilDef(401, WUXING_ELEMENT_FIRE, 1, first3_rounds_damage_pct=14),
    WuxingTowerSigilDef(402, WUXING_ELEMENT_FIRE, 2, single_target_damage_pct=12),
    WuxingTowerSigilDef(403, WUXING_ELEMENT_FIRE, 3, damage_pct=16, crit_burn_max_hp_pct=3),
    # 土系（501..503）
    WuxingTowerSigilDef(501, WUXING_ELEMENT_EARTH, 1, opening_shield_pct=12),
    WuxingTowerSigilDef(502, WUXING_ELEMENT_EARTH, 2, reflect_pct=12),
    WuxingTowerSigilDef(503, WUXING_ELEMENT_EARTH, 3, damage_taken_reduce_pct=12, opening_shield_pct=20, reflect_pct=15),
)

_TOWER_SIGIL_BY_ID: dict[int, WuxingTowerSigilDef] = {d.id: d for d in WUXING_TOWER_SIGILS}


def find_tower_sigil(sigil_id: int) -> WuxingTowerSigilDef | None:
    """按符印 ID 查找定义，不存在返回 None。"""
    return _TOWER_SIGIL_BY_ID.get(sigil_id)


def is_valid_tower_sigil_id(sigil_id: int) -> bool:
    """校验符印 ID 是否属于 15 枚合法五行幻境符印。"""
    return sigil_id in _TOWER_SIGIL_BY_ID


@dataclass(slots=True)
class WuxingTowerSigilBonuses:
    """聚合本轮五行幻境全部已选符印的局内确定性加成。"""

    damage_pct: int = 0
    crit_pct: int = 0
    crit_damage_pct: int = 0
    shield_break_bonus_pct: int = 0
    heal_shield_pct: int = 0
    pet_damage_pct: int = 0
    round_hp_regen_pct: int = 0
    round_mana_regen_pct: int = 0
    control_resist_pct: int = 0
    on_hit_mana_regen_pct: int = 0
    damage_taken_reduce_pct: int = 0
    first3_rounds_damage_pct: int = 0
    single_target_damage_pct: int = 0
    crit_burn_max_hp_pct: int = 0
    opening_shield_pct: int = 0
    reflect_pct: int = 0


def aggregate_tower_sigils(sigil_ids: Iterable[int]) -> WuxingTowerSigilBonuses:
    """去重聚合已选幻境符印的数值加成（忽略重复 ID）。"""
    total = WuxingTowerSigilBonuses()
    seen: set[int] = set()
    for sid in sigil_ids:
        if sid in seen:
            continue
        defn = _TOWER_SIGIL_BY_ID.get(sid)
        if defn is None:
            continue
        seen.add(sid)
        total.damage_pct += defn.damage_pct
        total.crit_pct += defn.crit_pct
        total.crit_damage_pct += defn.crit_damage_pct
        total.shield_break_bonus_pct += defn.shield_break_bonus_pct
        total.heal_shield_pct += defn.heal_shield_pct
        total.pet_damage_pct += defn.pet_damage_pct
        total.round_hp_regen_pct += defn.round_hp_regen_pct
        total.round_mana_regen_pct += defn.round_mana_regen_pct
        total.control_resist_pct += defn.control_resist_pct
        total.on_hit_mana_regen_pct += defn.on_hit_mana_regen_pct
        total.damage_taken_reduce_pct += defn.damage_taken_reduce_pct
        total.first3_rounds_damage_pct += defn.first3_rounds_damage_pct
        total.single_target_damage_pct += defn.single_target_damage_pct
        total.crit_burn_max_hp_pct += defn.crit_burn_max_hp_pct
        total.opening_shield_pct += defn.opening_shield_pct
        total.reflect_pct += defn.reflect_pct
    return total


def tower_floor_dungeon_id(floor: int) -> int:
    """幻境层数映射到既有稳定首领试炼普通副本 ID。"""
    return _TOWER_FLOOR_DUNGEONS.get(floor, 101)


def tower_floor_leyline_element(week_key: int, floor: int) -> int:
    """按周键与层数确定性计算本层环境五行灵脉（1..5）。"""
    clamped = floor if 1 <= floor <= WUXING_TOWER_MAX_FLOORS else 1
    return ((week_key + clamped - 1) % 5) + 1


def tower_monster_health_pct(floor: int) -> int:
    """计算五行幻境指定层数的怪物气血倍率百分比。"""
    f = floor if 1 <= floor <= WUXING_TOWER_MAX_FLOORS else 1
    pct = 85 + (f - 1) * 8
    if f == 5:
        pct += 10
    if f == 10:
        pct += 20
    return pct


def tower_monster_attack_pct(floor: int) -> int:
    """计算五行幻境指定层数的怪物攻击倍率百分比。"""
    f = floor if 1 <= floor <= WUXING_TOWER_MAX_FLOORS else 1
    pct = 85 + (f - 1) * 5
    if f == 5:
        pct += 6
    if f == 10:
        pct += 12
    return pct


def mix_seed_u64(x: int) -> int:
    """64 位无符号 SplitMix64 混淆（与 C++ `wuxing::MixSeed` 逐位一致）。"""
    x = (x + 0x9E3779B97F4A7C15) & U64_MAX
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & U64_MAX
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & U64_MAX
    return (x ^ (x >> 31)) & U64_MAX


def roll_tower_sigil_offer(
    week_key: int,
    run_id: int,
    cleared_floor: int,
    owned_sigils: Sequence[int],
) -> list[int]:
    """确定性抽取 3 枚不重复且未持有的幻境符印（与 C++ `RollTowerSigilOffer` 逐项一致）。"""
    owned = set(owned_sigils)
    pool = [
        defn.id
        for defn in WUXING_TOWER_SIGILS
        if defn.id not in owned and not (cleared_floor <= 3 and defn.rarity >= 3)
    ]
    if len(pool) < WUXING_TOWER_OFFER_COUNT:
        pool = [defn.id for defn in WUXING_TOWER_SIGILS if defn.id not in owned]
    offers: list[int] = []
    seed_in = (
        ((week_key & 0xFFFFFFFF) << 32)
        ^ mix_seed_u64(run_id & U64_MAX)
        ^ ((cleared_floor * 0x517CC1B727220A95) & U64_MAX)
    ) & U64_MAX
    state = mix_seed_u64(seed_in)
    while pool and len(offers) < WUXING_TOWER_OFFER_COUNT:
        state = mix_seed_u64(state)
        idx = state % len(pool)
        offers.append(pool.pop(idx))
    return offers


def is_valid_custom_duel_round_limit(round_limit_override: int) -> bool:
    """自定义切磋回合上限白名单：0=默认；仅允许 8/12/20 回合。"""
    return round_limit_override in (0, 8, 12, 20)
