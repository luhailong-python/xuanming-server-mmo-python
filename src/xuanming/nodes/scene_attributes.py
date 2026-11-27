"""角色属性面板、加点方案、战斗平滑升级曲线及存档入口。"""

from bisect import bisect_right
from dataclasses import dataclass, field
from inspect import isawaitable
import logging
import time

from ..data import client_request
from ..data.asset_inventory import FEATURE_UNAVAILABLE, INSUFFICIENT, INVALID, UNAVAILABLE, clone
from ..data.attribute_allocation import AllocationError, distribute_points, evaluate_pool, is_unlocked, validate_allocation
from .scene_mission import LEVEL, effective_level
from .scene_skills import _rescale
from .scene_stats import (
    STATS,
    UINT32_MAX,
    UINT64_MAX,
    _equipment,
    class_id,
    free_loadout_schemes_for_level,
    player_stats,
    unlocked_skill_schemes,
)

logger = logging.getLogger(__name__)

_SERVICE = "SceneAttributeClientPlayer"
_CREATE_SCHEME = 3
_POOL_NOT_FOUND = 25000
_POOL_LOCKED = 25001
_SCHEME_NOT_FOUND = 25006
_SCHEME_LIMIT = 25007
_SWITCH_COOLDOWN = 25008
_INVALID_NAME = 25009
_ALREADY_ACTIVE = 25010
_IN_BATTLE = 25011
_INSUFFICIENT_GOLD = 25012
_NO_PLAN = 25013
_NO_CHANGE = 25014
_LOADOUT_ATTRIBUTE_INVALID = 7034
_LOADOUT_ATTRIBUTE_RESET_REQUIRED = 7035
_ALLOCATION_TIPS = {
    AllocationError.POOL_LOCKED: _POOL_LOCKED,
    AllocationError.DIMENSION_NOT_IN_POOL: 25002,
    AllocationError.NOT_ENOUGH_POINTS: 25003,
    AllocationError.CANNOT_DECREASE: 25004,
    AllocationError.CAP_EXCEEDED: 25005,
    AllocationError.NOTHING_TO_CHANGE: _NO_CHANGE,
}

# 角色等级上限与全玩法战斗成长/银两周软上限纯规则常量（与 C++ battle_progression_rules.h 严格对齐）。
MAX_PLAYER_LEVEL = 85
LIGHT_CARD_BONUS_SCHEMES = 3
WUXING_TOWER_LOOKUP_MODE = 99
OUTCOME_TIER_WIN = 1
OUTCOME_TIER_QUALIFIED_NON_WIN = 2
PVP_MIN_ROUNDS_FOR_LOSS = 3
PVP_MIN_MANUAL_ROUNDS_FOR_LOSS = 2
PERMYRIAD_BASE = 10_000
_PVP_PROGRESSION_MODES = frozenset({1, 3, 7, 8, 9, 11})
_GUILD_WAR_MODES = frozenset({9, 10})
_MATCH_MODE_PVP_CHALLENGE = 6


@dataclass(frozen=True, slots=True)
class LevelExpRow:
    level: int
    required_total_exp: int


@dataclass(frozen=True, slots=True)
class ModeRewardRow:
    match_mode: int
    difficulty: int
    outcome_tier: int
    exp_gain: int
    silver_gain: int


@dataclass(frozen=True, slots=True)
class WeeklySilverRule:
    soft_cap: int = 30_000
    diminished_rate_permyriad: int = 2000
    hard_cap: int = 45_000


@dataclass(slots=True)
class LevelExpDisplay:
    total_exp: int = 0
    current_level_exp: int = 0
    next_level_exp: int = 0
    next_level_total_exp: int = 0
    is_max_level: bool = False


@dataclass(slots=True)
class LevelProgressResult:
    stored_level_before: int = 1
    stored_level_after: int = 1
    level_before: int = 1
    level_after: int = 1
    total_exp_after: int = 0
    next_level_total_exp: int = 0
    leveled_up: bool = False


@dataclass(slots=True)
class WeeklySilverEval:
    credited_silver: int = 0
    used_after: int = 0
    diminished: bool = False


@dataclass(slots=True)
class BattleProgressionDelta:
    exp_gain: int = 0
    silver_gain: int = 0
    level_before: int = 0
    level_after: int = 0
    total_exp_after: int = 0
    next_level_total_exp: int = 0
    weekly_silver_used_after: int = 0
    weekly_silver_soft_cap: int = 0
    silver_diminished: bool = False
    leveled_up: bool = False
    ledger_changed: bool = False


@dataclass(slots=True)
class SchemeAllocationSummary:
    """7 套战法·加点一体化方案的单个方案四维加点摘要。"""

    allocated: dict[int, int] = field(default_factory=dict)
    remaining: int = 0
    total_points: int = 0


@dataclass(slots=True)
class PreparedSchemeAllocation:
    """战法保存前预校验并准备好的四维加点变更计划。"""

    pool_id: int = 0
    reset_cost_gold: int = 0
    needs_reset: bool = False
    target: dict[int, int] = field(default_factory=dict)


def capped_level(stored_level: int) -> int:
    return min(MAX_PLAYER_LEVEL, max(1, int(stored_level)))


def validate_level_exp_rows(rows: list[LevelExpRow]) -> bool:
    """校验 1..85 经验表完整性：连续无空洞、1 级恒为 0、后续严格单调递增。"""
    if len(rows) != MAX_PLAYER_LEVEL:
        return False
    prev_exp = 0
    for expected in range(1, MAX_PLAYER_LEVEL + 1):
        row = rows[expected - 1]
        if row.level != expected:
            return False
        if expected == 1:
            if row.required_total_exp != 0:
                return False
        elif row.required_total_exp <= prev_exp:
            return False
        prev_exp = row.required_total_exp
    return True


def validate_weekly_silver_rule(rule: WeeklySilverRule) -> bool:
    return rule.diminished_rate_permyriad <= PERMYRIAD_BASE and rule.hard_cap >= rule.soft_cap


def compute_level_exp_display(eff_level: int, total_exp: int, sorted_rows: list[LevelExpRow]) -> LevelExpDisplay:
    """计算面板与战果展示用的本级经验进度（绝不下溢）。"""
    out = LevelExpDisplay(total_exp=total_exp)
    lvl = capped_level(eff_level)
    if lvl >= MAX_PLAYER_LEVEL:
        out.is_max_level = True
        return out
    if not validate_level_exp_rows(sorted_rows):
        return out
    cur_req = sorted_rows[lvl - 1].required_total_exp
    next_req = sorted_rows[lvl].required_total_exp
    out.next_level_total_exp = next_req
    out.next_level_exp = next_req - cur_req if next_req > cur_req else 0
    if total_exp >= cur_req and out.next_level_exp > 0:
        out.current_level_exp = min(total_exp - cur_req, out.next_level_exp)
    return out


def resolve_level_for_exp(
    stored_level: int, current_total_exp: int, exp_gain: int, sorted_rows: list[LevelExpRow],
) -> tuple[bool, LevelProgressResult]:
    """按累计总经验推进存储等级（绝不降级、不覆盖超上限旧档、溢出返回 False）。"""
    out = LevelProgressResult()
    if exp_gain > UINT64_MAX - current_total_exp:
        return False, out
    out.stored_level_before = stored_level
    out.level_before = capped_level(stored_level)
    out.total_exp_after = current_total_exp + exp_gain
    if not sorted_rows or not validate_level_exp_rows(sorted_rows) or stored_level >= MAX_PLAYER_LEVEL:
        out.stored_level_after = stored_level
        out.level_after = capped_level(stored_level)
    else:
        thresholds = [r.required_total_exp for r in sorted_rows]
        idx = bisect_right(thresholds, out.total_exp_after)
        target_level = sorted_rows[idx - 1].level if idx > 0 else 1
        target_level = min(target_level, MAX_PLAYER_LEVEL)
        base_stored = 1 if stored_level == 0 else stored_level
        out.stored_level_after = max(base_stored, target_level)
        out.level_after = capped_level(out.stored_level_after)
        out.leveled_up = out.stored_level_after > base_stored
    display = compute_level_exp_display(out.level_after, out.total_exp_after, sorted_rows)
    out.next_level_total_exp = display.next_level_total_exp
    return True, out


def is_pvp_progression_mode(match_mode: int) -> bool:
    return match_mode in _PVP_PROGRESSION_MODES


def is_guild_war_mode(match_mode: int) -> bool:
    return match_mode in _GUILD_WAR_MODES


def resolve_battle_lookup_key(settlement) -> tuple[bool, int, int, int]:
    """判定单局是否具备基础经验/银两资格，返回 (ok, lookup_mode, difficulty, outcome_tier)。"""
    if settlement.outcome in (0, 4) or settlement.total_rounds == 0 or settlement.fled:
        return False, 0, 0, 0
    boss_kind = int(getattr(settlement, "boss_trial_kind", 0))
    if (
        (settlement.HasField("festival_beast") and settlement.festival_beast.activity_instance_id != 0)
        or (9 <= boss_kind <= 11)
    ):
        return False, 0, 0, 0
    mode = int(settlement.match_mode)
    if mode in (0, _MATCH_MODE_PVP_CHALLENGE):
        return False, 0, 0, 0
    won = (
        (settlement.outcome == 1 and settlement.player_team_index == 0)
        or (settlement.outcome == 2 and settlement.player_team_index == 1)
    )
    is_tower = settlement.HasField("tower_settlement") and settlement.tower_settlement.floor != 0
    if is_tower:
        if not won:
            return False, 0, 0, 0
        return True, WUXING_TOWER_LOOKUP_MODE, 0, OUTCOME_TIER_WIN
    if is_pvp_progression_mode(mode):
        if won:
            return True, mode, 0, OUTCOME_TIER_WIN
        if (
            settlement.total_rounds >= PVP_MIN_ROUNDS_FOR_LOSS
            and int(getattr(settlement, "manual_action_rounds", 0)) >= PVP_MIN_MANUAL_ROUNDS_FOR_LOSS
            and bool(getattr(settlement, "effective_combat_contribution", False))
        ):
            return True, mode, 0, OUTCOME_TIER_QUALIFIED_NON_WIN
        return False, 0, 0, 0
    if not won:
        return False, 0, 0, 0
    return True, mode, int(getattr(settlement, "battle_difficulty", 0)), OUTCOME_TIER_WIN


def find_mode_reward(rows: list[ModeRewardRow], lookup_mode: int, difficulty: int, outcome_tier: int) -> ModeRewardRow | None:
    fallback = None
    for row in rows:
        if row.match_mode != lookup_mode or row.outcome_tier != outcome_tier:
            continue
        if row.difficulty == difficulty:
            return row
        if row.difficulty == 0 and fallback is None:
            fallback = row
    return fallback


def evaluate_weekly_silver_cap(raw_silver: int, used_this_week: int, rule: WeeklySilverRule) -> WeeklySilverEval:
    """对原始银两施加每周软上限 + 衰减比例 + 硬封顶（精确跨边界拆分）。"""
    out = WeeklySilverEval(used_after=used_this_week)
    if raw_silver == 0:
        out.diminished = rule.soft_cap > 0 and used_this_week >= rule.soft_cap
        return out
    if rule.hard_cap > 0 and used_this_week >= rule.hard_cap:
        out.diminished = True
        return out
    full_room = rule.soft_cap - used_this_week if rule.soft_cap > used_this_week else 0
    full_part = min(raw_silver, full_room)
    overflow_part = raw_silver - full_part
    diminished_part = (
        (overflow_part * rule.diminished_rate_permyriad) // PERMYRIAD_BASE
        if overflow_part > 0 and rule.diminished_rate_permyriad > 0 else 0
    )
    total = full_part + diminished_part
    if rule.hard_cap > 0:
        hard_room = rule.hard_cap - used_this_week if rule.hard_cap > used_this_week else 0
        total = min(total, hard_room)
    out.credited_silver = total
    out.used_after = used_this_week + total
    out.diminished = overflow_part > 0 or (rule.hard_cap > 0 and out.used_after >= rule.hard_cap)
    return out


_BATTLE_SILVER_WEEK_MS = 7 * 86_400_000
_BATTLE_SILVER_ROLLOVER_MAX_WEEKS = 3


def prepare_economy_ledger(
    current_ledger,
    current_week_start_ms: int,
    settlement,
    explicit_exp: int,
    explicit_silver: int,
    reward_rows: list[ModeRewardRow],
    rule: WeeklySilverRule | None,
) -> tuple[object, BattleProgressionDelta]:
    """统一在当前周账本上计算本局最终经验、实得银两与更新后的周滚存账本候选（最多滚存 3 周）。"""
    candidate = clone(current_ledger)
    delta = BattleProgressionDelta()

    rule_valid = rule is not None and validate_weekly_silver_rule(rule)
    week_reset = False
    if current_week_start_ms > candidate.week_starts_at_ms:
        prev_week_start = int(candidate.week_starts_at_ms)
        prev_used = int(candidate.battle_silver_this_week)
        if rule_valid:
            assert rule is not None
            max_soft = min(UINT64_MAX, rule.soft_cap * _BATTLE_SILVER_ROLLOVER_MAX_WEEKS)
            max_hard = min(UINT64_MAX, rule.hard_cap * _BATTLE_SILVER_ROLLOVER_MAX_WEEKS) if rule.hard_cap > 0 else 0
            next_soft = rule.soft_cap
            next_hard = rule.hard_cap
            if prev_week_start > 0:
                stored_soft = int(getattr(candidate, "silver_rollover_soft_cap", 0))
                stored_hard = int(getattr(candidate, "silver_rollover_hard_cap", 0))
                prev_soft = max(rule.soft_cap, min(max_soft, stored_soft)) if stored_soft > 0 else rule.soft_cap
                prev_hard = (
                    (max(rule.hard_cap, min(max_hard, stored_hard)) if stored_hard > 0 else rule.hard_cap)
                    if rule.hard_cap > 0
                    else 0
                )
                elapsed_weeks = max(1, (current_week_start_ms - prev_week_start) // _BATTLE_SILVER_WEEK_MS)
                added_weeks = min(elapsed_weeks, _BATTLE_SILVER_ROLLOVER_MAX_WEEKS)
                carry_soft = max(0, prev_soft - prev_used)
                next_soft = min(max_soft, max(rule.soft_cap, carry_soft + added_weeks * rule.soft_cap))
                if rule.hard_cap > 0:
                    carry_hard = max(0, prev_hard - prev_used)
                    next_hard = min(max_hard, max(rule.hard_cap, carry_hard + added_weeks * rule.hard_cap))
            if hasattr(candidate, "silver_rollover_soft_cap"):
                candidate.silver_rollover_soft_cap = next_soft
            if hasattr(candidate, "silver_rollover_hard_cap"):
                candidate.silver_rollover_hard_cap = next_hard
        candidate.week_starts_at_ms = current_week_start_ms
        candidate.battle_silver_this_week = 0
        candidate.battle_exp_this_week = 0
        week_reset = True

    table_exp = 0
    table_silver = 0
    if reward_rows:
        ok, lookup_mode, difficulty, outcome_tier = resolve_battle_lookup_key(settlement)
        if ok:
            row = find_mode_reward(reward_rows, lookup_mode, difficulty, outcome_tier)
            if row is not None:
                table_exp = row.exp_gain
                table_silver = 0 if is_guild_war_mode(int(settlement.match_mode)) else row.silver_gain

    final_exp = max(explicit_exp, table_exp)
    raw_silver = explicit_silver if is_guild_war_mode(int(settlement.match_mode)) else max(explicit_silver, table_silver)

    credited_silver = raw_silver
    diminished = False
    used_after = int(candidate.battle_silver_this_week)
    soft_cap = 0

    if rule_valid:
        assert rule is not None
        max_soft = min(UINT64_MAX, rule.soft_cap * _BATTLE_SILVER_ROLLOVER_MAX_WEEKS)
        max_hard = min(UINT64_MAX, rule.hard_cap * _BATTLE_SILVER_ROLLOVER_MAX_WEEKS) if rule.hard_cap > 0 else 0
        stored_soft = int(getattr(candidate, "silver_rollover_soft_cap", 0))
        stored_hard = int(getattr(candidate, "silver_rollover_hard_cap", 0))
        effective_soft = max(rule.soft_cap, min(max_soft, stored_soft)) if stored_soft > 0 else rule.soft_cap
        effective_hard = (
            (max(rule.hard_cap, min(max_hard, stored_hard)) if stored_hard > 0 else rule.hard_cap)
            if rule.hard_cap > 0
            else 0
        )
        effective_rule = WeeklySilverRule(
            soft_cap=effective_soft,
            diminished_rate_permyriad=rule.diminished_rate_permyriad,
            hard_cap=effective_hard,
        )
        soft_cap = effective_rule.soft_cap
        if table_silver > 0:
            ev = evaluate_weekly_silver_cap(raw_silver, int(candidate.battle_silver_this_week), effective_rule)
            credited_silver = ev.credited_silver
            used_after = ev.used_after
            diminished = ev.diminished
            if credited_silver > 0:
                candidate.battle_silver_this_week = used_after

    if final_exp > 0:
        cur_week_exp = int(candidate.battle_exp_this_week)
        candidate.battle_exp_this_week = UINT64_MAX if final_exp > UINT64_MAX - cur_week_exp else cur_week_exp + final_exp

    delta.exp_gain = final_exp
    delta.silver_gain = credited_silver
    delta.weekly_silver_used_after = used_after
    delta.weekly_silver_soft_cap = soft_cap
    delta.silver_diminished = diminished
    delta.ledger_changed = week_reset or final_exp > 0 or (table_silver > 0 and credited_silver > 0)
    return candidate, delta


def _default_name(ordinal: int) -> str:
    return f"方案{'一二三四五六七八九'[ordinal - 1] if 1 <= ordinal <= 9 else ordinal}"


def initialize_attribute_schemes(data) -> None:
    comp = data.attribute_component
    if not comp.schemes:
        comp.schemes.add(scheme_id=1, name=_default_name(1))
        comp.active_scheme_id = 1
    elif not comp.active_scheme_id:
        comp.active_scheme_id = comp.schemes[0].scheme_id
    if not comp.next_scheme_id:
        highest = max(scheme.scheme_id for scheme in comp.schemes)
        comp.next_scheme_id = min(highest + 1, UINT32_MAX)


class SceneAttributes:
    def __init__(self, scene, tables, assets, *, missions=None, allow_gm=False, now_s=None) -> None:
        self.scene, self.protocol, self.tables, self.assets = scene, scene.protocol, tables, assets
        self.missions, self.allow_gm = missions, allow_gm
        self.now_s = now_s or (lambda: time.time_ns() // 1_000_000_000)
        self.rule = next(iter(tables["AttributeRule"].rows()), None)
        self.pools = {row.id: row for row in tables["AttributePool"].rows() if row.owner_type == 0}
        self.dimensions = {row.id: row for row in tables["AttributeDimension"].rows() if row.pool_id in self.pools}
        self.pool_dims = {
            pool: tuple(row.id for row in self.dimensions.values() if row.pool_id == pool) for pool in self.pools
        }
        self.plans = {}
        for plan in tables["AttributeAutoPlan"].rows():
            self.plans.setdefault((plan.class_id, plan.pool_id), plan)
        self.combat_rows = {}
        for row in sorted(tables["EquipAttribute"].rows(), key=lambda row: (row.sort, row.id)):
            if row.effect == 5 and 1 <= row.effect_param <= 15:
                self.combat_rows.setdefault(row.effect_param, row)
        currency = self.protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
        self.gold = currency.values_by_name["CURRENCY_TYPE_GOLD"].number
        types = self.protocol.pool.FindEnumTypeByName("TransactionType")
        self.spend_tx = types.values_by_name["TX_CURRENCY_DEDUCT"].number
        self.mission_tx = types.values_by_name["TX_QUEST_REWARD"].number

    def register(self) -> None:
        self.scene.register(_SERVICE, "GetAttributePanel", self.get_panel)
        self.scene.register(_SERVICE, "AutoAllocateAttributePoints", self.suggest)
        for method, handler in (
            ("AllocateAttributePoints", self.allocate), ("ResetAttributePoints", self.reset),
            ("CreateAttributeScheme", self.create), ("SwitchAttributeScheme", self.switch),
            ("RenameAttributeScheme", self.rename), ("GmSetPlayerLevel", self.set_level),
        ):
            self.scene.register(_SERVICE, method, handler, mutating=True)

    @staticmethod
    def _scheme(comp, scheme_id):
        return next((scheme for scheme in comp.schemes if scheme.scheme_id == scheme_id), None)

    @staticmethod
    def _present(data) -> bool:
        return all(data.HasField(field) for field in (
            "attribute_component", "level_component", "uint32_pb_component", "derived_attributes_component",
        ))

    def _write_error(self, player, data) -> int:
        if not self._present(data):
            return UNAVAILABLE
        if not self.scene.mutable(player):
            return INVALID
        if self.scene.battle and self.scene.battle.frozen(player.player_id):
            return _IN_BATTLE
        return 0

    def _pool_state(self, comp, allocated, pool, level):
        current = {key: allocated.get(key, 0) for key in self.pool_dims[pool.id]}
        foreign = sum(value for key, value in allocated.items() if key not in self.dimensions)
        return current, evaluate_pool(pool, level, current, bonus=comp.bonus_points.get(pool.id, 0), foreign=foreign)

    @staticmethod
    def _reset_cost(pool, level, allocation) -> int:
        return pool.reset_cost_gold if allocation.effective and level >= pool.reset_free_below_level else 0

    def _scheme_cost(self, comp) -> int:
        return self.rule.create_scheme_cost_gold if self.rule and len(comp.schemes) >= self.rule.free_scheme_count else 0

    def _max_schemes(self) -> int:
        return (self.rule.max_schemes or 1) if self.rule else 1

    def _afk_pass_active(self, data) -> bool:
        afk_pass = getattr(self.scene, "afk_pass", None)
        if afk_pass is not None and hasattr(afk_pass, "is_active_data"):
            return bool(afk_pass.is_active_data(data))
        if data.HasField("uint64_pb_component"):
            return int(getattr(data.uint64_pb_component, "afk_pass_expires_at", 0)) > int(self.now_s())
        return False

    def _max_schemes_for(self, data) -> int:
        base = self._max_schemes()
        skills = getattr(self.scene, "skills", None)
        if skills is not None and hasattr(skills, "unlocked_loadout_scheme_count"):
            return max(base, int(skills.unlocked_loadout_scheme_count(data)))
        rented = int(data.skill_list.rented_extra_slots) if data.HasField("skill_list") else 0
        expires = int(data.skill_list.rent_expires_at_sec) if data.HasField("skill_list") else 0
        free = free_loadout_schemes_for_level(effective_level(data))
        unlocked = unlocked_skill_schemes(
            self._afk_pass_active(data), rented, expires, int(self.now_s()), free_schemes=free,
        )
        return max(base, unlocked)

    def _primary_player_pool(self):
        return next(iter(self.pools.values()), None)

    def _find_or_create_unified_scheme(self, comp, scheme_id: int, fallback_name: str):
        existing = self._scheme(comp, scheme_id)
        if existing is not None:
            return existing
        created = comp.schemes.add(scheme_id=scheme_id, name=fallback_name or _default_name(scheme_id))
        if comp.next_scheme_id <= scheme_id:
            comp.next_scheme_id = min(scheme_id + 1, UINT32_MAX)
        return created

    def fill_scheme_allocation_summary(self, data, scheme_id: int) -> SchemeAllocationSummary | None:
        """读取指定流派方案号（1..7）的四维加点摘要（不依赖是否已创建）。"""
        if not self._present(data):
            return None
        pool = self._primary_player_pool()
        if pool is None:
            return None
        comp = data.attribute_component
        scheme = self._scheme(comp, scheme_id)
        allocated = scheme.allocated if scheme is not None else {}
        current, state = self._pool_state(comp, allocated, pool, effective_level(data))
        return SchemeAllocationSummary(
            allocated=dict(current),
            remaining=int(state.remaining),
            total_points=int(state.total),
        )

    def player_pool_reset_cost_gold(self, data) -> int:
        """返回角色当前等级及激活方案下洗点所需银两（30 级以下为 0）。"""
        if not self._present(data):
            return 0
        pool = self._primary_player_pool()
        if pool is None:
            return 0
        comp = data.attribute_component
        scheme = self._scheme(comp, comp.active_scheme_id)
        allocated = scheme.allocated if scheme is not None else {}
        level = effective_level(data)
        _, state = self._pool_state(comp, allocated, pool, level)
        return int(self._reset_cost(pool, level, state))

    def switch_cooldown_until(self, data) -> int:
        """返回属性加点方案切换冷却截止 Unix 秒。"""
        if not data.HasField("attribute_component") or not self.rule or not self.rule.switch_cooldown_seconds:
            return 0
        return min(UINT64_MAX, int(data.attribute_component.last_switch_time) + int(self.rule.switch_cooldown_seconds))

    @staticmethod
    def active_scheme_id(data) -> int:
        return int(data.attribute_component.active_scheme_id) if data.HasField("attribute_component") else 0

    def prepare_scheme_allocation(
        self, player, data, scheme_id: int, target: dict[int, int], reset_if_needed: bool,
    ) -> tuple[int, PreparedSchemeAllocation]:
        """预检战法·加点一体化方案的四维加点目标，支持降点洗点费校验。"""
        prepared = PreparedSchemeAllocation()
        if error := self._write_error(player, data):
            return error, prepared
        pool = self._primary_player_pool()
        if pool is None:
            return _POOL_NOT_FOUND, prepared
        comp = data.attribute_component
        existing = self._scheme(comp, scheme_id)
        allocated = existing.allocated if existing is not None else {}
        level = effective_level(data)
        current, state = self._pool_state(comp, allocated, pool, level)
        foreign = sum(value for key, value in allocated.items() if key not in self.dimensions)

        for dim_id, want in target.items():
            if dim_id not in current:
                return _LOADOUT_ATTRIBUTE_INVALID, prepared
            if pool.dimension_cap and want > pool.dimension_cap:
                return _LOADOUT_ATTRIBUTE_INVALID, prepared

        new_known_used = 0
        decreased = False
        changed = False
        for dim_id, cur_val in current.items():
            want = int(target.get(dim_id, 0))
            new_known_used += want
            if want < cur_val:
                decreased = True
            if want != cur_val:
                changed = True
            prepared.target[dim_id] = want

        if not is_unlocked(pool, level) and new_known_used > 0:
            return _LOADOUT_ATTRIBUTE_INVALID, prepared
        if new_known_used > state.total or foreign > state.total - new_known_used:
            return _LOADOUT_ATTRIBUTE_INVALID, prepared

        needs_reset = decreased or (not state.effective and changed)
        if needs_reset:
            if not reset_if_needed:
                return _LOADOUT_ATTRIBUTE_RESET_REQUIRED, prepared
            reset_cost = self._reset_cost(pool, level, state) if state.known_used > 0 else 0
            if reset_cost > 0:
                if hasattr(self.assets, "can_afford_gold_with_bind"):
                    can_afford = self.assets.can_afford_gold_with_bind(data, reset_cost)
                else:
                    can_afford = (
                        len(data.currency.values) > self.gold and data.currency.values[self.gold] >= reset_cost
                    )
                if not can_afford:
                    return _INSUFFICIENT_GOLD, prepared
            prepared.needs_reset = True
            prepared.reset_cost_gold = int(reset_cost)
        prepared.pool_id = pool.id
        return 0, prepared

    def commit_scheme_allocation_and_switch(
        self,
        data,
        scheme_id: int,
        scheme_name: str,
        apply_allocation: bool,
        prepared: PreparedSchemeAllocation,
    ) -> tuple[int, str]:
        """原地提交一体化方案四维加点与名称，并在无冷却时同步激活对应属性方案。"""
        if not data.HasField("attribute_component"):
            return (UNAVAILABLE if apply_allocation else 0), ""
        if apply_allocation and prepared.needs_reset and prepared.reset_cost_gold > 0:
            if err := self._debit(data, prepared.reset_cost_gold):
                return err, ""
        comp = data.attribute_component
        scheme = self._find_or_create_unified_scheme(comp, scheme_id, scheme_name)
        if scheme_name:
            scheme.name = scheme_name
        if apply_allocation:
            for dim_id, want in prepared.target.items():
                if want == 0:
                    scheme.allocated.pop(dim_id, None)
                else:
                    scheme.allocated[dim_id] = want
            logger.info(
                "Unified loadout allocation saved scheme=%s reset_gold=%s",
                scheme_id, prepared.reset_cost_gold,
            )
        notice = ""
        if comp.active_scheme_id != scheme_id:
            now = int(self.now_s())
            if self.rule and self.rule.switch_cooldown_seconds and (
                comp.last_switch_time + self.rule.switch_cooldown_seconds > now
            ):
                notice = "战法与加点方案已保存，但加点切换处于冷却中，冷却结束后可切换生效"
            else:
                comp.active_scheme_id = scheme_id
                comp.last_switch_time = now
        return 0, notice

    def try_sync_linked_scheme_for_skill(self, data, scheme_id: int, scheme_name: str) -> str:
        """战法方案切换时同步切换同名同号属性方案；处于切换冷却时仅返回提示不报错。"""
        if not data.HasField("attribute_component"):
            return ""
        comp = data.attribute_component
        scheme = self._find_or_create_unified_scheme(comp, scheme_id, scheme_name)
        if scheme_name and not scheme.name:
            scheme.name = scheme_name
        if comp.active_scheme_id == scheme_id:
            return ""
        now = int(self.now_s())
        if self.rule and self.rule.switch_cooldown_seconds and (
            comp.last_switch_time + self.rule.switch_cooldown_seconds > now
        ):
            return "战法已切换；加点方案处于60秒切换冷却中，冷却结束后可重新切换同步"
        comp.active_scheme_id = scheme_id
        comp.last_switch_time = now
        return ""

    def load_level_exp_rows(self) -> list[LevelExpRow]:
        if "PlayerLevelExp" not in self.tables.tables:
            return []
        rows = [
            LevelExpRow(level=int(row.id), required_total_exp=int(row.required_total_exp))
            for row in self.tables["PlayerLevelExp"].rows()
        ]
        rows.sort(key=lambda r: r.level)
        return rows

    def load_battle_progression_rewards(self) -> list[ModeRewardRow]:
        if "BattleProgressionReward" not in self.tables.tables:
            return []
        return [
            ModeRewardRow(
                match_mode=int(row.match_mode),
                difficulty=int(row.difficulty),
                outcome_tier=int(row.outcome_tier),
                exp_gain=int(row.exp_gain),
                silver_gain=int(row.silver_gain),
            )
            for row in self.tables["BattleProgressionReward"].rows()
        ]

    def load_weekly_silver_rule(self) -> WeeklySilverRule | None:
        if "BattleProgressionRule" not in self.tables.tables:
            return None
        row = self.tables["BattleProgressionRule"].get(1)
        if row is None:
            return None
        return WeeklySilverRule(
            soft_cap=int(row.weekly_silver_soft_cap),
            diminished_rate_permyriad=int(row.diminished_rate_permyriad),
            hard_cap=int(row.weekly_silver_hard_cap),
        )

    def validate_progression_tables(self) -> bool:
        exp_rows = self.load_level_exp_rows()
        if not validate_level_exp_rows(exp_rows):
            logger.error("PlayerLevelExp table validation failed: rows=%d", len(exp_rows))
            return False
        rewards = self.load_battle_progression_rewards()
        if not rewards:
            logger.error("BattleProgressionReward table is empty")
            return False
        for row in rewards:
            if row.outcome_tier not in (OUTCOME_TIER_WIN, OUTCOME_TIER_QUALIFIED_NON_WIN):
                logger.error("BattleProgressionReward invalid outcome_tier=%d", row.outcome_tier)
                return False
            if is_guild_war_mode(row.match_mode) and row.silver_gain != 0:
                logger.error("BattleProgressionReward guild war mode must have silver_gain=0: mode=%d", row.match_mode)
                return False
        rule = self.load_weekly_silver_rule()
        if rule is None or not validate_weekly_silver_rule(rule):
            logger.error("BattleProgressionRule table (id=1) missing or invalid")
            return False
        return True

    def apply_battle_exp_and_level_up(self, data, exp_gain: int) -> tuple[bool, LevelProgressResult]:
        """推进战斗经验与升级，同步重算人物与宝宝二级属性（同步版；任务升级事件由调用方触发）。"""
        if not data.HasField("level_component"):
            return False, LevelProgressResult()
        stored_level = int(data.level_component.level)
        exp_rows = self.load_level_exp_rows()
        ok, res = resolve_level_for_exp(stored_level, int(data.level_component.total_exp), exp_gain, exp_rows)
        if not ok:
            return False, res
        if exp_gain > 0:
            data.level_component.total_exp = res.total_exp_after
        if res.leveled_up:
            old = player_stats(self.protocol, self.tables, data)
            data.level_component.level = res.stored_level_after
            self._rescale(old, data, level_changed=True)
            pets = getattr(self.scene, "pets", None)
            if pets is not None:
                pets.recalculate_all(data, level_changed=True)
            logger.info(
                "Player leveled up via battle exp level=%d->%d total_exp=%d",
                res.level_before, res.level_after, res.total_exp_after,
            )
        return True, res

    def try_switch_linked_scheme(self, data, scheme_id: int) -> tuple[bool, str]:
        """供装备流派一键联动切换加点方案（原地修改 data，不破坏主装备切换）。"""
        if not self._present(data):
            return False, "属性数据尚未就绪"
        comp = data.attribute_component
        if self._scheme(comp, scheme_id) is None:
            return False, f"加点方案 {scheme_id} 不存在"
        if comp.active_scheme_id == scheme_id:
            return True, ""
        now = int(self.now_s())
        if self.rule and self.rule.switch_cooldown_seconds:
            if comp.last_switch_time + self.rule.switch_cooldown_seconds > now:
                return False, "加点方案切换冷却中"
        old = player_stats(self.protocol, self.tables, data)
        comp.active_scheme_id = scheme_id
        comp.last_switch_time = now
        self._rescale(old, data)
        return True, ""

    def _fill_support_effects(self, effects, out) -> None:
        out.Clear()
        out.in_battle = False
        out.round_index = 0
        healing = out.healing
        shield = out.shield
        complete = bool(effects.support_sources_complete)
        healing.complete = complete
        shield.complete = complete
        healing.total_bonus_bps = float(effects.outgoing_heal_bps)
        shield.total_bonus_bps = float(effects.shield_capacity_bps)
        for src in effects.support_sources:
            target = shield if src.effect_kind == 2 else healing
            if src.source_cap_bps and (not target.equipment_cap_bps or src.source_cap_bps < target.equipment_cap_bps):
                target.equipment_cap_bps = int(src.source_cap_bps)
            target.sources.add().CopyFrom(src)
        for art in effects.artifacts:
            if art.effect_id in (5, 7):
                s = healing.sources.add()
                s.kind = 4
                s.name = art.name
                s.item_id = art.item_id
                s.raw_bonus_bps = float(art.bonus_bps)
                s.effective_bonus_bps = 0.0
                s.conditional = True
                s.effect_kind = 1
            elif art.effect_id in (6, 8):
                s = shield.sources.add()
                s.kind = 4
                s.name = art.name
                s.item_id = art.item_id
                s.raw_bonus_bps = float(art.bonus_bps)
                s.effective_bonus_bps = 0.0
                s.conditional = True
                s.effect_kind = 2

    def _valid_name(self, name) -> bool:
        limit = (self.rule.scheme_name_max_len or 12) if self.rule else 12
        if not 0 < len(name) <= limit or any(ord(char) < 0x20 or char == "\x7f" for char in name):
            return False
        invisible = {0x20, 0xA0, 0x2028, 0x2029, 0x202F, 0x205F, 0x2060, 0x3000, 0xFEFF}
        return any(ord(char) not in invisible and not 0x2000 <= ord(char) <= 0x200F for char in name)

    def _panel(self, response, data) -> None:
        if not self._present(data):
            response.error_message.id = UNAVAILABLE
            return
        comp = data.attribute_component
        level = effective_level(data)
        panel = response.panel
        panel.level, panel.active_scheme_id = level, comp.active_scheme_id
        exp_display = compute_level_exp_display(level, int(data.level_component.total_exp), self.load_level_exp_rows())
        panel.total_exp = exp_display.total_exp
        panel.current_level_exp = exp_display.current_level_exp
        panel.next_level_exp = exp_display.next_level_exp
        panel.is_max_level = exp_display.is_max_level
        panel.max_schemes, panel.create_scheme_cost_gold = self._max_schemes_for(data), self._scheme_cost(comp)
        if self.rule and self.rule.switch_cooldown_seconds:
            panel.switch_cooldown_until = min(UINT64_MAX, comp.last_switch_time + self.rule.switch_cooldown_seconds)
        for scheme in comp.schemes:
            panel.schemes.add(scheme_id=scheme.scheme_id, name=scheme.name)

        active = self._scheme(comp, comp.active_scheme_id)
        allocated = active.allocated if active is not None else {}
        effective = {}
        for pool in self.pools.values():
            _, state = self._pool_state(comp, allocated, pool, level)
            effective[pool.id] = state.effective
            panel.pools.add(pool_id=pool.id, name=pool.name, total=state.total, remaining=state.remaining,
                            dimension_cap=pool.dimension_cap, unlocked=is_unlocked(pool, level),
                            unlock_level=pool.unlock_level, reset_cost_gold=self._reset_cost(pool, level, state))
        equipped, all_points, _, _ = _equipment(self.protocol, self.tables, data)
        for dim in self.dimensions.values():
            points = allocated.get(dim.id, 0)
            bonus = comp.bonus_values.get(dim.id, 0) + equipped[dim.id] + all_points
            value = min(UINT64_MAX, dim.base_per_level * level + (points if effective[dim.pool_id] else 0) + bonus)
            panel.dimensions.add(dimension_id=dim.id, pool_id=dim.pool_id, name=dim.name, desc=dim.desc,
                                 allocated=points, value=value, cap=self.pools[dim.pool_id].dimension_cap,
                                 sort=dim.sort, bonus=min(UINT32_MAX, bonus))
        stats = player_stats(self.protocol, self.tables, data)
        base = data.derived_attributes_component
        for key in STATS:
            setattr(panel.derived, key, getattr(stats, key))
        panel.derived.health, panel.derived.mana = base.health, base.mana
        equip_craft = getattr(self.scene, "equip_craft", None)
        if equip_craft is not None and hasattr(equip_craft, "collect_effects"):
            equip_craft.collect_effects(data, panel.equipment_effects)
        else:
            panel.equipment_effects.support_sources_complete = True
        equip_fx = panel.equipment_effects
        for stat, row in sorted(self.combat_rows.items(), key=lambda item: (item[1].sort, item[0])):
            if not row.name:
                continue
            field = stats.combat.DESCRIPTOR.fields_by_number[stat]
            intrinsic = base.critchance if stat in (1, 2) else base.resistance if stat in (14, 15) else 0
            fixed_bps = (
                equip_fx.physical_crit_bps if stat == 1 else
                equip_fx.magic_crit_bps if stat == 2 else
                equip_fx.magic_resist_bps if stat == 14 else
                equip_fx.physical_resist_bps if stat == 15 else 0
            )
            value = min(100 if row.percent else UINT64_MAX, intrinsic + getattr(stats.combat, field.name) + fixed_bps // 100)
            panel.combat.add(combat_id=stat, name=row.name, value=value, percent=bool(row.percent), sort=row.sort)
        self._fill_support_effects(equip_fx, panel.support_effects)

    def _rescale(self, old, data, *, level_changed=False):
        new = player_stats(self.protocol, self.tables, data)
        base = data.derived_attributes_component
        base.speed = new.speed
        if level_changed:
            if base.health and 0 < old.max_health < new.max_health:
                base.health = min(new.max_health, base.health + new.max_health - old.max_health)
            if 0 < old.max_mana < new.max_mana:
                base.mana = min(new.max_mana, base.mana + new.max_mana - old.max_mana)
            base.health = min(base.health, new.max_health)
            if new.max_mana:
                base.mana = min(base.mana, new.max_mana)
        else:
            base.health = _rescale(base.health, old.max_health, new.max_health)
            base.mana = _rescale(base.mana, old.max_mana, new.max_mana)

    def _data(self, player):
        return self.scene.rows.assemble(player.snapshot)

    def _response(self, route):
        return self.protocol.message(route.response_type)

    @staticmethod
    def _reject(response, error):
        response.error_message.id = error
        return response

    def _pool(self, data, pool_id):
        pool = self.pools.get(pool_id)
        if pool is None:
            return None, None, _POOL_NOT_FOUND
        comp = data.attribute_component
        scheme = self._scheme(comp, comp.active_scheme_id)
        return pool, scheme, 0 if scheme is not None else _SCHEME_NOT_FOUND

    def _debit(self, data, cost):
        if not cost:
            return 0
        error = (
            self.assets.deduct_gold_prefer_bind(data, cost)
            if hasattr(self.assets, "deduct_gold_prefer_bind")
            else self.assets.deduct_currency(data, self.gold, cost)
        )
        return _INSUFFICIENT_GOLD if error == INSUFFICIENT else error

    async def _save(self, player, response, data, *, audit=None):
        await self.scene.commit_player_data(player, data, on_committed=audit)
        self._panel(response, data)
        return response

    def get_panel(self, _world, player, route, _request):
        response = self._response(route)
        self._panel(response, self._data(player))
        return response

    def suggest(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if not self._present(data):
            return self._reject(response, UNAVAILABLE)
        pool = self.pools.get(request.pool_id)
        if pool is None:
            return self._reject(response, _POOL_NOT_FOUND)
        level = effective_level(data)
        if not is_unlocked(pool, level):
            return self._reject(response, _POOL_LOCKED)
        _, scheme, error = self._pool(data, pool.id)
        if error:
            return self._reject(response, error)
        plan = self.plans.get((class_id(data), pool.id)) or self.plans.get((0, pool.id))
        if plan is None or not plan.dimension:
            return self._reject(response, _NO_PLAN)
        current, state = self._pool_state(data.attribute_component, scheme.allocated, pool, level)
        if not state.remaining:
            return self._reject(response, _NO_CHANGE)
        order, weights = [], []
        for index, key in enumerate(plan.dimension):
            if key in current:
                order.append(key)
                weights.append(plan.weight[index] if index < len(plan.weight) else 1)
        distribute_points(pool, order, weights, state.remaining, current)
        response.pool_id = pool.id
        response.suggested.update(current)
        return response

    async def allocate(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if not request.pool_id or not request.allocated:
            return self._reject(response, INVALID)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        pool, scheme, error = self._pool(data, request.pool_id)
        if error:
            return self._reject(response, error)
        level = effective_level(data)
        current, state = self._pool_state(data.attribute_component, scheme.allocated, pool, level)
        outcome = validate_allocation(pool, level, current, request.allocated, state.remaining)
        if outcome is not AllocationError.OK:
            return self._reject(response, _ALLOCATION_TIPS[outcome])
        old = player_stats(self.protocol, self.tables, data)
        for key, amount in request.allocated.items():
            if amount != current[key]:
                scheme.allocated[key] = amount
        self._rescale(old, data)
        return await self._save(player, response, data)

    async def reset(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        pool, scheme, error = self._pool(data, request.pool_id)
        if error:
            return self._reject(response, error)
        level = effective_level(data)
        current, state = self._pool_state(data.attribute_component, scheme.allocated, pool, level)
        if not state.known_used:
            return self._reject(response, _NO_CHANGE)
        old = player_stats(self.protocol, self.tables, data)
        cost = self._reset_cost(pool, level, state)
        before = clone(data) if cost and self.assets.audit_enabled else None
        if error := self._debit(data, cost):
            return self._reject(response, error)
        for key in current:
            scheme.allocated.pop(key, None)
        self._rescale(old, data)
        audit = self.assets.make_audit_callback(player, before, data, self.spend_tx)
        return await self._save(player, response, data, audit=audit)

    async def create(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        ledger = data.client_request_ledger
        error, fingerprint, prior = client_request.check(ledger, request, _CREATE_SCHEME)
        if error:
            return self._reject(response, error)
        if prior is not None:
            response.scheme_id, response.replayed = prior.created_scheme_id, True
            self._panel(response, data)
            return response
        if error := self._write_error(player, data):
            return self._reject(response, error)
        comp = data.attribute_component
        if len(comp.schemes) >= self._max_schemes_for(data):
            return self._reject(response, _SCHEME_LIMIT)
        name = request.name or _default_name(len(comp.schemes) + 1)
        if not self._valid_name(name):
            return self._reject(response, _INVALID_NAME)
        scheme_id = comp.next_scheme_id
        if not 0 < scheme_id < UINT32_MAX or self._scheme(comp, scheme_id) is not None:
            return self._reject(response, UNAVAILABLE)
        cost = self._scheme_cost(comp)
        before = clone(data) if cost and self.assets.audit_enabled else None
        if error := self._debit(data, cost):
            return self._reject(response, error)
        comp.schemes.add(scheme_id=scheme_id, name=name)
        comp.next_scheme_id += 1
        client_request.remember(ledger, request.client_request_id, _CREATE_SCHEME, fingerprint,
                                created_scheme_id=scheme_id)
        response.scheme_id = scheme_id
        audit = self.assets.make_audit_callback(player, before, data, self.spend_tx)
        return await self._save(player, response, data, audit=audit)

    async def switch(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        comp = data.attribute_component
        if self._scheme(comp, request.scheme_id) is None:
            return self._reject(response, _SCHEME_NOT_FOUND)
        if comp.active_scheme_id == request.scheme_id:
            return self._reject(response, _ALREADY_ACTIVE)
        now = self.now_s()
        if self.rule and self.rule.switch_cooldown_seconds:
            if comp.last_switch_time + self.rule.switch_cooldown_seconds > now:
                return self._reject(response, _SWITCH_COOLDOWN)
        old = player_stats(self.protocol, self.tables, data)
        comp.active_scheme_id, comp.last_switch_time = request.scheme_id, now
        self._rescale(old, data)
        return await self._save(player, response, data)

    async def rename(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        if not self._valid_name(request.name):
            return self._reject(response, _INVALID_NAME)
        scheme = self._scheme(data.attribute_component, request.scheme_id)
        if scheme is None:
            return self._reject(response, _SCHEME_NOT_FOUND)
        scheme.name = request.name
        return await self._save(player, response, data)

    async def set_level(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if not self.allow_gm:
            return self._reject(response, FEATURE_UNAVAILABLE)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        level = request.level
        if not 1 <= level <= 85:
            return self._reject(response, INVALID)
        old = player_stats(self.protocol, self.tables, data)
        before = clone(data) if self.missions and self.assets.audit_enabled else None
        data.level_component.level = level
        self._rescale(old, data, level_changed=True)
        pets = getattr(self.scene, "pets", None)
        if pets is not None:
            pets.recalculate_all(data, level_changed=True)
        gains = []
        if self.missions is not None:
            stamp = self.missions.clock()
            await self.missions._event(data, self.missions.scopes(data)[0], LEVEL, level, level, stamp, gains=gains)
        audit = self.assets.make_audit_callback(player, before, data, self.mission_tx, gains=gains, credit=True)

        async def committed(snapshot):
            if audit is not None:
                result = audit(snapshot)
                if isawaitable(result):
                    await result
            self.notify(player)
            if pets is not None:
                pets.notify(player)

        return await self._save(player, response, data, audit=committed)

    def notify(self, player) -> None:
        response = self.protocol.message("GetAttributePanelResponse")
        self._panel(response, self._data(player))
        if not response.error_message.id:
            self.scene._push(player, "SceneAttributeClientPlayerNotifyAttributePanelChanged",
                             self.protocol.message("AttributePanelChangedS2C", panel=response.panel))
