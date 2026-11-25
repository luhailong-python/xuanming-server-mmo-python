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
_EVENT_BUFF_REMOVE = 6
_EVENT_MANA = 14
_EVENT_RESIST = 15
_EVENT_SHIELD = 20
_EVENT_ARTIFACT = 21

_HIT_COUNTER = 2
_HIT_REFLECT = 3
_DAMAGE_MAGIC = 0
_DAMAGE_PHYSICAL = 1

_I64_MAX = (1 << 63) - 1
_ENGINE_ACTOR_FLAG = 1 << 63

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


# ---------------------------------------------------------------------------
# 1. 装备与法宝纯规则函数（对照 equipment_battle_rules.h）
# ---------------------------------------------------------------------------


def maximum_source_bps(effect: int) -> int:
    """返回装备单项属性来源的万分比上限。"""
    return _MAXIMUM_SOURCE_BPS.get(effect, 0)


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
        """校验单槽赛季印战斗机制参数是否合法。"""
        if season_id <= 0 or seal_id <= 0:
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
                for source in equipment.support_sources:
                    if source.effect_kind != effect_kind:
                        continue
                    part.sources.add().CopyFrom(source)
                    if source.source_cap_bps > 0:
                        part.equipment_cap_bps = min(part.equipment_cap_bps, source.source_cap_bps)

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


class EquipmentBattleEffects:
    """管理单场战斗内装备词条、法宝触发次数、护盾吸收与吸血效果。"""

    def __init__(self, engine: TurnEngine):
        self.engine = engine
        self._extra_bonuses: dict[int, ActorCombatBonus] = {}
        self._next_shield_buff_id: int = 1 << 40

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
            effects.physical_attack_bps > maximum_source_bps(1)
            or effects.magic_attack_bps > maximum_source_bps(2)
            or effects.max_health_bps > maximum_source_bps(3)
            or effects.speed_bps > maximum_source_bps(4)
            or effects.outgoing_heal_bps > maximum_source_bps(5)
            or effects.shield_capacity_bps > maximum_source_bps(6)
            or effects.physical_crit_bps > maximum_source_bps(7)
            or effects.magic_crit_bps > maximum_source_bps(8)
            or effects.physical_resist_bps > maximum_source_bps(9)
            or effects.magic_resist_bps > maximum_source_bps(10)
            or not self._bounded_map(effects.ailment_hit_bps, 7, maximum_source_bps(11))
            or not self._bounded_map(effects.ailment_resist_bps, 7, maximum_source_bps(12))
            or not self._bounded_map(effects.mana_reduction_bps, 12, maximum_source_bps(13))
            or len(effects.artifacts) > MAX_ARTIFACT_SLOTS
            or effects.season_max_health_bps > SeasonBattleRules.HARD_STAT_CAP_BPS
            or effects.season_defense_bps > SeasonBattleRules.HARD_STAT_CAP_BPS
        ):
            _LOG.warning("rejected equipment effects snapshot due to cap violation actor_id=%s", actor.actor_id)
            return False

        if effects.HasField("season_seal"):
            seal = effects.season_seal
            if not SeasonBattleRules.valid_seal_effect(
                seal.season_id,
                seal.seal_id,
                seal.effect_id,
                seal.bonus_bps,
                seal.cap_bps,
                seal.per_round_limit,
                seal.battle_limit,
                seal.active_rounds,
                seal.health_threshold_bps,
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
    ) -> int:
        """为目标附加独立容量护盾，自动结算施放方护盾容量词条/宠物修炼/法宝 5。"""
        caster = source if source is not None else actor
        capacity_float = base_capacity
        contributions: list[int] = []
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
            capacity_float = PetAndSupportEffects.apply_bonus(base_capacity, bonus)
        capacity = PetAndSupportEffects.whole(capacity_float)
        if capacity <= 0:
            return 0
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
                self.engine.trial_tracker.record_shield_break(source)

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
        for snapshot in request.players:
            players_by_team.setdefault(snapshot.team_index, []).append(snapshot)
            actor = self.engine.actors.get(snapshot.player_id)
            if snapshot.pets:
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
        """记录敌我直接伤害（更新双系直伤标记、有效贡献与最低血线）。"""
        if dealt <= 0 or source.team_index == target.team_index or source.team_index >= 2:
            return
        if damage_kind == _DAMAGE_PHYSICAL:
            self.team_dealt_physical_direct_damage[source.team_index] = True
        elif damage_kind == _DAMAGE_MAGIC:
            self.team_dealt_magic_direct_damage[source.team_index] = True
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
        facts.used_pet_table_id = self._used_pet_table_id.get(player_id, 0)
        facts.used_artifact_effect_ids.extend(self._used_artifact_effect_ids.get(player_id, ()))
        facts.used_season_seal_id = self._used_season_seal_id.get(player_id, 0)
        facts.teammate_names.extend(self._teammates_by_player.get(player_id, ()))
        facts.boss_wind_hit_unprotected_death = self.boss_wind_hit_unprotected_death
        facts.dealt_yang_bell_physical_damage = self.dealt_yang_bell_physical_damage
        facts.dealt_yin_drum_magic_damage = self.dealt_yin_drum_magic_damage
        facts.finished_in_ebb_tide = self.finished_in_ebb_tide

        settlement.manual_action_rounds = len(self.manual_action_rounds.get(player_id, ()))
        settlement.effective_combat_contribution = player_id in self.effective_contribution_players
        return facts
