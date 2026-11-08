"""Original battle skill targeting, casting and automatic choices."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from google.protobuf.message import Message

from .battle_effects import (
    SILENCE, STAT_TYPES, U64_MAX, effective_rank, landed_damage, landed_heal, rank_factor, scaled,
)
from .battle_tables import BattleSkill

if TYPE_CHECKING:
    from .battle import TurnEngine

ENEMY, ALLY, SELF = 1, 2, 3


def active(actor: Message) -> bool:
    return not actor.is_dead and not actor.fled


def health_ratio(actor: Message) -> float:
    return actor.attributes.health / actor.max_health if actor.max_health else 1.0


def mana_ratio(actor: Message) -> float:
    return actor.attributes.mana / actor.max_mana if actor.max_mana else 1.0


def mana_cost(actor: Message, skill: BattleSkill) -> int:
    if skill.class_id:
        return max(1, scaled(actor.max_mana, skill.mana_percent, up=True)) if skill.mana_percent > 0 else 0
    return skill.mana_cost


def restored_mana(target: Message, skill: BattleSkill, rank: int, before: int) -> int:
    return min(max(0, target.max_mana - before), scaled(target.max_mana, skill.restore_mana_percent * rank_factor(rank), up=True))


def base_stat(actor: Message, kind: int) -> int:
    if kind == 10:
        return actor.physical_attack
    if kind == 11:
        return actor.defense
    return actor.attributes.speed if kind == 13 else 0


class BattleSkills:
    def __init__(self, engine: TurnEngine, rules: Mapping[int, BattleSkill], permissions: tuple[int, ...] | None):
        self.engine = engine
        self.rules = rules
        self.permissions = permissions

    def initialize(self, actor: Message, snapshot: Message, *, pet: bool = False) -> None:
        if not pet:
            for skill_id, stored_rank in snapshot.skill_ranks.items():
                rule = self.rules.get(skill_id)
                if rule is None or not rule.class_id or rule.class_id != actor.class_id:
                    continue
                rank = effective_rank(stored_rank, actor.level, rule.unlock_level, rule.max_rank)
                if rank:
                    actor.skill_ranks[skill_id] = rank
        for skill_id in snapshot.skill_table_ids:
            rule = self.rules.get(skill_id)
            if rule and rule.castable and (not rule.class_id or (not pet and actor.skill_ranks.get(skill_id))):
                actor.skill_table_ids.append(skill_id)

    def validate(self, actor: Message, action: Message) -> int:
        skill_id = action.skill_table_id
        rule = self.rules.get(skill_id)
        if rule is None:
            return 1001
        if skill_id not in actor.skill_table_ids or rule.class_id and not actor.skill_ranks.get(skill_id):
            return 1005
        if not rule.castable:
            return 7004
        error = self._validate_target(actor, rule, action.target_id)
        if error:
            return error
        if rule.class_id:
            if actor.skill_cooldown_rounds.get(skill_id, 0):
                return 7003
        else:
            for cooling, remaining in actor.skill_cooldown_rounds.items():
                other = self.rules.get(cooling)
                if remaining and other and not other.class_id and other.cooldown_group == rule.cooldown_group:
                    return 7003
        if self.engine.effects.has(actor, SILENCE):
            if self.permissions is None:
                return 1002
            for bit in rule.skill_types:
                if bit >= len(self.permissions) or self.permissions[bit] == 0:
                    return 1002
                if self.permissions[bit] != 1000:
                    return self.permissions[bit]
        return 7004 if mana_cost(actor, rule) > actor.attributes.mana else 0

    def _validate_target(self, actor: Message, rule: BattleSkill, target_id: int) -> int:
        target = self.engine.actors.get(target_id)
        if rule.class_id:
            if rule.target_team == SELF:
                return 0 if target_id in (0, actor.actor_id) else 7002
            if rule.target_team not in (ENEMY, ALLY):
                return 1002
            if not target_id:
                return 0
            if target is None:
                return 7001
            same_team = target.team_index == actor.team_index
            return 0 if same_team == (rule.target_team == ALLY) else 7002
        if (rule.target_modes or rule.requires_target) and not target_id:
            return 7001
        for mode in rule.target_modes or ((1,) if rule.requires_target else ()):
            if mode in (0, 2):
                return 0
            if mode == 1:
                return 0 if target is not None and active(target) else 7001
        return 0

    def _class_targets(self, actor: Message, rule: BattleSkill, primary: int) -> list[Message]:
        if rule.target_team == SELF:
            return [actor] if active(actor) else []
        want_ally = rule.target_team == ALLY
        targets = [other for other in self.engine.actors.values() if active(other)
                   and (other.team_index == actor.team_index) == want_ally]
        healing = rule.heal_multiplier > 0 or rule.heal_per_level > 0
        targets.sort(key=lambda other: (other.actor_id != primary, health_ratio(other) if healing else 0.0,
                                        other.formation_slot, other.actor_id))
        return targets[:max(1, rule.max_targets)]

    def execute(self, actor: Message, action: Message, turn: Message, group: int) -> None:
        skill_id = action.skill_table_id
        rule = self.rules.get(skill_id)
        if rule is None or self.validate(actor, action):
            if rule is None or not rule.class_id or rule.target_team == ENEMY:
                self.engine._attack_target(turn, group, actor, action.target_id)
            return
        if rule.class_id:
            targets = self._class_targets(actor, rule, action.target_id)
        elif 2 in rule.target_modes:
            targets = [other for other in self.engine.actors.values() if self.engine._enemy(actor, other)]
        else:
            target = self.engine.actors.get(action.target_id)
            if target is None or not active(target):
                target = self.engine._target(actor, 0)
            targets = [target] if target is not None else []
        if not targets:
            return
        rank = actor.skill_ranks.get(skill_id, 0)
        if rule.cooldown_rounds:
            actor.skill_cooldown_rounds[skill_id] = rule.cooldown_rounds + bool(rule.class_id)
        self.engine._event(turn, group, 2, actor, targets[0], skill_table_id=skill_id)
        cost = mana_cost(actor, rule)
        if cost:
            cost = min(cost, actor.attributes.mana)
            actor.attributes.mana -= cost
            self._mana_event(turn, group, 0, actor, actor, skill_id, -cost)
        base = self._base_damage(actor, rule) if not rule.class_id or rule.target_team == ENEMY else 0.0
        for hit, target in enumerate(targets):
            if rule.class_id and not active(target):
                continue
            if base > 0:
                self.engine._damage(turn, group, actor, target, rule, skill_id=skill_id, hit_index=hit,
                                    base=base, rank=rank)
            if target.is_dead:
                continue
            if rule.class_id:
                self._apply_support(turn, group, hit, actor, target, rule, skill_id, rank)
            percent = rule.buff_percent * rank_factor(rank) if rule.class_id else 0.0
            for buff_id in rule.effects:
                self.engine.effects.add(target, buff_id, actor.actor_id, turn, group, hit, percent=percent)

    @staticmethod
    def _base_damage(actor: Message, rule: BattleSkill) -> float:
        return rule.damage(min(U64_MAX, actor.level + actor.combat.skill_level_bonus))

    def _apply_support(self, turn: Message, group: int, hit: int, actor: Message, target: Message,
                       rule: BattleSkill, skill_id: int, rank: int) -> None:
        if rule.heal_multiplier > 0 or rule.heal_per_level > 0:
            value = landed_heal(target, self._heal_amount(actor, target, rule, rank))
            target.attributes.health += value
            self.engine._event(turn, group, 4, actor, target, skill_table_id=skill_id, hit_index=hit, value=value,
                               target_health_after=target.attributes.health, target_mana_after=target.attributes.mana)
        if rule.restore_mana_percent > 0:
            value = restored_mana(target, rule, rank, target.attributes.mana)
            target.attributes.mana += value
            self._mana_event(turn, group, hit, actor, target, skill_id, value)
        if rule.dispel_count:
            self.engine.effects.dispel(target, rule.dispel_count, turn, group, hit)

    def _mana_event(self, turn: Message, group: int, hit: int, source: Message, target: Message,
                    skill_id: int, delta: int) -> None:
        delta = min(max(delta, -(1 << 63)), (1 << 63) - 1)
        self.engine._event(turn, group, 14, source, target, skill_table_id=skill_id, value=abs(delta),
                           hit_index=hit, target_health_after=target.attributes.health,
                           target_mana_after=target.attributes.mana, mana_delta=delta)

    def _heal_amount(self, actor: Message, target: Message, rule: BattleSkill, rank: int) -> float:
        bonus = 0.0
        for skill_id, passive_rank in sorted(actor.skill_ranks.items()):
            passive = self.rules.get(skill_id)
            if passive is not None and passive.passive_stat == "outgoingHeal":
                bonus += passive.passive_percent * rank_factor(passive_rank, 0.05)
        amount = (actor.magic_attack * rule.heal_multiplier + actor.level * rule.heal_per_level) * rank_factor(rank) * (1 + bonus)
        if rule.execute_heal_threshold > 0 and health_ratio(target) < rule.execute_heal_threshold:
            amount *= 1 + rule.execute_heal_bonus
        return amount

    def initiative(self, actor: Message, action: Message | None) -> int:
        if action is None or action.action_type != 2:
            return 0
        skill = self.rules.get(action.skill_table_id)
        if skill is None or not skill.class_id or not skill.initiative or self.engine.validate(actor, action):
            return 0
        return skill.initiative

    def auto_action(self, actor: Message) -> Message | None:
        engine = self.engine
        basic = self._basic_expectation(actor)
        best = None
        for skill_id in actor.skill_table_ids:
            rule = self.rules.get(skill_id)
            if rule is None or not rule.class_id:
                continue
            probe = engine.protocol.message("BattleAction", action_type=2, skill_table_id=skill_id)
            if engine.validate(actor, probe):
                continue
            rank, cost = actor.skill_ranks[skill_id], mana_cost(actor, rule)
            candidates = self._candidates(actor, rule, rank, cost, basic)
            for purpose, gain, target in candidates:
                if gain <= 0:
                    continue
                candidate = (purpose, -min(gain, U64_MAX), cost, rule.cooldown_rounds, skill_id, target)
                if best is None or candidate < best:
                    best = candidate
        return (engine.protocol.message("BattleAction", action_type=2, skill_table_id=best[4], target_id=best[5])
                if best is not None else None)

    def _candidates(self, actor: Message, rule: BattleSkill, rank: int, cost: int, basic: float):
        hostile = rule.target_team == ENEMY
        if not hostile:
            targets = self._class_targets(actor, rule, 0)
            if targets and health_ratio(targets[0]) <= 0.70:
                gain = sum(landed_heal(target, self._heal_amount(actor, target, rule, rank)) for target in targets)
                yield (0 if health_ratio(targets[0]) < 0.30 else 2), gain, 0
        if rule.target_team == SELF:
            primary = [actor]
        else:
            primary = [other for other in self.engine.actors.values() if active(other)
                       and (other.team_index != actor.team_index) == hostile]
        base = self._base_damage(actor, rule) if hostile else 0.0
        for candidate in primary:
            targets = self._class_targets(actor, rule, candidate.actor_id)
            if hostile:
                damage = min(U64_MAX, sum(landed_damage(target, self.engine._expected_damage(actor, target, rule, base) *
                                                        rank_factor(rank)) for target in targets))
                if damage > basic:
                    yield 5, damage, candidate.actor_id
            else:
                dispel = [min(rule.dispel_count, sum(self.engine.effects.is_debuff(buff) for buff in target.buffs))
                          for target in targets]
                if dispel and dispel[0]:
                    yield 1, sum(dispel), candidate.actor_id
                if any(mana_ratio(target) <= 0.30 for target in targets):
                    mana = sum(restored_mana(target, rule, rank, max(0, target.attributes.mana -
                                               (cost if target.actor_id == actor.actor_id else 0))) for target in targets)
                    yield 3, min(U64_MAX, mana) - cost, candidate.actor_id
            yield 4, self._stat_gain(rule, rank, targets), candidate.actor_id

    def _stat_gain(self, rule: BattleSkill, rank: int, targets: list[Message]) -> int:
        percent = rule.buff_percent * rank_factor(rank)
        if not percent or (percent > 0) == (rule.target_team == ENEMY):
            return 0
        effects = self.engine.effects
        gain = 0
        for target in targets:
            for buff_id in rule.effects:
                buff = effects.rules.get(buff_id)
                if buff is None or buff.buff_type not in STAT_TYPES or effects.immune(target, buff):
                    continue
                kind = buff.buff_type
                base = base_stat(target, kind)
                before = scaled(base, effects.multiplier(target, kind))
                after = scaled(base, effects.multiplier(target, kind, percent))
                if kind == 11 and percent > 0 and health_ratio(target) > 0.60:
                    continue
                if kind == 13 and not self._speed_flips(target, before, after):
                    continue
                gain += abs(after - before)
        return gain

    def _speed_flips(self, target: Message, before: int, after: int) -> bool:
        for other in self.engine.actors.values():
            if not self.engine._enemy(target, other):
                continue
            comparison = (self.engine._effective_stat(other, 13), -other.actor_id)
            if ((before, -target.actor_id) > comparison) != ((after, -target.actor_id) > comparison):
                return True
        return False

    def _basic_expectation(self, actor: Message) -> float:
        rule = BattleSkill(10, physical=True)
        targets = [other for other in self.engine.actors.values() if self.engine._enemy(actor, other)]
        factor = 1 + min(actor.combat.combo_rate / 100, 1.0) * 0.5
        total = sum(landed_damage(other, self.engine._expected_damage(actor, other, rule, 10) * factor) for other in targets)
        return min(total, U64_MAX) / len(targets) if targets else 0.0
