"""Battle buff lifecycle and the shared numeric rules for class skills."""

from __future__ import annotations

from collections.abc import Mapping
from math import ceil, floor, isfinite
from typing import TYPE_CHECKING

from google.protobuf.message import Message

from .battle_tables import BattleBuff

if TYPE_CHECKING:
    from .battle import TurnEngine


U64_MAX = (1 << 64) - 1
STUN, SILENCE, FREEZE = 30, 31, 52
CONTROL_TYPES = {STUN, SILENCE, FREEZE}
DEBUFF_TYPES = CONTROL_TYPES | {50, 51}
STAT_TYPES = {10, 11, 13}
_AILMENT_RESIST = {30: "resist_sleep", 31: "resist_forget", 50: "resist_poison", 52: "resist_freeze"}
_RATIO_SCALE = 1_000_000


def effective_rank(stored: int, level: int, unlock_level: int, max_rank: int) -> int:
    return min(stored, max_rank, 1 + (level - unlock_level) // 2) if level >= unlock_level else 0


def rank_factor(rank: int, growth: float = 0.02) -> float:
    return 1.0 + growth * max(0, rank - 1)


def scaled(value: int, ratio: float, *, up: bool = False) -> int:
    if ratio <= 0 or not isfinite(ratio):
        return 0
    maximum_ppm = U64_MAX // _RATIO_SCALE
    ppm = floor(min(maximum_ppm, ratio * _RATIO_SCALE) + 0.5)
    numerator = value * ppm + (_RATIO_SCALE - 1 if up else 0)
    return min(U64_MAX, numerator // _RATIO_SCALE)


def landed_damage(target: Message, amount: float) -> int:
    if amount <= 0 or not isfinite(amount):
        return 0
    return min(target.attributes.health, ceil(amount * (0.5 if target.is_defending else 1)))


def landed_heal(target: Message, amount: float) -> int:
    if amount <= 0 or not isfinite(amount) or target.is_dead:
        return 0
    missing = max(0, target.max_health - target.attributes.health)
    return min(missing, int(amount))


class BattleEffects:
    def __init__(self, engine: TurnEngine, rules: Mapping[int, BattleBuff]):
        self.engine = engine
        self.rules = rules
        self._next_id = 1

    def sanitize(self) -> None:
        for actor in self.engine.actors.values():
            for entry in actor.buffs:
                self._next_id = max(self._next_id, entry.buff_id + 1)
            kept = []
            for entry in actor.buffs:
                rule = self.rules.get(entry.buff_table_id)
                if rule is None or rule.buff_type in CONTROL_TYPES or not (rule.infinite or rule.duration_rounds):
                    continue
                copy = type(entry)()
                copy.CopyFrom(entry)
                copy.ClearField("stat_percent")
                copy.ClearField("applied_round")
                if copy.caster_id not in self.engine.actors:
                    copy.caster_id = 0
                if not rule.infinite and (not copy.remain_rounds or copy.remain_rounds > rule.duration_rounds):
                    copy.remain_rounds = rule.duration_rounds
                kept.append(copy)
            actor.ClearField("buffs")
            actor.buffs.extend(kept)

    def has(self, actor: Message, *kinds: int) -> bool:
        return any(self.rules[entry.buff_table_id].buff_type in kinds
                   for entry in actor.buffs if entry.buff_table_id in self.rules)

    def multiplier(self, actor: Message, kind: int, extra: float = 0.0) -> float:
        rates = [extra, 0.0]
        rates.extend(entry.stat_percent for entry in actor.buffs if entry.buff_table_id in self.rules
                     and self.rules[entry.buff_table_id].buff_type == kind)
        return max(0.1, 1.0 + max(rates) + min(rates))

    def is_debuff(self, entry: Message) -> bool:
        rule = self.rules.get(entry.buff_table_id)
        return entry.stat_percent < 0 or (rule is not None and (rule.buff_type in DEBUFF_TYPES or "Debuff" in rule.tags))

    def immune(self, actor: Message, incoming: BattleBuff) -> bool:
        return any(incoming.tags & self.rules[entry.buff_table_id].immune_tags
                   for entry in actor.buffs if entry.buff_table_id in self.rules)

    def _resisted(self, actor: Message, rule: BattleBuff, caster_id: int) -> bool:
        caster = self.engine.actors.get(caster_id)
        specific = _AILMENT_RESIST.get(rule.buff_type)
        if specific is None or caster is None or actor.team_index == caster.team_index:
            return False
        total = min(U64_MAX, getattr(actor.combat, specific) + actor.combat.resist_all_ailment)
        return self.engine.rng.percent(max(0, total - caster.combat.ignore_ailment_resist))

    def _remove(self, target: Message, index: int, turn: Message, group: int, hit: int) -> None:
        entry = target.buffs[index]
        turn.events.add(event_type=6, source_id=entry.caster_id, target_id=target.actor_id,
                        buff_table_id=entry.buff_table_id, group_id=group, hit_index=hit)
        del target.buffs[index]

    def dispel(self, target: Message, count: int, turn: Message, group: int, hit: int) -> None:
        index = 0
        while index < len(target.buffs) and count:
            if self.is_debuff(target.buffs[index]):
                self._remove(target, index, turn, group, hit)
                count -= 1
            else:
                index += 1

    def _index(self, target: Message, buff_id: int) -> int | None:
        return next((index for index, entry in enumerate(target.buffs) if entry.buff_id == buff_id), None)

    def add(self, target: Message, table_id: int, caster_id: int, turn: Message, group: int, hit: int,
            *, percent: float = 0.0, depth: int = 0) -> None:
        rule = self.rules.get(table_id)
        if rule is None or depth > 8 or target.is_dead or self.immune(target, rule):
            return
        if self._resisted(target, rule, caster_id):
            turn.events.add(event_type=15, source_id=caster_id, target_id=target.actor_id,
                            buff_table_id=table_id, group_id=group, hit_index=hit)
            return
        if rule.dispel_tags:
            for index in range(len(target.buffs) - 1, -1, -1):
                existing_rule = self.rules.get(target.buffs[index].buff_table_id)
                if existing_rule is not None and rule.dispel_tags & existing_rule.tags:
                    self._remove(target, index, turn, group, hit)
        caster = self.engine.actors.get(caster_id)
        encounter = getattr(self.engine, "encounter", None)
        if caster is not None and encounter is not None:
            if (rule.buff_type == 35 or rule.battle_op == 5) and caster.team_index != target.team_index:
                encounter.try_dispel_boss_stance(caster, target, turn, group)
            elif rule.battle_op == 4 and caster.team_index == target.team_index:
                encounter.clear_sunder_on_ally_dispel(caster, target)
        if rule.buff_type == 35:
            return
        for entry in target.buffs:
            if entry.buff_table_id != table_id or not (rule.no_caster or entry.caster_id == caster_id):
                continue
            if entry.layer < rule.max_layer:
                entry.layer += 1
            if entry.remain_rounds and rule.duration_rounds:
                entry.remain_rounds = rule.duration_rounds
            if percent:
                if abs(percent) > abs(entry.stat_percent):
                    entry.stat_percent = percent
                entry.applied_round = self.engine.round_index
            turn.events.add(event_type=5, source_id=caster_id, target_id=target.actor_id, value=entry.layer,
                            buff_table_id=table_id, group_id=group, hit_index=hit)
            return
        buff_id = self._next_id & U64_MAX
        self._next_id += 1
        entry = target.buffs.add(buff_id=buff_id, buff_table_id=table_id, caster_id=caster_id, layer=1,
                                 remain_rounds=0 if rule.infinite else max(1, rule.duration_rounds))
        if percent:
            entry.stat_percent = percent
            entry.applied_round = self.engine.round_index
        turn.events.add(event_type=5, source_id=caster_id, target_id=target.actor_id, value=1,
                        buff_table_id=table_id, group_id=group, hit_index=hit)
        for child in rule.sub_buffs:
            self.add(target, child, caster_id, turn, group, hit, depth=depth + 1)
        caster = self.engine.actors.get(caster_id)
        if caster is not None and not caster.is_dead:
            for child in rule.target_sub_buffs:
                self.add(caster, child, target.actor_id, turn, group, hit, depth=depth + 1)
        if not rule.infinite and not rule.duration_rounds:
            index = self._index(target, buff_id)
            if index is not None:
                self._remove(target, index, turn, group, hit)

    def tick(self, actor: Message, turn: Message, group: int) -> None:
        for buff_id in [entry.buff_id for entry in actor.buffs]:
            index = self._index(actor, buff_id)
            if index is None:
                continue
            entry = actor.buffs[index]
            rule = self.rules.get(entry.buff_table_id)
            if rule is None:
                continue
            if rule.interval_rounds:
                elapsed = (max(1, rule.duration_rounds - entry.remain_rounds + 1) if entry.remain_rounds
                           else self.engine.round_index)
                ticks, remainder = divmod(elapsed, rule.interval_rounds)
                if not remainder and (not rule.interval_count or ticks <= rule.interval_count):
                    self._tick_effect(actor, entry, rule, turn, group)
            if actor.is_dead:
                return
            if entry.remain_rounds and entry.applied_round != self.engine.round_index:
                if entry.remain_rounds == 1:
                    self._remove(actor, index, turn, group, 0)
                else:
                    entry.remain_rounds -= 1

    def _tick_effect(self, target: Message, entry: Message, rule: BattleBuff, turn: Message, group: int) -> None:
        caster_id = entry.caster_id
        if rule.buff_type in (40, 42) and rule.health_regeneration is not None:
            missing = (target.max_health - target.attributes.health) & U64_MAX
            value = landed_heal(target, rule.health_regeneration(target.level, missing))
            target.attributes.health += value
        elif rule.buff_type in (50, 51) and rule.interval_damage is not None:
            value = landed_damage(target, rule.interval_damage * max(1, entry.layer))
            target.attributes.health -= value
        else:
            return
        if value:
            turn.events.add(event_type=7, source_id=caster_id, target_id=target.actor_id,
                            buff_table_id=entry.buff_table_id, value=value, group_id=group,
                            target_health_after=target.attributes.health, target_mana_after=target.attributes.mana)
        if rule.buff_type in (50, 51):
            self.engine._death(turn, group, target, caster_id)
