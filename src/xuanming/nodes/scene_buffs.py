"""Realtime Scene buff instances, source-owned states and frame-driven effects."""

from __future__ import annotations

import itertools
import math
import sys
import time
from collections import Counter
from dataclasses import dataclass, field

from .battle_tables import _expression, rounds_from_seconds


NO_BUFF = (1 << 64) - 1
SUCCESS = 1000
INVALID_ENTITY, IMMUNE = 1009, 8001
SILENCE, STEALTH, DISPEL, NEXT_ATTACK, LOST_HEALTH_REGEN, COMBAT_IDLE = 31, 33, 35, 36, 42, 43
SILENCE_STATE = 1
USE_SKILL, ACTOR_COMBAT = 0, 0


@dataclass(slots=True)
class SceneBuff:
    id: int
    row: object
    caster: int
    processed_caster: int
    expires: float
    layer: int = 1
    periodic: float = 0.0
    ticks: int = 0
    idle_since_ms: int = 0
    added_sub_buff: bool = False
    idle_owner: int = NO_BUFF


@dataclass(slots=True)
class _ActorBuffs:
    actor: object
    buffs: dict[int, SceneBuff] = field(default_factory=dict)


class SceneBuffs:
    """Port the implemented C++ callbacks, leaving C++ TODO hooks inert."""

    def __init__(self, protocol, tables, *, valid=None, apply_allowed=None, periodic_allowed=None, clock=None):
        self.protocol = protocol
        self.rows = {row.id: row for row in tables["Buff"].rows()}
        self.permissions = tables["SkillPermission"]
        self.combat_actions = tables["ActorActionCombatState"]
        self.actor_actions = tables["ActorActionState"]
        self.valid = valid or (lambda actor: bool(actor.entity))
        self.apply_allowed = apply_allowed or self.valid
        self.periodic_allowed = periodic_allowed or self.valid
        self.visibility_changed = None
        self.state_changed = None
        self._clock = clock or time.monotonic
        self._ids = itertools.count(1)
        self._actors: dict[int, _ActorBuffs] = {}
        self._action_states: dict[int, tuple[object, set[int]]] = {}
        self._bonus_damage = {key: _expression(row.bonus_damage) for key, row in self.rows.items() if row.buff_type == NEXT_ATTACK}
        self._health_regen = {key: _expression(row.health_regeneration) for key, row in self.rows.items()
                              if row.buff_type == LOST_HEALTH_REGEN}

    def entries(self, actor) -> tuple[SceneBuff, ...]:
        bucket = self._actors.get(actor.entity)
        if bucket is None or bucket.actor is not actor:
            return ()
        now = self._clock()
        return tuple(entry for entry in bucket.buffs.values() if entry.expires > now)

    def _entry(self, actor, buff_id):
        bucket = self._actors.get(actor.entity)
        return bucket.buffs.get(buff_id) if bucket is not None and bucket.actor is actor else None

    def _stored(self, actor) -> tuple[SceneBuff, ...]:
        bucket = self._actors.get(actor.entity)
        return tuple(bucket.buffs.values()) if bucket is not None and bucket.actor is actor else ()

    def stealthed(self, actor) -> bool:
        return any(entry.row.buff_type == STEALTH for entry in self._stored(actor))

    def state_flags(self, actor) -> dict[int, bool]:
        return {SILENCE_STATE: True} if any(entry.row.buff_type == SILENCE for entry in self._stored(actor)) else {}

    def _notify_transition(self, actor, kind: int, was_present: bool) -> None:
        if kind not in (STEALTH, SILENCE):
            return
        present = any(entry.row.buff_type == kind for entry in self._stored(actor))
        if present == was_present:
            return
        callback = self.visibility_changed if kind == STEALTH else self.state_changed
        if callback is not None:
            callback(actor)

    def add(self, actor, table_id: int, caster: int = 0, *, idle_owner: int = NO_BUFF) -> tuple[int, int]:
        if not self.valid(actor):
            return INVALID_ENTITY, NO_BUFF
        if not self.apply_allowed(actor):
            return SUCCESS, NO_BUFF
        row = self.rows.get(table_id)
        if row is None:
            return 1001, NO_BUFF
        self._expire(actor, self._clock())
        entries = self._stored(actor)
        tags, dispels = set(row.tag), set(row.dispel_tag)
        if any(tags.intersection(entry.row.immune_tag) for entry in entries):
            return IMMUNE, NO_BUFF
        if dispels:
            for entry in entries:
                if dispels.intersection(entry.row.tag):
                    self.remove(actor, entry.id)
        if row.buff_type == DISPEL:
            return SUCCESS, NO_BUFF
        if idle_owner != NO_BUFF and self._entry(actor, idle_owner) is None:
            return SUCCESS, NO_BUFF
        processed = 0 if row.no_caster else caster
        if idle_owner == NO_BUFF and (row.no_caster or caster):
            for entry in self._stored(actor):
                if entry.row.id == table_id and entry.processed_caster == processed and entry.idle_owner == NO_BUFF:
                    entry.layer += int(entry.layer < row.max_layer)
                    # The C++ refresh callback is a TODO. Stacking does not extend duration.
                    return SUCCESS, NO_BUFF
        now = self._clock()
        buff_id = next(self._ids)
        deadline = math.inf if row.infinite_duration or row.duration < -sys.float_info.epsilon else now + max(0.0, row.duration)
        state = self._actors.get(actor.entity)
        if state is None or state.actor is not actor:
            state = self._actors[actor.entity] = _ActorBuffs(actor)
        already = any(entry.row.buff_type == row.buff_type for entry in state.buffs.values())
        state.buffs[buff_id] = SceneBuff(buff_id, row, caster, processed, deadline, idle_since_ms=int(now * 1000),
                                        idle_owner=idle_owner)
        self._notify_transition(actor, row.buff_type, already)
        if not row.infinite_duration and -sys.float_info.epsilon < row.duration <= 0:
            self.remove(actor, buff_id)
        return SUCCESS, buff_id

    def remove(self, actor, buff_id: int) -> None:
        state = self._actors.get(actor.entity)
        if state is None or state.actor is not actor:
            return
        entry = state.buffs.pop(buff_id, None)
        if entry is None:
            return
        if entry.row.buff_type == COMBAT_IDLE:
            for child in self._stored(actor):
                if child.idle_owner == buff_id:
                    self.remove(actor, child.id)
        self._notify_transition(actor, entry.row.buff_type, True)
        if not state.buffs:
            self._actors.pop(actor.entity, None)

    def _expire(self, actor, now: float) -> None:
        for entry in self._stored(actor):
            if entry.expires <= now:
                self.remove(actor, entry.id)

    def discard(self, actor) -> None:
        state = self._actors.get(actor.entity)
        if state is not None and state.actor is actor:
            self._actors.pop(actor.entity, None)
        states = self._action_states.get(actor.entity)
        if states is not None and states[0] is actor:
            self._action_states.pop(actor.entity, None)

    def clear(self) -> None:
        self._actors.clear()
        self._action_states.clear()

    def _add_children(self, actor, entry: SceneBuff) -> None:
        if entry.added_sub_buff:
            return
        entry.added_sub_buff = True
        owner = entry.id if entry.row.buff_type == COMBAT_IDLE else NO_BUFF
        for sub in entry.row.sub_buff:
            if self._entry(actor, entry.id) is not entry:
                break
            self.add(actor, sub, entry.caster, idle_owner=owner)

    def reset_combat_idle(self, victim) -> None:
        now = int(self._clock() * 1000)
        for owner in self.entries(victim):
            if owner.row.buff_type != COMBAT_IDLE:
                continue
            owner.idle_since_ms = now
            owner.added_sub_buff = False
            for child in self._stored(victim):
                if child.idle_owner == owner.id:
                    self.remove(victim, child.id)

    def before_damage(self, caster, target, damage: float) -> float:
        bonuses = [entry.id for entry in self.entries(caster) if entry.row.buff_type == NEXT_ATTACK]
        consumed = []
        for buff_id in bonuses:
            entry = self._entry(caster, buff_id)
            if entry is None:
                continue
            damage += self._bonus_damage[entry.row.id](0.0, 0.0)
            consumed.append(buff_id)
            self._add_children(caster, entry)
            for sub in entry.row.target_sub_buff:
                self.add(target, sub, entry.caster)
        for buff_id in consumed:
            self.remove(caster, buff_id)
        return damage

    def validate_skill(self, actor, skill_types, action: int = USE_SKILL) -> int:
        for state in self.state_flags(actor):
            row = self.permissions.get(state)
            if row is None:
                return 1001
            for bit in skill_types:
                if bit >= len(row.skill_type):
                    return 1002
                tip = row.skill_type[bit]
                if tip != SUCCESS:
                    return tip or 1002
            action_row = self.combat_actions.get(action)
            if action_row is None:
                return 1001
            if state < len(action_row.state) and action_row.state[state].state_mode == 1:
                tip = action_row.state[state].state_tip
                if tip != SUCCESS:
                    return tip or 1002
        return self.validate_action(actor, action)

    def _actions(self, actor) -> set[int]:
        current = self._action_states.get(actor.entity)
        return current[1] if current is not None and current[0] is actor else set()

    def set_action_state(self, actor, state: int, active: bool = True) -> None:
        if not self.valid(actor) or not 0 <= state < 3:
            return
        states = self._actions(actor)
        if active:
            states.add(state)
            self._action_states[actor.entity] = (actor, states)
        else:
            states.discard(state)
            if not states:
                self._action_states.pop(actor.entity, None)

    def validate_action(self, actor, action: int) -> int:
        if not self.valid(actor):
            return INVALID_ENTITY
        row = self.actor_actions.get(action)
        if row is None:
            return 1001
        for state in sorted(self._actions(actor)):
            if state < len(row.state) and row.state[state].state_mode == 0 and row.state[state].state_tip != SUCCESS:
                return row.state[state].state_tip or 1002
        return 0

    def perform_action(self, actor, action: int, success_state: int) -> int:
        if not 0 <= success_state < 3:
            return 1005
        error = self.validate_action(actor, action)
        if error:
            return error
        row = self.actor_actions.get(action)
        for state in tuple(self._actions(actor)):
            if state < len(row.state) and row.state[state].state_mode == 2:
                self.set_action_state(actor, state, False)
        self.set_action_state(actor, success_state)
        return 0

    def tick(self, delta: float, *, healing_busy=None):
        """Return due healing entries. Other implemented tick hooks apply synchronously."""
        if not math.isfinite(delta) or delta < 0:
            raise ValueError("invalid Scene buff delta")
        now = self._clock()
        now_ms = int(now * 1000)
        healing_due = []
        for state in tuple(self._actors.values()):
            actor = state.actor
            if not self.valid(actor):
                self.discard(actor)
                continue
            self._expire(actor, now)
            if not self.periodic_allowed(actor):
                continue
            due = Counter()
            for entry in self._stored(actor):
                if self._entry(actor, entry.id) is not entry or entry.row.interval <= 0:
                    continue
                entry.periodic += delta
                if entry.row.buff_type == LOST_HEALTH_REGEN and healing_busy and healing_busy(actor):
                    continue
                for _ in range(5):
                    if (self._entry(actor, entry.id) is not entry or entry.periodic < entry.row.interval
                            or entry.row.interval_count and entry.ticks >= entry.row.interval_count):
                        break
                    entry.periodic -= entry.row.interval
                    entry.ticks += 1
                    if entry.row.buff_type == LOST_HEALTH_REGEN:
                        due[entry.id] += 1
                    elif entry.row.buff_type == COMBAT_IDLE and entry.row.combat_idle_seconds > 0:
                        if (now_ms - entry.idle_since_ms) / 1000 >= entry.row.combat_idle_seconds:
                            self._add_children(actor, entry)
            if due:
                healing_due.append((actor, due))
        for actor, _ in tuple(self._action_states.values()):
            if not self.valid(actor):
                self.discard(actor)
        return healing_due

    def heal(self, actor, due, current: int, max_health: int, level: int) -> int:
        for buff_id, ticks in due.items():
            entry = self._entry(actor, buff_id)
            if entry is None or entry.expires <= self._clock():
                continue
            formula = self._health_regen[entry.row.id]
            for _ in range(ticks):
                amount = formula(float(level), float(max(0, max_health - current)))
                current = min(max_health, int(float(current) + amount)) if amount >= 0 else current
        return current

    def snapshot(self, player, snapshot) -> None:
        for entry in self.entries(player):
            row = entry.row
            if row.buff_type in (30, SILENCE, 52) or not row.infinite_duration and row.duration <= 0:
                continue
            snapshot.buffs.add(
                buff_id=entry.id, buff_table_id=row.id, layer=max(1, entry.layer),
                remain_rounds=0 if row.infinite_duration else rounds_from_seconds(row.duration),
                caster_id=player.player_id if entry.caster == player.entity else 0,
            )
