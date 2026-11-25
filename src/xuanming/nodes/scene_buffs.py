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
USE_SKILL, ACTOR_COMBAT, ACTOR_TEAM_FOLLOW, ACTOR_MOUNTED = 0, 0, 1, 2
MAX_MUTEX_STATES = 64
STATE_BLOCK, STATE_ALLOW, STATE_INTERRUPT = 0, 1, 2
EXIT_REMOVED, EXIT_INTERRUPTED = 0, 1


def _compile_action_matrix(table) -> dict[int, dict[int, tuple[int, int]]]:
    """Compile normalized (id, state_id, state_mode, state_tip) or legacy slot rows into O(1) lookup."""
    matrix: dict[int, dict[int, tuple[int, int]]] = {}
    rows = table.rows() if hasattr(table, "rows") else ()
    for row in rows:
        rules = matrix.setdefault(int(row.id), {})
        if hasattr(row, "state_id"):
            rules[int(row.state_id)] = (int(row.state_mode), int(row.state_tip))
        elif hasattr(row, "state"):
            for state_id, entry in enumerate(row.state):
                rules[state_id] = (int(entry.state_mode), int(entry.state_tip))
    return matrix


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
        self.combat_actions = _compile_action_matrix(tables["ActorActionCombatState"])
        self.actor_actions = _compile_action_matrix(tables["ActorActionState"])
        self.valid = valid or (lambda actor: bool(actor.entity))
        self.apply_allowed = apply_allowed or self.valid
        self.periodic_allowed = periodic_allowed or self.valid
        self.visibility_changed = None
        self.state_changed = None
        self.self_state_changed = None
        self._clock = clock or time.monotonic
        self._ids = itertools.count(1)
        self._actors: dict[int, _ActorBuffs] = {}
        self._action_states: dict[int, tuple[object, set[int]]] = {}
        self._action_masks: dict[int, tuple[object, int]] = {}
        self._exit_hooks: dict[int, list] = {}
        self._self_state_sync: dict[int, tuple[object, int, int, int]] = {}
        self._suppress_depth = 0
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
        if kind == SILENCE:
            self._sync_self_state(actor)

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
        masks = self._action_masks.get(actor.entity)
        if masks is not None and masks[0] is actor:
            self._action_masks.pop(actor.entity, None)
        synced = self._self_state_sync.get(actor.entity)
        if synced is not None and synced[0] is actor:
            self._self_state_sync.pop(actor.entity, None)

    def clear(self) -> None:
        self._actors.clear()
        self._action_states.clear()
        self._action_masks.clear()
        self._self_state_sync.clear()

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

    def register_exit_hook(self, state: int, hook) -> None:
        if 0 <= state < MAX_MUTEX_STATES and hook is not None:
            hooks = self._exit_hooks.setdefault(state, [])
            if hook not in hooks:
                hooks.append(hook)

    def unregister_exit_hook(self, state: int, hook) -> None:
        hooks = self._exit_hooks.get(state)
        if hooks and hook in hooks:
            hooks.remove(hook)

    def action_state_mask(self, actor) -> int:
        current = self._action_masks.get(actor.entity)
        return current[1] if current is not None and current[0] is actor else 0

    def combat_state_mask(self, actor) -> int:
        mask = 0
        for flag, active in self.state_flags(actor).items():
            if active and 0 <= flag < MAX_MUTEX_STATES:
                mask |= 1 << flag
        return mask

    def _sync_self_state(self, actor) -> None:
        if self._suppress_depth > 0 or not self.valid(actor):
            return
        action_mask = self.action_state_mask(actor)
        combat_mask = self.combat_state_mask(actor)
        existing = self._self_state_sync.get(actor.entity)
        if existing is not None and existing[0] is actor:
            _, prev_action, prev_combat, next_seq = existing
        else:
            prev_action, prev_combat, next_seq = 0, 0, 1
        if action_mask == prev_action and combat_mask == prev_combat:
            return
        self._self_state_sync[actor.entity] = (actor, action_mask, combat_mask, next_seq + 1)
        if self.self_state_changed is not None:
            self.self_state_changed(actor, action_mask, combat_mask, next_seq)

    def sync_self_state_after_entry(self, actor) -> None:
        if not self.valid(actor):
            return
        existing = self._self_state_sync.get(actor.entity)
        next_seq = existing[3] if existing is not None and existing[0] is actor else 1
        self._self_state_sync[actor.entity] = (actor, 0, 0, next_seq)
        self._sync_self_state(actor)

    def validate_skill(self, actor, skill_types, action: int = USE_SKILL) -> int:
        action_rules = self.combat_actions.get(action, {})
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
            rule = action_rules.get(state)
            if rule is not None and rule[0] == STATE_BLOCK:
                tip = rule[1]
                if tip != SUCCESS:
                    return tip or 1002
        return self.validate_action(actor, action)

    def _actions(self, actor) -> set[int]:
        current = self._action_states.get(actor.entity)
        return current[1] if current is not None and current[0] is actor else set()

    def set_action_state(self, actor, state: int, active: bool = True, *, _reason: int = EXIT_REMOVED) -> None:
        if not self.valid(actor) or not 0 <= state < MAX_MUTEX_STATES:
            return
        states = self._actions(actor)
        mask = self.action_state_mask(actor)
        bit = 1 << state
        if active:
            if mask & bit:
                return
            states.add(state)
            mask |= bit
            self._action_states[actor.entity] = (actor, states)
            self._action_masks[actor.entity] = (actor, mask)
            self._sync_self_state(actor)
        else:
            if not (mask & bit) and state not in states:
                return
            states.discard(state)
            mask &= ~bit
            if states:
                self._action_states[actor.entity] = (actor, states)
                self._action_masks[actor.entity] = (actor, mask)
            else:
                self._action_states.pop(actor.entity, None)
                self._action_masks.pop(actor.entity, None)
            self._sync_self_state(actor)
            for hook in tuple(self._exit_hooks.get(state, ())):
                hook(actor, state, _reason)

    def validate_action(self, actor, action: int) -> int:
        if not self.valid(actor):
            return INVALID_ENTITY
        rules = self.actor_actions.get(action)
        if rules is None:
            return 1001
        for state in sorted(self._actions(actor)):
            rule = rules.get(state)
            if rule is not None and rule[0] == STATE_BLOCK and rule[1] != SUCCESS:
                return rule[1] or 1002
        return 0

    def perform_action(self, actor, action: int, success_state: int) -> int:
        if not 0 <= success_state < MAX_MUTEX_STATES:
            return 1005
        error = self.validate_action(actor, action)
        if error:
            return error
        rules = self.actor_actions.get(action, {})
        self._suppress_depth += 1
        try:
            for state in tuple(self._actions(actor)):
                rule = rules.get(state)
                if rule is not None and rule[0] == STATE_INTERRUPT:
                    self.set_action_state(actor, state, False, _reason=EXIT_INTERRUPTED)
            self.set_action_state(actor, success_state, True)
        finally:
            self._suppress_depth -= 1
        self._sync_self_state(actor)
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
