"""Normalize frozen battle scripts and select deterministic automatic actions."""

from functools import cmp_to_key
import logging

from .battle_effects import U64_MAX
from .battle_skills import active

_LOG = logging.getLogger(__name__)
_STATUS_TYPES = {10, 11, 13, 30, 31, 32, 34, 40, 41, 42, 50, 51, 52}


def _mul(a, b):
    return min(U64_MAX, a * b)


def _ratio_below(value, maximum, threshold):
    return bool(maximum and _mul(value, 10000) < _mul(maximum, threshold))


def _ratio_less(a, b):
    left, right = (a.attributes.health, a.max_health or 1), (b.attributes.health, b.max_health or 1)
    if not a.max_health:
        left = (1, 1)
    if not b.max_health:
        right = (1, 1)
    return _mul(left[0], right[1]) < _mul(right[0], left[1])


def normalize(script, skills, buffs, monster_ids, monster_side):
    """Return an ordered copy or None when any native contract check fails."""
    if (script.schema_version != 1 or len(script.name.encode()) > 64 or len(script.description.encode()) > 512
            or not 1 <= len(script.roles) <= 8):
        return None
    ids, orders = set(), set()
    for role in script.roles:
        if (not role.role_id or role.role_id in ids or role.order in orders or len(role.name.encode()) > 64
                or role.actor_type not in (0, 1, 2, 3) or role.class_id > 1000 or role.max_members > 10
                or not 1 <= len(role.rules) <= 16
                or role.class_id and role.actor_type not in (0, 1)
                or role.monster_id and (role.actor_type not in (0, 2) or role.class_id
                                        or monster_ids is None or role.monster_id not in monster_ids)
                or monster_side and (role.actor_type not in (0, 2) or role.class_id)
                or not monster_side and (role.actor_type == 2 or role.monster_id)):
            return None
        ids.add(role.role_id)
        orders.add(role.order)
    for role in script.roles:
        priorities = set()
        for rule in role.rules:
            if (rule.priority in priorities or len(rule.note.encode()) > 256 or len(rule.conditions) > 2
                    or len(rule.target.filters) > 3 or rule.target.side not in (1, 2, 3, 4)
                    or rule.target.order not in (0, 1, 2, 3, 4)
                    or rule.target.side in (1, 4) and (rule.target.filters or rule.target.order)):
                return None
            priorities.add(rule.priority)
            for cond in rule.conditions:
                if (cond.kind == 1 and not (1 <= cond.p1 <= 10000 and cond.p2 == 0)
                        or cond.kind == 2 and not (1 <= cond.p1 <= 1000 and cond.p2 == 0)
                        or cond.kind == 3 and not (1 <= cond.p1 <= 1000 and 1 <= cond.p2 <= 1000)
                        or cond.kind == 4 and not (1 <= cond.p1 <= 20 and cond.p2 == 0)
                        or cond.kind not in (1, 2, 3, 4)):
                    return None
            for f in rule.target.filters:
                if (f.kind == 1 and not 1 <= f.p1 <= 10000
                        or f.kind == 2 and f.p1 not in _STATUS_TYPES
                        or f.kind == 3 and (not f.p1 or f.p1 not in buffs)
                        or f.kind == 4 and (not f.p1 or monster_ids is None or f.p1 not in monster_ids)
                        or f.kind == 5 and not 1 <= f.p1 <= 1000
                        or f.kind == 6 and f.p1 not in (1, 2, 3)
                        or f.kind == 7 and f.p1 != 0 and f.p1 not in ids
                        or f.kind not in (1, 2, 3, 4, 5, 6, 7)):
                    return None
            side = rule.target.side
            if rule.action == 1:
                if rule.skill_id or side not in (2, 4):
                    return None
            elif rule.action == 3:
                if rule.skill_id or side != 1:
                    return None
            elif rule.action == 2:
                skill = skills.get(rule.skill_id)
                if not skill or not skill.castable:
                    return None
                if not skill.class_id:
                    if side != 2 or monster_side and skill.mana_cost:
                        return None
                elif (monster_side or role.actor_type not in (0, 1)
                      or role.class_id and role.class_id != skill.class_id
                      or side != 4 and side != {1: 2, 2: 3, 3: 1}.get(skill.target_team)):
                    return None
            else:
                return None
    result = type(script)()
    result.CopyFrom(script)
    result.roles.sort(key=lambda role: role.order)
    for role in result.roles:
        role.rules.sort(key=lambda rule: rule.priority)
    return result


class BattleScriptRuntime:
    def __init__(self, engine, request, monster_ids=None):
        self.engine = engine
        self.scripts = {}
        self.roles = {}
        seen = set()
        for team_script in request.team_scripts:
            team = team_script.team_index
            if team > 1 or team in seen:
                _LOG.error("Battle script ignored: duplicate or invalid team index %s", team)
                continue
            seen.add(team)
            actors = [actor for actor in engine.actors.values() if actor.team_index == team]
            monster_side = any(actor.actor_type == 2 for actor in actors)
            script = normalize(team_script.script, engine.skills.rules, engine.effects.rules, monster_ids, monster_side)
            if script is None:
                _LOG.error("Battle script ignored: invalid script for team %s", team)
                continue
            self.scripts[team] = script
            counts = {}
            for actor in actors:
                for role in script.roles:
                    if (role.actor_type and actor.actor_type != role.actor_type
                            or role.class_id and (actor.actor_type != 1 or actor.class_id != role.class_id)
                            or role.monster_id and (actor.actor_type != 2 or actor.monster_table_id != role.monster_id)
                            or role.max_members and counts.get(role.role_id, 0) >= role.max_members):
                        continue
                    counts[role.role_id] = counts.get(role.role_id, 0) + 1
                    self.roles[actor.actor_id] = role.role_id
                    if actor.actor_type == 2:
                        ids = sorted({rule.skill_id for rule in role.rules if rule.action == 2 and rule.skill_id})
                        actor.skill_table_ids.extend(skill for skill in ids if skill not in actor.skill_table_ids)
                    break

    def fill(self):
        engine = self.engine
        for team, script in sorted(self.scripts.items()):
            actors = [actor for actor in engine.actors.values() if actor.team_index == team]
            claims = [(self.roles[actor.actor_id], engine.pending[actor.actor_id].target_id)
                      for actor in actors if actor.actor_id in self.roles and actor.actor_id in engine.pending
                      and engine.pending[actor.actor_id].target_id]
            for role in script.roles:
                for actor in actors:
                    if (not active(actor) or actor.actor_id in engine.pending
                            or self.roles.get(actor.actor_id) != role.role_id):
                        continue
                    action = self._select(actor, role, claims)
                    if action is not None:
                        if action.target_id:
                            claims.append((role.role_id, action.target_id))
                        engine.pending[actor.actor_id] = action

    def _condition(self, actor, condition):
        kind, p1, p2 = condition.kind, condition.p1, condition.p2
        if kind == 1:
            value = _ratio_below(actor.attributes.health, actor.max_health, p1)
        elif kind == 2:
            value = self.engine.round_index >= p1
        elif kind == 3:
            value = self.engine.round_index >= p2 and (self.engine.round_index - p2) % p1 == 0
        elif kind == 4:
            value = sum(active(other) and other.team_index != actor.team_index
                        for other in self.engine.actors.values()) >= p1
        else:
            return False
        return value != condition.negate

    def _filter(self, actor, f, own_role, claims):
        if f.kind == 1:
            value = _ratio_below(actor.attributes.health, actor.max_health, f.p1)
        elif f.kind == 2:
            value = self.engine.effects.has(actor, f.p1)
        elif f.kind == 3:
            value = any(buff.buff_table_id == f.p1 for buff in actor.buffs)
        elif f.kind == 4:
            value = actor.actor_type == 2 and actor.monster_table_id == f.p1
        elif f.kind == 5:
            value = actor.actor_type == 1 and actor.class_id == f.p1
        elif f.kind == 6:
            value = actor.actor_type == f.p1
        elif f.kind == 7:
            value = (f.p1 or own_role, actor.actor_id) in claims
        else:
            return False
        return value != f.negate

    def _targets(self, actor, target, role_id, claims):
        allies = target.side == 3
        candidates = [other for other in self.engine.actors.values() if active(other)
                      and (other.team_index == actor.team_index) == allies
                      and all(self._filter(other, f, role_id, claims) for f in target.filters)]
        def compare(a, b):
            if target.order == 3:
                if _ratio_less(a, b):
                    return -1
                if _ratio_less(b, a):
                    return 1
            elif target.order in (1, 2, 4):
                if target.order == 4:
                    av, bv = self.engine._effective_stat(a, 13), self.engine._effective_stat(b, 13)
                else:
                    av, bv = a.attributes.health, b.attributes.health
                if av != bv:
                    return (-1 if av < bv else 1) * (-1 if target.order in (2, 4) else 1)
            ak, bk = (a.formation_slot, a.actor_id), (b.formation_slot, b.actor_id)
            return (ak > bk) - (ak < bk)
        return sorted(candidates, key=cmp_to_key(compare))

    def _select(self, actor, role, claims):
        for rule in role.rules:
            if not all(self._condition(actor, condition) for condition in rule.conditions):
                continue
            if rule.target.side == 1:
                targets = [0 if rule.action == 3 else actor.actor_id]
            elif rule.target.side == 4:
                targets = [0]
            else:
                targets = [target.actor_id for target in self._targets(actor, rule.target, role.role_id, claims)]
            for target_id in targets:
                action = self.engine.protocol.message("BattleAction", action_type=rule.action,
                                                      skill_table_id=rule.skill_id, target_id=target_id)
                if not self.engine.validate(actor, action):
                    return action
        return None
