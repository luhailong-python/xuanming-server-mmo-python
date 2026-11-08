from dataclasses import dataclass

from xuanming.data import ConfigTables
from xuanming.nodes.scene_buffs import IMMUNE, NO_BUFF, SUCCESS, SceneBuffs
from xuanming.protocol import Protocol


@dataclass
class _Clock:
    now: float = 10.0

    def __call__(self) -> float:
        return self.now


@dataclass
class _Actor:
    entity: int
    player_id: int = 100


class _Table:
    def __init__(self, rows):
        self._rows = {row.id: row for row in rows}

    def rows(self):
        return tuple(self._rows.values())

    def get(self, key):
        return self._rows.get(key)


def _buffs(clock, *rows, **kwargs):
    config = ConfigTables()
    tables = dict(config.tables)
    stock = tables["Buff"].rows()
    buff_type = type(stock[0])
    tables["Buff"] = _Table([*stock, *(buff_type(**row) for row in rows)])
    permission = tables["SkillPermission"].get(1)
    permission.skill_type[1] = 7005
    tables["SkillPermission"] = _Table([*tables["SkillPermission"].rows(), permission])
    return SceneBuffs(Protocol(), tables, clock=clock, **kwargs)


def test_realtime_stacking_does_not_extend_expiration_and_keeps_caster_domains():
    actor, clock = _Actor(11), _Clock()
    buffs = _buffs(clock, {"id": 100, "duration": 3, "no_caster": 1, "max_layer": 2},
                  {"id": 101, "duration": -1, "max_layer": 2})
    error, buff = buffs.add(actor, 100, caster=1)
    assert error == SUCCESS and buff != NO_BUFF
    clock.now += 1
    assert buffs.add(actor, 100, caster=2) == (SUCCESS, NO_BUFF)
    assert buffs.add(actor, 100, caster=3) == (SUCCESS, NO_BUFF)
    assert [(entry.id, entry.layer, entry.caster, entry.processed_caster) for entry in buffs.entries(actor)] == [(buff, 2, 1, 0)]
    buffs.add(actor, 101, caster=0)
    buffs.add(actor, 101, caster=0)
    buffs.add(actor, 101, caster=3)
    assert len(buffs.entries(actor)) == 4
    clock.now += 2
    buffs.tick(2)
    assert [entry.row.id for entry in buffs.entries(actor)] == [101, 101, 101]


def test_realtime_immunity_discharge_and_stealth_source_reference_count():
    actor, clock = _Actor(11), _Clock()
    buffs = _buffs(clock, {"id": 100, "duration": 6, "buff_type": 33, "tag": {"Hidden": True}},
                  {"id": 101, "buff_type": 35, "dispel_tag": {"Hidden": True}})
    transitions = []
    buffs.visibility_changed = lambda actor: transitions.append(buffs.stealthed(actor))
    buffs.add(actor, 15)
    assert buffs.add(actor, 13) == (IMMUNE, NO_BUFF)
    assert len(buffs.entries(actor)) == 1
    first = buffs.add(actor, 100, caster=1)[1]
    buffs.add(actor, 100, caster=2)
    buffs.remove(actor, first)
    assert transitions == [True]
    assert buffs.add(actor, 101) == (SUCCESS, NO_BUFF)
    assert transitions == [True, False]
    buffs.add(actor, 100)
    clock.now += 6
    assert buffs.stealthed(actor) and transitions == [True, False, True]
    buffs.tick(6)
    assert not buffs.stealthed(actor) and transitions == [True, False, True, False]


def test_realtime_silence_uses_skill_permission_until_last_source_expires():
    actor, clock = _Actor(11), _Clock()
    buffs = _buffs(clock, {"id": 100, "duration": 1, "buff_type": 31},
                  {"id": 101, "duration": 2, "buff_type": 31})
    changes = []
    buffs.state_changed = lambda actor: changes.append(buffs.state_flags(actor))
    buffs.add(actor, 100, caster=22)
    buffs.add(actor, 101, caster=33)
    assert buffs.validate_skill(actor, [1]) == 7005
    assert buffs.validate_skill(actor, [5]) == 0
    assert buffs.validate_skill(actor, [9]) == 1002
    clock.now += 1
    buffs.tick(1)
    assert buffs.validate_skill(actor, [1]) == 7005
    clock.now += 1
    buffs.tick(1)
    assert buffs.validate_skill(actor, [1]) == 0 and changes == [{1: True}, {}]


def test_realtime_idle_children_are_independent_and_reset_on_hit():
    actor, clock = _Actor(11), _Clock()
    buffs = _buffs(clock, {"id": 100, "buff_type": 43, "infinite_duration": 1, "combat_idle_seconds": 5,
                           "interval": 1, "sub_buff": [101]},
                  {"id": 101, "buff_type": 42, "infinite_duration": 1, "interval": 1, "no_caster": 1,
                   "max_layer": 3, "health_regeneration": "0.013 * level * health"})
    parent = buffs.add(actor, 100)[1]
    unrelated = buffs.add(actor, 101)[1]
    clock.now = 14.875
    buffs.tick(4.875)
    assert [entry.id for entry in buffs.entries(actor)] == [parent, unrelated]
    clock.now = 15.0
    buffs.tick(0.125)
    assert len(buffs.entries(actor)) == 3 and buffs.entries(actor)[2].idle_owner == parent
    clock.now += 1
    due = buffs.tick(1)[0][1]
    assert set(due) == {entry.id for entry in buffs.entries(actor) if entry.row.buff_type == 42}
    assert buffs.heal(actor, due, 100, 200, 10) == 124
    buffs.reset_combat_idle(actor)
    assert [entry.id for entry in buffs.entries(actor)] == [parent, unrelated]
    clock.now += 5
    buffs.tick(5)
    assert len(buffs.entries(actor)) == 3
    buffs.remove(actor, parent)
    assert [entry.id for entry in buffs.entries(actor)] == [unrelated]


def test_realtime_next_damage_bonus_consumes_and_creates_target_sub_buff():
    caster, target, clock = _Actor(11, 1), _Actor(22, 2), _Clock()
    buffs = _buffs(clock)
    buffs.add(caster, 19, caster.entity)
    assert buffs.before_damage(caster, target, 5) == 71
    assert not buffs.entries(caster)
    assert [entry.row.id for entry in buffs.entries(target)] == [20]
    assert buffs.before_damage(caster, target, 5) == 5
    snapshot = buffs.protocol.message("BattlePlayerSnapshot", player_id=2)
    buffs.snapshot(target, snapshot)
    assert snapshot.buffs[0].caster_id == 0 and snapshot.buffs[0].remain_rounds == 1
