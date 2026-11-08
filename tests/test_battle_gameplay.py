from dataclasses import replace
from hashlib import sha256

from xuanming.data import ConfigTables
from xuanming.nodes.battle import TurnEngine
from xuanming.nodes.battle_tables import BattleBuff, BattleSkill, battle_table_fingerprint
from xuanming.protocol import Protocol


def _build(rules, *, buffs=None, statuses=None, permissions=None):
    p = Protocol()
    request = p.message("CreateBattleRequest", battle_id=1, match_mode=7, seed=5489)
    for player_id, team, speed in ((1, 0, 20), (2, 0, 10), (3, 1, 50), (4, 1, 40)):
        player = request.players.add(player_id=player_id, team_index=team, class_id=1, level=9,
                                     max_health=500, max_mana=100, physical_attack=100, magic_attack=200, defense=100)
        player.base_attributes.health = 500
        player.base_attributes.mana = 100
        player.base_attributes.speed = speed
        player.skill_table_ids.extend(rules)
        for skill_id, skill in rules.items():
            if skill.class_id:
                player.skill_ranks[skill_id] = 10
    if statuses:
        statuses(request)
    return p, TurnEngine(p, request, skills=rules, buffs=buffs, items={}, max_rounds=20, silence_permissions=permissions)


def _action(p, engine, actor, skill_id, target=0):
    action = p.message("BattleAction", action_type=2, skill_table_id=skill_id, target_id=target)
    assert not engine.validate(engine.actors[actor], action)
    engine.pending[actor] = action


def _defend(p, engine):
    for actor in engine.actors.values():
        engine.pending[actor.actor_id] = p.message("BattleAction", action_type=3)


def test_battle_fingerprint_matches_native_protobuf_for_same_tables():
    expected = "19ee31807f2393e449dcebebb5cca233"
    assert battle_table_fingerprint(ConfigTables()) == expected
    assert battle_table_fingerprint(ConfigTables(fmt="pb")) == expected


def test_class_rank_clamps_and_group_cast_spends_once_with_initiative():
    rule = BattleSkill(10, class_id=1, unlock_level=1, max_rank=10, target_team=1, max_targets=3,
                       mana_percent=0.07, initiative=1, cooldown_rounds=2)
    p, engine = _build({1: rule, 2: replace(rule, class_id=2)})
    actor = engine.actors[1]
    assert list(actor.skill_table_ids) == [1]
    assert dict(actor.skill_ranks) == {1: 5}
    assert engine.validate(actor, p.message("BattleAction", action_type=2, skill_table_id=1, target_id=2)) == 7002
    _defend(p, engine)
    _action(p, engine, 1, 1, 4)
    turn = engine.resolve()
    assert sha256(turn.SerializeToString(deterministic=True)).hexdigest() == (
        "901ece7451619ac423fc677828f76e6234616e2c98ff706e606422f062eb5d54"
    )
    assert turn.action_order[0] == 1
    assert [(e.event_type, e.target_id, e.hit_index) for e in turn.events[:4]] == [
        (2, 4, 0), (14, 1, 0), (3, 4, 0), (3, 3, 1),
    ]
    assert turn.events[1].mana_delta == -7 and actor.attributes.mana == 93
    assert actor.skill_cooldown_rounds[1] == 2
    assert engine.validate(actor, p.message("BattleAction", action_type=2, skill_table_id=1)) == 7003


def test_class_auto_heals_weakest_then_buff_expiry_keeps_full_rounds():
    heal = BattleSkill(0, class_id=1, unlock_level=1, max_rank=10, target_team=2, max_targets=1,
                       mana_percent=.06, heal_multiplier=.4, heal_per_level=1.0)
    guard = BattleSkill(0, class_id=1, unlock_level=1, max_rank=10, target_team=3, max_targets=1,
                        effects=(90,), buff_percent=.25, initiative=1)
    p, engine = _build({1: heal, 2: guard}, buffs={90: BattleBuff(buff_type=11, duration_rounds=2)})
    engine.actors[2].attributes.health = 80
    engine.actors[1].attributes.health = 300
    engine.actors[1].is_auto = True
    _defend(p, engine)
    del engine.pending[1]
    turn = engine.resolve()
    heal_event = next(e for e in turn.events if e.event_type == 4)
    assert heal_event.target_id == 2 and heal_event.value == 96
    assert engine.actors[2].attributes.health == 176 and engine.actors[1].attributes.mana == 94
    _defend(p, engine)
    _action(p, engine, 1, 2)
    engine.resolve()
    assert engine.actors[1].buffs[0].remain_rounds == 2
    assert engine._effective_stat(engine.actors[1], 11) == 127
    _defend(p, engine)
    engine.resolve()
    assert engine.actors[1].buffs[0].remain_rounds == 1
    _defend(p, engine)
    turn = engine.resolve()
    assert not engine.actors[1].buffs and any(e.event_type == 6 and e.buff_table_id == 90 for e in turn.events)


def test_legacy_poison_uses_defense_tick_resist_and_source_order():
    poison = BattleSkill(0, target_modes=(2,), skill_types=(1,), effects=(90,), cooldown_rounds=1)
    rules = {90: BattleBuff(buff_type=50, duration_rounds=2, interval_rounds=1, interval_damage=40, max_layer=2)}
    p, engine = _build({1: poison}, buffs=rules)
    engine.actors[3].combat.resist_poison = 100
    _defend(p, engine)
    _action(p, engine, 1, 1, 3)
    turn = engine.resolve()
    events = [(e.event_type, e.target_id, e.hit_index, e.value) for e in turn.events if e.buff_table_id == 90]
    assert events == [(15, 3, 0, 0), (5, 4, 1, 1), (7, 4, 0, 20)]
    assert not engine.actors[3].buffs and engine.actors[4].attributes.health == 480
    assert engine.actors[1].skill_cooldown_rounds[1] == 0
    _defend(p, engine)
    turn = engine.resolve()
    assert engine.actors[4].attributes.health == 460 and not engine.actors[4].buffs
    assert [e.event_type for e in turn.events if e.buff_table_id == 90] == [7, 6]


def test_counter_has_its_own_event_group_and_does_not_chain():
    p, engine = _build({})
    engine.actors[1].combat.counter_rate = 100
    engine.actors[3].combat.counter_rate = 100
    _defend(p, engine)
    engine.pending[3] = p.message("BattleAction", action_type=1, target_id=1)
    turn = engine.resolve()
    assert [(e.source_id, e.target_id, e.hit_kind, e.group_id) for e in turn.events[:4]] == [
        (3, 1, 0, 1), (3, 1, 0, 1), (1, 3, 2, 2), (1, 3, 2, 2),
    ]
    assert turn.events[4].group_id == 3
