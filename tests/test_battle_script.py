import json
from pathlib import Path

from xuanming.nodes.battle import TurnEngine
from xuanming.nodes.battle_script import _ratio_below, _ratio_less
from xuanming.nodes.battle_tables import BattleBuff, BattleSkill
from xuanming.protocol import Protocol


def _request(protocol):
    request = protocol.message('CreateBattleRequest', battle_id=20, match_mode=7, seed=1)
    for player_id, team in ((1, 0), (2, 1), (3, 1)):
        player = request.players.add(player_id=player_id, team_index=team, max_health=100)
        player.base_attributes.health = 100
    return request


def _rule(role, *, priority=0, action=1, side=2, order=0):
    return role.rules.add(priority=priority, action=action, target={'side': side, 'order': order})


def _engine(protocol, request, **kwargs):
    return TurnEngine(protocol, request, skills=kwargs.get('skills', {}), buffs=kwargs.get('buffs', {}),
                      items={}, max_rounds=20, monster_ids={10})


def test_script_sorts_rules_and_targets_without_consuming_randomness():
    protocol = Protocol()
    request = _request(protocol)
    role = request.team_scripts.add(team_index=0).script
    role.schema_version = 1
    slot = role.roles.add(role_id=1, actor_type=1, order=5)
    _rule(slot, priority=9, action=3, side=1)
    first = _rule(slot, priority=2, side=2, order=1)
    first.conditions.add(kind=2, p1=2)
    second = _rule(slot, priority=3, side=2, order=1)
    second.target.filters.add(kind=1, p1=8000)
    engine = _engine(protocol, request)
    engine.actors[3].attributes.health = 40
    engine._fill_default_actions()
    assert engine.pending[1].target_id == 3
    assert engine.pending[1].action_type == 1
    assert engine.rng.next() == _engine(protocol, request).rng.next()


def test_script_roles_use_manual_claims_and_invalid_script_falls_back():
    protocol = Protocol()
    request = _request(protocol)
    player = request.players.add(player_id=4, team_index=0, max_health=100)
    player.base_attributes.health = 100
    script = request.team_scripts.add(team_index=0).script
    script.schema_version = 1
    first = script.roles.add(role_id=1, order=1, actor_type=1, max_members=1)
    _rule(first, side=2)
    second = script.roles.add(role_id=2, order=2, actor_type=1)
    rule = _rule(second, side=2)
    rule.target.filters.add(kind=7, p1=1)
    engine = _engine(protocol, request)
    engine.pending[1] = protocol.message('BattleAction', action_type=1, target_id=3)
    engine._fill_default_actions()
    assert engine.pending[4].target_id == 3
    second.order = 1
    invalid = _engine(protocol, request)
    invalid._fill_default_actions()
    assert not invalid.scripts.scripts and invalid.pending[4].target_id == 0


def test_monster_script_grants_only_valid_zero_cost_legacy_skills():
    protocol = Protocol()
    request = _request(protocol)
    del request.players[1:]
    monster = protocol.message('BattleActorState', monster_table_id=10, max_health=100,
                               attributes={'health': 100})
    script = request.team_scripts.add(team_index=1).script
    script.schema_version = 1
    role = script.roles.add(role_id=1, actor_type=2, monster_id=10)
    role.rules.add(action=2, skill_id=77, target={'side': 2})
    engine = TurnEngine(protocol, request, monsters=[monster], skills={77: BattleSkill(1, requires_target=True)},
                        items={}, max_rounds=10, monster_ids={10})
    enemy = next(actor for actor in engine.actors.values() if actor.actor_type == 2)
    engine._fill_default_actions()
    assert list(enemy.skill_table_ids) == [77]
    assert engine.pending[enemy.actor_id].skill_table_id == 77
    invalid = TurnEngine(protocol, request, monsters=[monster], skills={77: BattleSkill(1, mana_cost=1)},
                         items={}, max_rounds=10, monster_ids={10})
    assert not invalid.scripts.scripts
    unknown_table = TurnEngine(protocol, request, monsters=[monster], skills={77: BattleSkill(1)},
                               items={}, max_rounds=10)
    assert not unknown_table.scripts.scripts


def test_malformed_script_is_ignored_as_a_whole():
    protocol = Protocol()
    request = _request(protocol)
    script = request.team_scripts.add(team_index=0).script
    script.schema_version = 1
    role = script.roles.add(role_id=3, actor_type=1)
    _rule(role, priority=1, action=3, side=1)
    bad = _rule(role, priority=2, side=2)
    bad.target.filters.add(kind=7, p1=999)
    engine = _engine(protocol, request)
    engine._fill_default_actions()
    assert not engine.scripts.scripts
    assert engine.pending[1].action_type == 1


def test_class_skill_rejects_wrong_target_side():
    protocol = Protocol()
    request = _request(protocol)
    script = request.team_scripts.add(team_index=0).script
    script.schema_version = 1
    role = script.roles.add(role_id=1, actor_type=1)
    role.rules.add(action=2, skill_id=8, target={'side': 2})
    engine = _engine(protocol, request, skills={8: BattleSkill(0, class_id=2, target_team=2)})
    assert not engine.scripts.scripts


def test_ratio_boundaries_match_native_battle_script_rules():
    protocol = Protocol()
    actor = lambda health, maximum: protocol.message(
        'BattleActorState', attributes={'health': health}, max_health=maximum)
    max_uint64 = (1 << 64) - 1
    for health, maximum, threshold, expected in (
        (29, 100, 3000, True), (30, 100, 3000, False),
        (0, 0, 10000, False), (0, 1, 1, True),
        (max_uint64, max_uint64, 10000, False),
    ):
        assert _ratio_below(health, maximum, threshold) is expected
    for lhs, rhs, expected in (
        ((1, 3), (1, 2), True), ((1, 2), (1, 3), False),
        ((2, 4), (1, 2), False), ((99, 100), (0, 0), True),
        ((5, 0), (99, 100), False),
    ):
        assert _ratio_less(actor(*lhs), actor(*rhs)) is expected


def test_same_role_spreads_targets_like_native_script():
    protocol = Protocol()
    request = _request(protocol)
    request.players.add(player_id=4, team_index=0, max_health=100).base_attributes.health = 100
    script = request.team_scripts.add(team_index=0).script
    script.schema_version = 1
    role = script.roles.add(role_id=1, actor_type=1)
    rule = _rule(role, side=2)
    rule.target.filters.add(kind=7, p1=0, negate=True)
    engine = _engine(protocol, request)
    engine._fill_default_actions()
    assert engine.pending[1].target_id == 2
    assert engine.pending[4].target_id == 3


def test_round_condition_and_fallback_match_native_script():
    protocol = Protocol()
    request = _request(protocol)
    script = request.team_scripts.add(team_index=0).script
    script.schema_version = 1
    role = script.roles.add(role_id=1, actor_type=1)
    defend = _rule(role, priority=10, action=3, side=1)
    defend.conditions.add(kind=3, p1=2, p2=1)
    _rule(role, priority=20, action=1, side=2)
    engine = _engine(protocol, request)
    for round_index, expected_action in ((1, 3), (2, 1), (3, 3)):
        engine.round_index = round_index
        engine.pending.clear()
        engine._fill_default_actions()
        assert engine.pending[1].action_type == expected_action


def test_cooldown_falls_through_to_next_rule_like_native_script():
    protocol = Protocol()
    request = _request(protocol)
    request.players[0].skill_table_ids.append(77)
    script = request.team_scripts.add(team_index=0).script
    script.schema_version = 1
    role = script.roles.add(role_id=1, actor_type=1)
    role.rules.add(priority=10, action=2, skill_id=77, target={'side': 2})
    _rule(role, priority=20, action=3, side=1)
    engine = _engine(protocol, request, skills={77: BattleSkill(1, requires_target=True)})
    engine._fill_default_actions()
    assert engine.pending[1].action_type == 2
    engine.pending.clear()
    engine.actors[1].skill_cooldown_rounds[77] = 1
    engine._fill_default_actions()
    assert engine.pending[1].action_type == 3


def test_combo_matches_native_head_each_round():
    fixture = json.loads((Path(__file__).parent / 'fixtures' / 'battle_script_combo_head.json').read_text())
    protocol = Protocol()
    request = protocol.parse('CreateBattleRequest', bytes.fromhex(fixture['ORACLE_REQUEST']))
    state = protocol.parse('BattleStateS2C', bytes.fromhex(fixture['ORACLE_STATE']))
    skills = {9501: BattleSkill(0, requires_target=True, effects=(9930,)),
              9502: BattleSkill(0, requires_target=True, effects=(9952,)),
              9503: BattleSkill(0, requires_target=True, effects=(9950,)),
              9504: BattleSkill(30, requires_target=True)}
    buffs = {9930: BattleBuff(buff_type=30, duration_rounds=3),
             9952: BattleBuff(buff_type=52, duration_rounds=3),
             9950: BattleBuff(buff_type=50, duration_rounds=3)}
    engine = TurnEngine(protocol, request, skills=skills, buffs=buffs,
                        monsters=[a for a in state.actors if a.actor_type == 2],
                        items={}, max_rounds=20, monster_ids={101, 102})
    for actor in engine.actors.values():
        if actor.actor_type == 1:
            actor.is_auto = True
    expected_order = [8001, 8002, 8003, 8004, 9223372041149743104, 9223372041149743105]
    for index in range(1, 3):
        native = protocol.parse('TurnResultS2C', bytes.fromhex(fixture[f'ORACLE_ROUND{index}']))
        actual = engine.resolve()
        # Native engine exposes this order separately; its battle node writes it into the response.
        assert list(actual.action_order) == expected_order
        assert not native.action_order
        actual.ClearField('action_order')
        assert actual == native
