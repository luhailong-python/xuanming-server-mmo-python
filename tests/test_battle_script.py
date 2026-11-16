from xuanming.nodes.battle import TurnEngine
from xuanming.nodes.battle_tables import BattleSkill
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
