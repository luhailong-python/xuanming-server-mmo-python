from test_battle import create_request, issue, runtime


def test_cooldowns_are_visible_only_to_player_and_pet_owner(runtime):
    protocol, node, _, _, _ = runtime
    request = create_request(protocol)
    for player, pet_id in zip(request.players, (11, 22)):
        pet = player.pets.add(pet_id=pet_id, owner_player_id=player.player_id, max_health=30)
        pet.base_attributes.health = 30
    assert not node.create(request).error_message.id
    room = node.rooms[10]
    for actor in room.engine.actors.values():
        actor.skill_cooldown_rounds[77] = 3

    query = protocol.message("GetBattleStateRequest", battle_id=10)
    for viewer in (101, 202):
        assignment = issue(protocol, node, viewer)
        ticket = node.authorize(assignment.token_payload, assignment.token_signature)
        state = node.dispatch(ticket, 140, query)
        for actor in state.actors:
            own = actor.actor_id == viewer or actor.owner_player_id == viewer
            assert bool(actor.skill_cooldown_rounds) == own
    spectator = room.engine.snapshot(0, 0)
    assert all(not actor.skill_cooldown_rounds for actor in spectator.actors)
