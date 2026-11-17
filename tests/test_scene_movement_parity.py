import asyncio
from types import SimpleNamespace

from test_scene_afk import _TOKEN, _enter, _manual_frames, _settings
from test_scene_assets import _scene
from xuanming.nodes.scene import ZERO
from xuanming.runtime import Runtime
from xuanming.smoke import connect


def test_repeated_default_input_sequence_is_accepted_over_real_tcp(tmp_path):
    async def run():
        async with Runtime(_settings(tmp_path), "local", dev_token=_TOKEN) as runtime:
            await _manual_frames(runtime)
            scene, protocol = runtime.scene, runtime.protocol
            async with connect(protocol, runtime.gate.info.endpoint) as client:
                actor = await _enter(runtime, client)
                # Use a legacy no-navigation world to isolate packet semantics.
                scene.tables = None
                original = actor.transform.position
                position = dict(zip(("x", "y", "z"), original))

                async def wait_for(predicate):
                    async with asyncio.timeout(5):
                        while not predicate():
                            await asyncio.sleep(.01)

                await client.rpc(protocol.message_ids["SceneMovementClientPlayerMoveStart"], 0,
                                 protocol.message("MoveStartC2S", start_location=position, velocity={"x": 1}))
                await wait_for(lambda: actor.motion.velocity == (1., 0., 0.))
                await client.rpc(protocol.message_ids["SceneMovementClientPlayerMoveSync"], 0,
                                 protocol.message("MoveSyncC2S", location=position, velocity={"x": 2}))
                await wait_for(lambda: actor.motion.velocity == (2., 0., 0.))
                # Omitted location is the protobuf default (0,0,0), as in C++.
                await client.rpc(protocol.message_ids["SceneMovementClientPlayerMoveStop"], 0,
                                 protocol.message("MoveStopC2S"))
                await wait_for(lambda: actor.motion.velocity == ZERO and actor.transform.position == ZERO)

    asyncio.run(run())


def test_strict_mask_recovers_an_invalid_origin_at_spawn_not_reported_position(tmp_path):
    class StrictNavigation:
        mask = object()

        def validate_move(self, start, target):
            return False, None  # starting point is not on the mesh

        def snap(self, position):
            return (position[0], position[1], 0.)

    async def run():
        protocol, scene, _, _ = await _scene(tmp_path)
        world = scene.worlds[1]
        actor = world.players[1]
        try:
            world.nav = StrictNavigation()
            actor.transform.position = (-500., -500., 0.)
            scene._movement(world, actor, SimpleNamespace(method="MoveStart"), protocol.message(
                "MoveStartC2S", start_location={"x": 7, "y": 8}, velocity={"x": 1}))
            assert actor.transform.position == scene._spawn_position(world)
            assert actor.transform.position != (7., 8., 0.)
        finally:
            await scene.close()

    asyncio.run(run())


def test_new_aoi_observer_receives_current_movement_after_create(tmp_path):
    async def run():
        protocol, scene, _, _ = await _scene(tmp_path)
        sent = []
        scene.emit = lambda player, content: sent.append((player.player_id, content))
        try:
            await scene.enter(protocol.message("PlayerEnterGameNodeRequest", player_id=2, session_id=2,
                                               scene_id=1, owner_revision=1, login_generation=1), "gate")
            world = scene.worlds[1]
            first, second = world.players[1], world.players[2]
            scene.set_velocity(first, (1., 0., 0.))
            scene.set_velocity(second, (0., 2., 0.))
            sent.clear()
            await scene.enter(protocol.message("PlayerEnterGameNodeRequest", player_id=3, session_id=3,
                                               scene_id=1, owner_revision=1, login_generation=1), "gate")
            own = [content for player, content in sent if player == 3]
            create_id = protocol.message_ids["SceneSceneClientPlayerNotifyActorListCreate"]
            list_id = protocol.message_ids["SceneMovementClientPlayerNotifyActorMoveList"]
            create_index = next(i for i, content in enumerate(own) if content.message_id == create_id)
            move_index = next(i for i, content in enumerate(own) if content.message_id == list_id)
            assert create_index < move_index
            created = protocol.parse("ActorListCreateS2C", own[create_index].serialized_message)
            assert {actor.entity for actor in created.actor_list} == {first.entity, second.entity}
            moving = protocol.parse("ActorMoveListS2C", own[move_index].serialized_message)
            assert {move.entity for move in moving.moves} == {first.entity, second.entity}
            assert all(move.move_state == 1 for move in moving.moves)

            # AFK actors are not moving in the native AOI snapshot even if a
            # late client input has populated velocity before AFK clears.
            first.activity.afk_start_frame = scene.current_frame
            sent.clear()
            scene._push_visibility(world, [world.players[3]], (first.entity, second.entity), shown=True)
            follow = [content for player, content in sent if player == 3]
            single_id = protocol.message_ids["SceneMovementClientPlayerNotifyActorMove"]
            assert [content.message_id for content in follow] == [create_id, single_id]
            assert protocol.parse("ActorMoveS2C", follow[1].serialized_message).entity == second.entity

            # A server-driven team follower remains advancing while AFK.
            scene.team = SimpleNamespace(driven=lambda player_id: player_id == first.player_id)
            sent.clear()
            scene._push_visibility(world, [world.players[3]], (first.entity, second.entity), shown=True)
            follow = [content for player, content in sent if player == 3]
            assert [content.message_id for content in follow] == [create_id, list_id]
        finally:
            scene.team = None
            await scene.close()

    asyncio.run(run())
