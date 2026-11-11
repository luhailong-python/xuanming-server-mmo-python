import asyncio
from types import SimpleNamespace

from xuanming.data import PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.scene import SceneNode, ZERO
from xuanming.nodes.scene_team import SceneTeam, _Membership
from xuanming.protocol import Protocol


def test_missing_required_navigation_fences_movement_and_team_follow(tmp_path):
    async def exercise():
        p, sent = Protocol(), []
        scene = SceneNode(p, p.message("NodeInfo", node_id=5, zone_id=1, node_uuid="nav-test"),
                          SqlitePlayerStore(tmp_path / "nav.sqlite"), PlayerRowCodec(p),
                          lambda player, content: sent.append((player.player_id, content)))
        scene.tables = {"BaseScene": {
            1: SimpleNamespace(nav_bin_file="data/scene_nav_bin/festival_maps/01001/scene.bin",
                               spawn_x=180., spawn_y=200., spawn_z=0.),
            2: SimpleNamespace(nav_bin_file="data/scene_nav_bin/legacy.bin",
                               spawn_x=180., spawn_y=200., spawn_z=0.),
        }}
        for scene_id in (1, 2):
            scene.create(p.message("CreateSceneRequest", scene_id=scene_id, config_id=scene_id))
        for player_id in (1, 2):
            await scene.enter(p.message("PlayerEnterGameNodeRequest", scene_id=1, session_id=player_id,
                                        player_id=player_id, owner_revision=1, login_generation=1), "gate")
        world = scene.worlds[1]
        leader, follower = world.players[1], world.players[2]
        team = scene.team = SceneTeam(scene, None)
        try:
            origin = leader.transform.position
            scene.set_velocity(leader, (2., 0., 0.))
            scene._movement(world, leader, SimpleNamespace(method="MoveStart"), p.message(
                "MoveStartC2S", input_seq=1, start_location={"x": origin[0] + 5, "y": origin[1]},
                velocity={"x": 2},
            ))
            assert leader.transform.position == origin and leader.motion.velocity == ZERO
            ack_id = p.message_ids["SceneMovementClientPlayerNotifyMoveAck"]
            ack = p.parse("MoveAckS2C", next(content.serialized_message for player, content in reversed(sent)
                                             if player == 1 and content.message_id == ack_id))
            assert (ack.server_location.x, ack.server_location.y) == origin[:2]
            assert (ack.server_velocity.x, ack.server_velocity.y, ack.server_velocity.z) == ZERO
            scene.set_velocity(leader, (2., 0., 0.))
            await scene.tick(.5)
            assert leader.transform.position == origin and leader.motion.velocity == ZERO
            assert not scene.teleport(world, leader, (1., 2., 0.), ZERO, reason=5)
            assert leader.transform.position == origin

            # A required mesh cannot be replaced by a straight-line approach or a teleport.
            scene._move_position(world, follower, (origin[0] + 20, origin[1], origin[2]))
            team._attach(world, follower, leader, _Membership(20, (1, 1), 1, 2), True)
            assert not team.driven(2)
            assert follower.transform.position == (origin[0] + 20, origin[1], origin[2])

            # Already on the trail may attach, but must detach if a later step needs movement.
            scene._move_position(world, follower, origin)
            team._attach(world, follower, leader, _Membership(20, (1, 1), 1, 2), True)
            assert team.driven(2)
            scene._move_position(world, leader, (origin[0] + 3, origin[1], origin[2]))
            team.tick(.5)
            assert not team.driven(2) and follower.transform.position == origin

            # Legacy maps retain the original fail-open contract.
            scene._relocate(world, scene.worlds[2], leader)
            legacy = scene.worlds[2]
            before = leader.transform.position
            scene._movement(legacy, leader, SimpleNamespace(method="MoveStart"), p.message(
                "MoveStartC2S", input_seq=1, start_location={"x": before[0] + 2, "y": before[1]},
                velocity={"x": 1},
            ))
            assert leader.transform.position[0] == before[0] + 2 and leader.motion.velocity == (1., 0., 0.)
        finally:
            await team.close()
            await scene.close()

    asyncio.run(exercise())
