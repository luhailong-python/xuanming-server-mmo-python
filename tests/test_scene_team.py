import asyncio
from types import SimpleNamespace

from xuanming.data import PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_team import SceneTeam, _AWAIT_LEADER_FRAMES, _LEADER_GONE, _Membership
from xuanming.protocol import Protocol


def test_follow_respects_native_membership_and_scene_fences(tmp_path):
    async def exercise():
        protocol = Protocol()
        messages = []
        scene = SceneNode(
            protocol, protocol.message("NodeInfo", node_id=5, zone_id=1, node_uuid="test"),
            SqlitePlayerStore(tmp_path / "team.sqlite"), PlayerRowCodec(protocol),
            lambda player, content: messages.append((player.player_id, content)),
        )
        for scene_id in (1, 2):
            scene.create(protocol.message("CreateSceneRequest", scene_id=scene_id, config_id=1))
        roles = {}
        for player_id in (1, 2):
            await scene.enter(protocol.message(
                "PlayerEnterGameNodeRequest", scene_id=1, session_id=player_id, player_id=player_id,
                owner_revision=1, login_generation=1,
            ), "gate")
            roles[player_id] = scene.locations[player_id].players[player_id]
        leader, follower = roles.values()
        replies = {
            i: protocol.message("teampb.ReadTeamMembershipResponse", team_id=20, membership_epoch=5, version=9,
                                record={"team_id": 20, "leader_id": 1,
                                        "members": [{"player_id": n, "join_seq": n} for n in roles]})
            for i in roles
        }
        locations = [protocol.message("storage.PlayerLocation", scene_id=1, zone_id=1, node_id="5")]
        requested = []
        location_started, location_release = asyncio.Event(), asyncio.Event()
        location_release.set()

        async def read_membership(player):
            return replies[player.player_id]

        async def read_location(player_id, leader_id):
            assert (player_id, leader_id) == (2, 1)
            location_started.set()
            await location_release.wait()
            return locations[0]

        team = SceneTeam(scene, SimpleNamespace(
            read_team_membership=read_membership, team_leader_location=read_location,
            follow_scene=lambda player, scene_id: requested.append((player.player_id, scene_id)),
        ))
        scene.team = team

        async def drain():
            while team._tasks:
                await asyncio.gather(*tuple(team._tasks.values()))

        def refresh(player):
            player.admission = SimpleNamespace(identity=protocol.message("Empty"))
            team.entered(player)

        def states():
            notify = protocol.message_ids["SceneMovementClientPlayerNotifyTeamFollowState"]
            return [protocol.parse("TeamFollowStateS2C", msg.serialized_message) for player, msg in messages
                    if player == 2 and msg.message_id == notify]

        try:
            world = scene.locations[1]
            refresh(leader)
            refresh(follower)
            await drain()
            assert not team.driven(2)
            team.follow(world, follower, None, protocol.message("TeamFollowC2S", follow=True))
            await drain()
            assert team.driven(2) and states()[-1].slot == 1
            x, y, z = leader.transform.position
            scene._move_position(world, leader, (x + 2., y, z))
            team.tick(0.2)
            assert abs(follower.transform.position[0] - (x + 0.6)) < 0.0001
            assert follower.motion.velocity[0] > 0
            replies[2] = protocol.message("teampb.ReadTeamMembershipResponse", membership_epoch=6)
            team.hint(2)
            await drain()
            assert not team.driven(2) and states()[-1].reason == 2
            replies[2] = replies[1]
            team.hint(2)
            await drain()
            assert team.membership[2].stamp == (6, 0) and not team.driven(2)

            replies[2] = protocol.message("teampb.ReadTeamMembershipResponse", team_id=20, membership_epoch=7,
                                          version=10, record=replies[1].record)
            locations[0].scene_id = 2
            locations[0].node_id = "6"
            team.hint(2)
            await drain()
            assert requested == [(2, 2)]
            requested.clear()
            locations[0].node_id = "5"
            location_started.clear()
            location_release.clear()
            team.hint(2)
            await location_started.wait()
            follower.admission.identity = None
            location_release.set()
            await drain()
            assert not requested
            refresh(follower)
            await drain()
            assert requested == [(2, 2)]
        finally:
            for player in roles.values():
                player.admission = None
            await team.close()
            await scene.close()

    asyncio.run(exercise())


def test_follow_rechecks_after_leader_handoff_settles(tmp_path):
    async def exercise():
        protocol = Protocol()
        scene = SceneNode(
            protocol, protocol.message("NodeInfo", node_id=5, zone_id=1, node_uuid="test"),
            SqlitePlayerStore(tmp_path / "await-team.sqlite"), PlayerRowCodec(protocol), lambda *args: None,
        )
        scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))
        for player_id in (1, 2):
            await scene.enter(protocol.message(
                "PlayerEnterGameNodeRequest", scene_id=1, session_id=player_id, player_id=player_id,
                owner_revision=1, login_generation=1,
            ), "gate")
        world = scene.worlds[1]
        leader, follower = world.players[1], world.players[2]
        frozen = set()
        scene.travel = SimpleNamespace(frozen=lambda player_id: player_id in frozen)
        team = scene.team = SceneTeam(scene, None)
        refreshed = []
        team.refresh = lambda player, **kwargs: refreshed.append(player.player_id)

        def attach():
            team.intents.add(2)
            team._attach(world, follower, leader, _Membership(20, (1, 1), 1, 2), True)
            assert team.driven(2)

        try:
            attach()
            frozen.add(1)
            team.tick(.05)
            assert not team.driven(2) and 2 in team._await_leader and not refreshed
            frozen.clear()
            team.tick(.05)
            assert refreshed == [2] and 2 not in team._await_leader

            attach()
            frozen.add(1)
            team.tick(.05)
            team.follow(world, follower, None, protocol.message("TeamFollowC2S", follow=False))
            frozen.clear()
            team.tick(.05)
            assert refreshed == [2] and not team._await_leader

            attach()
            frozen.add(1)
            scene.current_frame = 0xfffffff0
            team.tick(.05)
            scene.current_frame = (scene.current_frame + _AWAIT_LEADER_FRAMES) & 0xffffffff
            team.tick(.05)
            assert refreshed == [2] and not team._await_leader

            frozen.clear()
            attach()
            team._dissolve(1, _LEADER_GONE)
            world.players.pop(1)
            scene.locations.pop(1)
            team.tick(.05)
            assert refreshed == [2, 2] and not team._await_leader
        finally:
            # The last scenario removes the leader directly to simulate completed exit.
            world.players[1] = leader
            scene.locations[1] = world
            scene.travel = None
            await team.close()
            await scene.close()

    asyncio.run(exercise())
