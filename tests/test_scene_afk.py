import asyncio
from dataclasses import replace
from types import SimpleNamespace

from xuanming.data import PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_afk import FRAME_MASK, INACTIVITY_FRAMES, FrameActivity
from xuanming.nodes.scene_team import SceneTeam
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.settings import Settings
from xuanming.smoke import connect

_TOKEN = "afk-client-test"


def _settings(tmp_path):
    defaults = Settings()
    return replace(
        defaults, database=tmp_path / "afk.sqlite3",
        gate=replace(defaults.gate, port=0), scene=replace(defaults.scene, port=0, grpc_port=0),
        battle=replace(defaults.battle, port=0, grpc_port=0),
    )


async def _manual_frames(runtime):
    task = next(task for task in runtime._background if task.get_name() == "scene-tick")
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    for world in runtime.scene.worlds.values():
        world.nav = None


async def _enter(runtime, client):
    p = runtime.protocol
    await client.send(runtime.gate.issue_ticket())
    assert (await client.receive("ClientTokenVerifyResponse")).success
    assert not (await client.rpc(p.message_ids["ClientPlayerLoginLogin"], 1, p.message(
        "loginpb.LoginRequest", account="local", password=_TOKEN,
    ))).error_message.id
    assert (await client.rpc(p.message_ids["ClientPlayerLoginEnterGame"], 2, p.message(
        "loginpb.EnterGameRequest", player_id=1, request_id="afk-entry",
    ))).player_id == 1
    await client.content(p.message_ids["SceneSceneClientPlayerNotifyEnterScene"])
    await client.content(p.message_ids["SceneSceneClientPlayerNotifyActorCreate"])
    return runtime.scene.locations[1].players[1]


async def _advance(scene, count):
    for _ in range(count):
        await scene.tick(.05)


def test_native_afk_frame_boundaries_freeze_and_uint32_frame_record():
    activity = FrameActivity(100)
    activity.update(100 + INACTIVITY_FRAMES - 1)
    assert not activity.afk
    activity.update(100 + INACTIVITY_FRAMES)
    assert activity.afk and activity.afk_start_frame == 700
    activity.record(700)
    activity.update(701)
    assert activity.afk
    activity.record(701)
    activity.update(702, frozen=True)
    assert activity.afk
    activity.update(702)
    assert not activity.afk
    activity.update(1500, frozen=True)
    assert not activity.afk
    activity.update(1500)
    assert activity.afk
    wrapped = FrameActivity()
    wrapped.record(FRAME_MASK + 10)
    assert wrapped.last_active_frame == 9
    wrapped.update(608)
    assert not wrapped.afk
    wrapped.update(609)
    assert wrapped.afk


def test_real_client_activity_heartbeat_filters_and_afk_movement(tmp_path):
    async def exercise():
        async with Runtime(_settings(tmp_path), "local", dev_token=_TOKEN) as runtime:
            await _manual_frames(runtime)
            scene, p = runtime.scene, runtime.protocol
            async with connect(p, runtime.gate.info.endpoint) as client:
                actor = await _enter(runtime, client)
                start_frame = scene.current_frame
                await client.rpc(p.message_ids["SceneMovementClientPlayerMoveStart"], 0, p.message(
                    "MoveStartC2S", input_seq=1, velocity={"x": .1},
                ))
                await client.content(p.message_ids["SceneMovementClientPlayerNotifyMoveAck"])
                assert actor.activity.last_active_frame == start_frame
                await _advance(scene, INACTIVITY_FRAMES)
                assert scene.current_frame == start_frame + INACTIVITY_FRAMES and not actor.activity.afk
                before = actor.transform.position
                await scene.tick(.05)
                assert actor.activity.afk and actor.transform.position == before
                assert actor.motion.velocity == (.1, 0., 0.)
                await client.send(p.message("ClientHeartbeatRequest", seq=77))
                assert (await client.receive("ClientHeartbeatResponse")).seq == 77
                assert actor.activity.last_active_frame == start_frame
                await client.send(p.message("ClientRequest", id=3, message_id=999_999))
                await client.send(p.message("ClientHeartbeatRequest", seq=78))
                assert (await client.receive("ClientHeartbeatResponse")).seq == 78
                assert actor.activity.last_active_frame == start_frame

                teleport = p.message_ids["SceneMovementClientPlayerTeleportRequest"]
                await client.send(p.message("ClientRequest", id=4, message_id=teleport, body=b"\xff"))
                assert (await client.content(teleport, 4)).error_message.id == 1005
                assert actor.activity.last_active_frame == scene.current_frame
                assert actor.activity.afk
                await scene.tick(.05)
                assert not actor.activity.afk and actor.transform.position[0] > before[0]

                accepted_frame = actor.activity.last_active_frame
                await scene.tick(.05)
                request = p.message(
                    "ProcessClientPlayerMessageRequest", session_id=actor.session_id + 1, player_id=1,
                    message_content=p.content(teleport, 5),
                )
                try:
                    await scene.process(request, actor.gate_uuid)
                except ValueError:
                    pass
                else:
                    raise AssertionError("stale Scene session was accepted")
                assert actor.activity.last_active_frame == accepted_frame
                response = await client.rpc(teleport, 6, p.message("TeleportRequestC2S"))
                assert response.error_message.id == 1006
                assert actor.activity.last_active_frame == scene.current_frame

    asyncio.run(exercise())


def test_replacement_login_preserves_activity_and_fresh_load_seeds_current_frame(tmp_path):
    async def exercise():
        async with Runtime(_settings(tmp_path), "local", dev_token=_TOKEN) as runtime:
            await _manual_frames(runtime)
            scene, p = runtime.scene, runtime.protocol
            async with connect(p, runtime.gate.info.endpoint) as original:
                actor = await _enter(runtime, original)
                await _advance(scene, INACTIVITY_FRAMES + 1)
                assert actor.activity.afk
                remembered = actor.activity
                previous_session = actor.session_id
                async with connect(p, runtime.gate.info.endpoint) as replacement:
                    assert await _enter(runtime, replacement) is actor
                    assert actor.session_id != previous_session and actor.activity is remembered and actor.activity.afk
                    await scene.tick(.05)
                    assert actor.activity.afk
                    response = await replacement.rpc(p.message_ids["SceneMovementClientPlayerTeleportRequest"], 3,
                                                     p.message("TeleportRequestC2S"))
                    assert response.error_message.id == 1006
                    await scene.tick(.05)
                    assert not actor.activity.afk
                    await replacement.rpc(p.message_ids["ClientPlayerLoginLeaveGame"], 4,
                                          p.message("loginpb.LeaveGameRequest"))
                    assert 1 not in scene.locations
                async with connect(p, runtime.gate.info.endpoint) as fresh:
                    loaded = await _enter(runtime, fresh)
                    assert loaded is not actor and loaded.activity is not remembered
                    assert loaded.activity.last_active_frame == scene.current_frame and not loaded.activity.afk

    asyncio.run(exercise())


def test_monthly_hosted_afk_follower_still_uses_server_team_motion(tmp_path):
    async def exercise():
        p = Protocol()
        scene = SceneNode(p, p.message("NodeInfo", node_id=5, zone_id=1, node_uuid="afk-team"),
                          SqlitePlayerStore(tmp_path / "hosted.sqlite3"), PlayerRowCodec(p), lambda *_: None)
        scene.create(p.message("CreateSceneRequest", scene_id=1, config_id=1))
        for player_id in (1, 2):
            await scene.enter(p.message(
                "PlayerEnterGameNodeRequest", player_id=player_id, session_id=player_id,
                owner_revision=1, login_generation=1, scene_id=1,
            ), "gate")
        world = scene.locations[1]
        leader, follower = world.players[1], world.players[2]
        for actor in (leader, follower):
            actor.admission = SimpleNamespace(identity=p.message("Empty"))

        async def membership(_actor):
            return p.message("teampb.ReadTeamMembershipResponse", team_id=20, membership_epoch=1, version=1,
                             record={"team_id": 20, "leader_id": 1,
                                     "members": [{"player_id": n, "join_seq": n} for n in (1, 2)]})

        async def location(_follower, _leader):
            return p.message("storage.PlayerLocation", scene_id=1, zone_id=1, node_id="5")

        team = scene.team = SceneTeam(scene, SimpleNamespace(read_team_membership=membership,
                                                            team_leader_location=location))

        async def drain():
            while team._tasks:
                await asyncio.gather(*tuple(team._tasks.values()))

        async def read_pass(_player_id):
            return b"10000"

        try:
            team.entered(leader)
            team.entered(follower)
            await drain()
            assert not team.driven(2)
            scene.connection_holds.read_pass = read_pass
            scene.connection_holds.unix_clock = lambda: 100
            await scene.process(p.message(
                "ProcessClientPlayerMessageRequest", session_id=2, player_id=2,
                message_content=p.content(p.message_ids["ScenePlayerExitGame"], body=p.message(
                    "GameNodeExitGameRequest", reason=2,
                ).SerializeToString()),
            ), "gate")
            await asyncio.sleep(0)
            await drain()
            assert scene.connection_holds.hosted(follower) and team.driven(2)
            scene.current_frame = INACTIVITY_FRAMES
            leader.activity.record(scene.current_frame)
            scene.set_velocity(leader, (2., 0., 0.))
            before = follower.transform.position
            await _advance(scene, 35)
            assert follower.activity.afk and scene.connection_holds.held(follower)
            assert follower.transform.position[0] > before[0] and follower.motion.velocity[0] > 0
        finally:
            leader.admission = follower.admission = None
            await team.close()
            await scene.close()

    asyncio.run(exercise())
