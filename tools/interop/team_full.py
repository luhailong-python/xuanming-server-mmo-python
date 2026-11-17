"""Verify native Go team membership and Scene movement with two real TCP roles."""

import asyncio
import math
from uuid import uuid4

import login_full
from scene_full import _read_push, entry
from xuanming.nodes.scene import vector


async def team_reply(client, method, call_id, **fields):
    protocol = client.protocol
    name = f"ClientPlayerTeam{method}"
    route = protocol.routes[protocol.message_ids[name]]
    return await client.rpc(route.message_id, call_id, protocol.message(route.request_type, **fields))


async def team_call(client, method, call_id, **fields):
    reply = await team_reply(client, method, call_id, **fields)
    assert not reply.error_message.id, (method, reply.error_message.id)
    return reply.team



async def move(client, method, **fields):
    p = client.protocol
    await client.rpc(p.message_ids[f"SceneMovementClientPlayer{method}"], 0, p.message(f"{method}C2S", **fields))


async def verify_team(leader_session, leader, gate, runtime, ingress):
    protocol = leader_session.client.protocol
    async with login_full.exercise_login(protocol, gate.gate.info.client_endpoint, ingress=ingress,
                                         account=login_full.TEAM_ACCOUNT) as follower_session:
        client = follower_session.client
        await login_full.enter_game(client, follower_session.player_id)
        follower = await entry(client, runtime, follower_session.player_id)
        world = runtime.scene.locations[leader.player_id]
        assert world.nav and runtime.scene.locations[follower.player_id] is world
        _, actors = await _read_push(client, "SceneSceneClientPlayerNotifyActorListCreate", timeout=5)
        assert leader.entity in {actor.entity for actor in actors.actor_list}
        leader_client = leader_session.client
        team = await team_call(leader_client, "CreateTeam", 201, request_id=str(uuid4()))
        assert team.leader_id == leader.player_id
        application = await team_reply(client, "ApplyJoinTeam", 202, target_player_id=leader.player_id,
                                       request_id=str(uuid4()))
        assert not application.error_message.id, ("ApplyJoinTeam", application.error_message.id)
        joined = await team_call(leader_client, "HandleApplication", 203, applicant_id=follower.player_id,
                                 approve=True, expected_team_id=team.team_id, request_id=str(uuid4()))
        assert {member.player_id for member in joined.members} == {leader.player_id, follower.player_id}
        await move(client, "TeamFollow", follow=True)
        _, state = await _read_push(client, "SceneMovementClientPlayerNotifyTeamFollowState", timeout=12)
        assert state.driven and state.leader_entity == leader.entity and state.slot == 1

        start = leader.transform.position
        direction = next((dx, dy) for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1))
                         if world.nav.validate_move(start, (start[0] + 7*dx, start[1] + 7*dy, start[2]))[0])
        await move(leader_client, "MoveStart", start_location=vector(start),
                   velocity=vector((100.0*direction[0], 100.0*direction[1], 0.0)), input_seq=1)
        _, leader_motion = await _read_push(client, "SceneMovementClientPlayerNotifyActorMove", timeout=5)
        assert leader_motion.entity == leader.entity
        assert math.hypot(leader_motion.velocity.x, leader_motion.velocity.y) == 10
        async with asyncio.timeout(5):
            while math.dist(start, follower.transform.position) < 2:
                await asyncio.sleep(0.03)
        gap = math.dist(leader.transform.position, follower.transform.position)
        assert 1.35 <= gap <= 1.55, gap
        async with asyncio.timeout(5):
            while True:
                _, attributes = await _read_push(client, "ScenePlayerSyncSyncBaseAttribute", timeout=5)
                if attributes.entity_id == leader.player_id and attributes.HasField("velocity"):
                    assert math.hypot(attributes.velocity.x, attributes.velocity.y) == 10
                    break
        await move(leader_client, "MoveStop", end_location=vector(leader.transform.position), input_seq=2)
        async with asyncio.timeout(5):
            while follower.motion.velocity != (0.0, 0.0, 0.0):
                await asyncio.sleep(0.03)
        # A driven follower must ignore its own movement, even if the input is valid.
        # A subsequent Scene RPC fences the one-way movement on the same client lane.
        forged_rotation = (12345.0, 23456.0, 34567.0)
        await move(client, "MoveSync", location=vector(follower.transform.position),
                   rotation=vector(forged_rotation), input_seq=0xFFFFFFFF)
        bag = await client.rpc(protocol.message_ids["SceneBagClientPlayerGetBag"], 214,
                               protocol.message("GetBagRequest"))
        assert not bag.error_message.id
        assert follower.transform.rotation != forged_rotation
        await move(client, "TeamFollow", follow=False)
        async with asyncio.timeout(5):
            while True:
                _, state = await _read_push(client, "SceneMovementClientPlayerNotifyTeamFollowState", timeout=5)
                if not state.driven:
                    assert state.reason == 1
                    break
        stop = leader.transform.position
        far = (stop[0] + 2000, stop[1], stop[2])
        await move(leader_client, "MoveSync", location=vector(far), input_seq=3)
        _, correction = await _read_push(leader_client, "SceneMovementClientPlayerNotifyMoveAck", timeout=5)
        assert correction.input_seq == 3 and abs(correction.server_location.x - far[0]) > 0.5
        target = next(candidate for candidate in runtime.scene.worlds.values()
                      if candidate is not world and not candidate.info.mirror_config_id
                      and not candidate.info.dungeon_config_id and not candidate.draining)
        fields = ({"scene_config_id": target.info.scene_config_id}
                  if target.info.scene_config_id != world.info.scene_config_id else {"scene_id": target.info.scene_id})
        switch = await leader_client.rpc(protocol.message_ids["SceneSceneClientPlayerEnterScene"], 210,
                                         protocol.message("EnterSceneC2SRequest", scene_info=fields))
        assert not switch.error_message.id
        await asyncio.gather(entry(leader_client, runtime, leader.player_id), entry(client, runtime, follower.player_id))
        current = runtime.scene.locations[leader.player_id]
        assert current is not world and runtime.scene.locations[follower.player_id] is current
        left = await team_call(client, "LeaveTeam", 211, expected_team_id=team.team_id,
                               request_id=str(uuid4()))
        assert not left.team_id
        await team_call(leader_client, "InviteToTeam", 212, target_player_id=follower.player_id,
                        expected_team_id=team.team_id, request_id=str(uuid4()))
        invited = await team_call(client, "RespondInvite", 213, team_id=team.team_id,
                                  accept=True, request_id=str(uuid4()))
        assert {member.player_id for member in invited.members} == {leader.player_id, follower.player_id}
        result = await team_call(leader_client, "DisbandTeam", 204, expected_team_id=team.team_id,
                                 request_id=str(uuid4()))
        assert not result.team_id
        print("PASS two native accounts joined a Go Team; Python followed, clamped movement and released control.",
              flush=True)
        return {"team_id": team.team_id, "native_apply_join_tip": application.error_message.id,
                "isolated_application_fixture": False, "native_invitation_accepted": True,
                "native_approval_committed": True, "members": 2, "slot": 1, "speed_cap": 10,
                "gap": round(gap, 3), "off_mesh_ack": True, "client_opt_out": True, "same_node_follow_scene": True,
                "actor_list_create": True, "actor_velocity_delta": True}
