import asyncio
from dataclasses import replace

from xuanming.nodes.scene import Transform
from xuanming.runtime import Runtime
from xuanming.settings import Settings
from xuanming.smoke import connect


def test_gate_scene_npc_target_and_stealth_visibility(tmp_path):
    async def exercise():
        settings = Settings()
        settings = replace(
            settings, database=tmp_path / "players.sqlite3",
            gate=replace(settings.gate, port=0), scene=replace(settings.scene, port=0, grpc_port=0),
            battle=replace(settings.battle, port=0, grpc_port=0),
        )
        async with Runtime(settings, "local", dev_token="test-scene-npc") as runtime:
            scene, gate, protocol = runtime.scene, runtime.gate, runtime.protocol
            ids = protocol.message_ids
            world = scene.worlds[1]
            assert not world.npcs

            def attach(target_world, position):
                health = protocol.message("BaseAttributesComp", health=200)
                return scene.spawn_npc(target_world, Transform(position), health)

            spawn = scene._spawn_position(world)
            bystanders = (attach(world, spawn), attach(world, spawn))
            async with connect(protocol, gate.info.endpoint) as client:
                await client.send(gate.issue_ticket())
                assert (await client.receive("ClientTokenVerifyResponse")).success
                login = await client.rpc(ids["ClientPlayerLoginLogin"], 1, protocol.message(
                    "loginpb.LoginRequest", account="npc-test", password="test-scene-npc",
                ))
                player_id = login.players[0].player.player_id
                entered = await client.rpc(ids["ClientPlayerLoginEnterGame"], 2, protocol.message(
                    "loginpb.EnterGameRequest", player_id=player_id, request_id="npc-test",
                ))
                assert entered.player_id == player_id
                await client.content(ids["SceneSceneClientPlayerNotifyEnterScene"])
                create = ids["SceneSceneClientPlayerNotifyActorCreate"]
                destroy = ids["SceneSceneClientPlayerNotifyActorDestroy"]
                list_create = ids["SceneSceneClientPlayerNotifyActorListCreate"]
                list_destroy = ids["SceneSceneClientPlayerNotifyActorListDestroy"]
                assert scene.locations[player_id] is world
                player = world.players[player_id]
                own_actor = protocol.parse("ActorCreateS2C", (await client.content(create)).serialized_message)
                assert own_actor.guid == player_id and own_actor.entity == player.entity
                assert own_actor.class_id == 1 and own_actor.gender == 1
                assert player.entity >> 32 == scene.info.node_id
                shown = protocol.parse("ActorListCreateS2C", (await client.content(list_create)).serialized_message)
                assert [(actor.actor_type, actor.entity, actor.guid) for actor in shown.actor_list] == [
                    (2, bystander.entity, 0) for bystander in bystanders
                ]
                npc = attach(world, player.transform.position)
                shown = protocol.parse("ActorListCreateS2C", (await client.content(list_create)).serialized_message)
                assert [(actor.actor_type, actor.entity, actor.guid) for actor in shown.actor_list] == [(2, npc.entity, 0)]
                scene.create(protocol.message("CreateSceneRequest", scene_id=2, config_id=2))
                remote = attach(scene.worlds[2], player.transform.position)
                release = ids["SceneSkillClientPlayerReleaseSkill"]
                request = protocol.message("ReleaseSkillRequest", skill_table_id=1101, target_id=remote.entity)
                assert (await client.rpc(release, 3, request)).error_message.id == 7001
                request.target_id = npc.entity
                assert not (await client.rpc(release, 4, request)).error_message.id
                async with asyncio.timeout(3):
                    while npc.attributes.health == 200:
                        await asyncio.sleep(0.01)
                assert all(actor.attributes.health == 200 for actor in (*bystanders, remote))
                assert 0 <= npc.attributes.health < 200

                # The native engine supports this effect type without an active stealth table row.
                row_type = type(next(iter(scene.buffs.rows.values())))
                scene.buffs.rows[999_991] = row_type(id=999_991, buff_type=33, duration=1)
                _, buff_id = scene.buffs.add(npc, 999_991, player.entity)
                hidden = protocol.parse("ActorListDestroyS2C", (await client.content(list_destroy)).serialized_message)
                assert list(hidden.entity) == [npc.entity] and not tuple(scene.watchers(world, npc))
                scene.buffs.remove(npc, buff_id)
                visible = protocol.parse("ActorListCreateS2C", (await client.content(list_create)).serialized_message)
                assert [actor.entity for actor in visible.actor_list] == [npc.entity]

                scene.despawn_npc(npc)
                removed = protocol.parse("ActorDestroyS2C", (await client.content(destroy)).serialized_message)
                assert removed.entity == npc.entity and npc.entity not in world.actors

    asyncio.run(exercise())
