import asyncio
from types import SimpleNamespace

import pytest

from test_scene_assets import _scene
from xuanming.data import ConfigTables, PlayerRowCodec, StoreUnavailable
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_skills import SceneSkills, initialize_skill_list
from xuanming.protocol import Protocol


@pytest.mark.parametrize("skill_id,lost_ack", ((1101, False), (1106, True)))
def test_scene_skill_catalog_and_partial_upgrade_commit_together(tmp_path, skill_id, lost_ack):
    async def exercise():
        p, scene, seed_assets, store = await _scene(tmp_path)
        published = []
        assets = scene.assets = SceneAssets(
            scene, seed_assets.tables, {}, emit_audit=published.append, allocate_tx_id=lambda: 100,
        )
        service = SceneSkills(scene, assets.tables)
        service.register()
        attribute_updates = []
        scene.attributes = SimpleNamespace(
            notify=lambda player: attribute_updates.append(scene.rows.assemble(player.snapshot).currency.values[0]),
        )
        try:
            player = scene.locations[1].players[1]
            data = scene.rows.assemble(player.snapshot)
            setattr(data.uint32_pb_component, "class", 1)
            data.level_component.level = 5
            data.currency.values[0] = 2000
            initialize_skill_list(assets.tables, data)
            assert {entry.skill_table_id for entry in data.skill_list.skill_list} == {1101, 1106}
            await scene.commit_player_data(player, data)

            original = service.list_skills(None, player, None, None)
            assert original.HasField("skill_list") and original.currency_balance == 2000
            assert original.HasField("loadout") and len(original.loadout.schemes) == 3
            owned_ranks = {e.skill_table_id: e.rank for e in original.skill_list.skill_list}
            assert owned_ranks[skill_id] == 1
            assert any(entry.skill_table_id == 10002 for entry in original.catalog)

            message_id = p.message_ids["SceneSkillClientPlayerUpgradeSkill"]
            body = p.message("UpgradeSkillRequest", skill_table_id=skill_id, expected_rank=1, count=10)

            async def upgrade(call_id):
                request = p.message(
                    "ProcessClientPlayerMessageRequest", session_id=9, player_id=1,
                    message_content=p.content(message_id, call_id, body=body.SerializeToString()),
                )
                reply = (await scene.process(request, "gate")).message_content
                assert not reply.error_message.id and reply.id == call_id
                return p.parse("UpgradeSkillResponse", reply.serialized_message)

            store.armed = lost_ack
            response = await upgrade(1)
            if lost_ack:
                assert response.error_message.id == 1003 and response.state.currency_balance == 2000
                assert player.pending_save is not None
                await asyncio.sleep(0)
                assert not published and not attribute_updates
            else:
                assert not response.error_message.id and response.upgraded_count == 2
                after_ranks = {e.skill_table_id: e.rank for e in response.state.skill_list.skill_list}
                assert response.stop_tip_id == 7010 and after_ranks[skill_id] == 3
                assert response.state.currency_balance == 680

            retry = await upgrade(2)
            assert retry.error_message.id == 7008 and retry.state.currency_balance == 680
            retry_ranks = {e.skill_table_id: e.rank for e in retry.state.skill_list.skill_list}
            assert retry_ranks[skill_id] == 3 and player.pending_save is None
            assert attribute_updates == ([680] if skill_id == 1106 else [])
            await assets.close()
            entry, = published
            assert (entry.tx_id, entry.tx_type, entry.correlation_id) == (100, 15, skill_id)
            assert (entry.currency_type, entry.currency_delta, entry.balance_before, entry.balance_after) == (
                0, -1320, 2000, 680,
            )
            assert (entry.from_player, entry.to_player, entry.zone_id) == (1, 0, 1)
        finally:
            await service.close()
            await scene.close()
            await assets.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("race", (None, "save_timeout", "caster_relocates", "target_relocates"))
def test_realtime_release_resolves_opaque_target_and_commits_guarded_damage(race):
    p = Protocol()
    tables = ConfigTables()
    codec = PlayerRowCodec(p)
    players = {}
    for player_id, entity, position in ((1, 100, (0.0, 0.0, 0.0)), (2, 101, (0.0, 40.0, 0.0))):
        data = p.message("player_database", player_id=player_id, uint32_pb_component={"class": 1},
                         level_component={"level": 5}, derived_attributes_component={"health": 150, "mana": 200})
        initialize_skill_list(tables, data)
        snapshot = SimpleNamespace(player_id=player_id, schema_gen=1, rows=codec.project(data))
        players[player_id] = SimpleNamespace(player_id=player_id, entity=entity, snapshot=snapshot, disconnect_at=None,
                                             transform=SimpleNamespace(position=position, rotation=(0.0, 0.0, 0.0)))
    world = SimpleNamespace(players=players, nearby=lambda player: set(players) - {player.player_id})
    pushes = []
    writes = []
    pending = []
    engagements = []
    dirty = []
    locations = {1: world, 2: world}

    def complete(player, data, on_committed):
        writes.append((player.player_id, data.derived_attributes_component.health))
        player.snapshot = SimpleNamespace(player_id=player.player_id, schema_gen=1, rows=codec.project(data))
        on_committed(player.snapshot)

    async def commit(player, data, *, on_committed):
        if race in ("save_timeout", "target_relocates"):
            pending.append((player, data, on_committed))
            raise StoreUnavailable("save acknowledgment unavailable")
        complete(player, data, on_committed)

    async def flush(_):
        if race == "caster_relocates":
            locations[1] = SimpleNamespace(players={1: players[1]})

    scene = SimpleNamespace(protocol=p, rows=codec, mutable=lambda _: True, battle=None, worlds={1: world},
                            locations=locations, _lock=lambda _: asyncio.Lock(),
                            combat_engagement=SimpleNamespace(
                                record_damage=lambda caster, target: engagements.append((caster.entity, target.entity)),
                                on_death=lambda target: engagements.append(("death", target.entity)),
                            ), actor_sync=SimpleNamespace(mark=lambda actor, *fields: dirty.append((actor.entity, fields))),
                            _push=lambda player, name, message: pushes.append((player.player_id, name, message)),
                            flush_player_rows=flush, commit_player_data=commit)
    service = SceneSkills(scene, tables)
    service.skills[1101].effect.append(15)

    async def exercise():
        request = p.message("ReleaseSkillRequest", skill_table_id=1101, target_id=2)
        assert service.casts.release(world, players[1], None, request).error_message.id == 7001
        request.target_id = 101
        assert not service.casts.release(world, players[1], None, request).error_message.id
        await asyncio.gather(*tuple(service.casts._tasks))
        if race in ("save_timeout", "target_relocates"):
            assert not writes and not service.casts.buffs.entries(players[2])
            if race == "target_relocates":
                locations[2] = SimpleNamespace(players={2: players[2]})
            complete(*pending.pop())
        if race == "caster_relocates":
            assert not writes
        else:
            assert len(writes) == 1 and writes[0][0] == 2 and writes[0][1] < 150
        buffs = [entry.row.id for entry in service.casts.buffs.entries(players[2])]
        assert buffs == ([] if race in ("caster_relocates", "target_relocates") else [15])
        assert pushes[0][0:2] == (1, "SceneClientPlayerCommonNotifySelfActorStateFlags")
        assert (pushes[0][2].action_state_mask, pushes[0][2].combat_state_mask, pushes[0][2].state_seq) == (1, 0, 1)
        skill_used = next(msg for pid, name, msg in pushes if pid == 2 and name == "SceneSkillClientPlayerNotifySkillUsed")
        assert list(skill_used.target_entity) == [101]
        assert players[1].transform.rotation == (0.0, 0.0, 90.0)
        assert (100, ("transform",)) in dirty
        if race in ("caster_relocates", "target_relocates"):
            assert not engagements
        else:
            assert engagements == [(100, 101)] + ([("death", 101)] if writes[0][1] == 0 else [])
        await service.close()

    asyncio.run(exercise())
