import asyncio

import pytest

from test_monopoly import TOKEN, _enter, _settings
from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.scene_mission import _has_bit, _set_bit
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime


def test_completed_mission_progress_does_not_block_same_type_after_reload(tmp_path):
    async def exercise():
        settings = _settings(tmp_path)
        protocol = Protocol()
        tables = ConfigTables()
        bit4 = tables["Mission"].bit_index(4)
        codec = PlayerRowCodec(protocol)
        store = SqlitePlayerStore(settings.database)
        snapshot = await store.load_and_claim(1, b"mission-fixture")
        data = protocol.message("player_database", player_id=1, persist_schema_gen=1)
        state = data.mission_component
        scope = state.scopes.add(scope=0)
        scope.mission_list.complete_missions = _set_bit(b"", bit4)
        mission = scope.mission_list.missions[4]
        mission.id = 4
        mission.progress.append(1)
        mission.MergeFromString(b"\x98\x06\x01")
        snapshot = await store.commit(snapshot, codec.project(data), force=True)
        await store.release(snapshot)

        async def rpc(client, call_id, method, **fields):
            p = client.protocol
            message_id = p.message_ids["SceneMissionClientPlayer" + method]
            return await client.rpc(message_id, call_id, p.message(p.routes[message_id].request_type, **fields))

        def mission(response, mission_id):
            return next(row for row in response.missions if row.mission_id == mission_id and not row.scope)

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                listed = await rpc(client, 3, "GetMissionList")
                assert mission(listed, 4).status == 2
                assert mission(listed, 12).can_accept
                accepted = await rpc(client, 4, "AcceptMission", mission_id=12)
                assert not accepted.error_message.id and mission(accepted, 12).status == 1
                # An active mission still reserves its type.
                refused = await rpc(client, 5, "AcceptMission", mission_id=7)
                assert refused.error_message.id == 5000
                repeated = await rpc(client, 6, "AcceptMission", mission_id=12)
                assert repeated.error_message.id == 5004
                claimed = await rpc(client, 7, "ClaimMissionReward", mission_id=4)
                assert claimed.error_message.id == 12000
                player = runtime.scene.locations[player_id].players[player_id]
                saved = runtime.scene.rows.assemble(player.snapshot)
                assert not saved.bag_component.items
                scope = next(scope for scope in saved.mission_component.scopes if not scope.scope)
                carried = scope.mission_list.missions[4].SerializeToString()
                assert _has_bit(scope.mission_list.complete_missions, bit4)
                assert b"\x98\x06\x01" in carried

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, restored_id):
                assert restored_id == player_id
                listed = await rpc(client, 3, "GetMissionList")
                assert mission(listed, 4).status == 2
                assert mission(listed, 12).status == 1 and not mission(listed, 7).can_accept
                player = runtime.scene.locations[player_id].players[player_id]
                saved = runtime.scene.rows.assemble(player.snapshot)
                scope = next(scope for scope in saved.mission_component.scopes if not scope.scope)
                assert scope.mission_list.missions[4].SerializeToString() == carried

    asyncio.run(exercise())
