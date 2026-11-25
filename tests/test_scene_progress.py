import asyncio
import time
from dataclasses import replace

from xuanming.grpc_runtime import GrpcClient
from xuanming.nodes.player_inbox import BATTLE
from xuanming.runtime import Runtime, grpc_address
from xuanming.settings import Settings
from xuanming.smoke import connect


def test_client_mission_dungeon_and_durable_battle_inbox(tmp_path):
    async def run():
        defaults = Settings()
        settings = replace(
            defaults, database=tmp_path / "players.db",
            gate=replace(defaults.gate, port=0),
            scene=replace(defaults.scene, port=0, grpc_port=0),
            battle=replace(defaults.battle, port=0, grpc_port=0),
        )
        async with Runtime(settings, "local", dev_token="test-only") as runtime:
            p, scene, gate = runtime.protocol, runtime.scene, runtime.gate
            rpc_id = p.message_ids
            address = grpc_address(scene.info.grpc_endpoint.ip, scene.info.grpc_endpoint.port)
            control = GrpcClient(address)
            try:
                create = p.message("CreateSceneRequest", scene_id=1700, config_id=17,
                                   dungeon_config_id=1, creator_ids=[1], placement_generation=10)
                data = await control.call("/scene_node.SceneNodeGrpc/CreateScene", create.SerializeToString())
                assert p.parse("CreateSceneResponse", data).scene_info.dungeon_config_id == 1
                old_destroy = p.message("DestroySceneRequest", scene_id=1700, placement_generation=9)
                await control.call("/scene_node.SceneNodeGrpc/DestroyScene", old_destroy.SerializeToString())
                assert 1700 in scene.worlds

                async with connect(p, gate.info.endpoint) as client:
                    await client.send(gate.issue_ticket())
                    assert (await client.receive("ClientTokenVerifyResponse")).success
                    login = await client.rpc(rpc_id["ClientPlayerLoginLogin"], 1, p.message(
                        "loginpb.LoginRequest", account="local", password="test-only",
                    ))
                    assert not login.error_message.id
                    enter = await client.rpc(rpc_id["ClientPlayerLoginEnterGame"], 2, p.message(
                        "loginpb.EnterGameRequest", player_id=1, request_id="progress-test-enter",
                    ))
                    assert enter.player_id == 1
                    await client.content(rpc_id["SceneSceneClientPlayerNotifyEnterScene"])
                    result = await client.rpc(rpc_id["SceneMissionClientPlayerAcceptMission"], 3, p.message(
                        "MissionActionRequest", mission_id=4,
                    ))
                    assert not result.error_message.id
                    assert next(m for m in result.missions if m.mission_id == 4).status == 1
                    result = await client.rpc(rpc_id["SceneSceneClientPlayerEnterScene"], 4, p.message(
                        "EnterSceneC2SRequest", scene_info={"scene_id": 1700},
                    ))
                    assert not result.error_message.id
                    notification = await client.content(rpc_id["SceneSceneClientPlayerNotifyEnterScene"])
                    assert p.parse("EnterSceneS2C", notification.serialized_message).scene_info.scene_id == 1700

                    now = time.time_ns() // 1_000_000
                    prepare = p.message("PrepareBattleRequest", player_id=1, battle_id=42,
                                        deadline_ms=now + 60000, prepare_deadline_ms=now + 30000)
                    data = await control.call("/scene_node.SceneNodeGrpc/PrepareBattle", prepare.SerializeToString())
                    ready = p.parse("PrepareBattleResponse", data)
                    assert not ready.error_message.id
                    assert ready.table_fingerprint == "becf67dbc74146f15c4604e4461d1d53"
                    assert ready.snapshot.max_health >= ready.snapshot.base_attributes.health > 0
                    await scene.battle.confirm(p.message("BattleConfirmedEvent", player_id=1, battle_id=42,
                                                         deadline_ms=now + 60000))
                    settlement = p.message(
                        "BattleSettlementData", player_id=1, battle_id=42, outcome=1, health=0, is_dead=True,
                        total_rounds=2, defeated_monsters=[{"monster_config_id": 1, "count": 1}],
                        currency_gains=[{"currency_type": 0, "amount": 700}], battle_config_id=1, match_mode=4,
                    )
                    await runtime.local_inbox.enqueue([settlement, settlement])
                    scene.inbox.wake(1, BATTLE)
                    content = await client.content(rpc_id["BattleClientPlayerNotifyBattleEnd"])
                    assert p.parse("BattleEndS2C", content.serialized_message).battle_id == 42

                    async with asyncio.timeout(4):
                        while await asyncio.to_thread(runtime.local_inbox._fetch, 1):
                            await asyncio.sleep(.02)
                    assert not scene.battle.frozen(1)
                    player = scene.locations[1].players[1]
                    saved = scene.rows.assemble(player.snapshot)
                    assert saved.currency.values[0] == 700
                    assert saved.derived_attributes_component.health == ready.snapshot.max_health
                    assert 4 in saved.mission_component.scopes[0].completed_mission_ids
                    assert 4 not in saved.mission_component.scopes[0].claimable_mission_ids
                    assert saved.bag_component.items
                    assert len(saved.battle_results.entries) == 1
                    assert saved.inbox_ledger.cursors[0].recent[0].request_id == 42
                    assert saved.inbox_ledger.cursors[0].recent[0].outcome == 1

                    duplicate = p.message(
                        "player_inbox.PlayerInboxItem", seq=200, request_id=42, msg_type=4,
                        payload=settlement.SerializeToString(),
                    )
                    async with scene._lock(1):
                        ack = await scene.inbox._consume(player, BATTLE, duplicate)
                        assert ack.outcome == 1
                    after = scene.rows.assemble(player.snapshot)
                    assert after.currency == saved.currency and after.bag_component == saved.bag_component

                    ack = await client.rpc(rpc_id["SceneBattleResultClientPlayerAckBattleResults"], 5, p.message(
                        "AckBattleResultsRequest", battle_ids=[42, 42, 999],
                    ))
                    assert not ack.error_message.id
                    history = await client.rpc(rpc_id["SceneBattleResultClientPlayerGetRecentBattleResults"], 6,
                                               p.message("GetRecentBattleResultsRequest"))
                    assert len(history.results) == 1 and history.results[0].seen
                    await client.rpc(rpc_id["ClientPlayerLoginLeaveGame"], 7, p.message("loginpb.LeaveGameRequest"))
                persisted = await scene.store.load_and_claim(1, b"test-verifier")
                assert scene.rows.assemble(persisted).battle_results.entries[0].seen
                await scene.store.release(persisted)
            finally:
                await control.close()

    asyncio.run(run())


def test_dungeon_placement_tombstone_fences_late_create(tmp_path):
    async def run():
        defaults = Settings()
        settings = replace(
            defaults, database=tmp_path / "placement.db",
            gate=replace(defaults.gate, port=0), scene=replace(defaults.scene, port=0, grpc_port=0),
            battle=replace(defaults.battle, port=0, grpc_port=0),
        )
        async with Runtime(settings, "local", dev_token="test-only") as runtime:
            p, scene = runtime.protocol, runtime.scene
            scene.destroy(p.message("DestroySceneRequest", scene_id=54, placement_generation=2))
            create = p.message("CreateSceneRequest", scene_id=54, config_id=3, placement_generation=2)
            assert not scene.create(create).HasField("scene_info")
            create.placement_generation = 3
            assert scene.create(create).scene_info.scene_id == 54
            scene.destroy(p.message("DestroySceneRequest", scene_id=54, placement_generation=2))
            assert 54 in scene.worlds

    asyncio.run(run())
