import asyncio

import pytest

from test_scene_assets import _scene
from xuanming.nodes.battle_settlement import BattleSettlement
from xuanming.nodes.player_inbox import BATTLE, GM, LocalInbox, SceneInbox
from xuanming.nodes.scene_mission import Missions


@pytest.mark.parametrize("lost_ack", (False, True))
def test_confirmed_temporary_retrieval_wakes_only_bag_full_settlement(tmp_path, lost_ack):
    async def exercise():
        protocol, scene, assets, store = await _scene(tmp_path)
        assets.register_client_handlers()
        player = scene.locations[1].players[1]
        items = [row for row in assets.tables["Item"].rows() if row.max_stack_size > 2 and not row.equip_kind]
        held, reward = items[:2]
        peer = LocalInbox(protocol, tmp_path / "inbox.sqlite3")
        settlement = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        inbox = scene.inbox = SceneInbox(scene, peer, settlement, assets, clock=lambda: 100)
        try:
            data = scene.rows.assemble(player.snapshot)
            bag = data.bag_component
            bag.ClearField("items")
            bag.capacities.extend([1, 200, 10, 1])
            bag.items.add(bag_type=0, pos=0, instance={
                "item_id": 10, "config_id": held.id, "size": held.max_stack_size - 1, "acquire_seq": 1,
            })
            bag.items.add(bag_type=3, pos=0, instance={
                "item_id": 40, "config_id": held.id, "size": 1, "acquire_seq": 1,
            })
            await scene.commit_player_data(player, data)
            await peer.enqueue([protocol.message(
                "BattleSettlementData", player_id=1, battle_id=42, outcome=1, health=100,
                items_gained=[{"item_table_id": reward.id, "count": 1}],
            )])
            inbox._due[1, GM] = 500
            await inbox._poll(1, BATTLE)
            assert inbox._due[1, BATTLE] == 102 and await asyncio.to_thread(peer._fetch, 1)
            assert not scene.rows.assemble(player.snapshot).battle_results.entries
            assert inbox._bag_deferred[1, BATTLE] is player
            inbox._halted.add((1, BATTLE))
            inbox.on_bag_space_freed(player)
            assert inbox._due[1, BATTLE] == 102
            inbox._halted.clear()

            calls = []
            wake = inbox.on_bag_space_freed

            def track_wake(current):
                calls.append(current)
                wake(current)

            inbox.on_bag_space_freed = track_wake
            message_id = protocol.message_ids["SceneBagClientPlayerRetrieveTemporaryItems"]
            route = protocol.routes[message_id]

            async def retrieve(call_id):
                response = await scene.process(protocol.message(
                    "ProcessClientPlayerMessageRequest", player_id=1, session_id=9,
                    message_content=protocol.content(message_id, call_id, body=protocol.message(
                        route.request_type, item_id=40,
                    ).SerializeToString()),
                ), "gate")
                assert not response.message_content.error_message.id
                return protocol.parse(route.response_type, response.message_content.serialized_message)

            store.armed = lost_ack
            response = await retrieve(1)
            if lost_ack:
                assert response.error_message.id == 1003 and not calls
                assert inbox._due[1, BATTLE] == 102
                async with scene._lock(1):
                    await scene.flush_player_rows(player)
            else:
                assert not response.error_message.id
            assert calls == [player]
            assert inbox._due[1, BATTLE] == 100 and inbox._due[1, GM] == 500
            assert not inbox._jobs
            # Repeating the already committed move does not trigger another wake.
            assert (await retrieve(2)).error_message.id == 6015 and calls == [player]
            inbox.tick()
            await asyncio.gather(*tuple(inbox._jobs.values()))
            assert not await asyncio.to_thread(peer._fetch, 1)
            saved = scene.rows.assemble(player.snapshot)
            assert [entry.battle_id for entry in saved.battle_results.entries] == [42]
            assert [(item.config_id, item.size) for _, item in assets.entries(saved.bag_component, 3)] == [(reward.id, 1)]
            assert (1, BATTLE) not in inbox._bag_deferred and inbox._due[1, GM] == 500
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_free_space_does_not_wake_other_executor_failures_or_former_player(tmp_path):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        player = scene.locations[1].players[1]
        settlement = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        inbox = scene.inbox = SceneInbox(scene, None, settlement, assets, clock=lambda: 100)
        try:
            invalid = protocol.message(
                "BattleSettlementData", player_id=1, battle_id=42, outcome=1,
                currency_gains=[{"currency_type": 999, "amount": 1}],
            )
            item = protocol.message("player_inbox.PlayerInboxItem", seq=1, request_id=42, msg_type=4,
                                    payload=invalid.SerializeToString())
            assert await inbox._consume(player, BATTLE, item) is None
            inbox._due[1, BATTLE] = 500
            inbox.on_bag_space_freed(player)
            assert inbox._due[1, BATTLE] == 500 and not inbox._bag_deferred
            inbox._bag_deferred[1, BATTLE] = player
            scene.locations[1].players[1] = object()
            inbox.on_bag_space_freed(player)
            assert inbox._due[1, BATTLE] == 500
            scene.locations[1].players[1] = player
            inbox.departed(player)
            assert not inbox._bag_deferred
        finally:
            scene.locations[1].players[1] = player
            await scene.close()

    asyncio.run(exercise())
