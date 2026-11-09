import asyncio

import pytest

from test_scene_assets import _scene


@pytest.mark.parametrize("unknown,lost_ack", ((False, False), (False, True), (True, False)))
def test_retrieve_preserves_oversized_existing_stack_and_identity(tmp_path, unknown, lost_ack):
    async def exercise():
        protocol, scene, assets, store = await _scene(tmp_path)
        assets.register_client_handlers()
        item = next(row for row in assets.tables["Item"].rows() if row.max_stack_size > 2 and not row.equip_kind)
        limit = item.max_stack_size
        source_size = 2 * limit + 3
        player = scene.locations[1].players[1]
        try:
            data = scene.rows.assemble(player.snapshot)
            bag = data.bag_component
            bag.ClearField("items")
            bag.capacities.extend([2, 200, 10, 200])
            bag.items.add(bag_type=0, pos=0, instance={
                "item_id": 10, "config_id": item.id, "size": limit - 2, "acquire_seq": 5,
            })
            source = bag.items.add(bag_type=3, pos=0).instance
            source.item_id, source.config_id, source.size, source.acquire_seq = 40, item.id, source_size, 90
            if unknown:
                source.MergeFromString(b"\x98\x06\x01")
            await scene.commit_player_data(player, data)

            message_id = protocol.message_ids["SceneBagClientPlayerRetrieveTemporaryItems"]
            route = protocol.routes[message_id]

            async def retrieve(call_id):
                request = protocol.message(
                    "ProcessClientPlayerMessageRequest", player_id=1, session_id=9,
                    message_content=protocol.content(message_id, call_id, body=protocol.message(
                        route.request_type, item_id=40,
                    ).SerializeToString()),
                )
                response = (await scene.process(request, "gate")).message_content
                assert response.id == call_id and not response.error_message.id
                return protocol.parse(route.response_type, response.serialized_message)

            store.armed = lost_ack
            response = await retrieve(1)
            if lost_ack:
                assert response.error_message.id == 1003 and player.pending_save is not None
                assert len(assets.entries(scene.rows.assemble(player.snapshot).bag_component, 3)) == 1
            else:
                assert not response.error_message.id and not response.temporary.items
            # Retrying a completed move cannot merge the source stack twice.
            response = await retrieve(2)
            assert response.error_message.id == 6015 and player.pending_save is None
            saved = scene.rows.assemble(player.snapshot)
            entries = assets.entries(saved.bag_component, 0)
            by_id = {instance.item_id: instance for _, instance in entries}
            assert not assets.entries(saved.bag_component, 3)
            assert set(by_id) == {10, 40}
            assert by_id[10].size == (limit - 2 if unknown else limit)
            assert by_id[40].size == source_size - (0 if unknown else 2)
            assert by_id[40].size > limit and by_id[40].acquire_seq == 6
            assert sum(instance.size for instance in by_id.values()) == source_size + limit - 2
            if unknown:
                assert b"\x98\x06\x01" in by_id[40].SerializeToString()
            expected = saved.bag_component.SerializeToString(deterministic=True)
        finally:
            await scene.close()

        restored = await store.load_and_claim(1, b"temporary-verifier")
        try:
            assert scene.rows.assemble(restored).bag_component.SerializeToString(deterministic=True) == expected
        finally:
            await store.store.release(restored)

    asyncio.run(exercise())


def test_retrieve_rolls_back_partial_fill_when_remaining_stack_has_no_slot(tmp_path):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        assets.register_client_handlers()
        item = next(row for row in assets.tables["Item"].rows() if row.max_stack_size > 2 and not row.equip_kind)
        player = scene.locations[1].players[1]
        try:
            data = scene.rows.assemble(player.snapshot)
            bag = data.bag_component
            bag.ClearField("items")
            bag.capacities.extend([1, 200, 10, 200])
            bag.items.add(bag_type=0, pos=0, instance={
                "item_id": 10, "config_id": item.id, "size": item.max_stack_size - 2, "acquire_seq": 5,
            })
            bag.items.add(bag_type=3, pos=0, instance={
                "item_id": 40, "config_id": item.id, "size": 2 * item.max_stack_size + 3, "acquire_seq": 90,
            })
            await scene.commit_player_data(player, data)
            before = data.SerializeToString(deterministic=True)
            error, changed = assets.retrieve_temporary(data, 40, False)
            assert (error, changed) == (6016, False)
            assert data.SerializeToString(deterministic=True) == before

            message_id = protocol.message_ids["SceneBagClientPlayerRetrieveTemporaryItems"]
            route = protocol.routes[message_id]
            response = await scene.process(protocol.message(
                "ProcessClientPlayerMessageRequest", player_id=1, session_id=9,
                message_content=protocol.content(message_id, 1, body=protocol.message(
                    route.request_type, item_id=40,
                ).SerializeToString()),
            ), "gate")
            assert not response.message_content.error_message.id
            reply = protocol.parse(route.response_type, response.message_content.serialized_message)
            assert reply.error_message.id == 6016 and player.pending_save is None
            assert scene.rows.assemble(player.snapshot).bag_component == bag
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_retrieve_rejects_multiple_units_in_nonstackable_instance(tmp_path):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        try:
            item = next(row for row in assets.tables["Item"].rows() if row.max_stack_size == 1)
            data = protocol.message("player_database", player_id=1)
            data.bag_component.items.add(bag_type=3, pos=0, instance={
                "item_id": 40, "config_id": item.id, "size": 2,
            })
            before = data.SerializeToString(deterministic=True)
            assert assets.retrieve_temporary(data, 40, False) == (1002, False)
            assert data.SerializeToString(deterministic=True) == before
        finally:
            await scene.close()

    asyncio.run(exercise())
