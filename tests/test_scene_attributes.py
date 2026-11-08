import asyncio

from test_monopoly import TOKEN, _enter, _settings

from xuanming.runtime import Runtime


async def _invoke(client, call_id, rpc_name, **fields):
    protocol = client.protocol
    message_id = protocol.message_ids[rpc_name]
    request = protocol.message(protocol.routes[message_id].request_type, **fields)
    return await client.rpc(message_id, call_id, request)


async def _rpc(client, call_id, method, **fields):
    return await _invoke(client, call_id, f"SceneAttributeClientPlayer{method}", **fields)


async def _gold(client, call_id, amount=None):
    protocol = client.protocol
    if amount is not None:
        return await client.rpc(protocol.message_ids["SceneCurrencyClientPlayerGmAddCurrency"], call_id,
                                protocol.message("GmAddCurrencyRequest", amount=amount))
    response = await client.rpc(protocol.message_ids["SceneCurrencyClientPlayerGetCurrencyList"], call_id,
                                protocol.message("GetCurrencyListRequest"))
    return response.currency.values[0] if response.currency.values else 0


def test_attribute_points_and_scheme_spend_survive_restart(tmp_path, monkeypatch):
    monkeypatch.setenv("GATE_RUN_MODE", "test")
    monkeypatch.setenv("SCENE_RUN_MODE", "test")

    async def exercise():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                opening = await _rpc(client, 3, "GetAttributePanel")
                assert not opening.error_message.id and opening.panel.level == 1
                assert [row.pool_id for row in opening.panel.pools] == [1]
                assert opening.panel.pools[0].remaining == 5
                assert len(opening.panel.combat) == 15
                assert [(row.scheme_id, row.name) for row in opening.panel.schemes] == [(1, "方案一")]
                denied = await _rpc(client, 4, "AllocateAttributePoints", pool_id=1, allocated={103: 6})
                assert denied.error_message.id == 25003
                suggested = await _rpc(client, 5, "AutoAllocateAttributePoints", pool_id=1)
                assert dict(suggested.suggested) == {101: 1, 102: 0, 103: 4, 104: 0}
                allocated = await _rpc(client, 6, "AllocateAttributePoints", pool_id=1, allocated=suggested.suggested)
                assert not allocated.error_message.id and allocated.panel.pools[0].remaining == 0
                assert allocated.panel.derived.physical_attack >= opening.panel.derived.physical_attack

                poor = await _rpc(client, 7, "CreateAttributeScheme", client_request_id=2)
                assert poor.error_message.id == 25012
                assert not (await _gold(client, 8, 1700)).error_message.id
                created = await _rpc(client, 9, "CreateAttributeScheme", client_request_id=2)
                assert not created.error_message.id and created.scheme_id == 2 and not created.replayed
                assert created.panel.schemes[1].name == "方案二"
                switched = await _rpc(client, 10, "SwitchAttributeScheme", scheme_id=2)
                assert not switched.error_message.id and switched.panel.active_scheme_id == 2
                assert switched.panel.pools[0].remaining == 5
                assert (await _rpc(client, 11, "SwitchAttributeScheme", scheme_id=1)).error_message.id == 25008
                renamed = await _rpc(client, 12, "RenameAttributeScheme", scheme_id=2, name="日常方案")
                assert renamed.panel.schemes[1].name == "日常方案"
                mismatch = await _rpc(client, 13, "CreateAttributeScheme", client_request_id=2, name="其它")
                assert mismatch.error_message.id == 1005 and await _gold(client, 14) == 700

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, restored_id):
                assert restored_id == player_id
                replay = await _rpc(client, 3, "CreateAttributeScheme", client_request_id=2)
                assert not replay.error_message.id and replay.replayed and replay.scheme_id == 2
                assert replay.panel.active_scheme_id == 2 and replay.panel.schemes[1].name == "日常方案"
                assert replay.panel.switch_cooldown_until == switched.panel.switch_cooldown_until
                assert await _gold(client, 4) == 700

    asyncio.run(exercise())


def test_attribute_ineffective_pool_reset_keeps_foreign_points_and_gold(tmp_path, monkeypatch):
    monkeypatch.setenv("GATE_RUN_MODE", "test")
    monkeypatch.setenv("SCENE_RUN_MODE", "test")

    async def exercise():
        async with Runtime(_settings(tmp_path), "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                raised = await _rpc(client, 3, "GmSetPlayerLevel", level=35)
                assert not raised.error_message.id and raised.panel.level == 35
                assert raised.panel.pools[0].total == 175 and raised.panel.pools[0].reset_cost_gold == 500
                assert not (await _gold(client, 4, 800)).error_message.id
                scene = runtime.scene
                player = scene.locations[player_id].players[player_id]
                async with scene._lock(player_id):
                    data = scene.rows.assemble(player.snapshot)
                    data.attribute_component.schemes[0].allocated.update({103: 500, 199999: 10})
                    await scene.commit_player_data(player, data)

                bad = await _rpc(client, 5, "GetAttributePanel")
                assert bad.panel.pools[0].remaining == bad.panel.pools[0].reset_cost_gold == 0
                strength = next(item for item in bad.panel.dimensions if item.dimension_id == 103)
                assert strength.allocated == 500 and strength.value < 500
                reset = await _rpc(client, 6, "ResetAttributePoints", pool_id=1)
                assert not reset.error_message.id and reset.panel.pools[0].remaining == 165
                assert reset.panel.derived.health == bad.panel.derived.health
                assert await _gold(client, 7) == 800
                saved = scene.rows.assemble(player.snapshot)
                assert dict(saved.attribute_component.schemes[0].allocated) == {199999: 10}
                assert (await _rpc(client, 8, "ResetAttributePoints", pool_id=1)).error_message.id == 25014

                allocated = await _rpc(client, 9, "AllocateAttributePoints", pool_id=1, allocated={103: 5})
                assert not allocated.error_message.id
                paid = await _rpc(client, 10, "ResetAttributePoints", pool_id=1)
                assert not paid.error_message.id and await _gold(client, 11) == 300

    asyncio.run(exercise())


def test_equipment_and_passive_skill_push_the_confirmed_attribute_panel(tmp_path, monkeypatch):
    monkeypatch.setenv("GATE_RUN_MODE", "test")
    monkeypatch.setenv("SCENE_RUN_MODE", "test")

    async def exercise():
        async with Runtime(_settings(tmp_path), "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, _):
                protocol = client.protocol
                push_id = protocol.message_ids["SceneAttributeClientPlayerNotifyAttributePanelChanged"]

                async def panel_push():
                    message = await client.content(push_id)
                    return protocol.parse("AttributePanelChangedS2C", message.serialized_message).panel

                raised = await _rpc(client, 3, "GmSetPlayerLevel", level=5)
                assert await panel_push() == raised.panel
                assert not (await _gold(client, 4, 2000)).error_message.id
                granted = await _invoke(client, 5, "SceneBagClientPlayerGmGrantItem", config_id=1101, count=1)
                assert not granted.error_message.id
                item_id = next(item.item_id for item in granted.bag.items if item.config_id == 1101)
                worn = await _invoke(client, 6, "SceneBagClientPlayerEquipItem", item_id=item_id)
                assert not worn.error_message.id
                equipped_panel = await panel_push()
                assert equipped_panel == (await _rpc(client, 7, "GetAttributePanel")).panel
                assert equipped_panel.derived != raised.panel.derived

                upgraded = await _invoke(client, 8, "SceneSkillClientPlayerUpgradeSkill",
                                         skill_table_id=1106, expected_rank=1, count=1)
                assert not upgraded.error_message.id and upgraded.upgraded_count == 1
                assert await panel_push() == (await _rpc(client, 9, "GetAttributePanel")).panel

    asyncio.run(exercise())
