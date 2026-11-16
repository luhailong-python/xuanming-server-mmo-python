import asyncio

import pytest

from test_scene_assets import _scene
from xuanming.nodes.battle_settlement import BattleSettlement
from xuanming.nodes.player_inbox import BATTLE, LocalInbox, SceneInbox
from xuanming.nodes.scene_mission import Missions


@pytest.mark.parametrize("effect", ({}, {"health": 1}, {"gold_gain": 1},
                                    {"items_gained": [{"item_table_id": 1, "count": 1}]}))
def test_void_settlement_is_effect_free_and_releases_hold_atomically(tmp_path, effect):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        player = scene.locations[1].players[1]
        peer = LocalInbox(protocol, tmp_path / "void-inbox.sqlite3")
        settlement = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        inbox = scene.inbox = SceneInbox(scene, peer, settlement, assets, clock=lambda: 100)
        try:
            data = scene.rows.assemble(player.snapshot)
            data.derived_attributes_component.health = 50
            data.item_holds.holds.add(hold_id=40, kind=1).lines.add(config_id=123, count=2)
            await scene.commit_player_data(player, data)
            await peer.enqueue([protocol.message("BattleSettlementData", player_id=1, battle_id=40,
                                                  outcome=4, match_mode=1, **effect)])
            await inbox._poll(1, BATTLE)
            saved = scene.rows.assemble(player.snapshot)
            assert saved.derived_attributes_component.health == 50
            if effect:
                assert [hold.hold_id for hold in saved.item_holds.holds] == [40]
                assert not saved.battle_results.entries
                assert await asyncio.to_thread(peer._fetch, 1)
            else:
                assert not saved.item_holds.holds
                assert saved.battle_results.entries[0].outcome == 4
                assert not await asyncio.to_thread(peer._fetch, 1)
        finally:
            await scene.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("missing", ("gained", "consumed", "pet"))
def test_settlement_with_missing_config_defers_without_committing(tmp_path, missing):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        player = scene.locations[1].players[1]
        peer = LocalInbox(protocol, tmp_path / "missing-inbox.sqlite3")
        settlement = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        inbox = scene.inbox = SceneInbox(scene, peer, settlement, assets, clock=lambda: 100)
        try:
            data = scene.rows.assemble(player.snapshot)
            payload = {"player_id": 1, "battle_id": 40, "outcome": 1, "health": 100}
            if missing == "pet":
                data.pet_component.pets.add(pet_id=100, pet_table_id=0xffffffff, health=20)
                payload["pets"] = [{"pet_id": 100, "health": 1}]
            else:
                payload[f"items_{missing}"] = [{"item_table_id": 0xffffffff, "count": 1}]
            await scene.commit_player_data(player, data)
            before = dict(player.snapshot.rows)
            await peer.enqueue([protocol.message("BattleSettlementData", **payload)])
            await inbox._poll(1, BATTLE)
            assert player.snapshot.rows == before
            assert await asyncio.to_thread(peer._fetch, 1)
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_void_existing_result_still_releases_its_hold(tmp_path):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        player = scene.locations[1].players[1]
        service = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        try:
            data = scene.rows.assemble(player.snapshot)
            data.battle_results.entries.add(battle_id=40, outcome=4, result_seq=1)
            data.item_holds.holds.add(hold_id=40, kind=1).lines.add(config_id=123, count=2)
            assert await service.apply(player, data, protocol.message(
                "BattleSettlementData", player_id=1, battle_id=40, outcome=4,
            ))
            assert not data.item_holds.holds and len(data.battle_results.entries) == 1
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_missing_pet_pool_skips_pet_effect(tmp_path, monkeypatch):
    from xuanming.nodes import battle_settlement

    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        player = scene.locations[1].players[1]
        service = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        try:
            data = scene.rows.assemble(player.snapshot)
            pet_row = assets.tables["Pet"].rows()[0]
            data.pet_component.pets.add(pet_id=100, pet_table_id=pet_row.id, health=20)
            monkeypatch.setattr(battle_settlement, "pet_pool", lambda tables: None)
            assert await service.apply(player, data, protocol.message(
                "BattleSettlementData", player_id=1, battle_id=40, outcome=1, health=100,
                pets=[{"pet_id": 100, "health": 1}],
            ))
            assert data.pet_component.pets[0].health == 20
        finally:
            await scene.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("lines,halted", [
    ([{"currency_type": 9999, "amount": 1}], True),
    ([{"currency_type": 9999, "amount": 1}, {"currency_type": 9999, "amount": 2}], False),
    ([{"currency_type": 9999, "amount": 1}, {"currency_type": 0, "amount": 1 << 63}], False),
])
def test_currency_version_skew_defers_malformed_before_unknown(tmp_path, lines, halted):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        peer = LocalInbox(protocol, tmp_path / "currency-inbox.sqlite3")
        settlement = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        inbox = scene.inbox = SceneInbox(scene, peer, settlement, assets, clock=lambda: 100)
        try:
            await peer.enqueue([protocol.message("BattleSettlementData", player_id=1, battle_id=40,
                                                  outcome=1, health=100, currency_gains=lines)])
            await inbox._poll(1, BATTLE)
            assert ((1, BATTLE) in inbox._halted) == halted
            assert await asyncio.to_thread(peer._fetch, 1)
            assert not scene.rows.assemble(scene.locations[1].players[1].snapshot).battle_results.entries
        finally:
            await scene.close()

    asyncio.run(exercise())
