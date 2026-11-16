import asyncio

import pytest

from test_scene_assets import _scene
from xuanming.nodes.battle_settlement import BattleSettlement
from xuanming.nodes.player_inbox import BATTLE, LocalInbox, SceneInbox
from xuanming.nodes.scene_mission import Missions


@pytest.mark.parametrize("block", ("player", "global", "none"))
def test_battle_currency_unknown_debt_halts_even_if_gain_blocked(tmp_path, block):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        player = scene.locations[1].players[1]
        peer = LocalInbox(protocol, tmp_path / "inbox.sqlite3")
        settlement = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        inbox = scene.inbox = SceneInbox(scene, peer, settlement, assets, clock=lambda: 100)
        try:
            data = scene.rows.assemble(player.snapshot)
            debt = data.currency.debts.add(currency_type=0, owed=9)
            debt.MergeFromString(b"\xa0\x06\x01")  # Unknown future field 100.
            original_debt = debt.SerializeToString(deterministic=True)
            if block == "player":
                data.currency.blocked_types.append(0)
            elif block == "global":
                assets.global_blocks["currency"].add(0)
            await scene.commit_player_data(player, data)
            await peer.enqueue([protocol.message(
                "BattleSettlementData", player_id=1, battle_id=42, outcome=1, health=100,
                currency_gains=[{"currency_type": 0, "amount": 10}, {"currency_type": 1, "amount": 20}],
            )])

            await inbox._poll(1, BATTLE)
            saved = scene.rows.assemble(player.snapshot)
            assert saved.currency.debts[0].SerializeToString(deterministic=True) == original_debt
            assert saved.currency.values[0] == 100
            assert saved.currency.values[1] == 0
            assert not saved.battle_results.entries
            assert (1, BATTLE) in inbox._halted
            assert await asyncio.to_thread(peer._fetch, 1)
        finally:
            await scene.close()

    asyncio.run(exercise())
