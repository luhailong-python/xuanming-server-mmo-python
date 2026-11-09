import asyncio

import pytest

from test_scene_assets import _scene
from xuanming.nodes.player_inbox import BATTLE, GM, SceneInbox


class _DebtPeer:
    def __init__(self, protocol):
        self.protocol = protocol
        self.fetches = 0
        self.acks = []

    async def fetch(self, player, domain, correlation, cursor):
        self.fetches += 1
        payload = self.protocol.message("player_inbox.GmCurrencyDebtPayload", owe_amount=10)
        return self.protocol.message(
            "player_inbox.FetchPlayerInboxResponse", player_id=player.player_id, domain=domain,
            correlation_id=correlation, fence=3,
            items=[{"seq": 2, "request_id": 42, "msg_type": 3, "payload": payload.SerializeToString()}],
        )

    async def ack(self, player, domain, correlation, reports):
        self.acks.extend(reports)
        return self.protocol.message(
            "player_inbox.AckPlayerInboxResponse", player_id=player.player_id, domain=domain,
            correlation_id=correlation, fence=3,
            results=[{"seq": row.seq, "result": 1, "state": row.outcome + 1} for row in reports],
        )


def test_ledger_accepts_zero_replay_metadata_and_preserves_future_domain(tmp_path):
    async def exercise():
        protocol, scene, assets, store = await _scene(tmp_path)
        peer = _DebtPeer(protocol)
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets)
        player = scene.locations[1].players[1]
        try:
            data = scene.rows.assemble(player.snapshot)
            data.inbox_ledger.cursors.add(domain=GM, decided_through_seq=1, recent=[{"seq": 1, "outcome": 1}])
            future = data.inbox_ledger.cursors.add(
                domain=77, decided_through_seq=3, recent=[{"seq": 3, "outcome": 2}],
            )
            future.MergeFromString(b"\x98\x06\x01")
            carried = future.SerializeToString(deterministic=True)
            await scene.commit_player_data(player, data)
            await inbox._poll(1, GM)
            assert (1, GM) not in inbox._halted
            assert peer.fetches == len(peer.acks) == 1
            saved = scene.rows.assemble(player.snapshot)
            assert [(debt.owed, debt.paid) for debt in saved.currency.debts] == [(10, 0)]
            cursor = next(row for row in saved.inbox_ledger.cursors if row.domain == GM)
            assert [(row.seq, row.request_id, row.msg_type) for row in cursor.recent] == [(1, 0, 0), (2, 42, 3)]
            # Replayed delivery acknowledges the existing receipt without adding debt again.
            await inbox._poll(1, GM)
            repeated = scene.rows.assemble(player.snapshot)
            assert repeated.currency == saved.currency and repeated.inbox_ledger == saved.inbox_ledger
            assert peer.fetches == len(peer.acks) == 2
        finally:
            await scene.close()

        snapshot = await store.load_and_claim(1, b"inbox-verifier")
        try:
            restored = scene.rows.assemble(snapshot)
            assert restored.currency.debts[0].owed == 10
            future = next(row for row in restored.inbox_ledger.cursors if row.domain == 77)
            assert future.SerializeToString(deterministic=True) == carried
        finally:
            await store.store.release(snapshot)

    asyncio.run(exercise())


@pytest.mark.parametrize("corruption", ("domain_zero", "duplicate_domain", "seq_zero", "seq_order",
                                      "beyond_watermark", "unknown_outcome", "over_capacity"))
def test_foreign_domain_corruption_blocks_fetch_and_effects_in_all_domains(tmp_path, corruption):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        peer = _DebtPeer(protocol)
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets)
        player = scene.locations[1].players[1]
        try:
            data = scene.rows.assemble(player.snapshot)
            ledger = data.inbox_ledger
            cursor = ledger.cursors.add(
                domain=77, decided_through_seq=2, recent=[{"seq": 2, "outcome": 1, "request_id": 10}],
            )
            if corruption == "domain_zero":
                cursor.domain = 0
            elif corruption == "duplicate_domain":
                ledger.cursors.add().CopyFrom(cursor)
            elif corruption == "seq_zero":
                cursor.recent[0].seq = 0
            elif corruption == "seq_order":
                cursor.recent.add(seq=1, outcome=1)
            elif corruption == "beyond_watermark":
                cursor.recent[0].seq = 3
            elif corruption == "unknown_outcome":
                cursor.recent[0].outcome = 4
            else:
                cursor.ClearField("recent")
                cursor.decided_through_seq = 33
                for seq in range(1, 34):
                    cursor.recent.add(seq=seq, outcome=1)
            await scene.commit_player_data(player, data)
            before = dict(player.snapshot.rows)
            for domain in (GM, BATTLE):
                await inbox._poll(1, domain)
                assert (1, domain) in inbox._halted
            assert peer.fetches == 0 and not peer.acks
            assert player.snapshot.rows == before and player.pending_save is None
        finally:
            await scene.close()

    asyncio.run(exercise())
