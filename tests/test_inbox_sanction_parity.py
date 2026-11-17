import asyncio

import pytest

from test_scene_assets import _scene
from xuanming.nodes.player_inbox import GM, PAYLOAD_INVALID, SceneInbox


class BatchPeer:
    def __init__(self, protocol, items, *, fail_ack_once=False):
        self.protocol = protocol
        self.items = items
        self.fail_ack_once = fail_ack_once
        self.fetches = 0
        self.acks = []

    async def fetch(self, player, domain, correlation, cursor):
        self.fetches += 1
        return self.protocol.message("player_inbox.FetchPlayerInboxResponse", player_id=player.player_id,
                                     domain=domain, correlation_id=correlation, fence=3, items=self.items)

    async def ack(self, player, domain, correlation, reports):
        self.acks.append([(item.seq, item.outcome, item.reason) for item in reports])
        if self.fail_ack_once:
            self.fail_ack_once = False
            raise OSError("temporarily unavailable")
        return self.protocol.message("player_inbox.AckPlayerInboxResponse", player_id=player.player_id,
                                     domain=domain, correlation_id=correlation, fence=3,
                                     results=[{"seq": item.seq, "result": 1, "state": item.outcome + 1}
                                              for item in reports])


@pytest.mark.parametrize("kind,payload", [
    (1, b"\x80"),  # malformed GmCurrencyGainPayload
    (5, b""),  # item configuration id zero
    (3, b"\x08\xff\x07"),  # unknown currency plus invalid amount zero
    (3, b"\x08\xff\x07\x10\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01"),  # amount > INT64_MAX
])
def test_malformed_sanction_rejects_then_applies_the_next_entry(tmp_path, kind, payload):
    async def run():
        protocol, scene, assets, _ = await _scene(tmp_path)
        good = protocol.message("player_inbox.GmCurrencyDebtPayload", owe_amount=10).SerializeToString()
        peer = BatchPeer(protocol, [
            {"seq": 1, "request_id": 41, "msg_type": kind, "payload": payload},
            {"seq": 2, "request_id": 42, "msg_type": 3, "payload": good},
        ])
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets)
        player = scene.locations[1].players[1]
        try:
            await inbox._poll(1, GM)
            assert (1, GM) not in inbox._halted
            assert peer.acks == [[(1, 2, PAYLOAD_INVALID), (2, 1, 0)]]
            data = scene.rows.assemble(player.snapshot)
            assert data.currency.debts[0].owed == 10
            assert [(x.seq, x.outcome, x.reason) for x in data.inbox_ledger.cursors[0].recent] == [
                (1, 2, PAYLOAD_INVALID), (2, 1, 0)]
        finally:
            await scene.close()

    asyncio.run(run())


def test_halted_domain_retries_ack_of_already_durable_prefix_without_refetch(tmp_path):
    async def run():
        protocol, scene, assets, _ = await _scene(tmp_path)
        good = protocol.message("player_inbox.GmCurrencyDebtPayload", owe_amount=10).SerializeToString()
        peer = BatchPeer(protocol, [
            {"seq": 1, "request_id": 41, "msg_type": 3, "payload": good},
            {"seq": 2, "request_id": 42, "msg_type": 999},
        ], fail_ack_once=True)
        now = [100.0]
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets, clock=lambda: now[0])
        player = scene.locations[1].players[1]
        try:
            await inbox._poll(1, GM)
            assert (1, GM) in inbox._halted and (1, GM) in inbox._pending_ack
            assert peer.fetches == 1 and peer.acks == [[(1, 1, 0)]]
            data = scene.rows.assemble(player.snapshot)
            assert data.currency.debts[0].owed == 10 and data.inbox_ledger.cursors[0].decided_through_seq == 1
            now[0] = 106
            inbox.tick()
            await asyncio.gather(*tuple(inbox._jobs.values()))
            assert peer.fetches == 1 and peer.acks == [[(1, 1, 0)], [(1, 1, 0)]]
            assert (1, GM) in inbox._halted and (1, GM) not in inbox._pending_ack
            assert (1, GM) not in inbox._due
        finally:
            await scene.close()

    asyncio.run(run())


def test_future_sanction_payload_is_not_misclassified_as_invalid(tmp_path):
    async def run():
        protocol, scene, assets, _ = await _scene(tmp_path)
        payload = protocol.message("player_inbox.GmCurrencyDebtPayload", owe_amount=10)
        payload.MergeFromString(b"\xa0\x06\x01")  # Future field 100, valid protobuf encoding.
        peer = BatchPeer(protocol, [{"seq": 1, "request_id": 41, "msg_type": 3,
                                     "payload": payload.SerializeToString()}])
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets)
        player = scene.locations[1].players[1]
        try:
            before = dict(player.snapshot.rows)
            await inbox._poll(1, GM)
            assert (1, GM) in inbox._halted and not peer.acks
            assert player.snapshot.rows == before
        finally:
            await scene.close()

    asyncio.run(run())


def test_replay_mismatch_drops_the_whole_ack_batch(tmp_path):
    async def run():
        protocol, scene, assets, _ = await _scene(tmp_path)
        peer = BatchPeer(protocol, [
            {"seq": 1, "request_id": 41, "msg_type": 1},
            {"seq": 2, "request_id": 999, "msg_type": 1},
        ])
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets)
        player = scene.locations[1].players[1]
        try:
            data = scene.rows.assemble(player.snapshot)
            data.inbox_ledger.cursors.add(domain=GM, decided_through_seq=2, recent=[
                {"seq": 1, "request_id": 41, "msg_type": 1, "outcome": 1},
                {"seq": 2, "request_id": 42, "msg_type": 1, "outcome": 1},
            ])
            await scene.commit_player_data(player, data)
            await inbox._poll(1, GM)
            assert (1, GM) in inbox._halted and peer.acks == []
            assert (1, GM) not in inbox._pending_ack
        finally:
            await scene.close()

    asyncio.run(run())


def test_zero_echo_fields_are_accepted_for_fetch_and_ack(tmp_path):
    async def run():
        protocol, scene, assets, _ = await _scene(tmp_path)

        class ZeroEchoPeer(BatchPeer):
            async def fetch(self, player, domain, correlation, cursor):
                self.fetches += 1
                return self.protocol.message("player_inbox.FetchPlayerInboxResponse", fence=3, items=self.items)

            async def ack(self, player, domain, correlation, reports):
                self.acks.append([(x.seq, x.outcome, x.reason) for x in reports])
                return self.protocol.message("player_inbox.AckPlayerInboxResponse", fence=3,
                                             results=[{"seq": x.seq, "result": 1, "state": x.outcome + 1}
                                                      for x in reports])

        good = protocol.message("player_inbox.GmCurrencyDebtPayload", owe_amount=10).SerializeToString()
        peer = ZeroEchoPeer(protocol, [{"seq": 1, "request_id": 41, "msg_type": 3, "payload": good}])
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets)
        try:
            await inbox._poll(1, GM)
            assert peer.acks == [[(1, 1, 0)]] and (1, GM) not in inbox._halted
        finally:
            await scene.close()

    asyncio.run(run())


@pytest.mark.parametrize("fence,tip", [(5, 0), (6, 0), (7, 0), (3, 1005)])
def test_halted_prefix_discards_ack_for_stale_epoch_or_definitive_protocol_error(tmp_path, fence, tip):
    async def run():
        protocol, scene, assets, _ = await _scene(tmp_path)

        class FencedPeer(BatchPeer):
            async def ack(self, player, domain, correlation, reports):
                self.acks.append([(x.seq, x.outcome, x.reason) for x in reports])
                return self.protocol.message("player_inbox.AckPlayerInboxResponse", player_id=player.player_id,
                                             domain=domain, correlation_id=correlation, fence=fence,
                                             error_message={"id": tip})

        good = protocol.message("player_inbox.GmCurrencyDebtPayload", owe_amount=10).SerializeToString()
        peer = FencedPeer(protocol, [{"seq": 1, "request_id": 41, "msg_type": 3, "payload": good},
                                     {"seq": 2, "request_id": 42, "msg_type": 999}])
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets, clock=lambda: 100)
        try:
            await inbox._poll(1, GM)
            assert (1, GM) in inbox._halted and (1, GM) not in inbox._pending_ack
            assert peer.acks == [[(1, 1, 0)]] and (1, GM) not in inbox._due
        finally:
            await scene.close()

    asyncio.run(run())


def test_no_stream_fence_ignores_items_without_changing_state(tmp_path):
    async def run():
        protocol, scene, assets, _ = await _scene(tmp_path)

        class NoStreamPeer(BatchPeer):
            async def fetch(self, player, domain, correlation, cursor):
                return self.protocol.message("player_inbox.FetchPlayerInboxResponse", fence=1, items=self.items)

        peer = NoStreamPeer(protocol, [{"seq": 1, "request_id": 41, "msg_type": 3}])
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets)
        player = scene.locations[1].players[1]
        try:
            before = dict(player.snapshot.rows)
            await inbox._poll(1, GM)
            assert (1, GM) not in inbox._halted and not peer.acks and player.snapshot.rows == before
        finally:
            await scene.close()

    asyncio.run(run())


@pytest.mark.parametrize("reply_kind", ["stale", "mismatched_echo"])
def test_stale_fetch_or_mismatched_echo_defers_without_halting(tmp_path, reply_kind):
    async def run():
        protocol, scene, assets, _ = await _scene(tmp_path)

        class ReplyPeer(BatchPeer):
            async def fetch(self, player, domain, correlation, cursor):
                return self.protocol.message("player_inbox.FetchPlayerInboxResponse", player_id=player.player_id,
                                             domain=domain, correlation_id=correlation + (reply_kind == "mismatched_echo"),
                                             fence=5 if reply_kind == "stale" else 3)

        peer = ReplyPeer(protocol, [])
        inbox = scene.inbox = SceneInbox(scene, peer, None, assets, clock=lambda: 100)
        try:
            await inbox._poll(1, GM)
            assert (1, GM) not in inbox._halted
            assert inbox._due[1, GM] == (160 if reply_kind == "stale" else 105)
        finally:
            await scene.close()

    asyncio.run(run())
