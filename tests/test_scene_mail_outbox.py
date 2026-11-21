import asyncio
from types import SimpleNamespace

from test_scene_assets import _scene
from xuanming.data import PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.battle_settlement import BattleSettlement
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.battle_control import verify_service_call
from xuanming.nodes.player_inbox import BATTLE, LocalInbox, SceneInbox
from xuanming.nodes.scene_mail_outbox import MailSender, SceneMailOutbox, split_items
from xuanming.nodes.scene_mission import Missions
from xuanming.protocol import Protocol


def test_mail_split_respects_attachment_limits():
    batches = split_items({1: 5000, **{i: 1 for i in range(2, 23)}})
    assert batches[0] == {1: 4096} and batches[1][1] == 904
    assert all(len(batch) <= 16 and sum(batch.values()) <= 4096 for batch in batches)
    assert sum(batch.get(1, 0) for batch in batches) == 5000
    assert split_items({1: 4096 * 33}) is None


def test_only_committed_mail_is_retried_with_the_same_id(tmp_path):
    async def exercise():
        protocol = Protocol()
        scene = SceneNode(protocol, protocol.message("NodeInfo", node_id=5, zone_id=1, node_uuid="test"),
                          SqlitePlayerStore(tmp_path / "mail.sqlite"), PlayerRowCodec(protocol), lambda *args: None)
        scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))
        await scene.enter(protocol.message("PlayerEnterGameNodeRequest", scene_id=1, session_id=1,
                                           player_id=1, owner_revision=1, login_generation=1), "gate")
        player = scene.worlds[1].players[1]
        calls, now = [], [10.0]

        class Sender:
            def secret(self):
                return b"x" * 32

            async def __call__(self, player_id, entry):
                calls.append((player_id, entry.SerializeToString(deterministic=True)))
                if len(calls) == 1:
                    raise OSError("reply lost")
                return SimpleNamespace(error_message=SimpleNamespace(id=0), mail_id=9)

            async def close(self):
                pass

        async def allocate():
            return 120

        outbox = SceneMailOutbox(scene, allocate, Sender(), clock=lambda: now[0])
        try:
            data = scene.rows.assemble(player.snapshot)
            assert await outbox.prepare(data, 20, {1: 2})
            outbox.tick()
            assert not outbox._jobs and not calls
            await scene.commit_player_data(player, data)
            now[0] += 1
            outbox.tick()
            await asyncio.gather(*tuple(outbox._jobs.values()))
            assert len(scene.rows.assemble(player.snapshot).mail_outbox.entries) == 1
            now[0] += 1
            outbox.tick()
            await asyncio.gather(*tuple(outbox._jobs.values()))
            assert len(calls) == 2 and calls[0] == calls[1]
            assert not scene.rows.assemble(player.snapshot).mail_outbox.entries
        finally:
            await outbox.close()
            await scene.close()

    asyncio.run(exercise())


def test_mail_sender_signs_existing_entry_without_changing_its_identity(monkeypatch):
    async def exercise():
        protocol = Protocol()
        key = b"battle-secret-01234567890123456789"
        monkeypatch.setenv("MAIL_PRODUCER_SECRET_BATTLE", key.decode())
        node = protocol.message("NodeInfo", node_type=8, protocol_type=1, node_uuid="mail-a",
                                grpc_endpoint={"ip": "127.0.0.1", "port": 10001})

        class Directory:
            async def candidates(self, node_type, zone):
                assert node_type == 8 and zone is None
                return [node]

        class Client:
            async def call(self, path, data):
                request = protocol.parse("mailpb.SendMailRequest", data)
                assert path == "/mailpb.MailAdmin/SendPersonalMail"
                assert (request.sender_kind, request.request_id, request.recipient_player_id) == (5, 13, 7)
                assert [(item.config_id, item.count) for item in request.attachments.items] == [(11, 3)]
                assert verify_service_call(path, request, "battle", request.auth.timestamp_ms, key)
                return protocol.message("mailpb.SendMailResponse", mail_id=55).SerializeToString()

            async def close(self):
                pass

        sender = MailSender(protocol, Directory())
        sender.clients["127.0.0.1:10001"] = Client()
        entry = protocol.message("MailOutboxEntry", request_id=13, source=1, source_id=10,
                                 title="奖励", body="正文", items=[{"config_id": 11, "count": 3}])
        try:
            assert (await sender(7, entry)).mail_id == 55
        finally:
            await sender.close()

    asyncio.run(exercise())


def test_full_bags_commit_outbox_with_settlement_before_sending(tmp_path):
    async def exercise():
        protocol, scene, assets, store = await _scene(tmp_path)
        player = scene.locations[1].players[1]
        items = [row for row in assets.tables["Item"].rows() if row.max_stack_size > 2 and not row.equip_kind]
        held, reward = items[:2]
        sent = []
        notifications = []
        scene._push = lambda player, name, message: notifications.append((name, message))

        class Sender:
            def secret(self):
                return b"x" * 32

            async def __call__(self, player_id, entry):
                sent.append((player_id, entry.request_id, [(item.config_id, item.count) for item in entry.items]))
                return SimpleNamespace(error_message=SimpleNamespace(id=0), mail_id=44)

            async def close(self):
                pass

        async def allocate():
            return 12345

        outbox = scene.mail_outbox = SceneMailOutbox(scene, allocate, Sender(), clock=lambda: 100)
        peer = LocalInbox(protocol, tmp_path / "inbox.sqlite3")
        settlement = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        inbox = scene.inbox = SceneInbox(scene, peer, settlement, assets, clock=lambda: 100)
        try:
            data = scene.rows.assemble(player.snapshot)
            bag = data.bag_component
            bag.ClearField("items")
            bag.capacities.extend([1, 200, 10, 1])
            bag.items.add(bag_type=0, pos=0, instance={
                "item_id": 10, "config_id": held.id, "size": held.max_stack_size, "acquire_seq": 1,
            })
            bag.items.add(bag_type=3, pos=0, instance={
                "item_id": 40, "config_id": held.id, "size": held.max_stack_size, "acquire_seq": 1,
            })
            await scene.commit_player_data(player, data)
            await peer.enqueue([protocol.message("BattleSettlementData", player_id=1, battle_id=42, outcome=1,
                                                  health=100, items_gained=[{"item_table_id": reward.id, "count": 3}])])
            store.armed = True
            await inbox._poll(1, BATTLE)
            outbox.tick()
            assert not outbox._jobs and not sent
            assert not scene.rows.assemble(player.snapshot).battle_results.entries

            async with scene._lock(1):
                await scene.flush_player_rows(player)
            saved = scene.rows.assemble(player.snapshot)
            assert saved.battle_results.entries[0].items_mailed
            assert saved.mail_outbox.entries[0].request_id == 12345
            outbox._next_tick = 0
            outbox.tick()
            await asyncio.gather(*tuple(outbox._jobs.values()))
            assert sent == [(1, 12345, [(reward.id, 3)])]
            saved = scene.rows.assemble(player.snapshot)
            assert not saved.mail_outbox.entries and saved.battle_results.entries[0].items_mailed
            await inbox._poll(1, BATTLE)
            assert not await asyncio.to_thread(peer._fetch, 1) and len(sent) == 1
            ended = [message for name, message in notifications if name == "BattleClientPlayerNotifyBattleEnd"]
            assert ended and ended[-1].items_mailed
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_mail_sender_closes_retired_clients_on_endpoint_change(monkeypatch):
    async def exercise():
        protocol = Protocol()
        monkeypatch.setenv("MAIL_PRODUCER_SECRET_BATTLE", "battle-secret-01234567890123456789")
        ports = [10001]
        closed = []

        class Directory:
            async def candidates(self, node_type, zone):
                return [protocol.message("NodeInfo", node_type=8, protocol_type=1, node_uuid=f"mail-{p}",
                                         grpc_endpoint={"ip": "127.0.0.1", "port": p}) for p in ports]

        class Client:
            def __init__(self, address, **_kwargs):
                self.address = address

            async def call(self, _path, _data):
                return protocol.message("mailpb.SendMailResponse", mail_id=55).SerializeToString()

            async def close(self):
                closed.append(self.address)

        monkeypatch.setattr("xuanming.nodes.scene_mail_outbox.GrpcClient", Client)
        sender = MailSender(protocol, Directory())
        entry = protocol.message("MailOutboxEntry", request_id=13, source=1, source_id=10,
                                 title="奖励", body="正文", items=[{"config_id": 11, "count": 3}])
        try:
            assert (await sender(7, entry)).mail_id == 55
            assert list(sender.clients) == ["127.0.0.1:10001"]
            ports[:] = [10002]
            assert (await sender(7, entry)).mail_id == 55
            assert list(sender.clients) == ["127.0.0.1:10002"] and closed == ["127.0.0.1:10001"]
        finally:
            await sender.close()
        assert closed == ["127.0.0.1:10001", "127.0.0.1:10002"]

    asyncio.run(exercise())
