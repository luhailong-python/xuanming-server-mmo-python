import asyncio
import hmac
import json
import time
from itertools import count
from types import SimpleNamespace

from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore, StoreUnavailable
from xuanming.data.asset_anomaly import AnomalyThreshold, AssetGain
from xuanming.data.asset_audit import AssetAudit
from xuanming.data.asset_inventory import BAG_FULL, BAG_INSUFFICIENT, FEATURE_UNAVAILABLE, AssetInventory, release_item_hold
from xuanming.data.asset_ledger import Outcome, canonical, classify, find_stream, record, valid
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.protocol import Protocol

_SECRETS = {name: (name * 32).encode() for name in ("guild", "trade", "mail", "data_service")}


def test_item_holds_protect_consumption_and_allow_own_battle():
    protocol, tables = Protocol(), ConfigTables()
    inventory = AssetInventory(protocol, tables)
    config = next(row.id for row in tables["Item"].rows() if row.battle_usable)
    data = protocol.message("player_database")
    data.bag_component.items.add(item_uuid=1, config_id=config, stack_size=10)
    assert not inventory.place_item_hold(data, 40, {config: 6}, created_at_ms=1, review_after_ms=2)
    assert not inventory.place_item_hold(data, 41, {config: 2}, created_at_ms=1, review_after_ms=2)
    assert inventory.removable_count(data, config) == 2
    assert inventory.removable_count(data, config, except_hold_id=40) == 8
    assert inventory.consume_items(data, {config: 3}) == BAG_INSUFFICIENT
    assert not inventory.consume_items(data, {config: 7}, except_hold_id=40)
    assert release_item_hold(data, 40)
    assert inventory.removable_count(data, config) == 1
    assert not release_item_hold(data, 40)


def _sign(rpc, request, caller):
    request.auth.caller = caller
    request.auth.timestamp_ms = time.time_ns() // 1_000_000
    request.auth.signature_hex = hmac.digest(_SECRETS[caller], canonical(rpc, request), "sha256").hex()
    return request


def _request(protocol, stream, sequence, tx, *, player_id=1, epoch=17, **bundle):
    return protocol.message(
        "AssetOpRequest", player_id=player_id, stream=stream, stream_epoch=epoch,
        seq=sequence, tx_type=tx, correlation_id=99, bundle=bundle,
    )


def test_asset_signature_matches_cpp_and_window_prunes():
    p = Protocol()
    original = p.message(
        "AssetOpRequest", player_id=42, stream=1, stream_epoch=1700000000000,
        seq=7, correlation_id=99, tx_type=24,
        bundle={"currencies": [{"currency_type": 1, "amount": 30}]},
        auth={"caller": "guild", "timestamp_ms": 1700000000123},
    )
    assert canonical("debit", original) == (
        b"mmorpg-asset-op/v1\nguild\ndebit\n42\n1\n1700000000000\n7\n99\n24\nc=1:30;i=;u=;p=0\n1700000000123"
    )
    original.stream = 4
    original.auth.caller = "trade"
    original.bundle.item_instances.add(
        item_uuid=42, config_id=10, stack_size=1, instance_proto=b"\0\xff\1",
    )
    assert canonical("credit", original).endswith(
        b"c=1:30;i=;u=;p=0;v2=082a100a1801220300ff01,;\n1700000000123"
    )
    ledger = p.message("PlayerAssetOpLedgerComp")
    first = _request(p, 1, 1, 24)
    second = _request(p, 1, 1025, 24)
    assert record(ledger, first, Outcome.REJECTED, 27000)
    assert record(ledger, second, Outcome.APPLIED, 0)
    assert valid(ledger)
    stream = find_stream(ledger, 1)
    assert stream.watermark == 1 and len(stream.receipts) == 1
    assert classify(stream, 17, 1) == Outcome.UNKNOWN
    assert classify(stream, 17, 1025) == Outcome.APPLIED
    assert classify(stream, 18, 1025) == Outcome.UNKNOWN


class _LostAck:
    def __init__(self, store):
        self.store = store
        self.armed = False
        self.sent = None

    async def load_and_claim(self, *args, **kwargs):
        return await self.store.load_and_claim(*args, **kwargs)

    async def commit(self, snapshot, rows, **options):
        if self.sent is not None:
            old_snapshot, saved, original, original_options = self.sent
            self.sent = None
            assert snapshot is old_snapshot and rows == original and options == original_options
            return saved
        saved = await self.store.commit(snapshot, rows, **options)
        if self.armed:
            self.armed = False
            self.sent = (snapshot, saved, dict(rows), options)
            raise StoreUnavailable("simulated lost commit reply")
        return saved


async def _scene(tmp_path):
    p = Protocol()
    tables = ConfigTables()
    store = _LostAck(SqlitePlayerStore(tmp_path / "players.sqlite3"))
    codec = PlayerRowCodec(p)
    seed = await store.load_and_claim(1, b"seed")
    data = p.message("player_database", player_id=1)
    data.currency.values.extend([100, 0, 0])
    equipment = next(item for item in tables["Item"].rows() if item.equip_kind and item.max_stack_size == 1)
    data.bag_component.items.add(
        instance={"item_id": 30, "config_id": equipment.id, "size": 1, "equip": {"enhance_level": 1}},
    )
    await store.commit(seed, codec.project(data), release=True)
    node = p.message("NodeInfo", node_id=3, node_uuid="asset-scene", zone_id=1)
    scene = SceneNode(p, node, store, codec, lambda *_: None)
    scene.create(p.message("CreateSceneRequest", scene_id=1, config_id=1))
    await scene.enter(p.message(
        "PlayerEnterGameNodeRequest", player_id=1, session_id=9, scene_id=1,
        owner_revision=1, login_generation=1,
    ), "gate")
    next_guid = count(500)

    async def allocate():
        return next(next_guid)

    assets = SceneAssets(scene, tables, _SECRETS, allocate_item_id=allocate)
    return p, scene, assets, store


def test_authority_commits_receipt_with_currency_and_replays_after_lost_ack(tmp_path):
    async def exercise():
        p, scene, assets, store = await _scene(tmp_path)
        published = []
        alerts = []
        ids = count(10_000)

        async def publish(entry):
            published.append(entry)

        assets = SceneAssets(
            scene, assets.tables, _SECRETS, emit_audit=publish, allocate_tx_id=lambda: next(ids),
            emit_anomaly=lambda player_id, payload: alerts.append((player_id, json.loads(payload))),
        )
        assets.anomaly.currency_thresholds[0] = AnomalyThreshold(count=10, amount=30)
        try:
            currency = [{"currency_type": 0, "amount": 30}]
            request = _request(p, 1, 1, 24, currencies=currency)
            unsigned = await assets.debit(request)
            assert unsigned.outcome == Outcome.UNKNOWN and unsigned.reason.id == 27008
            _sign("debit", request, "guild")
            store.armed = True
            first = await assets.debit(request)
            assert first.outcome == Outcome.RETRY and not first.durable
            await asyncio.sleep(0)
            assert not published
            repeated = await assets.debit(_sign("debit", request, "guild"))
            assert repeated.outcome == Outcome.APPLIED and repeated.durable
            player = scene.locations[1].players[1]
            assert scene.rows.assemble(player.snapshot).currency.values[0] == 70

            request.bundle.currencies[0].amount = 31
            changed = await assets.debit(_sign("debit", request, "guild"))
            assert changed.outcome == Outcome.UNKNOWN and changed.reason.id == 27004
            request = _request(p, 1, 2, 24, currencies=currency)
            aborted = await assets.abort_debit(_sign("abort_debit", request, "guild"))
            assert aborted.outcome == Outcome.REJECTED and aborted.durable
            blocked = await assets.debit(_sign("debit", request, "guild"))
            assert blocked.outcome == Outcome.REJECTED and blocked.durable and not blocked.reason.id

            request = _request(p, 6, 1, 19, currencies=[{"currency_type": 0, "amount": 100}])
            recall = await assets.debit(_sign("debit", request, "data_service"))
            assert recall.durable and recall.snapshot.recall_lines[0].taken == 70
            assert recall.snapshot.recall_lines[0].debt == 30
            request = _request(p, 5, 1, 2, currencies=[{"currency_type": 0, "amount": 35}])
            store.armed = True
            first_credit = await assets.credit(_sign("credit", request, "mail"))
            assert first_credit.outcome == Outcome.RETRY and not first_credit.durable
            assert assets.anomaly.currency_gain_count(player, 0) == 0
            await asyncio.sleep(0)
            assert not alerts
            credit = await assets.credit(_sign("credit", request, "mail"))
            assert credit.durable and credit.outcome == Outcome.APPLIED
            assert (await assets.credit(_sign("credit", request, "mail"))).durable
            assert assets.anomaly.currency_gain_count(player, 0) == 1
            data = scene.rows.assemble(player.snapshot)
            assert data.currency.values[0] == 5 and not data.currency.debts
            await assets.close()
            assert [(entry.tx_type, entry.currency_delta) for entry in published] == [
                (24, -30), (19, -70), (18, -30), (2, 35),
            ]
            first, _, clawback, last = published
            assert (first.tx_id, first.zone_id, first.correlation_id) == (10_000, 1, 99)
            assert (first.from_player, first.to_player, first.balance_before, first.balance_after) == (1, 0, 100, 70)
            assert (clawback.extra, clawback.correlation_id) == ('{"debt_remaining":0}', 0)
            assert (last.from_player, last.to_player, last.balance_before, last.balance_after) == (0, 1, 0, 5)
            assert len(alerts) == 1
            alerted_player, alert = alerts[0]
            assert (alerted_player, alert["category"], alert["count"], alert["total_amount"]) == (1, "currency", 1, 35)
        finally:
            await scene.close()
            await assets.close()

    asyncio.run(exercise())


def test_trade_preserves_original_instance_and_ack_keeps_identity(tmp_path):
    async def exercise():
        p, scene, assets, _ = await _scene(tmp_path)
        try:
            request = _request(p, 3, 1, 1, item_uuids=[30])
            _sign("debit", request, "trade")
            debit = await assets.debit(request)
            assert debit.outcome == Outcome.APPLIED and debit.durable
            original = debit.snapshot.items[0]
            assert p.parse("ItemComp", original.instance_proto).equip.enhance_level == 1
            assert (await assets.debit(request)).snapshot.items[0] == original

            _sign("ack_receipt", request, "trade")
            ack = await assets.ack_receipt(request)
            assert ack.outcome == Outcome.APPLIED and ack.durable
            debit = await assets.debit(_sign("debit", request, "trade"))
            assert debit.outcome == Outcome.APPLIED and debit.durable and not debit.HasField("snapshot")

            credit_request = _request(p, 4, 1, 1, item_instances=[original])
            credit = await assets.credit(_sign("credit", credit_request, "trade"))
            assert credit.outcome == Outcome.APPLIED and credit.durable
            player = scene.locations[1].players[1]
            restored = scene.rows.assemble(player.snapshot).bag_component.items[0].instance
            assert restored.item_id == 30 and restored.equip.enhance_level == 1
            assert player.asset_anomaly is None
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_inventory_plans_are_atomic_and_preserve_unknown_instances():
    async def exercise():
        p = Protocol()
        tables = ConfigTables()
        next_guid = count(90)

        async def allocate():
            return next(next_guid)

        inventory = AssetInventory(p, tables, allocate)
        item = next(row for row in tables["Item"].rows() if row.max_stack_size > 1 and not row.equip_kind)
        data = p.message("player_database")
        data.bag_component.capacities.append(1)
        gains = []
        assert await inventory.add_items(data, {item.id: item.max_stack_size + 1}, gains=gains) == BAG_FULL
        assert not data.bag_component.items
        assert not gains
        assert await inventory.add_items(data, {item.id: 1}, gains=gains) == 0
        assert gains == [AssetGain("item", item.id, 1)]
        assert data.bag_component.items[0].item_uuid == 90
        entry = data.bag_component.items[0]
        entry.instance.MergeFromString(b"\x98\x06\x01")
        before = data.SerializeToString(deterministic=True)
        assert inventory.consume_items(data, {item.id: 1}) == FEATURE_UNAVAILABLE
        assert data.SerializeToString(deterministic=True) == before

    asyncio.run(exercise())

def test_item_audit_aggregates_stackable_grants_on_the_first_written_item():
    p, tables = Protocol(), ConfigTables()
    item = next(row for row in tables["Item"].rows() if row.max_stack_size > 1 and not row.equip_kind)
    before = p.message("player_database")
    before.bag_component.items.add(item_uuid=42, config_id=item.id, stack_size=1, acquire_seq=1)
    after = p.parse("player_database", before.SerializeToString())
    after.bag_component.items[0].stack_size = item.max_stack_size
    after.bag_component.items.add(item_uuid=12, config_id=item.id, stack_size=2, acquire_seq=2)
    audit = AssetAudit(p, 1, (), tables, None, None)
    result = audit.prepare(1, before, after, 11, correlation=90, item_extra={"source": "mystery_merchant"})
    assert len(result.entries) == 1
    entry = result.entries[0]
    assert (entry.item_uuid, entry.item_config_id, entry.item_quantity) == (42, item.id, item.max_stack_size + 1)
    assert (entry.from_player, entry.to_player, entry.correlation_id) == (0, 1, 90)
    assert entry.extra == '{"source":"mystery_merchant"}'


def test_bag_client_rpcs_commit_gems_and_replay_enhancement(tmp_path):
    async def exercise():
        p, scene, assets, _ = await _scene(tmp_path)
        try:
            assets.register_client_handlers()
            player = scene.locations[1].players[1]
            bag = scene.rows.assemble(player.snapshot)
            item = next(
                candidate for candidate in assets.tables["Item"].rows()
                if candidate.equip_kind and candidate.max_stack_size == 1
                and assets.tables["EquipGemSocket"].find_all(candidate.equip_kind, field="equip_kind")
            )
            old = bag.bag_component.items[0]
            old.instance.config_id = old.config_id = item.id
            old.instance.equip.enhance_level = 0
            bag.level_component.level = max(1, item.equip_level)
            setattr(bag.uint32_pb_component, "class", item.equip_class)
            bag.currency.values[0] = 1_000_000
            socket = assets.tables["EquipGemSocket"].find_all(item.equip_kind, field="equip_kind")[0]
            gem = next(
                row for row in assets.tables["EquipGem"].rows()
                if not socket.gem_type or row.gem_type == socket.gem_type
            )
            first_level = assets.tables["EquipEnhance"].get(1)
            grants = {gem.id: 1}
            if first_level.material_id and first_level.material_count:
                grants[first_level.material_id] = grants.get(first_level.material_id, 0) + first_level.material_count
            assert await assets.add_items(bag, grants) == 0
            await scene.commit_player_data(player, bag)

            async def rpc(method, **fields):
                message_id = p.message_ids[f"SceneBagClientPlayer{method}"]
                route = p.routes[message_id]
                body = p.message(route.request_type, **fields).SerializeToString()
                request = p.message(
                    "ProcessClientPlayerMessageRequest", session_id=9, player_id=1,
                    message_content=p.content(message_id, 1, body=body),
                )
                response = await scene.process(request, "gate")
                assert not response.message_content.error_message.id
                return p.parse(route.response_type, response.message_content.serialized_message)

            wear = await rpc("EquipItem", item_id=30)
            assert not wear.error_message.id and wear.equipment.items[0].item_id == 30
            inlay = await rpc("InlayGem", item_id=30, socket=socket.socket, gem_config_id=gem.id)
            assert not inlay.error_message.id
            taken = await rpc("RemoveGem", item_id=30, socket=socket.socket)
            assert not taken.error_message.id and any(row.config_id == gem.id for row in taken.bag.items)
            enhancement = await rpc("EnhanceEquip", item_id=30, client_request_id=41, expected_level=0)
            assert not enhancement.error_message.id and not enhancement.replayed
            gold = scene.rows.assemble(player.snapshot).currency.values[0]
            replay = await rpc("EnhanceEquip", item_id=30, client_request_id=41, expected_level=0)
            assert not replay.error_message.id and replay.replayed
            assert replay.success == enhancement.success and replay.enhance_level == enhancement.enhance_level
            assert scene.rows.assemble(player.snapshot).currency.values[0] == gold
            conflict = await rpc("EnhanceEquip", item_id=30, client_request_id=41, expected_level=1)
            assert conflict.error_message.id == 1005
            removed = await rpc("UnequipItem", item_id=30)
            assert not removed.error_message.id and not removed.equipment.items
            assert any(row.item_id == 30 for row in removed.bag.items)
        finally:
            await scene.close()

    asyncio.run(exercise())

def test_mystery_merchant_purchase_keeps_quote_limit_and_replay(tmp_path):
    from xuanming.nodes.scene_merchant import SceneMerchant

    async def exercise():
        p, scene, assets, _ = await _scene(tmp_path)
        now = [70_000]
        merchant = SceneMerchant(scene, assets.tables, assets, clock=lambda: now[0])
        merchant.register()
        player = scene.locations[1].players[1]
        world = scene.locations[1]
        data = scene.rows.assemble(player.snapshot)
        for currency in assets.currency_types:
            assets._set_balance(data, currency, 1_000_000_000)
        await scene.commit_player_data(player, data)

        row = next(row for row in assets.tables["MysteryMerchantGoods"].rows() if row.weight)
        x, y, z = player.transform.position
        state = p.message(
            "MysteryMerchantState", zone_id=1, scene_id=1, scene_config_id=1, round_id=60,
            revision=1, appear_at_ms=60_000, expire_at_ms=90_000, interval_seconds=60,
            position={"x": x, "y": y, "z": z},
            shelf=[{"goods_id": row.id, "discount_percent": 60}],
        )
        assert merchant.apply_state(state)
        info = merchant.query(world, player, None, None).info
        goods = info.goods[0]
        assert info.active and info.here and goods.price == (row.price * 60 + 99) // 100
        request = p.message(
            "BuyMysteryMerchantGoodsRequest", round_id=info.round_id, goods_id=goods.goods_id,
            count=1, expected_price=goods.price + 1, expected_currency_type=goods.currency_type,
            expected_item_config_id=goods.item_config_id, expected_item_count=goods.item_count,
            client_request_id=991,
        )
        assert (await merchant.buy(world, player, None, request)).error_message.id == 30003
        request.expected_price = goods.price
        bought = await merchant.buy(world, player, None, request)
        assert not bought.error_message.id and bought.info.goods[0].bought_count == 1
        assert bought.bag.currency.values[goods.currency_type] == 1_000_000_000 - goods.price
        assert merchant.apply_state(state)
        now[0] = 91_000
        replay = await merchant.buy(world, player, None, request)
        assert replay.replayed and not replay.info.active
        assert replay.bag.currency.values == bought.bag.currency.values
        request.client_request_id += 1
        assert (await merchant.buy(world, player, None, request)).error_message.id == 30000
        await scene.close()

    asyncio.run(exercise())
