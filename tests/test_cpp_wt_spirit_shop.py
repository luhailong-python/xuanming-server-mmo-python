"""官方灵石百宝阁（509/510）与 EquipCraft 集群账号配额同步传输测试。"""

from __future__ import annotations

import asyncio
import json
from contextlib import AsyncExitStack

from test_monopoly import TOKEN, _enter, _settings
from xuanming.data import ConfigTables, PlayerRowCodec, PlayerRowKind, SqlitePlayerStore
from xuanming.data.asset_inventory import INVALID, UNAVAILABLE, balance, clone
from xuanming.data.id_segments import IdSegments
from xuanming.grpc_runtime import verify_service_call
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_equip_craft import (
    CRAFTING_MATERIAL_ID,
    SceneEquipCraft,
    resolve_game_day,
)
from xuanming.nodes.scene_spirit_shop import (
    CLIENT_REQUEST_KIND_BUY_SPIRIT_SHOP,
    CURRENCY_BIND_DIAMOND,
    CURRENCY_DIAMOND,
    MAX_BUY_COUNT_PER_REQUEST,
    MAX_LIMIT_GROUP_WEIGHT,
    MAX_UNIT_PRICE,
    RESET_LIFETIME,
    RESET_WEEKLY,
    SPIRIT_SHOP_BAG_FULL,
    SPIRIT_SHOP_CURRENCY_NOT_ENOUGH,
    SPIRIT_SHOP_FROZEN,
    SPIRIT_SHOP_GOODS_OFF_SHELF,
    SPIRIT_SHOP_GROUP_LIMIT_REACHED,
    SPIRIT_SHOP_IN_BATTLE,
    SPIRIT_SHOP_INVALID_COUNT,
    SPIRIT_SHOP_ITEM_BLOCKED,
    SPIRIT_SHOP_LIMIT_REACHED,
    SPIRIT_SHOP_PRICE_CHANGED,
    SPIRIT_SHOP_UNAVAILABLE,
    TX_SPIRIT_SHOP_BUY,
    WEEK_MS,
    SceneSpiritShop,
    bought_count,
    record_purchase,
    resolve_week,
    total_cost,
    total_group_weight,
    used_group_weight,
    validate_comp,
    would_exceed_limit,
)
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime, _SYNC_STAGE1_TRADABLE_PATH

# 2026-11-25 12:00:00 UTC（位于 UTC+8 周一 05:00 之后的正常周内）
_NOW_MS = 1_795_521_600_000


async def _call(client, call_id: int, message_name: str, **fields):
    p = client.protocol
    message_id = p.message_ids[message_name]
    request = p.message(p.routes[message_id].request_type, **fields)
    return await client.rpc(message_id, call_id, request)


def _build_buy_req_from_info(protocol: Protocol, info, goods_id: int, count: int, req_id: int):
    req = protocol.message(
        "BuySpiritShopGoodsRequest",
        client_request_id=req_id,
        goods_id=goods_id,
        count=count,
    )
    for g in info.goods:
        if g.goods_id == goods_id:
            req.expected_currency_type = g.currency_type
            req.expected_unit_price = g.unit_price
            for item in g.items:
                req.expected_items.add(
                    item_config_id=item.item_config_id,
                    item_count=item.item_count,
                    bound=item.bound,
                )
            break
    return req


async def _make_scene_harness(tmp_path, *, diamond: int = 1000, bind_diamond: int = 500, clock_box=None):
    protocol = Protocol()
    tables = ConfigTables()
    db_path = tmp_path / "scene_spirit_shop.sqlite"
    store = SqlitePlayerStore(db_path)
    rows = PlayerRowCodec(protocol)
    emitted_audits = []
    tx_seq = iter(range(900_000, 999_999))

    async def emit_audit(entry):
        emitted_audits.append(entry)

    async def alloc_tx():
        return next(tx_seq)

    info = protocol.message("NodeInfo", node_id=1, node_type=3, zone_id=1, node_uuid="scene-spirit-shop")
    scene = SceneNode(protocol, info, store, rows, lambda *args, **kwargs: None)
    scene.tables = tables
    scene.create(protocol.message("CreateSceneRequest", config_id=1, scene_id=1))
    assets = SceneAssets(
        scene,
        tables,
        allow_gm=True,
        allocate_item_id=IdSegments.local(db_path, "item"),
        emit_audit=emit_audit,
        allocate_tx_id=alloc_tx,
        anomaly_journal_path=tmp_path / "anomaly.json",
    )
    await assets.anomaly.start()
    assets.register_client_handlers()
    scene.assets = assets
    clock_val = clock_box if clock_box is not None else [_NOW_MS]
    shop = SceneSpiritShop(scene, tables, assets, now_ms=lambda: clock_val[0])
    shop.register()
    scene.spirit_shop = shop

    enter_req = protocol.message(
        "PlayerEnterGameNodeRequest",
        player_id=1001,
        scene_id=1,
        home_zone_id=1,
        owner_epoch=1,
        session_id=1,
        login_generation=1,
        owner_revision=1,
    )
    await scene.enter(enter_req, "gate-test")
    player = scene.locations[1001].players[1001]
    data = scene.rows.assemble(player.snapshot)
    if diamond:
        assert assets.add_currency(data, CURRENCY_DIAMOND, diamond) == 0
    if bind_diamond:
        assert assets.add_currency(data, CURRENCY_BIND_DIAMOND, bind_diamond) == 0
    await scene.commit_player_data(player, data)
    return protocol, tables, scene, assets, shop, player, emitted_audits


async def _dispatch(scene: SceneNode, player, method: str, request):
    route = next(
        r for r in scene.protocol.routes.values()
        if r.service == "SceneSpiritShopClientPlayer" and r.method == method
    )
    handler = scene._handlers["SceneSpiritShopClientPlayer"][method]
    world = scene.locations[player.player_id]
    res = handler(world, player, route, request)
    if asyncio.iscoroutine(res):
        res = await res
    return res


# ── 1. 纯规则单测（周边界、抗时钟回拨、防溢出、组件校验） ─────────────────────────────────────


def test_spirit_shop_rules_weekly_reset_and_clock_rollback():
    protocol = Protocol()
    week = resolve_week(_NOW_MS)
    assert week is not None
    week1, next1 = week
    assert next1 - week1 == WEEK_MS
    assert resolve_week(1000) is None

    comp = protocol.message("PlayerSpiritShopComp")
    assert record_purchase(comp, 901, 2, 5, 2, 100, week1)
    assert bought_count(comp, 901, RESET_WEEKLY, week1) == 2
    assert used_group_weight(comp, 2, RESET_WEEKLY, week1) == 100
    assert comp.shop_revision == 1

    # 时钟回拨到上一周：保守保留当前已购/已用，绝不趁机清零。
    prev_week = week1 - WEEK_MS
    assert bought_count(comp, 901, RESET_WEEKLY, prev_week) == 2
    assert used_group_weight(comp, 2, RESET_WEEKLY, prev_week) == 100

    # 推进到下一周：周计数归零，终身计数永久保留。
    assert bought_count(comp, 901, RESET_WEEKLY, next1) == 0
    assert used_group_weight(comp, 2, RESET_WEEKLY, next1) == 0
    assert bought_count(comp, 901, RESET_LIFETIME, next1) == 2
    assert used_group_weight(comp, 2, RESET_LIFETIME, next1) == 100


def test_spirit_shop_rules_overflow_and_comp_validation():
    protocol = Protocol()
    assert total_cost(0, 1) is None
    assert total_cost(100, 0) is None
    assert total_cost(100, MAX_BUY_COUNT_PER_REQUEST + 1) is None
    assert total_cost(MAX_UNIT_PRICE + 1, 1) is None
    assert total_cost(180, 10) == 1800

    assert total_group_weight(0, 1) is None
    assert total_group_weight(MAX_LIMIT_GROUP_WEIGHT + 1, 1) is None
    assert total_group_weight(60, 5) == 300

    assert would_exceed_limit(240, 180, 300) is True
    assert would_exceed_limit(240, 60, 300) is False
    assert would_exceed_limit(1, 0xFFFFFFFF, 300) is True
    assert would_exceed_limit(999, 999, 0) is False

    bad = protocol.message("PlayerSpiritShopComp")
    bad.goods_records.add(goods_id=101)
    bad.goods_records.add(goods_id=101)
    assert validate_comp(bad) is not None

    zero_group = protocol.message("PlayerSpiritShopComp")
    zero_group.group_records.add(group_id=0)
    assert validate_comp(zero_group) is not None


# ── 2. 真实配表、限购组 300 封顶、严格绑定分堆、防呆/异常与审计流水 ──────────────────────────────


def test_spirit_shop_shelf_shared_group_cap_bound_isolation_and_audit(tmp_path):
    async def exercise():
        protocol, tables, scene, assets, shop, player, audits = await _make_scene_harness(
            tmp_path, diamond=1000, bind_diamond=500,
        )
        try:
            assert shop.validate_tables() is True
            assert shop.buy_tx == TX_SPIRIT_SHOP_BUY == 44

            # 509 查询货架与双余额，核对 9 款商品定案单价
            info_resp = await _dispatch(scene, player, "GetSpiritShopInfo", protocol.message("GetSpiritShopInfoRequest"))
            assert not info_resp.error_message.id
            info = info_resp.info
            assert info.diamond_balance == 1000
            assert info.bind_diamond_balance == 500
            assert len(info.limit_groups) == 1
            assert (info.limit_groups[0].group_id, info.limit_groups[0].max_weight, info.limit_groups[0].used_weight) == (1, 300, 0)
            assert len(info.goods) == 9
            prices = {g.goods_id: g.unit_price for g in info.goods}
            assert prices == {101: 60, 102: 180, 103: 300, 201: 60, 202: 180, 301: 100, 302: 100, 303: 120, 304: 180}

            # 预先放入 10 份可交易打造材料（3001, bound=False），验证购买绑定提速包绝不与之合堆洗白
            data = scene.rows.assemble(player.snapshot)
            assert await assets.add_items(data, {3001: 10}) == 0
            await scene.commit_player_data(player, data)

            # 买 1 份 101(60) + 1 份 102(180) = 240/300
            r1 = await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 101, 1, 1001))
            assert not r1.error_message.id and not r1.replayed
            assert r1.info.limit_groups[0].used_weight == 60
            assert r1.info.diamond_balance == 940
            assert r1.info.shop_revision == 1
            # 验证返回 BagInfo 显式携带 bound=True
            bound_3001 = [it for it in r1.bag.items if it.config_id == 3001 and it.bound]
            tradable_3001 = [it for it in r1.bag.items if it.config_id == 3001 and not it.bound]
            assert sum(it.count for it in bound_3001) == 30
            assert sum(it.count for it in tradable_3001) == 10

            r2 = await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 102, 1, 1002))
            assert not r2.error_message.id
            assert r2.info.limit_groups[0].used_weight == 240
            assert r2.info.diamond_balance == 760

            # 剩余 60 份，再买 102(180) 被共享组拦下（37005）且零扣费
            r_over = await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 102, 1, 1003))
            assert r_over.error_message.id == SPIRIT_SHOP_GROUP_LIMIT_REACHED
            assert balance(scene.rows.assemble(player.snapshot), CURRENCY_DIAMOND) == 760

            # 补买 1 份 101(60) 恰好达 300/300
            r3 = await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 101, 1, 1004))
            assert not r3.error_message.id
            assert r3.info.limit_groups[0].used_weight == 300
            assert r3.info.diamond_balance == 700

            # 满额后再买 101 或 103 均拒 37005
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 101, 1, 1005))).error_message.id == SPIRIT_SHOP_GROUP_LIMIT_REACHED
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 103, 1, 1006))).error_message.id == SPIRIT_SHOP_GROUP_LIMIT_REACHED

            # 核对背包：3001 共 160（其中纯可交易严格保持 10），3002 共 150（纯可交易 0）
            saved = scene.rows.assemble(player.snapshot)
            assert assets.removable_count(saved, 3001) == 160
            assert assets.removable_tradable_count(saved, 3001) == 10
            assert assets.removable_count(saved, 3002) == 150
            assert assets.removable_tradable_count(saved, 3002) == 0

            # 验证 spirit_shop 随 WALLET 行持久化
            wallet_part = protocol.parse("player_database", player.snapshot.rows[PlayerRowKind.WALLET])
            assert wallet_part.spirit_shop.shop_revision == 3
            assert used_group_weight(wallet_part.spirit_shop, 1, RESET_LIFETIME, 0) == 300

            # 验证审计流水（等待异步队列刷新）
            await assets._audit.close()
            shop_audits = [e for e in audits if e.tx_type == TX_SPIRIT_SHOP_BUY]
            assert any(e.correlation_id == 1001 and e.currency_type == CURRENCY_DIAMOND and e.currency_delta == -60 for e in shop_audits)
            item_audit = next(e for e in shop_audits if e.correlation_id == 1001 and e.item_config_id == 3001)
            assert json.loads(item_audit.extra) == {
                "source": "spirit_shop",
                "goods_id": 101,
                "count": 1,
                "unit_price": 60,
                "currency_type": 1,
            }
        finally:
            await assets.close()
            await scene.close()

    asyncio.run(exercise())


def test_spirit_shop_guards_idempotency_bag_full_and_per_good_limit(tmp_path):
    async def exercise():
        clock_box = [_NOW_MS]
        protocol, tables, scene, assets, shop, player, _ = await _make_scene_harness(
            tmp_path, diamond=300, bind_diamond=9999, clock_box=clock_box,
        )
        try:
            info = (await _dispatch(scene, player, "GetSpiritShopInfo", protocol.message("GetSpiritShopInfoRequest"))).info

            # 1. 报价防呆、下架商品、非法份数、client_request_id=0
            bad_price = _build_buy_req_from_info(protocol, info, 202, 1, 2001)
            bad_price.expected_unit_price = 240
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", bad_price)).error_message.id == SPIRIT_SHOP_PRICE_CHANGED

            bad_bound = _build_buy_req_from_info(protocol, info, 202, 1, 2002)
            bad_bound.expected_items[0].bound = False
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", bad_bound)).error_message.id == SPIRIT_SHOP_PRICE_CHANGED

            off_shelf = _build_buy_req_from_info(protocol, info, 202, 1, 2003)
            off_shelf.goods_id = 9999
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", off_shelf)).error_message.id == SPIRIT_SHOP_GOODS_OFF_SHELF

            bad_count = _build_buy_req_from_info(protocol, info, 202, 0, 2004)
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", bad_count)).error_message.id == SPIRIT_SHOP_INVALID_COUNT

            zero_req_id = _build_buy_req_from_info(protocol, info, 201, 1, 0)
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", zero_req_id)).error_message.id == INVALID

            # 2. 满包只剩 1 格空位买双物品包 101：整批拒 37007，零扣费零发物零增 revision
            data = scene.rows.assemble(player.snapshot)
            cap = data.bag_component.capacities[0] if data.bag_component.capacities else 100
            assert await assets.add_items(data, {3101: cap - 1}) == 0
            await scene.commit_player_data(player, data)
            full_resp = await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 101, 1, 2005))
            assert full_resp.error_message.id == SPIRIT_SHOP_BAG_FULL
            after_full = scene.rows.assemble(player.snapshot)
            assert balance(after_full, CURRENCY_DIAMOND) == 300
            assert assets.removable_count(after_full, 3001) == 0
            assert assets.removable_count(after_full, 3002) == 0
            assert after_full.spirit_shop.shop_revision == 0

            # 清空填充物品继续测其他防线
            after_full.bag_component.ClearField("items")
            await scene.commit_player_data(player, after_full)

            # 3. 物品封禁 / 战斗中 / 冻结 / 损坏组件
            assets.global_blocks["item"].add(3405)
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 201, 1, 2006))).error_message.id == SPIRIT_SHOP_ITEM_BLOCKED
            assets.global_blocks["item"].discard(3405)

            assets._in_battle = lambda _p: True
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 201, 1, 2007))).error_message.id == SPIRIT_SHOP_IN_BATTLE
            assets._in_battle = None

            orig_mutable = scene.mutable
            scene.mutable = lambda _p: False
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 201, 1, 2008))).error_message.id == SPIRIT_SHOP_FROZEN
            scene.mutable = orig_mutable

            bad_comp_data = scene.rows.assemble(player.snapshot)
            bad_comp_data.spirit_shop.goods_records.add(goods_id=0)
            await scene.commit_player_data(player, bad_comp_data)
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 201, 1, 2009))).error_message.id == SPIRIT_SHOP_UNAVAILABLE
            bad_comp_data.spirit_shop.ClearField("goods_records")
            await scene.commit_player_data(player, bad_comp_data)

            # 4. 余额恰好 300 买 103(300) -> 灵石归 0、组满 300 -> 幂等重放仍成功且不重复发物；同键换商品拒 1005
            buy_103 = _build_buy_req_from_info(protocol, info, 103, 1, 3001)
            ok = await _dispatch(scene, player, "BuySpiritShopGoods", buy_103)
            assert not ok.error_message.id and not ok.replayed
            assert ok.info.diamond_balance == 0
            assert ok.info.limit_groups[0].used_weight == 300

            replay = await _dispatch(scene, player, "BuySpiritShopGoods", buy_103)
            assert not replay.error_message.id and replay.replayed is True
            snap_after = scene.rows.assemble(player.snapshot)
            assert assets.removable_count(snap_after, 3001) == 150
            assert assets.removable_count(snap_after, 3002) == 150
            assert any(
                rec.key == 3001 and rec.kind == CLIENT_REQUEST_KIND_BUY_SPIRIT_SHOP
                for rec in snap_after.client_request_ledger.records
            )

            mismatch = _build_buy_req_from_info(protocol, info, 201, 1, 3001)
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", mismatch)).error_message.id == INVALID

            # 5. 绑定灵石 9999 绝不能代付灵石商品（余额 0 -> 37006）
            poor = await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 201, 1, 3002))
            assert poor.error_message.id == SPIRIT_SHOP_CURRENCY_NOT_ENOUGH
            assert balance(scene.rows.assemble(player.snapshot), CURRENCY_BIND_DIAMOND) == 9999

            # 6. 单商品周限购（通过测试替身覆盖 201 的 limit_count=1, limit_reset_kind=1）
            d_refill = scene.rows.assemble(player.snapshot)
            assert assets.add_currency(d_refill, CURRENCY_DIAMOND, 300) == 0
            await scene.commit_player_data(player, d_refill)
            row_201 = clone(tables["SpiritShopGoods"].get(201))
            row_201.limit_count = 1
            row_201.limit_reset_kind = RESET_WEEKLY
            orig_table_fn = shop._table

            class _PatchedGoodsTable:
                def get(self, key):
                    return row_201 if key == 201 else tables["SpiritShopGoods"].get(key)

                def rows(self):
                    return tuple(row_201 if r.id == 201 else r for r in tables["SpiritShopGoods"].rows())

            shop._table = lambda name: _PatchedGoodsTable() if name == "SpiritShopGoods" else orig_table_fn(name)
            first_201 = await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 201, 1, 4001))
            assert not first_201.error_message.id
            second_201 = await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 201, 1, 4002))
            assert second_201.error_message.id == SPIRIT_SHOP_LIMIT_REACHED
            # 时钟回拨仍拦截
            clock_box[0] = _NOW_MS - WEEK_MS
            assert (await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 201, 1, 4003))).error_message.id == SPIRIT_SHOP_LIMIT_REACHED
            # 推进到下一周后周限购重置，可再买 1 次
            clock_box[0] = _NOW_MS + WEEK_MS
            next_week_201 = await _dispatch(scene, player, "BuySpiritShopGoods", _build_buy_req_from_info(protocol, info, 201, 1, 4004))
            assert not next_week_201.error_message.id
        finally:
            await assets.close()
            await scene.close()

    asyncio.run(exercise())


# ── 3. 真实 TCP Runtime("local") 端到端购买与重启持久化 ────────────────────────────────────────


def test_spirit_shop_real_tcp_local_runtime_persistence_across_restart(tmp_path):
    async def exercise():
        settings = _settings(tmp_path)

        # 第一次启动：充值灵石 -> 509 查询 -> 510 购买 101(60) 与 201(60)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            assert runtime.scene.spirit_shop is not None
            async with _enter(runtime, account="shopper") as (client, player_id):
                player = runtime.scene.locations[player_id].players[player_id]
                data = runtime.scene.rows.assemble(player.snapshot)
                assert runtime.scene.assets.add_currency(data, CURRENCY_DIAMOND, 500) == 0
                assert runtime.scene.assets.add_currency(data, CURRENCY_BIND_DIAMOND, 200) == 0
                await runtime.scene.commit_player_data(player, data)

                info_reply = await _call(client, 1, "SceneSpiritShopClientPlayerGetSpiritShopInfo")
                assert not info_reply.error_message.id
                assert info_reply.info.diamond_balance == 500
                assert info_reply.info.bind_diamond_balance == 200
                assert len(info_reply.info.goods) == 9

                req_101 = _build_buy_req_from_info(runtime.protocol, info_reply.info, 101, 1, 77001)
                buy_101 = await client.rpc(
                    runtime.protocol.message_ids["SceneSpiritShopClientPlayerBuySpiritShopGoods"], 2, req_101,
                )
                assert not buy_101.error_message.id and not buy_101.replayed
                assert buy_101.info.diamond_balance == 440
                assert buy_101.info.limit_groups[0].used_weight == 60
                assert buy_101.info.shop_revision == 1
                assert any(it.config_id == 3001 and it.count == 30 and it.bound for it in buy_101.bag.items)
                assert any(it.config_id == 3002 and it.count == 30 and it.bound for it in buy_101.bag.items)

                req_201 = _build_buy_req_from_info(runtime.protocol, info_reply.info, 201, 2, 77002)
                buy_201 = await client.rpc(
                    runtime.protocol.message_ids["SceneSpiritShopClientPlayerBuySpiritShopGoods"], 3, req_201,
                )
                assert not buy_201.error_message.id
                assert buy_201.info.diamond_balance == 320
                assert buy_201.info.shop_revision == 2
                assert any(it.config_id == 3405 and it.count == 2 and it.bound for it in buy_201.bag.items)

        # 第二次重启（同目录持久化存储）：验证限额、余额、背包与幂等重放全部跨重启保留
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime, account="shopper") as (client, _player_id):
                after_restart = await _call(client, 10, "SceneSpiritShopClientPlayerGetSpiritShopInfo")
                assert not after_restart.error_message.id
                assert after_restart.info.diamond_balance == 320
                assert after_restart.info.bind_diamond_balance == 200
                assert after_restart.info.shop_revision == 2
                assert after_restart.info.limit_groups[0].used_weight == 60

                # 跨重启重放 77001：命中持久化幂等账本，不重复扣费不重复发物
                replay_req = _build_buy_req_from_info(runtime.protocol, after_restart.info, 101, 1, 77001)
                replay_resp = await client.rpc(
                    runtime.protocol.message_ids["SceneSpiritShopClientPlayerBuySpiritShopGoods"], 11, replay_req,
                )
                assert not replay_resp.error_message.id and replay_resp.replayed is True
                assert replay_resp.info.diamond_balance == 320
                assert sum(it.count for it in replay_resp.bag.items if it.config_id == 3001) == 30

    asyncio.run(exercise())


# ── 4. SceneEquipCraft 集群账号配额同步传输接线验证 ─────────────────────────────────────────────


def test_runtime_wires_equip_craft_cluster_account_quota_sync_transport(tmp_path):
    async def exercise():
        protocol = Protocol()
        tables = ConfigTables()
        db_path = tmp_path / "equip_quota_wire.sqlite"
        store = SqlitePlayerStore(db_path)
        rows = PlayerRowCodec(protocol)
        info = protocol.message("NodeInfo", node_id=2, node_type=3, zone_id=1, node_uuid="scene-quota-wire")
        scene = SceneNode(protocol, info, store, rows, lambda *args, **kwargs: None)
        scene.tables = tables
        scene.create(protocol.message("CreateSceneRequest", config_id=1, scene_id=1))
        assets = SceneAssets(
            scene,
            tables,
            allocate_item_id=IdSegments.local(db_path, "item"),
            anomaly_journal_path=tmp_path / "anomaly-quota.json",
        )
        await assets.anomaly.start()
        assets.register_client_handlers()
        scene.assets = assets
        day_ms, _ = resolve_game_day(_NOW_MS)
        craft = SceneEquipCraft(scene, tables, assets, clock=lambda: day_ms)
        craft.register()
        scene.equip_craft = craft

        secret_bytes = b"scene-cluster-directory-secret-32b!"
        captured = []
        reply_gate = asyncio.Event()

        async def fake_data_service_call(path: str, payload: bytes) -> bytes:
            req = protocol.parse("data_service.SyncAccountStage1TradableRequest", payload)
            assert path == _SYNC_STAGE1_TRADABLE_PATH
            assert verify_service_call(path, req, "scene", day_ms, secret_bytes)
            captured.append((path, req.player_id, req.desired_char_total))
            await reply_gate.wait()
            # 账号仅剩 1 份可交易额度（desired=2 -> 确认 1 份，超额 1 份平滑降级为绑定）
            reply = protocol.message(
                "data_service.SyncAccountStage1TradableResponse",
                found=True,
                confirmed_char_total=1,
                account_used_total=60,
            )
            return reply.SerializeToString()

        async with AsyncExitStack() as stack:
            dispatch = Runtime._bind_equip_craft_account_quota_sync(
                scene, fake_data_service_call, secret_bytes, stack=stack,
            )
            assert craft._account_quota_transport is dispatch

            enter_req = protocol.message(
                "PlayerEnterGameNodeRequest",
                player_id=2001,
                scene_id=1,
                home_zone_id=1,
                owner_epoch=1,
                session_id=2,
                login_generation=1,
                owner_revision=1,
            )
            await scene.enter(enter_req, "gate-cluster")
            player = scene.locations[2001].players[2001]
            data = scene.rows.assemble(player.snapshot)
            assert craft.credit_battle_material_reward_v2(2001, is_win=True, now_ms_val=day_ms, data=data) == (10, 1)
            assert craft.credit_battle_material_reward_v2(2001, is_win=True, now_ms_val=day_ms, data=data) == (10, 1)
            await scene.commit_player_data(player, data)

            claim_route = next(
                r for r in protocol.routes.values()
                if r.service == "SceneBagClientPlayer" and r.method == "ClaimEquipmentMaterialReward"
            )
            world = scene.locations[2001]
            # 第一次领取：未确认配额 -> fail-closed 1003 并触发已签名的异步 SyncAccountStage1Tradable
            blocked = await craft.claim_material_reward(
                world,
                player,
                claim_route,
                protocol.message(
                    "ClaimEquipmentMaterialRewardRequest",
                    client_request_id=9101,
                    expected_revision=0,
                    crafting_material_count=20,
                    enhancement_stone_count=0,
                ),
            )
            assert blocked.error_message.id == UNAVAILABLE and blocked.outcome == 0
            await asyncio.sleep(0)
            assert captured == [(_SYNC_STAGE1_TRADABLE_PATH, 2001, 2)]

            # 放行 data_service 回包并等待后台任务落盘
            reply_gate.set()
            for _ in range(50):
                snap = scene.rows.assemble(player.snapshot)
                if snap.client_request_ledger.material_reward.stage1_tradable_confirmed == 1:
                    break
                await asyncio.sleep(0.01)
            snap = scene.rows.assemble(player.snapshot)
            assert snap.client_request_ledger.material_reward.stage1_tradable_confirmed == 1
            assert snap.client_request_ledger.material_reward.tradable_earned_total == 1
            assert snap.client_request_ledger.material_reward.available_tradable_units == 1

            # 确认后再次领取：成功分堆发放（1 可交易 + 19 绑定）
            claimed = await craft.claim_material_reward(
                world,
                player,
                claim_route,
                protocol.message(
                    "ClaimEquipmentMaterialRewardRequest",
                    client_request_id=9102,
                    expected_revision=0,
                    crafting_material_count=20,
                    enhancement_stone_count=0,
                ),
            )
            assert not claimed.error_message.id and claimed.outcome == 2
            final_data = scene.rows.assemble(player.snapshot)
            stacks = sorted(
                (it.size, it.bound)
                for _, it in assets.entries(final_data.bag_component, 0)
                if it.config_id == CRAFTING_MATERIAL_ID
            )
            assert stacks == [(1, False), (19, True)]

        await assets.close()
        await scene.close()

    asyncio.run(exercise())
