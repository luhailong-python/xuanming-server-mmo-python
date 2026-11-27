"""C++ f4947968a..da97bfb11 增量移植回归测试：560..563 退役、江湖未结银两拦截、战斗/AFK 指标、帮会/交易/登录/收件箱/聊天加固。"""

from __future__ import annotations

import asyncio
import contextvars
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from xuanming.chat.model import ActorIdentity, CHANNEL_WORLD, ChatRules
from xuanming.chat.store import ChatStoreCore, MemoryChatStore, detach_background_context
from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.data.asset_inventory import FEATURE_UNAVAILABLE
from xuanming.guild.model import (
    ASSET_STREAM_GUILD_CREDIT,
    GuildTableCatalog,
    OP_KIND_SHOP,
    OP_KIND_WAR_REWARD,
    OP_STATUS_ABORTED,
    OP_STATUS_PENDING,
    RoleOriginal,
    TX_GUILD_WAR_REWARD,
    WarCreditOwed,
)
from xuanming.guild.service import GuildService
from xuanming.guild.sql_store import (
    SQL_COUNT_WAR_CREDIT_OWED_CAPPED,
    SQL_RESOLVE_ASSET_OP_QUIET,
)
from xuanming.guild.store import (
    ManualResolution,
    MemoryGuildStore,
    QuietWitness,
)
from xuanming.login.logic import enter_game
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_afk_pass import SceneAfkPass
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_battle import (
    FREEZE_HOLD_EVENT_NAMES,
    Freeze,
    FreezeHoldEvent,
    SceneBattle,
)
from xuanming.nodes.scene_jianghu import (
    SceneJianghu,
    WRIT_TIER_GOLD_ITEM,
    WRIT_TIER_SILVER,
    _normalize_owner_revision,
)
from xuanming.nodes.scene_skill_craft import SceneSkillCraft
from xuanming.player_inbox.errors import StoreUnavailableError as InboxStoreUnavailableError
from xuanming.player_inbox.store import INBOX_REQUIRED_SCHEMA, verify_inbox_schema
from xuanming.protocol import Protocol
from xuanming.trade.commerce_engine import (
    MANUAL_REASON_APPLIED_PARTIAL,
    MANUAL_REASON_FINAL_CREDIT,
    MAX_ATTEMPTS_PER_ACTOR,
    MAX_ATTEMPTS_PER_LISTING,
    CommerceEngine,
    ManualRequest,
    attempt_allowed,
    find_leg,
    mark_manual,
    parse_partial_part,
)
from xuanming.trade.errors import ERR_TRADE_ORDER_STATE_CONFLICT, ManualNotAllowedError, TradeRejection
from xuanming.trade.model import (
    ASSET_STREAM_TRADE_CREDIT,
    ASSET_STREAM_TRADE_DEBIT,
    COMMERCE_DELIVERING,
    COMMERCE_KIND_CONSIGNMENT,
    COMMERCE_KIND_RMB_CONSIGNMENT,
    COMMERCE_KIND_SKILL_BOOK_WANTED,
    COMMERCE_MANUAL,
    COMMERCE_SETTLED,
    COMMERCE_WAITING,
    CURRENCY_GOLD,
    OP_STATUS_APPLIED,
    OP_STATUS_APPLIED_PARTIAL,
    OP_STATUS_REJECTED,
    STAGE_WANTED_OPEN,
    RoleFenceRow as TradeRoleFenceRow,
    RoleOriginal as TradeRoleOriginal,
    TableWantedSkillCatalog,
    TradePolicy,
)
from xuanming.trade.rmb import plan_rmb
from xuanming.trade.service import validate_wanted_skill_tables
from xuanming.trade.store import MemoryTradeStore


# ---------------------------------------------------------------------------
# 1. 560..563 旧悬赏求书接口彻底退役验真
# ---------------------------------------------------------------------------


def test_560_to_563_fully_retired_across_protocol_and_scene():
    protocol = Protocol()
    for mid in (560, 561, 562, 563):
        assert mid not in protocol.routes
    assert 558 in protocol.routes
    assert 559 in protocol.routes
    assert 564 in protocol.routes
    assert 565 in protocol.routes
    assert 573 in protocol.routes
    assert protocol.message_ids["ClientPlayerJubaozhaiCreateWantedSkillBookOrder"] == 582
    assert protocol.message_ids["ClientPlayerJubaozhaiFulfillWantedSkillBookOrder"] == 583
    assert protocol.message_ids["ClientPlayerJubaozhaiListWantedSkillBookOrders"] == 584

    for attr in ("post_wanted_order", "cancel_wanted_order", "list_wanted_orders", "fulfill_wanted_order"):
        assert not hasattr(SceneSkillCraft, attr)
    snap_desc = protocol.message("SkillCraftingStateSnapshot").DESCRIPTOR
    assert "my_wanted_orders" not in snap_desc.fields_by_name
    craft_desc = protocol.message("PlayerCraftComp").DESCRIPTOR
    assert "wanted_orders" not in craft_desc.fields_by_name
    assert "next_wanted_order_seq" not in craft_desc.fields_by_name
    rec_desc = protocol.message("ClientRequestRecord").DESCRIPTOR
    assert "wanted_order_id" not in rec_desc.fields_by_name
    assert "wanted_order_gold" not in rec_desc.fields_by_name


# ---------------------------------------------------------------------------
# 2. 江湖普通生死状未结清预扣银两重发拦截
# ---------------------------------------------------------------------------


def test_jianghu_pending_silver_fee_blocks_eligibility_and_prepare(tmp_path):
    async def run():
        protocol = Protocol()
        tables = ConfigTables(fmt="json")
        store = SqlitePlayerStore(tmp_path / "jh.sqlite")
        rows = PlayerRowCodec(protocol)
        node = protocol.message("NodeInfo", node_id=1, node_uuid="jh-node", zone_id=1)
        scene = SceneNode(protocol, node, store, rows, lambda *_: None)
        scene.tables = tables
        assets = scene.assets = SceneAssets(scene, tables, {})
        jianghu = SceneJianghu(scene, tables, assets, clock=lambda: 1_800_000_000_000)
        scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))

        seed = await store.load_and_claim(101, b"seed-101")
        data = protocol.message("player_database", player_id=101)
        data.currency.values.extend([10_000, 0, 0, 0])
        data.jianghu_component.owner_revision = 1
        data.jianghu_component.pending_silver_fee_battle_id = 9001
        data.jianghu_component.pending_silver_fee_amount = 500
        data.jianghu_component.pending_silver_fee_review_after_ms = 1_800_000_999_000
        await store.commit(seed, rows.project(data), release=True)
        await scene.enter(
            protocol.message(
                "PlayerEnterGameNodeRequest",
                player_id=101,
                session_id=(101 << 17) | 1,
                scene_id=1,
                enter_gs_type=1,
                owner_revision=1,
            ),
            "gate",
        )
        try:
            # 1) CheckJianghuDuelEligibility 银两档见未结清预扣银两立即拦截并标 payer_silver_fee_pending=True
            req_silver = protocol.message(
                "CheckJianghuDuelEligibilityRequest",
                player_ids=[101],
                payer_player_id=101,
                writ_tier=WRIT_TIER_SILVER,
                silver_cost=500,
            )
            resp_silver = jianghu.check_duel_eligibility(req_silver)
            assert not resp_silver.error_message.id
            assert resp_silver.payer_silver_fee_pending is True
            assert resp_silver.payer_has_sufficient_fee is False

            # 2) ValidateAndCollectPrepareFee / CommitPrepareSilverFee 拒绝另一局，同局幂等
            fee = protocol.message("JianghuDuelPrepareFee", writ_tier=WRIT_TIER_SILVER, silver_cost=500)
            p_data = scene.rows.assemble(scene.locations[101].players[101].snapshot)
            err_other, silver_other = jianghu.validate_and_collect_prepare_fee(p_data, 9002, fee, {})
            assert err_other == FEATURE_UNAVAILABLE and silver_other == 0
            assert jianghu.commit_prepare_silver_fee(p_data, 9002, 500) == FEATURE_UNAVAILABLE

            err_same, silver_same = jianghu.validate_and_collect_prepare_fee(p_data, 9001, fee, {})
            assert err_same == 0 and silver_same == 0
            assert jianghu.commit_prepare_silver_fee(p_data, 9001, 500) == 0

            # 3) 过户换手清空待退三字段，之后资格检查放行
            _normalize_owner_revision(p_data.jianghu_component, 2)
            assert p_data.jianghu_component.pending_silver_fee_battle_id == 0
            assert p_data.jianghu_component.pending_silver_fee_amount == 0
            assert p_data.jianghu_component.pending_silver_fee_review_after_ms == 0
            await scene.commit_player_data(scene.locations[101].players[101], p_data)

            resp_after = jianghu.check_duel_eligibility(req_silver)
            assert not resp_after.error_message.id
            assert resp_after.payer_silver_fee_pending is False
            assert resp_after.payer_has_sufficient_fee is True

            # 金帖档不受银两待退字段影响（无金帖仅回 has_sufficient_fee=False, pending=False）
            req_gold = protocol.message(
                "CheckJianghuDuelEligibilityRequest",
                player_ids=[101],
                payer_player_id=101,
                writ_tier=WRIT_TIER_GOLD_ITEM,
            )
            resp_gold = jianghu.check_duel_eligibility(req_gold)
            assert resp_gold.payer_silver_fee_pending is False
        finally:
            await scene.close()
            await assets.close()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# 3. SceneBattle 冻结/复核指标计数 + spectate_chat_pref + 五行幻境公平预设
# ---------------------------------------------------------------------------


def test_battle_freeze_hold_metrics_and_snapshot_fields(tmp_path):
    async def run():
        assert len(FREEZE_HOLD_EVENT_NAMES) == len(FreezeHoldEvent) == 9
        protocol = Protocol()
        tables = ConfigTables(fmt="json")
        store = SqlitePlayerStore(tmp_path / "battle.sqlite")
        rows = PlayerRowCodec(protocol)
        node = protocol.message("NodeInfo", node_id=2, node_uuid="battle-node", zone_id=1)
        scene = SceneNode(protocol, node, store, rows, lambda *_: None)
        scene.tables = tables
        now_ref = [2_000_000_000]

        class _FakeRedis:
            async def eval(self, *_args):
                return None

        battle = scene.battle = SceneBattle(
            scene, tables, now_ms=lambda: now_ref[0], redis=_FakeRedis(),
        )
        scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))

        seed = await store.load_and_claim(201, b"seed-201")
        data = protocol.message("player_database", player_id=201)
        data.profile_component.name = "观战侠士"
        data.profile_component.spectate_chat_pref = 2  # SPECTATE_CHAT_PREF_FORBID
        setattr(data.uint32_pb_component, "class", 1)
        data.level_component.level = 30
        data.derived_attributes_component.health = 100
        data.derived_attributes_component.mana = 100
        # 预置一条已超期的战斗道具预留与待退银两（review_after_ms 远早于 now - 15min）
        old_review = now_ref[0] - 20 * 60_000
        hold = data.item_holds.holds.add(kind=1, hold_id=8801, created_at_ms=old_review - 1000, review_after_ms=old_review)
        hold.lines.add(config_id=20, count=1)
        data.jianghu_component.pending_silver_fee_battle_id = 8801
        data.jianghu_component.pending_silver_fee_amount = 500
        data.jianghu_component.pending_silver_fee_review_after_ms = old_review
        await store.commit(seed, rows.project(data), release=True)
        await scene.enter(
            protocol.message(
                "PlayerEnterGameNodeRequest",
                player_id=201,
                session_id=(201 << 17) | 1,
                scene_id=1,
                enter_gs_type=1,
                owner_revision=1,
            ),
            "gate",
        )
        try:
            player = scene.locations[201].players[201]

            # 1) _snapshot 透传 spectate_chat_pref
            snap = protocol.message("BattlePlayerSnapshot")
            assert battle._snapshot(player, snap) == 0
            assert snap.spectate_chat_pref == 2

            # 2) PREPARING 冻结过期计 PREPARE_EXPIRED
            prep_ctx = protocol.message("InBattleComp", battle_id=7001, state=1, prepare_deadline_ms=now_ref[0] - 1)
            prep_entry = Freeze(prep_ctx, player.session_id)
            battle._freezes[201] = prep_entry
            await battle._expire(201, prep_entry)
            assert battle.freeze_hold_event_count(FreezeHoldEvent.PREPARE_EXPIRED) == 1

            # 3) FIGHTING 冻结：verdict=(1, False) 计 HELD_COMPLETE；verdict=(0, True) 计 HELD_PENDING；
            #    verdict=(2, False) 计 EXPIRED_RELEASED
            fight_ctx = protocol.message("InBattleComp", battle_id=7002, state=2, deadline_ms=now_ref[0] - 200_000)
            fight_entry = Freeze(fight_ctx, player.session_id)
            battle._freezes[201] = fight_entry

            battle._decision = AsyncMock(return_value=(1, False))
            await battle._expire(201, fight_entry)
            assert battle.freeze_hold_event_count(FreezeHoldEvent.HELD_COMPLETE) == 1

            battle._decision = AsyncMock(return_value=(0, True))
            await battle._expire(201, fight_entry)
            assert battle.freeze_hold_event_count(FreezeHoldEvent.HELD_PENDING) == 1

            battle._decision = AsyncMock(return_value=(2, False))
            await battle._expire(201, fight_entry)
            assert battle.freeze_hold_event_count(FreezeHoldEvent.EXPIRED_RELEASED) == 1

            # 4) 超期道具预留 + 待退银两复核计 ITEM_HOLD_OVERDUE 与 DUEL_FEE_OVERDUE（且 10 分钟内节流不重复）
            battle._decision = AsyncMock(return_value=(1, False))
            await battle._review_holds(201)
            assert battle.freeze_hold_event_count(FreezeHoldEvent.ITEM_HOLD_OVERDUE) == 1
            assert battle.freeze_hold_event_count(FreezeHoldEvent.DUEL_FEE_OVERDUE) == 1
            await battle._review_holds(201)
            assert battle.freeze_hold_event_count(FreezeHoldEvent.ITEM_HOLD_OVERDUE) == 1
            assert battle.freeze_hold_event_count(FreezeHoldEvent.DUEL_FEE_OVERDUE) == 1

            metrics_text = battle.render_battle_freeze_metrics()
            assert 'scene_battle_freeze_events_total{event="prepare_expired"} 1' in metrics_text
            assert 'scene_battle_freeze_events_total{event="held_complete"} 1' in metrics_text
            assert 'scene_battle_freeze_events_total{event="held_pending"} 1' in metrics_text
            assert 'scene_battle_freeze_events_total{event="expired_released"} 1' in metrics_text
            assert 'scene_battle_freeze_events_total{event="item_hold_overdue"} 1' in metrics_text
            assert 'scene_battle_freeze_events_total{event="duel_fee_overdue"} 1' in metrics_text
        finally:
            await battle.close()
            await scene.close()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# 4. AFK 月卡投影写失败计数与 Prometheus 指标导出
# ---------------------------------------------------------------------------


def test_afk_pass_projection_failures_counter_and_metrics(tmp_path):
    async def run():
        protocol = Protocol()
        tables = ConfigTables(fmt="json")
        store = SqlitePlayerStore(tmp_path / "afk.sqlite")
        rows = PlayerRowCodec(protocol)
        node = protocol.message("NodeInfo", node_id=3, node_uuid="afk-node", zone_id=1)
        scene = SceneNode(protocol, node, store, rows, lambda *_: None)
        assets = SceneAssets(scene, tables, {})

        class _BrokenRedis:
            async def set(self, *_a, **_kw):
                raise ConnectionError("redis down")

            async def delete(self, *_a, **_kw):
                raise ConnectionError("redis down")

        afk = SceneAfkPass(scene, tables, assets, player_redis=_BrokenRedis(), now_s=lambda: 1_800_000_000)
        assert afk.projection_failures == 0
        ok1 = await afk.sync_projection(301, 1_800_086_400)
        ok2 = await afk.sync_projection(301, 0)
        assert ok1 is False and ok2 is False
        assert afk.projection_failures == 2

        text = afk.render_metrics({"probe": 5, "hosted": 2})
        assert 'scene_afk_hosting_events_total{event="probe"} 5' in text
        assert 'scene_afk_hosting_events_total{event="hosted"} 2' in text
        assert "scene_afk_pass_projection_failures_total 2" in text
        await scene.close()
        await assets.close()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# 5. Guild：静默人工终结 attempts 见证 + 注销清帮战数据与释放灵脉 + 待入账积压 gauge
# ---------------------------------------------------------------------------


def test_guild_resolve_manually_if_quiet_checks_attempts_and_sql_contracts():
    assert "AND `lease_until_ms`=%s AND `last_send_ms`=%s AND `attempts`=%s" in SQL_RESOLVE_ASSET_OP_QUIET
    assert "guild_war_credit_owed" in SQL_COUNT_WAR_CREDIT_OWED_CAPPED and "LIMIT %s" in SQL_COUNT_WAR_CREDIT_OWED_CAPPED

    async def run():
        protocol = Protocol()
        store = MemoryGuildStore(protocol, clock_ms=lambda: 1_000_000)
        leader = RoleOriginal(player_id=1001, account="acc-1001", owner_revision=1)
        guild = await store.create_guild(leader, name="静默测试帮", name_norm="静默测试帮", zone_id=1, now_ms=1_000_000)
        await store._run_tx(lambda tx: tx.credit_member_contribution(guild.guild_id, 1001, 500))

        # 构造一条未决商店购买指令
        bundle = protocol.message("AssetBundle")
        bundle.items.add(config_id=10, count=1)
        buy_op, _ = await store.reserve_shop_order(
            leader,
            guild_id=guild.guild_id,
            goods_id=1,
            count=1,
            required_guild_level=1,
            cost=10,
            limit_count=10,
            period_key_val=202611,
            lease_until_ms=1_000_000,
            payload=bundle.SerializeToString(),
            client_request_id=80001,
            now_ms=1_000_100,
        )
        op_id = buy_op.op_id

        # 首次领取 + 按本地 NOT_HERE 重排（lease 回 0，last_send_ms 不推进，attempts=1）
        p1 = await store.claim_asset_op(op_id, 1_001_000, 1_011_000, 1_060_000, 0xD00D)
        assert p1 is not None
        await store.reschedule_asset_op(p1, 1_061_000, 3, 404, False, True, 1_001_001)
        row1 = await store.get_asset_op(op_id)
        assert row1 is not None and row1.lease_until_ms == 0 and row1.attempts == 1
        witness = QuietWitness(lease_until_ms=row1.lease_until_ms, last_send_ms=row1.last_send_ms, attempts=row1.attempts)

        # 读证据之后再次领取并本地 NOT_HERE 重排：lease 又回 0、last_send_ms 仍不变，仅 attempts 变为 2
        p2 = await store.claim_asset_op(op_id, 1_002_000, 1_012_000, 1_060_000, 0xD00E)
        assert p2 is not None
        await store.reschedule_asset_op(p2, 1_121_000, 3, 404, False, True, 1_002_001)
        row2 = await store.get_asset_op(op_id)
        assert row2 is not None
        assert row2.lease_until_ms == witness.lease_until_ms
        assert row2.last_send_ms == witness.last_send_ms
        assert row2.attempts == witness.attempts + 1

        manual = ManualResolution(op_id=op_id, final_status=OP_STATUS_ABORTED, operator="ops-dave", reason="账本证实从未应用")
        # 旧 witness 必须落空（靠 attempts 拦住）
        assert await store.resolve_manually_if_quiet(manual, witness, 1_002_002) is False
        assert (await store.get_asset_op(op_id)).status == OP_STATUS_PENDING

        # 用最新 witness 人工终结成功，且保留 last_outcome/last_reason 并写入 resolved_by
        fresh = QuietWitness(lease_until_ms=row2.lease_until_ms, last_send_ms=row2.last_send_ms, attempts=row2.attempts)
        assert await store.resolve_manually_if_quiet(manual, fresh, 1_002_003) is True
        done = await store.get_asset_op(op_id)
        assert done.status == OP_STATUS_ABORTED
        assert done.durable == 0
        assert done.last_outcome == 3 and done.last_reason == 404
        assert done.resolved_by == "ops-dave" and done.resolve_reason == "账本证实从未应用"

    asyncio.run(run())


def test_guild_role_fence_purge_cleans_war_rows_disbands_leyline_and_backlog_gauge():
    async def run():
        protocol = Protocol()
        now_ms = 2_000_000
        store = MemoryGuildStore(protocol, clock_ms=lambda: now_ms)
        svc = GuildService(protocol, store, clock_ms=lambda: now_ms)

        leader = RoleOriginal(player_id=2001, account="acc-2001", owner_revision=1)
        other = RoleOriginal(player_id=2002, account="acc-2002", owner_revision=1)
        guild = await store.create_guild(leader, name="单人占旗帮", name_norm="单人占旗帮", zone_id=1, now_ms=now_ms)

        # 安排占领 1 号灵脉、挂守旗、写入两人的 war_credit_owed
        ll = svc.war._ensure_leyline(1, 1, now_ms)
        ll.owner_guild_id = guild.guild_id
        ll.banner_hp = 850
        svc.war._ensure_player(2001, 1, now_ms)
        svc.war._ensure_player(2002, 1, now_ms)
        for pid, oid in ((2001, 701), (2002, 702)):
            await store._run_tx(
                lambda tx, p=pid, o=oid: tx.insert_war_credit_owed(
                    WarCreditOwed(
                        player_id=p,
                        op_id=o,
                        guild_id=guild.guild_id,
                        kind=OP_KIND_WAR_REWARD,
                        tx_type=TX_GUILD_WAR_REWARD,
                        ref_id=1,
                        period_key=20261127,
                        payload=b"\x01",
                        source_id=o,
                        created_ms=now_ms,
                    )
                )
            )
        # 校验积压 gauge 上报与上限截断
        assert await store.count_war_credit_owed(1) == 1
        assert await svc.war.report_war_credit_owed_backlog() == 2
        assert svc.war.war_credit_owed_backlog == 2

        # 读失败时 gauge 保留上一轮值
        orig_count = store.count_war_credit_owed
        store.count_war_credit_owed = AsyncMock(side_effect=RuntimeError("db blip"))
        assert await svc.war.report_war_credit_owed_backlog() == 2
        store.count_war_credit_owed = orig_count

        # 执行墓碑删角（单人帮主 -> 整帮解散）：清 2001 的 war_credit_owed 与个人帮战态，释放灵脉，不动 2002
        tombstone = protocol.message("rolefence.RoleFenceState", player_id=2001, account="", owner_revision=2, version=2)
        res = await store.purge_removed_role(tombstone)
        assert res is not None and res.disbanded is True and res.guild_id == guild.guild_id
        await svc.war.purge_player_war_data(res.player_id)
        await svc.war.on_guild_disbanded(res.guild_id)

        assert ll.owner_guild_id == 0
        assert 2001 not in svc.war._players
        assert 2002 in svc.war._players
        assert await svc.war.report_war_credit_owed_backlog() == 1
        remaining = await store.list_war_credit_owed()
        assert [r.player_id for r in remaining] == [2002]

    asyncio.run(run())


# ---------------------------------------------------------------------------
# 6. Trade：按人限 3 次 + 按单限 256 次、RMB 交付腿部分发放补发不退款、表包校验
# ---------------------------------------------------------------------------


def test_trade_attempt_allowed_and_wanted_per_actor_cap():
    protocol = Protocol()
    w = protocol.message("trade.TradeCommerceWorkflow", quantity_reserved=3)
    w.legs.add(role="buyer_debit_1_a", player_id=5)
    w.legs.add(role="buyer_debit_2_b", player_id=5)
    w.legs.add(role="buyer_debit_3_c", player_id=6)
    w.legs.add(role="seller_debit", player_id=5)
    w.legs.add(role="buyer_receive", player_id=5)
    assert attempt_allowed(w, "buyer_debit_", 5) is True  # 仅 2 次，还可试第 3 次
    w.legs.add(role="buyer_debit_4_d", player_id=5)
    w.quantity_reserved = 4
    assert attempt_allowed(w, "buyer_debit_", 5) is False  # 同人试满 3 次被拦
    assert attempt_allowed(w, "buyer_debit_", 6) is True   # 别人照常允许
    assert attempt_allowed(w, "seller_debit_", 5) is True  # 不同前缀不计入
    w.quantity_reserved = MAX_ATTEMPTS_PER_LISTING
    assert attempt_allowed(w, "buyer_debit_", 7) is False  # 整单试满 256 次

    async def run():
        tables = ConfigTables(fmt="json")
        catalog = validate_wanted_skill_tables(tables)
        assert isinstance(catalog, TableWantedSkillCatalog)
        with pytest.raises(ValueError):
            validate_wanted_skill_tables(None)
        with pytest.raises(ValueError):
            validate_wanted_skill_tables(SimpleNamespace(tables={}))

        store = MemoryTradeStore(protocol, clock_ms=lambda: 5_000_000)
        for pid, acc in ((1, "poster"), (2, "abuser"), (3, "honest")):
            await store.apply_role_fence(
                protocol.message(
                    "rolefence.RoleFenceState",
                    player_id=pid,
                    account=acc,
                    owner_revision=1,
                    version=1,
                )
            )
        seq = [100]

        async def next_id():
            seq[0] += 1
            return seq[0]

        engine = CommerceEngine(
            protocol,
            store,
            commerce_ids=next_id,
            op_ids=next_id,
            policy=TradePolicy(currencies=(CURRENCY_GOLD,)),
            wanted_skills=catalog,
            clock_ms=lambda: 5_000_000,
        )
        # 挑一门合法常规永久技能发榜，并推进至 wanted_open
        reg_scroll = next(r for r in tables["SkillScrollCraft"].rows() if r.enabled and r.rarity_tier == 0)
        skill_row = next(
            r for r in tables["Skill"].rows()
            if getattr(r, "scroll_id", 0) == reg_scroll.id
            and getattr(r, "season_id", 0) == 0
            and getattr(r, "battle_model", 1) == 1
        )
        poster = TradeRoleOriginal(player_id=1, account="poster", owner_revision=1)
        abuser = TradeRoleOriginal(player_id=2, account="abuser", owner_revision=1)
        honest = TradeRoleOriginal(player_id=3, account="honest", owner_revision=1)

        req = protocol.message(
            "trade.CreateWantedSkillBookOrderRequest",
            client_request_id="wanted-post-0001",
            skill_table_id=skill_row.id,
            reward_gold=1000,
        )
        rec = await engine.create_wanted_skill_book_order(poster, req, poster_name="甲")
        # 模拟 buyer_debit 已到账 -> wanted_open
        async def _open(tx):
            r = await tx.lock_commerce(rec.commerce_id)
            wf = protocol.message("trade.TradeCommerceWorkflow")
            wf.ParseFromString(bytes(r.workflow_proto))
            engine._plan(r, wf, 5_000_000)
            find_leg(wf, "buyer_debit").outcome = OP_STATUS_APPLIED
            engine._plan(r, wf, 5_000_000)
            assert wf.stage == STAGE_WANTED_OPEN and r.status == COMMERCE_WAITING
            await tx.save_commerce(r, wf, 5_000_000)

        await store.run_tx(_open)

        # 同一个揭榜人（player 2）连续 3 次零成本失败尝试后被拒，第 3 方（player 3）照常揭榜！
        for idx in range(MAX_ATTEMPTS_PER_ACTOR):
            await engine.fulfill_wanted_skill_book_order(abuser, rec.commerce_id, f"fulfill-abuser-{idx:02d}", 9001)

            async def _reject_debit(tx):
                r = await tx.lock_commerce(rec.commerce_id)
                wf = protocol.message("trade.TradeCommerceWorkflow")
                wf.ParseFromString(bytes(r.workflow_proto))
                wf.legs[-1].outcome = OP_STATUS_REJECTED
                engine._plan(r, wf, 5_000_000)
                assert wf.stage == STAGE_WANTED_OPEN
                await tx.save_commerce(r, wf, 5_000_000)

            await store.run_tx(_reject_debit)

        with pytest.raises(TradeRejection) as exc_info:
            await engine.fulfill_wanted_skill_book_order(abuser, rec.commerce_id, "fulfill-abuser-99", 9001)
        assert exc_info.value.tip_id == ERR_TRADE_ORDER_STATE_CONFLICT

        ok_rec = await engine.fulfill_wanted_skill_book_order(honest, rec.commerce_id, "fulfill-honest-01", 9002)
        assert int(ok_rec.seller_player_id) == 3

    asyncio.run(run())


def test_trade_rmb_delivery_partial_reissues_shortfall_and_never_refunds():
    async def run():
        protocol = Protocol()
        store = MemoryTradeStore(protocol, clock_ms=lambda: 6_000_000)
        for pid, acc in ((1, "seller-rmb"), (2, "buyer-rmb")):
            await store.apply_role_fence(
                protocol.message(
                    "rolefence.RoleFenceState",
                    player_id=pid,
                    account=acc,
                    owner_revision=1,
                    version=1,
                )
            )
        seq = [500]

        async def next_id():
            seq[0] += 1
            return seq[0]

        engine = CommerceEngine(
            protocol,
            store,
            commerce_ids=next_id,
            op_ids=next_id,
            policy=TradePolicy(currencies=(CURRENCY_GOLD,)),
            clock_ms=lambda: 6_000_000,
        )

        # 构造人民币寄售交付部分发放（物品已到账、货币 10 缺口）
        bundle = protocol.message("AssetBundle")
        bundle.items.add(config_id=10, count=2)
        bundle.currencies.add(currency_type=CURRENCY_GOLD, amount=10)
        rec = protocol.message(
            "trade.TradeCommerceRecord",
            commerce_id=21,
            request_key="rmb-21",
            kind=COMMERCE_KIND_RMB_CONSIGNMENT,
            status=COMMERCE_DELIVERING,
            seller_player_id=1,
            buyer_player_id=2,
            version=3,
        )
        w = protocol.message("trade.TradeCommerceWorkflow", stage="rmb_delivery")
        w.seller_identity.account = "seller-rmb"
        w.seller_identity.owner_revision = 1
        w.buyer_identity.account = "buyer-rmb"
        w.buyer_identity.owner_revision = 1
        w.rmb.asset_kind = "ITEMS"
        l1 = w.legs.add(role="seller_debit", player_id=1, stream=ASSET_STREAM_TRADE_DEBIT, op_id=211, outcome=OP_STATUS_APPLIED)
        l1.bundle.CopyFrom(bundle)
        l2 = w.legs.add(
            role="rmb_buyer_credit",
            player_id=2,
            stream=ASSET_STREAM_TRADE_CREDIT,
            op_id=212,
            outcome=OP_STATUS_APPLIED_PARTIAL,
            escrow_owner_id=1,
        )
        l2.bundle.CopyFrom(bundle)
        mark_manual(rec, w, MANUAL_REASON_APPLIED_PARTIAL, "rmb_buyer_credit partial", 6_000_000)

        async def _seed(tx):
            rec.workflow_proto = w.SerializeToString(deterministic=True)
            await tx.insert_commerce(rec)

        await store.run_tx(_seed)

        # 1) partial 仅补发 currency:0 缺口，建 rmb_buyer_credit#2 并回到 rmb_delivery
        partial_req = ManualRequest(
            commerce_id=21,
            expect_version=3,
            action="partial",
            operator="ops-alice",
            reason="补发未到账货币",
            parts=(parse_partial_part("rmb_buyer_credit=currency:0"),),
        )
        res = await engine.resolve_manual(partial_req, execute=True)
        assert res.new_legs == ["rmb_buyer_credit#2"]
        assert res.stage == "rmb_delivery" and res.status == COMMERCE_DELIVERING

        updated = await store.get_commerce(21)
        uw = protocol.message("trade.TradeCommerceWorkflow")
        uw.ParseFromString(bytes(updated.workflow_proto))
        shortfall = find_leg(uw, "rmb_buyer_credit#2")
        assert shortfall is not None and shortfall.escrow_owner_id == 1
        assert len(shortfall.bundle.items) == 0 and len(shortfall.bundle.currencies) == 1

        # 2) 补发腿 #2 失败时：绝不退款（不进 rmb_refund），转人工 final_credit_failed，并可继续 reissue -> #3
        fail_r = protocol.message("trade.TradeCommerceRecord")
        fail_r.CopyFrom(updated)
        fail_w = protocol.message("trade.TradeCommerceWorkflow")
        fail_w.CopyFrom(uw)
        find_leg(fail_w, "rmb_buyer_credit#2").op_id = 213
        find_leg(fail_w, "rmb_buyer_credit#2").outcome = OP_STATUS_REJECTED
        plan_rmb(protocol, engine.policy, fail_r, fail_w, 6_002_000)
        assert fail_r.status == COMMERCE_MANUAL and fail_w.stage == "manual"
        assert fail_w.quarantine.reason == MANUAL_REASON_FINAL_CREDIT

        async def _save_fail(tx):
            cur = await tx.lock_commerce(21)
            cur.status = fail_r.status
            await tx.save_commerce(cur, fail_w, 6_002_000)

        await store.run_tx(_save_fail)
        cur_fail = await store.get_commerce(21)
        reissued = await engine.resolve_manual(
            ManualRequest(
                commerce_id=21,
                expect_version=int(cur_fail.version),
                action="reissue",
                operator="ops-alice",
                reason="重试缺口补发",
            ),
            execute=True,
        )
        assert reissued.new_legs == ["rmb_buyer_credit#3"] and reissued.stage == "rmb_delivery"

        # 3) 补发腿 #3 到账后交付完成 -> rmb_delivered / SETTLED
        ok_r = await store.get_commerce(21)
        ok_w = protocol.message("trade.TradeCommerceWorkflow")
        ok_w.ParseFromString(bytes(ok_r.workflow_proto))
        find_leg(ok_w, "rmb_buyer_credit#3").op_id = 214
        find_leg(ok_w, "rmb_buyer_credit#3").outcome = OP_STATUS_APPLIED
        plan_rmb(protocol, engine.policy, ok_r, ok_w, 6_003_000)
        assert ok_r.status == COMMERCE_SETTLED and ok_w.stage == "rmb_delivered"

        # 4) RMB 托管扣款腿 partial 仍拒绝
        debit_r = protocol.message(
            "trade.TradeCommerceRecord",
            commerce_id=22,
            request_key="rmb-22",
            kind=COMMERCE_KIND_RMB_CONSIGNMENT,
            status=COMMERCE_DELIVERING,
            seller_player_id=1,
            buyer_player_id=2,
            version=1,
        )
        debit_w = protocol.message("trade.TradeCommerceWorkflow", stage="rmb_escrow")
        debit_w.seller_identity.account = "seller-rmb"
        debit_w.seller_identity.owner_revision = 1
        debit_w.buyer_identity.account = "buyer-rmb"
        debit_w.buyer_identity.owner_revision = 1
        debit_w.rmb.asset_kind = "ITEMS"
        dl = debit_w.legs.add(
            role="seller_debit", player_id=1, stream=ASSET_STREAM_TRADE_DEBIT, op_id=221, outcome=OP_STATUS_APPLIED_PARTIAL,
        )
        dl.bundle.CopyFrom(bundle)
        mark_manual(debit_r, debit_w, MANUAL_REASON_APPLIED_PARTIAL, "seller_debit partial", 6_000_000)

        async def _seed_debit(tx):
            debit_r.workflow_proto = debit_w.SerializeToString(deterministic=True)
            await tx.insert_commerce(debit_r)

        await store.run_tx(_seed_debit)
        with pytest.raises(ManualNotAllowedError):
            await engine.resolve_manual(
                ManualRequest(
                    commerce_id=22,
                    expect_version=1,
                    action="partial",
                    operator="ops-alice",
                    reason="试图处理 RMB 托管部分扣出",
                    parts=(parse_partial_part("seller_debit=currency:0"),),
                ),
                execute=False,
            )

    asyncio.run(run())


# ---------------------------------------------------------------------------
# 7. PlayerInbox 启动表结构自检 + Chat/Login 后台上下文白名单解耦
# ---------------------------------------------------------------------------


def test_player_inbox_verify_schema_checks_tables_and_columns():
    async def run():
        all_rows = [
            (table, col)
            for table, cols in INBOX_REQUIRED_SCHEMA.items()
            for col in cols
        ]

        class _FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            async def execute(self, _sql, _params=None):
                return None

            async def fetchall(self):
                return list(self._rows)

        # 全齐通过
        await verify_inbox_schema(_FakeCursor(all_rows))

        # 缺表拒启并提示 -migrate
        missing_table = [(t, c) for t, c in all_rows if t != "player_inbox_audit"]
        with pytest.raises(InboxStoreUnavailableError, match="-migrate"):
            await verify_inbox_schema(_FakeCursor(missing_table))

        # 缺核心列拒启
        missing_col = [(t, c) for t, c in all_rows if not (t == "player_inbox_item" and c == "owner_revision")]
        with pytest.raises(InboxStoreUnavailableError, match="owner_revision"):
            await verify_inbox_schema(_FakeCursor(missing_col))

    asyncio.run(run())


def test_chat_world_window_and_login_enter_game_detach_request_context():
    req_secret: contextvars.ContextVar[str] = contextvars.ContextVar("req_secret", default="")

    async def run():
        # 1) Chat: WorldWindow 后台重读剥离触发请求的 contextvars
        seen_in_chat_load: list[str] = []
        protocol = Protocol()
        class _AllowAllVerifier:
            async def verify_actor(self, _identity) -> None:
                return None

        rules = ChatRules(world_history_cache_ms=500)
        mem = MemoryChatStore(protocol, _AllowAllVerifier(), rules=rules)
        orig_run_tx = mem._driver.run_tx

        async def spy_run_tx(work):
            seen_in_chat_load.append(req_secret.get())
            return await orig_run_tx(work)

        mem._driver.run_tx = spy_run_tx
        token = req_secret.set("caller-session-metadata")
        try:
            assert detach_background_context().run(req_secret.get) == ""
            actor = ActorIdentity(player_id=10, account="acc-10", owner_revision=1)
            await mem.pull(actor, protocol.message("chatpb.PullChatHistoryRequest", channel=CHANNEL_WORLD), 10)
            assert seen_in_chat_load == [""]
        finally:
            req_secret.reset(token)
            await mem.close()

        # 2) Login EnterGame: detach_enter_context 剥离请求级变量并绑定 player_id
        tok = req_secret.set("incoming-grpc-session-secret")
        try:
            detached = enter_game.detach_enter_context(4242)
            assert detached.run(req_secret.get) == ""
            assert detached.run(enter_game.ENTER_GAME_PLAYER_ID.get) == 4242
        finally:
            req_secret.reset(tok)

    asyncio.run(run())
