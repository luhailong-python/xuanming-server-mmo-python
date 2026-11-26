"""零拒审激励视频广告系统（540-542）与交易系统绑定银两（CURRENCY_TYPE_BIND_GOLD = 3）禁售单测。"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.data.asset_inventory import INVALID, balance
from xuanming.data.id_segments import IdSegments
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_rewarded_ad import (
    CURRENCY_BIND_DIAMOND,
    CURRENCY_BIND_GOLD,
    CURRENCY_DIAMOND,
    CURRENCY_GOLD,
    DAY_MS,
    REWARDED_AD_COOLDOWN,
    REWARDED_AD_CURRENCY_BLOCKED,
    REWARDED_AD_DAILY_LIMIT_REACHED,
    REWARDED_AD_DISABLED,
    REWARDED_AD_FROZEN,
    REWARDED_AD_IN_BATTLE,
    REWARDED_AD_LEVEL_NOT_ENOUGH,
    REWARDED_AD_PLATFORM_UNSUPPORTED,
    REWARDED_AD_SESSION_EXPIRED,
    REWARDED_AD_SESSION_INVALID,
    REWARDED_AD_SSV_REQUIRED,
    REWARDED_AD_UNAVAILABLE,
    REWARDED_AD_WATCH_TOO_SHORT,
    SESSION_EXPIRED,
    SESSION_INVALID,
    SESSION_OK,
    SESSION_TOO_SHORT,
    TX_SYSTEM_GRANT,
    SceneRewardedAd,
    SlotConfig,
    effective_claimed_today,
    resolve_game_day,
    validate_slot_config,
    verify_active_session,
)
from xuanming.protocol import Protocol
from xuanming.trade import (
    CURRENCY_BIND_DIAMOND as TRADE_BIND_DIAMOND,
    CURRENCY_BIND_GOLD as TRADE_BIND_GOLD,
    CURRENCY_DIAMOND as TRADE_DIAMOND,
    CURRENCY_GOLD as TRADE_GOLD,
    ERR_TRADE_ASSET_NOT_TRADABLE,
    RmbConsignments,
    TradeConfig,
    TradePolicy,
    TradeRejection,
    is_bound_currency,
    is_known_currency,
    is_tradable_currency,
)

# 2026-11-26 04:00:00 UTC = 2026-11-26 12:00:00 UTC+8
_NOW_MS = 1_795_579_200_000


class _FakeTable:
    def __init__(self, rows):
        self._rows = list(rows)
        self._by_id = {int(r.id): r for r in self._rows}

    def rows(self):
        return tuple(self._rows)

    def get(self, key: int):
        return self._by_id.get(int(key))


def _make_slot_row(
    *,
    slot_id: int = 1,
    enabled: int = 1,
    min_level: int = 10,
    daily_limit: int = 2,
    cooldown_sec: int = 60,
    min_watch_sec: int = 15,
    session_ttl_sec: int = 300,
    currency_type: int = CURRENCY_BIND_GOLD,
    reward_amount: int = 300,
    require_ssv: int = 0,
    platform_mask: int = 3,
):
    return SimpleNamespace(
        id=slot_id,
        enabled=enabled,
        min_level=min_level,
        daily_limit=daily_limit,
        cooldown_sec=cooldown_sec,
        min_watch_sec=min_watch_sec,
        session_ttl_sec=session_ttl_sec,
        currency_type=currency_type,
        reward_amount=reward_amount,
        require_ssv=require_ssv,
        platform_mask=platform_mask,
    )


async def _make_harness(tmp_path, *, level: int = 10, clock_box=None, in_battle=None, slot_rows=None):
    protocol = Protocol()
    tables = ConfigTables()
    if slot_rows is not None:
        tables.tables = dict(tables.tables)
        tables.tables["RewardedAdSlot"] = _FakeTable(slot_rows)
    db_path = tmp_path / "scene_rewarded_ad.sqlite"
    store = SqlitePlayerStore(db_path)
    rows = PlayerRowCodec(protocol)
    emitted_audits = []
    tx_seq = iter(range(920_000, 999_999))

    async def emit_audit(entry):
        emitted_audits.append(entry)

    async def alloc_tx():
        return next(tx_seq)

    info = protocol.message("NodeInfo", node_id=1, node_type=3, zone_id=1, node_uuid="scene-rewarded-ad")
    scene = SceneNode(protocol, info, store, rows, lambda *args, **kwargs: None)
    scene.tables = tables
    scene.create(protocol.message("CreateSceneRequest", config_id=1, scene_id=1))
    assets = SceneAssets(
        scene,
        tables,
        allow_gm=True,
        allocate_item_id=IdSegments.local(db_path, "item"),
        in_battle=in_battle,
        emit_audit=emit_audit,
        allocate_tx_id=alloc_tx,
        anomaly_journal_path=tmp_path / "anomaly.json",
    )
    await assets.anomaly.start()
    assets.register_client_handlers()
    scene.assets = assets

    clock_val = clock_box if clock_box is not None else [_NOW_MS]
    ad = SceneRewardedAd(scene, tables, assets, clock=lambda: clock_val[0])
    ad.register()
    scene.rewarded_ad = ad

    enter_req = protocol.message(
        "PlayerEnterGameNodeRequest",
        player_id=3001,
        scene_id=1,
        home_zone_id=1,
        owner_epoch=1,
        session_id=1,
        login_generation=1,
        owner_revision=1,
    )
    await scene.enter(enter_req, "gate-test")
    player = scene.locations[3001].players[3001]
    data = scene.rows.assemble(player.snapshot)
    data.level_component.level = level
    await scene.commit_player_data(player, data)
    return protocol, tables, store, scene, assets, ad, player, emitted_audits


async def _dispatch(scene: SceneNode, player, method: str, request):
    route = next(
        r for r in scene.protocol.routes.values()
        if r.service == "SceneRewardedAdClientPlayer" and r.method == method
    )
    handler = scene._handlers["SceneRewardedAdClientPlayer"][method]
    world = scene.locations[player.player_id]
    res = handler(world, player, route, request)
    if asyncio.iscoroutine(res):
        res = await res
    return res


def test_rewarded_ad_rules_strict_bind_gold_and_anti_clock_rollback():
    protocol = Protocol()
    # 验证 540..542 协议号与服务路由已同步
    routes = {
        mid: (r.service, r.method)
        for mid, r in protocol.routes.items()
        if r.service == "SceneRewardedAdClientPlayer"
    }
    assert routes == {
        540: ("SceneRewardedAdClientPlayer", "GetRewardedAdInfo"),
        541: ("SceneRewardedAdClientPlayer", "BeginRewardedAdSession"),
        542: ("SceneRewardedAdClientPlayer", "ClaimRewardedAd"),
    }

    base_cfg = SlotConfig(
        slot_id=1,
        enabled=True,
        min_level=10,
        daily_limit=2,
        cooldown_sec=60,
        min_watch_sec=15,
        session_ttl_sec=300,
        currency_type=CURRENCY_BIND_GOLD,
        reward_amount=300,
        require_ssv=False,
        platform_mask=3,
    )
    assert validate_slot_config(base_cfg) is None

    # 严禁配置流通银两 / 灵石 / 绑定灵石，仅允许 CURRENCY_TYPE_BIND_GOLD(3)
    for bad_cur in (CURRENCY_GOLD, CURRENCY_DIAMOND, CURRENCY_BIND_DIAMOND, 99):
        bad = replace(base_cfg, currency_type=bad_cur)
        assert validate_slot_config(bad) is not None

    # 未知平台位必须拒绝
    bad_mask = replace(base_cfg, platform_mask=0x10)
    assert validate_slot_config(bad_mask) is not None

    # 切日(UTC+8 05:00)与抗时钟回拨
    day_pair = resolve_game_day(_NOW_MS)
    assert day_pair is not None
    day_start, next_day = day_pair
    assert next_day == day_start + DAY_MS

    ledger = protocol.message("RewardedAdLedger", day_starts_at_ms=day_start, claimed_today=2)
    assert effective_claimed_today(ledger, day_start) == 2
    assert effective_claimed_today(ledger, day_start - DAY_MS) == 2  # 时钟回拨不重置
    assert effective_claimed_today(ledger, next_day) == 0

    # 会话核验状态机
    ledger.active_session_id = 777
    ledger.active_session_slot_id = 1
    ledger.active_session_issued_ms = _NOW_MS
    assert verify_active_session(ledger, 1, 0, _NOW_MS + 20_000, 15, 300) == SESSION_INVALID
    assert verify_active_session(ledger, 1, 778, _NOW_MS + 20_000, 15, 300) == SESSION_INVALID
    assert verify_active_session(ledger, 2, 777, _NOW_MS + 20_000, 15, 300) == SESSION_INVALID
    assert verify_active_session(ledger, 1, 777, _NOW_MS - 1_000, 15, 300) == SESSION_TOO_SHORT
    assert verify_active_session(ledger, 1, 777, _NOW_MS + 14_999, 15, 300) == SESSION_TOO_SHORT
    assert verify_active_session(ledger, 1, 777, _NOW_MS + 15_000, 15, 300) == SESSION_OK
    assert verify_active_session(ledger, 1, 777, _NOW_MS + 300_000, 15, 300) == SESSION_OK
    assert verify_active_session(ledger, 1, 777, _NOW_MS + 300_001, 15, 300) == SESSION_EXPIRED


def test_rewarded_ad_default_off_and_invalid_currency_fail_closed(tmp_path):
    async def run():
        # 1. 使用真实随仓配表：默认 enabled=0（首审默认关闭红线）
        protocol, tables, _store, scene, assets, ad, player, _ = await _make_harness(tmp_path)
        try:
            assert ad.validate_tables() is True
            row = tables["RewardedAdSlot"].get(1)
            assert row is not None
            assert int(row.enabled) == 0
            assert int(row.currency_type) == CURRENCY_BIND_GOLD

            info_resp = await _dispatch(
                scene, player, "GetRewardedAdInfo",
                protocol.message("GetRewardedAdInfoRequest", client_platform=1),
            )
            assert info_resp.error_message.id == 0
            assert len(info_resp.slots) == 1
            assert info_resp.slots[0].slot_id == 1
            assert info_resp.slots[0].enabled is False
            assert info_resp.slots[0].currency_type == CURRENCY_BIND_GOLD
            assert info_resp.slots[0].reward_amount == 300

            begin_resp = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=1),
            )
            assert begin_resp.error_message.id == REWARDED_AD_DISABLED

            claim_resp = await _dispatch(
                scene, player, "ClaimRewardedAd",
                protocol.message("ClaimRewardedAdRequest", client_request_id=1, slot_id=1, ad_session_id=123),
            )
            assert claim_resp.error_message.id == REWARDED_AD_DISABLED
        finally:
            await assets.close()

        # 2. 误配为流通银两(0)时 fail-closed：validate_tables 返回 False，运行期隐藏并拒载
        protocol2, _t2, _s2, scene2, assets2, ad2, player2, _ = await _make_harness(
            tmp_path / "bad_currency",
            slot_rows=[_make_slot_row(enabled=1, currency_type=CURRENCY_GOLD)],
        )
        try:
            assert ad2.validate_tables() is False
            info2 = await _dispatch(
                scene2, player2, "GetRewardedAdInfo",
                protocol2.message("GetRewardedAdInfoRequest", client_platform=1),
            )
            assert info2.error_message.id == 0
            assert len(info2.slots) == 0

            begin2 = await _dispatch(
                scene2, player2, "BeginRewardedAdSession",
                protocol2.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=1),
            )
            assert begin2.error_message.id == REWARDED_AD_UNAVAILABLE
        finally:
            await assets2.close()

    asyncio.run(run())


def test_rewarded_ad_full_lifecycle_idempotency_limits_ssv_and_guards(tmp_path):
    async def run():
        clock_box = [_NOW_MS]
        battle_flag = [False]
        protocol, _tables, store, scene, assets, ad, player, audits = await _make_harness(
            tmp_path,
            level=9,
            clock_box=clock_box,
            in_battle=lambda _p: battle_flag[0],
            slot_rows=[_make_slot_row(enabled=1, currency_type=CURRENCY_BIND_GOLD, platform_mask=3)],
        )
        try:
            assert ad.validate_tables() is True

            # 1. 平台掩码隔离（platform_mask=3 仅放行微信=1/抖音=2，拒绝 iOS=3）
            resp_ios = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=3),
            )
            assert resp_ios.error_message.id == REWARDED_AD_PLATFORM_UNSUPPORTED

            # 2. 10 级门槛拦截：9 级查询返回 enabled=False，开播返回 LEVEL_NOT_ENOUGH
            info_lv9 = await _dispatch(
                scene, player, "GetRewardedAdInfo",
                protocol.message("GetRewardedAdInfoRequest", client_platform=2),
            )
            assert info_lv9.slots[0].enabled is False
            resp_lv9 = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=2),
            )
            assert resp_lv9.error_message.id == REWARDED_AD_LEVEL_NOT_ENOUGH

            # 升至 10 级
            data = scene.rows.assemble(player.snapshot)
            data.level_component.level = 10
            await scene.commit_player_data(player, data)

            info_lv10 = await _dispatch(
                scene, player, "GetRewardedAdInfo",
                protocol.message("GetRewardedAdInfoRequest", client_platform=2),
            )
            assert info_lv10.slots[0].enabled is True

            # 3. 战斗中拦截
            battle_flag[0] = True
            resp_battle = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=2),
            )
            assert resp_battle.error_message.id == REWARDED_AD_IN_BATTLE
            battle_flag[0] = False

            # 4. 签发第 1 次会话，检验 too_fast / invalid_session / 0 幂等键拦截
            begin1 = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=2),
            )
            assert begin1.error_message.id == 0
            session1 = int(begin1.ad_session_id)
            assert session1 > 0
            assert int(begin1.expires_at_ms) == _NOW_MS + 300_000

            # client_request_id == 0 -> INVALID
            zero_key = await _dispatch(
                scene, player, "ClaimRewardedAd",
                protocol.message("ClaimRewardedAdRequest", client_request_id=0, slot_id=1, ad_session_id=session1),
            )
            assert zero_key.error_message.id == INVALID

            # 观看不足 15 秒 -> WATCH_TOO_SHORT
            clock_box[0] = _NOW_MS + 5_000
            too_fast = await _dispatch(
                scene, player, "ClaimRewardedAd",
                protocol.message("ClaimRewardedAdRequest", client_request_id=10001, slot_id=1, ad_session_id=session1),
            )
            assert too_fast.error_message.id == REWARDED_AD_WATCH_TOO_SHORT

            # 错误会话号 -> SESSION_INVALID
            bad_sess = await _dispatch(
                scene, player, "ClaimRewardedAd",
                protocol.message("ClaimRewardedAdRequest", client_request_id=10001, slot_id=1, ad_session_id=session1 + 1),
            )
            assert bad_sess.error_message.id == REWARDED_AD_SESSION_INVALID

            # 模拟 30 秒广告断线重连后从持久化存储重载快照，挂起会话仍可成功核销
            reloaded_snap = await store.load_and_claim(
                player.player_id, player.snapshot.owner, player.snapshot.owner_epoch,
            )
            assert reloaded_snap is not None
            player.snapshot = reloaded_snap
            reloaded_data = scene.rows.assemble(player.snapshot)
            assert int(reloaded_data.client_request_ledger.rewarded_ad.active_session_id) == session1

            clock_box[0] = _NOW_MS + 35_000
            claim1_req = protocol.message(
                "ClaimRewardedAdRequest", client_request_id=10001, slot_id=1, ad_session_id=session1,
            )
            claim1 = await _dispatch(scene, player, "ClaimRewardedAd", claim1_req)
            assert claim1.error_message.id == 0
            assert claim1.replayed is False
            assert int(claim1.currency_type) == CURRENCY_BIND_GOLD
            assert int(claim1.reward_amount) == 300
            assert int(claim1.currency_balance) == 300
            assert int(claim1.gold_balance) == 0
            assert int(claim1.slot.claimed_today) == 1
            assert int(claim1.slot.cooldown_until_ms) == _NOW_MS + 35_000 + 60_000

            # 核对真实余额与审计流水（仅发放绑定银两 3，流通银两必须为 0）
            after1 = scene.rows.assemble(player.snapshot)
            assert balance(after1, CURRENCY_BIND_GOLD) == 300
            assert balance(after1, CURRENCY_GOLD) == 0
            assert int(after1.client_request_ledger.rewarded_ad.active_session_id) == 0
            assert len(audits) == 1
            assert int(audits[0].tx_type) == TX_SYSTEM_GRANT
            assert int(audits[0].currency_type) == CURRENCY_BIND_GOLD
            assert int(audits[0].currency_delta) == 300
            assert int(audits[0].correlation_id) == session1

            # 5. 同幂等键重放不双发；换参数错键拒回 INVALID
            clock_box[0] = _NOW_MS + 36_000
            replay1 = await _dispatch(scene, player, "ClaimRewardedAd", claim1_req)
            assert replay1.error_message.id == 0
            assert replay1.replayed is True
            assert int(replay1.currency_type) == CURRENCY_BIND_GOLD
            assert int(replay1.reward_amount) == 300
            assert int(replay1.currency_balance) == 300
            assert balance(scene.rows.assemble(player.snapshot), CURRENCY_BIND_GOLD) == 300

            mismatch_req = protocol.message(
                "ClaimRewardedAdRequest", client_request_id=10001, slot_id=1, ad_session_id=session1 + 1,
            )
            mismatch = await _dispatch(scene, player, "ClaimRewardedAd", mismatch_req)
            assert mismatch.error_message.id == INVALID

            # 6. 冷却期(60s)拦截；满冷却后完成第 2 次领取；第 3 次触发每日上限
            clock_box[0] = _NOW_MS + 50_000
            cd_resp = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=1),
            )
            assert cd_resp.error_message.id == REWARDED_AD_COOLDOWN

            second_start = _NOW_MS + 100_000
            clock_box[0] = second_start
            begin2 = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=1),
            )
            assert begin2.error_message.id == 0
            session2 = int(begin2.ad_session_id)
            assert session2 != session1

            clock_box[0] = second_start + 20_000
            claim2 = await _dispatch(
                scene, player, "ClaimRewardedAd",
                protocol.message("ClaimRewardedAdRequest", client_request_id=10002, slot_id=1, ad_session_id=session2),
            )
            assert claim2.error_message.id == 0
            assert int(claim2.currency_balance) == 600
            assert int(claim2.slot.claimed_today) == 2

            clock_box[0] = second_start + 120_000
            limit_resp = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=1),
            )
            assert limit_resp.error_message.id == REWARDED_AD_DAILY_LIMIT_REACHED

            # 7. 跨过 UTC+8 05:00 次日重置；超时(>300s)会话返回 SESSION_EXPIRED
            next_day_ms = _NOW_MS + DAY_MS
            clock_box[0] = next_day_ms
            begin3 = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=1),
            )
            assert begin3.error_message.id == 0

            clock_box[0] = next_day_ms + 301_000
            expired_resp = await _dispatch(
                scene, player, "ClaimRewardedAd",
                protocol.message(
                    "ClaimRewardedAdRequest",
                    client_request_id=10003,
                    slot_id=1,
                    ad_session_id=int(begin3.ad_session_id),
                ),
            )
            assert expired_resp.error_message.id == REWARDED_AD_SESSION_EXPIRED

            # 8. 绑定银两被封禁时在 BeginSession 即拦截，避免白看广告
            assets.global_blocks["currency"].add(CURRENCY_BIND_GOLD)
            blocked_resp = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=1),
            )
            assert blocked_resp.error_message.id == REWARDED_AD_CURRENCY_BLOCKED
            assets.global_blocks["currency"].discard(CURRENCY_BIND_GOLD)

            # 9. 角色冻结/不可写拦截
            player.disconnect_at = 1.0
            frozen_resp = await _dispatch(
                scene, player, "BeginRewardedAdSession",
                protocol.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=1),
            )
            assert frozen_resp.error_message.id == REWARDED_AD_FROZEN
            player.disconnect_at = None
        finally:
            await assets.close()

        # 10. require_ssv=1 时空回执拦截，带回执通过
        ssv_clock = [_NOW_MS]
        p_ssv, _t_ssv, _st_ssv, s_ssv, a_ssv, _ad_ssv, pl_ssv, _au_ssv = await _make_harness(
            tmp_path / "ssv",
            level=10,
            clock_box=ssv_clock,
            slot_rows=[_make_slot_row(enabled=1, require_ssv=1)],
        )
        try:
            b_ssv = await _dispatch(
                s_ssv, pl_ssv, "BeginRewardedAdSession",
                p_ssv.message("BeginRewardedAdSessionRequest", slot_id=1, client_platform=1),
            )
            assert b_ssv.error_message.id == 0
            ssv_clock[0] = _NOW_MS + 20_000
            no_proof = await _dispatch(
                s_ssv, pl_ssv, "ClaimRewardedAd",
                p_ssv.message(
                    "ClaimRewardedAdRequest",
                    client_request_id=20001,
                    slot_id=1,
                    ad_session_id=int(b_ssv.ad_session_id),
                ),
            )
            assert no_proof.error_message.id == REWARDED_AD_SSV_REQUIRED

            with_proof = await _dispatch(
                s_ssv, pl_ssv, "ClaimRewardedAd",
                p_ssv.message(
                    "ClaimRewardedAdRequest",
                    client_request_id=20002,
                    slot_id=1,
                    ad_session_id=int(b_ssv.ad_session_id),
                    ssv_proof="ssv-receipt-001",
                ),
            )
            assert with_proof.error_message.id == 0
            assert int(with_proof.currency_balance) == 300
        finally:
            await a_ssv.close()

    asyncio.run(run())


def test_trade_rejects_bind_gold_across_currency_policy_rmb_and_config():
    protocol = Protocol()
    enum_desc = protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
    assert {int(k): v.name for k, v in enum_desc.values_by_number.items()} == {
        TRADE_GOLD: "CURRENCY_TYPE_GOLD",
        TRADE_DIAMOND: "CURRENCY_TYPE_DIAMOND",
        TRADE_BIND_DIAMOND: "CURRENCY_TYPE_BIND_DIAMOND",
        TRADE_BIND_GOLD: "CURRENCY_TYPE_BIND_GOLD",
    }

    # 1. currencytype 等价辅助函数
    for cur in (TRADE_GOLD, TRADE_DIAMOND, TRADE_BIND_DIAMOND, TRADE_BIND_GOLD):
        assert is_known_currency(cur) is True
    for unknown in (4, 0x7FFFFFFF + 1, 0xFFFFFFFF):
        assert is_known_currency(unknown) is False

    assert is_bound_currency(TRADE_BIND_DIAMOND) is True
    assert is_bound_currency(TRADE_BIND_GOLD) is True
    assert is_bound_currency(TRADE_GOLD) is False
    assert is_bound_currency(TRADE_DIAMOND) is False

    assert is_tradable_currency(TRADE_GOLD) is True
    assert is_tradable_currency(TRADE_DIAMOND) is True
    for non_tradable in (TRADE_BIND_DIAMOND, TRADE_BIND_GOLD, 4, 0xFFFFFFFF):
        assert is_tradable_currency(non_tradable) is False

    # 2. TradePolicy 即使绕过配置把绑定银两/绑定灵石配进白名单，仍一律拒绝选择与报价
    policy = TradePolicy(currencies=(TRADE_GOLD, TRADE_DIAMOND, TRADE_BIND_DIAMOND, TRADE_BIND_GOLD))
    policy.validate_selection(protocol.message("trade.TradeAssetSelection", currency_type=TRADE_DIAMOND, currency_amount=5))
    for bound_cur in (TRADE_BIND_DIAMOND, TRADE_BIND_GOLD):
        with pytest.raises(TradeRejection) as exc_info:
            policy.validate_selection(
                protocol.message("trade.TradeAssetSelection", currency_type=bound_cur, currency_amount=5),
            )
        assert exc_info.value.tip_id == ERR_TRADE_ASSET_NOT_TRADABLE
        assert policy.valid_price(5, bound_cur) is False
    assert policy.valid_price(5, TRADE_DIAMOND) is True
    assert TradePolicy(currencies=(TRADE_DIAMOND,)).valid_price(5, TRADE_GOLD) is False

    # 3. 人民币寄售即使绕过配置把绑定银两/绑定灵石配进 currency_types，仍一律拒绝
    fake_engine = SimpleNamespace(policy=policy)
    rmb = RmbConsignments(
        protocol,
        fake_engine,
        mall=None,
        directory=None,
        sealer=None,
        currency_types=(TRADE_GOLD, TRADE_DIAMOND, TRADE_BIND_DIAMOND, TRADE_BIND_GOLD),
    )
    ok_req = protocol.message(
        "trade.CreateRmbConsignmentRequest",
        assets={"currency_type": TRADE_DIAMOND, "currency_amount": 10},
    )
    assert rmb._classify_selection(ok_req) == "CURRENCY"
    for bound_cur in (TRADE_BIND_DIAMOND, TRADE_BIND_GOLD):
        bad_req = protocol.message(
            "trade.CreateRmbConsignmentRequest",
            assets={"currency_type": bound_cur, "currency_amount": 10},
        )
        with pytest.raises(TradeRejection) as exc_info:
            rmb._classify_selection(bad_req)
        assert exc_info.value.tip_id == ERR_TRADE_ASSET_NOT_TRADABLE

    # 4. TradeConfig.validate 拒绝把绑定银两配进 commerce_currencies 或 rmb_currencies
    secret_bytes = b"s" * 32
    base_cfg = TradeConfig(
        advertise_host="127.0.0.1",
        data_service="127.0.0.1:50500",
        asset_op_secret=secret_bytes,
        coordinator_secret=secret_bytes,
        role_fence_secret=secret_bytes,
        commerce_currencies=(TRADE_GOLD, TRADE_BIND_GOLD),
    )
    with pytest.raises(ValueError, match="non-tradable currency in commerce_currencies"):
        base_cfg.validate()

    rmb_cfg = TradeConfig(
        advertise_host="127.0.0.1",
        data_service="127.0.0.1:50500",
        asset_op_secret=secret_bytes,
        coordinator_secret=secret_bytes,
        role_fence_secret=secret_bytes,
        mall_url="https://mall.example.com",
        mall_gateway_secret=b"g" * 32,
        mall_delivery_secret=b"d" * 32,
        account_directory_secret=b"a" * 32,
        rmb_enabled=True,
        rmb_currencies=(TRADE_GOLD, TRADE_BIND_GOLD),
    )
    with pytest.raises(ValueError, match="non-tradable currency in rmb_currencies"):
        rmb_cfg.validate()
