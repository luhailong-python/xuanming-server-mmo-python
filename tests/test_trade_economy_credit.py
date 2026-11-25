"""聚宝斋卖家店铺信用、近 7 日中位参考价（p50）、当面交易防骗额度与定向寄售全链路测试。"""

from __future__ import annotations

import asyncio
import functools
from contextlib import AsyncExitStack
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from test_monopoly import TOKEN, _enter, _settings
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata
from xuanming.login.caller_auth import CallerVerifier
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.trade import (
    COMMERCE_KIND_AUCTION_LOT,
    COMMERCE_KIND_CONSIGNMENT,
    COMMERCE_KIND_MARKET_MATCH,
    COMMERCE_KIND_PLAYER_TRADE,
    COMMERCE_KIND_RMB_CONSIGNMENT,
    COMMERCE_SETTLED,
    CURRENCY_GOLD,
    DAY_MS,
    DEFAULT_COOLING_LOCK_DURATION_MS,
    DEFAULT_MAX_REFERENCE_DEVIATION_PCT,
    DEFAULT_MIN_FRIEND_DURATION_MS,
    DEFAULT_SELLER_INITIAL_SCORE,
    DEFAULT_SELLER_INITIAL_STAR_TENTHS,
    ERR_INVALID_PARAMETER,
    ERR_TRADE_ASSET_NOT_TRADABLE,
    ERR_TRADE_AUCTION_ENDED,
    ERR_TRADE_CANNOT_TRADE_SELF,
    ERR_TRADE_COUNTERPARTY_UNKNOWN,
    ERR_TRADE_FEATURE_DISABLED,
    ERR_TRADE_ORDER_NOT_FOUND,
    ERR_TRADE_ORDER_STATE_CONFLICT,
    ERR_TRADE_REQUEST_CONFLICT,
    ERR_TRADE_TITLE_SENSITIVE,
    PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED,
    PLAYER_TRADE_MODE_STRICT_CONSUMABLES,
    SELLER_CREDIT_SCHEMA_DDL,
    SELLER_CREDIT_TABLES,
    SELLER_RATING_NEGATIVE,
    SELLER_RATING_NEUTRAL,
    SELLER_RATING_POSITIVE,
    SELLER_RISK_TIER_NEW_OR_TRANSFERRED,
    SELLER_RISK_TIER_RESTRICTED,
    SELLER_RISK_TIER_TRUSTED,
    TEXT_VERDICT_PASS,
    TEXT_VERDICT_REJECT,
    CommerceEngine,
    CompletedTradeFact,
    ContentCheckResult,
    MarketFillSample,
    MemoryTradeStore,
    PlayerTradeGuard,
    PlayerTradePolicy,
    ReferencePrice,
    RmbConsignments,
    RoleOriginal,
    SellerCreditKey,
    SellerCreditPolicy,
    SellerCreditProfile,
    SellerCreditService,
    SellerEnvironmentEvent,
    SellerReviewRecord,
    SellerReviewRequest,
    SellerViolationRecord,
    SqlTradeStore,
    StoreDailyPlayerTradeTracker,
    StoreOwnershipLookup,
    StoreReferencePriceReader,
    TEXT_SCENE_TRADE_TITLE,
    TradePolicy,
    TradeRejection,
    TradeService,
    compute_reference_price,
    settled_trade_fact,
)
from xuanming.trade.mall_adapter import MallAdapter, parse_rmb_listing_view
from xuanming.trade.store import _MemoryTx

PROTOCOL = Protocol()
SECRET_COORDINATOR = b"role-fence-coordinator-secret-01234567"


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeClock:
    def __init__(self, start_ms: int = 1_760_000_000_000) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, delta_ms: int) -> int:
        self.now_ms += delta_ms
        return self.now_ms


class StubRoleDir:
    """可控的角色护栏目录与归属区桩。"""

    def __init__(self) -> None:
        self.states: dict[int, object] = {}
        self.zones: dict[int, int] = {}

    def register(self, player_id: int, account: str, owner_revision: int = 1, home_zone: int = 1) -> None:
        self.states[player_id] = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=player_id,
            account=account,
            owner_revision=owner_revision,
            version=owner_revision,
            home_zone=home_zone,
        )
        self.zones[player_id] = home_zone

    async def get_role_fence_state(self, player_id: int):
        return self.states.get(int(player_id))

    async def home_zone(self, player_id: int) -> int:
        return self.zones.get(int(player_id), 0)


class StubFriends:
    def __init__(self, rels: dict[tuple[int, int], tuple[bool, int]]) -> None:
        self._rels = rels

    async def friendship_duration_ms(self, player_a: int, player_b: int, now_ms: int) -> tuple[bool, int]:
        del now_ms
        return self._rels.get((player_a, player_b), self._rels.get((player_b, player_a), (False, 0)))


class StubContentGuard:
    def __init__(self, blocked_word: str = "禁词") -> None:
        self.blocked_word = blocked_word

    async def check_text(self, scene: int, player_id: int, text: str) -> ContentCheckResult:
        del scene, player_id
        if self.blocked_word in text:
            return ContentCheckResult(verdict=TEXT_VERDICT_REJECT, category=7)
        return ContentCheckResult(verdict=TEXT_VERDICT_PASS, category=0)


async def _seed_commerce_record(store: MemoryTradeStore, rec, wf) -> None:
    rec.workflow_proto = wf.SerializeToString(deterministic=True)

    async def _tx(tx: _MemoryTx) -> None:
        await tx.insert_commerce(rec)

    await store.run_tx(_tx)


# -- 1. SellerCreditService 核心规则（累计/过户清空/异地换机清 0/防刷分/违约） ---------------------------

@_async_test
async def test_seller_credit_accumulates_and_resets_completely_on_transfer():
    base_now = 1_760_000_000_000
    role_dir = StubRoleDir()
    role_dir.register(1001, "old-owner-acct", owner_revision=1)
    store = MemoryTradeStore(PROTOCOL, role_directory=role_dir)
    svc = SellerCreditService(
        store=store,
        ownership=StoreOwnershipLookup(store),
        policy=SellerCreditPolicy(min_score_amount_minor=100),
    )

    for i in range(1, 21):
        fact = CompletedTradeFact(
            order_id=f"order-old-{i:02d}",
            seller_player_id=1001,
            seller_account="old-owner-acct",
            seller_owner_revision=1,
            buyer_player_id=2000 + i,
            buyer_account=f"buyer-acct-{i:02d}",
            amount_minor=500,
            settled_at_ms=base_now + i * 1000,
        )
        rec, _, replayed = await svc.submit_review(
            SellerReviewRequest(trade=fact, rating=SELLER_RATING_POSITIVE),
            fact.settled_at_ms + 500,
        )
        assert not replayed
        assert rec.counted_for_score

    old_prof = await svc.get_profile(1001, base_now + 30_000)
    assert old_prof.completed_orders == 20
    assert old_prof.positive_reviews == 20
    assert old_prof.credit_score == 100
    assert old_prof.star_rating_tenths == 50
    assert old_prof.positive_rate_bps == 10000
    assert old_prof.risk_tier == SELLER_RISK_TIER_TRUSTED
    assert not old_prof.ownership_changed

    # 角色过户给新号主 -> 历史信用清空并进入 new_or_transferred 保护档
    transfer_time = base_now + 100_000
    role_dir.register(1001, "new-owner-acct", owner_revision=2)
    await svc.on_character_transferred(1001, "new-owner-acct", 2, transfer_time)

    new_prof = await svc.get_profile(1001, transfer_time + 1_000)
    assert new_prof.owner_account == "new-owner-acct"
    assert new_prof.owner_revision == 2
    assert new_prof.ownership_changed is True
    assert new_prof.ownership_changed_at_ms == transfer_time
    assert new_prof.completed_orders == 0
    assert new_prof.positive_reviews == 0
    assert new_prof.credit_score == DEFAULT_SELLER_INITIAL_SCORE
    assert new_prof.star_rating_tenths == DEFAULT_SELLER_INITIAL_STAR_TENTHS
    assert new_prof.risk_tier == SELLER_RISK_TIER_NEW_OR_TRANSFERRED

    # 过户前旧单迟到好评只落 revision=1，不污染新号主 revision=2
    late_old = CompletedTradeFact(
        order_id="order-old-late-99",
        seller_player_id=1001,
        seller_account="old-owner-acct",
        seller_owner_revision=1,
        buyer_player_id=2999,
        buyer_account="buyer-late-99",
        amount_minor=500,
        settled_at_ms=transfer_time - 1_000,
    )
    await svc.submit_review(SellerReviewRequest(trade=late_old, rating=SELLER_RATING_POSITIVE), transfer_time + 2_000)
    still_clean = await svc.get_profile(1001, transfer_time + 3_000)
    assert still_clean.completed_orders == 0
    assert still_clean.positive_reviews == 0
    assert still_clean.credit_score == DEFAULT_SELLER_INITIAL_SCORE


@_async_test
async def test_seller_credit_anti_wash_env_wipe_and_violations():
    now = 1_760_000_000_000
    role_dir = StubRoleDir()
    role_dir.register(5001, "seller-env", owner_revision=1)
    store = MemoryTradeStore(PROTOCOL, role_directory=role_dir)
    svc = SellerCreditService(
        store=store,
        ownership=StoreOwnershipLookup(store),
        policy=SellerCreditPolicy(
            max_scored_reviews_per_pair_in_window=1,
            min_score_amount_minor=100,
            post_env_change_score_cooldown_ms=5_000,
        ),
    )

    # 同账号自评拒绝
    with pytest.raises(TradeRejection) as exc_self:
        await svc.submit_review(
            SellerReviewRequest(
                trade=CompletedTradeFact(
                    order_id="self-ord",
                    seller_player_id=5001,
                    seller_account="seller-env",
                    seller_owner_revision=1,
                    buyer_player_id=5002,
                    buyer_account="seller-env",
                    amount_minor=500,
                    settled_at_ms=now,
                ),
                rating=SELLER_RATING_POSITIVE,
            ),
            now + 100,
        )
    assert exc_self.value.tip_id == ERR_TRADE_CANNOT_TRADE_SELF

    # 超出 7 天评价窗口拒绝
    with pytest.raises(TradeRejection) as exc_end:
        await svc.submit_review(
            SellerReviewRequest(
                trade=CompletedTradeFact(
                    order_id="expired-ord",
                    seller_player_id=5001,
                    seller_account="seller-env",
                    seller_owner_revision=1,
                    buyer_player_id=6000,
                    buyer_account="buyer-exp",
                    amount_minor=500,
                    settled_at_ms=now,
                ),
                rating=SELLER_RATING_POSITIVE,
            ),
            now + 8 * DAY_MS,
        )
    assert exc_end.value.tip_id == ERR_TRADE_AUCTION_ENDED

    # 基线环境 + 2 笔真实好评 (60 -> 64)
    p0, cleared = await svc.record_seller_environment(
        SellerEnvironmentEvent(5001, "seller-env", 1, ip="203.0.113.10", device_id="DEV-AAA", observed_at_ms=now)
    )
    assert not cleared and p0.credit_score == 60
    for i in (1, 2):
        await svc.submit_review(
            SellerReviewRequest(
                trade=CompletedTradeFact(
                    order_id=f"init-{i}",
                    seller_player_id=5001,
                    seller_account="seller-env",
                    seller_owner_revision=1,
                    buyer_player_id=5900 + i,
                    buyer_account=f"init-buyer-{i}",
                    buyer_ip=f"192.0.2.{i}",
                    buyer_device_id=f"init-dev-{i}",
                    amount_minor=500,
                    settled_at_ms=now + 500,
                ),
                rating=SELLER_RATING_POSITIVE,
            ),
            now + 1_000,
        )

    # 同设备换 IP（出差/切 5G）不扣分；同 IP 换新设备记入常用设备不扣分
    p_travel, cleared = await svc.record_seller_environment(
        SellerEnvironmentEvent(5001, "seller-env", 1, ip="198.51.100.22", device_id="dev-aaa", observed_at_ms=now + 2_000)
    )
    assert not cleared and p_travel.credit_score == 64
    p_pc, cleared = await svc.record_seller_environment(
        SellerEnvironmentEvent(5001, "seller-env", 1, ip="198.51.100.22", device_id="dev-home-pc", observed_at_ms=now + 3_000)
    )
    assert not cleared and p_pc.credit_score == 64

    # 异地 + 陌生新设备 -> 直接清 0
    p_wiped, cleared = await svc.record_seller_environment(
        SellerEnvironmentEvent(5001, "seller-env", 1, ip="198.51.100.88", device_id="dev-ccc", observed_at_ms=now + 8_000)
    )
    assert cleared is True
    assert p_wiped.env_credit_cleared is True
    assert p_wiped.credit_score == 0
    assert p_wiped.star_rating_tenths == 10
    assert p_wiped.risk_tier == SELLER_RISK_TIER_RESTRICTED

    # 冷静期内好评留痕但不涨分
    rec_cool, p_cool, _ = await svc.submit_review(
        SellerReviewRequest(
            trade=CompletedTradeFact(
                order_id="cool-ord",
                seller_player_id=5001,
                seller_account="seller-env",
                seller_owner_revision=1,
                buyer_player_id=6001,
                buyer_account="buyer-clean-1",
                buyer_ip="192.0.2.10",
                buyer_device_id="dev-buyer-1",
                amount_minor=500,
                settled_at_ms=now + 9_000,
            ),
            rating=SELLER_RATING_POSITIVE,
        ),
        now + 10_000,
    )
    assert not rec_cool.counted_for_score and p_cool.credit_score == 0

    # 过冷静期后同 IP / 同设备互评不计分
    rec_same_ip, p_same_ip, _ = await svc.submit_review(
        SellerReviewRequest(
            trade=CompletedTradeFact(
                order_id="same-ip-ord",
                seller_player_id=5001,
                seller_account="seller-env",
                seller_owner_revision=1,
                buyer_player_id=6002,
                buyer_account="buyer-same-ip",
                buyer_ip="198.51.100.88",
                buyer_device_id="dev-buyer-2",
                amount_minor=500,
                settled_at_ms=now + 14_000,
            ),
            rating=SELLER_RATING_POSITIVE,
        ),
        now + 15_000,
    )
    assert not rec_same_ip.counted_for_score and p_same_ip.credit_score == 0

    # 独立买家从 0 -> 2 起步，随后同设备换小号被拦截
    rec_real, p_real, _ = await svc.submit_review(
        SellerReviewRequest(
            trade=CompletedTradeFact(
                order_id="real-1",
                seller_player_id=5001,
                seller_account="seller-env",
                seller_owner_revision=1,
                buyer_player_id=6010,
                buyer_account="alt-1",
                buyer_ip="192.0.2.50",
                buyer_device_id="emu-1",
                amount_minor=500,
                settled_at_ms=now + 15_000,
            ),
            rating=SELLER_RATING_POSITIVE,
        ),
        now + 16_000,
    )
    assert rec_real.counted_for_score and p_real.credit_score == 2

    rec_alt, p_alt, _ = await svc.submit_review(
        SellerReviewRequest(
            trade=CompletedTradeFact(
                order_id="alt-same-dev",
                seller_player_id=5001,
                seller_account="seller-env",
                seller_owner_revision=1,
                buyer_player_id=6011,
                buyer_account="alt-2",
                buyer_ip="192.0.2.51",
                buyer_device_id="emu-1",
                amount_minor=500,
                settled_at_ms=now + 15_000,
            ),
            rating=SELLER_RATING_POSITIVE,
        ),
        now + 16_100,
    )
    assert not rec_alt.counted_for_score and p_alt.credit_score == 2

    # 违约扣分幂等且不可借再次异地换机洗白
    p_viol = await svc.record_violation(
        SellerViolationRecord("CASE-1", "real-1", 5001, "seller-env", 1, "fraud", now + 17_000)
    )
    assert p_viol.violation_count == 1
    p_viol_replay = await svc.record_violation(
        SellerViolationRecord("CASE-1", "real-1", 5001, "seller-env", 1, "fraud", now + 17_500)
    )
    assert p_viol_replay.violation_count == 1
    p_wash, cleared2 = await svc.record_seller_environment(
        SellerEnvironmentEvent(5001, "seller-env", 1, ip="198.51.100.99", device_id="dev-wash", observed_at_ms=now + 18_000)
    )
    assert cleared2 and p_wash.violation_count == 1 and p_wash.risk_tier == SELLER_RISK_TIER_RESTRICTED


def test_settled_trade_fact_extracts_official_escrow_only():
    now = 1_760_000_000_000
    w_base = PROTOCOL.message("trade.TradeCommerceWorkflow")
    w_base.seller_identity.account = "acct-seller"
    w_base.seller_identity.owner_revision = 2
    w_base.buyer_identity.account = "acct-buyer"
    w_base.buyer_identity.owner_revision = 1

    # 当面交易 (kind=2) 不计入
    p2p = PROTOCOL.message(
        "trade.TradeCommerceRecord",
        commerce_id=11,
        kind=COMMERCE_KIND_PLAYER_TRADE,
        status=COMMERCE_SETTLED,
        seller_player_id=101,
        buyer_player_id=202,
    )
    assert settled_trade_fact(p2p, w_base, now) is None

    # 固定价寄售 (kind=3) 计入
    consign = PROTOCOL.message(
        "trade.TradeCommerceRecord",
        commerce_id=12,
        kind=COMMERCE_KIND_CONSIGNMENT,
        status=COMMERCE_SETTLED,
        seller_player_id=101,
        buyer_player_id=202,
        unit_price=8800,
    )
    fact = settled_trade_fact(consign, w_base, now)
    assert fact is not None and fact.order_id == "commerce:12" and fact.amount_minor == 8800

    # 市场母单 (kind=4, side!=0) 不计入，子单 (side=0) 计入
    parent_mkt = PROTOCOL.message(
        "trade.TradeCommerceRecord",
        commerce_id=13,
        kind=COMMERCE_KIND_MARKET_MATCH,
        side=2,
        status=COMMERCE_SETTLED,
        seller_player_id=101,
        buyer_player_id=202,
        unit_price=100,
        quantity=5,
    )
    assert settled_trade_fact(parent_mkt, w_base, now) is None
    fill_mkt = PROTOCOL.message(
        "trade.TradeCommerceRecord",
        commerce_id=14,
        kind=COMMERCE_KIND_MARKET_MATCH,
        side=0,
        status=COMMERCE_SETTLED,
        seller_player_id=101,
        buyer_player_id=202,
        unit_price=100,
        quantity=5,
    )
    fact_fill = settled_trade_fact(fill_mkt, w_base, now)
    assert fact_fill is not None and fact_fill.amount_minor == 500

    # 拍卖 (kind=5) 取最高出价者与出价
    w_lot = PROTOCOL.message("trade.TradeCommerceWorkflow", highest_bidder=303, highest_bid=12000)
    w_lot.seller_identity.CopyFrom(w_base.seller_identity)
    w_lot.buyer_identity.CopyFrom(w_base.buyer_identity)
    lot = PROTOCOL.message(
        "trade.TradeCommerceRecord",
        commerce_id=15,
        kind=COMMERCE_KIND_AUCTION_LOT,
        status=COMMERCE_SETTLED,
        seller_player_id=101,
    )
    fact_lot = settled_trade_fact(lot, w_lot, now)
    assert fact_lot is not None and fact_lot.buyer_player_id == 303 and fact_lot.amount_minor == 12000

    # 非角色 RMB 寄售 (kind=6) 优先复用 mall_order_id；角色过户排除
    w_rmb = PROTOCOL.message("trade.TradeCommerceWorkflow")
    w_rmb.rmb.seller_account = "acct-seller"
    w_rmb.rmb.seller_owner_revision = 2
    w_rmb.rmb.buyer_account = "acct-buyer"
    w_rmb.rmb.price_fen = 3500
    w_rmb.rmb.mall_order_id = "00112233-4455-6677-8899-aabbccddeeff"
    rmb_rec = PROTOCOL.message(
        "trade.TradeCommerceRecord",
        commerce_id=16,
        kind=COMMERCE_KIND_RMB_CONSIGNMENT,
        status=COMMERCE_SETTLED,
        seller_player_id=101,
        buyer_player_id=202,
    )
    fact_rmb = settled_trade_fact(rmb_rec, w_rmb, now)
    assert fact_rmb is not None and fact_rmb.order_id == "00112233-4455-6677-8899-aabbccddeeff"
    w_rmb.rmb.character_player_id = 101
    assert settled_trade_fact(rmb_rec, w_rmb, now) is None


# -- 2. 近 7 日中位参考价 p50 与 PlayerTradeGuard --------------------------------------------------------

def test_compute_reference_price_filters_wash_and_calculates_p50():
    now = 1_760_000_000_000
    # 9 条有效 + 2 条同账号对敲 + 1 条 8 天前过期 -> 样本不足 10 笔
    samples = [
        MarketFillSample(3403, CURRENCY_GOLD, 1000 + i * 100, 1, f"s{i}", f"b{i}", now - 1000)
        for i in range(9)
    ]
    samples.append(MarketFillSample(3403, CURRENCY_GOLD, 999999, 1, "wash", "wash", now - 500))
    samples.append(MarketFillSample(3403, CURRENCY_GOLD, 10, 1, "s-old", "b-old", now - 8 * DAY_MS))
    low_ref = compute_reference_price(samples, 3403, CURRENCY_GOLD, now)
    assert low_ref.insufficient_sample is True
    assert low_ref.sample_count == 9
    assert low_ref.median_unit_price == 0

    # 补到 10 笔：价格 1000..1900 -> 偶数 p50 = (1400 + 1500) // 2 = 1450
    samples.append(MarketFillSample(3403, CURRENCY_GOLD, 1900, 1, "s9", "b9", now - 1000))
    even_ref = compute_reference_price(samples, 3403, CURRENCY_GOLD, now)
    assert even_ref.insufficient_sample is False
    assert even_ref.sample_count == 10
    assert even_ref.median_unit_price == 1450

    # 补到 11 笔：加入极高异常值 999999 -> 中位数稳健落在 1500
    samples.append(MarketFillSample(3403, CURRENCY_GOLD, 999999, 1, "s10", "b10", now - 500))
    odd_ref = compute_reference_price(samples, 3403, CURRENCY_GOLD, now)
    assert odd_ref.sample_count == 11
    assert odd_ref.median_unit_price == 1500


@_async_test
async def test_player_trade_guard_strict_and_acquaintance_modes():
    now = 1_760_000_000_000

    class FixedRef:
        async def reference_price(self, item_id: int, cur: int, now_ms: int) -> ReferencePrice:
            del now_ms
            if item_id == 9001:
                return ReferencePrice(item_id, cur, median_unit_price=10_000, sample_count=20, insufficient_sample=False)
            return ReferencePrice(item_id, cur, 0, 0, True)

    class FixedOutflow:
        def __init__(self, used: int) -> None:
            self.used = used

        async def daily_currency_outflow(self, player_id: int, currency_type: int, day_start_ms: int) -> int:
            del player_id, currency_type, day_start_ms
            return self.used

    # 方案 A：仅白名单耗材 + 每日银两上限
    guard_a = PlayerTradeGuard(
        policy=PlayerTradePolicy(
            mode=PLAYER_TRADE_MODE_STRICT_CONSUMABLES,
            consumable_items=(501,),
            max_daily_currency_amount=5_000,
        ),
        daily_out=FixedOutflow(used=2_000),
    )
    ok_seller = PROTOCOL.message("trade.TradeAssetSelection", currency_type=CURRENCY_GOLD, currency_amount=3_000)
    ok_buyer = PROTOCOL.message("trade.TradeAssetSelection")
    ok_buyer.items[501] = 2
    res_a = await guard_a.evaluate(101, 202, ok_seller, ok_buyer, now)
    assert res_a.reason == "strict_consumables_ok"

    # 超单日剩余额度 (2000 + 3001 > 5000) 被拦截
    over_seller = PROTOCOL.message("trade.TradeAssetSelection", currency_type=CURRENCY_GOLD, currency_amount=3_001)
    with pytest.raises(TradeRejection) as exc_cap:
        await guard_a.evaluate(101, 202, over_seller, ok_buyer, now)
    assert exc_cap.value.tip_id == ERR_TRADE_ASSET_NOT_TRADABLE

    # 非耗材被方案 A 拦截
    bad_item = PROTOCOL.message("trade.TradeAssetSelection")
    bad_item.items[9001] = 1
    with pytest.raises(TradeRejection) as exc_item:
        await guard_a.evaluate(101, 202, bad_item, ok_buyer, now)
    assert exc_item.value.tip_id == ERR_TRADE_ASSET_NOT_TRADABLE

    # 方案 B：熟人 7 天 + 参考价偏离保护 + 冷静锁
    guard_b = PlayerTradeGuard(
        policy=PlayerTradePolicy(
            mode=PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED,
            max_reference_deviation_pct=50,
            allow_acquaintance_cooling_lock=True,
        ),
        friends=StubFriends({(101, 202): (True, 8 * DAY_MS), (101, 303): (True, 2 * DAY_MS)}),
        prices=FixedRef(),
    )
    # 好友不满 7 天拒回
    fair_pay = PROTOCOL.message("trade.TradeAssetSelection", currency_type=CURRENCY_GOLD, currency_amount=9_000)
    with pytest.raises(TradeRejection):
        await guard_b.evaluate(101, 303, bad_item, fair_pay, now)
    # 满 7 天且在 50% 偏离带宽内放行
    res_fair = await guard_b.evaluate(101, 202, bad_item, fair_pay, now)
    assert not res_fair.require_cooling_lock and res_fair.reason == "acquaintance_fair_value_ok"
    # 偏离超过 50% (1000 vs 10000) 触发 48 小时冷静锁
    low_pay = PROTOCOL.message("trade.TradeAssetSelection", currency_type=CURRENCY_GOLD, currency_amount=1_000)
    res_lock = await guard_b.evaluate(101, 202, bad_item, low_pay, now)
    assert res_lock.require_cooling_lock and res_lock.cooling_lock_duration_ms == DEFAULT_COOLING_LOCK_DURATION_MS


# -- 3. 4 个客户端 gRPC RPC + 定向寄售 + RMB 收入争议字段测试 -------------------------------------------

class EconomyRpcRig:
    def __init__(
        self,
        *,
        credits_disabled: bool = False,
        ref_disabled: bool = False,
        player_trade_policy: PlayerTradePolicy = PlayerTradePolicy(),
        credit_policy: SellerCreditPolicy = SellerCreditPolicy(),
    ) -> None:
        self.clock = FakeClock()
        self.role_dir = StubRoleDir()
        self.store = MemoryTradeStore(PROTOCOL, role_directory=self.role_dir, clock_ms=self.clock)
        self._id = 1000
        self.key = Ed25519PrivateKey.generate()
        self.verifier = CallerVerifier((self.key.public_key(),))

        self.credits = None if credits_disabled else SellerCreditService(
            store=self.store,
            ownership=StoreOwnershipLookup(self.store),
            policy=credit_policy,
        )
        self.ref_reader = None if ref_disabled else StoreReferencePriceReader(self.store)
        self.daily_out = StoreDailyPlayerTradeTracker(self.store)
        self.engine = CommerceEngine(
            PROTOCOL,
            self.store,
            commerce_ids=self._next_id,
            op_ids=self._next_id,
            policy=TradePolicy(items=(501, 3403), item_titles={501: "红药", 3403: "灵石"}, currencies=(CURRENCY_GOLD,)),
            credits=self.credits,
            player_trade_guard=PlayerTradeGuard(policy=player_trade_policy, daily_out=self.daily_out),
            clock_ms=self.clock,
        )
        self.service = TradeService(
            PROTOCOL,
            self.store,
            caller_verifier=self.verifier,
            coordinator_secret=SECRET_COORDINATOR,
            home_zones=self.role_dir,
            commerce=self.engine,
            content_guard=StubContentGuard(),
            credits=self.credits,
            reference_prices=self.ref_reader,
            player_trade_policy=player_trade_policy,
            daily_outflow=self.daily_out,
            clock_ms=self.clock,
        )
        self._stack = AsyncExitStack()
        self.client: GrpcClient | None = None

    async def _next_id(self) -> int:
        self._id += 1
        return self._id

    async def __aenter__(self) -> "EconomyRpcRig":
        server, port = await serve_grpc(PROTOCOL, "127.0.0.1:0", self.service.services(), pass_context=True)
        self._stack.push_async_callback(server.stop, 0.2)
        self.client = GrpcClient(f"127.0.0.1:{port}", timeout=3.0, enable_retries=False)
        self._stack.push_async_callback(self.client.close)
        return self

    async def __aexit__(self, *args) -> None:
        await self._stack.aclose()

    def register_player(self, player_id: int, account: str, owner_revision: int = 1, home_zone: int = 1) -> None:
        self.role_dir.register(player_id, account, owner_revision=owner_revision, home_zone=home_zone)

    async def call_client(self, method: str, req, resp_type: str, player_id: int, account: str, owner_rev: int = 1):
        assert self.client is not None
        path = f"/trade.ClientPlayerJubaozhai/{method}"
        details = PROTOCOL.message(
            "SessionDetails",
            session_id=101,
            gate_instance_id="gate-1",
            session_incarnation=1,
            player_id=player_id,
            authenticated_account=account,
            owner_revision=owner_rev,
        )
        md = session_metadata(
            details.SerializeToString(deterministic=True),
            path,
            self.key,
        )
        raw = await self.client.call(path, req.SerializeToString(deterministic=True), metadata=md)
        return PROTOCOL.parse(resp_type, raw)


@_async_test
async def test_grpc_all_four_economy_credit_rpcs_and_errors():
    policy_a = PlayerTradePolicy(
        mode=PLAYER_TRADE_MODE_STRICT_CONSUMABLES,
        consumable_items=(501, 502),
        max_daily_currency_amount=10_000,
    )
    async with EconomyRpcRig(player_trade_policy=policy_a) as rig:
        rig.register_player(7001, "acct-seller", owner_revision=2, home_zone=1)
        rig.register_player(8001, "acct-buyer", owner_revision=1, home_zone=1)
        rig.register_player(8002, "acct-other", owner_revision=1, home_zone=1)

        # 1) GetSellerCredit：默认查自己 & 查指定卖家（严格脱敏无敏感字段）
        self_credit = await rig.call_client(
            "GetSellerCredit",
            PROTOCOL.message("trade.GetSellerCreditRequest", seller_player_id=0),
            "trade.SellerCreditResponse",
            7001,
            "acct-seller",
            owner_rev=2,
        )
        assert self_credit.error_message.id == 0
        assert self_credit.profile.seller_player_id == 7001
        assert self_credit.profile.owner_revision == 2
        assert self_credit.profile.credit_score == DEFAULT_SELLER_INITIAL_SCORE
        assert self_credit.profile.ownership_changed is True  # revision > 1

        # 查不存在的卖家 -> ERR_TRADE_ORDER_NOT_FOUND
        missing_credit = await rig.call_client(
            "GetSellerCredit",
            PROTOCOL.message("trade.GetSellerCreditRequest", seller_player_id=99999),
            "trade.SellerCreditResponse",
            8001,
            "acct-buyer",
        )
        assert missing_credit.error_message.id == ERR_TRADE_ORDER_NOT_FOUND

        # 2) SubmitSellerReview：造一笔已结案固定价寄售单 (commerce_id=5001)
        consign_rec = PROTOCOL.message(
            "trade.TradeCommerceRecord",
            commerce_id=5001,
            request_key="test:settled:5001",
            kind=COMMERCE_KIND_CONSIGNMENT,
            status=COMMERCE_SETTLED,
            seller_player_id=7001,
            buyer_player_id=8001,
            unit_price=8800,
            currency_type=CURRENCY_GOLD,
            created_ms=rig.clock() - 2000,
            updated_ms=rig.clock() - 1000,
            version=3,
        )
        consign_wf = PROTOCOL.message("trade.TradeCommerceWorkflow", stage="done")
        consign_wf.seller_identity.account = "acct-seller"
        consign_wf.seller_identity.owner_revision = 2
        consign_wf.buyer_identity.account = "acct-buyer"
        consign_wf.buyer_identity.owner_revision = 1
        await _seed_commerce_record(rig.store, consign_rec, consign_wf)

        # 非买家评价 -> ERR_TRADE_ORDER_NOT_FOUND
        not_buyer = await rig.call_client(
            "SubmitSellerReview",
            PROTOCOL.message("trade.SubmitSellerReviewRequest", order_id="commerce:5001", rating=SELLER_RATING_POSITIVE),
            "trade.SubmitSellerReviewResponse",
            8002,
            "acct-other",
        )
        assert not_buyer.error_message.id == ERR_TRADE_ORDER_NOT_FOUND

        # 违规评语/超长评语/非法评分 -> 明确错误码
        bad_comment = await rig.call_client(
            "SubmitSellerReview",
            PROTOCOL.message(
                "trade.SubmitSellerReviewRequest",
                order_id="5001",
                rating=SELLER_RATING_POSITIVE,
                comment="包含禁词的内容",
            ),
            "trade.SubmitSellerReviewResponse",
            8001,
            "acct-buyer",
        )
        assert bad_comment.error_message.id == ERR_TRADE_TITLE_SENSITIVE

        too_long = await rig.call_client(
            "SubmitSellerReview",
            PROTOCOL.message(
                "trade.SubmitSellerReviewRequest",
                order_id="5001",
                rating=SELLER_RATING_POSITIVE,
                comment="a" * 201,
            ),
            "trade.SubmitSellerReviewResponse",
            8001,
            "acct-buyer",
        )
        assert too_long.error_message.id == ERR_INVALID_PARAMETER

        bad_rating = await rig.call_client(
            "SubmitSellerReview",
            PROTOCOL.message("trade.SubmitSellerReviewRequest", order_id="5001", rating=0),
            "trade.SubmitSellerReviewResponse",
            8001,
            "acct-buyer",
        )
        assert bad_rating.error_message.id == ERR_INVALID_PARAMETER

        # 正常买家好评 -> 60 -> 62，重放幂等，改评冲突
        ok_rev = await rig.call_client(
            "SubmitSellerReview",
            PROTOCOL.message(
                "trade.SubmitSellerReviewRequest",
                order_id="commerce:5001",
                rating=SELLER_RATING_POSITIVE,
                comment="发货极快",
            ),
            "trade.SubmitSellerReviewResponse",
            8001,
            "acct-buyer",
        )
        assert ok_rev.error_message.id == 0
        assert ok_rev.counted_for_score is True
        assert ok_rev.replayed is False
        assert ok_rev.profile.credit_score == 62
        assert ok_rev.profile.completed_orders == 1
        assert ok_rev.profile.positive_reviews == 1

        replay_rev = await rig.call_client(
            "SubmitSellerReview",
            PROTOCOL.message("trade.SubmitSellerReviewRequest", order_id="5001", rating=SELLER_RATING_POSITIVE),
            "trade.SubmitSellerReviewResponse",
            8001,
            "acct-buyer",
        )
        assert replay_rev.error_message.id == 0
        assert replay_rev.replayed is True
        assert replay_rev.profile.credit_score == 62

        conflict_rev = await rig.call_client(
            "SubmitSellerReview",
            PROTOCOL.message("trade.SubmitSellerReviewRequest", order_id="5001", rating=SELLER_RATING_NEGATIVE),
            "trade.SubmitSellerReviewResponse",
            8001,
            "acct-buyer",
        )
        assert conflict_rev.error_message.id == ERR_TRADE_REQUEST_CONFLICT

        # 3) GetMarketReferencePrice：先查样本不足，再造 10 笔市场成交子单验证真实 p50
        p50_empty = await rig.call_client(
            "GetMarketReferencePrice",
            PROTOCOL.message("trade.GetMarketReferencePriceRequest", item_config_id=3403, currency_type=CURRENCY_GOLD),
            "trade.GetMarketReferencePriceResponse",
            8001,
            "acct-buyer",
        )
        assert p50_empty.error_message.id == 0
        assert p50_empty.insufficient_sample is True
        assert p50_empty.sample_count == 0
        assert p50_empty.window_days == 7

        for idx in range(10):
            fill_rec = PROTOCOL.message(
                "trade.TradeCommerceRecord",
                commerce_id=6000 + idx,
                request_key=f"mkt-fill:{idx}",
                kind=COMMERCE_KIND_MARKET_MATCH,
                side=0,
                status=COMMERCE_SETTLED,
                seller_player_id=7001,
                buyer_player_id=8001,
                item_config_id=3403,
                unit_price=20_000 + idx * 1_000,
                quantity=2,
                currency_type=CURRENCY_GOLD,
                created_ms=rig.clock() - 5000,
                updated_ms=rig.clock() - 1000,
                version=2,
            )
            fill_wf = PROTOCOL.message("trade.TradeCommerceWorkflow", stage="done")
            fill_wf.seller_identity.account = f"seller-{idx}"
            fill_wf.buyer_identity.account = f"buyer-{idx}"
            await _seed_commerce_record(rig.store, fill_rec, fill_wf)

        p50_ok = await rig.call_client(
            "GetMarketReferencePrice",
            PROTOCOL.message("trade.GetMarketReferencePriceRequest", item_config_id=3403, currency_type=CURRENCY_GOLD),
            "trade.GetMarketReferencePriceResponse",
            8001,
            "acct-buyer",
        )
        assert p50_ok.error_message.id == 0
        assert p50_ok.insufficient_sample is False
        assert p50_ok.sample_count == 10
        assert p50_ok.median_unit_price == 24_500

        bad_p50 = await rig.call_client(
            "GetMarketReferencePrice",
            PROTOCOL.message("trade.GetMarketReferencePriceRequest", item_config_id=0, currency_type=CURRENCY_GOLD),
            "trade.GetMarketReferencePriceResponse",
            8001,
            "acct-buyer",
        )
        assert bad_p50.error_message.id == ERR_INVALID_PARAMETER

        # 4) GetPlayerTradeQuota：造一笔当日当面交易流出 2500，验证剩余 7500 / 10000
        p2p_rec = PROTOCOL.message(
            "trade.TradeCommerceRecord",
            commerce_id=7001,
            request_key="p2p:7001",
            kind=COMMERCE_KIND_PLAYER_TRADE,
            status=COMMERCE_SETTLED,
            seller_player_id=8001,
            buyer_player_id=7001,
            created_ms=rig.clock() - 100,
            updated_ms=rig.clock() - 50,
            version=2,
        )
        p2p_wf = PROTOCOL.message("trade.TradeCommerceWorkflow", stage="done")
        p2p_wf.seller_assets.currency_type = CURRENCY_GOLD
        p2p_wf.seller_assets.currency_amount = 2_500
        await _seed_commerce_record(rig.store, p2p_rec, p2p_wf)

        quota = await rig.call_client(
            "GetPlayerTradeQuota",
            PROTOCOL.message("trade.GetPlayerTradeQuotaRequest", currency_type=CURRENCY_GOLD),
            "trade.GetPlayerTradeQuotaResponse",
            8001,
            "acct-buyer",
        )
        assert quota.error_message.id == 0
        assert quota.mode == PLAYER_TRADE_MODE_STRICT_CONSUMABLES
        assert quota.daily_currency_limit == 10_000
        assert quota.daily_currency_used == 2_500
        assert quota.daily_currency_remaining == 7_500
        assert list(quota.consumable_item_ids) == [501, 502]

        bad_quota = await rig.call_client(
            "GetPlayerTradeQuota",
            PROTOCOL.message("trade.GetPlayerTradeQuotaRequest", currency_type=99),
            "trade.GetPlayerTradeQuotaResponse",
            8001,
            "acct-buyer",
        )
        assert bad_quota.error_message.id == ERR_INVALID_PARAMETER


@_async_test
async def test_grpc_disabled_features_and_acquaintance_quota_defaults():
    policy_b = PlayerTradePolicy(mode=PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED, max_daily_currency_amount=5_000)
    async with EconomyRpcRig(credits_disabled=True, ref_disabled=True, player_trade_policy=policy_b) as rig:
        rig.register_player(8101, "acct-8101")
        r1 = await rig.call_client(
            "GetSellerCredit",
            PROTOCOL.message("trade.GetSellerCreditRequest", seller_player_id=8101),
            "trade.SellerCreditResponse",
            8101,
            "acct-8101",
        )
        assert r1.error_message.id == ERR_TRADE_FEATURE_DISABLED
        r2 = await rig.call_client(
            "SubmitSellerReview",
            PROTOCOL.message("trade.SubmitSellerReviewRequest", order_id="1", rating=SELLER_RATING_POSITIVE),
            "trade.SubmitSellerReviewResponse",
            8101,
            "acct-8101",
        )
        assert r2.error_message.id == ERR_TRADE_FEATURE_DISABLED
        r3 = await rig.call_client(
            "GetMarketReferencePrice",
            PROTOCOL.message("trade.GetMarketReferencePriceRequest", item_config_id=3403, currency_type=CURRENCY_GOLD),
            "trade.GetMarketReferencePriceResponse",
            8101,
            "acct-8101",
        )
        assert r3.error_message.id == ERR_TRADE_FEATURE_DISABLED
        r4 = await rig.call_client(
            "GetPlayerTradeQuota",
            PROTOCOL.message("trade.GetPlayerTradeQuotaRequest", currency_type=CURRENCY_GOLD),
            "trade.GetPlayerTradeQuotaResponse",
            8101,
            "acct-8101",
        )
        assert r4.error_message.id == 0
        assert r4.min_friend_duration_ms == DEFAULT_MIN_FRIEND_DURATION_MS
        assert r4.max_reference_deviation_pct == DEFAULT_MAX_REFERENCE_DEVIATION_PCT
        assert r4.cooling_lock_duration_ms == DEFAULT_COOLING_LOCK_DURATION_MS


@_async_test
async def test_designated_buyer_consignment_and_rmb_dispute_fields():
    async with EconomyRpcRig() as rig:
        rig.register_player(9001, "seller-a", home_zone=1)
        rig.register_player(9002, "buyer-ok", home_zone=1)
        rig.register_player(9003, "buyer-other", home_zone=1)

        # 指定不存在买家 -> ERR_TRADE_COUNTERPARTY_UNKNOWN
        req_unknown = PROTOCOL.message(
            "trade.CreateConsignmentRequest",
            client_request_id="desig-unknown-1",
            currency_type=CURRENCY_GOLD,
            price=1000,
            duration_ms=120_000,
            designated_buyer_player_id=999999,
        )
        req_unknown.assets.items[501] = 1
        resp_unk = await rig.call_client(
            "CreateConsignment", req_unknown, "trade.CommerceOrderResponse", 9001, "seller-a"
        )
        assert resp_unk.error_message.id == ERR_TRADE_COUNTERPARTY_UNKNOWN

        # 指定自己 -> ERR_TRADE_CANNOT_TRADE_SELF
        req_self = PROTOCOL.message(
            "trade.CreateConsignmentRequest",
            client_request_id="desig-self-0001",
            currency_type=CURRENCY_GOLD,
            price=1000,
            duration_ms=120_000,
            designated_buyer_player_id=9001,
        )
        req_self.assets.items[501] = 1
        resp_self = await rig.call_client("CreateConsignment", req_self, "trade.CommerceOrderResponse", 9001, "seller-a")
        assert resp_self.error_message.id == ERR_TRADE_CANNOT_TRADE_SELF

        # 指定 9002 成功，且视图带出 designated_buyer_player_id=9002
        req_ok = PROTOCOL.message(
            "trade.CreateConsignmentRequest",
            client_request_id="desig-ok-000001",
            currency_type=CURRENCY_GOLD,
            price=1000,
            duration_ms=120_000,
            designated_buyer_player_id=9002,
        )
        req_ok.assets.items[501] = 1
        resp_ok = await rig.call_client("CreateConsignment", req_ok, "trade.CommerceOrderResponse", 9001, "seller-a")
        assert resp_ok.error_message.id == 0
        assert resp_ok.order.designated_buyer_player_id == 9002
        lid = int(resp_ok.order.order_id)

        # 将合同置为 open 后测试：非指定买家 9003 被拒，指定买家 9002 可买
        async def _open(tx):
            r = await tx.lock_commerce(lid)
            wf = PROTOCOL.message("trade.TradeCommerceWorkflow")
            wf.ParseFromString(bytes(r.workflow_proto))
            wf.stage = "open"
            await tx.save_commerce(r, wf, rig.clock())

        await rig.store.run_tx(_open)

        buy_other = await rig.call_client(
            "BuyConsignment",
            PROTOCOL.message("trade.BuyConsignmentRequest", listing_id=str(lid), client_request_id="buy-other-001"),
            "trade.CommerceOrderResponse",
            9003,
            "buyer-other",
        )
        assert buy_other.error_message.id == ERR_TRADE_ORDER_STATE_CONFLICT

        buy_target = await rig.call_client(
            "BuyConsignment",
            PROTOCOL.message("trade.BuyConsignmentRequest", listing_id=str(lid), client_request_id="buy-target-01"),
            "trade.CommerceOrderResponse",
            9002,
            "buyer-ok",
        )
        assert buy_target.error_message.id == 0
        assert buy_target.order.buyer_player_id == 9002

    # parse_rmb_listing_view 校验 designated_only 字段
    view = parse_rmb_listing_view(
        PROTOCOL,
        {
            "listingId": "10001",
            "sellerPlayerId": "9001",
            "marketZone": 1,
            "assetKind": "ITEMS",
            "title": "定向神兵",
            "priceFen": 5000,
            "feeBps": 500,
            "coolingMs": 0,
            "noticeEndMs": 1000,
            "saleEndMs": 2000,
            "state": "LISTED",
            "phase": "ON_SALE",
            "orderId": "",
            "designatedBuyerAccount": "buyer-acct",
        },
    )
    assert view.designated_only is True

    # MallAdapter.rmb_incomes 校验 dispute_frozen_fen & dispute_case_id
    adapter = MallAdapter(
        PROTOCOL,
        "http://127.0.0.1:18080",
        b"gateway-secret-0123456789abcdef01",
        b"delivery-secret-0123456789abcdef0",
    )

    async def _fake_consignment_call(action: str, payload: dict, *, expect_json: bool = True):
        assert action == "incomes"
        return {
            "incomes": [
                {
                    "orderId": "00112233-4455-6677-8899-aabbccddeeff",
                    "grossFen": 10_000,
                    "feeFen": 500,
                    "netFen": 9_500,
                    "reservedFen": 1_000,
                    "paidFen": 2_000,
                    "forfeitedFen": 0,
                    "disputeFrozenFen": 1_500,
                    "disputeCaseId": "DISP-2026-01",
                    "availableMs": 1_760_000_000_000,
                }
            ]
        }

    adapter.consignment_call = _fake_consignment_call  # type: ignore[method-assign]
    inc_resp = await adapter.rmb_incomes("seller-a")
    assert len(inc_resp.incomes) == 1
    assert inc_resp.incomes[0].dispute_frozen_fen == 1_500
    assert inc_resp.incomes[0].dispute_case_id == "DISP-2026-01"

    # RmbConsignments.create 定向买家校验（查目录、禁同账号）+ rmb_register 透传 designatedBuyerAccount
    async with EconomyRpcRig() as rig:
        rig.register_player(9101, "rmb-seller", owner_revision=1, home_zone=1)
        rig.register_player(9102, "rmb-buyer", owner_revision=1, home_zone=1)
        rig.register_player(9103, "rmb-seller", owner_revision=1, home_zone=1)

        class StubRmbDir:
            def __init__(self, owners: dict[int, tuple[str, int]]) -> None:
                self._owners = owners

            async def lookup(self, player_id: int):
                acct, rev = self._owners[player_id]
                out = PROTOCOL.message("data_service.AccountRoleOwnership", account=acct, owner_revision=rev)
                out.role.player_id = player_id
                return out

            async def begin(self, req):
                raise NotImplementedError

            async def confirm(self, req):
                raise NotImplementedError

            async def cancel(self, req):
                raise NotImplementedError

            async def transfer(self, req):
                raise NotImplementedError

            async def role_slot_full(self, account: str) -> bool:
                return False

        class NoopSealer:
            async def seal(self, seal_msg) -> None:
                return None

        rmb = RmbConsignments(
            PROTOCOL,
            rig.engine,
            adapter,
            StubRmbDir({9101: ("rmb-seller", 1), 9102: ("rmb-buyer", 1), 9103: ("rmb-seller", 1)}),
            NoopSealer(),
            credits=rig.credits,
        )
        orig = RoleOriginal(player_id=9101, account="rmb-seller", owner_revision=1)
        # 指定同账号其他角色 -> ERR_TRADE_CANNOT_TRADE_SELF
        bad_rmb = PROTOCOL.message(
            "trade.CreateRmbConsignmentRequest",
            client_request_id="rmb-desig-self1",
            price_fen=5000,
            title="定向灵石",
            designated_buyer_player_id=9103,
        )
        bad_rmb.assets.items[501] = 1
        with pytest.raises(TradeRejection) as exc_rmb_self:
            await rmb.create(orig, "rmb-seller", 9101, 1, 1, bad_rmb)
        assert exc_rmb_self.value.tip_id == ERR_TRADE_CANNOT_TRADE_SELF

        # 指定不同账号角色 -> 写入 designated_buyer_account
        good_rmb = PROTOCOL.message(
            "trade.CreateRmbConsignmentRequest",
            client_request_id="rmb-desig-good1",
            price_fen=5000,
            title="定向灵石",
            designated_buyer_player_id=9102,
        )
        good_rmb.assets.items[501] = 1
        rec_rmb = await rmb.create(orig, "rmb-seller", 9101, 1, 1, good_rmb)
        wf_rmb = PROTOCOL.message("trade.TradeCommerceWorkflow")
        wf_rmb.ParseFromString(bytes(rec_rmb.workflow_proto))
        assert wf_rmb.rmb.designated_buyer_account == "rmb-buyer"


# -- 4. SqlTradeStore 卖家信用 DDL 与参数化 SQL 查询验证 -------------------------------------------------

@_async_test
async def test_sql_trade_store_seller_credit_ddl_and_queries():
    for table in SELLER_CREDIT_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS `{table}`" in SELLER_CREDIT_SCHEMA_DDL

    executed: list[tuple[str, tuple]] = []

    class FakeCursor:
        def __init__(self) -> None:
            self.rowcount = 1
            self._rows: list[tuple] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def execute(self, sql: str, args=()):
            executed.append((sql, tuple(args) if args else ()))
            if "FROM `trade_seller_credit` WHERE `seller_player_id`=%s AND `owner_revision`=%s" in sql:
                self._rows = [
                    (
                        1001, 1, "acct-1", 0, 0, 1000, 3, 2, 0, 0, 0,
                        0, 0, 0, "1.2.3.4", "dev-1", '["dev-1"]', 64, 36, 10000, "new_or_transferred", 2000,
                    )
                ]
            elif "SELECT COUNT(*) FROM `trade_seller_review`" in sql:
                self._rows = [(2,)]
            elif "FROM `trade_seller_violation`" in sql:
                self._rows = [(1,)]
            else:
                self._rows = []

        async def fetchall(self):
            return self._rows

        async def fetchone(self):
            return self._rows[0] if self._rows else None

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        async def commit(self):
            return None

        async def rollback(self):
            return None

    class FakePoolCtx:
        async def __aenter__(self):
            return FakeConn()

        async def __aexit__(self, *args):
            return None

    class FakePool:
        def acquire(self):
            return FakePoolCtx()

    id_seq = iter(range(500, 600))

    async def _alloc_id() -> int:
        return next(id_seq)

    sql_store = SqlTradeStore(FakePool(), PROTOCOL, id_allocator=_alloc_id, clock_ms=lambda: 1_760_000_000_000)
    await sql_store.bootstrap()
    assert any("trade_seller_credit" in stmt for stmt, _ in executed)
    assert any("trade_seller_review" in stmt for stmt, _ in executed)

    prof = await sql_store.get_credit_profile(SellerCreditKey(1001, 1))
    assert prof is not None and prof.credit_score == 64 and prof.known_devices == ("dev-1",)
    await sql_store.save_credit_profile(prof)
    inserted = await sql_store.mark_seller_order_completed("commerce:1", SellerCreditKey(1001, 1))
    assert inserted is True
    cnt = await sql_store.count_pair_scored_reviews(SellerCreditKey(1001, 1), "buyer-1", "9.9.9.9", "d-9", 0, 9999)
    assert cnt == 2
    await sql_store.save_seller_review(
        SellerReviewRecord("commerce:1", 1001, 1, 2001, "buyer-1", "9.9.9.9", "d-9", 1, True, "good", 1000)
    )
    assert await sql_store.has_seller_violation("CASE-SQL-1") is True
    await sql_store.save_seller_violation(
        SellerViolationRecord("CASE-SQL-1", "commerce:1", 1001, "acct-1", 1, "fraud", 1000)
    )
    fills = await sql_store.list_settled_market_fills(3403, CURRENCY_GOLD, 0, 2_000_000_000_000)
    assert fills == []
    outflow = await sql_store.daily_player_trade_currency_outflow(1001, CURRENCY_GOLD, 0)
    assert outflow == 0


# -- 5. Runtime("local") 单进程 Gate TCP 端到端验证（481 / 482 / 483 / 484） -----------------------------

async def _tcp_call(client, call_id: int, message_name: str, **fields):
    p = client.protocol
    message_id = p.message_ids[message_name]
    request = p.message(p.routes[message_id].request_type, **fields)
    return await client.rpc(message_id, call_id, request)


def test_local_runtime_gate_tcp_economy_credit_rpcs_end_to_end(tmp_path):
    async def exercise():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            assert runtime.local_services is not None
            async with _enter(runtime, account="seller_e2e") as (client_seller, seller_id):
                async with _enter(runtime, account="buyer_e2e") as (client_buyer, buyer_id):
                    # 481: GetSellerCredit
                    credit_resp = await _tcp_call(
                        client_buyer,
                        101,
                        "ClientPlayerJubaozhaiGetSellerCredit",
                        seller_player_id=seller_id,
                    )
                    assert credit_resp.error_message.id == 0
                    assert credit_resp.profile.seller_player_id == seller_id
                    assert credit_resp.profile.credit_score == DEFAULT_SELLER_INITIAL_SCORE

                    # 482: SubmitSellerReview（先写入一笔已结案托管寄售单，验证好评涨分与幂等重放）
                    now_ms = runtime.local_services.trade_store.now_ms()
                    settled_rec = runtime.protocol.message(
                        "trade.TradeCommerceRecord",
                        commerce_id=88001,
                        request_key="e2e:consign:88001",
                        kind=COMMERCE_KIND_CONSIGNMENT,
                        status=COMMERCE_SETTLED,
                        seller_player_id=seller_id,
                        buyer_player_id=buyer_id,
                        unit_price=5_000,
                        currency_type=CURRENCY_GOLD,
                        created_ms=now_ms - 2_000,
                        updated_ms=now_ms - 1_000,
                        version=2,
                    )
                    settled_wf = runtime.protocol.message("trade.TradeCommerceWorkflow", stage="done")
                    settled_wf.seller_identity.account = "seller_e2e"
                    settled_wf.seller_identity.owner_revision = 1
                    settled_wf.buyer_identity.account = "buyer_e2e"
                    settled_wf.buyer_identity.owner_revision = 1
                    await _seed_commerce_record(runtime.local_services.trade_store, settled_rec, settled_wf)

                    review_resp = await _tcp_call(
                        client_buyer,
                        102,
                        "ClientPlayerJubaozhaiSubmitSellerReview",
                        order_id="commerce:88001",
                        rating=SELLER_RATING_POSITIVE,
                        comment="交易顺利",
                    )
                    assert review_resp.error_message.id == 0
                    assert review_resp.counted_for_score is True
                    assert review_resp.replayed is False
                    assert review_resp.profile.credit_score == 62
                    assert review_resp.profile.positive_reviews == 1

                    # 再次查 481 确认信用分已更新
                    credit_after = await _tcp_call(
                        client_buyer,
                        103,
                        "ClientPlayerJubaozhaiGetSellerCredit",
                        seller_player_id=seller_id,
                    )
                    assert credit_after.error_message.id == 0
                    assert credit_after.profile.credit_score == 62

                    # 483: GetMarketReferencePrice（先不足 10 笔，补齐 10 笔真实成交后验证 p50）
                    p50_before = await _tcp_call(
                        client_buyer,
                        104,
                        "ClientPlayerJubaozhaiGetMarketReferencePrice",
                        item_config_id=3403,
                        currency_type=CURRENCY_GOLD,
                    )
                    assert p50_before.error_message.id == 0
                    assert p50_before.insufficient_sample is True
                    assert p50_before.window_days == 7

                    for i in range(10):
                        fill = runtime.protocol.message(
                            "trade.TradeCommerceRecord",
                            commerce_id=89000 + i,
                            request_key=f"e2e:fill:{i}",
                            kind=COMMERCE_KIND_MARKET_MATCH,
                            side=0,
                            status=COMMERCE_SETTLED,
                            seller_player_id=seller_id,
                            buyer_player_id=buyer_id,
                            item_config_id=3403,
                            unit_price=10_000 + i * 200,
                            quantity=1,
                            currency_type=CURRENCY_GOLD,
                            created_ms=now_ms - 5_000,
                            updated_ms=now_ms - 500,
                            version=2,
                        )
                        fw = runtime.protocol.message("trade.TradeCommerceWorkflow", stage="done")
                        fw.seller_identity.account = f"e2e-s-{i}"
                        fw.buyer_identity.account = f"e2e-b-{i}"
                        await _seed_commerce_record(runtime.local_services.trade_store, fill, fw)

                    p50_after = await _tcp_call(
                        client_buyer,
                        105,
                        "ClientPlayerJubaozhaiGetMarketReferencePrice",
                        item_config_id=3403,
                        currency_type=CURRENCY_GOLD,
                    )
                    assert p50_after.error_message.id == 0
                    assert p50_after.insufficient_sample is False
                    assert p50_after.sample_count == 10
                    assert p50_after.median_unit_price == 10_900

                    # 484: GetPlayerTradeQuota
                    quota_resp = await _tcp_call(
                        client_seller,
                        106,
                        "ClientPlayerJubaozhaiGetPlayerTradeQuota",
                        currency_type=CURRENCY_GOLD,
                    )
                    assert quota_resp.error_message.id == 0
                    assert quota_resp.currency_type == CURRENCY_GOLD
                    assert quota_resp.server_now_ms > 0

    asyncio.run(exercise())
