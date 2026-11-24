"""mall_game 全链路与协议兼容性测试。

覆盖范围：
1. ``config/mall_game_schema.sql`` 通过 ``ops/schemamigrate.py`` 全量校验；
2. ``MallGameConfig`` 生产安全红线与环境变量加载；
3. HMAC v1 鉴权、角色隔离、重放拦截与健康探针；
4. 官方 SKU 目录管理、下单/取消/过期、正常支付、重放、迟到退款与三类异常付款；
5. 交付轮询/回执、付款修订与扣回/补发全状态机、退款轮询/回执/转人工；
6. 三类人工单（官方订单/退款/寄售）列表与全部受控处置动作；
7. 人民币寄售完整生命周期（登记/购买/付款/交付/收益/提现/撤销退回/争议冻结与扣罚）；
8. Mock 支付与出款在 prod 禁用、在 dev/test 完整工作；
9. 真实 loopback 服务直连现有 ``payment.mall_client.HttpMallClient`` 与 ``trade.mall_adapter.MallAdapter``。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp import web

from xuanming.mall_game import (
    STORAGE_CONTRACT,
    MallGameConfig,
    MemoryMallStore,
    create_mall_app,
    session_init_sql,
    sign_v1,
)
from xuanming.mall_game.sql_store import REQUIRED_TABLE_COLUMNS
from xuanming.ops.schemamigrate import parse_schema_file
from xuanming.payment.mall_client import HttpMallClient
from xuanming.payment.model import MallPayment, MallRefund, MallRefundClosure, MallRevision
from xuanming.protocol import Protocol
from xuanming.trade.mall_adapter import MallAdapter
from xuanming.trade.model import RoleOriginal

GW_SECRET = "g" * 40
PAY_SECRET = "p" * 40
DEL_SECRET = "d" * 40
ADM_SECRET = "a" * 40

PROOF_A = "a" * 64
PROOF_B = "b" * 64
PROOF_C = "c" * 64


class FakeClock:
    """可控单调毫秒时钟，便于测试公示期、冷却期、订单过期与时间窗。"""

    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self.now = start_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, delta_ms: int) -> None:
        self.now += delta_ms


def make_config(
    *,
    run_mode: str = "test",
    require_tidb: bool = False,
    mock_payment_enabled: bool = True,
    allowed_channels: frozenset[str] = frozenset(("alipay", "wechat_pay", "apple", "devpay")),
    max_unpaid_orders: int = 5,
    consignment_notice_ms: int = 10_000,
    consignment_sale_ms: int = 60_000,
    consignment_cooling_ms: int = 5_000,
    consignment_fee_bps: int = 500,
) -> MallGameConfig:
    cfg = MallGameConfig(
        host="127.0.0.1",
        port=0,
        run_mode=run_mode,
        require_tidb=require_tidb,
        gateway_secret=GW_SECRET,
        payment_secret=PAY_SECRET,
        delivery_secret=DEL_SECRET,
        admin_secret=ADM_SECRET,
        allowed_channels=allowed_channels,
        mock_payment_enabled=mock_payment_enabled,
        max_unpaid_orders=max_unpaid_orders,
        consignment_notice_ms=consignment_notice_ms,
        consignment_sale_ms=consignment_sale_ms,
        consignment_cooling_ms=consignment_cooling_ms,
        consignment_fee_bps=consignment_fee_bps,
    )
    cfg.validate()
    return cfg


class LiveMallHarness:
    """真实 loopback aiohttp 服务测试桩，封装签名请求与可控时钟。"""

    def __init__(self, config: MallGameConfig, clock: FakeClock | None = None) -> None:
        self.config = config
        self.clock = clock or FakeClock()
        self.store = MemoryMallStore(config, clock_ms=self.clock)
        self.app = create_mall_app(self.store, config)
        self.runner: web.AppRunner | None = None
        self.base_url: str = ""
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> LiveMallHarness:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, host="127.0.0.1", port=0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args) -> None:
        if self.session is not None:
            await self.session.close()
        if self.runner is not None:
            await self.runner.cleanup()

    def _secret(self, role: str) -> str:
        return {
            "gateway": self.config.gateway_secret,
            "payment": self.config.payment_secret,
            "delivery": self.config.delivery_secret,
            "admin": self.config.admin_secret,
        }[role]

    async def signed(
        self,
        role: str,
        subject: str,
        method: str,
        path: str,
        payload: object = None,
        *,
        nonce: str | None = None,
        timestamp: int | None = None,
        secret_override: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[int, object]:
        assert self.session is not None
        body = b"" if payload is None else json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ts = str(self.clock() if timestamp is None else timestamp)
        nc = nonce or secrets.token_hex(16)
        sig = sign_v1(secret_override or self._secret(role), role, subject, ts, nc, method, path, body)
        headers = {
            "Content-Type": "application/json",
            "X-Commerce-Subject": subject,
            "X-Commerce-Timestamp": ts,
            "X-Commerce-Nonce": nc,
            "X-Commerce-Signature": sig,
        }
        if extra_headers:
            headers.update(extra_headers)
        async with self.session.request(method, self.base_url + path, data=body if body else None, headers=headers) as resp:
            raw = await resp.read()
            if not raw:
                return resp.status, None
            return resp.status, json.loads(raw.decode("utf-8"))


# -- 1. Schema DDL 与配置安全校验 -----------------------------------------------------------------------------

def test_schema_sql_compatible_with_schemamigrate() -> None:
    schema_path = Path(__file__).resolve().parents[1] / "config" / "mall_game_schema.sql"
    plan = parse_schema_file(schema_path)
    assert plan.db_name == "mall_game"
    assert len(plan.tables) == 28
    table_names = {t.table_name for t in plan.tables}
    assert table_names == set(REQUIRED_TABLE_COLUMNS.keys())
    assert "oms_order" in table_names
    assert "commerce_payment_receipt" in table_names
    assert "commerce_consignment" in table_names
    assert "commerce_reversal_intent" in table_names
    assert "commerce_schema_version" in table_names
    assert STORAGE_CONTRACT == "mall-game-official-sku-v3-manual-resolution"
    assert "tidb_txn_mode='pessimistic'" in session_init_sql(require_tidb=True)
    assert "READ-COMMITTED" in session_init_sql(require_tidb=False)


def test_config_validation_and_env_loading() -> None:
    # prod 拒绝 devpay 与 mock* 渠道
    with pytest.raises(ValueError, match="devpay/mock"):
        MallGameConfig(
            run_mode="prod",
            require_tidb=True,
            gateway_secret=GW_SECRET,
            payment_secret=PAY_SECRET,
            allowed_channels=frozenset(("devpay",)),
        ).validate()

    with pytest.raises(ValueError, match="devpay/mock"):
        MallGameConfig(
            run_mode="prod",
            require_tidb=True,
            gateway_secret=GW_SECRET,
            payment_secret=PAY_SECRET,
            allowed_channels=frozenset(("mock_local",)),
        ).validate()

    # prod 拒绝关闭 require_tidb 或开启 mock_payment_enabled
    with pytest.raises(ValueError, match="require_tidb"):
        MallGameConfig(run_mode="prod", require_tidb=False, gateway_secret=GW_SECRET, payment_secret=PAY_SECRET).validate()
    with pytest.raises(ValueError, match="mock_payment_enabled"):
        MallGameConfig(run_mode="prod", require_tidb=True, mock_payment_enabled=True, gateway_secret=GW_SECRET, payment_secret=PAY_SECRET).validate()

    # 密钥长度与独立性校验
    with pytest.raises(ValueError, match="32"):
        MallGameConfig(run_mode="test", require_tidb=False, gateway_secret="short", payment_secret=PAY_SECRET).validate()
    with pytest.raises(ValueError, match="distinct"):
        MallGameConfig(run_mode="test", require_tidb=False, gateway_secret=GW_SECRET, payment_secret=GW_SECRET).validate()

    # from_settings 读取 raw section 与 COMMERCE_* 覆盖
    fake_settings = SimpleNamespace(
        mall_game_node={
            "port": 18125,
            "run_mode": "dev",
            "require_tidb": False,
            "allowed_channels": ["alipay", "devpay"],
        }
    )
    cfg = MallGameConfig.from_settings(
        fake_settings,
        environ={
            "COMMERCE_GATEWAY_SECRET": GW_SECRET,
            "COMMERCE_PAYMENT_SECRET": PAY_SECRET,
            "COMMERCE_DELIVERY_SECRET": DEL_SECRET,
            "COMMERCE_ADMIN_SECRET": ADM_SECRET,
            "COMMERCE_MOCK_PAYMENT_ENABLED": "true",
        },
    )
    assert cfg.port == 18125
    assert cfg.run_mode == "dev"
    assert cfg.mock_payment_enabled is True
    assert cfg.allowed_channels == frozenset(("alipay", "devpay"))


def _async_test(fn):
    def wrapper():
        return asyncio.run(fn())
    wrapper.__name__ = fn.__name__
    return wrapper


# -- 2. HMAC 鉴权、角色隔离、防重放与健康探针 ----------------------------------------------------------------

@_async_test
async def test_hmac_auth_role_isolation_nonce_replay_and_health() -> None:
    async with LiveMallHarness(make_config()) as h:
        assert h.session is not None
        # 健康探针免签
        for path in ("/actuator/health", "/actuator/health/liveness", "/actuator/health/readiness"):
            async with h.session.get(h.base_url + path) as resp:
                assert resp.status == 200
                assert (await resp.json())["status"] == "UP"

        # 缺签被拒 401
        async with h.session.get(h.base_url + "/api/skus") as resp:
            assert resp.status == 401

        # 正确签名成功
        fixed_nonce = "nonce_unique_00000001"
        st, body = await h.signed("gateway", "10001", "GET", "/api/skus", nonce=fixed_nonce)
        assert st == 200
        assert body == {"skus": []}

        # 同 role 同 nonce 重放返回 409
        st, _ = await h.signed("gateway", "10001", "GET", "/api/skus", nonce=fixed_nonce)
        assert st == 409

        # 错误角色密钥签名被拒 401
        st, _ = await h.signed("payment", "payment-adapter", "POST", "/internal/payments/order-view", {"orderId": "00000000-0000-0000-0000-000000000001"}, secret_override=GW_SECRET)
        assert st == 401

        # 显式传入不匹配的 X-Commerce-Role 被拒 401
        st, _ = await h.signed(
            "gateway", "10001", "GET", "/api/skus", extra_headers={"X-Commerce-Role": "admin"}
        )
        assert st == 401

        # 过期时间戳被拒 401
        st, _ = await h.signed("gateway", "10001", "GET", "/api/skus", timestamp=h.clock() - 500_000)
        assert st == 401

        # 未知 /internal/* 返回 404
        st, _ = await h.signed("payment", "payment-adapter", "POST", "/internal/unknown", {})
        assert st == 404

        # 查询串与非法路径被拒 400
        async with h.session.get(h.base_url + "/api/skus?foo=1") as resp:
            assert resp.status == 400


# -- 3. 官方 SKU、下单/取消/过期、正常支付、重放、迟到退款与异常付款 ------------------------------------------

@_async_test
async def test_official_order_payment_abnormal_and_refund_flows() -> None:
    async with LiveMallHarness(make_config(max_unpaid_orders=2)) as h:
        # 1) 管理员创建 SKU（物品型与货币型）
        item_terms = {
            "productId": 100,
            "skuCode": "SWORD-01",
            "priceMinor": 600,
            "currency": "CNY",
            "published": True,
            "itemConfigId": 5001,
            "countPerUnit": 2,
            "title": "玄冥长剑礼盒",
        }
        st, sku1 = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/skus/create",
            {"requestId": "adm-create-sku-1", "skuId": 1001, "terms": item_terms, "stock": 10},
        )
        assert st == 200 and sku1["revision"] == 1 and sku1["stock"] == 10

        # 幂等重放返回同快照；改参数复报 409
        st, sku1_replay = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/skus/create",
            {"requestId": "adm-create-sku-1", "skuId": 1001, "terms": item_terms, "stock": 10},
        )
        assert st == 200 and sku1_replay == sku1
        st, _ = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/skus/create",
            {"requestId": "adm-create-sku-1", "skuId": 1001, "terms": item_terms, "stock": 99},
        )
        assert st == 409

        # 修改条款 + 调整库存（CAS 修订号）
        updated_terms = dict(item_terms, priceMinor=800)
        st, sku1 = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/skus/update",
            {"requestId": "adm-upd-sku-1", "skuId": 1001, "expectedRevision": 1, "terms": updated_terms},
        )
        assert st == 200 and sku1["revision"] == 2 and sku1["priceMinor"] == 800

        st, sku1 = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/skus/stock",
            {"requestId": "adm-stk-sku-1", "skuId": 1001, "expectedRevision": 2, "delta": 5},
        )
        assert st == 200 and sku1["revision"] == 3 and sku1["stock"] == 15

        # 公开目录与 payment 查询目录
        st, pub = await h.signed("gateway", "20001", "POST", "/api/skus/list", {"afterSkuId": 0, "limit": 20})
        assert st == 200 and len(pub["skus"]) == 1 and pub["skus"][0]["available"] == 15
        st, pay_pub = await h.signed("payment", "payment-adapter", "POST", "/internal/payments/skus", {"afterSkuId": 0, "limit": 20})
        assert st == 200 and pay_pub == pub

        # 2) 创建订单 + 幂等重放 + 查询/列表 + 旧路由 404
        st, ord1 = await h.signed(
            "gateway", "20001", "POST", "/api/orders",
            {"requestId": "buy-req-0001", "skuId": 1001, "quantity": 2, "buyerAccount": "acc_alice", "buyerOwnerRevision": "1"},
        )
        assert st == 200 and ord1["state"] == "AWAITING_PAYMENT" and ord1["amountMinor"] == 1600
        oid1 = ord1["orderId"]

        st, ord1_replay = await h.signed(
            "gateway", "20001", "POST", "/api/orders",
            {"requestId": "buy-req-0001", "skuId": 1001, "quantity": 2, "buyerAccount": "acc_alice", "buyerOwnerRevision": "1"},
        )
        assert st == 200 and ord1_replay["orderId"] == oid1

        st, queried = await h.signed(
            "gateway", "20001", "POST", "/api/orders/query",
            {"orderId": oid1, "buyerAccount": "acc_alice", "buyerOwnerRevision": "1"},
        )
        assert st == 200 and queried["orderId"] == oid1

        # 旧无身份路由返回 404
        st, _ = await h.signed("gateway", "20001", "GET", f"/api/orders/{oid1}")
        assert st == 404
        st, _ = await h.signed("gateway", "20001", "POST", f"/api/orders/{oid1}/cancel")
        assert st == 404

        # 3) 创建第二单并取消 -> 迟到付款触发 CANCELLED_LATE
        st, ord2 = await h.signed(
            "gateway", "20001", "POST", "/api/orders",
            {"requestId": "buy-req-0002", "skuId": 1001, "quantity": 1, "buyerAccount": "acc_alice", "buyerOwnerRevision": "1"},
        )
        assert st == 200
        oid2 = ord2["orderId"]

        # 未支付上限 2 拦截第三单（429）
        st, _ = await h.signed(
            "gateway", "20001", "POST", "/api/orders",
            {"requestId": "buy-req-0003", "skuId": 1001, "quantity": 1, "buyerAccount": "acc_alice", "buyerOwnerRevision": "1"},
        )
        assert st == 429

        # 取消第二单
        st, ord2_cancelled = await h.signed(
            "gateway", "20001", "POST", "/api/orders/cancel",
            {"orderId": oid2, "buyerAccount": "acc_alice", "buyerOwnerRevision": "1"},
        )
        assert st == 200 and ord2_cancelled["state"] == "CANCELLED"

        # 迟到付款 -> 订单转 REFUND_PENDING，refundReason = CANCELLED_LATE
        st, late_out = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid2, "channel": "alipay", "transactionId": "ali-late-1", "amountMinor": 800, "currency": "CNY"},
        )
        assert st == 200 and late_out["order"]["state"] == "REFUND_PENDING" and late_out["refundReason"] == "CANCELLED_LATE"

        # 4) 第一单先遇金额不符 + 币种不符异常付款（不改订单状态），再正常付款，再遇重复付款
        st, abn_amt = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid1, "channel": "alipay", "transactionId": "ali-amt-bad", "amountMinor": 1500, "currency": "CNY"},
        )
        assert st == 200 and abn_amt["order"]["state"] == "AWAITING_PAYMENT" and abn_amt["refundReason"] == "AMOUNT_MISMATCH"

        st, abn_cur = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid1, "channel": "alipay", "transactionId": "ali-cur-bad", "amountMinor": 1600, "currency": "USD"},
        )
        assert st == 200 and abn_cur["order"]["state"] == "AWAITING_PAYMENT" and abn_cur["refundReason"] == "CURRENCY_MISMATCH"

        # 正常付款
        st, ok_pay = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid1, "channel": "alipay", "transactionId": "ali-ok-1", "amountMinor": 1600, "currency": "CNY"},
        )
        assert st == 200 and ok_pay["order"]["state"] == "DELIVERY_PENDING" and ok_pay["refundReason"] is None

        # 同流水幂等重放
        st, ok_replay = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid1, "channel": "alipay", "transactionId": "ali-ok-1", "amountMinor": 1600, "currency": "CNY"},
        )
        assert st == 200 and ok_replay["refundReason"] is None

        # 重复付款 DUPLICATE_PAYMENT
        st, abn_dup = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid1, "channel": "wechat_pay", "transactionId": "wx-dup-1", "amountMinor": 1600, "currency": "CNY"},
        )
        assert st == 200 and abn_dup["order"]["state"] == "DELIVERY_PENDING" and abn_dup["refundReason"] == "DUPLICATE_PAYMENT"

        # 5) 退款轮询应列出全部 4 条（CANCELLED_LATE + 3 异常付款），测试分页与回执/关闭
        st, page1 = await h.signed("payment", "payment-adapter", "POST", "/internal/refunds/poll", {"limit": 2, "cursor": ""})
        assert st == 200 and len(page1["entries"]) == 2 and page1["nextCursor"]
        st, page2 = await h.signed("payment", "payment-adapter", "POST", "/internal/refunds/poll", {"limit": 10, "cursor": page1["nextCursor"]})
        assert st == 200 and len(page2["entries"]) == 2 and page2["nextCursor"] == ""

        # 完成迟到付款退款 -> oid2 转 REFUNDED
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/refunds/receipt",
            {
                "orderId": oid2, "channel": "alipay", "paymentTransactionId": "ali-late-1",
                "transactionId": "ali-ref-late-1", "amountMinor": 800, "currency": "CNY",
            },
        )
        assert st == 200
        st, ord2_view = await h.signed("payment", "payment-adapter", "POST", "/internal/payments/order-view", {"orderId": oid2})
        assert st == 200 and ord2_view["state"] == "REFUNDED"

        # 异常付款退款不影响 oid1 状态
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/refunds/receipt",
            {
                "orderId": oid1, "channel": "alipay", "paymentTransactionId": "ali-amt-bad",
                "transactionId": "ali-ref-amt-1", "amountMinor": 1500, "currency": "CNY",
            },
        )
        assert st == 200
        st, ord1_view = await h.signed("payment", "payment-adapter", "POST", "/internal/payments/order-view", {"orderId": oid1})
        assert st == 200 and ord1_view["state"] == "DELIVERY_PENDING"

        # 退款转人工 close -> 异常付款转 MANUAL
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/refunds/close",
            {"orderId": oid1, "channel": "wechat_pay", "paymentTransactionId": "wx-dup-1", "reason": "channel_unsupported"},
        )
        assert st == 200


# -- 4. 交付、付款修订、扣回/补发与人工单处置 ----------------------------------------------------------------

@_async_test
async def test_delivery_payment_revision_reversal_and_manual_admin() -> None:
    async with LiveMallHarness(make_config()) as h:
        # 创建货币充值 SKU
        terms = {
            "productId": 200,
            "skuCode": "GEMS-648",
            "priceMinor": 64800,
            "currency": "CNY",
            "published": True,
            "currencyType": 1,
            "currencyAmount": 6480,
            "title": "6480灵石充值包",
        }
        st, _ = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/skus/create",
            {"requestId": "adm-gem-sku-1", "skuId": 2001, "terms": terms, "stock": 100},
        )
        assert st == 200

        # 下单并付款
        st, ord1 = await h.signed(
            "gateway", "30001", "POST", "/api/orders",
            {"requestId": "gem-order-0001", "skuId": 2001, "quantity": 1, "buyerAccount": "acc_bob", "buyerOwnerRevision": "2"},
        )
        assert st == 200
        oid = ord1["orderId"]
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid, "channel": "apple", "transactionId": "apl-tx-1", "amountMinor": 64800, "currency": "CNY"},
        )
        assert st == 200

        # trade 拉取交付并回执 DELIVERED
        st, poll = await h.signed("delivery", "trade", "POST", "/internal/deliveries/poll", {"limit": 10, "cursor": ""})
        assert st == 200 and len(poll["entries"]) == 1
        intent = poll["entries"][0]["intent"]
        assert intent["orderId"] == oid and intent["currencyType"] == 1 and intent["currencyAmount"] == 6480

        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/deliveries/receipt",
            {"orderId": oid, "tradeOrderId": "90001", "state": "DELIVERED", "proof": PROOF_A},
        )
        assert st == 200

        # 渠道撤销付款（revision=1, REVOKED）-> 订单转 REVERSING
        st, rev1 = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments/revision",
            {"orderId": oid, "channel": "apple", "transactionId": "apl-tx-1", "revision": 1, "fact": "REVOKED"},
        )
        assert st == 200 and rev1 == {"revision": 1, "fact": "REVOKED", "orderState": "REVERSING"}

        # trade 拉取扣回待办，先报 MANUAL（模拟资产已被花光）
        st, rpoll = await h.signed("delivery", "trade", "POST", "/internal/reversals/poll", {"limit": 10, "cursor": ""})
        assert st == 200 and len(rpoll["entries"]) == 1
        assert rpoll["entries"][0]["revision"] == 1 and rpoll["entries"][0]["entitled"] is False

        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/reversals/receipt",
            {"orderId": oid, "revision": 1, "tradeOrderId": "90001", "state": "MANUAL", "proof": PROOF_B},
        )
        assert st == 200

        # 管理员列出人工单 -> 看到该订单
        st, manual = await h.signed("admin", "mall-admin", "POST", "/internal/admin/manual/list", {"limit": 20})
        assert st == 200 and len(manual["orders"]) == 1 and manual["orders"][0]["orderId"] == oid

        # 管理员发起 RETRY_REVERSAL -> 回到 REVERSING 且可重新拉取并回执 REVERSED
        st, res = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/manual/orders/resolve",
            {"requestId": "adm-retry-rev-1", "orderId": oid, "action": "RETRY_REVERSAL", "reason": "player topped up"},
        )
        assert st == 200 and res["resultingState"] == "REVERSING"

        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/reversals/receipt",
            {"orderId": oid, "revision": 1, "tradeOrderId": "90001", "state": "REVERSED", "proof": PROOF_C},
        )
        assert st == 200

        # 渠道撤回撤销（revision=2, PAID）-> 补发 REVERSING -> DELIVERED
        st, rev2 = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments/revision",
            {"orderId": oid, "channel": "apple", "transactionId": "apl-tx-1", "revision": 2, "fact": "PAID"},
        )
        assert st == 200 and rev2 == {"revision": 2, "fact": "PAID", "orderState": "REVERSING"}
        st, rpoll2 = await h.signed("delivery", "trade", "POST", "/internal/reversals/poll", {"limit": 10, "cursor": ""})
        assert st == 200 and rpoll2["entries"][0]["revision"] == 2 and rpoll2["entries"][0]["entitled"] is True

        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/reversals/receipt",
            {"orderId": oid, "revision": 2, "tradeOrderId": "90001", "state": "DELIVERED", "proof": PROOF_A},
        )
        assert st == 200
        st, view = await h.signed("admin", "mall-admin", "POST", "/internal/admin/order-view", {"orderId": oid})
        assert st == 200 and view["state"] == "DELIVERED" and view["paymentRevision"] == 2

        # 再测一个订单：交付前付款被撤销 -> trade 报 VOIDED + 退款人工单处置
        st, ord2 = await h.signed(
            "gateway", "30001", "POST", "/api/orders",
            {"requestId": "gem-order-0002", "skuId": 2001, "quantity": 1, "buyerAccount": "acc_bob", "buyerOwnerRevision": "2"},
        )
        oid2 = ord2["orderId"]
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid2, "channel": "apple", "transactionId": "apl-tx-2", "amountMinor": 64800, "currency": "CNY"},
        )
        assert st == 200
        # 交付被拒转 MANUAL -> 管理员 CREATE_REFUND -> 渠道关闭转 MANUAL -> 管理员 MARK_REFUNDED
        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/deliveries/receipt",
            {"orderId": oid2, "tradeOrderId": "90002", "state": "MANUAL", "proof": PROOF_A},
        )
        assert st == 200
        st, r_create = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/manual/orders/resolve",
            {"requestId": "adm-cr-ref-2", "orderId": oid2, "action": "CREATE_REFUND", "reason": "delivery impossible"},
        )
        assert st == 200 and r_create["resultingState"] == "REFUND_PENDING"
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/refunds/close",
            {"orderId": oid2, "channel": "apple", "paymentTransactionId": "apl-tx-2", "reason": "apple_no_api"},
        )
        assert st == 200
        st, r_mark = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/manual/refunds/resolve",
            {
                "requestId": "adm-mk-ref-2", "orderId": oid2, "channel": "apple",
                "paymentTransactionId": "apl-tx-2", "action": "MARK_REFUNDED",
                "externalRefundId": "offline-ref-2", "reason": "manual bank refund",
            },
        )
        assert st == 200 and r_mark["resultingState"] == "REFUNDED"
        st, view2 = await h.signed("admin", "mall-admin", "POST", "/internal/admin/order-view", {"orderId": oid2})
        assert st == 200 and view2["state"] == "REFUNDED"


# -- 5. 人民币寄售全生命周期、提现、退回与争议冻结/扣罚 -------------------------------------------------------

@_async_test
async def test_rmb_consignment_full_lifecycle_and_dispute_resolution() -> None:
    clock = FakeClock()
    cfg = make_config(consignment_notice_ms=10_000, consignment_sale_ms=60_000, consignment_cooling_ms=5_000, consignment_fee_bps=500)
    async with LiveMallHarness(cfg, clock) as h:
        # 1) 登记两条寄售（101 走成交提现，102 走卖家撤单退回，103 走争议冻结+扣罚）
        for lid, escrow, price in (("101", "8001", 10000), ("102", "8002", 5000), ("103", "8003", 20000)):
            st, listing = await h.signed(
                "delivery", "trade", "POST", "/internal/consignments/register",
                {
                    "listingId": lid,
                    "sellerPlayerId": "40001",
                    "sellerAccount": "seller_acc",
                    "marketZone": 1,
                    "assetKind": "ITEMS",
                    "assetCurrencyType": None,
                    "escrowReference": escrow,
                    "escrowProof": PROOF_A,
                    "priceFen": price,
                    "title": f"稀有神兵-{lid}",
                },
            )
            assert st == 200 and listing["state"] == "LISTED" and listing["phase"] == "PUBLIC_NOTICE"

        # 列表与查询
        st, lst = await h.signed("delivery", "trade", "POST", "/internal/consignments/list", {"afterListingId": "", "limit": 20})
        assert st == 200 and len(lst["listings"]) == 3
        st, got = await h.signed("delivery", "trade", "POST", "/internal/consignments/get", {"listingId": "101"})
        assert st == 200 and got["listingId"] == "101"

        # 公示期内不可购买（409）
        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/consignments/buy",
            {"listingId": "101", "buyerPlayerId": "40002", "buyerAccount": "buyer_acc", "requestId": "rmb-buy-101", "buyerOwnerRevision": "1"},
        )
        assert st == 409

        # 推进越过公示期
        clock.advance(10_001)

        # 102 卖家下架 -> 退回轮询与回执
        st, cancelled = await h.signed("delivery", "trade", "POST", "/internal/consignments/cancel", {"listingId": "102", "sellerAccount": "seller_acc"})
        assert st == 200 and cancelled["state"] == "RETURNING"
        st, ret_poll = await h.signed("delivery", "trade", "POST", "/internal/consignments/returns/poll", {"limit": 10, "cursor": ""})
        assert st == 200 and len(ret_poll["entries"]) == 1 and ret_poll["entries"][0]["intent"]["listingId"] == "102"
        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/consignments/returns/receipt",
            {"listingId": "102", "escrowReference": "8002", "escrowProof": PROOF_A, "returnProof": PROOF_B},
        )
        assert st == 200
        st, got102 = await h.signed("delivery", "trade", "POST", "/internal/consignments/get", {"listingId": "102"})
        assert st == 200 and got102["state"] == "RETURNED"

        # 101 买家下单 -> mock 支付 -> 交付轮询与回执 -> 卖家收益 -> 冷却期满提现 -> mock 出款
        st, buy101 = await h.signed(
            "delivery", "trade", "POST", "/internal/consignments/buy",
            {"listingId": "101", "buyerPlayerId": "40002", "buyerAccount": "buyer_acc", "requestId": "rmb-buy-101", "buyerOwnerRevision": "1"},
        )
        assert st == 200 and buy101["skuId"] == 0 and buy101["amountMinor"] == 10000
        oid101 = buy101["orderId"]

        st, paid101 = await h.signed("payment", "payment-adapter", "POST", "/internal/mock/pay", {"orderId": oid101, "requestId": "mock-pay-req-101"})
        assert st == 200 and paid101["state"] == "DELIVERY_PENDING"

        st, dpoll = await h.signed("delivery", "trade", "POST", "/internal/consignments/deliveries/poll", {"limit": 10, "cursor": ""})
        assert st == 200 and len(dpoll["entries"]) == 1 and dpoll["entries"][0]["intent"]["listingId"] == "101"

        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/deliveries/receipt",
            {"orderId": oid101, "tradeOrderId": "70001", "state": "DELIVERED", "proof": PROOF_C},
        )
        assert st == 200

        st, incs = await h.signed("delivery", "trade", "POST", "/internal/consignments/incomes", {"account": "seller_acc", "afterOrderId": "", "limit": 20})
        assert st == 200 and len(incs["incomes"]) == 1
        assert incs["incomes"][0]["grossFen"] == 10000 and incs["incomes"][0]["feeFen"] == 500 and incs["incomes"][0]["netFen"] == 9500

        # 冷却期内提现被拒 409
        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/consignments/withdrawals/create",
            {"account": "seller_acc", "requestId": "wd-req-101", "amountFen": 9500},
        )
        assert st == 409

        # 推进越过冷却期 -> 提现创建 + 查询 + mock 出款
        clock.advance(5_001)
        st, wd = await h.signed(
            "delivery", "trade", "POST", "/internal/consignments/withdrawals/create",
            {"account": "seller_acc", "requestId": "wd-req-101", "amountFen": 9500},
        )
        assert st == 200 and wd["state"] == "REQUESTED"
        wid = wd["withdrawalId"]

        st, wd_paid = await h.signed("payment", "payment-adapter", "POST", "/internal/mock/withdraw", {"withdrawalId": wid, "requestId": "mock-wd-req-101"})
        assert st == 200 and wd_paid["state"] == "PAID" and wd_paid["channel"] == "mock"

        st, wd_get = await h.signed("delivery", "trade", "POST", "/internal/consignments/withdrawals/get", {"account": "seller_acc", "withdrawalId": wid})
        assert st == 200 and wd_get["state"] == "PAID"

        # 103 交付后买家渠道撤款 -> 冻结冷却期收入并转 MANUAL -> 管理员 FORFEIT_INCOME
        st, buy103 = await h.signed(
            "delivery", "trade", "POST", "/internal/consignments/buy",
            {"listingId": "103", "buyerPlayerId": "40002", "buyerAccount": "buyer_acc", "requestId": "rmb-buy-103", "buyerOwnerRevision": "1"},
        )
        oid103 = buy103["orderId"]
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid103, "channel": "alipay", "transactionId": "ali-rmb-103", "amountMinor": 20000, "currency": "CNY"},
        )
        assert st == 200
        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/deliveries/receipt",
            {"orderId": oid103, "tradeOrderId": "70003", "state": "DELIVERED", "proof": PROOF_C},
        )
        assert st == 200
        st, rev103 = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments/revision",
            {"orderId": oid103, "channel": "alipay", "transactionId": "ali-rmb-103", "revision": 1, "fact": "REVOKED"},
        )
        assert st == 200 and rev103["orderState"] == "MANUAL"

        st, manual = await h.signed("admin", "mall-admin", "POST", "/internal/admin/manual/list", {"limit": 20})
        assert st == 200 and any(c["listingId"] == "103" for c in manual["consignments"])

        st, forfeit = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/manual/consignments/resolve",
            {"requestId": "adm-forfeit-103", "listingId": "103", "action": "FORFEIT_INCOME", "reason": "chargeback confirmed"},
        )
        assert st == 200 and forfeit["resultingState"] == "FORFEITED"


# -- 6. Mock 端点在 prod 被拦截、在 test 完整对账 -------------------------------------------------------------

@_async_test
async def test_mock_blocked_in_prod_and_reconcile_in_test() -> None:
    prod_cfg = make_config(run_mode="prod", require_tidb=True, mock_payment_enabled=False, allowed_channels=frozenset(("alipay",)))
    async with LiveMallHarness(prod_cfg) as h:
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/mock/pay",
            {"orderId": "00000000-0000-0000-0000-000000000001", "requestId": "mock-req-blocked"},
        )
        assert st == 503
        st, _ = await h.signed("payment", "payment-adapter", "POST", "/internal/mock/reconcile", {})
        assert st == 503
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/mock/withdraw",
            {"withdrawalId": "00000000-0000-0000-0000-000000000001", "requestId": "mock-wd-blocked"},
        )
        assert st == 503
        st, _ = await h.signed("payment", "payment-adapter", "POST", "/internal/mock/withdrawals/reconcile", {})
        assert st == 503

    # test 模式下 mock 付款后交付被拒 -> mock/reconcile 自动完成退款
    async with LiveMallHarness(make_config()) as h:
        st, _ = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/skus/create",
            {
                "requestId": "adm-mock-sku-1",
                "skuId": 3001,
                "terms": {
                    "productId": 300, "skuCode": "MOCK-1", "priceMinor": 100, "currency": "CNY",
                    "published": True, "itemConfigId": 1001, "countPerUnit": 1, "title": "测试礼包",
                },
                "stock": 5,
            },
        )
        assert st == 200
        st, ord1 = await h.signed(
            "gateway", "50001", "POST", "/api/orders",
            {"requestId": "mock-ord-0001", "skuId": 3001, "quantity": 1, "buyerAccount": "acc_m", "buyerOwnerRevision": "1"},
        )
        oid = ord1["orderId"]
        st, paid = await h.signed("payment", "payment-adapter", "POST", "/internal/mock/pay", {"orderId": oid, "requestId": "mock-pay-0001"})
        assert st == 200 and paid["state"] == "DELIVERY_PENDING"
        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/deliveries/receipt",
            {"orderId": oid, "tradeOrderId": "88001", "state": "REFUND_PENDING", "proof": PROOF_A},
        )
        assert st == 200
        st, rec = await h.signed("payment", "payment-adapter", "POST", "/internal/mock/reconcile", {})
        assert st == 200 and rec["attempted"] == 1 and rec["succeeded"] == 1 and rec["failed"] == 0
        st, view = await h.signed("payment", "payment-adapter", "POST", "/internal/payments/order-view", {"orderId": oid})
        assert st == 200 and view["state"] == "REFUNDED"
        st, wrec = await h.signed("payment", "payment-adapter", "POST", "/internal/mock/withdrawals/reconcile", {})
        assert st == 200 and wrec["attempted"] == 0


# -- 7. 真实协议线级兼容：HttpMallClient + MallAdapter 直连 loopback mall_game --------------------------------

@_async_test
async def test_wire_compatibility_with_http_mall_client_and_trade_mall_adapter() -> None:
    clock = FakeClock()
    cfg = make_config(consignment_notice_ms=1_000, consignment_sale_ms=60_000, consignment_cooling_ms=1_000)
    protocol = Protocol()

    async with LiveMallHarness(cfg, clock) as h:
        # 准备官方 SKU
        st, _ = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/skus/create",
            {
                "requestId": "adm-wire-sku-1",
                "skuId": 9001,
                "terms": {
                    "productId": 900, "skuCode": "WIRE-01", "priceMinor": 1200, "currency": "CNY",
                    "published": True, "itemConfigId": 7001, "countPerUnit": 3, "title": "联调礼包",
                },
                "stock": 50,
            },
        )
        assert st == 200

        pay_client = HttpMallClient(h.base_url, "payment-adapter", PAY_SECRET, clock_ms=clock)
        trade_adapter = MallAdapter(protocol, h.base_url, GW_SECRET, DEL_SECRET, clock_ms=clock)
        try:
            # 1) MallAdapter 官方商城流：列表 -> 下单 -> 查询 -> 列表
            skus_resp = await trade_adapter.list_official_skus(60001)
            assert len(skus_resp.skus) == 1 and skus_resp.skus[0].sku_id == 9001

            orig = RoleOriginal(player_id=60001, account="wire_buyer", owner_revision=1)
            create_req = protocol.message("trade.CreateOfficialOrderRequest", sku_id=9001, quantity=2, client_request_id="wire-ord-req-1")
            order_resp = await trade_adapter.create_official_order(orig, create_req)
            oid = order_resp.order.order_id
            assert order_resp.order.amount == 2400

            orders_list = await trade_adapter.list_official_orders(orig)
            assert len(orders_list) == 1 and orders_list[0].order_id == oid

            queried = await trade_adapter.get_or_cancel_official_order(orig, oid, cancel=False)
            assert queried.order.order_id == oid

            # 2) HttpMallClient 支付流：order_view -> record_payment -> push_revision
            mall_ord = await pay_client.order_view(oid)
            assert mall_ord.order_id == oid and mall_ord.amount_minor == 2400 and mall_ord.state == "AWAITING_PAYMENT"

            outcome = await pay_client.record_payment(
                MallPayment(order_id=oid, channel="devpay", transaction_id="dev-wire-1", amount_minor=2400, currency="CNY")
            )
            assert outcome.order_state == "DELIVERY_PENDING" and outcome.refund_reason == ""

            # 模拟重复付款，验证 pending_refunds / report_refund / close_refund
            dup_out = await pay_client.record_payment(
                MallPayment(order_id=oid, channel="devpay", transaction_id="dev-wire-dup", amount_minor=2400, currency="CNY")
            )
            assert dup_out.refund_reason == "DUPLICATE_PAYMENT"

            refund_page = await pay_client.pending_refunds()
            assert len(refund_page.entries) == 1 and refund_page.entries[0].transaction_id == "dev-wire-dup"

            await pay_client.report_refund(
                MallRefund(
                    order_id=oid, channel="devpay", payment_transaction_id="dev-wire-dup",
                    transaction_id="dev-ref-dup-1", amount_minor=2400, currency="CNY",
                )
            )
            assert len((await pay_client.pending_refunds()).entries) == 0

            # 再造一条金额不符异常付款并用 close_refund 关闭
            await pay_client.record_payment(
                MallPayment(order_id=oid, channel="devpay", transaction_id="dev-wire-bad", amount_minor=100, currency="CNY")
            )
            await pay_client.close_refund(
                MallRefundClosure(order_id=oid, channel="devpay", payment_transaction_id="dev-wire-bad", reason="manual_test")
            )

            rev_num = await pay_client.push_revision(
                MallRevision(order_id=oid, channel="devpay", transaction_id="dev-wire-1", revision=1, fact="REVOKED")
            )
            assert rev_num == 1

            # 3) MallAdapter 人民币寄售流：register -> list -> get -> buy -> order -> orders -> cancel
            rec = protocol.message("trade.TradeCommerceRecord", commerce_id=55501, seller_player_id=60002)
            wf = protocol.message("trade.TradeCommerceWorkflow")
            wf.rmb.seller_account = "wire_seller"
            wf.rmb.market_zone = 1
            wf.rmb.asset_kind = "ITEMS"
            wf.rmb.escrow_reference = "77701"
            wf.rmb.escrow_proof = hashlib.sha256(b"escrow-wire").hexdigest()
            wf.rmb.price_fen = 3000
            wf.rmb.title = "联调寄售法宝"

            reg_view = await trade_adapter.rmb_register(rec, wf)
            assert int(reg_view.listing_id) == 55501 and reg_view.state == "LISTED"

            list_resp = await trade_adapter.rmb_list()
            assert len(list_resp.listings) == 1
            got_view = await trade_adapter.rmb_get(55501)
            assert int(got_view.listing_id) == 55501

            clock.advance(2_000)
            buy_req = protocol.message("trade.BuyRmbConsignmentRequest", listing_id=55501, client_request_id="wire-rmb-buy-1")
            rmb_ord = await trade_adapter.rmb_buy("wire_buyer", 60001, 1, buy_req)
            rmb_oid = rmb_ord.order.order_id
            assert rmb_ord.order.amount == 3000

            got_ord = await trade_adapter.rmb_order("wire_buyer", rmb_oid, cancel=False)
            assert got_ord.order.order_id == rmb_oid
            rmb_orders = await trade_adapter.rmb_orders("wire_buyer")
            assert len(rmb_orders.orders) == 1

            cancelled_ord = await trade_adapter.rmb_order("wire_buyer", rmb_oid, cancel=True)
            assert cancelled_ord.order.order_id == rmb_oid

            cancelled_listing = await trade_adapter.rmb_cancel("wire_seller", 55501)
            assert cancelled_listing.state == "RETURNING"

            incomes_resp = await trade_adapter.rmb_incomes("wire_seller")
            assert len(incomes_resp.incomes) == 0
        finally:
            await pay_client.close()
            await trade_adapter.close()


# -- 8. 补充覆盖：VOIDED 交付、全部人工处置动作、订单过期清理与 46 路由全量清点 ------------------------------

@_async_test
async def test_voided_delivery_all_manual_actions_expiry_and_route_inventory() -> None:
    clock = FakeClock()
    cfg = make_config(consignment_notice_ms=0, consignment_sale_ms=60_000, consignment_cooling_ms=10_000)
    async with LiveMallHarness(cfg, clock) as h:
        # 校验全部 46 个业务路由 + 3 个健康路由均已注册
        routes = {
            (r.method, r.resource.canonical)
            for r in h.app.router.routes()
            if r.method in ("GET", "POST")
        }
        expected_business_routes = {
            ("POST", "/api/orders"),
            ("GET", "/api/orders/{orderId}"),
            ("POST", "/api/orders/query"),
            ("POST", "/api/orders/list"),
            ("POST", "/api/orders/cancel"),
            ("POST", "/api/orders/{orderId}/cancel"),
            ("POST", "/internal/payments"),
            ("POST", "/internal/payments/order-view"),
            ("POST", "/internal/payments/skus"),
            ("POST", "/internal/payments/revision"),
            ("GET", "/api/skus"),
            ("POST", "/api/skus/list"),
            ("POST", "/internal/deliveries/poll"),
            ("POST", "/internal/deliveries/receipt"),
            ("POST", "/internal/refunds/poll"),
            ("POST", "/internal/refunds/receipt"),
            ("POST", "/internal/refunds/close"),
            ("POST", "/internal/reversals/poll"),
            ("POST", "/internal/reversals/receipt"),
            ("POST", "/internal/admin/order-view"),
            ("POST", "/internal/admin/manual/list"),
            ("POST", "/internal/admin/manual/orders/resolve"),
            ("POST", "/internal/admin/manual/refunds/resolve"),
            ("POST", "/internal/admin/manual/consignments/resolve"),
            ("POST", "/internal/admin/skus/list"),
            ("POST", "/internal/admin/skus/create"),
            ("POST", "/internal/admin/skus/update"),
            ("POST", "/internal/admin/skus/stock"),
            ("POST", "/internal/consignments/register"),
            ("POST", "/internal/consignments/list"),
            ("POST", "/internal/consignments/get"),
            ("POST", "/internal/consignments/cancel"),
            ("POST", "/internal/consignments/buy"),
            ("POST", "/internal/consignments/orders/get"),
            ("POST", "/internal/consignments/orders/list"),
            ("POST", "/internal/consignments/orders/cancel"),
            ("POST", "/internal/consignments/deliveries/poll"),
            ("POST", "/internal/consignments/returns/poll"),
            ("POST", "/internal/consignments/returns/receipt"),
            ("POST", "/internal/consignments/incomes"),
            ("POST", "/internal/consignments/withdrawals/create"),
            ("POST", "/internal/consignments/withdrawals/get"),
            ("POST", "/internal/mock/pay"),
            ("POST", "/internal/mock/reconcile"),
            ("POST", "/internal/mock/withdraw"),
            ("POST", "/internal/mock/withdrawals/reconcile"),
        }
        assert len(expected_business_routes) == 46
        assert expected_business_routes.issubset(routes)
        assert ("GET", "/actuator/health") in routes

        # 创建 SKU
        st, _ = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/skus/create",
            {
                "requestId": "adm-edge-sku-1",
                "skuId": 4001,
                "terms": {
                    "productId": 400, "skuCode": "EDGE-01", "priceMinor": 500, "currency": "CNY",
                    "published": True, "itemConfigId": 4001, "countPerUnit": 1, "title": "边界测试包",
                },
                "stock": 20,
            },
        )
        assert st == 200

        # A) 交付前撤销付款 -> deliveries/poll 带 paymentRevision=1 -> 回执 VOIDED -> reversals/receipt REVERSED
        st, o_void = await h.signed(
            "gateway", "70001", "POST", "/api/orders",
            {"requestId": "edge-ord-void-1", "skuId": 4001, "quantity": 1, "buyerAccount": "acc_v", "buyerOwnerRevision": "1"},
        )
        oid_v = o_void["orderId"]
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": oid_v, "channel": "alipay", "transactionId": "ali-void-1", "amountMinor": 500, "currency": "CNY"},
        )
        assert st == 200
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments/revision",
            {"orderId": oid_v, "channel": "alipay", "transactionId": "ali-void-1", "revision": 1, "fact": "REVOKED"},
        )
        assert st == 200
        st, dpoll = await h.signed("delivery", "trade", "POST", "/internal/deliveries/poll", {"limit": 10, "cursor": ""})
        assert st == 200 and dpoll["entries"][0]["paymentRevision"] == 1
        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/deliveries/receipt",
            {"orderId": oid_v, "tradeOrderId": "99001", "state": "VOIDED", "proof": PROOF_A},
        )
        assert st == 200
        # 扣回回执先报 MANUAL，再由管理员 MARK_REVERSED 收敛
        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/reversals/receipt",
            {"orderId": oid_v, "revision": 1, "tradeOrderId": "99001", "state": "MANUAL", "proof": PROOF_B},
        )
        assert st == 200
        st, mr = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/manual/orders/resolve",
            {"requestId": "adm-mk-rev-1", "orderId": oid_v, "action": "MARK_REVERSED", "reason": "confirmed void"},
        )
        assert st == 200 and mr["resultingState"] == "REVERSED"

        # B) 寄售未交付撤款 -> RETURN_TO_SELLER；寄售已交付撤款冻结 -> UNFREEZE_INCOME
        for lid, esc in (("201", "9101"), ("202", "9102")):
            st, _ = await h.signed(
                "delivery", "trade", "POST", "/internal/consignments/register",
                {
                    "listingId": lid, "sellerPlayerId": "70010", "sellerAccount": "sell_m",
                    "marketZone": 1, "assetKind": "ITEMS", "assetCurrencyType": None,
                    "escrowReference": esc, "escrowProof": PROOF_A, "priceFen": 1000, "title": f"人工测试-{lid}",
                },
            )
            assert st == 200

        # 201 付款后未交付即撤款 -> RETURN_TO_SELLER
        st, b201 = await h.signed(
            "delivery", "trade", "POST", "/internal/consignments/buy",
            {"listingId": "201", "buyerPlayerId": "70011", "buyerAccount": "buy_m", "requestId": "rmb-m-201", "buyerOwnerRevision": "1"},
        )
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": b201["orderId"], "channel": "alipay", "transactionId": "ali-m-201", "amountMinor": 1000, "currency": "CNY"},
        )
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments/revision",
            {"orderId": b201["orderId"], "channel": "alipay", "transactionId": "ali-m-201", "revision": 1, "fact": "REVOKED"},
        )
        st, r201 = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/manual/consignments/resolve",
            {"requestId": "adm-ret-201", "listingId": "201", "action": "RETURN_TO_SELLER", "reason": "buyer revoked before delivery"},
        )
        assert st == 200 and r201["resultingState"] == "RETURNING"

        # 202 交付后撤款冻结 -> 管理员 UNFREEZE_INCOME 解冻为 SOLD
        st, b202 = await h.signed(
            "delivery", "trade", "POST", "/internal/consignments/buy",
            {"listingId": "202", "buyerPlayerId": "70011", "buyerAccount": "buy_m", "requestId": "rmb-m-202", "buyerOwnerRevision": "1"},
        )
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments",
            {"orderId": b202["orderId"], "channel": "alipay", "transactionId": "ali-m-202", "amountMinor": 1000, "currency": "CNY"},
        )
        st, _ = await h.signed(
            "delivery", "trade", "POST", "/internal/deliveries/receipt",
            {"orderId": b202["orderId"], "tradeOrderId": "99202", "state": "DELIVERED", "proof": PROOF_A},
        )
        st, _ = await h.signed(
            "payment", "payment-adapter", "POST", "/internal/payments/revision",
            {"orderId": b202["orderId"], "channel": "alipay", "transactionId": "ali-m-202", "revision": 1, "fact": "REVOKED"},
        )
        st, u202 = await h.signed(
            "admin", "mall-admin", "POST", "/internal/admin/manual/consignments/resolve",
            {"requestId": "adm-unf-202", "listingId": "202", "action": "UNFREEZE_INCOME", "reason": "dispute won"},
        )
        assert st == 200 and u202["resultingState"] == "SOLD"

        # C) 未支付订单超期自动清理 + 过期 nonce 清理
        st, o_exp = await h.signed(
            "gateway", "70001", "POST", "/api/orders",
            {"requestId": "edge-ord-exp-1", "skuId": 4001, "quantity": 1, "buyerAccount": "acc_v", "buyerOwnerRevision": "1"},
        )
        assert st == 200
        clock.advance(cfg.order_ttl_ms + 60_001)
        expired_count = await h.store.expire_overdue_orders()
        assert expired_count == 1
        pruned_nonces = await h.store.prune_expired_nonces()
        assert pruned_nonces > 0
        st, exp_view = await h.signed("admin", "mall-admin", "POST", "/internal/admin/order-view", {"orderId": o_exp["orderId"]})
        assert st == 200 and exp_view["state"] == "CANCELLED"

