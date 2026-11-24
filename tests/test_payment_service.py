"""payment 服务全面测试：gRPC 鉴权与契约、aiohttp 回调服务、商城 HTTP v1 HMAC、
后台工作器状态机、SubmitPurchase 语义、未成年人限额与 SqlPaymentStore DDL/SQL 验证。"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import hmac
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass

import aiohttp
from aiohttp import web
import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata
from xuanming.login.caller_auth import CallerVerifier
from xuanming.payment import (
    AckKind,
    CHANNEL_APPLE,
    CHANNEL_DEVPAY,
    CHANNEL_GOOGLE,
    CHANNEL_WECHAT_MINIGAME,
    ChannelNotification,
    ChannelOrder,
    ChannelPayment,
    ChannelPrepared,
    ChannelQueryRequest,
    ChannelQueryResult,
    ChannelReceipt,
    ChannelRefundRequest,
    ChannelRefundResult,
    ChannelRefundState,
    ChannelRevocation,
    ClientParams,
    DEVPAY_SIGNATURE_HEADER,
    DevPayChannel,
    ERR_CHANNEL_MISMATCH,
    ERR_CHANNEL_UNAVAILABLE,
    ERR_CURRENCY_UNSUPPORTED,
    ERR_INVALID_PARAMETER,
    ERR_MINOR_DAILY_LIMIT,
    ERR_MINOR_MONTHLY_LIMIT,
    ERR_MINOR_NOT_ALLOWED,
    ERR_MINOR_SINGLE_LIMIT,
    ERR_ORDER_EXPIRED,
    ERR_ORDER_NOT_FOUND,
    ERR_ORDER_NOT_PAYABLE,
    FACT_REVOKED,
    FINALIZE_DONE,
    FINALIZE_PENDING,
    FactConflictError,
    HttpMallClient,
    INTENT_CLOSED,
    INTENT_CREATED,
    INTENT_PAID,
    InvalidKeyError,
    MALL_BOUND,
    MALL_ORPHAN,
    MALL_PENDING,
    MALL_REFUND_QUEUED,
    MALL_REJECTED,
    MAX_BODY_BYTES,
    MINOR_SPENDING_GATE_NONE,
    MallConflictError,
    MallInvalidError,
    MallNotFoundError,
    MallOrder,
    MallPayment,
    MallPendingRefund,
    MallStatusError,
    MemoryPaymentStore,
    ORDER_STATE_AWAITING_PAYMENT,
    PAYMENT_NODE_TYPE,
    PAY_CHANNEL_APPLE,
    PAY_CHANNEL_DEVPAY,
    PAY_CHANNEL_GOOGLE,
    PAY_CHANNEL_WECHAT_MINIGAME,
    PLATFORM_ANDROID_APP,
    PLATFORM_DESKTOP,
    PLATFORM_IOS_APP,
    PLATFORM_WECHAT_MINIGAME,
    PaymentConfig,
    PaymentCore,
    PaymentGrpcService,
    PaymentIntentRecord,
    PaymentNotifyServer,
    PaymentOptions,
    PaymentTxnRecord,
    PaymentWorkers,
    ProductMapping,
    REFUND_FAILED,
    REFUND_REPORTED,
    REFUND_REQUESTED,
    REFUND_UNSUPPORTED,
    REVOCATION_TOMBSTONE_REASON,
    SCHEMA_FILE,
    SpendLimit,
    SqlPaymentStore,
    TradeState,
    devpay_sign,
    mall_signature,
    order_id_from_key,
    order_id_from_out_trade_no,
    order_key,
    out_refund_no,
    out_trade_no,
    pi_birth_prefix,
    sorted_sha1,
    spender_key,
    txn_key,
)
from xuanming.protocol import Protocol

MALL_SECRET = "commerce-golden-payment-secret-0123456789"
DEVPAY_SECRET = "devpay-local-secret-0123456789abcdef"
GATE_SEED = bytes(range(32))
GATE_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(GATE_SEED)
GATE_PUBLIC_KEY = GATE_PRIVATE_KEY.public_key()

ORDER_1 = "00112233-4455-6677-8899-aabbccddeeff"
ORDER_2 = "11112222-3333-4444-5555-666677778888"
ORDER_3 = "22223333-4444-5555-6666-777788889999"
BASE_NOW_MS = 1_700_000_000_000


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class ManualClock:
    def __init__(self, ms: int = BASE_NOW_MS) -> None:
        self.ms = ms

    def __call__(self) -> int:
        return self.ms

    def advance(self, delta_ms: int) -> int:
        self.ms += delta_ms
        return self.ms


def _session_bytes(
    protocol: Protocol,
    *,
    player_id: int = 1001,
    account: str = "acc_1001",
    owner_revision: int = 1,
    session_id: int = 77,
    gate_instance_id: str = "gate-1",
    session_incarnation: int = 1,
) -> bytes:
    msg = protocol.message(
        "SessionDetails",
        session_id=session_id,
        gate_instance_id=gate_instance_id,
        session_incarnation=session_incarnation,
        player_id=player_id,
        authenticated_account=account,
        owner_revision=owner_revision,
    )
    return msg.SerializeToString()


def _signed_metadata(
    protocol: Protocol,
    path: str,
    *,
    player_id: int = 1001,
    account: str = "acc_1001",
    owner_revision: int = 1,
    signer: Ed25519PrivateKey = GATE_PRIVATE_KEY,
) -> tuple:
    raw = _session_bytes(protocol, player_id=player_id, account=account, owner_revision=owner_revision)
    return session_metadata(raw, path, signer)


class FakeMallHttpServer:
    """真实 aiohttp 商城内部接口桩，严格验证 v1 HMAC 签名、nonce 单次消费与全部 6 个端点。"""

    def __init__(self, secret: str = MALL_SECRET, subject: str = "payment-adapter") -> None:
        self.secret = secret
        self.subject = subject
        self.orders: dict[str, dict] = {}
        self.recorded_payments: list[dict] = []
        self.revisions: list[dict] = []
        self.pending_refunds_pages: list[dict] = [{"entries": [], "nextCursor": ""}]
        self.refund_receipts: list[dict] = []
        self.refund_closures: list[dict] = []
        self.seen_nonces: set[str] = set()
        self.payment_refund_reason: str | None = None
        self.payment_status_override: int | None = None
        self._runner: web.AppRunner | None = None
        self.base_url: str = ""

    def put_order(
        self,
        order_id: str,
        *,
        buyer_id: int = 1001,
        sku_id: int = 10,
        quantity: int = 1,
        amount_minor: int = 600,
        currency: str = "CNY",
        state: str = ORDER_STATE_AWAITING_PAYMENT,
        deadline_ms: int = BASE_NOW_MS + 30 * 60 * 1000,
    ) -> None:
        self.orders[order_id] = {
            "orderId": order_id,
            "buyerId": str(buyer_id),
            "skuId": sku_id,
            "quantity": quantity,
            "amountMinor": amount_minor,
            "currency": currency,
            "state": state,
            "paymentDeadlineMs": deadline_ms,
        }

    async def start(self) -> str:
        app = web.Application()
        app.router.add_post("/internal/payments/order-view", self._handle)
        app.router.add_post("/internal/payments", self._handle)
        app.router.add_post("/internal/payments/revision", self._handle)
        app.router.add_post("/internal/refunds/poll", self._handle)
        app.router.add_post("/internal/refunds/receipt", self._handle)
        app.router.add_post("/internal/refunds/close", self._handle)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"
        return self.base_url

    async def close(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _handle(self, request: web.Request) -> web.Response:
        body = await request.read()
        subject = request.headers.get("X-Commerce-Subject", "")
        ts = request.headers.get("X-Commerce-Timestamp", "")
        nonce = request.headers.get("X-Commerce-Nonce", "")
        sig = request.headers.get("X-Commerce-Signature", "")
        if subject != self.subject or not ts or len(nonce) != 32:
            return web.Response(status=401, text="bad headers")
        if nonce in self.seen_nonces:
            return web.Response(status=409, text="replayed nonce")
        expected = mall_signature(self.secret, "payment", subject, ts, nonce, "POST", request.path, body)
        if not hmac.compare_digest(sig, expected):
            return web.Response(status=401, text="bad signature")
        self.seen_nonces.add(nonce)

        payload = json.loads(body.decode("utf-8")) if body else {}
        if request.path == "/internal/payments/order-view":
            order = self.orders.get(payload.get("orderId", ""))
            if order is None:
                return web.Response(status=404, text="not found")
            return web.json_response(order)

        if request.path == "/internal/payments":
            if self.payment_status_override is not None:
                return web.Response(status=self.payment_status_override, text="override")
            self.recorded_payments.append(payload)
            resp = {"order": {"orderId": payload["orderId"], "state": "PAID"}}
            if self.payment_refund_reason is not None:
                resp["refundReason"] = self.payment_refund_reason
            return web.json_response(resp)

        if request.path == "/internal/payments/revision":
            self.revisions.append(payload)
            return web.json_response({"revision": payload["revision"]})

        if request.path == "/internal/refunds/poll":
            cursor = payload.get("cursor", "")
            idx = 0 if not cursor else int(cursor)
            page = self.pending_refunds_pages[idx] if idx < len(self.pending_refunds_pages) else {"entries": [], "nextCursor": ""}
            return web.json_response(page)

        if request.path == "/internal/refunds/receipt":
            self.refund_receipts.append(payload)
            return web.json_response({"ok": True})

        if request.path == "/internal/refunds/close":
            self.refund_closures.append(payload)
            return web.json_response({"ok": True})

        return web.Response(status=404)


class StubStoreChannel:
    """可配置的商店渠道桩（支持 Google Play / App Store / Finalizer / Handshake 测试）。"""

    def __init__(
        self,
        channel_id: int,
        name: str,
        *,
        needs_product: bool = False,
        verify_fn=None,
        query_fn=None,
        refund_fn=None,
        finalize_fn=None,
        account_ref_fn=None,
    ) -> None:
        self._id = channel_id
        self._name = name
        self._needs_product = needs_product
        self._verify_fn = verify_fn
        self._query_fn = query_fn
        self._refund_fn = refund_fn
        self._finalize_fn = finalize_fn
        self._account_ref_fn = account_ref_fn
        self.closed_trades: list[str] = []

    @property
    def channel_id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    def supports(self, currency: str) -> bool:
        return bool(currency)

    def needs_product(self) -> bool:
        return self._needs_product

    async def prepare(self, order: ChannelOrder, cached_ref_value: str, cached_ref_ms: int) -> ChannelPrepared:
        if self._id == PAY_CHANNEL_APPLE:
            return ChannelPrepared(
                params=ClientParams(apple={"product_id": order.product_id, "app_account_token": order.order_id})
            )
        if self._id == PAY_CHANNEL_GOOGLE:
            return ChannelPrepared(
                params=ClientParams(
                    google={
                        "product_id": order.product_id,
                        "obfuscated_account_id": self.account_ref(order.player_id),
                        "obfuscated_profile_id": order.order_id,
                    }
                )
            )
        return ChannelPrepared(params=ClientParams(devpay={"out_trade_no": order.out_trade_no}))

    async def query(self, req: ChannelQueryRequest) -> ChannelQueryResult:
        if self._query_fn is not None:
            return await self._query_fn(req)
        return ChannelQueryResult(state=TradeState.NOT_PAID)

    async def close_trade(self, out_trade_no_val: str) -> None:
        self.closed_trades.append(out_trade_no_val)

    async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult:
        if self._refund_fn is not None:
            return await self._refund_fn(req)
        return REFUND_UNSUPPORTED

    async def parse_notification(self, headers, query, body: bytes) -> ChannelNotification:
        return ChannelNotification()

    def format_ack(self, kind: AckKind) -> tuple[int, bytes, str]:
        if kind == AckKind.OK:
            return 200, b"ok", "text/plain; charset=utf-8"
        if kind == AckKind.REJECTED:
            return 401, b"", "text/plain; charset=utf-8"
        return 503, b"", "text/plain; charset=utf-8"

    async def verify(self, receipt: ChannelReceipt) -> ChannelQueryResult:
        return await self._verify_fn(receipt)

    def account_ref(self, player_id: int) -> str:
        if self._account_ref_fn is not None:
            return self._account_ref_fn(player_id)
        return f"acc-{player_id}"

    async def finalize(self, product_id: str, channel_token: str) -> None:
        if self._finalize_fn is not None:
            await self._finalize_fn(product_id, channel_token)


class StubMinigameHandshakeChannel(DevPayChannel):
    """带 GET echostr 握手能力的小游戏渠道桩。"""

    def __init__(self, push_token: str) -> None:
        super().__init__(DEVPAY_SECRET)
        self._push_token = push_token

    @property
    def channel_id(self) -> int:
        return PAY_CHANNEL_WECHAT_MINIGAME

    @property
    def name(self) -> str:
        return CHANNEL_WECHAT_MINIGAME

    def handshake(self, query) -> tuple[int, bytes, str] | None:
        echostr = query.get("echostr", "")
        expected = sorted_sha1(self._push_token, query.get("timestamp", ""), query.get("nonce", ""))
        if not echostr or not hmac.compare_digest(query.get("signature", ""), expected):
            return 401, b"", "text/plain; charset=utf-8"
        return 200, echostr.encode("utf-8"), "text/plain; charset=utf-8"


# ---------------------------------------------------------------------------
# 1. 黄金样例签名、确定性键派生与配置校验
# ---------------------------------------------------------------------------


def test_mall_signature_matches_cross_language_golden() -> None:
    body = (
        b'{"orderId":"00112233-4455-6677-8899-aabbccddeeff","channel":"mock-dev",'
        b'"transactionId":"mockdev-00112233445566778899aabbccddeeff","amountMinor":600,"currency":"CNY"}'
    )
    got = mall_signature(
        MALL_SECRET,
        "payment",
        "payment-adapter",
        "1700000000123",
        "golden-nonce-0000000001",
        "POST",
        "/internal/payments",
        body,
    )
    assert got == "886584609f3fb1fa50a5ddb30f02837fb7e75d11fa0075f344447872c7eaf051"


def test_deterministic_keys_match_go_spec() -> None:
    hi, lo = order_key(ORDER_1)
    assert hi == 0x0011223344556677
    assert lo == 0x8899AABBCCDDEEFF
    assert order_id_from_key(hi, lo) == ORDER_1

    trade_no = out_trade_no(ORDER_1)
    assert trade_no == "00112233445566778899aabbccddeeff"
    assert len(trade_no) == 32
    assert order_id_from_out_trade_no(trade_no) == ORDER_1

    with pytest.raises(InvalidKeyError):
        order_key("00000000-0000-0000-0000-000000000000")
    with pytest.raises(InvalidKeyError):
        order_key("NOT-A-UUID")
    with pytest.raises(InvalidKeyError):
        order_id_from_out_trade_no("00000000000000000000000000000000")

    tx_hi, tx_lo = txn_key("devpay-abc")
    raw_tx = hashlib.sha256(b"devpay-abc").digest()
    assert (tx_hi, tx_lo) == (int.from_bytes(raw_tx[:8], "big"), int.from_bytes(raw_tx[8:16], "big"))

    sp_hi, sp_lo = spender_key("pi-123")
    raw_sp = hashlib.sha256(b"spender|pi-123").digest()
    assert (sp_hi, sp_lo) == (int.from_bytes(raw_sp[:8], "big"), int.from_bytes(raw_sp[8:16], "big"))

    rf = out_refund_no("devpay", "tx-1")
    raw_rf = hashlib.sha256(b"refund|devpay|tx-1").digest()
    assert rf == "rf" + raw_rf[:15].hex()
    assert len(rf) == 32


def test_config_fail_closed_and_canary_mode() -> None:
    assert PAYMENT_NODE_TYPE == 19
    valid_cfg = PaymentConfig(
        advertise_host="127.0.0.1",
        mall_payment_secret=MALL_SECRET.encode(),
        devpay_secret=DEVPAY_SECRET.encode(),
        release_track="canary",
    )
    valid_cfg.validate()
    assert not valid_cfg.runs_background_work

    stable_cfg = PaymentConfig(
        advertise_host="127.0.0.1",
        mall_payment_secret=MALL_SECRET.encode(),
        devpay_secret=DEVPAY_SECRET.encode(),
        release_track="stable",
    )
    stable_cfg.validate()
    assert stable_cfg.runs_background_work

    # 生产模式禁止启用 devpay
    with pytest.raises(ValueError, match="devpay"):
        PaymentConfig(
            advertise_host="10.0.0.1",
            mode="pro",
            mall_payment_secret=MALL_SECRET.encode(),
            devpay_enabled=True,
            devpay_secret=DEVPAY_SECRET.encode(),
        ).validate()

    # 生产模式合规闸为 none 时禁止开启 App 真钱渠道
    with pytest.raises(ValueError, match="minor_spending_gate=none"):
        PaymentConfig(
            advertise_host="10.0.0.1",
            mode="pro",
            minor_spending_gate=MINOR_SPENDING_GATE_NONE,
            mall_payment_secret=MALL_SECRET.encode(),
            devpay_enabled=False,
            apple_enabled=True,
            platforms=(("ios_app", (CHANNEL_APPLE,)),),
        ).validate()

    # iOS App 不允许挂载 google（苹果 3.1.1 规则）
    with pytest.raises(ValueError, match="cannot be offered"):
        PaymentConfig(
            advertise_host="127.0.0.1",
            mall_payment_secret=MALL_SECRET.encode(),
            devpay_enabled=False,
            google_enabled=True,
            platforms=(("ios_app", (CHANNEL_GOOGLE,)),),
        ).validate()


# ---------------------------------------------------------------------------
# 2. Gate Ed25519 鉴权、ListPayChannels / CreatePayIntent / GetPayIntent 全链路 gRPC 测试
# ---------------------------------------------------------------------------


@_async_test
async def test_grpc_auth_and_intent_lifecycle_with_devpay_webhook() -> None:
    protocol = Protocol()
    clock = ManualClock(BASE_NOW_MS)
    mall_server = FakeMallHttpServer()
    await mall_server.start()
    mall_server.put_order(ORDER_1, buyer_id=1001, amount_minor=600, currency="CNY", deadline_ms=BASE_NOW_MS + 300_000)
    mall_server.put_order(ORDER_2, buyer_id=2002, amount_minor=900, currency="CNY", deadline_ms=BASE_NOW_MS + 300_000)
    mall_server.put_order(ORDER_3, buyer_id=1001, amount_minor=500, currency="CNY", deadline_ms=BASE_NOW_MS + 30_000)

    store = MemoryPaymentStore()
    mall_client = HttpMallClient(mall_server.base_url, "payment-adapter", MALL_SECRET, clock_ms=clock)
    devpay = DevPayChannel(DEVPAY_SECRET, clock_ms=clock)
    apple_stub = StubStoreChannel(PAY_CHANNEL_APPLE, CHANNEL_APPLE)
    core = PaymentCore(
        store,
        mall_client,
        [devpay, apple_stub],
        {
            PLATFORM_ANDROID_APP: [PAY_CHANNEL_DEVPAY],
            PLATFORM_IOS_APP: [PAY_CHANNEL_APPLE, PAY_CHANNEL_DEVPAY],
            PLATFORM_DESKTOP: [PAY_CHANNEL_DEVPAY],
        },
        clock_ms=clock,
    )
    verifier = CallerVerifier([GATE_PUBLIC_KEY], allowed_callers=("gate",))
    grpc_svc = PaymentGrpcService(protocol, core, caller_verifier=verifier)
    grpc_server, grpc_port = await serve_grpc(protocol, "127.0.0.1:0", grpc_svc.services(), pass_context=True)
    notify_server = PaymentNotifyServer(core, host="127.0.0.1", port=0)
    notify_port = await notify_server.start()
    client = GrpcClient(f"127.0.0.1:{grpc_port}")

    try:
        # --- 鉴权 fail-closed ---
        list_path = "/paymentpb.ClientPlayerPayment/ListPayChannels"
        list_req = protocol.message("paymentpb.ListPayChannelsRequest", platform=PLATFORM_IOS_APP)

        # 1) 缺 gate 会话 -> PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as exc_missing:
            await client.call(list_path, list_req.SerializeToString())
        assert exc_missing.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 2) 错误签名 -> UNAUTHENTICATED
        bad_key = Ed25519PrivateKey.generate()
        with pytest.raises(grpc.aio.AioRpcError) as exc_bad_sig:
            await client.call(
                list_path,
                list_req.SerializeToString(),
                metadata=_signed_metadata(protocol, list_path, signer=bad_key),
            )
        assert exc_bad_sig.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # 3) player_id == 0 或 owner_revision == 0 或空账号 -> PERMISSION_DENIED
        for bad_kwargs in (
            {"player_id": 0},
            {"owner_revision": 0},
            {"account": ""},
        ):
            with pytest.raises(grpc.aio.AioRpcError) as exc_inc:
                await client.call(
                    list_path,
                    list_req.SerializeToString(),
                    metadata=_signed_metadata(protocol, list_path, **bad_kwargs),
                )
            assert exc_inc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # --- ListPayChannels ---
        raw = await client.call(
            list_path,
            list_req.SerializeToString(),
            metadata=_signed_metadata(protocol, list_path),
        )
        list_resp = protocol.parse("paymentpb.ListPayChannelsResponse", raw)
        assert list_resp.error_message.id == 0
        assert list(list_resp.channels) == [PAY_CHANNEL_APPLE, PAY_CHANNEL_DEVPAY]

        # 非法 platform -> 1005
        bad_plat_req = protocol.message("paymentpb.ListPayChannelsRequest", platform=0)
        raw = await client.call(
            list_path,
            bad_plat_req.SerializeToString(),
            metadata=_signed_metadata(protocol, list_path),
        )
        assert protocol.parse("paymentpb.ListPayChannelsResponse", raw).error_message.id == ERR_INVALID_PARAMETER

        # --- CreatePayIntent ---
        create_path = "/paymentpb.ClientPlayerPayment/CreatePayIntent"
        create_req = protocol.message(
            "paymentpb.CreatePayIntentRequest",
            order_id=ORDER_1,
            channel=PAY_CHANNEL_DEVPAY,
            platform=PLATFORM_IOS_APP,
        )
        raw = await client.call(
            create_path,
            create_req.SerializeToString(),
            metadata=_signed_metadata(protocol, create_path, player_id=1001),
        )
        resp1 = protocol.parse("paymentpb.CreatePayIntentResponse", raw)
        assert resp1.error_message.id == 0
        assert resp1.server_now_ms == BASE_NOW_MS
        assert resp1.intent.order_id == ORDER_1
        assert resp1.intent.state == INTENT_CREATED
        assert resp1.intent.amount_minor == 600
        assert resp1.devpay.out_trade_no == out_trade_no(ORDER_1)

        # 幂等重复建意图
        raw2 = await client.call(
            create_path,
            create_req.SerializeToString(),
            metadata=_signed_metadata(protocol, create_path, player_id=1001),
        )
        resp2 = protocol.parse("paymentpb.CreatePayIntentResponse", raw2)
        assert resp2.error_message.id == 0
        assert resp2.devpay.out_trade_no == resp1.devpay.out_trade_no

        # 同一订单换渠道 -> 31004 且返回已有意图、不返回 SDK 参数
        mismatch_req = protocol.message(
            "paymentpb.CreatePayIntentRequest",
            order_id=ORDER_1,
            channel=PAY_CHANNEL_APPLE,
            platform=PLATFORM_IOS_APP,
        )
        raw_mm = await client.call(
            create_path,
            mismatch_req.SerializeToString(),
            metadata=_signed_metadata(protocol, create_path, player_id=1001),
        )
        resp_mm = protocol.parse("paymentpb.CreatePayIntentResponse", raw_mm)
        assert resp_mm.error_message.id == ERR_CHANNEL_MISMATCH
        assert resp_mm.intent.channel == PAY_CHANNEL_DEVPAY
        assert not resp_mm.apple.product_id
        assert resp_mm.server_now_ms == BASE_NOW_MS

        # 他人订单 -> 31001（不泄露存在性）
        other_req = protocol.message(
            "paymentpb.CreatePayIntentRequest",
            order_id=ORDER_2,
            channel=PAY_CHANNEL_DEVPAY,
            platform=PLATFORM_ANDROID_APP,
        )
        raw_other = await client.call(
            create_path,
            other_req.SerializeToString(),
            metadata=_signed_metadata(protocol, create_path, player_id=1001),
        )
        assert protocol.parse("paymentpb.CreatePayIntentResponse", raw_other).error_message.id == ERR_ORDER_NOT_FOUND

        # 已建意图被他人查询 -> 31001
        get_path = "/paymentpb.ClientPlayerPayment/GetPayIntent"
        get_req = protocol.message("paymentpb.GetPayIntentRequest", order_id=ORDER_1)
        raw_other_get = await client.call(
            get_path,
            get_req.SerializeToString(),
            metadata=_signed_metadata(protocol, get_path, player_id=2002),
        )
        assert protocol.parse("paymentpb.GetPayIntentResponse", raw_other_get).error_message.id == ERR_ORDER_NOT_FOUND

        # 剩余支付窗口不足 min_pay_window (60s) -> 31003
        exp_req = protocol.message(
            "paymentpb.CreatePayIntentRequest",
            order_id=ORDER_3,
            channel=PAY_CHANNEL_DEVPAY,
            platform=PLATFORM_ANDROID_APP,
        )
        raw_exp = await client.call(
            create_path,
            exp_req.SerializeToString(),
            metadata=_signed_metadata(protocol, create_path, player_id=1001),
        )
        assert protocol.parse("paymentpb.CreatePayIntentResponse", raw_exp).error_message.id == ERR_ORDER_EXPIRED

        # --- Webhook POST /notify/devpay ---
        notify_url = f"http://127.0.0.1:{notify_port}/notify/devpay"
        body = json.dumps(
            {"out_trade_no": out_trade_no(ORDER_1), "amount_minor": 600, "currency": "CNY"}
        ).encode("utf-8")
        good_sig = devpay_sign(DEVPAY_SECRET, body).hex()

        async with aiohttp.ClientSession() as http:
            # 1) 错误 HMAC -> 401
            async with http.post(notify_url, data=body, headers={DEVPAY_SIGNATURE_HEADER: "00" * 32}) as r:
                assert r.status == 401

            # 2) 严格 JSON 含未知字段 -> 401
            bad_json = json.dumps(
                {"out_trade_no": out_trade_no(ORDER_1), "amount_minor": 600, "currency": "CNY", "extra": 1}
            ).encode("utf-8")
            async with http.post(
                notify_url,
                data=bad_json,
                headers={DEVPAY_SIGNATURE_HEADER: devpay_sign(DEVPAY_SECRET, bad_json).hex()},
            ) as r:
                assert r.status == 401

            # 3) 超出 256 KiB -> 401
            oversized = b"{" + b" " * (MAX_BODY_BYTES + 10) + b"}"
            async with http.post(
                notify_url,
                data=oversized,
                headers={DEVPAY_SIGNATURE_HEADER: devpay_sign(DEVPAY_SECRET, oversized).hex()},
            ) as r:
                assert r.status == 401

            # 4) 合法回调 -> 200 "ok" + 事实先落库 + kick_event 置位
            assert not core.kick_event.is_set()
            async with http.post(notify_url, data=body, headers={DEVPAY_SIGNATURE_HEADER: good_sig}) as r:
                assert r.status == 200
                assert await r.text() == "ok"
            assert core.kick_event.is_set()

            # 5) 重复回调幂等 -> 200 "ok"
            async with http.post(notify_url, data=body, headers={DEVPAY_SIGNATURE_HEADER: good_sig}) as r:
                assert r.status == 200
                assert await r.text() == "ok"

            # 6) 同一交易号不同金额 -> 事实冲突：不覆写已落库金额，仍返回 200（避免无限重投冲突包）
            conflict_body = json.dumps(
                {"out_trade_no": out_trade_no(ORDER_1), "amount_minor": 999, "currency": "CNY"}
            ).encode("utf-8")
            async with http.post(
                notify_url,
                data=conflict_body,
                headers={DEVPAY_SIGNATURE_HEADER: devpay_sign(DEVPAY_SECRET, conflict_body).hex()},
            ) as r:
                assert r.status == 200

            tx_hi, tx_lo = txn_key("devpay-" + out_trade_no(ORDER_1))
            stored_txn = await store.txn(PAY_CHANNEL_DEVPAY, tx_hi, tx_lo)
            assert stored_txn.paid_amount_minor == 600
            assert stored_txn.mall_state == MALL_PENDING
            assert stored_txn.mall_due == 1

            # --- GetPayIntent 应已变为 PAID ---
            raw_get = await client.call(
                get_path,
                get_req.SerializeToString(),
                metadata=_signed_metadata(protocol, get_path, player_id=1001),
            )
            get_resp = protocol.parse("paymentpb.GetPayIntentResponse", raw_get)
            assert get_resp.error_message.id == 0
            assert get_resp.intent.state == INTENT_PAID
            assert get_resp.server_now_ms == BASE_NOW_MS

            # --- 退款通知先于付款通知到达 -> 写 ORPHAN 墓碑行 (fact_revision=1)，拦截晚到付款 ---
            tomb_trade_no = out_trade_no(ORDER_2)
            revoke_body = json.dumps({"out_trade_no": tomb_trade_no, "revoke": True}).encode("utf-8")
            async with http.post(
                notify_url,
                data=revoke_body,
                headers={DEVPAY_SIGNATURE_HEADER: devpay_sign(DEVPAY_SECRET, revoke_body).hex()},
            ) as r:
                assert r.status == 200

            t_hi, t_lo = txn_key("devpay-" + tomb_trade_no)
            tomb_txn = await store.txn(PAY_CHANNEL_DEVPAY, t_hi, t_lo)
            assert tomb_txn.mall_state == MALL_ORPHAN
            assert tomb_txn.mall_due == 0
            assert tomb_txn.fact_revision == 1
            assert tomb_txn.mall_last_error == REVOCATION_TOMBSTONE_REASON

            # 晚到的付款通知被吞掉，不报冲突也不投商城
            late_pay_body = json.dumps(
                {"out_trade_no": tomb_trade_no, "amount_minor": 900, "currency": "CNY"}
            ).encode("utf-8")
            async with http.post(
                notify_url,
                data=late_pay_body,
                headers={DEVPAY_SIGNATURE_HEADER: devpay_sign(DEVPAY_SECRET, late_pay_body).hex()},
            ) as r:
                assert r.status == 200
            tomb_after = await store.txn(PAY_CHANNEL_DEVPAY, t_hi, t_lo)
            assert tomb_after.mall_state == MALL_ORPHAN
            assert tomb_after.mall_due == 0
            assert tomb_after.fact_revision == 1
    finally:
        await client.close()
        await notify_server.close()
        await grpc_server.stop(0)
        await mall_client.close()
        await mall_server.close()


# ---------------------------------------------------------------------------
# 3. 后台工作器：push_mall / process_refunds（含自发退款不翻 fact_revision）/ sweep_intents / finalize
# ---------------------------------------------------------------------------


@_async_test
async def test_workers_push_mall_revision_refunds_and_sweep() -> None:
    clock = ManualClock(BASE_NOW_MS)
    mall_server = FakeMallHttpServer()
    await mall_server.start()
    mall_server.put_order(ORDER_1, buyer_id=1001, amount_minor=600, currency="CNY", deadline_ms=BASE_NOW_MS + 300_000)
    mall_server.put_order(ORDER_2, buyer_id=1001, amount_minor=800, currency="CNY", deadline_ms=BASE_NOW_MS + 300_000)
    mall_server.put_order(ORDER_3, buyer_id=1001, amount_minor=400, currency="CNY", deadline_ms=BASE_NOW_MS + 120_000)

    store = MemoryPaymentStore()
    mall_client = HttpMallClient(mall_server.base_url, "payment-adapter", MALL_SECRET, clock_ms=clock)

    # 模拟渠道退款途中收到己方退款回调：因 ClaimRefund 已原子置 REQUESTED + out_refund_no，绝不能翻转 fact_revision
    self_revoke_fired = False
    core_holder: list[PaymentCore] = []

    class HookedDevPay(DevPayChannel):
        async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult:
            nonlocal self_revoke_fired
            # 确认调用 channel.refund 之前库中已原子置 REFUND_REQUESTED + out_refund_no
            tx_hi, tx_lo = txn_key(req.transaction_id)
            in_flight = await store.txn(PAY_CHANNEL_DEVPAY, tx_hi, tx_lo)
            assert in_flight.refund_state == REFUND_REQUESTED
            assert in_flight.out_refund_no == out_refund_no(CHANNEL_DEVPAY, req.transaction_id)
            # 模拟并发到达的己方退款回调
            await core_holder[0].revoke(
                self,
                ChannelRevocation(
                    transaction_id=req.transaction_id,
                    revoked=True,
                    observed_ms=clock() + 10,
                    out_refund_no=req.out_refund_no,
                    detail="self_refund_callback",
                ),
            )
            self_revoke_fired = True
            return await super().refund(req)

    devpay = HookedDevPay(DEVPAY_SECRET, clock_ms=clock)
    core = PaymentCore(
        store,
        mall_client,
        [devpay],
        {PLATFORM_DESKTOP: [PAY_CHANNEL_DEVPAY]},
        clock_ms=clock,
    )
    core_holder.append(core)
    workers = PaymentWorkers(core)

    try:
        # --- (A) push_mall 首次记账 -> BOUND，再渠道撤销 -> push_revision(REVOKED, rev=1) ---
        from xuanming.payment import CreateIntentCommand

        await core.create_intent(
            CreateIntentCommand(player_id=1001, order_id=ORDER_1, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
        )
        tx1_id = "devpay-" + out_trade_no(ORDER_1)
        await core.record_payment(
            devpay,
            ChannelPayment(
                transaction_id=tx1_id,
                out_trade_no=out_trade_no(ORDER_1),
                amount_minor=600,
                currency="CNY",
                paid_ms=clock(),
            ),
        )
        pushed = await workers.push_mall_once()
        assert pushed == 1
        assert len(mall_server.recorded_payments) == 1
        assert mall_server.recorded_payments[0]["orderId"] == ORDER_1

        hi1, lo1 = txn_key(tx1_id)
        t1 = await store.txn(PAY_CHANNEL_DEVPAY, hi1, lo1)
        assert t1.mall_state == MALL_BOUND
        assert t1.mall_due == 0
        assert t1.mall_revision == 0

        # 渠道侧撤销（非自发退款）
        clock.advance(5_000)
        await core.revoke(
            devpay,
            ChannelRevocation(transaction_id=tx1_id, revoked=True, observed_ms=clock(), detail="chargeback"),
        )
        t1_rev = await store.txn(PAY_CHANNEL_DEVPAY, hi1, lo1)
        assert t1_rev.fact_revision == 1
        assert t1_rev.mall_due == 1
        i_hi1, i_lo1 = order_key(ORDER_1)
        assert (await store.intent(i_hi1, i_lo1)).state == INTENT_CLOSED

        pushed_rev = await workers.push_mall_once()
        assert pushed_rev == 1
        assert len(mall_server.revisions) == 1
        assert mall_server.revisions[0]["revision"] == 1
        assert mall_server.revisions[0]["fact"] == FACT_REVOKED
        t1_done = await store.txn(PAY_CHANNEL_DEVPAY, hi1, lo1)
        assert t1_done.mall_revision == 1
        assert t1_done.mall_due == 0

        # --- (B) process_refunds：分页拉取、ClaimRefund 防自发退款翻转、回执商城 ---
        await core.create_intent(
            CreateIntentCommand(player_id=1001, order_id=ORDER_2, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
        )
        tx2_id = "devpay-" + out_trade_no(ORDER_2)
        await core.record_payment(
            devpay,
            ChannelPayment(
                transaction_id=tx2_id,
                out_trade_no=out_trade_no(ORDER_2),
                amount_minor=800,
                currency="CNY",
                paid_ms=clock(),
            ),
        )
        mall_server.payment_refund_reason = "CANCELLED_LATE"
        await workers.push_mall_once()
        mall_server.payment_refund_reason = None

        mall_server.pending_refunds_pages = [
            {
                "entries": [
                    {
                        "orderId": ORDER_2,
                        "channel": CHANNEL_DEVPAY,
                        "transactionId": tx2_id,
                        "amountMinor": 800,
                        "currency": "CNY",
                        "reason": "CANCELLED_LATE",
                    }
                ],
                "nextCursor": "1",
            },
            {
                "entries": [
                    {
                        "orderId": ORDER_3,
                        "channel": CHANNEL_DEVPAY,
                        "transactionId": "tx-missing-in-store",
                        "amountMinor": 100,
                        "currency": "CNY",
                        "reason": "DUPLICATE_PAYMENT",
                    }
                ],
                "nextCursor": "",
            },
        ]
        refunded = await workers.process_refunds_once()
        assert refunded == 2
        assert self_revoke_fired
        hi2, lo2 = txn_key(tx2_id)
        t2 = await store.txn(PAY_CHANNEL_DEVPAY, hi2, lo2)
        assert t2.fact_revision == 0  # 自发退款绝不翻转 fact_revision
        assert t2.refund_state == REFUND_REPORTED
        assert t2.out_refund_no == out_refund_no(CHANNEL_DEVPAY, tx2_id)
        assert len(mall_server.refund_receipts) == 1
        assert mall_server.refund_receipts[0]["paymentTransactionId"] == tx2_id
        # 找不到记录的待办通过 /internal/refunds/close 转人工
        assert len(mall_server.refund_closures) == 1
        assert mall_server.refund_closures[0]["reason"] == "TXN_NOT_FOUND"
        # CANCELLED_LATE 退款成功后关闭意图
        i_hi2, i_lo2 = order_key(ORDER_2)
        assert (await store.intent(i_hi2, i_lo2)).state == INTENT_CLOSED

        # --- (C) sweep_intents：到期且过宽限期后关闭未付意图 ---
        await core.create_intent(
            CreateIntentCommand(player_id=1001, order_id=ORDER_3, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
        )
        i_hi3, i_lo3 = order_key(ORDER_3)
        intent3 = await store.intent(i_hi3, i_lo3)
        assert intent3.state == INTENT_CREATED

        # 推进时钟越过 expire_ms + close_grace_ms
        clock.ms = intent3.expire_ms + core.options.close_grace_ms + 1000
        swept = await workers.sweep_intents_once()
        assert swept == 1
        assert (await store.intent(i_hi3, i_lo3)).state == INTENT_CLOSED
    finally:
        await mall_client.close()
        await mall_server.close()


# ---------------------------------------------------------------------------
# 4. SubmitPurchase 语义（Google vs Apple）+ 乱序撤销保护 + Finalizer + GET 握手
# ---------------------------------------------------------------------------


@_async_test
async def test_submit_purchase_google_and_apple_finish_semantics_and_handshake() -> None:
    protocol = Protocol()
    clock = ManualClock(BASE_NOW_MS)
    mall_server = FakeMallHttpServer()
    await mall_server.start()
    mall_server.put_order(ORDER_1, buyer_id=1001, sku_id=10, amount_minor=600, currency="CNY")
    mall_server.put_order(ORDER_2, buyer_id=1001, sku_id=10, amount_minor=600, currency="CNY")

    google_result: list[ChannelQueryResult] = []
    apple_result: list[ChannelQueryResult] = []
    finalized_tokens: list[tuple[str, str]] = []

    async def g_verify(_receipt: ChannelReceipt) -> ChannelQueryResult:
        return google_result.pop(0)

    async def a_verify(_receipt: ChannelReceipt) -> ChannelQueryResult:
        return apple_result.pop(0)

    async def g_finalize(product_id: str, token: str) -> None:
        finalized_tokens.append((product_id, token))

    google_ch = StubStoreChannel(PAY_CHANNEL_GOOGLE, CHANNEL_GOOGLE, verify_fn=g_verify, finalize_fn=g_finalize)
    apple_ch = StubStoreChannel(PAY_CHANNEL_APPLE, CHANNEL_APPLE, verify_fn=a_verify)
    wx_ch = StubMinigameHandshakeChannel("wx-push-token-123")

    store = MemoryPaymentStore()
    mall_client = HttpMallClient(mall_server.base_url, "payment-adapter", MALL_SECRET, clock_ms=clock)
    core = PaymentCore(
        store,
        mall_client,
        [google_ch, apple_ch, wx_ch],
        {
            PLATFORM_ANDROID_APP: [PAY_CHANNEL_GOOGLE],
            PLATFORM_IOS_APP: [PAY_CHANNEL_APPLE],
            PLATFORM_WECHAT_MINIGAME: [PAY_CHANNEL_WECHAT_MINIGAME],
        },
        clock_ms=clock,
    )
    verifier = CallerVerifier([GATE_PUBLIC_KEY], allowed_callers=("gate",))
    grpc_svc = PaymentGrpcService(protocol, core, caller_verifier=verifier)
    grpc_server, grpc_port = await serve_grpc(protocol, "127.0.0.1:0", grpc_svc.services(), pass_context=True)
    notify_server = PaymentNotifyServer(core, host="127.0.0.1", port=0)
    notify_port = await notify_server.start()
    client = GrpcClient(f"127.0.0.1:{grpc_port}")
    workers = PaymentWorkers(core)

    try:
        # 预建 ORDER_1 (Google) 与 ORDER_2 (Apple) 意图
        create_path = "/paymentpb.ClientPlayerPayment/CreatePayIntent"
        for oid, ch_id, plat in (
            (ORDER_1, PAY_CHANNEL_GOOGLE, PLATFORM_ANDROID_APP),
            (ORDER_2, PAY_CHANNEL_APPLE, PLATFORM_IOS_APP),
        ):
            req = protocol.message("paymentpb.CreatePayIntentRequest", order_id=oid, channel=ch_id, platform=plat)
            raw = await client.call(create_path, req.SerializeToString(), metadata=_signed_metadata(protocol, create_path))
            assert protocol.parse("paymentpb.CreatePayIntentResponse", raw).error_message.id == 0

        submit_path = "/paymentpb.ClientPlayerPayment/SubmitPurchase"

        # 1) TradeNotPaid -> finish_transaction=False
        google_result.append(ChannelQueryResult(state=TradeState.NOT_PAID))
        req = protocol.message(
            "paymentpb.SubmitPurchaseRequest",
            channel=PAY_CHANNEL_GOOGLE,
            order_id=ORDER_1,
            google_purchase_token="tok-pending",
        )
        resp = protocol.parse(
            "paymentpb.SubmitPurchaseResponse",
            await client.call(submit_path, req.SerializeToString(), metadata=_signed_metadata(protocol, submit_path)),
        )
        assert resp.error_message.id == 0
        assert resp.finish_transaction is False

        # 2) Google Orphan (anomaly) -> finish_transaction=False
        google_result.append(
            ChannelQueryResult(
                state=TradeState.PAID,
                payment=ChannelPayment(
                    transaction_id="g-orphan",
                    order_id=ORDER_1,
                    paid_ms=clock(),
                    store_priced=True,
                    anomaly="quantity mismatch",
                ),
            )
        )
        resp = protocol.parse(
            "paymentpb.SubmitPurchaseResponse",
            await client.call(submit_path, req.SerializeToString(), metadata=_signed_metadata(protocol, submit_path)),
        )
        assert resp.error_message.id == 0
        assert resp.finish_transaction is False

        # 3) Google 正常付费 (MALL_PENDING + needs_finalize) -> finish_transaction=True + finalize_once 执行 consume
        google_result.append(
            ChannelQueryResult(
                state=TradeState.PAID,
                payment=ChannelPayment(
                    transaction_id="g-ok",
                    order_id=ORDER_1,
                    paid_ms=clock(),
                    store_priced=True,
                    product_id="sku_10",
                    channel_token="tok-ok",
                    needs_finalize=True,
                ),
            )
        )
        resp = protocol.parse(
            "paymentpb.SubmitPurchaseResponse",
            await client.call(submit_path, req.SerializeToString(), metadata=_signed_metadata(protocol, submit_path)),
        )
        assert resp.error_message.id == 0
        assert resp.finish_transaction is True
        assert resp.intent.state == INTENT_PAID

        await workers.finalize_once()
        assert finalized_tokens == [("sku_10", "tok-ok")]
        g_hi, g_lo = txn_key("g-ok")
        assert (await store.txn(PAY_CHANNEL_GOOGLE, g_hi, g_lo)).finalize_state == FINALIZE_DONE

        # 4) Google 事实冲突 -> finish_transaction=False
        google_result.append(
            ChannelQueryResult(
                state=TradeState.PAID,
                payment=ChannelPayment(
                    transaction_id="g-ok",
                    order_id=ORDER_2,  # 与已落库的 ORDER_1 冲突
                    paid_ms=clock(),
                    store_priced=True,
                ),
            )
        )
        resp = protocol.parse(
            "paymentpb.SubmitPurchaseResponse",
            await client.call(submit_path, req.SerializeToString(), metadata=_signed_metadata(protocol, submit_path)),
        )
        assert resp.error_message.id == 0
        assert resp.finish_transaction is False

        # 5) Apple Orphan 与冲突均返回 finish_transaction=True
        apple_result.append(
            ChannelQueryResult(
                state=TradeState.PAID,
                payment=ChannelPayment(
                    transaction_id="a-orphan",
                    order_id=ORDER_2,
                    paid_ms=clock(),
                    store_priced=True,
                    anomaly="sandbox rejected",
                ),
            )
        )
        req_a = protocol.message(
            "paymentpb.SubmitPurchaseRequest",
            channel=PAY_CHANNEL_APPLE,
            order_id=ORDER_2,
            apple_transaction_id="a-orphan",
        )
        resp_a = protocol.parse(
            "paymentpb.SubmitPurchaseResponse",
            await client.call(submit_path, req_a.SerializeToString(), metadata=_signed_metadata(protocol, submit_path)),
        )
        assert resp_a.error_message.id == 0
        assert resp_a.finish_transaction is True

        # Apple 冲突同样 finish_transaction=True
        apple_result.append(
            ChannelQueryResult(
                state=TradeState.PAID,
                payment=ChannelPayment(
                    transaction_id="a-orphan",
                    order_id=ORDER_1,  # 冲突订单
                    paid_ms=clock(),
                    store_priced=True,
                ),
            )
        )
        resp_a2 = protocol.parse(
            "paymentpb.SubmitPurchaseResponse",
            await client.call(submit_path, req_a.SerializeToString(), metadata=_signed_metadata(protocol, submit_path)),
        )
        assert resp_a2.error_message.id == 0
        assert resp_a2.finish_transaction is True

        # 6) Apple 乱序保护：T2 到达 REFUND_REVERSED 后，旧的 T1 REFUND 不得翻转事实
        a_hi, a_lo = txn_key("apple-ooo")
        o2_hi, o2_lo = order_key(ORDER_2)
        await store.insert_txn(
            PaymentTxnRecord(
                channel=PAY_CHANNEL_APPLE,
                txn_hi=a_hi,
                txn_lo=a_lo,
                transaction_id="apple-ooo",
                order_hi=o2_hi,
                order_lo=o2_lo,
                mall_state=MALL_BOUND,
                fact_revision=0,
                fact_observed_ms=BASE_NOW_MS,
            )
        )
        await core.revoke(
            apple_ch,
            ChannelRevocation(
                transaction_id="apple-ooo",
                revoked=False,
                observed_ms=BASE_NOW_MS + 20_000,
                detail="REFUND_REVERSED",
            ),
        )
        await core.revoke(
            apple_ch,
            ChannelRevocation(
                transaction_id="apple-ooo",
                revoked=True,
                observed_ms=BASE_NOW_MS + 10_000,
                detail="REFUND",
            ),
        )
        ooo_txn = await store.txn(PAY_CHANNEL_APPLE, a_hi, a_lo)
        assert ooo_txn.fact_revision == 0
        assert ooo_txn.fact_observed_ms == BASE_NOW_MS + 20_000

        # 7) 小游戏 GET /notify/wechat_minigame 握手测试
        async with aiohttp.ClientSession() as http:
            sig = sorted_sha1("wx-push-token-123", "1700000000", "nonce123")
            async with http.get(
                f"http://127.0.0.1:{notify_port}/notify/wechat_minigame",
                params={"timestamp": "1700000000", "nonce": "nonce123", "signature": sig, "echostr": "hello-wx"},
            ) as r:
                assert r.status == 200
                assert await r.text() == "hello-wx"
            # 未知渠道 -> 404
            async with http.post(f"http://127.0.0.1:{notify_port}/notify/unknown", data=b"{}") as r:
                assert r.status == 404
    finally:
        await client.close()
        await notify_server.close()
        await grpc_server.stop(0)
        await mall_client.close()
        await mall_server.close()


# ---------------------------------------------------------------------------
# 5. 未成年人合规限额测试
# ---------------------------------------------------------------------------


@_async_test
async def test_minor_spending_limits() -> None:
    # BASE_NOW_MS 对应 2023-11-14（北京时间 2023-11-15）
    clock = ManualClock(BASE_NOW_MS)
    mall_server = FakeMallHttpServer()
    await mall_server.start()
    mall_server.put_order(ORDER_1, buyer_id=3001, amount_minor=60_00, currency="CNY")  # 60 元 > 10岁单次50元
    mall_server.put_order(ORDER_2, buyer_id=3002, amount_minor=10_00, currency="CNY")  # 7岁禁充
    mall_server.put_order(ORDER_3, buyer_id=3001, amount_minor=40_00, currency="CNY")  # 40 元 <= 50 元

    class FakeRealNames:
        def __init__(self) -> None:
            self.data = {
                3001: (pi_birth_prefix(2013, 1, 1) + "a" * 32, True),  # 10 岁
                3002: (pi_birth_prefix(2016, 1, 1) + "b" * 32, True),  # 7 岁
            }

        async def real_name_of(self, player_id: int) -> tuple[str, bool]:
            return self.data.get(player_id, ("", False))

    store = MemoryPaymentStore()
    mall_client = HttpMallClient(mall_server.base_url, "payment-adapter", MALL_SECRET, clock_ms=clock)
    devpay = DevPayChannel(DEVPAY_SECRET, clock_ms=clock)
    core = PaymentCore(
        store,
        mall_client,
        [devpay],
        {PLATFORM_DESKTOP: [PAY_CHANNEL_DEVPAY]},
        options=PaymentOptions(real_names=FakeRealNames()),
        clock_ms=clock,
    )

    from xuanming.payment import CreateIntentCommand, PaymentError

    try:
        # 10 岁单次超 50 元 -> 31010
        with pytest.raises(PaymentError) as e_single:
            await core.create_intent(
                CreateIntentCommand(player_id=3001, order_id=ORDER_1, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
            )
        assert e_single.value.tip_id == ERR_MINOR_SINGLE_LIMIT

        # 7 岁禁止充值 -> 31009
        with pytest.raises(PaymentError) as e_under8:
            await core.create_intent(
                CreateIntentCommand(player_id=3002, order_id=ORDER_2, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
            )
        assert e_under8.value.tip_id == ERR_MINOR_NOT_ALLOWED

        # 10 岁 40 元成功预占，再买第二单 20 元超当日 50 元 -> 31011
        await core.create_intent(
            CreateIntentCommand(player_id=3001, order_id=ORDER_3, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
        )
        order_4 = "33334444-5555-6666-7777-888899990000"
        mall_server.put_order(order_4, buyer_id=3001, amount_minor=20_00, currency="CNY")
        with pytest.raises(PaymentError) as e_daily:
            await core.create_intent(
                CreateIntentCommand(player_id=3001, order_id=order_4, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
            )
        assert e_daily.value.tip_id == ERR_MINOR_DAILY_LIMIT
    finally:
        await mall_client.close()
        await mall_server.close()


# ---------------------------------------------------------------------------
# 6. SqlPaymentStore DDL、bootstrap / verify_schema 与 READ COMMITTED SQL 验证
# ---------------------------------------------------------------------------


@_async_test
async def test_sql_payment_store_ddl_verify_and_queries() -> None:
    ddl = SCHEMA_FILE.read_text("utf-8")
    for table in ("payment_intent", "payment_spend", "payment_txn", "payment_channel_cursor"):
        assert f"CREATE TABLE IF NOT EXISTS `{table}`" in ddl
    for idx in (
        "idx_payment_intent_0",
        "idx_payment_intent_1",
        "idx_payment_txn_0",
        "idx_payment_txn_1",
    ):
        assert f"INDEX `{idx}`" in ddl

    executed: list[tuple[str, tuple]] = []
    begins = 0
    commits = 0
    rollbacks = 0

    class FakeCursor:
        def __init__(self) -> None:
            self.rowcount = 1
            self._rows: list[tuple] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, sql: str, args=None) -> None:
            executed.append((sql, tuple(args or ())))
            if "information_schema.statistics" in sql:
                table = args[0]
                if table == "payment_intent":
                    self._rows = [
                        ("idx_payment_intent_0", "state"),
                        ("idx_payment_intent_0", "next_check_ms"),
                        ("idx_payment_intent_1", "spender_hi"),
                        ("idx_payment_intent_1", "spender_lo"),
                        ("idx_payment_intent_1", "state"),
                        ("idx_payment_intent_1", "paid_ms"),
                    ]
                elif table == "payment_txn":
                    self._rows = [
                        ("idx_payment_txn_0", "mall_due"),
                        ("idx_payment_txn_0", "next_attempt_ms"),
                        ("idx_payment_txn_1", "finalize_state"),
                        ("idx_payment_txn_1", "finalize_next_ms"),
                    ]
            else:
                self._rows = []

        async def fetchall(self):
            return list(self._rows)

        async def fetchone(self):
            return self._rows[0] if self._rows else None

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        async def begin(self):
            nonlocal begins
            begins += 1

        async def commit(self):
            nonlocal commits
            commits += 1

        async def rollback(self):
            nonlocal rollbacks
            rollbacks += 1

        def close(self):
            pass

    class FakePool:
        @asynccontextmanager
        async def acquire(self):
            yield FakeConn()

    sql_store = SqlPaymentStore(FakePool())
    await sql_store.bootstrap()
    create_stmts = [sql for sql, _ in executed if sql.startswith("CREATE TABLE")]
    assert len(create_stmts) == 4

    executed.clear()
    await sql_store.verify_schema()
    assert len([sql for sql, _ in executed if "LIMIT 0" in sql]) == 4
    assert len([sql for sql, _ in executed if "information_schema.statistics" in sql]) == 4

    # 验证 reserve_intent 在 READ COMMITTED 显式事务下锁 payment_spend 并 FOR UPDATE 校验额度
    executed.clear()
    hi, lo = order_key(ORDER_1)
    rec = PaymentIntentRecord(
        order_hi=hi,
        order_lo=lo,
        order_id=ORDER_1,
        player_id=1001,
        channel=PAY_CHANNEL_DEVPAY,
        amount_minor=1000,
        currency="CNY",
        state=INTENT_CREATED,
        spender_hi=11,
        spender_lo=22,
        created_ms=BASE_NOW_MS,
        updated_ms=BASE_NOW_MS,
    )
    stored, inserted = await sql_store.reserve_intent(
        rec,
        SpendLimit(monthly_fen=200_00, daily_fen=50_00, month_start_ms=BASE_NOW_MS - 1000, day_start_ms=BASE_NOW_MS - 1000),
    )
    assert inserted is True
    assert stored.order_id == ORDER_1
    assert begins == 1 and commits == 1 and rollbacks == 0
    assert any("SET TRANSACTION ISOLATION LEVEL READ COMMITTED" in sql for sql, _ in executed)
    assert any("INSERT INTO `payment_spend`" in sql for sql, _ in executed)
    assert any("FOR UPDATE" in sql for sql, _ in executed)
    assert any("INSERT INTO `payment_intent`" in sql for sql, _ in executed)

    # 验证 ClaimRefund 在调用渠道退款前原子置 REFUND_REQUESTED + out_refund_no
    executed.clear()
    tx = PaymentTxnRecord(channel=PAY_CHANNEL_DEVPAY, txn_hi=1, txn_lo=2, transaction_id="tx-1", refund_next_ms=0)
    ok = await sql_store.claim_refund(tx, out_refund_no(CHANNEL_DEVPAY, "tx-1"), BASE_NOW_MS + 30_000, BASE_NOW_MS)
    assert ok is True
    assert len(executed) == 1
    claim_sql, claim_args = executed[0]
    assert "`refund_state`=CASE WHEN `refund_state`=%s THEN %s ELSE `refund_state` END" in claim_sql
    assert "`out_refund_no`=CASE WHEN `out_refund_no`='' THEN %s ELSE `out_refund_no` END" in claim_sql
    assert claim_args[1] == REFUND_REQUESTED
    assert claim_args[2] == out_refund_no(CHANNEL_DEVPAY, "tx-1")


@_async_test
async def test_mall_client_status_mapping_and_fresh_nonces() -> None:
    mall_server = FakeMallHttpServer()
    await mall_server.start()
    mall_client = HttpMallClient(mall_server.base_url, "payment-adapter", MALL_SECRET)
    try:
        # 404 -> MallNotFoundError
        with pytest.raises(MallNotFoundError):
            await mall_client.order_view(ORDER_1)

        # 400 -> MallInvalidError, 409 -> MallConflictError, 503 -> MallStatusError
        pay = MallPayment(ORDER_1, CHANNEL_DEVPAY, "tx-err", 100, "CNY")
        for status, exc_cls in ((400, MallInvalidError), (409, MallConflictError), (503, MallStatusError)):
            mall_server.payment_status_override = status
            with pytest.raises(exc_cls):
                await mall_client.record_payment(pay)
        # 每次调用必须换全新 16 字节十六进制 nonce
        assert len(mall_server.seen_nonces) == 4
        assert all(len(n) == 32 for n in mall_server.seen_nonces)
    finally:
        await mall_client.close()
        await mall_server.close()


@_async_test
async def test_mall_push_409_rejected_and_503_retry_and_error_tips() -> None:
    clock = ManualClock(BASE_NOW_MS)
    mall_server = FakeMallHttpServer()
    await mall_server.start()
    mall_server.put_order(ORDER_1, buyer_id=1001, amount_minor=600, currency="CNY", state="CANCELLED")
    mall_server.put_order(ORDER_2, buyer_id=1001, amount_minor=600, currency="USD")
    mall_server.put_order(ORDER_3, buyer_id=1001, amount_minor=600, currency="CNY")

    class CnyOnlyDevPay(DevPayChannel):
        def supports(self, currency: str) -> bool:
            return currency == "CNY"

    store = MemoryPaymentStore()
    mall_client = HttpMallClient(mall_server.base_url, "payment-adapter", MALL_SECRET, clock_ms=clock)
    devpay = CnyOnlyDevPay(DEVPAY_SECRET, clock_ms=clock)
    core = PaymentCore(store, mall_client, [devpay], {PLATFORM_DESKTOP: [PAY_CHANNEL_DEVPAY]}, clock_ms=clock)
    workers = PaymentWorkers(core)

    from xuanming.payment import CreateIntentCommand, PaymentError

    try:
        with pytest.raises(PaymentError) as e_state:
            await core.create_intent(
                CreateIntentCommand(player_id=1001, order_id=ORDER_1, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
            )
        assert e_state.value.tip_id == ERR_ORDER_NOT_PAYABLE

        with pytest.raises(PaymentError) as e_cur:
            await core.create_intent(
                CreateIntentCommand(player_id=1001, order_id=ORDER_2, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
            )
        assert e_cur.value.tip_id == ERR_CURRENCY_UNSUPPORTED

        await core.create_intent(
            CreateIntentCommand(player_id=1001, order_id=ORDER_3, channel=PAY_CHANNEL_DEVPAY, platform=PLATFORM_DESKTOP)
        )
        tx_id = "devpay-" + out_trade_no(ORDER_3)
        await core.record_payment(
            devpay,
            ChannelPayment(
                transaction_id=tx_id,
                out_trade_no=out_trade_no(ORDER_3),
                amount_minor=600,
                currency="CNY",
                paid_ms=clock(),
            ),
        )
        hi, lo = txn_key(tx_id)

        # 503 -> 退避重试，mall_due 保持 1
        mall_server.payment_status_override = 503
        await workers.push_mall_once()
        t_retry = await store.txn(PAY_CHANNEL_DEVPAY, hi, lo)
        assert t_retry.mall_state == MALL_PENDING
        assert t_retry.mall_due == 1
        assert t_retry.mall_last_error == "mall_http_503"
        assert t_retry.next_attempt_ms > clock()

        # 推进时钟后 409 -> 终局 MALL_REJECTED，mall_due=0
        clock.ms = t_retry.next_attempt_ms + 1000
        mall_server.payment_status_override = 409
        await workers.push_mall_once()
        t_rej = await store.txn(PAY_CHANNEL_DEVPAY, hi, lo)
        assert t_rej.mall_state == MALL_REJECTED
        assert t_rej.mall_due == 0
        assert t_rej.mall_last_error == "mall_conflict"
    finally:
        await mall_client.close()
        await mall_server.close()
