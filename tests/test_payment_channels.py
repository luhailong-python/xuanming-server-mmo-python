"""6 个生产支付渠道适配器（微信支付 / 支付宝 / 微信小游戏 / 抖音小游戏 / Apple / Google）
与实名未成年人限额闸（GrpcRealNameClient）全链路密码学与协议测试。"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import replace
import datetime as dt
import functools
import json
import re
from urllib.parse import parse_qs, urlencode, urlsplit

import aiohttp
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.x509.oid import NameOID

from xuanming.grpc_runtime import GrpcClient, serve_grpc, verify_service_call
from xuanming.payment import (
    APPLE_INTERMEDIATE_OID,
    APPLE_LEAF_OID,
    AckKind,
    AlipayChannel,
    AlipayConfig,
    AppleChannel,
    AppleConfig,
    BadSignatureError,
    CHANNEL_ALIPAY,
    CHANNEL_APPLE,
    CHANNEL_DEVPAY,
    CHANNEL_DOUYIN_MINIGAME,
    CHANNEL_GOOGLE,
    CHANNEL_WECHAT_MINIGAME,
    CHANNEL_WECHAT_PAY,
    ChannelCallError,
    ChannelLoginRequiredError,
    ChannelOrder,
    ChannelPayment,
    ChannelQueryRequest,
    ChannelReceipt,
    ChannelRef,
    ChannelRefundRequest,
    ChannelRefundState,
    CreateIntentCommand,
    DouyinMinigameChannel,
    DouyinMinigameConfig,
    GET_ACCOUNT_REALNAME_PATH,
    GoogleChannel,
    GoogleConfig,
    GrpcRealNameClient,
    HttpResponseData,
    INTENT_PAID,
    MINOR_SPENDING_GATE_REALNAME,
    MallOrder,
    MallOutcome,
    MallPendingRefundPage,
    MemoryPaymentStore,
    MinorDailyLimitError,
    MinorMonthlyLimitError,
    MinorNotAllowedError,
    MinorSingleLimitError,
    ORDER_STATE_AWAITING_PAYMENT,
    OrderPaidError,
    PAY_CHANNEL_APPLE,
    PAY_CHANNEL_DEVPAY,
    PAY_CHANNEL_DOUYIN_MINIGAME,
    PAY_CHANNEL_GOOGLE,
    PAY_CHANNEL_WECHAT_MINIGAME,
    PAY_CHANNEL_WECHAT_PAY,
    PLATFORM_ANDROID_APP,
    PLATFORM_DOUYIN_MINIGAME,
    PLATFORM_IOS_APP,
    PLATFORM_WECHAT_MINIGAME,
    PaymentBusyError,
    PaymentConfig,
    PaymentCore,
    PaymentNotifyServer,
    PaymentOptions,
    ProductMapping,
    QueryUnsupportedError,
    RealNameRequiredError,
    SubmitPurchaseCommand,
    TempUnavailableError,
    TradeState,
    WechatMinigameChannel,
    WechatMinigameConfig,
    WechatPayChannel,
    WechatPayConfig,
    alipay_sign_content,
    apple_milliunits_to_minor,
    b64url_encode,
    build_channels,
    es256_sign_raw,
    format_yuan,
    google_transaction_id,
    hmac_sha256_hex,
    jwt_sign,
    out_trade_no,
    parse_ec_private_key,
    parse_rsa_private_key,
    parse_rsa_public_key,
    parse_x509_certificates,
    parse_yuan,
    pi_birth_prefix,
    rs256_sign_raw,
    rsa_sign_sha256_b64,
    rsa_verify_sha256_b64,
    sorted_sha1,
    wechat_answer_message,
    wechat_app_sign_message,
    wechat_minigame_encrypt_push,
    wechat_request_message,
)
from xuanming.protocol import Protocol

ORDER_1 = "00112233-4455-6677-8899-aabbccddeeff"
ORDER_2 = "11112222-3333-4444-5555-666677778888"
ORDER_3 = "22223333-4444-5555-6666-777788889999"
ORDER_4 = "33334444-5555-6666-7777-88889999aaaa"
ORDER_5 = "44445555-6666-7777-8888-9999aaaabbbb"
ORDER_6 = "55556666-7777-8888-9999-aaaabbbbcccc"

# 固定基准时间：2026-11-24T04:00:00Z (北京时间 2026-11-24 12:00:00)
BASE_NOW_MS = int(dt.datetime(2026, 11, 24, 4, 0, 0, tzinfo=dt.timezone.utc).timestamp() * 1000)


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


def _gen_rsa() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _gen_ec() -> ec.EllipticCurvePrivateKey:
    return ec.generate_private_key(ec.SECP256R1())


def _pem_priv(key) -> str:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("ascii")


def _pem_pub(pub) -> str:
    return pub.public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")


def _pem_cert(cert: x509.Certificate) -> str:
    return cert.public_bytes(serialization.Encoding.PEM).decode("ascii")


def _build_apple_cert_chain(
    *,
    now_utc: dt.datetime,
    intermediate_is_ca: bool = True,
    include_leaf_oid: bool = True,
    include_inter_oid: bool = True,
    leaf_not_after_offset_days: int = 30,
):
    """生成真实 3 级 ECDSA P-256 X.509 证书链（Root -> Intermediate -> Leaf）。"""
    root_key = _gen_ec()
    inter_key = _gen_ec()
    leaf_key = _gen_ec()

    root_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test Apple Root CA")])
    inter_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test Apple WWDR Intermediate")])
    leaf_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test App Store Leaf")])

    not_before = now_utc - dt.timedelta(days=10)
    not_after = now_utc + dt.timedelta(days=365)

    root_cert = (
        x509.CertificateBuilder()
        .subject_name(root_name)
        .issuer_name(root_name)
        .public_key(root_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(root_key, hashes.SHA256())
    )

    inter_builder = (
        x509.CertificateBuilder()
        .subject_name(inter_name)
        .issuer_name(root_name)
        .public_key(inter_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=intermediate_is_ca, path_length=None), critical=True)
    )
    if include_inter_oid:
        inter_builder = inter_builder.add_extension(
            x509.UnrecognizedExtension(x509.ObjectIdentifier(APPLE_INTERMEDIATE_OID), b"\x05\x00"),
            critical=False,
        )
    inter_cert = inter_builder.sign(root_key, hashes.SHA256())

    leaf_builder = (
        x509.CertificateBuilder()
        .subject_name(leaf_name)
        .issuer_name(inter_name)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(now_utc + dt.timedelta(days=leaf_not_after_offset_days))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
    )
    if include_leaf_oid:
        leaf_builder = leaf_builder.add_extension(
            x509.UnrecognizedExtension(x509.ObjectIdentifier(APPLE_LEAF_OID), b"\x05\x00"),
            critical=False,
        )
    leaf_cert = leaf_builder.sign(inter_key, hashes.SHA256())

    x5c = [
        base64.b64encode(c.public_bytes(serialization.Encoding.DER)).decode("ascii")
        for c in (leaf_cert, inter_cert, root_cert)
    ]
    return root_cert, inter_cert, leaf_cert, leaf_key, x5c


def _sign_apple_jws(leaf_key: ec.EllipticCurvePrivateKey, x5c: list[str], payload: dict, *, alg: str = "ES256") -> str:
    return jwt_sign(
        {"alg": alg, "x5c": x5c},
        payload,
        lambda data: es256_sign_raw(leaf_key, data),
    )


def _rsa_jwk(kid: str, pub: rsa.RSAPublicKey) -> dict[str, str]:
    nums = pub.public_numbers()
    n_bytes = nums.n.to_bytes((nums.n.bit_length() + 7) // 8, "big")
    e_bytes = nums.e.to_bytes((nums.e.bit_length() + 7) // 8, "big")
    return {
        "kid": kid,
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "n": b64url_encode(n_bytes),
        "e": b64url_encode(e_bytes),
    }


def _make_pi(year: int, month: int, day: int, suffix: str = "a" * 32) -> str:
    return pi_birth_prefix(year, month, day) + suffix


class InMemoryMall:
    """最小内存商城桩，供核心链路单测使用。"""

    def __init__(self) -> None:
        self.orders: dict[str, MallOrder] = {}

    def put(self, order: MallOrder) -> None:
        self.orders[order.order_id] = order

    async def order_view(self, order_id: str) -> MallOrder:
        return self.orders[order_id]

    async def record_payment(self, payment) -> MallOutcome:
        return MallOutcome(order_state="PAID")

    async def push_revision(self, revision) -> int:
        return revision.revision

    async def pending_refunds(self, cursor: str = "") -> MallPendingRefundPage:
        return MallPendingRefundPage()

    async def report_refund(self, refund) -> None:
        return None

    async def close_refund(self, closure) -> None:
        return None

    async def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# 1. 元/分纯整数转换、密钥解析与 build_channels 工厂全量装配
# ---------------------------------------------------------------------------


def test_yuan_integer_conversion_and_key_parsers() -> None:
    assert format_yuan(0) == "0.00"
    assert format_yuan(610) == "6.10"
    assert format_yuan(1) == "0.01"
    assert parse_yuan("6.10") == 610
    assert parse_yuan("6.1") == 610
    assert parse_yuan("100") == 10000
    assert parse_yuan("0.01") == 1
    assert parse_yuan("99999.99") == 9999999

    for bad in ("", "-1.00", "6.100", "6.", ".50", "1e2", "abc", "90000000000000000"):
        with pytest.raises(ValueError):
            parse_yuan(bad)

    rsa_priv = _gen_rsa()
    assert parse_rsa_private_key(_pem_priv(rsa_priv)).key_size == 2048
    # 支持裸 base64 DER 公钥（支付宝控制台常见格式）
    raw_spki_b64 = base64.b64encode(
        rsa_priv.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    ).decode("ascii")
    assert parse_rsa_public_key(raw_spki_b64).key_size == 2048

    ec_priv = _gen_ec()
    assert parse_ec_private_key(_pem_priv(ec_priv)).curve.name == "secp256r1"
    with pytest.raises(ValueError):
        parse_ec_private_key(_pem_priv(rsa_priv))
    with pytest.raises(ValueError):
        parse_rsa_private_key(_pem_priv(ec_priv))

    root_cert, _, _, _, _ = _build_apple_cert_chain(now_utc=dt.datetime.fromtimestamp(BASE_NOW_MS / 1000, dt.timezone.utc))
    parsed_certs = parse_x509_certificates(_pem_cert(root_cert))
    assert len(parsed_certs) == 1
    assert parsed_certs[0].serial_number == root_cert.serial_number


def test_build_channels_factory_registers_all_6_production_channels() -> None:
    wx_mch = _gen_rsa()
    wx_plat = _gen_rsa()
    ali_app = _gen_rsa()
    ali_pub = _gen_rsa()
    apple_p8 = _gen_ec()
    apple_root, _, _, _, _ = _build_apple_cert_chain(now_utc=dt.datetime.fromtimestamp(BASE_NOW_MS / 1000, dt.timezone.utc))
    google_sa = _gen_rsa()
    aes_key_43 = base64.b64encode(bytes(range(32))).decode("ascii").rstrip("=")

    env = {
        "PAYMENT_DEVPAY_SECRET": "d" * 32,
        "PAYMENT_WECHATPAY_PRIVATE_KEY": _pem_priv(wx_mch),
        "PAYMENT_WECHATPAY_PUBLIC_KEY": _pem_pub(wx_plat.public_key()),
        "PAYMENT_WECHATPAY_APIV3_KEY": "a" * 32,
        "PAYMENT_ALIPAY_PRIVATE_KEY": _pem_priv(ali_app),
        "PAYMENT_ALIPAY_PUBLIC_KEY": _pem_pub(ali_pub.public_key()),
        "PAYMENT_WECHAT_MINIGAME_APP_SECRET": "wx-mini-secret-123456",
        "PAYMENT_WECHAT_MINIGAME_APP_KEY": "wx-mini-appkey-123456",
        "PAYMENT_WECHAT_MINIGAME_PUSH_TOKEN": "wx_push_tok",
        "PAYMENT_WECHAT_MINIGAME_AES_KEY": aes_key_43,
        "PAYMENT_DOUYIN_PUSH_TOKEN": "dy_push_tok",
        "PAYMENT_APPLE_PRIVATE_KEY": _pem_priv(apple_p8),
        "PAYMENT_APPLE_ROOT_CERTIFICATES": _pem_cert(apple_root),
        "PAYMENT_GOOGLE_SERVICE_ACCOUNT": json.dumps(
            {"client_email": "pay@proj.iam.gserviceaccount.com", "private_key": _pem_priv(google_sa)}
        ),
    }

    cfg = PaymentConfig(
        advertise_host="127.0.0.1",
        mode="dev",
        notify_base_url="http://127.0.0.1:18130",
        mall_payment_secret=b"m" * 32,
        devpay_enabled=True,
        devpay_secret=b"d" * 32,
        wechat_pay_enabled=True,
        wechat_pay_app_id="wx_app_1",
        wechat_pay_mch_id="1900000109",
        wechat_pay_merchant_serial_no="SERIAL001",
        wechat_pay_platform_key_id="PUB_KEY_ID_01000001",
        alipay_enabled=True,
        alipay_app_id="2021000116695358",
        wechat_minigame_enabled=True,
        wechat_minigame_app_id="wx_mini_1",
        wechat_minigame_offer_id="1450000001",
        wechat_minigame_encoding_aes_key_env="PAYMENT_WECHAT_MINIGAME_AES_KEY",
        douyin_minigame_enabled=True,
        douyin_minigame_app_id="tt_mini_1",
        apple_enabled=True,
        apple_bundle_id="com.xuanming.game",
        apple_app_apple_id=123456789,
        apple_issuer_id="issuer-uuid",
        apple_key_id="KEYID123",
        google_enabled=True,
        google_package_name="com.xuanming.game",
        google_push_audience="https://pay.example.com/notify/google",
        google_push_service_account="pubsub@proj.iam.gserviceaccount.com",
        platforms=(
            ("android_app", (CHANNEL_WECHAT_PAY, CHANNEL_ALIPAY, CHANNEL_GOOGLE, CHANNEL_DEVPAY)),
            ("ios_app", (CHANNEL_APPLE,)),
            ("wechat_minigame", (CHANNEL_WECHAT_MINIGAME,)),
            ("douyin_minigame", (CHANNEL_DOUYIN_MINIGAME,)),
        ),
    )
    cfg.validate()
    channels = build_channels(cfg, environ=env)
    names = [ch.name for ch in channels]
    assert names == [
        CHANNEL_DEVPAY,
        CHANNEL_WECHAT_PAY,
        CHANNEL_ALIPAY,
        CHANNEL_WECHAT_MINIGAME,
        CHANNEL_DOUYIN_MINIGAME,
        CHANNEL_APPLE,
        CHANNEL_GOOGLE,
    ]

    # 生产模式禁改写微信/支付宝网关、禁微信小游戏沙箱
    with pytest.raises(ValueError, match="overriding"):
        PaymentConfig(
            advertise_host="10.0.0.1",
            mode="pro",
            minor_spending_gate="overseas",
            notify_base_url="https://pay.example.com",
            mall_payment_secret=b"m" * 32,
            devpay_enabled=False,
            wechat_pay_enabled=True,
            wechat_pay_endpoint="https://mock.local",
            platforms=(),
        ).validate()
    with pytest.raises(ValueError, match="sandbox"):
        PaymentConfig(
            advertise_host="10.0.0.1",
            mode="pro",
            mall_payment_secret=b"m" * 32,
            devpay_enabled=False,
            wechat_minigame_enabled=True,
            wechat_minigame_env=1,
            platforms=(),
        ).validate()


# ---------------------------------------------------------------------------
# 2. WechatPayChannel（APIv3 请求签名、应答验签、AES-GCM 回调、查单、关单、退款、Ack）
# ---------------------------------------------------------------------------


@_async_test
async def test_wechat_pay_channel_full_lifecycle_and_security() -> None:
    clock = ManualClock(BASE_NOW_MS)
    mch_key = _gen_rsa()
    plat_key = _gen_rsa()
    apiv3_key = b"0123456789abcdef0123456789abcdef"
    plat_key_id = "PUB_KEY_ID_0114232134912410000000000000"
    trade_no = out_trade_no(ORDER_1)

    def sign_wx_resp(body: bytes, *, ts_override: str | None = None, serial: str = plat_key_id) -> dict[str, str]:
        ts = ts_override if ts_override is not None else str(clock() // 1000)
        nonce = "resp-nonce-123456"
        sig = rsa_sign_sha256_b64(plat_key, wechat_answer_message(ts, nonce, body))
        return {
            "Wechatpay-Serial": serial,
            "Wechatpay-Signature-Type": "WECHATPAY2-SHA256-RSA2048",
            "Wechatpay-Timestamp": ts,
            "Wechatpay-Nonce": nonce,
            "Wechatpay-Signature": sig,
        }

    def encrypt_wx_notify(event_type: str, plain_obj: dict, *, aad: str = "transaction", nonce: str = "123456789012") -> bytes:
        plain = json.dumps(plain_obj, separators=(",", ":")).encode("utf-8")
        ct = AESGCM(apiv3_key).encrypt(nonce.encode("utf-8"), plain, aad.encode("utf-8"))
        return json.dumps(
            {
                "event_type": event_type,
                "resource": {
                    "algorithm": "AEAD_AES_256_GCM",
                    "ciphertext": base64.b64encode(ct).decode("ascii"),
                    "associated_data": aad,
                    "nonce": nonce,
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")

    prepay_calls = 0
    order_paid_mode = False
    refund_queried = False

    async def mock_transport(method: str, url: str, headers: dict, body: bytes | None) -> HttpResponseData:
        nonlocal prepay_calls, refund_queried
        # 校验发出的每个微信 APIv3 请求签名都合法
        auth = headers["Authorization"]
        assert auth.startswith("WECHATPAY2-SHA256-RSA2048 ")
        kv = dict(re.findall(r'(\w+)="([^"]*)"', auth))
        assert kv["mchid"] == "1900000109"
        assert kv["serial_no"] == "MCH_SERIAL_01"
        parsed = urlsplit(url)
        path_q = parsed.path + (f"?{parsed.query}" if parsed.query else "")
        rsa_verify_sha256_b64(
            mch_key.public_key(),
            wechat_request_message(method, path_q, kv["timestamp"], kv["nonce_str"], body or b""),
            kv["signature"],
        )

        if parsed.path == "/v3/pay/transactions/app":
            prepay_calls += 1
            if order_paid_mode:
                return HttpResponseData(403, {}, b'{"code":"ORDERPAID","message":"paid"}')
            raw = b'{"prepay_id":"wx_prepay_998877"}'
            return HttpResponseData(200, sign_wx_resp(raw), raw)

        if parsed.path == f"/v3/pay/transactions/out-trade-no/{trade_no}":
            raw = json.dumps(
                {
                    "appid": "wx_app_001",
                    "mchid": "1900000109",
                    "out_trade_no": trade_no,
                    "transaction_id": "4200001234567890",
                    "trade_state": "SUCCESS",
                    "success_time": "2026-11-24T12:00:00+08:00",
                    "amount": {"total": 600, "currency": "CNY"},
                }
            ).encode("utf-8")
            return HttpResponseData(200, sign_wx_resp(raw), raw)

        if parsed.path == f"/v3/pay/transactions/out-trade-no/{trade_no}/close":
            return HttpResponseData(204, {}, b"")

        if parsed.path == "/v3/refund/domestic/refunds/rf_test_001":
            refund_queried = True
            return HttpResponseData(404, {}, b'{"code":"RESOURCE_NOT_EXISTS"}')

        if parsed.path == "/v3/refund/domestic/refunds" and method == "POST":
            raw = json.dumps(
                {
                    "refund_id": "503000123456789",
                    "out_refund_no": "rf_test_001",
                    "status": "SUCCESS",
                    "success_time": "2026-11-24T12:05:00+08:00",
                    "amount": {"refund": 600},
                }
            ).encode("utf-8")
            return HttpResponseData(200, sign_wx_resp(raw), raw)

        return HttpResponseData(404, {}, b'{"code":"ORDER_NOT_EXIST"}')

    ch = WechatPayChannel(
        WechatPayConfig(
            app_id="wx_app_001",
            mch_id="1900000109",
            merchant_serial_no="MCH_SERIAL_01",
            merchant_key=mch_key,
            api_v3_key=apiv3_key,
            platform_key_id=plat_key_id,
            platform_key=plat_key.public_key(),
            notify_url="https://pay.example.com/notify/wechat_pay",
            endpoint="https://wx.mock",
        ),
        clock_ms=clock,
        transport=mock_transport,
    )

    order = ChannelOrder(
        order_id=ORDER_1,
        out_trade_no=trade_no,
        player_id=1001,
        amount_minor=600,
        currency="CNY",
        expire_ms=BASE_NOW_MS + 300_000,
    )
    # 1) prepare + 验证调起签名 + prepay_id 复用
    prep1 = await ch.prepare(order, "", 0)
    assert prepay_calls == 1
    assert prep1.ref.value == "wx_prepay_998877"
    p = prep1.params.wechat_app
    assert p is not None and p["package_value"] == "Sign=WXPay"
    rsa_verify_sha256_b64(
        mch_key.public_key(),
        wechat_app_sign_message(p["app_id"], p["timestamp"], p["nonce_str"], p["prepay_id"]),
        p["sign"],
    )

    # 110 分钟内复用缓存 prepay_id
    clock.advance(60 * 60 * 1000)
    prep2 = await ch.prepare(order, prep1.ref.value, prep1.ref.at_ms)
    assert prepay_calls == 1
    assert prep2.ref.value == ""

    # ORDERPAID -> OrderPaidError
    order_paid_mode = True
    with pytest.raises(OrderPaidError):
        await ch.prepare(order, "", 0)
    order_paid_mode = False

    # 2) query / close / refund
    q = await ch.query(ChannelQueryRequest(out_trade_no=trade_no))
    assert q.state == TradeState.PAID
    assert q.payment is not None and q.payment.transaction_id == "4200001234567890" and q.payment.amount_minor == 600
    await ch.close_trade(trade_no)
    rf = await ch.refund(
        ChannelRefundRequest(
            out_trade_no=trade_no,
            transaction_id="4200001234567890",
            out_refund_no="rf_test_001",
            amount_minor=600,
            currency="CNY",
            reason="order cancel",
        )
    )
    assert refund_queried
    assert rf.state == ChannelRefundState.SUCCEEDED and rf.refund_id == "503000123456789"

    # 3) Webhook 正常付款 + 退款 + 各类安全拒绝
    pay_body = encrypt_wx_notify(
        "TRANSACTION.SUCCESS",
        {
            "appid": "wx_app_001",
            "mchid": "1900000109",
            "out_trade_no": trade_no,
            "transaction_id": "4200001234567890",
            "trade_state": "SUCCESS",
            "success_time": "2026-11-24T12:00:00+08:00",
            "amount": {"total": 600, "currency": "CNY"},
        },
    )
    note = await ch.parse_notification(sign_wx_resp(pay_body), {}, pay_body)
    assert note.payment is not None and note.payment.amount_minor == 600

    refund_body = encrypt_wx_notify(
        "REFUND.SUCCESS",
        {
            "mchid": "1900000109",
            "out_trade_no": trade_no,
            "transaction_id": "4200001234567890",
            "out_refund_no": "rf_outside_1",
            "refund_status": "SUCCESS",
            "success_time": "2026-11-24T12:10:00+08:00",
        },
        aad="refund",
    )
    r_note = await ch.parse_notification(sign_wx_resp(refund_body), {}, refund_body)
    assert len(r_note.revocations) == 1 and r_note.revocations[0].out_refund_no == "rf_outside_1"

    # 错误签名 / 未知序列号 / 超出 ±5 分钟时间窗 / 篡改密文 / appid 不匹配
    bad_hdrs = sign_wx_resp(pay_body)
    bad_hdrs["Wechatpay-Signature"] = base64.b64encode(b"bad" * 80).decode("ascii")
    with pytest.raises(BadSignatureError):
        await ch.parse_notification(bad_hdrs, {}, pay_body)

    with pytest.raises(BadSignatureError):
        await ch.parse_notification(sign_wx_resp(pay_body, serial="PUB_KEY_ID_OTHER"), {}, pay_body)

    expired_ts = str(clock() // 1000 - 301)
    with pytest.raises(BadSignatureError):
        await ch.parse_notification(sign_wx_resp(pay_body, ts_override=expired_ts), {}, pay_body)

    tampered = json.loads(pay_body.decode("utf-8"))
    tampered["resource"]["associated_data"] = "tampered"
    tampered_bytes = json.dumps(tampered).encode("utf-8")
    with pytest.raises(BadSignatureError):
        await ch.parse_notification(sign_wx_resp(tampered_bytes), {}, tampered_bytes)

    foreign_body = encrypt_wx_notify(
        "TRANSACTION.SUCCESS",
        {
            "appid": "wx_other_app",
            "mchid": "1900000109",
            "out_trade_no": trade_no,
            "transaction_id": "4200001234567890",
            "trade_state": "SUCCESS",
            "amount": {"total": 600, "currency": "CNY"},
        },
    )
    with pytest.raises(BadSignatureError):
        await ch.parse_notification(sign_wx_resp(foreign_body), {}, foreign_body)

    # Ack 格式验证
    assert ch.format_ack(AckKind.OK)[0] == 204
    assert ch.format_ack(AckKind.REJECTED)[0] == 401
    assert ch.format_ack(AckKind.RETRY)[0] == 500


# ---------------------------------------------------------------------------
# 3. AlipayChannel（表单重复键拒绝、RSA2 验签、原始 JSON 节点切片验签、纯整数 parse_yuan、Ack）
# ---------------------------------------------------------------------------


@_async_test
async def test_alipay_channel_full_lifecycle_and_security() -> None:
    clock = ManualClock(BASE_NOW_MS)
    app_key = _gen_rsa()
    alipay_key = _gen_rsa()
    trade_no = out_trade_no(ORDER_1)

    def sign_gateway_resp(node_name: str, node_raw_bytes: bytes, *, sign_over: bytes | None = None) -> bytes:
        sig = rsa_sign_sha256_b64(alipay_key, sign_over if sign_over is not None else node_raw_bytes)
        return b'{"' + node_name.encode("utf-8") + b'":' + node_raw_bytes + b',"sign":"' + sig.encode("ascii") + b'"}'

    tamper_gateway_sig = False
    slash_escape_mode = False

    async def mock_transport(method: str, url: str, headers: dict, body: bytes | None) -> HttpResponseData:
        assert method == "POST"
        form = {k: v[0] for k, v in parse_qs((body or b"").decode("utf-8")).items()}
        # 校验应用私钥 RSA2 请求签名
        rsa_verify_sha256_b64(app_key.public_key(), alipay_sign_content(form, "sign").encode("utf-8"), form["sign"])
        m = form["method"]
        if m == "alipay.trade.query":
            if slash_escape_mode:
                # 测试官方 SDK 兼容：节点原文含 \/，但签名基于替换成 / 后的内容
                node_raw = (
                    f'{{"code":"10000","msg":"Success","out_trade_no":"{trade_no}",'
                    f'"trade_no":"202611240001","trade_status":"TRADE_SUCCESS",'
                    f'"total_amount":"6.10","send_pay_date":"2026-11-24 12:00:00","note":"a\\/b"}}'
                ).encode("utf-8")
                return HttpResponseData(200, {}, sign_gateway_resp("alipay_trade_query_response", node_raw, sign_over=node_raw.replace(b"\\/", b"/")))
            node_raw = (
                f'{{"code":"10000","out_trade_no":"{trade_no}","trade_no":"202611240001",'
                f'"trade_status":"TRADE_SUCCESS","total_amount":"6.10","send_pay_date":"2026-11-24 12:00:00"}}'
            ).encode("utf-8")
            resp_bytes = sign_gateway_resp("alipay_trade_query_response", node_raw)
            if tamper_gateway_sig:
                resp_bytes = resp_bytes.replace(b'"6.10"', b'"6.20"')
            return HttpResponseData(200, {}, resp_bytes)

        if m == "alipay.trade.close":
            node_raw = b'{"code":"10000","msg":"Success"}'
            return HttpResponseData(200, {}, sign_gateway_resp("alipay_trade_close_response", node_raw))

        if m == "alipay.trade.fastpay.refund.query":
            node_raw = b'{"code":"10000","msg":"Success"}'
            return HttpResponseData(200, {}, sign_gateway_resp("alipay_trade_fastpay_refund_query_response", node_raw))

        if m == "alipay.trade.refund":
            node_raw = b'{"code":"10000","fund_change":"Y","gmt_refund_pay":"2026-11-24 12:05:00"}'
            return HttpResponseData(200, {}, sign_gateway_resp("alipay_trade_refund_response", node_raw))

        return HttpResponseData(500, {}, b"")

    ch = AlipayChannel(
        AlipayConfig(
            app_id="2021000116695358",
            app_key=app_key,
            alipay_key=alipay_key.public_key(),
            notify_url="https://pay.example.com/notify/alipay",
            seller_id="2088102146225135",
            gateway="https://alipay.mock/gateway.do",
            subject="宝石礼包",
        ),
        clock_ms=clock,
        transport=mock_transport,
    )

    order = ChannelOrder(
        order_id=ORDER_1,
        out_trade_no=trade_no,
        player_id=1001,
        amount_minor=610,
        currency="CNY",
        expire_ms=BASE_NOW_MS + 300_000,
    )
    prep = await ch.prepare(order, "", 0)
    assert prep.params.alipay_app is not None
    order_str = prep.params.alipay_app["order_string"]
    parsed_order = {k: v[0] for k, v in parse_qs(order_str).items()}
    assert parsed_order["sign_type"] == "RSA2"
    assert '"total_amount":"6.10"' in parsed_order["biz_content"]
    rsa_verify_sha256_b64(app_key.public_key(), alipay_sign_content(parsed_order, "sign").encode("utf-8"), parsed_order["sign"])

    # query（正常 + 转义 \/ 兼容 + 篡改拒绝）
    q = await ch.query(ChannelQueryRequest(out_trade_no=trade_no))
    assert q.state == TradeState.PAID and q.payment is not None and q.payment.amount_minor == 610

    slash_escape_mode = True
    q2 = await ch.query(ChannelQueryRequest(out_trade_no=trade_no))
    assert q2.state == TradeState.PAID
    slash_escape_mode = False

    tamper_gateway_sig = True
    with pytest.raises(Exception, match="signature rejected"):
        await ch.query(ChannelQueryRequest(out_trade_no=trade_no))
    tamper_gateway_sig = False

    await ch.close_trade(trade_no)
    rf = await ch.refund(
        ChannelRefundRequest(
            out_trade_no=trade_no,
            transaction_id="202611240001",
            out_refund_no="rf_ali_01",
            amount_minor=610,
            currency="CNY",
            reason="refund",
        )
    )
    assert rf.state == ChannelRefundState.SUCCEEDED and rf.refund_id == "rf_ali_01"

    # Webhook 验签：正常 + 退款撤销 + 重复键拒绝 + 非 RSA2 拒绝 + 篡改拒绝
    def make_form(params: dict[str, str]) -> bytes:
        p = dict(params)
        p["sign"] = rsa_sign_sha256_b64(alipay_key, alipay_sign_content(p, "sign", "sign_type").encode("utf-8"))
        return urlencode(sorted(p.items())).encode("utf-8")

    good_params = {
        "app_id": "2021000116695358",
        "seller_id": "2088102146225135",
        "sign_type": "RSA2",
        "trade_status": "TRADE_SUCCESS",
        "trade_no": "202611240001",
        "out_trade_no": trade_no,
        "total_amount": "6.10",
        "gmt_payment": "2026-11-24 12:00:00",
    }
    good_form = make_form(good_params)
    note = await ch.parse_notification({}, {}, good_form)
    assert note.payment is not None and note.payment.amount_minor == 610 and note.payment.transaction_id == "202611240001"

    # 重复键注入（HTTP 参数污染）必须拒绝
    dup_form = good_form + b"&total_amount=0.01"
    with pytest.raises(BadSignatureError):
        await ch.parse_notification({}, {}, dup_form)

    # sign_type != RSA2 拒绝
    bad_type = make_form({**good_params, "sign_type": "RSA"})
    with pytest.raises(BadSignatureError):
        await ch.parse_notification({}, {}, bad_type)

    # 退款异步通知（带 refund_fee）产生 Revocation
    refund_form = make_form({**good_params, "trade_status": "TRADE_CLOSED", "refund_fee": "6.10", "gmt_refund": "2026-11-24 12:05:00"})
    r_note = await ch.parse_notification({}, {}, refund_form)
    assert r_note.payment is None and len(r_note.revocations) == 1 and r_note.revocations[0].revoked

    # Ack 格式验证
    assert ch.format_ack(AckKind.OK) == (200, b"success", "text/plain; charset=utf-8")
    assert ch.format_ack(AckKind.REJECTED) == (200, b"fail", "text/plain; charset=utf-8")
    assert ch.format_ack(AckKind.RETRY) == (200, b"fail", "text/plain; charset=utf-8")


# ---------------------------------------------------------------------------
# 4. WechatMinigameChannel（GET echostr 握手、明文与 AES-256-CBC 回调、session_key 加密、90016）
# ---------------------------------------------------------------------------


@_async_test
async def test_wechat_minigame_channel_handshake_aes_and_query() -> None:
    clock = ManualClock(BASE_NOW_MS)
    trade_no = out_trade_no(ORDER_1)
    aes_raw = bytes(range(10, 42))
    encoding_aes_key = base64.b64encode(aes_raw).decode("ascii").rstrip("=")
    query_errcode = 0
    token_fetches = 0

    async def mock_transport(method: str, url: str, headers: dict, body: bytes | None) -> HttpResponseData:
        nonlocal token_fetches
        parsed = urlsplit(url)
        if parsed.path == "/sns/jscode2session":
            qs = parse_qs(parsed.query)
            if qs["js_code"][0] == "expired_code":
                return HttpResponseData(200, {}, b'{"errcode":40029,"errmsg":"invalid code"}')
            return HttpResponseData(200, {}, b'{"openid":"openid_u1","session_key":"sess_secret_xyz"}')
        if parsed.path == "/cgi-bin/stable_token":
            token_fetches += 1
            return HttpResponseData(200, {}, b'{"access_token":"tok_1","expires_in":7200}')
        if parsed.path == "/wxa/game/queryorderinfo":
            if query_errcode != 0:
                return HttpResponseData(200, {}, json.dumps({"errcode": query_errcode}).encode("utf-8"))
            return HttpResponseData(
                200,
                {},
                json.dumps(
                    {
                        "errcode": 0,
                        "pay_state": 2,
                        "pay_finish_time": BASE_NOW_MS // 1000,
                        "out_trade_no": trade_no,
                        "transaction_id": "wx_mini_tx_01",
                        "product_id": "sku_gem_60",
                    }
                ).encode("utf-8"),
            )
        return HttpResponseData(404, {}, b"{}")

    ch = WechatMinigameChannel(
        WechatMinigameConfig(
            app_id="wx_mini_app_01",
            app_secret="wx-mini-secret-0123456789",
            offer_id="1450000123",
            app_key="wx-mini-appkey-0123456789",
            push_token="wx_push_token_01",
            env=0,
            encoding_aes_key=encoding_aes_key,
            api_base="https://wxmini.mock",
        ),
        clock_ms=clock,
        transport=mock_transport,
    )

    # 1) GET echostr 握手
    sig = sorted_sha1("wx_push_token_01", "1700000000", "nonce1")
    assert ch.handshake({"signature": sig, "timestamp": "1700000000", "nonce": "nonce1", "echostr": "hello_wx"}) == (
        200,
        b"hello_wx",
        "text/plain; charset=utf-8",
    )
    assert ch.handshake({"signature": "00" * 20, "timestamp": "1700000000", "nonce": "nonce1", "echostr": "hello_wx"})[0] == 401

    # 2) prepare：缺少或过期 code 抛 ChannelLoginRequiredError；成功时加密 session_key
    order = ChannelOrder(
        order_id=ORDER_1,
        out_trade_no=trade_no,
        player_id=1001,
        amount_minor=600,
        currency="CNY",
        expire_ms=BASE_NOW_MS + 300_000,
        product_id="sku_gem_60",
        login_code="",
    )
    with pytest.raises(ChannelLoginRequiredError):
        await ch.prepare(order, "", 0)
    with pytest.raises(ChannelLoginRequiredError):
        await ch.prepare(replace(order, login_code="expired_code"), "", 0)

    prep = await ch.prepare(replace(order, login_code="good_code"), "", 0)
    assert prep.payer_id == "openid_u1"
    assert prep.ref.value and "sess_secret_xyz" not in prep.ref.value
    assert ch.unseal_session_key(prep.ref.value) == "sess_secret_xyz"
    wm = prep.params.wechat_minigame
    assert wm is not None
    assert wm["pay_sig"] == hmac_sha256_hex(b"wx-mini-appkey-0123456789", "requestMidasPaymentGameItem&" + wm["sign_data"])
    assert wm["signature"] == hmac_sha256_hex(b"sess_secret_xyz", wm["sign_data"])

    # 3) query：正常 PAID 与 errcode=90016 -> QueryUnsupportedError
    q = await ch.query(ChannelQueryRequest(out_trade_no=trade_no, payer_id="openid_u1", ref=prep.ref, amount_minor=600, currency="CNY"))
    assert q.state == TradeState.PAID and q.payment is not None and q.payment.transaction_id == trade_no
    assert token_fetches == 1

    query_errcode = 90016
    with pytest.raises(QueryUnsupportedError):
        await ch.query(ChannelQueryRequest(out_trade_no=trade_no, payer_id="openid_u1", ref=prep.ref, amount_minor=600, currency="CNY"))
    query_errcode = 0

    # 4) 明文与 AES-256-CBC 安全模式回调 + PayEventSig 校验 + 篡改/错 AppID 拒绝
    payload_str = json.dumps(
        {
            "Env": 0,
            "OutTradeNo": trade_no,
            "GoodsInfo": {"ProductId": "sku_gem_60", "Quantity": 1, "OrigPrice": 600},
            "WeChatPayInfo": {"TransactionId": "wx_pay_id_01"},
        },
        separators=(",", ":"),
    )
    event = "minigame_game_pay_goods_deliver_notify"
    push_plain = json.dumps(
        {
            "Event": event,
            "MiniGame": {
                "Payload": payload_str,
                "PayEventSig": hmac_sha256_hex(b"wx-mini-appkey-0123456789", f"{event}&{payload_str}"),
                "IsMock": False,
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")

    # 明文模式
    note_plain = await ch.parse_notification(
        {},
        {"timestamp": "1700000000", "nonce": "n1", "signature": sorted_sha1("wx_push_token_01", "1700000000", "n1")},
        push_plain,
    )
    assert note_plain.payment is not None and note_plain.payment.amount_minor == 600

    # AES-256-CBC 安全模式（32B PKCS#7 + 16B rand + 4B BE len + AppID）
    enc_b64 = wechat_minigame_encrypt_push(aes_raw, "wx_mini_app_01", push_plain)
    aes_body = json.dumps({"Encrypt": enc_b64}).encode("utf-8")
    msg_sig = sorted_sha1("wx_push_token_01", "1700000000", "n2", enc_b64)
    note_aes = await ch.parse_notification(
        {},
        {"encrypt_type": "aes", "timestamp": "1700000000", "nonce": "n2", "msg_signature": msg_sig},
        aes_body,
    )
    assert note_aes.payment is not None and note_aes.payment.channel_order_id == "wx_pay_id_01"

    # AES 明文内 AppID 不匹配必须拒绝
    wrong_app_enc = wechat_minigame_encrypt_push(aes_raw, "wx_wrong_app", push_plain)
    wrong_msg_sig = sorted_sha1("wx_push_token_01", "1700000000", "n3", wrong_app_enc)
    with pytest.raises(BadSignatureError):
        await ch.parse_notification(
            {},
            {"encrypt_type": "aes", "timestamp": "1700000000", "nonce": "n3", "msg_signature": wrong_msg_sig},
            json.dumps({"Encrypt": wrong_app_enc}).encode("utf-8"),
        )

    # PayEventSig 被篡改必须拒绝
    bad_ev = json.loads(push_plain.decode("utf-8"))
    bad_ev["MiniGame"]["PayEventSig"] = "00" * 32
    with pytest.raises(BadSignatureError):
        await ch.parse_notification(
            {},
            {"timestamp": "1700000000", "nonce": "n1", "signature": sorted_sha1("wx_push_token_01", "1700000000", "n1")},
            json.dumps(bad_ev).encode("utf-8"),
        )

    assert ch.format_ack(AckKind.OK) == (200, b'{"ErrCode":0,"ErrMsg":"Success"}', "application/json")
    assert ch.format_ack(AckKind.REJECTED)[0] == 200


# ---------------------------------------------------------------------------
# 5. DouyinMinigameChannel（GET echostr 握手、POST SHA1 回调、appid 校验、Ack）
# ---------------------------------------------------------------------------


@_async_test
async def test_douyin_minigame_channel_handshake_and_webhook() -> None:
    trade_no = out_trade_no(ORDER_1)
    ch = DouyinMinigameChannel(
        DouyinMinigameConfig(app_id="tt_app_123", push_token="dy_token_secret", zone_id="2", good_name="默认名")
    )

    # prepare 截断商品名至 10 字符
    prep = await ch.prepare(
        ChannelOrder(
            order_id=ORDER_1,
            out_trade_no=trade_no,
            player_id=1001,
            amount_minor=1200,
            currency="CNY",
            expire_ms=BASE_NOW_MS + 300_000,
            title="超长十二个字符的豪华礼包名称",
        ),
        "",
        0,
    )
    assert prep.params.douyin_minigame is not None
    assert len(prep.params.douyin_minigame["good_name"]) == 10
    assert prep.params.douyin_minigame["good_type"] == 2

    with pytest.raises(QueryUnsupportedError):
        await ch.query(ChannelQueryRequest(out_trade_no=trade_no))

    # GET echostr 握手
    hs_sig = sorted_sha1("dy_token_secret", "1700000000", "n1", "hello")
    assert ch.handshake(
        {"timestamp": "1700000000", "nonce": "n1", "msg": "hello", "signature": hs_sig, "echostr": "echo_dy"}
    ) == (200, b"echo_dy", "text/plain; charset=utf-8")
    assert ch.handshake({"timestamp": "1700000000", "nonce": "n1", "msg": "hello", "signature": "bad", "echostr": "echo_dy"})[0] == 401

    # POST 回调验签 + appid 校验
    msg_str = json.dumps(
        {
            "appid": "tt_app_123",
            "cp_orderno": trade_no,
            "order_no_channel": "dy_ch_999",
            "amount_cent": 1200,
            "currency": "CNY",
        },
        separators=(",", ":"),
    )
    post_sig = sorted_sha1("dy_token_secret", "1700000001", "n9", msg_str)
    body = json.dumps(
        {"timestamp": "1700000001", "nonce": "n9", "msg": msg_str, "signature": post_sig, "type": "payment"}
    ).encode("utf-8")
    note = await ch.parse_notification({}, {}, body)
    assert note.payment is not None and note.payment.amount_minor == 1200 and note.payment.channel_order_id == "dy_ch_999"

    # 错误 appid 拒绝
    wrong_msg = json.dumps({"appid": "tt_other", "cp_orderno": trade_no, "amount_cent": 1200})
    wrong_sig = sorted_sha1("dy_token_secret", "1700000001", "n9", wrong_msg)
    with pytest.raises(BadSignatureError):
        await ch.parse_notification(
            {},
            {},
            json.dumps({"timestamp": "1700000001", "nonce": "n9", "msg": wrong_msg, "signature": wrong_sig}).encode("utf-8"),
        )

    assert ch.format_ack(AckKind.OK) == (200, b'{"err_no":0,"err_tips":"success"}', "application/json")
    assert ch.format_ack(AckKind.REJECTED)[0] == 500


# ---------------------------------------------------------------------------
# 6. AppleChannel（ES256 JWS + 3 证 x5c 链 + OIDs + 毫单位价格 + SubmitPurchase + 历史减半窗）
# ---------------------------------------------------------------------------


@_async_test
async def test_apple_channel_x5c_verification_submit_purchase_and_history_halving() -> None:
    clock = ManualClock(BASE_NOW_MS)
    now_utc = dt.datetime.fromtimestamp(BASE_NOW_MS / 1000, dt.timezone.utc)
    api_key = _gen_ec()
    root_cert, _, _, leaf_key, x5c = _build_apple_cert_chain(now_utc=now_utc)

    # ISO-4217 毫单位价格纯整数转换
    assert apple_milliunits_to_minor(6000, "CNY") == 600
    assert apple_milliunits_to_minor(9990, "USD") == 999
    assert apple_milliunits_to_minor(120000, "JPY") == 120
    assert apple_milliunits_to_minor(1250, "KWD") == 1250

    txn_payload = {
        "transactionId": "2000000123456789",
        "bundleId": "com.xuanming.game",
        "productId": "com.xuanming.gem60",
        "purchaseDate": BASE_NOW_MS,
        "quantity": 1,
        "type": "Consumable",
        "appAccountToken": ORDER_1,
        "environment": "Production",
        "price": 6000,
        "currency": "CNY",
        "signedDate": BASE_NOW_MS,
    }
    signed_txn = _sign_apple_jws(leaf_key, x5c, txn_payload)
    signed_notif = _sign_apple_jws(
        leaf_key,
        x5c,
        {
            "notificationType": "ONE_TIME_CHARGE",
            "signedDate": BASE_NOW_MS,
            "data": {
                "appAppleId": 987654321,
                "bundleId": "com.xuanming.game",
                "environment": "Production",
                "signedTransactionInfo": signed_txn,
            },
        },
    )

    history_windows: list[tuple[int, int]] = []

    async def mock_transport(method: str, url: str, headers: dict, body: bytes | None) -> HttpResponseData:
        assert headers["Authorization"].startswith("Bearer ")
        parsed = urlsplit(url)
        if parsed.path == "/inApps/v1/transactions/2000000123456789":
            return HttpResponseData(200, {}, json.dumps({"signedTransactionInfo": signed_txn}).encode("utf-8"))
        if parsed.path == "/inApps/v1/notifications/history":
            req_obj = json.loads((body or b"{}").decode("utf-8"))
            start_d, end_d = req_obj["startDate"], req_obj["endDate"]
            history_windows.append((start_d, end_d))
            # 当窗口宽于 30 分钟时模拟 >50 页迫使触发窗口减半
            if (end_d - start_d) > 30 * 60 * 1000:
                return HttpResponseData(
                    200,
                    {},
                    json.dumps({"notificationHistory": [], "hasMore": True, "paginationToken": "next_p"}).encode("utf-8"),
                )
            return HttpResponseData(
                200,
                {},
                json.dumps({"notificationHistory": [{"signedPayload": signed_notif}], "hasMore": False}).encode("utf-8"),
            )
        return HttpResponseData(404, {}, b'{"errorCode":4040010}')

    ch = AppleChannel(
        AppleConfig(
            bundle_id="com.xuanming.game",
            app_apple_id=987654321,
            issuer_id="issuer-id",
            key_id="KID001",
            private_key=api_key,
            root_certificates=(root_cert,),
            allow_sandbox=True,
            production_api="https://apple.mock",
            sandbox_api="https://apple-sandbox.mock",
        ),
        clock_ms=clock,
        transport=mock_transport,
    )

    # 1) SubmitPurchase 端到端链路（PaymentCore -> AppleChannel.verify -> 记账并转 INTENT_PAID）
    store = MemoryPaymentStore()
    mall = InMemoryMall()
    mall.put(
        MallOrder(
            order_id=ORDER_1,
            buyer_id="1001",
            sku_id=10,
            quantity=1,
            amount_minor=600,
            currency="CNY",
            state=ORDER_STATE_AWAITING_PAYMENT,
            payment_deadline_ms=BASE_NOW_MS + 300_000,
        )
    )
    core = PaymentCore(
        store,
        mall,
        [ch],
        {PLATFORM_IOS_APP: [PAY_CHANNEL_APPLE]},
        options=PaymentOptions(products=(ProductMapping(sku_id=10, by_channel=((CHANNEL_APPLE, "com.xuanming.gem60"),)),)),
        clock_ms=clock,
    )
    created = await core.create_intent(CreateIntentCommand(1001, ORDER_1, PAY_CHANNEL_APPLE, PLATFORM_IOS_APP))
    assert created.params.apple == {"product_id": "com.xuanming.gem60", "app_account_token": ORDER_1}

    submitted = await core.submit_purchase(
        SubmitPurchaseCommand(player_id=1001, channel=PAY_CHANNEL_APPLE, order_id=ORDER_1, apple_transaction_id="2000000123456789")
    )
    assert submitted.finish_transaction is True
    assert submitted.intent is not None and submitted.intent.state == INTENT_PAID

    # 2) Webhook ONE_TIME_CHARGE + REFUND + REFUND_REVERSED
    n1 = await ch.parse_notification({}, {}, json.dumps({"signedPayload": signed_notif}).encode("utf-8"))
    assert n1.payment is not None and n1.payment.store_amount_minor == 600 and n1.payment.store_currency == "CNY"

    signed_refund = _sign_apple_jws(
        leaf_key,
        x5c,
        {
            "notificationType": "REFUND",
            "signedDate": BASE_NOW_MS + 5000,
            "data": {
                "appAppleId": 987654321,
                "bundleId": "com.xuanming.game",
                "environment": "Production",
                "signedTransactionInfo": signed_txn,
            },
        },
    )
    n_ref = await ch.parse_notification({}, {}, json.dumps({"signedPayload": signed_refund}).encode("utf-8"))
    assert len(n_ref.revocations) == 1 and n_ref.revocations[0].revoked is True

    # 3) 证书链与 JWS 安全拒绝：缺少 OID / 中间证书非 CA / 过期证书 / 非受信任根 / alg=none / 篡改签名
    _, _, _, leaf_no_oid_key, x5c_no_oid = _build_apple_cert_chain(now_utc=now_utc, include_leaf_oid=False)
    _, _, _, leaf_noca_key, x5c_noca = _build_apple_cert_chain(now_utc=now_utc, intermediate_is_ca=False)
    _, _, _, leaf_exp_key, x5c_exp = _build_apple_cert_chain(now_utc=now_utc, leaf_not_after_offset_days=-1)
    other_root, _, _, leaf_other_key, x5c_other = _build_apple_cert_chain(now_utc=now_utc)
    assert other_root.serial_number != root_cert.serial_number

    for bad_jws in (
        _sign_apple_jws(leaf_no_oid_key, x5c_no_oid, txn_payload),
        _sign_apple_jws(leaf_noca_key, x5c_noca, txn_payload),
        _sign_apple_jws(leaf_exp_key, x5c_exp, txn_payload),
        _sign_apple_jws(leaf_other_key, x5c_other, txn_payload),
        _sign_apple_jws(leaf_key, x5c, txn_payload, alg="none"),
        signed_notif[:-4] + ("AAAA" if not signed_notif.endswith("AAAA") else "BBBB"),
    ):
        with pytest.raises(BadSignatureError):
            await ch.parse_notification({}, {}, json.dumps({"signedPayload": bad_jws}).encode("utf-8"))

    # 4) poll_history 超过 50 页自动将时间窗减半并推进游标
    since_ms = BASE_NOW_MS - 120 * 60 * 1000  # 2 小时窗口 -> 减半至 ~30 分钟内完成
    notes, new_cursor = await ch.poll_history(since_ms)
    assert len(notes) == 1
    assert since_ms < new_cursor < BASE_NOW_MS - 60_000
    assert len(history_windows) > 50  # 首次宽窗翻满 50 页触发减半

    assert ch.format_ack(AckKind.OK)[0] == 200
    assert ch.format_ack(AckKind.RETRY)[0] == 500


# ---------------------------------------------------------------------------
# 7. GoogleChannel（OIDC RS256 JWKS 缓存与 1m 冷却、500 TempUnavailable、Pub/Sub、Consume、Voided）
# ---------------------------------------------------------------------------


@_async_test
async def test_google_channel_jwks_caching_submit_finalize_and_poll_history() -> None:
    clock = ManualClock(BASE_NOW_MS)
    sa_key = _gen_rsa()
    oidc_key1 = _gen_rsa()
    oidc_key2 = _gen_rsa()
    certs_fetches = 0
    certs_fail_mode = False
    certs_keys = [_rsa_jwk("kid-1", oidc_key1.public_key())]
    consumed_tokens: list[str] = []

    def sign_oidc(
        key: rsa.RSAPrivateKey,
        kid: str,
        *,
        iss: str = "https://accounts.google.com",
        aud: str = "https://pay.example.com/notify/google",
        email: str = "pubsub@proj.iam.gserviceaccount.com",
        email_verified: bool = True,
        exp: int | None = None,
    ) -> str:
        claims = {
            "iss": iss,
            "aud": aud,
            "email": email,
            "email_verified": email_verified,
            "exp": exp if exp is not None else (clock() // 1000) + 300,
        }
        return jwt_sign({"alg": "RS256", "kid": kid, "typ": "JWT"}, claims, lambda d: rs256_sign_raw(key, d))

    async def mock_transport(method: str, url: str, headers: dict, body: bytes | None) -> HttpResponseData:
        nonlocal certs_fetches
        parsed = urlsplit(url)
        if url == "https://oauth2.mock/token":
            return HttpResponseData(200, {}, b'{"access_token":"g_tok_1","expires_in":3600}')
        if url == "https://certs.mock/jwks":
            certs_fetches += 1
            if certs_fail_mode:
                return HttpResponseData(503, {}, b"unavailable")
            return HttpResponseData(200, {}, json.dumps({"keys": certs_keys}).encode("utf-8"))
        if parsed.path.endswith("/purchases/productsv2/tokens/ptok_abc"):
            return HttpResponseData(
                200,
                {},
                json.dumps(
                    {
                        "productLineItem": [
                            {
                                "productId": "sku_gem_60",
                                "productOfferDetails": {
                                    "quantity": 1,
                                    "refundableQuantity": 1,
                                    "consumptionState": "CONSUMPTION_STATE_YET_TO_BE_CONSUMED",
                                },
                            }
                        ],
                        "purchaseStateContext": {"purchaseState": "PURCHASED"},
                        "orderId": "GPA.1234-5678",
                        "obfuscatedExternalAccountId": ch.account_ref(1001),
                        "obfuscatedExternalProfileId": ORDER_1,
                        "purchaseCompletionTime": "2026-11-24T04:00:00Z",
                    }
                ).encode("utf-8"),
            )
        if parsed.path.endswith(":consume"):
            consumed_tokens.append(parsed.path)
            return HttpResponseData(204, {}, b"")
        if parsed.path.endswith("/purchases/voidedpurchases"):
            return HttpResponseData(
                200,
                {},
                json.dumps(
                    {
                        "voidedPurchases": [
                            {"purchaseToken": "ptok_voided_1", "voidedTimeMillis": str(BASE_NOW_MS - 120_000)}
                        ]
                    }
                ).encode("utf-8"),
            )
        return HttpResponseData(404, {}, b"{}")

    ch = GoogleChannel(
        GoogleConfig(
            package_name="com.xuanming.game",
            client_email="pay@proj.iam.gserviceaccount.com",
            private_key=sa_key,
            push_audience="https://pay.example.com/notify/google",
            push_service_account="pubsub@proj.iam.gserviceaccount.com",
            api_base="https://gplay.mock",
            token_url="https://oauth2.mock/token",
            certs_url="https://certs.mock/jwks",
        ),
        clock_ms=clock,
        transport=mock_transport,
    )

    # 1) SubmitPurchase + finalize(consume) + poll_history(voided)
    store = MemoryPaymentStore()
    mall = InMemoryMall()
    mall.put(
        MallOrder(
            order_id=ORDER_1,
            buyer_id="1001",
            sku_id=10,
            quantity=1,
            amount_minor=600,
            currency="CNY",
            state=ORDER_STATE_AWAITING_PAYMENT,
            payment_deadline_ms=BASE_NOW_MS + 300_000,
        )
    )
    core = PaymentCore(
        store,
        mall,
        [ch],
        {PLATFORM_ANDROID_APP: [PAY_CHANNEL_GOOGLE]},
        options=PaymentOptions(products=(ProductMapping(sku_id=10, by_channel=((CHANNEL_GOOGLE, "sku_gem_60"),)),)),
        clock_ms=clock,
    )
    await core.create_intent(CreateIntentCommand(1001, ORDER_1, PAY_CHANNEL_GOOGLE, PLATFORM_ANDROID_APP))
    submitted = await core.submit_purchase(
        SubmitPurchaseCommand(
            player_id=1001,
            channel=PAY_CHANNEL_GOOGLE,
            order_id=ORDER_1,
            google_purchase_token="ptok_abc",
            google_product_id="sku_gem_60",
        )
    )
    assert submitted.finish_transaction is True
    assert submitted.intent is not None and submitted.intent.state == INTENT_PAID

    await ch.finalize("sku_gem_60", "ptok_abc")
    assert len(consumed_tokens) == 1

    voided_notes, cursor_ms = await ch.poll_history(BASE_NOW_MS - 3600_000)
    assert len(voided_notes) == 1
    assert voided_notes[0].revocations[0].transaction_id == google_transaction_id("ptok_voided_1")
    assert cursor_ms == BASE_NOW_MS - 60_000

    # 2) Pub/Sub OIDC 推送：JWKS 失败 -> TempUnavailableError -> NotifyServer 回 500 重试
    pubsub_data = base64.b64encode(
        json.dumps(
            {
                "packageName": "com.xuanming.game",
                "eventTimeMillis": str(BASE_NOW_MS),
                "oneTimeProductNotification": {"notificationType": 1, "purchaseToken": "ptok_abc", "sku": "sku_gem_60"},
            }
        ).encode("utf-8")
    ).decode("ascii")
    pubsub_body = json.dumps({"message": {"data": pubsub_data}}).encode("utf-8")

    certs_fail_mode = True
    with pytest.raises(TempUnavailableError):
        await ch.parse_notification({"Authorization": "Bearer " + sign_oidc(oidc_key1, "kid-1")}, {}, pubsub_body)

    notify_server = PaymentNotifyServer(core, host="127.0.0.1", port=0)
    port = await notify_server.start()
    try:
        async with aiohttp.ClientSession() as http:
            async with http.post(
                f"http://127.0.0.1:{port}/notify/google",
                data=pubsub_body,
                headers={"Authorization": "Bearer " + sign_oidc(oidc_key1, "kid-1")},
            ) as resp:
                assert resp.status == 500  # JWKS 不可用时回 500 让 Pub/Sub 重投

            # 恢复 JWKS 后：正常验证并回 204
            certs_fail_mode = False
            async with http.post(
                f"http://127.0.0.1:{port}/notify/google",
                data=pubsub_body,
                headers={"Authorization": "Bearer " + sign_oidc(oidc_key1, "kid-1")},
            ) as resp:
                assert resp.status == 204
    finally:
        await notify_server.close()

    fetches_after_first = certs_fetches

    # 3) 未知 kid 在 1 分钟冷却期内不触发 JWKS 重拉（防 DoS）
    certs_keys.append(_rsa_jwk("kid-2", oidc_key2.public_key()))
    with pytest.raises(BadSignatureError):
        await ch.parse_notification({"Authorization": "Bearer " + sign_oidc(oidc_key2, "kid-2")}, {}, pubsub_body)
    assert certs_fetches == fetches_after_first

    # 超过 1 分钟后自动重拉 JWKS，kid-2 立即验签通过
    clock.advance(61_000)
    note_k2 = await ch.parse_notification({"Authorization": "Bearer " + sign_oidc(oidc_key2, "kid-2")}, {}, pubsub_body)
    assert certs_fetches == fetches_after_first + 1
    assert len(note_k2.receipts) == 1 and note_k2.receipts[0].purchase_token == "ptok_abc"

    # 4) OIDC 声明异常（错误 iss / aud / email / email_verified=False / 过期 exp）全部拒绝
    for bad_token in (
        sign_oidc(oidc_key1, "kid-1", iss="https://evil.example"),
        sign_oidc(oidc_key1, "kid-1", aud="https://other.example"),
        sign_oidc(oidc_key1, "kid-1", email="other@proj.iam.gserviceaccount.com"),
        sign_oidc(oidc_key1, "kid-1", email_verified=False),
        sign_oidc(oidc_key1, "kid-1", exp=(clock() // 1000) - 1),
    ):
        with pytest.raises(BadSignatureError):
            await ch.parse_notification({"Authorization": "Bearer " + bad_token}, {}, pubsub_body)

    assert ch.format_ack(AckKind.OK)[0] == 204
    assert ch.format_ack(AckKind.REJECTED)[0] == 500


# ---------------------------------------------------------------------------
# 8. GrpcRealNameClient + 未成年人限额闸（<8 / 8-15 / 16-17 / 成年 / 未实名 / 小游戏平台管辖）
# ---------------------------------------------------------------------------


@_async_test
async def test_grpc_realname_client_and_minor_spending_limits() -> None:
    protocol = Protocol()
    clock = ManualClock(BASE_NOW_MS)  # 北京时间 2026-11-24 12:00:00
    secret = b"payment-account-directory-secret-0123456789"

    # 模拟账号目录数据（不同年龄段）
    directory_pis: dict[int, tuple[str, bool]] = {
        101: (_make_pi(2019, 1, 1), True),  # 7 岁（<8 禁充）
        102: (_make_pi(2014, 6, 1), True),  # 12 岁（8-15 岁：单笔/日 50 元，月 200 元）
        103: (_make_pi(2009, 6, 1), True),  # 17 岁（16-17 岁：单笔/日 100 元，月 400 元）
        104: (_make_pi(1998, 1, 1), True),  # 28 岁（成年无限制）
        105: ("", True),  # 未在游戏方实名（App 拒绝，小游戏放行）
        106: ("", False),  # 目录无此角色
        107: ("malformed-pi", True),  # 异常畸形 pi -> fail-closed
    }
    verified_calls = 0

    async def handle_get_real_name(request, context, path: str):
        nonlocal verified_calls
        assert path == GET_ACCOUNT_REALNAME_PATH
        assert verify_service_call(path, request, "payment", clock(), secret)
        verified_calls += 1
        pi, found = directory_pis.get(int(request.player_id), ("", False))
        return protocol.message("data_service.GetAccountRealNameResponse", found=found, real_name_pi=pi)

    server, port = await serve_grpc(
        protocol,
        "127.0.0.1:0",
        {"data_service.DataService": {"GetAccountRealName": handle_get_real_name}},
        pass_context=True,
    )
    rn_client = GrpcRealNameClient.from_endpoint(protocol, f"127.0.0.1:{port}", secret, clock_ms=clock)

    try:
        # 直接校验 GrpcRealNameClient 对异常 pi fail-closed
        with pytest.raises(RuntimeError, match="malformed pi"):
            await rn_client.real_name_of(107)

        # 装配 PaymentCore（DevPay + DouyinMinigame + GrpcRealNameClient）
        store = MemoryPaymentStore()
        mall = InMemoryMall()
        from xuanming.payment import DevPayChannel

        devpay = DevPayChannel("devpay-secret-for-minor-gate-0123456789", clock_ms=clock)
        douyin = DouyinMinigameChannel(DouyinMinigameConfig(app_id="tt_1", push_token="tok_1"))
        core = PaymentCore(
            store,
            mall,
            [devpay, douyin],
            {
                PLATFORM_ANDROID_APP: [PAY_CHANNEL_DEVPAY],
                PLATFORM_DOUYIN_MINIGAME: [PAY_CHANNEL_DOUYIN_MINIGAME],
            },
            options=PaymentOptions(real_names=rn_client),
            clock_ms=clock,
        )

        def put_order(oid: str, player_id: int, amount_minor: int) -> None:
            mall.put(
                MallOrder(
                    order_id=oid,
                    buyer_id=str(player_id),
                    sku_id=10,
                    quantity=1,
                    amount_minor=amount_minor,
                    currency="CNY",
                    state=ORDER_STATE_AWAITING_PAYMENT,
                    payment_deadline_ms=clock() + 300_000,
                )
            )

        # 1) <8 岁禁充
        put_order(ORDER_1, 101, 100)
        with pytest.raises(MinorNotAllowedError):
            await core.create_intent(CreateIntentCommand(101, ORDER_1, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))

        # 2) 8-15 岁：单笔 >50 元拒绝；50 元通过；同日再充 1 分触发日限；跨日累计到 200 元后第 5 日触发月限
        put_order(ORDER_1, 102, 5001)
        with pytest.raises(MinorSingleLimitError):
            await core.create_intent(CreateIntentCommand(102, ORDER_1, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))

        # 4 天各充并付成 50 元（月累计满 200 元）
        day_ms = 24 * 3600 * 1000
        # 从 2026-11-01 开始测试当月累计
        clock.ms = int(dt.datetime(2026, 11, 1, 4, 0, 0, tzinfo=dt.timezone.utc).timestamp() * 1000)
        for idx, oid in enumerate((ORDER_1, ORDER_2, ORDER_3, ORDER_4)):
            put_order(oid, 102, 5000)
            res = await core.create_intent(CreateIntentCommand(102, oid, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))
            assert res.intent is not None and res.intent.minor_age == 12
            if idx == 0:
                # 同一自然日再充 1 分触发日限
                put_order(ORDER_5, 102, 1)
                with pytest.raises(MinorDailyLimitError):
                    await core.create_intent(CreateIntentCommand(102, ORDER_5, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))
            # 当日付款落账（转 INTENT_PAID 并记录 paid_ms），次日释放日额度但计入当月累计
            await core.record_payment(
                devpay,
                ChannelPayment(
                    transaction_id=f"devpay-{out_trade_no(oid)}",
                    out_trade_no=out_trade_no(oid),
                    amount_minor=5000,
                    currency="CNY",
                    paid_ms=clock(),
                ),
            )
            clock.advance(day_ms)

        # 第 5 天当月已满 200 元，再充 1 分触发月限
        put_order(ORDER_6, 102, 1)
        with pytest.raises(MinorMonthlyLimitError):
            await core.create_intent(CreateIntentCommand(102, ORDER_6, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))

        # 3) 16-17 岁：单笔 >100 元拒绝；100 元通过
        oid_16_over = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0001"
        oid_16_ok = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0002"
        put_order(oid_16_over, 103, 10001)
        with pytest.raises(MinorSingleLimitError):
            await core.create_intent(CreateIntentCommand(103, oid_16_over, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))
        put_order(oid_16_ok, 103, 10000)
        ok16 = await core.create_intent(CreateIntentCommand(103, oid_16_ok, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))
        assert ok16.intent is not None and ok16.intent.minor_age == 17

        # 4) 成年人无限制
        oid_adult = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0003"
        put_order(oid_adult, 104, 50000)
        ok_adult = await core.create_intent(CreateIntentCommand(104, oid_adult, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))
        assert ok_adult.intent is not None and ok_adult.intent.minor_age == 0

        # 5) 未实名账号：App 渠道拒绝（RealNameRequiredError），小游戏平台管辖渠道放行
        oid_unverified_app = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0004"
        oid_unverified_mini = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0005"
        put_order(oid_unverified_app, 105, 600)
        with pytest.raises(RealNameRequiredError):
            await core.create_intent(CreateIntentCommand(105, oid_unverified_app, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))

        put_order(oid_unverified_mini, 105, 600)
        ok_mini = await core.create_intent(
            CreateIntentCommand(105, oid_unverified_mini, PAY_CHANNEL_DOUYIN_MINIGAME, PLATFORM_DOUYIN_MINIGAME)
        )
        assert ok_mini.intent is not None and ok_mini.params.douyin_minigame is not None

        # 6) 畸形 pi -> PaymentBusyError（fail-closed）
        oid_malformed = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0006"
        put_order(oid_malformed, 107, 600)
        with pytest.raises(PaymentBusyError):
            await core.create_intent(CreateIntentCommand(107, oid_malformed, PAY_CHANNEL_DEVPAY, PLATFORM_ANDROID_APP))

        assert verified_calls >= 12
    finally:
        await rn_client.close()
        await server.stop(1.0)
