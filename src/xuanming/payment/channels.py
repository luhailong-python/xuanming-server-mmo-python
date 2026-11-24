"""支付渠道策略协议（Strategy）与内置本地联调渠道 ``DevPayChannel``。

渠道协议按职责解耦为五个可组合 Protocol（与 Go ``internal/channel/channel.go`` 一致）：
- ``PaymentChannel``：所有渠道的基础能力（调起参数、查单、关单、退款、回调验签与应答格式化）；
- ``ReceiptChannel``：商店扣款渠道（Apple / Google）的回执核实与账号绑定校验；
- ``FinalizerChannel``：记账后的渠道收尾动作（如 Google Play consume）；
- ``HistoryPollerChannel``：按时间窗补拉历史事件；
- ``HandshakeChannel``：回调地址配置的 GET 握手校验（如微信/抖音小游戏 echostr）。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from collections.abc import Callable, Mapping
from typing import Protocol, runtime_checkable

from .config import MIN_SECRET_BYTES, PaymentConfig, load_env_pem, load_env_secret
from .errors import BadSignatureError
from .model import (
    AckKind,
    CHANNEL_ALIPAY,
    CHANNEL_DEVPAY,
    CHANNEL_WECHAT_PAY,
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
    PAY_CHANNEL_DEVPAY,
    TradeState,
)

DEVPAY_SIGNATURE_HEADER = "X-Devpay-Signature"
_DEVPAY_ALLOWED_FIELDS = frozenset(("out_trade_no", "amount_minor", "currency", "seq", "revoke"))

REFUND_UNSUPPORTED = ChannelRefundResult(state=ChannelRefundState.FAILED, detail="NO_REFUND_API")


@runtime_checkable
class PaymentChannel(Protocol):
    """支付渠道基础策略接口（并发安全，不碰存储、不碰商城）。"""

    @property
    def channel_id(self) -> int: ...

    @property
    def name(self) -> str: ...

    def supports(self, currency: str) -> bool: ...

    def needs_product(self) -> bool: ...

    async def prepare(self, order: ChannelOrder, cached_ref_value: str, cached_ref_ms: int) -> ChannelPrepared: ...

    async def query(self, req: ChannelQueryRequest) -> ChannelQueryResult: ...

    async def close_trade(self, out_trade_no: str) -> None: ...

    async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult: ...

    async def parse_notification(
        self,
        headers: Mapping[str, str],
        query: Mapping[str, str],
        body: bytes,
    ) -> ChannelNotification: ...

    def format_ack(self, kind: AckKind) -> tuple[int, bytes, str]:
        """返回 (HTTP status, body bytes, content_type)。"""
        ...


@runtime_checkable
class ReceiptChannel(Protocol):
    """商店渠道扩展协议（App Store / Google Play）：按交易号或令牌向商店核实。"""

    async def verify(self, receipt: ChannelReceipt) -> ChannelQueryResult: ...

    def account_ref(self, player_id: int) -> str: ...


@runtime_checkable
class FinalizerChannel(Protocol):
    """记账后的渠道侧收尾扩展协议（Google Play consume）。"""

    async def finalize(self, product_id: str, channel_token: str) -> None: ...


@runtime_checkable
class HistoryPollerChannel(Protocol):
    """按时间窗补拉遗漏事件扩展协议（Google 作废列表 / Apple 通知历史）。"""

    async def poll_history(self, since_ms: int) -> tuple[list[ChannelNotification], int]:
        """返回 (补拉到的通知列表, 新游标时间 ms)。"""
        ...


@runtime_checkable
class HandshakeChannel(Protocol):
    """回调地址配置的 GET 握手协议（wechat_minigame / douyin_minigame echostr）。"""

    def handshake(self, query: Mapping[str, str]) -> tuple[int, bytes, str] | None:
        """若处理了握手返回 (status, body, content_type)，否则返回 None。"""
        ...


def devpay_sign(secret: bytes | str, body: bytes) -> bytes:
    """计算 DevPay 回调原始正文的 HMAC-SHA256 原始摘要。"""
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    return hmac.new(key, body, hashlib.sha256).digest()


def sorted_sha1(*parts: str) -> str:
    """将多个字符串按字典序排序拼接后计算 SHA-1 小写十六进制（微信/抖音小游戏推送签名共用）。"""
    joined = "".join(sorted(parts)).encode("utf-8")
    return hashlib.sha1(joined).hexdigest()


def _header_value(headers: Mapping[str, str], target: str) -> str:
    lower = target.lower()
    for key, value in headers.items():
        if key.lower() == lower:
            return value
    return ""


def _strict_uint(value: object, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BadSignatureError("devpay: integer field invalid")
    return value


class DevPayChannel:
    """本地联调渠道：仅用于 mode=dev/test，靠 X-Devpay-Signature HMAC-SHA256 验签。"""

    def __init__(
        self,
        secret: bytes | str,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
        if len(secret_bytes) < MIN_SECRET_BYTES:
            raise ValueError(f"payment channel: devpay secret must be at least {MIN_SECRET_BYTES} bytes")
        self._secret = secret_bytes
        self._clock_ms = clock_ms

    @property
    def channel_id(self) -> int:
        return PAY_CHANNEL_DEVPAY

    @property
    def name(self) -> str:
        return CHANNEL_DEVPAY

    def supports(self, currency: str) -> bool:
        return bool(currency)

    def needs_product(self) -> bool:
        return False

    async def prepare(self, order: ChannelOrder, cached_ref_value: str, cached_ref_ms: int) -> ChannelPrepared:
        return ChannelPrepared(params=ClientParams(devpay={"out_trade_no": order.out_trade_no}))

    async def query(self, req: ChannelQueryRequest) -> ChannelQueryResult:
        return ChannelQueryResult(state=TradeState.NOT_PAID)

    async def close_trade(self, out_trade_no: str) -> None:
        return None

    async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult:
        return ChannelRefundResult(
            state=ChannelRefundState.SUCCEEDED,
            refund_id="devpay-" + req.out_refund_no,
            at_ms=self._clock_ms(),
        )

    async def parse_notification(
        self,
        headers: Mapping[str, str],
        query: Mapping[str, str],
        body: bytes,
    ) -> ChannelNotification:
        sig_hex = _header_value(headers, DEVPAY_SIGNATURE_HEADER)
        try:
            got_sig = bytes.fromhex(sig_hex)
        except ValueError as exc:
            raise BadSignatureError("devpay: signature is not valid hex") from exc
        expected = devpay_sign(self._secret, body)
        if not hmac.compare_digest(got_sig, expected):
            raise BadSignatureError("devpay: signature mismatch")

        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadSignatureError("devpay: invalid JSON body") from exc
        if not isinstance(payload, dict):
            raise BadSignatureError("devpay: body must be a JSON object")
        unknown = set(payload) - _DEVPAY_ALLOWED_FIELDS
        if unknown:
            raise BadSignatureError(f"devpay: unknown fields {sorted(unknown)}")

        trade_no = payload.get("out_trade_no", "")
        currency = payload.get("currency", "")
        revoke = payload.get("revoke", False)
        if not isinstance(trade_no, str) or not trade_no:
            raise BadSignatureError("devpay: out_trade_no is required")
        if not isinstance(currency, str):
            raise BadSignatureError("devpay: currency must be a string")
        if not isinstance(revoke, bool):
            raise BadSignatureError("devpay: revoke must be a boolean")
        amount_minor = _strict_uint(payload.get("amount_minor"), 0)
        seq = _strict_uint(payload.get("seq"), 0)
        if seq > 0xFFFFFFFF:
            raise BadSignatureError("devpay: seq out of uint32 range")
        if not revoke and (amount_minor == 0 or not currency):
            raise BadSignatureError("devpay: amount_minor and currency are required")

        tx_id = f"devpay-{trade_no}" if seq == 0 else f"devpay-{trade_no}-{seq}"
        now_ms = self._clock_ms()
        if revoke:
            return ChannelNotification(
                revocations=(
                    ChannelRevocation(
                        transaction_id=tx_id,
                        revoked=True,
                        observed_ms=now_ms,
                        out_trade_no=trade_no,
                        detail="devpay",
                    ),
                )
            )
        return ChannelNotification(
            payment=ChannelPayment(
                transaction_id=tx_id,
                out_trade_no=trade_no,
                amount_minor=amount_minor,
                currency=currency,
                paid_ms=now_ms,
            )
        )

    def format_ack(self, kind: AckKind) -> tuple[int, bytes, str]:
        if kind == AckKind.OK:
            return 200, b"ok", "text/plain; charset=utf-8"
        if kind == AckKind.REJECTED:
            return 401, b"", "text/plain; charset=utf-8"
        return 503, b"", "text/plain; charset=utf-8"


def build_channels(
    config: PaymentConfig,
    *,
    environ: Mapping[str, str] | None = None,
    clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    session=None,
    transport=None,
) -> list[PaymentChannel]:
    """按配置工厂装配全部启用的渠道（DevPay 与 6 个生产渠道）；密钥一律从环境变量 fail-closed 读取。"""
    from .channel_crypto import (
        parse_ec_private_key,
        parse_rsa_private_key,
        parse_rsa_public_key,
        parse_x509_certificates,
    )
    from .channels_cn import AlipayChannel, AlipayConfig, WechatPayChannel, WechatPayConfig
    from .channels_minigame import (
        DouyinMinigameChannel,
        DouyinMinigameConfig,
        WechatMinigameChannel,
        WechatMinigameConfig,
    )
    from .channels_store import AppleChannel, AppleConfig, GoogleChannel, GoogleConfig

    env = os.environ if environ is None else environ
    notify_base = config.notify_base_url.rstrip("/")

    def notify_url(ch_name: str) -> str:
        return f"{notify_base}/notify/{ch_name}"

    channels: list[PaymentChannel] = []

    if config.devpay_enabled:
        secret = config.devpay_secret or load_env_secret(env, "devpay_secret_env", config.devpay_secret_env, MIN_SECRET_BYTES)
        channels.append(DevPayChannel(secret, clock_ms=clock_ms))

    if config.wechat_pay_enabled:
        priv_pem = load_env_pem(env, "wechat_pay_private_key_env", config.wechat_pay_private_key_env)
        pub_pem = load_env_pem(env, "wechat_pay_platform_key_env", config.wechat_pay_platform_key_env)
        apiv3 = load_env_secret(env, "wechat_pay_apiv3_key_env", config.wechat_pay_apiv3_key_env, 32)
        channels.append(
            WechatPayChannel(
                WechatPayConfig(
                    app_id=config.wechat_pay_app_id,
                    mch_id=config.wechat_pay_mch_id,
                    merchant_serial_no=config.wechat_pay_merchant_serial_no,
                    merchant_key=parse_rsa_private_key(priv_pem),
                    api_v3_key=apiv3,
                    platform_key_id=config.wechat_pay_platform_key_id,
                    platform_key=parse_rsa_public_key(pub_pem),
                    notify_url=notify_url(CHANNEL_WECHAT_PAY),
                    description=config.wechat_pay_description,
                    endpoint=config.wechat_pay_endpoint or "https://api.mch.weixin.qq.com",
                ),
                clock_ms=clock_ms,
                session=session,
                transport=transport,
            )
        )

    if config.alipay_enabled:
        priv_pem = load_env_pem(env, "alipay_private_key_env", config.alipay_private_key_env)
        pub_pem = load_env_pem(env, "alipay_public_key_env", config.alipay_public_key_env)
        channels.append(
            AlipayChannel(
                AlipayConfig(
                    app_id=config.alipay_app_id,
                    app_key=parse_rsa_private_key(priv_pem),
                    alipay_key=parse_rsa_public_key(pub_pem),
                    notify_url=notify_url(CHANNEL_ALIPAY),
                    seller_id=config.alipay_seller_id,
                    gateway=config.alipay_gateway or "https://openapi.alipay.com/gateway.do",
                    subject=config.alipay_subject,
                ),
                clock_ms=clock_ms,
                session=session,
                transport=transport,
            )
        )

    if config.wechat_minigame_enabled:
        app_secret = load_env_secret(
            env, "wechat_minigame_app_secret_env", config.wechat_minigame_app_secret_env, 16
        ).decode("utf-8")
        app_key = load_env_secret(
            env, "wechat_minigame_app_key_env", config.wechat_minigame_app_key_env, 16
        ).decode("utf-8")
        push_token = load_env_secret(
            env, "wechat_minigame_push_token_env", config.wechat_minigame_push_token_env, 3
        ).decode("utf-8")
        aes_key = ""
        if config.wechat_minigame_encoding_aes_key_env:
            aes_key = load_env_secret(
                env, "wechat_minigame_encoding_aes_key_env", config.wechat_minigame_encoding_aes_key_env, 43
            ).decode("utf-8")
        channels.append(
            WechatMinigameChannel(
                WechatMinigameConfig(
                    app_id=config.wechat_minigame_app_id,
                    app_secret=app_secret,
                    offer_id=config.wechat_minigame_offer_id,
                    app_key=app_key,
                    push_token=push_token,
                    env=config.wechat_minigame_env,
                    zone_id=config.wechat_minigame_zone_id,
                    encoding_aes_key=aes_key,
                    api_base=config.wechat_minigame_api_base or "https://api.weixin.qq.com",
                ),
                clock_ms=clock_ms,
                session=session,
                transport=transport,
            )
        )

    if config.douyin_minigame_enabled:
        push_token = load_env_secret(
            env, "douyin_minigame_push_token_env", config.douyin_minigame_push_token_env, 3
        ).decode("utf-8")
        channels.append(
            DouyinMinigameChannel(
                DouyinMinigameConfig(
                    app_id=config.douyin_minigame_app_id,
                    push_token=push_token,
                    zone_id=config.douyin_minigame_zone_id,
                    good_name=config.douyin_minigame_good_name,
                )
            )
        )

    if config.apple_enabled:
        priv_pem = load_env_pem(env, "apple_private_key_env", config.apple_private_key_env)
        roots_pem = load_env_pem(env, "apple_root_certificates_env", config.apple_root_certificates_env)
        channels.append(
            AppleChannel(
                AppleConfig(
                    bundle_id=config.apple_bundle_id,
                    app_apple_id=config.apple_app_apple_id,
                    issuer_id=config.apple_issuer_id,
                    key_id=config.apple_key_id,
                    private_key=parse_ec_private_key(priv_pem),
                    root_certificates=parse_x509_certificates(roots_pem),
                    allow_sandbox=config.apple_allow_sandbox,
                    production_api=config.apple_production_api or "https://api.storekit.apple.com",
                    sandbox_api=config.apple_sandbox_api or "https://api.storekit-sandbox.apple.com",
                ),
                clock_ms=clock_ms,
                session=session,
                transport=transport,
            )
        )

    if config.google_enabled:
        sa_raw = load_env_secret(env, "google_service_account_env", config.google_service_account_env, 64)
        try:
            sa = json.loads(sa_raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"payment config: {config.google_service_account_env} does not hold a Google service account JSON key"
            ) from exc
        if not isinstance(sa, dict) or not sa.get("client_email") or not sa.get("private_key"):
            raise ValueError(
                f"payment config: {config.google_service_account_env} does not hold a Google service account JSON key"
            )
        channels.append(
            GoogleChannel(
                GoogleConfig(
                    package_name=config.google_package_name,
                    client_email=str(sa["client_email"]),
                    private_key=parse_rsa_private_key(str(sa["private_key"])),
                    push_audience=config.google_push_audience,
                    push_service_account=config.google_push_service_account,
                    api_base=config.google_api_base or "https://androidpublisher.googleapis.com",
                    token_url=config.google_token_url or "https://oauth2.googleapis.com/token",
                    certs_url=config.google_certs_url or "https://www.googleapis.com/oauth2/v3/certs",
                ),
                clock_ms=clock_ms,
                session=session,
                transport=transport,
            )
        )

    return channels
