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
import time
from collections.abc import Callable, Mapping
from typing import Protocol, runtime_checkable

from .config import MIN_SECRET_BYTES, PaymentConfig
from .errors import BadSignatureError
from .model import (
    AckKind,
    CHANNEL_DEVPAY,
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
    clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
) -> list[PaymentChannel]:
    """按配置工厂装配启用的渠道列表（生产渠道可按 PaymentChannel 协议扩展接入）。"""
    channels: list[PaymentChannel] = []
    if config.devpay_enabled:
        channels.append(DevPayChannel(config.devpay_secret, clock_ms=clock_ms))
    return channels
