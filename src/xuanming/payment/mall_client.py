"""商城（java/mall_game）HTTP v1 HMAC 客户端。

签名算法与 Go ``go/payment/internal/mall/client.go``、``go/trade`` 及 Java ``HmacAuthentication`` 逐字一致：
``HMAC-SHA256(secret, "v1\\n{role}\\n{subject}\\n{ts}\\n{nonce}\\n{METHOD}\\n{path}\\n{sha256_hex(body)}")``。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from collections.abc import Callable
from typing import Any, Protocol

import aiohttp

from .config import MIN_SECRET_BYTES, validate_base_url
from .errors import (
    MallConflictError,
    MallError,
    MallInvalidError,
    MallNotFoundError,
    MallStatusError,
)
from .model import (
    MallOrder,
    MallOutcome,
    MallPayment,
    MallPendingRefund,
    MallPendingRefundPage,
    MallRefund,
    MallRefundClosure,
    MallRevision,
)

MAX_RESPONSE_BYTES = 256 * 1024
MALL_ROLE_PAYMENT = "payment"

PATH_ORDER_VIEW = "/internal/payments/order-view"
PATH_RECORD_PAYMENT = "/internal/payments"
PATH_PUSH_REVISION = "/internal/payments/revision"
PATH_POLL_REFUNDS = "/internal/refunds/poll"
PATH_REPORT_REFUND = "/internal/refunds/receipt"
PATH_CLOSE_REFUND = "/internal/refunds/close"


def mall_signature(
    secret: bytes | str,
    role: str,
    subject: str,
    timestamp: str,
    nonce: str,
    method: str,
    path: str,
    body: bytes,
) -> str:
    """计算商城 v1 HMAC-SHA256 十六进制小写签名。"""
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    body_digest = hashlib.sha256(body).hexdigest()
    canonical = "\n".join((
        "v1",
        role,
        subject,
        timestamp,
        nonce,
        method,
        path,
        body_digest,
    )).encode("utf-8")
    return hmac.new(key, canonical, hashlib.sha256).hexdigest()


class MallClient(Protocol):
    """支付服务对商城的全部内部接口依赖。"""

    async def order_view(self, order_id: str) -> MallOrder: ...
    async def record_payment(self, payment: MallPayment) -> MallOutcome: ...
    async def push_revision(self, revision: MallRevision) -> int: ...
    async def pending_refunds(self, cursor: str = "") -> MallPendingRefundPage: ...
    async def report_refund(self, refund: MallRefund) -> None: ...
    async def close_refund(self, closure: MallRefundClosure) -> None: ...
    async def close(self) -> None: ...


class HttpMallClient:
    """基于 aiohttp 的商城内部接口客户端（每次调用生成全新 16 字节随机 nonce，不跟随重定向）。"""

    def __init__(
        self,
        base_url: str,
        subject: str,
        secret: bytes | str,
        *,
        timeout_sec: float = 2.5,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        validate_base_url("mall_base_url", base_url, allow_http=True, allow_path=False)
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
        if len(secret_bytes) < MIN_SECRET_BYTES:
            raise ValueError(f"mall: payment secret must be at least {MIN_SECRET_BYTES} bytes")
        if not subject:
            raise ValueError("mall: payment subject is empty")
        self._base = base_url.rstrip("/")
        self._subject = subject
        self._secret = secret_bytes
        self._timeout_sec = timeout_sec if timeout_sec > 0 else 2.5
        self._clock_ms = clock_ms
        self._external_session = session is not None
        self._session = session

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=64, limit_per_host=32, keepalive_timeout=90.0)
            self._session = aiohttp.ClientSession(connector=connector)
            self._external_session = False
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._external_session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _call(self, path: str, payload: dict[str, Any] | None, *, expect_json: bool = True) -> Any:
        body = b""
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        # 每次调用必须换新 16 字节随机 nonce，商城单次消费防重放
        nonce = secrets.token_hex(16)
        stamp = str(self._clock_ms())
        signature = mall_signature(
            self._secret,
            MALL_ROLE_PAYMENT,
            self._subject,
            stamp,
            nonce,
            "POST",
            path,
            body,
        )
        headers = {
            "Content-Type": "application/json",
            "X-Commerce-Subject": self._subject,
            "X-Commerce-Timestamp": stamp,
            "X-Commerce-Nonce": nonce,
            "X-Commerce-Signature": signature,
        }
        session = self._get_session()
        timeout = aiohttp.ClientTimeout(total=self._timeout_sec)
        async with session.post(
            self._base + path,
            data=body,
            headers=headers,
            allow_redirects=False,
            timeout=timeout,
        ) as resp:
            raw = await resp.content.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                raise MallError("mall: answer is too large")
            if resp.status == 400:
                raise MallInvalidError("mall: request rejected as invalid (400)")
            if resp.status == 404:
                raise MallNotFoundError("mall: order not found (404)")
            if resp.status == 409:
                raise MallConflictError("mall: request conflicts with what the mall has recorded (409)")
            if resp.status != 200:
                raise MallStatusError(resp.status)
            if not expect_json:
                return None
            try:
                return json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MallError(f"mall: answer is not readable: {exc}") from exc

    async def order_view(self, order_id: str) -> MallOrder:
        data = await self._call(PATH_ORDER_VIEW, {"orderId": order_id})
        if not isinstance(data, dict) or data.get("orderId") != order_id:
            raise MallError("mall: order view answers another order")
        return MallOrder(
            order_id=str(data["orderId"]),
            buyer_id=str(data.get("buyerId", "")),
            sku_id=int(data.get("skuId", 0)),
            quantity=int(data.get("quantity", 0)),
            amount_minor=int(data.get("amountMinor", 0)),
            currency=str(data.get("currency", "")),
            state=str(data.get("state", "")),
            payment_deadline_ms=int(data.get("paymentDeadlineMs", 0)),
        )

    async def record_payment(self, payment: MallPayment) -> MallOutcome:
        data = await self._call(
            PATH_RECORD_PAYMENT,
            {
                "orderId": payment.order_id,
                "channel": payment.channel,
                "transactionId": payment.transaction_id,
                "amountMinor": payment.amount_minor,
                "currency": payment.currency,
            },
        )
        if not isinstance(data, dict):
            raise MallError("mall: payment answer is not an object")
        order = data.get("order")
        if not isinstance(order, dict) or order.get("orderId") != payment.order_id or not order.get("state"):
            raise MallError("mall: payment answer lacks the order it was reported for")
        refund_reason = data.get("refundReason")
        if refund_reason is not None:
            if not isinstance(refund_reason, str) or not refund_reason:
                raise MallError("mall: payment answer has an empty refund reason")
        return MallOutcome(
            order_state=str(order["state"]),
            refund_reason=refund_reason or "",
        )

    async def push_revision(self, revision: MallRevision) -> int:
        data = await self._call(
            PATH_PUSH_REVISION,
            {
                "orderId": revision.order_id,
                "channel": revision.channel,
                "transactionId": revision.transaction_id,
                "revision": revision.revision,
                "fact": revision.fact,
            },
        )
        if not isinstance(data, dict) or data.get("revision") is None or isinstance(data.get("revision"), bool):
            raise MallError("mall: revision answer has no revision")
        return int(data["revision"])

    async def pending_refunds(self, cursor: str = "") -> MallPendingRefundPage:
        payload = {"cursor": cursor} if cursor else None
        data = await self._call(PATH_POLL_REFUNDS, payload)
        if not isinstance(data, dict):
            raise MallError("mall: pending refunds answer is not an object")
        raw_entries = data.get("entries") or []
        entries = tuple(
            MallPendingRefund(
                order_id=str(item.get("orderId", "")),
                channel=str(item.get("channel", "")),
                transaction_id=str(item.get("transactionId", "")),
                amount_minor=int(item.get("amountMinor", 0)),
                currency=str(item.get("currency", "")),
                reason=str(item.get("reason", "")),
            )
            for item in raw_entries
        )
        return MallPendingRefundPage(
            entries=entries,
            next_cursor=str(data.get("nextCursor") or ""),
        )

    async def report_refund(self, refund: MallRefund) -> None:
        await self._call(
            PATH_REPORT_REFUND,
            {
                "orderId": refund.order_id,
                "channel": refund.channel,
                "paymentTransactionId": refund.payment_transaction_id,
                "transactionId": refund.transaction_id,
                "amountMinor": refund.amount_minor,
                "currency": refund.currency,
            },
            expect_json=False,
        )

    async def close_refund(self, closure: MallRefundClosure) -> None:
        await self._call(
            PATH_CLOSE_REFUND,
            {
                "orderId": closure.order_id,
                "channel": closure.channel,
                "paymentTransactionId": closure.payment_transaction_id,
                "reason": closure.reason,
            },
            expect_json=False,
        )
