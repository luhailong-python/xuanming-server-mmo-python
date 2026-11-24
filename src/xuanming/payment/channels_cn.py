"""国内 App 直连支付渠道适配：微信支付 APIv3（``WechatPayChannel``）与支付宝 RSA2（``AlipayChannel``）。

协议规则与 Go ``go/payment/internal/channel/wechatpay.go``、``alipay.go`` 逐项对齐。
"""

from __future__ import annotations

import base64
import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import parse_qsl, quote, quote_plus, urlencode

import aiohttp
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .channel_crypto import (
    ChannelHttpClient,
    HttpTransport,
    REFUND_ID_RE,
    all_digits,
    format_cst_datetime,
    format_rfc3339_cst,
    format_yuan,
    header_value,
    parse_cst_datetime_ms,
    parse_rfc3339_ms,
    parse_yuan,
    random_nonce_hex,
    rsa_sign_sha256_b64,
    rsa_verify_sha256_b64,
)
from .errors import BadSignatureError, ChannelCallError, ChannelError, OrderPaidError
from .model import (
    AckKind,
    CHANNEL_ALIPAY,
    CHANNEL_WECHAT_PAY,
    ChannelNotification,
    ChannelOrder,
    ChannelPayment,
    ChannelPrepared,
    ChannelQueryRequest,
    ChannelQueryResult,
    ChannelRef,
    ChannelRefundRequest,
    ChannelRefundResult,
    ChannelRefundState,
    ChannelRevocation,
    ClientParams,
    PAY_CHANNEL_ALIPAY,
    PAY_CHANNEL_WECHAT_PAY,
    TradeState,
)

# ---------------------------------------------------------------------------
# 微信支付 APIv3（直连商户 APP 支付，微信支付公钥验签模式）
# ---------------------------------------------------------------------------

WECHAT_DEFAULT_ENDPOINT = "https://api.mch.weixin.qq.com"
WECHAT_PREPAY_REUSE_MS = 110 * 60 * 1000  # prepay_id 2 小时有效，预留 10 分钟余量
WECHAT_CLOCK_SKEW_MS = 5 * 60 * 1000  # 应答与回调时间戳允许偏差 ±5 分钟
_WECHAT_PUBKEY_ID_RE = re.compile(r"^PUB_KEY_ID_[0-9A-Za-z_]{1,64}$")
_WECHAT_ERR_CODE_RE = re.compile(r"^[A-Z0-9_]{1,64}$")


@dataclass(frozen=True, slots=True)
class WechatPayConfig:
    """微信支付 APIv3 渠道配置。"""

    app_id: str
    mch_id: str
    merchant_serial_no: str
    merchant_key: rsa.RSAPrivateKey
    api_v3_key: bytes
    platform_key_id: str
    platform_key: rsa.RSAPublicKey
    notify_url: str
    description: str = "游戏商品"
    endpoint: str = WECHAT_DEFAULT_ENDPOINT


def wechat_request_message(method: str, path_and_query: str, timestamp: str, nonce: str, body: bytes) -> bytes:
    """构造微信支付 APIv3 请求签名串。"""
    prefix = f"{method}\n{path_and_query}\n{timestamp}\n{nonce}\n".encode("utf-8")
    return prefix + body + b"\n"


def wechat_answer_message(timestamp: str, nonce: str, body: bytes) -> bytes:
    """构造微信支付 APIv3 应答与回调验签串。"""
    prefix = f"{timestamp}\n{nonce}\n".encode("utf-8")
    return prefix + body + b"\n"


def wechat_app_sign_message(app_id: str, timestamp: str, nonce: str, prepay_id: str) -> bytes:
    """构造微信 APP 调起支付签名串。"""
    return f"{app_id}\n{timestamp}\n{nonce}\n{prepay_id}\n".encode("utf-8")


def wechat_decrypt_resource(api_v3_key: bytes, nonce: str, associated_data: str, ciphertext_b64: str) -> bytes:
    """按 AEAD_AES_256_GCM 解密微信回调 ``resource.ciphertext``。"""
    try:
        sealed = base64.b64decode(ciphertext_b64, validate=True)
    except ValueError as exc:
        raise ValueError("payment channel: wechat_pay ciphertext is not valid base64") from exc
    nonce_bytes = nonce.encode("utf-8")
    if len(nonce_bytes) != 12:
        raise ValueError("payment channel: wechat_pay resource nonce has the wrong length")
    aad_bytes = associated_data.encode("utf-8")
    return AESGCM(api_v3_key).decrypt(nonce_bytes, sealed, aad_bytes)


def _wechat_error_code(raw: bytes) -> str:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ""
    if not isinstance(data, dict):
        return ""
    code = data.get("code", "")
    if isinstance(code, str) and _WECHAT_ERR_CODE_RE.fullmatch(code):
        return code
    return ""


class WechatPayChannel:
    """微信支付 APIv3 直连商户 APP 支付渠道适配器。"""

    def __init__(
        self,
        config: WechatPayConfig,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        session: aiohttp.ClientSession | None = None,
        transport: HttpTransport | None = None,
    ) -> None:
        if not config.app_id or not config.mch_id or not config.merchant_serial_no:
            raise ValueError("payment channel: wechat_pay needs app_id, mch_id and merchant_serial_no")
        if config.merchant_key is None or config.platform_key is None:
            raise ValueError("payment channel: wechat_pay needs merchant_key and platform_key")
        if len(config.api_v3_key) != 32:
            raise ValueError("payment channel: wechat_pay api_v3_key must be exactly 32 bytes")
        if not _WECHAT_PUBKEY_ID_RE.fullmatch(config.platform_key_id):
            raise ValueError(
                "payment channel: wechat_pay platform_key_id must be a PUB_KEY_ID_... value"
            )
        if not config.notify_url or "?" in config.notify_url or "#" in config.notify_url:
            raise ValueError("payment channel: wechat_pay notify_url is empty or carries a query string")
        if not config.description or len(config.description) > 127:
            raise ValueError("payment channel: wechat_pay description must be 1-127 characters")

        self._cfg = config
        self._endpoint = (config.endpoint or WECHAT_DEFAULT_ENDPOINT).rstrip("/")
        self._clock_ms = clock_ms
        self._http = ChannelHttpClient(session=session, transport=transport)

    async def close(self) -> None:
        await self._http.close()

    @property
    def channel_id(self) -> int:
        return PAY_CHANNEL_WECHAT_PAY

    @property
    def name(self) -> str:
        return CHANNEL_WECHAT_PAY

    def supports(self, currency: str) -> bool:
        return currency == "CNY"

    def needs_product(self) -> bool:
        return False

    async def prepare(self, order: ChannelOrder, cached_ref_value: str, cached_ref_ms: int) -> ChannelPrepared:
        if not self.supports(order.currency):
            raise ChannelError(f"payment channel: wechat_pay does not take currency {order.currency}")
        now_ms = self._clock_ms()
        prepay_id = cached_ref_value
        fresh_ref = ChannelRef()
        if (
            not prepay_id
            or cached_ref_ms <= 0
            or now_ms < cached_ref_ms
            or (now_ms - cached_ref_ms) >= WECHAT_PREPAY_REUSE_MS
        ):
            prepay_id = await self._prepay(order)
            fresh_ref = ChannelRef(value=prepay_id, at_ms=now_ms)
        return ChannelPrepared(ref=fresh_ref, params=ClientParams(wechat_app=self._app_params(prepay_id, now_ms)))

    async def _prepay(self, order: ChannelOrder) -> str:
        payload = {
            "appid": self._cfg.app_id,
            "mchid": self._cfg.mch_id,
            "description": self._cfg.description,
            "out_trade_no": order.out_trade_no,
            "time_expire": format_rfc3339_cst(order.expire_ms),
            "notify_url": self._cfg.notify_url,
            "amount": {"total": order.amount_minor, "currency": "CNY"},
        }
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        status, raw = await self._do("POST", "/v3/pay/transactions/app", body)
        if status == 200:
            try:
                out = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ChannelError("payment channel: wechat_pay prepay answer is not valid JSON") from exc
            prepay_id = out.get("prepay_id", "") if isinstance(out, dict) else ""
            if not isinstance(prepay_id, str) or not prepay_id or len(prepay_id) > 64:
                raise ChannelError("payment channel: wechat_pay prepay answer carries no usable prepay_id")
            return prepay_id
        code = _wechat_error_code(raw)
        if code == "ORDERPAID":
            raise OrderPaidError("payment channel: wechat_pay order is already paid")
        raise ChannelCallError(CHANNEL_WECHAT_PAY, "prepay", status, code)

    def _app_params(self, prepay_id: str, now_ms: int) -> dict[str, str]:
        nonce = random_nonce_hex()
        timestamp = str(now_ms // 1000)
        sign = rsa_sign_sha256_b64(
            self._cfg.merchant_key,
            wechat_app_sign_message(self._cfg.app_id, timestamp, nonce, prepay_id),
        )
        return {
            "app_id": self._cfg.app_id,
            "partner_id": self._cfg.mch_id,
            "prepay_id": prepay_id,
            "package_value": "Sign=WXPay",
            "nonce_str": nonce,
            "timestamp": timestamp,
            "sign": sign,
        }

    async def _do(self, method: str, path_and_query: str, body: bytes | None) -> tuple[int, bytes]:
        nonce = random_nonce_hex()
        timestamp = str(self._clock_ms() // 1000)
        raw_body = body if body is not None else b""
        signature = rsa_sign_sha256_b64(
            self._cfg.merchant_key,
            wechat_request_message(method, path_and_query, timestamp, nonce, raw_body),
        )
        headers = {
            "Authorization": (
                f'WECHATPAY2-SHA256-RSA2048 mchid="{self._cfg.mch_id}",'
                f'nonce_str="{nonce}",signature="{signature}",'
                f'timestamp="{timestamp}",serial_no="{self._cfg.merchant_serial_no}"'
            ),
            "Accept": "application/json",
            "User-Agent": "mmorpg-payment/1",
            "Wechatpay-Serial": self._cfg.platform_key_id,
        }
        if body is not None:
            headers["Content-Type"] = "application/json"

        resp = await self._http.request(method, self._endpoint + path_and_query, headers=headers, body=body)
        # 带正文的 2xx 应答一律核验微信支付公钥签名，不只依赖 TLS
        if 200 <= resp.status < 300 and resp.body:
            self._verify_headers(resp.headers, resp.body)
        return resp.status, resp.body

    def _verify_headers(self, headers: Mapping[str, str], body: bytes) -> None:
        if header_value(headers, "Wechatpay-Serial") != self._cfg.platform_key_id:
            raise ChannelError("payment channel: wechat_pay message is signed with an unknown key id")
        sig_type = header_value(headers, "Wechatpay-Signature-Type")
        if sig_type and sig_type != "WECHATPAY2-SHA256-RSA2048":
            raise ChannelError("payment channel: wechat_pay message uses an unsupported signature type")
        timestamp = header_value(headers, "Wechatpay-Timestamp")
        nonce = header_value(headers, "Wechatpay-Nonce")
        signature = header_value(headers, "Wechatpay-Signature")
        if not timestamp or not nonce or not signature:
            raise ChannelError("payment channel: wechat_pay message lacks signature headers")
        try:
            seconds = int(timestamp)
        except ValueError as exc:
            raise ChannelError("payment channel: wechat_pay timestamp is not an integer") from exc
        skew_ms = self._clock_ms() - seconds * 1000
        if skew_ms > WECHAT_CLOCK_SKEW_MS or skew_ms < -WECHAT_CLOCK_SKEW_MS:
            raise ChannelError("payment channel: wechat_pay message timestamp is outside the allowed skew")
        try:
            rsa_verify_sha256_b64(
                self._cfg.platform_key,
                wechat_answer_message(timestamp, nonce, body),
                signature,
            )
        except ValueError as exc:
            raise ChannelError("payment channel: wechat_pay message signature rejected") from exc

    def _build_payment(self, txn: dict) -> ChannelPayment:
        tx_id = txn.get("transaction_id", "")
        out_trade_no = txn.get("out_trade_no", "")
        amount = txn.get("amount") or {}
        total = amount.get("total", 0) if isinstance(amount, dict) else 0
        currency = amount.get("currency", "") if isinstance(amount, dict) else ""
        if (
            not isinstance(tx_id, str)
            or not tx_id
            or not isinstance(out_trade_no, str)
            or isinstance(total, bool)
            or not isinstance(total, int)
            or total <= 0
            or currency != "CNY"
        ):
            raise ChannelError("payment channel: wechat_pay transaction lacks transaction_id, amount or currency")
        paid_ms = self._clock_ms()
        success_time = txn.get("success_time", "")
        if success_time:
            if not isinstance(success_time, str):
                raise ChannelError("payment channel: wechat_pay success_time is not a string")
            try:
                paid_ms = parse_rfc3339_ms(success_time)
            except ValueError as exc:
                raise ChannelError("payment channel: wechat_pay success_time is not RFC3339") from exc
        return ChannelPayment(
            transaction_id=tx_id,
            out_trade_no=out_trade_no,
            amount_minor=total,
            currency="CNY",
            paid_ms=paid_ms,
        )

    async def query(self, req: ChannelQueryRequest) -> ChannelQueryResult:
        path = (
            "/v3/pay/transactions/out-trade-no/"
            + quote(req.out_trade_no, safe="")
            + "?mchid="
            + quote_plus(self._cfg.mch_id)
        )
        status, raw = await self._do("GET", path, None)
        if status == 404 and _wechat_error_code(raw) == "ORDER_NOT_EXIST":
            return ChannelQueryResult(state=TradeState.NOT_PAID)
        if status != 200:
            raise ChannelCallError(CHANNEL_WECHAT_PAY, "query", status, _wechat_error_code(raw))
        try:
            txn = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelError("payment channel: wechat_pay query answer is not a transaction") from exc
        if (
            not isinstance(txn, dict)
            or txn.get("appid") != self._cfg.app_id
            or txn.get("mchid") != self._cfg.mch_id
            or txn.get("out_trade_no") != req.out_trade_no
        ):
            raise ChannelError("payment channel: wechat_pay query answer belongs to another app, merchant or order")

        state = txn.get("trade_state", "")
        if state == "SUCCESS":
            return ChannelQueryResult(state=TradeState.PAID, payment=self._build_payment(txn))
        if state == "REFUND":
            return ChannelQueryResult(state=TradeState.REFUNDED_OUTSIDE, payment=self._build_payment(txn))
        if state in ("NOTPAY", "USERPAYING"):
            return ChannelQueryResult(state=TradeState.NOT_PAID)
        if state in ("CLOSED", "REVOKED", "PAYERROR"):
            return ChannelQueryResult(state=TradeState.CLOSED)
        raise ChannelError("payment channel: wechat_pay query answer has an unknown trade_state")

    async def close_trade(self, out_trade_no: str) -> None:
        body = json.dumps({"mchid": self._cfg.mch_id}, separators=(",", ":")).encode("utf-8")
        path = "/v3/pay/transactions/out-trade-no/" + quote(out_trade_no, safe="") + "/close"
        status, raw = await self._do("POST", path, body)
        code = _wechat_error_code(raw)
        if status in (200, 204) or code in ("ORDER_NOT_EXIST", "ORDER_CLOSED", "ORDERPAID"):
            return None
        raise ChannelCallError(CHANNEL_WECHAT_PAY, "close", status, code)

    async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult:
        query_path = "/v3/refund/domestic/refunds/" + quote(req.out_refund_no, safe="")
        status, raw = await self._do("GET", query_path, None)
        if status == 200:
            return self._parse_refund_result(raw, req)
        if not (status == 404 and _wechat_error_code(raw) == "RESOURCE_NOT_EXISTS"):
            raise ChannelCallError(CHANNEL_WECHAT_PAY, "refund_query", status, _wechat_error_code(raw))

        if req.currency != "CNY":
            return ChannelRefundResult(state=ChannelRefundState.FAILED, detail="CURRENCY")

        payload: dict[str, object] = {
            "transaction_id": req.transaction_id,
            "out_refund_no": req.out_refund_no,
            "amount": {"refund": req.amount_minor, "total": req.amount_minor, "currency": "CNY"},
        }
        if req.reason:
            payload["reason"] = req.reason
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        status, raw = await self._do("POST", "/v3/refund/domestic/refunds", body)
        if status == 200:
            return self._parse_refund_result(raw, req)
        code = _wechat_error_code(raw)
        if status == 429 or status >= 500 or status == 401:
            raise ChannelCallError(CHANNEL_WECHAT_PAY, "refund", status, code)
        if code == "NOT_ENOUGH":
            return ChannelRefundResult(state=ChannelRefundState.PROCESSING, detail=code)
        return ChannelRefundResult(state=ChannelRefundState.FAILED, detail=code)

    def _parse_refund_result(self, raw: bytes, req: ChannelRefundRequest) -> ChannelRefundResult:
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelError("payment channel: wechat_pay refund answer is not valid JSON") from exc
        if not isinstance(data, dict) or data.get("out_refund_no") != req.out_refund_no:
            raise ChannelError("payment channel: wechat_pay refund answer belongs to another refund")
        r_status = data.get("status", "")
        if r_status == "SUCCESS":
            refund_id = data.get("refund_id", "")
            amount = data.get("amount") or {}
            refund_amt = amount.get("refund") if isinstance(amount, dict) else None
            if (
                not isinstance(refund_id, str)
                or not REFUND_ID_RE.fullmatch(refund_id)
                or isinstance(refund_amt, bool)
                or refund_amt != req.amount_minor
            ):
                raise ChannelError(
                    "payment channel: wechat_pay refund answer has an unusable refund_id or different amount"
                )
            at_ms = 0
            success_time = data.get("success_time", "")
            if success_time:
                try:
                    at_ms = parse_rfc3339_ms(str(success_time))
                except ValueError as exc:
                    raise ChannelError("payment channel: wechat_pay refund success_time is not RFC3339") from exc
            return ChannelRefundResult(state=ChannelRefundState.SUCCEEDED, refund_id=refund_id, at_ms=at_ms)
        if r_status == "PROCESSING":
            return ChannelRefundResult(state=ChannelRefundState.PROCESSING, detail="PROCESSING")
        if r_status in ("ABNORMAL", "CLOSED"):
            return ChannelRefundResult(state=ChannelRefundState.FAILED, detail=r_status)
        raise ChannelError("payment channel: wechat_pay refund answer has an unknown status")

    async def parse_notification(
        self,
        headers: Mapping[str, str],
        query: Mapping[str, str],
        body: bytes,
    ) -> ChannelNotification:
        try:
            self._verify_headers(headers, body)
        except ChannelError as exc:
            raise BadSignatureError(str(exc)) from exc

        try:
            envelope = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadSignatureError("wechat_pay: invalid JSON envelope") from exc
        if not isinstance(envelope, dict):
            raise BadSignatureError("wechat_pay: envelope is not a JSON object")

        event_type = envelope.get("event_type", "")
        if event_type not in ("TRANSACTION.SUCCESS", "REFUND.SUCCESS"):
            return ChannelNotification()

        resource = envelope.get("resource")
        if not isinstance(resource, dict) or resource.get("algorithm") != "AEAD_AES_256_GCM":
            raise BadSignatureError("wechat_pay: unsupported resource encryption algorithm")

        nonce = resource.get("nonce", "")
        aad = resource.get("associated_data", "")
        ciphertext = resource.get("ciphertext", "")
        if not isinstance(nonce, str) or not isinstance(aad, str) or not isinstance(ciphertext, str):
            raise BadSignatureError("wechat_pay: malformed resource fields")

        try:
            plain = wechat_decrypt_resource(self._cfg.api_v3_key, nonce, aad, ciphertext)
        except Exception as exc:
            raise BadSignatureError("wechat_pay: resource decryption failed") from exc

        if event_type == "REFUND.SUCCESS":
            return self._parse_refund_notification(plain)

        try:
            txn = json.loads(plain.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadSignatureError("wechat_pay: decrypted transaction is not JSON") from exc
        if (
            not isinstance(txn, dict)
            or txn.get("appid") != self._cfg.app_id
            or txn.get("mchid") != self._cfg.mch_id
            or txn.get("trade_state") != "SUCCESS"
        ):
            raise BadSignatureError("wechat_pay: transaction appid/mchid/state mismatch")
        try:
            payment = self._build_payment(txn)
        except ChannelError as exc:
            raise BadSignatureError(str(exc)) from exc
        return ChannelNotification(payment=payment)

    def _parse_refund_notification(self, plain: bytes) -> ChannelNotification:
        try:
            refund = json.loads(plain.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadSignatureError("wechat_pay: decrypted refund is not JSON") from exc
        if (
            not isinstance(refund, dict)
            or refund.get("mchid") != self._cfg.mch_id
            or not isinstance(refund.get("transaction_id"), str)
            or not refund.get("transaction_id")
        ):
            raise BadSignatureError("wechat_pay: refund notification mchid or transaction_id invalid")
        if refund.get("refund_status") != "SUCCESS":
            return ChannelNotification()
        at_ms = self._clock_ms()
        success_time = refund.get("success_time", "")
        if isinstance(success_time, str) and success_time:
            try:
                at_ms = parse_rfc3339_ms(success_time)
            except ValueError:
                pass
        return ChannelNotification(
            revocations=(
                ChannelRevocation(
                    transaction_id=refund["transaction_id"],
                    revoked=True,
                    observed_ms=at_ms,
                    out_trade_no=str(refund.get("out_trade_no", "")),
                    out_refund_no=str(refund.get("out_refund_no", "")),
                    detail="REFUND.SUCCESS",
                ),
            )
        )

    def format_ack(self, kind: AckKind) -> tuple[int, bytes, str]:
        if kind == AckKind.OK:
            return 204, b"", ""
        status = 401 if kind == AckKind.REJECTED else 500
        return status, b'{"code":"FAIL","message":"failed"}', "application/json"


# ---------------------------------------------------------------------------
# 支付宝开放平台（公钥模式 RSA2，APP 支付）
# ---------------------------------------------------------------------------

ALIPAY_DEFAULT_GATEWAY = "https://openapi.alipay.com/gateway.do"
_BAD_PERCENT_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")


@dataclass(frozen=True, slots=True)
class AlipayConfig:
    """支付宝 RSA2 公钥模式渠道配置。"""

    app_id: str
    app_key: rsa.RSAPrivateKey
    alipay_key: rsa.RSAPublicKey
    notify_url: str
    seller_id: str = ""
    gateway: str = ALIPAY_DEFAULT_GATEWAY
    subject: str = "游戏商品"


def alipay_sign_content(params: Mapping[str, str], *exclude: str) -> str:
    """去掉排除键与空值项，按键名升序拼成 ``k=v&k=v`` 原文（不 URL 编码）。"""
    excluded = set(exclude)
    keys = sorted(k for k, v in params.items() if k and v and k not in excluded)
    return "&".join(f"{k}={params[k]}" for k in keys)


def alipay_encode_params(params: Mapping[str, str]) -> str:
    """按键名升序对非空值做 URL 编码，生成支付宝 APP 订单串。"""
    keys = sorted(k for k, v in params.items() if k and v)
    return "&".join(f"{k}={quote_plus(params[k])}" for k in keys)


def _skip_ws(raw: bytes, idx: int) -> int:
    n = len(raw)
    while idx < n and raw[idx] in (0x20, 0x09, 0x0A, 0x0D):
        idx += 1
    return idx


def _scan_json_string(raw: bytes, idx: int) -> int:
    """从开引号位置扫描到闭引号之后的下标；非法抛 ValueError。"""
    n = len(raw)
    if idx >= n or raw[idx] != 0x22:  # '"'
        raise ValueError("expected JSON string")
    idx += 1
    while idx < n:
        b = raw[idx]
        if b == 0x5C:  # '\\'
            idx += 2
            continue
        if b == 0x22:
            return idx + 1
        idx += 1
    raise ValueError("unterminated JSON string")


def _scan_json_value(raw: bytes, idx: int) -> int:
    """从任意 JSON 值起始下标扫描到该值结束后的下标（保持原始字节不变）。"""
    n = len(raw)
    if idx >= n:
        raise ValueError("unexpected end of JSON")
    b = raw[idx]
    if b == 0x22:
        return _scan_json_string(raw, idx)
    if b in (0x7B, 0x5B):  # '{' or '['
        stack = [b]
        idx += 1
        while idx < n and stack:
            cur = raw[idx]
            if cur == 0x22:
                idx = _scan_json_string(raw, idx)
                continue
            if cur in (0x7B, 0x5B):
                stack.append(cur)
            elif cur == 0x7D:  # '}'
                if stack[-1] != 0x7B:
                    raise ValueError("mismatched '}'")
                stack.pop()
            elif cur == 0x5D:  # ']'
                if stack[-1] != 0x5B:
                    raise ValueError("mismatched ']'")
                stack.pop()
            idx += 1
        if stack:
            raise ValueError("unterminated JSON container")
        return idx
    # 数字 / true / false / null
    while idx < n and raw[idx] not in (0x2C, 0x7D, 0x5D, 0x20, 0x09, 0x0A, 0x0D):
        idx += 1
    return idx


def extract_top_level_json_slices(raw: bytes) -> dict[str, bytes]:
    """提取 JSON 顶层对象的原始值字节切片（不重新序列化，并拒绝重复键）。"""
    n = len(raw)
    idx = _skip_ws(raw, 0)
    if idx >= n or raw[idx] != 0x7B:
        raise ValueError("JSON root is not an object")
    idx = _skip_ws(raw, idx + 1)
    members: dict[str, bytes] = {}
    if idx < n and raw[idx] == 0x7D:
        return members
    while idx < n:
        key_end = _scan_json_string(raw, idx)
        key = json.loads(raw[idx:key_end].decode("utf-8"))
        idx = _skip_ws(raw, key_end)
        if idx >= n or raw[idx] != 0x3A:  # ':'
            raise ValueError("expected ':' after JSON object key")
        val_start = _skip_ws(raw, idx + 1)
        val_end = _scan_json_value(raw, val_start)
        if key in members:
            raise ValueError(f"duplicate top-level JSON key {key!r}")
        members[key] = raw[val_start:val_end]
        idx = _skip_ws(raw, val_end)
        if idx < n and raw[idx] == 0x2C:  # ','
            idx = _skip_ws(raw, idx + 1)
            continue
        if idx < n and raw[idx] == 0x7D:
            return members
        raise ValueError("expected ',' or '}' in JSON object")
    raise ValueError("unterminated JSON object")


class AlipayChannel:
    """支付宝 RSA2 公钥模式 APP 支付渠道适配器。"""

    def __init__(
        self,
        config: AlipayConfig,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        session: aiohttp.ClientSession | None = None,
        transport: HttpTransport | None = None,
    ) -> None:
        if not config.app_id or not all_digits(config.app_id):
            raise ValueError("payment channel: alipay app_id must be the numeric app id")
        if config.app_key is None or config.alipay_key is None:
            raise ValueError("payment channel: alipay needs app_key and alipay_key")
        if not config.notify_url or "?" in config.notify_url or "#" in config.notify_url:
            raise ValueError("payment channel: alipay notify_url is empty or carries a query string")
        if (
            not config.subject
            or len(config.subject) > 256
            or any(ch in config.subject for ch in ("/", "=", "&"))
        ):
            raise ValueError("payment channel: alipay subject must be 1-256 characters without / = &")
        if config.seller_id and not all_digits(config.seller_id):
            raise ValueError("payment channel: alipay seller_id must be the numeric user id")

        self._cfg = config
        self._gateway = config.gateway or ALIPAY_DEFAULT_GATEWAY
        self._clock_ms = clock_ms
        self._http = ChannelHttpClient(session=session, transport=transport)

    async def close(self) -> None:
        await self._http.close()

    @property
    def channel_id(self) -> int:
        return PAY_CHANNEL_ALIPAY

    @property
    def name(self) -> str:
        return CHANNEL_ALIPAY

    def supports(self, currency: str) -> bool:
        return currency == "CNY"

    def needs_product(self) -> bool:
        return False

    def _common_params(self, method: str, biz_json: str) -> dict[str, str]:
        return {
            "app_id": self._cfg.app_id,
            "method": method,
            "format": "JSON",
            "charset": "utf-8",
            "sign_type": "RSA2",
            "timestamp": format_cst_datetime(self._clock_ms()),
            "version": "1.0",
            "biz_content": biz_json,
        }

    def _sign_params(self, params: dict[str, str]) -> None:
        canonical = alipay_sign_content(params, "sign").encode("utf-8")
        params["sign"] = rsa_sign_sha256_b64(self._cfg.app_key, canonical)

    async def prepare(self, order: ChannelOrder, cached_ref_value: str, cached_ref_ms: int) -> ChannelPrepared:
        if not self.supports(order.currency):
            raise ChannelError("payment channel: alipay only takes CNY")
        biz = json.dumps(
            {
                "out_trade_no": order.out_trade_no,
                "total_amount": format_yuan(order.amount_minor),
                "subject": self._cfg.subject,
                "product_code": "QUICK_MSECURITY_PAY",
                "time_expire": format_cst_datetime(order.expire_ms),
            },
            separators=(",", ":"),
            ensure_ascii=False,
        )
        params = self._common_params("alipay.trade.app.pay", biz)
        params["notify_url"] = self._cfg.notify_url
        self._sign_params(params)
        return ChannelPrepared(params=ClientParams(alipay_app={"order_string": alipay_encode_params(params)}))

    async def _call(self, method: str, biz: dict) -> bytes:
        biz_json = json.dumps(biz, separators=(",", ":"), ensure_ascii=False)
        params = self._common_params(method, biz_json)
        self._sign_params(params)
        form_body = urlencode(sorted(params.items())).encode("utf-8")
        resp = await self._http.request(
            "POST",
            self._gateway,
            headers={"Content-Type": "application/x-www-form-urlencoded;charset=utf-8"},
            body=form_body,
        )
        if resp.status != 200:
            raise ChannelCallError(CHANNEL_ALIPAY, method, resp.status, "")
        return self._verify_answer(method, resp.body)

    def _verify_answer(self, method: str, raw: bytes) -> bytes:
        node = method.replace(".", "_") + "_response"
        try:
            # 确认整体是合法 JSON，并提取顶层原始字节切片
            json.loads(raw.decode("utf-8"))
            slices = extract_top_level_json_slices(raw)
        except Exception as exc:
            raise ChannelError("payment channel: alipay answer is not valid JSON") from exc

        content = slices.get(node)
        if content is None:
            error_slice = slices.get("error_response")
            if error_slice is not None:
                try:
                    err_obj = json.loads(error_slice.decode("utf-8"))
                except Exception:
                    err_obj = {}
                if isinstance(err_obj, dict):
                    code = f"{err_obj.get('code', '')}/{err_obj.get('sub_code', '')}"
                    raise ChannelCallError(CHANNEL_ALIPAY, method, 200, code)
            raise ChannelError("payment channel: alipay answer has no response node")

        if raw.count(f'"{node}"'.encode("utf-8")) != 1:
            raise ChannelError("payment channel: alipay answer repeats its response node")

        sign_slice = slices.get("sign")
        if sign_slice is None:
            raise ChannelError("payment channel: alipay answer is not signed")
        try:
            signature = json.loads(sign_slice.decode("utf-8"))
        except Exception as exc:
            raise ChannelError("payment channel: alipay answer sign is invalid") from exc
        if not isinstance(signature, str) or not signature:
            raise ChannelError("payment channel: alipay answer is not signed")

        try:
            rsa_verify_sha256_b64(self._cfg.alipay_key, content, signature)
            return content
        except ValueError:
            pass
        # 官方 SDK 兼容：若原始节点里斜杠被转义为 \/，替换为 / 再验一次
        if b"\\/" in content:
            try:
                rsa_verify_sha256_b64(self._cfg.alipay_key, content.replace(b"\\/", b"/"), signature)
                return content
            except ValueError:
                pass
        raise ChannelError("payment channel: alipay answer signature rejected")

    def _build_payment(self, trade_no: str, out_trade_no: str, total_amount: str, paid_at: str) -> ChannelPayment:
        if not trade_no or not out_trade_no:
            raise ChannelError("payment channel: alipay trade lacks trade_no or out_trade_no")
        try:
            amount = parse_yuan(total_amount)
        except ValueError as exc:
            raise ChannelError("payment channel: alipay trade amount is not usable") from exc
        if amount <= 0:
            raise ChannelError("payment channel: alipay trade amount is not usable")
        at_ms = self._clock_ms()
        if paid_at:
            try:
                at_ms = parse_cst_datetime_ms(paid_at)
            except ValueError as exc:
                raise ChannelError("payment channel: alipay payment time is not in expected format") from exc
        return ChannelPayment(
            transaction_id=trade_no,
            out_trade_no=out_trade_no,
            amount_minor=amount,
            currency="CNY",
            paid_ms=at_ms,
        )

    async def query(self, req: ChannelQueryRequest) -> ChannelQueryResult:
        content = await self._call("alipay.trade.query", {"out_trade_no": req.out_trade_no})
        try:
            ans = json.loads(content.decode("utf-8"))
        except Exception as exc:
            raise ChannelError("payment channel: alipay query answer is not readable") from exc
        if not isinstance(ans, dict):
            raise ChannelError("payment channel: alipay query answer is not an object")

        code = str(ans.get("code", ""))
        sub_code = str(ans.get("sub_code", ""))
        if code == "40004" and sub_code == "ACQ.TRADE_NOT_EXIST":
            return ChannelQueryResult(state=TradeState.NOT_PAID)
        if code != "10000":
            raise ChannelCallError(CHANNEL_ALIPAY, "query", 200, f"{code}/{sub_code}")
        if ans.get("out_trade_no") != req.out_trade_no:
            raise ChannelError("payment channel: alipay query answer belongs to another order")

        status = str(ans.get("trade_status", ""))
        if status == "WAIT_BUYER_PAY":
            return ChannelQueryResult(state=TradeState.NOT_PAID)
        if status in ("TRADE_SUCCESS", "TRADE_FINISHED"):
            p = self._build_payment(
                str(ans.get("trade_no", "")),
                str(ans.get("out_trade_no", "")),
                str(ans.get("total_amount", "")),
                str(ans.get("send_pay_date", "")),
            )
            return ChannelQueryResult(state=TradeState.PAID, payment=p)
        if status == "TRADE_CLOSED":
            send_pay_date = str(ans.get("send_pay_date", ""))
            if not send_pay_date:
                return ChannelQueryResult(state=TradeState.CLOSED)
            p = self._build_payment(
                str(ans.get("trade_no", "")),
                str(ans.get("out_trade_no", "")),
                str(ans.get("total_amount", "")),
                send_pay_date,
            )
            return ChannelQueryResult(state=TradeState.REFUNDED_OUTSIDE, payment=p)
        raise ChannelError("payment channel: alipay query answer has an unknown trade_status")

    async def close_trade(self, out_trade_no: str) -> None:
        content = await self._call("alipay.trade.close", {"out_trade_no": out_trade_no})
        try:
            ans = json.loads(content.decode("utf-8"))
        except Exception as exc:
            raise ChannelError("payment channel: alipay close answer is not readable") from exc
        code = str(ans.get("code", ""))
        sub_code = str(ans.get("sub_code", ""))
        if code == "10000" or (code == "40004" and sub_code in ("ACQ.TRADE_NOT_EXIST", "ACQ.TRADE_STATUS_ERROR")):
            return None
        raise ChannelCallError(CHANNEL_ALIPAY, "close", 200, f"{code}/{sub_code}")

    def _parse_refund_time_ms(self, text: str) -> int:
        if not text:
            return 0
        try:
            return parse_cst_datetime_ms(text)
        except ValueError:
            return 0

    async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult:
        content = await self._call(
            "alipay.trade.fastpay.refund.query",
            {
                "out_trade_no": req.out_trade_no,
                "out_request_no": req.out_refund_no,
                "query_options": ["gmt_refund_pay"],
            },
        )
        try:
            queried = json.loads(content.decode("utf-8"))
        except Exception as exc:
            raise ChannelError("payment channel: alipay refund query answer is not readable") from exc
        q_code = str(queried.get("code", ""))
        q_sub = str(queried.get("sub_code", ""))
        if q_code != "10000":
            raise ChannelCallError(CHANNEL_ALIPAY, "refund_query", 200, f"{q_code}/{q_sub}")
        if queried.get("refund_status") == "REFUND_SUCCESS":
            try:
                refunded = parse_yuan(str(queried.get("refund_amount", "")))
            except ValueError as exc:
                raise ChannelError("payment channel: alipay refund query amount invalid") from exc
            if refunded != req.amount_minor:
                raise ChannelError("payment channel: alipay refund query reports a different amount")
            return ChannelRefundResult(
                state=ChannelRefundState.SUCCEEDED,
                refund_id=req.out_refund_no,
                at_ms=self._parse_refund_time_ms(str(queried.get("gmt_refund_pay", ""))),
            )

        if req.currency != "CNY":
            return ChannelRefundResult(state=ChannelRefundState.FAILED, detail="CURRENCY")

        content = await self._call(
            "alipay.trade.refund",
            {
                "out_trade_no": req.out_trade_no,
                "refund_amount": format_yuan(req.amount_minor),
                "out_request_no": req.out_refund_no,
                "refund_reason": req.reason,
            },
        )
        try:
            ans = json.loads(content.decode("utf-8"))
        except Exception as exc:
            raise ChannelError("payment channel: alipay refund answer is not readable") from exc
        code = str(ans.get("code", ""))
        sub_code = str(ans.get("sub_code", ""))
        if code == "10000" and ans.get("fund_change") == "Y":
            return ChannelRefundResult(
                state=ChannelRefundState.SUCCEEDED,
                refund_id=req.out_refund_no,
                at_ms=self._parse_refund_time_ms(str(ans.get("gmt_refund_pay", ""))),
            )
        if code == "10000":
            return ChannelRefundResult(state=ChannelRefundState.PROCESSING, detail="FUND_CHANGE_N")
        if code == "20000" or sub_code == "ACQ.SYSTEM_ERROR":
            raise ChannelCallError(CHANNEL_ALIPAY, "refund", 200, f"{code}/{sub_code}")
        if sub_code == "ACQ.SELLER_BALANCE_NOT_ENOUGH":
            return ChannelRefundResult(state=ChannelRefundState.PROCESSING, detail=sub_code)
        return ChannelRefundResult(state=ChannelRefundState.FAILED, detail=f"{code}/{sub_code}")

    async def parse_notification(
        self,
        headers: Mapping[str, str],
        query: Mapping[str, str],
        body: bytes,
    ) -> ChannelNotification:
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise BadSignatureError("alipay: form body is not valid UTF-8") from exc
        if _BAD_PERCENT_RE.search(text):
            raise BadSignatureError("alipay: form body has malformed percent-encoding")

        pairs = parse_qsl(text, keep_blank_values=True, strict_parsing=False)
        params: dict[str, str] = {}
        for key, value in pairs:
            if key in params:
                raise BadSignatureError(f"alipay: duplicate form key {key!r}")
            params[key] = value

        if params.get("sign_type") != "RSA2" or not params.get("sign"):
            raise BadSignatureError("alipay: missing RSA2 sign_type or sign")

        canonical = alipay_sign_content(params, "sign", "sign_type").encode("utf-8")
        try:
            rsa_verify_sha256_b64(self._cfg.alipay_key, canonical, params["sign"])
        except ValueError as exc:
            raise BadSignatureError("alipay: RSA2 signature verification failed") from exc

        if params.get("app_id") != self._cfg.app_id:
            raise BadSignatureError("alipay: app_id mismatch")
        if self._cfg.seller_id and params.get("seller_id") != self._cfg.seller_id:
            raise BadSignatureError("alipay: seller_id mismatch")

        trade_status = params.get("trade_status", "")
        payment: ChannelPayment | None = None
        if trade_status in ("TRADE_SUCCESS", "TRADE_FINISHED"):
            try:
                payment = self._build_payment(
                    params.get("trade_no", ""),
                    params.get("out_trade_no", ""),
                    params.get("total_amount", ""),
                    params.get("gmt_payment", ""),
                )
            except ChannelError as exc:
                raise BadSignatureError(str(exc)) from exc
        elif trade_status != "TRADE_CLOSED":
            return ChannelNotification()

        revocations: list[ChannelRevocation] = []
        refund_fee_raw = params.get("refund_fee", "")
        trade_no = params.get("trade_no", "")
        if refund_fee_raw and trade_no:
            try:
                refund_fee = parse_yuan(refund_fee_raw)
            except ValueError:
                refund_fee = 0
            if refund_fee > 0:
                at_ms = self._clock_ms()
                gmt_refund = params.get("gmt_refund", "")
                if gmt_refund:
                    try:
                        at_ms = parse_cst_datetime_ms(gmt_refund)
                    except ValueError:
                        pass
                revocations.append(
                    ChannelRevocation(
                        transaction_id=trade_no,
                        revoked=True,
                        observed_ms=at_ms,
                        out_trade_no=params.get("out_trade_no", ""),
                        detail="REFUND",
                    )
                )

        return ChannelNotification(payment=payment, revocations=tuple(revocations))

    def format_ack(self, kind: AckKind) -> tuple[int, bytes, str]:
        if kind == AckKind.OK:
            return 200, b"success", "text/plain; charset=utf-8"
        return 200, b"fail", "text/plain; charset=utf-8"
