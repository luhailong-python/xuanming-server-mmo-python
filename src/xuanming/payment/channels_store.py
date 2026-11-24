"""商店扣款渠道适配：App Store StoreKit 2（``AppleChannel``）与 Google Play Billing（``GoogleChannel``）。

协议规则与 Go ``go/payment/internal/channel/apple.go``、``google.go`` 逐项对齐。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import quote, quote_plus, urlencode

import aiohttp
from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import ExtensionOID

from .channel_crypto import (
    ChannelHttpClient,
    HttpTransport,
    all_digits,
    b64url_decode,
    cert_valid_at,
    es256_sign_raw,
    es256_verify_raw,
    header_value,
    jwt_sign,
    parse_rfc3339_ms,
    rs256_sign_raw,
    rs256_verify_raw,
    split_jws,
)
from .channels import REFUND_UNSUPPORTED
from .errors import (
    BadSignatureError,
    ChannelCallError,
    ChannelError,
    QueryUnsupportedError,
    TempUnavailableError,
)
from .model import (
    AckKind,
    CHANNEL_APPLE,
    CHANNEL_GOOGLE,
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
    PAY_CHANNEL_APPLE,
    PAY_CHANNEL_GOOGLE,
    TradeState,
)

# ---------------------------------------------------------------------------
# App Store（StoreKit 2 消耗型内购）
# ---------------------------------------------------------------------------

APPLE_PRODUCTION_API = "https://api.storekit.apple.com"
APPLE_SANDBOX_API = "https://api.storekit-sandbox.apple.com"
APPLE_LEAF_OID = "1.2.840.113635.100.6.11.1"
APPLE_INTERMEDIATE_OID = "1.2.840.113635.100.6.2.1"
APPLE_NOT_FOUND_CODE = 4040010
APPLE_HISTORY_MAX_PAGES = 50
APPLE_HISTORY_KEPT_MS = 179 * 24 * 3600 * 1000

# ISO-4217 特殊小数位币种集合（0 位小数 / 3 位小数）
_ZERO_DECIMAL_CURRENCIES = frozenset((
    "BIF", "CLP", "DJF", "GNF", "ISK", "JPY", "KMF", "KRW",
    "PYG", "RWF", "UGX", "UYI", "VND", "VUV", "XAF", "XOF", "XPF",
))
_THREE_DECIMAL_CURRENCIES = frozenset((
    "BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND",
))


def apple_milliunits_to_minor(price: int, currency: str) -> int:
    """将 StoreKit 2 毫单位（×1000）价格按 ISO-4217 小数位纯整数折算为最小货币单位。"""
    if price <= 0:
        return 0
    code = currency.strip().upper()
    if code in _ZERO_DECIMAL_CURRENCIES:
        return price // 1000
    if code in _THREE_DECIMAL_CURRENCIES:
        return price
    return price // 10


def _has_extension_oid(cert: x509.Certificate, oid_dotted: str) -> bool:
    return any(ext.oid.dotted_string == oid_dotted for ext in cert.extensions)


def _apple_error_code(raw: bytes) -> int:
    try:
        out = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return 0
    if isinstance(out, dict):
        code = out.get("errorCode", 0)
        if isinstance(code, int) and not isinstance(code, bool):
            return code
    return 0


@dataclass(frozen=True, slots=True)
class AppleConfig:
    """App Store Server API 与通知 V2 渠道配置。"""

    bundle_id: str
    app_apple_id: int
    issuer_id: str
    key_id: str
    private_key: ec.EllipticCurvePrivateKey
    root_certificates: tuple[x509.Certificate, ...]
    allow_sandbox: bool = False
    production_api: str = APPLE_PRODUCTION_API
    sandbox_api: str = APPLE_SANDBOX_API


class AppleChannel:
    """App Store StoreKit 2 消耗型内购渠道适配器（支持 ReceiptChannel 与 HistoryPollerChannel）。"""

    def __init__(
        self,
        config: AppleConfig,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        session: aiohttp.ClientSession | None = None,
        transport: HttpTransport | None = None,
    ) -> None:
        if not config.bundle_id or not config.issuer_id or not config.key_id:
            raise ValueError("payment channel: apple needs bundle_id, issuer_id and key_id")
        if config.private_key is None:
            raise ValueError("payment channel: apple needs the App Store Connect API private key (.p8)")
        if not config.root_certificates:
            raise ValueError("payment channel: apple needs the Apple root certificates")
        if config.app_apple_id <= 0:
            raise ValueError("payment channel: apple needs app_apple_id (positive numeric App Store id)")

        self._cfg = config
        self._prod_api = (config.production_api or APPLE_PRODUCTION_API).rstrip("/")
        self._sandbox_api = (config.sandbox_api or APPLE_SANDBOX_API).rstrip("/")
        self._clock_ms = clock_ms
        self._http = ChannelHttpClient(session=session, transport=transport)

    async def close(self) -> None:
        await self._http.close()

    @property
    def channel_id(self) -> int:
        return PAY_CHANNEL_APPLE

    @property
    def name(self) -> str:
        return CHANNEL_APPLE

    def supports(self, currency: str) -> bool:
        return bool(currency)

    def needs_product(self) -> bool:
        return True

    async def prepare(self, order: ChannelOrder, cached_ref_value: str, cached_ref_ms: int) -> ChannelPrepared:
        if not order.product_id:
            raise ChannelError("payment channel: apple needs a product id for the order")
        return ChannelPrepared(
            params=ClientParams(
                apple={
                    "product_id": order.product_id,
                    "app_account_token": order.order_id,
                }
            )
        )

    async def query(self, req: ChannelQueryRequest) -> ChannelQueryResult:
        raise QueryUnsupportedError("apple: active query by out_trade_no is not supported")

    async def close_trade(self, out_trade_no: str) -> None:
        return None

    async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult:
        return REFUND_UNSUPPORTED

    def account_ref(self, player_id: int) -> str:
        return ""

    def verify_jws(self, token: str) -> dict:
        """校验 App Store 紧凑 JWS（ES256 + 3 证 x5c 链 + Apple OIDs + signedDate 有效期）。"""
        try:
            parts = split_jws(token)
        except ValueError as exc:
            raise ChannelError(str(exc)) from exc

        try:
            header = json.loads(parts.header.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelError("payment channel: apple JWS header is not JSON") from exc
        if not isinstance(header, dict) or header.get("alg") != "ES256":
            raise ChannelError("payment channel: apple JWS algorithm is not ES256")
        x5c = header.get("x5c")
        if not isinstance(x5c, list) or len(x5c) != 3 or not all(isinstance(item, str) for item in x5c):
            raise ChannelError("payment channel: apple JWS certificate chain must have exactly 3 certificates")

        certs: list[x509.Certificate] = []
        for encoded in x5c:
            try:
                der = base64.b64decode(encoded, validate=True)
                certs.append(x509.load_der_x509_certificate(der))
            except Exception as exc:
                raise ChannelError("payment channel: apple JWS certificate cannot be parsed") from exc

        try:
            payload = json.loads(parts.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelError("payment channel: apple JWS payload is not JSON") from exc
        if not isinstance(payload, dict):
            raise ChannelError("payment channel: apple JWS payload is not an object")
        signed_date = payload.get("signedDate")
        if isinstance(signed_date, bool) or not isinstance(signed_date, int) or signed_date <= 0:
            raise ChannelError("payment channel: apple JWS payload has no signedDate")

        at = datetime.fromtimestamp(signed_date / 1000.0, tz=timezone.utc)
        leaf, intermediate = certs[0], certs[1]

        try:
            bc = intermediate.extensions.get_extension_for_oid(ExtensionOID.BASIC_CONSTRAINTS).value
        except x509.ExtensionNotFound as exc:
            raise ChannelError("payment channel: apple intermediate certificate is not a CA") from exc
        if not isinstance(bc, x509.BasicConstraints) or not bc.ca:
            raise ChannelError("payment channel: apple intermediate certificate is not a CA")

        trusted = False
        for root in self._cfg.root_certificates:
            if not cert_valid_at(root, at):
                continue
            try:
                intermediate.verify_directly_issued_by(root)
                trusted = True
                break
            except Exception:
                continue
        if not trusted:
            raise ChannelError("payment channel: apple certificate chain does not end at a configured Apple root")

        if not cert_valid_at(leaf, at) or not cert_valid_at(intermediate, at):
            raise ChannelError("payment channel: apple leaf or intermediate certificate is not valid at signedDate")
        try:
            leaf.verify_directly_issued_by(intermediate)
        except Exception as exc:
            raise ChannelError("payment channel: apple leaf certificate is not signed by the intermediate") from exc

        if not _has_extension_oid(leaf, APPLE_LEAF_OID) or not _has_extension_oid(intermediate, APPLE_INTERMEDIATE_OID):
            raise ChannelError("payment channel: apple certificates lack the App Store extensions")

        pub = leaf.public_key()
        if not isinstance(pub, ec.EllipticCurvePublicKey) or not isinstance(pub.curve, ec.SECP256R1):
            raise ChannelError("payment channel: apple leaf key is not P-256 ECDSA")
        try:
            es256_verify_raw(pub, parts.signing_input, parts.signature)
        except ValueError as exc:
            raise ChannelError(str(exc)) from exc
        return payload

    def _api_token(self) -> str:
        now_sec = self._clock_ms() // 1000
        return jwt_sign(
            {"alg": "ES256", "kid": self._cfg.key_id, "typ": "JWT"},
            {
                "iss": self._cfg.issuer_id,
                "iat": now_sec,
                "exp": now_sec + 300,
                "aud": "appstoreconnect-v1",
                "bid": self._cfg.bundle_id,
            },
            lambda signing_input: es256_sign_raw(self._cfg.private_key, signing_input),
        )

    async def _api(self, base: str, method: str, path: str, body: bytes | None) -> tuple[int, bytes]:
        token = self._api_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        resp = await self._http.request(method, base + path, headers=headers, body=body)
        return resp.status, resp.body

    async def _fetch_transaction(self, transaction_id: str) -> dict | None:
        if not transaction_id or len(transaction_id) > 64 or not all_digits(transaction_id):
            raise ChannelError("payment channel: apple transaction id is not numeric")
        path = "/inApps/v1/transactions/" + quote(transaction_id, safe="")
        bases = [self._prod_api]
        if self._cfg.allow_sandbox:
            bases.append(self._sandbox_api)
        for base in bases:
            status, raw = await self._api(base, "GET", path, None)
            if status == 404 and _apple_error_code(raw) == APPLE_NOT_FOUND_CODE:
                continue
            if status != 200:
                raise ChannelCallError(CHANNEL_APPLE, "transaction", status, str(_apple_error_code(raw)))
            try:
                out = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ChannelError("payment channel: apple transaction answer is not JSON") from exc
            signed_info = out.get("signedTransactionInfo", "") if isinstance(out, dict) else ""
            if not isinstance(signed_info, str) or not signed_info:
                raise ChannelError("payment channel: apple transaction answer has no signedTransactionInfo")
            return self.verify_jws(signed_info)
        return None

    def _classify(self, txn: dict) -> ChannelQueryResult:
        if txn.get("bundleId") != self._cfg.bundle_id:
            raise ChannelError("payment channel: apple transaction belongs to another app")
        env = txn.get("environment", "")
        if env == "Sandbox":
            if not self._cfg.allow_sandbox:
                return ChannelQueryResult(state=TradeState.CLOSED)
        elif env != "Production":
            raise ChannelError("payment channel: apple transaction environment is not accepted")

        tx_id = str(txn.get("transactionId", ""))
        app_account_token = str(txn.get("appAccountToken", ""))
        product_id = str(txn.get("productId", ""))
        currency = str(txn.get("currency", ""))
        purchase_date = int(txn.get("purchaseDate", 0) or 0)
        price = int(txn.get("price", 0) or 0)
        quantity = int(txn.get("quantity", 0) or 0)
        tx_type = str(txn.get("type", ""))
        revocation_date = int(txn.get("revocationDate", 0) or 0)

        anomaly = ""
        if tx_type != "Consumable":
            anomaly = f"apple product type {tx_type} is not a consumable"
        elif quantity != 1:
            anomaly = f"apple transaction quantity is {quantity}"
        elif not app_account_token:
            anomaly = "apple transaction carries no appAccountToken"

        payment = ChannelPayment(
            transaction_id=tx_id,
            order_id=app_account_token.lower(),
            currency="",
            paid_ms=purchase_date,
            product_id=product_id,
            store_priced=True,
            store_amount_minor=apple_milliunits_to_minor(price, currency) if price > 0 else 0,
            store_currency=currency,
            test=(env == "Sandbox"),
            anomaly=anomaly,
        )
        if revocation_date > 0:
            return ChannelQueryResult(state=TradeState.REFUNDED_OUTSIDE, payment=payment)
        return ChannelQueryResult(state=TradeState.PAID, payment=payment)

    async def verify(self, receipt: ChannelReceipt) -> ChannelQueryResult:
        txn = await self._fetch_transaction(receipt.transaction_id)
        if txn is None:
            return ChannelQueryResult(state=TradeState.CLOSED)
        return self._classify(txn)

    def _decode_notification(self, signed_payload: str) -> ChannelNotification:
        try:
            note = self.verify_jws(signed_payload)
        except ChannelError as exc:
            raise BadSignatureError(str(exc)) from exc

        data = note.get("data")
        if data is None:
            # TEST / summary 等不含 data 的合法签名通知，验过签照常应答 200
            return ChannelNotification()
        if not isinstance(data, dict) or data.get("bundleId") != self._cfg.bundle_id:
            raise BadSignatureError("apple: notification bundleId mismatch")

        env = data.get("environment", "")
        if env == "Production":
            if data.get("appAppleId") != self._cfg.app_apple_id:
                raise BadSignatureError("apple: notification appAppleId mismatch")
        elif env == "Sandbox":
            if not self._cfg.allow_sandbox:
                return ChannelNotification()
        else:
            return ChannelNotification()

        n_type = note.get("notificationType", "")
        if n_type not in ("ONE_TIME_CHARGE", "REFUND", "REFUND_REVERSED"):
            return ChannelNotification()

        signed_txn = data.get("signedTransactionInfo", "")
        if not isinstance(signed_txn, str) or not signed_txn:
            raise BadSignatureError("apple: notification missing signedTransactionInfo")
        try:
            txn = self.verify_jws(signed_txn)
        except ChannelError as exc:
            raise BadSignatureError(str(exc)) from exc

        try:
            result = self._classify(txn)
        except ChannelError:
            return ChannelNotification()
        if result.payment is None:
            return ChannelNotification()

        signed_date_ms = int(note.get("signedDate", 0) or 0)
        if n_type == "ONE_TIME_CHARGE":
            if result.state != TradeState.PAID:
                return ChannelNotification()
            return ChannelNotification(payment=result.payment)

        revoked = n_type == "REFUND"
        return ChannelNotification(
            revocations=(
                ChannelRevocation(
                    transaction_id=result.payment.transaction_id,
                    revoked=revoked,
                    observed_ms=signed_date_ms,
                    detail=n_type,
                    payment=result.payment,
                ),
            )
        )

    async def parse_notification(
        self,
        headers: Mapping[str, str],
        query: Mapping[str, str],
        body: bytes,
    ) -> ChannelNotification:
        try:
            envelope = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadSignatureError("apple: notification body is not JSON") from exc
        signed_payload = envelope.get("signedPayload", "") if isinstance(envelope, dict) else ""
        if not isinstance(signed_payload, str) or not signed_payload:
            raise BadSignatureError("apple: missing signedPayload")
        return self._decode_notification(signed_payload)

    async def poll_history(self, since_ms: int) -> tuple[list[ChannelNotification], int]:
        now_ms = self._clock_ms()
        floor_ms = now_ms - APPLE_HISTORY_KEPT_MS
        if since_ms < floor_ms:
            since_ms = floor_ms
        until_ms = now_ms - 60_000
        if until_ms <= since_ms:
            return [], since_ms

        while True:
            out, complete = await self._history_window(since_ms, until_ms)
            if complete:
                return out, until_ms
            until_ms = since_ms + (until_ms - since_ms) // 2
            if until_ms - since_ms < 1000:
                raise ChannelError("payment channel: apple notification history has too many notifications within one second")

    async def _history_window(self, since_ms: int, until_ms: int) -> tuple[list[ChannelNotification], bool]:
        body = json.dumps({"startDate": since_ms, "endDate": until_ms}, separators=(",", ":")).encode("utf-8")
        out: list[ChannelNotification] = []
        pagination = ""
        for _ in range(APPLE_HISTORY_MAX_PAGES):
            path = "/inApps/v1/notifications/history"
            if pagination:
                path += "?paginationToken=" + quote_plus(pagination)
            status, raw = await self._api(self._prod_api, "POST", path, body)
            if status != 200:
                raise ChannelCallError(CHANNEL_APPLE, "notification_history", status, str(_apple_error_code(raw)))
            try:
                history = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ChannelError("payment channel: apple notification history answer is not readable") from exc
            if not isinstance(history, dict):
                raise ChannelError("payment channel: apple notification history answer is not an object")
            items = history.get("notificationHistory") or []
            for item in items:
                signed_payload = item.get("signedPayload", "") if isinstance(item, dict) else ""
                try:
                    note = self._decode_notification(signed_payload)
                except BadSignatureError as exc:
                    raise ChannelError(f"payment channel: apple notification history item rejected: {exc}") from exc
                if note.payment is not None or note.revocations:
                    out.append(note)
            has_more = bool(history.get("hasMore", False))
            next_token = str(history.get("paginationToken", "") or "")
            if not has_more or not next_token:
                return out, True
            pagination = next_token
        return [], False

    def format_ack(self, kind: AckKind) -> tuple[int, bytes, str]:
        if kind == AckKind.OK:
            return 200, b"", ""
        return 500, b"", ""


# ---------------------------------------------------------------------------
# Google Play（一次性消耗型商品 + Pub/Sub OIDC 推送 + 作废列表补拉）
# ---------------------------------------------------------------------------

GOOGLE_API_BASE = "https://androidpublisher.googleapis.com"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_CERTS_URL = "https://www.googleapis.com/oauth2/v3/certs"
GOOGLE_SCOPE = "https://www.googleapis.com/auth/androidpublisher"
GOOGLE_TOKEN_EARLY_REFRESH_MS = 5 * 60 * 1000
GOOGLE_CERTS_TTL_MS = 3600 * 1000
GOOGLE_CERTS_MIN_REFETCH_MS = 60 * 1000
GOOGLE_HISTORY_MAX_PAGES = 50
GOOGLE_HISTORY_KEPT_MS = 29 * 24 * 3600 * 1000


def google_transaction_id(purchase_token: str) -> str:
    """将 Google Play ``purchaseToken`` 单向哈希为商城合法交易号（64 位小写十六进制）。"""
    return hashlib.sha256(purchase_token.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class GoogleConfig:
    """Google Play Billing 与 Pub/Sub 推送校验配置。"""

    package_name: str
    client_email: str
    private_key: rsa.RSAPrivateKey
    push_audience: str
    push_service_account: str
    api_base: str = GOOGLE_API_BASE
    token_url: str = GOOGLE_TOKEN_URL
    certs_url: str = GOOGLE_CERTS_URL


class GoogleChannel:
    """Google Play 消耗型商品渠道适配器（支持 Receipt / Finalizer / HistoryPoller）。"""

    def __init__(
        self,
        config: GoogleConfig,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        session: aiohttp.ClientSession | None = None,
        transport: HttpTransport | None = None,
    ) -> None:
        if not config.package_name or not config.client_email or config.private_key is None:
            raise ValueError(
                "payment channel: google needs package_name and service account (client_email, private_key)"
            )
        if not config.push_audience or not config.push_service_account:
            raise ValueError("payment channel: google needs push_audience and push_service_account")

        self._cfg = config
        self._api_base = (config.api_base or GOOGLE_API_BASE).rstrip("/")
        self._token_url = config.token_url or GOOGLE_TOKEN_URL
        self._certs_url = config.certs_url or GOOGLE_CERTS_URL
        self._clock_ms = clock_ms
        self._http = ChannelHttpClient(session=session, transport=transport)

        # OAuth2 access_token 缓存与 single-flight
        self._access_token: str = ""
        self._token_expiry_ms: int = 0
        self._token_inflight: asyncio.Future[str] | None = None

        # Google OIDC JWKS 缓存（1 小时 TTL + 1 分钟未知 kid 防刷新 DoS 冷却）
        self._certs: dict[str, rsa.RSAPublicKey] = {}
        self._certs_expiry_ms: int = 0
        self._certs_fetched_at_ms: int = 0
        self._certs_inflight: asyncio.Future[dict[str, rsa.RSAPublicKey]] | None = None

    async def close(self) -> None:
        await self._http.close()

    @property
    def channel_id(self) -> int:
        return PAY_CHANNEL_GOOGLE

    @property
    def name(self) -> str:
        return CHANNEL_GOOGLE

    def supports(self, currency: str) -> bool:
        return bool(currency)

    def needs_product(self) -> bool:
        return True

    def account_ref(self, player_id: int) -> str:
        """买家玩家号的单向 SHA-256 派生值（64 字符，匹配 obfuscatedExternalAccountId 上限）。"""
        return hashlib.sha256(f"account:{player_id}".encode("utf-8")).hexdigest()

    async def prepare(self, order: ChannelOrder, cached_ref_value: str, cached_ref_ms: int) -> ChannelPrepared:
        if not order.product_id:
            raise ChannelError("payment channel: google needs a product id for the order")
        return ChannelPrepared(
            params=ClientParams(
                google={
                    "product_id": order.product_id,
                    "obfuscated_account_id": self.account_ref(order.player_id),
                    "obfuscated_profile_id": order.order_id,
                }
            )
        )

    async def query(self, req: ChannelQueryRequest) -> ChannelQueryResult:
        raise QueryUnsupportedError("google: active query by out_trade_no is not supported")

    async def close_trade(self, out_trade_no: str) -> None:
        return None

    async def _token(self) -> str:
        now_ms = self._clock_ms()
        if self._access_token and now_ms < self._token_expiry_ms - GOOGLE_TOKEN_EARLY_REFRESH_MS:
            return self._access_token
        if self._token_inflight is not None:
            return await asyncio.shield(self._token_inflight)

        loop = asyncio.get_running_loop()
        inflight: asyncio.Future[str] = loop.create_future()
        self._token_inflight = inflight
        try:
            tok, expiry_ms = await self._fetch_token()
        except Exception as exc:
            self._token_inflight = None
            if not inflight.done():
                inflight.set_exception(exc)
                inflight.exception()  # 避免无并发等待者时触发 asyncio 未检索异常警告
            raise
        self._access_token = tok
        self._token_expiry_ms = expiry_ms
        self._token_inflight = None
        if not inflight.done():
            inflight.set_result(tok)
        return tok

    def _invalidate_token(self, bad: str) -> None:
        if self._access_token == bad:
            self._access_token = ""
            self._token_expiry_ms = 0

    async def _fetch_token(self) -> tuple[str, int]:
        now_ms = self._clock_ms()
        now_sec = now_ms // 1000
        assertion = jwt_sign(
            {"alg": "RS256", "typ": "JWT"},
            {
                "iss": self._cfg.client_email,
                "scope": GOOGLE_SCOPE,
                "aud": self._token_url,
                "iat": now_sec,
                "exp": now_sec + 3600,
            },
            lambda signing_input: rs256_sign_raw(self._cfg.private_key, signing_input),
        )
        form_body = urlencode(
            {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
            }
        ).encode("utf-8")
        resp = await self._http.request(
            "POST",
            self._token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=form_body,
        )
        if resp.status != 200:
            raise ChannelCallError(CHANNEL_GOOGLE, "token", resp.status, "")
        try:
            out = json.loads(resp.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelError("payment channel: google token answer is not usable") from exc
        tok = out.get("access_token", "") if isinstance(out, dict) else ""
        expires_in = out.get("expires_in", 0) if isinstance(out, dict) else 0
        if not isinstance(tok, str) or not tok or isinstance(expires_in, bool) or not isinstance(expires_in, int) or expires_in <= 0:
            raise ChannelError("payment channel: google token answer is not usable")
        return tok, now_ms + expires_in * 1000

    async def _api(self, method: str, path: str, body: bytes | None) -> tuple[int, bytes]:
        pkg = quote(self._cfg.package_name, safe="")
        url = f"{self._api_base}/androidpublisher/v3/applications/{pkg}{path}"
        for attempt in range(2):
            token = await self._token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
            if body is not None:
                headers["Content-Type"] = "application/json"
            resp = await self._http.request(method, url, headers=headers, body=body)
            if resp.status == 401 and attempt == 0:
                self._invalidate_token(token)
                continue
            return resp.status, resp.body
        raise ChannelError("payment channel: google api retry exhausted")

    async def _fetch_purchase(self, purchase_token: str) -> tuple[dict, int]:
        if not purchase_token or len(purchase_token) > 1024:
            raise ChannelError("payment channel: google purchase token is empty or too long")
        status, raw = await self._api("GET", "/purchases/productsv2/tokens/" + quote(purchase_token, safe=""), None)
        if status != 200:
            raise ChannelCallError(CHANNEL_GOOGLE, "purchase", status, "")
        try:
            out = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelError("payment channel: google purchase answer is not readable") from exc
        if not isinstance(out, dict):
            raise ChannelError("payment channel: google purchase answer is not an object")
        return out, status

    async def verify(self, receipt: ChannelReceipt) -> ChannelQueryResult:
        try:
            bought, _ = await self._fetch_purchase(receipt.purchase_token)
        except ChannelCallError as exc:
            if exc.status in (400, 404, 410):
                return ChannelQueryResult(state=TradeState.CLOSED)
            raise

        line_items = bought.get("productLineItem")
        if not isinstance(line_items, list) or len(line_items) != 1 or not isinstance(line_items[0], dict):
            raise ChannelError("payment channel: google purchase does not carry exactly one product")
        line = line_items[0]
        product_id = str(line.get("productId", ""))
        if receipt.product_id and product_id != receipt.product_id:
            return ChannelQueryResult(state=TradeState.CLOSED)

        details = line.get("productOfferDetails") or {}
        if not isinstance(details, dict):
            details = {}
        quantity = int(details.get("quantity", 0) or 0)
        refundable_qty = int(details.get("refundableQuantity", 0) or 0)
        consumption_state = str(details.get("consumptionState", ""))

        order_id = str(bought.get("obfuscatedExternalProfileId", "")).lower()
        account_ref = str(bought.get("obfuscatedExternalAccountId", ""))
        channel_order_id = str(bought.get("orderId", ""))
        paid_ms = self._clock_ms()
        comp_time = bought.get("purchaseCompletionTime", "")
        if isinstance(comp_time, str) and comp_time:
            try:
                paid_ms = parse_rfc3339_ms(comp_time)
            except ValueError:
                pass

        anomaly = ""
        if quantity > 1:
            anomaly = f"google purchase quantity is {quantity}"
        if not order_id:
            anomaly = "google purchase carries no obfuscatedExternalProfileId"

        payment = ChannelPayment(
            transaction_id=google_transaction_id(receipt.purchase_token),
            order_id=order_id,
            paid_ms=paid_ms,
            product_id=product_id,
            store_priced=True,
            channel_token=receipt.purchase_token,
            channel_order_id=channel_order_id,
            needs_finalize=(consumption_state != "CONSUMPTION_STATE_CONSUMED"),
            test=(bought.get("testPurchaseContext") is not None),
            account_ref=account_ref,
            anomaly=anomaly,
        )

        state_ctx = bought.get("purchaseStateContext") or {}
        purchase_state = str(state_ctx.get("purchaseState", "")) if isinstance(state_ctx, dict) else ""
        if purchase_state == "PURCHASED":
            if quantity > 0 and refundable_qty < quantity:
                return ChannelQueryResult(state=TradeState.REFUNDED_OUTSIDE, payment=payment)
            return ChannelQueryResult(state=TradeState.PAID, payment=payment)
        if purchase_state == "PENDING":
            return ChannelQueryResult(state=TradeState.NOT_PAID)
        if purchase_state == "CANCELLED":
            return ChannelQueryResult(state=TradeState.CLOSED, payment=payment)
        raise ChannelError("payment channel: google purchase has an unknown purchaseState")

    async def finalize(self, product_id: str, channel_token: str) -> None:
        path = (
            "/purchases/products/"
            + quote(product_id, safe="")
            + "/tokens/"
            + quote(channel_token, safe="")
            + ":consume"
        )
        status, _ = await self._api("POST", path, None)
        if 200 <= status < 300:
            return None
        # 已经消耗过的再调 consume 可能回错：回查确认已消耗即算收尾成功
        try:
            bought, _ = await self._fetch_purchase(channel_token)
            items = bought.get("productLineItem") or []
            if (
                len(items) == 1
                and (items[0].get("productOfferDetails") or {}).get("consumptionState") == "CONSUMPTION_STATE_CONSUMED"
            ):
                return None
        except Exception:
            pass
        raise ChannelCallError(CHANNEL_GOOGLE, "consume", status, "")

    async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult:
        if not req.channel_order_id:
            return ChannelRefundResult(state=ChannelRefundState.FAILED, detail="NO_ORDER_ID")
        try:
            bought, _ = await self._fetch_purchase(req.channel_token)
            state = (bought.get("purchaseStateContext") or {}).get("purchaseState", "")
            if state == "CANCELLED":
                return ChannelRefundResult(
                    state=ChannelRefundState.SUCCEEDED,
                    refund_id=req.out_refund_no,
                    at_ms=self._clock_ms(),
                )
        except Exception:
            pass

        path = "/orders/" + quote(req.channel_order_id, safe="") + ":refund?revoke=false"
        status, _ = await self._api("POST", path, None)
        if 200 <= status < 300:
            return ChannelRefundResult(
                state=ChannelRefundState.SUCCEEDED,
                refund_id=req.out_refund_no,
                at_ms=self._clock_ms(),
            )
        if status == 429 or status >= 500 or status == 401:
            raise ChannelCallError(CHANNEL_GOOGLE, "refund", status, "")
        return ChannelRefundResult(state=ChannelRefundState.FAILED, detail=f"HTTP_{status}")

    # -- Pub/Sub OIDC JWKS 验签 ------------------------------------------------------------------

    async def _cert_key(self, kid: str) -> rsa.RSAPublicKey | None:
        now_ms = self._clock_ms()
        if now_ms < self._certs_expiry_ms:
            if kid in self._certs:
                return self._certs[kid]
            # 距上次成功拉取不足 1 分钟遇到未知 kid 直接回 None，防止随机 kid 触发 JWKS 刷新 DoS
            if self._certs_fetched_at_ms > 0 and (now_ms - self._certs_fetched_at_ms) < GOOGLE_CERTS_MIN_REFETCH_MS:
                return None

        if self._certs_inflight is not None:
            keys = await asyncio.shield(self._certs_inflight)
            return keys.get(kid)

        loop = asyncio.get_running_loop()
        inflight: asyncio.Future[dict[str, rsa.RSAPublicKey]] = loop.create_future()
        self._certs_inflight = inflight
        try:
            keys = await self._fetch_certs()
        except Exception as exc:
            self._certs_inflight = None
            err = TempUnavailableError(f"payment channel: google OIDC certificates are unavailable: {exc}")
            if not inflight.done():
                inflight.set_exception(err)
                inflight.exception()  # 避免无并发等待者时触发 asyncio 未检索异常警告
            raise err from exc

        fetched_at_ms = self._clock_ms()
        self._certs = keys
        self._certs_fetched_at_ms = fetched_at_ms
        self._certs_expiry_ms = fetched_at_ms + GOOGLE_CERTS_TTL_MS
        self._certs_inflight = None
        if not inflight.done():
            inflight.set_result(keys)
        return keys.get(kid)

    async def _fetch_certs(self) -> dict[str, rsa.RSAPublicKey]:
        resp = await self._http.request("GET", self._certs_url)
        if resp.status != 200:
            raise ChannelCallError(CHANNEL_GOOGLE, "certs", resp.status, "")
        try:
            payload = json.loads(resp.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelError("payment channel: google certs are not readable") from exc
        raw_keys = payload.get("keys") if isinstance(payload, dict) else None
        if not isinstance(raw_keys, list):
            raise ChannelError("payment channel: google certs have no keys list")

        keys: dict[str, rsa.RSAPublicKey] = {}
        for item in raw_keys:
            if not isinstance(item, dict) or item.get("kty") != "RSA":
                continue
            kid = item.get("kid", "")
            n_b64 = item.get("n", "")
            e_b64 = item.get("e", "")
            if not isinstance(kid, str) or not kid or not isinstance(n_b64, str) or not isinstance(e_b64, str):
                continue
            try:
                n_bytes = b64url_decode(n_b64)
                e_bytes = b64url_decode(e_b64)
            except ValueError:
                continue
            if not n_bytes or not 1 <= len(e_bytes) <= 4:
                continue
            n_int = int.from_bytes(n_bytes, "big")
            e_int = int.from_bytes(e_bytes, "big")
            if e_int < 3:
                continue
            try:
                keys[kid] = rsa.RSAPublicNumbers(e_int, n_int).public_key()
            except ValueError:
                continue
        if not keys:
            raise ChannelError("payment channel: google certs contain no usable RSA keys")
        return keys

    async def _verify_push_token(self, authorization: str) -> None:
        if not authorization.startswith("Bearer ") or len(authorization) <= 7:
            raise BadSignatureError("google: push carries no bearer token")
        token = authorization[7:]
        try:
            parts = split_jws(token)
            header = json.loads(parts.header.decode("utf-8"))
        except Exception as exc:
            raise BadSignatureError("google: push token is malformed") from exc
        if (
            not isinstance(header, dict)
            or header.get("alg") != "RS256"
            or not isinstance(header.get("kid"), str)
            or not header.get("kid")
        ):
            raise BadSignatureError("google: push token is not RS256 with a key id")

        # 拉取 JWKS 失败抛 TempUnavailableError（入口回 500 重试），不误判为 BadSignatureError
        key = await self._cert_key(header["kid"])
        if key is None:
            raise BadSignatureError("google: push token signed by unknown key")

        try:
            rs256_verify_raw(key, parts.signing_input, parts.signature)
        except ValueError as exc:
            raise BadSignatureError("google: push token RS256 signature rejected") from exc

        try:
            claims = json.loads(parts.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadSignatureError("google: push token claims are not readable") from exc
        if not isinstance(claims, dict):
            raise BadSignatureError("google: push token claims are not an object")

        iss = claims.get("iss")
        aud = claims.get("aud")
        email = claims.get("email")
        email_verified = claims.get("email_verified")
        exp = claims.get("exp")
        if iss not in ("https://accounts.google.com", "accounts.google.com"):
            raise BadSignatureError("google: push token has a foreign issuer")
        if aud != self._cfg.push_audience:
            raise BadSignatureError("google: push token audience differs")
        if email != self._cfg.push_service_account or email_verified is not True:
            raise BadSignatureError("google: push token is not from the push service account")
        if isinstance(exp, bool) or not isinstance(exp, int) or (self._clock_ms() // 1000) >= exp:
            raise BadSignatureError("google: push token has expired")

    async def parse_notification(
        self,
        headers: Mapping[str, str],
        query: Mapping[str, str],
        body: bytes,
    ) -> ChannelNotification:
        await self._verify_push_token(header_value(headers, "Authorization"))

        try:
            envelope = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadSignatureError("google: push body is not JSON") from exc
        message = envelope.get("message") if isinstance(envelope, dict) else None
        data_b64 = message.get("data", "") if isinstance(message, dict) else ""
        if not isinstance(data_b64, str) or not data_b64:
            raise BadSignatureError("google: push message lacks data")
        try:
            decoded = base64.b64decode(data_b64, validate=True)
            note = json.loads(decoded.decode("utf-8"))
        except Exception as exc:
            raise BadSignatureError("google: push message.data is not valid base64 JSON") from exc
        if not isinstance(note, dict) or note.get("packageName") != self._cfg.package_name:
            raise BadSignatureError("google: notification packageName mismatch")

        at_ms = self._clock_ms()
        event_ms_raw = note.get("eventTimeMillis", "")
        if isinstance(event_ms_raw, str) and event_ms_raw:
            try:
                parsed_ms = int(event_ms_raw)
                if parsed_ms > 0:
                    at_ms = parsed_ms
            except ValueError:
                pass

        one_time = note.get("oneTimeProductNotification")
        if isinstance(one_time, dict) and one_time.get("purchaseToken"):
            return ChannelNotification(
                receipts=(
                    ChannelReceipt(
                        purchase_token=str(one_time["purchaseToken"]),
                        product_id=str(one_time.get("sku", "")),
                    ),
                )
            )

        voided = note.get("voidedPurchaseNotification")
        if (
            isinstance(voided, dict)
            and voided.get("purchaseToken")
            and int(voided.get("productType", 0) or 0) == 2
        ):
            refund_type = int(voided.get("refundType", 0) or 0)
            return ChannelNotification(
                revocations=(
                    ChannelRevocation(
                        transaction_id=google_transaction_id(str(voided["purchaseToken"])),
                        revoked=True,
                        observed_ms=at_ms,
                        detail=f"VOIDED_REFUND_TYPE_{refund_type}",
                    ),
                )
            )
        return ChannelNotification()

    async def poll_history(self, since_ms: int) -> tuple[list[ChannelNotification], int]:
        now_ms = self._clock_ms()
        floor_ms = now_ms - GOOGLE_HISTORY_KEPT_MS
        if since_ms < floor_ms:
            since_ms = floor_ms
        until_ms = now_ms - 60_000
        if until_ms <= since_ms:
            return [], since_ms

        while True:
            out, complete = await self._voided_window(since_ms, until_ms)
            if complete:
                return out, until_ms
            until_ms = since_ms + (until_ms - since_ms) // 2
            if until_ms - since_ms < 1000:
                raise ChannelError("payment channel: google voided purchases have too many pages within one second")

    async def _voided_window(self, since_ms: int, until_ms: int) -> tuple[list[ChannelNotification], bool]:
        out: list[ChannelNotification] = []
        page_token = ""
        for _ in range(GOOGLE_HISTORY_MAX_PAGES):
            if page_token:
                qs = urlencode({"token": page_token, "maxResults": "1000"})
            else:
                qs = urlencode(
                    {
                        "startTime": str(since_ms),
                        "endTime": str(until_ms),
                        "maxResults": "1000",
                        "type": "0",
                        "includeQuantityBasedPartialRefund": "true",
                    }
                )
            status, raw = await self._api("GET", "/purchases/voidedpurchases?" + qs, None)
            if status != 200:
                raise ChannelCallError(CHANNEL_GOOGLE, "voided_purchases", status, "")
            try:
                listing = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ChannelError("payment channel: google voided purchases answer is not readable") from exc
            if not isinstance(listing, dict):
                raise ChannelError("payment channel: google voided purchases answer is not an object")
            for voided in listing.get("voidedPurchases") or []:
                if not isinstance(voided, dict) or not voided.get("purchaseToken"):
                    continue
                at_ms = until_ms
                v_raw = voided.get("voidedTimeMillis", "")
                if isinstance(v_raw, str) and v_raw:
                    try:
                        parsed = int(v_raw)
                        if parsed > 0:
                            at_ms = parsed
                    except ValueError:
                        pass
                out.append(
                    ChannelNotification(
                        revocations=(
                            ChannelRevocation(
                                transaction_id=google_transaction_id(str(voided["purchaseToken"])),
                                revoked=True,
                                observed_ms=at_ms,
                                detail="VOIDED",
                            ),
                        )
                    )
                )
            pagination = listing.get("tokenPagination") or {}
            next_token = str(pagination.get("nextPageToken", "") or "") if isinstance(pagination, dict) else ""
            if not next_token:
                return out, True
            page_token = next_token
        return [], False

    def format_ack(self, kind: AckKind) -> tuple[int, bytes, str]:
        if kind == AckKind.OK:
            return 204, b"", ""
        return 500, b"", ""
