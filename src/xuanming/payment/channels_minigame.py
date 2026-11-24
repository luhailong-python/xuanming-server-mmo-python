"""小游戏支付渠道适配：微信小游戏虚拟支付 2.0（``WechatMinigameChannel``）与抖音小游戏（``DouyinMinigameChannel``）。

协议规则与 Go ``go/payment/internal/channel/wechatminigame.go``、``douyin.go`` 逐项对齐。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import secrets
import struct
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import urlencode

import aiohttp
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .channel_crypto import (
    ChannelHttpClient,
    HttpTransport,
    equal_hex,
    hmac_sha256_hex,
    time_from_unix_text_ms,
)
from .channels import REFUND_UNSUPPORTED, sorted_sha1
from .errors import (
    BadSignatureError,
    ChannelCallError,
    ChannelError,
    ChannelLoginRequiredError,
    QueryUnsupportedError,
)
from .model import (
    AckKind,
    CHANNEL_DOUYIN_MINIGAME,
    CHANNEL_WECHAT_MINIGAME,
    ChannelNotification,
    ChannelOrder,
    ChannelPayment,
    ChannelPrepared,
    ChannelQueryRequest,
    ChannelQueryResult,
    ChannelRef,
    ChannelRefundRequest,
    ChannelRefundResult,
    ChannelRevocation,
    ClientParams,
    PAY_CHANNEL_DOUYIN_MINIGAME,
    PAY_CHANNEL_WECHAT_MINIGAME,
    TradeState,
)

# ---------------------------------------------------------------------------
# 微信小游戏虚拟支付 2.0（道具直购）
# ---------------------------------------------------------------------------

WECHAT_MINI_API_BASE = "https://api.weixin.qq.com"
_TOKEN_EARLY_REFRESH_MS = 5 * 60 * 1000
_AES_BLOCK_SIZE = 16
_WECHAT_MINI_PKCS7_BLOCK = 32


@dataclass(frozen=True, slots=True)
class WechatMinigameConfig:
    """微信小游戏虚拟支付 2.0 渠道配置。"""

    app_id: str
    app_secret: str
    offer_id: str
    app_key: str
    push_token: str
    env: int = 0  # 0 = 正式，1 = 沙箱
    zone_id: str = "1"
    encoding_aes_key: str = ""  # 安全模式 43 字符密钥，明文模式留空
    api_base: str = WECHAT_MINI_API_BASE


def wechat_minigame_encrypt_push(aes_key: bytes, app_id: str, plaintext: bytes, *, rand16: bytes | None = None) -> str:
    """按微信消息推送安全模式规则加密载荷（AES-256-CBC, IV=key[:16], 32 字节 PKCS#7），供测试与工具复用。"""
    prefix = rand16 if rand16 is not None else secrets.token_bytes(16)
    if len(prefix) != 16:
        raise ValueError("rand16 must be 16 bytes")
    raw = prefix + struct.pack(">I", len(plaintext)) + plaintext + app_id.encode("utf-8")
    pad_len = _WECHAT_MINI_PKCS7_BLOCK - (len(raw) % _WECHAT_MINI_PKCS7_BLOCK)
    padded = raw + bytes([pad_len]) * pad_len
    encryptor = Cipher(algorithms.AES(aes_key), modes.CBC(aes_key[:_AES_BLOCK_SIZE])).encryptor()
    ciphertext = encryptor.update(padded) + encryptor.finalize()
    return base64.b64encode(ciphertext).decode("ascii")


class WechatMinigameChannel:
    """微信小游戏虚拟支付 2.0（道具直购）渠道适配器。"""

    def __init__(
        self,
        config: WechatMinigameConfig,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        session: aiohttp.ClientSession | None = None,
        transport: HttpTransport | None = None,
    ) -> None:
        if not config.app_id or not config.app_secret or not config.offer_id or not config.app_key:
            raise ValueError("payment channel: wechat_minigame needs app_id, app_secret, offer_id and app_key")
        if not config.push_token:
            raise ValueError("payment channel: wechat_minigame needs push_token")
        if config.env not in (0, 1):
            raise ValueError("payment channel: wechat_minigame env must be 0 (production) or 1 (sandbox)")

        self._cfg = config
        self._zone_id = config.zone_id or "1"
        self._api_base = (config.api_base or WECHAT_MINI_API_BASE).rstrip("/")
        self._clock_ms = clock_ms
        self._http = ChannelHttpClient(session=session, transport=transport)
        self._app_key_bytes = config.app_key.encode("utf-8")
        # 由 AppSecret 派生 session_key 落库加密密钥（AES-256-GCM）
        self._seal_key = hashlib.sha256(
            ("payment:wechat-minigame:session-key-seal:v1\n" + config.app_secret).encode("utf-8")
        ).digest()
        self._aes_key: bytes | None = None
        if config.encoding_aes_key:
            try:
                decoded = base64.b64decode(config.encoding_aes_key + "=", validate=True)
            except ValueError as exc:
                raise ValueError(
                    "payment channel: wechat_minigame encoding_aes_key must be the 43-character key"
                ) from exc
            if len(decoded) != 32:
                raise ValueError("payment channel: wechat_minigame encoding_aes_key must decode to 32 bytes")
            self._aes_key = decoded

        # access_token 缓存与 single-flight 合并（锁不跨外网 HTTP）
        self._access_token: str = ""
        self._token_expiry_ms: int = 0
        self._token_inflight: asyncio.Future[str] | None = None

    async def close(self) -> None:
        await self._http.close()

    @property
    def channel_id(self) -> int:
        return PAY_CHANNEL_WECHAT_MINIGAME

    @property
    def name(self) -> str:
        return CHANNEL_WECHAT_MINIGAME

    def supports(self, currency: str) -> bool:
        return currency == "CNY"

    def needs_product(self) -> bool:
        return True

    async def close_trade(self, out_trade_no: str) -> None:
        return None

    async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult:
        return REFUND_UNSUPPORTED

    def seal_session_key(self, session_key: str) -> str:
        """使用 AES-256-GCM 加密 session_key，供写入 ``payment_intent.channel_ref``。"""
        nonce = secrets.token_bytes(12)
        sealed = AESGCM(self._seal_key).encrypt(nonce, session_key.encode("utf-8"), None)
        return base64.b64encode(nonce + sealed).decode("ascii")

    def unseal_session_key(self, sealed_b64: str) -> str:
        """解密 ``payment_intent.channel_ref`` 中存放的加密 session_key。"""
        try:
            raw = base64.b64decode(sealed_b64, validate=True)
        except ValueError as exc:
            raise ValueError("payment channel: sealed session key is not base64") from exc
        if len(raw) < 12 + 16:
            raise ValueError("payment channel: sealed session key is too short")
        try:
            plain = AESGCM(self._seal_key).decrypt(raw[:12], raw[12:], None)
        except Exception as exc:
            raise ValueError("payment channel: sealed session key cannot be opened") from exc
        return plain.decode("utf-8")

    async def _code2session(self, code: str) -> tuple[str, str]:
        qs = urlencode(
            {
                "appid": self._cfg.app_id,
                "secret": self._cfg.app_secret,
                "js_code": code,
                "grant_type": "authorization_code",
            }
        )
        resp = await self._http.request("GET", f"{self._api_base}/sns/jscode2session?{qs}")
        try:
            out = json.loads(resp.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelError("payment channel: wechat_minigame code2session answer is not readable") from exc
        if not isinstance(out, dict):
            raise ChannelError("payment channel: wechat_minigame code2session answer is not an object")
        errcode = int(out.get("errcode", 0) or 0)
        if errcode in (40029, 40226, 40163):
            raise ChannelLoginRequiredError("payment channel: mini-game login code invalid or used")
        openid = str(out.get("openid", ""))
        session_key = str(out.get("session_key", ""))
        if errcode != 0 or not openid or not session_key:
            raise ChannelCallError(CHANNEL_WECHAT_MINIGAME, "code2session", resp.status, str(errcode))
        return openid, session_key

    async def prepare(self, order: ChannelOrder, cached_ref_value: str, cached_ref_ms: int) -> ChannelPrepared:
        if not order.login_code:
            raise ChannelLoginRequiredError("payment channel: wechat_minigame requires login_code")
        if not order.product_id or order.currency != "CNY":
            raise ChannelError("payment channel: wechat_minigame needs a product id and a CNY order")

        openid, session_key = await self._code2session(order.login_code)
        platform = "ios" if order.device_os == "ios" else "android"
        sign_data = json.dumps(
            {
                "mode": "goods",
                "offerId": self._cfg.offer_id,
                "buyQuantity": 1,
                "env": self._cfg.env,
                "currencyType": "CNY",
                "platform": platform,
                "zoneId": self._zone_id,
                "productId": order.product_id,
                "goodsPrice": order.amount_minor,
                "outTradeNo": order.out_trade_no,
                "attach": "",
            },
            separators=(",", ":"),
            ensure_ascii=False,
        )
        sealed = self.seal_session_key(session_key)
        return ChannelPrepared(
            ref=ChannelRef(value=sealed, at_ms=self._clock_ms()),
            payer_id=openid,
            params=ClientParams(
                wechat_minigame={
                    "sign_data": sign_data,
                    "pay_sig": hmac_sha256_hex(self._app_key_bytes, "requestMidasPaymentGameItem&" + sign_data),
                    "signature": hmac_sha256_hex(session_key.encode("utf-8"), sign_data),
                }
            ),
        )

    async def _token(self) -> str:
        now_ms = self._clock_ms()
        if self._access_token and now_ms < self._token_expiry_ms - _TOKEN_EARLY_REFRESH_MS:
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
        body = json.dumps(
            {
                "grant_type": "client_credential",
                "appid": self._cfg.app_id,
                "secret": self._cfg.app_secret,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        resp = await self._http.request(
            "POST",
            f"{self._api_base}/cgi-bin/stable_token",
            headers={"Content-Type": "application/json"},
            body=body,
        )
        try:
            out = json.loads(resp.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelCallError(CHANNEL_WECHAT_MINIGAME, "stable_token", resp.status, "") from exc
        if not isinstance(out, dict):
            raise ChannelCallError(CHANNEL_WECHAT_MINIGAME, "stable_token", resp.status, "")
        errcode = int(out.get("errcode", 0) or 0)
        token = str(out.get("access_token", ""))
        expires_in = int(out.get("expires_in", 0) or 0)
        if errcode != 0 or not token or expires_in <= 0:
            raise ChannelCallError(CHANNEL_WECHAT_MINIGAME, "stable_token", resp.status, str(errcode))
        return token, now_ms + expires_in * 1000

    async def query(self, req: ChannelQueryRequest) -> ChannelQueryResult:
        if not req.payer_id or not req.ref.value:
            raise QueryUnsupportedError("wechat_minigame: missing payer_id or sealed session_key")
        try:
            session_key = self.unseal_session_key(req.ref.value)
        except ValueError as exc:
            raise QueryUnsupportedError("wechat_minigame: cannot unseal session_key") from exc

        body_str = json.dumps(
            {
                "openid": req.payer_id,
                "offer_id": self._cfg.offer_id,
                "ts": self._clock_ms() // 1000,
                "zone_id": self._zone_id,
                "env": self._cfg.env,
                "out_trade_no": req.out_trade_no,
                "biz_id": 2,
            },
            separators=(",", ":"),
        )
        body = body_str.encode("utf-8")
        uri = "/wxa/game/queryorderinfo"

        for attempt in range(2):
            token = await self._token()
            qs = urlencode(
                {
                    "access_token": token,
                    "signature": hmac_sha256_hex(session_key.encode("utf-8"), body_str),
                    "sig_method": "hmac_sha256",
                    "pay_sig": hmac_sha256_hex(self._app_key_bytes, uri + "&" + body_str),
                }
            )
            resp = await self._http.request(
                "POST",
                f"{self._api_base}{uri}?{qs}",
                headers={"Content-Type": "application/json"},
                body=body,
            )
            try:
                out = json.loads(resp.body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ChannelError("payment channel: wechat_minigame query answer is not readable") from exc
            if not isinstance(out, dict):
                raise ChannelError("payment channel: wechat_minigame query answer is not an object")
            errcode = int(out.get("errcode", 0) or 0)
            if errcode in (40001, 42001) and attempt == 0:
                self._invalidate_token(token)
                continue
            if errcode == 90016:
                raise QueryUnsupportedError("wechat_minigame: session_key expired (90016)")
            if errcode == 90019:
                return ChannelQueryResult(state=TradeState.NOT_PAID)
            if errcode != 0:
                raise ChannelCallError(CHANNEL_WECHAT_MINIGAME, "query", resp.status, str(errcode))
            if int(out.get("pay_state", 0) or 0) != 2:
                return ChannelQueryResult(state=TradeState.NOT_PAID)
            if out.get("out_trade_no") != req.out_trade_no or req.amount_minor <= 0:
                raise ChannelError("payment channel: wechat_minigame query answer belongs to another order")
            finish_ts = int(out.get("pay_finish_time", 0) or 0)
            paid_ms = finish_ts * 1000 if finish_ts > 0 else self._clock_ms()
            return ChannelQueryResult(
                state=TradeState.PAID,
                payment=ChannelPayment(
                    transaction_id=req.out_trade_no,
                    out_trade_no=req.out_trade_no,
                    amount_minor=req.amount_minor,
                    currency="CNY",
                    paid_ms=paid_ms,
                    product_id=str(out.get("product_id", "")),
                    channel_order_id=str(out.get("transaction_id", "")),
                    test=(self._cfg.env == 1),
                ),
            )
        raise ChannelError("payment channel: wechat_minigame query retry exhausted")

    def handshake(self, query: Mapping[str, str]) -> tuple[int, bytes, str] | None:
        expected = sorted_sha1(self._cfg.push_token, query.get("timestamp", ""), query.get("nonce", ""))
        if not equal_hex(query.get("signature", ""), expected):
            return 401, b"", "text/plain; charset=utf-8"
        return 200, query.get("echostr", "").encode("utf-8"), "text/plain; charset=utf-8"

    def _decrypt_push(self, encrypted_b64: str) -> bytes:
        if self._aes_key is None:
            raise ValueError("payment channel: wechat_minigame aes key is not configured")
        try:
            sealed = base64.b64decode(encrypted_b64, validate=True)
        except ValueError as exc:
            raise ValueError("payment channel: wechat_minigame encrypted push is not base64") from exc
        if not sealed or len(sealed) % _AES_BLOCK_SIZE != 0:
            raise ValueError("payment channel: wechat_minigame encrypted push is malformed")
        decryptor = Cipher(algorithms.AES(self._aes_key), modes.CBC(self._aes_key[:_AES_BLOCK_SIZE])).decryptor()
        plain = decryptor.update(sealed) + decryptor.finalize()
        pad = plain[-1]
        if pad < 1 or pad > _WECHAT_MINI_PKCS7_BLOCK or pad > len(plain):
            raise ValueError("payment channel: wechat_minigame encrypted push has bad padding")
        if any(b != pad for b in plain[-pad:]):
            raise ValueError("payment channel: wechat_minigame encrypted push has bad padding")
        plain = plain[:-pad]
        if len(plain) < 20:
            raise ValueError("payment channel: wechat_minigame encrypted push is too short")
        size = struct.unpack(">I", plain[16:20])[0]
        if 20 + size > len(plain):
            raise ValueError("payment channel: wechat_minigame encrypted push has a bad length")
        if plain[20 + size :] != self._cfg.app_id.encode("utf-8"):
            raise ValueError("payment channel: wechat_minigame encrypted push is for another app")
        return plain[20 : 20 + size]

    async def parse_notification(
        self,
        headers: Mapping[str, str],
        query: Mapping[str, str],
        body: bytes,
    ) -> ChannelNotification:
        timestamp = query.get("timestamp", "")
        nonce = query.get("nonce", "")
        if query.get("encrypt_type") == "aes":
            try:
                envelope = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BadSignatureError("wechat_minigame: invalid encrypted envelope") from exc
            encrypted = envelope.get("Encrypt", "") if isinstance(envelope, dict) else ""
            expected = sorted_sha1(self._cfg.push_token, timestamp, nonce, str(encrypted))
            if (
                self._aes_key is None
                or not isinstance(encrypted, str)
                or not encrypted
                or not equal_hex(query.get("msg_signature", ""), expected)
            ):
                raise BadSignatureError("wechat_minigame: msg_signature mismatch")
            try:
                body = self._decrypt_push(encrypted)
            except ValueError as exc:
                raise BadSignatureError(str(exc)) from exc
        else:
            expected = sorted_sha1(self._cfg.push_token, timestamp, nonce)
            if not equal_hex(query.get("signature", ""), expected):
                raise BadSignatureError("wechat_minigame: url signature mismatch")

        try:
            push = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadSignatureError("wechat_minigame: invalid push JSON") from exc
        event = push.get("Event", "") if isinstance(push, dict) else ""
        if not isinstance(event, str) or not event:
            raise BadSignatureError("wechat_minigame: missing Event")
        if event not in ("minigame_game_pay_goods_deliver_notify", "minigame_pay_refund_succ_notify"):
            return ChannelNotification()

        minigame = push.get("MiniGame")
        if not isinstance(minigame, dict):
            raise BadSignatureError("wechat_minigame: missing MiniGame block")
        payload_str = minigame.get("Payload", "")
        pay_event_sig = minigame.get("PayEventSig", "")
        is_mock = bool(minigame.get("IsMock", False))
        if not isinstance(payload_str, str) or not isinstance(pay_event_sig, str):
            raise BadSignatureError("wechat_minigame: malformed MiniGame fields")
        expected_sig = hmac_sha256_hex(self._app_key_bytes, f"{event}&{payload_str}")
        if not equal_hex(pay_event_sig, expected_sig):
            raise BadSignatureError("wechat_minigame: PayEventSig mismatch")
        if is_mock:
            return ChannelNotification()

        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError as exc:
            raise BadSignatureError("wechat_minigame: invalid MiniGame.Payload JSON") from exc
        if not isinstance(payload, dict):
            raise BadSignatureError("wechat_minigame: MiniGame.Payload is not an object")

        if event == "minigame_pay_refund_succ_notify":
            out_trade_no = payload.get("OutTradeNo", "")
            env = payload.get("Env")
            refund_source = int(payload.get("RefundSource", 0) or 0)
            if not isinstance(out_trade_no, str) or not out_trade_no or env != self._cfg.env:
                raise BadSignatureError("wechat_minigame: refund payload invalid or env mismatch")
            return ChannelNotification(
                revocations=(
                    ChannelRevocation(
                        transaction_id=out_trade_no,
                        revoked=True,
                        observed_ms=self._clock_ms(),
                        out_trade_no=out_trade_no,
                        detail=f"REFUND_SOURCE_{refund_source}",
                    ),
                )
            )

        out_trade_no = payload.get("OutTradeNo", "")
        env = payload.get("Env")
        goods_info = payload.get("GoodsInfo")
        wx_info = payload.get("WeChatPayInfo") or {}
        if not isinstance(goods_info, dict) or not isinstance(wx_info, dict):
            raise BadSignatureError("wechat_minigame: missing GoodsInfo")
        orig_price = goods_info.get("OrigPrice", 0)
        quantity = goods_info.get("Quantity", 0)
        if (
            not isinstance(out_trade_no, str)
            or not out_trade_no
            or env != self._cfg.env
            or isinstance(orig_price, bool)
            or not isinstance(orig_price, int)
            or orig_price <= 0
            or isinstance(quantity, bool)
            or not isinstance(quantity, int)
            or quantity < 0
        ):
            raise BadSignatureError("wechat_minigame: deliver payload invalid or env mismatch")
        effective_qty = quantity if quantity > 0 else 1
        anomaly = f"wechat_minigame goods quantity is {effective_qty}" if effective_qty != 1 else ""
        return ChannelNotification(
            payment=ChannelPayment(
                transaction_id=out_trade_no,
                out_trade_no=out_trade_no,
                amount_minor=orig_price * effective_qty,
                currency="CNY",
                paid_ms=self._clock_ms(),
                product_id=str(goods_info.get("ProductId", "")),
                channel_order_id=str(wx_info.get("TransactionId", "")),
                test=(env == 1),
                anomaly=anomaly,
            )
        )

    def format_ack(self, kind: AckKind) -> tuple[int, bytes, str]:
        if kind == AckKind.OK:
            return 200, b'{"ErrCode":0,"ErrMsg":"Success"}', "application/json"
        return 200, b'{"ErrCode":99999,"ErrMsg":"internal error"}', "application/json"


# ---------------------------------------------------------------------------
# 抖音小游戏支付（道具直购 goodType=2）
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DouyinMinigameConfig:
    """抖音小游戏支付渠道配置。"""

    app_id: str
    push_token: str
    zone_id: str = "1"
    good_name: str = ""


class DouyinMinigameChannel:
    """抖音小游戏道具直购渠道适配器。"""

    def __init__(self, config: DouyinMinigameConfig) -> None:
        if not config.app_id or not config.push_token:
            raise ValueError("payment channel: douyin_minigame needs app_id and push_token")
        self._cfg = config
        self._zone_id = config.zone_id or "1"

    @property
    def channel_id(self) -> int:
        return PAY_CHANNEL_DOUYIN_MINIGAME

    @property
    def name(self) -> str:
        return CHANNEL_DOUYIN_MINIGAME

    def supports(self, currency: str) -> bool:
        return currency == "CNY"

    def needs_product(self) -> bool:
        return False

    async def prepare(self, order: ChannelOrder, cached_ref_value: str, cached_ref_ms: int) -> ChannelPrepared:
        if order.currency != "CNY":
            raise ChannelError("payment channel: douyin_minigame only takes CNY")
        good_name = order.title or self._cfg.good_name or "游戏道具"
        if len(good_name) > 10:
            good_name = good_name[:10]
        return ChannelPrepared(
            params=ClientParams(
                douyin_minigame={
                    "mode": "game",
                    "env": 0,
                    "currency_type": "CNY",
                    "platform": "android",
                    "buy_quantity": 1,
                    "zone_id": self._zone_id,
                    "custom_id": order.out_trade_no,
                    "extra_info": "",
                    "good_type": 2,
                    "order_amount": order.amount_minor,
                    "good_name": good_name,
                }
            )
        )

    async def query(self, req: ChannelQueryRequest) -> ChannelQueryResult:
        raise QueryUnsupportedError("douyin_minigame: active query is not supported")

    async def close_trade(self, out_trade_no: str) -> None:
        return None

    async def refund(self, req: ChannelRefundRequest) -> ChannelRefundResult:
        return REFUND_UNSUPPORTED

    def handshake(self, query: Mapping[str, str]) -> tuple[int, bytes, str] | None:
        echostr = query.get("echostr", "")
        expected = sorted_sha1(
            self._cfg.push_token,
            query.get("timestamp", ""),
            query.get("nonce", ""),
            query.get("msg", ""),
        )
        if not echostr or not equal_hex(query.get("signature", ""), expected):
            return 401, b"", "text/plain; charset=utf-8"
        return 200, echostr.encode("utf-8"), "text/plain; charset=utf-8"

    async def parse_notification(
        self,
        headers: Mapping[str, str],
        query: Mapping[str, str],
        body: bytes,
    ) -> ChannelNotification:
        try:
            envelope = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadSignatureError("douyin_minigame: invalid JSON envelope") from exc
        if not isinstance(envelope, dict):
            raise BadSignatureError("douyin_minigame: envelope is not an object")
        timestamp = envelope.get("timestamp", "")
        nonce = envelope.get("nonce", "")
        msg_raw = envelope.get("msg", "")
        signature = envelope.get("signature", "")
        event_type = envelope.get("type", "")
        if (
            not isinstance(timestamp, str)
            or not timestamp
            or not isinstance(nonce, str)
            or not nonce
            or not isinstance(msg_raw, str)
            or not msg_raw
            or not isinstance(signature, str)
        ):
            raise BadSignatureError("douyin_minigame: missing required envelope fields")

        expected = sorted_sha1(self._cfg.push_token, timestamp, nonce, msg_raw)
        if not equal_hex(signature, expected):
            raise BadSignatureError("douyin_minigame: signature mismatch")

        if event_type and event_type != "payment":
            return ChannelNotification()

        try:
            msg = json.loads(msg_raw)
        except json.JSONDecodeError as exc:
            raise BadSignatureError("douyin_minigame: msg is not valid JSON") from exc
        if not isinstance(msg, dict):
            raise BadSignatureError("douyin_minigame: msg is not an object")
        cp_orderno = msg.get("cp_orderno", "")
        amount_cent = msg.get("amount_cent", 0)
        if (
            msg.get("appid") != self._cfg.app_id
            or not isinstance(cp_orderno, str)
            or not cp_orderno
            or isinstance(amount_cent, bool)
            or not isinstance(amount_cent, int)
            or amount_cent <= 0
        ):
            raise BadSignatureError("douyin_minigame: appid/cp_orderno/amount_cent invalid")

        currency = str(msg.get("currency") or "CNY")
        return ChannelNotification(
            payment=ChannelPayment(
                transaction_id=cp_orderno,
                out_trade_no=cp_orderno,
                amount_minor=amount_cent,
                currency=currency,
                paid_ms=time_from_unix_text_ms(timestamp),
                channel_order_id=str(msg.get("order_no_channel", "")),
            )
        )

    def format_ack(self, kind: AckKind) -> tuple[int, bytes, str]:
        if kind == AckKind.OK:
            return 200, b'{"err_no":0,"err_tips":"success"}', "application/json"
        return 500, b'{"err_no":1,"err_tips":"failed"}', "application/json"
