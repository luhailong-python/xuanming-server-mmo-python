"""官方商城与人民币寄售 HTTP HMAC 适配器及后台拉取/付款撤销状态机。

严格对齐 Go 侧 ``trade/internal/commerce/{mall.go,official_reversal.go,rmb_mall.go}``：
- 双角色独立密钥（``COMMERCE_GATEWAY_SECRET`` / ``COMMERCE_DELIVERY_SECRET``，均 >= 32 字节且互不相同）；
- v1 规范串 ``v1\\n<role>\\n<subject>\\n<ts>\\n<nonce>\\n<method>\\n<path>\\n<sha256_hex(body)>``，每次尝试生成 16 字节随机 hex nonce；
- 官方订单发货（``TRADE_CREDIT``）与渠道付款撤销扣回（``TRADE_DEBIT`` + ``TX_PAYMENT_REVERSAL``）及回执上报；
- 人民币寄售的商城登记、查询、列表、购买、订单管理、收益列表与提现 HTTP 契约适配。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import aiohttp
from google.protobuf.message import Message

from ..protocol import Protocol
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_TRADE_ORDER_NOT_FOUND,
    ERR_TRADE_ORDER_STATE_CONFLICT,
    ERR_TRADE_REQUEST_CONFLICT,
    RoleFenceStaleError,
    RolePreparingError,
    StoreUnavailableError,
    TradeRejection,
    TradeRoleConflictError,
)
from .model import (
    ASSET_STREAM_TRADE_CREDIT,
    ASSET_STREAM_TRADE_DEBIT,
    COMMERCE_AWAITING_PAYMENT,
    COMMERCE_CANCELLED,
    COMMERCE_DELIVERED,
    COMMERCE_DELIVERING,
    COMMERCE_KIND_OFFICIAL,
    COMMERCE_KIND_RMB_CONSIGNMENT,
    COMMERCE_MANUAL,
    COMMERCE_REFUNDED,
    COMMERCE_REFUND_PENDING,
    COMMERCE_REVERSED,
    COMMERCE_REVERSING,
    COMMERCE_SETTLED,
    INT64_MAX,
    MAX_RMB_PRICE_FEN,
    OFFICIAL_CREDIT_ROLE,
    OFFICIAL_REGRANT_PREFIX,
    OFFICIAL_REVERSE_PREFIX,
    QUARANTINE_DETAIL_MAX_BYTES,
    STAGE_OFFICIAL_DELIVER,
    STAGE_OFFICIAL_REVERSED,
    RoleOriginal,
    is_known_currency,
    request_key,
    truncate_utf8,
    valid_account,
)

logger = logging.getLogger(__name__)

# 商城契约限值与正则
MIN_MALL_SECRET_BYTES = 32
MAX_MALL_RESPONSE_BYTES = 256 * 1024
MALL_POLL_LIMIT = 20
DEFAULT_MALL_TIMEOUT_SEC = 2.5

MALL_ORDER_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

MANUAL_REASON_PAYMENT_REVERSAL = "payment_reversal_failed"
_MANUAL_REASON_OWNER_CHANGED = "owner_changed"

_MALL_ORDER_STATES: dict[str, int] = {
    "AWAITING_PAYMENT": COMMERCE_AWAITING_PAYMENT,
    "DELIVERY_PENDING": COMMERCE_DELIVERING,
    "DELIVERED": COMMERCE_DELIVERED,
    "CANCELLED": COMMERCE_CANCELLED,
    "REFUND_PENDING": COMMERCE_REFUND_PENDING,
    "REFUNDED": COMMERCE_REFUNDED,
    "MANUAL": COMMERCE_MANUAL,
    "REVERSING": COMMERCE_REVERSING,
    "REVERSED": COMMERCE_REVERSED,
}

# 同一 payment_revision 内生命周期排秩，确保版本号 (payment_revision << 4) | rank 跨撤销/补发严格单调递增
_MALL_ORDER_STATE_RANKS: dict[str, int] = {
    "AWAITING_PAYMENT": 1,
    "DELIVERY_PENDING": 2,
    "CANCELLED": 3,
    "REVERSING": 3,
    "REFUND_PENDING": 4,
    "DELIVERED": 5,
    "REVERSED": 5,
    "REFUNDED": 5,
    "MANUAL": 6,
}

_RMB_ASSET_KINDS = frozenset(("ITEMS", "PET", "CURRENCY", "CHARACTER"))
_RMB_LISTING_STATES = frozenset(("LISTED", "LOCKED", "SOLD", "RETURNING", "RETURNED", "MANUAL"))
_RMB_WITHDRAWAL_STATES = frozenset(("REQUESTED", "PAID", "FAILED"))


def _as_secret_bytes(secret: bytes | str) -> bytes:
    if isinstance(secret, bytes):
        return secret
    return secret.encode("utf-8")


def commerce_signature(
    secret: bytes | str,
    role: str,
    subject: str,
    timestamp: str | int,
    nonce: str,
    method: str,
    path: str,
    body: bytes | bytearray | None = None,
) -> str:
    """计算商城 X-Commerce-Signature v1 HMAC-SHA256 签名（与 Go / Java 三边黄金样例逐字一致）。"""
    body_bytes = b"" if body is None else bytes(body)
    body_hash = hashlib.sha256(body_bytes).hexdigest()
    canonical = "\n".join(
        ("v1", str(role), str(subject), str(timestamp), str(nonce), str(method), str(path), body_hash)
    ).encode("utf-8")
    return hmac.digest(_as_secret_bytes(secret), canonical, "sha256").hex()


def verify_commerce_signature(
    secret: bytes | str,
    role: str,
    subject: str,
    timestamp: str | int,
    nonce: str,
    method: str,
    path: str,
    body: bytes | bytearray | None,
    signature_hex: str,
) -> bool:
    """常量时间校验商城 X-Commerce-Signature v1 签名。"""
    if not signature_hex or len(signature_hex) != 64 or not signature_hex.isascii():
        return False
    expected = commerce_signature(secret, role, subject, timestamp, nonce, method, path, body)
    return hmac.compare_digest(expected, signature_hex)


def canonical_uint64(raw: Any) -> int:
    """严格解析规范无前导零正 uint64 字符串（与 Go canonicalID 对齐）。"""
    if not isinstance(raw, str) or not raw.isdigit():
        raise ValueError(f"mall id is not canonical uint64: {raw!r}")
    val = int(raw)
    if val <= 0 or val >= (1 << 64) or str(val) != raw:
        raise ValueError(f"mall id is not canonical uint64: {raw!r}")
    return val


def validate_mall_url(base_url: str) -> str:
    """校验商城基地址仅含 scheme + host[:port]，拒绝凭据、查询串、片段与路径前缀。"""
    parsed = urlsplit(base_url)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
    ):
        raise ValueError(f"invalid mall base url: {base_url!r}")
    return base_url.rstrip("/")


# -- 商城交付/撤销分页复合游标校验（与 Go validateMallPage / parseMallCursor 一致） --------------------------

@dataclass(frozen=True, slots=True)
class MallScanCursor:
    """商城稳定扫描复合游标：v1:<afterMs>:<afterId>:<upperMs>:<upperId>。"""

    after_ms: int
    after_id: str
    upper_ms: int
    upper_id: str


def parse_mall_cursor(raw: str) -> MallScanCursor:
    fields = raw.split(":")
    if (
        len(raw) > 256
        or len(fields) != 5
        or fields[0] != "v1"
        or not MALL_ORDER_ID_RE.match(fields[2])
        or not MALL_ORDER_ID_RE.match(fields[4])
    ):
        raise ValueError(f"invalid mall scan cursor: {raw!r}")
    try:
        after_ms = int(fields[1])
        upper_ms = int(fields[3])
    except ValueError as exc:
        raise ValueError(f"invalid mall cursor timestamp: {raw!r}") from exc
    if (
        after_ms < 0
        or str(after_ms) != fields[1]
        or upper_ms < 0
        or str(upper_ms) != fields[3]
        or after_ms > upper_ms
        or (after_ms == upper_ms and fields[2] > fields[4])
    ):
        raise ValueError(f"invalid mall cursor bounds: {raw!r}")
    return MallScanCursor(after_ms=after_ms, after_id=fields[2], upper_ms=upper_ms, upper_id=fields[4])


def validate_mall_page(entries: Any, requested_cursor: str, *, order_id_getter: Callable[[dict], Any]) -> None:
    """整页校验商城轮询页结构、游标格式、单调递增与上界稳定性。"""
    if not isinstance(entries, list) or len(entries) > MALL_POLL_LIMIT:
        raise ValueError("invalid mall poll page entries")
    previous: MallScanCursor | None = parse_mall_cursor(requested_cursor) if requested_cursor else None
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError("mall poll entry is not an object")
        intent = entry.get("intent")
        cursor_raw = entry.get("cursor")
        if not isinstance(intent, dict) or not isinstance(cursor_raw, str):
            raise ValueError("mall poll entry missing intent or cursor")
        order_id = order_id_getter(intent)
        if not isinstance(order_id, str) or not MALL_ORDER_ID_RE.match(order_id):
            raise ValueError(f"mall poll entry has invalid orderId: {order_id!r}")
        if cursor_raw == "":
            if idx != len(entries) - 1:
                raise ValueError("mall poll page terminated cursor early")
            continue
        cur = parse_mall_cursor(cursor_raw)
        if cur.after_id != order_id:
            raise ValueError("mall poll cursor does not match entry orderId")
        if previous is not None and (
            cur.upper_ms != previous.upper_ms
            or cur.upper_id != previous.upper_id
            or cur.after_ms < previous.after_ms
            or (cur.after_ms == previous.after_ms and cur.after_id <= previous.after_id)
        ):
            raise ValueError("mall poll cursor regressed or upper bound changed")
        previous = cur


# -- 官方商城发放包与付款撤销纯状态机（与 Go official_reversal.go 逐行对齐） ----------------------------------

def official_bundle(protocol: Protocol, intent: dict[str, Any]) -> tuple[Message | None, bool]:
    """将商城发放意图转为 AssetBundle：物品或货币二选一，越界/混装/全空均拒绝。"""
    cfg = int(intent.get("itemConfigId", 0) or 0)
    cnt = int(intent.get("itemCount", 0) or 0)
    cur_type = int(intent.get("currencyType", 0) or 0)
    cur_amount = int(intent.get("currencyAmount", 0) or 0)
    if cfg != 0 and cnt != 0 and 0 < cnt <= 4096 and cur_type == 0 and cur_amount == 0:
        bundle = protocol.message("AssetBundle")
        grant = bundle.items.add()
        grant.config_id = cfg
        grant.count = cnt
        return bundle, True
    if cfg == 0 and cnt == 0 and 0 < cur_amount <= INT64_MAX and is_known_currency(cur_type):
        bundle = protocol.message("AssetBundle")
        cur = bundle.currencies.add()
        cur.currency_type = cur_type
        cur.amount = cur_amount
        return bundle, True
    return None, False


def _find_leg(w: Message, role: str) -> Message | None:
    for item in w.legs:
        if item.role == role:
            return item
    return None


def _current_leg(w: Message, role: str) -> Message | None:
    cur = _find_leg(w, role)
    seen: set[str] = set()
    while cur is not None and cur.superseded_by:
        if cur.superseded_by in seen:
            break
        seen.add(cur.superseded_by)
        nxt = _find_leg(w, cur.superseded_by)
        if nxt is None:
            break
        cur = nxt
    return cur


def _add_leg(w: Message, role: str, player_id: int, stream: int, bundle: Message) -> Message:
    existing = _find_leg(w, role)
    if existing is not None:
        return existing
    leg = w.legs.add()
    leg.role = role
    leg.player_id = int(player_id)
    leg.stream = int(stream)
    leg.bundle.CopyFrom(bundle)
    return leg


def official_held(w: Message) -> tuple[bool, bool]:
    """按未取代腿的结局计算买家当前是否持有官方发放，以及是否已全部收敛。"""
    held = False
    for leg in w.legs:
        if leg.superseded_by:
            continue
        if int(leg.outcome) == 0:
            return False, False
        if int(leg.outcome) == 2:  # OP_STATUS_APPLIED
            held = int(leg.stream) == ASSET_STREAM_TRADE_CREDIT
    return held, True


def plan_official(rec: Message, w: Message, now_ms: int) -> None:
    """按最新付款修订号收敛官方订单资产发放/扣回/补发（仅在 official_deliver 阶段调用）。"""
    held, settled = official_held(w)
    if not settled:
        return
    entitled = not bool(w.payment_revoked)
    if held == entitled:
        if entitled:
            w.stage = "done"
            rec.status = COMMERCE_SETTLED
        else:
            w.stage = STAGE_OFFICIAL_REVERSED
            rec.status = COMMERCE_CANCELLED
        return

    rev = int(w.payment_revision)
    role = OFFICIAL_CREDIT_ROLE
    stream = ASSET_STREAM_TRADE_CREDIT
    if not entitled:
        role = f"{OFFICIAL_REVERSE_PREFIX}{rev}"
        stream = ASSET_STREAM_TRADE_DEBIT
    elif _find_leg(w, OFFICIAL_CREDIT_ROLE) is not None and rev > 0:
        role = f"{OFFICIAL_REGRANT_PREFIX}{rev}"

    if _find_leg(w, role) is None:
        _add_leg(w, role, int(rec.buyer_player_id), stream, w.official_bundle)
        return

    if role == OFFICIAL_CREDIT_ROLE and rev == 0:
        w.stage = "official_refund"
        rec.status = COMMERCE_REFUND_PENDING
        return

    if w.stage != "manual":
        w.quarantine.stage = w.stage
        w.quarantine.status = int(rec.status)
        w.quarantine.reason = MANUAL_REASON_PAYMENT_REVERSAL
        w.quarantine.detail = truncate_utf8(
            f"leg {role} failed for payment_revision={rev}", QUARANTINE_DETAIL_MAX_BYTES
        )
        w.quarantine.at_ms = int(now_ms)
    rec.status = COMMERCE_MANUAL
    w.stage = "manual"


def official_delivery_state(rec: Message, w: Message) -> str:
    """计算回报商城交付待办的终态结论；未收敛时返回空串。"""
    status = int(rec.status)
    if status == COMMERCE_SETTLED:
        return "DELIVERED"
    if status == COMMERCE_REFUND_PENDING:
        return "REFUND_PENDING"
    if status == COMMERCE_MANUAL:
        return "MANUAL"
    if status == COMMERCE_CANCELLED:
        credit = _current_leg(w, OFFICIAL_CREDIT_ROLE)
        if credit is not None and int(credit.outcome) == 2:
            return "DELIVERED"
        return "VOIDED"
    return ""


def official_reversal_state(rec: Message, w: Message) -> str:
    """计算回报商城扣回待办的终态结论；未收敛到当前修订号时返回空串。"""
    status = int(rec.status)
    if status == COMMERCE_MANUAL:
        return "MANUAL"
    if status == COMMERCE_SETTLED and not bool(w.payment_revoked):
        return "DELIVERED"
    if status == COMMERCE_CANCELLED and bool(w.payment_revoked):
        return "REVERSED"
    return ""


# -- 订单 / 寄售视图投影与严格应答校验（与 Go mall.go / rmb_mall.go 一致） -------------------------------------

def mall_order_answer(protocol: Protocol, data: dict[str, Any], now_ms: int) -> Message:
    """校验商城订单 JSON 并投影为 CommerceOrderResponse（版本号跨撤销/补发严格单调递增）。"""
    if not isinstance(data, dict):
        raise StoreUnavailableError("mall order response is not an object")
    state_name = data.get("state")
    state_enum = _MALL_ORDER_STATES.get(state_name)
    order_id = data.get("orderId")
    buyer_raw = data.get("buyerId")
    if (
        state_enum is None
        or not isinstance(order_id, str)
        or not MALL_ORDER_ID_RE.match(order_id)
        or not isinstance(buyer_raw, str)
        or not buyer_raw.isdigit()
    ):
        raise StoreUnavailableError("invalid mall order response fields")
    buyer_id = int(buyer_raw)
    if buyer_id <= 0:
        raise StoreUnavailableError("invalid mall order buyerId")
    payment_rev = int(data.get("paymentRevision", 0) or 0)
    if payment_rev < 0:
        raise StoreUnavailableError("negative paymentRevision in mall order")
    version = (payment_rev << 4) | _MALL_ORDER_STATE_RANKS[state_name]
    sku_id = int(data.get("skuId", 0) or 0)
    amount = int(data.get("amountMinor", 0) or 0)
    currency = str(data.get("currency", ""))
    resp = protocol.message("trade.CommerceOrderResponse", server_now_ms=int(now_ms))
    resp.order.order_id = order_id
    resp.order.kind = COMMERCE_KIND_OFFICIAL
    resp.order.state = state_enum
    resp.order.version = version
    resp.order.buyer_player_id = buyer_id
    resp.order.amount = amount
    resp.order.currency = currency
    resp.order.related_id = str(sku_id)
    return resp


def rmb_order_answer(protocol: Protocol, data: dict[str, Any], now_ms: int) -> Message:
    """校验人民币寄售订单 JSON（skuId=0, quantity=1, currency='CNY', amount>0）。"""
    resp = mall_order_answer(protocol, data, now_ms)
    sku = int(data.get("skuId", 0) or 0)
    qty = int(data.get("quantity", 0) or 0)
    amount = int(data.get("amountMinor", 0) or 0)
    currency = str(data.get("currency", ""))
    if sku != 0 or qty != 1 or currency != "CNY" or amount <= 0:
        raise StoreUnavailableError("invalid rmb order terms from mall")
    resp.order.kind = COMMERCE_KIND_RMB_CONSIGNMENT
    return resp


def parse_rmb_listing_view(protocol: Protocol, data: dict[str, Any]) -> Message:
    """严格校验商城返回的人民币寄售视图（与 Go rmbListing.view 对齐）。"""
    if not isinstance(data, dict):
        raise StoreUnavailableError("rmb listing response is not an object")
    try:
        listing_id = canonical_uint64(data.get("listingId"))
        seller_id = canonical_uint64(data.get("sellerPlayerId"))
    except ValueError as exc:
        raise StoreUnavailableError(str(exc)) from exc
    zone = int(data.get("marketZone", 0) or 0)
    price = int(data.get("priceFen", 0) or 0)
    fee = int(data.get("feeBps", 0) or 0)
    cooling = int(data.get("coolingMs", 0) or 0)
    notice_end = int(data.get("noticeEndMs", 0) or 0)
    sale_end = int(data.get("saleEndMs", 0) or 0)
    kind = str(data.get("assetKind", ""))
    title = str(data.get("title", ""))
    state = str(data.get("state", ""))
    phase = str(data.get("phase", ""))
    order_id = str(data.get("orderId", "") or "")
    if (
        zone <= 0
        or price <= 0
        or price > MAX_RMB_PRICE_FEN
        or fee < 0
        or fee > 10000
        or notice_end <= 0
        or sale_end <= notice_end
        or not title
        or kind not in _RMB_ASSET_KINDS
        or state not in _RMB_LISTING_STATES
        or (order_id and not MALL_ORDER_ID_RE.match(order_id))
    ):
        raise StoreUnavailableError("invalid rmb listing fields from mall")
    return protocol.message(
        "trade.RmbConsignmentView",
        listing_id=listing_id,
        seller_player_id=seller_id,
        market_zone=zone,
        asset_kind=kind,
        title=title,
        price_fen=price,
        fee_bps=fee,
        cooling_ms=cooling,
        notice_end_ms=notice_end,
        sale_end_ms=sale_end,
        state=state,
        phase=phase,
        order_id=order_id,
        designated_only=bool(str(data.get("designatedBuyerAccount", "") or "")),
    )


# -- MallAdapter（官方商城 + 人民币寄售 HTTP 客户端） ---------------------------------------------------------

class MallAdapter:
    """官方商城与人民币寄售双密钥 HTTP HMAC 适配器。"""

    def __init__(
        self,
        protocol: Protocol,
        base_url: str,
        gateway_secret: bytes | str,
        delivery_secret: bytes | str,
        *,
        timeout_sec: float = DEFAULT_MALL_TIMEOUT_SEC,
        session: aiohttp.ClientSession | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.base_url = validate_mall_url(base_url)
        gw = _as_secret_bytes(gateway_secret)
        dl = _as_secret_bytes(delivery_secret)
        if len(gw) < MIN_MALL_SECRET_BYTES or len(dl) < MIN_MALL_SECRET_BYTES or gw == dl:
            raise ValueError("mall requires two distinct HMAC secrets of at least 32 bytes each")
        self._gateway_secret = gw
        self._delivery_secret = dl
        self._timeout_sec = timeout_sec
        self._session = session
        self._owns_session = session is None
        self._clock_ms = clock_ms

    def _now_ms(self) -> int:
        return int(self._clock_ms())

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._timeout_sec)
            connector = aiohttp.TCPConnector(limit=64, limit_per_host=32, keepalive_timeout=90.0)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()

    async def call(
        self,
        role: str,
        subject: str,
        method: str,
        path: str,
        payload: Any = None,
        *,
        expect_json: bool = True,
    ) -> Any:
        """向商城发起带 v1 HMAC 签名的 HTTP 请求，每次尝试使用独立 16 字节随机 hex nonce。"""
        if role not in ("gateway", "delivery"):
            raise ValueError(f"unsupported mall auth role: {role!r}")
        body = b""
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        stamp = str(self._now_ms())
        nonce = secrets.token_bytes(16).hex()
        secret = self._delivery_secret if role == "delivery" else self._gateway_secret
        sig = commerce_signature(secret, role, subject, stamp, nonce, method, path, body)
        headers = {
            "Content-Type": "application/json",
            "X-Commerce-Subject": subject,
            "X-Commerce-Timestamp": stamp,
            "X-Commerce-Nonce": nonce,
            "X-Commerce-Signature": sig,
        }
        session = self._get_session()
        try:
            async with session.request(
                method,
                self.base_url + path,
                data=body if body else None,
                headers=headers,
                allow_redirects=False,
            ) as resp:
                raw = await resp.content.read(MAX_MALL_RESPONSE_BYTES + 1)
                status = resp.status
        except TradeRejection:
            raise
        except Exception as exc:
            raise StoreUnavailableError(f"mall HTTP request failed path={path}: {exc}") from exc

        if len(raw) > MAX_MALL_RESPONSE_BYTES:
            raise StoreUnavailableError(f"mall response exceeds {MAX_MALL_RESPONSE_BYTES} bytes path={path}")
        if status != 200:
            if status == 400:
                raise TradeRejection(ERR_INVALID_PARAMETER, f"mall rejected request (400) path={path}")
            if status == 404:
                raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, f"mall resource not found (404) path={path}")
            if status == 409:
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, f"mall state conflict (409) path={path}")
            raise StoreUnavailableError(f"mall request failed status={status} path={path}")
        if not expect_json:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise StoreUnavailableError(f"mall returned invalid JSON path={path}: {exc}") from exc

    # -- 官方商城面向玩家网关 API（role="gateway", subject=str(player_id)） ----------------------------------

    async def list_official_skus(self, player_id: int, after_sku_id: int = 0, limit: int = 20) -> Message:
        clamped = min(max(int(limit), 1), 100)
        data = await self.call(
            "gateway",
            str(player_id),
            "POST",
            "/api/skus/list",
            {"afterSkuId": int(after_sku_id), "limit": clamped},
        )
        if not isinstance(data, dict) or not isinstance(data.get("skus", []), list):
            raise StoreUnavailableError("invalid skus response from mall")
        out = self.protocol.message("trade.ListOfficialSkusResponse")
        for item in data.get("skus", []):
            sku_id = int(item.get("skuId", 0) or 0)
            if sku_id <= int(after_sku_id):
                continue
            view = out.skus.add()
            view.sku_id = sku_id
            view.title = str(item.get("title", ""))
            view.price_minor = int(item.get("priceMinor", 0) or 0)
            view.currency = str(item.get("currency", ""))
            view.available = int(item.get("available", 0) or 0)
            out.next_sku_id = sku_id
            if len(out.skus) >= clamped:
                break
        return out

    async def create_official_order(self, original: RoleOriginal, req: Message) -> Message:
        player = original.player_id
        sku_id = int(req.sku_id)
        qty = int(req.quantity)
        req_id = str(req.client_request_id)
        if player <= 0 or sku_id <= 0 or qty <= 0 or qty > 4096 or not 8 <= len(req_id) <= 80:
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid official order parameters")
        data = await self.call(
            "gateway",
            str(player),
            "POST",
            "/api/orders",
            {
                "requestId": req_id,
                "skuId": sku_id,
                "quantity": qty,
                "buyerAccount": original.account,
                "buyerOwnerRevision": str(original.owner_revision),
            },
        )
        if not isinstance(data, dict) or data.get("buyerId") != str(player):
            raise StoreUnavailableError("mall order buyer mismatch")
        return mall_order_answer(self.protocol, data, self._now_ms())

    async def list_official_orders(
        self, original: RoleOriginal, after_order_id: str = "", limit: int = 20
    ) -> list[Message]:
        player = original.player_id
        clamped = min(max(int(limit), 1), 100)
        data = await self.call(
            "gateway",
            str(player),
            "POST",
            "/api/orders/list",
            {
                "afterOrderId": str(after_order_id),
                "limit": clamped,
                "buyerAccount": original.account,
                "buyerOwnerRevision": str(original.owner_revision),
            },
        )
        if not isinstance(data, list):
            raise StoreUnavailableError("invalid mall orders list response")
        out: list[Message] = []
        now = self._now_ms()
        for item in data:
            if not isinstance(item, dict) or item.get("buyerId") != str(player):
                raise StoreUnavailableError("mall order buyer mismatch in list")
            ans = mall_order_answer(self.protocol, item, now)
            out.append(ans.order)
        return out

    async def get_or_cancel_official_order(
        self, original: RoleOriginal, order_id: str, *, cancel: bool = False
    ) -> Message:
        if not MALL_ORDER_ID_RE.match(order_id):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid official order_id")
        player = original.player_id
        path = "/api/orders/cancel" if cancel else "/api/orders/query"
        data = await self.call(
            "gateway",
            str(player),
            "POST",
            path,
            {
                "orderId": order_id,
                "buyerAccount": original.account,
                "buyerOwnerRevision": str(original.owner_revision),
            },
        )
        if not isinstance(data, dict) or data.get("buyerId") != str(player):
            raise StoreUnavailableError("mall order buyer mismatch")
        return mall_order_answer(self.protocol, data, self._now_ms())

    # -- 人民币寄售商城内部 API（role="delivery", subject="trade"） -------------------------------------------

    async def consignment_call(self, action: str, payload: dict[str, Any], *, expect_json: bool = True) -> Any:
        return await self.call(
            "delivery",
            "trade",
            "POST",
            f"/internal/consignments/{action}",
            payload,
            expect_json=expect_json,
        )

    async def rmb_get(self, listing_id: int) -> Message:
        if listing_id <= 0:
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid listing_id")
        data = await self.consignment_call("get", {"listingId": str(listing_id)})
        view = parse_rmb_listing_view(self.protocol, data)
        if int(view.listing_id) != listing_id:
            raise StoreUnavailableError("mall returned mismatched rmb listing_id")
        return view

    async def rmb_list(self, after: str = "", limit: int = 20) -> Message:
        if after:
            try:
                canonical_uint64(after)
            except ValueError as exc:
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid rmb list cursor") from exc
        clamped = min(max(int(limit), 1), 100)
        data = await self.consignment_call("list", {"afterListingId": after, "limit": clamped})
        if not isinstance(data, dict) or not isinstance(data.get("listings"), list) or len(data["listings"]) > clamped:
            raise StoreUnavailableError("invalid rmb listings page from mall")
        out = self.protocol.message("trade.ListRmbConsignmentsResponse")
        previous = after
        for item in data["listings"]:
            view = parse_rmb_listing_view(self.protocol, item)
            raw_id = str(item.get("listingId", ""))
            if raw_id <= previous:
                raise StoreUnavailableError("rmb listings cursor order violation")
            previous = raw_id
            out.listings.add().CopyFrom(view)
            out.next_cursor = raw_id
        return out

    async def rmb_register(self, rec: Message, w: Message) -> Message:
        t = w.rmb
        payload: dict[str, Any] = {
            "listingId": str(int(rec.commerce_id)),
            "sellerPlayerId": str(int(rec.seller_player_id)),
            "sellerAccount": str(t.seller_account),
            "marketZone": int(t.market_zone),
            "assetKind": str(t.asset_kind),
            "escrowReference": str(t.escrow_reference),
            "escrowProof": str(t.escrow_proof),
            "priceFen": int(t.price_fen),
            "title": str(t.title),
            "assetCurrencyType": int(w.seller_assets.currency_type) if str(t.asset_kind) == "CURRENCY" else None,
        }
        if str(t.designated_buyer_account):
            payload["designatedBuyerAccount"] = str(t.designated_buyer_account)
        data = await self.consignment_call("register", payload)
        view = parse_rmb_listing_view(self.protocol, data)
        if (
            int(view.listing_id) != int(rec.commerce_id)
            or int(view.seller_player_id) != int(rec.seller_player_id)
            or int(view.market_zone) != int(t.market_zone)
            or int(view.price_fen) != int(t.price_fen)
            or str(view.title) != str(t.title)
            or str(view.asset_kind) != str(t.asset_kind)
        ):
            raise StoreUnavailableError("mall registered rmb listing terms mismatch")
        return view

    async def rmb_cancel(self, account: str, listing_id: int) -> Message:
        data = await self.consignment_call(
            "cancel", {"listingId": str(listing_id), "sellerAccount": account}
        )
        view = parse_rmb_listing_view(self.protocol, data)
        if int(view.listing_id) != listing_id:
            raise StoreUnavailableError("mall cancel returned mismatched rmb listing_id")
        return view

    async def rmb_buy(self, account: str, player_id: int, owner_revision: int, req: Message) -> Message:
        listing_id = int(req.listing_id)
        req_id = str(req.client_request_id)
        if listing_id <= 0 or not 8 <= len(req_id) <= 80:
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid rmb buy parameters")
        data = await self.consignment_call(
            "buy",
            {
                "listingId": str(listing_id),
                "buyerPlayerId": str(player_id),
                "buyerAccount": account,
                "buyerOwnerRevision": str(owner_revision),
                "requestId": req_id,
            },
        )
        if not isinstance(data, dict) or data.get("buyerId") != str(player_id):
            raise StoreUnavailableError("mall rmb buy returned mismatched buyerId")
        return rmb_order_answer(self.protocol, data, self._now_ms())

    async def rmb_order(self, account: str, order_id: str, *, cancel: bool = False) -> Message:
        if not MALL_ORDER_ID_RE.match(order_id):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid rmb order_id")
        action = "orders/cancel" if cancel else "orders/get"
        data = await self.consignment_call(action, {"account": account, "orderId": order_id})
        if not isinstance(data, dict) or data.get("orderId") != order_id:
            raise StoreUnavailableError("mall returned mismatched rmb orderId")
        return rmb_order_answer(self.protocol, data, self._now_ms())

    async def rmb_orders(self, account: str, after: str = "", limit: int = 20) -> Message:
        if after and not MALL_ORDER_ID_RE.match(after):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid rmb orders cursor")
        clamped = min(max(int(limit), 1), 100)
        data = await self.consignment_call(
            "orders/list", {"account": account, "afterOrderId": after, "limit": clamped}
        )
        if not isinstance(data, list) or len(data) > clamped:
            raise StoreUnavailableError("invalid rmb orders page from mall")
        out = self.protocol.message("trade.ListCommerceOrdersResponse")
        now = self._now_ms()
        for item in data:
            ans = rmb_order_answer(self.protocol, item, now)
            out.orders.add().CopyFrom(ans.order)
            out.next_cursor = str(item["orderId"])
        return out

    async def rmb_incomes(self, account: str, after: str = "", limit: int = 20) -> Message:
        if after and not MALL_ORDER_ID_RE.match(after):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid seller incomes cursor")
        clamped = min(max(int(limit), 1), 100)
        data = await self.consignment_call(
            "incomes", {"account": account, "afterOrderId": after, "limit": clamped}
        )
        if not isinstance(data, dict) or not isinstance(data.get("incomes"), list) or len(data["incomes"]) > clamped:
            raise StoreUnavailableError("invalid seller incomes page from mall")
        out = self.protocol.message("trade.ListSellerIncomesResponse")
        for item in data["incomes"]:
            if not isinstance(item, dict):
                raise StoreUnavailableError("invalid seller income entry")
            oid = item.get("orderId")
            gross = int(item.get("grossFen", 0) or 0)
            fee = int(item.get("feeFen", 0) or 0)
            net = int(item.get("netFen", 0) or 0)
            reserved = int(item.get("reservedFen", 0) or 0)
            paid = int(item.get("paidFen", 0) or 0)
            forfeited = int(item.get("forfeitedFen", 0) or 0)
            dispute_frozen = int(item.get("disputeFrozenFen", 0) or 0)
            dispute_case = str(item.get("disputeCaseId", "") or "")
            avail = int(item.get("availableMs", 0) or 0)
            if (
                not isinstance(oid, str)
                or not MALL_ORDER_ID_RE.match(oid)
                or gross < 0
                or gross > MAX_RMB_PRICE_FEN
                or fee < 0
                or fee > gross
                or net != gross - fee
                or paid < 0
                or paid > net
                or reserved < 0
                or reserved > net - paid
                or forfeited < 0
                or forfeited > net - paid - reserved
                or dispute_frozen < 0
                or dispute_frozen > net - paid - reserved - forfeited
            ):
                raise StoreUnavailableError("invalid seller income arithmetic from mall")
            inc = out.incomes.add()
            inc.order_id = oid
            inc.gross_fen = gross
            inc.fee_fen = fee
            inc.net_fen = net
            inc.reserved_fen = reserved
            inc.paid_fen = paid
            inc.available_ms = avail
            inc.dispute_frozen_fen = dispute_frozen
            inc.dispute_case_id = dispute_case
            out.next_cursor = oid
        return out

    async def rmb_withdrawal(
        self,
        account: str,
        *,
        request_id: str = "",
        withdrawal_id: str = "",
        amount_fen: int = 0,
    ) -> Message:
        if request_id:
            if not 8 <= len(request_id) <= 80 or amount_fen <= 0 or amount_fen > MAX_RMB_PRICE_FEN:
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid withdrawal create parameters")
            action = "withdrawals/create"
            payload: dict[str, Any] = {"account": account, "requestId": request_id, "amountFen": int(amount_fen)}
        else:
            if not MALL_ORDER_ID_RE.match(withdrawal_id):
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid withdrawal_id")
            action = "withdrawals/get"
            payload = {"account": account, "withdrawalId": withdrawal_id}

        data = await self.consignment_call(action, payload)
        if not isinstance(data, dict):
            raise StoreUnavailableError("invalid withdrawal response from mall")
        wid = data.get("withdrawalId")
        amt = int(data.get("amountFen", 0) or 0)
        state = str(data.get("state", ""))
        channel = str(data.get("channel", "") or "")
        tx_id = str(data.get("transactionId", "") or "")
        if (
            not isinstance(wid, str)
            or not MALL_ORDER_ID_RE.match(wid)
            or amt <= 0
            or amt > MAX_RMB_PRICE_FEN
            or (not request_id and wid != withdrawal_id)
            or (request_id and amt != int(amount_fen))
            or state not in _RMB_WITHDRAWAL_STATES
        ):
            raise StoreUnavailableError("invalid withdrawal fields from mall")
        resp = self.protocol.message("trade.WithdrawalResponse")
        resp.withdrawal.withdrawal_id = wid
        resp.withdrawal.amount_fen = amt
        resp.withdrawal.state = state
        resp.withdrawal.channel = channel
        resp.withdrawal.transaction_id = tx_id
        return resp


# -- MallPoller（拉取官方商城交付与付款撤销意图、推进合同并回报回执） ------------------------------------------

class MallPoller:
    """单 worker 轮询器：拉取官方商城交付与扣回待办，建/推进官方合同并回报回执。"""

    def __init__(self, mall: MallAdapter, engine) -> None:
        self.mall = mall
        self.engine = engine
        self.poll_cursor: str = ""
        self.reversal_cursor: str = ""

    async def accept_revised(self, intent: dict[str, Any], payment_revision: int = 0) -> Message:
        """按官方订单号幂等找回或新建交付合同，锁内复核买家角色归属与冻结态。"""
        buyer_raw = intent.get("buyerId")
        rev_raw = intent.get("buyerOwnerRevision")
        order_id = intent.get("orderId")
        buyer_account = str(intent.get("buyerAccount", ""))
        qty = int(intent.get("quantity", 0) or 0)
        try:
            player_id = canonical_uint64(buyer_raw)
            owner_rev = canonical_uint64(rev_raw)
        except ValueError as exc:
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid buyerId or buyerOwnerRevision") from exc
        bundle, bundle_ok = official_bundle(self.mall.protocol, intent)
        if (
            not valid_account(buyer_account)
            or not isinstance(order_id, str)
            or not MALL_ORDER_ID_RE.match(order_id)
            or not bundle_ok
            or bundle is None
            or qty <= 0
        ):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid official mall delivery intent")

        rkey = request_key("official", player_id, order_id)
        digest_raw = json.dumps(intent, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        rdigest = hashlib.sha256(digest_raw).digest()

        existing = await self.engine.store.get_commerce_by_request(rkey)
        if existing is not None:
            if bytes(existing.request_digest) != rdigest:
                raise TradeRejection(ERR_TRADE_REQUEST_CONFLICT, "official intent digest mismatch")
            return existing

        cid = await self.engine.next_commerce_id()
        now = self.engine.now_ms()
        rev = int(payment_revision)
        w = self.mall.protocol.message(
            "trade.TradeCommerceWorkflow",
            external_order_sn=order_id,
            sku_id=int(intent.get("skuId", 0) or 0),
            sku_quantity=qty,
            external_amount=int(intent.get("amountMinor", 0) or 0),
            external_currency=str(intent.get("currency", "")),
            payment_revision=rev,
            payment_revoked=(rev % 2 == 1),
        )
        w.official_bundle.CopyFrom(bundle)
        w.buyer_identity.account = buyer_account
        w.buyer_identity.owner_revision = owner_rev

        rec = self.mall.protocol.message(
            "trade.TradeCommerceRecord",
            commerce_id=cid,
            request_key=rkey,
            request_digest=rdigest,
            kind=COMMERCE_KIND_OFFICIAL,
            buyer_player_id=player_id,
            created_ms=now,
            updated_ms=now,
            version=1,
        )

        attempt = 0

        async def _tx(tx) -> None:
            nonlocal attempt
            attempt += 1
            healed = attempt > 1
            w.stage = STAGE_OFFICIAL_DELIVER
            w.ClearField("quarantine")
            rec.status = COMMERCE_DELIVERING
            if w.payment_revoked:
                w.stage = STAGE_OFFICIAL_REVERSED
                rec.status = COMMERCE_CANCELLED
            else:
                fence = await self.engine.store.read_ownership(tx, player_id)
                trade_listing_id = fence.freeze_token if fence.freeze_final else 0
                prepare_token = 0 if fence.freeze_final else fence.freeze_token
                if not healed and (trade_listing_id != 0 or prepare_token != 0 or fence.owner_revision < owner_rev):
                    raise RoleFenceStaleError(player_id, reject=TradeRoleConflictError())
                if fence.account == buyer_account and fence.owner_revision == owner_rev and trade_listing_id == 0:
                    if prepare_token != 0:
                        raise RolePreparingError(f"buyer role {player_id} is preparing freeze")
                elif rev == 0:
                    w.stage = "official_refund"
                    rec.status = COMMERCE_REFUND_PENDING
                else:
                    w.quarantine.stage = STAGE_OFFICIAL_DELIVER
                    w.quarantine.status = COMMERCE_DELIVERING
                    w.quarantine.reason = _MANUAL_REASON_OWNER_CHANGED
                    w.quarantine.detail = truncate_utf8(
                        f"official order buyer role changed owner payment_revision={rev}",
                        QUARANTINE_DETAIL_MAX_BYTES,
                    )
                    w.quarantine.at_ms = now
                    w.stage = "manual"
                    rec.status = COMMERCE_MANUAL
            rec.workflow_proto = w.SerializeToString(deterministic=True)
            await tx.insert_commerce(rec)

        try:
            await self.engine.store.run_role_tx(_tx)
        except Exception:
            old = await self.engine.store.get_commerce_by_request(rkey)
            if old is not None and bytes(old.request_digest) == rdigest:
                return old
            raise
        return rec

    async def contract(self, intent: dict[str, Any], payment_revision: int) -> tuple[Message, Message]:
        rec = await self.accept_revised(intent, payment_revision)
        if payment_revision > 0:
            rec = await self.engine.apply_payment_revision(int(rec.commerce_id), payment_revision)
        w = self.mall.protocol.message("trade.TradeCommerceWorkflow")
        w.ParseFromString(bytes(rec.workflow_proto))
        return rec, w

    async def _send_receipt(self, path: str, rec: Message, body: dict[str, Any]) -> None:
        proof = hashlib.sha256(bytes(rec.workflow_proto)).hexdigest()
        payload = dict(body)
        payload["tradeOrderId"] = str(int(rec.commerce_id))
        payload["proof"] = proof
        await self.mall.call("delivery", "trade", "POST", path, payload, expect_json=False)

    async def deliver_intent(self, intent: dict[str, Any], payment_revision: int = 0) -> None:
        rec, w = await self.contract(intent, payment_revision)
        state = official_delivery_state(rec, w)
        if not state:
            return
        await self._send_receipt(
            "/internal/deliveries/receipt", rec, {"orderId": str(intent["orderId"]), "state": state}
        )

    async def reverse_intent(self, intent: dict[str, Any], revision: int) -> None:
        rec, w = await self.contract(intent, revision)
        if int(w.payment_revision) != int(revision):
            return
        state = official_reversal_state(rec, w)
        if not state:
            return
        await self._send_receipt(
            "/internal/reversals/receipt",
            rec,
            {"orderId": str(intent["orderId"]), "revision": int(revision), "state": state},
        )

    async def tick_deliveries(self) -> list[Exception]:
        data = await self.mall.call(
            "delivery",
            "trade",
            "POST",
            "/internal/deliveries/poll",
            {"limit": MALL_POLL_LIMIT, "cursor": self.poll_cursor},
        )
        entries = data.get("entries") if isinstance(data, dict) else None
        validate_mall_page(entries, self.poll_cursor, order_id_getter=lambda item: item.get("orderId"))
        if not entries:
            self.poll_cursor = ""
            return []
        errors: list[Exception] = []
        for entry in entries:
            intent = entry["intent"]
            rev = int(entry.get("paymentRevision", 0) or 0)
            try:
                await self.deliver_intent(intent, rev)
            except Exception as exc:
                logger.warning("trade mall deliver intent failed order_id=%s err=%s", intent.get("orderId"), exc)
                errors.append(exc)
            self.poll_cursor = entry["cursor"]
        return errors

    async def tick_reversals(self) -> list[Exception]:
        data = await self.mall.call(
            "delivery",
            "trade",
            "POST",
            "/internal/reversals/poll",
            {"limit": MALL_POLL_LIMIT, "cursor": self.reversal_cursor},
        )
        entries = data.get("entries") if isinstance(data, dict) else None
        if not isinstance(entries, list):
            raise ValueError("invalid mall reversal page entries")
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError("mall reversal entry is not an object")
            rev = int(entry.get("revision", 0) or 0)
            entitled = entry.get("entitled")
            if rev <= 0 or not isinstance(entitled, bool) or entitled != (rev % 2 == 0):
                raise ValueError(f"invalid mall reversal revision/entitled: rev={rev} entitled={entitled!r}")
        validate_mall_page(entries, self.reversal_cursor, order_id_getter=lambda item: item.get("orderId"))
        if not entries:
            self.reversal_cursor = ""
            return []
        errors: list[Exception] = []
        for entry in entries:
            intent = entry["intent"]
            rev = int(entry["revision"])
            try:
                await self.reverse_intent(intent, rev)
            except Exception as exc:
                logger.warning("trade mall reverse intent failed order_id=%s rev=%d err=%s", intent.get("orderId"), rev, exc)
                errors.append(exc)
            self.reversal_cursor = entry["cursor"]
        return errors

    async def tick(self) -> list[Exception]:
        """推进一轮官方商城交付待办与付款撤销待办，单通道或单条故障不阻断其余条目。"""
        errors: list[Exception] = []
        try:
            errors.extend(await self.tick_deliveries())
        except Exception as exc:
            logger.warning("trade mall deliveries poll failed err=%s", exc)
            errors.append(exc)
        try:
            errors.extend(await self.tick_reversals())
        except Exception as exc:
            logger.warning("trade mall reversals poll failed err=%s", exc)
            errors.append(exc)
        return errors
