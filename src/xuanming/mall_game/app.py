"""mall_game HTTP 路由层（aiohttp.web.Application 工厂）。

完整暴露与 Java ``mall_game`` 全部控制器逐字一致的 46 个业务路由与健康探针端点：
- Commerce（7）：订单创建/查询/列表/取消 + 支付回执入账
- Payment Query & Revision（3）：内部订单视图、商品目录、付款修订
- Delivery / Refund / Reversal / SKU（9）：公开 SKU、交付/退款/扣回轮询与回执、退款转人工
- Catalog & Manual Admin（9）：SKU 增改调库存、订单视图、人工单查询与三类人工单受控处置
- RMB Consignment（14）：寄售登记/列表/查询/下架/购买/订单管理/交付与退回轮询回执/收益/提现
- Mock Payment & Payout（4）：仅 dev/test 且显式启用时放行的本地 mock 支付/出款与对账
- Health：``/actuator/health``、``/actuator/health/liveness``、``/actuator/health/readiness``
"""

from __future__ import annotations

import json
from typing import Any

from aiohttp import web

from .auth import REQ_BODY_KEY, REQ_ROLE_KEY, REQ_SUBJECT_KEY, build_hmac_middleware
from .config import MallGameConfig
from .models import CommerceError, SkuTerms
from .store import MallStore


def _no_dup_pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in pairs:
        if k in out:
            raise ValueError(f"duplicate JSON key: {k}")
        out[k] = v
    return out


def _reject_json_constant(_val: str) -> Any:
    raise ValueError("invalid JSON constant")


def _parse_json_object(
    body: bytes,
    *,
    allowed_keys: frozenset[str] | None = None,
    required_keys: frozenset[str] = frozenset(),
    exact_keys: frozenset[str] | None = None,
    allow_empty: bool = False,
) -> dict[str, Any]:
    """严格解析 JSON 对象：拒绝重复键、非法常量、非对象根与未知字段。"""
    if not body:
        if allow_empty:
            return {}
        raise CommerceError(400, "INVALID_JSON", "请求体必须是完整 JSON 对象")
    try:
        parsed = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_no_dup_pairs_hook,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise CommerceError(400, "INVALID_JSON", "请求体必须是合法且无重复键的 JSON 对象") from None
    if not isinstance(parsed, dict):
        raise CommerceError(400, "INVALID_JSON", "请求体必须是 JSON 对象")
    keys = set(parsed.keys())
    if exact_keys is not None and keys != set(exact_keys):
        raise CommerceError(400, "INVALID_FIELDS", "请求字段集合非法")
    if allowed_keys is not None and (keys - set(allowed_keys)):
        raise CommerceError(400, "UNKNOWN_FIELDS", "请求包含未定义字段")
    if required_keys and (set(required_keys) - keys):
        raise CommerceError(400, "MISSING_FIELDS", "请求缺少必要字段")
    return parsed


def _req_str(data: dict[str, Any], key: str, *, default: str | None = None) -> str:
    if key not in data or data[key] is None:
        if default is not None:
            return default
        raise CommerceError(400, "INVALID_FIELD", f"字段 {key} 缺失")
    val = data[key]
    if not isinstance(val, str):
        raise CommerceError(400, "INVALID_FIELD", f"字段 {key} 必须是字符串")
    return val


def _opt_str(data: dict[str, Any], key: str) -> str | None:
    if key not in data or data[key] is None:
        return None
    val = data[key]
    if not isinstance(val, str):
        raise CommerceError(400, "INVALID_FIELD", f"字段 {key} 必须是字符串")
    return val


def _req_int(data: dict[str, Any], key: str, *, default: int | None = None) -> int:
    if key not in data or data[key] is None:
        if default is not None:
            return default
        raise CommerceError(400, "INVALID_FIELD", f"字段 {key} 缺失")
    val = data[key]
    if not isinstance(val, int) or isinstance(val, bool):
        raise CommerceError(400, "INVALID_FIELD", f"字段 {key} 必须是整数")
    return val


def _opt_int(data: dict[str, Any], key: str) -> int | None:
    if key not in data or data[key] is None:
        return None
    val = data[key]
    if not isinstance(val, int) or isinstance(val, bool):
        raise CommerceError(400, "INVALID_FIELD", f"字段 {key} 必须是整数")
    return val


def _req_bool(data: dict[str, Any], key: str, *, default: bool | None = None) -> bool:
    if key not in data or data[key] is None:
        if default is not None:
            return default
        raise CommerceError(400, "INVALID_FIELD", f"字段 {key} 缺失")
    val = data[key]
    if not isinstance(val, bool):
        raise CommerceError(400, "INVALID_FIELD", f"字段 {key} 必须是布尔值")
    return val


def _parse_terms(raw: Any) -> SkuTerms:
    if not isinstance(raw, dict):
        raise CommerceError(400, "INVALID_SKU_TERMS", "SKU条款非法")
    allowed = frozenset((
        "productId", "skuCode", "priceMinor", "currency", "published",
        "itemConfigId", "countPerUnit", "title", "currencyType", "currencyAmount",
    ))
    if set(raw.keys()) - allowed:
        raise CommerceError(400, "INVALID_SKU_TERMS", "SKU条款包含未知字段")
    return SkuTerms(
        product_id=_req_int(raw, "productId"),
        sku_code=_req_str(raw, "skuCode"),
        price_minor=_req_int(raw, "priceMinor"),
        currency=_req_str(raw, "currency"),
        published=_req_bool(raw, "published", default=False),
        item_config_id=_req_int(raw, "itemConfigId", default=0),
        count_per_unit=_req_int(raw, "countPerUnit", default=0),
        title=_req_str(raw, "title"),
        currency_type=_req_int(raw, "currencyType", default=0),
        currency_amount=_req_int(raw, "currencyAmount", default=0),
    )


def _require_role(request: web.Request, expected: str, *, subject: str | None = None) -> str:
    if request.get(REQ_ROLE_KEY) != expected:
        raise CommerceError(401, "AUTH_FAILED", "调用方认证失败")
    actual_subject = str(request.get(REQ_SUBJECT_KEY, ""))
    if subject is not None and actual_subject != subject:
        raise CommerceError(401, "AUTH_FAILED", "调用方认证失败")
    return actual_subject


def _json_resp(data: Any, status: int = 200) -> web.Response:
    return web.Response(
        body=json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        status=status,
        content_type="application/json",
        headers={"Cache-Control": "no-store"},
    )


MALL_STORE_KEY = web.AppKey("mall_store", object)
MALL_CONFIG_KEY = web.AppKey("mall_config", MallGameConfig)


def create_mall_app(store: MallStore, config: MallGameConfig) -> web.Application:
    """构建带 HMAC v1 认证中间件与全部 46 业务路由的 mall_game 应用实例。"""
    app = web.Application(middlewares=[build_hmac_middleware(config, store)])
    app[MALL_STORE_KEY] = store
    app[MALL_CONFIG_KEY] = config

    # -- Health ------------------------------------------------------------------------------------------------

    async def health(_request: web.Request) -> web.Response:
        try:
            await store.ping()
            return _json_resp({"status": "UP"})
        except Exception:
            return _json_resp({"status": "DOWN"}, status=503)

    async def probe_up(_request: web.Request) -> web.Response:
        return _json_resp({"status": "UP"})

    # -- Commerce (7 routes) -----------------------------------------------------------------------------------

    async def create_order(request: web.Request) -> web.Response:
        buyer = _require_role(request, "gateway")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("requestId", "skuId", "quantity", "buyerAccount", "buyerOwnerRevision")),
            required_keys=frozenset(("requestId", "skuId", "quantity", "buyerAccount", "buyerOwnerRevision")),
        )
        res = await store.create_order(
            buyer,
            _req_str(data, "requestId"),
            _req_int(data, "skuId"),
            _req_int(data, "quantity"),
            _req_str(data, "buyerAccount"),
            _req_str(data, "buyerOwnerRevision"),
        )
        return _json_resp(res)

    async def get_order_legacy(request: web.Request) -> web.Response:
        buyer = _require_role(request, "gateway")
        res = await store.get_order_legacy(buyer, request.match_info["orderId"])
        return _json_resp(res)

    async def query_order(request: web.Request) -> web.Response:
        buyer = _require_role(request, "gateway")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId", "buyerAccount", "buyerOwnerRevision")),
            required_keys=frozenset(("orderId", "buyerAccount", "buyerOwnerRevision")),
        )
        res = await store.query_order(
            buyer, _req_str(data, "orderId"), _req_str(data, "buyerAccount"), _req_str(data, "buyerOwnerRevision")
        )
        return _json_resp(res)

    async def list_orders(request: web.Request) -> web.Response:
        buyer = _require_role(request, "gateway")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("afterOrderId", "limit", "buyerAccount", "buyerOwnerRevision")),
            required_keys=frozenset(("afterOrderId", "limit", "buyerAccount", "buyerOwnerRevision")),
        )
        res = await store.list_orders(
            buyer,
            _req_str(data, "afterOrderId"),
            _req_int(data, "limit"),
            _req_str(data, "buyerAccount"),
            _req_str(data, "buyerOwnerRevision"),
        )
        return _json_resp(res)

    async def cancel_order(request: web.Request) -> web.Response:
        buyer = _require_role(request, "gateway")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId", "buyerAccount", "buyerOwnerRevision")),
            required_keys=frozenset(("orderId", "buyerAccount", "buyerOwnerRevision")),
        )
        res = await store.cancel_order(
            buyer, _req_str(data, "orderId"), _req_str(data, "buyerAccount"), _req_str(data, "buyerOwnerRevision")
        )
        return _json_resp(res)

    async def cancel_order_legacy(request: web.Request) -> web.Response:
        buyer = _require_role(request, "gateway")
        res = await store.cancel_order_legacy(buyer, request.match_info["orderId"])
        return _json_resp(res)

    async def record_payment(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId", "channel", "transactionId", "amountMinor", "currency")),
            required_keys=frozenset(("orderId", "channel", "transactionId", "amountMinor", "currency")),
        )
        res = await store.record_payment(
            _req_str(data, "orderId"),
            _req_str(data, "channel"),
            _req_str(data, "transactionId"),
            _req_int(data, "amountMinor"),
            _req_str(data, "currency"),
        )
        return _json_resp(res)

    # -- Payment Query & Revision (3 routes) -------------------------------------------------------------------

    async def payment_order_view(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId",)),
            required_keys=frozenset(("orderId",)),
        )
        return _json_resp(await store.payment_order_view(_req_str(data, "orderId")))

    async def payment_skus(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("afterSkuId", "limit")),
            required_keys=frozenset(("limit",)),
        )
        return _json_resp(await store.public_skus(_req_int(data, "afterSkuId", default=0), _req_int(data, "limit")))

    async def payment_revision(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId", "channel", "transactionId", "revision", "fact")),
            required_keys=frozenset(("orderId", "channel", "transactionId", "revision", "fact")),
        )
        res = await store.record_payment_revision(
            _req_str(data, "orderId"),
            _req_str(data, "channel"),
            _req_str(data, "transactionId"),
            _req_int(data, "revision"),
            _req_str(data, "fact"),
        )
        return _json_resp(res)

    # -- Delivery / Refund / Reversal / SKU (9 routes) ---------------------------------------------------------

    async def get_skus(request: web.Request) -> web.Response:
        _require_role(request, "gateway")
        return _json_resp(await store.public_skus(0, 100))

    async def list_skus(request: web.Request) -> web.Response:
        _require_role(request, "gateway")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("afterSkuId", "limit")),
            required_keys=frozenset(("limit",)),
        )
        return _json_resp(await store.public_skus(_req_int(data, "afterSkuId", default=0), _req_int(data, "limit")))

    async def poll_deliveries(request: web.Request) -> web.Response:
        _require_role(request, "delivery")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("limit", "cursor")),
            required_keys=frozenset(("limit",)),
        )
        return _json_resp(await store.poll_deliveries(_req_int(data, "limit"), _req_str(data, "cursor", default="")))

    async def delivery_receipt(request: web.Request) -> web.Response:
        _require_role(request, "delivery")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId", "tradeOrderId", "state", "proof")),
            required_keys=frozenset(("orderId", "tradeOrderId", "state", "proof")),
        )
        await store.delivery_receipt(
            _req_str(data, "orderId"),
            _req_str(data, "tradeOrderId"),
            _req_str(data, "state"),
            _req_str(data, "proof"),
        )
        return web.Response(status=200)

    async def poll_refunds(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("limit", "cursor")),
            allow_empty=True,
        )
        return _json_resp(
            await store.poll_refunds(_req_int(data, "limit", default=0), _req_str(data, "cursor", default=""))
        )

    async def refund_receipt(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId", "channel", "paymentTransactionId", "transactionId", "amountMinor", "currency")),
            required_keys=frozenset(("orderId", "channel", "paymentTransactionId", "transactionId", "amountMinor", "currency")),
        )
        await store.refund_receipt(
            _req_str(data, "orderId"),
            _req_str(data, "channel"),
            _req_str(data, "paymentTransactionId"),
            _req_str(data, "transactionId"),
            _req_int(data, "amountMinor"),
            _req_str(data, "currency"),
        )
        return web.Response(status=200)

    async def close_refund(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId", "channel", "paymentTransactionId", "reason")),
            required_keys=frozenset(("orderId", "channel", "paymentTransactionId", "reason")),
        )
        await store.close_refund(
            _req_str(data, "orderId"),
            _req_str(data, "channel"),
            _req_str(data, "paymentTransactionId"),
            _req_str(data, "reason"),
        )
        return web.Response(status=200)

    async def poll_reversals(request: web.Request) -> web.Response:
        _require_role(request, "delivery")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("limit", "cursor")),
            required_keys=frozenset(("limit",)),
        )
        return _json_resp(await store.poll_reversals(_req_int(data, "limit"), _req_str(data, "cursor", default="")))

    async def reversal_receipt(request: web.Request) -> web.Response:
        _require_role(request, "delivery")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId", "revision", "tradeOrderId", "state", "proof")),
            required_keys=frozenset(("orderId", "revision", "tradeOrderId", "state", "proof")),
        )
        await store.reversal_receipt(
            _req_str(data, "orderId"),
            _req_int(data, "revision"),
            _req_str(data, "tradeOrderId"),
            _req_str(data, "state"),
            _req_str(data, "proof"),
        )
        return web.Response(status=200)

    # -- Catalog & Manual Admin (9 routes) ---------------------------------------------------------------------

    async def admin_order_view(request: web.Request) -> web.Response:
        _require_role(request, "admin")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("orderId",)),
            required_keys=frozenset(("orderId",)),
        )
        return _json_resp(await store.payment_order_view(_req_str(data, "orderId")))

    async def admin_manual_list(request: web.Request) -> web.Response:
        _require_role(request, "admin")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("afterOrderId", "afterListingId", "afterRefundCursor", "limit")),
            allow_empty=True,
        )
        return _json_resp(
            await store.admin_list_manual(
                _req_str(data, "afterOrderId", default=""),
                _req_str(data, "afterListingId", default=""),
                _req_str(data, "afterRefundCursor", default=""),
                _req_int(data, "limit", default=0),
            )
        )

    async def admin_resolve_order(request: web.Request) -> web.Response:
        actor = _require_role(request, "admin")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("requestId", "orderId", "action", "reason")),
            required_keys=frozenset(("requestId", "orderId", "action", "reason")),
        )
        res = await store.admin_resolve_order(
            actor,
            _req_str(data, "requestId"),
            _req_str(data, "orderId"),
            _req_str(data, "action"),
            _req_str(data, "reason"),
        )
        return _json_resp(res)

    async def admin_resolve_refund(request: web.Request) -> web.Response:
        actor = _require_role(request, "admin")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("requestId", "orderId", "channel", "paymentTransactionId", "action", "externalRefundId", "reason")),
            required_keys=frozenset(("requestId", "orderId", "channel", "paymentTransactionId", "action", "reason")),
        )
        res = await store.admin_resolve_refund(
            actor,
            _req_str(data, "requestId"),
            _req_str(data, "orderId"),
            _req_str(data, "channel"),
            _req_str(data, "paymentTransactionId"),
            _req_str(data, "action"),
            _opt_str(data, "externalRefundId"),
            _req_str(data, "reason"),
        )
        return _json_resp(res)

    async def admin_resolve_consignment(request: web.Request) -> web.Response:
        actor = _require_role(request, "admin")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("requestId", "listingId", "action", "reason")),
            required_keys=frozenset(("requestId", "listingId", "action", "reason")),
        )
        res = await store.admin_resolve_consignment(
            actor,
            _req_str(data, "requestId"),
            _req_str(data, "listingId"),
            _req_str(data, "action"),
            _req_str(data, "reason"),
        )
        return _json_resp(res)

    async def admin_skus_list(request: web.Request) -> web.Response:
        _require_role(request, "admin")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("afterSkuId", "limit")),
            required_keys=frozenset(("limit",)),
        )
        return _json_resp(await store.admin_list_skus(_req_int(data, "afterSkuId", default=0), _req_int(data, "limit")))

    async def admin_skus_create(request: web.Request) -> web.Response:
        actor = _require_role(request, "admin")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("requestId", "skuId", "terms", "stock")),
            required_keys=frozenset(("requestId", "skuId", "terms", "stock")),
        )
        res = await store.admin_create_sku(
            actor,
            _req_str(data, "requestId"),
            _req_int(data, "skuId"),
            _parse_terms(data.get("terms")),
            _req_int(data, "stock"),
        )
        return _json_resp(res)

    async def admin_skus_update(request: web.Request) -> web.Response:
        actor = _require_role(request, "admin")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("requestId", "skuId", "expectedRevision", "terms")),
            required_keys=frozenset(("requestId", "skuId", "expectedRevision", "terms")),
        )
        res = await store.admin_update_sku(
            actor,
            _req_str(data, "requestId"),
            _req_int(data, "skuId"),
            _req_int(data, "expectedRevision"),
            _parse_terms(data.get("terms")),
        )
        return _json_resp(res)

    async def admin_skus_stock(request: web.Request) -> web.Response:
        actor = _require_role(request, "admin")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("requestId", "skuId", "expectedRevision", "delta")),
            required_keys=frozenset(("requestId", "skuId", "expectedRevision", "delta")),
        )
        res = await store.admin_adjust_stock(
            actor,
            _req_str(data, "requestId"),
            _req_int(data, "skuId"),
            _req_int(data, "expectedRevision"),
            _req_int(data, "delta"),
        )
        return _json_resp(res)

    # -- RMB Consignment (14 routes) ---------------------------------------------------------------------------

    async def consignment_register(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset((
                "listingId", "sellerPlayerId", "sellerAccount", "marketZone",
                "assetKind", "assetCurrencyType", "escrowReference", "escrowProof", "priceFen", "title",
            )),
            required_keys=frozenset((
                "listingId", "sellerPlayerId", "sellerAccount", "marketZone",
                "assetKind", "escrowReference", "escrowProof", "priceFen", "title",
            )),
        )
        res = await store.consignment_register(
            _req_str(data, "listingId"),
            _req_str(data, "sellerPlayerId"),
            _req_str(data, "sellerAccount"),
            _req_int(data, "marketZone"),
            _req_str(data, "assetKind"),
            _opt_int(data, "assetCurrencyType"),
            _req_str(data, "escrowReference"),
            _req_str(data, "escrowProof"),
            _req_int(data, "priceFen"),
            _req_str(data, "title"),
        )
        return _json_resp(res)

    async def consignment_list(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("afterListingId", "limit")),
            required_keys=frozenset(("afterListingId", "limit")),
        )
        return _json_resp(await store.consignment_list(_req_str(data, "afterListingId"), _req_int(data, "limit")))

    async def consignment_get(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("listingId",)),
            required_keys=frozenset(("listingId",)),
        )
        return _json_resp(await store.consignment_get(_req_str(data, "listingId")))

    async def consignment_cancel(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("listingId", "sellerAccount")),
            required_keys=frozenset(("listingId", "sellerAccount")),
        )
        return _json_resp(await store.consignment_cancel(_req_str(data, "listingId"), _req_str(data, "sellerAccount")))

    async def consignment_buy(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("listingId", "buyerPlayerId", "buyerAccount", "requestId", "buyerOwnerRevision")),
            required_keys=frozenset(("listingId", "buyerPlayerId", "buyerAccount", "requestId", "buyerOwnerRevision")),
        )
        res = await store.consignment_buy(
            _req_str(data, "listingId"),
            _req_str(data, "buyerPlayerId"),
            _req_str(data, "buyerAccount"),
            _req_str(data, "requestId"),
            _req_str(data, "buyerOwnerRevision"),
        )
        return _json_resp(res)

    async def consignment_orders_get(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("account", "orderId")),
            required_keys=frozenset(("account", "orderId")),
        )
        return _json_resp(await store.consignment_order_get(_req_str(data, "account"), _req_str(data, "orderId")))

    async def consignment_orders_list(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("account", "afterOrderId", "limit")),
            required_keys=frozenset(("account", "afterOrderId", "limit")),
        )
        return _json_resp(
            await store.consignment_order_list(
                _req_str(data, "account"), _req_str(data, "afterOrderId"), _req_int(data, "limit")
            )
        )

    async def consignment_orders_cancel(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("account", "orderId")),
            required_keys=frozenset(("account", "orderId")),
        )
        return _json_resp(await store.consignment_order_cancel(_req_str(data, "account"), _req_str(data, "orderId")))

    async def consignment_deliveries_poll(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("limit", "cursor")),
            required_keys=frozenset(("limit",)),
        )
        return _json_resp(
            await store.consignment_poll_deliveries(_req_int(data, "limit"), _req_str(data, "cursor", default=""))
        )

    async def consignment_returns_poll(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("limit", "cursor")),
            required_keys=frozenset(("limit",)),
        )
        return _json_resp(
            await store.consignment_poll_returns(_req_int(data, "limit"), _req_str(data, "cursor", default=""))
        )

    async def consignment_returns_receipt(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("listingId", "escrowReference", "escrowProof", "returnProof")),
            required_keys=frozenset(("listingId", "escrowReference", "escrowProof", "returnProof")),
        )
        await store.consignment_return_receipt(
            _req_str(data, "listingId"),
            _req_str(data, "escrowReference"),
            _req_str(data, "escrowProof"),
            _req_str(data, "returnProof"),
        )
        return web.Response(status=200)

    async def consignment_incomes(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("account", "afterOrderId", "limit")),
            required_keys=frozenset(("account", "afterOrderId", "limit")),
        )
        return _json_resp(
            await store.consignment_incomes(
                _req_str(data, "account"), _req_str(data, "afterOrderId"), _req_int(data, "limit")
            )
        )

    async def consignment_withdraw_create(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("account", "requestId", "amountFen")),
            required_keys=frozenset(("account", "requestId", "amountFen")),
        )
        return _json_resp(
            await store.consignment_withdraw_create(
                _req_str(data, "account"), _req_str(data, "requestId"), _req_int(data, "amountFen")
            )
        )

    async def consignment_withdraw_get(request: web.Request) -> web.Response:
        _require_role(request, "delivery", subject="trade")
        data = _parse_json_object(
            request[REQ_BODY_KEY],
            allowed_keys=frozenset(("account", "withdrawalId")),
            required_keys=frozenset(("account", "withdrawalId")),
        )
        return _json_resp(
            await store.consignment_withdraw_get(_req_str(data, "account"), _req_str(data, "withdrawalId"))
        )

    # -- Mock Payment & Payout (4 routes, dev/test only) -------------------------------------------------------

    async def mock_pay(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        config.require_mock_payment()
        data = _parse_json_object(request[REQ_BODY_KEY], exact_keys=frozenset(("orderId", "requestId")))
        return _json_resp(await store.mock_pay(_req_str(data, "orderId"), _req_str(data, "requestId")))

    async def mock_reconcile(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        config.require_mock_payment()
        _parse_json_object(request[REQ_BODY_KEY], exact_keys=frozenset())
        status, body = await store.mock_reconcile()
        return _json_resp(body, status=status)

    async def mock_withdraw(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        config.require_mock_payment()
        data = _parse_json_object(request[REQ_BODY_KEY], exact_keys=frozenset(("withdrawalId", "requestId")))
        return _json_resp(await store.mock_withdraw(_req_str(data, "withdrawalId"), _req_str(data, "requestId")))

    async def mock_withdrawals_reconcile(request: web.Request) -> web.Response:
        _require_role(request, "payment")
        config.require_mock_payment()
        _parse_json_object(request[REQ_BODY_KEY], exact_keys=frozenset())
        status, body = await store.mock_withdrawals_reconcile()
        return _json_resp(body, status=status)

    # 注册全部路由（46 业务路由 + 健康探针）
    r = app.router
    r.add_get("/actuator/health", health)
    r.add_get("/actuator/health/liveness", probe_up)
    r.add_get("/actuator/health/readiness", probe_up)

    # Commerce (7)
    r.add_post("/api/orders", create_order)
    r.add_post("/api/orders/query", query_order)
    r.add_post("/api/orders/list", list_orders)
    r.add_post("/api/orders/cancel", cancel_order)
    r.add_get("/api/orders/{orderId}", get_order_legacy)
    r.add_post("/api/orders/{orderId}/cancel", cancel_order_legacy)
    r.add_post("/internal/payments", record_payment)

    # Payment Query & Revision (3)
    r.add_post("/internal/payments/order-view", payment_order_view)
    r.add_post("/internal/payments/skus", payment_skus)
    r.add_post("/internal/payments/revision", payment_revision)

    # Delivery / Refund / Reversal / SKU (9)
    r.add_get("/api/skus", get_skus)
    r.add_post("/api/skus/list", list_skus)
    r.add_post("/internal/deliveries/poll", poll_deliveries)
    r.add_post("/internal/deliveries/receipt", delivery_receipt)
    r.add_post("/internal/refunds/poll", poll_refunds)
    r.add_post("/internal/refunds/receipt", refund_receipt)
    r.add_post("/internal/refunds/close", close_refund)
    r.add_post("/internal/reversals/poll", poll_reversals)
    r.add_post("/internal/reversals/receipt", reversal_receipt)

    # Catalog & Manual Admin (9)
    r.add_post("/internal/admin/order-view", admin_order_view)
    r.add_post("/internal/admin/manual/list", admin_manual_list)
    r.add_post("/internal/admin/manual/orders/resolve", admin_resolve_order)
    r.add_post("/internal/admin/manual/refunds/resolve", admin_resolve_refund)
    r.add_post("/internal/admin/manual/consignments/resolve", admin_resolve_consignment)
    r.add_post("/internal/admin/skus/list", admin_skus_list)
    r.add_post("/internal/admin/skus/create", admin_skus_create)
    r.add_post("/internal/admin/skus/update", admin_skus_update)
    r.add_post("/internal/admin/skus/stock", admin_skus_stock)

    # RMB Consignment (14)
    r.add_post("/internal/consignments/register", consignment_register)
    r.add_post("/internal/consignments/list", consignment_list)
    r.add_post("/internal/consignments/get", consignment_get)
    r.add_post("/internal/consignments/cancel", consignment_cancel)
    r.add_post("/internal/consignments/buy", consignment_buy)
    r.add_post("/internal/consignments/orders/get", consignment_orders_get)
    r.add_post("/internal/consignments/orders/list", consignment_orders_list)
    r.add_post("/internal/consignments/orders/cancel", consignment_orders_cancel)
    r.add_post("/internal/consignments/deliveries/poll", consignment_deliveries_poll)
    r.add_post("/internal/consignments/returns/poll", consignment_returns_poll)
    r.add_post("/internal/consignments/returns/receipt", consignment_returns_receipt)
    r.add_post("/internal/consignments/incomes", consignment_incomes)
    r.add_post("/internal/consignments/withdrawals/create", consignment_withdraw_create)
    r.add_post("/internal/consignments/withdrawals/get", consignment_withdraw_get)

    # Mock Payment & Payout (4)
    r.add_post("/internal/mock/pay", mock_pay)
    r.add_post("/internal/mock/reconcile", mock_reconcile)
    r.add_post("/internal/mock/withdraw", mock_withdraw)
    r.add_post("/internal/mock/withdrawals/reconcile", mock_withdrawals_reconcile)

    return app
