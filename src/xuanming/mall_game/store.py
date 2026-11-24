"""mall_game 抽象存储契约（MallStore）与完整内存事务实现（MemoryMallStore）。

``MemoryMallStore`` 严格复现 Java ``mall_game`` 全部 8 个 Store（NonceStore、CommerceStore、
DeliveryStore、PaymentRevisionStore、CatalogAdminStore、ManualAdminStore、RmbConsignmentStore、
MockPaymentStore、MockPayoutStore）的状态机不变量、锁序语义与幂等边界，供单测、本地联调与契约回归使用。
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from .config import MallGameConfig
from .models import (
    ACTOR_RE,
    ALLOWED_CURRENCY_TYPES,
    BOUND_OFFICIAL_REVERSAL_STATUSES,
    CHANNEL_RE,
    CLOSE_REASON_RE,
    CURRENCY_RE,
    EXPIRY_GRACE_MS,
    FACT_PAID,
    FACT_REVOKED,
    INT32_MAX,
    INT64_MAX,
    MANUAL_CONSIGNMENT_ACTIONS,
    MANUAL_ORDER_ACTIONS,
    MANUAL_REFUND_ACTIONS,
    MAX_AMOUNT_MINOR,
    MAX_CURRENCY_GRANT,
    MAX_ITEM_GRANT_COUNT,
    ORDER_STATUS_AWAITING_PAYMENT,
    ORDER_STATUS_CANCELLED,
    ORDER_STATUS_DELIVERED,
    ORDER_STATUS_DELIVERY_PENDING,
    ORDER_STATUS_MANUAL,
    ORDER_STATUS_REFUNDED,
    ORDER_STATUS_REFUND_PENDING,
    ORDER_STATUS_REVERSED,
    ORDER_STATUS_REVERSING,
    PAID_ORDER_STATUSES,
    REFUND_AMOUNT_MISMATCH,
    REFUND_CANCELLED_LATE,
    REFUND_CURRENCY_MISMATCH,
    REFUND_DELIVERY_REJECTED,
    REFUND_DUPLICATE_PAYMENT,
    RMB_ASSET_KINDS,
    TXN_ID_RE,
    UUID_RE,
    AbnormalPayment,
    CommerceError,
    DeliveryIntent,
    Order,
    OrderGrant,
    PaymentReceipt,
    PendingScanCursor,
    RefundIntent,
    ReversalIntent,
    RmbConsignment,
    RmbIncome,
    RmbOrder,
    RmbWithdrawal,
    Sku,
    SkuTerms,
    calc_consignment_fee,
    order_state_name,
    refunds_orders_own_payment,
    validate_account,
    validate_channel,
    validate_currency,
    validate_order_id,
    validate_proof,
    validate_request_id,
    validate_revision,
    validate_sku_terms,
    validate_txn_id,
    validate_uint64_str,
)

logger = logging.getLogger(__name__)


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _command_digest(kind: str, payload: Any) -> str:
    raw = (kind + "\n" + _canonical_json(payload)).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _consignment_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _require_reason(reason: Any) -> str:
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 256:
        raise CommerceError(400, "INVALID_REASON", "reason must be 1..256 non-blank characters")
    return reason.strip()


def _parse_refund_cursor(raw_cursor: str, *, error_msg: str = "退款分页游标非法") -> tuple[str, str, str] | None:
    if not raw_cursor:
        return None
    parts = raw_cursor.split(":")
    if (
        len(parts) != 4
        or parts[0] != "r1"
        or not UUID_RE.fullmatch(parts[1])
        or not CHANNEL_RE.fullmatch(parts[2])
        or not TXN_ID_RE.fullmatch(parts[3])
    ):
        raise CommerceError(400, "INVALID_REFUND_CURSOR", error_msg)
    return parts[1], parts[2], parts[3]


class MallStore(Protocol):
    """mall_game 全部存储操作异步接口协议。"""

    async def ping(self) -> None: ...
    async def claim_nonce(self, role: str, nonce: str, sent_ms: int) -> None: ...
    async def prune_expired_nonces(self) -> int: ...
    async def expire_overdue_orders(self) -> int: ...

    async def create_order(
        self, buyer: str, request_id: str, sku_id: int, quantity: int, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]: ...
    async def get_order_legacy(self, buyer: str, order_id: str) -> dict[str, Any]: ...
    async def query_order(
        self, buyer: str, order_id: str, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]: ...
    async def list_orders(
        self, buyer: str, after_order_id: str, limit: int, buyer_account: str, buyer_owner_revision: str
    ) -> list[dict[str, Any]]: ...
    async def cancel_order_legacy(self, buyer: str, order_id: str) -> dict[str, Any]: ...
    async def cancel_order(
        self, buyer: str, order_id: str, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]: ...
    async def record_payment(
        self, order_id: str, channel: str, transaction_id: str, amount_minor: int, currency: str
    ) -> dict[str, Any]: ...

    async def payment_order_view(self, order_id: str) -> dict[str, Any]: ...
    async def public_skus(self, after_sku_id: int = 0, limit: int = 100) -> dict[str, Any]: ...
    async def record_payment_revision(
        self, order_id: str, channel: str, transaction_id: str, revision: int, fact: str
    ) -> dict[str, Any]: ...

    async def poll_deliveries(self, limit: int, cursor: str = "") -> dict[str, Any]: ...
    async def delivery_receipt(self, order_id: str, trade_order_id: str, state: str, proof: str) -> None: ...
    async def poll_refunds(self, limit: int = 100, cursor: str = "") -> dict[str, Any]: ...
    async def refund_receipt(
        self,
        order_id: str,
        channel: str,
        payment_transaction_id: str,
        transaction_id: str,
        amount_minor: int,
        currency: str,
    ) -> None: ...
    async def close_refund(
        self, order_id: str, channel: str, payment_transaction_id: str, reason: str
    ) -> None: ...
    async def poll_reversals(self, limit: int, cursor: str = "") -> dict[str, Any]: ...
    async def reversal_receipt(
        self, order_id: str, revision: int, trade_order_id: str, state: str, proof: str
    ) -> None: ...

    async def admin_list_skus(self, after_sku_id: int, limit: int) -> dict[str, Any]: ...
    async def admin_create_sku(
        self, actor: str, request_id: str, sku_id: int, terms: SkuTerms, stock: int
    ) -> dict[str, Any]: ...
    async def admin_update_sku(
        self, actor: str, request_id: str, sku_id: int, expected_revision: int, terms: SkuTerms
    ) -> dict[str, Any]: ...
    async def admin_adjust_stock(
        self, actor: str, request_id: str, sku_id: int, expected_revision: int, delta: int
    ) -> dict[str, Any]: ...
    async def admin_list_manual(
        self, after_order_id: str, after_listing_id: str, after_refund_cursor: str, limit: int
    ) -> dict[str, Any]: ...
    async def admin_resolve_order(
        self, actor: str, request_id: str, order_id: str, action: str, reason: str
    ) -> dict[str, Any]: ...
    async def admin_resolve_refund(
        self,
        actor: str,
        request_id: str,
        order_id: str,
        channel: str,
        payment_transaction_id: str,
        action: str,
        external_refund_id: str | None,
        reason: str,
    ) -> dict[str, Any]: ...
    async def admin_resolve_consignment(
        self, actor: str, request_id: str, listing_id: str, action: str, reason: str
    ) -> dict[str, Any]: ...

    async def consignment_register(
        self,
        listing_id: str,
        seller_player_id: str,
        seller_account: str,
        market_zone: int,
        asset_kind: str,
        asset_currency_type: int | None,
        escrow_reference: str,
        escrow_proof: str,
        price_fen: int,
        title: str,
    ) -> dict[str, Any]: ...
    async def consignment_list(self, after_listing_id: str, limit: int) -> dict[str, Any]: ...
    async def consignment_get(self, listing_id: str) -> dict[str, Any]: ...
    async def consignment_cancel(self, listing_id: str, seller_account: str) -> dict[str, Any]: ...
    async def consignment_buy(
        self,
        listing_id: str,
        buyer_player_id: str,
        buyer_account: str,
        request_id: str,
        buyer_owner_revision: str,
    ) -> dict[str, Any]: ...
    async def consignment_order_get(self, account: str, order_id: str) -> dict[str, Any]: ...
    async def consignment_order_list(
        self, account: str, after_order_id: str, limit: int
    ) -> list[dict[str, Any]]: ...
    async def consignment_order_cancel(self, account: str, order_id: str) -> dict[str, Any]: ...
    async def consignment_poll_deliveries(self, limit: int, cursor: str = "") -> dict[str, Any]: ...
    async def consignment_poll_returns(self, limit: int, cursor: str = "") -> dict[str, Any]: ...
    async def consignment_return_receipt(
        self, listing_id: str, escrow_reference: str, escrow_proof: str, return_proof: str
    ) -> None: ...
    async def consignment_incomes(self, account: str, after_order_id: str, limit: int) -> dict[str, Any]: ...
    async def consignment_withdraw_create(
        self, account: str, request_id: str, amount_fen: int
    ) -> dict[str, Any]: ...
    async def consignment_withdraw_get(self, account: str, withdrawal_id: str) -> dict[str, Any]: ...

    async def mock_pay(self, order_id: str, request_id: str) -> dict[str, Any]: ...
    async def mock_reconcile(self) -> tuple[int, dict[str, Any]]: ...
    async def mock_withdraw(self, withdrawal_id: str, request_id: str) -> dict[str, Any]: ...
    async def mock_withdrawals_reconcile(self) -> tuple[int, dict[str, Any]]: ...


@dataclass(slots=True)
class _MockTxFact:
    kind: str
    order_sn: str
    request_id: str
    transaction_id: str
    payment_transaction_id: str
    amount_minor: int
    currency: str
    created_ms: int
    applied: int = 0


@dataclass(slots=True)
class _MockPayoutFact:
    withdrawal_id: str
    request_id: str
    transaction_id: str
    amount_fen: int
    successful: bool
    created_ms: int
    applied: int = 0


@dataclass(slots=True)
class _AdminJournalRow:
    command_type: str
    request_digest: str
    result_json: str
    created_ms: int


class MemoryMallStore:
    """进程内强一致内存存储实现（通过单把 asyncio.Lock 模拟事务原子性）。"""

    def __init__(
        self,
        config: MallGameConfig,
        *,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.config = config
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._lock = asyncio.Lock()

        # 自增订单序号
        self._next_order_seq = 1

        # 核心表映射
        self._nonces: dict[tuple[str, str], int] = {}
        self._skus: dict[int, Sku] = {}
        self._orders: dict[str, Order] = {}
        self._order_by_buyer_req: dict[tuple[str, str], str] = {}
        self._official_owners: dict[str, tuple[str, str]] = {}
        self._order_grants: dict[str, OrderGrant] = {}
        self._payment_receipts: dict[tuple[str, str], PaymentReceipt] = {}
        self._bound_receipt_by_order: dict[str, tuple[str, str]] = {}
        self._abnormal_payments: dict[tuple[str, str], AbnormalPayment] = {}
        self._delivery_intents: dict[str, DeliveryIntent] = {}
        self._delivery_receipts: dict[str, tuple[str, str, str]] = {}
        self._refund_intents: dict[tuple[str, str], RefundIntent] = {}
        self._refund_receipts: dict[tuple[str, str], tuple[str, str, int, str]] = {}
        self._refund_receipt_by_txn: dict[tuple[str, str], str] = {}
        self._reversal_intents: dict[str, ReversalIntent] = {}
        self._reversal_receipts: dict[tuple[str, int], tuple[str, str, str]] = {}
        self._admin_commands: dict[tuple[str, str], _AdminJournalRow] = {}

        # 人民币寄售表映射
        self._consignments: dict[str, RmbConsignment] = {}
        self._consignment_by_escrow: dict[str, str] = {}
        self._consignment_orders: dict[str, RmbOrder] = {}
        self._consignment_order_by_req: dict[tuple[str, str], str] = {}
        self._consignment_returns: dict[str, str] = {}
        self._seller_incomes: dict[str, RmbIncome] = {}
        self._withdrawals: dict[str, RmbWithdrawal] = {}
        self._withdrawal_by_req: dict[tuple[str, str], str] = {}
        self._withdrawal_allocs: dict[str, dict[str, int]] = {}
        self._withdrawal_receipts: dict[str, tuple[str, str, int, bool]] = {}
        self._withdrawal_receipt_by_txn: dict[tuple[str, str], str] = {}

        # Mock 原件表映射
        self._mock_txs: dict[tuple[str, str], _MockTxFact] = {}
        self._mock_tx_by_req: dict[tuple[str, str], str] = {}
        self._mock_tx_by_id: dict[str, tuple[str, str]] = {}
        self._mock_payouts: dict[str, _MockPayoutFact] = {}
        self._mock_payout_by_req: dict[str, str] = {}
        self._mock_payout_by_txn: dict[str, str] = {}

    def _now_ms(self) -> int:
        return int(self._clock_ms())

    def _order_view(self, order: Order) -> dict[str, Any]:
        bound_key = self._bound_receipt_by_order.get(order.order_id)
        rev = self._payment_receipts[bound_key].revision if bound_key is not None else 0
        out = copy.copy(order)
        out.payment_revision = rev
        return out.to_dict()

    # -- 健康、防重放与后台清理 --------------------------------------------------------------------------------

    async def ping(self) -> None:
        return None

    async def claim_nonce(self, role: str, nonce: str, sent_ms: int) -> None:
        async with self._lock:
            now = self._now_ms()
            skew = self.config.clock_skew_ms
            if now < skew:
                raise CommerceError(503, "CLOCK_UNAVAILABLE", "权威认证时间不可用")
            if sent_ms < now - skew or sent_ms > now + skew:
                raise CommerceError(401, "EXPIRED_REQUEST", "签名请求已过期")
            key = (role, nonce)
            if key in self._nonces:
                raise CommerceError(409, "NONCE_REPLAY", "签名请求已使用，请以新 nonce 重试原业务请求号")
            self._nonces[key] = sent_ms + self.config.clock_skew_ms + self.config.nonce_ttl_ms

    async def prune_expired_nonces(self) -> int:
        async with self._lock:
            now = self._now_ms()
            expired = [key for key, exp in self._nonces.items() if exp < now][:500]
            for key in expired:
                del self._nonces[key]
            return len(expired)

    async def expire_overdue_orders(self) -> int:
        async with self._lock:
            cutoff = self._now_ms() - EXPIRY_GRACE_MS
            overdue = sorted(
                (o for o in self._orders.values() if o.status == ORDER_STATUS_AWAITING_PAYMENT and o.payment_deadline_ms <= cutoff),
                key=lambda o: o.payment_deadline_ms,
            )[:50]
            expired = 0
            for order in overdue:
                try:
                    self._cancel_order_locked(order)
                    expired += 1
                except CommerceError:
                    continue
            return expired

    # -- 官方订单与支付入账 ------------------------------------------------------------------------------------

    async def create_order(
        self,
        buyer: str,
        request_id: str,
        sku_id: int,
        quantity: int,
        buyer_account: str,
        buyer_owner_revision: str,
    ) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        if (
            not isinstance(request_id, str)
            or not isinstance(sku_id, int)
            or isinstance(sku_id, bool)
            or not isinstance(quantity, int)
            or isinstance(quantity, bool)
            or sku_id <= 0
            or not 1 <= quantity <= 10_000
        ):
            raise CommerceError(400, "INVALID_ORDER", "下单参数非法")
        validate_request_id(request_id)
        validate_account(buyer_account)
        validate_revision(buyer_owner_revision)

        async with self._lock:
            prior_sn = self._order_by_buyer_req.get((buyer, request_id))
            if prior_sn is not None:
                return self._same_official_request(self._orders[prior_sn], sku_id, quantity, buyer_account, buyer_owner_revision)

            unpaid = sum(1 for o in self._orders.values() if o.buyer_id == buyer and o.status == ORDER_STATUS_AWAITING_PAYMENT)
            if unpaid >= self.config.max_unpaid_orders:
                raise CommerceError(429, "TOO_MANY_UNPAID", "未支付订单数量已达上限，请先处理已有订单")

            sku = self._skus.get(sku_id)
            if sku is None:
                raise CommerceError(404, "SKU_NOT_FOUND", "商品不存在")
            if not sku.published or sku.stock < 0 or sku.locked_stock < 0 or sku.locked_stock > sku.stock:
                raise CommerceError(409, "SKU_UNAVAILABLE", "商品未开放或库存状态异常")

            total = sku.price_minor * quantity
            if sku.price_minor <= 0 or total <= 0 or total > MAX_AMOUNT_MINOR or not CURRENCY_RE.fullmatch(sku.currency):
                raise CommerceError(409, "INVALID_AMOUNT", "商品金额或币种非法")
            if sku.stock - sku.locked_stock < quantity:
                raise CommerceError(409, "OUT_OF_STOCK", "库存不足")

            # 校验并冻结发放物
            item_ok = (
                sku.item_config_id > 0
                and sku.count_per_unit > 0
                and sku.count_per_unit * quantity <= MAX_ITEM_GRANT_COUNT
                and sku.currency_type == 0
                and sku.currency_amount == 0
            )
            currency_ok = (
                sku.item_config_id == 0
                and sku.count_per_unit == 0
                and sku.currency_type in ALLOWED_CURRENCY_TYPES
                and sku.currency_amount > 0
                and sku.currency_amount * quantity <= MAX_CURRENCY_GRANT
            )
            if not sku.title.strip() or not (item_ok or currency_ok):
                raise CommerceError(409, "INVALID_GRANT", "SKU没有合法游戏交付映射或单次数量越界")

            order_id = str(uuid.uuid4())
            now = self._now_ms()
            seq = self._next_order_seq
            self._next_order_seq += 1

            order = Order(
                id=seq,
                order_id=order_id,
                buyer_id=buyer,
                request_id=request_id,
                sku_id=sku_id,
                quantity=quantity,
                amount_minor=total,
                currency=sku.currency,
                status=ORDER_STATUS_AWAITING_PAYMENT,
                create_time_ms=now,
                payment_deadline_ms=now + self.config.order_ttl_ms,
            )
            sku.locked_stock += quantity
            self._orders[order_id] = order
            self._order_by_buyer_req[(buyer, request_id)] = order_id
            self._official_owners[order_id] = (buyer_account, buyer_owner_revision)
            self._order_grants[order_id] = OrderGrant(
                order_sn=order_id,
                item_config_id=sku.item_config_id,
                count_per_unit=sku.count_per_unit,
                currency_type=sku.currency_type,
                currency_amount=sku.currency_amount,
                title=sku.title,
            )
            return self._order_view(order)

    def _same_official_request(
        self, prior: Order, sku_id: int, quantity: int, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]:
        if prior.sku_id != sku_id or prior.quantity != quantity:
            raise CommerceError(409, "REQUEST_CONFLICT", "请求号已用于不同下单参数")
        owner = self._official_owners.get(prior.order_id)
        if owner is None:
            raise CommerceError(503, "OWNER_MISSING", "官方订单原身份缺失")
        if owner != (buyer_account, buyer_owner_revision):
            raise CommerceError(409, "OWNER_CONFLICT", "请求号的原账号或归属修订不符")
        return self._order_view(prior)

    async def get_order_legacy(self, buyer: str, order_id: str) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        validate_order_id(order_id)
        raise CommerceError(404, "OWNER_REQUIRED", "订单查询需要原账号和归属修订")

    async def cancel_order_legacy(self, buyer: str, order_id: str) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        validate_order_id(order_id)
        raise CommerceError(404, "OWNER_REQUIRED", "订单取消需要原账号和归属修订")

    def _official_owned_locked(self, buyer: str, order_id: str, buyer_account: str, buyer_owner_revision: str) -> Order:
        validate_order_id(order_id)
        validate_account(buyer_account)
        validate_revision(buyer_owner_revision)
        order = self._orders.get(order_id)
        if order is None or order.buyer_id != buyer:
            raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
        if order.sku_id == 0:
            raise CommerceError(404, "USE_CONSIGNMENT_ENDPOINT", "请通过原账号的寄售订单入口操作")
        owner = self._official_owners.get(order_id)
        if owner is None:
            raise CommerceError(503, "OWNER_MISSING", "官方订单原身份缺失")
        if owner != (buyer_account, buyer_owner_revision):
            raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
        return order

    async def query_order(
        self, buyer: str, order_id: str, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        async with self._lock:
            order = self._official_owned_locked(buyer, order_id, buyer_account, buyer_owner_revision)
            return self._order_view(order)

    async def list_orders(
        self, buyer: str, after_order_id: str, limit: int, buyer_account: str, buyer_owner_revision: str
    ) -> list[dict[str, Any]]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100 or not isinstance(after_order_id, str):
            raise CommerceError(400, "INVALID_PAGE", "订单分页非法")
        validate_account(buyer_account)
        validate_revision(buyer_owner_revision)
        async with self._lock:
            after_seq = 0
            if after_order_id:
                after_seq = self._official_owned_locked(buyer, after_order_id, buyer_account, buyer_owner_revision).id
            matched = [
                o
                for o in self._orders.values()
                if o.buyer_id == buyer
                and o.sku_id != 0
                and o.id > after_seq
                and self._official_owners.get(o.order_id) == (buyer_account, buyer_owner_revision)
            ]
            matched.sort(key=lambda o: o.id)
            return [self._order_view(o) for o in matched[:limit]]

    def _cancel_order_locked(self, order: Order) -> dict[str, Any]:
        if order.status == ORDER_STATUS_CANCELLED:
            return self._order_view(order)
        if order.status != ORDER_STATUS_AWAITING_PAYMENT:
            raise CommerceError(409, "ORDER_PAID", "已支付订单不能直接取消")
        if order.sku_id == 0:
            self._consignment_cancelled_locked(order.order_id)
        else:
            sku = self._skus.get(order.sku_id)
            if sku is None or sku.locked_stock < order.quantity:
                raise CommerceError(409, "LOCK_STOCK_INVALID", "库存预占状态异常")
            sku.locked_stock -= order.quantity
        order.status = ORDER_STATUS_CANCELLED
        return self._order_view(order)

    async def cancel_order(
        self, buyer: str, order_id: str, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        async with self._lock:
            order = self._official_owned_locked(buyer, order_id, buyer_account, buyer_owner_revision)
            return self._cancel_order_locked(order)

    def _verify_mock_payment_policy(
        self, order_id: str, channel: str, transaction_id: str, amount_minor: int, currency: str
    ) -> None:
        self.config.require_payment_channel(channel)
        if channel == "mock":
            fact = self._mock_txs.get(("PAYMENT", order_id))
            if fact is None:
                raise CommerceError(503, "MOCK_FACT_MISSING", "缺少本地mock交易原件")
            if (
                fact.transaction_id != transaction_id
                or fact.amount_minor != amount_minor
                or fact.currency != currency
            ):
                raise CommerceError(409, "MOCK_MISMATCH", "mock付款回执与原交易不符")

    def _record_payment_locked(
        self, order_id: str, channel: str, transaction_id: str, amount_minor: int, currency: str
    ) -> dict[str, Any]:
        order = self._orders.get(order_id)
        if order is None:
            raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
        if order.sku_id != 0 and order_id not in self._official_owners:
            raise CommerceError(503, "OWNER_MISSING", "官方订单原身份缺失")

        self._verify_mock_payment_policy(order_id, channel, transaction_id, amount_minor, currency)

        key = (channel, transaction_id)
        existing = self._payment_receipts.get(key)
        if existing is not None:
            abnormal = self._abnormal_payments.get(key)
            refund = self._refund_intents.get(key)
            refund_reason = refund.source if refund is not None else None
            if existing.order_sn is not None:
                consistent = abnormal is None and (refund_reason is None or refunds_orders_own_payment(refund_reason))
                owner_order = existing.order_sn
            else:
                consistent = (
                    abnormal is not None
                    and not refunds_orders_own_payment(abnormal.reason)
                    and abnormal.reason == refund_reason
                )
                owner_order = abnormal.order_sn if abnormal is not None else None
            if not consistent:
                raise CommerceError(503, "INCONSISTENT_PAYMENT", "付款回执与异常付款记录或退款待办不一致")
            if owner_order != order_id:
                raise CommerceError(409, "TXN_USED_OTHER_ORDER", "支付流水已用于其他订单")
            if existing.amount_minor != amount_minor or existing.currency != currency:
                raise CommerceError(409, "TXN_AMOUNT_CONFLICT", "支付流水已按不同金额或币种记录")
            return {"order": self._order_view(order), "refundReason": refund_reason}

        # 币种/金额核对先于订单状态判定
        mismatch: str | None = None
        if order.currency != currency:
            mismatch = REFUND_CURRENCY_MISMATCH
        elif order.amount_minor != amount_minor:
            mismatch = REFUND_AMOUNT_MISMATCH

        if mismatch is not None:
            return self._record_abnormal_locked(order, channel, transaction_id, amount_minor, currency, mismatch)

        if order.status in PAID_ORDER_STATUSES:
            return self._record_abnormal_locked(order, channel, transaction_id, amount_minor, currency, REFUND_DUPLICATE_PAYMENT)

        if order.status == ORDER_STATUS_CANCELLED:
            if order_id in self._bound_receipt_by_order:
                raise CommerceError(503, "ORDER_ALREADY_BOUND", "付款记录撞上未预期的唯一约束")
            self._payment_receipts[key] = PaymentReceipt(
                channel=channel,
                transaction_id=transaction_id,
                order_sn=order_id,
                amount_minor=amount_minor,
                currency=currency,
            )
            self._bound_receipt_by_order[order_id] = key
            self._refund_intents[key] = RefundIntent(
                channel=channel,
                transaction_id=transaction_id,
                order_sn=order_id,
                source=REFUND_CANCELLED_LATE,
                status="PENDING",
            )
            order.status = ORDER_STATUS_REFUND_PENDING
            return {"order": self._order_view(order), "refundReason": REFUND_CANCELLED_LATE}

        if order.status != ORDER_STATUS_AWAITING_PAYMENT:
            raise CommerceError(503, "ORDER_STATE_INVALID", "订单状态无法处理")

        if order_id in self._bound_receipt_by_order:
            raise CommerceError(503, "ORDER_ALREADY_BOUND", "付款记录撞上未预期的唯一约束")

        if order.sku_id == 0:
            self._consignment_paid_locked(order_id)
        else:
            sku = self._skus.get(order.sku_id)
            if sku is None or sku.stock < order.quantity or sku.locked_stock < order.quantity:
                raise CommerceError(503, "STOCK_INVARIANT_BROKEN", "库存预占状态异常")
            sku.stock -= order.quantity
            sku.locked_stock -= order.quantity

        now = self._now_ms()
        self._payment_receipts[key] = PaymentReceipt(
            channel=channel,
            transaction_id=transaction_id,
            order_sn=order_id,
            amount_minor=amount_minor,
            currency=currency,
        )
        self._bound_receipt_by_order[order_id] = key
        order.status = ORDER_STATUS_DELIVERY_PENDING
        order.payment_time_ms = now
        self._delivery_intents[order_id] = DeliveryIntent(
            order_sn=order_id,
            buyer_id=order.buyer_id,
            sku_id=order.sku_id,
            quantity=order.quantity,
            status="PENDING",
            created_ms=now,
        )
        return {"order": self._order_view(order), "refundReason": None}

    def _record_abnormal_locked(
        self,
        order: Order,
        channel: str,
        transaction_id: str,
        amount_minor: int,
        currency: str,
        reason: str,
    ) -> dict[str, Any]:
        key = (channel, transaction_id)
        now = self._now_ms()
        self._payment_receipts[key] = PaymentReceipt(
            channel=channel,
            transaction_id=transaction_id,
            order_sn=None,
            amount_minor=amount_minor,
            currency=currency,
        )
        self._abnormal_payments[key] = AbnormalPayment(
            channel=channel,
            transaction_id=transaction_id,
            order_sn=order.order_id,
            reason=reason,
            created_ms=now,
        )
        self._refund_intents[key] = RefundIntent(
            channel=channel,
            transaction_id=transaction_id,
            order_sn=order.order_id,
            source=reason,
            status="PENDING",
        )
        logger.warning(
            "abnormal payment recorded and queued for refund: reason=%s order=%s channel=%s",
            reason,
            order.order_id,
            channel,
        )
        return {"order": self._order_view(order), "refundReason": reason}

    async def record_payment(
        self, order_id: str, channel: str, transaction_id: str, amount_minor: int, currency: str
    ) -> dict[str, Any]:
        validate_order_id(order_id)
        validate_channel(channel)
        validate_txn_id(transaction_id)
        if not isinstance(amount_minor, int) or isinstance(amount_minor, bool) or not 1 <= amount_minor <= MAX_AMOUNT_MINOR:
            raise CommerceError(400, "INVALID_PAYMENT", "支付回执参数非法")
        validate_currency(currency)
        async with self._lock:
            return self._record_payment_locked(order_id, channel, transaction_id, amount_minor, currency)

    # -- Payment Query & Revision -----------------------------------------------------------------------------

    async def payment_order_view(self, order_id: str) -> dict[str, Any]:
        validate_order_id(order_id)
        async with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
            return self._order_view(order)

    async def public_skus(self, after_sku_id: int = 0, limit: int = 100) -> dict[str, Any]:
        if (
            not isinstance(after_sku_id, int)
            or isinstance(after_sku_id, bool)
            or after_sku_id < 0
            or not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise CommerceError(400, "INVALID_SKU_PAGE", "商品分页非法")
        async with self._lock:
            items = [
                s.to_public_dict()
                for s in sorted(self._skus.values(), key=lambda x: x.sku_id)
                if s.published and s.stock > s.locked_stock and s.sku_id > after_sku_id
            ][:limit]
            return {"skus": items}

    async def record_payment_revision(
        self, order_id: str, channel: str, transaction_id: str, revision: int, fact: str
    ) -> dict[str, Any]:
        if (
            not isinstance(order_id, str)
            or not UUID_RE.fullmatch(order_id)
            or not isinstance(channel, str)
            or not CHANNEL_RE.fullmatch(channel)
            or not isinstance(transaction_id, str)
            or not TXN_ID_RE.fullmatch(transaction_id)
            or not isinstance(revision, int)
            or isinstance(revision, bool)
            or revision < 1
            or fact not in (FACT_PAID, FACT_REVOKED)
            or (revision % 2 == 1) != (fact == FACT_REVOKED)
        ):
            raise CommerceError(400, "INVALID_REVISION", "付款修订参数非法")
        self.config.require_payment_channel(channel)

        async with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
            key = (channel, transaction_id)
            receipt = self._payment_receipts.get(key)
            if receipt is None:
                raise CommerceError(404, "RECEIPT_NOT_FOUND", "付款回执不存在")
            owner_order = receipt.order_sn
            if owner_order is None:
                abn = self._abnormal_payments.get(key)
                if abn is None:
                    raise CommerceError(503, "ABNORMAL_MISSING", "不绑定订单的付款缺异常付款记录")
                owner_order = abn.order_sn
            if owner_order != order_id:
                raise CommerceError(409, "PAYMENT_ORDER_MISMATCH", "这笔付款不属于这张订单")
            if revision <= receipt.revision:
                return {"revision": receipt.revision, "fact": receipt.fact, "orderState": order_state_name(order.status)}

            receipt.revision = revision
            receipt.fact = fact
            revoked = fact == FACT_REVOKED

            refund = self._refund_intents.get(key)
            if refund is not None:
                self._apply_revision_refund(order, refund, revoked)
            elif receipt.order_sn is None:
                raise CommerceError(503, "ABNORMAL_REFUND_MISSING", "异常付款缺退款待办")
            elif order.sku_id == 0:
                self._apply_revision_consignment(order, revoked)
            else:
                self._apply_revision_official(order, revision, revoked)
            return {"revision": revision, "fact": fact, "orderState": order_state_name(order.status)}

    def _apply_revision_refund(self, order: Order, refund: RefundIntent, revoked: bool) -> None:
        own = refunds_orders_own_payment(refund.source)
        cur = refund.status
        if revoked and cur in ("PENDING", "MANUAL"):
            expected_order = ORDER_STATUS_REFUND_PENDING if cur == "PENDING" else ORDER_STATUS_MANUAL
            next_order = ORDER_STATUS_REFUNDED
            refund.status = "REVOKED"
            refund.close_reason = "CHANNEL_REVOKED"
        elif not revoked and cur == "REVOKED":
            expected_order = ORDER_STATUS_REFUNDED
            next_order = ORDER_STATUS_MANUAL
            refund.status = "MANUAL"
            refund.close_reason = "REVOCATION_REVERSED"
        else:
            return
        if own:
            if order.status != expected_order:
                raise CommerceError(503, "ORDER_REFUND_INCONSISTENT", "订单本身的退款待办与订单状态不一致")
            order.status = next_order

    def _apply_revision_consignment(self, order: Order, revoked: bool) -> None:
        if revoked and order.status == ORDER_STATUS_DELIVERY_PENDING:
            intent = self._delivery_intents.get(order.order_id)
            if intent is not None and intent.status == "PENDING":
                intent.status = "REVOKED"
            for c in self._consignments.values():
                if c.current_order_sn == order.order_id:
                    c.state = "MANUAL"
            order.status = ORDER_STATUS_MANUAL
        elif revoked and order.status == ORDER_STATUS_DELIVERED:
            income = self._seller_incomes.get(order.order_id)
            if income is not None and income.paid_fen + income.reserved_fen + income.forfeited_fen < income.net_fen:
                income.available_ms = INT64_MAX
            for c in self._consignments.values():
                if c.current_order_sn == order.order_id:
                    c.state = "MANUAL"
            order.status = ORDER_STATUS_MANUAL

    def _apply_revision_official(self, order: Order, revision: int, revoked: bool) -> None:
        if order.status not in BOUND_OFFICIAL_REVERSAL_STATUSES:
            raise CommerceError(503, "ORDER_STATE_INVALID", "绑定付款的官方订单状态无法处理")
        if order.order_id not in self._delivery_intents:
            raise CommerceError(503, "DELIVERY_INTENT_MISSING", "官方订单的绑定付款缺交付待办")
        existing = self._reversal_intents.get(order.order_id)
        if existing is None and not revoked:
            return
        now = self._now_ms()
        if existing is None:
            self._reversal_intents[order.order_id] = ReversalIntent(
                order_sn=order.order_id, revision=revision, status="PENDING", updated_ms=now
            )
        else:
            if revision <= existing.revision:
                raise CommerceError(503, "REVERSAL_REVISION_AHEAD", "扣回待办的修订号超过付款回执")
            existing.revision = revision
            existing.status = "PENDING"
            existing.updated_ms = now
        order.status = ORDER_STATUS_REVERSING

    # -- Delivery / Refund / Reversal -------------------------------------------------------------------------

    def _build_official_intent_dict(self, order_id: str) -> dict[str, Any]:
        intent = self._delivery_intents[order_id]
        order = self._orders[order_id]
        grant = self._order_grants[order_id]
        owner = self._official_owners.get(order_id)
        if owner is None:
            raise CommerceError(503, "OWNER_MISSING", "订单原账号缺失或损坏")
        if intent.buyer_id != order.buyer_id or intent.sku_id != order.sku_id or intent.quantity != order.quantity:
            raise CommerceError(503, "INTENT_MISMATCH", "交付意图与原OMS角色或商品不符")

        item_count = 0
        currency_amount = 0
        if grant.item_config_id > 0 and grant.count_per_unit > 0 and grant.currency_type == 0 and grant.currency_amount == 0:
            count = intent.quantity * grant.count_per_unit
            if not 1 <= count <= MAX_ITEM_GRANT_COUNT:
                raise CommerceError(503, "GRANT_OUT_OF_RANGE", "已冻结的SKU游戏数量越界")
            item_count = count
        elif (
            grant.item_config_id == 0
            and grant.count_per_unit == 0
            and grant.currency_type in ALLOWED_CURRENCY_TYPES
            and grant.currency_amount > 0
            and intent.quantity > 0
        ):
            currency_amount = intent.quantity * grant.currency_amount
            if currency_amount > MAX_CURRENCY_GRANT:
                raise CommerceError(503, "GRANT_OUT_OF_RANGE", "已冻结的SKU货币数量越界")
        else:
            raise CommerceError(503, "GRANT_INVALID", "已冻结的SKU发放物非法")

        return {
            "orderId": order_id,
            "buyerId": intent.buyer_id,
            "buyerAccount": owner[0],
            "buyerOwnerRevision": owner[1],
            "skuId": intent.sku_id,
            "quantity": intent.quantity,
            "itemConfigId": grant.item_config_id,
            "itemCount": item_count,
            "title": grant.title,
            "amountMinor": order.amount_minor,
            "currency": order.currency,
            "currencyType": grant.currency_type,
            "currencyAmount": currency_amount,
        }

    async def poll_deliveries(self, limit: int, cursor: str = "") -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_LIMIT", "交付批量非法")
        parsed = PendingScanCursor.parse(cursor)
        async with self._lock:
            pending_all = [d for d in self._delivery_intents.values() if d.status == "PENDING"]
            if parsed is None:
                if not pending_all:
                    return {"entries": []}
                upper = max(pending_all, key=lambda d: (d.created_ms, d.order_sn))
                parsed = PendingScanCursor(0, "", upper.created_ms, upper.order_sn)

            rows = [
                d
                for d in pending_all
                if d.sku_id != 0
                and (d.created_ms, d.order_sn) > (parsed.after_ms, parsed.after_id)
                and (d.created_ms, d.order_sn) <= (parsed.upper_ms, parsed.upper_id)
            ]
            rows.sort(key=lambda d: (d.created_ms, d.order_sn))
            has_more = len(rows) > limit
            page = rows[:limit]
            entries = []
            for idx, d in enumerate(page):
                rev = self._reversal_intents[d.order_sn].revision if d.order_sn in self._reversal_intents else 0
                cur_str = "" if (not has_more and idx == len(page) - 1) else parsed.after(d.created_ms, d.order_sn)
                entries.append({
                    "intent": self._build_official_intent_dict(d.order_sn),
                    "cursor": cur_str,
                    "paymentRevision": rev,
                })
            return {"entries": entries}

    async def delivery_receipt(self, order_id: str, trade_order_id: str, state: str, proof: str) -> None:
        if (
            not isinstance(order_id, str)
            or not UUID_RE.fullmatch(order_id)
            or not isinstance(trade_order_id, str)
            or not isinstance(proof, str)
            or state not in ("DELIVERED", "REFUND_PENDING", "MANUAL", "VOIDED")
        ):
            raise CommerceError(400, "INVALID_DELIVERY_RECEIPT", "交付持久回执非法")
        validate_uint64_str(trade_order_id, status=400, code="INVALID_TRADE_ORDER_ID", message="交付持久回执非法")
        validate_proof(proof)

        async with self._lock:
            intent = self._delivery_intents.get(order_id)
            if intent is None:
                raise CommerceError(404, "DELIVERY_NOT_FOUND", "交付意图不存在")
            existing = self._delivery_receipts.get(order_id)
            if existing is not None:
                if existing[0] != trade_order_id or existing[1] != state:
                    raise CommerceError(409, "DELIVERY_CONFLICT", "同一交付意图的终局发生冲突")
                return
            if intent.status != "PENDING":
                raise CommerceError(409, "DELIVERY_ALREADY_FINAL", "交付意图已终结但缺原始回执")
            order = self._orders.get(order_id)
            if order is None:
                raise CommerceError(409, "ORDER_STATE_CONFLICT", "交付订单不在已支付待交付状态")

            reversal_owned = order_id in self._reversal_intents
            if not reversal_owned and (order.status != ORDER_STATUS_DELIVERY_PENDING or state == "VOIDED"):
                raise CommerceError(409, "ORDER_STATE_CONFLICT", "交付订单不在已支付待交付状态")

            if order.sku_id == 0:
                if order_id not in self._consignment_orders:
                    raise CommerceError(503, "CONSIGNMENT_OWNER_MISSING", "寄售交付原身份缺失")
                self._consignment_delivered_locked(order_id, state)
            else:
                if order_id not in self._official_owners:
                    raise CommerceError(503, "OFFICIAL_OWNER_MISSING", "官方交付原身份缺失")

            self._delivery_receipts[order_id] = (trade_order_id, state, proof)
            intent.status = state
            if reversal_owned:
                return
            if state == "DELIVERED":
                order.status = ORDER_STATUS_DELIVERED
            elif state == "MANUAL":
                order.status = ORDER_STATUS_MANUAL
            elif state == "REFUND_PENDING":
                bound = self._bound_receipt_by_order.get(order_id)
                if bound is None:
                    raise CommerceError(503, "PAYMENT_RECEIPT_MISSING", "交付退款缺订单付款回执")
                self._refund_intents[bound] = RefundIntent(
                    channel=bound[0],
                    transaction_id=bound[1],
                    order_sn=order_id,
                    source=REFUND_DELIVERY_REJECTED,
                    status="PENDING",
                )
                order.status = ORDER_STATUS_REFUND_PENDING

    async def poll_refunds(self, limit: int = 100, cursor: str = "") -> dict[str, Any]:
        eff_limit = 100 if limit == 0 else limit
        if not isinstance(eff_limit, int) or isinstance(eff_limit, bool) or not 1 <= eff_limit <= 100:
            raise CommerceError(400, "INVALID_REFUND_LIMIT", "退款分页批量非法")
        after = _parse_refund_cursor(cursor)
        async with self._lock:
            rows = []
            for r in self._refund_intents.values():
                if r.status != "PENDING":
                    continue
                rec = self._payment_receipts.get((r.channel, r.transaction_id))
                if rec is None:
                    continue
                sort_key = (r.order_sn, r.channel, r.transaction_id)
                if after is not None and sort_key <= after:
                    continue
                rows.append((sort_key, r, rec))
            rows.sort(key=lambda item: item[0])
            has_more = len(rows) > eff_limit
            page = rows[:eff_limit]
            entries = [
                {
                    "orderId": r.order_sn,
                    "channel": rec.channel,
                    "transactionId": rec.transaction_id,
                    "amountMinor": rec.amount_minor,
                    "currency": rec.currency,
                    "reason": r.source,
                }
                for _, r, rec in page
            ]
            next_cursor = ""
            if has_more and page:
                last = page[-1][1]
                next_cursor = f"r1:{last.order_sn}:{last.channel}:{last.transaction_id}"
            return {"entries": entries, "nextCursor": next_cursor}

    def _refund_receipt_locked(
        self,
        order_id: str,
        channel: str,
        payment_transaction_id: str,
        transaction_id: str,
        amount_minor: int,
        currency: str,
    ) -> None:
        order = self._orders.get(order_id)
        if order is None:
            raise CommerceError(404, "ORDER_NOT_FOUND", "退款订单不存在")
        key = (channel, payment_transaction_id)
        intent = self._refund_intents.get(key)
        if intent is None or intent.order_sn != order_id:
            raise CommerceError(404, "REFUND_NOT_FOUND", "退款待办不存在")
        receipt = self._payment_receipts.get(key)
        if receipt is None:
            raise CommerceError(503, "RECEIPT_MISSING", "退款待办缺原付款回执")
        if receipt.amount_minor != amount_minor or receipt.currency != currency:
            raise CommerceError(409, "REFUND_AMOUNT_MISMATCH", "退款金额币种不符")
        own = refunds_orders_own_payment(intent.source)
        if own != (receipt.order_sn == order_id):
            raise CommerceError(503, "REFUND_BINDING_MISMATCH", "退款待办来源与付款绑定不一致")

        self.config.require_payment_channel(channel)
        if channel == "mock":
            p_fact = self._mock_txs.get(("PAYMENT", order_id))
            r_fact = self._mock_txs.get(("REFUND", order_id))
            if p_fact is None or r_fact is None:
                raise CommerceError(503, "MOCK_FACT_MISSING", "缺少本地mock交易原件")
            if (
                p_fact.transaction_id != payment_transaction_id
                or r_fact.transaction_id != transaction_id
                or r_fact.payment_transaction_id != p_fact.transaction_id
                or r_fact.amount_minor != p_fact.amount_minor
                or r_fact.currency != p_fact.currency
            ):
                raise CommerceError(409, "MOCK_REFUND_MISMATCH", "mock退款与原付款事实不符")

        prior = self._refund_receipts.get(key)
        if prior is not None:
            if prior[0] != transaction_id:
                raise CommerceError(409, "REFUND_TXN_CONFLICT", "退款流水冲突")
            return

        if (channel, transaction_id) in self._refund_receipt_by_txn:
            raise CommerceError(409, "REFUND_TXN_REUSED", "退款流水已用于其他付款")
        if intent.status in ("MANUAL", "REVOKED"):
            raise CommerceError(409, "REFUND_FINALIZED", "退款待办已终结")
        if intent.status != "PENDING":
            raise CommerceError(503, "REFUND_STATE_BROKEN", "退款待办已终结但缺退款回执")
        if own and order.status != ORDER_STATUS_REFUND_PENDING:
            raise CommerceError(409, "ORDER_NOT_REFUNDING", "订单不在退款中")

        self._refund_receipts[key] = (transaction_id, order_id, amount_minor, currency)
        self._refund_receipt_by_txn[(channel, transaction_id)] = payment_transaction_id
        intent.status = "REFUNDED"
        if own:
            order.status = ORDER_STATUS_REFUNDED

    async def refund_receipt(
        self,
        order_id: str,
        channel: str,
        payment_transaction_id: str,
        transaction_id: str,
        amount_minor: int,
        currency: str,
    ) -> None:
        validate_order_id(order_id)
        validate_channel(channel)
        validate_txn_id(payment_transaction_id)
        validate_txn_id(transaction_id)
        if not isinstance(amount_minor, int) or isinstance(amount_minor, bool) or amount_minor < 1:
            raise CommerceError(400, "INVALID_REFUND", "退款回执非法")
        validate_currency(currency)
        async with self._lock:
            self._refund_receipt_locked(order_id, channel, payment_transaction_id, transaction_id, amount_minor, currency)

    async def close_refund(
        self, order_id: str, channel: str, payment_transaction_id: str, reason: str
    ) -> None:
        if (
            not isinstance(order_id, str)
            or not UUID_RE.fullmatch(order_id)
            or not isinstance(channel, str)
            or not CHANNEL_RE.fullmatch(channel)
            or not isinstance(payment_transaction_id, str)
            or not TXN_ID_RE.fullmatch(payment_transaction_id)
            or not isinstance(reason, str)
            or not CLOSE_REASON_RE.fullmatch(reason)
        ):
            raise CommerceError(400, "INVALID_CLOSE_REFUND", "退款转人工参数非法")
        self.config.require_payment_channel(channel)
        async with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "退款订单不存在")
            intent = self._refund_intents.get((channel, payment_transaction_id))
            if intent is None or intent.order_sn != order_id:
                raise CommerceError(404, "REFUND_NOT_FOUND", "退款待办不存在")
            if intent.status == "MANUAL":
                return
            if intent.status != "PENDING":
                raise CommerceError(409, "REFUND_FINALIZED", "退款待办已终结")
            intent.status = "MANUAL"
            intent.close_reason = reason
            if refunds_orders_own_payment(intent.source):
                if order.status != ORDER_STATUS_REFUND_PENDING:
                    raise CommerceError(503, "ORDER_REFUND_INCONSISTENT", "订单本身的退款待办与订单状态不一致")
                order.status = ORDER_STATUS_MANUAL

    async def poll_reversals(self, limit: int, cursor: str = "") -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_LIMIT", "扣回批量非法")
        parsed = PendingScanCursor.parse(cursor)
        async with self._lock:
            pending_all = [v for v in self._reversal_intents.values() if v.status == "PENDING"]
            if parsed is None:
                if not pending_all:
                    return {"entries": []}
                upper = max(pending_all, key=lambda v: (v.updated_ms, v.order_sn))
                parsed = PendingScanCursor(0, "", upper.updated_ms, upper.order_sn)
            rows = [
                v
                for v in pending_all
                if (v.updated_ms, v.order_sn) > (parsed.after_ms, parsed.after_id)
                and (v.updated_ms, v.order_sn) <= (parsed.upper_ms, parsed.upper_id)
            ]
            rows.sort(key=lambda v: (v.updated_ms, v.order_sn))
            has_more = len(rows) > limit
            page = rows[:limit]
            entries = []
            for idx, v in enumerate(page):
                if v.revision < 1:
                    raise CommerceError(503, "INVALID_REVERSAL_REV", "扣回待办修订号非法")
                cur_str = "" if (not has_more and idx == len(page) - 1) else parsed.after(v.updated_ms, v.order_sn)
                entries.append({
                    "intent": self._build_official_intent_dict(v.order_sn),
                    "revision": v.revision,
                    "entitled": v.revision % 2 == 0,
                    "cursor": cur_str,
                })
            return {"entries": entries}

    async def reversal_receipt(
        self, order_id: str, revision: int, trade_order_id: str, state: str, proof: str
    ) -> None:
        if (
            not isinstance(order_id, str)
            or not UUID_RE.fullmatch(order_id)
            or not isinstance(revision, int)
            or isinstance(revision, bool)
            or revision < 1
            or state not in ("REVERSED", "DELIVERED", "MANUAL")
        ):
            raise CommerceError(400, "INVALID_REVERSAL_RECEIPT", "扣回回执非法")
        validate_uint64_str(trade_order_id, status=400, code="INVALID_TRADE_ORDER_ID", message="扣回回执非法")
        validate_proof(proof)
        entitled = revision % 2 == 0
        if state == ("REVERSED" if entitled else "DELIVERED"):
            raise CommerceError(400, "REVERSAL_STATE_MISMATCH", "扣回回执的结论与修订号不符")

        async with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "扣回订单不存在")
            intent = self._reversal_intents.get(order_id)
            if intent is None:
                raise CommerceError(404, "REVERSAL_NOT_FOUND", "扣回待办不存在")
            if revision > intent.revision:
                raise CommerceError(409, "REVISION_TOO_NEW", "扣回回执的修订号超过商城记下的最新修订号")
            existing = self._reversal_receipts.get((order_id, revision))
            if existing is not None:
                if existing[0] != trade_order_id or existing[1] != state:
                    raise CommerceError(409, "REVERSAL_CONFLICT", "同一修订号的扣回结论发生冲突")
                return
            self._reversal_receipts[(order_id, revision)] = (trade_order_id, state, proof)
            if revision < intent.revision:
                return
            if intent.status != "PENDING":
                raise CommerceError(503, "REVERSAL_ALREADY_FINAL", "扣回待办已终结但缺这个修订号的回执")
            if order.status != ORDER_STATUS_REVERSING:
                raise CommerceError(503, "ORDER_REVERSING_INCONSISTENT", "扣回中的订单状态不一致")
            intent.status = state
            intent.updated_ms = self._now_ms()
            if state == "REVERSED":
                order.status = ORDER_STATUS_REVERSED
            elif state == "DELIVERED":
                order.status = ORDER_STATUS_DELIVERED
            else:
                order.status = ORDER_STATUS_MANUAL

    # -- Catalog & Manual Admin -------------------------------------------------------------------------------

    def _run_admin_command_locked(
        self,
        actor: str,
        request_id: str,
        kind: str,
        payload: dict[str, Any],
        mutation: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        if not isinstance(actor, str) or not ACTOR_RE.fullmatch(actor) or not isinstance(request_id, str):
            raise CommerceError(400, "INVALID_ADMIN_REQ", "管理主体或请求号非法")
        validate_request_id(request_id)
        digest = _command_digest(kind, payload)
        key = (actor, request_id)
        prior = self._admin_commands.get(key)
        if prior is not None:
            if prior.command_type != kind or prior.request_digest != digest:
                raise CommerceError(409, "ADMIN_REQ_CONFLICT", "管理请求号已用于不同条款")
            return json.loads(prior.result_json)
        result = mutation()
        self._admin_commands[key] = _AdminJournalRow(
            command_type=kind,
            request_digest=digest,
            result_json=_canonical_json(result),
            created_ms=self._now_ms(),
        )
        return result

    async def admin_list_skus(self, after_sku_id: int, limit: int) -> dict[str, Any]:
        if (
            not isinstance(after_sku_id, int)
            or isinstance(after_sku_id, bool)
            or after_sku_id < 0
            or not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise CommerceError(400, "INVALID_SKU_PAGE", "管理目录分页非法")
        async with self._lock:
            items = [
                s.to_admin_dict()
                for s in sorted(self._skus.values(), key=lambda x: x.sku_id)
                if s.sku_id > after_sku_id
            ][:limit]
            return {"skus": items}

    async def admin_create_sku(
        self, actor: str, request_id: str, sku_id: int, terms: SkuTerms, stock: int
    ) -> dict[str, Any]:
        if (
            not isinstance(sku_id, int)
            or isinstance(sku_id, bool)
            or sku_id <= 0
            or not isinstance(stock, int)
            or isinstance(stock, bool)
            or stock < 0
            or stock > INT32_MAX
        ):
            raise CommerceError(400, "INVALID_CREATE_SKU", "新增SKU参数非法")
        validate_sku_terms(terms)
        payload = {"requestId": request_id, "skuId": sku_id, "terms": terms.to_dict(), "stock": stock}
        async with self._lock:
            def _mutate() -> dict[str, Any]:
                if sku_id in self._skus:
                    raise CommerceError(409, "SKU_EXISTS", "SKU已存在或管理请求冲突")
                sku = Sku(
                    sku_id=sku_id,
                    revision=1,
                    product_id=terms.product_id,
                    sku_code=terms.sku_code,
                    price_minor=terms.price_minor,
                    currency=terms.currency,
                    stock=stock,
                    locked_stock=0,
                    published=terms.published,
                    item_config_id=terms.item_config_id,
                    count_per_unit=terms.count_per_unit,
                    title=terms.title,
                    currency_type=terms.currency_type,
                    currency_amount=terms.currency_amount,
                )
                self._skus[sku_id] = sku
                return sku.to_admin_dict()

            return self._run_admin_command_locked(actor, request_id, "create", payload, _mutate)

    async def admin_update_sku(
        self, actor: str, request_id: str, sku_id: int, expected_revision: int, terms: SkuTerms
    ) -> dict[str, Any]:
        if not isinstance(sku_id, int) or isinstance(sku_id, bool) or sku_id <= 0:
            raise CommerceError(400, "INVALID_UPDATE_SKU", "修改SKU参数非法")
        if (
            not isinstance(expected_revision, int)
            or isinstance(expected_revision, bool)
            or expected_revision < 0
            or expected_revision >= INT64_MAX
        ):
            raise CommerceError(400, "INVALID_REVISION", "管理修订非法或已耗尽")
        validate_sku_terms(terms)
        payload = {
            "requestId": request_id,
            "skuId": sku_id,
            "expectedRevision": expected_revision,
            "terms": terms.to_dict(),
        }
        async with self._lock:
            def _mutate() -> dict[str, Any]:
                sku = self._skus.get(sku_id)
                if sku is None:
                    raise CommerceError(404, "SKU_NOT_FOUND", "SKU不存在")
                if sku.revision != expected_revision:
                    raise CommerceError(409, "REVISION_CONFLICT", "管理目录修订已变化")
                sku.product_id = terms.product_id
                sku.sku_code = terms.sku_code
                sku.price_minor = terms.price_minor
                sku.currency = terms.currency
                sku.published = terms.published
                sku.item_config_id = terms.item_config_id
                sku.count_per_unit = terms.count_per_unit
                sku.currency_type = terms.currency_type
                sku.currency_amount = terms.currency_amount
                sku.title = terms.title
                sku.revision += 1
                return sku.to_admin_dict()

            return self._run_admin_command_locked(actor, request_id, "update", payload, _mutate)

    async def admin_adjust_stock(
        self, actor: str, request_id: str, sku_id: int, expected_revision: int, delta: int
    ) -> dict[str, Any]:
        if (
            not isinstance(sku_id, int)
            or isinstance(sku_id, bool)
            or sku_id <= 0
            or not isinstance(delta, int)
            or isinstance(delta, bool)
            or delta == 0
        ):
            raise CommerceError(400, "INVALID_STOCK_ADJ", "库存调整参数非法")
        if (
            not isinstance(expected_revision, int)
            or isinstance(expected_revision, bool)
            or expected_revision < 0
            or expected_revision >= INT64_MAX
        ):
            raise CommerceError(400, "INVALID_REVISION", "管理修订非法或已耗尽")
        payload = {
            "requestId": request_id,
            "skuId": sku_id,
            "expectedRevision": expected_revision,
            "delta": delta,
        }
        async with self._lock:
            def _mutate() -> dict[str, Any]:
                sku = self._skus.get(sku_id)
                if sku is None:
                    raise CommerceError(404, "SKU_NOT_FOUND", "SKU不存在")
                if sku.revision != expected_revision:
                    raise CommerceError(409, "REVISION_CONFLICT", "管理目录修订已变化")
                try:
                    validate_sku_terms(sku.terms())
                except CommerceError:
                    raise CommerceError(409, "SKU_TERMS_INCOMPLETE", "请先修复SKU条款和发物映射再调整库存") from None
                after = sku.stock + delta
                if after < sku.locked_stock or after > INT32_MAX:
                    raise CommerceError(409, "STOCK_OUT_OF_RANGE", "库存调整越界或侵占未付款预占")
                sku.stock = after
                sku.revision += 1
                return sku.to_admin_dict()

            return self._run_admin_command_locked(actor, request_id, "stock", payload, _mutate)

    async def admin_list_manual(
        self, after_order_id: str, after_listing_id: str, after_refund_cursor: str, limit: int
    ) -> dict[str, Any]:
        eff_limit = 50 if limit == 0 else limit
        if not isinstance(eff_limit, int) or isinstance(eff_limit, bool) or not 1 <= eff_limit <= 100:
            raise CommerceError(400, "INVALID_LIMIT", "manual list limit must be between 1 and 100")
        if after_order_id and not UUID_RE.fullmatch(after_order_id):
            raise CommerceError(400, "INVALID_CURSOR", "invalid afterOrderId cursor")
        if after_listing_id:
            validate_uint64_str(after_listing_id, status=400, code="INVALID_CURSOR", message="invalid afterListingId cursor")
        after_refund = _parse_refund_cursor(after_refund_cursor, error_msg="invalid afterRefundCursor")

        async with self._lock:
            manual_orders = []
            for o in sorted(self._orders.values(), key=lambda x: x.order_id):
                if o.status != ORDER_STATUS_MANUAL or o.order_id <= after_order_id:
                    continue
                bound = self._bound_receipt_by_order.get(o.order_id)
                pay_rev = self._payment_receipts[bound].revision if bound is not None else 0
                d_intent = self._delivery_intents.get(o.order_id)
                r_intent = self._reversal_intents.get(o.order_id)
                co = self._consignment_orders.get(o.order_id)
                cons = self._consignments.get(co.listing_id) if co is not None else None
                inc = self._seller_incomes.get(o.order_id)
                manual_orders.append({
                    "orderId": o.order_id,
                    "buyerId": o.buyer_id,
                    "skuId": o.sku_id,
                    "quantity": o.quantity,
                    "amountMinor": o.amount_minor,
                    "currency": o.currency,
                    "paymentRevision": pay_rev,
                    "deliveryStatus": d_intent.status if d_intent is not None else None,
                    "reversalRevision": r_intent.revision if r_intent is not None else None,
                    "reversalStatus": r_intent.status if r_intent is not None else None,
                    "consignmentListingId": co.listing_id if co is not None else None,
                    "consignmentState": cons.state if cons is not None else None,
                    "incomeNetFen": inc.net_fen if inc is not None else None,
                    "incomeReservedFen": inc.reserved_fen if inc is not None else None,
                    "incomePaidFen": inc.paid_fen if inc is not None else None,
                    "incomeForfeitedFen": inc.forfeited_fen if inc is not None else None,
                    "incomeAvailableMs": inc.available_ms if inc is not None else None,
                })
                if len(manual_orders) >= eff_limit:
                    break

            refund_rows = []
            for r in self._refund_intents.values():
                if r.status != "MANUAL":
                    continue
                rec = self._payment_receipts.get((r.channel, r.transaction_id))
                if rec is None:
                    continue
                sort_key = (r.order_sn, r.channel, r.transaction_id)
                if after_refund is not None and sort_key <= after_refund:
                    continue
                refund_rows.append((sort_key, r, rec))
            refund_rows.sort(key=lambda item: item[0])
            has_more_refunds = len(refund_rows) > eff_limit
            refund_page = refund_rows[:eff_limit]
            manual_refunds = [
                {
                    "orderId": r.order_sn,
                    "channel": rec.channel,
                    "transactionId": rec.transaction_id,
                    "amountMinor": rec.amount_minor,
                    "currency": rec.currency,
                    "reason": r.source,
                    "closeReason": r.close_reason,
                    "paymentRevision": rec.revision,
                    "paymentFact": rec.fact,
                }
                for _, r, rec in refund_page
            ]
            next_refund_cursor = ""
            if has_more_refunds and refund_page:
                last = refund_page[-1][1]
                next_refund_cursor = f"r1:{last.order_sn}:{last.channel}:{last.transaction_id}"

            manual_consignments = []
            for c in sorted(self._consignments.values(), key=lambda x: x.listing_id):
                if c.state != "MANUAL" or c.listing_id <= after_listing_id:
                    continue
                ord_obj = self._orders.get(c.current_order_sn) if c.current_order_sn else None
                inc = self._seller_incomes.get(c.current_order_sn) if c.current_order_sn else None
                manual_consignments.append({
                    "listingId": c.listing_id,
                    "sellerPlayerId": c.seller_player_id,
                    "marketZone": c.market_zone,
                    "assetKind": c.asset_kind,
                    "title": c.title,
                    "priceFen": c.price_fen,
                    "currentOrderId": c.current_order_sn,
                    "orderState": order_state_name(ord_obj.status) if ord_obj is not None else None,
                    "incomeNetFen": inc.net_fen if inc is not None else None,
                    "incomeReservedFen": inc.reserved_fen if inc is not None else None,
                    "incomePaidFen": inc.paid_fen if inc is not None else None,
                    "incomeForfeitedFen": inc.forfeited_fen if inc is not None else None,
                    "incomeAvailableMs": inc.available_ms if inc is not None else None,
                })
                if len(manual_consignments) >= eff_limit:
                    break

            return {
                "orders": manual_orders,
                "refunds": manual_refunds,
                "nextRefundCursor": next_refund_cursor,
                "consignments": manual_consignments,
            }

    async def admin_resolve_order(
        self, actor: str, request_id: str, order_id: str, action: str, reason: str
    ) -> dict[str, Any]:
        if not isinstance(order_id, str) or not UUID_RE.fullmatch(order_id) or action not in MANUAL_ORDER_ACTIONS:
            raise CommerceError(400, "INVALID_MANUAL_ORDER", "invalid manual order resolution request")
        cleaned_reason = _require_reason(reason)
        payload = {"requestId": request_id, "orderId": order_id, "action": action, "reason": reason}
        async with self._lock:
            def _mutate() -> dict[str, Any]:
                order = self._orders.get(order_id)
                if order is None:
                    raise CommerceError(404, "ORDER_NOT_FOUND", "order not found")
                if order.sku_id == 0:
                    raise CommerceError(409, "CONSIGNMENT_ORDER", "consignment orders must be resolved through manual/consignments/resolve")
                if order.status != ORDER_STATUS_MANUAL:
                    raise CommerceError(409, "NOT_MANUAL", "order is not in MANUAL state")
                now = self._now_ms()
                reversal = self._reversal_intents.get(order_id)
                if action == "RETRY_REVERSAL":
                    if reversal is None or reversal.status != "MANUAL":
                        raise CommerceError(409, "NO_MANUAL_REVERSAL", "order has no MANUAL reversal intent to retry")
                    rec = self._reversal_receipts.get((order_id, reversal.revision))
                    if rec is not None and rec[1] == "MANUAL":
                        del self._reversal_receipts[(order_id, reversal.revision)]
                    reversal.status = "PENDING"
                    reversal.updated_ms = now
                    order.status = ORDER_STATUS_REVERSING
                    resulting_state = order_state_name(ORDER_STATUS_REVERSING)
                elif action == "MARK_REVERSED":
                    if reversal is None or reversal.status != "MANUAL":
                        raise CommerceError(409, "NO_MANUAL_REVERSAL", "order has no MANUAL reversal intent")
                    if reversal.revision % 2 == 0:
                        raise CommerceError(409, "EVEN_REVISION", "even payment revision is entitled and cannot be marked REVERSED")
                    reversal.status = "REVERSED"
                    reversal.updated_ms = now
                    order.status = ORDER_STATUS_REVERSED
                    resulting_state = order_state_name(ORDER_STATUS_REVERSED)
                elif action == "MARK_DELIVERED":
                    if reversal is not None:
                        if reversal.status != "MANUAL":
                            raise CommerceError(409, "REVERSAL_NOT_MANUAL", "order reversal intent is not in MANUAL state")
                        if reversal.revision % 2 != 0:
                            raise CommerceError(409, "ODD_REVISION", "odd payment revision is revoked and cannot be marked DELIVERED")
                        reversal.status = "DELIVERED"
                        reversal.updated_ms = now
                    else:
                        delivery = self._delivery_intents.get(order_id)
                        if delivery is None or delivery.status != "MANUAL":
                            raise CommerceError(409, "NO_MANUAL_DELIVERY", "order has no MANUAL delivery intent")
                        delivery.status = "DELIVERED"
                    order.status = ORDER_STATUS_DELIVERED
                    resulting_state = order_state_name(ORDER_STATUS_DELIVERED)
                elif action == "CREATE_REFUND":
                    if reversal is not None:
                        raise CommerceError(409, "REVERSAL_GOVERNED", "orders governed by payment reversal cannot create a new refund intent")
                    delivery = self._delivery_intents.get(order_id)
                    if delivery is None or delivery.status != "MANUAL":
                        raise CommerceError(409, "NO_MANUAL_DELIVERY", "CREATE_REFUND requires a MANUAL delivery intent")
                    bound = self._bound_receipt_by_order.get(order_id)
                    if bound is None:
                        raise CommerceError(503, "RECEIPT_MISSING", "bound payment receipt missing for official order")
                    receipt = self._payment_receipts[bound]
                    if receipt.fact != FACT_PAID:
                        raise CommerceError(409, "PAYMENT_NOT_PAID", "bound payment is not PAID")
                    if bound in self._refund_intents:
                        raise CommerceError(409, "REFUND_EXISTS", "refund intent already exists; resolve it through manual/refunds/resolve")
                    self._refund_intents[bound] = RefundIntent(
                        channel=bound[0],
                        transaction_id=bound[1],
                        order_sn=order_id,
                        source=REFUND_DELIVERY_REJECTED,
                        status="PENDING",
                    )
                    delivery.status = "REFUND_PENDING"
                    order.status = ORDER_STATUS_REFUND_PENDING
                    resulting_state = order_state_name(ORDER_STATUS_REFUND_PENDING)
                else:
                    raise CommerceError(400, "UNSUPPORTED_ACTION", "unsupported manual order action")
                return {
                    "targetType": "ORDER",
                    "targetId": order_id,
                    "action": action,
                    "resultingState": resulting_state,
                    "reason": cleaned_reason,
                    "resolvedAtMs": now,
                }

            return self._run_admin_command_locked(actor, request_id, "manual_order", payload, _mutate)

    async def admin_resolve_refund(
        self,
        actor: str,
        request_id: str,
        order_id: str,
        channel: str,
        payment_transaction_id: str,
        action: str,
        external_refund_id: str | None,
        reason: str,
    ) -> dict[str, Any]:
        if (
            not isinstance(order_id, str)
            or not UUID_RE.fullmatch(order_id)
            or not isinstance(channel, str)
            or not CHANNEL_RE.fullmatch(channel)
            or not isinstance(payment_transaction_id, str)
            or not TXN_ID_RE.fullmatch(payment_transaction_id)
            or action not in MANUAL_REFUND_ACTIONS
        ):
            raise CommerceError(400, "INVALID_MANUAL_REFUND", "invalid manual refund resolution request")
        cleaned_reason = _require_reason(reason)
        if action == "MARK_REFUNDED":
            if not isinstance(external_refund_id, str) or not TXN_ID_RE.fullmatch(external_refund_id):
                raise CommerceError(400, "MISSING_EXTERNAL_REFUND_ID", "MARK_REFUNDED requires a valid externalRefundId")
        elif external_refund_id:
            raise CommerceError(400, "UNEXPECTED_EXTERNAL_REFUND_ID", "externalRefundId is only accepted for MARK_REFUNDED")

        target_id = f"{order_id}:{channel}:{payment_transaction_id}"
        payload = {
            "requestId": request_id,
            "orderId": order_id,
            "channel": channel,
            "paymentTransactionId": payment_transaction_id,
            "action": action,
            "externalRefundId": external_refund_id,
            "reason": reason,
        }
        async with self._lock:
            def _mutate() -> dict[str, Any]:
                order = self._orders.get(order_id)
                if order is None:
                    raise CommerceError(404, "ORDER_NOT_FOUND", "order not found")
                key = (channel, payment_transaction_id)
                intent = self._refund_intents.get(key)
                if intent is None or intent.order_sn != order_id:
                    raise CommerceError(404, "REFUND_NOT_FOUND", "refund intent not found")
                if intent.status != "MANUAL":
                    raise CommerceError(409, "REFUND_NOT_MANUAL", "refund intent is not in MANUAL state")
                receipt = self._payment_receipts.get(key)
                if receipt is None:
                    raise CommerceError(503, "RECEIPT_MISSING", "payment receipt missing for refund intent")
                own = refunds_orders_own_payment(intent.source)
                if own != (receipt.order_sn == order_id):
                    raise CommerceError(503, "REFUND_BINDING_MISMATCH", "refund intent source does not match payment binding")
                if own and order.status != ORDER_STATUS_MANUAL:
                    raise CommerceError(409, "ORDER_NOT_MANUAL", "linked order is not in MANUAL state")

                now = self._now_ms()
                if action == "RETRY_REFUND":
                    if receipt.fact != FACT_PAID:
                        raise CommerceError(409, "PAYMENT_NOT_PAID", "payment fact is not PAID and cannot be retried for refund")
                    intent.status = "PENDING"
                    intent.close_reason = None
                    if own:
                        order.status = ORDER_STATUS_REFUND_PENDING
                    resulting_state = "PENDING"
                elif action == "MARK_REFUNDED":
                    assert external_refund_id is not None
                    if key in self._refund_receipts:
                        raise CommerceError(409, "RECEIPT_EXISTS", "refund receipt already recorded for this payment")
                    if (channel, external_refund_id) in self._refund_receipt_by_txn:
                        raise CommerceError(409, "TXN_CONFLICT", "unique receipt or admin command conflict")
                    self._refund_receipts[key] = (external_refund_id, order_id, receipt.amount_minor, receipt.currency)
                    self._refund_receipt_by_txn[(channel, external_refund_id)] = payment_transaction_id
                    intent.status = "REFUNDED"
                    intent.close_reason = "MANUAL_REFUNDED"
                    if own:
                        order.status = ORDER_STATUS_REFUNDED
                    resulting_state = "REFUNDED"
                elif action == "MARK_REVOKED":
                    intent.status = "REVOKED"
                    intent.close_reason = "MANUAL_REVOKED"
                    if own:
                        order.status = ORDER_STATUS_REFUNDED
                    resulting_state = "REVOKED"
                else:
                    raise CommerceError(400, "UNSUPPORTED_ACTION", "unsupported manual refund action")

                return {
                    "targetType": "REFUND",
                    "targetId": target_id,
                    "action": action,
                    "resultingState": resulting_state,
                    "reason": cleaned_reason,
                    "resolvedAtMs": now,
                }

            return self._run_admin_command_locked(actor, request_id, "manual_refund", payload, _mutate)

    async def admin_resolve_consignment(
        self, actor: str, request_id: str, listing_id: str, action: str, reason: str
    ) -> dict[str, Any]:
        if action not in MANUAL_CONSIGNMENT_ACTIONS:
            raise CommerceError(400, "INVALID_MANUAL_CONSIGNMENT", "invalid manual consignment resolution request")
        validate_uint64_str(listing_id, status=400, code="INVALID_LISTING_ID", message="人民币寄售或结算参数非法")
        cleaned_reason = _require_reason(reason)
        payload = {"requestId": request_id, "listingId": listing_id, "action": action, "reason": reason}
        async with self._lock:
            def _mutate() -> dict[str, Any]:
                cons = self._consignments.get(listing_id)
                if cons is None:
                    raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "consignment listing not found")
                if cons.state != "MANUAL":
                    raise CommerceError(409, "CONSIGNMENT_NOT_MANUAL", "consignment is not in MANUAL state")
                order = self._orders.get(cons.current_order_sn) if cons.current_order_sn else None
                if cons.current_order_sn and (order is None or order.sku_id != 0):
                    raise CommerceError(503, "CONSIGNMENT_ORDER_BROKEN", "consignment current order record inconsistent")

                now = self._now_ms()
                if action == "RETURN_TO_SELLER":
                    if cons.current_order_sn:
                        if cons.current_order_sn in self._seller_incomes:
                            raise CommerceError(
                                409,
                                "INCOME_EXISTS",
                                "asset already delivered and seller income recorded; use UNFREEZE_INCOME or FORFEIT_INCOME",
                            )
                        d_intent = self._delivery_intents.get(cons.current_order_sn)
                        if d_intent is not None:
                            if d_intent.status == "DELIVERED":
                                raise CommerceError(409, "ALREADY_DELIVERED", "consignment delivery already DELIVERED; cannot return asset to seller")
                            if d_intent.status in ("PENDING", "MANUAL"):
                                d_intent.status = "REVOKED"
                        if order is not None and order.status == ORDER_STATUS_MANUAL:
                            order.status = ORDER_STATUS_REVERSED
                    cons.state = "RETURNING"
                    resulting_state = "RETURNING"
                elif action == "UNFREEZE_INCOME":
                    if not cons.current_order_sn:
                        raise CommerceError(409, "NO_BOUND_ORDER", "consignment has no bound order for income unfreeze")
                    inc = self._seller_incomes.get(cons.current_order_sn)
                    if inc is None:
                        raise CommerceError(409, "NO_INCOME", "consignment has no seller income record to unfreeze")
                    if inc.available_ms != INT64_MAX:
                        raise CommerceError(409, "NOT_FROZEN", "seller income is not frozen")
                    if inc.net_fen - inc.reserved_fen - inc.paid_fen - inc.forfeited_fen <= 0:
                        raise CommerceError(409, "NO_BALANCE", "seller income has no remaining balance to unfreeze")
                    inc.available_ms = now
                    cons.state = "SOLD"
                    if order is not None and order.status == ORDER_STATUS_MANUAL:
                        order.status = ORDER_STATUS_DELIVERED
                    resulting_state = "SOLD"
                elif action == "FORFEIT_INCOME":
                    if not cons.current_order_sn:
                        raise CommerceError(409, "NO_BOUND_ORDER", "consignment has no bound order for income forfeiture")
                    inc = self._seller_incomes.get(cons.current_order_sn)
                    if inc is None:
                        raise CommerceError(409, "NO_INCOME", "consignment has no seller income record to forfeit")
                    if inc.reserved_fen > 0:
                        raise CommerceError(409, "IN_FLIGHT_WITHDRAWAL", "in-flight withdrawal reserves this income; settle withdrawal before forfeiting")
                    remaining = inc.net_fen - inc.paid_fen - inc.forfeited_fen
                    if remaining < 0:
                        raise CommerceError(503, "ACCOUNTING_BROKEN", "seller income accounting invariant broken")
                    inc.forfeited_fen += remaining
                    inc.available_ms = INT64_MAX
                    cons.state = "FORFEITED"
                    if order is not None and order.status == ORDER_STATUS_MANUAL:
                        order.status = ORDER_STATUS_REVERSED
                    resulting_state = "FORFEITED"
                else:
                    raise CommerceError(400, "UNSUPPORTED_ACTION", "unsupported manual consignment action")

                return {
                    "targetType": "CONSIGNMENT",
                    "targetId": listing_id,
                    "action": action,
                    "resultingState": resulting_state,
                    "reason": cleaned_reason,
                    "resolvedAtMs": now,
                }

            return self._run_admin_command_locked(actor, request_id, "manual_consign", payload, _mutate)

    # -- RMB 寄售全部 14 个操作 --------------------------------------------------------------------------------

    async def consignment_register(
        self,
        listing_id: str,
        seller_player_id: str,
        seller_account: str,
        market_zone: int,
        asset_kind: str,
        asset_currency_type: int | None,
        escrow_reference: str,
        escrow_proof: str,
        price_fen: int,
        title: str,
    ) -> dict[str, Any]:
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_uint64_str(seller_player_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_uint64_str(escrow_reference, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_account(seller_account)
        validate_proof(escrow_proof)
        if (
            not isinstance(market_zone, int)
            or isinstance(market_zone, bool)
            or not 1 <= market_zone <= 0xFFFF_FFFF
            or asset_kind not in RMB_ASSET_KINDS
            or not isinstance(price_fen, int)
            or isinstance(price_fen, bool)
            or not 1 <= price_fen <= MAX_AMOUNT_MINOR
            or not isinstance(title, str)
            or not title.strip()
            or len(title) > 128
        ):
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        if asset_kind == "CURRENCY":
            if (
                asset_currency_type is None
                or isinstance(asset_currency_type, bool)
                or asset_currency_type not in self.config.consignment_currencies
            ):
                raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        elif asset_currency_type is not None:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")

        payload = {
            "listingId": listing_id,
            "sellerPlayerId": seller_player_id,
            "sellerAccount": seller_account,
            "marketZone": market_zone,
            "assetKind": asset_kind,
            "assetCurrencyType": asset_currency_type,
            "escrowReference": escrow_reference,
            "escrowProof": escrow_proof,
            "priceFen": price_fen,
            "title": title,
        }
        digest = _consignment_digest(payload)

        async with self._lock:
            now = self._now_ms()
            existing = self._consignments.get(listing_id)
            if existing is not None:
                if existing.request_digest != digest:
                    raise CommerceError(409, "CONSIGNMENT_CONFLICT", "寄售编号或托管引用已用于不同条款")
                return existing.to_dict(now)
            if escrow_reference in self._consignment_by_escrow:
                raise CommerceError(409, "ESCROW_REUSED", "托管合同已登记其它寄售")

            notice_end = now + self.config.consignment_notice_ms
            sale_end = notice_end + self.config.consignment_sale_ms
            cons = RmbConsignment(
                listing_id=listing_id,
                seller_player_id=seller_player_id,
                seller_account=seller_account,
                market_zone=market_zone,
                asset_kind=asset_kind,
                asset_currency_type=asset_currency_type,
                escrow_reference=escrow_reference,
                escrow_proof=escrow_proof,
                request_digest=digest,
                title=title,
                price_fen=price_fen,
                fee_bps=self.config.consignment_fee_bps,
                cooling_ms=self.config.consignment_cooling_ms,
                notice_end_ms=notice_end,
                sale_end_ms=sale_end,
                state="LISTED",
                current_order_sn=None,
                created_ms=now,
            )
            self._consignments[listing_id] = cons
            self._consignment_by_escrow[escrow_reference] = listing_id
            return cons.to_dict(now)

    async def consignment_list(self, after_listing_id: str, limit: int) -> dict[str, Any]:
        if not isinstance(after_listing_id, str) or not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        if after_listing_id:
            validate_uint64_str(after_listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        async with self._lock:
            now = self._now_ms()
            items = [
                c.to_dict(now)
                for c in sorted(self._consignments.values(), key=lambda x: x.listing_id)
                if c.listing_id > after_listing_id and c.state in ("LISTED", "LOCKED") and c.sale_end_ms > now
            ][:limit]
            return {"listings": items}

    async def consignment_get(self, listing_id: str) -> dict[str, Any]:
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        async with self._lock:
            cons = self._consignments.get(listing_id)
            if cons is None:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "寄售不存在")
            return cons.to_dict(self._now_ms())

    async def consignment_cancel(self, listing_id: str, seller_account: str) -> dict[str, Any]:
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_account(seller_account)
        async with self._lock:
            now = self._now_ms()
            cons = self._consignments.get(listing_id)
            if cons is None or cons.seller_account != seller_account:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "寄售不存在")
            if cons.state in ("RETURNING", "RETURNED"):
                return cons.to_dict(now)
            if cons.state != "LISTED":
                raise CommerceError(409, "CONSIGNMENT_LOCKED", "交易中的寄售不能下架")
            cons.state = "RETURNING"
            return cons.to_dict(now)

    async def consignment_buy(
        self,
        listing_id: str,
        buyer_player_id: str,
        buyer_account: str,
        request_id: str,
        buyer_owner_revision: str,
    ) -> dict[str, Any]:
        validate_account(buyer_account)
        validate_uint64_str(buyer_player_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_revision(buyer_owner_revision)
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_request_id(request_id)

        async with self._lock:
            prior_sn = self._consignment_order_by_req.get((buyer_account, request_id))
            if prior_sn is not None:
                return self._same_consignment_request(prior_sn, listing_id, buyer_player_id, buyer_account, buyer_owner_revision)
            if (buyer_player_id, request_id) in self._order_by_buyer_req:
                raise CommerceError(409, "REQ_USED_OTHER_ORDER", "请求号已用于其它订单")

            unpaid = sum(1 for o in self._orders.values() if o.buyer_id == buyer_player_id and o.status == ORDER_STATUS_AWAITING_PAYMENT)
            if unpaid >= self.config.max_unpaid_orders:
                raise CommerceError(429, "TOO_MANY_UNPAID", "未支付订单数量已达上限")

            cons = self._consignments.get(listing_id)
            if cons is None:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "寄售不存在")
            if cons.seller_account == buyer_account:
                raise CommerceError(409, "SELF_BUY", "同账号不能购买自己的角色或资产")
            now = self._now_ms()
            if cons.state != "LISTED" or now < cons.notice_end_ms or now >= cons.sale_end_ms:
                raise CommerceError(409, "CONSIGNMENT_UNAVAILABLE", "寄售仍在公示、已结束或已被占用")

            order_id = str(uuid.uuid4())
            seq = self._next_order_seq
            self._next_order_seq += 1

            cons.state = "LOCKED"
            cons.current_order_sn = order_id
            order = Order(
                id=seq,
                order_id=order_id,
                buyer_id=buyer_player_id,
                request_id=request_id,
                sku_id=0,
                quantity=1,
                amount_minor=cons.price_fen,
                currency="CNY",
                status=ORDER_STATUS_AWAITING_PAYMENT,
                create_time_ms=now,
                payment_deadline_ms=now + self.config.order_ttl_ms,
            )
            self._orders[order_id] = order
            self._order_by_buyer_req[(buyer_player_id, request_id)] = order_id
            self._consignment_orders[order_id] = RmbOrder(
                order_sn=order_id,
                listing_id=listing_id,
                buyer_account=buyer_account,
                buyer_owner_revision=buyer_owner_revision,
                request_id=request_id,
                seller_account=cons.seller_account,
                escrow_reference=cons.escrow_reference,
                escrow_proof=cons.escrow_proof,
                fee_bps=cons.fee_bps,
                cooling_ms=cons.cooling_ms,
            )
            self._consignment_order_by_req[(buyer_account, request_id)] = order_id
            return self._order_view(order)

    def _same_consignment_request(
        self, order_id: str, listing_id: str, buyer_player_id: str, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]:
        co = self._consignment_orders.get(order_id)
        if (
            co is None
            or co.listing_id != listing_id
            or co.buyer_account != buyer_account
            or co.buyer_owner_revision != buyer_owner_revision
        ):
            raise CommerceError(409, "CONSIGNMENT_REQ_CONFLICT", "请求号已用于不同寄售或账号")
        order = self._orders.get(order_id)
        if order is None or order.buyer_id != buyer_player_id:
            raise CommerceError(409, "CONSIGNMENT_BUYER_CONFLICT", "购买请求号的角色原件不符")
        return self._order_view(order)

    def _consignment_owned_locked(self, account: str, order_id: str) -> Order:
        validate_account(account)
        validate_order_id(order_id)
        order = self._orders.get(order_id)
        co = self._consignment_orders.get(order_id)
        if order is None or co is None or co.buyer_account != account:
            raise CommerceError(404, "CONSIGNMENT_ORDER_NOT_FOUND", "寄售订单不存在")
        return order

    def _consignment_cancelled_locked(self, order_id: str) -> None:
        co = self._consignment_orders.get(order_id)
        if co is None:
            raise CommerceError(503, "CONSIGNMENT_MAPPING_MISSING", "寄售订单关联缺失")
        cons = self._consignments.get(co.listing_id)
        if cons is None or cons.state != "LOCKED" or cons.current_order_sn != order_id:
            raise CommerceError(503, "CONSIGNMENT_STATE_MISMATCH", "寄售当前订单原件与OMS不符")
        cons.state = "RETURNING" if self._now_ms() >= cons.sale_end_ms else "LISTED"
        cons.current_order_sn = None

    def _consignment_paid_locked(self, order_id: str) -> None:
        co = self._consignment_orders.get(order_id)
        if co is None:
            raise CommerceError(503, "CONSIGNMENT_MAPPING_MISSING", "寄售订单关联缺失")
        cons = self._consignments.get(co.listing_id)
        if cons is None or cons.state != "LOCKED" or cons.current_order_sn != order_id:
            raise CommerceError(503, "CONSIGNMENT_STATE_MISMATCH", "寄售当前订单原件与OMS不符")
        cons.state = "SOLD"

    def _consignment_delivered_locked(self, order_id: str, outcome: str) -> None:
        co = self._consignment_orders.get(order_id)
        if co is None:
            return
        cons = self._consignments.get(co.listing_id)
        if cons is None or cons.state != "SOLD" or cons.current_order_sn != order_id:
            raise CommerceError(503, "CONSIGNMENT_STATE_MISMATCH", "寄售当前订单原件与OMS不符")
        order = self._orders[order_id]
        if outcome == "DELIVERED":
            fee = calc_consignment_fee(order.amount_minor, co.fee_bps)
            available_ms = self._now_ms() + co.cooling_ms
            self._seller_incomes[order_id] = RmbIncome(
                order_sn=order_id,
                account=co.seller_account,
                gross_fen=order.amount_minor,
                fee_fen=fee,
                net_fen=order.amount_minor - fee,
                available_ms=available_ms,
            )
        elif outcome == "REFUND_PENDING":
            cons.state = "RETURNING"
        elif outcome == "MANUAL":
            cons.state = "MANUAL"
        else:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")

    async def consignment_order_get(self, account: str, order_id: str) -> dict[str, Any]:
        async with self._lock:
            return self._order_view(self._consignment_owned_locked(account, order_id))

    async def consignment_order_list(self, account: str, after_order_id: str, limit: int) -> list[dict[str, Any]]:
        if not isinstance(after_order_id, str) or not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_PAGE", "寄售订单分页非法")
        validate_account(account)
        async with self._lock:
            after_seq = 0
            if after_order_id:
                after_seq = self._consignment_owned_locked(account, after_order_id).id
            matched = [
                self._orders[co.order_sn]
                for co in self._consignment_orders.values()
                if co.buyer_account == account and self._orders[co.order_sn].id > after_seq
            ]
            matched.sort(key=lambda o: o.id)
            return [self._order_view(o) for o in matched[:limit]]

    async def consignment_order_cancel(self, account: str, order_id: str) -> dict[str, Any]:
        async with self._lock:
            order = self._consignment_owned_locked(account, order_id)
            return self._cancel_order_locked(order)

    async def consignment_poll_deliveries(self, limit: int, cursor: str = "") -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        parsed = PendingScanCursor.parse(cursor)
        async with self._lock:
            pending_all = [
                d
                for d in self._delivery_intents.values()
                if d.status == "PENDING" and d.order_sn in self._consignment_orders
            ]
            if parsed is None:
                if not pending_all:
                    return {"entries": []}
                upper = max(pending_all, key=lambda d: (d.created_ms, d.order_sn))
                parsed = PendingScanCursor(0, "", upper.created_ms, upper.order_sn)
            rows = [
                d
                for d in pending_all
                if (d.created_ms, d.order_sn) > (parsed.after_ms, parsed.after_id)
                and (d.created_ms, d.order_sn) <= (parsed.upper_ms, parsed.upper_id)
            ]
            rows.sort(key=lambda d: (d.created_ms, d.order_sn))
            has_more = len(rows) > limit
            page = rows[:limit]
            entries = []
            for idx, d in enumerate(page):
                co = self._consignment_orders[d.order_sn]
                order = self._orders[d.order_sn]
                cur_str = "" if (not has_more and idx == len(page) - 1) else parsed.after(d.created_ms, d.order_sn)
                entries.append({
                    "intent": {
                        "orderId": d.order_sn,
                        "buyerPlayerId": d.buyer_id,
                        "buyerAccount": co.buyer_account,
                        "buyerOwnerRevision": co.buyer_owner_revision,
                        "listingId": co.listing_id,
                        "escrowReference": co.escrow_reference,
                        "escrowProof": co.escrow_proof,
                        "amountFen": order.amount_minor,
                    },
                    "cursor": cur_str,
                })
            return {"entries": entries}

    async def consignment_poll_returns(self, limit: int, cursor: str = "") -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        after = ""
        upper = ""
        if cursor:
            fields = cursor.split(":")
            if len(fields) != 3 or fields[0] != "r1":
                raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
            validate_uint64_str(fields[1], status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
            validate_uint64_str(fields[2], status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
            after, upper = fields[1], fields[2]
            if after > upper:
                raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")

        async with self._lock:
            now = self._now_ms()
            candidates = [
                c
                for c in self._consignments.values()
                if c.state == "RETURNING" or (c.state == "LISTED" and c.sale_end_ms <= now)
            ]
            if not upper:
                if not candidates:
                    return {"entries": []}
                upper = max(c.listing_id for c in candidates)
            rows = [c for c in candidates if after < c.listing_id <= upper]
            rows.sort(key=lambda c: c.listing_id)
            has_more = len(rows) > limit
            page = rows[:limit]
            entries = []
            for idx, c in enumerate(page):
                cur_str = "" if (not has_more and idx == len(page) - 1) else f"r1:{c.listing_id}:{upper}"
                entries.append({
                    "intent": {
                        "listingId": c.listing_id,
                        "sellerPlayerId": c.seller_player_id,
                        "sellerAccount": c.seller_account,
                        "escrowReference": c.escrow_reference,
                        "escrowProof": c.escrow_proof,
                    },
                    "cursor": cur_str,
                })
            return {"entries": entries}

    async def consignment_return_receipt(
        self, listing_id: str, escrow_reference: str, escrow_proof: str, return_proof: str
    ) -> None:
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_uint64_str(escrow_reference, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_proof(escrow_proof)
        validate_proof(return_proof)
        async with self._lock:
            cons = self._consignments.get(listing_id)
            if cons is None:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "寄售不存在")
            if cons.escrow_reference != escrow_reference or cons.escrow_proof != escrow_proof:
                raise CommerceError(409, "ESCROW_MISMATCH", "退回资产原合同不符")
            prior = self._consignment_returns.get(listing_id)
            if prior is not None:
                if prior != return_proof:
                    raise CommerceError(409, "RETURN_CONFLICT", "退回终局原件冲突")
                return
            now = self._now_ms()
            if cons.state != "RETURNING" and not (cons.state == "LISTED" and now >= cons.sale_end_ms):
                raise CommerceError(409, "NOT_RETURNING", "寄售不在待退回状态")
            self._consignment_returns[listing_id] = return_proof
            cons.state = "RETURNED"

    async def consignment_incomes(self, account: str, after_order_id: str, limit: int) -> dict[str, Any]:
        if not isinstance(after_order_id, str) or not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        validate_account(account)
        if after_order_id:
            validate_order_id(after_order_id)
        async with self._lock:
            items = [
                inc.to_dict()
                for inc in sorted(self._seller_incomes.values(), key=lambda x: x.order_sn)
                if inc.account == account and inc.order_sn > after_order_id
            ][:limit]
            return {"incomes": items}

    async def consignment_withdraw_create(
        self, account: str, request_id: str, amount_fen: int
    ) -> dict[str, Any]:
        if not isinstance(amount_fen, int) or isinstance(amount_fen, bool) or not 1 <= amount_fen <= MAX_AMOUNT_MINOR:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        validate_account(account)
        validate_request_id(request_id)
        async with self._lock:
            prior_id = self._withdrawal_by_req.get((account, request_id))
            if prior_id is not None:
                w = self._withdrawals[prior_id]
                if w.amount_fen != amount_fen:
                    raise CommerceError(409, "WITHDRAW_AMOUNT_CONFLICT", "提现请求号已用于不同金额")
                return w.to_dict()

            now = self._now_ms()
            funds = [
                inc
                for inc in sorted(self._seller_incomes.values(), key=lambda x: x.order_sn)
                if inc.account == account
                and inc.available_ms <= now
                and (inc.net_fen - inc.reserved_fen - inc.paid_fen - inc.forfeited_fen) > 0
            ][:100]

            remaining = amount_fen
            for inc in funds:
                avail = inc.net_fen - inc.reserved_fen - inc.paid_fen - inc.forfeited_fen
                remaining -= min(remaining, avail)
            if remaining != 0:
                raise CommerceError(409, "INSUFFICIENT_INCOME", "可提现收入不足或本次分配超过100单上限")

            wid = str(uuid.uuid4())
            w = RmbWithdrawal(
                withdrawal_id=wid,
                account=account,
                request_id=request_id,
                amount_fen=amount_fen,
                state="REQUESTED",
                created_ms=now,
            )
            self._withdrawals[wid] = w
            self._withdrawal_by_req[(account, request_id)] = wid
            allocs: dict[str, int] = {}
            remaining = amount_fen
            for inc in funds:
                avail = inc.net_fen - inc.reserved_fen - inc.paid_fen - inc.forfeited_fen
                take = min(remaining, avail)
                if take == 0:
                    break
                inc.reserved_fen += take
                allocs[inc.order_sn] = take
                remaining -= take
            self._withdrawal_allocs[wid] = allocs
            return w.to_dict()

    async def consignment_withdraw_get(self, account: str, withdrawal_id: str) -> dict[str, Any]:
        validate_order_id(withdrawal_id)
        validate_account(account)
        async with self._lock:
            w = self._withdrawals.get(withdrawal_id)
            if w is None or w.account != account:
                raise CommerceError(404, "WITHDRAWAL_NOT_FOUND", "提现单不存在")
            return w.to_dict()

    def _complete_withdrawal_locked(
        self, withdrawal_id: str, channel: str, transaction_id: str, amount_fen: int, currency: str, successful: bool
    ) -> dict[str, Any]:
        if currency != "CNY" or amount_fen < 1:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        w = self._withdrawals.get(withdrawal_id)
        if w is None:
            raise CommerceError(404, "WITHDRAWAL_NOT_FOUND", "提现单不存在")
        if w.amount_fen != amount_fen:
            raise CommerceError(409, "WITHDRAW_AMOUNT_MISMATCH", "出款金额不符")

        prior = self._withdrawal_receipts.get(withdrawal_id)
        if prior is not None:
            if prior[0] != channel or prior[1] != transaction_id or prior[3] != successful:
                raise CommerceError(409, "WITHDRAW_RECEIPT_CONFLICT", "出款终局回执冲突")
            return w.to_dict()

        if (channel, transaction_id) in self._withdrawal_receipt_by_txn:
            raise CommerceError(409, "WITHDRAW_TXN_REUSED", "出款流水已用于其它提现")
        if w.state != "REQUESTED":
            raise CommerceError(409, "WITHDRAW_NOT_REQUESTED", "提现单不在待出款状态")

        allocs = self._withdrawal_allocs.get(withdrawal_id, {})
        allocated = 0
        for order_sn, amt in sorted(allocs.items()):
            inc = self._seller_incomes.get(order_sn)
            if inc is None or inc.account != w.account or inc.reserved_fen < amt:
                raise CommerceError(503, "INCOME_RESERVATION_BROKEN", "收入预留原件不符")
            inc.reserved_fen -= amt
            if successful:
                inc.paid_fen += amt
            allocated += amt
        if allocated != amount_fen:
            raise CommerceError(503, "ALLOCATION_SUM_MISMATCH", "出款分配总额不符")

        self._withdrawal_receipts[withdrawal_id] = (channel, transaction_id, amount_fen, successful)
        self._withdrawal_receipt_by_txn[(channel, transaction_id)] = withdrawal_id
        w.state = "PAID" if successful else "FAILED"
        w.channel = channel
        w.transaction_id = transaction_id
        return w.to_dict()

    # -- Mock Payment & Payout --------------------------------------------------------------------------------

    async def mock_pay(self, order_id: str, request_id: str) -> dict[str, Any]:
        self.config.require_mock_payment()
        validate_order_id(order_id)
        validate_request_id(request_id)
        async with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "原订单不存在")
            key = ("PAYMENT", order_id)
            fact = self._mock_txs.get(key)
            if fact is not None:
                if fact.request_id != request_id:
                    raise CommerceError(409, "MOCK_REQ_CONFLICT", "原订单已绑定其他mock请求号")
                if fact.amount_minor != order.amount_minor or fact.currency != order.currency:
                    raise CommerceError(503, "MOCK_AMOUNT_BROKEN", "mock原件与冻结订单金额不符")
            else:
                if ("PAYMENT", request_id) in self._mock_tx_by_req:
                    raise CommerceError(409, "MOCK_REQ_REUSED", "mock请求号或原订单已绑定其他交易")
                if order.status not in (ORDER_STATUS_AWAITING_PAYMENT, ORDER_STATUS_CANCELLED) or order_id in self._bound_receipt_by_order:
                    raise CommerceError(409, "ORDER_NOT_PAYABLE", "原订单已有付款或不可付款")
                txn = f"mock-pay-{uuid.uuid4()}"
                fact = _MockTxFact(
                    kind="PAYMENT",
                    order_sn=order_id,
                    request_id=request_id,
                    transaction_id=txn,
                    payment_transaction_id=txn,
                    amount_minor=order.amount_minor,
                    currency=order.currency,
                    created_ms=self._now_ms(),
                    applied=0,
                )
                self._mock_txs[key] = fact
                self._mock_tx_by_req[("PAYMENT", request_id)] = order_id
                self._mock_tx_by_id[txn] = key

            outcome = self._record_payment_locked(
                order_id, "mock", fact.transaction_id, fact.amount_minor, fact.currency
            )
            fact.applied = 1
            return outcome["order"]

    async def mock_reconcile(self) -> tuple[int, dict[str, Any]]:
        self.config.require_mock_payment()
        attempted = 0
        succeeded = 0
        failed = 0
        async with self._lock:
            # 1. 补偿未应用 mock 付款原件
            pending_pays = [
                f for f in sorted(self._mock_txs.values(), key=lambda x: x.order_sn)
                if f.kind == "PAYMENT" and f.applied == 0
            ][:20]
            for fact in pending_pays:
                attempted += 1
                try:
                    self._record_payment_locked(
                        fact.order_sn, "mock", fact.transaction_id, fact.amount_minor, fact.currency
                    )
                    fact.applied = 1
                    succeeded += 1
                except CommerceError:
                    failed += 1

            # 2. 补偿待退款的 mock 订单
            mock_orders = [
                f for f in sorted(self._mock_txs.values(), key=lambda x: x.order_sn)
                if f.kind == "PAYMENT"
            ]
            for p_fact in mock_orders:
                if attempted >= 40:
                    break
                r_intent = self._refund_intents.get(("mock", p_fact.transaction_id))
                r_fact = self._mock_txs.get(("REFUND", p_fact.order_sn))
                needs_refund = (r_intent is not None and r_intent.status == "PENDING") or (
                    r_fact is not None and r_fact.applied == 0
                )
                if not needs_refund:
                    continue
                attempted += 1
                try:
                    if r_fact is None:
                        r_txn = f"mock-refund-{uuid.uuid4()}"
                        r_req = f"refund_{p_fact.order_sn.replace('-', '')}"
                        r_fact = _MockTxFact(
                            kind="REFUND",
                            order_sn=p_fact.order_sn,
                            request_id=r_req,
                            transaction_id=r_txn,
                            payment_transaction_id=p_fact.transaction_id,
                            amount_minor=p_fact.amount_minor,
                            currency=p_fact.currency,
                            created_ms=self._now_ms(),
                            applied=0,
                        )
                        self._mock_txs[("REFUND", p_fact.order_sn)] = r_fact
                        self._mock_tx_by_req[("REFUND", r_req)] = p_fact.order_sn
                        self._mock_tx_by_id[r_txn] = ("REFUND", p_fact.order_sn)
                    self._refund_receipt_locked(
                        r_fact.order_sn,
                        "mock",
                        r_fact.payment_transaction_id,
                        r_fact.transaction_id,
                        r_fact.amount_minor,
                        r_fact.currency,
                    )
                    r_fact.applied = 1
                    succeeded += 1
                except CommerceError:
                    failed += 1

        body = {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "scanFailures": 0,
            "busy": False,
            "budgetExhausted": False,
        }
        return (503 if failed != 0 else 200), body

    async def mock_withdraw(self, withdrawal_id: str, request_id: str) -> dict[str, Any]:
        self.config.require_mock_payment()
        validate_order_id(withdrawal_id)
        validate_request_id(request_id)
        async with self._lock:
            w = self._withdrawals.get(withdrawal_id)
            if w is None:
                raise CommerceError(404, "WITHDRAWAL_NOT_FOUND", "提现单不存在")
            fact = self._mock_payouts.get(withdrawal_id)
            if fact is not None:
                if fact.request_id != request_id:
                    raise CommerceError(409, "MOCK_PAYOUT_CONFLICT", "同一提现不能重建渠道出款请求")
            else:
                if request_id in self._mock_payout_by_req:
                    raise CommerceError(409, "MOCK_PAYOUT_REQ_REUSED", "mock出款请求号已用于其它提现")
                if w.state != "REQUESTED":
                    raise CommerceError(409, "WITHDRAW_NOT_REQUESTED", "提现单不在待出款状态")
                txn = f"mock-payout-{uuid.uuid4()}"
                fact = _MockPayoutFact(
                    withdrawal_id=withdrawal_id,
                    request_id=request_id,
                    transaction_id=txn,
                    amount_fen=w.amount_fen,
                    successful=True,
                    created_ms=self._now_ms(),
                    applied=0,
                )
                self._mock_payouts[withdrawal_id] = fact
                self._mock_payout_by_req[request_id] = withdrawal_id
                self._mock_payout_by_txn[txn] = withdrawal_id

            result = self._complete_withdrawal_locked(
                withdrawal_id, "mock", fact.transaction_id, fact.amount_fen, "CNY", fact.successful
            )
            fact.applied = 1
            return result

    async def mock_withdrawals_reconcile(self) -> tuple[int, dict[str, Any]]:
        self.config.require_mock_payment()
        attempted = 0
        succeeded = 0
        failed = 0
        async with self._lock:
            pending = [
                f for f in sorted(self._mock_payouts.values(), key=lambda x: x.withdrawal_id)
                if f.applied == 0
            ][:40]
            for fact in pending:
                attempted += 1
                try:
                    self._complete_withdrawal_locked(
                        fact.withdrawal_id, "mock", fact.transaction_id, fact.amount_fen, "CNY", fact.successful
                    )
                    fact.applied = 1
                    succeeded += 1
                except CommerceError:
                    failed += 1
        body = {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "scanFailures": 0,
            "busy": False,
            "budgetExhausted": False,
        }
        return (503 if failed != 0 else 200), body
