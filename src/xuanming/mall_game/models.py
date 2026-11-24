"""mall_game 领域模型、状态常量、格式校验与业务异常定义。

所有 JSON 字段命名与 Java ``mall_game`` 控制器/API 记录逐字一致（camelCase），
保证现有 Python ``payment/mall_client.py``、``trade/mall_adapter.py`` 与 ``portal_mall_admin.py`` 零改造直连。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# 金额与发放上限常量（与 Java CommerceStore / DeliveryStore 对齐）
MAX_AMOUNT_MINOR = 9_000_000_000_000_000
MAX_CURRENCY_GRANT = 9_000_000_000_000_000
MAX_ITEM_GRANT_COUNT = 4096
INT32_MAX = 2_147_483_647
INT64_MAX = 9_223_372_036_854_775_807
UINT64_MAX = (1 << 64) - 1
EXPIRY_GRACE_MS = 60_000

# 存储合同戳（与 Java SchemaVerifier.STORAGE_CONTRACT 一致）
STORAGE_CONTRACT = "mall-game-official-sku-v3-manual-resolution"

# 订单状态编码（对应 oms_order.status）
ORDER_STATUS_AWAITING_PAYMENT = 0
ORDER_STATUS_DELIVERY_PENDING = 1
ORDER_STATUS_DELIVERED = 2
ORDER_STATUS_CANCELLED = 4
ORDER_STATUS_REFUND_PENDING = 5
ORDER_STATUS_REFUNDED = 6
ORDER_STATUS_MANUAL = 7
ORDER_STATUS_REVERSING = 8
ORDER_STATUS_REVERSED = 9

ORDER_STATE_NAMES: dict[int, str] = {
    ORDER_STATUS_AWAITING_PAYMENT: "AWAITING_PAYMENT",
    ORDER_STATUS_DELIVERY_PENDING: "DELIVERY_PENDING",
    ORDER_STATUS_DELIVERED: "DELIVERED",
    ORDER_STATUS_CANCELLED: "CANCELLED",
    ORDER_STATUS_REFUND_PENDING: "REFUND_PENDING",
    ORDER_STATUS_REFUNDED: "REFUNDED",
    ORDER_STATUS_MANUAL: "MANUAL",
    ORDER_STATUS_REVERSING: "REVERSING",
    ORDER_STATUS_REVERSED: "REVERSED",
}

PAID_ORDER_STATUSES = frozenset((
    ORDER_STATUS_DELIVERY_PENDING,
    ORDER_STATUS_DELIVERED,
    ORDER_STATUS_REFUND_PENDING,
    ORDER_STATUS_REFUNDED,
    ORDER_STATUS_MANUAL,
    ORDER_STATUS_REVERSING,
    ORDER_STATUS_REVERSED,
))

BOUND_OFFICIAL_REVERSAL_STATUSES = frozenset((
    ORDER_STATUS_DELIVERY_PENDING,
    ORDER_STATUS_DELIVERED,
    ORDER_STATUS_MANUAL,
    ORDER_STATUS_REVERSING,
    ORDER_STATUS_REVERSED,
))

# 付款事实
FACT_PAID = "PAID"
FACT_REVOKED = "REVOKED"

# 退款原因（对应 CommerceApi.RefundReason）
REFUND_CANCELLED_LATE = "CANCELLED_LATE"
REFUND_DELIVERY_REJECTED = "DELIVERY_REJECTED"
REFUND_DUPLICATE_PAYMENT = "DUPLICATE_PAYMENT"
REFUND_AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
REFUND_CURRENCY_MISMATCH = "CURRENCY_MISMATCH"

OWN_PAYMENT_REFUND_REASONS = frozenset((REFUND_CANCELLED_LATE, REFUND_DELIVERY_REJECTED))
ABNORMAL_PAYMENT_REASONS = frozenset((
    REFUND_DUPLICATE_PAYMENT,
    REFUND_AMOUNT_MISMATCH,
    REFUND_CURRENCY_MISMATCH,
))
ALL_REFUND_REASONS = OWN_PAYMENT_REFUND_REASONS | ABNORMAL_PAYMENT_REASONS

# 允许的游戏货币类型（0=银两, 1=灵石, 2=绑定灵石）
ALLOWED_CURRENCY_TYPES = frozenset((0, 1, 2))
RMB_ASSET_KINDS = frozenset(("ITEMS", "CURRENCY", "PET", "CHARACTER"))

# 人工处置动作集合
MANUAL_ORDER_ACTIONS = frozenset(("RETRY_REVERSAL", "MARK_REVERSED", "MARK_DELIVERED", "CREATE_REFUND"))
MANUAL_REFUND_ACTIONS = frozenset(("RETRY_REFUND", "MARK_REFUNDED", "MARK_REVOKED"))
MANUAL_CONSIGNMENT_ACTIONS = frozenset(("RETURN_TO_SELLER", "UNFREEZE_INCOME", "FORFEIT_INCOME"))

# 严格格式正则
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
CHANNEL_RE = re.compile(r"^[a-z][a-z0-9_-]{1,31}$")
TXN_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
CURRENCY_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,15}$")
SKU_CODE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
PROOF_RE = re.compile(r"^[0-9a-f]{64}$")
PLAYER_ID_RE = re.compile(r"^[1-9][0-9]{0,19}$")
ACTOR_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
CLOSE_REASON_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
NON_NEG_INT_RE = re.compile(r"^(?:0|[1-9][0-9]*)$")


class CommerceError(Exception):
    """商城统一业务/协议异常，映射到确定性 HTTP 状态码与 JSON 错误体。"""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = int(status)
        self.code = str(code)
        self.message = str(message)


def order_state_name(status: int) -> str:
    """将 oms_order.status 整数映射为协议对外状态名。"""
    name = ORDER_STATE_NAMES.get(status)
    if name is None:
        raise CommerceError(503, "INVALID_ORDER_STATE", "订单状态无法处理")
    return name


def refunds_orders_own_payment(reason: str) -> bool:
    """判断退款原因是否退订单绑定付款（True）或异常付款（False）。"""
    if reason in OWN_PAYMENT_REFUND_REASONS:
        return True
    if reason in ABNORMAL_PAYMENT_REASONS:
        return False
    raise CommerceError(503, "UNKNOWN_REFUND_REASON", "退款原因无法识别")


def validate_uint64_str(value: Any, *, status: int = 400, code: str = "INVALID_ID", message: str = "标识非法") -> str:
    """校验规范无前导零正 uint64 十进制字符串。"""
    if not isinstance(value, str) or not PLAYER_ID_RE.fullmatch(value):
        raise CommerceError(status, code, message)
    num = int(value)
    if num <= 0 or num > UINT64_MAX or str(num) != value:
        raise CommerceError(status, code, message)
    return value


def validate_order_id(value: Any) -> str:
    if not isinstance(value, str) or not UUID_RE.fullmatch(value):
        raise CommerceError(400, "INVALID_ORDER_ID", "订单号非法")
    return value


def validate_account(value: Any, *, status: int = 400) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 191:
        raise CommerceError(status, "INVALID_ACCOUNT", "订单账号非法")
    return value


def validate_revision(value: Any, *, status: int = 400) -> str:
    return validate_uint64_str(value, status=status, code="INVALID_OWNER_REVISION", message="订单角色归属修订非法")


def validate_request_id(value: Any) -> str:
    if not isinstance(value, str) or not REQUEST_ID_RE.fullmatch(value):
        raise CommerceError(400, "INVALID_REQUEST_ID", "请求号非法")
    return value


def validate_proof(value: Any) -> str:
    if not isinstance(value, str) or not PROOF_RE.fullmatch(value):
        raise CommerceError(400, "INVALID_PROOF", "证明摘要非法")
    return value


def validate_channel(value: Any) -> str:
    if not isinstance(value, str) or not CHANNEL_RE.fullmatch(value):
        raise CommerceError(400, "INVALID_CHANNEL", "支付渠道非法")
    return value


def validate_txn_id(value: Any) -> str:
    if not isinstance(value, str) or not TXN_ID_RE.fullmatch(value):
        raise CommerceError(400, "INVALID_TRANSACTION_ID", "交易流水号非法")
    return value


def validate_currency(value: Any) -> str:
    if not isinstance(value, str) or not CURRENCY_RE.fullmatch(value):
        raise CommerceError(400, "INVALID_CURRENCY", "币种非法")
    return value


def validate_sku_terms(terms: SkuTerms) -> None:
    """校验 SKU 条款与发放物二选一规则（与 Java CatalogAdminStore.validate 对齐）。"""
    if (
        terms.product_id <= 0
        or not isinstance(terms.sku_code, str)
        or not SKU_CODE_RE.fullmatch(terms.sku_code)
        or terms.price_minor < 1
        or terms.price_minor > MAX_AMOUNT_MINOR
        or not isinstance(terms.currency, str)
        or not CURRENCY_RE.fullmatch(terms.currency)
        or not isinstance(terms.title, str)
        or not terms.title.strip()
        or len(terms.title) > 128
    ):
        raise CommerceError(400, "INVALID_SKU_TERMS", "SKU条款非法")
    is_item = (
        terms.item_config_id >= 1
        and 1 <= terms.count_per_unit <= MAX_ITEM_GRANT_COUNT
        and terms.currency_type == 0
        and terms.currency_amount == 0
    )
    is_currency = (
        terms.item_config_id == 0
        and terms.count_per_unit == 0
        and terms.currency_type in ALLOWED_CURRENCY_TYPES
        and 1 <= terms.currency_amount <= MAX_CURRENCY_GRANT
    )
    if not (is_item or is_currency):
        raise CommerceError(400, "INVALID_SKU_TERMS", "SKU条款非法")


def calc_consignment_fee(gross_fen: int, fee_bps: int) -> int:
    """人民币寄售手续费向上取整算法（与 Java RmbConsignmentStore.fee 逐字一致）。"""
    if gross_fen < 1 or gross_fen > MAX_AMOUNT_MINOR or not 0 <= fee_bps <= 10_000:
        raise CommerceError(400, "INVALID_FEE_ARGS", "人民币寄售或结算参数非法")
    return (gross_fen // 10_000) * fee_bps + ((gross_fen % 10_000) * fee_bps + 9_999) // 10_000


@dataclass(frozen=True, slots=True)
class PendingScanCursor:
    """待办稳定扫描复合游标：``v1:<afterMs>:<afterId>:<upperMs>:<upperId>``。"""

    after_ms: int
    after_id: str
    upper_ms: int
    upper_id: str

    def after(self, created_ms: int, order_id: str) -> str:
        return f"v1:{created_ms}:{order_id}:{self.upper_ms}:{self.upper_id}"

    @classmethod
    def parse(cls, raw: str | None) -> PendingScanCursor | None:
        if not raw:
            return None
        if not isinstance(raw, str) or len(raw) > 256:
            raise CommerceError(400, "INVALID_CURSOR", "交付游标非法或越过扫描上界")
        fields = raw.split(":")
        if (
            len(fields) != 5
            or fields[0] != "v1"
            or not NON_NEG_INT_RE.fullmatch(fields[1])
            or not NON_NEG_INT_RE.fullmatch(fields[3])
            or not UUID_RE.fullmatch(fields[2])
            or not UUID_RE.fullmatch(fields[4])
        ):
            raise CommerceError(400, "INVALID_CURSOR", "交付游标非法或越过扫描上界")
        after_ms = int(fields[1])
        upper_ms = int(fields[3])
        if after_ms > INT64_MAX or upper_ms > INT64_MAX or after_ms > upper_ms or (
            after_ms == upper_ms and fields[2] > fields[4]
        ):
            raise CommerceError(400, "INVALID_CURSOR", "交付游标非法或越过扫描上界")
        return cls(after_ms=after_ms, after_id=fields[2], upper_ms=upper_ms, upper_id=fields[4])


# -- 领域数据结构 -------------------------------------------------------------------------------------------

@dataclass(slots=True)
class SkuTerms:
    """SKU 销售条款与游戏发放映射（物品或货币二选一）。"""

    product_id: int
    sku_code: str
    price_minor: int
    currency: str
    published: bool
    item_config_id: int = 0
    count_per_unit: int = 0
    title: str = ""
    currency_type: int = 0
    currency_amount: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "productId": self.product_id,
            "skuCode": self.sku_code,
            "priceMinor": self.price_minor,
            "currency": self.currency,
            "published": self.published,
            "itemConfigId": self.item_config_id,
            "countPerUnit": self.count_per_unit,
            "title": self.title,
            "currencyType": self.currency_type,
            "currencyAmount": self.currency_amount,
        }


@dataclass(slots=True)
class Sku:
    """官方商品 SKU 存储记录（对应 pms_sku_stock + commerce_sku_grant）。"""

    sku_id: int
    revision: int
    product_id: int
    sku_code: str
    price_minor: int
    currency: str
    stock: int
    locked_stock: int
    published: bool
    item_config_id: int = 0
    count_per_unit: int = 0
    title: str = ""
    currency_type: int = 0
    currency_amount: int = 0

    def terms(self) -> SkuTerms:
        return SkuTerms(
            product_id=self.product_id,
            sku_code=self.sku_code,
            price_minor=self.price_minor,
            currency=self.currency,
            published=self.published,
            item_config_id=self.item_config_id,
            count_per_unit=self.count_per_unit,
            title=self.title,
            currency_type=self.currency_type,
            currency_amount=self.currency_amount,
        )

    def to_admin_dict(self) -> dict[str, Any]:
        return {
            "skuId": self.sku_id,
            "revision": self.revision,
            "productId": self.product_id,
            "skuCode": self.sku_code,
            "priceMinor": self.price_minor,
            "currency": self.currency,
            "stock": self.stock,
            "lockedStock": self.locked_stock,
            "published": self.published,
            "itemConfigId": self.item_config_id,
            "countPerUnit": self.count_per_unit,
            "title": self.title,
            "currencyType": self.currency_type,
            "currencyAmount": self.currency_amount,
        }

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "skuId": self.sku_id,
            "title": self.title,
            "priceMinor": self.price_minor,
            "currency": self.currency,
            "available": self.stock - self.locked_stock,
        }


@dataclass(slots=True)
class OrderGrant:
    """下单冻结的发放物（对应 commerce_order_grant）。"""

    order_sn: str
    item_config_id: int
    count_per_unit: int
    currency_type: int
    currency_amount: int
    title: str


@dataclass(slots=True)
class Order:
    """商城订单记录（对应 oms_order）。"""

    id: int
    order_id: str
    buyer_id: str
    request_id: str
    sku_id: int
    quantity: int
    amount_minor: int
    currency: str
    status: int
    create_time_ms: int
    payment_deadline_ms: int
    payment_time_ms: int | None = None
    payment_revision: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "orderId": self.order_id,
            "buyerId": self.buyer_id,
            "skuId": self.sku_id,
            "quantity": self.quantity,
            "amountMinor": self.amount_minor,
            "currency": self.currency,
            "state": order_state_name(self.status),
            "paymentDeadlineMs": self.payment_deadline_ms,
            "paymentRevision": self.payment_revision,
        }


@dataclass(slots=True)
class PaymentReceipt:
    """渠道付款回执（对应 commerce_payment_receipt）。"""

    channel: str
    transaction_id: str
    order_sn: str | None
    amount_minor: int
    currency: str
    revision: int = 0
    fact: str = FACT_PAID


@dataclass(slots=True)
class AbnormalPayment:
    """异常付款记录（对应 commerce_abnormal_payment）。"""

    channel: str
    transaction_id: str
    order_sn: str
    reason: str
    created_ms: int


@dataclass(slots=True)
class DeliveryIntent:
    """交付待办（对应 commerce_delivery_intent）。"""

    order_sn: str
    buyer_id: str
    sku_id: int
    quantity: int
    status: str
    created_ms: int


@dataclass(slots=True)
class RefundIntent:
    """退款待办（对应 commerce_refund_intent）。"""

    channel: str
    transaction_id: str
    order_sn: str
    source: str
    status: str
    close_reason: str | None = None


@dataclass(slots=True)
class ReversalIntent:
    """付款撤销/恢复发放收敛待办（对应 commerce_reversal_intent）。"""

    order_sn: str
    revision: int
    status: str
    updated_ms: int


@dataclass(slots=True)
class PaymentRevision:
    """渠道付款修订记录视图。"""

    revision: int
    fact: str
    order_state: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "fact": self.fact,
            "orderState": self.order_state,
        }


@dataclass(slots=True)
class RmbConsignment:
    """人民币寄售挂单记录（对应 commerce_consignment）。"""

    listing_id: str
    seller_player_id: str
    seller_account: str
    market_zone: int
    asset_kind: str
    asset_currency_type: int | None
    escrow_reference: str
    escrow_proof: str
    request_digest: str
    title: str
    price_fen: int
    fee_bps: int
    cooling_ms: int
    notice_end_ms: int
    sale_end_ms: int
    state: str
    current_order_sn: str | None
    created_ms: int

    def to_dict(self, now_ms: int) -> dict[str, Any]:
        if self.state == "LISTED":
            if now_ms < self.notice_end_ms:
                phase = "PUBLIC_NOTICE"
            elif now_ms < self.sale_end_ms:
                phase = "ON_SALE"
            else:
                phase = "ENDED"
        else:
            phase = self.state
        return {
            "listingId": self.listing_id,
            "sellerPlayerId": self.seller_player_id,
            "marketZone": self.market_zone,
            "assetKind": self.asset_kind,
            "title": self.title,
            "priceFen": self.price_fen,
            "feeBps": self.fee_bps,
            "coolingMs": self.cooling_ms,
            "noticeEndMs": self.notice_end_ms,
            "saleEndMs": self.sale_end_ms,
            "state": self.state,
            "phase": phase,
            "orderId": self.current_order_sn,
        }


@dataclass(slots=True)
class RmbOrder:
    """人民币寄售订单关联（对应 commerce_consignment_order）。"""

    order_sn: str
    listing_id: str
    buyer_account: str
    buyer_owner_revision: str
    request_id: str
    seller_account: str
    escrow_reference: str
    escrow_proof: str
    fee_bps: int
    cooling_ms: int


@dataclass(slots=True)
class RmbIncome:
    """寄售卖家人民币收入账本行（对应 commerce_seller_income）。"""

    order_sn: str
    account: str
    gross_fen: int
    fee_fen: int
    net_fen: int
    reserved_fen: int = 0
    paid_fen: int = 0
    forfeited_fen: int = 0
    available_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "orderId": self.order_sn,
            "grossFen": self.gross_fen,
            "feeFen": self.fee_fen,
            "netFen": self.net_fen,
            "reservedFen": self.reserved_fen,
            "paidFen": self.paid_fen,
            "availableMs": self.available_ms,
            "forfeitedFen": self.forfeited_fen,
        }


@dataclass(slots=True)
class RmbWithdrawal:
    """寄售卖家提现单（对应 commerce_withdrawal）。"""

    withdrawal_id: str
    account: str
    request_id: str
    amount_fen: int
    state: str
    channel: str | None = None
    transaction_id: str | None = None
    created_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "withdrawalId": self.withdrawal_id,
            "amountFen": self.amount_fen,
            "state": self.state,
            "channel": self.channel,
            "transactionId": self.transaction_id,
        }
