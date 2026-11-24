"""payment 领域常量、确定性键派生、未成年人限额纯规则与不可变数据契约。

与 Go ``go/payment/internal/data/tables.go``、``internal/core/`` 及 ``shared/antiaddiction`` 逐字对齐。
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import IntEnum
from typing import Protocol

from .errors import InvalidKeyError

# 节点与独占库常量（proto/common/base/node.proto: PaymentNodeService = 19）
PAYMENT_NODE_TYPE = 19
DATABASE_NAME = "mmorpg_payment"

# PayChannel 枚举值（与 proto/payment/payment.proto 一致）
PAY_CHANNEL_UNSPECIFIED = 0
PAY_CHANNEL_DEVPAY = 1
PAY_CHANNEL_WECHAT_PAY = 2
PAY_CHANNEL_ALIPAY = 3
PAY_CHANNEL_WECHAT_MINIGAME = 4
PAY_CHANNEL_DOUYIN_MINIGAME = 5
PAY_CHANNEL_APPLE = 6
PAY_CHANNEL_GOOGLE = 7

# 渠道在商城回执与配置中的名字（已上线不改）
CHANNEL_DEVPAY = "devpay"
CHANNEL_WECHAT_PAY = "wechat_pay"
CHANNEL_ALIPAY = "alipay"
CHANNEL_WECHAT_MINIGAME = "wechat_minigame"
CHANNEL_DOUYIN_MINIGAME = "douyin_minigame"
CHANNEL_APPLE = "apple"
CHANNEL_GOOGLE = "google"

CHANNEL_NAME_BY_ID: dict[int, str] = {
    PAY_CHANNEL_DEVPAY: CHANNEL_DEVPAY,
    PAY_CHANNEL_WECHAT_PAY: CHANNEL_WECHAT_PAY,
    PAY_CHANNEL_ALIPAY: CHANNEL_ALIPAY,
    PAY_CHANNEL_WECHAT_MINIGAME: CHANNEL_WECHAT_MINIGAME,
    PAY_CHANNEL_DOUYIN_MINIGAME: CHANNEL_DOUYIN_MINIGAME,
    PAY_CHANNEL_APPLE: CHANNEL_APPLE,
    PAY_CHANNEL_GOOGLE: CHANNEL_GOOGLE,
}
CHANNEL_ID_BY_NAME: dict[str, int] = {name: cid for cid, name in CHANNEL_NAME_BY_ID.items()}

# PayClientPlatform 枚举值（与 proto/payment/payment.proto 一致）
PLATFORM_UNSPECIFIED = 0
PLATFORM_ANDROID_APP = 1
PLATFORM_IOS_APP = 2
PLATFORM_WECHAT_MINIGAME = 3
PLATFORM_DOUYIN_MINIGAME = 4
PLATFORM_DESKTOP = 5

PLATFORM_NAME_BY_ID: dict[int, str] = {
    PLATFORM_ANDROID_APP: "android_app",
    PLATFORM_IOS_APP: "ios_app",
    PLATFORM_WECHAT_MINIGAME: "wechat_minigame",
    PLATFORM_DOUYIN_MINIGAME: "douyin_minigame",
    PLATFORM_DESKTOP: "desktop",
}
PLATFORM_ID_BY_NAME: dict[str, int] = {name: pid for pid, name in PLATFORM_NAME_BY_ID.items()}
VALID_PLATFORMS = frozenset(PLATFORM_NAME_BY_ID)

# 各发行形态允许挂载的渠道（苹果审核规则 3.1.1：iOS App 仅允许 apple，dev/test 可挂 devpay）
ALLOWED_PLATFORM_CHANNELS: dict[str, tuple[str, ...]] = {
    "android_app": (CHANNEL_WECHAT_PAY, CHANNEL_ALIPAY, CHANNEL_GOOGLE, CHANNEL_DEVPAY),
    "ios_app": (CHANNEL_APPLE, CHANNEL_DEVPAY),
    "wechat_minigame": (CHANNEL_WECHAT_MINIGAME, CHANNEL_DEVPAY),
    "douyin_minigame": (CHANNEL_DOUYIN_MINIGAME, CHANNEL_DEVPAY),
    "desktop": (CHANNEL_DEVPAY,),
}

# PayIntentState 枚举值
INTENT_CREATED = 1
INTENT_PAID = 2
INTENT_CLOSED = 3

# PayMallState 枚举值
MALL_PENDING = 1
MALL_BOUND = 2
MALL_REFUND_QUEUED = 3
MALL_REJECTED = 4
MALL_ORPHAN = 5

# PayRefundState 枚举值
REFUND_NONE = 0
REFUND_REQUESTED = 1
REFUND_SUCCEEDED = 2
REFUND_REPORTED = 3
REFUND_FAILED = 4

# PayFinalizeState 枚举值
FINALIZE_NONE = 0
FINALIZE_PENDING = 1
FINALIZE_DONE = 2
FINALIZE_FAILED = 3

# 商城契约常量
ORDER_STATE_AWAITING_PAYMENT = "AWAITING_PAYMENT"
FACT_PAID = "PAID"
FACT_REVOKED = "REVOKED"

# 特殊原因常量（与 Go internal/core/service.go 对齐）
REVOCATION_TOMBSTONE_REASON = "revoked at the channel before payment notification arrived; not delivered"
MINOR_LATE_OVER_LIMIT_REASON = "minor spending limit reached by a payment that arrived after its intent closed"
LIMIT_CURRENCY = "CNY"

# 合法标识符正则（与 Go internal/data/tables.go 对齐）
_ORDER_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_OUT_TRADE_NO_RE = re.compile(r"^[0-9a-f]{32}$")
_TXN_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

# 防沉迷北京时区与国家实名标识 pi 编码（shared/antiaddiction）
BEIJING_TZ = timezone(timedelta(hours=8))
ADULT_AGE = 18
PI_LENGTH = 38
_PI_DIGITS = "0123456789abcdefghijklmnop"


class TradeState(IntEnum):
    """渠道侧订单核实状态。"""

    NOT_PAID = 1
    PAID = 2
    CLOSED = 3
    REFUNDED_OUTSIDE = 4


class ChannelRefundState(IntEnum):
    """渠道退款接口返回状态。"""

    PROCESSING = 1
    SUCCEEDED = 2
    FAILED = 3


class AckKind(IntEnum):
    """回调入口 HTTP 应答类型。"""

    OK = 1
    RETRY = 2
    REJECTED = 3


# ---------------------------------------------------------------------------
# 确定性键派生与工具函数
# ---------------------------------------------------------------------------


def channel_name_of(channel_id: int) -> str:
    """渠道枚举转渠道名；未知渠道返回空串。"""
    return CHANNEL_NAME_BY_ID.get(channel_id, "")


def is_valid_platform(platform: int) -> bool:
    return platform in VALID_PLATFORMS


def is_revoked(fact_revision: int) -> bool:
    """修订号奇偶代表事实方向：偶数为 PAID，奇数为 REVOKED。"""
    return (fact_revision % 2) == 1


def refunds_orders_own_payment(reason: str) -> bool:
    """判断商城退款原因退的是否为订单本身绑定的那笔付款。"""
    return reason in ("CANCELLED_LATE", "DELIVERY_REJECTED")


def is_platform_managed_channel(channel_id: int) -> bool:
    """微信/抖音小游戏渠道由平台方执行实名与未成年人消费限额。"""
    return channel_id in (PAY_CHANNEL_WECHAT_MINIGAME, PAY_CHANNEL_DOUYIN_MINIGAME)


def order_key(order_id: str) -> tuple[int, int]:
    """将小写 UUID 订单号拆成大端 (order_hi, order_lo)；全零 UUID 视为非法。"""
    if not _ORDER_ID_RE.fullmatch(order_id):
        raise InvalidKeyError(f"invalid order_id format: {order_id!r}")
    raw = bytes.fromhex(order_id.replace("-", ""))
    hi = int.from_bytes(raw[:8], "big")
    lo = int.from_bytes(raw[8:], "big")
    if hi == 0 and lo == 0:
        raise InvalidKeyError("zero UUID is reserved")
    return hi, lo


def order_id_from_key(hi: int, lo: int) -> str:
    """从 (order_hi, order_lo) 还原标准小写 UUID 文本。"""
    raw = hi.to_bytes(8, "big") + lo.to_bytes(8, "big")
    text = raw.hex()
    return f"{text[0:8]}-{text[8:12]}-{text[12:16]}-{text[16:20]}-{text[20:32]}"


def out_trade_no(order_id: str) -> str:
    """订单号去掉连字符得到 32 位十六进制商户单号。"""
    order_key(order_id)
    return order_id.replace("-", "")


def order_id_from_out_trade_no(trade_no: str) -> str:
    """将 32 位十六进制商户单号还原为 UUID 订单号；非本服务格式抛 InvalidKeyError。"""
    if not _OUT_TRADE_NO_RE.fullmatch(trade_no):
        raise InvalidKeyError(f"invalid out_trade_no format: {trade_no!r}")
    oid = f"{trade_no[0:8]}-{trade_no[8:12]}-{trade_no[12:16]}-{trade_no[16:20]}-{trade_no[20:32]}"
    order_key(oid)
    return oid


def valid_transaction_id(transaction_id: str) -> bool:
    """校验渠道交易号是否满足商城回执白名单字符集。"""
    return bool(_TXN_ID_RE.fullmatch(transaction_id))


def txn_key(transaction_id: str) -> tuple[int, int]:
    """取 SHA-256(transaction_id) 前 16 字节大端拆分 (txn_hi, txn_lo)。"""
    digest = hashlib.sha256(transaction_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big"), int.from_bytes(digest[8:16], "big")


def spender_key(pi: str) -> tuple[int, int]:
    """由实名标识 pi 派生未成年人消费主体键 SHA-256('spender|' + pi)[:16]。"""
    digest = hashlib.sha256(f"spender|{pi}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big"), int.from_bytes(digest[8:16], "big")


def out_refund_no(channel_name: str, transaction_id: str) -> str:
    """确定性派生渠道退款单号：'rf' + SHA-256('refund|' + channel + '|' + tx_id)[:15].hex()（32 字符）。"""
    digest = hashlib.sha256(f"refund|{channel_name}|{transaction_id}".encode("utf-8")).digest()
    return "rf" + digest[:15].hex()


def close_reason(detail: str) -> str:
    """将渠道失败说明清洗为商城接受的原因码（[A-Za-z0-9_.:-]，最多 64 字节）。"""
    out: list[str] = []
    for ch in detail.encode("utf-8", errors="ignore")[:64]:
        c = chr(ch)
        if c.isalnum() or c in "_.:-":
            out.append(c)
        else:
            out.append("_")
    return "".join(out) if out else "CHANNEL_REFUND_FAILED"


def tail(identifier: str) -> str:
    """日志脱敏：仅保留交易号末 6 位。"""
    return identifier if len(identifier) <= 6 else identifier[-6:]


# ---------------------------------------------------------------------------
# 未成年人实名与限额纯规则（shared/antiaddiction）
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MinorAgeRule:
    """年龄段限额规则（分）。"""

    unlimited: bool = False
    single_fen: int = 0
    daily_fen: int = 0
    monthly_fen: int = 0


@dataclass(frozen=True, slots=True)
class SpendLimit:
    """未成年人额度与北京时间月/日窗口起点（Unix 毫秒）。"""

    monthly_fen: int
    daily_fen: int
    month_start_ms: int
    day_start_ms: int


def birth_date_from_pi(pi: str) -> date | None:
    """从 38 位国家实名标识 pi 前 6 位解码出生日期；非法返回 None。"""
    if len(pi) != PI_LENGTH:
        return None
    for ch in pi:
        if not ("0" <= ch <= "9" or "a" <= ch <= "z"):
            return None
    value = 0
    for ch in pi[:6]:
        idx = _PI_DIGITS.find(ch)
        if idx < 0:
            return None
        value = value * 26 + idx
    year, month, day = value // 10000, (value // 100) % 100, value % 100
    if year < 1900:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def pi_birth_prefix(year: int, month: int, day: int) -> str:
    """将出生日期编码为 pi 前 6 位（用于单测构造实名标识）。"""
    value = year * 10000 + month * 100 + day
    digits = [""] * 6
    for idx in range(5, -1, -1):
        digits[idx] = _PI_DIGITS[value % 26]
        value //= 26
    return "".join(digits)


def age_at(birth: date, at_ms: int) -> int:
    """按北京时间计算 at_ms 时刻的周岁。"""
    now_bj = datetime.fromtimestamp(at_ms / 1000.0, tz=BEIJING_TZ)
    age = now_bj.year - birth.year
    if (now_bj.month, now_bj.day) < (birth.month, birth.day):
        age -= 1
    return age


def minor_age_rule(age: int) -> MinorAgeRule:
    """国新出发〔2019〕34 号充值限额：未满 8 岁禁充；8-15 岁 50/50/200 元；16-17 岁 100/100/400 元；18+ 无限制。"""
    if age >= ADULT_AGE:
        return MinorAgeRule(unlimited=True)
    if age >= 16:
        return MinorAgeRule(single_fen=100_00, daily_fen=100_00, monthly_fen=400_00)
    if age >= 8:
        return MinorAgeRule(single_fen=50_00, daily_fen=50_00, monthly_fen=200_00)
    return MinorAgeRule()


def spend_limit_for_age(age: int, at_ms: int) -> SpendLimit:
    """计算指定周岁在 at_ms 所在北京自然月、自然日的限额窗口。"""
    rule = minor_age_rule(age)
    now_bj = datetime.fromtimestamp(at_ms / 1000.0, tz=BEIJING_TZ)
    month_start = datetime(now_bj.year, now_bj.month, 1, 0, 0, 0, tzinfo=BEIJING_TZ)
    day_start = datetime(now_bj.year, now_bj.month, now_bj.day, 0, 0, 0, tzinfo=BEIJING_TZ)
    return SpendLimit(
        monthly_fen=rule.monthly_fen,
        daily_fen=rule.daily_fen,
        month_start_ms=int(month_start.timestamp() * 1000),
        day_start_ms=int(day_start.timestamp() * 1000),
    )


# ---------------------------------------------------------------------------
# 数据记录与不可变消息 DTO
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PaymentIntentRecord:
    """对应 payment_intent 表的一行记录。"""

    order_hi: int = 0
    order_lo: int = 0
    order_id: str = ""
    player_id: int = 0
    channel: int = 0
    amount_minor: int = 0
    currency: str = ""
    state: int = 0
    expire_ms: int = 0
    next_check_ms: int = 0
    check_count: int = 0
    checked_ms: int = 0
    created_ms: int = 0
    updated_ms: int = 0
    channel_ref: str = ""
    channel_ref_ms: int = 0
    sku_id: int = 0
    payer_id: str = ""
    spender_hi: int = 0
    spender_lo: int = 0
    paid_ms: int = 0
    minor_age: int = 0


@dataclass(slots=True)
class PaymentTxnRecord:
    """对应 payment_txn 表的一行记录。"""

    channel: int = 0
    txn_hi: int = 0
    txn_lo: int = 0
    transaction_id: str = ""
    order_hi: int = 0
    order_lo: int = 0
    out_trade_no: str = ""
    player_id: int = 0
    paid_amount_minor: int = 0
    paid_currency: str = ""
    paid_ms: int = 0
    mall_state: int = 0
    mall_refund_reason: str = ""
    mall_attempts: int = 0
    next_attempt_ms: int = 0
    mall_last_error: str = ""
    refund_state: int = 0
    out_refund_no: str = ""
    refund_id: str = ""
    refund_ms: int = 0
    refund_attempts: int = 0
    refund_next_ms: int = 0
    refund_last_error: str = ""
    created_ms: int = 0
    updated_ms: int = 0
    fact_revision: int = 0
    fact_observed_ms: int = 0
    mall_revision: int = 0
    mall_due: int = 0
    channel_token: str = ""
    channel_order_id: str = ""
    store_amount_minor: int = 0
    store_currency: str = ""
    finalize_state: int = 0
    finalize_next_ms: int = 0
    product_id: str = ""


@dataclass(frozen=True, slots=True)
class MallSync:
    """推进商城后写回 payment_txn 的结论。"""

    state: int
    refund_reason: str = ""
    revision: int = 0
    keep_syncing: bool = False
    last_error: str = ""


@dataclass(frozen=True, slots=True)
class RefundProgress:
    """执行退款后写回 payment_txn 的进度。"""

    state: int
    out_refund_no: str = ""
    refund_id: str = ""
    refund_ms: int = 0
    next_ms: int = 0
    last_error: str = ""


@dataclass(frozen=True, slots=True)
class RecordMinorPaymentOutcome:
    """未成年人单事务落账付款事实与转已付的结论。"""

    stored: PaymentTxnRecord
    inserted: bool
    was_closed: bool = False
    over_limit: bool = False


# 商城 HTTP DTO
@dataclass(frozen=True, slots=True)
class MallOrder:
    order_id: str
    buyer_id: str
    sku_id: int
    quantity: int
    amount_minor: int
    currency: str
    state: str
    payment_deadline_ms: int


@dataclass(frozen=True, slots=True)
class MallPayment:
    order_id: str
    channel: str
    transaction_id: str
    amount_minor: int
    currency: str


@dataclass(frozen=True, slots=True)
class MallOutcome:
    order_state: str
    refund_reason: str = ""


@dataclass(frozen=True, slots=True)
class MallPendingRefund:
    order_id: str
    channel: str
    transaction_id: str
    amount_minor: int
    currency: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class MallPendingRefundPage:
    entries: tuple[MallPendingRefund, ...] = ()
    next_cursor: str = ""


@dataclass(frozen=True, slots=True)
class MallRefund:
    order_id: str
    channel: str
    payment_transaction_id: str
    transaction_id: str
    amount_minor: int
    currency: str


@dataclass(frozen=True, slots=True)
class MallRefundClosure:
    order_id: str
    channel: str
    payment_transaction_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class MallRevision:
    order_id: str
    channel: str
    transaction_id: str
    revision: int
    fact: str


# 渠道 DTO
@dataclass(frozen=True, slots=True)
class ChannelRef:
    value: str = ""
    at_ms: int = 0


@dataclass(frozen=True, slots=True)
class ChannelOrder:
    order_id: str
    out_trade_no: str
    player_id: int
    amount_minor: int
    currency: str
    expire_ms: int
    product_id: str = ""
    title: str = ""
    login_code: str = ""
    device_os: str = ""


@dataclass(frozen=True, slots=True)
class ClientParams:
    """交给客户端调起渠道 SDK 的参数载荷（对应 payment.proto 7 种渠道之一）。"""

    devpay: dict | None = None
    wechat_app: dict | None = None
    alipay_app: dict | None = None
    wechat_minigame: dict | None = None
    douyin_minigame: dict | None = None
    apple: dict | None = None
    google: dict | None = None


@dataclass(frozen=True, slots=True)
class ChannelPrepared:
    params: ClientParams = field(default_factory=ClientParams)
    ref: ChannelRef = field(default_factory=ChannelRef)
    payer_id: str = ""


@dataclass(frozen=True, slots=True)
class ChannelQueryRequest:
    out_trade_no: str
    payer_id: str = ""
    ref: ChannelRef = field(default_factory=ChannelRef)
    amount_minor: int = 0
    currency: str = ""


@dataclass(frozen=True, slots=True)
class ChannelPayment:
    transaction_id: str
    out_trade_no: str = ""
    order_id: str = ""
    amount_minor: int = 0
    currency: str = ""
    paid_ms: int = 0
    product_id: str = ""
    store_priced: bool = False
    store_amount_minor: int = 0
    store_currency: str = ""
    channel_token: str = ""
    channel_order_id: str = ""
    needs_finalize: bool = False
    test: bool = False
    account_ref: str = ""
    anomaly: str = ""


@dataclass(frozen=True, slots=True)
class ChannelQueryResult:
    state: TradeState
    payment: ChannelPayment | None = None


@dataclass(frozen=True, slots=True)
class ChannelRefundRequest:
    out_trade_no: str
    transaction_id: str
    out_refund_no: str
    amount_minor: int
    currency: str
    reason: str
    channel_token: str = ""
    channel_order_id: str = ""


@dataclass(frozen=True, slots=True)
class ChannelRefundResult:
    state: ChannelRefundState
    refund_id: str = ""
    at_ms: int = 0
    detail: str = ""


@dataclass(frozen=True, slots=True)
class ChannelRevocation:
    transaction_id: str
    revoked: bool
    observed_ms: int = 0
    out_trade_no: str = ""
    out_refund_no: str = ""
    detail: str = ""
    payment: ChannelPayment | None = None


@dataclass(frozen=True, slots=True)
class ChannelReceipt:
    transaction_id: str = ""
    purchase_token: str = ""
    product_id: str = ""


@dataclass(frozen=True, slots=True)
class ChannelNotification:
    payment: ChannelPayment | None = None
    revocations: tuple[ChannelRevocation, ...] = ()
    receipts: tuple[ChannelReceipt, ...] = ()


# 领域配置与命令/结果对象
@dataclass(frozen=True, slots=True)
class ProductMapping:
    sku_id: int
    title: str = ""
    by_channel: tuple[tuple[str, str], ...] = ()

    def product_for(self, channel_name: str) -> str:
        for ch_name, prod_id in self.by_channel:
            if ch_name == channel_name:
                return prod_id
        return ""


@dataclass(frozen=True, slots=True)
class TestPolicy:
    accept: bool = False
    players: frozenset[int] = frozenset()


class RealNames(Protocol):
    """实名查询接口（合规闸为 realname 时接入）。"""

    async def real_name_of(self, player_id: int) -> tuple[str, bool]:
        """返回 (pi, found)。found=False 表示目录中无该角色，pi='' 表示未实名。"""
        ...


@dataclass(frozen=True, slots=True)
class PaymentOptions:
    """时间窗、批量与策略参数（缺省值与 Go internal/core/service.go 一致）。"""

    min_pay_window_ms: int = 60_000
    receipt_window_ms: int = 72 * 3600 * 1000
    first_check_delay_ms: int = 60_000
    check_interval_ms: int = 120_000
    close_grace_ms: int = 120_000
    give_up_after_ms: int = 24 * 3600 * 1000
    active_query_interval_ms: int = 3_000
    request_call_timeout_sec: float = 2.0
    background_call_timeout_sec: float = 8.0
    lease_ms: int = 30_000
    batch: int = 50
    interval_sec: float = 2.0
    refund_poll_interval_sec: float = 10.0
    history_interval_sec: float = 600.0
    history_lookback_ms: int = 72 * 3600 * 1000
    mall_backoff_max_ms: int = 10 * 60 * 1000
    refund_backoff_max_ms: int = 30 * 60 * 1000
    refund_reason: str = "游戏订单退款"
    products: tuple[ProductMapping, ...] = ()
    test_policies: tuple[tuple[str, TestPolicy], ...] = ()
    real_names: RealNames | None = None


@dataclass(frozen=True, slots=True)
class CreateIntentCommand:
    player_id: int
    order_id: str
    channel: int
    platform: int
    login_code: str = ""
    device_os: str = ""


@dataclass(frozen=True, slots=True)
class CreateIntentResult:
    intent: PaymentIntentRecord | None = None
    params: ClientParams = field(default_factory=ClientParams)


@dataclass(frozen=True, slots=True)
class SubmitPurchaseCommand:
    player_id: int
    channel: int
    order_id: str = ""
    apple_transaction_id: str = ""
    google_purchase_token: str = ""
    google_product_id: str = ""


@dataclass(frozen=True, slots=True)
class SubmitPurchaseResult:
    intent: PaymentIntentRecord | None = None
    finish_transaction: bool = False
