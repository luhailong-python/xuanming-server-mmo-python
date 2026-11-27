"""trade 领域模型、常量表、纯校验函数与协议投影（与 Go trade/internal/{constants,logic,commerce} 对齐）。"""

from __future__ import annotations

import hashlib
import math
import unicodedata
from dataclasses import dataclass, field
from typing import Protocol as TypingProtocol

from google.protobuf.message import Message

from ..protocol import Protocol
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_TRADE_ASSET_NOT_TRADABLE,
    TradeRejection,
)

# 节点、库名与调用方常量
DATABASE_NAME = "mmorpg_trade"
TRADE_NODE_TYPE = 12  # common.base.eNodeType.TradeNodeService
ASSET_OP_CALLER = "trade"
ROLE_FENCE_CALLER = "trade"
ACCOUNT_DIRECTORY_CALLER = "trade"
MANUAL_RESOLUTION_CALLER = "ops-commerce"

# 内容安全场景与判定枚举（与 proto/content_guard/content_guard.proto 对齐）
TEXT_SCENE_TRADE_TITLE = 9
TEXT_VERDICT_PASS = 1
TEXT_VERDICT_MASKED = 2
TEXT_VERDICT_REJECT = 3

# 官方商城交付与付款撤销常量（与 Go official_reversal.go 对齐）
STAGE_OFFICIAL_DELIVER = "official_deliver"
STAGE_OFFICIAL_REVERSED = "official_reversed"
OFFICIAL_CREDIT_ROLE = "official_credit"
OFFICIAL_REVERSE_PREFIX = "official_reverse_"
OFFICIAL_REGRANT_PREFIX = "official_regrant_"

# 人民币寄售限值（与 Go rmb.go / rmb_mall.go 对齐）
MAX_RMB_PRICE_FEN = 9_000_000_000_000_000
MAX_RMB_TITLE_UTF16 = 128

# 资产通道流与交易类型（与 proto/common/asset/asset_op.proto & transaction_log.proto 对齐）
ASSET_STREAM_TRADE_DEBIT = 3
ASSET_STREAM_TRADE_CREDIT = 4
TX_AUCTION_SELL = 3
TX_AUCTION_BUY = 4
TX_AUCTION_RETURN = 30
TX_PAYMENT_REVERSAL = 31

# 货币枚举（与 proto/common/base/currency_type.proto 对齐）
CURRENCY_GOLD = 0
CURRENCY_DIAMOND = 1
CURRENCY_BIND_DIAMOND = 2
CURRENCY_BIND_GOLD = 3

# P1 商品存储状态（ListingStatus）
LISTING_STATUS_UNSPECIFIED = 0
LISTING_STATUS_ESCROWING = 1
LISTING_STATUS_LISTED = 2
LISTING_STATUS_LOCKED = 3
LISTING_STATUS_SOLD = 4
LISTING_STATUS_RETURNING = 5
LISTING_STATUS_RETURNED = 6
LISTING_STATUS_ESCROW_REJECTED = 7

# P1 展示阶段（ListingPhase）
LISTING_PHASE_UNSPECIFIED = 0
LISTING_PHASE_PUBLIC_NOTICE = 1
LISTING_PHASE_ON_SALE = 2
LISTING_PHASE_LOCKED = 3
LISTING_PHASE_ENDED = 4

# P1 页签、分区、排序与市场范围
LISTING_TAB_PUBLIC_NOTICE = 1
LISTING_TAB_ON_SALE = 2
LISTING_SECTION_CONSIGNMENT = 1
LISTING_SECTION_AUCTION = 2
LISTING_SORT_DEFAULT = 0
LISTING_SORT_PRICE_ASC = 1
LISTING_SORT_PRICE_DESC = 2
LISTING_SORT_LEVEL_DESC = 3
LISTING_SORT_REMAINING_ASC = 4
MARKET_SCOPE_UNSPECIFIED = 0
MARKET_SCOPE_ZONE = 1
MARKET_SCOPE_GLOBAL = 2

# 合同类型（CommerceOrderKind + BidKind）
COMMERCE_KIND_OFFICIAL = 1
COMMERCE_KIND_PLAYER_TRADE = 2
COMMERCE_KIND_CONSIGNMENT = 3
COMMERCE_KIND_MARKET_MATCH = 4
COMMERCE_KIND_AUCTION_LOT = 5
COMMERCE_KIND_RMB_CONSIGNMENT = 6
COMMERCE_KIND_SKILL_BOOK_WANTED = 7
COMMERCE_KIND_BID = 100

# 悬赏求书求购单常量（kind 7，与 Go commerce/wanted_skill_book.go 及 C++ equip_craft_quality_rules.h 对齐）
WANTED_REWARD_MIN_GOLD = 100
WANTED_REWARD_MAX_GOLD = 50_000
MAX_ACTIVE_WANTED_ORDERS = 3
WANTED_TAX_BPS = 500
SEALED_SKILL_BOOK_CONFIG_ID = 3414
MAX_WANTED_FULFILL_ATTEMPTS = 32
STAGE_WANTED_ESCROW = "wanted_escrow"
STAGE_WANTED_OPEN = "wanted_open"
STAGE_WANTED_FILL = "wanted_fill"
WANTED_FULFILL_SCOPE = "wanted_fulfill"
WANTED_DEBIT_PREFIX = "seller_debit_"
WANTED_RETURN_PREFIX = "seller_return_"

# WantedSkillBookOrderPhase 枚举值（与 proto/trade/commerce.proto 对齐）
WANTED_PHASE_UNSPECIFIED = 0
WANTED_PHASE_ESCROWING = 1
WANTED_PHASE_OPEN = 2
WANTED_PHASE_FULFILLING = 3
WANTED_PHASE_DELIVERING = 4
WANTED_PHASE_SETTLED = 5
WANTED_PHASE_CANCELLING = 6
WANTED_PHASE_CANCELLED = 7
WANTED_PHASE_MANUAL = 8

# 合同状态（CommerceOrderState）
COMMERCE_WAITING = 1
COMMERCE_AWAITING_PAYMENT = 2
COMMERCE_ESCROWING = 3
COMMERCE_DELIVERING = 4
COMMERCE_DELIVERED = 5
COMMERCE_CANCEL_PENDING = 6
COMMERCE_CANCELLED = 7
COMMERCE_REFUND_PENDING = 8
COMMERCE_REFUNDED = 9
COMMERCE_MANUAL = 10
COMMERCE_SETTLED = 11
COMMERCE_REVERSING = 12
COMMERCE_REVERSED = 13

DUE_STATUSES = (
    COMMERCE_WAITING,
    COMMERCE_ESCROWING,
    COMMERCE_DELIVERING,
    COMMERCE_CANCEL_PENDING,
    COMMERCE_REFUND_PENDING,
)

# 资产待办行类型、外键与状态（TradeAssetOp*）
OP_KIND_ESCROW_DEBIT = 1
OP_KIND_DELIVER_CREDIT = 2
OP_KIND_RETURN_CREDIT = 3

OP_REF_KIND_LISTING = 1
OP_REF_KIND_ORDER = 2

OP_STATUS_UNSPECIFIED = 0
OP_STATUS_PENDING = 1
OP_STATUS_APPLIED = 2
OP_STATUS_REJECTED = 3
OP_STATUS_ABORTED = 4
OP_STATUS_APPLIED_PARTIAL = 5
OP_STATUS_ABANDONED = 6

OP_TERMINAL_STATUSES = frozenset((
    OP_STATUS_APPLIED,
    OP_STATUS_REJECTED,
    OP_STATUS_ABORTED,
    OP_STATUS_APPLIED_PARTIAL,
    OP_STATUS_ABANDONED,
))
OP_FINALIZABLE_STATUSES = frozenset((
    OP_STATUS_APPLIED,
    OP_STATUS_REJECTED,
    OP_STATUS_ABORTED,
    OP_STATUS_APPLIED_PARTIAL,
))
OP_ROLE_CLEAR_STATUSES = frozenset((
    OP_STATUS_APPLIED,
    OP_STATUS_REJECTED,
    OP_STATUS_ABORTED,
    OP_STATUS_ABANDONED,
))

# 资产通道背压与调度常量（与 Go assetop.DefaultLimits 对齐）
ASSET_OP_MAX_PENDING = 16
ASSET_OP_MAX_SPAN = 1024
FRESH_ATTEMPT_LIMIT = 3
MAX_NEW_LEGS_PER_STEP = 8

# 角色护栏阻塞枚举（低基数）
ROLE_BLOCKER_COMMERCE = "trade_commerce"
ROLE_BLOCKER_ASSET_OP = "trade_asset_op"
ROLE_BLOCKER_TOO_MANY_DEALS = "too_many_contracts"
MAX_ROLE_PENDING_COMMERCE = 256

# 人工处置常量
MANUAL_LIST_PAGE_MAX = 500
MANUAL_OPERATOR_MAX_BYTES = 64
MANUAL_REASON_MAX_BYTES = 191
QUARANTINE_DETAIL_MAX_BYTES = 256

# 输入限值（与 Go constants.go 对齐）
MAX_SEARCH_RUNES = 64
MAX_TITLE_RUNES = 64
MAX_SUMMARY_RUNES = 128
MAX_DESCRIPTION_RUNES = 512
MAX_ICON_KEY_LEN = 64
MAX_LEVEL = 1000
MAX_PRICE_FEN = 10_000_000_000
MAX_NOTICE_DURATION_MS = 30 * 24 * 3600 * 1000
MAX_SALE_DURATION_MS = 90 * 24 * 3600 * 1000
INT64_MAX = (1 << 63) - 1
UINT32_MAX = (1 << 32) - 1
UINT64_MAX = (1 << 64) - 1

# 类目子类上限映射（与 Go constants.MaxSubcategory 逐项对齐）
MAX_SUBCATEGORIES: dict[int, int] = {
    1: 4,  # CHARACTER: 破军 玄霄 逐风 丹心
    2: 6,  # PET: 普通 灵兽 变异 神兽 元灵 其他
    3: 5,  # WEAPON: 枪 爪 剑 扇 锤
    4: 5,  # ARMOR: 男帽 女帽 男衣 女衣 鞋子
    5: 0,  # SET
    6: 0,  # TREASURE
    7: 0,  # JEWELRY
    8: 2,  # SUMMONING_ORDER: 神兽召唤令 元灵召唤令
    9: 0,  # CURRENCY
}


def is_bound_currency(currency_type: int) -> bool:
    """判定币种是否属于禁止玩家间交易/寄售的绑定货币（绑定灵石 2 / 绑定银两 3）。"""
    return currency_type in (CURRENCY_BIND_DIAMOND, CURRENCY_BIND_GOLD)


def is_tradable_currency(currency_type: int) -> bool:
    """交易与人民币寄售允许的币种：仅银两(0)与灵石(1)，绑定灵石(2)与绑定银两(3)禁售。"""
    return currency_type in (CURRENCY_GOLD, CURRENCY_DIAMOND)


def is_known_currency(currency_type: int) -> bool:
    """协议 CurrencyType 完整取值表校验（银两/灵石/绑定灵石/绑定银两）。"""
    return currency_type in (CURRENCY_GOLD, CURRENCY_DIAMOND, CURRENCY_BIND_DIAMOND, CURRENCY_BIND_GOLD)


def valid_account(account: str) -> bool:
    """校验账号字符串非空、合法 UTF-8、无控制字符且不超过 191 字节。"""
    if not account:
        return False
    encoded = account.encode("utf-8")
    if len(encoded) > 191:
        return False
    return not any(unicodedata.category(ch).startswith("C") for ch in account)


@dataclass(frozen=True, slots=True)
class MarketLimits:
    """P1 市场范围、分页与收藏上限配置。"""

    scope: str = "global"
    default_page_size: int = 20
    max_page_size: int = 20
    max_page: int = 100
    max_favorites_per_player: int = 100

    @property
    def scope_enum(self) -> int:
        if self.scope == "global":
            return MARKET_SCOPE_GLOBAL
        if self.scope == "zone":
            return MARKET_SCOPE_ZONE
        return MARKET_SCOPE_UNSPECIFIED

    def validate(self) -> None:
        if self.scope not in ("zone", "global"):
            raise ValueError(f"trade market.scope must be 'zone' or 'global', got {self.scope!r}")
        for name, val in (
            ("default_page_size", self.default_page_size),
            ("max_page_size", self.max_page_size),
            ("max_page", self.max_page),
            ("max_favorites_per_player", self.max_favorites_per_player),
        ):
            if val <= 0:
                raise ValueError(f"trade market.{name} must be > 0, got {val}")
        if self.default_page_size > self.max_page_size:
            raise ValueError("trade market.default_page_size cannot exceed max_page_size")


@dataclass(frozen=True, slots=True)
class TradePolicy:
    """交易写面资产白名单策略（与 Go commerce.Policy 对齐）。"""

    items: tuple[int, ...] = ()
    item_titles: dict[int, str] = field(default_factory=dict)
    pets: tuple[int, ...] = ()
    currencies: tuple[int, ...] = (CURRENCY_GOLD,)
    allow_instances: bool = False

    def validate_selection(self, sel: Message | None) -> None:
        """校验客户端提交的 TradeAssetSelection。"""
        if sel is None:
            raise TradeRejection(ERR_INVALID_PARAMETER, "missing asset selection")
        items = dict(sel.items)
        uuids = list(sel.item_uuids)
        pet_id = int(sel.pet_id)
        cur_type = int(sel.currency_type)
        cur_amount = int(sel.currency_amount)
        if len(items) > 16 or len(uuids) > 8 or (len(items) + len(uuids) == 0 and pet_id == 0 and cur_amount == 0):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid asset selection bounds")
        if (cur_amount == 0 and cur_type != 0) or cur_amount > INT64_MAX:
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid currency selection")
        if cur_amount > 0 and (is_bound_currency(cur_type) or cur_type not in self.currencies):
            raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "currency not tradable")
        total = 0
        for cfg_id, count in items.items():
            if cfg_id == 0 or count == 0 or count > 4096 or cfg_id not in self.items:
                raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "item not tradable")
            total += int(count)
            if total > 4096:
                raise TradeRejection(ERR_INVALID_PARAMETER, "too many stackable items")
        seen: set[int] = set()
        for uid in uuids:
            if uid == 0 or uid in seen:
                raise TradeRejection(ERR_INVALID_PARAMETER, "duplicate or zero item_uuid")
            seen.add(uid)
        if (uuids or pet_id != 0) and not self.allow_instances:
            raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "instances not tradable")

    def allow_snapshot(self, snapshot: Message | None) -> bool:
        """核对 Scene 扣出快照中的实例与宠物配置号是否在白名单内。"""
        if snapshot is None:
            return True
        for item in snapshot.items:
            if int(item.config_id) not in self.items:
                return False
        if snapshot.HasField("pet") and int(snapshot.pet.pet_id) != 0:
            return int(snapshot.pet.pet_table_id) in self.pets
        return True

    def valid_price(self, price: int, currency_type: int) -> bool:
        return 0 < price <= INT64_MAX and not is_bound_currency(currency_type) and currency_type in self.currencies


class MergedZones:
    """合服映射不可变快照：支持沿链解析最终区与反查合入某最终区的全部原始区。"""

    def __init__(self, merged_into: dict[int, int] | None = None) -> None:
        raw = {int(k): int(v) for k, v in (merged_into or {}).items() if int(k) != 0 and int(v) != 0}
        self._final: dict[int, int] = {}
        self._members: dict[int, tuple[int, ...]] = {}
        for src in raw:
            self._final[src] = self._resolve_chain(raw, src)
        grouped: dict[int, list[int]] = {}
        for src, dst in self._final.items():
            grouped.setdefault(dst, [dst])
            if src not in grouped[dst]:
                grouped[dst].append(src)
        for dst, members in grouped.items():
            self._members[dst] = tuple(sorted(set(members)))

    @staticmethod
    def _resolve_chain(raw: dict[int, int], zone: int) -> int:
        seen: set[int] = set()
        cur = zone
        while cur in raw and cur not in seen:
            seen.add(cur)
            cur = raw[cur]
        return cur

    def final(self, zone: int) -> int:
        if zone == 0:
            return 0
        return self._final.get(zone, zone)

    def members(self, zone: int) -> tuple[int, ...]:
        if zone == 0:
            return ()
        dst = self.final(zone)
        return self._members.get(dst, (dst,))


@dataclass(frozen=True, slots=True)
class RoleOriginal:
    """Gate Ed25519 验签后的在线角色原身份。"""

    player_id: int
    account: str
    owner_revision: int


@dataclass(frozen=True, slots=True)
class RoleFenceRow:
    """本库 role_fence 副本行（与 Go shared/rolefence 对齐）。"""

    player_id: int
    account: str
    owner_revision: int
    freeze_token: int
    freeze_final: bool
    version: int
    home_zone: int
    updated_ms: int = 0


@dataclass(frozen=True, slots=True)
class RoleFenceExpect:
    """role_fence 锁校验期望。"""

    player_id: int
    account: str = ""
    owner_revision: int = 0
    allow_token: int = 0
    reject: Exception | None = None


@dataclass(frozen=True, slots=True)
class ListingQuery:
    """P1 浏览列表查询条件（经纯校验后传给存储层）。"""

    tab: int
    category: int
    subcategory: int
    sort: int
    now_ms: int
    market_zones: tuple[int, ...] = ()
    title_like_pattern: str = ""
    search_listing_id: int = 0
    favorites_of: int = 0


@dataclass(frozen=True, slots=True)
class CommerceCursor:
    """合同翻页游标（created_ms:commerce_id）。"""

    created_ms: int = 0
    commerce_id: int = 0

    @classmethod
    def parse(cls, raw: str) -> "CommerceCursor":
        if ":" not in raw:
            return cls()
        left, right = raw.split(":", 1)
        if not left.isdigit() or not right.isdigit():
            return cls()
        return cls(created_ms=int(left), commerce_id=int(right))

    def encode(self) -> str:
        return f"{self.created_ms}:{self.commerce_id}"


@dataclass(frozen=True, slots=True)
class PendingAssetOp:
    """资产重投循环领取到的单条待办指令（与 Go assetop.Op 对齐）。"""

    op_id: int
    player_id: int
    stream: int
    stream_epoch: int
    seq: int
    correlation_id: int
    tx_type: int
    bundle: Message
    attempts: int
    deadline_ms: int
    lease_token: int
    last_reason: int
    created_ms: int
    last_send_ms: int


@dataclass(frozen=True, slots=True)
class LegOutcome:
    """合同单条腿对应的 trade_asset_op 结局。"""

    status: int
    snapshot: bytes = b""
    resolved_by: str = ""
    final_ms: int = 0

    @property
    def is_pending(self) -> bool:
        return self.status == OP_STATUS_PENDING

    @property
    def is_final(self) -> bool:
        return self.status in OP_TERMINAL_STATUSES

    @property
    def source(self) -> str:
        if self.status == OP_STATUS_ABANDONED:
            return "abandoned"
        if self.resolved_by:
            return "manual"
        return "scene"


# -- P1 聚宝斋纯函数（与 Go logic/phase.go 完全一致） -------------------------------------------------

def derive_phase(rec: Message, now_ms: int) -> int:
    status = int(rec.status)
    if status == LISTING_STATUS_LISTED:
        if now_ms < int(rec.notice_end_ms):
            return LISTING_PHASE_PUBLIC_NOTICE
        if now_ms < int(rec.sale_end_ms):
            return LISTING_PHASE_ON_SALE
        return LISTING_PHASE_ENDED
    if status == LISTING_STATUS_LOCKED:
        return LISTING_PHASE_LOCKED
    return LISTING_PHASE_ENDED


def visible_to_buyer(
    rec: Message,
    now_ms: int,
    scope: int,
    caller_home_zone: int,
    zones: MergedZones,
) -> bool:
    if int(rec.status) not in (LISTING_STATUS_LISTED, LISTING_STATUS_LOCKED):
        return False
    if now_ms >= int(rec.sale_end_ms):
        return False
    if scope == MARKET_SCOPE_GLOBAL:
        return True
    if scope == MARKET_SCOPE_ZONE:
        return caller_home_zone != 0 and zones.final(int(rec.market_zone)) == zones.final(caller_home_zone)
    return False


def clamp_page_size(requested: int, default_size: int, max_size: int) -> int:
    size = default_size if requested == 0 else requested
    if size > max_size:
        size = max_size
    return max(size, 1)


def page_window(total: int, page: int, page_size: int, max_page: int) -> tuple[int, int, int]:
    page_size = max(page_size, 1)
    count = max(1, (total + page_size - 1) // page_size)
    count = min(count, UINT32_MAX)
    clamped = min(max(page, 1), count)
    if max_page > 0:
        clamped = min(clamped, max_page)
    offset = (clamped - 1) * page_size
    return clamped, count, offset


def escape_like(text: str) -> str:
    """以 '!' 作为 LIKE 转义符（单遍替换，避免重复转义）。"""
    return text.replace("!", "!!").replace("%", "!%").replace("_", "!_")


def valid_text(text: str, max_runes: int) -> bool:
    if len(text) > max_runes:
        return False
    return not any(unicodedata.category(ch).startswith("C") for ch in text)


def valid_icon_key(key: str) -> bool:
    if len(key.encode("utf-8")) > MAX_ICON_KEY_LEN:
        return False
    return all(("a" <= ch <= "z") or ("0" <= ch <= "9") or ch == "_" for ch in key)


def normalize_search(raw: str) -> tuple[str, bool]:
    trimmed = raw.strip()
    if not valid_text(trimmed, MAX_SEARCH_RUNES):
        return "", False
    return trimmed, True


def valid_category(category: int, subcategory: int) -> bool:
    limit = MAX_SUBCATEGORIES.get(category)
    return limit is not None and subcategory <= limit


def valid_tab(tab: int) -> bool:
    return tab in (LISTING_TAB_PUBLIC_NOTICE, LISTING_TAB_ON_SALE)


def valid_section(section: int) -> bool:
    return section in (LISTING_SECTION_CONSIGNMENT, LISTING_SECTION_AUCTION)


def valid_sort(sort: int) -> bool:
    return sort in (
        LISTING_SORT_DEFAULT,
        LISTING_SORT_PRICE_ASC,
        LISTING_SORT_PRICE_DESC,
        LISTING_SORT_LEVEL_DESC,
        LISTING_SORT_REMAINING_ASC,
    )


def valid_seed_request(req: Message) -> bool:
    if int(req.seller_player_id) == 0:
        return False
    if not valid_category(int(req.category), int(req.subcategory)):
        return False
    if not req.title.strip() or not valid_text(req.title, MAX_TITLE_RUNES):
        return False
    if not valid_text(req.summary, MAX_SUMMARY_RUNES):
        return False
    if not valid_text(req.description, MAX_DESCRIPTION_RUNES):
        return False
    if not valid_icon_key(req.icon_key):
        return False
    if int(req.level) > MAX_LEVEL:
        return False
    price = int(req.price_fen)
    if price == 0 or price > MAX_PRICE_FEN:
        return False
    sale_dur = int(req.sale_duration_ms)
    if sale_dur == 0 or sale_dur > MAX_SALE_DURATION_MS:
        return False
    if int(req.notice_duration_ms) > MAX_NOTICE_DURATION_MS:
        return False
    return True


def to_listing_summary(
    protocol: Protocol,
    rec: Message,
    now_ms: int,
    caller: int,
    is_favorite: bool,
    zones: MergedZones,
) -> Message:
    return protocol.message(
        "trade.ListingSummary",
        listing_id=int(rec.listing_id),
        category=int(rec.category),
        subcategory=int(rec.subcategory),
        title=str(rec.title),
        level=int(rec.level),
        price_fen=int(rec.price_fen),
        phase=derive_phase(rec, now_ms),
        notice_end_ms=int(rec.notice_end_ms),
        sale_end_ms=int(rec.sale_end_ms),
        market_zone=zones.final(int(rec.market_zone)),
        is_favorite=bool(is_favorite),
        is_mine=(int(rec.seller_player_id) == caller),
        summary=str(rec.summary),
        icon_key=str(rec.icon_key),
    )


# -- 角色护栏纯校验函数（与 Go shared/rolefence 一致） ------------------------------------------------

def valid_role_fence_state(state: Message | None) -> bool:
    if state is None:
        return False
    if int(state.player_id) == 0 or int(state.version) == 0:
        return False
    if state.account == "":
        return int(state.owner_revision) == 0 and int(state.freeze_token) == 0 and not state.freeze_final
    if not valid_account(state.account) or int(state.owner_revision) == 0:
        return False
    if state.freeze_final and int(state.freeze_token) == 0:
        return False
    return True


def check_role_fence(
    fence: RoleFenceRow | None,
    expect: RoleFenceExpect,
    *,
    check_freeze: bool = True,
) -> str:
    """判定单名角色的护栏副本：返回 'ok' / 'stale' / 'reject'。"""
    if fence is None:
        return "stale"
    if fence.account == "":
        return "reject"
    if expect.owner_revision:
        if fence.owner_revision < expect.owner_revision:
            return "stale"
        if fence.owner_revision > expect.owner_revision or (expect.account and fence.account != expect.account):
            return "reject"
    elif expect.account and fence.account != expect.account:
        return "reject"
    if check_freeze and fence.freeze_token != 0 and fence.freeze_token != expect.allow_token:
        return "stale"
    return "ok"


# -- Commerce 纯函数（与 Go commerce/*.go 一致） ------------------------------------------------------

def on_book(record: Message, workflow: Message | None) -> bool:
    """材料盘口投影：kind=4 且 side!=0 且 workflow.stage=='market_open'。"""
    if workflow is None:
        return False
    return (
        int(record.kind) == COMMERCE_KIND_MARKET_MATCH
        and int(record.side) != 0
        and str(workflow.stage) == "market_open"
    )


def build_asset_bundle(protocol: Protocol, sel: Message | None) -> Message:
    bundle = protocol.message("AssetBundle")
    if sel is None:
        return bundle
    bundle.item_uuids.extend(int(uid) for uid in sel.item_uuids)
    bundle.pet_id = int(sel.pet_id)
    if int(sel.currency_amount) > 0:
        cur = bundle.currencies.add()
        cur.currency_type = int(sel.currency_type)
        cur.amount = int(sel.currency_amount)
    for cfg_id in sorted(int(k) for k in sel.items):
        grant = bundle.items.add()
        grant.config_id = cfg_id
        grant.count = int(sel.items[cfg_id])
    return bundle


def transfer_bundle(protocol: Protocol, bundle: Message | None, snapshot: Message | None) -> Message:
    """将扣出 bundle + scene 托管快照转换为入账 bundle（绝不凭 config_id 重铸实例）。"""
    if bundle is None or len(bundle.item_instances) > 0 or bundle.HasField("pet_instance"):
        raise ValueError("escrow debit bundle contains credit-only instance fields")
    out = protocol.message("AssetBundle")
    out.CopyFrom(bundle)
    has_uuids = len(bundle.item_uuids) > 0
    has_pet = int(bundle.pet_id) > 0
    if (has_uuids or has_pet) and snapshot is None:
        raise ValueError("missing escrow snapshot for instance or pet transfer")
    if snapshot is not None:
        for inst in snapshot.items:
            out.item_instances.add().CopyFrom(inst)
        if snapshot.HasField("pet") and int(snapshot.pet.pet_id) != 0:
            out.pet_instance.CopyFrom(snapshot.pet)
    if len(out.item_uuids) != len(out.item_instances) or (int(out.pet_id) != 0) != out.HasField("pet_instance"):
        raise ValueError("escrow snapshot does not match contract bundle")
    wanted: set[int] = set()
    for uid in bundle.item_uuids:
        uid_int = int(uid)
        if uid_int == 0 or uid_int in wanted:
            raise ValueError("corrupt item_uuids in escrow bundle")
        wanted.add(uid_int)
    for inst in out.item_instances:
        uid_int = int(inst.item_uuid)
        if uid_int not in wanted:
            raise ValueError("escrow item instance identity mismatch")
        wanted.remove(uid_int)
    if wanted or (out.HasField("pet_instance") and int(out.pet_instance.pet_id) != int(bundle.pet_id)):
        raise ValueError("escrow instance identity mismatch")
    del out.item_uuids[:]
    out.pet_id = 0
    return out


def empty_bundle(bundle: Message | None) -> bool:
    if bundle is None:
        return True
    return (
        len(bundle.currencies) == 0
        and len(bundle.items) == 0
        and len(bundle.item_instances) == 0
        and len(bundle.item_uuids) == 0
        and not bundle.HasField("pet_instance")
        and int(bundle.pet_id) == 0
    )


def is_official_reverse_leg(role: str) -> bool:
    base = role.split("#", 1)[0]
    return base.startswith("official_reverse_")


def is_return_leg(role: str) -> bool:
    base = role.split("#", 1)[0]
    if base in ("seller_return", "buyer_return", "bid_return", "buyer_surplus"):
        return True
    return base.endswith("/refund") or base.startswith(WANTED_RETURN_PREFIX)


def leg_tx_type(leg: Message) -> int:
    """计算一条合同腿进入资产通道时的 TransactionType（与 Go commerce/leg_tx_type.go 一致）。"""
    stream = int(leg.stream)
    role = str(leg.role)
    if stream == ASSET_STREAM_TRADE_DEBIT and is_official_reverse_leg(role):
        return TX_PAYMENT_REVERSAL
    if stream == ASSET_STREAM_TRADE_DEBIT:
        return TX_AUCTION_SELL
    escrow_owner = int(leg.escrow_owner_id)
    if is_return_leg(role) and (escrow_owner == 0 or escrow_owner == int(leg.player_id)):
        return TX_AUCTION_RETURN
    return TX_AUCTION_BUY


def request_key(scope: str, player_id: int, client_request_id: str) -> str:
    return hashlib.sha256(f"{scope}:{player_id}:{client_request_id}".encode("utf-8")).hexdigest()


def _append_uvarint(value: int) -> bytes:
    out = bytearray()
    v = int(value)
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)
    return bytes(out)


def request_digest(protocol: Protocol, msg: Message, original: RoleOriginal | None = None) -> bytes:
    """与 Go commerce.digest 完全一致的请求摘要（绑定原身份以防止角色过户后重放）。"""
    raw = msg.SerializeToString(deterministic=True)
    if original is not None:
        ident = protocol.message(
            "trade.TradeRoleIdentity",
            account=original.account,
            owner_revision=original.owner_revision,
        ).SerializeToString(deterministic=True)
        raw = b"trade-request-v2\n" + _append_uvarint(len(raw)) + raw + ident
    return hashlib.sha256(raw).digest()


def truncate_utf8(text: str, max_bytes: int) -> str:
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text
    return raw[:max_bytes].decode("utf-8", errors="ignore")


def valid_manual_environment(target: str) -> bool:
    if not 1 <= len(target) <= 64:
        return False
    return all(
        ("A" <= ch <= "Z") or ("a" <= ch <= "z") or ("0" <= ch <= "9") or ch in (".", "_", "-")
        for ch in target
    )


def clamp_uint32(value: int) -> int:
    return min(max(int(value), 0), UINT32_MAX)


def market_remainder(protocol: Protocol, rec: Message) -> Message:
    """计算市场单尚未成交的托管剩余量（与 Go commerce.marketRemainder 一致）。"""
    filled = int(rec.filled_quantity)
    qty = int(rec.quantity)
    if filled > qty:
        raise ValueError(f"market order filled_quantity {filled} exceeds quantity {qty}")
    remaining = qty - filled
    out = protocol.message("AssetBundle")
    if remaining == 0:
        return out
    if int(rec.side) == 1:
        unit_price = int(rec.unit_price)
        if unit_price > INT64_MAX // remaining:
            raise ValueError("market remainder currency overflow")
        cur = out.currencies.add()
        cur.currency_type = int(rec.currency_type)
        cur.amount = remaining * unit_price
    else:
        grant = out.items.add()
        grant.config_id = int(rec.item_config_id)
        grant.count = remaining
    return out


# -- 悬赏求书求购单纯计算、配表目录与游标（与 Go commerce/wanted_skill_book.go 对齐） -------------------

def wanted_tax_gold(reward_gold: int) -> int:
    """计算悬赏求书集市税：悬赏额 × 5% 向上取整（销毁，不发给任何人）。"""
    reward = int(reward_gold)
    if reward <= 0:
        return 0
    return (reward * WANTED_TAX_BPS + 9999) // 10000


def wanted_seller_net_gold(reward_gold: int) -> int:
    """计算揭榜人税后实得流通银两。"""
    reward = int(reward_gold)
    tax = wanted_tax_gold(reward)
    if reward <= tax:
        return 0
    return reward - tax


def wanted_phase(rec: Message, w: Message) -> int:
    """根据合同状态与工作流阶段推导客户端展示用的 WantedSkillBookOrderPhase。"""
    status = int(rec.status)
    if status == COMMERCE_MANUAL:
        return WANTED_PHASE_MANUAL
    if status == COMMERCE_SETTLED:
        return WANTED_PHASE_SETTLED
    if status == COMMERCE_CANCELLED:
        return WANTED_PHASE_CANCELLED
    if status == COMMERCE_CANCEL_PENDING:
        return WANTED_PHASE_CANCELLING
    stage = str(w.stage)
    if stage == STAGE_WANTED_ESCROW:
        return WANTED_PHASE_ESCROWING
    if stage == STAGE_WANTED_OPEN:
        return WANTED_PHASE_OPEN
    if stage == STAGE_WANTED_FILL:
        return WANTED_PHASE_FULFILLING
    if stage == "deliver":
        return WANTED_PHASE_DELIVERING
    if stage == "return":
        return WANTED_PHASE_CANCELLING
    return WANTED_PHASE_UNSPECIFIED


@dataclass(frozen=True, slots=True)
class WantedSkillInfo:
    """技能在配表中的展示信息（卷宗号、技能名、卷宗名）。"""

    scroll_id: int = 0
    skill_name: str = ""
    scroll_name: str = ""


class WantedSkillCatalog(TypingProtocol):
    """查询某门技能是否允许发榜求购及其配表展示信息。"""

    def wanted_skill(self, skill_table_id: int) -> tuple[WantedSkillInfo, bool]: ...


class TableWantedSkillCatalog:
    """从配置表 ``Skill`` 与 ``SkillScrollCraft`` 现查可悬赏求购的常规永久技能。"""

    def __init__(self, tables=None) -> None:
        if tables is None:
            from ..data import ConfigTables

            tables = ConfigTables()
        self._skills = tables["Skill"]
        self._scrolls = tables["SkillScrollCraft"]

    def wanted_skill(self, skill_table_id: int) -> tuple[WantedSkillInfo, bool]:
        skill = self._skills.get(int(skill_table_id))
        if skill is None:
            return WantedSkillInfo(), False
        scroll_id = int(getattr(skill, "scroll_id", 0))
        skill_name = str(getattr(skill, "name", "") or "")
        scroll = self._scrolls.get(scroll_id)
        if scroll is None:
            return WantedSkillInfo(scroll_id=scroll_id, skill_name=skill_name), False
        scroll_name = str(getattr(scroll, "name", "") or "")
        info = WantedSkillInfo(scroll_id=scroll_id, skill_name=skill_name, scroll_name=scroll_name)
        eligible = (
            int(getattr(skill, "season_id", 0)) == 0
            and int(getattr(skill, "battle_model", 0)) == 1
            and int(getattr(skill, "rarity_tier", 0)) == 0
            and int(getattr(scroll, "enabled", 0)) != 0
            and int(getattr(scroll, "rarity_tier", 0)) == 0
        )
        return info, eligible


@dataclass(frozen=True, slots=True)
class WantedBoardCursor:
    """公开悬赏榜按 (reward_gold DESC, commerce_id DESC) 翻页的不透明游标。"""

    reward_gold: int
    commerce_id: int

    def format(self) -> str:
        return f"{int(self.reward_gold)}:{int(self.commerce_id)}"

    @classmethod
    def parse(cls, raw: str) -> "WantedBoardCursor | None":
        if ":" not in raw:
            return None
        left, right = raw.split(":", 1)
        if not left.isdigit() or not right.isdigit():
            return None
        return cls(reward_gold=int(left), commerce_id=int(right))

