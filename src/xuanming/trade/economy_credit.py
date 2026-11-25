"""聚宝斋卖家店铺信用评分、近 7 日中位成交参考价（p50）与当面交易防骗护栏。

严格对齐 Go 侧 ``trade/internal/commerce/{seller_credit.go,reference_price.go,player_trade_guard.go}``
与 ``trade/internal/logic/economy_credit_logic.go``：
- 卖家信用按 ``(seller_player_id, owner_revision)`` 物理隔离每一任持有者；过户换人强制清空重算，
  异地陌生新设备登录直接清 0 正面信用且保留黑历史（防借换机洗白）；
- 近 7 日参考价仅统计真实已结案市场撮合子单（kind=4, side=0, status=11），剔除同账号对敲，
  样本不足 10 笔明确标记 ``insufficient_sample=True``；
- 当面交易防骗支持 ``strict_consumables``（方案 A）与 ``acquaintance_locked``（方案 B），未知模式 fail-closed。
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Protocol as TypingProtocol

from google.protobuf.message import DecodeError, Message

from ..protocol import Protocol
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_TRADE_ASSET_NOT_TRADABLE,
    ERR_TRADE_AUCTION_ENDED,
    ERR_TRADE_CANNOT_TRADE_SELF,
    ERR_TRADE_ORDER_NOT_FOUND,
    ERR_TRADE_ORDER_STATE_CONFLICT,
    ERR_TRADE_REQUEST_CONFLICT,
    RoleDirectoryMissingError,
    StoreUnavailableError,
    TradeRejection,
)
from .model import (
    COMMERCE_KIND_AUCTION_LOT,
    COMMERCE_KIND_CONSIGNMENT,
    COMMERCE_KIND_MARKET_MATCH,
    COMMERCE_KIND_RMB_CONSIGNMENT,
    COMMERCE_SETTLED,
    CURRENCY_GOLD,
    UINT64_MAX,
    is_known_currency,
)
from .role_trade import RoleTradeAuthority

logger = logging.getLogger(__name__)

# -- 卖家店铺信用常量 ------------------------------------------------------------------------------------

SELLER_RATING_UNSPECIFIED = 0
SELLER_RATING_POSITIVE = 1  # 好评
SELLER_RATING_NEUTRAL = 2   # 中评
SELLER_RATING_NEGATIVE = 3  # 差评

SELLER_RISK_TIER_RESTRICTED = "restricted"                  # 有核实违约/拒付或低分受限
SELLER_RISK_TIER_NEW_OR_TRANSFERRED = "new_or_transferred"  # 新店或刚过户换人重算保护期
SELLER_RISK_TIER_STANDARD = "standard"                      # 正常稳定卖家
SELLER_RISK_TIER_TRUSTED = "trusted"                        # 高信誉老店

DEFAULT_SELLER_INITIAL_SCORE = 60
DEFAULT_SELLER_INITIAL_STAR_TENTHS = 30  # 3.0 星
DEFAULT_REVIEW_WINDOW_MS = 7 * 24 * 3600 * 1000
DEFAULT_PAIR_SCORE_WINDOW_MS = 7 * 24 * 3600 * 1000
DEFAULT_MAX_SCORED_PAIR_REVIEWS = 1
POSITIVE_DELTA = 2
NEGATIVE_PENALTY = 6
VIOLATION_PENALTY = 25
MAX_KNOWN_SELLER_DEVICES = 8
MAX_SELLER_REVIEW_COMMENT_RUNES = 200

# -- 近 7 日参考价与当面交易防骗常量 ---------------------------------------------------------------------

DAY_MS = 24 * 3600 * 1000
DEFAULT_REFERENCE_WINDOW_MS = 7 * DAY_MS
DEFAULT_REFERENCE_MIN_SAMPLES = 10
MAX_REFERENCE_SCAN_ROWS = 5000

PLAYER_TRADE_MODE_STRICT_CONSUMABLES = "strict_consumables"
PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED = "acquaintance_locked"
DEFAULT_MIN_FRIEND_DURATION_MS = 7 * DAY_MS
DEFAULT_COOLING_LOCK_DURATION_MS = 48 * 3600 * 1000
DEFAULT_MAX_REFERENCE_DEVIATION_PCT = 50
MAX_DAILY_PLAYER_TRADE_SCAN_ROWS = 1000

_MAX_CREDIT_WINDOW_MS = 90 * DAY_MS
_MAX_CREDIT_COOLDOWN_MS = 30 * DAY_MS


# -- 卖家信用领域模型与存储/目录协议 ---------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SellerCreditKey:
    """用 (角色 ID, 归属修订号) 唯一隔离一任持有者的店铺信用。"""

    seller_player_id: int
    owner_revision: int


@dataclass(slots=True)
class SellerCreditProfile:
    """某角色在当前持有周期内的店铺信用档案（last_ip/last_device_id/known_devices 仅服务端内部使用）。"""

    seller_player_id: int
    owner_account: str = ""
    owner_revision: int = 0
    ownership_changed: bool = False
    ownership_changed_at_ms: int = 0
    epoch_started_at_ms: int = 0
    completed_orders: int = 0
    positive_reviews: int = 0
    neutral_reviews: int = 0
    negative_reviews: int = 0
    violation_count: int = 0
    env_credit_cleared: bool = False
    env_reset_count: int = 0
    last_env_changed_at_ms: int = 0
    last_ip: str = ""
    last_device_id: str = ""
    known_devices: tuple[str, ...] = ()
    credit_score: int = DEFAULT_SELLER_INITIAL_SCORE
    star_rating_tenths: int = DEFAULT_SELLER_INITIAL_STAR_TENTHS
    positive_rate_bps: int = 0
    risk_tier: str = SELLER_RISK_TIER_NEW_OR_TRANSFERRED


@dataclass(frozen=True, slots=True)
class SellerEnvironmentEvent:
    """卖家一次由服务端可信网关/会话采集的登录或挂单环境事件。"""

    seller_player_id: int
    owner_account: str
    owner_revision: int
    ip: str = ""
    device_id: str = ""
    observed_at_ms: int = 0


@dataclass(frozen=True, slots=True)
class CompletedTradeFact:
    """一笔经官方托管真实完成的交易事实（含可选的服务端可信 IP/设备指纹）。"""

    order_id: str
    seller_player_id: int
    seller_account: str
    seller_owner_revision: int
    buyer_player_id: int
    buyer_account: str = ""
    seller_ip: str = ""
    seller_device_id: str = ""
    buyer_ip: str = ""
    buyer_device_id: str = ""
    amount_minor: int = 0
    settled_at_ms: int = 0


@dataclass(frozen=True, slots=True)
class SellerReviewRequest:
    """买家在评价窗口内提交的一次店铺评价。"""

    trade: CompletedTradeFact
    rating: int
    comment: str = ""


@dataclass(frozen=True, slots=True)
class SellerReviewRecord:
    """持久化保存的单笔评价凭证（按 order_id 幂等唯一）。"""

    order_id: str
    seller_player_id: int
    seller_owner_revision: int
    buyer_player_id: int
    buyer_account: str
    buyer_ip: str
    buyer_device_id: str
    rating: int
    counted_for_score: bool
    comment: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class SellerViolationRecord:
    """经核实的卖家违约/欺诈/拒付扣分记录（按 case_id 幂等唯一）。"""

    case_id: str
    order_id: str
    seller_player_id: int
    seller_account: str
    seller_owner_revision: int
    reason: str
    recorded_at_ms: int


@dataclass(frozen=True, slots=True)
class SellerCreditPolicy:
    """卖家信用防刷分窗口、最低计分门槛与异地换机清零后好评冷静期配置。"""

    review_window_ms: int = 0
    pair_score_window_ms: int = 0
    max_scored_reviews_per_pair_in_window: int = 0
    min_score_amount_minor: int = 0
    post_env_change_score_cooldown_ms: int = 0

    def normalized(self) -> "SellerCreditPolicy":
        return SellerCreditPolicy(
            review_window_ms=self.review_window_ms or DEFAULT_REVIEW_WINDOW_MS,
            pair_score_window_ms=self.pair_score_window_ms or DEFAULT_PAIR_SCORE_WINDOW_MS,
            max_scored_reviews_per_pair_in_window=(
                self.max_scored_reviews_per_pair_in_window
                if self.max_scored_reviews_per_pair_in_window > 0
                else DEFAULT_MAX_SCORED_PAIR_REVIEWS
            ),
            min_score_amount_minor=max(0, int(self.min_score_amount_minor)),
            post_env_change_score_cooldown_ms=max(0, int(self.post_env_change_score_cooldown_ms)),
        )

    def validate(self) -> None:
        if not 0 <= int(self.max_scored_reviews_per_pair_in_window) <= 10:
            raise ValueError(
                f"seller_credit.max_scored_reviews_per_pair_in_window({self.max_scored_reviews_per_pair_in_window}) "
                "must be within [0, 10]"
            )
        if int(self.review_window_ms) < 0 or int(self.review_window_ms) > _MAX_CREDIT_WINDOW_MS:
            raise ValueError(f"seller_credit.review_window_ms({self.review_window_ms}) exceeds 90 days")
        if int(self.pair_score_window_ms) < 0 or int(self.pair_score_window_ms) > _MAX_CREDIT_WINDOW_MS:
            raise ValueError(f"seller_credit.pair_score_window_ms({self.pair_score_window_ms}) exceeds 90 days")
        if (
            int(self.post_env_change_score_cooldown_ms) < 0
            or int(self.post_env_change_score_cooldown_ms) > _MAX_CREDIT_COOLDOWN_MS
        ):
            raise ValueError(
                f"seller_credit.post_env_change_score_cooldown_ms({self.post_env_change_score_cooldown_ms}) "
                "exceeds 30 days"
            )
        if int(self.min_score_amount_minor) < 0:
            raise ValueError("seller_credit.min_score_amount_minor must be >= 0")


class RoleOwnershipLookup(TypingProtocol):
    """查询角色当前权威持有账号与过户修订号 owner_revision。"""

    async def lookup_ownership(self, player_id: int) -> tuple[str, int]: ...


class SellerCreditStore(TypingProtocol):
    """卖家信用、成交计数、评价与违约记录持久化协议（MemoryTradeStore 与 SqlTradeStore 共同实现）。"""

    async def get_credit_profile(self, key: SellerCreditKey) -> SellerCreditProfile | None: ...
    async def has_prior_credit_epoch(self, seller_player_id: int, current_revision: int) -> bool: ...
    async def save_credit_profile(self, profile: SellerCreditProfile) -> None: ...
    async def mark_seller_order_completed(self, order_id: str, key: SellerCreditKey) -> bool: ...
    async def get_seller_review_by_order(self, order_id: str) -> SellerReviewRecord | None: ...
    async def count_pair_scored_reviews(
        self,
        key: SellerCreditKey,
        buyer_key: str,
        buyer_ip: str,
        buyer_device_id: str,
        since_ms: int,
        now_ms: int,
    ) -> int: ...
    async def save_seller_review(self, rec: SellerReviewRecord) -> None: ...
    async def has_seller_violation(self, case_id: str) -> bool: ...
    async def save_seller_violation(self, rec: SellerViolationRecord) -> None: ...


class DirectoryOwnershipLookup:
    """将 RoleTradeAuthority 适配为 RoleOwnershipLookup。"""

    def __init__(self, directory: RoleTradeAuthority) -> None:
        self._directory = directory

    async def lookup_ownership(self, player_id: int) -> tuple[str, int]:
        try:
            owned = await self._directory.lookup(int(player_id))
        except RoleDirectoryMissingError as exc:
            raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, f"seller role {player_id} not found") from exc
        if owned is None or int(owned.owner_revision) == 0 or not str(owned.account):
            raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, f"seller role {player_id} ownership invalid")
        return str(owned.account), int(owned.owner_revision)


class StoreOwnershipLookup:
    """基于 TradeStoreBase（优先直读 role_directory 权威状态，无目录时回退本地 role_fence）的归属查询适配器。"""

    def __init__(self, store) -> None:
        self._store = store

    async def lookup_ownership(self, player_id: int) -> tuple[str, int]:
        pid = int(player_id)
        if pid <= 0:
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid seller_player_id")
        role_dir = getattr(self._store, "role_directory", None)
        if role_dir is not None:
            state = await role_dir.get_role_fence_state(pid)
            if state is not None:
                await self._store.merge_role_fence(state, self._store.now_ms())
                account = str(state.account)
                rev = int(state.owner_revision)
                if not account or rev == 0:
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, f"seller role {pid} deleted or invalid")
                return account, rev
            fence = await self._store.get_role_fence_local(pid)
            if fence is None:
                raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, f"seller role {pid} not found")
            if not fence.account or fence.owner_revision == 0:
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, f"seller role {pid} deleted or invalid")
            return fence.account, fence.owner_revision
        fence = await self._store.get_role_fence_local(pid)
        if fence is None:
            raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, f"seller role {pid} not found")
        if not fence.account or fence.owner_revision == 0:
            raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, f"seller role {pid} deleted or invalid")
        return fence.account, fence.owner_revision


# -- 卖家信用纯计算与核心服务 ----------------------------------------------------------------------------

def normalize_env_token(raw: str) -> str:
    return raw.strip().lower()


def _remember_known_device(devices: tuple[str, ...], device: str) -> tuple[str, ...]:
    if not device or device in devices:
        return devices
    out = (*devices, device)
    if len(out) > MAX_KNOWN_SELLER_DEVICES:
        out = out[-MAX_KNOWN_SELLER_DEVICES:]
    return out


def new_initial_profile(
    player_id: int,
    account: str,
    revision: int,
    changed: bool,
    now_ms: int,
) -> SellerCreditProfile:
    return SellerCreditProfile(
        seller_player_id=int(player_id),
        owner_account=str(account),
        owner_revision=int(revision),
        ownership_changed=bool(changed),
        ownership_changed_at_ms=int(now_ms) if changed else 0,
        epoch_started_at_ms=int(now_ms),
        credit_score=DEFAULT_SELLER_INITIAL_SCORE,
        star_rating_tenths=DEFAULT_SELLER_INITIAL_STAR_TENTHS,
        risk_tier=SELLER_RISK_TIER_NEW_OR_TRANSFERRED,
    )


def recompute_credit_metrics(profile: SellerCreditProfile) -> None:
    """根据当前持有周期统计项重算信用分、好评率、星级与风控档位（与 Go recomputeCreditMetrics 逐行一致）。"""
    base_score = 0 if profile.env_credit_cleared else DEFAULT_SELLER_INITIAL_SCORE
    score = (
        base_score
        + int(profile.positive_reviews) * POSITIVE_DELTA
        - int(profile.negative_reviews) * NEGATIVE_PENALTY
        - int(profile.violation_count) * VIOLATION_PENALTY
    )
    score = max(0, min(100, score))
    profile.credit_score = score

    total_rated = int(profile.positive_reviews) + int(profile.neutral_reviews) + int(profile.negative_reviews)
    if total_rated > 0:
        profile.positive_rate_bps = (int(profile.positive_reviews) * 10000) // total_rated
    else:
        profile.positive_rate_bps = 0

    if not profile.env_credit_cleared and total_rated == 0 and int(profile.violation_count) == 0:
        profile.star_rating_tenths = DEFAULT_SELLER_INITIAL_STAR_TENTHS
    else:
        profile.star_rating_tenths = 10 + (score * 40 + 50) // 100

    if int(profile.violation_count) > 0 or score < 40:
        profile.risk_tier = SELLER_RISK_TIER_RESTRICTED
    elif (
        int(profile.completed_orders) < 5
        or (profile.ownership_changed and int(profile.completed_orders) < 10)
        or (profile.env_credit_cleared and int(profile.completed_orders) < 10)
    ):
        profile.risk_tier = SELLER_RISK_TIER_NEW_OR_TRANSFERRED
    elif int(profile.completed_orders) >= 20 and score >= 85 and int(profile.positive_rate_bps) >= 9500:
        profile.risk_tier = SELLER_RISK_TIER_TRUSTED
    else:
        profile.risk_tier = SELLER_RISK_TIER_STANDARD


class SellerCreditService:
    """类店铺卖家信用评分服务：过户换人强制清空、异地陌生新设备清 0、多维度防刷分。"""

    def __init__(
        self,
        store: SellerCreditStore,
        ownership: RoleOwnershipLookup | None = None,
        policy: SellerCreditPolicy = SellerCreditPolicy(),
    ) -> None:
        self.store = store
        self.ownership = ownership
        self.policy = policy

    async def _ensure_epoch_profile(
        self,
        player_id: int,
        account: str,
        revision: int,
        now_ms: int,
    ) -> SellerCreditProfile:
        key = SellerCreditKey(seller_player_id=int(player_id), owner_revision=int(revision))
        existing = await self.store.get_credit_profile(key)
        if existing is not None:
            # 即使修订号未变但账号发生变化（异常或后台修复场景），也强制按换人重置保护买家。
            if existing.owner_account and account and existing.owner_account != account:
                reset = new_initial_profile(player_id, account, revision, True, now_ms)
                await self.store.save_credit_profile(reset)
                return reset
            return replace(existing)
        has_prior = await self.store.has_prior_credit_epoch(int(player_id), int(revision))
        changed = has_prior or int(revision) > 1
        fresh = new_initial_profile(player_id, account, revision, changed, now_ms)
        await self.store.save_credit_profile(fresh)
        return fresh

    async def on_character_transferred(
        self,
        player_id: int,
        new_account: str,
        new_owner_revision: int,
        now_ms: int,
    ) -> SellerCreditProfile:
        """角色官方过户换账号后主动清空新持有周期信用档案并标记换人重算。"""
        if self.store is None:
            raise StoreUnavailableError("seller credit store not configured")
        if int(player_id) <= 0 or not new_account or int(new_owner_revision) <= 0:
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid character transfer credit reset arguments")
        fresh = new_initial_profile(int(player_id), new_account, int(new_owner_revision), True, int(now_ms))
        await self.store.save_credit_profile(fresh)
        return fresh

    async def record_seller_environment(
        self,
        ev: SellerEnvironmentEvent,
    ) -> tuple[SellerCreditProfile, bool]:
        """记录卖家登录/操作环境（IP + 设备指纹），命中异地陌生新设备时清 0 正面信用并保留黑历史。"""
        if self.store is None:
            raise StoreUnavailableError("seller credit store not configured")
        ip = normalize_env_token(ev.ip)
        device = normalize_env_token(ev.device_id)
        if int(ev.seller_player_id) <= 0 or int(ev.owner_revision) <= 0 or not ev.owner_account or (not ip and not device):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid seller environment event")

        profile = await self._ensure_epoch_profile(
            ev.seller_player_id, ev.owner_account, ev.owner_revision, ev.observed_at_ms
        )
        ip_changed = bool(ip and profile.last_ip and ip != profile.last_ip)
        has_device_baseline = bool(profile.known_devices or profile.last_device_id)
        is_unknown_new_device = bool(
            device and has_device_baseline and device not in profile.known_devices and device != profile.last_device_id
        )

        updated = False
        cleared = False
        if ip and profile.last_ip != ip:
            profile.last_ip = ip
            updated = True

        if is_unknown_new_device and ip_changed:
            profile.completed_orders = 0
            profile.positive_reviews = 0
            profile.neutral_reviews = 0
            # 刻意保留 negative_reviews 与 violation_count，严禁借异地换机洗白劣迹。
            profile.env_credit_cleared = True
            profile.env_reset_count += 1
            profile.last_env_changed_at_ms = int(ev.observed_at_ms)
            profile.last_device_id = device
            profile.known_devices = (device,)
            recompute_credit_metrics(profile)
            cleared = True
            updated = True
        elif device:
            if profile.last_device_id != device:
                profile.last_device_id = device
                updated = True
            if device not in profile.known_devices:
                profile.known_devices = _remember_known_device(profile.known_devices, device)
                updated = True

        if updated:
            await self.store.save_credit_profile(profile)
        return profile, cleared

    async def get_profile(self, seller_player_id: int, now_ms: int) -> SellerCreditProfile:
        """按当前权威归属查询卖家店铺信用档案（一旦过户立即自动切换新持有周期）。"""
        if self.store is None or self.ownership is None:
            raise StoreUnavailableError("seller credit service not configured")
        pid = int(seller_player_id)
        if pid <= 0:
            raise TradeRejection(ERR_INVALID_PARAMETER, "seller_player_id must be positive")
        account, rev = await self.ownership.lookup_ownership(pid)
        if not account or rev <= 0:
            raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "seller ownership invalid")
        return await self._ensure_epoch_profile(pid, account, rev, int(now_ms))

    async def record_completed_trade(self, fact: CompletedTradeFact) -> SellerCreditProfile:
        """在官方托管交易结案后按 order_id 幂等累计卖家真实成交笔数（拒绝同账号对敲）。"""
        if self.store is None:
            raise StoreUnavailableError("seller credit store not configured")
        if (
            not fact.order_id
            or int(fact.seller_player_id) <= 0
            or int(fact.buyer_player_id) <= 0
            or int(fact.seller_owner_revision) <= 0
            or not fact.seller_account
        ):
            raise TradeRejection(ERR_INVALID_PARAMETER, "incomplete completed trade fact")
        if int(fact.seller_player_id) == int(fact.buyer_player_id) or (
            fact.buyer_account and fact.seller_account == fact.buyer_account
        ):
            raise TradeRejection(ERR_TRADE_CANNOT_TRADE_SELF, "cannot count self trade")

        key = SellerCreditKey(
            seller_player_id=int(fact.seller_player_id),
            owner_revision=int(fact.seller_owner_revision),
        )
        profile = await self._ensure_epoch_profile(
            fact.seller_player_id, fact.seller_account, fact.seller_owner_revision, fact.settled_at_ms
        )
        inserted = await self.store.mark_seller_order_completed(fact.order_id, key)
        if inserted:
            profile.completed_orders += 1
            recompute_credit_metrics(profile)
            await self.store.save_credit_profile(profile)
        return profile

    async def submit_review(
        self,
        req: SellerReviewRequest,
        now_ms: int,
    ) -> tuple[SellerReviewRecord, SellerCreditProfile, bool]:
        """买家对已完成托管订单在 7 日窗口内提交评价（幂等、防改评、多维防刷分）。"""
        if self.store is None:
            raise StoreUnavailableError("seller credit store not configured")
        t = req.trade
        rating = int(req.rating)
        if (
            not t.order_id
            or int(t.seller_player_id) <= 0
            or int(t.buyer_player_id) <= 0
            or int(t.seller_owner_revision) <= 0
            or not t.seller_account
            or rating not in (SELLER_RATING_POSITIVE, SELLER_RATING_NEUTRAL, SELLER_RATING_NEGATIVE)
        ):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid seller review request")
        if int(t.seller_player_id) == int(t.buyer_player_id) or (
            t.buyer_account and t.seller_account == t.buyer_account
        ):
            raise TradeRejection(ERR_TRADE_CANNOT_TRADE_SELF, "cannot review self")

        policy = self.policy.normalized()
        now = int(now_ms)
        settled = int(t.settled_at_ms)
        if now < settled or now - settled > policy.review_window_ms:
            raise TradeRejection(ERR_TRADE_AUCTION_ENDED, "review window expired")

        key = SellerCreditKey(
            seller_player_id=int(t.seller_player_id),
            owner_revision=int(t.seller_owner_revision),
        )
        prior = await self.store.get_seller_review_by_order(t.order_id)
        if prior is not None:
            if (
                int(prior.rating) != rating
                or int(prior.buyer_player_id) != int(t.buyer_player_id)
                or int(prior.seller_player_id) != int(t.seller_player_id)
            ):
                raise TradeRejection(ERR_TRADE_REQUEST_CONFLICT, "conflicting review replay")
            profile = await self._ensure_epoch_profile(
                t.seller_player_id, t.seller_account, t.seller_owner_revision, now
            )
            return prior, profile, True

        profile = await self.record_completed_trade(t)

        buyer_key = t.buyer_account or f"player:{int(t.buyer_player_id)}"
        seller_ip = normalize_env_token(t.seller_ip) or profile.last_ip
        seller_dev = normalize_env_token(t.seller_device_id) or profile.last_device_id
        buyer_ip = normalize_env_token(t.buyer_ip)
        buyer_dev = normalize_env_token(t.buyer_device_id)

        window_start = now - policy.pair_score_window_ms if now > policy.pair_score_window_ms else 0
        pair_count = await self.store.count_pair_scored_reviews(
            key, buyer_key, buyer_ip, buyer_dev, window_start, now
        )

        counted = True
        # 1) 买卖双方同 IP 或同设备互评：防自开小号或工作室同机刷分。
        if (seller_ip and buyer_ip and seller_ip == buyer_ip) or (
            seller_dev and buyer_dev and seller_dev == buyer_dev
        ):
            counted = False
        # 2) 低于最低计分金额的好评不涨分。
        if rating == SELLER_RATING_POSITIVE and int(t.amount_minor) < policy.min_score_amount_minor:
            counted = False
        # 3) 同一买家账号/IP/设备在窗口内超出计分次数上限。
        if pair_count >= policy.max_scored_reviews_per_pair_in_window:
            counted = False
        # 4) 卖家异地换机清 0 后的好评冷静期内不涨分。
        if (
            rating == SELLER_RATING_POSITIVE
            and policy.post_env_change_score_cooldown_ms > 0
            and profile.last_env_changed_at_ms > 0
            and now >= profile.last_env_changed_at_ms
            and now - profile.last_env_changed_at_ms < policy.post_env_change_score_cooldown_ms
        ):
            counted = False

        rec = SellerReviewRecord(
            order_id=t.order_id,
            seller_player_id=int(t.seller_player_id),
            seller_owner_revision=int(t.seller_owner_revision),
            buyer_player_id=int(t.buyer_player_id),
            buyer_account=buyer_key,
            buyer_ip=buyer_ip,
            buyer_device_id=buyer_dev,
            rating=rating,
            counted_for_score=counted,
            comment=req.comment.strip(),
            created_at_ms=now,
        )
        await self.store.save_seller_review(rec)
        if counted:
            if rating == SELLER_RATING_POSITIVE:
                profile.positive_reviews += 1
            elif rating == SELLER_RATING_NEUTRAL:
                profile.neutral_reviews += 1
            elif rating == SELLER_RATING_NEGATIVE:
                profile.negative_reviews += 1
            recompute_credit_metrics(profile)
            await self.store.save_credit_profile(profile)
        return rec, profile, False

    async def record_violation(self, rec: SellerViolationRecord) -> SellerCreditProfile:
        """记入核实违约/欺诈/拒付（按 case_id 幂等扣分并立即降级风控档）。"""
        if self.store is None:
            raise StoreUnavailableError("seller credit store not configured")
        if (
            not rec.case_id
            or int(rec.seller_player_id) <= 0
            or int(rec.seller_owner_revision) <= 0
            or not rec.seller_account
        ):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid seller violation record")
        profile = await self._ensure_epoch_profile(
            rec.seller_player_id, rec.seller_account, rec.seller_owner_revision, rec.recorded_at_ms
        )
        if await self.store.has_seller_violation(rec.case_id):
            return profile
        await self.store.save_seller_violation(rec)
        profile.violation_count += 1
        recompute_credit_metrics(profile)
        await self.store.save_credit_profile(profile)
        return profile


def settled_trade_fact(
    rec: Message | None,
    w: Message | None,
    settled_at_ms: int,
) -> CompletedTradeFact | None:
    """从已结案（Settled）的官方托管合同抽取卖家信用成交事实（与 Go SettledTradeFact 逐行一致）。

    仅统计固定价寄售（kind=3）、集市撮合子单（kind=4/side=0）、拍卖（kind=5）与非角色人民币寄售（kind=6）；
    刻意排除当面交易（kind=2）与角色过户（kind=6 且 character_player_id!=0）。
    """
    if rec is None or w is None or int(rec.status) != COMMERCE_SETTLED or int(rec.seller_player_id) == 0:
        return None
    kind = int(rec.kind)
    order_id = f"commerce:{int(rec.commerce_id)}"
    buyer_id = 0
    amount = 0
    if kind == COMMERCE_KIND_CONSIGNMENT:
        buyer_id = int(rec.buyer_player_id)
        amount = int(rec.unit_price)
    elif kind == COMMERCE_KIND_MARKET_MATCH:
        if int(rec.side) != 0:
            return None
        buyer_id = int(rec.buyer_player_id)
        qty = int(rec.quantity)
        unit = int(rec.unit_price)
        amount = min(UINT64_MAX, unit * qty) if qty > 0 else 0
    elif kind == COMMERCE_KIND_AUCTION_LOT:
        buyer_id = int(w.highest_bidder)
        amount = int(w.highest_bid)
    elif kind == COMMERCE_KIND_RMB_CONSIGNMENT:
        if not w.HasField("rmb") or int(w.rmb.character_player_id) != 0:
            return None
        buyer_id = int(rec.buyer_player_id)
        amount = int(w.rmb.price_fen)
        if str(w.rmb.mall_order_id):
            order_id = str(w.rmb.mall_order_id)
    else:
        return None

    seller_id = int(rec.seller_player_id)
    if buyer_id == 0 or buyer_id == seller_id:
        return None

    seller_acct = str(w.seller_identity.account) if w.HasField("seller_identity") else ""
    seller_rev = int(w.seller_identity.owner_revision) if w.HasField("seller_identity") else 0
    buyer_acct = str(w.buyer_identity.account) if w.HasField("buyer_identity") else ""
    if kind == COMMERCE_KIND_RMB_CONSIGNMENT and w.HasField("rmb"):
        if not seller_acct:
            seller_acct = str(w.rmb.seller_account)
        if seller_rev == 0:
            seller_rev = int(w.rmb.seller_owner_revision)
        if not buyer_acct:
            buyer_acct = str(w.rmb.buyer_account)
    if not seller_acct or seller_rev == 0 or (buyer_acct and seller_acct == buyer_acct):
        return None

    return CompletedTradeFact(
        order_id=order_id,
        seller_player_id=seller_id,
        seller_account=seller_acct,
        seller_owner_revision=seller_rev,
        buyer_player_id=buyer_id,
        buyer_account=buyer_acct,
        amount_minor=amount,
        settled_at_ms=int(settled_at_ms),
    )


def parse_commerce_order_id(raw: str) -> int | None:
    """解析评价订单号：支持纯数字或 ``commerce:<id>`` 前缀。"""
    trimmed = raw.strip()
    if trimmed.startswith("commerce:"):
        trimmed = trimmed[len("commerce:") :]
    if not trimmed or not trimmed.isdigit():
        return None
    val = int(trimmed)
    return val if 0 < val <= UINT64_MAX else None


def seller_credit_view(protocol: Protocol, profile: SellerCreditProfile) -> Message:
    """投影对外脱敏的 trade.SellerCreditView（严禁下发账号、IP 与设备指纹）。"""
    return protocol.message(
        "trade.SellerCreditView",
        seller_player_id=int(profile.seller_player_id),
        owner_revision=int(profile.owner_revision),
        ownership_changed=bool(profile.ownership_changed),
        ownership_changed_at_ms=int(profile.ownership_changed_at_ms),
        epoch_started_at_ms=int(profile.epoch_started_at_ms),
        completed_orders=int(profile.completed_orders),
        positive_reviews=int(profile.positive_reviews),
        neutral_reviews=int(profile.neutral_reviews),
        negative_reviews=int(profile.negative_reviews),
        violation_count=int(profile.violation_count),
        env_credit_cleared=bool(profile.env_credit_cleared),
        env_reset_count=int(profile.env_reset_count),
        last_env_changed_at_ms=int(profile.last_env_changed_at_ms),
        credit_score=int(profile.credit_score),
        star_rating_tenths=int(profile.star_rating_tenths),
        positive_rate_bps=int(profile.positive_rate_bps),
        risk_tier=str(profile.risk_tier),
    )


# -- 近 7 日中位成交参考价（p50） ------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class MarketFillSample:
    """一笔真实已结案市场撮合子单观测样本。"""

    item_config_id: int
    currency_type: int
    unit_price: int
    quantity: int
    seller_account: str = ""
    buyer_account: str = ""
    settled_at_ms: int = 0


@dataclass(frozen=True, slots=True)
class ReferencePrice:
    """服务端基于真实成交计算的近窗中位成交参考价（p50）。"""

    item_config_id: int
    currency_type: int
    median_unit_price: int = 0
    sample_count: int = 0
    insufficient_sample: bool = True


class ReferencePriceReader(TypingProtocol):
    """查询某标准品在指定币种下的近期真实中位成交价。"""

    async def reference_price(self, item_config_id: int, currency_type: int, now_ms: int) -> ReferencePrice: ...


def compute_reference_price(
    samples: Sequence[MarketFillSample],
    item_config_id: int,
    currency_type: int,
    now_ms: int,
    window_ms: int = 0,
    min_samples: int = 0,
) -> ReferencePrice:
    """从成交样本中剔除同账号对敲、越界与无效样本，计算真实成交单价的 p50 中位数。"""
    win = int(window_ms) if int(window_ms) > 0 else DEFAULT_REFERENCE_WINDOW_MS
    minimum = int(min_samples) if int(min_samples) > 0 else DEFAULT_REFERENCE_MIN_SAMPLES
    now = int(now_ms)
    floor_ms = now - win if now > win else 0

    prices: list[int] = []
    for s in samples:
        if int(s.item_config_id) != int(item_config_id) or int(s.currency_type) != int(currency_type):
            continue
        if int(s.unit_price) <= 0 or int(s.quantity) <= 0:
            continue
        if int(s.settled_at_ms) < floor_ms or int(s.settled_at_ms) > now:
            continue
        if s.seller_account and s.seller_account == s.buyer_account:
            continue
        prices.append(int(s.unit_price))

    count = len(prices)
    if count < minimum:
        return ReferencePrice(
            item_config_id=int(item_config_id),
            currency_type=int(currency_type),
            median_unit_price=0,
            sample_count=count,
            insufficient_sample=True,
        )
    prices.sort()
    mid = count // 2
    if count % 2 == 1:
        median = prices[mid]
    else:
        low, high = prices[mid - 1], prices[mid]
        median = low + (high - low) // 2
    return ReferencePrice(
        item_config_id=int(item_config_id),
        currency_type=int(currency_type),
        median_unit_price=median,
        sample_count=count,
        insufficient_sample=False,
    )


class StoreReferencePriceReader:
    """基于 TradeStoreBase 的真实成交参考价读取器（MemoryTradeStore 与 SqlTradeStore 通用）。"""

    def __init__(
        self,
        store,
        *,
        window_ms: int = 0,
        min_samples: int = 0,
    ) -> None:
        self._store = store
        self.window_ms = int(window_ms)
        self.min_samples = int(min_samples)

    async def reference_price(self, item_config_id: int, currency_type: int, now_ms: int) -> ReferencePrice:
        if self._store is None:
            raise StoreUnavailableError("reference price store not configured")
        win = self.window_ms if self.window_ms > 0 else DEFAULT_REFERENCE_WINDOW_MS
        now = int(now_ms)
        floor_ms = now - win if now > win else 0
        samples = await self._store.list_settled_market_fills(
            int(item_config_id),
            int(currency_type),
            floor_ms,
            now,
            MAX_REFERENCE_SCAN_ROWS,
        )
        return compute_reference_price(samples, item_config_id, currency_type, now, win, self.min_samples)


async def estimate_selection_value(
    reader: ReferencePriceReader | None,
    sel: Message | None,
    currency_type: int,
    now_ms: int,
) -> tuple[int, bool]:
    """按参考价估算一侧资产选择在指定币种下的公允参考总额（含实例/宠物/跨币种/缺样本时 has_unpriced=True）。"""
    if sel is None:
        return 0, False
    total = 0
    has_unpriced = bool(len(sel.item_uuids) > 0 or int(sel.pet_id) != 0)
    if int(sel.currency_amount) > 0:
        if int(sel.currency_type) != int(currency_type):
            has_unpriced = True
        else:
            total = int(sel.currency_amount)
    if len(sel.items) == 0:
        return total, has_unpriced
    if reader is None:
        return total, True
    for cfg_id, count in sel.items.items():
        cnt = int(count)
        if cnt == 0:
            continue
        ref = await reader.reference_price(int(cfg_id), int(currency_type), int(now_ms))
        if ref.insufficient_sample or ref.median_unit_price == 0:
            has_unpriced = True
            continue
        part = ref.median_unit_price * cnt
        if part > UINT64_MAX or total + part > UINT64_MAX:
            return UINT64_MAX, True
        total += part
    return total, has_unpriced


# -- 当面交易防骗护栏与单日流出统计 -----------------------------------------------------------------------

class FriendRelationReader(TypingProtocol):
    """查询两名角色在权威好友服务中的系统好友关系与持续时长。"""

    async def friendship_duration_ms(self, player_a: int, player_b: int, now_ms: int) -> tuple[bool, int]: ...


class DailyPlayerTradeTracker(TypingProtocol):
    """查询指定角色当日已通过游戏内当面交易流出的货币总额。"""

    async def daily_currency_outflow(self, player_id: int, currency_type: int, day_start_ms: int) -> int: ...


@dataclass(frozen=True, slots=True)
class PlayerTradePolicy:
    """游戏内当面交易防骗策略参数。"""

    mode: str = ""
    consumable_items: tuple[int, ...] = ()
    max_daily_currency_amount: int = 0
    reference_currency_type: int = CURRENCY_GOLD
    min_friend_duration_ms: int = 0
    max_reference_deviation_pct: int = 0
    cooling_lock_duration_ms: int = 0
    allow_acquaintance_cooling_lock: bool = False

    def validate(self) -> None:
        if self.mode and self.mode not in (
            PLAYER_TRADE_MODE_STRICT_CONSUMABLES,
            PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED,
        ):
            raise ValueError(
                f"player_trade.mode({self.mode!r}) must be empty, "
                f"{PLAYER_TRADE_MODE_STRICT_CONSUMABLES!r} or {PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED!r}"
            )
        if int(self.max_reference_deviation_pct) < 0 or int(self.max_reference_deviation_pct) >= 100:
            raise ValueError(
                f"player_trade.max_reference_deviation_pct({self.max_reference_deviation_pct}) must be in [0, 99]"
            )
        if int(self.max_daily_currency_amount) < 0:
            raise ValueError("player_trade.max_daily_currency_amount must be >= 0")


@dataclass(frozen=True, slots=True)
class PlayerTradeGuardResult:
    """当面交易防骗评估结果。"""

    require_cooling_lock: bool = False
    cooling_lock_duration_ms: int = 0
    reason: str = ""


def saturating_add_uint64(a: int, b: int) -> int:
    res = int(a) + int(b)
    return UINT64_MAX if res > UINT64_MAX else res


def workflow_currency_outflow(
    protocol: Protocol,
    raw_workflow: bytes,
    currency_type: int,
    *,
    as_seller: bool,
) -> int:
    """从 TradeCommerceWorkflow 二进制提取买/卖一侧指定币种的当面交易流出额。"""
    if not raw_workflow:
        return 0
    w = protocol.message("trade.TradeCommerceWorkflow")
    try:
        w.ParseFromString(bytes(raw_workflow))
    except DecodeError as exc:
        raise StoreUnavailableError(f"failed to decode player trade workflow: {exc}") from exc
    if as_seller:
        sel = w.seller_assets if w.HasField("seller_assets") else None
    else:
        sel = w.buyer_assets if w.HasField("buyer_assets") else None
    if sel is None or int(sel.currency_amount) == 0 or int(sel.currency_type) != int(currency_type):
        return 0
    return int(sel.currency_amount)


class StoreDailyPlayerTradeTracker:
    """基于 TradeStoreBase 的当日当面交易货币流出统计器（MemoryTradeStore 与 SqlTradeStore 通用）。"""

    def __init__(self, store) -> None:
        self._store = store

    async def daily_currency_outflow(self, player_id: int, currency_type: int, day_start_ms: int) -> int:
        if self._store is None:
            raise StoreUnavailableError("daily player trade tracker store not configured")
        pid = int(player_id)
        cur = int(currency_type)
        if pid <= 0 or not is_known_currency(cur):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid player_id or currency_type")
        return await self._store.daily_player_trade_currency_outflow(pid, cur, int(day_start_ms))


def _selection_empty(sel: Message | None) -> bool:
    return sel is None or (
        len(sel.items) == 0
        and len(sel.item_uuids) == 0
        and int(sel.pet_id) == 0
        and int(sel.currency_amount) == 0
    )


class PlayerTradeGuard:
    """游戏内当面交易防站外引流与低价偷渡门禁（与 Go PlayerTradeGuard 逐行一致）。"""

    def __init__(
        self,
        policy: PlayerTradePolicy = PlayerTradePolicy(),
        *,
        friends: FriendRelationReader | None = None,
        prices: ReferencePriceReader | None = None,
        daily_out: DailyPlayerTradeTracker | None = None,
    ) -> None:
        self.policy = policy
        self.friends = friends
        self.prices = prices
        self.daily_out = daily_out

    def _is_consumable_only(self, sel: Message | None) -> bool:
        if _selection_empty(sel):
            return True
        assert sel is not None
        if len(sel.item_uuids) > 0 or int(sel.pet_id) != 0:
            return False
        allowed = set(self.policy.consumable_items)
        return all(int(cfg_id) in allowed for cfg_id in sel.items)

    async def _check_currency_cap(self, player_id: int, sel: Message | None, now_ms: int) -> None:
        if sel is None or int(sel.currency_amount) == 0:
            return
        amt = int(sel.currency_amount)
        cap = int(self.policy.max_daily_currency_amount)
        if cap == 0 or amt > cap:
            raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "daily player trade currency limit exceeded")
        if self.daily_out is not None:
            day_start = (int(now_ms) // DAY_MS) * DAY_MS
            used = await self.daily_out.daily_currency_outflow(int(player_id), int(sel.currency_type), day_start)
            if used > cap or amt > cap - used:
                raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "daily player trade currency quota exhausted")

    async def evaluate(
        self,
        seller_id: int,
        buyer_id: int,
        seller_assets: Message | None,
        buyer_assets: Message | None,
        now_ms: int,
    ) -> PlayerTradeGuardResult:
        """校验游戏内当面交易是否符合防骗策略；Mode 为空不介入，未知模式 fail-closed。"""
        if not self.policy.mode:
            return PlayerTradeGuardResult()
        if int(seller_id) <= 0 or int(buyer_id) <= 0 or int(seller_id) == int(buyer_id):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid player trade participants")
        if _selection_empty(seller_assets) and _selection_empty(buyer_assets):
            raise TradeRejection(ERR_INVALID_PARAMETER, "empty player trade assets")
        if self.policy.mode == PLAYER_TRADE_MODE_STRICT_CONSUMABLES:
            return await self._eval_strict_consumables(seller_id, buyer_id, seller_assets, buyer_assets, now_ms)
        if self.policy.mode == PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED:
            return await self._eval_acquaintance_locked(seller_id, buyer_id, seller_assets, buyer_assets, now_ms)
        raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, f"unknown player trade mode: {self.policy.mode!r}")

    async def _eval_strict_consumables(
        self,
        seller_id: int,
        buyer_id: int,
        seller_assets: Message | None,
        buyer_assets: Message | None,
        now_ms: int,
    ) -> PlayerTradeGuardResult:
        if not self._is_consumable_only(seller_assets) or not self._is_consumable_only(buyer_assets):
            raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "only whitelisted consumables allowed in face-to-face trade")
        await self._check_currency_cap(seller_id, seller_assets, now_ms)
        await self._check_currency_cap(buyer_id, buyer_assets, now_ms)
        return PlayerTradeGuardResult(reason="strict_consumables_ok")

    async def _eval_acquaintance_locked(
        self,
        seller_id: int,
        buyer_id: int,
        seller_assets: Message | None,
        buyer_assets: Message | None,
        now_ms: int,
    ) -> PlayerTradeGuardResult:
        one_sided = _selection_empty(seller_assets) or _selection_empty(buyer_assets)
        cap = int(self.policy.max_daily_currency_amount)
        seller_amt = int(seller_assets.currency_amount) if seller_assets is not None else 0
        buyer_amt = int(buyer_assets.currency_amount) if buyer_assets is not None else 0
        small_consumable_swap = (
            not one_sided
            and self._is_consumable_only(seller_assets)
            and self._is_consumable_only(buyer_assets)
            and (cap == 0 or (seller_amt <= cap and buyer_amt <= cap))
        )
        if small_consumable_swap:
            try:
                await self._check_currency_cap(seller_id, seller_assets, now_ms)
                await self._check_currency_cap(buyer_id, buyer_assets, now_ms)
                return PlayerTradeGuardResult(reason="small_consumable_swap_ok")
            except TradeRejection:
                # 超单日小额互助上限时转入下方熟人硬锁判定。
                pass

        # 硬锁 1：权威系统好友及最短好友时长。
        if self.friends is None:
            raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "friend relation reader not configured")
        min_friend_ms = int(self.policy.min_friend_duration_ms) or DEFAULT_MIN_FRIEND_DURATION_MS
        is_friend, duration_ms = await self.friends.friendship_duration_ms(int(seller_id), int(buyer_id), int(now_ms))
        if not is_friend or int(duration_ms) < min_friend_ms:
            raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "friendship duration below minimum requirement")

        # 硬锁 2：近 7 日中位参考价偏离保护。
        ref_currency = int(self.policy.reference_currency_type) or CURRENCY_GOLD
        max_dev = int(self.policy.max_reference_deviation_pct)
        if max_dev == 0 or max_dev >= 100:
            max_dev = DEFAULT_MAX_REFERENCE_DEVIATION_PCT
        seller_val, seller_unpriced = await estimate_selection_value(self.prices, seller_assets, ref_currency, now_ms)
        buyer_val, buyer_unpriced = await estimate_selection_value(self.prices, buyer_assets, ref_currency, now_ms)

        deviated = one_sided or seller_unpriced or buyer_unpriced
        if not deviated:
            low, high = (seller_val, buyer_val) if seller_val <= buyer_val else (buyer_val, seller_val)
            keep_pct = 100 - max_dev
            deviated = True if high == 0 else (low * 100 < high * keep_pct)

        if not deviated:
            return PlayerTradeGuardResult(reason="acquaintance_fair_value_ok")
        if not self.policy.allow_acquaintance_cooling_lock:
            raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "value deviation exceeds limit")
        lock_ms = int(self.policy.cooling_lock_duration_ms) or DEFAULT_COOLING_LOCK_DURATION_MS
        return PlayerTradeGuardResult(
            require_cooling_lock=True,
            cooling_lock_duration_ms=lock_ms,
            reason="acquaintance_deviation_cooling_lock",
        )
