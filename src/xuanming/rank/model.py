"""rank 领域模型、纯计算规则与校验函数（与 Go ``go/rank/internal/data`` 完全对齐）。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from google.protobuf.message import Message

from .errors import RankInvalidError

DATABASE_NAME = "mmorpg_rank"
RANK_NODE_TYPE = 13  # common.base.eNodeType.RankNodeService

# RankScope 枚举取值
RANK_SCOPE_UNSPECIFIED = 0
RANK_SCOPE_GLOBAL = 1
RANK_SCOPE_GUILD = 2
RANK_SCOPE_INSTANCE = 3
RANK_SCOPE_CUSTOM = 4

# RankGenerationState 枚举取值
RANK_GEN_UNSPECIFIED = 0
RANK_GEN_OPEN = 1
RANK_GEN_SEALED = 2

# RankSubmitMode 枚举取值
SUBMIT_UNSPECIFIED = 0
SUBMIT_SET_IF_HIGHER = 1
SUBMIT_SET = 2
SUBMIT_INCREMENT = 3

# rank_mail_reward.status 取值
REWARD_PENDING = 1
REWARD_DELIVERED = 2
REWARD_ABANDONED = 3

# 容量与边界常量
MAX_SOURCE_SHARDS = 1024
MAX_BATCH_ENTRIES = 128
DEFAULT_TOP_N = 100
MAX_TOP_N = 1000
MAX_REWARD_TIERS = 100
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 50
DEFAULT_AROUND_RADIUS = 10
MAX_AROUND_RADIUS = 25
RANK_EXACT_LIMIT = 10_000
MAX_PERIOD_BYTES = 64
MAX_ACCOUNT_BYTES = 191

# 64 位整数边界
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
UINT64_MAX = (1 << 64) - 1

# 奖励邮件有效期窗口常量（毫秒）
HOUR_MS = 60 * 60 * 1000
DAY_MS = 24 * HOUR_MS
MAX_REWARD_MAIL_WINDOW_MS = 30 * DAY_MS
MIN_REWARD_MAIL_CLAIM_WINDOW_MS = 24 * HOUR_MS
MIN_REWARD_MAIL_WINDOW_MS = 2 * MIN_REWARD_MAIL_CLAIM_WINDOW_MS
MAIL_TTL_DAYS_MIN = 1
MAIL_TTL_DAYS_MAX = 365

# 邮件附件限制（与 Go shared/mailgrant/bundle.go 一致）
MAX_ATTACHMENT_CURRENCIES = 4
MAX_ATTACHMENT_ITEMS = 16
MAX_ATTACHMENT_TOTAL_ITEMS = 4096
# 对应 proto/common/base/currency_type.proto 的完整取值表
KNOWN_CURRENCY_TYPES = frozenset((0, 1, 2))


@dataclass(slots=True)
class BoardRow:
    """``rank_board`` 表行快照。"""

    board_id: int
    board_type: int
    scope: int
    scope_id: int
    period: str
    generation: int
    producer: str
    source_epoch: int
    ascending: int
    state: int
    settlement_id: int
    open_request_id: int
    open_request_hash: str
    created_ms: int
    sealed_ms: int
    deleted_ms: int


@dataclass(slots=True)
class ShardRow:
    """``rank_board_shard`` 表行快照。"""

    source_shard: int
    committed_cursor: int
    entries: int


@dataclass(slots=True)
class ScoreRow:
    """``rank_score`` 表行（合并写分时使用的现值）。"""

    score: int
    achieved_ms: int
    score_version: int


@dataclass(frozen=True, slots=True)
class ScoreWrite:
    """单条合并后的分数写回项。"""

    entity_id: int
    score: int
    achieved_ms: int
    score_version: int
    existed: bool


@dataclass(slots=True)
class RewardRow:
    """``rank_mail_reward`` 表行（工作器领租约与结算所需字段）。"""

    reward_id: int
    settlement_id: int
    player_id: int
    rank: int
    attachments: bytes
    status: int = REWARD_PENDING
    mail_id: int = 0
    next_attempt_ms: int = 0
    lease_until_ms: int = 0
    lease_token: int = 0
    attempts: int = 0
    created_ms: int = 0
    updated_ms: int = 0
    expire_ms: int = 0


@dataclass(frozen=True, slots=True)
class PageWindow:
    """一次翻页落在名次上的窗口。"""

    page: int
    pages: int
    offset: int
    rows: int


@dataclass(slots=True)
class RankedEntry:
    """读路径查出的本人分数行及名次索引位置。"""

    entity_id: int
    score: int
    order_key: int
    achieved_ms: int
    ahead: int
    capped: bool


def order_key(ascending: int | bool, score: int) -> int:
    """将有符号 int64 分数映射为“名次越靠前越小”的排序键。

    升序榜取 ``score``；降序榜取按位取反 ``~score``（即 ``-score - 1``），
    严格单调递减且覆盖整个 ``[-2^63, 2^63-1]`` 不溢出。
    """
    if score < INT64_MIN or score > INT64_MAX:
        raise RankInvalidError(f"score out of int64 range: {score}")
    return score if ascending else ~score


def checked_increment_int64(old_score: int, delta: int) -> int:
    """执行带显式有符号 int64 溢出检查的加法，越界抛 ``RankInvalidError``。"""
    if delta < INT64_MIN or delta > INT64_MAX:
        raise RankInvalidError(f"increment delta out of int64 range: {delta}")
    if (delta > 0 and old_score > INT64_MAX - delta) or (delta < 0 and old_score < INT64_MIN - delta):
        raise RankInvalidError(f"int64 score increment overflow: {old_score} + {delta}")
    return old_score + delta


def valid_board_key(board: Message | None) -> bool:
    """校验 OpenRankGeneration 使用的完整榜键（period 非空、generation > 0）。"""
    if board is None:
        return False
    period_len = len(board.period.encode("utf-8"))
    return (
        board.board_type > 0
        and RANK_SCOPE_GLOBAL <= board.scope <= RANK_SCOPE_CUSTOM
        and 0 < period_len <= MAX_PERIOD_BYTES
        and board.generation > 0
    )


def valid_lookup_board_key(board: Message | None) -> bool:
    """校验读路径 lookup 使用的榜键（允许 period 为空、generation 为 0 表示查最新）。"""
    if board is None:
        return False
    period_len = len(board.period.encode("utf-8"))
    return (
        board.board_type > 0
        and RANK_SCOPE_GLOBAL <= board.scope <= RANK_SCOPE_CUSTOM
        and period_len <= MAX_PERIOD_BYTES
    )


def valid_account(account: str) -> bool:
    """校验在线原账号是否合法（非空且 UTF-8 长度 <= 191 字节）。"""
    if not account:
        return False
    return len(account.encode("utf-8")) <= MAX_ACCOUNT_BYTES


def reward_mail_window_ms(mail_max_ttl_days: int) -> int:
    """计算封榜到奖励邮件过期的时长（毫秒），范围 ``[2 天, 30 天]``。"""
    if mail_max_ttl_days < MAIL_TTL_DAYS_MIN or mail_max_ttl_days > MAIL_TTL_DAYS_MAX:
        raise ValueError(
            f"rank: mail_max_personal_mail_ttl_days={mail_max_ttl_days} outside allowed range "
            f"{MAIL_TTL_DAYS_MIN}..{MAIL_TTL_DAYS_MAX}"
        )
    window_ms = min(mail_max_ttl_days * DAY_MS, MAX_REWARD_MAIL_WINDOW_MS)
    if window_ms < MIN_REWARD_MAIL_WINDOW_MS:
        raise ValueError(
            f"rank: mail_max_personal_mail_ttl_days={mail_max_ttl_days} gives window {window_ms}ms < "
            f"{MIN_REWARD_MAIL_WINDOW_MS}ms"
        )
    return window_ms


def validate_reward_mail_window_ms(window_ms: int) -> int:
    """校验直接注入的奖励邮件有效期窗口（毫秒）。"""
    if window_ms < MIN_REWARD_MAIL_WINDOW_MS or window_ms > MAX_REWARD_MAIL_WINDOW_MS:
        raise ValueError(
            f"rank: reward_mail_window_ms={window_ms} outside [{MIN_REWARD_MAIL_WINDOW_MS}, {MAX_REWARD_MAIL_WINDOW_MS}]"
        )
    return window_ms


def _has_unknown_fields(message: Message) -> bool:
    """兼容 protobuf upb 后端的未知字段检测（递归覆盖嵌套子消息）。"""
    clone = type(message)()
    clone.CopyFrom(message)
    clone.DiscardUnknownFields()
    return clone.ByteSize() != message.ByteSize()


def validate_attachments(
    attachments: Message | None,
    item_exists: Callable[[int], bool] | None,
    *,
    known_currencies: frozenset[int] = KNOWN_CURRENCY_TYPES,
) -> None:
    """按 Go ``shared/mailgrant.Bundle`` 校验排行榜奖励附件，并要求非空。"""
    if attachments is None:
        raise RankInvalidError("reward tier attachments missing")
    if (
        _has_unknown_fields(attachments)
        or len(attachments.currencies) > MAX_ATTACHMENT_CURRENCIES
        or len(attachments.items) > MAX_ATTACHMENT_ITEMS
    ):
        raise RankInvalidError("reward attachments exceed limits or have unknown fields")
    if len(attachments.currencies) == 0 and len(attachments.items) == 0:
        raise RankInvalidError("reward attachments cannot be empty")

    seen_currencies: set[int] = set()
    for currency in attachments.currencies:
        if (
            currency.currency_type not in known_currencies
            or currency.amount == 0
            or currency.amount > INT64_MAX
            or currency.currency_type in seen_currencies
        ):
            raise RankInvalidError("invalid currency attachment")
        seen_currencies.add(currency.currency_type)

    seen_items: set[int] = set()
    total_items = 0
    for item in attachments.items:
        if (
            item.config_id == 0
            or item.count == 0
            or item.config_id in seen_items
            or item_exists is None
            or not item_exists(int(item.config_id))
        ):
            raise RankInvalidError("invalid item attachment")
        total_items += int(item.count)
        if total_items > MAX_ATTACHMENT_TOTAL_ITEMS:
            raise RankInvalidError("total attachment item count exceeds 4096")
        seen_items.add(item.config_id)


def page_window_for(total: int, page: int, size: int, limit: int) -> PageWindow:
    """计算翻页窗口，名次翻页截断在第 ``limit`` 名为止。"""
    ranked = min(total, limit)
    pages = max((ranked + size - 1) // size, 1)
    selected = min(max(page, 1), pages)
    offset = (selected - 1) * size
    rows = size
    if offset + rows > ranked:
        rows = ranked - offset
    return PageWindow(page=selected, pages=pages, offset=offset, rows=rows)


def position_around(ahead: int, capped: bool, delta: int) -> int:
    """根据本人前方人数与偏移 ``delta`` 推导精确名次；已数满（capped）时返回 0。"""
    if capped:
        return 0
    pos = ahead + 1 + delta
    return pos if pos > 0 else 0


def apply_rank_or_beyond(entry: Message, rank: int, limit: int) -> None:
    """为 ``RankEntry`` 填充名次：超出 ``limit`` 或未数出（0）时置 ``rank=0, beyond_rank_limit=True``。"""
    if rank == 0 or rank > limit:
        entry.rank = 0
        entry.beyond_rank_limit = True
        return
    entry.rank = rank
    entry.beyond_rank_limit = False


def shard_to_decrement(shards: list[ShardRow]) -> int | None:
    """选出移除实体时扣减计数的片号（片号最小且 ``entries > 0``）。"""
    for row in shards:
        if row.entries > 0:
            return row.source_shard
    return None
