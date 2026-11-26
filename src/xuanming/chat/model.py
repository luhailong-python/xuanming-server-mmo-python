"""chat 服务领域常量、不可变数据模型与零 I/O 纯判定函数（与 Go authority/logic 对齐）。"""

from __future__ import annotations

import hashlib
import struct
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cmp_to_key
from typing import Any

from google.protobuf.message import DecodeError

from ..protocol import Protocol
from .errors import (
    ERR_CHANNEL_UNAVAILABLE,
    ERR_INVALID_PARAMETER,
    StoreUnavailableError,
)

# 节点与库表布局常量（与 Go constants.go / schema.go 一致）
CHAT_NODE_TYPE = 9  # common.base.eNodeType.ChatNodeService
DATABASE_NAME = "mmorpg_chat"
WORLD_STREAM_SHARDS = 8
ORIGINAL_RETENTION_DAYS = 30
MAX_HISTORY_PULL_LIMIT = 200
MAX_HISTORY_TTL_SECONDS = (ORIGINAL_RETENTION_DAYS - 1) * 86400
MAX_ACTOR_VERIFY_TTL_SECONDS = 3600
MAX_WORLD_HISTORY_CACHE_MS = 5000
MAX_WORLD_CHANNEL_COOLDOWN_MS = 600_000

# 协议字段与扫描上限
MAX_REQUEST_ID_BYTES = 64
MAX_SYSTEM_NOTICE_ID_BYTES = 64
MAX_SYSTEM_NOTICE_NAME_BYTES = 64
MAX_ACCOUNT_BYTES = 191
MAX_RETRACT_REASON_BYTES = 128
MAX_REPORT_DETAIL_BYTES = 200
MAX_REPORT_REASON = 4
MAX_REPEAT_SCAN_ROWS = 64
MIN_SPAM_NORMALIZED_RUNES = 4
ACCEPTED_STATE = 1
INT64_MAX = (1 << 63) - 1

# 系统全服模板播报枚举（chatpb.SystemWorldNoticeTemplate）
SYSTEM_WORLD_NOTICE_TEMPLATE_UNSPECIFIED = 0
SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_START_SILVER = 1
SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_START_GOLD = 2
SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_WIN_SILVER = 3
SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_WIN_GOLD = 4
SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_DRAW = 5
SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_MILESTONE = 6
SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_BROKEN = 7

# 世界频道 Kafka 推送协议号（与 proto/message_id.txt 对齐）
WORLD_BROADCAST_MSG_ID = 383  # ClientPlayerChatNotifyWorldChat
WORLD_RETRACT_MSG_ID = 435  # ClientPlayerChatNotifyWorldChatRetracted

# 频道枚举（chatpb.ChatChannelType）
CHANNEL_UNSPECIFIED = 0
CHANNEL_WORLD = 1
CHANNEL_TEAM = 2
CHANNEL_PRIVATE = 3
CHANNEL_SYSTEM = 4

# 内容安全枚举（content_guard.proto）
TEXT_SCENE_CHAT_WORLD = 5
TEXT_SCENE_CHAT_PRIVATE = 7
TEXT_VERDICT_UNSPECIFIED = 0
TEXT_VERDICT_PASS = 1
TEXT_VERDICT_MASKED = 2
TEXT_VERDICT_REJECT = 3
TEXT_CATEGORY_AD = 4

# 内部内容审查结论（对应 Go shared/contentguard.Verdict）
REVIEW_PASS = 1
REVIEW_MASKED = 2
REVIEW_REJECT = 3
REVIEW_UNAVAILABLE = 4

WORLD_STREAM_KEYS: tuple[str, ...] = tuple(f"world-v3:{shard}" for shard in range(WORLD_STREAM_SHARDS))


@dataclass(frozen=True, slots=True)
class ActorIdentity:
    """已通过 Gate Ed25519 验签的在线原角色身份。"""

    player_id: int
    account: str
    owner_revision: int


@dataclass(frozen=True, slots=True)
class ChatRules:
    """聊天权威存储与防刷策略参数（与 Go authority.Rules 一致）。"""

    max_content_bytes: int = 512
    rate_limit_per_second: int = 5
    history_max_entries: int = 200
    history_ttl_seconds: int = 604800
    history_default_limit: int = 20
    history_max_limit: int = 50
    actor_verify_ttl_seconds: int = 30
    world_history_cache_ms: int = 500
    world_channel_cooldown_ms: int = 5000
    world_min_level: int = 0
    duplicate_spam_window_ms: int = 0
    duplicate_spam_max_senders: int = 0
    duplicate_spam_max_per_sender: int = 0
    new_account_age_hours: int = 0
    new_account_daily_private_limit: int = 0

    def validate(self) -> None:
        """校验规则边界（与 Go authority.New 保持一致）。"""
        if (
            self.max_content_bytes <= 0
            or self.rate_limit_per_second <= 0
            or self.history_max_entries <= 0
            or self.history_default_limit <= 0
            or self.history_max_limit < self.history_default_limit
            or self.history_max_limit > MAX_HISTORY_PULL_LIMIT
            or self.history_ttl_seconds <= 0
            or self.history_ttl_seconds > MAX_HISTORY_TTL_SECONDS
            or not 0 <= self.actor_verify_ttl_seconds <= MAX_ACTOR_VERIFY_TTL_SECONDS
            or not 0 <= self.world_history_cache_ms <= MAX_WORLD_HISTORY_CACHE_MS
            or not 0 <= self.world_channel_cooldown_ms <= MAX_WORLD_CHANNEL_COOLDOWN_MS
            or self.world_min_level < 0
            or self.duplicate_spam_window_ms < 0
            or self.duplicate_spam_max_senders < 0
            or self.duplicate_spam_max_per_sender < 0
            or self.new_account_age_hours < 0
            or self.new_account_daily_private_limit < 0
        ):
            raise ValueError("invalid chat authority rules")

    def max_pull_rows(self) -> int:
        return min(self.history_max_limit, self.history_max_entries, MAX_HISTORY_PULL_LIMIT)


@dataclass(frozen=True, slots=True)
class PlayerProfile:
    """防刷/等级门槛所需的玩家基础档案。"""

    level: int = 0
    account_created_ms: int = 0


@dataclass(frozen=True, slots=True)
class ReviewResult:
    """正文内容安全送检结果。"""

    verdict: int
    masked: str = ""
    category: int = 0
    pending_platform_review: bool = False


@dataclass(frozen=True, slots=True)
class ReceiptRow:
    """chat_send_authority 持久化收据行。"""

    account: bytes
    revision: int
    digest: bytes
    request: bytes
    message: bytes
    stream: bytes
    state: int
    accepted_ms: int
    tip: int


@dataclass(frozen=True, slots=True)
class HistoryRow:
    """频道历史流中的单条已提交原件行。"""

    sender: int
    request_id: bytes
    payload: bytes
    accepted_ms: int


@dataclass(frozen=True, slots=True)
class SendResult:
    """权威发送结果（与 Go authority.SendResult 对齐）。"""

    tip: int = 0
    duplicate: bool = False
    cooldown: bool = False
    message: Any = None
    pending_platform_review: bool = False
    platform_review_content: str = ""


def valid_account(account: str) -> bool:
    """与 Go accountdirectory.ValidAccount 一致：非空、合法 UTF-8 且 <= 191 字节。"""
    if not isinstance(account, str):
        return False
    try:
        raw = account.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return 0 < len(raw) <= MAX_ACCOUNT_BYTES


def world_stream_key(sender_player_id: int) -> str:
    """返回发送者对应的世界频道分段流键 world-v3:<sender % 8>。"""
    return WORLD_STREAM_KEYS[sender_player_id % WORLD_STREAM_SHARDS]


def stream_key(actor: int, peer: int) -> str:
    """返回世界频道或私聊频道的规范流键。"""
    if peer == 0:
        return world_stream_key(actor)
    left, right = (actor, peer) if actor <= peer else (peer, actor)
    return f"private-v2:{left}:{right}"


def policy_bytes(rules: ChatRules) -> bytes:
    """生成 chat_policy 策略摘要的固定字节序原像（与 Go policyBytes 逐位一致）。"""
    return b"chat-authority-v2" + struct.pack(
        ">9Q",
        rules.max_content_bytes,
        rules.rate_limit_per_second,
        rules.history_max_entries,
        rules.history_ttl_seconds,
        rules.history_default_limit,
        rules.history_max_limit,
        WORLD_STREAM_SHARDS,
        ORIGINAL_RETENTION_DAYS,
        0,  # 历史遗留本地敏感词数量固定写 0，与 Go v2 策略行逐字节兼容
    )


def policy_hash(rules: ChatRules) -> bytes:
    """计算 32 字节策略 SHA-256 摘要。"""
    return hashlib.sha256(policy_bytes(rules)).digest()


def accepted_at_utc(ms: int) -> str:
    """将毫秒时间戳格式化为 UTC DATETIME(3) 字符串（供 TiDB TTL 列使用）。"""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms % 1000:03d}"


def utc_day_ymd(ms: int) -> int:
    """将毫秒时间戳转换为 UTC YYYYMMDD 整数（供新号每日私聊人数统计）。"""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.year * 10000 + dt.month * 100 + dt.day


def validate_receipt(row: ReceiptRow) -> ReceiptRow:
    """核对持久化收据行字段完整性，损坏时 fail-closed 抛出 StoreUnavailableError。"""
    try:
        account_str = row.account.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise StoreUnavailableError("corrupt chat receipt account encoding") from exc
    if (
        not valid_account(account_str)
        or (row.revision == 0 and account_str != "system")
        or len(row.digest) != 32
        or len(row.request) == 0
        or row.state != ACCEPTED_STATE
        or row.accepted_ms == 0
        or row.accepted_ms > INT64_MAX
        or row.tip != 0
        or len(row.message) == 0
        or len(row.stream) == 0
    ):
        raise StoreUnavailableError("corrupt chat authority receipt")
    return row


def sanitize_notice_actor_name(raw: str) -> tuple[str, bool]:
    """清理系统播报参与方展示名：剥离控制字符与换行，要求非空且不超过 64 字节。"""
    if not isinstance(raw, str):
        return "", False
    cleaned = "".join(
        ch for ch in raw if ch not in ("\n", "\r", "\t") and unicodedata.category(ch)[0] != "C"
    ).strip()
    if not cleaned:
        return "", False
    try:
        raw_bytes = cleaned.encode("utf-8")
    except UnicodeEncodeError:
        return "", False
    if len(raw_bytes) > MAX_SYSTEM_NOTICE_NAME_BYTES:
        return "", False
    return cleaned, True


def render_system_world_notice(request: Any) -> tuple[str, bool]:
    """严格按白名单模板渲染全服系统播报正文（绝不接收客户端自由文本）。"""
    if request is None:
        return "", False
    notice_id = str(getattr(request, "notice_id", "") or "").strip()
    if not notice_id:
        return "", False
    try:
        notice_bytes = notice_id.encode("utf-8")
    except UnicodeEncodeError:
        return "", False
    if len(notice_bytes) > MAX_SYSTEM_NOTICE_ID_BYTES:
        return "", False

    actor_a, ok_a = sanitize_notice_actor_name(str(getattr(request, "actor_a_name", "") or ""))
    actor_b, ok_b = sanitize_notice_actor_name(str(getattr(request, "actor_b_name", "") or ""))
    if not ok_a or not ok_b:
        return "", False

    team_size = int(getattr(request, "team_size", 0) or 0) or 1
    if team_size > 5:
        return "", False
    rounds = int(getattr(request, "total_rounds", 0) or 0)
    streak = int(getattr(request, "streak_count", 0) or 0)
    template_id = int(getattr(request, "template_id", 0) or 0)

    if template_id == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_START_SILVER:
        if team_size <= 1:
            return f"【天墉生死擂】{actor_a} 向 {actor_b} 下达江湖战书，点击观战！", True
        return f"【天墉生死擂】{actor_a} 向 {actor_b} 下达江湖战书（{team_size}V{team_size}），点击观战！", True
    if template_id == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_START_GOLD:
        if team_size <= 1:
            return f"【金帖生死状】{actor_a} 向 {actor_b} 掷出金帖生死状，名动天墉，点击观战！", True
        return f"【金帖生死状】{actor_a} 向 {actor_b} 掷出金帖生死状（{team_size}V{team_size}），名动天墉，点击观战！", True
    if template_id == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_WIN_SILVER:
        rounds = rounds or 1
        return f"【天墉生死擂】{actor_a} 历经 {rounds} 回合击败 {actor_b}，略胜一筹！", True
    if template_id == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_WIN_GOLD:
        rounds = rounds or 1
        return f"【金帖生死状】{actor_a} 历经 {rounds} 回合大破 {actor_b}，快意恩仇！", True
    if template_id == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_DRAW:
        rounds = rounds or 1
        return f"【天墉生死擂】{actor_a} 与 {actor_b} 大战 {rounds} 回合难分胜负，各自收剑！", True
    if template_id == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_MILESTONE:
        if streak < 3:
            return "", False
        return f"【宿敌连胜】{actor_a} 再度力克宿敌 {actor_b}，豪取 {streak} 连胜！", True
    if template_id == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_BROKEN:
        if streak < 3:
            return "", False
        return f"【快意雪耻】{actor_a} 终结了宿敌 {actor_b} 的 {streak} 连胜，扬眉吐气！", True
    return "", False


def _is_skip_spam_char(ch: str) -> bool:
    """与 Go unicode.IsSpace / IsPunct / IsSymbol / IsControl 对齐。"""
    if ch.isspace():
        return True
    cat = unicodedata.category(ch)
    return cat[0] in ("Z", "P", "S", "C")


def normalize_spam_hash(content: str) -> bytes | None:
    """忽略空白、标点、符号、控制符与全角/大小写差异计算 32 字节摘要（少于 4 字符返回 None）。"""
    chars: list[str] = []
    for ch in content:
        code = ord(ch)
        if 0xFF21 <= code <= 0xFF3A:  # 'Ａ'..'Ｚ'
            ch = chr(ord("a") + (code - 0xFF21))
        elif 0xFF41 <= code <= 0xFF5A:  # 'ａ'..'ｚ'
            ch = chr(ord("a") + (code - 0xFF41))
        elif 0xFF10 <= code <= 0xFF19:  # '０'..'９'
            ch = chr(ord("0") + (code - 0xFF10))
        elif 0x41 <= code <= 0x5A:  # 'A'..'Z'
            ch = chr(code + 32)
        if _is_skip_spam_char(ch):
            continue
        chars.append(ch)
    if len(chars) < MIN_SPAM_NORMALIZED_RUNES:
        return None
    return hashlib.sha256("".join(chars).encode("utf-8")).digest()


def _compare_history_row_desc(a: HistoryRow, b: HistoryRow) -> int:
    """按 accepted_ms DESC, sender DESC, request_id DESC 排序（与 Go newestRows 一致）。"""
    if a.accepted_ms != b.accepted_ms:
        return -1 if a.accepted_ms > b.accepted_ms else 1
    if a.sender != b.sender:
        return -1 if a.sender > b.sender else 1
    if a.request_id != b.request_id:
        return -1 if a.request_id > b.request_id else 1
    return 0


def newest_rows(rows: list[HistoryRow], limit: int) -> list[HistoryRow]:
    """合并多段世界频道原件行并按新到旧截取前 limit 行。"""
    ordered = sorted(rows, key=cmp_to_key(_compare_history_row_desc))
    return ordered[:limit]


def filter_retracted_rows(rows: list[HistoryRow], retracted: set[tuple[int, int]]) -> list[HistoryRow]:
    """按 (sender_player_id, accepted_ms) 过滤已撤回的世界频道消息。"""
    if not retracted or not rows:
        return rows
    return [row for row in rows if (row.sender, row.accepted_ms) not in retracted]


def visible_history(
    protocol: Protocol,
    rows: list[HistoryRow],
    now_ms: int,
    window_ms: int,
    channel: int,
    actor: int,
    peer: int,
) -> list:
    """从新到旧筛选可见历史并逐行校验原件一致性（与 Go visibleHistory 完全一致）。

    规则：
    - 最新一条距 now_ms 达到 window_ms 则整窗不可见；
    - 相邻两条时间间隔达到 window_ms 则截断后续历史（保留闲置 TTL 语义）；
    - 只校验被遍历到的可见行，截断之后的行不再解析。
    """
    result = []
    newer = 0
    for row in rows:
        if row.accepted_ms == 0 or not row.request_id or row.accepted_ms > INT64_MAX:
            raise StoreUnavailableError("corrupt committed chat history row")
        if newer == 0:
            if now_ms >= row.accepted_ms and now_ms - row.accepted_ms >= window_ms:
                return result
        elif newer >= row.accepted_ms and newer - row.accepted_ms >= window_ms:
            break
        newer = row.accepted_ms
        try:
            msg = protocol.parse("chatpb.ChatMessage", row.payload)
        except DecodeError as exc:
            raise StoreUnavailableError("corrupt chat message payload") from exc
        if (
            int(msg.sender_player_id) != row.sender
            or int(msg.send_time_ms) != row.accepted_ms
            or int(msg.channel) != channel
            or not msg.content.strip()
        ):
            raise StoreUnavailableError("chat display message does not match SQL authority decision")
        if peer != 0 and not (
            (row.sender == actor and int(msg.target_player_id) == peer)
            or (row.sender == peer and int(msg.target_player_id) == actor)
        ):
            raise StoreUnavailableError("private chat row does not belong to authorized partition")
        if peer == 0 and int(msg.target_player_id) != 0:
            raise StoreUnavailableError("world chat row has non-zero target_player_id")
        result.append(msg)
    return result


def channel_accepted(channel: int, self_player_id: int, peer_player_id: int) -> int:
    """校验频道与对端合法性，通过返回 0，否则返回对应 TipInfoMessage 码。"""
    if channel == CHANNEL_WORLD:
        return 0
    if channel == CHANNEL_PRIVATE:
        if peer_player_id == 0 or peer_player_id == self_player_id:
            return ERR_INVALID_PARAMETER
        return 0
    if channel in (CHANNEL_TEAM, CHANNEL_SYSTEM, CHANNEL_UNSPECIFIED):
        return ERR_CHANNEL_UNAVAILABLE
    return ERR_INVALID_PARAMETER


def clamp_history_limit(requested: int, default: int, maximum: int) -> int:
    """收敛历史拉取条数：0 取默认值，超出上限按上限截断。"""
    if requested <= 0:
        return default
    return min(int(requested), maximum)
