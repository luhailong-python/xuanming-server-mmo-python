"""friend 领域常量、不可变数据模型与零 I/O 纯校验函数（与 Go friend/internal/data 及 shared/rolefence 对齐）。"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from .errors import InvalidDirectoryInputError

# 节点、库名与协议常量
FRIEND_NODE_TYPE = 27  # common.base.eNodeType.FriendNodeService
DATABASE_NAME = "mmorpg_friend"
REQUEST_ID_BIZ_TAG = "friend_request"

# Kafka 推送与事件协议号（与 proto/message_id.txt 及 event_id.txt 对齐）
PUSH_TO_PLAYER_EVENT_ID = 39  # ContractsKafkaPushToPlayerEvent
NOTIFY_FRIEND_EVENT_MSG_ID = 235  # ClientPlayerFriendNotifyFriendEvent

# 好友推送原因枚举（friendpb.FriendEventReason）
FRIEND_EVENT_REASON_UNSPECIFIED = 0
FRIEND_EVENT_REASON_REQUEST_RECEIVED = 1
FRIEND_EVENT_REASON_REQUEST_ACCEPTED = 2

# 落地恢复同步摘要域（scenepb.SyncDomain）
SYNC_DOMAIN_FRIEND_LIST = 1
SYNC_DOMAIN_FRIEND_REQUEST = 2
SYNC_DOMAIN_FRIEND_BLOCK = 3

# 好友申请状态（与 friend.proto FriendRequestStatus 同码）
REQUEST_STATUS_UNKNOWN = 0
REQUEST_STATUS_PENDING = 1
REQUEST_STATUS_ACCEPTED = 2
REQUEST_STATUS_REJECTED = 3

# 玩家会话状态（playerlocator.PlayerSessionState）
SESSION_STATE_ONLINE = 1

# 业务硬上限与扫描窗口常量（与 Go config.go / recommend_repo.go / online_directory.go 一致）
MAX_FRIENDS_CEILING = 300
RECOMMEND_MAX_LIMIT_CEILING = 20
RECOMMEND_ANCHOR_WINDOW = 1024
MAX_ACCOUNT_BYTES = 191
UINT64_MAX = (1 << 64) - 1

DIRECTORY_SCAN_COUNT = 64
DIRECTORY_SCAN_ROUNDS = 4
DIRECTORY_MAX_BATCH = 1024
DIRECTORY_DEFAULT_PAGE = 12
DIRECTORY_MAX_PAGE = 50
DIRECTORY_QUOTA_PER_MINUTE = 60
DIRECTORY_MAX_QUERY_RUNES = 64
DIRECTORY_MAX_CURSOR_BYTES = 48
REQUEST_QUOTA_WINDOW_SEC = 60
SYNC_DIGEST_TTL_SEC = 30 * 24 * 3600

# 外部调用超时预算（秒）
ROLE_FENCE_TIMEOUT_SEC = 2.0
PUSH_TIMEOUT_SEC = 1.5

# 游标指纹常量（与 Go shared/clusterredis/scanpage.go 一致）
STANDALONE_SCAN_DIGEST = "00000000"
_SCAN_DIGEST_RE = re.compile(r"^[0-9a-f]{8}$")
_SCAN_CURSOR_MAX_MASTER = 65535
_SESSION_KEY_PREFIX = "player:session:"
SESSION_KEY_PATTERN = "player:session:{*}"


@dataclass(frozen=True, slots=True)
class ActorIdentity:
    """已通过 Gate Ed25519 验签的在线原角色身份。"""

    player_id: int
    account: str
    owner_revision: int


@dataclass(frozen=True, slots=True)
class FriendRules:
    """好友业务阈值与限流参数（与 Go config.FriendConf 一致）。"""

    max_friends: int = 200
    max_pending_requests: int = 50
    max_incoming_requests: int = 200
    max_blocks: int = 200
    recommend_default_limit: int = 10
    recommend_max_limit: int = 20
    recommend_max_exclude: int = 64
    request_quota_per_minute: int = 10
    list_read_hard_limit: int = 1000
    cache_ttl_sec: int = 1800

    def validate(self) -> None:
        """启动期阈值合法性校验（与 Go config.Validate 对齐）。"""
        for name, value in (
            ("max_friends", self.max_friends),
            ("max_pending_requests", self.max_pending_requests),
            ("max_incoming_requests", self.max_incoming_requests),
            ("max_blocks", self.max_blocks),
            ("recommend_default_limit", self.recommend_default_limit),
            ("recommend_max_limit", self.recommend_max_limit),
            ("recommend_max_exclude", self.recommend_max_exclude),
            ("request_quota_per_minute", self.request_quota_per_minute),
            ("list_read_hard_limit", self.list_read_hard_limit),
            ("cache_ttl_sec", self.cache_ttl_sec),
        ):
            if value <= 0:
                raise ValueError(f"friend.{name} must be positive, got {value}")
        if self.recommend_default_limit > self.recommend_max_limit:
            raise ValueError(
                f"friend.recommend_default_limit ({self.recommend_default_limit}) "
                f"> recommend_max_limit ({self.recommend_max_limit})"
            )
        if self.recommend_max_limit > RECOMMEND_MAX_LIMIT_CEILING:
            raise ValueError(
                f"friend.recommend_max_limit ({self.recommend_max_limit}) exceeds ceiling {RECOMMEND_MAX_LIMIT_CEILING}"
            )
        if self.max_friends > MAX_FRIENDS_CEILING:
            raise ValueError(f"friend.max_friends ({self.max_friends}) exceeds ceiling {MAX_FRIENDS_CEILING}")


@dataclass(frozen=True, slots=True)
class AddFriendLimits:
    """AddFriend 事务的三项上限，按名称区分出站与入站防止传反。"""

    max_friends: int
    max_pending_requests: int
    max_incoming_requests: int


@dataclass(frozen=True, slots=True)
class FriendEntry:
    """好友表持久化视图条目（不含在线状态，避免与缓存耦合）。"""

    friend_player_id: int
    since_ms: int


@dataclass(frozen=True, slots=True)
class FriendRequestEntry:
    """好友申请持久化视图条目。"""

    request_id: int
    from_player_id: int
    to_player_id: int
    request_time_ms: int
    status: int


@dataclass(frozen=True, slots=True)
class BlockEntry:
    """黑名单持久化视图条目。"""

    blocked_player_id: int
    since_ms: int


@dataclass(frozen=True, slots=True)
class RecommendCandidate:
    """普通推荐候选条目。"""

    candidate_player_id: int
    mutual_friends: int


@dataclass(frozen=True, slots=True)
class RequestChange:
    """好友申请写操作提交结果：changed=False 表示幂等重放，不重复推送与抬摘要。"""

    request_id: int = 0
    from_player_id: int = 0
    to_player_id: int = 0
    changed: bool = False


@dataclass(frozen=True, slots=True)
class BlockEffect:
    """拉黑事务对被拉黑方可见状态的实际影响（用于精确决定被动方摘要 bump）。"""

    edge_removed: bool = False
    outbound_pending_rejected: bool = False


@dataclass(frozen=True, slots=True)
class OnlineStatus:
    """玩家会话在线展示态。"""

    online: bool = False
    last_active_ms: int = 0


@dataclass(frozen=True, slots=True)
class PlayerBrief:
    """展示资料摘要（来自 data_service BatchGetPlayerDisplay）。"""

    name: str = ""
    level: int = 0
    class_id: int = 0
    gender: int = 0
    appearance_id: str = ""


@dataclass(frozen=True, slots=True)
class RoleFenceRow:
    """本库 role_fence 副本行。"""

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
    """单名角色的护栏放行条件（owner_revision=0 表示对端，只判冻结与删除）。"""

    player_id: int
    account: str = ""
    owner_revision: int = 0
    allow_token: int = 0


@dataclass(frozen=True, slots=True)
class ScanCursor:
    """跨主节点分页 SCAN 游标（与 Go clusterredis.ScanCursor 线格式一致）。"""

    digest: str = ""
    master: int = 0
    node: int = 0

    @property
    def is_start(self) -> bool:
        return not self.digest and self.master == 0 and self.node == 0

    def encode(self) -> str:
        if self.is_start:
            return ""
        return f"{self.digest}.{self.master}.{self.node}"


@dataclass(frozen=True, slots=True)
class DirectoryCursor:
    """在线目录不透明分页游标：v2:<ScanCursor>:<offset>。"""

    scan: ScanCursor = ScanCursor()
    offset: int = 0


@dataclass(frozen=True, slots=True)
class ScanPageResult:
    """单次 SCAN 分页结果。"""

    keys: tuple[str, ...]
    current: ScanCursor
    next: ScanCursor
    restarted: bool = False


def valid_account(account: str) -> bool:
    """与 Go accountdirectory.ValidAccount 一致：非空、合法 UTF-8 且 <= 191 字节。"""
    if not isinstance(account, str):
        return False
    try:
        raw = account.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return 0 < len(raw) <= MAX_ACCOUNT_BYTES


def valid_role_fence_state(state) -> bool:
    """与 Go rolefence.ValidState 一致。"""
    if state is None:
        return False
    if int(state.player_id) == 0 or int(state.owner_revision) == 0 or int(state.version) == 0 or int(state.home_zone) == 0:
        return False
    if bool(state.freeze_final) and int(state.freeze_token) == 0:
        return False
    if state.account == "":
        return int(state.freeze_token) == 0
    return valid_account(state.account)


def ascending_unique_ids(player_ids: Iterable[int]) -> tuple[int, ...]:
    """规整为升序去重的玩家 ID 元组，作为全局加锁顺序的唯一实现点。"""
    return tuple(sorted(set(int(pid) for pid in player_ids)))


def recommend_limit(requested: int, default_limit: int, max_limit: int) -> int:
    """将客户端请求的推荐条数收敛至配置区间（0 取默认，超上限截断）。"""
    limit = int(requested)
    if limit <= 0:
        limit = default_limit
    if max_limit > 0 and limit > max_limit:
        limit = max_limit
    return limit


def player_session_key(player_id: int) -> str:
    """跨运行时玩家会话契约键：player:session:{<id>}。"""
    return f"{_SESSION_KEY_PREFIX}{{{player_id}}}"


def parse_session_key(key: str) -> int | None:
    """player_session_key 的逆；拒绝前导零、0 或超出 uint64 的键。"""
    if not key.startswith(f"{_SESSION_KEY_PREFIX}{{") or not key.endswith("}"):
        return None
    text = key[len(_SESSION_KEY_PREFIX) + 1 : -1]
    if not text or text[0] == "0" or not text.isascii() or not text.isdigit():
        return None
    value = int(text)
    if value <= 0 or value > UINT64_MAX:
        return None
    return value


def sync_digest_key(player_id: int) -> str:
    """落地同步摘要契约键：player:{<id>}:sync_digest。"""
    return f"player:{{{player_id}}}:sync_digest"


def request_quota_key(player_id: int) -> str:
    """好友申请每分钟配额键（ControlRedis 单槽 hash tag）。"""
    return f"friend:{{rq:{player_id}}}"


def directory_quota_key(player_id: int) -> str:
    """在线目录每分钟分页配额键（ControlRedis 单槽 hash tag）。"""
    return f"friend:{{directory:{player_id}}}"


def friend_list_cache_key(player_id: int) -> str:
    """好友列表私有缓存键（同槽 generation 后缀）。"""
    return f"friend:{{f:{player_id}}}:list:v3"


def pending_requests_cache_key(player_id: int) -> str:
    """待处理申请私有缓存键（同槽 generation 后缀）。"""
    return f"friend:{{f:{player_id}}}:req:v4"


def parse_scan_cursor(raw: str) -> ScanCursor:
    """解析 Go clusterredis.ScanCursor 字符串格式：<digest>.<master>.<node>。"""
    if raw == "":
        return ScanCursor()
    parts = raw.split(".")
    if len(parts) != 3 or not _SCAN_DIGEST_RE.fullmatch(parts[0]):
        raise InvalidDirectoryInputError("invalid scan cursor digest")
    if not parts[1].isascii() or not parts[1].isdigit() or not parts[2].isascii() or not parts[2].isdigit():
        raise InvalidDirectoryInputError("invalid scan cursor numbers")
    master = int(parts[1])
    node = int(parts[2])
    if master < 0 or master > _SCAN_CURSOR_MAX_MASTER or node < 0 or node > UINT64_MAX:
        raise InvalidDirectoryInputError("scan cursor out of range")
    return ScanCursor(digest=parts[0], master=master, node=node)


def parse_directory_cursor(raw: str) -> DirectoryCursor:
    """解析在线目录不透明游标：v2:<ScanCursor>:<offset>。"""
    if raw == "":
        return DirectoryCursor()
    if len(raw.encode("utf-8")) > DIRECTORY_MAX_CURSOR_BYTES:
        raise InvalidDirectoryInputError("directory cursor too long")
    parts = raw.split(":")
    if len(parts) != 3 or parts[0] != "v2":
        raise InvalidDirectoryInputError("directory cursor prefix invalid")
    scan = parse_scan_cursor(parts[1])
    if scan.is_start:
        raise InvalidDirectoryInputError("directory cursor scan cannot be start")
    if not parts[2].isascii() or not parts[2].isdigit():
        raise InvalidDirectoryInputError("directory cursor offset invalid")
    offset = int(parts[2])
    if offset < 0 or offset > DIRECTORY_MAX_BATCH:
        raise InvalidDirectoryInputError("directory cursor offset out of range")
    return DirectoryCursor(scan=scan, offset=offset)


def encode_directory_cursor(scan: ScanCursor, offset: int) -> str:
    """编码在线目录游标；回到起点时返回空串。"""
    if scan.is_start:
        return ""
    return f"v2:{scan.encode()}:{offset}"


def validate_directory_input(cursor: str, query: str) -> DirectoryCursor:
    """在限流计数与依赖访问前校验在线目录游标与搜索词（最长 64 Unicode 字符）。"""
    parsed = parse_directory_cursor(cursor)
    try:
        query.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise InvalidDirectoryInputError("invalid utf-8 query") from exc
    if len(query) > DIRECTORY_MAX_QUERY_RUNES:
        raise InvalidDirectoryInputError("directory query exceeds 64 characters")
    return parsed
