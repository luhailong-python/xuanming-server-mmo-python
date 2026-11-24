"""Python data_service 共享常量、校验器与值对象（对齐 Go ``shared/accountdirectory``、``placement``、``antiaddiction``）。"""

from dataclasses import dataclass
from datetime import date
import re

FIRST_STORAGE_ID = 1_000_000          # placement.FirstStorageID；>= 此值代表玩家存档库 ID
ROUTE_HOME_ZONE_MAX = 999_999         # store.RouteHomeZoneMax
ROUTE_SHARDS = 64                     # placement.RouteRowShards；对应 chk_route_shard
ROUTE_BATCH_LIMIT = 1000              # store.RouteBatchLimit
NAME_BATCH_LIMIT = 500                # store.PlayerNameBatchLimit
MAX_ROLE_BYTES = 16384
MAX_ID_STEP, DEFAULT_ID_STEP = 10_000_000, 100   # idissuer.MaxStep / DefaultStep
INITIAL_OWNER_EPOCH = INITIAL_SAVE_SEQ = 1
SCHEMA_GEN = 1                        # shared/generated/persistschema.Gen
MAX_TRADE_HOLDERS = 256               # accountdirectory.MaxTradeHolders
MAX_APPEARANCE_ID_BYTES = 64          # server.maxAppearanceIDBytes
MAX_UINT64 = (1 << 64) - 1
REMOVAL_TOKEN_BIT = 1 << 63           # rolefence.RemovalToken 最高位标识删角准备令牌

# 账号注销状态与常量（account_deletion.go）
DELETION_NONE, DELETION_PENDING, DELETION_DELETING = 0, 1, 2
DELETION_SOURCE_USER, DELETION_SOURCE_ADMIN = 1, 2
DELETION_OUTCOME_OK, DELETION_OUTCOME_NOT_FOUND = 1, 2
DELETION_OUTCOME_BLOCKED, DELETION_OUTCOME_RETRY, DELETION_OUTCOME_TOO_LATE = 3, 4, 5
MAX_DELETION_CLAIM = 32
DELETION_LEASE_MS = 120_000
MAX_DELETION_COOLING_OFF_MS = 30 * 86400 * 1000
DEFAULT_DELETION_COOLING_OFF_MS = 7 * 86400 * 1000

# 角色交易状态与证据类型（data_service.proto）
TRADE_FREEZING = 1
TRADE_FROZEN = 2
TRADE_CANCELLED = 3
TRADE_TRANSFERRED = 4
TRADE_CANCEL_PENDING = 5
TRADE_PREPARING = 6

EVIDENCE_NO_ADMISSION = 1
EVIDENCE_SCENE = 2
EVIDENCE_INITIAL_COMMIT = 3

# 角色改名结果与内部记录状态（data_service.proto / account_role_rename.go）
RENAME_OK = 1
RENAME_TAKEN = 2
RENAME_INVALID = 3
RENAME_COOLDOWN = 4
RENAME_FROZEN = 5
RENAME_STALE_SEQ = 6
RENAME_SAME_NAME = 7
RENAME_NOT_FOUND = 8
RENAME_EXPIRED = 9
RENAME_BUSY = 10

RENAME_STATE_PENDING = 1
RENAME_STATE_COMMITTED = 2
RENAME_STATE_SETTLED = 3
RENAME_STATE_REJECTED_DIRTY = 4
RENAME_STATE_REJECTED = 5

# GM 批量回收状态与常量（rollback_database_table.proto / recall_store.go）
RECALL_KIND_CURRENCY = 1
RECALL_KIND_ITEM = 2
RECALL_BATCH_RUNNING = 1
RECALL_BATCH_FINISHED = 2
RECALL_OP_PENDING = 1
RECALL_OP_APPLIED = 2
RECALL_OP_REJECTED = 3
RECALL_OP_ABORTED = 4
RECALL_OP_PARTIAL = 5
RECALL_OP_PLANNED = 6
RECALL_OP_CANCELLED = 7
RECALL_OP_ROLE_GONE = 8

# constants/error_codes.go
ERR_NOT_FOUND = 4
ERR_SNAPSHOT_DB = 11
ERR_INVALID_REQUEST = 14
ERR_RESULT_TRUNCATED = 17
ERR_ID_SOURCE, ERR_ID_EXHAUSTED, ERR_ID_UNKNOWN_TAG = 18, 19, 20
ERR_ZONE_CONFLICT, ERR_ADMIN_AUTH, ERR_NAME_DB, ERR_NAME_CONFLICT = 21, 23, 25, 26
ERR_ROUTE_STORE = 30
ERR_RECALL_REQUEST_CONFLICT = 31
ERR_RECALL_PLAN_CHANGED = 32
ERR_RECALL_DISABLED = 33
ERR_RECALL_NOT_FOUND = 34
ERR_RECALL_PENDING = 35
ERR_RECALL_INCOMPLETE = 36
ERR_RECALL_NOTHING_TO_RECALL = 37
ERR_NAME_IN_USE = 38
ERR_RECALL_EVIDENCE_REFUSED = 39
ERR_RECALL_OP_CHANGED = 40

# config.DefaultIdSegmentTags（data_service.yaml IdSegment.Tags）
DEFAULT_ID_TAGS = (
    "player", "guild", "item", "txlog", "snapshot", "trade_listing", "guild_asset_op", "friend_request",
    "trade_asset_op", "mail", "mail_asset_op", "rank_board", "rank_settlement", "rank_reward", "battle",
    "snapshot_row", "rollback_audit", "gm_recall_op",
)

_TAG = re.compile(r"[a-z0-9_]{1,64}")
_INSTANCE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_PI_DIGITS = "0123456789abcdefghijklmnop"
_PI_CHARS = re.compile(r"[0-9a-z]{38}")


@dataclass(frozen=True, slots=True)
class Ownership:
    account: str
    role: bytes  # 序列化的 AccountSimplePlayer
    owner_revision: int
    trade_listing_id: int = 0
    prepare_token: int = 0
    initial_player_commit: bool = False


@dataclass(frozen=True, slots=True)
class AccountDirectoryRead:
    """账号目录同一次读返回的列表、注销生命周期与实名 pi。"""
    roles: list[bytes]
    deletion_state: int = 0
    deletion_execute_after_ms: int = 0
    previous_deleted_ms: int = 0
    real_name_pi: str = ""


def valid_account(account: str) -> bool:
    try:
        return 0 < len(account.encode()) <= 191
    except UnicodeEncodeError:
        return False


def valid_pi(pi: str) -> bool:
    """校验国家防沉迷 pi 格式与前 6 位 26 进制出生日期段（对齐 Go antiaddiction.ValidPI）。"""
    if _PI_CHARS.fullmatch(pi) is None:
        return False
    value = 0
    for ch in pi[:6]:
        digit = _PI_DIGITS.find(ch)
        if digit < 0:
            return False
        value = value * 26 + digit
    year, month, day = value // 10000, (value // 100) % 100, value % 100
    if year < 1900:
        return False
    try:
        date(year, month, day)
    except ValueError:
        return False
    return True


def valid_tag(tag: str) -> bool:
    return _TAG.fullmatch(tag) is not None


def valid_instance(instance: str) -> bool:
    return _INSTANCE.fullmatch(instance) is not None


def valid_save_floor(epoch: int, seq: int) -> bool:
    return (epoch == 0) == (seq == 0)


def save_floor_after(epoch: int, seq: int, old_epoch: int, old_seq: int) -> bool:
    return epoch > old_epoch or (epoch == old_epoch and seq > old_seq)


def removal_token(player_id: int) -> int:
    return REMOVAL_TOKEN_BIT | int(player_id)


def is_removal_token(token: int) -> bool:
    return (int(token) & REMOVAL_TOKEN_BIT) != 0


def valid_trade_tuple(listing_id: int, player_id: int, seller_account: str, revision: int) -> bool:
    return (listing_id > 0 and not is_removal_token(listing_id) and player_id > 0
            and revision > 0 and valid_account(seller_account))


def _valid_node(node: str) -> bool:
    return 0 < len(node.encode()) <= 96 and not any(char in node for char in "\x00\r\n\t ")


def valid_admission(identity) -> bool:
    i = identity
    return (i.player_id > 0 and valid_instance(i.scene_instance_id) and i.generation > 0 and i.owner_revision > 0
            and i.owner_epoch > 0 and _valid_node(i.scene_node_id) and i.scene_id > 0 and i.zone_id > 0
            and i.session_id > 0 and i.login_generation > 0)


def valid_trade_seal(seal) -> bool:
    s = seal
    return (s is not None and s.listing_id > 0 and not is_removal_token(s.listing_id) and s.player_id > 0
            and s.owner_revision > 0 and valid_instance(s.scene_instance_id) and s.through_generation > 0
            and _valid_node(s.scene_node_id) and s.zone_id > 0)


def valid_freeze_evidence(evidence) -> bool:
    if (evidence is None or not evidence.player_id or not evidence.listing_id
            or not evidence.owner_revision or len(evidence.persisted_digest) != 32):
        return False
    if evidence.kind == EVIDENCE_NO_ADMISSION:
        return evidence.owner_epoch == 0 and evidence.save_seq == 0
    if evidence.kind == EVIDENCE_SCENE:
        return (evidence.owner_epoch > 0 and evidence.save_seq > 0
                and not (evidence.owner_epoch == INITIAL_OWNER_EPOCH and evidence.save_seq == INITIAL_SAVE_SEQ))
    if evidence.kind == EVIDENCE_INITIAL_COMMIT:
        return evidence.owner_epoch == INITIAL_OWNER_EPOCH and evidence.save_seq == INITIAL_SAVE_SEQ
    return False


def no_actor_freeze_evidence(evidence) -> bool:
    return evidence is not None and evidence.kind in (EVIDENCE_NO_ADMISSION, EVIDENCE_INITIAL_COMMIT)


class MergedZones:
    """合服映射快照（src -> dst）；``final`` 沿合服链解析并内置环保护。"""

    def __init__(self, mapping: dict[int, int]) -> None:
        self.mapping = mapping

    def final(self, zone: int) -> int:
        for _ in range(len(self.mapping) + 1):  # 上界同时阻断损坏环
            if zone not in self.mapping:
                return zone
            zone = self.mapping[zone]
        return zone
