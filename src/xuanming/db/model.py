"""PlayerStore 领域常量、写权围栏纯判定与审计发件箱构造。

与 Go 版 ``go/db/internal/playerstore/{decide.go,store.go,audit_outbox.go}`` 保持逐项语义一致，
不包含任何网络或数据库 I/O。
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from enum import IntEnum

from google.protobuf.message import DecodeError

logger = logging.getLogger(__name__)

# common.base.eNodeType.PlayerStoreNodeService
PLAYER_STORE_NODE_TYPE = 31

# 地域存档库编号下界（go/shared/placement/placement.go: FirstStorageID）
FIRST_STORAGE_ID = 1_000_000

# playerstore.PlayerStoreKind 行种定义（1/2/5 为已退役号，永不复用）
OWNER_KIND = 0
KNOWN_DATA_KINDS = frozenset({3, 4, 6, 7, 8, 9})
NEWEST_KNOWN_KIND = 9
MAX_INSTANCE_BYTES = 64
DEFAULT_KNOWN_GEN = 1

# 存盘与认领消息大小预算（单位：字节，与 Go store.go / audit_outbox.go 同值）
MAX_CLAIM_RESPONSE_BYTES = 8 << 20
CLAIM_RESPONSE_OVERHEAD_BYTES = 64 << 10
MAX_SAVE_REQUEST_BYTES = 16 << 20
SAVE_REQUEST_WIRE_LIMIT = MAX_SAVE_REQUEST_BYTES - (64 << 10)
SAVE_REQUEST_OVERHEAD_BYTES = 64 << 10
MAX_AUDIT_BYTES_PER_SAVE = 8 << 20
MAX_AUDIT_SNAPSHOT_BYTES = 4 << 20
MAX_AUDIT_TRANSACTION_LOG_BYTES = 64 << 10

# 审计发件箱分片与批写参数
AUDIT_OUTBOX_TABLE = "player_audit_outbox"
AUDIT_OUTBOX_SHARDS = 16
AUDIT_OUTBOX_INSERT_BATCH = 1000
AUDIT_KIND_TRANSACTION_LOG = 1
AUDIT_KIND_PLAYER_SNAPSHOT = 2

# 建表/验表必备列清单（与 config/db_schema.sql 一致）
REQUIRED_PLAYER_STORE_COLUMNS = (
    "player_id",
    "kind",
    "owner_epoch",
    "save_seq",
    "schema_gen",
    "holder_instance",
    "released",
    "payload",
    "claim_seq",
)
REQUIRED_AUDIT_OUTBOX_COLUMNS = (
    "shard",
    "kind",
    "created_ms",
    "player_id",
    "record_id",
    "save_seq",
    "payload",
)


class SaveResult(IntEnum):
    """对应 ``playerstore.SavePlayerResult`` 枚举值。"""

    UNSPECIFIED = 0
    COMMITTED = 1
    EQUAL = 2
    NO_OWNER = 3
    INVALID = 4
    STALE_OWNER = 5
    NOT_HOLDER = 6
    UNCLAIMED = 7
    OLDER_SEQ = 8
    SCHEMA_REGRESSED = 9
    SCHEMA_TOO_NEW = 10
    TOO_LARGE = 11


class ClaimResult(IntEnum):
    """对应 ``playerstore.LoadAndClaimResult`` 枚举值。"""

    UNSPECIFIED = 0
    CLAIMED = 1
    NO_OWNER = 2
    INVALID = 3
    STALE_OWNER = 4
    NOT_HOLDER = 5
    SCHEMA_TOO_NEW = 6


class StorageUnavailableError(RuntimeError):
    """落点未解析或目标存档库不可用（映射为 gRPC UNAVAILABLE）。"""


class StoreBackendError(RuntimeError):
    """存档库 SQL 执行或提交失败，结局未知（映射为 gRPC UNAVAILABLE）。"""


@dataclass(frozen=True, slots=True)
class SaveLimits:
    """存盘字节预算配置（单位：字节）。"""

    max_row_bytes: int = 1 << 20
    max_request_bytes: int = 3 << 20

    def validate(self) -> None:
        if self.max_row_bytes <= 0 or self.max_request_bytes < self.max_row_bytes:
            raise ValueError(
                f"无效存盘预算: max_row_bytes={self.max_row_bytes}, max_request_bytes={self.max_request_bytes}"
            )
        if self.max_row_bytes * len(KNOWN_DATA_KINDS) + CLAIM_RESPONSE_OVERHEAD_BYTES > MAX_CLAIM_RESPONSE_BYTES:
            raise ValueError(
                f"max_row_bytes={self.max_row_bytes} 乘以 {len(KNOWN_DATA_KINDS)} 行超出认领响应上限 {MAX_CLAIM_RESPONSE_BYTES}"
            )
        if self.max_request_bytes + MAX_AUDIT_BYTES_PER_SAVE + SAVE_REQUEST_OVERHEAD_BYTES > SAVE_REQUEST_WIRE_LIMIT:
            raise ValueError(
                f"max_request_bytes={self.max_request_bytes} 叠加审计预算超出存盘请求上限 {SAVE_REQUEST_WIRE_LIMIT}"
            )


@dataclass(frozen=True, slots=True)
class OwnerRow:
    """写权行（kind=0）参与围栏判定的只读快照。"""

    owner_epoch: int
    save_seq: int
    schema_gen: int
    holder: bytes


@dataclass(frozen=True, slots=True)
class AuditRecord:
    """待写入 ``player_audit_outbox`` 的单条序列化记录。"""

    kind: int
    record_id: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class ClaimOutcome:
    """``LoadAndClaim`` 的仓储层返回结果。"""

    result: ClaimResult
    owner_epoch: int = 0
    save_seq: int = 0
    schema_gen: int = 0
    rows: tuple[tuple[int, bytes], ...] = ()


@dataclass(frozen=True, slots=True)
class SaveOutcome:
    """``SavePlayer`` 的仓储层返回结果。"""

    result: SaveResult
    row_owner_epoch: int = 0
    row_save_seq: int = 0
    audit_outbox_written: bool = False


def storage_db_name(storage_id: int) -> str:
    """校验并返回地域存档库名 ``player_store_{storage_id}_db``。"""
    if not isinstance(storage_id, int) or isinstance(storage_id, bool) or storage_id < FIRST_STORAGE_ID:
        raise ValueError(f"非法地域存档库编号 storage_id={storage_id}（要求 >= {FIRST_STORAGE_ID}）")
    return f"player_store_{storage_id}_db"


def audit_shard_of(player_id: int) -> int:
    """计算玩家在审计发件箱中的分片号。"""
    return player_id % AUDIT_OUTBOX_SHARDS


def validate_claim_request(player_id: int, owner_epoch: int, instance_id: bytes) -> ClaimResult:
    """认领入事务前校验；返回 ``UNSPECIFIED`` 表示通过。"""
    if player_id == 0 or owner_epoch == 0 or not 1 <= len(instance_id) <= MAX_INSTANCE_BYTES:
        return ClaimResult.INVALID
    return ClaimResult.UNSPECIFIED


def validate_save_request(
    player_id: int,
    owner_epoch: int,
    save_seq: int,
    instance_id: bytes,
    rows: Sequence[tuple[int, bytes]],
    limits: SaveLimits,
) -> SaveResult:
    """存盘入事务前校验（与 Go ``validateSave`` 逐条一致）；返回 ``UNSPECIFIED`` 表示通过。"""
    if player_id == 0 or owner_epoch == 0 or save_seq == 0 or not 1 <= len(instance_id) <= MAX_INSTANCE_BYTES:
        return SaveResult.INVALID
    seen: set[int] = set()
    total = 0
    for kind, payload in rows:
        if kind <= OWNER_KIND:
            return SaveResult.INVALID
        if kind not in KNOWN_DATA_KINDS:
            if kind > NEWEST_KNOWN_KIND:
                return SaveResult.SCHEMA_TOO_NEW
            # 1、2、5 为已退役行种号，明确拒绝而非让调用方按版本落后重试。
            return SaveResult.INVALID
        if kind in seen:
            return SaveResult.INVALID
        seen.add(kind)
        size = len(payload)
        if size > limits.max_row_bytes:
            return SaveResult.TOO_LARGE
        total += size
    if total > limits.max_request_bytes:
        return SaveResult.TOO_LARGE
    return SaveResult.UNSPECIFIED


def decide_save(
    row: OwnerRow | None,
    owner_epoch: int,
    save_seq: int,
    schema_gen: int,
    has_rows: bool,
    instance_id: bytes,
    known_gen: int,
) -> SaveResult:
    """存盘写权纯判定（与 Go ``decideSave`` 分支顺序逐行一致）。"""
    if row is None:
        return SaveResult.NO_OWNER
    if owner_epoch < row.owner_epoch:
        return SaveResult.STALE_OWNER
    if row.holder != instance_id:
        if owner_epoch == row.owner_epoch:
            return SaveResult.NOT_HOLDER
        return SaveResult.UNCLAIMED
    if save_seq == row.save_seq:
        return SaveResult.EQUAL
    if save_seq < row.save_seq:
        return SaveResult.OLDER_SEQ
    if has_rows and schema_gen < row.schema_gen:
        return SaveResult.SCHEMA_REGRESSED
    if schema_gen > known_gen:
        return SaveResult.SCHEMA_TOO_NEW
    return SaveResult.COMMITTED


def decide_claim(
    row: OwnerRow | None,
    owner_epoch: int,
    instance_id: bytes,
    binary_schema_gen: int,
    known_gen: int,
) -> ClaimResult:
    """认领写权纯判定（与 Go ``decideClaim`` 分支顺序逐行一致）。"""
    if row is None:
        return ClaimResult.NO_OWNER
    if owner_epoch < row.owner_epoch:
        return ClaimResult.STALE_OWNER
    if owner_epoch == row.owner_epoch and len(row.holder) > 0 and row.holder != instance_id:
        return ClaimResult.NOT_HOLDER
    if row.schema_gen > known_gen or binary_schema_gen > known_gen:
        return ClaimResult.SCHEMA_TOO_NEW
    return ClaimResult.CLAIMED


def prepare_transaction_log_records(player_id: int, save_seq: int, entries: Sequence) -> list[AuditRecord]:
    """将 ``SavePlayerRequest.transaction_logs`` 转为发件箱记录；单条非法或超长仅跳过，不阻断存盘。"""
    records: list[AuditRecord] = []
    for entry in entries:
        tx_id = int(entry.tx_id)
        if tx_id == 0:
            logger.error(
                "audit_outbox skipped transaction log reason=invalid tx_id=0 player_id=%d save_seq=%d",
                player_id,
                save_seq,
            )
            continue
        payload = entry.SerializeToString(deterministic=True)
        if len(payload) > MAX_AUDIT_TRANSACTION_LOG_BYTES:
            logger.error(
                "audit_outbox skipped transaction log reason=too_large tx_id=%d player_id=%d bytes=%d limit=%d",
                tx_id,
                player_id,
                len(payload),
                MAX_AUDIT_TRANSACTION_LOG_BYTES,
            )
            continue
        records.append(AuditRecord(AUDIT_KIND_TRANSACTION_LOG, tx_id, payload))
    return records


def build_audit_snapshot_payload(
    protocol,
    player_id: int,
    schema_gen: int,
    request_entry,
    all_rows: Sequence[tuple[int, bytes]],
    now_sec: int,
) -> tuple[bytes | None, str]:
    """按事务内完整数据行拼接 ``PlayerSnapshotEntry`` 序列化字节（与 Go ``buildAuditSnapshot`` 一致）。"""
    snapshot_id = int(request_entry.snapshot_id)
    req_player_id = int(request_entry.player_id)
    if snapshot_id == 0 or (req_player_id != 0 and req_player_id != player_id):
        return None, "invalid"
    chunks: list[bytes] = []
    for kind, payload in all_rows:
        if kind not in KNOWN_DATA_KINDS:
            return None, "unknown_row_kind"
        chunks.append(payload)
    joined = b"".join(chunks)
    database_cls = protocol.message_class("player_database")
    data = database_cls()
    try:
        data.ParseFromString(joined)
    except DecodeError:
        return None, "undecodable"
    data.player_id = player_id
    data.persist_schema_gen = schema_gen
    blob = data.SerializeToString(deterministic=True)

    entry = type(request_entry)()
    entry.CopyFrom(request_entry)
    entry.player_id = player_id
    entry.snapshot_time = now_sec
    entry.player_database_blob = blob
    # 先按 total_bytes=0 序列化测长，再回填 total_bytes 二次序列化，与 Go/C++ 口径一致。
    entry.total_bytes = 0
    first_pass = entry.SerializeToString()
    entry.total_bytes = len(first_pass)
    payload = entry.SerializeToString()
    if len(payload) > MAX_AUDIT_SNAPSHOT_BYTES:
        return None, "too_large"
    return payload, ""


def prepare_snapshot_records(
    protocol,
    player_id: int,
    save_seq: int,
    schema_gen: int,
    snapshot_requests: Sequence,
    all_rows: Sequence[tuple[int, bytes]],
    now_sec: int,
) -> list[AuditRecord]:
    """组装全部快照发件箱记录；单条异常跳过并记录错误日志，不阻断存盘。"""
    records: list[AuditRecord] = []
    for req_entry in snapshot_requests:
        payload, reason = build_audit_snapshot_payload(
            protocol, player_id, schema_gen, req_entry, all_rows, now_sec
        )
        if reason:
            logger.error(
                "audit_outbox skipped snapshot reason=%s snapshot_id=%d trigger=%d player_id=%d save_seq=%d",
                reason,
                int(req_entry.snapshot_id),
                int(req_entry.trigger),
                player_id,
                save_seq,
            )
            continue
        assert payload is not None
        records.append(AuditRecord(AUDIT_KIND_PLAYER_SNAPSHOT, int(req_entry.snapshot_id), payload))
    return records
