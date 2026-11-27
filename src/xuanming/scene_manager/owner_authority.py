"""线性一致的玩家归属权威状态机、重连协议与 scene_manager 桥接层。

对照 Go ``go/shared/ownerauthority``（``ownerauthority.go``、``resume.go``）与
``go/scene_manager/internal/logic/owner_authority_bridge.go`` 实现。
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import Protocol as TypingProtocol

logger = logging.getLogger(__name__)

# ── 常量与 DDL（与 Go ownerauthority.go 逐值对齐） ─────────────────────────────

DEFAULT_SKEW_MARGIN_MS = 2_000
MAX_LEASE_SECONDS = 30
MAX_DETAIL_LEN = 512
DEFAULT_RESUME_RETRY_AFTER_MS = 1_000

DDL_MYSQL = """
CREATE TABLE IF NOT EXISTS owner_record (
    player_id          BIGINT UNSIGNED NOT NULL,
    owner_epoch        BIGINT UNSIGNED NOT NULL,
    owner_type         TINYINT         NOT NULL DEFAULT 0,
    phase              TINYINT         NOT NULL DEFAULT 0,
    pod_name           VARCHAR(128)    NOT NULL DEFAULT '',
    instance_uid       VARCHAR(128)    NOT NULL DEFAULT '',
    instance_epoch     INT UNSIGNED    NOT NULL DEFAULT 0,
    assignment_id      VARCHAR(128)    NOT NULL DEFAULT '',
    release_track      VARCHAR(64)     NOT NULL DEFAULT '',
    operation_id       CHAR(36)        NOT NULL DEFAULT '',
    admit_not_before_ms BIGINT         NOT NULL DEFAULT 0,
    source_revision    BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at_ms      BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (player_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS owner_instance_lease (
    pod_name          VARCHAR(128) NOT NULL,
    instance_uid      VARCHAR(128) NOT NULL,
    instance_epoch    INT UNSIGNED NOT NULL,
    lease_deadline_ms BIGINT       NOT NULL,
    updated_at_ms     BIGINT       NOT NULL,
    PRIMARY KEY (pod_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS owner_transition_log (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    player_id     BIGINT UNSIGNED NOT NULL,
    owner_epoch   BIGINT UNSIGNED NOT NULL,
    operation_id  CHAR(36)        NOT NULL,
    action        VARCHAR(32)     NOT NULL,
    detail        VARCHAR(512)    NOT NULL DEFAULT '',
    created_at_ms BIGINT          NOT NULL,
    PRIMARY KEY (id),
    KEY idx_player_epoch (player_id, owner_epoch),
    KEY idx_created_at (created_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
"""

DDL_TIDB = """
CREATE TABLE IF NOT EXISTS owner_record (
    player_id          BIGINT UNSIGNED NOT NULL,
    owner_epoch        BIGINT UNSIGNED NOT NULL,
    owner_type         TINYINT         NOT NULL DEFAULT 0,
    phase              TINYINT         NOT NULL DEFAULT 0,
    pod_name           VARCHAR(128)    NOT NULL DEFAULT '',
    instance_uid       VARCHAR(128)    NOT NULL DEFAULT '',
    instance_epoch     INT UNSIGNED    NOT NULL DEFAULT 0,
    assignment_id      VARCHAR(128)    NOT NULL DEFAULT '',
    release_track      VARCHAR(64)     NOT NULL DEFAULT '',
    operation_id       CHAR(36)        NOT NULL DEFAULT '',
    admit_not_before_ms BIGINT         NOT NULL DEFAULT 0,
    source_revision    BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at_ms      BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (player_id) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4;

CREATE TABLE IF NOT EXISTS owner_instance_lease (
    pod_name          VARCHAR(128) NOT NULL,
    instance_uid      VARCHAR(128) NOT NULL,
    instance_epoch    INT UNSIGNED NOT NULL,
    lease_deadline_ms BIGINT       NOT NULL,
    updated_at_ms     BIGINT       NOT NULL,
    PRIMARY KEY (pod_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS owner_transition_log (
    id            BIGINT UNSIGNED NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,
    player_id     BIGINT UNSIGNED NOT NULL,
    owner_epoch   BIGINT UNSIGNED NOT NULL,
    operation_id  CHAR(36)        NOT NULL,
    action        VARCHAR(32)     NOT NULL,
    detail        VARCHAR(512)    NOT NULL DEFAULT '',
    created_at_ms BIGINT          NOT NULL,
    PRIMARY KEY (id),
    KEY idx_player_epoch (player_id, owner_epoch),
    KEY idx_created_at (created_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
"""


# ── 枚举与数据结构 ─────────────────────────────────────────────────────────────

class OwnerType(IntEnum):
    """玩家当前归属的权威运行域分类。"""

    NONE = 0
    HUB = 1
    INSTANCE = 2


class Phase(IntEnum):
    """单个归属纪元（owner_epoch）的流转阶段。"""

    NONE = 0
    PENDING = 1
    ADMITTED = 2


class EntryState(IntEnum):
    """面向客户端的最小重连状态机输出。"""

    WAIT = 0
    TARGET = 1
    ROLE_REQUIRED = 2


class WaitReason(IntEnum):
    """客户端需在原入口等待重试的具体原因。"""

    NONE = 0
    AUTHORITY_UNAVAILABLE = 1
    BARRIER_PENDING = 2
    TRANSITION_IN_PROGRESS = 3
    NO_OWNER_ASSIGNED = 4


@dataclass(frozen=True, slots=True)
class OwnerTarget:
    """精确 5 元组目标承载者标识。"""

    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    assignment_id: str = ""
    release_track: str = ""

    def complete(self) -> bool:
        return bool(self.pod_name and self.instance_uid and self.instance_epoch > 0 and self.assignment_id)

    def equal(self, other: OwnerTarget) -> bool:
        return self == other

    def same_instance(self, other: OwnerTarget) -> bool:
        return (
            self.pod_name == other.pod_name
            and self.instance_uid == other.instance_uid
            and self.instance_epoch == other.instance_epoch
        )


@dataclass(frozen=True, slots=True)
class OwnerRecord:
    """单个玩家的权威归属记录。"""

    player_id: int = 0
    owner_epoch: int = 0
    owner_type: OwnerType = OwnerType.NONE
    phase: Phase = Phase.NONE
    target: OwnerTarget = OwnerTarget()
    operation_id: str = ""
    admit_not_before_ms: int = 0
    source_revision: int = 0
    updated_at_ms: int = 0


@dataclass(frozen=True, slots=True)
class BeginRequest:
    """请求通过 CAS 发起向新承载者的归属交接。"""

    player_id: int
    expect_epoch: int
    target_type: OwnerType
    target: OwnerTarget
    operation_id: str = ""
    source_revision: int = 0
    detail: str = ""


@dataclass(frozen=True, slots=True)
class AdmitRequest:
    """请求将匹配的 PENDING 纪元提升为 ADMITTED。"""

    player_id: int
    owner_epoch: int
    operation_id: str
    target: OwnerTarget


@dataclass(frozen=True, slots=True)
class RenewLeaseRequest:
    """续约指定运行实例的租约截止时间。"""

    pod_name: str
    instance_uid: str
    instance_epoch: int
    lease_seconds: int


@dataclass(frozen=True, slots=True)
class ReleaseRequest:
    """仅当 (owner_epoch, operation_id) 精确匹配当前活跃行时释放归属。"""

    player_id: int
    owner_epoch: int
    operation_id: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class TransitionLogEntry:
    """权威状态变迁审计日志条目。"""

    player_id: int
    owner_epoch: int
    operation_id: str
    action: str
    detail: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class InstanceLease:
    """实例租约行。"""

    pod_name: str
    instance_uid: str
    instance_epoch: int
    lease_deadline_ms: int
    updated_at_ms: int


# ── 错误层次 ───────────────────────────────────────────────────────────────────

class OwnerAuthorityError(RuntimeError):
    """OwnerAuthority 领域基础异常。"""


class EpochConflictError(OwnerAuthorityError):
    """CAS expect_epoch 冲突，携带当前权威记录。"""

    def __init__(self, current: OwnerRecord | None = None) -> None:
        self.current = current if current is not None else OwnerRecord()
        super().__init__(f"ownerauthority: expect_epoch conflict: current_epoch={self.current.owner_epoch}")


class BarrierNotOpenError(OwnerAuthorityError):
    """跨实例交接隔离屏障尚未到达 admit_not_before_ms。"""

    def __init__(self, admit_not_before_ms: int, retry_after_ms: int) -> None:
        self.admit_not_before_ms = int(admit_not_before_ms)
        self.retry_after_ms = int(retry_after_ms)
        super().__init__(
            f"ownerauthority: admit barrier not open: "
            f"admit_not_before_ms={self.admit_not_before_ms} retry_after_ms={self.retry_after_ms}"
        )


class IdentityMismatchError(OwnerAuthorityError):
    """目标五元组或 operation_id 身份不匹配。"""


class LeaseRegressedError(OwnerAuthorityError):
    """实例租约纪元或截止时间发生回退。"""


class InvalidOperationError(OwnerAuthorityError):
    """非法请求参数（如 player_id=0、目标不完整、非 UUIDv4 等）。"""


class SourceRevisionStaleError(OwnerAuthorityError):
    """source_revision 落后于已记录版本或同版本跨目标变更。"""


# ── 配置与校验（对应 Go config.OwnerAuthorityConfig） ─────────────────────────

def parse_sql_backend(raw: str) -> str:
    """严格解析 SQL 后端档位：空串/tidb -> tidb，mysql -> mysql，其余立即报错。"""
    if raw in ("", "tidb"):
        return "tidb"
    if raw == "mysql":
        return "mysql"
    raise ValueError(f"invalid authority sql backend (want tidb or mysql): {raw!r}")


@dataclass(frozen=True, slots=True)
class OwnerAuthorityConfig:
    """scene_manager 可选线性一致 SQL 归属权威配置。

    ``dsn`` 与 ``dsn_env`` 均为空时自动停用，保持 100% 向后兼容。
    """

    mode: str = ""  # "" / "shadow" / "enforce"
    dsn: str = ""
    dsn_env: str = ""
    database: str = ""
    backend: str = ""  # "" / "tidb" / "mysql"
    max_open_conns: int = 0
    skew_margin_ms: int = 0

    def enabled(self) -> bool:
        return bool(self.dsn or self.dsn_env)

    def effective_mode(self) -> str:
        if not self.enabled():
            return ""
        return "enforce" if self.mode == "enforce" else "shadow"

    def resolve_dsn(self, lookup: Callable[[str], str | None] | None = None) -> str:
        """校验全部字段并按单一来源规则解析 DSN（禁止同时配置 dsn 与 dsn_env）。"""
        if not self.enabled():
            return ""
        if self.mode not in ("", "shadow", "enforce"):
            raise ValueError(f"OwnerAuthority.mode must be empty, shadow, or enforce, got {self.mode!r}")
        # r2-oa#1：在实例租约续期与准入屏障做实之前拒绝 enforce 启服，防止产生无时间保护的额外否决点。
        if self.mode == "enforce":
            raise ValueError(
                "OwnerAuthority.Mode=enforce is refused until the admit barrier is implemented: "
                "no process renews owner_instance_lease and the scene target identity is a reusable "
                "node id with a constant instance epoch, so admit_not_before is always now; use shadow"
            )
        parse_sql_backend(self.backend)
        if self.skew_margin_ms < 0:
            raise ValueError(f"OwnerAuthority.skew_margin_ms must be >= 0, got {self.skew_margin_ms}")
        if self.dsn and self.dsn_env:
            raise ValueError("OwnerAuthority.dsn and OwnerAuthority.dsn_env cannot both be set")
        if self.dsn_env:
            getter = os.environ.get if lookup is None else lookup
            resolved = getter(self.dsn_env)
            if not resolved:
                raise ValueError(f"OwnerAuthority.dsn_env {self.dsn_env!r} is not set")
            return resolved
        return self.dsn


# ── 重连收敛协议（对照 Go resume.go） ──────────────────────────────────────────

def clamp_retry_ms(delta_ms: int) -> int:
    if delta_ms <= 0:
        return 1
    if delta_ms > 60_000:
        return 60_000
    return int(delta_ms)


@dataclass(frozen=True, slots=True)
class ResumeInput:
    """重连路由裁决所需的权威事实集合。"""

    query_err: BaseException | None = None
    has_role: bool = False
    found: bool = False
    record: OwnerRecord = OwnerRecord()
    now_ms: int = 0
    default_retry_after_ms: int = 0


@dataclass(frozen=True, slots=True)
class ResumeDecision:
    """resolve_resume 的确定性输出。"""

    state: EntryState
    wait_reason: WaitReason
    retry_after_ms: int = 0
    record: OwnerRecord = OwnerRecord()


def resolve_resume(inp: ResumeInput) -> ResumeDecision:
    """基于权威归属记录确定性收敛重连状态，绝不依赖本地猜测路由。"""
    default_retry = inp.default_retry_after_ms if inp.default_retry_after_ms > 0 else DEFAULT_RESUME_RETRY_AFTER_MS
    if inp.query_err is not None:
        return ResumeDecision(
            state=EntryState.WAIT,
            wait_reason=WaitReason.AUTHORITY_UNAVAILABLE,
            retry_after_ms=default_retry,
        )
    if not inp.has_role:
        return ResumeDecision(state=EntryState.ROLE_REQUIRED, wait_reason=WaitReason.NONE)
    if not inp.found or inp.record.phase == Phase.NONE or inp.record.owner_type == OwnerType.NONE:
        return ResumeDecision(
            state=EntryState.WAIT,
            wait_reason=WaitReason.NO_OWNER_ASSIGNED,
            retry_after_ms=default_retry,
        )
    if inp.record.phase == Phase.PENDING:
        if inp.now_ms < inp.record.admit_not_before_ms:
            return ResumeDecision(
                state=EntryState.WAIT,
                wait_reason=WaitReason.BARRIER_PENDING,
                retry_after_ms=clamp_retry_ms(inp.record.admit_not_before_ms - inp.now_ms),
                record=inp.record,
            )
        return ResumeDecision(
            state=EntryState.WAIT,
            wait_reason=WaitReason.TRANSITION_IN_PROGRESS,
            retry_after_ms=default_retry,
            record=inp.record,
        )
    if inp.record.phase == Phase.ADMITTED:
        return ResumeDecision(
            state=EntryState.TARGET,
            wait_reason=WaitReason.NONE,
            record=inp.record,
        )
    return ResumeDecision(
        state=EntryState.WAIT,
        wait_reason=WaitReason.AUTHORITY_UNAVAILABLE,
        retry_after_ms=default_retry,
    )


# ── 存储协议与内存/SQL 线性一致实现 ────────────────────────────────────────────

def _parse_uuid4(raw: str) -> str:
    text = raw.strip()
    try:
        parsed = uuid.UUID(text)
    except ValueError as exc:
        raise InvalidOperationError("ownerauthority: invalid operation_id uuid") from exc
    if parsed.version != 4:
        raise InvalidOperationError("ownerauthority: operation_id must be uuid v4")
    return str(parsed)


def _truncate_detail(detail: str) -> str:
    if len(detail) > MAX_DETAIL_LEN:
        return detail[:MAX_DETAIL_LEN]
    return detail


class OwnerAuthorityStore(TypingProtocol):
    """线性一致的玩家归属权威存储协议。"""

    async def query(self, player_id: int) -> tuple[OwnerRecord, bool]: ...
    async def begin_transition(self, req: BeginRequest) -> OwnerRecord: ...
    async def admit(self, req: AdmitRequest) -> OwnerRecord: ...
    async def renew_instance_lease(self, req: RenewLeaseRequest) -> int: ...
    async def release(self, req: ReleaseRequest) -> tuple[OwnerRecord, bool]: ...


class MemoryOwnerAuthorityStore:
    """线性一致内存权威存储，严格执行 Go ``Store`` 全部 CAS、围栏与租约不变量。"""

    def __init__(
        self,
        *,
        skew_margin_ms: int = DEFAULT_SKEW_MARGIN_MS,
        clock_ms: Callable[[], int] | None = None,
        new_uuid: Callable[[], str] | None = None,
    ) -> None:
        self.skew_margin_ms = skew_margin_ms if skew_margin_ms > 0 else DEFAULT_SKEW_MARGIN_MS
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._new_uuid = new_uuid or (lambda: str(uuid.uuid4()))
        self._lock = asyncio.Lock()
        self.records: dict[int, OwnerRecord] = {}
        self.leases: dict[str, InstanceLease] = {}
        self.logs: list[TransitionLogEntry] = []

    async def query(self, player_id: int) -> tuple[OwnerRecord, bool]:
        if player_id <= 0:
            raise InvalidOperationError("ownerauthority: player_id must be positive")
        async with self._lock:
            rec = self.records.get(player_id)
            if rec is None:
                return OwnerRecord(), False
            return rec, True

    async def begin_transition(self, req: BeginRequest) -> OwnerRecord:
        if req.player_id <= 0 or req.target_type not in (OwnerType.HUB, OwnerType.INSTANCE) or not req.target.complete():
            raise InvalidOperationError("ownerauthority: invalid operation or target")
        op_id = req.operation_id.strip()
        op_id = self._new_uuid() if not op_id else _parse_uuid4(op_id)

        async with self._lock:
            cur = self.records.get(req.player_id)
            found = cur is not None
            if found:
                assert cur is not None
                # 1. operation_id 幂等重放检测
                if cur.operation_id == op_id:
                    if cur.owner_type == req.target_type and cur.target.equal(req.target):
                        return cur
                    raise IdentityMismatchError("ownerauthority: target or operation identity mismatch")
                # 2. 已归属至完全相同五元组目标时的幂等无操作
                if (
                    cur.phase != Phase.NONE
                    and cur.owner_type == req.target_type
                    and cur.target.equal(req.target)
                    and (req.source_revision == 0 or req.source_revision == cur.source_revision)
                ):
                    return cur
                # 3. expect_epoch CAS 校验
                if cur.owner_epoch != req.expect_epoch:
                    raise EpochConflictError(cur)
                # 4. source_revision 单调性校验
                if cur.source_revision > 0:
                    if req.source_revision == 0 or req.source_revision < cur.source_revision:
                        raise SourceRevisionStaleError("ownerauthority: source revision stale")
                    if req.source_revision == cur.source_revision and not cur.target.equal(req.target):
                        raise SourceRevisionStaleError("ownerauthority: source revision stale")
            elif req.expect_epoch != 0:
                raise EpochConflictError(OwnerRecord())

            now_ms = int(self._clock_ms())
            admit_not_before_ms = now_ms
            # 跨物理实例交接：按固定第二锁顺序读取旧承载者租约并叠加时钟偏斜余量
            if (
                found
                and cur is not None
                and cur.phase != Phase.NONE
                and cur.target.pod_name
                and not cur.target.same_instance(req.target)
            ):
                lease = self.leases.get(cur.target.pod_name)
                if (
                    lease is not None
                    and lease.instance_uid == cur.target.instance_uid
                    and lease.instance_epoch == cur.target.instance_epoch
                ):
                    barrier = lease.lease_deadline_ms + self.skew_margin_ms
                    if barrier > admit_not_before_ms:
                        admit_not_before_ms = barrier

            prev_epoch = cur.owner_epoch if cur is not None else 0
            prev_source_rev = cur.source_revision if cur is not None else 0
            next_source_rev = max(prev_source_rev, req.source_revision)
            next_rec = OwnerRecord(
                player_id=req.player_id,
                owner_epoch=prev_epoch + 1,
                owner_type=req.target_type,
                phase=Phase.PENDING,
                target=req.target,
                operation_id=op_id,
                admit_not_before_ms=admit_not_before_ms,
                source_revision=next_source_rev,
                updated_at_ms=now_ms,
            )
            self.records[req.player_id] = next_rec
            self.logs.append(
                TransitionLogEntry(
                    player_id=next_rec.player_id,
                    owner_epoch=next_rec.owner_epoch,
                    operation_id=next_rec.operation_id,
                    action="begin",
                    detail=_truncate_detail(req.detail),
                    created_at_ms=now_ms,
                )
            )
            return next_rec

    async def admit(self, req: AdmitRequest) -> OwnerRecord:
        if req.player_id <= 0 or req.owner_epoch <= 0 or not req.target.complete():
            raise InvalidOperationError("ownerauthority: invalid operation or target")
        op_id = _parse_uuid4(req.operation_id)

        async with self._lock:
            cur = self.records.get(req.player_id)
            if (
                cur is None
                or cur.owner_epoch != req.owner_epoch
                or cur.operation_id != op_id
                or not cur.target.equal(req.target)
            ):
                raise IdentityMismatchError("ownerauthority: target or operation identity mismatch")
            if cur.phase == Phase.ADMITTED:
                return cur
            if cur.phase != Phase.PENDING:
                raise IdentityMismatchError("ownerauthority: target or operation identity mismatch")
            now_ms = int(self._clock_ms())
            if now_ms < cur.admit_not_before_ms:
                raise BarrierNotOpenError(cur.admit_not_before_ms, clamp_retry_ms(cur.admit_not_before_ms - now_ms))
            admitted = OwnerRecord(
                player_id=cur.player_id,
                owner_epoch=cur.owner_epoch,
                owner_type=cur.owner_type,
                phase=Phase.ADMITTED,
                target=cur.target,
                operation_id=cur.operation_id,
                admit_not_before_ms=cur.admit_not_before_ms,
                source_revision=cur.source_revision,
                updated_at_ms=now_ms,
            )
            self.records[req.player_id] = admitted
            self.logs.append(
                TransitionLogEntry(
                    player_id=admitted.player_id,
                    owner_epoch=admitted.owner_epoch,
                    operation_id=admitted.operation_id,
                    action="admit",
                    detail="",
                    created_at_ms=now_ms,
                )
            )
            return admitted

    async def renew_instance_lease(self, req: RenewLeaseRequest) -> int:
        if not req.pod_name or not req.instance_uid or req.instance_epoch <= 0 or req.lease_seconds <= 0:
            raise InvalidOperationError("ownerauthority: invalid lease renew request")
        lease_sec = min(req.lease_seconds, MAX_LEASE_SECONDS)
        now_ms = int(self._clock_ms())
        new_deadline = now_ms + lease_sec * 1000

        async with self._lock:
            cur = self.leases.get(req.pod_name)
            if cur is not None:
                if cur.instance_uid == req.instance_uid:
                    if req.instance_epoch < cur.instance_epoch or (
                        req.instance_epoch == cur.instance_epoch and new_deadline < cur.lease_deadline_ms
                    ):
                        raise LeaseRegressedError("ownerauthority: instance lease regressed")
                elif req.instance_epoch <= cur.instance_epoch:
                    raise LeaseRegressedError("ownerauthority: instance lease regressed")
            self.leases[req.pod_name] = InstanceLease(
                pod_name=req.pod_name,
                instance_uid=req.instance_uid,
                instance_epoch=req.instance_epoch,
                lease_deadline_ms=new_deadline,
                updated_at_ms=now_ms,
            )
            return new_deadline

    async def release(self, req: ReleaseRequest) -> tuple[OwnerRecord, bool]:
        if req.player_id <= 0 or req.owner_epoch <= 0 or not req.operation_id.strip():
            raise InvalidOperationError("ownerauthority: invalid release request")

        async with self._lock:
            cur = self.records.get(req.player_id)
            if (
                cur is None
                or cur.phase == Phase.NONE
                or cur.owner_epoch != req.owner_epoch
                or cur.operation_id != req.operation_id
            ):
                return (cur if cur is not None else OwnerRecord()), False

            now_ms = int(self._clock_ms())
            released = OwnerRecord(
                player_id=cur.player_id,
                owner_epoch=cur.owner_epoch,
                owner_type=OwnerType.NONE,
                phase=Phase.NONE,
                target=OwnerTarget(),
                operation_id=cur.operation_id,
                admit_not_before_ms=0,
                source_revision=cur.source_revision,
                updated_at_ms=now_ms,
            )
            self.records[req.player_id] = released
            self.logs.append(
                TransitionLogEntry(
                    player_id=released.player_id,
                    owner_epoch=released.owner_epoch,
                    operation_id=released.operation_id,
                    action="release",
                    detail=_truncate_detail(req.detail),
                    created_at_ms=now_ms,
                )
            )
            return released, True


class SqlOwnerAuthorityStore:
    """基于异步 SQL 连接池（如 aiomysql.Pool）的强一致归属权威存储。

    事务加锁顺序严格固定为 ``owner_record FOR UPDATE`` -> ``owner_instance_lease FOR UPDATE``。
    """

    def __init__(
        self,
        pool,
        *,
        skew_margin_ms: int = DEFAULT_SKEW_MARGIN_MS,
        clock_ms: Callable[[], int] | None = None,
        new_uuid: Callable[[], str] | None = None,
    ) -> None:
        self._pool = pool
        self.skew_margin_ms = skew_margin_ms if skew_margin_ms > 0 else DEFAULT_SKEW_MARGIN_MS
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._new_uuid = new_uuid or (lambda: str(uuid.uuid4()))

    @staticmethod
    def _row_to_record(row) -> OwnerRecord:
        return OwnerRecord(
            player_id=int(row[0]),
            owner_epoch=int(row[1]),
            owner_type=OwnerType(int(row[2])),
            phase=Phase(int(row[3])),
            target=OwnerTarget(
                pod_name=str(row[4]),
                instance_uid=str(row[5]),
                instance_epoch=int(row[6]),
                assignment_id=str(row[7]),
                release_track=str(row[8]),
            ),
            operation_id=str(row[9]),
            admit_not_before_ms=int(row[10]),
            source_revision=int(row[11]),
            updated_at_ms=int(row[12]),
        )

    async def query(self, player_id: int) -> tuple[OwnerRecord, bool]:
        if player_id <= 0:
            raise InvalidOperationError("ownerauthority: player_id must be positive")
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT player_id, owner_epoch, owner_type, phase, pod_name, instance_uid, instance_epoch, "
                    "assignment_id, release_track, operation_id, admit_not_before_ms, source_revision, updated_at_ms "
                    "FROM owner_record WHERE player_id = %s",
                    (player_id,),
                )
                row = await cur.fetchone()
                if row is None:
                    return OwnerRecord(), False
                return self._row_to_record(row), True

    async def begin_transition(self, req: BeginRequest) -> OwnerRecord:
        if req.player_id <= 0 or req.target_type not in (OwnerType.HUB, OwnerType.INSTANCE) or not req.target.complete():
            raise InvalidOperationError("ownerauthority: invalid operation or target")
        op_id = req.operation_id.strip()
        op_id = self._new_uuid() if not op_id else _parse_uuid4(op_id)

        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "SELECT player_id, owner_epoch, owner_type, phase, pod_name, instance_uid, instance_epoch, "
                        "assignment_id, release_track, operation_id, admit_not_before_ms, source_revision, updated_at_ms "
                        "FROM owner_record WHERE player_id = %s FOR UPDATE",
                        (req.player_id,),
                    )
                    row = await cursor.fetchone()
                    cur_rec = self._row_to_record(row) if row is not None else None
                    if cur_rec is not None:
                        if cur_rec.operation_id == op_id:
                            await conn.rollback()
                            if cur_rec.owner_type == req.target_type and cur_rec.target.equal(req.target):
                                return cur_rec
                            raise IdentityMismatchError("ownerauthority: target or operation identity mismatch")
                        if (
                            cur_rec.phase != Phase.NONE
                            and cur_rec.owner_type == req.target_type
                            and cur_rec.target.equal(req.target)
                            and (req.source_revision == 0 or req.source_revision == cur_rec.source_revision)
                        ):
                            await conn.rollback()
                            return cur_rec
                        if cur_rec.owner_epoch != req.expect_epoch:
                            raise EpochConflictError(cur_rec)
                        if cur_rec.source_revision > 0:
                            if req.source_revision == 0 or req.source_revision < cur_rec.source_revision:
                                raise SourceRevisionStaleError("ownerauthority: source revision stale")
                            if req.source_revision == cur_rec.source_revision and not cur_rec.target.equal(req.target):
                                raise SourceRevisionStaleError("ownerauthority: source revision stale")
                    elif req.expect_epoch != 0:
                        raise EpochConflictError(OwnerRecord())

                    now_ms = int(self._clock_ms())
                    admit_not_before_ms = now_ms
                    if (
                        cur_rec is not None
                        and cur_rec.phase != Phase.NONE
                        and cur_rec.target.pod_name
                        and not cur_rec.target.same_instance(req.target)
                    ):
                        await cursor.execute(
                            "SELECT instance_uid, instance_epoch, lease_deadline_ms "
                            "FROM owner_instance_lease WHERE pod_name = %s FOR UPDATE",
                            (cur_rec.target.pod_name,),
                        )
                        lrow = await cursor.fetchone()
                        if (
                            lrow is not None
                            and str(lrow[0]) == cur_rec.target.instance_uid
                            and int(lrow[1]) == cur_rec.target.instance_epoch
                        ):
                            barrier = int(lrow[2]) + self.skew_margin_ms
                            if barrier > admit_not_before_ms:
                                admit_not_before_ms = barrier

                    prev_epoch = cur_rec.owner_epoch if cur_rec is not None else 0
                    prev_source_rev = cur_rec.source_revision if cur_rec is not None else 0
                    next_rec = OwnerRecord(
                        player_id=req.player_id,
                        owner_epoch=prev_epoch + 1,
                        owner_type=req.target_type,
                        phase=Phase.PENDING,
                        target=req.target,
                        operation_id=op_id,
                        admit_not_before_ms=admit_not_before_ms,
                        source_revision=max(prev_source_rev, req.source_revision),
                        updated_at_ms=now_ms,
                    )
                    await cursor.execute(
                        "INSERT INTO owner_record ("
                        "player_id, owner_epoch, owner_type, phase, pod_name, instance_uid, instance_epoch, "
                        "assignment_id, release_track, operation_id, admit_not_before_ms, source_revision, updated_at_ms"
                        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON DUPLICATE KEY UPDATE "
                        "owner_epoch=VALUES(owner_epoch), owner_type=VALUES(owner_type), phase=VALUES(phase), "
                        "pod_name=VALUES(pod_name), instance_uid=VALUES(instance_uid), "
                        "instance_epoch=VALUES(instance_epoch), assignment_id=VALUES(assignment_id), "
                        "release_track=VALUES(release_track), operation_id=VALUES(operation_id), "
                        "admit_not_before_ms=VALUES(admit_not_before_ms), "
                        "source_revision=VALUES(source_revision), updated_at_ms=VALUES(updated_at_ms)",
                        (
                            next_rec.player_id, next_rec.owner_epoch, int(next_rec.owner_type), int(next_rec.phase),
                            next_rec.target.pod_name, next_rec.target.instance_uid, next_rec.target.instance_epoch,
                            next_rec.target.assignment_id, next_rec.target.release_track, next_rec.operation_id,
                            next_rec.admit_not_before_ms, next_rec.source_revision, next_rec.updated_at_ms,
                        ),
                    )
                    await cursor.execute(
                        "INSERT INTO owner_transition_log "
                        "(player_id, owner_epoch, operation_id, action, detail, created_at_ms) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (
                            next_rec.player_id, next_rec.owner_epoch, next_rec.operation_id,
                            "begin", _truncate_detail(req.detail), now_ms,
                        ),
                    )
                await conn.commit()
                return next_rec
            except Exception:
                await conn.rollback()
                raise

    async def admit(self, req: AdmitRequest) -> OwnerRecord:
        if req.player_id <= 0 or req.owner_epoch <= 0 or not req.target.complete():
            raise InvalidOperationError("ownerauthority: invalid operation or target")
        op_id = _parse_uuid4(req.operation_id)

        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "SELECT player_id, owner_epoch, owner_type, phase, pod_name, instance_uid, instance_epoch, "
                        "assignment_id, release_track, operation_id, admit_not_before_ms, source_revision, updated_at_ms "
                        "FROM owner_record WHERE player_id = %s FOR UPDATE",
                        (req.player_id,),
                    )
                    row = await cursor.fetchone()
                    cur_rec = self._row_to_record(row) if row is not None else None
                    if (
                        cur_rec is None
                        or cur_rec.owner_epoch != req.owner_epoch
                        or cur_rec.operation_id != op_id
                        or not cur_rec.target.equal(req.target)
                    ):
                        raise IdentityMismatchError("ownerauthority: target or operation identity mismatch")
                    if cur_rec.phase == Phase.ADMITTED:
                        await conn.rollback()
                        return cur_rec
                    if cur_rec.phase != Phase.PENDING:
                        raise IdentityMismatchError("ownerauthority: target or operation identity mismatch")
                    now_ms = int(self._clock_ms())
                    if now_ms < cur_rec.admit_not_before_ms:
                        raise BarrierNotOpenError(
                            cur_rec.admit_not_before_ms,
                            clamp_retry_ms(cur_rec.admit_not_before_ms - now_ms),
                        )
                    await cursor.execute(
                        "UPDATE owner_record SET phase = %s, updated_at_ms = %s WHERE player_id = %s",
                        (int(Phase.ADMITTED), now_ms, cur_rec.player_id),
                    )
                    await cursor.execute(
                        "INSERT INTO owner_transition_log "
                        "(player_id, owner_epoch, operation_id, action, detail, created_at_ms) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (cur_rec.player_id, cur_rec.owner_epoch, cur_rec.operation_id, "admit", "", now_ms),
                    )
                await conn.commit()
                return OwnerRecord(
                    player_id=cur_rec.player_id,
                    owner_epoch=cur_rec.owner_epoch,
                    owner_type=cur_rec.owner_type,
                    phase=Phase.ADMITTED,
                    target=cur_rec.target,
                    operation_id=cur_rec.operation_id,
                    admit_not_before_ms=cur_rec.admit_not_before_ms,
                    source_revision=cur_rec.source_revision,
                    updated_at_ms=now_ms,
                )
            except Exception:
                await conn.rollback()
                raise

    async def renew_instance_lease(self, req: RenewLeaseRequest) -> int:
        if not req.pod_name or not req.instance_uid or req.instance_epoch <= 0 or req.lease_seconds <= 0:
            raise InvalidOperationError("ownerauthority: invalid lease renew request")
        lease_sec = min(req.lease_seconds, MAX_LEASE_SECONDS)
        now_ms = int(self._clock_ms())
        new_deadline = now_ms + lease_sec * 1000

        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "SELECT instance_uid, instance_epoch, lease_deadline_ms "
                        "FROM owner_instance_lease WHERE pod_name = %s FOR UPDATE",
                        (req.pod_name,),
                    )
                    row = await cursor.fetchone()
                    if row is not None:
                        cur_uid, cur_epoch, cur_deadline = str(row[0]), int(row[1]), int(row[2])
                        if cur_uid == req.instance_uid:
                            if req.instance_epoch < cur_epoch or (
                                req.instance_epoch == cur_epoch and new_deadline < cur_deadline
                            ):
                                raise LeaseRegressedError("ownerauthority: instance lease regressed")
                        elif req.instance_epoch <= cur_epoch:
                            raise LeaseRegressedError("ownerauthority: instance lease regressed")
                    await cursor.execute(
                        "INSERT INTO owner_instance_lease "
                        "(pod_name, instance_uid, instance_epoch, lease_deadline_ms, updated_at_ms) "
                        "VALUES (%s,%s,%s,%s,%s) "
                        "ON DUPLICATE KEY UPDATE "
                        "instance_uid=VALUES(instance_uid), instance_epoch=VALUES(instance_epoch), "
                        "lease_deadline_ms=VALUES(lease_deadline_ms), updated_at_ms=VALUES(updated_at_ms)",
                        (req.pod_name, req.instance_uid, req.instance_epoch, new_deadline, now_ms),
                    )
                await conn.commit()
                return new_deadline
            except Exception:
                await conn.rollback()
                raise

    async def release(self, req: ReleaseRequest) -> tuple[OwnerRecord, bool]:
        if req.player_id <= 0 or req.owner_epoch <= 0 or not req.operation_id.strip():
            raise InvalidOperationError("ownerauthority: invalid release request")

        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "SELECT player_id, owner_epoch, owner_type, phase, pod_name, instance_uid, instance_epoch, "
                        "assignment_id, release_track, operation_id, admit_not_before_ms, source_revision, updated_at_ms "
                        "FROM owner_record WHERE player_id = %s FOR UPDATE",
                        (req.player_id,),
                    )
                    row = await cursor.fetchone()
                    cur_rec = self._row_to_record(row) if row is not None else None
                    if (
                        cur_rec is None
                        or cur_rec.phase == Phase.NONE
                        or cur_rec.owner_epoch != req.owner_epoch
                        or cur_rec.operation_id != req.operation_id
                    ):
                        await conn.rollback()
                        return (cur_rec if cur_rec is not None else OwnerRecord()), False
                    now_ms = int(self._clock_ms())
                    await cursor.execute(
                        "UPDATE owner_record "
                        "SET owner_type = %s, phase = %s, pod_name = '', instance_uid = '', instance_epoch = 0, "
                        "assignment_id = '', release_track = '', admit_not_before_ms = 0, updated_at_ms = %s "
                        "WHERE player_id = %s",
                        (int(OwnerType.NONE), int(Phase.NONE), now_ms, cur_rec.player_id),
                    )
                    await cursor.execute(
                        "INSERT INTO owner_transition_log "
                        "(player_id, owner_epoch, operation_id, action, detail, created_at_ms) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (
                            cur_rec.player_id, cur_rec.owner_epoch, cur_rec.operation_id,
                            "release", _truncate_detail(req.detail), now_ms,
                        ),
                    )
                await conn.commit()
                return (
                    OwnerRecord(
                        player_id=cur_rec.player_id,
                        owner_epoch=cur_rec.owner_epoch,
                        owner_type=OwnerType.NONE,
                        phase=Phase.NONE,
                        target=OwnerTarget(),
                        operation_id=cur_rec.operation_id,
                        admit_not_before_ms=0,
                        source_revision=cur_rec.source_revision,
                        updated_at_ms=now_ms,
                    ),
                    True,
                )
            except Exception:
                await conn.rollback()
                raise


# ── scene_manager 桥接层（对照 Go owner_authority_bridge.go） ──────────────────

def owner_authority_target(node_id: str, scene_id: int, zone_id: int) -> OwnerTarget:
    """将 scene_manager 落点三元组投影为 OwnerAuthority 5 元组目标标识。"""
    return OwnerTarget(
        pod_name=node_id,
        instance_uid=node_id,
        instance_epoch=1,
        assignment_id=f"z{zone_id}:s{scene_id}",
        release_track="scene",
    )


def classify_owner_authority_error(err: BaseException | None) -> str:
    """将权威层错误映射为指标结果标签（与 Go classifyOwnerAuthorityErr 一致）。"""
    if err is None:
        return "ok"
    if isinstance(err, BarrierNotOpenError):
        return "barrier"
    if isinstance(err, EpochConflictError):
        return "conflict"
    if isinstance(err, SourceRevisionStaleError):
        return "stale_rev"
    if isinstance(err, IdentityMismatchError):
        return "mismatch"
    return "error"


class OwnerAuthorityBridge:
    """将 EnterScene / LeaveScene 桥接到线性一致 OwnerAuthority（支持 shadow 旁路与 enforce 强门禁）。"""

    def __init__(
        self,
        store: OwnerAuthorityStore | None,
        *,
        mode: str = "shadow",
        metrics_hook: Callable[[str, str, str], None] | None = None,
    ) -> None:
        self.store = store
        self._configured_mode = mode
        self._metrics_hook = metrics_hook

    @property
    def mode(self) -> str:
        if self.store is None:
            return ""
        return "enforce" if self._configured_mode == "enforce" else "shadow"

    @property
    def enforce(self) -> bool:
        return self.mode == "enforce"

    def _observe(self, mode: str, op: str, result: str) -> None:
        if self._metrics_hook is not None:
            self._metrics_hook(mode, op, result)

    async def sync_enter(
        self,
        *,
        player_id: int,
        scene_id: int,
        node_id: str,
        zone_id: int,
        redis_epoch: int,
        owner_revision: int,
        enforce: bool | None = None,
    ) -> None:
        """将 EnterScene 同步到 OwnerAuthority：shadow 模式旁路记录，enforce 模式 fail-closed 拦截。"""
        if self.store is None:
            return
        use_enforce = self.enforce if enforce is None else enforce
        mode = "enforce" if use_enforce else "shadow"
        target = owner_authority_target(node_id, scene_id, zone_id)

        try:
            cur, found = await self.store.query(player_id)
        except Exception as exc:
            self._observe(mode, "enter", "query_error")
            logger.error(
                "[OwnerAuthority] enter query failed mode=%s player=%d scene=%d node=%s err=%s",
                mode, player_id, scene_id, node_id, exc,
            )
            if use_enforce:
                raise
            return

        expect_epoch = cur.owner_epoch if found else 0
        try:
            rec = await self.store.begin_transition(
                BeginRequest(
                    player_id=player_id,
                    expect_epoch=expect_epoch,
                    target_type=OwnerType.INSTANCE,
                    target=target,
                    source_revision=owner_revision,
                    detail=f"enter zone={zone_id} scene={scene_id} node={node_id} redis_epoch={redis_epoch}",
                )
            )
        except Exception as exc:
            res = classify_owner_authority_error(exc)
            self._observe(mode, "enter", res)
            logger.error(
                "[OwnerAuthority] begin transition failed mode=%s player=%d scene=%d node=%s result=%s err=%s",
                mode, player_id, scene_id, node_id, res, exc,
            )
            if use_enforce:
                raise
            return

        try:
            await self.store.admit(
                AdmitRequest(
                    player_id=player_id,
                    owner_epoch=rec.owner_epoch,
                    operation_id=rec.operation_id,
                    target=target,
                )
            )
        except Exception as exc:
            res = classify_owner_authority_error(exc)
            self._observe(mode, "enter", res)
            logger.error(
                "[OwnerAuthority] admit failed mode=%s player=%d epoch=%d scene=%d node=%s result=%s err=%s",
                mode, player_id, rec.owner_epoch, scene_id, node_id, res, exc,
            )
            if use_enforce:
                raise
            return

        self._observe(mode, "enter", "ok")

    async def sync_leave(
        self,
        *,
        player_id: int,
        scene_id: int,
        node_id: str,
        zone_id: int,
    ) -> None:
        """在 LeaveScene 成功删除 Redis 位置后，释放匹配的 OwnerAuthority 归属记录。"""
        mode = self.mode
        if not mode or self.store is None:
            return
        use_enforce = mode == "enforce"

        try:
            cur, found = await self.store.query(player_id)
        except Exception as exc:
            self._observe(mode, "leave", "query_error")
            logger.error(
                "[OwnerAuthority] leave query failed mode=%s player=%d scene=%d err=%s",
                mode, player_id, scene_id, exc,
            )
            if use_enforce:
                raise
            return

        if not found or cur.phase == Phase.NONE:
            self._observe(mode, "leave", "noop")
            return
        if node_id and not cur.target.equal(owner_authority_target(node_id, scene_id, zone_id)):
            self._observe(mode, "leave", "target_changed")
            return

        try:
            _, released = await self.store.release(
                ReleaseRequest(
                    player_id=player_id,
                    owner_epoch=cur.owner_epoch,
                    operation_id=cur.operation_id,
                    detail=f"leave zone={zone_id} scene={scene_id} node={node_id}",
                )
            )
        except Exception as exc:
            self._observe(mode, "leave", "error")
            logger.error(
                "[OwnerAuthority] release failed mode=%s player=%d epoch=%d err=%s",
                mode, player_id, cur.owner_epoch, exc,
            )
            if use_enforce:
                raise
            return

        self._observe(mode, "leave", "ok" if released else "noop")
