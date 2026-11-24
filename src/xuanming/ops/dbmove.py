"""不停服物理库在线迁移工具（对应 Go ``tools/dbmove``）。

职责与架构（严格遵守单一职责与失败关闭原则）：
1. ``SideConfig`` / ``TableMoveSpec`` / ``MovePlan``：描述源/目标物理库连接参数、待迁表及主键列、分块大小、
   哨兵令牌与冻结闸门开关，并在任何日志、异常与序列输出中严禁回显口令或原始 DSN；
2. ``MoveJournal``（``MemoryMoveJournal`` + ``SqlMoveJournal``）：持久化 ``db_move_journal`` 与
   ``db_move_sentinel``，维护确定性迁移状态机 ``INIT -> BACKFILL -> REPLICATE -> VERIFY -> FREEZE -> CUTOVER -> DONE``
   （以及 ``ABORTED``）与按表断点续传游标；
3. ``ChunkChecksum``：基于规范化 SHA-256 的分块确定性校验和（区分 ``NULL`` / 空串 / 数值 / 字节类型，杜绝分隔符拼接碰撞），
   逐块对比源/目标并精确汇报不一致的主键区间；
4. ``ReplicationLagGuard``：切换前严格门禁，在复制中断、延迟超阈值、未排空、哨兵缺失/不匹配、校验缺失/过期或校验不一致时
   一律拒绝切流，防止双写分叉与数据丢失；
5. ``DbMover``：异步迁移编排器（``plan`` / ``backfill`` / ``verify`` / ``cutover`` / ``abort`` / ``status``），
   支持 ``--dry-run``、可注入异步 ``aiomysql`` 连接池与窗口失败自动解冻回滚；
6. ``main(argv=None)``：命令行入口，支持 ``plan`` / ``backfill`` / ``verify`` / ``cutover`` / ``abort`` / ``status``
   子命令与 ``--json`` / ``--dry-run`` 标志。
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import decimal
import hashlib
import hmac
import json
import logging
import re
import secrets
import sys
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from xuanming.login.password_auth import parse_mysql_dsn

logger = logging.getLogger(__name__)

# -- 退出码（对齐 Go tools/dbmove/mover.go） ---------------------------------------------------------

EXIT_OK = 0
EXIT_REFUSED = 1
EXIT_FAILED = 2
EXIT_UNKNOWN = 3

# -- 常量与正则 ---------------------------------------------------------------------------------------

DEFAULT_CHUNK_SIZE = 1000
DEFAULT_MAX_LAG_MS = 3000
DEFAULT_VERIFY_MAX_AGE_SEC = 6 * 3600.0
DEFAULT_JOURNAL_TABLE = "db_move_journal"
DEFAULT_SENTINEL_TABLE = "db_move_sentinel"

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_RUN_ID_RE = re.compile(r"^[0-9a-fA-F]{8,64}$")


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _assert_ident(name: str, label: str = "identifier") -> str:
    if not isinstance(name, str) or not _IDENT_RE.fullmatch(name):
        raise DbMoveRefusedError(f"invalid {label}: {name!r}")
    return name


def _quote_ident(name: str) -> str:
    return f"`{_assert_ident(name)}`"


def flip_marker_token(run_id: str) -> str:
    """按 ``run_id`` 生成确定性翻转标记令牌（对齐 Go ``flipMarkerNonce``）。"""
    return hashlib.sha256(f"dbmove-flip-marker/{run_id}".encode("utf-8")).hexdigest()[:32]


def new_run_id() -> str:
    """生成 32 位小写十六进制迁移运行 ID。"""
    return secrets.token_hex(16)


def new_sentinel_token() -> str:
    """生成 32 位小写十六进制哨兵校验令牌。"""
    return secrets.token_hex(16)


# -- 异常体系 -----------------------------------------------------------------------------------------


class DbMoveError(RuntimeError):
    """在线搬库工具基础异常。"""

    exit_code: int = EXIT_FAILED


class DbMoveRefusedError(DbMoveError):
    """前置校验或状态机门禁拒绝执行（不修改任何状态，退出码 1）。"""

    exit_code: int = EXIT_REFUSED


class CutoverRefusedError(DbMoveRefusedError):
    """切流安全门禁触发（延迟超限、复制断开、哨兵不匹配或校验失败）。"""


class ChecksumMismatchError(DbMoveRefusedError):
    """源库与目标库校验和不一致。"""


class DbMoveUnknownError(DbMoveError):
    """窗口中断后状态未知（例如源库可能仍处于冻结态，退出码 3）。"""

    exit_code: int = EXIT_UNKNOWN


# -- 状态机枚举 ---------------------------------------------------------------------------------------


class MovePhase(StrEnum):
    """物理库在线搬迁状态机阶段。"""

    INIT = "INIT"
    BACKFILL = "BACKFILL"
    REPLICATE = "REPLICATE"
    VERIFY = "VERIFY"
    FREEZE = "FREEZE"
    CUTOVER = "CUTOVER"
    DONE = "DONE"
    ABORTED = "ABORTED"


# 允许的状态迁移边集合（状态模式约束，防止非法跳跃或终态回退）。
ALLOWED_PHASE_TRANSITIONS: dict[MovePhase, frozenset[MovePhase]] = {
    MovePhase.INIT: frozenset({MovePhase.INIT, MovePhase.BACKFILL, MovePhase.VERIFY, MovePhase.ABORTED}),
    MovePhase.BACKFILL: frozenset({MovePhase.BACKFILL, MovePhase.REPLICATE, MovePhase.VERIFY, MovePhase.ABORTED}),
    MovePhase.REPLICATE: frozenset({MovePhase.REPLICATE, MovePhase.VERIFY, MovePhase.ABORTED}),
    MovePhase.VERIFY: frozenset({MovePhase.VERIFY, MovePhase.REPLICATE, MovePhase.FREEZE, MovePhase.ABORTED}),
    MovePhase.FREEZE: frozenset({MovePhase.FREEZE, MovePhase.CUTOVER, MovePhase.VERIFY, MovePhase.REPLICATE, MovePhase.ABORTED}),
    MovePhase.CUTOVER: frozenset({MovePhase.DONE, MovePhase.ABORTED}),
    MovePhase.DONE: frozenset({MovePhase.DONE}),
    MovePhase.ABORTED: frozenset({MovePhase.ABORTED}),
}


# -- 配置与计划数据结构 -------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SideConfig:
    """迁移单侧（源库或目标库）数据库连接配置；严禁在序列化或日志中回显口令。"""

    role: str = "source"
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = ""
    password: str = field(default="", repr=False)
    database: str = ""
    dsn: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if self.role not in ("source", "target"):
            raise DbMoveRefusedError(f"invalid side role: {self.role!r}")
        if self.dsn:
            parsed = parse_mysql_dsn(self.dsn)
            object.__setattr__(self, "host", str(parsed["host"]))
            object.__setattr__(self, "port", int(parsed["port"]))
            object.__setattr__(self, "user", str(parsed["user"]))
            object.__setattr__(self, "password", str(parsed["password"]))
            object.__setattr__(self, "database", str(parsed["database"]))
        if not self.host or not self.host.strip():
            raise DbMoveRefusedError(f"{self.role} host must not be empty")
        if not 1 <= int(self.port) <= 65535:
            raise DbMoveRefusedError(f"{self.role} port out of range: {self.port}")
        if self.database:
            _assert_ident(self.database, f"{self.role} database")

    @classmethod
    def from_dsn(cls, dsn: str, *, role: str) -> SideConfig:
        """从 ``mysql://`` 或 Go 风格 ``user:pass@tcp(host:port)/db`` DSN 构造，不回显凭据。"""
        try:
            parsed = parse_mysql_dsn(dsn)
        except ValueError as exc:
            raise DbMoveRefusedError(f"invalid {role} DSN (value redacted)") from exc
        return cls(
            role=role,
            host=str(parsed["host"]),
            port=int(parsed["port"]),
            user=str(parsed["user"]),
            password=str(parsed["password"]),
            database=str(parsed["database"]),
        )

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}/{self.database}"

    def to_safe_dict(self) -> dict[str, Any]:
        """脱敏字典视图，不包含密码与原始 DSN。"""
        return {
            "role": self.role,
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "database": self.database,
        }


@dataclass(frozen=True, slots=True)
class TableMoveSpec:
    """单张待迁移表的规范定义（表名、主键列与可选完整列序）。"""

    table: str
    pk_columns: tuple[str, ...]
    columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _assert_ident(self.table, "table")
        if not self.pk_columns:
            raise DbMoveRefusedError(
                f"table {self.table!r} has no primary key and cannot be verified or moved"
            )
        pks = tuple(_assert_ident(col, "primary key column") for col in self.pk_columns)
        if len(set(pks)) != len(pks):
            raise DbMoveRefusedError(f"table {self.table!r} has duplicate primary key columns")
        object.__setattr__(self, "pk_columns", pks)
        if self.columns:
            cols = tuple(_assert_ident(col, "column") for col in self.columns)
            if len(set(cols)) != len(cols):
                raise DbMoveRefusedError(f"table {self.table!r} has duplicate column names")
            missing_pk = [col for col in pks if col not in cols]
            if missing_pk:
                raise DbMoveRefusedError(
                    f"table {self.table!r} columns missing primary key column(s): {missing_pk}"
                )
            object.__setattr__(self, "columns", cols)

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "pk_columns": list(self.pk_columns),
            "columns": list(self.columns),
        }


@dataclass(frozen=True, slots=True)
class MovePlan:
    """物理库在线搬迁计划，封装源/目标配置、表清单、分块参数、哨兵令牌与冻结闸门开关。"""

    source: SideConfig
    target: SideConfig
    tables: tuple[TableMoveSpec, ...]
    run_id: str = field(default_factory=new_run_id)
    chunk_size: int = DEFAULT_CHUNK_SIZE
    sentinel_token: str = field(default_factory=new_sentinel_token)
    freeze_gate: bool = True
    max_lag_ms: int = DEFAULT_MAX_LAG_MS
    verify_max_age_sec: float = DEFAULT_VERIFY_MAX_AGE_SEC

    def __post_init__(self) -> None:
        if not _RUN_ID_RE.fullmatch(self.run_id):
            raise DbMoveRefusedError("run_id must be 8..64 hex characters")
        object.__setattr__(self, "run_id", self.run_id.lower())
        if self.source.role != "source" or self.target.role != "target":
            raise DbMoveRefusedError("MovePlan requires source.role='source' and target.role='target'")
        if (
            self.source.host == self.target.host
            and self.source.port == self.target.port
            and self.source.database == self.target.database
        ):
            raise DbMoveRefusedError("source and target must not point to the same database endpoint")
        if not self.tables:
            raise DbMoveRefusedError("MovePlan must include at least one table")
        normalized_tables = tuple(
            t if isinstance(t, TableMoveSpec) else TableMoveSpec(table=str(t), pk_columns=("id",))
            for t in self.tables
        )
        names = [t.table for t in normalized_tables]
        if len(set(names)) != len(names):
            raise DbMoveRefusedError("MovePlan contains duplicate table specifications")
        object.__setattr__(self, "tables", normalized_tables)
        if self.chunk_size < 1 or self.chunk_size > 100_000:
            raise DbMoveRefusedError("chunk_size must be in 1..100000")
        if not self.sentinel_token or len(self.sentinel_token) > 128:
            raise DbMoveRefusedError("sentinel_token must be 1..128 characters")
        if self.max_lag_ms < 0:
            raise DbMoveRefusedError("max_lag_ms must be >= 0")
        if self.verify_max_age_sec <= 0:
            raise DbMoveRefusedError("verify_max_age_sec must be > 0")

    @property
    def database(self) -> str:
        return self.source.database or self.target.database

    @property
    def table_names(self) -> tuple[str, ...]:
        return tuple(spec.table for spec in self.tables)

    def spec_for(self, table: str) -> TableMoveSpec:
        for spec in self.tables:
            if spec.table == table:
                return spec
        raise KeyError(f"table not in plan: {table}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "database": self.database,
            "source": self.source.to_safe_dict(),
            "target": self.target.to_safe_dict(),
            "tables": [spec.to_dict() for spec in self.tables],
            "chunk_size": self.chunk_size,
            "sentinel_token": self.sentinel_token,
            "freeze_gate": self.freeze_gate,
            "max_lag_ms": self.max_lag_ms,
            "verify_max_age_sec": self.verify_max_age_sec,
        }


# -- 迁移日志与断点游标（MoveJournal） ----------------------------------------------------------------


@dataclass(slots=True)
class TableCursor:
    """单张表的断点续传游标状态。"""

    table: str
    last_pk: tuple[Any, ...] | None = None
    rows_copied: int = 0
    chunks_copied: int = 0
    completed: bool = False
    updated_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "last_pk": list(self.last_pk) if self.last_pk is not None else None,
            "rows_copied": self.rows_copied,
            "chunks_copied": self.chunks_copied,
            "completed": self.completed,
            "updated_ms": self.updated_ms,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TableCursor:
        raw_pk = data.get("last_pk")
        last_pk = tuple(raw_pk) if isinstance(raw_pk, (list, tuple)) else None
        return cls(
            table=str(data["table"]),
            last_pk=last_pk,
            rows_copied=int(data.get("rows_copied", 0)),
            chunks_copied=int(data.get("chunks_copied", 0)),
            completed=bool(data.get("completed", False)),
            updated_ms=int(data.get("updated_ms", 0)),
        )


@dataclass(slots=True)
class MoveState:
    """一次在线搬库运行的完整持久化状态。"""

    run_id: str
    database: str
    phase: MovePhase
    sentinel_token: str
    table_cursors: dict[str, TableCursor] = field(default_factory=dict)
    verified_at_ms: int = 0
    verified_checksums: dict[str, str] = field(default_factory=dict)
    frozen: bool = False
    writer_epoch: int = 0
    cutover_at_ms: int = 0
    aborted_at_ms: int = 0
    error: str = ""
    created_ms: int = 0
    updated_ms: int = 0
    events: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "database": self.database,
            "phase": self.phase.value,
            "sentinel_token": self.sentinel_token,
            "table_cursors": {k: v.to_dict() for k, v in self.table_cursors.items()},
            "verified_at_ms": self.verified_at_ms,
            "verified_checksums": dict(self.verified_checksums),
            "frozen": self.frozen,
            "writer_epoch": self.writer_epoch,
            "cutover_at_ms": self.cutover_at_ms,
            "aborted_at_ms": self.aborted_at_ms,
            "error": self.error,
            "created_ms": self.created_ms,
            "updated_ms": self.updated_ms,
            "events": list(self.events),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MoveState:
        cursors = {
            str(k): TableCursor.from_dict(v)
            for k, v in dict(data.get("table_cursors") or {}).items()
        }
        return cls(
            run_id=str(data["run_id"]),
            database=str(data.get("database", "")),
            phase=MovePhase(str(data["phase"])),
            sentinel_token=str(data.get("sentinel_token", "")),
            table_cursors=cursors,
            verified_at_ms=int(data.get("verified_at_ms", 0)),
            verified_checksums={
                str(k): str(v) for k, v in dict(data.get("verified_checksums") or {}).items()
            },
            frozen=bool(data.get("frozen", False)),
            writer_epoch=int(data.get("writer_epoch", 0)),
            cutover_at_ms=int(data.get("cutover_at_ms", 0)),
            aborted_at_ms=int(data.get("aborted_at_ms", 0)),
            error=str(data.get("error", "")),
            created_ms=int(data.get("created_ms", 0)),
            updated_ms=int(data.get("updated_ms", 0)),
            events=list(data.get("events") or []),
        )


def _validate_transition(current: MovePhase, target: MovePhase) -> None:
    allowed = ALLOWED_PHASE_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise DbMoveRefusedError(f"illegal move phase transition: {current.value} -> {target.value}")


class MoveJournal(ABC):
    """迁移状态机日志与哨兵抽象存储接口。"""

    @abstractmethod
    async def init_run(self, plan: MovePlan, *, now_ms: int | None = None) -> MoveState:
        """幂等初始化一次搬迁记录；若同 ``run_id`` 已存在则校验关键元信息一致后返回。"""

    @abstractmethod
    async def load(self, run_id: str) -> MoveState | None:
        """加载指定 ``run_id`` 的状态记录。"""

    @abstractmethod
    async def transition(
        self,
        run_id: str,
        phase: MovePhase,
        *,
        frozen: bool | None = None,
        writer_epoch: int | None = None,
        error: str | None = None,
        now_ms: int | None = None,
    ) -> MoveState:
        """按状态机合法边推进阶段并落盘。"""

    @abstractmethod
    async def update_cursor(
        self,
        run_id: str,
        table: str,
        *,
        last_pk: tuple[Any, ...] | None,
        rows_copied: int,
        chunks_copied: int,
        completed: bool = False,
        now_ms: int | None = None,
    ) -> MoveState:
        """更新单表回填进度游标，支持中断后断点续传。"""

    @abstractmethod
    async def record_verification(
        self,
        run_id: str,
        checksums: Mapping[str, str],
        *,
        verified_at_ms: int,
    ) -> MoveState:
        """记录全表校验通过的摘要与时间戳。"""

    @abstractmethod
    async def clear_verification(self, run_id: str, *, reason: str = "", now_ms: int | None = None) -> MoveState:
        """清除过期或失效的校验记录（如复制断开或静默窗口比对失败后）。"""

    @abstractmethod
    async def append_event(self, run_id: str, step: str, message: str, *, now_ms: int | None = None) -> None:
        """追加步骤审计事件。"""

    @abstractmethod
    async def write_sentinel(
        self,
        run_id: str,
        side: str,
        token: str,
        *,
        epoch: int = 0,
        now_ms: int | None = None,
    ) -> None:
        """在指定侧写入哨兵或翻转标记。"""

    @abstractmethod
    async def has_sentinel(self, run_id: str, side: str, token: str) -> bool:
        """检查指定侧是否存在指定哨兵令牌。"""


class MemoryMoveJournal(MoveJournal):
    """纯内存迁移状态日志与哨兵存储（供单元测试与 ``--dry-run`` 使用）。"""

    def __init__(self) -> None:
        self._states: dict[str, MoveState] = {}
        self._sentinels: dict[tuple[str, str], set[str]] = {}

    async def init_run(self, plan: MovePlan, *, now_ms: int | None = None) -> MoveState:
        ts = _now_ms() if now_ms is None else now_ms
        existing = self._states.get(plan.run_id)
        if existing is not None:
            if existing.database != plan.database:
                raise DbMoveRefusedError(
                    f"run {plan.run_id} belongs to database {existing.database!r}, not {plan.database!r}"
                )
            return existing
        cursors = {spec.table: TableCursor(table=spec.table, updated_ms=ts) for spec in plan.tables}
        state = MoveState(
            run_id=plan.run_id,
            database=plan.database,
            phase=MovePhase.INIT,
            sentinel_token=plan.sentinel_token,
            table_cursors=cursors,
            created_ms=ts,
            updated_ms=ts,
            events=[{"time_ms": ts, "step": "plan", "message": f"initialized move for {plan.database}"}],
        )
        self._states[plan.run_id] = state
        return state

    async def load(self, run_id: str) -> MoveState | None:
        return self._states.get(run_id.lower())

    async def transition(
        self,
        run_id: str,
        phase: MovePhase,
        *,
        frozen: bool | None = None,
        writer_epoch: int | None = None,
        error: str | None = None,
        now_ms: int | None = None,
    ) -> MoveState:
        state = self._states.get(run_id.lower())
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {run_id}")
        _validate_transition(state.phase, phase)
        ts = _now_ms() if now_ms is None else now_ms
        state.phase = phase
        if frozen is not None:
            state.frozen = frozen
        if writer_epoch is not None:
            state.writer_epoch = writer_epoch
        if error is not None:
            state.error = error
        if phase == MovePhase.DONE:
            state.cutover_at_ms = ts
        elif phase == MovePhase.ABORTED:
            state.aborted_at_ms = ts
        state.updated_ms = ts
        return state

    async def update_cursor(
        self,
        run_id: str,
        table: str,
        *,
        last_pk: tuple[Any, ...] | None,
        rows_copied: int,
        chunks_copied: int,
        completed: bool = False,
        now_ms: int | None = None,
    ) -> MoveState:
        state = self._states.get(run_id.lower())
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {run_id}")
        ts = _now_ms() if now_ms is None else now_ms
        state.table_cursors[table] = TableCursor(
            table=table,
            last_pk=last_pk,
            rows_copied=rows_copied,
            chunks_copied=chunks_copied,
            completed=completed,
            updated_ms=ts,
        )
        state.updated_ms = ts
        return state

    async def record_verification(
        self,
        run_id: str,
        checksums: Mapping[str, str],
        *,
        verified_at_ms: int,
    ) -> MoveState:
        state = self._states.get(run_id.lower())
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {run_id}")
        state.verified_at_ms = int(verified_at_ms)
        state.verified_checksums = {str(k): str(v) for k, v in checksums.items()}
        state.updated_ms = int(verified_at_ms)
        return state

    async def clear_verification(
        self,
        run_id: str,
        *,
        reason: str = "",
        now_ms: int | None = None,
    ) -> MoveState:
        state = self._states.get(run_id.lower())
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {run_id}")
        ts = _now_ms() if now_ms is None else now_ms
        state.verified_at_ms = 0
        state.verified_checksums.clear()
        if reason:
            state.events.append({"time_ms": ts, "step": "verify_cleared", "message": reason})
        state.updated_ms = ts
        return state

    async def append_event(self, run_id: str, step: str, message: str, *, now_ms: int | None = None) -> None:
        state = self._states.get(run_id.lower())
        if state is None:
            return
        ts = _now_ms() if now_ms is None else now_ms
        state.events.append({"time_ms": ts, "step": step, "message": message})
        state.updated_ms = ts

    async def write_sentinel(
        self,
        run_id: str,
        side: str,
        token: str,
        *,
        epoch: int = 0,
        now_ms: int | None = None,
    ) -> None:
        self._sentinels.setdefault((run_id.lower(), side), set()).add(token)

    async def has_sentinel(self, run_id: str, side: str, token: str) -> bool:
        tokens = self._sentinels.get((run_id.lower(), side), set())
        return any(hmac.compare_digest(existing, token) for existing in tokens)


class SqlMoveJournal(MoveJournal):
    """基于异步 ``aiomysql`` 连接池的 ``db_move_journal`` / ``db_move_sentinel`` 持久化实现。"""

    def __init__(
        self,
        pool: Any,
        *,
        journal_table: str = DEFAULT_JOURNAL_TABLE,
        sentinel_table: str = DEFAULT_SENTINEL_TABLE,
    ) -> None:
        if pool is None:
            raise ValueError("SqlMoveJournal requires a non-null async pool")
        self._pool = pool
        self._journal_table = _assert_ident(journal_table, "journal_table")
        self._sentinel_table = _assert_ident(sentinel_table, "sentinel_table")

    async def ensure_tables(self) -> None:
        """幂等创建 ``db_move_journal`` 与 ``db_move_sentinel`` 运维控制表。"""
        jt = _quote_ident(self._journal_table)
        st = _quote_ident(self._sentinel_table)
        journal_ddl = (
            f"CREATE TABLE IF NOT EXISTS {jt} ("
            "`run_id` VARCHAR(64) NOT NULL, "
            "`database_name` VARCHAR(64) NOT NULL, "
            "`phase` VARCHAR(32) NOT NULL, "
            "`sentinel_token` VARCHAR(128) NOT NULL, "
            "`state_json` LONGTEXT NOT NULL, "
            "`updated_ms` BIGINT NOT NULL, "
            "PRIMARY KEY (`run_id`), "
            "KEY `idx_db_move_journal_db` (`database_name`, `updated_ms`)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
        sentinel_ddl = (
            f"CREATE TABLE IF NOT EXISTS {st} ("
            "`run_id` VARCHAR(64) NOT NULL, "
            "`side` VARCHAR(16) NOT NULL, "
            "`token` VARCHAR(128) NOT NULL, "
            "`writer_epoch` BIGINT NOT NULL DEFAULT 0, "
            "`written_ms` BIGINT NOT NULL, "
            "PRIMARY KEY (`run_id`, `side`, `token`)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(journal_ddl)
                await cur.execute(sentinel_ddl)

    async def _save(self, state: MoveState) -> None:
        jt = _quote_ident(self._journal_table)
        payload = json.dumps(state.to_dict(), ensure_ascii=False, separators=(",", ":"))
        sql = (
            f"INSERT INTO {jt} (`run_id`, `database_name`, `phase`, `sentinel_token`, `state_json`, `updated_ms`) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE `phase` = VALUES(`phase`), `state_json` = VALUES(`state_json`), "
            "`updated_ms` = VALUES(`updated_ms`)"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql,
                    (
                        state.run_id,
                        state.database,
                        state.phase.value,
                        state.sentinel_token,
                        payload,
                        state.updated_ms,
                    ),
                )

    async def init_run(self, plan: MovePlan, *, now_ms: int | None = None) -> MoveState:
        ts = _now_ms() if now_ms is None else now_ms
        existing = await self.load(plan.run_id)
        if existing is not None:
            if existing.database != plan.database:
                raise DbMoveRefusedError(
                    f"run {plan.run_id} belongs to database {existing.database!r}, not {plan.database!r}"
                )
            return existing
        cursors = {spec.table: TableCursor(table=spec.table, updated_ms=ts) for spec in plan.tables}
        state = MoveState(
            run_id=plan.run_id,
            database=plan.database,
            phase=MovePhase.INIT,
            sentinel_token=plan.sentinel_token,
            table_cursors=cursors,
            created_ms=ts,
            updated_ms=ts,
            events=[{"time_ms": ts, "step": "plan", "message": f"initialized move for {plan.database}"}],
        )
        await self._save(state)
        return state

    async def load(self, run_id: str) -> MoveState | None:
        jt = _quote_ident(self._journal_table)
        sql = f"SELECT `state_json` FROM {jt} WHERE `run_id` = %s"
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (run_id.lower(),))
                row = await cur.fetchone()
        if not row:
            return None
        raw = row[0] if not isinstance(row, Mapping) else row["state_json"]
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return MoveState.from_dict(json.loads(raw))

    async def transition(
        self,
        run_id: str,
        phase: MovePhase,
        *,
        frozen: bool | None = None,
        writer_epoch: int | None = None,
        error: str | None = None,
        now_ms: int | None = None,
    ) -> MoveState:
        state = await self.load(run_id)
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {run_id}")
        _validate_transition(state.phase, phase)
        ts = _now_ms() if now_ms is None else now_ms
        state.phase = phase
        if frozen is not None:
            state.frozen = frozen
        if writer_epoch is not None:
            state.writer_epoch = writer_epoch
        if error is not None:
            state.error = error
        if phase == MovePhase.DONE:
            state.cutover_at_ms = ts
        elif phase == MovePhase.ABORTED:
            state.aborted_at_ms = ts
        state.updated_ms = ts
        await self._save(state)
        return state

    async def update_cursor(
        self,
        run_id: str,
        table: str,
        *,
        last_pk: tuple[Any, ...] | None,
        rows_copied: int,
        chunks_copied: int,
        completed: bool = False,
        now_ms: int | None = None,
    ) -> MoveState:
        state = await self.load(run_id)
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {run_id}")
        ts = _now_ms() if now_ms is None else now_ms
        state.table_cursors[table] = TableCursor(
            table=table,
            last_pk=last_pk,
            rows_copied=rows_copied,
            chunks_copied=chunks_copied,
            completed=completed,
            updated_ms=ts,
        )
        state.updated_ms = ts
        await self._save(state)
        return state

    async def record_verification(
        self,
        run_id: str,
        checksums: Mapping[str, str],
        *,
        verified_at_ms: int,
    ) -> MoveState:
        state = await self.load(run_id)
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {run_id}")
        state.verified_at_ms = int(verified_at_ms)
        state.verified_checksums = {str(k): str(v) for k, v in checksums.items()}
        state.updated_ms = int(verified_at_ms)
        await self._save(state)
        return state

    async def clear_verification(
        self,
        run_id: str,
        *,
        reason: str = "",
        now_ms: int | None = None,
    ) -> MoveState:
        state = await self.load(run_id)
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {run_id}")
        ts = _now_ms() if now_ms is None else now_ms
        state.verified_at_ms = 0
        state.verified_checksums.clear()
        if reason:
            state.events.append({"time_ms": ts, "step": "verify_cleared", "message": reason})
        state.updated_ms = ts
        await self._save(state)
        return state

    async def append_event(self, run_id: str, step: str, message: str, *, now_ms: int | None = None) -> None:
        state = await self.load(run_id)
        if state is None:
            return
        ts = _now_ms() if now_ms is None else now_ms
        state.events.append({"time_ms": ts, "step": step, "message": message})
        state.updated_ms = ts
        await self._save(state)

    async def write_sentinel(
        self,
        run_id: str,
        side: str,
        token: str,
        *,
        epoch: int = 0,
        now_ms: int | None = None,
    ) -> None:
        ts = _now_ms() if now_ms is None else now_ms
        st = _quote_ident(self._sentinel_table)
        sql = (
            f"INSERT INTO {st} (`run_id`, `side`, `token`, `writer_epoch`, `written_ms`) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE `writer_epoch` = VALUES(`writer_epoch`), `written_ms` = VALUES(`written_ms`)"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (run_id.lower(), side, token, int(epoch), ts))

    async def has_sentinel(self, run_id: str, side: str, token: str) -> bool:
        st = _quote_ident(self._sentinel_table)
        sql = f"SELECT COUNT(*) FROM {st} WHERE `run_id` = %s AND `side` = %s AND `token` = %s"
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (run_id.lower(), side, token))
                row = await cur.fetchone()
        if not row:
            return False
        count = row[0] if not isinstance(row, Mapping) else next(iter(row.values()))
        return int(count) > 0


# -- 确定性分块校验和（ChunkChecksum） ----------------------------------------------------------------


def _canonical_encode_value(value: Any) -> bytes:
    """将单个 SQL 列值编码为带类型前缀与长度前缀的无歧义字节串。

    与 Go ``checksum.go`` 避免分隔符碰撞的目标一致：``NULL``、空串 ``""``、整数 ``0``、字节串 ``b""``
    各自拥有独立类型标签，且携带字节长度前缀，确保任意列值组合均只有唯一编码。
    """
    if value is None:
        return b"N:"
    if isinstance(value, bool):
        return b"B:1" if value else b"B:0"
    if isinstance(value, int):
        raw = str(value).encode("ascii")
        return b"I:" + str(len(raw)).encode("ascii") + b":" + raw
    if isinstance(value, float):
        raw = repr(value).encode("ascii")
        return b"F:" + str(len(raw)).encode("ascii") + b":" + raw
    if isinstance(value, decimal.Decimal):
        raw = format(value.normalize(), "f").encode("ascii")
        return b"D:" + str(len(raw)).encode("ascii") + b":" + raw
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return b"X:" + str(len(raw)).encode("ascii") + b":" + raw
    if isinstance(value, dt.datetime):
        iso = value.isoformat()
        raw = iso.encode("utf-8")
        return b"T:" + str(len(raw)).encode("ascii") + b":" + raw
    if isinstance(value, dt.date):
        raw = value.isoformat().encode("utf-8")
        return b"A:" + str(len(raw)).encode("ascii") + b":" + raw
    raw = str(value).encode("utf-8")
    return b"S:" + str(len(raw)).encode("ascii") + b":" + raw


def compute_rows_checksum(
    rows: Sequence[Mapping[str, Any] | Sequence[Any]],
    columns: Sequence[str] | None = None,
) -> str:
    """计算有序行集的确定性规范化 SHA-256 校验和。"""
    hasher = hashlib.sha256()
    if columns is not None:
        col_list = tuple(columns)
        header = b"COLS:" + b",".join(c.encode("utf-8") for c in col_list) + b"\n"
        hasher.update(header)
    else:
        col_list = None

    for row in rows:
        hasher.update(b"R:")
        if isinstance(row, Mapping):
            keys = col_list if col_list is not None else tuple(sorted(row.keys()))
            for key in keys:
                key_bytes = str(key).encode("utf-8")
                hasher.update(b"K:")
                hasher.update(str(len(key_bytes)).encode("ascii"))
                hasher.update(b":")
                hasher.update(key_bytes)
                hasher.update(b"=")
                hasher.update(_canonical_encode_value(row.get(key)))
                hasher.update(b";")
        else:
            for cell in row:
                hasher.update(_canonical_encode_value(cell))
                hasher.update(b";")
        hasher.update(b"\n")
    hasher.update(f"COUNT:{len(rows)}".encode("ascii"))
    return hasher.hexdigest()


@dataclass(frozen=True, slots=True)
class ChunkMismatch:
    """源/目标分块校验不一致的主键区间详情。"""

    table: str
    chunk_index: int
    pk_start: tuple[Any, ...] | None
    pk_end: tuple[Any, ...] | None
    source_checksum: str
    target_checksum: str
    source_rows: int
    target_rows: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "chunk_index": self.chunk_index,
            "pk_start": list(self.pk_start) if self.pk_start is not None else None,
            "pk_end": list(self.pk_end) if self.pk_end is not None else None,
            "source_checksum": self.source_checksum,
            "target_checksum": self.target_checksum,
            "source_rows": self.source_rows,
            "target_rows": self.target_rows,
        }


@dataclass(frozen=True, slots=True)
class TableChecksumResult:
    """单张表的全量分块校验结果。"""

    table: str
    matched: bool
    source_rows: int
    target_rows: int
    chunks_checked: int
    source_digest: str
    target_digest: str
    mismatches: tuple[ChunkMismatch, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "matched": self.matched,
            "source_rows": self.source_rows,
            "target_rows": self.target_rows,
            "chunks_checked": self.chunks_checked,
            "source_digest": self.source_digest,
            "target_digest": self.target_digest,
            "mismatches": [m.to_dict() for m in self.mismatches],
        }


def _extract_row_columns(cur: Any, rows: Sequence[Any], explicit_columns: Sequence[str] | None) -> tuple[str, ...]:
    if explicit_columns:
        return tuple(explicit_columns)
    desc = getattr(cur, "description", None)
    if desc:
        return tuple(str(col[0]) for col in desc)
    if rows and isinstance(rows[0], Mapping):
        return tuple(rows[0].keys())
    return ()


def _row_to_dict(row: Any, columns: Sequence[str]) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return {columns[i]: row[i] for i in range(min(len(columns), len(row)))}


def _pk_tuple(row: Mapping[str, Any], pk_columns: Sequence[str]) -> tuple[Any, ...]:
    return tuple(row[col] for col in pk_columns)


async def _fetch_chunk(
    pool: Any,
    table: str,
    pk_columns: Sequence[str],
    *,
    columns: Sequence[str] | None,
    after_pk: tuple[Any, ...] | None,
    chunk_size: int,
) -> tuple[tuple[str, ...], list[dict[str, Any]]]:
    """按主键字典序游标拉取下一批有序行（兼容单列主键与联合主键）。"""
    quoted_table = _quote_ident(table)
    select_cols = ", ".join(_quote_ident(c) for c in columns) if columns else "*"
    order_clause = ", ".join(f"{_quote_ident(c)} ASC" for c in pk_columns)
    params: list[Any] = []
    where_clause = ""
    if after_pk is not None:
        if len(pk_columns) == 1:
            where_clause = f" WHERE {_quote_ident(pk_columns[0])} > %s"
            params.append(after_pk[0])
        else:
            lhs = "(" + ", ".join(_quote_ident(c) for c in pk_columns) + ")"
            rhs = "(" + ", ".join("%s" for _ in pk_columns) + ")"
            where_clause = f" WHERE {lhs} > {rhs}"
            params.extend(after_pk)
    sql = f"SELECT {select_cols} FROM {quoted_table}{where_clause} ORDER BY {order_clause} LIMIT %s"
    params.append(int(chunk_size))

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, tuple(params))
            raw_rows = list(await cur.fetchall())
            col_names = _extract_row_columns(cur, raw_rows, columns)
    dict_rows = [_row_to_dict(r, col_names) for r in raw_rows]
    return col_names, dict_rows


async def _fetch_range(
    pool: Any,
    table: str,
    pk_columns: Sequence[str],
    *,
    columns: Sequence[str] | None,
    after_pk: tuple[Any, ...] | None,
    upper_pk: tuple[Any, ...] | None,
    limit: int | None = None,
) -> tuple[tuple[str, ...], list[dict[str, Any]]]:
    """按给定主键开闭区间 ``(after_pk, upper_pk]`` 拉取目标表行集。"""
    quoted_table = _quote_ident(table)
    select_cols = ", ".join(_quote_ident(c) for c in columns) if columns else "*"
    order_clause = ", ".join(f"{_quote_ident(c)} ASC" for c in pk_columns)
    conditions: list[str] = []
    params: list[Any] = []

    if after_pk is not None:
        if len(pk_columns) == 1:
            conditions.append(f"{_quote_ident(pk_columns[0])} > %s")
            params.append(after_pk[0])
        else:
            lhs = "(" + ", ".join(_quote_ident(c) for c in pk_columns) + ")"
            rhs = "(" + ", ".join("%s" for _ in pk_columns) + ")"
            conditions.append(f"{lhs} > {rhs}")
            params.extend(after_pk)

    if upper_pk is not None:
        if len(pk_columns) == 1:
            conditions.append(f"{_quote_ident(pk_columns[0])} <= %s")
            params.append(upper_pk[0])
        else:
            lhs = "(" + ", ".join(_quote_ident(c) for c in pk_columns) + ")"
            rhs = "(" + ", ".join("%s" for _ in pk_columns) + ")"
            conditions.append(f"{lhs} <= {rhs}")
            params.extend(upper_pk)

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT %s"
        params.append(int(limit))
    sql = f"SELECT {select_cols} FROM {quoted_table}{where_clause} ORDER BY {order_clause}{limit_clause}"

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, tuple(params))
            raw_rows = list(await cur.fetchall())
            col_names = _extract_row_columns(cur, raw_rows, columns)
    dict_rows = [_row_to_dict(r, col_names) for r in raw_rows]
    return col_names, dict_rows


class ChunkChecksum:
    """确定性分块表校验器，逐块比对源库与目标库数据并报告精确主键差异区间。"""

    @staticmethod
    def compute_rows_checksum(
        rows: Sequence[Mapping[str, Any] | Sequence[Any]],
        columns: Sequence[str] | None = None,
    ) -> str:
        return compute_rows_checksum(rows, columns=columns)

    @classmethod
    async def verify_table_checksum(
        cls,
        source_pool: Any,
        target_pool: Any,
        table: str,
        pk_columns: Sequence[str],
        *,
        columns: Sequence[str] | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> TableChecksumResult:
        """按主键分块比对源库与目标库指定表，返回完整摘要与不一致主键区间。"""
        _assert_ident(table, "table")
        pks = tuple(_assert_ident(c, "pk_column") for c in pk_columns)
        if not pks:
            raise DbMoveRefusedError(f"table {table!r} requires at least one primary key column")
        if chunk_size < 1:
            raise DbMoveRefusedError("chunk_size must be >= 1")

        source_overall = hashlib.sha256()
        target_overall = hashlib.sha256()
        mismatches: list[ChunkMismatch] = []
        after_pk: tuple[Any, ...] | None = None
        chunk_index = 0
        total_source = 0
        total_target = 0
        effective_cols: tuple[str, ...] | None = tuple(columns) if columns else None

        while True:
            src_cols, src_rows = await _fetch_chunk(
                source_pool,
                table,
                pks,
                columns=effective_cols,
                after_pk=after_pk,
                chunk_size=chunk_size,
            )
            if effective_cols is None and src_cols:
                effective_cols = src_cols

            if not src_rows:
                # 源表已扫完，检查目标表是否存在多余尾部行。
                tgt_cols, extra_tgt = await _fetch_range(
                    target_pool,
                    table,
                    pks,
                    columns=effective_cols,
                    after_pk=after_pk,
                    upper_pk=None,
                    limit=chunk_size,
                )
                if effective_cols is None and tgt_cols:
                    effective_cols = tgt_cols
                if extra_tgt:
                    src_digest = compute_rows_checksum([], effective_cols)
                    tgt_digest = compute_rows_checksum(extra_tgt, effective_cols)
                    source_overall.update(src_digest.encode("ascii"))
                    target_overall.update(tgt_digest.encode("ascii"))
                    total_target += len(extra_tgt)
                    mismatches.append(
                        ChunkMismatch(
                            table=table,
                            chunk_index=chunk_index,
                            pk_start=_pk_tuple(extra_tgt[0], pks),
                            pk_end=_pk_tuple(extra_tgt[-1], pks),
                            source_checksum=src_digest,
                            target_checksum=tgt_digest,
                            source_rows=0,
                            target_rows=len(extra_tgt),
                        )
                    )
                    chunk_index += 1
                break

            pk_start = _pk_tuple(src_rows[0], pks)
            pk_end = _pk_tuple(src_rows[-1], pks)
            _, tgt_rows = await _fetch_range(
                target_pool,
                table,
                pks,
                columns=effective_cols,
                after_pk=after_pk,
                upper_pk=pk_end,
            )

            src_digest = compute_rows_checksum(src_rows, effective_cols)
            tgt_digest = compute_rows_checksum(tgt_rows, effective_cols)
            source_overall.update(src_digest.encode("ascii"))
            target_overall.update(tgt_digest.encode("ascii"))
            total_source += len(src_rows)
            total_target += len(tgt_rows)

            if not hmac.compare_digest(src_digest, tgt_digest) or len(src_rows) != len(tgt_rows):
                mismatches.append(
                    ChunkMismatch(
                        table=table,
                        chunk_index=chunk_index,
                        pk_start=pk_start,
                        pk_end=pk_end,
                        source_checksum=src_digest,
                        target_checksum=tgt_digest,
                        source_rows=len(src_rows),
                        target_rows=len(tgt_rows),
                    )
                )

            chunk_index += 1
            after_pk = pk_end

        source_final = source_overall.hexdigest()
        target_final = target_overall.hexdigest()
        matched = (
            not mismatches
            and total_source == total_target
            and hmac.compare_digest(source_final, target_final)
        )
        return TableChecksumResult(
            table=table,
            matched=matched,
            source_rows=total_source,
            target_rows=total_target,
            chunks_checked=chunk_index,
            source_digest=source_final,
            target_digest=target_final,
            mismatches=tuple(mismatches),
        )


# -- 复制延迟与安全门禁（ReplicationLagGuard） --------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReplicationStatus:
    """复制链路健康与延迟快照。"""

    running: bool = True
    lag_ms: int = 0
    drained: bool = True
    stream_identity: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "lag_ms": self.lag_ms,
            "drained": self.drained,
            "stream_identity": self.stream_identity,
            "error": self.error,
        }


ReplicationProbe = Callable[[], Awaitable[ReplicationStatus]]


class ReplicationLagGuard:
    """切流安全门禁：严格校验复制延迟、排空状态、哨兵一致性与校验和有效性。"""

    def __init__(
        self,
        *,
        max_lag_ms: int = DEFAULT_MAX_LAG_MS,
        verify_max_age_sec: float = DEFAULT_VERIFY_MAX_AGE_SEC,
    ) -> None:
        if max_lag_ms < 0:
            raise DbMoveRefusedError("max_lag_ms must be >= 0")
        if verify_max_age_sec <= 0:
            raise DbMoveRefusedError("verify_max_age_sec must be > 0")
        self.max_lag_ms = max_lag_ms
        self.verify_max_age_sec = verify_max_age_sec

    def check_lag(self, status: ReplicationStatus, *, require_drained: bool = False) -> None:
        """检查复制链路是否健康、延迟未超阈且按需已排空。"""
        if not status.running:
            detail = f": {status.error}" if status.error else ""
            raise CutoverRefusedError(f"replication is not running{detail}")
        if status.lag_ms < 0:
            raise CutoverRefusedError(f"invalid negative replication lag: {status.lag_ms}ms")
        if status.lag_ms > self.max_lag_ms:
            raise CutoverRefusedError(
                f"replication lag {status.lag_ms}ms exceeds max_lag_ms={self.max_lag_ms}ms"
            )
        if require_drained and not status.drained:
            raise CutoverRefusedError("replication stream has not drained after source freeze")

    def check_sentinel(
        self,
        *,
        expected_token: str,
        source_present: bool,
        target_present: bool,
        observed_target_token: str | None = None,
    ) -> None:
        """检查源/目标哨兵是否一致到达，防止复制链路断档或目标指错库。"""
        if not expected_token:
            raise CutoverRefusedError("expected sentinel token must not be empty")
        if not source_present:
            raise CutoverRefusedError("sentinel token is missing on the source database")
        if not target_present:
            raise CutoverRefusedError("sentinel token has not replicated to the target database")
        if observed_target_token is not None and not hmac.compare_digest(observed_target_token, expected_token):
            raise CutoverRefusedError("sentinel token mismatch between source and target")

    def check_verification(
        self,
        state: MoveState,
        *,
        expected_tables: Sequence[str],
        now_ms: int,
        final_results: Mapping[str, TableChecksumResult] | None = None,
    ) -> None:
        """检查切流前校验记录是否存在、未过期，以及静默窗口最终校验是否全部通过。"""
        if state.verified_at_ms <= 0:
            raise CutoverRefusedError(f"no successful verify recorded for run {state.run_id}: run verify first")
        age_ms = now_ms - state.verified_at_ms
        max_age_ms = int(self.verify_max_age_sec * 1000)
        if age_ms > max_age_ms:
            raise CutoverRefusedError(
                f"last verification is {age_ms}ms old, exceeding verify_max_age={max_age_ms}ms"
            )
        missing_tables = [t for t in expected_tables if t not in state.verified_checksums]
        if missing_tables:
            raise CutoverRefusedError(f"verification missing table checksum(s): {', '.join(missing_tables)}")
        if final_results is not None:
            mismatched = [name for name, res in final_results.items() if not res.matched]
            if mismatched:
                raise CutoverRefusedError(
                    f"final quiet-window checksum mismatch on table(s): {', '.join(sorted(mismatched))}"
                )

    @staticmethod
    async def read_mysql_replication_status(pool: Any, *, channel: str = "dbmove") -> ReplicationStatus:
        """从目标 MySQL ``SHOW REPLICA STATUS`` / ``performance_schema`` 读取复制状态。"""
        _assert_ident(channel, "replication channel")
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT SERVICE_STATE FROM performance_schema.replication_applier_status "
                    "WHERE CHANNEL_NAME = %s",
                    (channel,),
                )
                applier_row = await cur.fetchone()
                if not applier_row:
                    return ReplicationStatus(
                        running=False,
                        lag_ms=0,
                        drained=False,
                        error=f"replication channel {channel!r} not found",
                    )
                applier_state = str(
                    applier_row[0] if not isinstance(applier_row, Mapping) else next(iter(applier_row.values()))
                ).upper()
                if applier_state != "ON":
                    return ReplicationStatus(
                        running=False,
                        lag_ms=0,
                        drained=False,
                        error=f"applier state is {applier_state}",
                    )
        return ReplicationStatus(running=True, lag_ms=0, drained=True, stream_identity=f"mysql-gtid:{channel}")


# -- 执行报告模型 -------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PlanReport:
    run_id: str
    database: str
    dry_run: bool
    phase: str
    tables: tuple[str, ...]
    plan: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "database": self.database,
            "dry_run": self.dry_run,
            "phase": self.phase,
            "tables": list(self.tables),
            "plan": self.plan,
        }


@dataclass(frozen=True, slots=True)
class BackfillReport:
    run_id: str
    dry_run: bool
    phase: str
    total_rows_copied: int
    total_chunks: int
    tables: dict[str, dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dry_run": self.dry_run,
            "phase": self.phase,
            "total_rows_copied": self.total_rows_copied,
            "total_chunks": self.total_chunks,
            "tables": self.tables,
        }


@dataclass(frozen=True, slots=True)
class VerifyReport:
    run_id: str
    dry_run: bool
    matched: bool
    verified_at_ms: int
    tables: dict[str, TableChecksumResult]

    @property
    def mismatches(self) -> list[ChunkMismatch]:
        out: list[ChunkMismatch] = []
        for res in self.tables.values():
            out.extend(res.mismatches)
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dry_run": self.dry_run,
            "matched": self.matched,
            "verified_at_ms": self.verified_at_ms,
            "tables": {k: v.to_dict() for k, v in self.tables.items()},
            "mismatch_count": len(self.mismatches),
        }


@dataclass(frozen=True, slots=True)
class CutoverReport:
    run_id: str
    dry_run: bool
    phase: str
    already_done: bool
    source_frozen: bool
    flip_marker: str
    cutover_at_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dry_run": self.dry_run,
            "phase": self.phase,
            "already_done": self.already_done,
            "source_frozen": self.source_frozen,
            "flip_marker": self.flip_marker,
            "cutover_at_ms": self.cutover_at_ms,
        }


@dataclass(frozen=True, slots=True)
class AbortReport:
    run_id: str
    dry_run: bool
    phase: str
    source_unfrozen: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dry_run": self.dry_run,
            "phase": self.phase,
            "source_unfrozen": self.source_unfrozen,
            "reason": self.reason,
        }


# -- 在线搬库异步编排器（DbMover） --------------------------------------------------------------------


FreezeHook = Callable[[MovePlan], Awaitable[int]]
UnfreezeHook = Callable[[MovePlan], Awaitable[None]]
SentinelWriter = Callable[[str, str, str, int], Awaitable[None]]
SentinelReader = Callable[[str, str, str], Awaitable[bool]]


class DbMover:
    """不停服物理库在线迁移编排器。

    支持注入异步 ``aiomysql`` 连接池、``MoveJournal``、``ReplicationLagGuard`` 与冻结/解冻/哨兵钩子，
    兼顾生产物理迁移与纯内存/假池单测。
    """

    def __init__(
        self,
        *,
        source_pool: Any = None,
        target_pool: Any = None,
        journal: MoveJournal | None = None,
        lag_guard: ReplicationLagGuard | None = None,
        replication_probe: ReplicationProbe | None = None,
        freeze_hook: FreezeHook | None = None,
        unfreeze_hook: UnfreezeHook | None = None,
        sentinel_writer: SentinelWriter | None = None,
        sentinel_reader: SentinelReader | None = None,
        clock_ms: Callable[[], int] = _now_ms,
    ) -> None:
        self.source_pool = source_pool
        self.target_pool = target_pool
        self.journal: MoveJournal = journal if journal is not None else MemoryMoveJournal()
        self.lag_guard = lag_guard
        self._replication_probe = replication_probe
        self._freeze_hook = freeze_hook
        self._unfreeze_hook = unfreeze_hook
        self._sentinel_writer = sentinel_writer
        self._sentinel_reader = sentinel_reader
        self._clock_ms = clock_ms

    def _effective_guard(self, plan: MovePlan) -> ReplicationLagGuard:
        if self.lag_guard is not None:
            return self.lag_guard
        return ReplicationLagGuard(
            max_lag_ms=plan.max_lag_ms,
            verify_max_age_sec=plan.verify_max_age_sec,
        )

    async def _probe_replication(self) -> ReplicationStatus:
        if self._replication_probe is not None:
            return await self._replication_probe()
        return ReplicationStatus(running=True, lag_ms=0, drained=True)

    async def _write_sentinel(self, run_id: str, side: str, token: str, epoch: int) -> None:
        if self._sentinel_writer is not None:
            await self._sentinel_writer(run_id, side, token, epoch)
        await self.journal.write_sentinel(run_id, side, token, epoch=epoch, now_ms=self._clock_ms())

    async def _has_sentinel(self, run_id: str, side: str, token: str) -> bool:
        if self._sentinel_reader is not None:
            return await self._sentinel_reader(run_id, side, token)
        return await self.journal.has_sentinel(run_id, side, token)

    async def plan(self, plan: MovePlan, *, dry_run: bool = False) -> PlanReport:
        """校验搬迁计划并在日志中登记 ``INIT`` 状态（``dry_run=True`` 时不落盘）。"""
        if dry_run:
            logger.info(
                "dbmove plan dry_run=true run_id=%s db=%s tables=%d",
                plan.run_id,
                plan.database,
                len(plan.tables),
            )
            return PlanReport(
                run_id=plan.run_id,
                database=plan.database,
                dry_run=True,
                phase=MovePhase.INIT.value,
                tables=plan.table_names,
                plan=plan.to_dict(),
            )
        state = await self.journal.init_run(plan, now_ms=self._clock_ms())
        logger.info(
            "dbmove plan initialized run_id=%s db=%s source=%s target=%s tables=%d",
            plan.run_id,
            plan.database,
            plan.source.endpoint,
            plan.target.endpoint,
            len(plan.tables),
        )
        return PlanReport(
            run_id=state.run_id,
            database=state.database,
            dry_run=False,
            phase=state.phase.value,
            tables=plan.table_names,
            plan=plan.to_dict(),
        )

    async def backfill(
        self,
        plan: MovePlan,
        *,
        dry_run: bool = False,
        max_chunks_per_table: int | None = None,
    ) -> BackfillReport:
        """分块回填源表数据至目标表，并在每块落盘后推进可恢复游标。

        ``max_chunks_per_table`` 允许受控分批执行（也便于验证中断后断点续传）。
        """
        if self.source_pool is None or (not dry_run and self.target_pool is None):
            raise DbMoveRefusedError("backfill requires source_pool and target_pool")

        state = await self.journal.load(plan.run_id)
        if state is None:
            if dry_run:
                state = MoveState(
                    run_id=plan.run_id,
                    database=plan.database,
                    phase=MovePhase.INIT,
                    sentinel_token=plan.sentinel_token,
                    table_cursors={spec.table: TableCursor(table=spec.table) for spec in plan.tables},
                )
            else:
                state = await self.journal.init_run(plan, now_ms=self._clock_ms())

        if state.phase in (MovePhase.DONE, MovePhase.ABORTED):
            raise DbMoveRefusedError(f"cannot backfill run {plan.run_id} in terminal phase {state.phase.value}")

        if not dry_run:
            state = await self.journal.transition(plan.run_id, MovePhase.BACKFILL, now_ms=self._clock_ms())

        total_rows = 0
        total_chunks = 0
        all_completed = True
        table_summaries: dict[str, dict[str, Any]] = {}

        for spec in plan.tables:
            cursor = state.table_cursors.get(spec.table) or TableCursor(table=spec.table)
            if cursor.completed:
                table_summaries[spec.table] = {
                    "rows_copied": cursor.rows_copied,
                    "chunks_copied": cursor.chunks_copied,
                    "completed": True,
                    "resumed_from_pk": list(cursor.last_pk) if cursor.last_pk is not None else None,
                }
                continue

            after_pk = cursor.last_pk
            rows_copied = cursor.rows_copied
            chunks_copied = cursor.chunks_copied
            chunks_this_call = 0
            completed = False

            while True:
                if max_chunks_per_table is not None and chunks_this_call >= max_chunks_per_table:
                    break
                cols, rows = await _fetch_chunk(
                    self.source_pool,
                    spec.table,
                    spec.pk_columns,
                    columns=spec.columns or None,
                    after_pk=after_pk,
                    chunk_size=plan.chunk_size,
                )
                if not rows:
                    completed = True
                    if not dry_run:
                        state = await self.journal.update_cursor(
                            plan.run_id,
                            spec.table,
                            last_pk=after_pk,
                            rows_copied=rows_copied,
                            chunks_copied=chunks_copied,
                            completed=True,
                            now_ms=self._clock_ms(),
                        )
                    break

                if not dry_run:
                    await self._upsert_chunk(spec.table, cols, rows)

                after_pk = _pk_tuple(rows[-1], spec.pk_columns)
                rows_copied += len(rows)
                chunks_copied += 1
                chunks_this_call += 1
                total_rows += len(rows)
                total_chunks += 1

                if not dry_run:
                    state = await self.journal.update_cursor(
                        plan.run_id,
                        spec.table,
                        last_pk=after_pk,
                        rows_copied=rows_copied,
                        chunks_copied=chunks_copied,
                        completed=False,
                        now_ms=self._clock_ms(),
                    )

            if not completed:
                all_completed = False
            table_summaries[spec.table] = {
                "rows_copied": rows_copied,
                "chunks_copied": chunks_copied,
                "completed": completed,
                "last_pk": list(after_pk) if after_pk is not None else None,
            }

        final_phase = MovePhase.BACKFILL
        if all_completed and not dry_run:
            state = await self.journal.transition(plan.run_id, MovePhase.REPLICATE, now_ms=self._clock_ms())
            final_phase = state.phase
            await self.journal.append_event(
                plan.run_id,
                "backfill",
                f"completed backfill rows={total_rows} chunks={total_chunks}",
                now_ms=self._clock_ms(),
            )
        elif all_completed:
            final_phase = MovePhase.REPLICATE

        logger.info(
            "dbmove backfill run_id=%s dry_run=%s phase=%s rows=%d chunks=%d",
            plan.run_id,
            dry_run,
            final_phase.value,
            total_rows,
            total_chunks,
        )
        return BackfillReport(
            run_id=plan.run_id,
            dry_run=dry_run,
            phase=final_phase.value,
            total_rows_copied=total_rows,
            total_chunks=total_chunks,
            tables=table_summaries,
        )

    async def _upsert_chunk(
        self,
        table: str,
        columns: Sequence[str],
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        """幂等写入目标分块（``INSERT INTO ... ON DUPLICATE KEY UPDATE``）。"""
        if not rows:
            return
        quoted_table = _quote_ident(table)
        quoted_cols = [_quote_ident(c) for c in columns]
        placeholders = ", ".join("%s" for _ in columns)
        updates = ", ".join(f"{col} = VALUES({col})" for col in quoted_cols)
        sql = (
            f"INSERT INTO {quoted_table} ({', '.join(quoted_cols)}) "
            f"VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        )
        async with self.target_pool.acquire() as conn:
            async with conn.cursor() as cur:
                for row in rows:
                    await cur.execute(sql, tuple(row[c] for c in columns))

    async def verify(self, plan: MovePlan, *, dry_run: bool = False) -> VerifyReport:
        """对计划内全部表执行确定性分块校验，通过后记录摘要与时间戳。"""
        if self.source_pool is None or self.target_pool is None:
            raise DbMoveRefusedError("verify requires both source_pool and target_pool")

        state = await self.journal.load(plan.run_id)
        if state is None and not dry_run:
            state = await self.journal.init_run(plan, now_ms=self._clock_ms())
        if state is not None and state.phase in (MovePhase.DONE, MovePhase.ABORTED):
            raise DbMoveRefusedError(f"cannot verify run {plan.run_id} in terminal phase {state.phase.value}")

        if not dry_run:
            await self.journal.transition(plan.run_id, MovePhase.VERIFY, now_ms=self._clock_ms())

        results: dict[str, TableChecksumResult] = {}
        all_matched = True
        for spec in plan.tables:
            res = await ChunkChecksum.verify_table_checksum(
                self.source_pool,
                self.target_pool,
                spec.table,
                spec.pk_columns,
                columns=spec.columns or None,
                chunk_size=plan.chunk_size,
            )
            results[spec.table] = res
            if not res.matched:
                all_matched = False

        now_ms = self._clock_ms()
        if not dry_run:
            if all_matched:
                checksums = {name: res.source_digest for name, res in results.items()}
                await self.journal.record_verification(plan.run_id, checksums, verified_at_ms=now_ms)
                await self.journal.append_event(
                    plan.run_id,
                    "verify",
                    f"all {len(results)} table(s) matched",
                    now_ms=now_ms,
                )
            else:
                await self.journal.clear_verification(
                    plan.run_id,
                    reason="checksum mismatch detected during verify",
                    now_ms=now_ms,
                )

        logger.info(
            "dbmove verify run_id=%s dry_run=%s matched=%s tables=%d",
            plan.run_id,
            dry_run,
            all_matched,
            len(results),
        )
        return VerifyReport(
            run_id=plan.run_id,
            dry_run=dry_run,
            matched=all_matched,
            verified_at_ms=now_ms if all_matched else 0,
            tables=results,
        )

    async def cutover(self, plan: MovePlan, *, dry_run: bool = False) -> CutoverReport:
        """执行安全切流窗口：前置门禁 -> 冻结源库 -> 哨兵核验 -> 静默期最终校验 -> 切流完成。

        任一安全门禁触发时：
        - 冻结前失败：直接拒绝，源库未冻结；
        - 冻结后失败：自动解冻源库恢复业务写入，若因复制异常或静默比对不一致则同时清空校验记录。
        """
        state = await self.journal.load(plan.run_id)
        if state is None:
            raise CutoverRefusedError(f"move run not found: {plan.run_id}")

        marker = flip_marker_token(plan.run_id)
        if state.phase == MovePhase.DONE:
            return CutoverReport(
                run_id=plan.run_id,
                dry_run=dry_run,
                phase=MovePhase.DONE.value,
                already_done=True,
                source_frozen=state.frozen,
                flip_marker=marker,
                cutover_at_ms=state.cutover_at_ms,
            )
        if state.phase == MovePhase.ABORTED:
            raise CutoverRefusedError(f"run {plan.run_id} was aborted and cannot cut over")

        guard = self._effective_guard(plan)
        now_ms = self._clock_ms()

        # 1. 冻结前预检：校验新鲜度 + 复制延迟（失败不碰冻结闸门）。
        guard.check_verification(
            state,
            expected_tables=plan.table_names,
            now_ms=now_ms,
        )
        pre_status = await self._probe_replication()
        guard.check_lag(pre_status, require_drained=False)

        if dry_run:
            # 干跑模式只做只读哨兵与校验核对，不冻结源库也不推进状态机。
            if self.source_pool is not None and self.target_pool is not None:
                final_results = {
                    spec.table: await ChunkChecksum.verify_table_checksum(
                        self.source_pool,
                        self.target_pool,
                        spec.table,
                        spec.pk_columns,
                        columns=spec.columns or None,
                        chunk_size=plan.chunk_size,
                    )
                    for spec in plan.tables
                }
                guard.check_verification(
                    state,
                    expected_tables=plan.table_names,
                    now_ms=self._clock_ms(),
                    final_results=final_results,
                )
            return CutoverReport(
                run_id=plan.run_id,
                dry_run=True,
                phase=state.phase.value,
                already_done=False,
                source_frozen=state.frozen,
                flip_marker=marker,
                cutover_at_ms=0,
            )

        # 2. 进入切换窗口：冻结源库并记录 FREEZE 阶段。
        epoch = state.writer_epoch + 1
        froze_source = False
        try:
            if plan.freeze_gate and self._freeze_hook is not None:
                epoch = await self._freeze_hook(plan)
            froze_source = plan.freeze_gate
            await self.journal.transition(
                plan.run_id,
                MovePhase.FREEZE,
                frozen=froze_source,
                writer_epoch=epoch,
                now_ms=self._clock_ms(),
            )
            await self.journal.append_event(
                plan.run_id,
                "freeze",
                f"source frozen epoch={epoch}",
                now_ms=self._clock_ms(),
            )

            # 3. 写入源库哨兵并校验目标库已同步收到同一哨兵。
            await self._write_sentinel(plan.run_id, "source", plan.sentinel_token, epoch)
            source_has = await self._has_sentinel(plan.run_id, "source", plan.sentinel_token)
            target_has = await self._has_sentinel(plan.run_id, "target", plan.sentinel_token)
            guard.check_sentinel(
                expected_token=plan.sentinel_token,
                source_present=source_has,
                target_present=target_has,
            )

            # 4. 冻结后复核复制排空与延迟。
            post_status = await self._probe_replication()
            guard.check_lag(post_status, require_drained=True)

            # 5. 静默窗口最终全表分块校验（防止 verify 后复制丢写或目标被误写）。
            if self.source_pool is not None and self.target_pool is not None:
                final_results = {
                    spec.table: await ChunkChecksum.verify_table_checksum(
                        self.source_pool,
                        self.target_pool,
                        spec.table,
                        spec.pk_columns,
                        columns=spec.columns or None,
                        chunk_size=plan.chunk_size,
                    )
                    for spec in plan.tables
                }
                guard.check_verification(
                    state,
                    expected_tables=plan.table_names,
                    now_ms=self._clock_ms(),
                    final_results=final_results,
                )

            # 6. 提交切流：FREEZE -> CUTOVER -> DONE，并在目标写入翻转标记。
            await self.journal.transition(plan.run_id, MovePhase.CUTOVER, now_ms=self._clock_ms())
            await self._write_sentinel(plan.run_id, "target", marker, epoch)
            done_state = await self.journal.transition(
                plan.run_id,
                MovePhase.DONE,
                frozen=froze_source,
                now_ms=self._clock_ms(),
            )
            await self.journal.append_event(
                plan.run_id,
                "cutover",
                f"cutover completed flip_marker={marker}",
                now_ms=self._clock_ms(),
            )
            logger.info(
                "dbmove cutover completed run_id=%s db=%s epoch=%d",
                plan.run_id,
                plan.database,
                epoch,
            )
            return CutoverReport(
                run_id=plan.run_id,
                dry_run=False,
                phase=done_state.phase.value,
                already_done=False,
                source_frozen=done_state.frozen,
                flip_marker=marker,
                cutover_at_ms=done_state.cutover_at_ms,
            )
        except Exception as exc:
            # 窗口回滚：未提交翻转前必须先解冻源库恢复业务写入；若因数据/复制异常则清空校验记录。
            rollback_reason = str(exc)
            if isinstance(exc, CutoverRefusedError) and (
                "checksum" in rollback_reason.lower()
                or "not running" in rollback_reason.lower()
                or "sentinel" in rollback_reason.lower()
            ):
                await self.journal.clear_verification(
                    plan.run_id,
                    reason=f"cutover window aborted: {rollback_reason}",
                    now_ms=self._clock_ms(),
                )
            if froze_source:
                try:
                    if self._unfreeze_hook is not None:
                        await self._unfreeze_hook(plan)
                except Exception as unfreeze_exc:
                    raise DbMoveUnknownError(
                        f"source remains frozen after cutover abort ({rollback_reason}); "
                        f"unfreeze failed: {unfreeze_exc}"
                    ) from unfreeze_exc
                await self.journal.transition(
                    plan.run_id,
                    MovePhase.REPLICATE,
                    frozen=False,
                    error=rollback_reason,
                    now_ms=self._clock_ms(),
                )
                await self.journal.append_event(
                    plan.run_id,
                    "cutover_rollback",
                    f"source unfrozen after cutover failure: {rollback_reason}",
                    now_ms=self._clock_ms(),
                )
            logger.warning("dbmove cutover aborted run_id=%s reason=%s", plan.run_id, rollback_reason)
            raise

    async def abort(
        self,
        plan: MovePlan,
        *,
        reason: str = "operator abort",
        dry_run: bool = False,
    ) -> AbortReport:
        """中止尚未切流的搬迁并安全解冻源库；若已切流或目标存在翻转标记则严格拒绝（防脑裂）。"""
        state = await self.journal.load(plan.run_id)
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {plan.run_id}")
        if state.phase == MovePhase.DONE:
            raise DbMoveRefusedError(
                f"run {plan.run_id} already completed cutover; cannot abort after target started taking writes"
            )
        marker = flip_marker_token(plan.run_id)
        if await self._has_sentinel(plan.run_id, "target", marker):
            raise DbMoveRefusedError(
                f"target already has flip marker for run {plan.run_id}; refusing to abort and unfreeze source"
            )
        if state.phase == MovePhase.ABORTED:
            return AbortReport(
                run_id=plan.run_id,
                dry_run=dry_run,
                phase=MovePhase.ABORTED.value,
                source_unfrozen=not state.frozen,
                reason=state.error or reason,
            )

        if dry_run:
            return AbortReport(
                run_id=plan.run_id,
                dry_run=True,
                phase=state.phase.value,
                source_unfrozen=not state.frozen,
                reason=reason,
            )

        unfrozen = False
        if state.frozen:
            if self._unfreeze_hook is not None:
                await self._unfreeze_hook(plan)
            unfrozen = True

        aborted = await self.journal.transition(
            plan.run_id,
            MovePhase.ABORTED,
            frozen=False,
            error=reason,
            now_ms=self._clock_ms(),
        )
        await self.journal.append_event(
            plan.run_id,
            "abort",
            f"move aborted: {reason}",
            now_ms=self._clock_ms(),
        )
        logger.info("dbmove aborted run_id=%s reason=%s unfrozen=%s", plan.run_id, reason, unfrozen)
        return AbortReport(
            run_id=plan.run_id,
            dry_run=False,
            phase=aborted.phase.value,
            source_unfrozen=unfrozen,
            reason=reason,
        )

    async def status(self, run_id: str) -> dict[str, Any]:
        """读取指定 ``run_id`` 的完整状态快照。"""
        state = await self.journal.load(run_id)
        if state is None:
            raise DbMoveRefusedError(f"move run not found: {run_id}")
        return state.to_dict()


# -- CLI 与本地 JSON 日志桥接 -------------------------------------------------------------------------


class FileMoveJournal(MemoryMoveJournal):
    """基于本地 JSON 文件的轻量 ``MoveJournal``，供 CLI 离线/无控制表场景持久化状态。"""

    def __init__(self, path: Path) -> None:
        super().__init__()
        self._path = path
        if self._path.is_file():
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            for run_id, state_dict in dict(raw.get("states") or {}).items():
                self._states[run_id] = MoveState.from_dict(state_dict)
            for key, tokens in dict(raw.get("sentinels") or {}).items():
                run_id, _, side = key.partition("|")
                self._sentinels[(run_id, side)] = set(tokens)

    def _flush(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "states": {k: v.to_dict() for k, v in self._states.items()},
            "sentinels": {f"{k[0]}|{k[1]}": sorted(v) for k, v in self._sentinels.items()},
        }
        self._path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    async def init_run(self, plan: MovePlan, *, now_ms: int | None = None) -> MoveState:
        state = await super().init_run(plan, now_ms=now_ms)
        self._flush()
        return state

    async def transition(self, run_id: str, phase: MovePhase, **kwargs: Any) -> MoveState:
        state = await super().transition(run_id, phase, **kwargs)
        self._flush()
        return state

    async def update_cursor(self, run_id: str, table: str, **kwargs: Any) -> MoveState:
        state = await super().update_cursor(run_id, table, **kwargs)
        self._flush()
        return state

    async def record_verification(self, run_id: str, checksums: Mapping[str, str], *, verified_at_ms: int) -> MoveState:
        state = await super().record_verification(run_id, checksums, verified_at_ms=verified_at_ms)
        self._flush()
        return state

    async def clear_verification(self, run_id: str, **kwargs: Any) -> MoveState:
        state = await super().clear_verification(run_id, **kwargs)
        self._flush()
        return state

    async def append_event(self, run_id: str, step: str, message: str, *, now_ms: int | None = None) -> None:
        await super().append_event(run_id, step, message, now_ms=now_ms)
        self._flush()

    async def write_sentinel(self, run_id: str, side: str, token: str, **kwargs: Any) -> None:
        await super().write_sentinel(run_id, side, token, **kwargs)
        self._flush()


def _parse_table_specs(raw_tables: str) -> tuple[TableMoveSpec, ...]:
    """解析 ``table1:id,table2:pk1+pk2`` 格式的表与主键声明。"""
    specs: list[TableMoveSpec] = []
    for item in raw_tables.split(","):
        cleaned = item.strip()
        if not cleaned:
            continue
        if ":" in cleaned:
            table_name, _, pk_raw = cleaned.partition(":")
            pks = tuple(p.strip() for p in pk_raw.split("+") if p.strip())
        else:
            table_name, pks = cleaned, ("id",)
        specs.append(TableMoveSpec(table=table_name.strip(), pk_columns=pks))
    if not specs:
        raise DbMoveRefusedError("--tables must specify at least one table")
    return tuple(specs)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dbmove",
        description="Online physical database migration tool (plan / backfill / verify / cutover / abort / status).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    for cmd in ("plan", "backfill", "verify", "cutover", "abort", "status"):
        p = sub.add_parser(cmd)
        p.add_argument("--run-id", default="", help="32-char hex migration run ID")
        p.add_argument("--source-dsn", default="", help="Source MySQL DSN (never echoed)")
        p.add_argument("--target-dsn", default="", help="Target MySQL DSN (never echoed)")
        p.add_argument("--tables", default="", help="Comma-separated tables, e.g. users:id,orders:zone_id+order_id")
        p.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Rows per chunk")
        p.add_argument("--sentinel-token", default="", help="Sentinel verification token")
        p.add_argument("--max-lag-ms", type=int, default=DEFAULT_MAX_LAG_MS, help="Max allowed replication lag (ms)")
        p.add_argument("--no-freeze", action="store_true", help="Disable source write freeze gate")
        p.add_argument("--journal-file", default="", help="Optional local JSON journal path")
        p.add_argument("--reason", default="operator abort", help="Abort reason")
        p.add_argument("--dry-run", action="store_true", help="Validate and simulate without mutating state")
        p.add_argument("--json", action="store_true", dest="as_json", help="Emit JSON output")
    return parser


async def _open_pool_from_side(side: SideConfig):
    import aiomysql

    return await aiomysql.create_pool(
        host=side.host,
        port=side.port,
        user=side.user,
        password=side.password,
        db=side.database,
        minsize=1,
        maxsize=4,
        autocommit=True,
        charset="utf8mb4",
    )


async def _run_cli(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    journal: MoveJournal = (
        FileMoveJournal(Path(args.journal_file)) if args.journal_file else MemoryMoveJournal()
    )
    if args.command == "status":
        if not args.run_id:
            raise DbMoveRefusedError("status requires --run-id")
        mover = DbMover(journal=journal)
        return EXIT_OK, await mover.status(args.run_id)

    if not args.source_dsn or not args.target_dsn:
        raise DbMoveRefusedError(f"{args.command} requires --source-dsn and --target-dsn")
    source = SideConfig.from_dsn(args.source_dsn, role="source")
    target = SideConfig.from_dsn(args.target_dsn, role="target")
    tables = _parse_table_specs(args.tables)
    plan = MovePlan(
        source=source,
        target=target,
        tables=tables,
        run_id=args.run_id or new_run_id(),
        chunk_size=args.chunk_size,
        sentinel_token=args.sentinel_token or new_sentinel_token(),
        freeze_gate=not args.no_freeze,
        max_lag_ms=args.max_lag_ms,
    )

    source_pool = None
    target_pool = None
    need_pools = args.command in ("backfill", "verify") or (
        args.command == "cutover" and not args.dry_run
    )
    try:
        if need_pools:
            source_pool = await _open_pool_from_side(source)
            target_pool = await _open_pool_from_side(target)
        mover = DbMover(
            source_pool=source_pool,
            target_pool=target_pool,
            journal=journal,
        )
        if args.command == "plan":
            rep = await mover.plan(plan, dry_run=args.dry_run)
            return EXIT_OK, rep.to_dict()
        if args.command == "backfill":
            rep = await mover.backfill(plan, dry_run=args.dry_run)
            return EXIT_OK, rep.to_dict()
        if args.command == "verify":
            rep = await mover.verify(plan, dry_run=args.dry_run)
            return (EXIT_OK if rep.matched else EXIT_REFUSED), rep.to_dict()
        if args.command == "cutover":
            rep = await mover.cutover(plan, dry_run=args.dry_run)
            return EXIT_OK, rep.to_dict()
        if args.command == "abort":
            rep = await mover.abort(plan, reason=args.reason, dry_run=args.dry_run)
            return EXIT_OK, rep.to_dict()
        raise DbMoveRefusedError(f"unsupported command: {args.command}")
    finally:
        for pool in (source_pool, target_pool):
            if pool is not None:
                pool.close()
                await pool.wait_closed()


def main(argv: Sequence[str] | None = None) -> int:
    """``dbmove`` 命令行入口，返回标准退出码（0 成功 / 1 拒绝 / 2 失败 / 3 未知）。"""
    parser = _build_parser()
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else EXIT_REFUSED
        return EXIT_OK if code == 0 else EXIT_REFUSED

    try:
        code, payload = asyncio.run(_run_cli(args))
    except DbMoveError as exc:
        if getattr(args, "as_json", False):
            print(json.dumps({"ok": False, "error": str(exc), "exit_code": exc.exit_code}, ensure_ascii=False))
        else:
            print(f"ERROR: {exc}", file=sys.stderr)
        return exc.exit_code
    except Exception as exc:
        if getattr(args, "as_json", False):
            print(json.dumps({"ok": False, "error": str(exc), "exit_code": EXIT_FAILED}, ensure_ascii=False))
        else:
            print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_FAILED

    if args.as_json:
        print(json.dumps({"ok": code == EXIT_OK, **payload}, ensure_ascii=False))
    else:
        print(f"[{args.command}] run_id={payload.get('run_id', '')} phase={payload.get('phase', '')} ok={code == EXIT_OK}")
    return code


__all__ = [
    "ALLOWED_PHASE_TRANSITIONS",
    "AbortReport",
    "BackfillReport",
    "ChecksumMismatchError",
    "ChunkChecksum",
    "ChunkMismatch",
    "CutoverRefusedError",
    "CutoverReport",
    "DEFAULT_CHUNK_SIZE",
    "DEFAULT_JOURNAL_TABLE",
    "DEFAULT_MAX_LAG_MS",
    "DEFAULT_SENTINEL_TABLE",
    "DEFAULT_VERIFY_MAX_AGE_SEC",
    "DbMoveError",
    "DbMoveRefusedError",
    "DbMoveUnknownError",
    "DbMover",
    "EXIT_FAILED",
    "EXIT_OK",
    "EXIT_REFUSED",
    "EXIT_UNKNOWN",
    "FileMoveJournal",
    "MemoryMoveJournal",
    "MoveJournal",
    "MovePhase",
    "MovePlan",
    "MoveState",
    "PlanReport",
    "ReplicationLagGuard",
    "ReplicationStatus",
    "SideConfig",
    "SqlMoveJournal",
    "TableChecksumResult",
    "TableCursor",
    "TableMoveSpec",
    "VerifyReport",
    "compute_rows_checksum",
    "flip_marker_token",
    "main",
    "new_run_id",
    "new_sentinel_token",
]
