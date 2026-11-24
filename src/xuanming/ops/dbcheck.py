"""只读数据库容量、Schema 登记清单、清理索引前导列与大字段体检工具（对应 Go ``tools/dbcheck``）。

红线约束：
1. 本模块严格**只读**，每条 SQL 执行前均通过只读守卫断言（仅允许 ``SELECT``），永不执行任何
   ``DELETE`` / ``UPDATE`` / ``INSERT`` / ``TRUNCATE`` / ``ALTER`` / ``DROP``；
2. 离线模式（``--offline``）基于 ``KNOWN_SCHEMA_FILES`` 与 ``parse_schema_file`` 校验全部
   ``*_schema.sql``、表登记清单与清理路径索引前导列；
3. 在线模式基于 ``aiomysql`` 连接池执行广度体检、精确 outbox 计数、大字段 ``MAX/AVG LENGTH``
   及 ``top_rows`` 定位、待清理量上界 ``SELECT COUNT(*)``，以及 JSON 快照写入与增量对比。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit

from .schemamigrate import (
    KNOWN_SCHEMA_FILES,
    DatabaseMigrationPlan,
    SchemaValidationError,
    TablePlan,
    parse_schema_file,
)

logger = logging.getLogger(__name__)

CLASS_BOUNDED = "bounded"
CLASS_SWEPT = "swept"
CLASS_OUTBOX = "outbox"
CLASS_EXEMPT = "exempt"
_VALID_TABLE_CLASSES = frozenset({CLASS_BOUNDED, CLASS_SWEPT, CLASS_OUTBOX, CLASS_EXEMPT})

DEFAULT_OUTBOX_MAX = 200
DEFAULT_AVG_ROW_WARN = 64 * 1024
DEFAULT_SCHEMA_LIKE = "mmorpg%"

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_WRITE_KEYWORD_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|REPLACE|TRUNCATE|DROP|ALTER|CREATE|GRANT|REVOKE|LOCK|CALL|SET|LOAD|INTO\s+OUTFILE)\b",
    re.IGNORECASE,
)
_GO_MYSQL_DSN_RE = re.compile(
    r"^(?:(?P<user>[^:@/]*)(?::(?P<password>[^@/]*))?@)?tcp\((?P<host>[^):]+)(?::(?P<port>\d+))?\)/(?P<database>[^?]*)"
)

# 各域逻辑库共用的框架与权威元数据表，不计入业务未登记表。
IGNORED_TABLES: frozenset[str] = frozenset(
    {
        "schema_migrations",
        "schemamigrate_history",
        "db_move_sentinel",
        "route_meta",
        "writer_registry",
        "writer_slot",
        "writer_lifecycle_receipt",
        "sql_writer_drain_run",
        "sql_writer_freeze_ticket",
        "sql_writer_maintenance_receipt",
        "role_fence",
    }
)


@dataclass(frozen=True, slots=True)
class RequiredIndexSpec:
    """清理路径必需索引的名称与严格前导列顺序。"""

    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TableRegistryEntry:
    """表登记清单项：分类、清理必需索引与只读待清理统计条件。"""

    table_class: str
    required_indexes: tuple[RequiredIndexSpec, ...] = ()
    pending_where: str = ""


@dataclass(frozen=True, slots=True)
class FieldBudget:
    """单列大字段字节预算配置。"""

    db: str
    table: str
    column: str
    pk: str
    max_bytes: int
    why: str = ""


@dataclass(frozen=True, slots=True)
class LargeRow:
    """大字段 Top-N 定位单条记录。"""

    pk: str
    size_bytes: int

    def to_dict(self) -> dict[str, object]:
        return {"pk": self.pk, "bytes": self.size_bytes}


@dataclass(frozen=True, slots=True)
class BigFieldReport:
    """单个大字段列的深度体检结论。"""

    db: str
    table: str
    column: str
    pk: str
    rows: int
    max_bytes: int
    avg_bytes: int
    budget_bytes: int
    over_rows: int
    status: str  # "OK" | "WARN" | "FAIL"
    top_rows: tuple[LargeRow, ...] = ()
    error: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "target": f"{self.db}.{self.table}.{self.column}",
            "db": self.db,
            "table": self.table,
            "column": self.column,
            "pk": self.pk,
            "rows": self.rows,
            "max_bytes": self.max_bytes,
            "avg_bytes": self.avg_bytes,
            "budget_bytes": self.budget_bytes,
            "over_rows": self.over_rows,
            "status": self.status,
            "top_rows": [r.to_dict() for r in self.top_rows],
            "error": self.error,
        }


@dataclass(frozen=True, slots=True)
class PendingReport:
    """单张清理表的只读待清理上界统计结果。"""

    db: str
    table: str
    pending_rows: int
    error: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "target": f"{self.db}.{self.table}",
            "db": self.db,
            "table": self.table,
            "pending_rows": self.pending_rows,
            "error": self.error,
        }


@dataclass(frozen=True, slots=True)
class SnapshotDelta:
    """两次行数快照对比的逐表增量。"""

    key: str
    before: int
    after: int
    delta: int

    def to_dict(self) -> dict[str, object]:
        return {
            "table": self.key,
            "before": self.before,
            "after": self.after,
            "delta": self.delta,
        }


@dataclass(frozen=True, slots=True)
class DbCheckReport:
    """``dbcheck`` 只读体检汇总报告。"""

    mode: str  # "offline" | "live"
    passed: bool
    checked_tables: int
    violations: tuple[str, ...]
    warnings: tuple[str, ...]
    snapshot: dict[str, int]
    deltas: tuple[SnapshotDelta, ...] = ()
    big_fields: tuple[BigFieldReport, ...] = ()
    pending: tuple[PendingReport, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "passed": self.passed,
            "checked_tables": self.checked_tables,
            "violation_count": len(self.violations),
            "violations": list(self.violations),
            "warnings": list(self.warnings),
            "snapshot": dict(sorted(self.snapshot.items())),
            "deltas": [d.to_dict() for d in self.deltas],
            "big_fields": [bf.to_dict() for bf in self.big_fields],
            "pending": [p.to_dict() for p in self.pending],
        }


# 默认表登记清单（对齐 Go tools/dbcheck/main.go defaultRegistry）。
DEFAULT_REGISTRY: dict[str, dict[str, TableRegistryEntry]] = {
    "mmorpg_player_inbox": {
        "player_inbox_stream": TableRegistryEntry(CLASS_BOUNDED),
        "player_inbox_item": TableRegistryEntry(CLASS_EXEMPT),
        "player_inbox_audit": TableRegistryEntry(CLASS_EXEMPT),
    },
    "mmorpg_chat": {
        "chat_send_authority": TableRegistryEntry(CLASS_SWEPT, pending_where="accepted_at < NOW()"),
        "chat_policy": TableRegistryEntry(CLASS_BOUNDED),
        "chat_world_retraction": TableRegistryEntry(CLASS_SWEPT, pending_where="accepted_at < NOW()"),
        "chat_report": TableRegistryEntry(CLASS_SWEPT, pending_where="accepted_at < NOW()"),
        "chat_repeat_event": TableRegistryEntry(CLASS_SWEPT),
        "chat_private_daily_target": TableRegistryEntry(CLASS_SWEPT),
    },
    "mmorpg_friend": {
        "friend": TableRegistryEntry(CLASS_BOUNDED),
        "friend_request": TableRegistryEntry(CLASS_SWEPT),
        "friend_capacity": TableRegistryEntry(CLASS_BOUNDED),
        "friend_block": TableRegistryEntry(CLASS_BOUNDED),
    },
    "mmorpg_mail": {
        "mail_message": TableRegistryEntry(CLASS_SWEPT),
        "mail_state": TableRegistryEntry(CLASS_SWEPT),
        "mail_campaign": TableRegistryEntry(CLASS_SWEPT),
        "mail_campaign_shard": TableRegistryEntry(CLASS_SWEPT),
        "mail_campaign_recipient": TableRegistryEntry(CLASS_SWEPT),
        "mail_box": TableRegistryEntry(CLASS_BOUNDED),
        "mail_player_op_seq": TableRegistryEntry(CLASS_BOUNDED),
        "mail_asset_op": TableRegistryEntry(CLASS_SWEPT),
    },
    "mmorpg_guild": {
        "guild": TableRegistryEntry(CLASS_BOUNDED),
        "guild_member": TableRegistryEntry(CLASS_BOUNDED),
        "guild_trial_lobby": TableRegistryEntry(CLASS_SWEPT),
        "guild_trial_lobby_slot": TableRegistryEntry(CLASS_SWEPT),
        "guild_trial_launch": TableRegistryEntry(CLASS_SWEPT),
        "guild_trial_launch_role": TableRegistryEntry(CLASS_SWEPT),
        "guild_player_state": TableRegistryEntry(CLASS_BOUNDED),
        "guild_application": TableRegistryEntry(CLASS_SWEPT),
        "guild_player_op_seq": TableRegistryEntry(CLASS_BOUNDED),
        "guild_asset_op": TableRegistryEntry(CLASS_SWEPT),
        "guild_daily_counter": TableRegistryEntry(CLASS_SWEPT),
        "guild_activity_progress": TableRegistryEntry(CLASS_BOUNDED),
        "guild_trial_battle": TableRegistryEntry(CLASS_SWEPT),
        "guild_trial_reward_owed": TableRegistryEntry(CLASS_SWEPT),
        "guild_client_request": TableRegistryEntry(CLASS_SWEPT),
        "guild_name_hold": TableRegistryEntry(CLASS_SWEPT),
    },
}

# 默认大字段预算（对齐 Go tools/dbcheck/bigfield.go defaultBigFields）。
DEFAULT_BIG_FIELDS: tuple[FieldBudget, ...] = (
    FieldBudget(
        db="mmorpg_player_inbox",
        table="player_inbox_item",
        column="payload",
        pk="seq",
        max_bytes=64 * 1024,
        why="player_inbox_item.payload should stay bounded by inbox registry max payload limits",
    ),
    FieldBudget(
        db="mmorpg_chat",
        table="chat_send_authority",
        column="request_payload",
        pk="request_id",
        max_bytes=16 * 1024,
        why="chat_send_authority.request_payload is MEDIUMBLOB but chat messages should remain under 16KiB",
    ),
    FieldBudget(
        db="mmorpg_chat",
        table="chat_send_authority",
        column="message_payload",
        pk="request_id",
        max_bytes=16 * 1024,
        why="chat_send_authority.message_payload is MEDIUMBLOB but serialized chat frames should remain under 16KiB",
    ),
)


def _assert_identifier(value: str, label: str) -> str:
    if not _IDENT_RE.match(value):
        raise ValueError(f"dbcheck: invalid SQL identifier for {label}: {value!r}")
    return value


def _assert_read_only_sql(sql: str) -> None:
    """硬只读守卫：禁止任何非 SELECT 语句或写关键字进入数据库游标。"""
    stripped = sql.strip()
    if not stripped.upper().startswith("SELECT "):
        raise ValueError(f"dbcheck read-only guard blocked non-SELECT statement: {stripped[:80]!r}")
    if ";" in stripped:
        raise ValueError("dbcheck read-only guard blocked multi-statement SQL")
    if _WRITE_KEYWORD_RE.search(stripped):
        raise ValueError(f"dbcheck read-only guard blocked write keyword in SQL: {stripped[:80]!r}")


async def _execute_readonly(cursor, sql: str, args=None) -> None:
    _assert_read_only_sql(sql)
    if args is None:
        await cursor.execute(sql)
    else:
        await cursor.execute(sql, args)


def _decode_str(val: object) -> str:
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8")
    return "" if val is None else str(val)


def index_columns_match_prefix(actual_columns: Sequence[str], required_prefix: Sequence[str]) -> bool:
    """严格前导列顺序匹配：索引前 ``len(required_prefix)`` 列必须与目标逐列一致。"""
    if not required_prefix:
        return True
    if len(actual_columns) < len(required_prefix):
        return False
    return tuple(actual_columns[: len(required_prefix)]) == tuple(required_prefix)


def write_snapshot(path: Path | str, snapshot: Mapping[str, int]) -> None:
    """将表行数快照按确定性顺序写入 JSON 文件。"""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    ordered = {k: int(snapshot[k]) for k in sorted(snapshot)}
    target.write_text(json.dumps(ordered, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_snapshot(path: Path | str) -> dict[str, int]:
    """从 JSON 文件读取表行数快照。"""
    target = Path(path)
    raw = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"dbcheck: snapshot file must contain a JSON object: {target}")
    return {str(k): int(v) for k, v in raw.items()}


def compare_snapshots(before: Mapping[str, int], current: Mapping[str, int]) -> tuple[SnapshotDelta, ...]:
    """比对两份表行数快照，返回所有发生变化的表增量（按表全名升序）。"""
    deltas: list[SnapshotDelta] = []
    for key in sorted(set(before.keys()) | set(current.keys())):
        old_val = int(before.get(key, 0))
        new_val = int(current.get(key, 0))
        diff = new_val - old_val
        if diff != 0:
            deltas.append(SnapshotDelta(key=key, before=old_val, after=new_val, delta=diff))
    return tuple(deltas)


def load_registry_file(path: Path | str) -> dict[str, dict[str, TableRegistryEntry]]:
    """从 JSON 文件加载自定义表登记清单。"""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("dbcheck: registry JSON root must be an object mapping db -> tables")
    registry: dict[str, dict[str, TableRegistryEntry]] = {}
    for db_name, tables in raw.items():
        if not isinstance(tables, dict):
            raise ValueError(f"dbcheck: registry entry for {db_name!r} must be an object")
        db_entries: dict[str, TableRegistryEntry] = {}
        for table_name, spec in tables.items():
            if not isinstance(spec, dict):
                raise ValueError(f"dbcheck: registry spec for {db_name}.{table_name} must be an object")
            table_class = str(spec.get("class", CLASS_BOUNDED))
            if table_class not in _VALID_TABLE_CLASSES:
                raise ValueError(
                    f"dbcheck: invalid table class {table_class!r} for {db_name}.{table_name}"
                )
            req_indexes: list[RequiredIndexSpec] = []
            for item in spec.get("required_indexes", ()):
                req_indexes.append(
                    RequiredIndexSpec(
                        name=str(item["name"]),
                        columns=tuple(str(c) for c in item["columns"]),
                    )
                )
            pending_where = str(spec.get("pending_where", ""))
            db_entries[str(table_name)] = TableRegistryEntry(
                table_class=table_class,
                required_indexes=tuple(req_indexes),
                pending_where=pending_where,
            )
        registry[str(db_name)] = db_entries
    return registry


def _lookup_db_registry(
    registry: Mapping[str, Mapping[str, TableRegistryEntry]],
    db_name: str,
) -> Mapping[str, TableRegistryEntry] | None:
    if db_name in registry:
        return registry[db_name]
    prefixed = f"mmorpg_{db_name}"
    if prefixed in registry:
        return registry[prefixed]
    if db_name.startswith("mmorpg_"):
        bare = db_name[len("mmorpg_") :]
        if bare in registry:
            return registry[bare]
    return None


def _check_table_index_offline(table: TablePlan, spec: RequiredIndexSpec) -> tuple[bool, str]:
    if spec.name.upper() == "PRIMARY":
        actual = table.primary_key
        if index_columns_match_prefix(actual, spec.columns):
            return True, ""
        return False, f"actual PRIMARY columns {list(actual)}"
    for idx in table.indexes:
        if idx.name == spec.name:
            if index_columns_match_prefix(idx.columns, spec.columns):
                return True, ""
            return False, f"actual columns {list(idx.columns)}"
    return False, "index not found"


def audit_offline_schemas(
    schema_dir: Path | str,
    *,
    registry: Mapping[str, Mapping[str, TableRegistryEntry]] | None = None,
    big_fields: Sequence[FieldBudget] | None = None,
    strict_registry: bool = False,
    size_check: bool = False,
    snapshot_path: Path | str | None = None,
    compare_path: Path | str | None = None,
) -> DbCheckReport:
    """离线审计 ``schema_dir`` 下全部 ``KNOWN_SCHEMA_FILES``：语法校验、登记覆盖与清理索引前导列核对。"""
    root = Path(schema_dir)
    if not root.is_dir():
        raise SchemaValidationError(f"schema directory not found: {root}")

    reg = DEFAULT_REGISTRY if registry is None else registry
    fields = DEFAULT_BIG_FIELDS if big_fields is None else big_fields

    violations: list[str] = []
    warnings: list[str] = []
    snapshot: dict[str, int] = {}
    parsed_dbs: dict[str, DatabaseMigrationPlan] = {}
    checked_tables = 0

    for filename in KNOWN_SCHEMA_FILES:
        file_path = root / filename
        if not file_path.is_file():
            violations.append(f"missing known schema file: {filename}")
            continue
        try:
            db_plan = parse_schema_file(file_path)
        except SchemaValidationError as exc:
            violations.append(f"invalid schema {filename}: {exc}")
            continue
        parsed_dbs[db_plan.db_name] = db_plan
        db_reg = _lookup_db_registry(reg, db_plan.db_name)

        for table in db_plan.tables:
            checked_tables += 1
            snapshot[f"{db_plan.db_name}.{table.table_name}"] = len(table.columns)
            if table.table_name in IGNORED_TABLES:
                continue
            entry = db_reg.get(table.table_name) if db_reg is not None else None
            if db_reg is not None and entry is None and strict_registry:
                violations.append(f"unregistered table: {db_plan.db_name}.{table.table_name}")
            if entry is not None and entry.table_class == CLASS_SWEPT and entry.required_indexes:
                for idx_spec in entry.required_indexes:
                    ok, detail = _check_table_index_offline(table, idx_spec)
                    if not ok:
                        violations.append(
                            f"missing/mismatched cleanup index {db_plan.db_name}.{table.table_name}.{idx_spec.name} "
                            f"({detail}; want prefix {list(idx_spec.columns)})"
                        )

    if size_check:
        for budget in fields:
            bare_db = budget.db[len("mmorpg_") :] if budget.db.startswith("mmorpg_") else budget.db
            db_plan = parsed_dbs.get(budget.db) or parsed_dbs.get(bare_db)
            if db_plan is None:
                continue
            table_map = {t.table_name: t for t in db_plan.tables}
            table = table_map.get(budget.table)
            if table is None:
                violations.append(f"big-field table missing in schema: {budget.db}.{budget.table}")
                continue
            col_names = {c.name for c in table.columns}
            if budget.column not in col_names:
                violations.append(
                    f"big-field column missing in schema: {budget.db}.{budget.table}.{budget.column}"
                )
            if budget.pk and budget.pk not in col_names:
                violations.append(
                    f"big-field pk missing in schema: {budget.db}.{budget.table}.{budget.pk}"
                )

    if snapshot_path is not None:
        write_snapshot(snapshot_path, snapshot)

    deltas: tuple[SnapshotDelta, ...] = ()
    if compare_path is not None:
        before = read_snapshot(compare_path)
        deltas = compare_snapshots(before, snapshot)

    logger.info(
        "dbcheck offline audit finished: schema_dir=%s checked_tables=%d violations=%d warnings=%d",
        root,
        checked_tables,
        len(violations),
        len(warnings),
    )
    return DbCheckReport(
        mode="offline",
        passed=not violations,
        checked_tables=checked_tables,
        violations=tuple(violations),
        warnings=tuple(warnings),
        snapshot=snapshot,
        deltas=deltas,
    )


async def audit_live_database(
    pool,
    *,
    schema_like: str = DEFAULT_SCHEMA_LIKE,
    registry: Mapping[str, Mapping[str, TableRegistryEntry]] | None = None,
    big_fields: Sequence[FieldBudget] | None = None,
    exact: bool = False,
    strict_registry: bool = False,
    outbox_max: int = DEFAULT_OUTBOX_MAX,
    avg_row_warn: int = DEFAULT_AVG_ROW_WARN,
    size_check: bool = False,
    top_rows: int = 0,
    pending: bool = False,
    snapshot_path: Path | str | None = None,
    compare_path: Path | str | None = None,
) -> DbCheckReport:
    """基于异步 ``aiomysql`` 连接池执行严格只读（仅 ``SELECT``）数据库体检。"""
    if pool is None:
        raise ValueError("pool is required for live dbcheck")
    if top_rows < 0:
        raise ValueError(f"top_rows must be non-negative, got {top_rows}")

    reg = DEFAULT_REGISTRY if registry is None else registry
    fields = DEFAULT_BIG_FIELDS if big_fields is None else big_fields

    violations: list[str] = []
    warnings: list[str] = []
    snapshot: dict[str, int] = {}
    big_field_reports: list[BigFieldReport] = []
    pending_reports: list[PendingReport] = []
    checked_tables = 0

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await _execute_readonly(
                cursor,
                "SELECT TABLE_SCHEMA, TABLE_NAME, COALESCE(TABLE_ROWS, 0), COALESCE(AVG_ROW_LENGTH, 0) "
                "FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA LIKE %s AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_SCHEMA, TABLE_NAME",
                (schema_like,),
            )
            raw_tables = await cursor.fetchall()

            actual: dict[str, list[tuple[str, int, int]]] = {}
            for row in raw_tables:
                db_name = _decode_str(row[0])
                table_name = _decode_str(row[1])
                rows_est = int(row[2] or 0)
                avg_len = int(row[3] or 0)
                actual.setdefault(db_name, []).append((table_name, rows_est, avg_len))

            if not actual:
                violations.append(f"no base tables matched schema pattern {schema_like!r}")
                return DbCheckReport(
                    mode="live",
                    passed=False,
                    checked_tables=0,
                    violations=tuple(violations),
                    warnings=tuple(warnings),
                    snapshot=snapshot,
                )

            present_by_db: dict[str, set[str]] = {}
            for db_name in sorted(actual.keys()):
                _assert_identifier(db_name, "database")
                present_by_db[db_name] = set()
                db_reg = _lookup_db_registry(reg, db_name)

                for table_name, rows_est, avg_len in actual[db_name]:
                    _assert_identifier(table_name, "table")
                    present_by_db[db_name].add(table_name)
                    if table_name in IGNORED_TABLES:
                        continue
                    checked_tables += 1
                    entry = db_reg.get(table_name) if db_reg is not None else None

                    if db_reg is not None and entry is None and strict_registry:
                        violations.append(f"unregistered table: {db_name}.{table_name}")
                    if avg_row_warn > 0 and avg_len > avg_row_warn:
                        warnings.append(
                            f"high AVG_ROW_LENGTH: {db_name}.{table_name} "
                            f"avg_row_bytes={avg_len} threshold={avg_row_warn}"
                        )

                    # outbox 表始终使用精确 SELECT COUNT(*)，防止统计信息滞后漏报积压。
                    use_exact = exact or (entry is not None and entry.table_class == CLASS_OUTBOX)
                    row_count = rows_est
                    if use_exact:
                        await _execute_readonly(
                            cursor,
                            f"SELECT COUNT(*) FROM `{db_name}`.`{table_name}`",
                        )
                        cnt_row = await cursor.fetchone()
                        row_count = int(cnt_row[0] or 0) if cnt_row else 0
                    snapshot[f"{db_name}.{table_name}"] = row_count

                    if entry is not None and entry.table_class == CLASS_OUTBOX and row_count > outbox_max:
                        violations.append(
                            f"outbox backlog: {db_name}.{table_name} rows={row_count} max={outbox_max}"
                        )

                    if entry is not None and entry.table_class == CLASS_SWEPT and entry.required_indexes:
                        for idx_spec in entry.required_indexes:
                            await _execute_readonly(
                                cursor,
                                "SELECT COLUMN_NAME FROM information_schema.STATISTICS "
                                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND INDEX_NAME = %s "
                                "ORDER BY SEQ_IN_INDEX",
                                (db_name, table_name, idx_spec.name),
                            )
                            idx_rows = await cursor.fetchall()
                            actual_cols = tuple(_decode_str(r[0]) for r in idx_rows)
                            if not actual_cols:
                                violations.append(
                                    f"missing/mismatched cleanup index {db_name}.{table_name}.{idx_spec.name} "
                                    f"(index not found; want prefix {list(idx_spec.columns)})"
                                )
                            elif not index_columns_match_prefix(actual_cols, idx_spec.columns):
                                violations.append(
                                    f"missing/mismatched cleanup index {db_name}.{table_name}.{idx_spec.name} "
                                    f"(actual columns {list(actual_cols)}; want prefix {list(idx_spec.columns)})"
                                )

            if size_check:
                for budget in fields:
                    if budget.table not in present_by_db.get(budget.db, set()):
                        continue
                    _assert_identifier(budget.db, "bigfield.db")
                    _assert_identifier(budget.table, "bigfield.table")
                    _assert_identifier(budget.column, "bigfield.column")
                    if budget.pk:
                        _assert_identifier(budget.pk, "bigfield.pk")
                    try:
                        await _execute_readonly(
                            cursor,
                            f"SELECT COUNT(*), COALESCE(MAX(LENGTH(`{budget.column}`)), 0), "
                            f"COALESCE(AVG(LENGTH(`{budget.column}`)), 0), "
                            f"COALESCE(SUM(CASE WHEN LENGTH(`{budget.column}`) > %s THEN 1 ELSE 0 END), 0) "
                            f"FROM `{budget.db}`.`{budget.table}`",
                            (budget.max_bytes,),
                        )
                        stat_row = await cursor.fetchone()
                        rows_cnt = int(stat_row[0] or 0) if stat_row else 0
                        max_b = int(stat_row[1] or 0) if stat_row else 0
                        avg_b = int(float(stat_row[2] or 0)) if stat_row else 0
                        over_cnt = int(stat_row[3] or 0) if stat_row else 0

                        ratio = float(max_b) / float(max(budget.max_bytes, 1))
                        status = "OK"
                        if over_cnt > 0:
                            status = "FAIL"
                            violations.append(
                                f"big-field budget exceeded: {budget.db}.{budget.table}.{budget.column} "
                                f"max={max_b} budget={budget.max_bytes} over_rows={over_cnt}"
                            )
                        elif ratio >= 0.8:
                            status = "WARN"
                            warnings.append(
                                f"big-field approaching budget: {budget.db}.{budget.table}.{budget.column} "
                                f"max={max_b} budget={budget.max_bytes}"
                            )

                        top_list: list[LargeRow] = []
                        if top_rows > 0 and budget.pk and (over_cnt > 0 or ratio >= 0.8):
                            await _execute_readonly(
                                cursor,
                                f"SELECT CAST(`{budget.pk}` AS CHAR), LENGTH(`{budget.column}`) AS n "
                                f"FROM `{budget.db}`.`{budget.table}` ORDER BY n DESC LIMIT %s",
                                (top_rows,),
                            )
                            for r in await cursor.fetchall():
                                top_list.append(LargeRow(pk=_decode_str(r[0]), size_bytes=int(r[1] or 0)))

                        big_field_reports.append(
                            BigFieldReport(
                                db=budget.db,
                                table=budget.table,
                                column=budget.column,
                                pk=budget.pk,
                                rows=rows_cnt,
                                max_bytes=max_b,
                                avg_bytes=avg_b,
                                budget_bytes=budget.max_bytes,
                                over_rows=over_cnt,
                                status=status,
                                top_rows=tuple(top_list),
                            )
                        )
                    except Exception as exc:
                        warnings.append(
                            f"big-field probe failed: {budget.db}.{budget.table}.{budget.column}: {exc}"
                        )
                        big_field_reports.append(
                            BigFieldReport(
                                db=budget.db,
                                table=budget.table,
                                column=budget.column,
                                pk=budget.pk,
                                rows=0,
                                max_bytes=0,
                                avg_bytes=0,
                                budget_bytes=budget.max_bytes,
                                over_rows=0,
                                status="WARN",
                                error=str(exc),
                            )
                        )

                # 按 max/budget 占比降序排列（与 Go checkBigFields 一致）。
                big_field_reports.sort(
                    key=lambda item: float(item.max_bytes) / float(max(item.budget_bytes, 1)),
                    reverse=True,
                )

            if pending:
                for db_name in sorted(actual.keys()):
                    db_reg = _lookup_db_registry(reg, db_name)
                    if db_reg is None:
                        continue
                    for table_name in sorted(db_reg.keys()):
                        entry = db_reg[table_name]
                        if (
                            entry.table_class != CLASS_SWEPT
                            or not entry.pending_where
                            or table_name not in present_by_db.get(db_name, set())
                        ):
                            continue
                        _assert_identifier(db_name, "pending.db")
                        _assert_identifier(table_name, "pending.table")
                        try:
                            await _execute_readonly(
                                cursor,
                                f"SELECT COUNT(*) FROM `{db_name}`.`{table_name}` WHERE {entry.pending_where}",
                            )
                            row = await cursor.fetchone()
                            cnt = int(row[0] or 0) if row else 0
                            pending_reports.append(
                                PendingReport(db=db_name, table=table_name, pending_rows=cnt)
                            )
                        except Exception as exc:
                            warnings.append(f"pending count failed: {db_name}.{table_name}: {exc}")
                            pending_reports.append(
                                PendingReport(
                                    db=db_name,
                                    table=table_name,
                                    pending_rows=0,
                                    error=str(exc),
                                )
                            )

    if snapshot_path is not None:
        write_snapshot(snapshot_path, snapshot)

    deltas: tuple[SnapshotDelta, ...] = ()
    if compare_path is not None:
        before = read_snapshot(compare_path)
        deltas = compare_snapshots(before, snapshot)

    logger.info(
        "dbcheck live audit finished: checked_tables=%d violations=%d warnings=%d",
        checked_tables,
        len(violations),
        len(warnings),
    )
    return DbCheckReport(
        mode="live",
        passed=not violations,
        checked_tables=checked_tables,
        violations=tuple(violations),
        warnings=tuple(warnings),
        snapshot=snapshot,
        deltas=deltas,
        big_fields=tuple(big_field_reports),
        pending=tuple(pending_reports),
    )


def _default_schema_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "config"


def _parse_optional_db_dsn(dsn: str) -> dict[str, object]:
    raw = dsn.strip()
    if raw.startswith(("mysql://", "mysql+pymysql://", "mysql+aiomysql://")):
        parsed = urlsplit(raw)
        if not parsed.hostname:
            raise ValueError("invalid MySQL DSN: missing host")
        return {
            "host": parsed.hostname,
            "port": int(parsed.port or 3306),
            "user": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "database": unquote(parsed.path.lstrip("/")),
        }
    match = _GO_MYSQL_DSN_RE.match(raw)
    if match:
        return {
            "host": match.group("host"),
            "port": int(match.group("port") or 3306),
            "user": match.group("user") or "",
            "password": match.group("password") or "",
            "database": match.group("database") or "",
        }
    raise ValueError("unsupported MySQL DSN format (value not echoed)")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dbcheck",
        description="Read-only database capacity, schema registry, cleanup-index and big-field auditor.",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=_default_schema_dir(),
        help="Directory containing *_schema.sql files for offline audit.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Run offline schema & registry audit without connecting to MySQL/TiDB.",
    )
    parser.add_argument(
        "--registry",
        nargs="?",
        const="__default__",
        default=None,
        help="Enable strict registry verification (optionally pass a JSON registry file path).",
    )
    parser.add_argument(
        "--strict-registry",
        action="store_true",
        help="Fail on tables present in known schemas but absent from the registry.",
    )
    parser.add_argument(
        "--size-check",
        action="store_true",
        help="Run read-only big-field MAX/AVG LENGTH audit on registered blob columns.",
    )
    parser.add_argument(
        "--top-rows",
        type=int,
        default=0,
        help="With --size-check, print top N largest primary keys for over-budget/warning columns.",
    )
    parser.add_argument(
        "--pending",
        action="store_true",
        help="Report read-only worst-case pending counts for swept tables (never deletes).",
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=None,
        help="Write table count snapshot to JSON file.",
    )
    parser.add_argument(
        "--compare",
        type=Path,
        default=None,
        help="Compare current table counts against a previous snapshot JSON file.",
    )
    parser.add_argument("--json", action="store_true", dest="as_json", help="Emit JSON output.")
    parser.add_argument("--dsn", default="", help="MySQL/TiDB DSN (never logged or echoed).")
    parser.add_argument("--host", default="", help="MySQL/TiDB host.")
    parser.add_argument("--port", type=int, default=3306, help="MySQL/TiDB port.")
    parser.add_argument("--user", default="root", help="MySQL/TiDB username.")
    parser.add_argument("--password", default="", help="MySQL/TiDB password.")
    parser.add_argument(
        "--schema-like",
        default=DEFAULT_SCHEMA_LIKE,
        help="LIKE pattern for logical databases to inspect.",
    )
    parser.add_argument(
        "--exact",
        action="store_true",
        help="Use exact SELECT COUNT(*) for all tables.",
    )
    parser.add_argument(
        "--outbox-max",
        type=int,
        default=DEFAULT_OUTBOX_MAX,
        help="Maximum allowed rows in any outbox table.",
    )
    parser.add_argument(
        "--avg-row-warn",
        type=int,
        default=DEFAULT_AVG_ROW_WARN,
        help="Warn when AVG_ROW_LENGTH exceeds this byte threshold.",
    )
    return parser


def _format_text_report(report: DbCheckReport) -> str:
    lines = [f"=== dbcheck ({report.mode}) checked_tables={report.checked_tables} ==="]
    for warn in report.warnings:
        lines.append(f"[WARN] {warn}")
    for viol in report.violations:
        lines.append(f"[FAIL] {viol}")
    if report.deltas:
        lines.append("-- Row count delta vs snapshot --")
        for d in report.deltas:
            lines.append(f"  {d.key:<50s} {d.delta:+d} ({d.before} -> {d.after})")
    if report.big_fields:
        lines.append("-- Big-field bloat check (read-only) --")
        for bf in report.big_fields:
            target = f"{bf.db}.{bf.table}.{bf.column}"
            if bf.error:
                lines.append(f"  [WARN] {target:<52s} probe failed: {bf.error}")
                continue
            lines.append(
                f"  [{bf.status:^4s}] {target:<52s} rows={bf.rows:<9d} "
                f"max={bf.max_bytes:<9d} avg={bf.avg_bytes:<7d} "
                f"budget={bf.budget_bytes:<8d} over={bf.over_rows}"
            )
            for row in bf.top_rows:
                lines.append(f"         {bf.pk}={row.pk} bytes={row.size_bytes}")
    if report.pending:
        lines.append("-- Read-only pending retention upper bound (no deletes) --")
        for item in report.pending:
            if item.error:
                lines.append(f"  [WARN] {item.db}.{item.table:<40s} count failed: {item.error}")
            else:
                lines.append(f"  {item.db}.{item.table:<46s} pending_rows={item.pending_rows}")
    if report.passed:
        lines.append("[RESULT] PASS: all read-only dbcheck gates succeeded")
    else:
        lines.append(f"[RESULT] FAIL: {len(report.violations)} violation(s) found")
    return "\n".join(lines)


async def _run_cli_async(args: argparse.Namespace) -> int:
    registry = DEFAULT_REGISTRY
    strict_registry = bool(args.strict_registry)
    if args.registry is not None:
        strict_registry = True
        if args.registry != "__default__":
            registry = load_registry_file(args.registry)

    use_offline = bool(args.offline or (not args.dsn and not args.host))
    if use_offline:
        report = audit_offline_schemas(
            args.schema_dir,
            registry=registry,
            strict_registry=strict_registry,
            size_check=args.size_check,
            snapshot_path=args.snapshot,
            compare_path=args.compare,
        )
    else:
        import aiomysql

        if args.dsn:
            conn_params = _parse_optional_db_dsn(args.dsn)
        else:
            conn_params = {
                "host": args.host,
                "port": args.port,
                "user": args.user,
                "password": args.password,
                "database": "",
            }
        pool = await aiomysql.create_pool(
            host=conn_params["host"],
            port=conn_params["port"],
            user=conn_params["user"],
            password=conn_params["password"],
            db=conn_params["database"] or None,
            minsize=1,
            maxsize=2,
            autocommit=True,
            charset="utf8mb4",
            init_command="SET SESSION transaction_read_only = 1",
        )
        try:
            report = await audit_live_database(
                pool,
                schema_like=args.schema_like,
                registry=registry,
                exact=args.exact,
                strict_registry=strict_registry,
                outbox_max=args.outbox_max,
                avg_row_warn=args.avg_row_warn,
                size_check=args.size_check,
                top_rows=args.top_rows,
                pending=args.pending,
                snapshot_path=args.snapshot,
                compare_path=args.compare,
            )
        finally:
            pool.close()
            await pool.wait_closed()

    if args.as_json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    else:
        print(_format_text_report(report))
    return 0 if report.passed else 1


def main(argv: list[str] | None = None) -> int:
    """``dbcheck`` 命令行入口，返回进程退出码（0=通过，1=违规，2=参数或运行异常）。"""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_run_cli_async(args))
    except (SchemaValidationError, ValueError, OSError, json.JSONDecodeError) as exc:
        logger.error("dbcheck fatal error: %s", exc)
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
