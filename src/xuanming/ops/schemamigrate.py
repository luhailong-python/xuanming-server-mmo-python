"""全库 Schema 校验与幂等迁移工具（对应 Go 侧 ``schemamigrate``）。

职责（严格遵守单一职责）：
1. 词法与结构级解析 ``config/*_schema.sql`` 中的全部 ``CREATE TABLE`` 语句，校验语法完整性、
   标识符规范、单库无重名表/重名列/重名索引、主键与索引列引用存在性；
2. 生成确定性的分库与全库 Schema 摘要（SHA-256），支持按库过滤（``plan_migrations``）；
3. 针对 ``aiomysql`` 连接池执行幂等 ``CREATE TABLE IF NOT EXISTS`` 迁移或只读 ``dry_run``，
   清晰汇报已执行、待执行与已跳过的表清单（``apply_plan``）。
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
import sys
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# 仓库内已登记的 15 份服务 Schema SQL 文件。
KNOWN_SCHEMA_FILES: tuple[str, ...] = (
    "portal_schema.sql",
    "login_schema.sql",
    "data_service_schema.sql",
    "db_schema.sql",
    "player_inbox_schema.sql",
    "match_schema.sql",
    "chat_schema.sql",
    "mail_schema.sql",
    "rank_schema.sql",
    "friend_schema.sql",
    "payment_schema.sql",
    "trade_schema.sql",
    "guild_schema.sql",
    "mall_game_schema.sql",
    "gm_admin_schema.sql",
)

# MySQL / TiDB 标识符最大长度 64 字符，仅允许字母、数字与下划线。
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_CREATE_HEADER_RE = re.compile(
    r"^CREATE\s+TABLE\s+(?:(?P<if_not_exists>IF\s+NOT\s+EXISTS)\s+)?"
    r"(?:`(?P<quoted>[^`]+)`|(?P<bare>[A-Za-z_][A-Za-z0-9_]*))\s*\(",
    re.IGNORECASE,
)
_LEADING_CONSTRAINT_RE = re.compile(
    r"^CONSTRAINT\s+(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s+",
    re.IGNORECASE,
)
_PRIMARY_KEY_RE = re.compile(r"^PRIMARY\s+KEY\s*\(", re.IGNORECASE)
_UNIQUE_KEY_RE = re.compile(
    r"^UNIQUE(?:\s+(?:KEY|INDEX))?(?:\s+(?:`(?P<quoted>[^`]+)`|(?P<bare>[A-Za-z_][A-Za-z0-9_]*)))?\s*\(",
    re.IGNORECASE,
)
_INDEX_RE = re.compile(
    r"^(?:KEY|INDEX)(?:\s+(?:`(?P<quoted>[^`]+)`|(?P<bare>[A-Za-z_][A-Za-z0-9_]*)))?\s*\(",
    re.IGNORECASE,
)
_CHECK_RE = re.compile(r"^CHECK\s*\(", re.IGNORECASE)
_COLUMN_HEAD_RE = re.compile(
    r"^(?:`(?P<quoted>[^`]+)`|(?P<bare>[A-Za-z_][A-Za-z0-9_]*))\s+(?P<rest>.+)$",
    re.DOTALL,
)
_INLINE_PK_RE = re.compile(r"\bPRIMARY\s+KEY\b", re.IGNORECASE)
_INDEX_COL_RE = re.compile(
    r"^(?:`(?P<quoted>[^`]+)`|(?P<bare>[A-Za-z_][A-Za-z0-9_]*))"
    r"(?:\s*\(\s*\d+\s*\))?(?:\s+(?:ASC|DESC))?$",
    re.IGNORECASE,
)


class SchemaValidationError(ValueError):
    """Schema SQL 存在语法错误、重复定义或结构不合法。"""


@dataclass(frozen=True)
class ColumnSpec:
    """表内单个列的定义。"""

    name: str
    definition: str


@dataclass(frozen=True)
class IndexSpec:
    """表内普通索引或唯一索引的定义。"""

    name: str
    columns: tuple[str, ...]
    unique: bool = False


@dataclass(frozen=True)
class TablePlan:
    """单张表的建表迁移计划。"""

    db_name: str
    table_name: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    indexes: tuple[IndexSpec, ...]
    sql: str
    source_file: str

    @property
    def idempotent_sql(self) -> str:
        """返回保证带 ``IF NOT EXISTS`` 的幂等建表语句。"""
        match = _CREATE_HEADER_RE.match(self.sql)
        if match and match.group("if_not_exists"):
            return self.sql
        return re.sub(
            r"^CREATE\s+TABLE\s+",
            "CREATE TABLE IF NOT EXISTS ",
            self.sql,
            count=1,
            flags=re.IGNORECASE,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "db_name": self.db_name,
            "table_name": self.table_name,
            "columns": [{"name": c.name, "definition": c.definition} for c in self.columns],
            "primary_key": list(self.primary_key),
            "indexes": [
                {"name": idx.name, "columns": list(idx.columns), "unique": idx.unique}
                for idx in self.indexes
            ],
            "source_file": self.source_file,
        }


@dataclass(frozen=True)
class DatabaseMigrationPlan:
    """单个逻辑库的完整 Schema 计划与确定性摘要。"""

    db_name: str
    source_file: str
    tables: tuple[TablePlan, ...]
    digest: str

    def to_dict(self) -> dict[str, object]:
        return {
            "db_name": self.db_name,
            "source_file": self.source_file,
            "table_count": len(self.tables),
            "digest": self.digest,
            "tables": [t.to_dict() for t in self.tables],
        }


@dataclass(frozen=True)
class MigrationPlan:
    """多库汇总 Schema 迁移计划与全局摘要。"""

    schema_dir: str
    databases: tuple[DatabaseMigrationPlan, ...]
    digest: str

    @property
    def tables(self) -> tuple[TablePlan, ...]:
        return tuple(table for db in self.databases for table in db.tables)

    @property
    def table_count(self) -> int:
        return sum(len(db.tables) for db in self.databases)

    def by_db(self, db_name: str) -> DatabaseMigrationPlan:
        normalized = _normalize_db_name(db_name)
        for db in self.databases:
            if db.db_name == normalized:
                return db
        raise KeyError(f"database plan not found: {db_name}")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_dir": self.schema_dir,
            "database_count": len(self.databases),
            "table_count": self.table_count,
            "digest": self.digest,
            "databases": [db.to_dict() for db in self.databases],
        }


@dataclass(frozen=True)
class ApplyReport:
    """``apply_plan`` 的执行结果报告。"""

    dry_run: bool
    digest: str
    applied: tuple[str, ...]
    skipped: tuple[str, ...]
    pending: tuple[str, ...]
    statements: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "dry_run": self.dry_run,
            "digest": self.digest,
            "applied": list(self.applied),
            "skipped": list(self.skipped),
            "pending": list(self.pending),
            "statement_count": len(self.statements),
        }


def _normalize_db_name(name: str) -> str:
    cleaned = name.strip()
    if cleaned.endswith("_schema.sql"):
        cleaned = cleaned[: -len("_schema.sql")]
    return cleaned


def _collapse_sql(statement: str) -> str:
    return " ".join(statement.split())


def _compute_digest(parts: Iterable[str]) -> str:
    hasher = hashlib.sha256()
    for part in parts:
        hasher.update(part.encode("utf-8"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def _strip_leading_comments(text: str) -> str:
    """剥离语句前导的纯空白与注释（保留语句内部的 TiDB 可执行注释）。"""
    n = len(text)
    i = 0
    while i < n:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            return ""
        if text.startswith("--", i) and (i + 2 == n or text[i + 2].isspace() or text[i + 2] == "-"):
            newline = text.find("\n", i + 2)
            if newline < 0:
                return ""
            i = newline + 1
            continue
        if text[i] == "#":
            newline = text.find("\n", i + 1)
            if newline < 0:
                return ""
            i = newline + 1
            continue
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            if end < 0:
                raise SchemaValidationError("unterminated block comment in SQL")
            i = end + 2
            continue
        break
    return text[i:].strip()


def _strip_comments_and_strings(text: str, *, mask_strings: bool = False) -> str:
    """移除注释以便做结构分析；可选把字符串字面量替换成空占位，避免注释/默认值干扰关键字识别。"""
    out: list[str] = []
    n = len(text)
    i = 0
    while i < n:
        ch = text[i]
        if ch == "'" or ch == '"':
            quote = ch
            start = i
            i += 1
            while i < n:
                if text[i] == "\\" and i + 1 < n:
                    i += 2
                    continue
                if text[i] == quote:
                    if i + 1 < n and text[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise SchemaValidationError(f"unterminated string literal starting with {quote}")
            out.append("''" if mask_strings else text[start:i])
            continue
        if ch == "`":
            start = i
            i += 1
            while i < n:
                if text[i] == "`":
                    if i + 1 < n and text[i + 1] == "`":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise SchemaValidationError("unterminated backtick identifier")
            out.append(text[start:i])
            continue
        if text.startswith("--", i) and (i + 2 == n or text[i + 2].isspace() or text[i + 2] == "-"):
            newline = text.find("\n", i + 2)
            if newline < 0:
                break
            out.append(" ")
            i = newline + 1
            continue
        if ch == "#":
            newline = text.find("\n", i + 1)
            if newline < 0:
                break
            out.append(" ")
            i = newline + 1
            continue
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            if end < 0:
                raise SchemaValidationError("unterminated block comment")
            out.append(" ")
            i = end + 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _split_sql_statements(sql_text: str, source_label: str) -> list[str]:
    """按顶层分号切分 SQL 语句，同时校验引号、注释与括号配对。"""
    statements: list[str] = []
    buf: list[str] = []
    depth = 0
    n = len(sql_text)
    i = 0

    while i < n:
        ch = sql_text[i]
        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
            i += 1
            while i < n:
                cur = sql_text[i]
                buf.append(cur)
                if cur == "\\" and i + 1 < n:
                    buf.append(sql_text[i + 1])
                    i += 2
                    continue
                if cur == quote:
                    if i + 1 < n and sql_text[i + 1] == quote:
                        buf.append(sql_text[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise SchemaValidationError(f"{source_label}: unterminated string literal")
            continue

        if ch == "`":
            buf.append(ch)
            i += 1
            while i < n:
                cur = sql_text[i]
                buf.append(cur)
                if cur == "`":
                    if i + 1 < n and sql_text[i + 1] == "`":
                        buf.append(sql_text[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise SchemaValidationError(f"{source_label}: unterminated backtick identifier")
            continue

        if sql_text.startswith("--", i) and (
            i + 2 == n or sql_text[i + 2].isspace() or sql_text[i + 2] == "-"
        ):
            newline = sql_text.find("\n", i + 2)
            if newline < 0:
                buf.append(sql_text[i:])
                break
            buf.append(sql_text[i : newline + 1])
            i = newline + 1
            continue

        if ch == "#":
            newline = sql_text.find("\n", i + 1)
            if newline < 0:
                buf.append(sql_text[i:])
                break
            buf.append(sql_text[i : newline + 1])
            i = newline + 1
            continue

        if sql_text.startswith("/*", i):
            end = sql_text.find("*/", i + 2)
            if end < 0:
                raise SchemaValidationError(f"{source_label}: unterminated block comment")
            buf.append(sql_text[i : end + 2])
            i = end + 2
            continue

        if ch == "(":
            depth += 1
            buf.append(ch)
            i += 1
            continue

        if ch == ")":
            depth -= 1
            if depth < 0:
                raise SchemaValidationError(f"{source_label}: unexpected closing parenthesis")
            buf.append(ch)
            i += 1
            continue

        if ch == ";" and depth == 0:
            stmt = _strip_leading_comments("".join(buf))
            if stmt:
                statements.append(stmt)
            buf.clear()
            i += 1
            continue

        buf.append(ch)
        i += 1

    if depth != 0:
        raise SchemaValidationError(f"{source_label}: unbalanced parentheses in SQL")

    trailing = _strip_leading_comments("".join(buf))
    if trailing:
        statements.append(trailing)
    return statements


def _find_matching_paren(text: str, open_idx: int, source_label: str) -> int:
    """从指定左括号位置找到与之配对的右括号索引（跳过字符串、反引号与注释）。"""
    depth = 0
    n = len(text)
    i = open_idx
    while i < n:
        ch = text[i]
        if ch in ("'", '"'):
            quote = ch
            i += 1
            while i < n:
                if text[i] == "\\" and i + 1 < n:
                    i += 2
                    continue
                if text[i] == quote:
                    if i + 1 < n and text[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise SchemaValidationError(f"{source_label}: unterminated string literal")
            continue
        if ch == "`":
            i += 1
            while i < n:
                if text[i] == "`":
                    if i + 1 < n and text[i + 1] == "`":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise SchemaValidationError(f"{source_label}: unterminated backtick identifier")
            continue
        if text.startswith("--", i) and (i + 2 == n or text[i + 2].isspace() or text[i + 2] == "-"):
            newline = text.find("\n", i + 2)
            if newline < 0:
                break
            i = newline + 1
            continue
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            if end < 0:
                raise SchemaValidationError(f"{source_label}: unterminated block comment")
            i = end + 2
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise SchemaValidationError(f"{source_label}: missing closing parenthesis for CREATE TABLE body")


def _split_top_level_commas(body: str, source_label: str) -> list[str]:
    """按顶层逗号切分建表体内的列、索引与约束项。"""
    items: list[str] = []
    buf: list[str] = []
    depth = 0
    n = len(body)
    i = 0

    while i < n:
        ch = body[i]
        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
            i += 1
            while i < n:
                cur = body[i]
                buf.append(cur)
                if cur == "\\" and i + 1 < n:
                    buf.append(body[i + 1])
                    i += 2
                    continue
                if cur == quote:
                    if i + 1 < n and body[i + 1] == quote:
                        buf.append(body[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise SchemaValidationError(f"{source_label}: unterminated string literal")
            continue
        if ch == "`":
            buf.append(ch)
            i += 1
            while i < n:
                cur = body[i]
                buf.append(cur)
                if cur == "`":
                    if i + 1 < n and body[i + 1] == "`":
                        buf.append(body[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise SchemaValidationError(f"{source_label}: unterminated backtick identifier")
            continue
        if body.startswith("--", i) and (i + 2 == n or body[i + 2].isspace() or body[i + 2] == "-"):
            newline = body.find("\n", i + 2)
            if newline < 0:
                break
            i = newline + 1
            continue
        if body.startswith("/*", i):
            end = body.find("*/", i + 2)
            if end < 0:
                raise SchemaValidationError(f"{source_label}: unterminated block comment")
            buf.append(body[i : end + 2])
            i = end + 2
            continue
        if ch == "(":
            depth += 1
            buf.append(ch)
            i += 1
            continue
        if ch == ")":
            depth -= 1
            if depth < 0:
                raise SchemaValidationError(f"{source_label}: unexpected ')' inside CREATE TABLE body")
            buf.append(ch)
            i += 1
            continue
        if ch == "," and depth == 0:
            item = "".join(buf).strip()
            if not item:
                raise SchemaValidationError(f"{source_label}: empty clause or trailing comma in CREATE TABLE")
            items.append(item)
            buf.clear()
            i += 1
            continue
        buf.append(ch)
        i += 1

    if depth != 0:
        raise SchemaValidationError(f"{source_label}: unbalanced parentheses inside CREATE TABLE body")
    tail = "".join(buf).strip()
    if not tail:
        raise SchemaValidationError(f"{source_label}: trailing comma before ')' in CREATE TABLE")
    items.append(tail)
    return items


def _parse_column_list(clause: str, open_idx: int, source_label: str) -> tuple[str, ...]:
    close_idx = _find_matching_paren(clause, open_idx, source_label)
    inner = clause[open_idx + 1 : close_idx].strip()
    if not inner:
        raise SchemaValidationError(f"{source_label}: empty key column list in {clause!r}")
    cols: list[str] = []
    for raw_part in _split_top_level_commas(inner, source_label):
        match = _INDEX_COL_RE.match(raw_part.strip())
        if not match:
            raise SchemaValidationError(f"{source_label}: invalid index column specification {raw_part!r}")
        col = match.group("quoted") or match.group("bare")
        if not _IDENT_RE.match(col):
            raise SchemaValidationError(f"{source_label}: invalid identifier {col!r}")
        cols.append(col)
    return tuple(cols)


def _parse_create_table(statement: str, *, db_name: str, source_file: str) -> TablePlan:
    source_label = f"{source_file or db_name}"
    match = _CREATE_HEADER_RE.match(statement)
    if not match:
        raise SchemaValidationError(
            f"{source_label}: expected CREATE TABLE statement, got: {_collapse_sql(statement)[:120]}"
        )
    table_name = match.group("quoted") or match.group("bare")
    if not _IDENT_RE.match(table_name):
        raise SchemaValidationError(f"{source_label}: invalid table name {table_name!r}")

    open_idx = match.end() - 1
    close_idx = _find_matching_paren(statement, open_idx, f"{source_label}.{table_name}")
    tail = _strip_comments_and_strings(statement[close_idx + 1 :], mask_strings=True).strip()
    if re.search(r"\bCREATE\s+TABLE\b", tail, re.IGNORECASE):
        raise SchemaValidationError(
            f"{source_label}.{table_name}: missing semicolon between CREATE TABLE statements"
        )
    body = statement[open_idx + 1 : close_idx].strip()
    if not body:
        raise SchemaValidationError(f"{source_label}.{table_name}: empty CREATE TABLE body")

    columns: list[ColumnSpec] = []
    seen_columns: set[str] = set()
    primary_key: tuple[str, ...] = ()
    indexes: list[IndexSpec] = []
    seen_indexes: set[str] = set()
    unnamed_idx_counter = 0

    for raw_item in _split_top_level_commas(body, f"{source_label}.{table_name}"):
        cleaned = _strip_comments_and_strings(raw_item, mask_strings=False).strip()
        if not cleaned:
            raise SchemaValidationError(f"{source_label}.{table_name}: empty clause in CREATE TABLE")

        # 剥离可选前导 CONSTRAINT <name>，统一识别 CHECK / PRIMARY KEY / UNIQUE。
        clause_no_constraint = _LEADING_CONSTRAINT_RE.sub("", cleaned, count=1).strip()

        if _CHECK_RE.match(clause_no_constraint):
            continue

        if _PRIMARY_KEY_RE.match(clause_no_constraint):
            if primary_key:
                raise SchemaValidationError(f"{source_label}.{table_name}: duplicate PRIMARY KEY definition")
            pk_open = clause_no_constraint.find("(")
            primary_key = _parse_column_list(clause_no_constraint, pk_open, f"{source_label}.{table_name}")
            continue

        unique_match = _UNIQUE_KEY_RE.match(clause_no_constraint)
        if unique_match:
            idx_name = unique_match.group("quoted") or unique_match.group("bare")
            if not idx_name:
                unnamed_idx_counter += 1
                idx_name = f"uk_{table_name}_{unnamed_idx_counter}"
            if idx_name in seen_indexes:
                raise SchemaValidationError(f"{source_label}.{table_name}: duplicate index name {idx_name!r}")
            idx_cols = _parse_column_list(
                clause_no_constraint, unique_match.end() - 1, f"{source_label}.{table_name}"
            )
            seen_indexes.add(idx_name)
            indexes.append(IndexSpec(name=idx_name, columns=idx_cols, unique=True))
            continue

        index_match = _INDEX_RE.match(clause_no_constraint)
        if index_match:
            idx_name = index_match.group("quoted") or index_match.group("bare")
            if not idx_name:
                unnamed_idx_counter += 1
                idx_name = f"idx_{table_name}_{unnamed_idx_counter}"
            if idx_name in seen_indexes:
                raise SchemaValidationError(f"{source_label}.{table_name}: duplicate index name {idx_name!r}")
            idx_cols = _parse_column_list(
                clause_no_constraint, index_match.end() - 1, f"{source_label}.{table_name}"
            )
            seen_indexes.add(idx_name)
            indexes.append(IndexSpec(name=idx_name, columns=idx_cols, unique=False))
            continue

        # 按列定义解析。
        col_match = _COLUMN_HEAD_RE.match(cleaned)
        if not col_match:
            raise SchemaValidationError(
                f"{source_label}.{table_name}: malformed column or constraint clause: {raw_item!r}"
            )
        col_name = col_match.group("quoted") or col_match.group("bare")
        if not _IDENT_RE.match(col_name):
            raise SchemaValidationError(f"{source_label}.{table_name}: invalid column name {col_name!r}")
        if col_name in seen_columns:
            raise SchemaValidationError(f"{source_label}.{table_name}: duplicate column {col_name!r}")
        col_def = col_match.group("rest").strip()
        if not col_def:
            raise SchemaValidationError(f"{source_label}.{table_name}: column {col_name!r} has empty type definition")

        seen_columns.add(col_name)
        columns.append(ColumnSpec(name=col_name, definition=col_def))

        # 识别列内联 PRIMARY KEY（如 portal_schema.sql 中的 `zone_id BIGINT PRIMARY KEY`）。
        masked_def = _strip_comments_and_strings(col_def, mask_strings=True)
        if _INLINE_PK_RE.search(masked_def):
            if primary_key:
                raise SchemaValidationError(f"{source_label}.{table_name}: duplicate PRIMARY KEY definition")
            primary_key = (col_name,)

    if not columns:
        raise SchemaValidationError(f"{source_label}.{table_name}: table has no columns")
    if not primary_key:
        raise SchemaValidationError(f"{source_label}.{table_name}: table is missing PRIMARY KEY")

    for col in primary_key:
        if col not in seen_columns:
            raise SchemaValidationError(
                f"{source_label}.{table_name}: PRIMARY KEY references unknown column {col!r}"
            )
    for idx in indexes:
        for col in idx.columns:
            if col not in seen_columns:
                raise SchemaValidationError(
                    f"{source_label}.{table_name}: index {idx.name!r} references unknown column {col!r}"
                )

    return TablePlan(
        db_name=db_name,
        table_name=table_name,
        columns=tuple(columns),
        primary_key=primary_key,
        indexes=tuple(indexes),
        sql=statement,
        source_file=source_file,
    )


def parse_schema_sql(
    sql_text: str,
    *,
    db_name: str,
    source_file: str = "",
) -> DatabaseMigrationPlan:
    """解析并校验单份 ``*_schema.sql`` 文本，产出 ``DatabaseMigrationPlan``。"""
    normalized_db = _normalize_db_name(db_name)
    if not normalized_db:
        raise SchemaValidationError("db_name must not be empty")

    statements = _split_sql_statements(sql_text, source_file or normalized_db)
    if not statements:
        raise SchemaValidationError(f"{source_file or normalized_db}: schema contains no CREATE TABLE statements")

    tables: list[TablePlan] = []
    seen_tables: set[str] = set()
    for stmt in statements:
        table = _parse_create_table(stmt, db_name=normalized_db, source_file=source_file)
        if table.table_name in seen_tables:
            raise SchemaValidationError(
                f"{source_file or normalized_db}: duplicate table definition {table.table_name!r}"
            )
        seen_tables.add(table.table_name)
        tables.append(table)

    digest_parts = sorted(f"{t.table_name}:{_collapse_sql(t.sql)}" for t in tables)
    return DatabaseMigrationPlan(
        db_name=normalized_db,
        source_file=source_file,
        tables=tuple(tables),
        digest=_compute_digest(digest_parts),
    )


def parse_schema_file(path: Path | str, *, db_name: str | None = None) -> DatabaseMigrationPlan:
    """从磁盘读取并校验单份 ``*_schema.sql`` 文件。"""
    file_path = Path(path)
    if not file_path.is_file():
        raise SchemaValidationError(f"schema file not found: {file_path}")
    inferred_db = _normalize_db_name(db_name if db_name is not None else file_path.name)
    text = file_path.read_text(encoding="utf-8")
    return parse_schema_sql(text, db_name=inferred_db, source_file=file_path.name)


def plan_migrations(
    schema_dir: Path | str,
    db_filter: str | Collection[str] | None = None,
) -> MigrationPlan:
    """扫描并校验 ``schema_dir`` 下全部（或过滤后的）``*_schema.sql``，生成确定性迁移计划。"""
    root = Path(schema_dir)
    if not root.is_dir():
        raise SchemaValidationError(f"schema directory not found: {root}")

    discovered = {
        _normalize_db_name(path.name): path
        for path in sorted(root.glob("*_schema.sql"), key=lambda p: p.name)
        if path.is_file()
    }
    if not discovered:
        raise SchemaValidationError(f"no *_schema.sql files found in {root}")

    if db_filter is None:
        selected_dbs = sorted(discovered.keys())
    else:
        raw_items = [db_filter] if isinstance(db_filter, str) else list(db_filter)
        wanted = [_normalize_db_name(item) for item in raw_items if item.strip()]
        if not wanted:
            raise SchemaValidationError("db_filter must contain at least one database name")
        missing = sorted(set(wanted) - set(discovered.keys()))
        if missing:
            raise SchemaValidationError(
                f"unknown database(s) in db_filter: {', '.join(missing)}"
            )
        selected_dbs = sorted(set(wanted))

    db_plans = tuple(parse_schema_file(discovered[name], db_name=name) for name in selected_dbs)
    overall_digest = _compute_digest(f"{db.db_name}:{db.digest}" for db in db_plans)

    logger.info(
        "schemamigrate plan ready: schema_dir=%s databases=%d tables=%d digest=%s",
        root,
        len(db_plans),
        sum(len(db.tables) for db in db_plans),
        overall_digest[:12],
    )
    return MigrationPlan(
        schema_dir=str(root),
        databases=db_plans,
        digest=overall_digest,
    )


async def _fetch_existing_tables(cursor) -> set[str]:
    await cursor.execute(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()"
    )
    rows = await cursor.fetchall()
    existing: set[str] = set()
    for row in rows:
        raw_name = row[0] if isinstance(row, (tuple, list)) else row.get("TABLE_NAME")
        if isinstance(raw_name, (bytes, bytearray)):
            raw_name = raw_name.decode("utf-8")
        if raw_name:
            existing.add(str(raw_name))
    return existing


async def apply_plan(
    pool,
    plan: MigrationPlan | DatabaseMigrationPlan,
    *,
    dry_run: bool = False,
) -> ApplyReport:
    """对连接池执行幂等建表迁移；``dry_run=True`` 时仅比对现状而不执行任何 DDL。"""
    if not dry_run and pool is None:
        raise ValueError("pool is required when dry_run is False")

    tables = plan.tables
    digest = plan.digest
    applied: list[str] = []
    skipped: list[str] = []
    pending: list[str] = []
    statements: list[str] = []

    if dry_run and pool is None:
        for table in tables:
            pending.append(table.table_name)
            statements.append(table.idempotent_sql)
        logger.info(
            "schemamigrate dry-run (offline): digest=%s pending=%d skipped=0",
            digest[:12],
            len(pending),
        )
        return ApplyReport(
            dry_run=True,
            digest=digest,
            applied=(),
            skipped=(),
            pending=tuple(pending),
            statements=tuple(statements),
        )

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            existing = await _fetch_existing_tables(cur)
            for table in tables:
                if table.table_name in existing:
                    skipped.append(table.table_name)
                    continue
                stmt = table.idempotent_sql
                statements.append(stmt)
                if dry_run:
                    pending.append(table.table_name)
                    continue
                logger.info(
                    "schemamigrate applying table: db=%s table=%s",
                    table.db_name,
                    table.table_name,
                )
                await cur.execute(stmt)
                applied.append(table.table_name)
                existing.add(table.table_name)
        if not dry_run and applied and hasattr(conn, "commit"):
            await conn.commit()

    logger.info(
        "schemamigrate %s finished: digest=%s applied=%d pending=%d skipped=%d",
        "dry-run" if dry_run else "apply",
        digest[:12],
        len(applied),
        len(pending),
        len(skipped),
    )
    return ApplyReport(
        dry_run=dry_run,
        digest=digest,
        applied=tuple(applied),
        skipped=tuple(skipped),
        pending=tuple(pending),
        statements=tuple(statements),
    )


def _default_schema_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "config"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="schemamigrate",
        description="Validate and apply MySQL/TiDB *_schema.sql migrations idempotently.",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=_default_schema_dir(),
        help="Directory containing *_schema.sql files.",
    )
    parser.add_argument(
        "--db",
        action="append",
        default=None,
        help="Filter to specific database name(s) (repeatable or comma-separated).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan and check without executing DDL statements.",
    )
    parser.add_argument("--host", default="", help="MySQL/TiDB host for live apply/dry-run.")
    parser.add_argument("--port", type=int, default=3306, help="MySQL/TiDB port.")
    parser.add_argument("--user", default="root", help="MySQL/TiDB username.")
    parser.add_argument("--password", default="", help="MySQL/TiDB password.")
    parser.add_argument("--database", default="", help="Target MySQL/TiDB database name.")
    return parser


def _parse_db_filter(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    out: list[str] = []
    for item in values:
        out.extend(part.strip() for part in item.split(",") if part.strip())
    return out or None


async def _run_cli_async(args: argparse.Namespace) -> int:
    plan = plan_migrations(args.schema_dir, db_filter=_parse_db_filter(args.db))
    if not args.host:
        if not args.dry_run:
            # 未提供数据库连接参数时仅输出已校验的迁移计划。
            print(json.dumps(plan.to_dict(), indent=2, ensure_ascii=False))
            return 0
        report = await apply_plan(None, plan, dry_run=True)
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
        return 0

    import aiomysql

    pool = await aiomysql.create_pool(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        db=args.database,
        autocommit=True,
    )
    try:
        report = await apply_plan(pool, plan, dry_run=args.dry_run)
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
        return 0
    finally:
        pool.close()
        await pool.wait_closed()


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_run_cli_async(args))
    except (SchemaValidationError, ValueError, OSError) as exc:
        logger.error("schemamigrate failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
