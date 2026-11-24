"""player_inbox MySQL/TiDB 存储实现（SqlInboxStore）。

所有二进制字段统一通过 ``UNHEX(%s)`` 绑定，避免 utf8mb4 连接下二进制字节损坏；
全部事务运行在 ``READ-COMMITTED`` 隔离级别下，对 1205/1213/9007 瞬态冲突做有界重试。
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TypeVar

import aiomysql
import pymysql
from google.protobuf.message import Message

from .. import portal_sql
from ..protocol import Protocol
from .errors import InboxError, StoreUnavailableError
from .model import (
    AuditRow,
    DOMAIN_UNSPECIFIED,
    DIRECTIVE_VOID,
    FINALIZER_ADMIN,
    ItemRow,
    RoleFenceRow,
    STATE_PENDING,
    STATE_UNSPECIFIED,
    STATE_VOIDED,
    StreamRow,
    TERMINAL_STATES,
    row_shard,
    valid_role_fence_state,
)
from .notify import InboxNotifier
from .registry import InboxRegistry
from .store import DataServiceDirectory, InboxStoreCore, _TxRollbackReturn

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "player_inbox_schema.sql"
DB_NOW_MS = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)"
LOCK_ERRORS = (1205, 1213, 9007)  # 锁等待超时、死锁、TiDB 写冲突
DUP_ENTRY = 1062
TX_MAX_ATTEMPTS = 3

_STREAM_TABLE = "`player_inbox_stream`"
_ITEM_TABLE = "`player_inbox_item`"
_AUDIT_TABLE = "`player_inbox_audit`"
_FENCE_TABLE = "`role_fence`"

_STREAM_COLUMNS = (
    "`shard`,`player_id`,`domain`,`next_seq`,`fence_epoch`,`open_count`,"
    "`notify_due_ms`,`created_ms`,`updated_ms`"
)
_ITEM_COLUMNS = (
    "`shard`,`player_id`,`domain`,`seq`,`msg_type`,`producer_kind`,`request_id`,"
    "`content_sha256`,`payload`,`state`,`directive`,`void_reason`,`delivery_count`,"
    "`last_delivered_epoch`,`last_delivered_ms`,`outcome_reason`,`finalized_by`,"
    "`decided_epoch`,`issued_at_ms`,`created_ms`,`updated_ms`,`operator_name`,"
    "`note`,`owner_account`,`owner_revision`"
)
_AUDIT_COLUMNS = (
    "`shard`,`player_id`,`domain`,`seq`,`action`,`operator`,`peer_addr`,`reason`,"
    "`created_ms`,`msg_type`,`request_id`,`issued_at_ms`,`content_sha256`,`payload`,`void_reason`"
)
_STREAM_PK = "`shard` = %s AND `player_id` = %s AND `domain` = %s"
_ITEM_PK = "`shard` = %s AND `player_id` = %s AND `domain` = %s AND `seq` = %s"
_TERMINAL_STATES_SQL = "(" + ",".join(str(s) for s in sorted(TERMINAL_STATES)) + ")"

_APPLY_ROLE_FENCE_SQL = (
    "INSERT INTO role_fence(player_id,account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms) "
    f"VALUES (%s,UNHEX(%s),%s,%s,%s,%s,%s,{DB_NOW_MS}) "
    "ON DUPLICATE KEY UPDATE "
    "account=IF(VALUES(version)>version,VALUES(account),account),"
    "owner_revision=IF(VALUES(version)>version,VALUES(owner_revision),owner_revision),"
    "freeze_token=IF(VALUES(version)>version,VALUES(freeze_token),freeze_token),"
    "freeze_final=IF(VALUES(version)>version,VALUES(freeze_final),freeze_final),"
    "home_zone=IF(VALUES(version)>version,VALUES(home_zone),home_zone),"
    "updated_ms=IF(VALUES(version)>version,VALUES(updated_ms),updated_ms),"
    "version=IF(VALUES(version)>version,VALUES(version),version)"
)

T = TypeVar("T")


def _to_bytes(value) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


def _to_str(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8")
    return str(value)


def _check_row_shard(shard: int, player_id: int) -> None:
    want = row_shard(player_id)
    if shard != want:
        raise StoreUnavailableError(f"corrupt row shard={shard} for player_id={player_id} (want {want})")


def _scan_stream(row) -> StreamRow:
    shard, player_id = int(row[0]), int(row[1])
    _check_row_shard(shard, player_id)
    return StreamRow(
        shard=shard,
        player_id=player_id,
        domain=int(row[2]),
        next_seq=int(row[3]),
        fence_epoch=int(row[4]),
        open_count=int(row[5]),
        notify_due_ms=int(row[6]),
        created_ms=int(row[7]),
        updated_ms=int(row[8]),
    )


def _scan_item(row) -> ItemRow:
    shard, player_id = int(row[0]), int(row[1])
    _check_row_shard(shard, player_id)
    return ItemRow(
        shard=shard,
        player_id=player_id,
        domain=int(row[2]),
        seq=int(row[3]),
        msg_type=int(row[4]),
        producer_kind=int(row[5]),
        request_id=int(row[6]),
        content_sha256=_to_bytes(row[7]),
        payload=_to_bytes(row[8]),
        state=int(row[9]),
        directive=int(row[10]),
        void_reason=int(row[11]),
        delivery_count=int(row[12]),
        last_delivered_epoch=int(row[13]),
        last_delivered_ms=int(row[14]),
        outcome_reason=int(row[15]),
        finalized_by=int(row[16]),
        decided_epoch=int(row[17]),
        issued_at_ms=int(row[18]),
        created_ms=int(row[19]),
        updated_ms=int(row[20]),
        operator_name=_to_str(row[21]),
        note=_to_str(row[22]),
        owner_account=_to_str(row[23]),
        owner_revision=int(row[24]),
    )


def _scan_role_fence(player_id: int, row) -> RoleFenceRow:
    fence = RoleFenceRow(
        player_id=player_id,
        account=_to_str(row[0]),
        owner_revision=int(row[1]),
        freeze_token=int(row[2]),
        freeze_final=bool(row[3]),
        version=int(row[4]),
        home_zone=int(row[5]),
        updated_ms=int(row[6]),
    )
    if not valid_role_fence_state(fence):
        raise StoreUnavailableError(f"corrupt role_fence row player={player_id}")
    return fence


def _require_one_row(rowcount: int, what: str) -> None:
    if rowcount != 1:
        raise StoreUnavailableError(f"{what} affected {rowcount} rows, want 1")


def _mysql_errno(exc: BaseException) -> int | None:
    if isinstance(exc, pymysql.MySQLError) and exc.args:
        code = exc.args[0]
        return code if isinstance(code, int) else None
    return None


class _SqlTx:
    """单 MySQL/TiDB 事务内的游标句柄，全部 bytes 参数走 UNHEX(%s)。"""

    def __init__(self, cursor) -> None:
        self._cur = cursor

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        await self._cur.execute(
            "SELECT account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms "
            f"FROM {_FENCE_TABLE} WHERE player_id=%s FOR UPDATE",
            (player_id,),
        )
        row = await self._cur.fetchone()
        return _scan_role_fence(player_id, row) if row is not None else None

    async def apply_role_fence(self, state: Message, _now_ms: int) -> bool:
        await self._cur.execute(
            _APPLY_ROLE_FENCE_SQL,
            (
                int(state.player_id),
                state.account.encode("utf-8").hex(),
                int(state.owner_revision),
                int(state.freeze_token),
                1 if state.freeze_final else 0,
                int(state.version),
                int(state.home_zone),
            ),
        )
        return self._cur.rowcount != 0

    async def has_pending_item_any_domain(self, shard: int, player_id: int) -> bool:
        await self._cur.execute(
            f"SELECT `player_id` FROM {_ITEM_TABLE} "
            f"WHERE `shard`=%s AND `player_id`=%s AND (`state` IS NULL OR `state` NOT IN {_TERMINAL_STATES_SQL}) "
            "LIMIT 1 FOR UPDATE",
            (shard, player_id),
        )
        return await self._cur.fetchone() is not None

    async def has_mismatched_pending_original(
        self, shard: int, player_id: int, account: str, owner_revision: int
    ) -> bool:
        await self._cur.execute(
            f"SELECT `seq` FROM {_ITEM_TABLE} "
            f"WHERE `shard`=%s AND `player_id`=%s "
            f"AND (`state` IS NULL OR `state` NOT IN {_TERMINAL_STATES_SQL}) "
            "AND (`state` IS NULL OR `state`<>%s OR `owner_account` IS NULL "
            "OR BINARY `owner_account`<>UNHEX(%s) OR `owner_revision` IS NULL OR `owner_revision`<>%s) "
            "LIMIT 1",
            (shard, player_id, STATE_PENDING, account.encode("utf-8").hex(), owner_revision),
        )
        return await self._cur.fetchone() is not None

    async def ensure_stream(self, shard: int, player_id: int, domain: int, now_ms: int) -> None:
        await self._cur.execute(
            f"INSERT IGNORE INTO {_STREAM_TABLE} ({_STREAM_COLUMNS}) VALUES (%s,%s,%s,1,0,0,0,%s,%s)",
            (shard, player_id, domain, now_ms, now_ms),
        )

    async def lock_stream(self, shard: int, player_id: int, domain: int) -> StreamRow | None:
        await self._cur.execute(
            f"SELECT {_STREAM_COLUMNS} FROM {_STREAM_TABLE} WHERE {_STREAM_PK} FOR UPDATE",
            (shard, player_id, domain),
        )
        row = await self._cur.fetchone()
        return _scan_stream(row) if row is not None else None

    async def update_stream_enqueue(
        self, shard: int, player_id: int, domain: int, next_seq: int, open_count: int, notify_due_ms: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            f"UPDATE {_STREAM_TABLE} SET `next_seq`=%s,`open_count`=%s,`notify_due_ms`=%s,`updated_ms`=%s "
            f"WHERE {_STREAM_PK}",
            (next_seq, open_count, notify_due_ms, now_ms, shard, player_id, domain),
        )
        _require_one_row(self._cur.rowcount, "advance stream")

    async def update_stream_fetch(
        self, shard: int, player_id: int, domain: int, fence_epoch: int, next_seq: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            f"UPDATE {_STREAM_TABLE} SET `fence_epoch`=%s,`next_seq`=%s,`updated_ms`=%s WHERE {_STREAM_PK}",
            (fence_epoch, next_seq, now_ms, shard, player_id, domain),
        )
        _require_one_row(self._cur.rowcount, "write fence")

    async def update_stream_ack(
        self, shard: int, player_id: int, domain: int, open_count: int, fence_epoch: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            f"UPDATE {_STREAM_TABLE} SET `open_count`=%s,`fence_epoch`=%s,`updated_ms`=%s WHERE {_STREAM_PK}",
            (open_count, fence_epoch, now_ms, shard, player_id, domain),
        )
        _require_one_row(self._cur.rowcount, "update stream after ack")

    async def update_stream_open_count(
        self, shard: int, player_id: int, domain: int, open_count: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            f"UPDATE {_STREAM_TABLE} SET `open_count`=%s,`updated_ms`=%s WHERE {_STREAM_PK}",
            (open_count, now_ms, shard, player_id, domain),
        )
        _require_one_row(self._cur.rowcount, "update stream open_count")

    async def read_item_by_request(
        self, shard: int, player_id: int, producer_kind: int, request_id: int
    ) -> ItemRow | None:
        await self._cur.execute(
            f"SELECT {_ITEM_COLUMNS} FROM {_ITEM_TABLE} "
            "WHERE `shard`=%s AND `player_id`=%s AND `producer_kind`=%s AND `request_id`=%s",
            (shard, player_id, producer_kind, request_id),
        )
        row = await self._cur.fetchone()
        return _scan_item(row) if row is not None else None

    async def read_item(self, shard: int, player_id: int, domain: int, seq: int) -> ItemRow | None:
        await self._cur.execute(
            f"SELECT {_ITEM_COLUMNS} FROM {_ITEM_TABLE} WHERE {_ITEM_PK}",
            (shard, player_id, domain, seq),
        )
        row = await self._cur.fetchone()
        return _scan_item(row) if row is not None else None

    async def read_items_by_seq(
        self, shard: int, player_id: int, domain: int, seqs: tuple[int, ...]
    ) -> dict[int, ItemRow]:
        if not seqs:
            return {}
        marks = ",".join(["%s"] * len(seqs))
        await self._cur.execute(
            f"SELECT {_ITEM_COLUMNS} FROM {_ITEM_TABLE} "
            f"WHERE `shard`=%s AND `player_id`=%s AND `domain`=%s AND `seq` IN ({marks})",
            (shard, player_id, domain, *seqs),
        )
        rows = await self._cur.fetchall()
        out: dict[int, ItemRow] = {}
        for raw in rows:
            item = _scan_item(raw)
            out[item.seq] = item
        return out

    async def query_pending_items(self, shard: int, player_id: int, domain: int, limit: int) -> list[ItemRow]:
        await self._cur.execute(
            f"SELECT {_ITEM_COLUMNS} FROM {_ITEM_TABLE} "
            "WHERE `shard`=%s AND `player_id`=%s AND `domain`=%s AND `state`=%s "
            "ORDER BY `seq` ASC LIMIT %s",
            (shard, player_id, domain, STATE_PENDING, limit),
        )
        return [_scan_item(row) for row in await self._cur.fetchall()]

    async def insert_item(self, row: ItemRow) -> None:
        try:
            await self._cur.execute(
                f"INSERT INTO {_ITEM_TABLE} ({_ITEM_COLUMNS}) VALUES ("
                "%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),UNHEX(%s),"
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    row.shard,
                    row.player_id,
                    row.domain,
                    row.seq,
                    row.msg_type,
                    row.producer_kind,
                    row.request_id,
                    (row.content_sha256 or b"").hex(),
                    (row.payload or b"").hex(),
                    row.state,
                    row.directive,
                    row.void_reason,
                    row.delivery_count,
                    row.last_delivered_epoch,
                    row.last_delivered_ms,
                    row.outcome_reason,
                    row.finalized_by,
                    row.decided_epoch,
                    row.issued_at_ms,
                    row.created_ms,
                    row.updated_ms,
                    row.operator_name,
                    row.note,
                    row.owner_account,
                    row.owner_revision,
                ),
            )
        except pymysql.err.IntegrityError as exc:
            if _mysql_errno(exc) == DUP_ENTRY:
                raise StoreUnavailableError("duplicate key while role fence is held") from exc
            raise

    async def mark_item_delivered(
        self, shard: int, player_id: int, domain: int, seq: int, epoch: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            f"UPDATE {_ITEM_TABLE} SET `delivery_count`=`delivery_count`+1,"
            "`last_delivered_epoch`=%s,`last_delivered_ms`=%s,`updated_ms`=%s "
            f"WHERE {_ITEM_PK} AND `state`=%s",
            (epoch, now_ms, now_ms, shard, player_id, domain, seq, STATE_PENDING),
        )
        _require_one_row(self._cur.rowcount, "mark delivered")

    async def finalize_item(
        self,
        shard: int,
        player_id: int,
        domain: int,
        seq: int,
        new_state: int,
        finalizer: int,
        decided_epoch: int,
        outcome_reason: int,
        now_ms: int,
    ) -> None:
        await self._cur.execute(
            f"UPDATE {_ITEM_TABLE} SET `state`=%s,`finalized_by`=%s,`decided_epoch`=%s,"
            "`outcome_reason`=%s,`updated_ms`=%s "
            f"WHERE {_ITEM_PK} AND `state`=%s",
            (new_state, finalizer, decided_epoch, outcome_reason, now_ms, shard, player_id, domain, seq, STATE_PENDING),
        )
        _require_one_row(self._cur.rowcount, "finalize item")

    async def void_item_direct(
        self, shard: int, player_id: int, domain: int, seq: int, void_reason: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            f"UPDATE {_ITEM_TABLE} SET `state`=%s,`finalized_by`=%s,`void_reason`=%s,`updated_ms`=%s "
            f"WHERE {_ITEM_PK} AND `state`=%s AND `delivery_count`=0",
            (STATE_VOIDED, FINALIZER_ADMIN, void_reason, now_ms, shard, player_id, domain, seq, STATE_PENDING),
        )
        _require_one_row(self._cur.rowcount, "void item")

    async def attach_void_directive(
        self, shard: int, player_id: int, domain: int, seq: int, void_reason: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            f"UPDATE {_ITEM_TABLE} SET `directive`=%s,`void_reason`=%s,`updated_ms`=%s "
            f"WHERE {_ITEM_PK} AND `state`=%s",
            (DIRECTIVE_VOID, void_reason, now_ms, shard, player_id, domain, seq, STATE_PENDING),
        )
        _require_one_row(self._cur.rowcount, "attach void directive")

    async def force_void_item(
        self, shard: int, player_id: int, domain: int, seq: int, void_reason: int, request_id: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            f"UPDATE {_ITEM_TABLE} SET `state`=%s,`finalized_by`=%s,`void_reason`=%s,`updated_ms`=%s "
            f"WHERE {_ITEM_PK} AND `state`=%s AND `delivery_count`>0 AND `request_id`=%s",
            (
                STATE_VOIDED,
                FINALIZER_ADMIN,
                void_reason,
                now_ms,
                shard,
                player_id,
                domain,
                seq,
                STATE_PENDING,
                request_id,
            ),
        )
        _require_one_row(self._cur.rowcount, "force void item")

    async def insert_audit(self, row: AuditRow) -> None:
        await self._cur.execute(
            f"INSERT IGNORE INTO {_AUDIT_TABLE} ({_AUDIT_COLUMNS}) VALUES ("
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),UNHEX(%s),%s)",
            (
                row.shard,
                row.player_id,
                row.domain,
                row.seq,
                row.action,
                row.operator,
                row.peer_addr,
                row.reason,
                row.created_ms,
                row.msg_type,
                row.request_id,
                row.issued_at_ms,
                (row.content_sha256 or b"").hex(),
                (row.payload or b"").hex(),
                row.void_reason,
            ),
        )

    async def purge_player_rows(self, shard: int, player_id: int) -> None:
        for table in (_STREAM_TABLE, _ITEM_TABLE):
            await self._cur.execute(
                f"DELETE FROM {table} WHERE `shard`=%s AND `player_id`=%s",
                (shard, player_id),
            )


class _SqlDriver:
    """基于 aiomysql 连接池的事务与非事务存储驱动。"""

    def __init__(self, pool) -> None:
        self._pool = pool

    async def run_tx(self, work: Callable[[_SqlTx], Awaitable[T]]) -> T:
        for attempt in range(1, TX_MAX_ATTEMPTS + 1):
            try:
                return await self._run_tx_once(work)
            except (InboxError, _TxRollbackReturn):
                raise
            except pymysql.MySQLError as exc:
                if _mysql_errno(exc) in LOCK_ERRORS and attempt < TX_MAX_ATTEMPTS:
                    await asyncio.sleep(0.01 * attempt + random.uniform(0.0, 0.01))
                    continue
                raise StoreUnavailableError(f"mysql transaction failed: {exc}") from exc
        raise StoreUnavailableError("mysql transaction exhausted retries")

    async def _run_tx_once(self, work: Callable[[_SqlTx], Awaitable[T]]) -> T:
        business_error: BaseException | None = None
        rollback_return: _TxRollbackReturn | None = None
        async with portal_sql.connection(self._pool) as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cursor:
                    result = await work(_SqlTx(cursor))
                await conn.commit()
                return result
            except _TxRollbackReturn as ret:
                await conn.rollback()
                rollback_return = ret
            except InboxError as exc:
                await conn.rollback()
                business_error = exc
        if rollback_return is not None:
            return rollback_return.value
        assert business_error is not None
        raise business_error

    async def apply_role_fence_direct(self, state: Message, _now_ms: int) -> bool:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                _APPLY_ROLE_FENCE_SQL,
                (
                    int(state.player_id),
                    state.account.encode("utf-8").hex(),
                    int(state.owner_revision),
                    int(state.freeze_token),
                    1 if state.freeze_final else 0,
                    int(state.version),
                    int(state.home_zone),
                ),
            )
            return cursor.rowcount != 0

    async def clear_notify_due(self, shard: int, player_id: int, domain: int, expect_next_seq: int) -> bool:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"UPDATE {_STREAM_TABLE} SET `notify_due_ms`=0 "
                f"WHERE {_STREAM_PK} AND `next_seq`=%s AND `notify_due_ms`<>0",
                (shard, player_id, domain, expect_next_seq),
            )
            return cursor.rowcount == 1

    async def read_streams_for_player(self, shard: int, player_id: int) -> list[StreamRow]:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {_STREAM_COLUMNS} FROM {_STREAM_TABLE} "
                "WHERE `shard`=%s AND `player_id`=%s ORDER BY `domain` ASC",
                (shard, player_id),
            )
            return [_scan_stream(row) for row in await cursor.fetchall()]

    async def read_stream(self, shard: int, player_id: int, domain: int) -> StreamRow | None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {_STREAM_COLUMNS} FROM {_STREAM_TABLE} WHERE {_STREAM_PK}",
                (shard, player_id, domain),
            )
            row = await cursor.fetchone()
            return _scan_stream(row) if row is not None else None

    async def read_pending_aggregates(self, shard: int, player_id: int) -> dict[int, tuple[int, int]]:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "SELECT `domain`, COALESCE(SUM(CASE WHEN `delivery_count` > 0 THEN 1 ELSE 0 END), 0), "
                f"COALESCE(MIN(`created_ms`), 0) FROM {_ITEM_TABLE} "
                "WHERE `shard`=%s AND `player_id`=%s AND `state`=%s GROUP BY `domain`",
                (shard, player_id, STATE_PENDING),
            )
            return {int(row[0]): (int(row[1]), int(row[2])) for row in await cursor.fetchall()}

    async def list_items(
        self, shard: int, player_id: int, domain: int, state: int, after_seq: int, limit: int
    ) -> list[ItemRow]:
        query = (
            f"SELECT {_ITEM_COLUMNS} FROM {_ITEM_TABLE} "
            "WHERE `shard`=%s AND `player_id`=%s AND `domain`=%s AND `seq`>%s"
        )
        args: list = [shard, player_id, domain, after_seq]
        if state != STATE_UNSPECIFIED:
            query += " AND `state`=%s"
            args.append(state)
        query += " ORDER BY `seq` ASC LIMIT %s"
        args.append(limit)
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(query, tuple(args))
            return [_scan_item(row) for row in await cursor.fetchall()]

    async def read_item_by_request_direct(
        self, shard: int, player_id: int, producer_kind: int, request_id: int
    ) -> ItemRow | None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {_ITEM_COLUMNS} FROM {_ITEM_TABLE} "
                "WHERE `shard`=%s AND `player_id`=%s AND `producer_kind`=%s AND `request_id`=%s",
                (shard, player_id, producer_kind, request_id),
            )
            row = await cursor.fetchone()
            return _scan_item(row) if row is not None else None

    async def list_player_items_for_reconcile(self, shard: int, player_id: int, domain: int) -> list[ItemRow]:
        query = f"SELECT {_ITEM_COLUMNS} FROM {_ITEM_TABLE} WHERE `shard`=%s AND `player_id`=%s"
        args: list = [shard, player_id]
        if domain != DOMAIN_UNSPECIFIED:
            query += " AND `domain`=%s"
            args.append(domain)
        query += " ORDER BY `domain` ASC, `seq` ASC"
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(query, tuple(args))
            return [_scan_item(row) for row in await cursor.fetchall()]

    async def read_pending_item_gain_sanctions(
        self, shard: int, player_id: int, domain: int, msg_types: tuple[int, ...], limit: int
    ) -> list[ItemRow]:
        marks = ",".join(["%s"] * len(msg_types))
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {_ITEM_COLUMNS} FROM {_ITEM_TABLE} "
                f"WHERE `shard`=%s AND `player_id`=%s AND `domain`=%s AND `state`=%s "
                f"AND `msg_type` IN ({marks}) ORDER BY `seq` ASC LIMIT %s",
                (shard, player_id, domain, STATE_PENDING, *msg_types, limit),
            )
            return [_scan_item(row) for row in await cursor.fetchall()]


class SqlInboxStore(InboxStoreCore):
    """基于 MySQL/TiDB 的收件箱存储，带建表自检、挂起索引核对与分片布局核对。"""

    def __init__(
        self,
        pool,
        protocol: Protocol,
        registry: InboxRegistry,
        directory: DataServiceDirectory,
        *,
        allowed_types: frozenset[int] = frozenset(),
        notifier: InboxNotifier | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._pool = pool
        super().__init__(
            protocol,
            registry,
            directory,
            _SqlDriver(pool),
            allowed_types=allowed_types,
            notifier=notifier,
            clock_ms=clock_ms,
        )

    async def bootstrap(self, schema: Path = SCHEMA_FILE) -> None:
        """幂等执行 config/player_inbox_schema.sql 中的全部建表语句。"""
        text = "\n".join(line for line in schema.read_text("utf-8").splitlines() if not line.startswith("--"))
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for statement in re.split(r";\s*\n", text):
                sql = statement.strip().rstrip(";")
                if sql:
                    await cursor.execute(sql)

    async def verify_schema(self) -> None:
        """启动前核对核心表与 role_fence 副本结构（fail-closed）。"""
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(f"SELECT {_STREAM_COLUMNS} FROM {_STREAM_TABLE} LIMIT 0")
            await cursor.execute(f"SELECT {_ITEM_COLUMNS} FROM {_ITEM_TABLE} LIMIT 0")
            await cursor.execute(f"SELECT {_AUDIT_COLUMNS} FROM {_AUDIT_TABLE} LIMIT 0")
            await cursor.execute(
                f"SELECT player_id,account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms "
                f"FROM {_FENCE_TABLE} LIMIT 0"
            )
            await cursor.execute(
                "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='role_fence' AND COLUMN_NAME='account'"
            )
            col = await cursor.fetchone()
            if col is None or str(col[0]).lower() != "varbinary" or int(col[1] or 0) != 191:
                raise RuntimeError("role_fence.account must be VARBINARY(191)")

    async def check_authority_indexes(self) -> None:
        """核对 player_inbox_item 具备 (shard, player_id, state) 挂起核验索引。"""
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "SELECT index_name, column_name FROM information_schema.statistics "
                "WHERE table_schema=DATABASE() AND table_name='player_inbox_item' "
                "ORDER BY index_name, seq_in_index"
            )
            indexes: dict[str, list[str]] = {}
            for name, column in await cursor.fetchall():
                indexes.setdefault(str(name), []).append(str(column))
            if not any(cols == ["shard", "player_id", "state"] for cols in indexes.values()):
                raise RuntimeError("player_inbox_item missing (shard, player_id, state) index")

    async def check_row_shards(self) -> None:
        """逐表核对 (shard, player_id) 与 splitmix64(player_id) % 64 一致性。"""
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for table in (_STREAM_TABLE, _ITEM_TABLE, _AUDIT_TABLE):
                await cursor.execute(f"SELECT DISTINCT `shard`, `player_id` FROM {table}")
                for shard, player_id in await cursor.fetchall():
                    _check_row_shard(int(shard), int(player_id))


async def open_pool(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    maxsize: int = 20,
    minsize: int = 1,
):
    """创建 READ-COMMITTED / autocommit / utf8mb4 连接池。"""
    return await aiomysql.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        db=database,
        minsize=minsize,
        maxsize=maxsize,
        connect_timeout=3,
        autocommit=True,
        charset="utf8mb4",
        init_command="SET SESSION transaction_isolation='READ-COMMITTED'",
    )
