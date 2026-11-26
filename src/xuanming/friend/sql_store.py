"""friend MySQL/TiDB 权威存储实现（SqlFriendStore）。

全部二进制字段（``role_fence.account``）统一通过 ``UNHEX(%s)`` 绑定，避免 utf8mb4 连接下的字节转码损坏；
事务统一运行在 ``READ-COMMITTED`` 隔离级别下，严格遵守 ``role_fence -> friend_capacity -> 单行主键点锁`` 锁序。
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import TypeVar

import aiomysql
import pymysql
from google.protobuf.message import Message

from .. import portal_sql
from ..protocol import Protocol
from .errors import FriendError, StoreUnavailableError
from .model import (
    BlockEntry,
    FriendEntry,
    FriendRequestEntry,
    FriendRules,
    RECOMMEND_ANCHOR_WINDOW,
    REQUEST_STATUS_PENDING,
    RecommendCandidate,
    RoleFenceRow,
    valid_role_fence_state,
)
from .redis_client import CacheInvalidator
from .store import (
    FriendDataServiceClient,
    FriendStoreCore,
    _CapacityRowsMissing,
    _RequestIdRequired,
    _RoleFenceStale,
    _TxRollbackReturn,
)

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "friend_schema.sql"
DB_NOW_MS = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)"
LOCK_ERRORS = (1205, 1213, 9007)  # 锁等待超时、死锁、TiDB 写冲突
TX_MAX_ATTEMPTS = 3

# 与 Go shared/rolefence/rolefence.go 完全一致：version 放最后，确保前面 IF 读到旧 version。
APPLY_ROLE_FENCE_SQL = (
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

LOCK_ROLE_FENCE_SQL = (
    "SELECT account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms "
    "FROM role_fence WHERE player_id=%s FOR UPDATE"
)

# 守卫之后全部锁定读写均为完整主键点查 / 点更新（与 Go friend_repo.go / block_repo.go 一致）。
ENSURE_CAPACITY_ROW_SQL = (
    "INSERT INTO friend_capacity (player_id, friend_count, created_ms) VALUES (%s, %s, %s) "
    "ON DUPLICATE KEY UPDATE player_id = player_id"
)
LOCK_CAPACITY_ROW_SQL = "SELECT friend_count FROM friend_capacity WHERE player_id = %s FOR UPDATE"
LOCK_BLOCK_ROW_SQL = "SELECT 1 FROM friend_block WHERE player_id = %s AND blocked_player_id = %s FOR UPDATE"
LOCK_FRIEND_EDGE_ROW_SQL = "SELECT 1 FROM friend WHERE player_id = %s AND friend_player_id = %s FOR UPDATE"
LOCK_REQUEST_ROW_SQL = (
    "SELECT status, request_id, request_time_ms FROM friend_request "
    "WHERE from_player_id = %s AND to_player_id = %s FOR UPDATE"
)
INSERT_BLOCK_ROW_SQL = (
    "INSERT INTO friend_block (player_id, blocked_player_id, since_ms) VALUES (%s, %s, %s) "
    "ON DUPLICATE KEY UPDATE player_id = player_id"
)
CANCEL_PENDING_REQUEST_SQL = (
    "UPDATE friend_request SET status = %s, updated_ms = %s "
    "WHERE from_player_id = %s AND to_player_id = %s AND status = %s"
)
UNBLOCK_SQL = "DELETE FROM friend_block WHERE player_id = %s AND blocked_player_id = %s"
ROLE_PURGE_PEERS_SQL = (
    "SELECT friend_player_id FROM friend WHERE player_id = %s"
    " UNION SELECT player_id FROM friend WHERE friend_player_id = %s"
    " UNION SELECT to_player_id FROM friend_request WHERE from_player_id = %s"
    " UNION SELECT from_player_id FROM friend_request WHERE to_player_id = %s"
    " UNION SELECT blocked_player_id FROM friend_block WHERE player_id = %s"
    " UNION SELECT player_id FROM friend_block WHERE blocked_player_id = %s"
)

RECOMMEND_BY_MUTUAL_BASE_SQL = """SELECT c.candidate_id, c.mutual
FROM (SELECT f2.friend_player_id AS candidate_id, COUNT(*) AS mutual, RAND() AS shuffle
      FROM friend f1 FORCE INDEX (PRIMARY)
      STRAIGHT_JOIN friend f2 FORCE INDEX (PRIMARY) ON f2.player_id = f1.friend_player_id
      WHERE f1.player_id = %s
        AND f2.friend_player_id <> %s
      GROUP BY f2.friend_player_id
      ORDER BY mutual DESC, shuffle
      LIMIT %s) AS c
WHERE NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend f FORCE INDEX (PRIMARY)
        WHERE f.player_id = %s AND f.friend_player_id = c.candidate_id)
  AND NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend_block b_out FORCE INDEX (PRIMARY)
        WHERE b_out.player_id = %s AND b_out.blocked_player_id = c.candidate_id)
  AND NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend_block b_in FORCE INDEX (PRIMARY)
        WHERE b_in.player_id = c.candidate_id AND b_in.blocked_player_id = %s)
  AND NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend_request r_out FORCE INDEX (PRIMARY)
        WHERE r_out.from_player_id = %s AND r_out.to_player_id = c.candidate_id AND r_out.status = 1)
  AND NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend_request r_in FORCE INDEX (PRIMARY)
        WHERE r_in.from_player_id = c.candidate_id AND r_in.to_player_id = %s AND r_in.status = 1)"""

RECOMMEND_ANCHOR_BASE_SQL = """SELECT c.player_id,
       (SELECT COUNT(*) FROM friend m1 FORCE INDEX (PRIMARY)
        STRAIGHT_JOIN friend m2 FORCE INDEX (PRIMARY)
                ON m2.player_id = c.player_id AND m2.friend_player_id = m1.friend_player_id
        WHERE m1.player_id = %s) AS mutual
FROM (SELECT DISTINCT player_id FROM friend FORCE INDEX (PRIMARY)
      WHERE player_id >= %s
      ORDER BY player_id
      LIMIT %s) AS c
WHERE c.player_id <> %s
  AND NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend f FORCE INDEX (PRIMARY)
        WHERE f.player_id = %s AND f.friend_player_id = c.player_id)
  AND NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend_block b_out FORCE INDEX (PRIMARY)
        WHERE b_out.player_id = %s AND b_out.blocked_player_id = c.player_id)
  AND NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend_block b_in FORCE INDEX (PRIMARY)
        WHERE b_in.player_id = c.player_id AND b_in.blocked_player_id = %s)
  AND NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend_request r_out FORCE INDEX (PRIMARY)
        WHERE r_out.from_player_id = %s AND r_out.to_player_id = c.player_id AND r_out.status = 1)
  AND NOT EXISTS (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ 1 FROM friend_request r_in FORCE INDEX (PRIMARY)
        WHERE r_in.from_player_id = c.player_id AND r_in.to_player_id = %s AND r_in.status = 1)"""

T = TypeVar("T")


def _to_str(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8")
    return str(value)


def _mysql_errno(exc: BaseException) -> int | None:
    if isinstance(exc, pymysql.MySQLError) and exc.args:
        code = exc.args[0]
        return code if isinstance(code, int) else None
    return None


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


def _exclude_clause(col: str, exclude: Sequence[int]) -> tuple[str, list[int]]:
    ids = [int(x) for x in exclude]
    if not ids:
        return "", []
    marks = ",".join(["%s"] * len(ids))
    return f" AND {col} NOT IN ({marks})", ids


class _SqlTx:
    """单 MySQL/TiDB 事务内游标句柄：二进制列统一经 UNHEX(%s) 绑定。"""

    def __init__(self, cursor) -> None:
        self._cur = cursor

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        await self._cur.execute(LOCK_ROLE_FENCE_SQL, (player_id,))
        row = await self._cur.fetchone()
        return _scan_role_fence(player_id, row) if row is not None else None

    async def apply_role_fence(self, state: Message, _now_ms: int) -> bool:
        await self._cur.execute(
            APPLY_ROLE_FENCE_SQL,
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
        return int(self._cur.rowcount) != 0

    async def count_authoritative_friends(self, player_id: int) -> int:
        await self._cur.execute("SELECT COUNT(*) FROM friend WHERE player_id = %s", (player_id,))
        row = await self._cur.fetchone()
        return int(row[0]) if row is not None else 0

    async def ensure_capacity_row(self, player_id: int, count: int, now_ms: int) -> None:
        await self._cur.execute(ENSURE_CAPACITY_ROW_SQL, (player_id, count, now_ms))

    async def lock_capacity_row(self, player_id: int) -> int | None:
        await self._cur.execute(LOCK_CAPACITY_ROW_SQL, (player_id,))
        row = await self._cur.fetchone()
        return int(row[0]) if row is not None else None

    async def lock_block_row_exists(self, player_id: int, blocked_player_id: int) -> bool:
        await self._cur.execute(LOCK_BLOCK_ROW_SQL, (player_id, blocked_player_id))
        return await self._cur.fetchone() is not None

    async def lock_friend_edge_exists(self, player_id: int, friend_player_id: int) -> bool:
        await self._cur.execute(LOCK_FRIEND_EDGE_ROW_SQL, (player_id, friend_player_id))
        return await self._cur.fetchone() is not None

    async def lock_request_row(self, from_player_id: int, to_player_id: int) -> tuple[int, int, int] | None:
        await self._cur.execute(LOCK_REQUEST_ROW_SQL, (from_player_id, to_player_id))
        row = await self._cur.fetchone()
        if row is None:
            return None
        return (int(row[0]), int(row[1]), int(row[2]))

    async def count_outgoing_pending(self, from_player_id: int) -> int:
        await self._cur.execute(
            "SELECT COUNT(*) FROM friend_request WHERE from_player_id = %s AND status = %s",
            (from_player_id, REQUEST_STATUS_PENDING),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row is not None else 0

    async def count_incoming_pending(self, to_player_id: int) -> int:
        await self._cur.execute(
            "SELECT COUNT(*) FROM friend_request WHERE to_player_id = %s AND status = %s",
            (to_player_id, REQUEST_STATUS_PENDING),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row is not None else 0

    async def upsert_pending_request(
        self, from_player_id: int, to_player_id: int, now_ms: int, request_id: int
    ) -> None:
        await self._cur.execute(
            "INSERT INTO friend_request (from_player_id, to_player_id, request_time_ms, status, updated_ms, request_id) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE status=VALUES(status), request_time_ms=VALUES(request_time_ms), "
            "updated_ms=VALUES(updated_ms), request_id=VALUES(request_id)",
            (from_player_id, to_player_id, now_ms, REQUEST_STATUS_PENDING, now_ms, request_id),
        )

    async def backfill_pending_request_id(self, from_player_id: int, to_player_id: int, request_id: int) -> int:
        await self._cur.execute(
            "UPDATE friend_request SET request_id = %s "
            "WHERE from_player_id = %s AND to_player_id = %s AND status = %s AND request_id = 0",
            (request_id, from_player_id, to_player_id, REQUEST_STATUS_PENDING),
        )
        return int(self._cur.rowcount)

    async def find_request_by_id(self, request_id: int) -> list[tuple[int, int]]:
        await self._cur.execute(
            "SELECT from_player_id, to_player_id FROM friend_request WHERE request_id = %s LIMIT 2",
            (request_id,),
        )
        return [(int(row[0]), int(row[1])) for row in await self._cur.fetchall()]

    async def update_request_status_cas(
        self, from_player_id: int, to_player_id: int, new_status: int, now_ms: int, request_id: int
    ) -> int:
        await self._cur.execute(
            "UPDATE friend_request SET status = %s, updated_ms = %s "
            "WHERE from_player_id = %s AND to_player_id = %s AND status = %s AND request_id = %s",
            (new_status, now_ms, from_player_id, to_player_id, REQUEST_STATUS_PENDING, request_id),
        )
        return int(self._cur.rowcount)

    async def resolve_reverse_pending(
        self, from_player_id: int, to_player_id: int, new_status: int, now_ms: int
    ) -> int:
        await self._cur.execute(
            CANCEL_PENDING_REQUEST_SQL,
            (new_status, now_ms, from_player_id, to_player_id, REQUEST_STATUS_PENDING),
        )
        return int(self._cur.rowcount)

    async def insert_friend_edge_ignore(self, player_id: int, friend_player_id: int, now_ms: int) -> int:
        await self._cur.execute(
            "INSERT IGNORE INTO friend (player_id, friend_player_id, since_ms) VALUES (%s, %s, %s)",
            (player_id, friend_player_id, now_ms),
        )
        return int(self._cur.rowcount)

    async def increment_friend_count(self, player_id: int) -> None:
        await self._cur.execute(
            "UPDATE friend_capacity SET friend_count = friend_count + 1 WHERE player_id = %s",
            (player_id,),
        )

    async def delete_friend_edge(self, player_id: int, friend_player_id: int) -> int:
        await self._cur.execute(
            "DELETE FROM friend WHERE player_id = %s AND friend_player_id = %s",
            (player_id, friend_player_id),
        )
        return int(self._cur.rowcount)

    async def decrement_friend_count(self, player_id: int, now_ms: int) -> int:
        await self._cur.execute(
            "UPDATE friend_capacity SET friend_count = friend_count - 1, created_ms = %s "
            "WHERE player_id = %s AND friend_count > 0",
            (now_ms, player_id),
        )
        return int(self._cur.rowcount)

    async def friend_edge_exists_plain(self, a: int, b: int) -> bool:
        await self._cur.execute(
            "SELECT 1 FROM friend "
            "WHERE (player_id = %s AND friend_player_id = %s) OR (player_id = %s AND friend_player_id = %s) LIMIT 1",
            (a, b, b, a),
        )
        return await self._cur.fetchone() is not None

    async def pair_blocked_either_way_plain(self, a: int, b: int) -> bool:
        await self._cur.execute(
            "SELECT 1 FROM friend_block WHERE player_id = %s AND blocked_player_id = %s LIMIT 1",
            (a, b),
        )
        if await self._cur.fetchone() is not None:
            return True
        await self._cur.execute(
            "SELECT 1 FROM friend_block WHERE player_id = %s AND blocked_player_id = %s LIMIT 1",
            (b, a),
        )
        return await self._cur.fetchone() is not None

    async def pair_mutual_friends_plain(self, a: int, b: int) -> bool:
        await self._cur.execute(
            "SELECT 1 FROM friend WHERE player_id = %s AND friend_player_id = %s LIMIT 1",
            (a, b),
        )
        if await self._cur.fetchone() is None:
            return False
        await self._cur.execute(
            "SELECT 1 FROM friend WHERE player_id = %s AND friend_player_id = %s LIMIT 1",
            (b, a),
        )
        return await self._cur.fetchone() is not None

    async def query_friend_list(self, player_id: int, limit: int) -> list[FriendEntry]:
        await self._cur.execute(
            "SELECT friend_player_id, since_ms FROM friend WHERE player_id = %s LIMIT %s",
            (player_id, limit),
        )
        return [FriendEntry(friend_player_id=int(row[0]), since_ms=int(row[1])) for row in await self._cur.fetchall()]

    async def query_pending_requests(self, player_id: int, limit: int) -> list[FriendRequestEntry]:
        await self._cur.execute(
            "SELECT from_player_id, to_player_id, request_time_ms, status, request_id "
            "FROM friend_request WHERE to_player_id = %s AND status = %s LIMIT %s",
            (player_id, REQUEST_STATUS_PENDING, limit),
        )
        return [
            FriendRequestEntry(
                from_player_id=int(row[0]),
                to_player_id=int(row[1]),
                request_time_ms=int(row[2]),
                status=int(row[3]),
                request_id=int(row[4]),
            )
            for row in await self._cur.fetchall()
        ]

    async def probe_block_count(self, player_id: int, target_player_id: int) -> tuple[int, int]:
        await self._cur.execute(
            "SELECT COUNT(*), COUNT(CASE WHEN blocked_player_id = %s THEN 1 END) "
            "FROM friend_block WHERE player_id = %s",
            (target_player_id, player_id),
        )
        row = await self._cur.fetchone()
        if row is None:
            return 0, 0
        return int(row[0]), int(row[1])

    async def count_blocks(self, player_id: int) -> int:
        await self._cur.execute("SELECT COUNT(*) FROM friend_block WHERE player_id = %s", (player_id,))
        row = await self._cur.fetchone()
        return int(row[0]) if row is not None else 0

    async def insert_block_odku(self, player_id: int, target_player_id: int, now_ms: int) -> None:
        await self._cur.execute(INSERT_BLOCK_ROW_SQL, (player_id, target_player_id, now_ms))

    async def delete_block(self, player_id: int, target_player_id: int) -> int:
        await self._cur.execute(UNBLOCK_SQL, (player_id, target_player_id))
        return int(self._cur.rowcount)

    async def query_blocks(self, player_id: int, limit: int) -> list[BlockEntry]:
        await self._cur.execute(
            "SELECT blocked_player_id, since_ms FROM friend_block WHERE player_id = %s LIMIT %s",
            (player_id, limit),
        )
        return [BlockEntry(blocked_player_id=int(row[0]), since_ms=int(row[1])) for row in await self._cur.fetchall()]

    async def query_recommend_mutual(
        self, player_id: int, exclude: Sequence[int], limit: int
    ) -> list[RecommendCandidate]:
        clause, ex_args = _exclude_clause("c.candidate_id", exclude)
        sql = RECOMMEND_BY_MUTUAL_BASE_SQL + clause + "\nORDER BY c.mutual DESC, c.shuffle\nLIMIT %s"
        args = (
            player_id,
            player_id,
            RECOMMEND_ANCHOR_WINDOW,
            player_id,
            player_id,
            player_id,
            player_id,
            player_id,
            *ex_args,
            limit,
        )
        await self._cur.execute(sql, args)
        return [
            RecommendCandidate(candidate_player_id=int(row[0]), mutual_friends=int(row[1]))
            for row in await self._cur.fetchall()
        ]

    async def query_friend_id_range(self) -> tuple[int | None, int | None]:
        await self._cur.execute("SELECT MIN(player_id), MAX(player_id) FROM friend")
        row = await self._cur.fetchone()
        if row is None or row[0] is None or row[1] is None:
            return None, None
        return int(row[0]), int(row[1])

    async def query_recommend_anchor(
        self, player_id: int, exclude: Sequence[int], pivot: int, limit: int
    ) -> list[RecommendCandidate]:
        clause, ex_args = _exclude_clause("c.player_id", exclude)
        sql = RECOMMEND_ANCHOR_BASE_SQL + clause + "\nORDER BY c.player_id\nLIMIT %s"
        args = (
            player_id,
            pivot,
            RECOMMEND_ANCHOR_WINDOW,
            player_id,
            player_id,
            player_id,
            player_id,
            player_id,
            player_id,
            *ex_args,
            limit,
        )
        await self._cur.execute(sql, args)
        return [
            RecommendCandidate(candidate_player_id=int(row[0]), mutual_friends=int(row[1]))
            for row in await self._cur.fetchall()
        ]

    async def query_purge_peers(self, player_id: int) -> list[int]:
        await self._cur.execute(
            ROLE_PURGE_PEERS_SQL,
            (player_id, player_id, player_id, player_id, player_id, player_id),
        )
        return [int(row[0]) for row in await self._cur.fetchall()]

    async def delete_request_row(self, from_player_id: int, to_player_id: int) -> None:
        await self._cur.execute(
            "DELETE FROM friend_request WHERE from_player_id = %s AND to_player_id = %s",
            (from_player_id, to_player_id),
        )

    async def delete_capacity_row(self, player_id: int) -> None:
        await self._cur.execute("DELETE FROM friend_capacity WHERE player_id = %s", (player_id,))


_CONTROL_FLOW = (FriendError, _TxRollbackReturn, _RoleFenceStale, _CapacityRowsMissing, _RequestIdRequired)


class _SqlDriver:
    """基于 aiomysql 连接池的事务驱动（READ-COMMITTED + 瞬态锁冲突有界重试）。"""

    def __init__(self, pool) -> None:
        self._pool = pool

    async def run_tx(self, work: Callable[[_SqlTx], Awaitable[T]]) -> T:
        for attempt in range(1, TX_MAX_ATTEMPTS + 1):
            try:
                return await self._run_tx_once(work)
            except _CONTROL_FLOW:
                raise
            except pymysql.MySQLError as exc:
                if _mysql_errno(exc) in LOCK_ERRORS and attempt < TX_MAX_ATTEMPTS:
                    await asyncio.sleep(0.01 * attempt + random.uniform(0.0, 0.01))
                    continue
                raise StoreUnavailableError(f"friend mysql transaction failed: {exc}") from exc
        raise StoreUnavailableError("friend mysql transaction exhausted retries")

    async def _run_tx_once(self, work: Callable[[_SqlTx], Awaitable[T]]) -> T:
        rollback_return: _TxRollbackReturn | None = None
        rethrow: BaseException | None = None
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
            except BaseException as exc:
                await conn.rollback()
                rethrow = exc
        if rollback_return is not None:
            return rollback_return.value
        assert rethrow is not None
        raise rethrow

    async def apply_role_fence_direct(self, state: Message, now_ms: int) -> bool:
        try:
            async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
                return await _SqlTx(cursor).apply_role_fence(state, now_ms)
        except pymysql.MySQLError as exc:
            raise StoreUnavailableError(f"friend role_fence apply failed: {exc}") from exc


class SqlFriendStore(FriendStoreCore):
    """基于 MySQL/TiDB 的权威好友存储，带建表引导与启动期结构校验。"""

    def __init__(
        self,
        pool,
        protocol: Protocol,
        data_service: FriendDataServiceClient,
        rules: FriendRules,
        *,
        cache_invalidator: CacheInvalidator | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._pool = pool
        super().__init__(
            protocol,
            _SqlDriver(pool),
            data_service,
            rules,
            cache_invalidator=cache_invalidator,
            clock_ms=clock_ms,
        )

    async def bootstrap(self, schema: Path = SCHEMA_FILE) -> None:
        """幂等执行 config/friend_schema.sql 中的 5 张权威表 DDL。"""
        text = "\n".join(line for line in schema.read_text("utf-8").splitlines() if not line.startswith("--"))
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for statement in re.split(r";\s*\n", text):
                sql = statement.strip().rstrip(";")
                if sql:
                    await cursor.execute(sql)

    async def verify_schema(self) -> None:
        """启动前核对 5 张表的列、friend_request.request_id 索引及 role_fence.account 列形。"""
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for probe in (
                "SELECT player_id,friend_player_id,since_ms FROM friend LIMIT 0",
                "SELECT from_player_id,to_player_id,request_time_ms,status,updated_ms,request_id "
                "FROM friend_request LIMIT 0",
                "SELECT player_id,friend_count,created_ms FROM friend_capacity LIMIT 0",
                "SELECT player_id,blocked_player_id,since_ms FROM friend_block LIMIT 0",
                "SELECT player_id,account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms "
                "FROM role_fence LIMIT 0",
            ):
                await cursor.execute(probe)

            await cursor.execute(
                "SELECT COUNT(*) FROM information_schema.statistics "
                "WHERE table_schema=DATABASE() AND table_name='friend_request' "
                "AND column_name='request_id' AND seq_in_index=1"
            )
            idx_row = await cursor.fetchone()
            if idx_row is None or int(idx_row[0]) == 0:
                raise RuntimeError("friend_request missing request_id leading index")

            await cursor.execute(
                "SELECT DATA_TYPE,CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='role_fence' AND COLUMN_NAME='account'"
            )
            col_row = await cursor.fetchone()
            if col_row is None or str(col_row[0]).lower() != "varbinary" or int(col_row[1] or 0) != 191:
                raise RuntimeError("role_fence.account must be VARBINARY(191)")

            await cursor.execute(
                "SELECT COLUMN_NAME FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='role_fence' AND INDEX_NAME='PRIMARY' "
                "ORDER BY SEQ_IN_INDEX"
            )
            pk_cols = [str(row[0]) for row in await cursor.fetchall()]
            if pk_cols != ["player_id"]:
                raise RuntimeError(f"role_fence primary key must be (player_id), got {pk_cols}")


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
