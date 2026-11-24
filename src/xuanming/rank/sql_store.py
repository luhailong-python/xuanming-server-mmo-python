"""rank MySQL/TiDB 权威存储实现（SqlRankStore）。

与 Go ``go/rank/internal/data`` 完全对齐：
- 100% SQL 权威，无 Redis；
- 写事务运行在 ``READ COMMITTED`` 隔离级别下，对 1205/1213/9007 及并发新插入 1062 竞态做 3 次带抖动指数退避重试；
- 读事务运行在 ``REPEATABLE READ`` 隔离级别下，保证榜行、总数、当前页与本人名次取自同一快照；
- 所有二进制字段（``rank_mail_reward.attachments``）统一用 ``UNHEX(%s)`` 绑定；
- MySQL/TiDB 保留字列 `` `rank` `` 一律反引号转义。
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
from .errors import (
    EntriesDriftError,
    RankClosedError,
    RankConflictError,
    RankCursorError,
    RankDeniedError,
    RankInvalidError,
    RankNotFoundError,
    ScoreInsertRaceError,
)
from .model import (
    BoardRow,
    MAX_REWARD_MAIL_WINDOW_MS,
    RANK_EXACT_LIMIT,
    RANK_GEN_OPEN,
    RANK_GEN_SEALED,
    REWARD_ABANDONED,
    REWARD_DELIVERED,
    REWARD_PENDING,
    RankedEntry,
    RewardRow,
    ScoreRow,
    ScoreWrite,
    ShardRow,
    UINT64_MAX,
    apply_rank_or_beyond,
    order_key,
    page_window_for,
    position_around,
    shard_to_decrement,
    validate_reward_mail_window_ms,
)
from .store import (
    IdGenerator,
    ItemExistsFn,
    fill_board_key,
    known_currencies_from_protocol,
    merge_score_batch,
    normalize_around_request,
    normalize_page_request,
    validate_freeze_request,
    validate_open_request,
    validate_remove_request,
    validate_submit_request,
)

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "rank_schema.sql"
LOCK_ERRORS = (1205, 1213, 9007)  # 锁等待超时、死锁、TiDB 写冲突
DUP_ENTRY = 1062
TX_MAX_ATTEMPTS = 3

_BOARD_COLUMNS = (
    "`board_id`,`board_type`,`scope`,`scope_id`,`period`,`generation`,"
    "`producer`,`source_epoch`,`ascending`,`state`,`settlement_id`,"
    "`open_request_id`,`open_request_hash`,`created_ms`,`sealed_ms`,`deleted_ms`"
)
_SCORE_COLUMNS = "`board_id`,`generation`,`entity_id`,`score`,`order_key`,`achieved_ms`,`source_cursor`,`score_version`"
_RANK_INDEX_ORDER = "`order_key`,`achieved_ms`,`entity_id`"
_RANK_INDEX_ORDER_REVERSE = "`order_key` DESC,`achieved_ms` DESC,`entity_id` DESC"
_AHEAD_OF_TUPLE = (
    "(`order_key`<%s OR (`order_key`=%s AND `achieved_ms`<%s) "
    "OR (`order_key`=%s AND `achieved_ms`=%s AND `entity_id`<%s))"
)
_BEHIND_OF_TUPLE = (
    "(`order_key`>%s OR (`order_key`=%s AND `achieved_ms`>%s) "
    "OR (`order_key`=%s AND `achieved_ms`=%s AND `entity_id`>%s))"
)

_REQUIRED_TABLES = (
    "rank_board",
    "rank_board_shard",
    "rank_score",
    "rank_input_receipt",
    "rank_settlement",
    "rank_snapshot",
    "rank_mail_reward",
    "rank_mutation",
    "rank_removed_entry",
)

T = TypeVar("T")


def _mysql_errno(exc: BaseException) -> int | None:
    if isinstance(exc, pymysql.MySQLError) and exc.args:
        code = exc.args[0]
        if isinstance(code, int):
            return code
    return None


def _is_retryable_tx_error(exc: BaseException) -> bool:
    if isinstance(exc, ScoreInsertRaceError):
        return True
    return _mysql_errno(exc) in LOCK_ERRORS


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


def _scan_board(row) -> BoardRow:
    if row is None:
        raise RankNotFoundError("rank board not found")
    return BoardRow(
        board_id=int(row[0]),
        board_type=int(row[1]),
        scope=int(row[2]),
        scope_id=int(row[3]),
        period=_to_str(row[4]),
        generation=int(row[5]),
        producer=_to_str(row[6]),
        source_epoch=int(row[7]),
        ascending=int(row[8]),
        state=int(row[9]),
        settlement_id=int(row[10]),
        open_request_id=int(row[11]),
        open_request_hash=_to_str(row[12]),
        created_ms=int(row[13]),
        sealed_ms=int(row[14]),
        deleted_ms=int(row[15]),
    )


def _order_tuple_args(okey: int, achieved_ms: int, entity_id: int) -> tuple[int, int, int, int, int, int]:
    return (okey, okey, achieved_ms, okey, achieved_ms, entity_id)


def _placeholders(count: int) -> str:
    if count <= 0:
        return ""
    return ",".join(["%s"] * count)


class SqlRankStore:
    """基于 MySQL/TiDB（``aiomysql``）的排行榜权威存储。"""

    def __init__(
        self,
        pool,
        protocol: Protocol,
        *,
        next_board_id: IdGenerator,
        next_settlement_id: IdGenerator,
        next_reward_id: IdGenerator,
        item_exists: ItemExistsFn | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        rank_limit: int = RANK_EXACT_LIMIT,
    ) -> None:
        self._pool = pool
        self.protocol = protocol
        self._next_board_id = next_board_id
        self._next_settlement_id = next_settlement_id
        self._next_reward_id = next_reward_id
        self._item_exists = item_exists
        self._clock_ms = clock_ms
        self._known_currencies = known_currencies_from_protocol(protocol)
        self._reward_mail_window_ms = MAX_REWARD_MAIL_WINDOW_MS
        self.rank_limit = rank_limit

    def configure_reward_mail_window(self, window_ms: int) -> None:
        self._reward_mail_window_ms = validate_reward_mail_window_ms(window_ms)

    # -- 建表与启动验表（fail-closed） ----------------------------------------------------------------

    async def bootstrap(self, schema: Path = SCHEMA_FILE) -> None:
        """幂等执行 ``config/rank_schema.sql`` 中的全部 9 张建表语句。"""
        text = "\n".join(line for line in schema.read_text("utf-8").splitlines() if not line.startswith("--"))
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for statement in re.split(r";\s*\n", text):
                sql = statement.strip().rstrip(";")
                if sql:
                    await cursor.execute(sql)

    async def verify_schema(self) -> None:
        """启动前核对全部 9 张表、列、索引与唯一键均就位（fail-closed）。"""
        probes = (
            f"SELECT {_BOARD_COLUMNS} FROM `rank_board` LIMIT 0",
            "SELECT `board_id`,`generation`,`source_shard`,`committed_cursor`,`entries` FROM `rank_board_shard` LIMIT 0",
            f"SELECT {_SCORE_COLUMNS} FROM `rank_score` LIMIT 0",
            "SELECT `board_id`,`generation`,`source_cursor`,`request_id`,`request_hash`,`committed_ms`,`source_shard` "
            "FROM `rank_input_receipt` LIMIT 0",
            "SELECT `settlement_id`,`board_id`,`generation`,`source_epoch`,`final_cursor`,`request_id`,"
            "`request_hash`,`winner_count`,`sealed_ms` FROM `rank_settlement` LIMIT 0",
            "SELECT `settlement_id`,`rank`,`entity_id`,`score`,`achieved_ms` FROM `rank_snapshot` LIMIT 0",
            "SELECT `reward_id`,`settlement_id`,`player_id`,`rank`,`attachments`,`status`,`mail_id`,"
            "`next_attempt_ms`,`lease_until_ms`,`lease_token`,`attempts`,`created_ms`,`updated_ms`,`expire_ms` "
            "FROM `rank_mail_reward` LIMIT 0",
            "SELECT `request_hash`,`board_id`,`generation`,`request_id`,`created_ms` FROM `rank_mutation` LIMIT 0",
            "SELECT `board_id`,`generation`,`entity_id`,`removed_ms` FROM `rank_removed_entry` LIMIT 0",
        )
        required_indexes: tuple[tuple[str, list[str]], ...] = (
            ("rank_score", ["board_id", "generation", "order_key", "achieved_ms", "entity_id"]),
            ("rank_mail_reward", ["status", "next_attempt_ms"]),
            ("rank_board", ["board_type", "scope", "scope_id", "period", "generation"]),
            ("rank_settlement", ["board_id", "generation"]),
            ("rank_mail_reward", ["settlement_id", "player_id"]),
        )
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for probe in probes:
                await cursor.execute(probe)
            for table, expected_cols in required_indexes:
                await cursor.execute(
                    "SELECT index_name, column_name FROM information_schema.statistics "
                    "WHERE table_schema=DATABASE() AND table_name=%s "
                    "ORDER BY index_name, seq_in_index",
                    (table,),
                )
                indexes: dict[str, list[str]] = {}
                for name, column in await cursor.fetchall():
                    indexes.setdefault(str(name), []).append(str(column))
                if not any(cols == expected_cols for cols in indexes.values()):
                    raise RuntimeError(f"table {table} missing required index {expected_cols}")

    # -- 事务骨架 -------------------------------------------------------------------------------------

    async def _run_write_tx(self, fn: Callable[[object], Awaitable[T]]) -> T:
        """在 ``READ COMMITTED`` 隔离级别执行写事务，对可安全重试冲突做有界抖动退避重试。"""
        delay = 0.02
        for attempt in range(TX_MAX_ATTEMPTS):
            try:
                async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
                    await cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    await conn.begin()
                    try:
                        result = await fn(cursor)
                    except BaseException:
                        await conn.rollback()
                        raise
                    await conn.commit()
                    return result
            except Exception as exc:
                if attempt + 1 >= TX_MAX_ATTEMPTS or not _is_retryable_tx_error(exc):
                    raise
                jitter = random.uniform(0.5, 1.5)
                logger.warning(
                    "rank write tx retry attempt=%d err=%s",
                    attempt + 1,
                    type(exc).__name__,
                )
                await asyncio.sleep(delay * jitter)
                delay *= 2
        raise AssertionError("unreachable")

    async def _run_read_tx(self, fn: Callable[[object], Awaitable[T]]) -> T:
        """在 ``REPEATABLE READ`` 隔离级别执行只读快照事务。"""
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            await conn.begin()
            try:
                result = await fn(cursor)
            except BaseException:
                await conn.rollback()
                raise
            await conn.commit()
            return result

    # -- RankAdmin 写路径 -----------------------------------------------------------------------------

    async def open_generation(self, caller: str, request: Message) -> Message:
        digest = validate_open_request(caller, request)
        board_id = await self._next_board_id()
        now = self._clock_ms()
        key = request.board
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    f"INSERT INTO `rank_board` ({_BOARD_COLUMNS}) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1,0,%s,%s,%s,0,0)",
                    (
                        board_id,
                        int(key.board_type),
                        int(key.scope),
                        int(key.scope_id),
                        key.period,
                        int(key.generation),
                        caller,
                        int(request.source_epoch),
                        1 if request.ascending else 0,
                        int(request.request_id),
                        digest,
                        now,
                    ),
                )
                return self.protocol.message("rankpb.OpenRankGenerationResponse", board_id=board_id)
            except pymysql.MySQLError as exc:
                if _mysql_errno(exc) != DUP_ENTRY:
                    raise
            await cursor.execute(
                f"SELECT {_BOARD_COLUMNS} FROM `rank_board` "
                "WHERE `board_type`=%s AND `scope`=%s AND `scope_id`=%s AND `period`=%s AND `generation`=%s",
                (int(key.board_type), int(key.scope), int(key.scope_id), key.period, int(key.generation)),
            )
            existing = _scan_board(await cursor.fetchone())
            if existing.open_request_hash != digest or existing.deleted_ms != 0:
                raise RankConflictError("open rank generation conflict")
            return self.protocol.message(
                "rankpb.OpenRankGenerationResponse",
                board_id=existing.board_id,
                duplicated=True,
            )

    async def submit_scores(self, caller: str, request: Message) -> Message:
        entries, digest = validate_submit_request(request)
        board_id = int(request.board_id)
        generation = int(request.generation)
        source_shard = int(request.source_shard)
        source_cursor = int(request.source_cursor)

        async def _tx(cursor) -> Message:
            shard = await self._lock_shard(cursor, board_id, generation, source_shard)
            board_sql = f"SELECT {_BOARD_COLUMNS} FROM `rank_board` WHERE `board_id`=%s"
            if shard is None:
                board_sql += " FOR UPDATE"
            await cursor.execute(board_sql, (board_id,))
            board = _scan_board(await cursor.fetchone())
            if shard is None:
                shard = await self._lock_shard(cursor, board_id, generation, source_shard)

            if board.producer != caller:
                raise RankDeniedError("caller is not board producer")
            if (
                board.generation != generation
                or board.source_epoch != int(request.source_epoch)
                or board.deleted_ms != 0
            ):
                raise RankConflictError("board generation/epoch/deleted conflict")

            committed = shard.committed_cursor if shard is not None else 0
            if source_cursor <= committed:
                await cursor.execute(
                    "SELECT `request_hash` FROM `rank_input_receipt` "
                    "WHERE `board_id`=%s AND `generation`=%s AND `source_shard`=%s AND `source_cursor`=%s",
                    (board_id, generation, source_shard, source_cursor),
                )
                row = await cursor.fetchone()
                if row is None or _to_str(row[0]) != digest:
                    raise RankConflictError("submit replay hash mismatch")
                return self.protocol.message(
                    "rankpb.SubmitRankScoresResponse",
                    committed_cursor=committed,
                    duplicated=True,
                )

            if board.state != RANK_GEN_OPEN:
                raise RankClosedError("rank generation is closed")
            if committed == UINT64_MAX or source_cursor != committed + 1:
                raise RankCursorError("source cursor gap")

            removed = await self._removed_entries(cursor, board_id, generation)
            existing = await self._load_scores(cursor, board_id, generation, entries, removed)
            writes, inserted = merge_score_batch(
                mode=int(request.mode),
                ascending=board.ascending,
                entries=entries,
                existing=existing,
                removed=removed,
            )
            await self._write_scores(cursor, board, source_cursor, writes)
            await cursor.execute(
                "INSERT INTO `rank_input_receipt` "
                "(`board_id`,`generation`,`source_shard`,`source_cursor`,`request_id`,`request_hash`,`committed_ms`) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (board_id, generation, source_shard, source_cursor, int(request.request_id), digest, self._clock_ms()),
            )
            if shard is None:
                await cursor.execute(
                    "INSERT INTO `rank_board_shard` "
                    "(`board_id`,`generation`,`source_shard`,`committed_cursor`,`entries`) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (board_id, generation, source_shard, source_cursor, inserted),
                )
            else:
                await cursor.execute(
                    "UPDATE `rank_board_shard` SET `committed_cursor`=%s,`entries`=`entries`+%s "
                    "WHERE `board_id`=%s AND `generation`=%s AND `source_shard`=%s",
                    (source_cursor, inserted, board_id, generation, source_shard),
                )
            return self.protocol.message(
                "rankpb.SubmitRankScoresResponse",
                committed_cursor=source_cursor,
                duplicated=False,
            )

        return await self._run_write_tx(_tx)

    async def freeze_generation(self, caller: str, request: Message) -> Message:
        top_n, digest = validate_freeze_request(
            request,
            self._item_exists,
            known_currencies=self._known_currencies,
        )
        settlement_id = await self._next_settlement_id()
        reward_ids = [await self._next_reward_id() for _ in range(top_n)] if request.rewards else []
        board_id = int(request.board_id)
        generation = int(request.generation)

        async def _tx(cursor) -> Message:
            await cursor.execute(
                f"SELECT {_BOARD_COLUMNS} FROM `rank_board` WHERE `board_id`=%s FOR UPDATE",
                (board_id,),
            )
            board = _scan_board(await cursor.fetchone())
            if board.producer != caller:
                raise RankDeniedError("caller is not board producer")
            if (
                board.generation != generation
                or board.source_epoch != int(request.source_epoch)
                or board.deleted_ms != 0
            ):
                raise RankConflictError("board generation/epoch/deleted conflict")

            if board.settlement_id != 0:
                await cursor.execute(
                    "SELECT `request_hash`,`winner_count` FROM `rank_settlement` WHERE `settlement_id`=%s",
                    (board.settlement_id,),
                )
                row = await cursor.fetchone()
                if row is None or _to_str(row[0]) != digest:
                    raise RankConflictError("freeze replay hash mismatch")
                return self.protocol.message(
                    "rankpb.FreezeRankGenerationResponse",
                    settlement_id=board.settlement_id,
                    winner_count=int(row[1]),
                    duplicated=True,
                )

            if board.state != RANK_GEN_OPEN:
                raise RankClosedError("rank generation is closed")

            shards = await self._lock_all_shards(cursor, board.board_id, board.generation)
            final_cursor = sum(s.committed_cursor for s in shards)
            if final_cursor != int(request.expected_source_cursor):
                raise RankCursorError("committed cursor total does not match expected_source_cursor")

            await cursor.execute(
                f"SELECT `entity_id`,`score`,`achieved_ms` FROM `rank_score` "
                f"WHERE `board_id`=%s AND `generation`=%s ORDER BY {_RANK_INDEX_ORDER} LIMIT %s",
                (board.board_id, board.generation, top_n),
            )
            winners = [(int(r[0]), int(r[1]), int(r[2])) for r in await cursor.fetchall()]
            now = self._clock_ms()
            await cursor.execute(
                "INSERT INTO `rank_settlement` "
                "(`settlement_id`,`board_id`,`generation`,`source_epoch`,`final_cursor`,"
                "`request_id`,`request_hash`,`winner_count`,`sealed_ms`) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    settlement_id,
                    board.board_id,
                    board.generation,
                    board.source_epoch,
                    final_cursor,
                    int(request.request_id),
                    digest,
                    len(winners),
                    now,
                ),
            )
            for idx, (entity_id, score, achieved_ms) in enumerate(winners):
                rank_num = idx + 1
                await cursor.execute(
                    "INSERT INTO `rank_snapshot` (`settlement_id`,`rank`,`entity_id`,`score`,`achieved_ms`) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (settlement_id, rank_num, entity_id, score, achieved_ms),
                )
                for tier in request.rewards:
                    if int(tier.first_rank) <= rank_num <= int(tier.last_rank):
                        payload = tier.attachments.SerializeToString(deterministic=True)
                        expire_ms = now + self._reward_mail_window_ms
                        await cursor.execute(
                            "INSERT INTO `rank_mail_reward` "
                            "(`reward_id`,`settlement_id`,`player_id`,`rank`,`attachments`,`status`,`mail_id`,"
                            "`next_attempt_ms`,`lease_until_ms`,`lease_token`,`attempts`,"
                            "`created_ms`,`updated_ms`,`expire_ms`) "
                            "VALUES (%s,%s,%s,%s,UNHEX(%s),%s,0,%s,0,0,0,%s,%s,%s)",
                            (
                                reward_ids[idx],
                                settlement_id,
                                entity_id,
                                rank_num,
                                payload.hex(),
                                REWARD_PENDING,
                                now,
                                now,
                                now,
                                expire_ms,
                            ),
                        )
            await cursor.execute(
                "UPDATE `rank_board` SET `state`=%s,`settlement_id`=%s,`sealed_ms`=%s WHERE `board_id`=%s",
                (RANK_GEN_SEALED, settlement_id, now, board.board_id),
            )
            return self.protocol.message(
                "rankpb.FreezeRankGenerationResponse",
                settlement_id=settlement_id,
                winner_count=len(winners),
                duplicated=False,
            )

        return await self._run_write_tx(_tx)

    async def remove_entry(self, caller: str, request: Message, *, hide: bool = False) -> None:
        digest = validate_remove_request(request, hide=hide)
        board_id = int(request.board_id)
        generation = int(request.generation)
        request_id = int(request.request_id)

        async def _tx(cursor) -> None:
            await cursor.execute(
                f"SELECT {_BOARD_COLUMNS} FROM `rank_board` WHERE `board_id`=%s FOR UPDATE",
                (board_id,),
            )
            board = _scan_board(await cursor.fetchone())
            if board.producer != caller:
                raise RankDeniedError("caller is not board producer")
            if board.generation != generation:
                raise RankConflictError("board generation mismatch")

            await cursor.execute(
                "SELECT `request_hash` FROM `rank_mutation` "
                "WHERE `board_id`=%s AND `generation`=%s AND `request_id`=%s",
                (board_id, generation, request_id),
            )
            existing = await cursor.fetchone()
            if existing is not None:
                if _to_str(existing[0]) != digest:
                    raise RankConflictError("mutation request_id hash conflict")
                return

            if not hide and (board.state != RANK_GEN_OPEN or board.deleted_ms != 0):
                raise RankClosedError("rank generation is closed")

            shards = await self._lock_all_shards(cursor, board.board_id, board.generation)
            now = self._clock_ms()
            if hide:
                await cursor.execute(
                    "UPDATE `rank_board` SET `deleted_ms`=%s WHERE `board_id`=%s",
                    (now, board.board_id),
                )
            else:
                entity_id = int(request.entity_id)
                await cursor.execute(
                    "INSERT INTO `rank_removed_entry` (`board_id`,`generation`,`entity_id`,`removed_ms`) "
                    "VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE `removed_ms`=`removed_ms`",
                    (board.board_id, board.generation, entity_id, now),
                )
                await cursor.execute(
                    "DELETE FROM `rank_score` WHERE `board_id`=%s AND `generation`=%s AND `entity_id`=%s",
                    (board.board_id, board.generation, entity_id),
                )
                if cursor.rowcount > 0:
                    dec_shard = shard_to_decrement(shards)
                    if dec_shard is None:
                        raise EntriesDriftError("rank entry count drift on remove")
                    await cursor.execute(
                        "UPDATE `rank_board_shard` SET `entries`=`entries`-1 "
                        "WHERE `board_id`=%s AND `generation`=%s AND `source_shard`=%s AND `entries`>0",
                        (board.board_id, board.generation, dec_shard),
                    )
                    if cursor.rowcount != 1:
                        raise EntriesDriftError("rank shard entry decrement mismatch")

            await cursor.execute(
                "INSERT INTO `rank_mutation` (`board_id`,`generation`,`request_id`,`request_hash`,`created_ms`) "
                "VALUES (%s,%s,%s,%s,%s)",
                (board.board_id, board.generation, request_id, digest, now),
            )

        await self._run_write_tx(_tx)

    # -- ClientPlayerRank 读路径（REPEATABLE READ） ---------------------------------------------------

    async def get_page(self, player_id: int, request: Message) -> Message:
        page, size = normalize_page_request(player_id, request)

        async def _tx(cursor) -> Message:
            board = await self._lookup_board(cursor, request.board)
            await cursor.execute(
                "SELECT COALESCE(SUM(`entries`),0) FROM `rank_board_shard` "
                "WHERE `board_id`=%s AND `generation`=%s",
                (board.board_id, board.generation),
            )
            total_row = await cursor.fetchone()
            total = int(total_row[0]) if total_row and total_row[0] is not None else 0
            window = page_window_for(total, page, size, self.rank_limit)

            resp = self.protocol.message(
                "rankpb.GetRankPageResponse",
                state=board.state,
                total_count=total,
                page=window.page,
                page_size=size,
                page_count=window.pages,
                rank_limit=self.rank_limit,
            )
            fill_board_key(resp.board, board)

            await cursor.execute(
                f"SELECT `entity_id`,`score`,`achieved_ms` FROM `rank_score` "
                f"WHERE `board_id`=%s AND `generation`=%s ORDER BY {_RANK_INDEX_ORDER} LIMIT %s OFFSET %s",
                (board.board_id, board.generation, window.rows, window.offset),
            )
            self_found = False
            for idx, row in enumerate(await cursor.fetchall()):
                entry = resp.entries.add()
                entry.rank = window.offset + idx + 1
                entry.entity_id = int(row[0])
                entry.score = int(row[1])
                entry.achieved_ms = int(row[2])
                entry.beyond_rank_limit = False
                if entry.entity_id == player_id:
                    resp.self.CopyFrom(entry)
                    self_found = True

            if not self_found:
                self_ranked = await self._self_rank(cursor, board, player_id, self.rank_limit)
                if self_ranked is not None:
                    resp.self.entity_id = self_ranked.entity_id
                    resp.self.score = self_ranked.score
                    resp.self.achieved_ms = self_ranked.achieved_ms
                    apply_rank_or_beyond(
                        resp.self,
                        position_around(self_ranked.ahead, self_ranked.capped, 0),
                        self.rank_limit,
                    )
            return resp

        return await self._run_read_tx(_tx)

    async def get_around(self, player_id: int, request: Message) -> Message:
        radius = normalize_around_request(player_id, request)

        async def _tx(cursor) -> Message:
            board = await self._lookup_board(cursor, request.board)
            resp = self.protocol.message("rankpb.GetRankAroundResponse", rank_limit=self.rank_limit)
            fill_board_key(resp.board, board)

            self_ranked = await self._self_rank(cursor, board, player_id, self.rank_limit + radius)
            if self_ranked is None:
                return resp

            tuple_args = _order_tuple_args(self_ranked.order_key, self_ranked.achieved_ms, player_id)
            await cursor.execute(
                f"SELECT `entity_id`,`score`,`achieved_ms` FROM `rank_score` "
                f"WHERE `board_id`=%s AND `generation`=%s AND {_AHEAD_OF_TUPLE} "
                f"ORDER BY {_RANK_INDEX_ORDER_REVERSE} LIMIT %s",
                (board.board_id, board.generation, *tuple_args, radius),
            )
            before_rows = list(await cursor.fetchall())

            await cursor.execute(
                f"SELECT `entity_id`,`score`,`achieved_ms` FROM `rank_score` "
                f"WHERE `board_id`=%s AND `generation`=%s AND {_BEHIND_OF_TUPLE} "
                f"ORDER BY {_RANK_INDEX_ORDER} LIMIT %s",
                (board.board_id, board.generation, *tuple_args, radius),
            )
            after_rows = list(await cursor.fetchall())

            for idx in range(len(before_rows) - 1, -1, -1):
                row = before_rows[idx]
                entry = resp.entries.add()
                entry.entity_id = int(row[0])
                entry.score = int(row[1])
                entry.achieved_ms = int(row[2])
                apply_rank_or_beyond(
                    entry,
                    position_around(self_ranked.ahead, self_ranked.capped, -idx - 1),
                    self.rank_limit,
                )

            self_entry = resp.entries.add()
            self_entry.entity_id = self_ranked.entity_id
            self_entry.score = self_ranked.score
            self_entry.achieved_ms = self_ranked.achieved_ms
            apply_rank_or_beyond(
                self_entry,
                position_around(self_ranked.ahead, self_ranked.capped, 0),
                self.rank_limit,
            )
            resp.self.CopyFrom(self_entry)

            for idx, row in enumerate(after_rows):
                entry = resp.entries.add()
                entry.entity_id = int(row[0])
                entry.score = int(row[1])
                entry.achieved_ms = int(row[2])
                apply_rank_or_beyond(
                    entry,
                    position_around(self_ranked.ahead, self_ranked.capped, idx + 1),
                    self.rank_limit,
                )

            return resp

        return await self._run_read_tx(_tx)

    # -- RewardStore 实现 -----------------------------------------------------------------------------

    async def due_rewards(self, now_ms: int, limit: int) -> list[int]:
        if limit <= 0 or limit > 1000:
            raise RankInvalidError("invalid due_rewards limit")
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "SELECT `reward_id` FROM `rank_mail_reward` "
                "WHERE `status`=%s AND `next_attempt_ms`<=%s AND `lease_until_ms`<%s "
                "ORDER BY `next_attempt_ms`,`reward_id` LIMIT %s",
                (REWARD_PENDING, now_ms, now_ms, limit),
            )
            return [int(row[0]) for row in await cursor.fetchall()]

    async def claim_reward(self, reward_id: int, now_ms: int, until_ms: int, token: int) -> RewardRow | None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `rank_mail_reward` SET `lease_until_ms`=%s,`lease_token`=%s "
                "WHERE `reward_id`=%s AND `status`=%s AND `lease_until_ms`<%s",
                (until_ms, token, reward_id, REWARD_PENDING, now_ms),
            )
            if cursor.rowcount == 0:
                return None
            await cursor.execute(
                "SELECT `settlement_id`,`player_id`,`rank`,`attachments`,`attempts`,`created_ms`,`expire_ms` "
                "FROM `rank_mail_reward` WHERE `reward_id`=%s",
                (reward_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return RewardRow(
                reward_id=reward_id,
                settlement_id=int(row[0]),
                player_id=int(row[1]),
                rank=int(row[2]),
                attachments=_to_bytes(row[3]),
                status=REWARD_PENDING,
                lease_until_ms=until_ms,
                lease_token=token,
                attempts=int(row[4]),
                created_ms=int(row[5]),
                expire_ms=int(row[6]),
            )

    async def settle_reward(self, reward: RewardRow, mail_id: int, next_ms: int, now_ms: int) -> None:
        state = REWARD_DELIVERED if mail_id != 0 else REWARD_PENDING
        await self._settle_reward(reward, state, mail_id, next_ms, now_ms, counted_attempts=1)

    async def abandon_reward(self, reward: RewardRow, now_ms: int) -> None:
        await self._settle_reward(reward, REWARD_ABANDONED, 0, now_ms, now_ms, counted_attempts=1)

    async def defer_reward(self, reward: RewardRow, next_ms: int, now_ms: int) -> None:
        await self._settle_reward(reward, REWARD_PENDING, 0, next_ms, now_ms, counted_attempts=0)

    async def _settle_reward(
        self,
        reward: RewardRow,
        state: int,
        mail_id: int,
        next_ms: int,
        now_ms: int,
        *,
        counted_attempts: int,
    ) -> None:
        async def _tx(cursor) -> None:
            await cursor.execute(
                "SELECT `status`,`lease_token` FROM `rank_mail_reward` WHERE `reward_id`=%s FOR UPDATE",
                (reward.reward_id,),
            )
            row = await cursor.fetchone()
            if row is None or int(row[0]) != REWARD_PENDING or int(row[1]) != reward.lease_token:
                raise RankConflictError("reward lease lost or already settled")
            await cursor.execute(
                "UPDATE `rank_mail_reward` "
                "SET `status`=%s,`mail_id`=%s,`next_attempt_ms`=%s,`lease_until_ms`=0,"
                "`attempts`=`attempts`+%s,`updated_ms`=%s WHERE `reward_id`=%s",
                (state, mail_id, next_ms, counted_attempts, now_ms, reward.reward_id),
            )

        await self._run_write_tx(_tx)

    # -- SQL 内部查询与写入辅助 -----------------------------------------------------------------------

    async def _lock_shard(self, cursor, board_id: int, generation: int, source_shard: int) -> ShardRow | None:
        await cursor.execute(
            "SELECT `committed_cursor`,`entries` FROM `rank_board_shard` "
            "WHERE `board_id`=%s AND `generation`=%s AND `source_shard`=%s FOR UPDATE",
            (board_id, generation, source_shard),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return ShardRow(source_shard=source_shard, committed_cursor=int(row[0]), entries=int(row[1]))

    async def _lock_all_shards(self, cursor, board_id: int, generation: int) -> list[ShardRow]:
        await cursor.execute(
            "SELECT `source_shard`,`committed_cursor`,`entries` FROM `rank_board_shard` "
            "WHERE `board_id`=%s AND `generation`=%s ORDER BY `source_shard` FOR UPDATE",
            (board_id, generation),
        )
        return [
            ShardRow(source_shard=int(r[0]), committed_cursor=int(r[1]), entries=int(r[2]))
            for r in await cursor.fetchall()
        ]

    async def _removed_entries(self, cursor, board_id: int, generation: int) -> set[int]:
        await cursor.execute(
            "SELECT `entity_id` FROM `rank_removed_entry` WHERE `board_id`=%s AND `generation`=%s",
            (board_id, generation),
        )
        return {int(r[0]) for r in await cursor.fetchall()}

    async def _load_scores(
        self,
        cursor,
        board_id: int,
        generation: int,
        entries: list[Message],
        removed: set[int],
    ) -> dict[int, ScoreRow]:
        ids = [int(entry.entity_id) for entry in entries if int(entry.entity_id) not in removed]
        if not ids:
            return {}
        await cursor.execute(
            f"SELECT `entity_id`,`score`,`achieved_ms`,`score_version` FROM `rank_score` "
            f"WHERE `board_id`=%s AND `generation`=%s AND `entity_id` IN ({_placeholders(len(ids))}) FOR UPDATE",
            (board_id, generation, *ids),
        )
        return {
            int(r[0]): ScoreRow(score=int(r[1]), achieved_ms=int(r[2]), score_version=int(r[3]))
            for r in await cursor.fetchall()
        }

    async def _write_scores(self, cursor, board: BoardRow, source_cursor: int, writes: list[ScoreWrite]) -> None:
        existed = [w for w in writes if w.existed]
        fresh = [w for w in writes if not w.existed]
        if existed:
            values_sql, args = self._score_rows_sql(board, source_cursor, existed)
            await cursor.execute(
                f"INSERT INTO `rank_score` ({_SCORE_COLUMNS}) VALUES {values_sql} "
                "ON DUPLICATE KEY UPDATE "
                "`score`=VALUES(`score`),`order_key`=VALUES(`order_key`),"
                "`achieved_ms`=VALUES(`achieved_ms`),`source_cursor`=VALUES(`source_cursor`),"
                "`score_version`=VALUES(`score_version`)",
                args,
            )
        if fresh:
            values_sql, args = self._score_rows_sql(board, source_cursor, fresh)
            try:
                await cursor.execute(f"INSERT INTO `rank_score` ({_SCORE_COLUMNS}) VALUES {values_sql}", args)
            except pymysql.MySQLError as exc:
                if _mysql_errno(exc) == DUP_ENTRY:
                    raise ScoreInsertRaceError("concurrent fresh score insert race") from exc
                raise

    @staticmethod
    def _score_rows_sql(board: BoardRow, source_cursor: int, writes: list[ScoreWrite]) -> tuple[str, tuple]:
        placeholders = ",".join(["(%s,%s,%s,%s,%s,%s,%s,%s)"] * len(writes))
        args: list[int] = []
        for w in writes:
            args.extend((
                board.board_id,
                board.generation,
                w.entity_id,
                w.score,
                order_key(board.ascending, w.score),
                w.achieved_ms,
                source_cursor,
                w.score_version,
            ))
        return placeholders, tuple(args)

    async def _lookup_board(self, cursor, key: Message) -> BoardRow:
        query = (
            f"SELECT {_BOARD_COLUMNS} FROM `rank_board` "
            "WHERE `board_type`=%s AND `scope`=%s AND `scope_id`=%s AND `deleted_ms`=0"
        )
        args: list = [int(key.board_type), int(key.scope), int(key.scope_id)]
        if key.period:
            query += " AND `period`=%s"
            args.append(key.period)
        if key.generation > 0:
            query += " AND `generation`=%s"
            args.append(int(key.generation))
        if not key.period:
            query += " ORDER BY `created_ms` DESC,`board_id` DESC LIMIT 1"
        else:
            query += " ORDER BY `generation` DESC LIMIT 1"
        await cursor.execute(query, tuple(args))
        return _scan_board(await cursor.fetchone())

    async def _self_rank(self, cursor, board: BoardRow, player_id: int, count_cap: int) -> RankedEntry | None:
        await cursor.execute(
            "SELECT `score`,`order_key`,`achieved_ms` FROM `rank_score` "
            "WHERE `board_id`=%s AND `generation`=%s AND `entity_id`=%s",
            (board.board_id, board.generation, player_id),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        score, okey, achieved_ms = int(row[0]), int(row[1]), int(row[2])
        tuple_args = _order_tuple_args(okey, achieved_ms, player_id)
        await cursor.execute(
            f"SELECT COUNT(*) FROM ("
            f"SELECT 1 FROM `rank_score` WHERE `board_id`=%s AND `generation`=%s AND {_AHEAD_OF_TUPLE} LIMIT %s"
            f") AS `ahead`",
            (board.board_id, board.generation, *tuple_args, count_cap),
        )
        cnt_row = await cursor.fetchone()
        ahead = int(cnt_row[0]) if cnt_row else 0
        return RankedEntry(
            entity_id=player_id,
            score=score,
            order_key=okey,
            achieved_ms=achieved_ms,
            ahead=ahead,
            capped=ahead >= count_cap,
        )


async def open_pool(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    maxsize: int = 16,
    minsize: int = 1,
):
    """创建 ``READ-COMMITTED`` / ``autocommit`` / ``utf8mb4`` 连接池。"""
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
