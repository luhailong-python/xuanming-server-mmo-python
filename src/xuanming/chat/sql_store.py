"""chat MySQL/TiDB 权威存储实现（SqlChatStore）。

全部二进制列统一通过 ``UNHEX(%s)`` 绑定，避免 utf8mb4 连接下的字节转码损坏；
事务统一运行在 ``READ-COMMITTED`` 隔离级别下，并对 1205/1213/9007 瞬态锁冲突做有界重试。
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

from .. import portal_sql
from ..protocol import Protocol
from .errors import ChatError, PolicyMismatchError, StoreUnavailableError
from .model import (
    ACCEPTED_STATE,
    ChatRules,
    HistoryRow,
    ORIGINAL_RETENTION_DAYS,
    ReceiptRow,
    policy_hash,
    validate_receipt,
)
from .store import (
    ActorVerifier,
    ChatStoreCore,
    ContentReviewer,
    ProfileReader,
    _TxRollbackReturn,
)

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "chat_schema.sql"
DB_NOW_MS = "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS UNSIGNED)"
LOCK_ERRORS = (1205, 1213, 9007)  # 锁等待超时、死锁、TiDB 写冲突
DUP_ENTRY = 1062
TX_MAX_ATTEMPTS = 3

_RECEIPT_COLUMNS = (
    "owner_account,owner_revision,request_digest,request_payload,"
    "message_payload,stream_key,state,accepted_ms,result_tip"
)
_RECEIPT_SELECT_SQL = (
    f"SELECT {_RECEIPT_COLUMNS} FROM chat_send_authority WHERE player_id=%s AND request_id=UNHEX(%s)"
)
_CHAT_TABLES_IN = (
    "('chat_send_authority','chat_policy','chat_world_retraction',"
    "'chat_report','chat_repeat_event','chat_private_daily_target')"
)

_EXPECTED_INDEXES = {
    "chat_send_authority/PRIMARY": "player_id,request_id",
    "chat_send_authority/idx_chat_stream": "stream_key,state,accepted_ms,player_id,request_id",
    "chat_send_authority/idx_chat_rate": "player_id,state,accepted_ms",
    "chat_policy/PRIMARY": "id",
    "chat_world_retraction/PRIMARY": "sender_player_id,accepted_ms",
    "chat_world_retraction/idx_chat_retract_ms": "accepted_ms,sender_player_id",
    "chat_report/PRIMARY": "reporter_player_id,channel,target_sender_player_id,send_time_ms",
    "chat_report/idx_chat_report_target": "target_sender_player_id,send_time_ms",
    "chat_repeat_event/PRIMARY": "content_hash,accepted_ms,sender_player_id",
    "chat_private_daily_target/PRIMARY": "sender_player_id,day_ymd,target_player_id",
}

_EXPECTED_COLUMNS = {
    "chat_send_authority/player_id": "bigint unsigned",
    "chat_send_authority/request_id": "varbinary/64",
    "chat_send_authority/owner_account": "varbinary/191",
    "chat_send_authority/owner_revision": "bigint unsigned",
    "chat_send_authority/request_digest": "binary/32",
    "chat_send_authority/request_payload": "mediumblob",
    "chat_send_authority/message_payload": "mediumblob",
    "chat_send_authority/stream_key": "varbinary/191",
    "chat_send_authority/state": "tinyint unsigned",
    "chat_send_authority/accepted_ms": "bigint unsigned",
    "chat_send_authority/result_tip": "int unsigned",
    "chat_send_authority/accepted_at": "datetime(3)",
    "chat_policy/id": "tinyint unsigned",
    "chat_policy/policy_hash": "binary/32",
    "chat_world_retraction/sender_player_id": "bigint unsigned",
    "chat_world_retraction/accepted_ms": "bigint unsigned",
    "chat_world_retraction/retracted_ms": "bigint unsigned",
    "chat_world_retraction/reason": "varbinary/128",
    "chat_world_retraction/accepted_at": "datetime(3)",
    "chat_report/reporter_player_id": "bigint unsigned",
    "chat_report/channel": "tinyint unsigned",
    "chat_report/target_sender_player_id": "bigint unsigned",
    "chat_report/send_time_ms": "bigint unsigned",
    "chat_report/reason": "int unsigned",
    "chat_report/detail": "varbinary/255",
    "chat_report/reported_ms": "bigint unsigned",
    "chat_report/accepted_at": "datetime(3)",
    "chat_repeat_event/content_hash": "binary/32",
    "chat_repeat_event/accepted_ms": "bigint unsigned",
    "chat_repeat_event/sender_player_id": "bigint unsigned",
    "chat_repeat_event/accepted_at": "datetime(3)",
    "chat_private_daily_target/sender_player_id": "bigint unsigned",
    "chat_private_daily_target/day_ymd": "int unsigned",
    "chat_private_daily_target/target_player_id": "bigint unsigned",
    "chat_private_daily_target/accepted_at": "datetime(3)",
}

T = TypeVar("T")


def _to_bytes(value) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


def _mysql_errno(exc: BaseException) -> int | None:
    if isinstance(exc, pymysql.MySQLError) and exc.args:
        code = exc.args[0]
        return code if isinstance(code, int) else None
    return None


def _scan_receipt(row) -> ReceiptRow:
    return validate_receipt(
        ReceiptRow(
            account=_to_bytes(row[0]),
            revision=int(row[1]),
            digest=_to_bytes(row[2]),
            request=_to_bytes(row[3]),
            message=_to_bytes(row[4]),
            stream=_to_bytes(row[5]),
            state=int(row[6]),
            accepted_ms=int(row[7]),
            tip=int(row[8]),
        )
    )


def has_retention_attributes(ddl: str) -> bool:
    """校验 SHOW CREATE TABLE 输出中是否启用了 30 天 accepted_at TTL 属性（TiDB）。"""
    normalized = re.sub(r"[\s`]+", "", ddl).upper()
    want = f"TTL=ACCEPTED_AT+INTERVAL{ORIGINAL_RETENTION_DAYS}DAY"
    return want in normalized and "TTL_ENABLE='ON'" in normalized


class _SqlTx:
    """单 MySQL/TiDB 事务内游标句柄：全部二进制参数统一经 UNHEX(%s) 传入。"""

    def __init__(self, cursor) -> None:
        self._cur = cursor

    async def now_ms(self) -> int:
        await self._cur.execute(f"SELECT {DB_NOW_MS}")
        row = await self._cur.fetchone()
        if row is None:
            raise StoreUnavailableError("failed to read database timestamp")
        return int(row[0])

    async def read_receipt(self, player_id: int, request_id: bytes) -> ReceiptRow | None:
        await self._cur.execute(_RECEIPT_SELECT_SQL, (player_id, request_id.hex()))
        row = await self._cur.fetchone()
        return _scan_receipt(row) if row is not None else None

    async def count_rate_window(self, player_id: int, start_ms: int, end_ms: int) -> int:
        await self._cur.execute(
            "SELECT COUNT(*) FROM chat_send_authority "
            "WHERE player_id=%s AND state=%s AND accepted_ms>=%s AND accepted_ms<%s",
            (player_id, ACCEPTED_STATE, start_ms, end_ms),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row is not None else 0

    async def has_world_cooldown(self, player_id: int, since_ms: int, stream_key_bytes: bytes) -> bool:
        await self._cur.execute(
            "SELECT 1 FROM chat_send_authority "
            "WHERE player_id=%s AND state=%s AND accepted_ms>%s AND stream_key=UNHEX(%s) LIMIT 1",
            (player_id, ACCEPTED_STATE, since_ms, stream_key_bytes.hex()),
        )
        return await self._cur.fetchone() is not None

    async def list_repeat_senders(self, content_hash: bytes, since_ms: int, limit: int) -> list[int]:
        await self._cur.execute(
            "SELECT sender_player_id FROM chat_repeat_event "
            "WHERE content_hash=UNHEX(%s) AND accepted_ms>=%s LIMIT %s",
            (content_hash.hex(), since_ms, limit),
        )
        return [int(row[0]) for row in await self._cur.fetchall()]

    async def insert_repeat_event(
        self, content_hash: bytes, accepted_ms: int, sender_player_id: int, accepted_at: str
    ) -> None:
        await self._cur.execute(
            "INSERT IGNORE INTO chat_repeat_event(content_hash,accepted_ms,sender_player_id,accepted_at) "
            "VALUES (UNHEX(%s),%s,%s,%s)",
            (content_hash.hex(), accepted_ms, sender_player_id, accepted_at),
        )

    async def has_private_daily_target(self, sender_player_id: int, day_ymd: int, target_player_id: int) -> bool:
        await self._cur.execute(
            "SELECT 1 FROM chat_private_daily_target "
            "WHERE sender_player_id=%s AND day_ymd=%s AND target_player_id=%s LIMIT 1",
            (sender_player_id, day_ymd, target_player_id),
        )
        return await self._cur.fetchone() is not None

    async def count_private_daily_targets(self, sender_player_id: int, day_ymd: int) -> int:
        await self._cur.execute(
            "SELECT COUNT(*) FROM chat_private_daily_target WHERE sender_player_id=%s AND day_ymd=%s",
            (sender_player_id, day_ymd),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row is not None else 0

    async def insert_private_daily_target(
        self, sender_player_id: int, day_ymd: int, target_player_id: int, accepted_at: str
    ) -> None:
        await self._cur.execute(
            "INSERT IGNORE INTO chat_private_daily_target(sender_player_id,day_ymd,target_player_id,accepted_at) "
            "VALUES (%s,%s,%s,%s)",
            (sender_player_id, day_ymd, target_player_id, accepted_at),
        )

    async def insert_send_authority(
        self,
        player_id: int,
        request_id: bytes,
        owner_account: bytes,
        owner_revision: int,
        request_digest: bytes,
        request_payload: bytes,
        message_payload: bytes,
        stream_key_bytes: bytes,
        accepted_ms: int,
        accepted_at: str,
    ) -> bool:
        try:
            await self._cur.execute(
                f"INSERT INTO chat_send_authority(player_id,request_id,{_RECEIPT_COLUMNS},accepted_at) "
                "VALUES (%s,UNHEX(%s),UNHEX(%s),%s,UNHEX(%s),UNHEX(%s),UNHEX(%s),UNHEX(%s),%s,%s,%s,%s)",
                (
                    player_id,
                    request_id.hex(),
                    owner_account.hex(),
                    owner_revision,
                    request_digest.hex(),
                    request_payload.hex(),
                    message_payload.hex(),
                    stream_key_bytes.hex(),
                    ACCEPTED_STATE,
                    accepted_ms,
                    0,
                    accepted_at,
                ),
            )
            return True
        except pymysql.err.IntegrityError as exc:
            if _mysql_errno(exc) == DUP_ENTRY:
                return False
            raise

    async def read_history_rows(self, stream_key_bytes: bytes, limit: int) -> list[HistoryRow]:
        await self._cur.execute(
            "SELECT player_id,request_id,message_payload,accepted_ms FROM chat_send_authority "
            "WHERE stream_key=UNHEX(%s) AND state=%s "
            "ORDER BY accepted_ms DESC,player_id DESC,request_id DESC LIMIT %s",
            (stream_key_bytes.hex(), ACCEPTED_STATE, limit),
        )
        return [
            HistoryRow(
                sender=int(row[0]),
                request_id=_to_bytes(row[1]),
                payload=_to_bytes(row[2]),
                accepted_ms=int(row[3]),
            )
            for row in await self._cur.fetchall()
        ]

    async def load_retracted_world_keys(self, min_ms: int) -> set[tuple[int, int]]:
        await self._cur.execute(
            "SELECT sender_player_id,accepted_ms FROM chat_world_retraction WHERE accepted_ms>=%s",
            (min_ms,),
        )
        return {(int(row[0]), int(row[1])) for row in await self._cur.fetchall()}

    async def message_exists(self, player_id: int, accepted_ms: int, stream_key_bytes: bytes) -> bool:
        await self._cur.execute(
            "SELECT 1 FROM chat_send_authority "
            "WHERE player_id=%s AND state=%s AND accepted_ms=%s AND stream_key=UNHEX(%s) LIMIT 1",
            (player_id, ACCEPTED_STATE, accepted_ms, stream_key_bytes.hex()),
        )
        return await self._cur.fetchone() is not None

    async def insert_world_retraction(
        self, sender_player_id: int, accepted_ms: int, retracted_ms: int, reason: bytes, accepted_at: str
    ) -> bool:
        await self._cur.execute(
            "INSERT IGNORE INTO chat_world_retraction(sender_player_id,accepted_ms,retracted_ms,reason,accepted_at) "
            "VALUES (%s,%s,%s,UNHEX(%s),%s)",
            (sender_player_id, accepted_ms, retracted_ms, reason.hex(), accepted_at),
        )
        return int(self._cur.rowcount) > 0

    async def insert_report(
        self,
        reporter_player_id: int,
        channel: int,
        target_sender_player_id: int,
        send_time_ms: int,
        reason: int,
        detail: bytes,
        reported_ms: int,
        accepted_at: str,
    ) -> None:
        await self._cur.execute(
            "INSERT IGNORE INTO chat_report("
            "reporter_player_id,channel,target_sender_player_id,send_time_ms,"
            "reason,detail,reported_ms,accepted_at"
            ") VALUES (%s,%s,%s,%s,%s,UNHEX(%s),%s,%s)",
            (
                reporter_player_id,
                channel,
                target_sender_player_id,
                send_time_ms,
                reason,
                detail.hex(),
                reported_ms,
                accepted_at,
            ),
        )


class _SqlDriver:
    """基于 aiomysql 连接池的聊天存储驱动。"""

    def __init__(self, pool) -> None:
        self._pool = pool

    async def run_tx(self, work: Callable[[_SqlTx], Awaitable[T]]) -> T:
        for attempt in range(1, TX_MAX_ATTEMPTS + 1):
            try:
                return await self._run_tx_once(work)
            except (ChatError, _TxRollbackReturn):
                raise
            except pymysql.MySQLError as exc:
                if _mysql_errno(exc) in LOCK_ERRORS and attempt < TX_MAX_ATTEMPTS:
                    await asyncio.sleep(0.01 * attempt + random.uniform(0.0, 0.01))
                    continue
                raise StoreUnavailableError(f"chat mysql transaction failed: {exc}") from exc
        raise StoreUnavailableError("chat mysql transaction exhausted retries")

    async def _run_tx_once(self, work: Callable[[_SqlTx], Awaitable[T]]) -> T:
        domain_error: BaseException | None = None
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
            except ChatError as exc:
                await conn.rollback()
                domain_error = exc
        if rollback_return is not None:
            return rollback_return.value
        assert domain_error is not None
        raise domain_error

    async def read_receipt_direct(self, player_id: int, request_id: bytes) -> ReceiptRow | None:
        try:
            async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
                await cursor.execute(_RECEIPT_SELECT_SQL, (player_id, request_id.hex()))
                row = await cursor.fetchone()
                return _scan_receipt(row) if row is not None else None
        except ChatError:
            raise
        except pymysql.MySQLError as exc:
            raise StoreUnavailableError(f"chat receipt read failed: {exc}") from exc


class SqlChatStore(ChatStoreCore):
    """基于 MySQL/TiDB 的权威聊天存储，带建表引导、结构校验与策略摘要锁钉。"""

    def __init__(
        self,
        pool,
        protocol: Protocol,
        actor_verifier: ActorVerifier,
        rules: ChatRules,
        *,
        reviewer: ContentReviewer | None = None,
        profile_reader: ProfileReader | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._pool = pool
        super().__init__(
            protocol,
            _SqlDriver(pool),
            actor_verifier,
            rules,
            reviewer=reviewer,
            profile_reader=profile_reader,
            monotonic_clock=monotonic_clock,
        )

    async def bootstrap(self, schema: Path = SCHEMA_FILE) -> None:
        """幂等执行 config/chat_schema.sql 中的 6 张权威表 DDL。"""
        text = "\n".join(line for line in schema.read_text("utf-8").splitlines() if not line.startswith("--"))
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for statement in re.split(r";\s*\n", text):
                sql = statement.strip().rstrip(";")
                if sql:
                    await cursor.execute(sql)

    async def verify_schema(self) -> None:
        """启动前核对 6 张表的列集合、索引、非空约束与列类型（与 Go CheckSchema 对齐）。"""
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for probe in (
                "SELECT player_id,request_id,owner_account,owner_revision,request_digest,request_payload,"
                "message_payload,stream_key,state,accepted_ms,result_tip,accepted_at FROM chat_send_authority LIMIT 0",
                "SELECT id,policy_hash FROM chat_policy LIMIT 0",
                "SELECT sender_player_id,accepted_ms,retracted_ms,reason,accepted_at "
                "FROM chat_world_retraction LIMIT 0",
                "SELECT reporter_player_id,channel,target_sender_player_id,send_time_ms,reason,detail,"
                "reported_ms,accepted_at FROM chat_report LIMIT 0",
                "SELECT content_hash,accepted_ms,sender_player_id,accepted_at FROM chat_repeat_event LIMIT 0",
                "SELECT sender_player_id,day_ymd,target_player_id,accepted_at FROM chat_private_daily_target LIMIT 0",
            ):
                await cursor.execute(probe)

            await cursor.execute(
                "SELECT table_name,index_name,column_name,non_unique,sub_part "
                f"FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name IN {_CHAT_TABLES_IN} "
                "ORDER BY table_name,index_name,seq_in_index"
            )
            indexes: dict[str, list[str]] = {}
            for table, name, column, non_unique, sub_part in await cursor.fetchall():
                if (
                    sub_part is not None
                    or (name == "PRIMARY" and int(non_unique) != 0)
                    or (str(name).startswith("idx_chat_") and int(non_unique) != 1)
                ):
                    raise RuntimeError(f"chat schema index prefix or uniqueness mismatch on {table}/{name}")
                indexes.setdefault(f"{table}/{name}", []).append(str(column))
            for key, expected in _EXPECTED_INDEXES.items():
                if ",".join(indexes.get(key, ())) != expected:
                    raise RuntimeError(f"chat schema required index mismatch: {key}")

            await cursor.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                f"WHERE table_schema=DATABASE() AND table_name IN {_CHAT_TABLES_IN} AND is_nullable<>'NO'"
            )
            nullable_row = await cursor.fetchone()
            if nullable_row is None or int(nullable_row[0]) != 0:
                raise RuntimeError("chat schema columns must all be NOT NULL")

            await cursor.execute(
                "SELECT table_name,column_name,data_type,column_type,character_maximum_length "
                f"FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name IN {_CHAT_TABLES_IN}"
            )
            shapes: dict[str, str] = {}
            for table, column, data_type, column_type, max_len in await cursor.fetchall():
                kind = str(data_type).lower()
                full = str(column_type).lower()
                shape = kind
                if "unsigned" in full:
                    shape += " unsigned"
                if max_len is not None and kind in ("binary", "varbinary"):
                    shape += f"/{int(max_len)}"
                if kind == "datetime":
                    shape = full
                shapes[f"{table}/{column}"] = shape
            for key, expected in _EXPECTED_COLUMNS.items():
                if shapes.get(key) != expected:
                    raise RuntimeError(f"chat schema column shape mismatch on {key}: got {shapes.get(key)!r}")

    async def ensure_policy(self) -> None:
        """核对或首次登记 chat_policy(id=1) 的 32 字节策略摘要（与 Go ensurePolicy 对齐）。"""
        digest = policy_hash(self._rules)
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute("SELECT policy_hash FROM chat_policy WHERE id=1")
            row = await cursor.fetchone()
            if row is not None:
                if _to_bytes(row[0]) != digest:
                    raise PolicyMismatchError("chat_policy hash does not match configured rules")
                return

        async with portal_sql.connection(self._pool) as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO chat_policy(id,policy_hash) VALUES (1,UNHEX(%s)) "
                        "ON DUPLICATE KEY UPDATE id=id",
                        (digest.hex(),),
                    )
                    await cursor.execute("SELECT policy_hash FROM chat_policy WHERE id=1 FOR UPDATE")
                    row = await cursor.fetchone()
                    if row is None or _to_bytes(row[0]) != digest:
                        raise PolicyMismatchError("chat_policy hash mismatch after lock")
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def check_retention(self) -> bool:
        """检查 chat_send_authority 是否启用了 TiDB TTL 属性（仅观测告警，不拒启）。"""
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute("SHOW CREATE TABLE chat_send_authority")
            row = await cursor.fetchone()
            if row is None or len(row) < 2:
                return False
            return has_retention_attributes(str(row[1]))


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
