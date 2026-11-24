"""PlayerStore 的 MySQL/TiDB 存储实现（基于 ``aiomysql``）。

严格对齐 Go 版 ``go/db/internal/playerstore/{store.go,audit_outbox.go}``：
- 每个地域存档库 ``player_store_{storage_id}_db`` 独占连接池，绝不跨池 ``USE``；
- 二进制列（``holder_instance``、``payload``）全部经 ``UNHEX(%s)`` + hex 字符串绑定，
  读取时统一 ``bytes(val or b"")``，规避 ``pymysql`` ``_binary`` 转义在 TiDB 上的兼容风险；
- 任意连接、查询、写入或提交失败一律丢弃连接并抛出 ``StoreBackendError``，由 gRPC 层返回 ``UNAVAILABLE``。
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path

import aiomysql

from .model import (
    AUDIT_OUTBOX_INSERT_BATCH,
    AUDIT_OUTBOX_TABLE,
    DEFAULT_KNOWN_GEN,
    OWNER_KIND,
    REQUIRED_AUDIT_OUTBOX_COLUMNS,
    REQUIRED_PLAYER_STORE_COLUMNS,
    AuditRecord,
    ClaimOutcome,
    ClaimResult,
    OwnerRow,
    SaveLimits,
    SaveOutcome,
    SaveResult,
    StorageUnavailableError,
    StoreBackendError,
    audit_shard_of,
    decide_claim,
    decide_save,
    prepare_snapshot_records,
    prepare_transaction_log_records,
    storage_db_name,
    validate_claim_request,
    validate_save_request,
)

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "db_schema.sql"
ACQUIRE_TIMEOUT_SEC = 3.0
DEFAULT_SQL_TIMEOUT_SEC = 12.0

SQL_SELECT_OWNER = (
    "SELECT owner_epoch, save_seq, schema_gen, holder_instance "
    "FROM player_store WHERE player_id = %s AND kind = 0 FOR UPDATE"
)
SQL_CLAIM_OWNER = (
    "UPDATE player_store "
    "SET owner_epoch = %s, holder_instance = UNHEX(%s), released = 0, claim_seq = claim_seq + 1 "
    "WHERE player_id = %s AND kind = 0"
)
SQL_SELECT_ROWS = "SELECT kind, payload FROM player_store WHERE player_id = %s AND kind > 0"
SQL_SAVE_HEAD = (
    "INSERT INTO player_store "
    "(player_id, kind, owner_epoch, save_seq, schema_gen, holder_instance, released, payload) VALUES "
)
SQL_SAVE_ROW = "(%s, %s, %s, %s, %s, UNHEX(%s), %s, UNHEX(%s))"
# schema_gen 取 GREATEST：保证空行释放带着旧二进制世代时不会拉低写权行记录的最高世代。
SQL_SAVE_TAIL = (
    " ON DUPLICATE KEY UPDATE "
    "owner_epoch = VALUES(owner_epoch), "
    "save_seq = VALUES(save_seq), "
    "schema_gen = GREATEST(schema_gen, VALUES(schema_gen)), "
    "holder_instance = VALUES(holder_instance), "
    "released = VALUES(released), "
    "payload = VALUES(payload)"
)

SQL_AUDIT_OUTBOX_HEAD = (
    "INSERT INTO player_audit_outbox "
    "(shard, kind, created_ms, player_id, record_id, save_seq, payload) VALUES "
)
SQL_AUDIT_OUTBOX_ROW = "(%s, %s, %s, %s, %s, %s, UNHEX(%s))"
# 同笔请求重复记录命中主键时不报错，也不用 INSERT IGNORE 以免吞掉截断错误。
SQL_AUDIT_OUTBOX_TAIL = " ON DUPLICATE KEY UPDATE record_id = record_id"

SQL_SELECT_TABLE_COLUMNS = (
    "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s"
)


def _to_hex(data: bytes | None) -> str:
    """将任意二进制载荷安全转为 hex 字符串；空字节转 ``''``，``UNHEX('')`` 返回非 NULL 空串。"""
    return (data or b"").hex()


def build_save_sql(
    player_id: int,
    owner_epoch: int,
    save_seq: int,
    schema_gen: int,
    instance_id: bytes,
    rows: Sequence[tuple[int, bytes]],
    release: bool,
) -> tuple[str, list]:
    """拼装单条多行 ``INSERT ... ON DUPLICATE KEY UPDATE``（写权行 + 全部脏行，不修改 ``claim_seq``）。"""
    placeholders = [SQL_SAVE_ROW]
    args: list = [
        player_id,
        OWNER_KIND,
        owner_epoch,
        save_seq,
        schema_gen,
        _to_hex(instance_id),
        1 if release else 0,
        "",
    ]
    for kind, payload in rows:
        placeholders.append(SQL_SAVE_ROW)
        args.extend((
            player_id,
            int(kind),
            owner_epoch,
            save_seq,
            schema_gen,
            "",
            0,
            _to_hex(payload),
        ))
    query = f"{SQL_SAVE_HEAD}{','.join(placeholders)}{SQL_SAVE_TAIL}"
    return query, args


def build_audit_outbox_sql(
    player_id: int,
    save_seq: int,
    created_ms: int,
    records: Sequence[AuditRecord],
) -> list[tuple[str, list]]:
    """按批次（最多 1000 行/条）拼装 ``player_audit_outbox`` 插入语句与参数。"""
    if not records:
        return []
    shard = audit_shard_of(player_id)
    statements: list[tuple[str, list]] = []
    for start in range(0, len(records), AUDIT_OUTBOX_INSERT_BATCH):
        batch = records[start : start + AUDIT_OUTBOX_INSERT_BATCH]
        placeholders: list[str] = []
        args: list = []
        for record in batch:
            placeholders.append(SQL_AUDIT_OUTBOX_ROW)
            args.extend((
                shard,
                int(record.kind),
                created_ms,
                player_id,
                int(record.record_id),
                save_seq,
                _to_hex(record.payload),
            ))
        query = f"{SQL_AUDIT_OUTBOX_HEAD}{','.join(placeholders)}{SQL_AUDIT_OUTBOX_TAIL}"
        statements.append((query, args))
    return statements


@asynccontextmanager
async def _acquire_connection(
    pool,
    *,
    acquire_timeout_sec: float = ACQUIRE_TIMEOUT_SEC,
    sql_timeout_sec: float = DEFAULT_SQL_TIMEOUT_SEC,
):
    """带独立超时边界获取连接；任一异常或取消发生时关闭并丢弃连接，防止脏事务回池。"""
    acquire = pool.acquire()
    async with asyncio.timeout(acquire_timeout_sec):
        conn = await acquire.__aenter__()
    try:
        try:
            async with asyncio.timeout(sql_timeout_sec):
                yield conn
        except BaseException:
            conn.close()
            raise
    finally:
        await acquire.__aexit__(None, None, None)


class SqlPlayerStore:
    """基于 ``aiomysql`` 连接池的玩家存档库后端实现。"""

    def __init__(
        self,
        pools: Mapping[int, object],
        protocol,
        *,
        known_gen: int = DEFAULT_KNOWN_GEN,
        limits: SaveLimits | None = None,
        sql_timeout_sec: float = DEFAULT_SQL_TIMEOUT_SEC,
        acquire_timeout_sec: float = ACQUIRE_TIMEOUT_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._pools = dict(pools)
        self._protocol = protocol
        self._known_gen = known_gen
        self._limits = limits or SaveLimits()
        self._limits.validate()
        self._sql_timeout_sec = sql_timeout_sec
        self._acquire_timeout_sec = acquire_timeout_sec
        self._clock_ms = clock_ms

    def _pool(self, storage_id: int):
        pool = self._pools.get(storage_id)
        if pool is None:
            raise StorageUnavailableError(f"storage_id={storage_id} is not configured")
        return pool

    async def bootstrap_schema(self, storage_id: int, schema_path: Path = SCHEMA_FILE) -> None:
        """对指定地域存档库执行幂等建表脚本（用于开发/测试环境初始化）。"""
        pool = self._pool(storage_id)
        text = "\n".join(
            line for line in schema_path.read_text("utf-8").splitlines() if not line.lstrip().startswith("--")
        )
        async with _acquire_connection(
            pool, acquire_timeout_sec=self._acquire_timeout_sec, sql_timeout_sec=self._sql_timeout_sec
        ) as conn, conn.cursor() as cursor:
            for statement in re.split(r";\s*\n", text):
                sql = statement.strip().rstrip(";").strip()
                if sql:
                    await cursor.execute(sql)

    async def bootstrap_all(self, schema_path: Path = SCHEMA_FILE) -> None:
        for storage_id in sorted(self._pools):
            await self.bootstrap_schema(storage_id, schema_path)

    async def verify_schema(self, storage_id: int) -> None:
        """核对 ``player_store`` 与 ``player_audit_outbox`` 所需全部列已存在（fail-closed）。"""
        pool = self._pool(storage_id)
        async with _acquire_connection(
            pool, acquire_timeout_sec=self._acquire_timeout_sec, sql_timeout_sec=self._sql_timeout_sec
        ) as conn, conn.cursor() as cursor:
            for table, required in (
                ("player_store", REQUIRED_PLAYER_STORE_COLUMNS),
                (AUDIT_OUTBOX_TABLE, REQUIRED_AUDIT_OUTBOX_COLUMNS),
            ):
                await cursor.execute(SQL_SELECT_TABLE_COLUMNS, (table,))
                fetched = await cursor.fetchall()
                have = {str(row[0]).lower() for row in fetched}
                missing = [col for col in required if col not in have]
                if missing:
                    raise RuntimeError(
                        f"storage_id={storage_id} table {table} missing required columns: {missing}"
                    )

    async def verify_all_schemas(self) -> None:
        for storage_id in sorted(self._pools):
            await self.verify_schema(storage_id)

    async def _read_owner(self, cursor, player_id: int) -> OwnerRow | None:
        await cursor.execute(SQL_SELECT_OWNER, (player_id,))
        row = await cursor.fetchone()
        if row is None:
            return None
        owner_epoch, save_seq, schema_gen, holder = row
        return OwnerRow(
            owner_epoch=int(owner_epoch),
            save_seq=int(save_seq),
            schema_gen=int(schema_gen),
            holder=bytes(holder or b""),
        )

    async def _read_rows(self, cursor, player_id: int) -> list[tuple[int, bytes]]:
        await cursor.execute(SQL_SELECT_ROWS, (player_id,))
        fetched = await cursor.fetchall()
        rows = [(int(kind), bytes(payload or b"")) for kind, payload in fetched]
        rows.sort(key=lambda item: item[0])
        return rows

    async def load_and_claim(
        self,
        storage_id: int,
        player_id: int,
        owner_epoch: int,
        instance_id: bytes,
        binary_schema_gen: int,
    ) -> ClaimOutcome:
        pool = self._pool(storage_id)
        invalid = validate_claim_request(player_id, owner_epoch, instance_id)
        if invalid != ClaimResult.UNSPECIFIED:
            return ClaimOutcome(result=invalid)
        try:
            async with _acquire_connection(
                pool, acquire_timeout_sec=self._acquire_timeout_sec, sql_timeout_sec=self._sql_timeout_sec
            ) as conn:
                await conn.begin()
                async with conn.cursor() as cursor:
                    owner = await self._read_owner(cursor, player_id)
                    decision = decide_claim(owner, owner_epoch, instance_id, binary_schema_gen, self._known_gen)
                    if decision != ClaimResult.CLAIMED:
                        await conn.rollback()
                        if owner is None:
                            return ClaimOutcome(result=decision)
                        return ClaimOutcome(
                            result=decision,
                            owner_epoch=owner.owner_epoch,
                            save_seq=owner.save_seq,
                            schema_gen=owner.schema_gen,
                        )
                    assert owner is not None
                    await cursor.execute(SQL_CLAIM_OWNER, (owner_epoch, _to_hex(instance_id), player_id))
                    rows = await self._read_rows(cursor, player_id)
                await conn.commit()
                return ClaimOutcome(
                    result=ClaimResult.CLAIMED,
                    owner_epoch=owner_epoch,
                    save_seq=owner.save_seq,
                    schema_gen=owner.schema_gen,
                    rows=tuple(rows),
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "player_store sql load_and_claim failed storage_id=%d player_id=%d",
                storage_id,
                player_id,
                exc_info=exc,
            )
            raise StoreBackendError("load_and_claim SQL transaction failed") from exc

    async def save_player(
        self,
        storage_id: int,
        player_id: int,
        owner_epoch: int,
        save_seq: int,
        schema_gen: int,
        instance_id: bytes,
        rows: Sequence[tuple[int, bytes]],
        release: bool,
        *,
        transaction_logs: Sequence = (),
        audit_snapshots: Sequence = (),
    ) -> SaveOutcome:
        pool = self._pool(storage_id)
        invalid = validate_save_request(player_id, owner_epoch, save_seq, instance_id, rows, self._limits)
        if invalid != SaveResult.UNSPECIFIED:
            return SaveOutcome(result=invalid)
        try:
            async with _acquire_connection(
                pool, acquire_timeout_sec=self._acquire_timeout_sec, sql_timeout_sec=self._sql_timeout_sec
            ) as conn:
                await conn.begin()
                async with conn.cursor() as cursor:
                    owner = await self._read_owner(cursor, player_id)
                    decision = decide_save(
                        owner, owner_epoch, save_seq, schema_gen, len(rows) > 0, instance_id, self._known_gen
                    )
                    if decision != SaveResult.COMMITTED:
                        await conn.rollback()
                        return SaveOutcome(
                            result=decision,
                            row_owner_epoch=0 if owner is None else owner.owner_epoch,
                            row_save_seq=0 if owner is None else owner.save_seq,
                            audit_outbox_written=(decision == SaveResult.EQUAL),
                        )
                    query, args = build_save_sql(
                        player_id, owner_epoch, save_seq, schema_gen, instance_id, rows, release
                    )
                    await cursor.execute(query, args)

                    if transaction_logs or audit_snapshots:
                        now_ms = int(self._clock_ms())
                        records = prepare_transaction_log_records(player_id, save_seq, transaction_logs)
                        if audit_snapshots:
                            # 同事务 READ COMMITTED 可读取刚写入的脏行，拼出提交后的完整角色快照。
                            all_rows = await self._read_rows(cursor, player_id)
                            records.extend(
                                prepare_snapshot_records(
                                    self._protocol,
                                    player_id,
                                    save_seq,
                                    schema_gen,
                                    audit_snapshots,
                                    all_rows,
                                    now_ms // 1000,
                                )
                            )
                        for audit_query, audit_args in build_audit_outbox_sql(
                            player_id, save_seq, now_ms, records
                        ):
                            await cursor.execute(audit_query, audit_args)
                await conn.commit()
                return SaveOutcome(
                    result=SaveResult.COMMITTED,
                    row_owner_epoch=owner_epoch,
                    row_save_seq=save_seq,
                    audit_outbox_written=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "player_store sql save_player failed storage_id=%d player_id=%d save_seq=%d",
                storage_id,
                player_id,
                save_seq,
                exc_info=exc,
            )
            raise StoreBackendError("save_player SQL transaction failed") from exc


async def open_storage_pool(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    storage_id: int | None = None,
    database: str | None = None,
    maxsize: int = 8,
    minsize: int = 1,
    lock_wait_timeout_sec: int = 10,
    connect_timeout: int = 3,
):
    """创建绑定单个地域存档库的 ``aiomysql`` 连接池，设置 READ-COMMITTED、STRICT_TRANS_TABLES 与锁等待超时。"""
    db_name = database if database is not None else storage_db_name(storage_id)  # type: ignore[arg-type]
    init_cmd = (
        "SET SESSION transaction_isolation='READ-COMMITTED', "
        "sql_mode='STRICT_TRANS_TABLES', "
        f"innodb_lock_wait_timeout={int(lock_wait_timeout_sec)}"
    )
    return await aiomysql.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db_name,
        minsize=minsize,
        maxsize=maxsize,
        connect_timeout=connect_timeout,
        autocommit=True,
        charset="utf8mb4",
        init_command=init_cmd,
    )
