"""mall_game MySQL/TiDB 权威存储实现（SqlMallStore）与启动期 SchemaVerifier。

核心契约（对齐 Java DatabaseConfiguration / SchemaVerifier 与全部 8 个 Store）：
- 每个物理连接在建连初始化时固定 ``READ-COMMITTED`` 隔离级别、严格 SQL 模式与 5 秒锁等待超时；
  ``require_tidb=True`` 时额外强设 TiDB 悲观事务与 leader 强一致读并验证 ``tidb_version()``；
- 启动期由 ``verify_schema()`` 校验 27 张表、关键列类型、唯一约束、扫描索引及 ``STORAGE_CONTRACT`` 合同戳；
- 全部 ``VARBINARY`` 列均使用 ``UNHEX(%s)`` 绑定原始字节，防止 utf8mb4 连接字符集破坏二进制键值；
- 所有事务遵循与 Java 一致的全局锁序（如 ``buyer_guard -> oms_order -> pms_sku_stock / commerce_consignment -> payment_receipt -> delivery/refund/reversal``），
  对瞬态锁超时/死锁（1205/1213/9007）做有界重试。
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, TypeVar

import aiomysql
import pymysql

from .config import MallGameConfig
from .models import (
    ACTOR_RE,
    ALLOWED_CURRENCY_TYPES,
    BOUND_OFFICIAL_REVERSAL_STATUSES,
    CHANNEL_RE,
    CLOSE_REASON_RE,
    CURRENCY_RE,
    EXPIRY_GRACE_MS,
    FACT_PAID,
    FACT_REVOKED,
    INT32_MAX,
    INT64_MAX,
    MANUAL_CONSIGNMENT_ACTIONS,
    MANUAL_ORDER_ACTIONS,
    MANUAL_REFUND_ACTIONS,
    MAX_AMOUNT_MINOR,
    MAX_CURRENCY_GRANT,
    MAX_ITEM_GRANT_COUNT,
    ORDER_STATUS_AWAITING_PAYMENT,
    ORDER_STATUS_CANCELLED,
    ORDER_STATUS_DELIVERED,
    ORDER_STATUS_DELIVERY_PENDING,
    ORDER_STATUS_MANUAL,
    ORDER_STATUS_REFUNDED,
    ORDER_STATUS_REFUND_PENDING,
    ORDER_STATUS_REVERSED,
    ORDER_STATUS_REVERSING,
    PAID_ORDER_STATUSES,
    REFUND_AMOUNT_MISMATCH,
    REFUND_CANCELLED_LATE,
    REFUND_CURRENCY_MISMATCH,
    REFUND_DELIVERY_REJECTED,
    REFUND_DUPLICATE_PAYMENT,
    RMB_ASSET_KINDS,
    STORAGE_CONTRACT,
    TXN_ID_RE,
    UUID_RE,
    CommerceError,
    PendingScanCursor,
    SkuTerms,
    calc_consignment_fee,
    order_state_name,
    refunds_orders_own_payment,
    validate_account,
    validate_channel,
    validate_currency,
    validate_order_id,
    validate_proof,
    validate_request_id,
    validate_revision,
    validate_sku_terms,
    validate_txn_id,
    validate_uint64_str,
)
from .store import _canonical_json, _command_digest, _consignment_digest, _parse_refund_cursor, _require_reason

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "mall_game_schema.sql"
DUP_ENTRY = 1062
LOCK_ERRORS = (1205, 1213, 9007)
TX_MAX_ATTEMPTS = 3

T = TypeVar("T")

# 全部 27 张核心表及必选列（与 Java SchemaVerifier.COLUMNS 逐字一致）
REQUIRED_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "commerce_schema_version": ("version", "storage_contract"),
    "commerce_buyer_guard": ("buyer_id",),
    "pms_sku_stock": ("id", "product_id", "sku_code", "price_minor", "currency", "stock", "lock_stock", "published", "catalog_revision"),
    "commerce_admin_command": ("actor", "request_id", "command_type", "request_digest", "result_json", "created_ms"),
    "oms_order": ("id", "order_sn", "buyer_id", "request_id", "sku_id", "quantity", "pay_amount", "currency", "status", "create_time_ms", "payment_deadline_ms", "payment_time_ms"),
    "oms_order_item": ("order_sn", "product_id", "product_sku_id", "product_sku_code", "product_quantity", "product_price_minor"),
    "commerce_official_owner": ("order_sn", "buyer_account", "buyer_owner_revision"),
    "commerce_payment_receipt": ("channel", "transaction_id", "order_sn", "amount_minor", "currency", "revision", "fact"),
    "commerce_sku_grant": ("sku_id", "item_config_id", "count_per_unit", "currency_type", "currency_amount", "title"),
    "commerce_order_grant": ("order_sn", "item_config_id", "count_per_unit", "currency_type", "currency_amount", "title"),
    "commerce_reversal_intent": ("order_sn", "revision", "status", "updated_ms"),
    "commerce_reversal_receipt": ("order_sn", "revision", "trade_order_id", "state", "proof"),
    "commerce_delivery_intent": ("order_sn", "buyer_id", "sku_id", "quantity", "status", "created_ms"),
    "commerce_delivery_receipt": ("order_sn", "trade_order_id", "state", "proof"),
    "commerce_refund_intent": ("channel", "transaction_id", "order_sn", "source", "status", "close_reason"),
    "commerce_refund_receipt": ("channel", "payment_transaction_id", "transaction_id", "order_sn", "amount_minor", "currency"),
    "commerce_abnormal_payment": ("channel", "transaction_id", "order_sn", "reason", "created_ms"),
    "commerce_mock_transaction": ("kind", "order_sn", "request_id", "transaction_id", "payment_transaction_id", "amount_minor", "currency", "created_ms", "applied"),
    "commerce_consignment": ("listing_id", "seller_player_id", "seller_account", "market_zone", "asset_kind", "asset_currency_type", "escrow_reference", "escrow_proof", "request_digest", "title", "price_fen", "fee_bps", "cooling_ms", "notice_end_ms", "sale_end_ms", "state", "current_order_sn", "created_ms"),
    "commerce_consignment_order": ("order_sn", "listing_id", "buyer_account", "buyer_owner_revision", "request_id", "seller_account", "escrow_reference", "escrow_proof", "fee_bps", "cooling_ms"),
    "commerce_consignment_return": ("listing_id", "return_proof"),
    "commerce_seller_guard": ("account",),
    "commerce_seller_income": ("order_sn", "account", "gross_fen", "fee_fen", "net_fen", "reserved_fen", "paid_fen", "forfeited_fen", "available_ms"),
    "commerce_withdrawal": ("withdrawal_id", "account", "request_id", "amount_fen", "state", "channel", "transaction_id", "created_ms"),
    "commerce_withdrawal_allocation": ("withdrawal_id", "order_sn", "amount_fen"),
    "commerce_withdrawal_receipt": ("withdrawal_id", "channel", "transaction_id", "amount_fen", "successful"),
    "commerce_mock_payout": ("withdrawal_id", "request_id", "transaction_id", "amount_fen", "successful", "created_ms", "applied"),
    "commerce_request_nonce": ("caller_role", "nonce", "expires_ms"),
}

_VARBINARY_COLUMNS = frozenset((
    "actor", "account", "buyer_account", "buyer_owner_revision", "seller_account",
    "request_id", "channel", "transaction_id", "payment_transaction_id", "nonce",
))

_BIGINT_MONEY_COLUMNS = frozenset((
    "price_minor", "pay_amount", "amount_minor", "product_price_minor", "price_fen",
    "gross_fen", "fee_fen", "net_fen", "reserved_fen", "paid_fen", "forfeited_fen", "amount_fen",
))

_REQUIRED_UNIQUE_INDEXES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("commerce_schema_version", ("version",)),
    ("commerce_buyer_guard", ("buyer_id",)),
    ("pms_sku_stock", ("id",)),
    ("oms_order", ("order_sn",)),
    ("oms_order", ("buyer_id", "request_id")),
    ("oms_order_item", ("order_sn",)),
    ("commerce_official_owner", ("order_sn",)),
    ("commerce_payment_receipt", ("channel", "transaction_id")),
    ("commerce_payment_receipt", ("order_sn",)),
    ("commerce_abnormal_payment", ("channel", "transaction_id")),
    ("commerce_refund_intent", ("channel", "transaction_id")),
    ("commerce_refund_receipt", ("channel", "payment_transaction_id")),
    ("commerce_refund_receipt", ("channel", "transaction_id")),
    ("commerce_delivery_intent", ("order_sn",)),
    ("commerce_reversal_intent", ("order_sn",)),
    ("commerce_reversal_receipt", ("order_sn", "revision")),
    ("commerce_request_nonce", ("caller_role", "nonce")),
    ("commerce_admin_command", ("actor", "request_id")),
    ("commerce_mock_transaction", ("kind", "order_sn")),
    ("commerce_mock_transaction", ("kind", "request_id")),
    ("commerce_mock_transaction", ("transaction_id",)),
    ("commerce_consignment", ("listing_id",)),
    ("commerce_consignment", ("escrow_reference",)),
    ("commerce_consignment_order", ("order_sn",)),
    ("commerce_consignment_order", ("buyer_account", "request_id")),
    ("commerce_consignment_return", ("listing_id",)),
    ("commerce_seller_guard", ("account",)),
    ("commerce_seller_income", ("order_sn",)),
    ("commerce_withdrawal", ("withdrawal_id",)),
    ("commerce_withdrawal", ("account", "request_id")),
    ("commerce_withdrawal_allocation", ("withdrawal_id", "order_sn")),
    ("commerce_withdrawal_receipt", ("withdrawal_id",)),
    ("commerce_withdrawal_receipt", ("channel", "transaction_id")),
    ("commerce_mock_payout", ("withdrawal_id",)),
    ("commerce_mock_payout", ("request_id",)),
    ("commerce_mock_payout", ("transaction_id",)),
)

_REQUIRED_SCAN_INDEXES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("commerce_reversal_intent", ("status", "updated_ms", "order_sn")),
    ("commerce_delivery_intent", ("status", "created_ms", "order_sn")),
    ("commerce_mock_payout", ("applied", "withdrawal_id")),
    ("commerce_consignment", ("state", "listing_id")),
    ("commerce_seller_income", ("account", "order_sn")),
    ("commerce_mock_transaction", ("kind", "applied", "order_sn")),
    ("commerce_refund_intent", ("status", "order_sn", "channel", "transaction_id")),
    ("oms_order", ("buyer_id", "status")),
    ("oms_order", ("status", "payment_deadline_ms")),
    ("commerce_request_nonce", ("expires_ms",)),
)


def _mysql_errno(exc: BaseException) -> int | None:
    if isinstance(exc, pymysql.MySQLError) and exc.args:
        code = exc.args[0]
        if isinstance(code, int):
            return code
    return None


def _is_dup_key(exc: BaseException) -> bool:
    return _mysql_errno(exc) == DUP_ENTRY


def _is_lock_retryable(exc: BaseException) -> bool:
    return _mysql_errno(exc) in LOCK_ERRORS


def _hex_utf8(value: str) -> str:
    return value.encode("utf-8").hex()


def _hex_ascii(value: str) -> str:
    return value.encode("ascii").hex()


def _decode_utf8(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8")
    return str(value)


def _decode_ascii(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("ascii")
    return str(value)


def _decode_opt_ascii(value: Any) -> str | None:
    if value is None:
        return None
    return _decode_ascii(value)


def session_init_sql(*, require_tidb: bool) -> str:
    """生成单条物理连接初始化 SQL（RC + 严格模式；TiDB 模式加悲观事务与强一致读）。"""
    if require_tidb:
        return (
            "SET SESSION transaction_isolation='READ-COMMITTED',"
            "tidb_snapshot='',tidb_txn_mode='pessimistic',tidb_replica_read='leader',"
            "tidb_read_consistency='strict',tidb_read_staleness=0,tidb_low_resolution_tso=OFF,"
            "sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION',innodb_lock_wait_timeout=5"
        )
    return (
        "SET SESSION transaction_isolation='READ-COMMITTED',"
        "sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION',innodb_lock_wait_timeout=5"
    )


async def open_pool(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    require_tidb: bool = True,
    minsize: int = 4,
    maxsize: int = 16,
    connect_timeout: float = 3.0,
) -> aiomysql.Pool:
    """打开商城独立库 aiomysql 连接池，自动为每条连接注入会话强一致参数。"""
    return await aiomysql.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        db=database,
        minsize=minsize,
        maxsize=maxsize,
        autocommit=False,
        charset="utf8mb4",
        connect_timeout=connect_timeout,
        init_command=session_init_sql(require_tidb=require_tidb),
    )


class SchemaVerifier:
    """启动期只读 Schema 合同与 TiDB 能力校验器（对齐 Java SchemaVerifier + DatabaseConfiguration）。"""

    @staticmethod
    async def verify(pool: aiomysql.Pool, *, require_tidb: bool) -> None:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if require_tidb:
                    await cur.execute("SELECT tidb_version() AS v")
                    v_row = await cur.fetchone()
                    await cur.execute(
                        "SELECT @@session.tidb_txn_mode AS txn_mode,"
                        "@@session.tidb_replica_read AS replica_read,"
                        "@@session.tidb_read_staleness AS staleness,"
                        "@@session.tidb_low_resolution_tso AS low_tso,"
                        "@@session.tidb_read_consistency AS consistency,"
                        "@@session.tidb_snapshot AS snapshot,"
                        "@@session.sql_mode AS sql_mode"
                    )
                    s_row = await cur.fetchone()
                    sql_mode = str((s_row or {}).get("sql_mode") or "")
                    if (
                        not v_row
                        or not str(v_row.get("v") or "").strip()
                        or not s_row
                        or str(s_row.get("txn_mode") or "").lower() != "pessimistic"
                        or str(s_row.get("replica_read") or "").lower() != "leader"
                        or int(s_row.get("staleness") or 0) != 0
                        or int(s_row.get("low_tso") or 0) != 0
                        or str(s_row.get("consistency") or "").lower() != "strict"
                        or str(s_row.get("snapshot") or "") != ""
                        or ("STRICT_TRANS_TABLES" not in sql_mode and "STRICT_ALL_TABLES" not in sql_mode)
                    ):
                        raise RuntimeError("mall production database must be TiDB with pessimistic leader reads")

                # 校验存储合同版本戳
                await cur.execute("SELECT version, storage_contract FROM commerce_schema_version")
                versions = await cur.fetchall()
                if (
                    len(versions) != 1
                    or int(versions[0]["version"]) != 1
                    or str(versions[0]["storage_contract"]) != STORAGE_CONTRACT
                ):
                    raise RuntimeError("incompatible mall storage contract version")

                # 校验事务隔离级别
                await cur.execute("SELECT @@session.transaction_isolation AS iso")
                iso_row = await cur.fetchone()
                iso = str((iso_row or {}).get("iso") or "").upper()
                if iso not in ("READ-COMMITTED", "READ COMMITTED"):
                    raise RuntimeError(f"mall connection must use READ-COMMITTED, got {iso!r}")

                # 校验全部表和列是否存在
                for table, cols in REQUIRED_TABLE_COLUMNS.items():
                    col_list = ",".join(cols)
                    await cur.execute(f"SELECT {col_list} FROM `{table}` WHERE 1=0")

                # 校验列类型与可空性
                await cur.execute(
                    "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE "
                    "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE()"
                )
                col_meta: dict[tuple[str, str], dict[str, Any]] = {}
                for row in await cur.fetchall():
                    col_meta[(str(row["TABLE_NAME"]).lower(), str(row["COLUMN_NAME"]).lower())] = row

                for table, cols in REQUIRED_TABLE_COLUMNS.items():
                    for col in cols:
                        meta = col_meta.get((table, col))
                        if meta is None:
                            raise RuntimeError(f"missing column {table}.{col}")
                        dtype = str(meta["DATA_TYPE"]).lower()
                        if col in _VARBINARY_COLUMNS and dtype != "varbinary":
                            raise RuntimeError(f"opaque identifier column must be VARBINARY: {table}.{col}")
                        if col == "buyer_owner_revision" and int(meta.get("CHARACTER_MAXIMUM_LENGTH") or 0) != 20:
                            raise RuntimeError("buyer_owner_revision must be VARBINARY(20)")
                        if table == "commerce_payment_receipt" and col == "order_sn" and str(meta["IS_NULLABLE"]).upper() != "YES":
                            raise RuntimeError("commerce_payment_receipt.order_sn must be nullable for abnormal payments")
                        if col in _BIGINT_MONEY_COLUMNS and dtype != "bigint":
                            raise RuntimeError(f"money column must be BIGINT: {table}.{col}")

                # 校验唯一键与扫描索引
                await cur.execute(
                    "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
                    "FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() "
                    "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
                )
                unique_idx: dict[str, dict[str, list[str]]] = {}
                all_idx: dict[str, dict[str, list[str]]] = {}
                for row in await cur.fetchall():
                    t = str(row["TABLE_NAME"]).lower()
                    idx_name = str(row["INDEX_NAME"])
                    col_name = str(row["COLUMN_NAME"]).lower()
                    all_idx.setdefault(t, {}).setdefault(idx_name, []).append(col_name)
                    if int(row["NON_UNIQUE"]) == 0:
                        unique_idx.setdefault(t, {}).setdefault(idx_name, []).append(col_name)

                for table, expected_cols in _REQUIRED_UNIQUE_INDEXES:
                    found = [tuple(cols) for cols in unique_idx.get(table, {}).values()]
                    if expected_cols not in found:
                        raise RuntimeError(f"missing required unique constraint on {table}{expected_cols}")

                for table, prefix in _REQUIRED_SCAN_INDEXES:
                    found_all = [tuple(cols) for cols in all_idx.get(table, {}).values()]
                    if not any(len(cols) >= len(prefix) and cols[: len(prefix)] == prefix for cols in found_all):
                        raise RuntimeError(f"missing required scan index on {table}{prefix}")
            await conn.rollback()


class SqlMallStore:
    """生产级 MySQL/TiDB 商城存储实现（实现完整 MallStore 协议）。"""

    def __init__(
        self,
        pool: aiomysql.Pool,
        config: MallGameConfig,
        *,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._pool = pool
        self.config = config
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._now_sql = (
            "(TIDB_CURRENT_TSO() >> 18)"
            if config.require_tidb
            else "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)"
        )

    def _now_ms(self) -> int:
        return int(self._clock_ms())

    @asynccontextmanager
    async def _tx(self):
        async with self._pool.acquire() as conn:
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    yield conn, cur
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def _run_tx(self, fn: Callable[[Any, Any], Awaitable[T]]) -> T:
        """带瞬态锁冲突重试的 READ COMMITTED 事务执行器。"""
        for attempt in range(TX_MAX_ATTEMPTS):
            try:
                async with self._tx() as (conn, cur):
                    return await fn(conn, cur)
            except pymysql.MySQLError as exc:
                if _is_lock_retryable(exc) and attempt + 1 < TX_MAX_ATTEMPTS:
                    await asyncio.sleep(0.02 * (attempt + 1) + random.uniform(0.0, 0.02))
                    continue
                raise
        raise CommerceError(503, "TX_RETRY_EXHAUSTED", "数据库事务冲突重试耗尽")

    async def bootstrap(self, schema_path: Path | None = None) -> None:
        """按 mall_game_schema.sql 幂等建表并写入 STORAGE_CONTRACT 版本戳。"""
        path = schema_path or SCHEMA_FILE
        sql_text = path.read_text(encoding="utf-8")
        statements = [s.strip() for s in sql_text.split(";") if s.strip() and not all(
            line.strip().startswith("--") or not line.strip() for line in s.splitlines()
        )]
        async with self._tx() as (_conn, cur):
            for stmt in statements:
                await cur.execute(stmt)
            await cur.execute(
                "INSERT IGNORE INTO commerce_schema_version(version, storage_contract) VALUES (1, %s)",
                (STORAGE_CONTRACT,),
            )

    async def verify_schema(self) -> None:
        await SchemaVerifier.verify(self._pool, require_tidb=self.config.require_tidb)

    async def ping(self) -> None:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                row = await cur.fetchone()
            await conn.rollback()
            if not row or row[0] != 1:
                raise RuntimeError("unexpected database ping response")

    # -- Nonce 与后台清理 --------------------------------------------------------------------------------------

    async def claim_nonce(self, role: str, nonce: str, sent_ms: int) -> None:
        nonce_hex = _hex_ascii(nonce)
        skew = self.config.clock_skew_ms
        expires_ms = sent_ms + self.config.clock_skew_ms + self.config.nonce_ttl_ms

        async def _op(_conn, cur) -> None:
            # 点读初始化 TiDB 事务，避免 lazy BEGIN 导致 TIDB_CURRENT_TSO() 为 0
            await cur.execute(
                "SELECT nonce FROM commerce_request_nonce WHERE caller_role=%s AND nonce=UNHEX(%s)",
                (role, nonce_hex),
            )
            await cur.fetchone()
            await cur.execute(f"SELECT {self._now_sql} AS now_ms")
            row = await cur.fetchone()
            now_ms = int(row["now_ms"]) if row and row["now_ms"] is not None else 0
            if now_ms < skew:
                raise CommerceError(503, "CLOCK_UNAVAILABLE", "权威认证时间不可用")
            try:
                changed = await cur.execute(
                    f"INSERT INTO commerce_request_nonce(caller_role, nonce, expires_ms) "
                    f"SELECT %s, UNHEX(%s), %s WHERE %s >= {self._now_sql} - %s AND %s <= {self._now_sql} + %s",
                    (role, nonce_hex, expires_ms, sent_ms, skew, sent_ms, skew),
                )
            except pymysql.MySQLError as exc:
                if _is_dup_key(exc):
                    raise CommerceError(409, "NONCE_REPLAY", "签名请求已使用，请以新 nonce 重试原业务请求号") from None
                raise
            if changed != 1:
                raise CommerceError(401, "EXPIRED_REQUEST", "签名请求已过期")

        try:
            await self._run_tx(_op)
        except pymysql.MySQLError as exc:
            logger.warning("nonce claim storage error: errno=%s", _mysql_errno(exc))
            raise CommerceError(503, "AUTH_STORE_UNAVAILABLE", "认证存储暂不可用") from None

    async def prune_expired_nonces(self) -> int:
        skew = self.config.clock_skew_ms

        async def _op(_conn, cur) -> int:
            await cur.execute("SELECT expires_ms FROM commerce_request_nonce LIMIT 1")
            await cur.fetchone()
            await cur.execute(f"SELECT {self._now_sql} AS now_ms")
            row = await cur.fetchone()
            now_ms = int(row["now_ms"]) if row and row["now_ms"] is not None else 0
            if now_ms < skew:
                return 0
            return int(await cur.execute(
                "DELETE FROM commerce_request_nonce WHERE expires_ms < %s ORDER BY expires_ms LIMIT 500",
                (now_ms,),
            ))

        return await self._run_tx(_op)

    async def expire_overdue_orders(self) -> int:
        cutoff = self._now_ms() - EXPIRY_GRACE_MS
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT order_sn FROM oms_order WHERE status=0 AND payment_deadline_ms<=%s "
                    "ORDER BY payment_deadline_ms LIMIT 50",
                    (cutoff,),
                )
                rows = await cur.fetchall()
            await conn.rollback()

        expired = 0
        for row in rows:
            order_id = str(row["order_sn"])
            try:
                async def _cancel_one(_conn, cur) -> bool:
                    order = await self._load_order(cur, order_id, lock=True)
                    if order is None or int(order["status"]) != 0 or int(order["payment_deadline_ms"]) > cutoff:
                        return False
                    await self._cancel_order_row(cur, order)
                    return True

                if await self._run_tx(_cancel_one):
                    expired += 1
            except CommerceError:
                continue
        return expired

    # -- 内部行加载与视图转换 ----------------------------------------------------------------------------------

    async def _load_order(self, cur, order_id: str, *, lock: bool) -> dict[str, Any] | None:
        if lock:
            await cur.execute("SELECT * FROM oms_order WHERE order_sn=%s FOR UPDATE", (order_id,))
            row = await cur.fetchone()
            if row is not None:
                row["payment_revision"] = -1
            return row
        await cur.execute(
            "SELECT o.*, COALESCE(pr.revision, 0) AS payment_revision FROM oms_order o "
            "LEFT JOIN commerce_payment_receipt pr ON pr.order_sn=o.order_sn WHERE o.order_sn=%s",
            (order_id,),
        )
        return await cur.fetchone()

    async def _load_order_by_req(self, cur, buyer: str, request_id: str) -> dict[str, Any] | None:
        await cur.execute(
            "SELECT o.*, COALESCE(pr.revision, 0) AS payment_revision FROM oms_order o "
            "LEFT JOIN commerce_payment_receipt pr ON pr.order_sn=o.order_sn "
            "WHERE o.buyer_id=%s AND o.request_id=UNHEX(%s)",
            (buyer, _hex_ascii(request_id)),
        )
        return await cur.fetchone()

    async def _order_view_sql(self, cur, row: dict[str, Any]) -> dict[str, Any]:
        rev = int(row.get("payment_revision", -1))
        status = int(row["status"])
        if rev < 0:
            if status in (ORDER_STATUS_AWAITING_PAYMENT, ORDER_STATUS_CANCELLED):
                rev = 0
            else:
                await cur.execute(
                    "SELECT revision FROM commerce_payment_receipt WHERE order_sn=%s",
                    (row["order_sn"],),
                )
                r = await cur.fetchone()
                rev = int(r["revision"]) if r is not None else 0
        return {
            "orderId": str(row["order_sn"]),
            "buyerId": str(row["buyer_id"]),
            "skuId": int(row["sku_id"]),
            "quantity": int(row["quantity"]),
            "amountMinor": int(row["pay_amount"]),
            "currency": str(row["currency"]),
            "state": order_state_name(status),
            "paymentDeadlineMs": int(row["payment_deadline_ms"]),
            "paymentRevision": rev,
        }

    async def _official_owner(self, cur, order_id: str) -> tuple[str, str]:
        await cur.execute(
            "SELECT buyer_account, buyer_owner_revision FROM commerce_official_owner WHERE order_sn=%s",
            (order_id,),
        )
        row = await cur.fetchone()
        if row is None:
            raise CommerceError(503, "OWNER_MISSING", "官方订单原身份缺失")
        acc = _decode_utf8(row["buyer_account"])
        rev = _decode_ascii(row["buyer_owner_revision"])
        validate_account(acc, status=503)
        validate_revision(rev, status=503)
        return acc, rev

    async def _same_official_req(
        self, cur, prior: dict[str, Any], sku_id: int, quantity: int, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]:
        if int(prior["sku_id"]) != sku_id or int(prior["quantity"]) != quantity:
            raise CommerceError(409, "REQUEST_CONFLICT", "请求号已用于不同下单参数")
        owner = await self._official_owner(cur, str(prior["order_sn"]))
        if owner != (buyer_account, buyer_owner_revision):
            raise CommerceError(409, "OWNER_CONFLICT", "请求号的原账号或归属修订不符")
        return await self._order_view_sql(cur, prior)

    async def _official_owned(
        self, cur, buyer: str, order_id: str, buyer_account: str, buyer_owner_revision: str, *, lock: bool
    ) -> dict[str, Any]:
        validate_order_id(order_id)
        validate_account(buyer_account)
        validate_revision(buyer_owner_revision)
        order = await self._load_order(cur, order_id, lock=lock)
        if order is None or str(order["buyer_id"]) != buyer:
            raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
        if int(order["sku_id"]) == 0:
            raise CommerceError(404, "USE_CONSIGNMENT_ENDPOINT", "请通过原账号的寄售订单入口操作")
        owner = await self._official_owner(cur, order_id)
        if owner != (buyer_account, buyer_owner_revision):
            raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
        return order

    # -- 官方订单与支付入账 ------------------------------------------------------------------------------------

    async def create_order(
        self,
        buyer: str,
        request_id: str,
        sku_id: int,
        quantity: int,
        buyer_account: str,
        buyer_owner_revision: str,
    ) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        if (
            not isinstance(sku_id, int) or isinstance(sku_id, bool) or sku_id <= 0
            or not isinstance(quantity, int) or isinstance(quantity, bool) or not 1 <= quantity <= 10_000
        ):
            raise CommerceError(400, "INVALID_ORDER", "下单参数非法")
        validate_request_id(request_id)
        validate_account(buyer_account)
        validate_revision(buyer_owner_revision)

        async def _ensure_guard(_conn, cur) -> None:
            await cur.execute(
                "INSERT INTO commerce_buyer_guard(buyer_id) VALUES (%s) ON DUPLICATE KEY UPDATE buyer_id=buyer_id",
                (buyer,),
            )

        await self._run_tx(_ensure_guard)

        async def _create(_conn, cur) -> dict[str, Any]:
            await cur.execute("SELECT buyer_id FROM commerce_buyer_guard WHERE buyer_id=%s FOR UPDATE", (buyer,))
            prior = await self._load_order_by_req(cur, buyer, request_id)
            if prior is not None:
                return await self._same_official_req(cur, prior, sku_id, quantity, buyer_account, buyer_owner_revision)

            await cur.execute("SELECT COUNT(*) AS cnt FROM oms_order WHERE buyer_id=%s AND status=0", (buyer,))
            unpaid = int((await cur.fetchone())["cnt"])
            if unpaid >= self.config.max_unpaid_orders:
                raise CommerceError(429, "TOO_MANY_UNPAID", "未支付订单数量已达上限，请先处理已有订单")

            await cur.execute("SELECT * FROM pms_sku_stock WHERE id=%s FOR UPDATE", (sku_id,))
            sku = await cur.fetchone()
            if sku is None:
                raise CommerceError(404, "SKU_NOT_FOUND", "商品不存在")

            prior2 = await self._load_order_by_req(cur, buyer, request_id)
            if prior2 is not None:
                return await self._same_official_req(cur, prior2, sku_id, quantity, buyer_account, buyer_owner_revision)

            stock = int(sku["stock"])
            lock_stock = int(sku["lock_stock"])
            if int(sku["published"]) != 1 or stock < 0 or lock_stock < 0 or lock_stock > stock:
                raise CommerceError(409, "SKU_UNAVAILABLE", "商品未开放或库存状态异常")

            unit_price = int(sku["price_minor"])
            total = unit_price * quantity
            currency = str(sku["currency"])
            if unit_price <= 0 or total <= 0 or total > MAX_AMOUNT_MINOR or not CURRENCY_RE.fullmatch(currency):
                raise CommerceError(409, "INVALID_AMOUNT", "商品金额或币种非法")

            order_id = str(uuid.uuid4())
            now = self._now_ms()
            await cur.execute(
                "INSERT INTO oms_order(order_sn,buyer_id,request_id,sku_id,quantity,pay_amount,currency,status,"
                "create_time_ms,payment_deadline_ms) VALUES (%s,%s,UNHEX(%s),%s,%s,%s,%s,0,%s,%s)",
                (order_id, buyer, _hex_ascii(request_id), sku_id, quantity, total, currency, now, now + self.config.order_ttl_ms),
            )
            await cur.execute(
                "INSERT INTO commerce_official_owner(order_sn,buyer_account,buyer_owner_revision) "
                "VALUES (%s,UNHEX(%s),UNHEX(%s))",
                (order_id, _hex_utf8(buyer_account), _hex_ascii(buyer_owner_revision)),
            )
            changed = await cur.execute(
                "UPDATE pms_sku_stock SET lock_stock=lock_stock+%s WHERE id=%s AND published=1 AND stock-lock_stock>=%s",
                (quantity, sku_id, quantity),
            )
            if changed != 1:
                raise CommerceError(409, "OUT_OF_STOCK", "库存不足")
            await cur.execute(
                "INSERT INTO oms_order_item(order_sn,product_id,product_sku_id,product_sku_code,"
                "product_quantity,product_price_minor) VALUES (%s,%s,%s,%s,%s,%s)",
                (order_id, int(sku["product_id"]), sku_id, str(sku["sku_code"]), quantity, unit_price),
            )
            grant_changed = await cur.execute(
                "INSERT INTO commerce_order_grant(order_sn,item_config_id,count_per_unit,currency_type,currency_amount,title) "
                "SELECT %s,item_config_id,count_per_unit,currency_type,currency_amount,title FROM commerce_sku_grant "
                "WHERE sku_id=%s AND CHAR_LENGTH(TRIM(title))>0 AND ("
                "(item_config_id>0 AND count_per_unit>0 AND count_per_unit*%s<=4096 AND currency_type=0 AND currency_amount=0) OR "
                "(item_config_id=0 AND count_per_unit=0 AND currency_type IN (0,1,2) AND currency_amount>0 "
                "AND CAST(currency_amount AS DECIMAL(30,0))*%s<=%s))",
                (order_id, sku_id, quantity, quantity, MAX_CURRENCY_GRANT),
            )
            if grant_changed != 1:
                raise CommerceError(409, "INVALID_GRANT", "SKU没有合法游戏交付映射或单次数量越界")
            created = await self._load_order(cur, order_id, lock=False)
            assert created is not None
            return await self._order_view_sql(cur, created)

        try:
            return await self._run_tx(_create)
        except pymysql.MySQLError as exc:
            if _is_dup_key(exc):
                async def _check(_conn, cur) -> dict[str, Any]:
                    existing = await self._load_order_by_req(cur, buyer, request_id)
                    if existing is not None:
                        return await self._same_official_req(cur, existing, sku_id, quantity, buyer_account, buyer_owner_revision)
                    raise CommerceError(409, "REQUEST_CONFLICT", "请求冲突，请使用原请求号重试")

                return await self._run_tx(_check)
            raise

    async def get_order_legacy(self, buyer: str, order_id: str) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        validate_order_id(order_id)
        raise CommerceError(404, "OWNER_REQUIRED", "订单查询需要原账号和归属修订")

    async def cancel_order_legacy(self, buyer: str, order_id: str) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        validate_order_id(order_id)
        raise CommerceError(404, "OWNER_REQUIRED", "订单取消需要原账号和归属修订")

    async def query_order(
        self, buyer: str, order_id: str, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")

        async def _op(_conn, cur) -> dict[str, Any]:
            row = await self._official_owned(cur, buyer, order_id, buyer_account, buyer_owner_revision, lock=False)
            return await self._order_view_sql(cur, row)

        return await self._run_tx(_op)

    async def list_orders(
        self, buyer: str, after_order_id: str, limit: int, buyer_account: str, buyer_owner_revision: str
    ) -> list[dict[str, Any]]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100 or not isinstance(after_order_id, str):
            raise CommerceError(400, "INVALID_PAGE", "订单分页非法")
        validate_account(buyer_account)
        validate_revision(buyer_owner_revision)

        async def _op(_conn, cur) -> list[dict[str, Any]]:
            after_id = 0
            if after_order_id:
                after_row = await self._official_owned(cur, buyer, after_order_id, buyer_account, buyer_owner_revision, lock=False)
                after_id = int(after_row["id"])
            await cur.execute(
                "SELECT o.*, COALESCE(pr.revision, 0) AS payment_revision FROM oms_order o "
                "JOIN commerce_official_owner p ON p.order_sn=o.order_sn "
                "LEFT JOIN commerce_payment_receipt pr ON pr.order_sn=o.order_sn "
                "WHERE o.buyer_id=%s AND p.buyer_account=UNHEX(%s) AND p.buyer_owner_revision=UNHEX(%s) "
                "AND o.sku_id<>0 AND o.id>%s ORDER BY o.id LIMIT %s",
                (buyer, _hex_utf8(buyer_account), _hex_ascii(buyer_owner_revision), after_id, limit),
            )
            return [await self._order_view_sql(cur, r) for r in await cur.fetchall()]

        return await self._run_tx(_op)

    async def _cancel_order_row(self, cur, order: dict[str, Any]) -> dict[str, Any]:
        order_id = str(order["order_sn"])
        status = int(order["status"])
        if status == ORDER_STATUS_CANCELLED:
            return await self._order_view_sql(cur, order)
        if status != ORDER_STATUS_AWAITING_PAYMENT:
            raise CommerceError(409, "ORDER_PAID", "已支付订单不能直接取消")
        if int(order["sku_id"]) == 0:
            await self._consignment_cancelled_sql(cur, order_id)
        else:
            released = await cur.execute(
                "UPDATE pms_sku_stock SET lock_stock=lock_stock-%s WHERE id=%s AND lock_stock>=%s",
                (int(order["quantity"]), int(order["sku_id"]), int(order["quantity"])),
            )
            if released != 1:
                raise CommerceError(409, "LOCK_STOCK_INVALID", "库存预占状态异常")
        changed = await cur.execute("UPDATE oms_order SET status=4 WHERE order_sn=%s AND status=0", (order_id,))
        if changed != 1:
            raise CommerceError(409, "ORDER_STATE_CONFLICT", "订单状态冲突")
        order["status"] = ORDER_STATUS_CANCELLED
        return await self._order_view_sql(cur, order)

    async def cancel_order(
        self, buyer: str, order_id: str, buyer_account: str, buyer_owner_revision: str
    ) -> dict[str, Any]:
        validate_uint64_str(buyer, status=401, code="AUTH_FAILED", message="调用方认证失败")

        async def _op(_conn, cur) -> dict[str, Any]:
            order = await self._official_owned(cur, buyer, order_id, buyer_account, buyer_owner_revision, lock=True)
            return await self._cancel_order_row(cur, order)

        return await self._run_tx(_op)

    async def _known_payment_sql(self, cur, channel: str, transaction_id: str) -> dict[str, Any] | None:
        await cur.execute(
            "SELECT p.order_sn AS bound_order, a.order_sn AS abnormal_of, a.reason AS abnormal_reason, "
            "p.amount_minor, p.currency, r.source AS refund_reason "
            "FROM commerce_payment_receipt p "
            "LEFT JOIN commerce_abnormal_payment a ON a.channel=p.channel AND a.transaction_id=p.transaction_id "
            "LEFT JOIN commerce_refund_intent r ON r.channel=p.channel AND r.transaction_id=p.transaction_id "
            "WHERE p.channel=UNHEX(%s) AND p.transaction_id=UNHEX(%s)",
            (_hex_ascii(channel), _hex_ascii(transaction_id)),
        )
        return await cur.fetchone()

    async def _record_payment_in_cur(
        self, cur, order_id: str, channel: str, transaction_id: str, amount_minor: int, currency: str
    ) -> dict[str, Any]:
        order = await self._load_order(cur, order_id, lock=True)
        if order is None:
            raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
        if int(order["sku_id"]) != 0:
            await self._official_owner(cur, order_id)

        self.config.require_payment_channel(channel)
        if channel == "mock":
            await cur.execute(
                "SELECT transaction_id, amount_minor, currency FROM commerce_mock_transaction "
                "WHERE kind='PAYMENT' AND order_sn=%s",
                (order_id,),
            )
            mf = await cur.fetchone()
            if mf is None:
                raise CommerceError(503, "MOCK_FACT_MISSING", "缺少本地mock交易原件")
            if (
                _decode_ascii(mf["transaction_id"]) != transaction_id
                or int(mf["amount_minor"]) != amount_minor
                or str(mf["currency"]) != currency
            ):
                raise CommerceError(409, "MOCK_MISMATCH", "mock付款回执与原交易不符")

        known = await self._known_payment_sql(cur, channel, transaction_id)
        if known is not None:
            bound = known["bound_order"]
            abn_of = known["abnormal_of"]
            abn_reason = known["abnormal_reason"]
            ref_reason = known["refund_reason"]
            if bound is not None:
                consistent = abn_of is None and (ref_reason is None or refunds_orders_own_payment(str(ref_reason)))
                owner = str(bound)
            else:
                consistent = (
                    abn_of is not None
                    and abn_reason is not None
                    and not refunds_orders_own_payment(str(abn_reason))
                    and str(abn_reason) == str(ref_reason)
                )
                owner = str(abn_of) if abn_of is not None else ""
            if not consistent:
                raise CommerceError(503, "INCONSISTENT_PAYMENT", "付款回执与异常付款记录或退款待办不一致")
            if owner != order_id:
                raise CommerceError(409, "TXN_USED_OTHER_ORDER", "支付流水已用于其他订单")
            if int(known["amount_minor"]) != amount_minor or str(known["currency"]) != currency:
                raise CommerceError(409, "TXN_AMOUNT_CONFLICT", "支付流水已按不同金额或币种记录")
            return {"order": await self._order_view_sql(cur, order), "refundReason": ref_reason}

        ch_hex = _hex_ascii(channel)
        tx_hex = _hex_ascii(transaction_id)

        mismatch: str | None = None
        if str(order["currency"]) != currency:
            mismatch = REFUND_CURRENCY_MISMATCH
        elif int(order["pay_amount"]) != amount_minor:
            mismatch = REFUND_AMOUNT_MISMATCH

        if mismatch is not None:
            return await self._insert_abnormal_sql(cur, order, ch_hex, tx_hex, amount_minor, currency, mismatch)

        state = int(order["status"])
        if state in PAID_ORDER_STATUSES:
            return await self._insert_abnormal_sql(cur, order, ch_hex, tx_hex, amount_minor, currency, REFUND_DUPLICATE_PAYMENT)

        if state == ORDER_STATUS_CANCELLED:
            await cur.execute(
                "INSERT INTO commerce_payment_receipt(channel,transaction_id,order_sn,amount_minor,currency) "
                "VALUES (UNHEX(%s),UNHEX(%s),%s,%s,%s)",
                (ch_hex, tx_hex, order_id, amount_minor, currency),
            )
            await cur.execute(
                "INSERT INTO commerce_refund_intent(channel,transaction_id,order_sn,source,status) "
                "VALUES (UNHEX(%s),UNHEX(%s),%s,%s,'PENDING')",
                (ch_hex, tx_hex, order_id, REFUND_CANCELLED_LATE),
            )
            await cur.execute("UPDATE oms_order SET status=5 WHERE order_sn=%s AND status=4", (order_id,))
            order["status"] = ORDER_STATUS_REFUND_PENDING
            return {"order": await self._order_view_sql(cur, order), "refundReason": REFUND_CANCELLED_LATE}

        if state != ORDER_STATUS_AWAITING_PAYMENT:
            raise CommerceError(503, "ORDER_STATE_INVALID", "订单状态无法处理")

        await cur.execute(
            "INSERT INTO commerce_payment_receipt(channel,transaction_id,order_sn,amount_minor,currency) "
            "VALUES (UNHEX(%s),UNHEX(%s),%s,%s,%s)",
            (ch_hex, tx_hex, order_id, amount_minor, currency),
        )
        if int(order["sku_id"]) == 0:
            await self._consignment_paid_sql(cur, order_id)
        else:
            qty = int(order["quantity"])
            deducted = await cur.execute(
                "UPDATE pms_sku_stock SET stock=stock-%s, lock_stock=lock_stock-%s "
                "WHERE id=%s AND stock>=%s AND lock_stock>=%s",
                (qty, qty, int(order["sku_id"]), qty, qty),
            )
            if deducted != 1:
                raise CommerceError(503, "STOCK_INVARIANT_BROKEN", "库存预占状态异常")
        now = self._now_ms()
        changed = await cur.execute(
            "UPDATE oms_order SET status=1, payment_time_ms=%s WHERE order_sn=%s AND status=0",
            (now, order_id),
        )
        if changed != 1:
            raise CommerceError(503, "ORDER_STATE_CONFLICT", "订单状态冲突")
        await cur.execute(
            "INSERT INTO commerce_delivery_intent(order_sn,buyer_id,sku_id,quantity,status,created_ms) "
            "VALUES (%s,%s,%s,%s,'PENDING',%s)",
            (order_id, str(order["buyer_id"]), int(order["sku_id"]), int(order["quantity"]), now),
        )
        order["status"] = ORDER_STATUS_DELIVERY_PENDING
        return {"order": await self._order_view_sql(cur, order), "refundReason": None}

    async def _insert_abnormal_sql(
        self, cur, order: dict[str, Any], ch_hex: str, tx_hex: str, amount_minor: int, currency: str, reason: str
    ) -> dict[str, Any]:
        order_id = str(order["order_sn"])
        await cur.execute(
            "INSERT INTO commerce_payment_receipt(channel,transaction_id,order_sn,amount_minor,currency) "
            "VALUES (UNHEX(%s),UNHEX(%s),NULL,%s,%s)",
            (ch_hex, tx_hex, amount_minor, currency),
        )
        await cur.execute(
            "INSERT INTO commerce_abnormal_payment(channel,transaction_id,order_sn,reason,created_ms) "
            "VALUES (UNHEX(%s),UNHEX(%s),%s,%s,%s)",
            (ch_hex, tx_hex, order_id, reason, self._now_ms()),
        )
        await cur.execute(
            "INSERT INTO commerce_refund_intent(channel,transaction_id,order_sn,source,status) "
            "VALUES (UNHEX(%s),UNHEX(%s),%s,%s,'PENDING')",
            (ch_hex, tx_hex, order_id, reason),
        )
        return {"order": await self._order_view_sql(cur, order), "refundReason": reason}

    async def record_payment(
        self, order_id: str, channel: str, transaction_id: str, amount_minor: int, currency: str
    ) -> dict[str, Any]:
        validate_order_id(order_id)
        validate_channel(channel)
        validate_txn_id(transaction_id)
        if not isinstance(amount_minor, int) or isinstance(amount_minor, bool) or not 1 <= amount_minor <= MAX_AMOUNT_MINOR:
            raise CommerceError(400, "INVALID_PAYMENT", "支付回执参数非法")
        validate_currency(currency)

        async def _op(_conn, cur) -> dict[str, Any]:
            return await self._record_payment_in_cur(cur, order_id, channel, transaction_id, amount_minor, currency)

        try:
            return await self._run_tx(_op)
        except pymysql.MySQLError as exc:
            if _is_dup_key(exc):
                async def _check(_conn, cur) -> None:
                    known = await self._known_payment_sql(cur, channel, transaction_id)
                    if known is not None:
                        owner = known["bound_order"] or known["abnormal_of"]
                        if owner and str(owner) != order_id:
                            raise CommerceError(409, "TXN_USED_OTHER_ORDER", "支付流水已用于其他订单")
                    raise CommerceError(503, "UNEXPECTED_UNIQUE_KEY", "付款记录撞上未预期的唯一约束")

                await self._run_tx(_check)
            raise

    # -- Payment Query & Revision -----------------------------------------------------------------------------

    async def payment_order_view(self, order_id: str) -> dict[str, Any]:
        validate_order_id(order_id)

        async def _op(_conn, cur) -> dict[str, Any]:
            row = await self._load_order(cur, order_id, lock=False)
            if row is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
            return await self._order_view_sql(cur, row)

        return await self._run_tx(_op)

    async def public_skus(self, after_sku_id: int = 0, limit: int = 100) -> dict[str, Any]:
        if (
            not isinstance(after_sku_id, int) or isinstance(after_sku_id, bool) or after_sku_id < 0
            or not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100
        ):
            raise CommerceError(400, "INVALID_SKU_PAGE", "商品分页非法")

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute(
                "SELECT s.id, g.title, s.price_minor, s.currency, s.stock - s.lock_stock AS available "
                "FROM pms_sku_stock s JOIN commerce_sku_grant g ON g.sku_id=s.id "
                "WHERE s.published=1 AND s.stock>s.lock_stock AND s.id>%s ORDER BY s.id LIMIT %s",
                (after_sku_id, limit),
            )
            skus = [
                {
                    "skuId": int(r["id"]),
                    "title": str(r["title"]),
                    "priceMinor": int(r["price_minor"]),
                    "currency": str(r["currency"]),
                    "available": int(r["available"]),
                }
                for r in await cur.fetchall()
            ]
            return {"skus": skus}

        return await self._run_tx(_op)

    async def record_payment_revision(
        self, order_id: str, channel: str, transaction_id: str, revision: int, fact: str
    ) -> dict[str, Any]:
        if (
            not isinstance(order_id, str) or not UUID_RE.fullmatch(order_id)
            or not isinstance(channel, str) or not CHANNEL_RE.fullmatch(channel)
            or not isinstance(transaction_id, str) or not TXN_ID_RE.fullmatch(transaction_id)
            or not isinstance(revision, int) or isinstance(revision, bool) or revision < 1
            or fact not in (FACT_PAID, FACT_REVOKED)
            or (revision % 2 == 1) != (fact == FACT_REVOKED)
        ):
            raise CommerceError(400, "INVALID_REVISION", "付款修订参数非法")
        self.config.require_payment_channel(channel)
        ch_hex = _hex_ascii(channel)
        tx_hex = _hex_ascii(transaction_id)

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute("SELECT status, sku_id FROM oms_order WHERE order_sn=%s FOR UPDATE", (order_id,))
            ord_row = await cur.fetchone()
            if ord_row is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "订单不存在")
            order_status = int(ord_row["status"])
            sku_id = int(ord_row["sku_id"])

            await cur.execute(
                "SELECT order_sn, revision, fact FROM commerce_payment_receipt "
                "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) FOR UPDATE",
                (ch_hex, tx_hex),
            )
            rec = await cur.fetchone()
            if rec is None:
                raise CommerceError(404, "RECEIPT_NOT_FOUND", "付款回执不存在")
            bound = rec["order_sn"]
            owner = bound
            if owner is None:
                await cur.execute(
                    "SELECT order_sn FROM commerce_abnormal_payment WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s)",
                    (ch_hex, tx_hex),
                )
                abn = await cur.fetchone()
                if abn is None:
                    raise CommerceError(503, "ABNORMAL_MISSING", "不绑定订单的付款缺异常付款记录")
                owner = abn["order_sn"]
            if str(owner) != order_id:
                raise CommerceError(409, "PAYMENT_ORDER_MISMATCH", "这笔付款不属于这张订单")
            stored_rev = int(rec["revision"])
            stored_fact = str(rec["fact"])
            if revision <= stored_rev:
                return {"revision": stored_rev, "fact": stored_fact, "orderState": order_state_name(order_status)}

            changed = await cur.execute(
                "UPDATE commerce_payment_receipt SET revision=%s, fact=%s "
                "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) AND revision=%s",
                (revision, fact, ch_hex, tx_hex, stored_rev),
            )
            if changed != 1:
                raise CommerceError(503, "REVISION_CONFLICT", "付款回执修订冲突")

            revoked = fact == FACT_REVOKED
            await cur.execute(
                "SELECT source, status FROM commerce_refund_intent "
                "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) FOR UPDATE",
                (ch_hex, tx_hex),
            )
            refund = await cur.fetchone()
            next_status = order_status
            if refund is not None:
                own = refunds_orders_own_payment(str(refund["source"]))
                cur_st = str(refund["status"])
                if revoked and cur_st in ("PENDING", "MANUAL"):
                    expected_ord = ORDER_STATUS_REFUND_PENDING if cur_st == "PENDING" else ORDER_STATUS_MANUAL
                    await cur.execute(
                        "UPDATE commerce_refund_intent SET status='REVOKED', close_reason='CHANNEL_REVOKED' "
                        "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) AND status=%s",
                        (ch_hex, tx_hex, cur_st),
                    )
                    if own:
                        if await cur.execute("UPDATE oms_order SET status=6 WHERE order_sn=%s AND status=%s", (order_id, expected_ord)) != 1:
                            raise CommerceError(503, "ORDER_REFUND_INCONSISTENT", "订单本身的退款待办与订单状态不一致")
                        next_status = ORDER_STATUS_REFUNDED
                elif not revoked and cur_st == "REVOKED":
                    await cur.execute(
                        "UPDATE commerce_refund_intent SET status='MANUAL', close_reason='REVOCATION_REVERSED' "
                        "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) AND status='REVOKED'",
                        (ch_hex, tx_hex),
                    )
                    if own:
                        if await cur.execute("UPDATE oms_order SET status=7 WHERE order_sn=%s AND status=6", (order_id,)) != 1:
                            raise CommerceError(503, "ORDER_REFUND_INCONSISTENT", "订单本身的退款待办与订单状态不一致")
                        next_status = ORDER_STATUS_MANUAL
            elif bound is None:
                raise CommerceError(503, "ABNORMAL_REFUND_MISSING", "异常付款缺退款待办")
            elif sku_id == 0:
                if revoked and order_status == ORDER_STATUS_DELIVERY_PENDING:
                    await cur.execute(
                        "UPDATE commerce_delivery_intent SET status='REVOKED' WHERE order_sn=%s AND status='PENDING'",
                        (order_id,),
                    )
                    await cur.execute("UPDATE commerce_consignment SET state='MANUAL' WHERE current_order_sn=%s", (order_id,))
                    if await cur.execute("UPDATE oms_order SET status=7 WHERE order_sn=%s AND status=1", (order_id,)) != 1:
                        raise CommerceError(503, "CONSIGNMENT_ORDER_CONFLICT", "寄售订单状态冲突")
                    next_status = ORDER_STATUS_MANUAL
                elif revoked and order_status == ORDER_STATUS_DELIVERED:
                    await cur.execute("SELECT account FROM commerce_seller_income WHERE order_sn=%s", (order_id,))
                    inc = await cur.fetchone()
                    if inc is not None:
                        acc_hex = bytes(inc["account"]).hex()
                        await cur.execute(
                            "INSERT INTO commerce_seller_guard(account) VALUES (UNHEX(%s)) ON DUPLICATE KEY UPDATE account=account",
                            (acc_hex,),
                        )
                        await cur.execute("SELECT account FROM commerce_seller_guard WHERE account=UNHEX(%s) FOR UPDATE", (acc_hex,))
                        await cur.execute(
                            "UPDATE commerce_seller_income SET available_ms=%s "
                            "WHERE order_sn=%s AND paid_fen+reserved_fen+forfeited_fen<net_fen",
                            (INT64_MAX, order_id),
                        )
                    await cur.execute("UPDATE commerce_consignment SET state='MANUAL' WHERE current_order_sn=%s", (order_id,))
                    if await cur.execute("UPDATE oms_order SET status=7 WHERE order_sn=%s AND status=2", (order_id,)) != 1:
                        raise CommerceError(503, "CONSIGNMENT_ORDER_CONFLICT", "寄售订单状态冲突")
                    next_status = ORDER_STATUS_MANUAL
            else:
                if order_status not in BOUND_OFFICIAL_REVERSAL_STATUSES:
                    raise CommerceError(503, "ORDER_STATE_INVALID", "绑定付款的官方订单状态无法处理")
                await cur.execute("SELECT status FROM commerce_delivery_intent WHERE order_sn=%s", (order_id,))
                if await cur.fetchone() is None:
                    raise CommerceError(503, "DELIVERY_INTENT_MISSING", "官方订单的绑定付款缺交付待办")
                await cur.execute("SELECT revision FROM commerce_reversal_intent WHERE order_sn=%s FOR UPDATE", (order_id,))
                rev_row = await cur.fetchone()
                if rev_row is None and not revoked:
                    next_status = order_status
                else:
                    now = self._now_ms()
                    if rev_row is None:
                        await cur.execute(
                            "INSERT INTO commerce_reversal_intent(order_sn,revision,status,updated_ms) "
                            "VALUES (%s,%s,'PENDING',%s)",
                            (order_id, revision, now),
                        )
                    else:
                        updated = await cur.execute(
                            "UPDATE commerce_reversal_intent SET revision=%s, status='PENDING', updated_ms=%s "
                            "WHERE order_sn=%s AND revision<%s",
                            (revision, now, order_id, revision),
                        )
                        if updated != 1:
                            raise CommerceError(503, "REVERSAL_REVISION_AHEAD", "扣回待办的修订号超过付款回执")
                    if order_status != ORDER_STATUS_REVERSING:
                        if await cur.execute("UPDATE oms_order SET status=8 WHERE order_sn=%s AND status=%s", (order_id, order_status)) != 1:
                            raise CommerceError(503, "ORDER_STATE_CONFLICT", "订单状态冲突")
                    next_status = ORDER_STATUS_REVERSING

            return {"revision": revision, "fact": fact, "orderState": order_state_name(next_status)}

        return await self._run_tx(_op)

    # -- Delivery / Refund / Reversal -------------------------------------------------------------------------

    def _row_to_official_intent(self, r: dict[str, Any]) -> dict[str, Any]:
        if (
            str(r["d_buyer_id"]) != str(r["o_buyer_id"])
            or int(r["d_sku_id"]) != int(r["o_sku_id"])
            or int(r["d_quantity"]) != int(r["o_quantity"])
        ):
            raise CommerceError(503, "INTENT_MISMATCH", "交付意图与原OMS角色或商品不符")
        qty = int(r["d_quantity"])
        item_cfg = int(r["item_config_id"])
        per_unit = int(r["count_per_unit"])
        cur_type = int(r["currency_type"])
        cur_per = int(r["currency_amount"])
        item_count = 0
        currency_amount = 0
        if item_cfg > 0 and per_unit > 0 and cur_type == 0 and cur_per == 0:
            count = qty * per_unit
            if not 1 <= count <= MAX_ITEM_GRANT_COUNT:
                raise CommerceError(503, "GRANT_OUT_OF_RANGE", "已冻结的SKU游戏数量越界")
            item_count = count
        elif item_cfg == 0 and per_unit == 0 and cur_type in ALLOWED_CURRENCY_TYPES and cur_per > 0 and qty > 0:
            currency_amount = qty * cur_per
            if currency_amount > MAX_CURRENCY_GRANT:
                raise CommerceError(503, "GRANT_OUT_OF_RANGE", "已冻结的SKU货币数量越界")
        else:
            raise CommerceError(503, "GRANT_INVALID", "已冻结的SKU发放物非法")
        acc = _decode_utf8(r["buyer_account"])
        rev = _decode_ascii(r["buyer_owner_revision"])
        validate_account(acc, status=503)
        validate_revision(rev, status=503)
        return {
            "orderId": str(r["order_sn"]),
            "buyerId": str(r["d_buyer_id"]),
            "buyerAccount": acc,
            "buyerOwnerRevision": rev,
            "skuId": int(r["d_sku_id"]),
            "quantity": qty,
            "itemConfigId": item_cfg,
            "itemCount": item_count,
            "title": str(r["title"]),
            "amountMinor": int(r["pay_amount"]),
            "currency": str(r["currency"]),
            "currencyType": cur_type,
            "currencyAmount": currency_amount,
        }

    async def poll_deliveries(self, limit: int, cursor: str = "") -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_LIMIT", "交付批量非法")
        parsed = PendingScanCursor.parse(cursor)

        async def _op(_conn, cur) -> dict[str, Any]:
            nonlocal parsed
            if parsed is None:
                await cur.execute(
                    "SELECT created_ms, order_sn FROM commerce_delivery_intent WHERE status='PENDING' "
                    "ORDER BY created_ms DESC, order_sn DESC LIMIT 1"
                )
                top = await cur.fetchone()
                if top is None:
                    return {"entries": []}
                parsed = PendingScanCursor(0, "", int(top["created_ms"]), str(top["order_sn"]))
            await cur.execute(
                "SELECT d.order_sn, d.buyer_id AS d_buyer_id, d.sku_id AS d_sku_id, d.quantity AS d_quantity, "
                "g.item_config_id, g.count_per_unit, g.title, o.pay_amount, o.currency, "
                "p.buyer_account, p.buyer_owner_revision, o.buyer_id AS o_buyer_id, o.sku_id AS o_sku_id, "
                "o.quantity AS o_quantity, g.currency_type, g.currency_amount, "
                "d.created_ms, COALESCE(v.revision, 0) AS payment_revision "
                "FROM commerce_delivery_intent d "
                "JOIN commerce_order_grant g ON g.order_sn=d.order_sn "
                "JOIN oms_order o ON o.order_sn=d.order_sn "
                "LEFT JOIN commerce_official_owner p ON p.order_sn=d.order_sn "
                "LEFT JOIN commerce_reversal_intent v ON v.order_sn=d.order_sn "
                "WHERE d.status='PENDING' AND d.sku_id<>0 "
                "AND (d.created_ms>%s OR (d.created_ms=%s AND d.order_sn>%s)) "
                "AND (d.created_ms<%s OR (d.created_ms=%s AND d.order_sn<=%s)) "
                "ORDER BY d.created_ms, d.order_sn LIMIT %s",
                (parsed.after_ms, parsed.after_ms, parsed.after_id, parsed.upper_ms, parsed.upper_ms, parsed.upper_id, limit + 1),
            )
            rows = await cur.fetchall()
            has_more = len(rows) > limit
            page = rows[:limit]
            entries = []
            for idx, r in enumerate(page):
                intent = self._row_to_official_intent(r)
                cur_str = "" if (not has_more and idx == len(page) - 1) else parsed.after(int(r["created_ms"]), intent["orderId"])
                entries.append({"intent": intent, "cursor": cur_str, "paymentRevision": int(r["payment_revision"])})
            return {"entries": entries}

        return await self._run_tx(_op)

    async def delivery_receipt(self, order_id: str, trade_order_id: str, state: str, proof: str) -> None:
        if (
            not isinstance(order_id, str) or not UUID_RE.fullmatch(order_id)
            or state not in ("DELIVERED", "REFUND_PENDING", "MANUAL", "VOIDED")
        ):
            raise CommerceError(400, "INVALID_DELIVERY_RECEIPT", "交付持久回执非法")
        validate_uint64_str(trade_order_id, status=400, code="INVALID_TRADE_ORDER_ID", message="交付持久回执非法")
        validate_proof(proof)

        async def _op(_conn, cur) -> None:
            await cur.execute("SELECT status, sku_id, pay_amount FROM oms_order WHERE order_sn=%s FOR UPDATE", (order_id,))
            order = await cur.fetchone()
            await cur.execute("SELECT status FROM commerce_delivery_intent WHERE order_sn=%s FOR UPDATE", (order_id,))
            intent = await cur.fetchone()
            if intent is None:
                raise CommerceError(404, "DELIVERY_NOT_FOUND", "交付意图不存在")
            await cur.execute("SELECT trade_order_id, state FROM commerce_delivery_receipt WHERE order_sn=%s", (order_id,))
            existing = await cur.fetchone()
            if existing is not None:
                if str(existing["trade_order_id"]) != trade_order_id or str(existing["state"]) != state:
                    raise CommerceError(409, "DELIVERY_CONFLICT", "同一交付意图的终局发生冲突")
                return
            if str(intent["status"]) != "PENDING":
                raise CommerceError(409, "DELIVERY_ALREADY_FINAL", "交付意图已终结但缺原始回执")
            if order is None:
                raise CommerceError(409, "ORDER_STATE_CONFLICT", "交付订单不在已支付待交付状态")
            await cur.execute("SELECT revision FROM commerce_reversal_intent WHERE order_sn=%s FOR UPDATE", (order_id,))
            reversal_owned = (await cur.fetchone()) is not None
            if not reversal_owned and (int(order["status"]) != 1 or state == "VOIDED"):
                raise CommerceError(409, "ORDER_STATE_CONFLICT", "交付订单不在已支付待交付状态")
            if int(order["sku_id"]) == 0:
                await self._consignment_delivered_sql(cur, order_id, state)
            else:
                await self._official_owner(cur, order_id)
            await cur.execute(
                "INSERT INTO commerce_delivery_receipt(order_sn,trade_order_id,state,proof) VALUES (%s,%s,%s,%s)",
                (order_id, trade_order_id, state, proof),
            )
            await cur.execute(
                "UPDATE commerce_delivery_intent SET status=%s WHERE order_sn=%s AND status='PENDING'",
                (state, order_id),
            )
            if reversal_owned:
                return
            if state == "DELIVERED":
                await cur.execute("UPDATE oms_order SET status=2 WHERE order_sn=%s AND status=1", (order_id,))
            elif state == "MANUAL":
                await cur.execute("UPDATE oms_order SET status=7 WHERE order_sn=%s AND status=1", (order_id,))
            elif state == "REFUND_PENDING":
                changed = await cur.execute(
                    "INSERT INTO commerce_refund_intent(channel,transaction_id,order_sn,source,status) "
                    "SELECT channel,transaction_id,order_sn,%s,'PENDING' FROM commerce_payment_receipt WHERE order_sn=%s",
                    (REFUND_DELIVERY_REJECTED, order_id),
                )
                if changed != 1:
                    raise CommerceError(503, "PAYMENT_RECEIPT_MISSING", "交付退款缺订单付款回执")
                await cur.execute("UPDATE oms_order SET status=5 WHERE order_sn=%s AND status=1", (order_id,))

        await self._run_tx(_op)

    async def poll_refunds(self, limit: int = 100, cursor: str = "") -> dict[str, Any]:
        eff_limit = 100 if limit == 0 else limit
        if not isinstance(eff_limit, int) or isinstance(eff_limit, bool) or not 1 <= eff_limit <= 100:
            raise CommerceError(400, "INVALID_REFUND_LIMIT", "退款分页批量非法")
        after = _parse_refund_cursor(cursor)

        async def _op(_conn, cur) -> dict[str, Any]:
            if after is None:
                await cur.execute(
                    "SELECT r.order_sn, p.channel, p.transaction_id, p.amount_minor, p.currency, r.source "
                    "FROM commerce_refund_intent r "
                    "JOIN commerce_payment_receipt p ON p.channel=r.channel AND p.transaction_id=r.transaction_id "
                    "WHERE r.status='PENDING' ORDER BY r.order_sn, r.channel, r.transaction_id LIMIT %s",
                    (eff_limit + 1,),
                )
            else:
                ch_hex = _hex_ascii(after[1])
                tx_hex = _hex_ascii(after[2])
                await cur.execute(
                    "SELECT r.order_sn, p.channel, p.transaction_id, p.amount_minor, p.currency, r.source "
                    "FROM commerce_refund_intent r "
                    "JOIN commerce_payment_receipt p ON p.channel=r.channel AND p.transaction_id=r.transaction_id "
                    "WHERE r.status='PENDING' AND (r.order_sn>%s OR (r.order_sn=%s AND "
                    "(r.channel>UNHEX(%s) OR (r.channel=UNHEX(%s) AND r.transaction_id>UNHEX(%s))))) "
                    "ORDER BY r.order_sn, r.channel, r.transaction_id LIMIT %s",
                    (after[0], after[0], ch_hex, ch_hex, tx_hex, eff_limit + 1),
                )
            rows = await cur.fetchall()
            has_more = len(rows) > eff_limit
            page = rows[:eff_limit]
            entries = [
                {
                    "orderId": str(r["order_sn"]),
                    "channel": _decode_ascii(r["channel"]),
                    "transactionId": _decode_ascii(r["transaction_id"]),
                    "amountMinor": int(r["amount_minor"]),
                    "currency": str(r["currency"]),
                    "reason": str(r["source"]),
                }
                for r in page
            ]
            next_cursor = ""
            if has_more and entries:
                last = entries[-1]
                next_cursor = f"r1:{last['orderId']}:{last['channel']}:{last['transactionId']}"
            return {"entries": entries, "nextCursor": next_cursor}

        return await self._run_tx(_op)

    async def _refund_receipt_in_cur(
        self,
        cur,
        order_id: str,
        channel: str,
        payment_transaction_id: str,
        transaction_id: str,
        amount_minor: int,
        currency: str,
    ) -> None:
        ch_hex = _hex_ascii(channel)
        ptx_hex = _hex_ascii(payment_transaction_id)
        tx_hex = _hex_ascii(transaction_id)

        await cur.execute("SELECT status FROM oms_order WHERE order_sn=%s FOR UPDATE", (order_id,))
        ord_row = await cur.fetchone()
        if ord_row is None:
            raise CommerceError(404, "ORDER_NOT_FOUND", "退款订单不存在")
        await cur.execute(
            "SELECT order_sn, source, status FROM commerce_refund_intent "
            "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) FOR UPDATE",
            (ch_hex, ptx_hex),
        )
        intent = await cur.fetchone()
        if intent is None or str(intent["order_sn"]) != order_id:
            raise CommerceError(404, "REFUND_NOT_FOUND", "退款待办不存在")
        await cur.execute(
            "SELECT order_sn, amount_minor, currency FROM commerce_payment_receipt "
            "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s)",
            (ch_hex, ptx_hex),
        )
        orig = await cur.fetchone()
        if orig is None:
            raise CommerceError(503, "RECEIPT_MISSING", "退款待办缺原付款回执")
        if int(orig["amount_minor"]) != amount_minor or str(orig["currency"]) != currency:
            raise CommerceError(409, "REFUND_AMOUNT_MISMATCH", "退款金额币种不符")
        own = refunds_orders_own_payment(str(intent["source"]))
        if own != (orig["order_sn"] == order_id):
            raise CommerceError(503, "REFUND_BINDING_MISMATCH", "退款待办来源与付款绑定不一致")

        self.config.require_payment_channel(channel)
        if channel == "mock":
            await cur.execute(
                "SELECT transaction_id, amount_minor, currency FROM commerce_mock_transaction "
                "WHERE kind='PAYMENT' AND order_sn=%s",
                (order_id,),
            )
            pf = await cur.fetchone()
            await cur.execute(
                "SELECT transaction_id, payment_transaction_id, amount_minor, currency FROM commerce_mock_transaction "
                "WHERE kind='REFUND' AND order_sn=%s",
                (order_id,),
            )
            rf = await cur.fetchone()
            if pf is None or rf is None:
                raise CommerceError(503, "MOCK_FACT_MISSING", "缺少本地mock交易原件")
            if (
                _decode_ascii(pf["transaction_id"]) != payment_transaction_id
                or _decode_ascii(rf["transaction_id"]) != transaction_id
                or _decode_ascii(rf["payment_transaction_id"]) != _decode_ascii(pf["transaction_id"])
                or int(rf["amount_minor"]) != int(pf["amount_minor"])
                or str(rf["currency"]) != str(pf["currency"])
            ):
                raise CommerceError(409, "MOCK_REFUND_MISMATCH", "mock退款与原付款事实不符")

        await cur.execute(
            "SELECT transaction_id FROM commerce_refund_receipt "
            "WHERE channel=UNHEX(%s) AND payment_transaction_id=UNHEX(%s)",
            (ch_hex, ptx_hex),
        )
        prior = await cur.fetchone()
        if prior is not None:
            if _decode_ascii(prior["transaction_id"]) != transaction_id:
                raise CommerceError(409, "REFUND_TXN_CONFLICT", "退款流水冲突")
            return

        cur_st = str(intent["status"])
        if cur_st in ("MANUAL", "REVOKED"):
            raise CommerceError(409, "REFUND_FINALIZED", "退款待办已终结")
        if cur_st != "PENDING":
            raise CommerceError(503, "REFUND_STATE_BROKEN", "退款待办已终结但缺退款回执")
        if own and int(ord_row["status"]) != ORDER_STATUS_REFUND_PENDING:
            raise CommerceError(409, "ORDER_NOT_REFUNDING", "订单不在退款中")

        await cur.execute(
            "INSERT INTO commerce_refund_receipt(channel,payment_transaction_id,transaction_id,order_sn,amount_minor,currency) "
            "VALUES (UNHEX(%s),UNHEX(%s),UNHEX(%s),%s,%s,%s)",
            (ch_hex, ptx_hex, tx_hex, order_id, amount_minor, currency),
        )
        if await cur.execute(
            "UPDATE commerce_refund_intent SET status='REFUNDED' "
            "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) AND status='PENDING'",
            (ch_hex, ptx_hex),
        ) != 1:
            raise CommerceError(409, "REFUND_CONFLICT", "退款待办状态冲突")
        if own:
            if await cur.execute("UPDATE oms_order SET status=6 WHERE order_sn=%s AND status=5", (order_id,)) != 1:
                raise CommerceError(409, "ORDER_STATE_CONFLICT", "订单状态冲突")

    async def refund_receipt(
        self,
        order_id: str,
        channel: str,
        payment_transaction_id: str,
        transaction_id: str,
        amount_minor: int,
        currency: str,
    ) -> None:
        validate_order_id(order_id)
        validate_channel(channel)
        validate_txn_id(payment_transaction_id)
        validate_txn_id(transaction_id)
        if not isinstance(amount_minor, int) or isinstance(amount_minor, bool) or amount_minor < 1:
            raise CommerceError(400, "INVALID_REFUND", "退款回执非法")
        validate_currency(currency)

        async def _op(_conn, cur) -> None:
            await self._refund_receipt_in_cur(
                cur, order_id, channel, payment_transaction_id, transaction_id, amount_minor, currency
            )

        try:
            await self._run_tx(_op)
        except pymysql.MySQLError as exc:
            if _is_dup_key(exc):
                raise CommerceError(409, "REFUND_TXN_REUSED", "退款流水已用于其他付款") from None
            raise

    async def close_refund(
        self, order_id: str, channel: str, payment_transaction_id: str, reason: str
    ) -> None:
        if (
            not isinstance(order_id, str) or not UUID_RE.fullmatch(order_id)
            or not isinstance(channel, str) or not CHANNEL_RE.fullmatch(channel)
            or not isinstance(payment_transaction_id, str) or not TXN_ID_RE.fullmatch(payment_transaction_id)
            or not isinstance(reason, str) or not CLOSE_REASON_RE.fullmatch(reason)
        ):
            raise CommerceError(400, "INVALID_CLOSE_REFUND", "退款转人工参数非法")
        self.config.require_payment_channel(channel)
        ch_hex = _hex_ascii(channel)
        ptx_hex = _hex_ascii(payment_transaction_id)

        async def _op(_conn, cur) -> None:
            await cur.execute("SELECT status FROM oms_order WHERE order_sn=%s FOR UPDATE", (order_id,))
            if await cur.fetchone() is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "退款订单不存在")
            await cur.execute(
                "SELECT order_sn, source, status FROM commerce_refund_intent "
                "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) FOR UPDATE",
                (ch_hex, ptx_hex),
            )
            intent = await cur.fetchone()
            if intent is None or str(intent["order_sn"]) != order_id:
                raise CommerceError(404, "REFUND_NOT_FOUND", "退款待办不存在")
            cur_st = str(intent["status"])
            if cur_st == "MANUAL":
                return
            if cur_st != "PENDING":
                raise CommerceError(409, "REFUND_FINALIZED", "退款待办已终结")
            await cur.execute(
                "UPDATE commerce_refund_intent SET status='MANUAL', close_reason=%s "
                "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) AND status='PENDING'",
                (reason, ch_hex, ptx_hex),
            )
            if refunds_orders_own_payment(str(intent["source"])):
                if await cur.execute("UPDATE oms_order SET status=7 WHERE order_sn=%s AND status=5", (order_id,)) != 1:
                    raise CommerceError(503, "ORDER_REFUND_INCONSISTENT", "订单本身的退款待办与订单状态不一致")

        await self._run_tx(_op)

    async def poll_reversals(self, limit: int, cursor: str = "") -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_LIMIT", "扣回批量非法")
        parsed = PendingScanCursor.parse(cursor)

        async def _op(_conn, cur) -> dict[str, Any]:
            nonlocal parsed
            if parsed is None:
                await cur.execute(
                    "SELECT updated_ms, order_sn FROM commerce_reversal_intent WHERE status='PENDING' "
                    "ORDER BY updated_ms DESC, order_sn DESC LIMIT 1"
                )
                top = await cur.fetchone()
                if top is None:
                    return {"entries": []}
                parsed = PendingScanCursor(0, "", int(top["updated_ms"]), str(top["order_sn"]))
            await cur.execute(
                "SELECT d.order_sn, d.buyer_id AS d_buyer_id, d.sku_id AS d_sku_id, d.quantity AS d_quantity, "
                "g.item_config_id, g.count_per_unit, g.title, o.pay_amount, o.currency, "
                "p.buyer_account, p.buyer_owner_revision, o.buyer_id AS o_buyer_id, o.sku_id AS o_sku_id, "
                "o.quantity AS o_quantity, g.currency_type, g.currency_amount, v.updated_ms, v.revision "
                "FROM commerce_reversal_intent v "
                "JOIN commerce_delivery_intent d ON d.order_sn=v.order_sn "
                "JOIN commerce_order_grant g ON g.order_sn=d.order_sn "
                "JOIN oms_order o ON o.order_sn=d.order_sn "
                "LEFT JOIN commerce_official_owner p ON p.order_sn=d.order_sn "
                "WHERE v.status='PENDING' "
                "AND (v.updated_ms>%s OR (v.updated_ms=%s AND v.order_sn>%s)) "
                "AND (v.updated_ms<%s OR (v.updated_ms=%s AND v.order_sn<=%s)) "
                "ORDER BY v.updated_ms, v.order_sn LIMIT %s",
                (parsed.after_ms, parsed.after_ms, parsed.after_id, parsed.upper_ms, parsed.upper_ms, parsed.upper_id, limit + 1),
            )
            rows = await cur.fetchall()
            has_more = len(rows) > limit
            page = rows[:limit]
            entries = []
            for idx, r in enumerate(page):
                rev = int(r["revision"])
                if rev < 1:
                    raise CommerceError(503, "INVALID_REVERSAL_REV", "扣回待办修订号非法")
                intent = self._row_to_official_intent(r)
                cur_str = "" if (not has_more and idx == len(page) - 1) else parsed.after(int(r["updated_ms"]), intent["orderId"])
                entries.append({"intent": intent, "revision": rev, "entitled": rev % 2 == 0, "cursor": cur_str})
            return {"entries": entries}

        return await self._run_tx(_op)

    async def reversal_receipt(
        self, order_id: str, revision: int, trade_order_id: str, state: str, proof: str
    ) -> None:
        if (
            not isinstance(order_id, str) or not UUID_RE.fullmatch(order_id)
            or not isinstance(revision, int) or isinstance(revision, bool) or revision < 1
            or state not in ("REVERSED", "DELIVERED", "MANUAL")
        ):
            raise CommerceError(400, "INVALID_REVERSAL_RECEIPT", "扣回回执非法")
        validate_uint64_str(trade_order_id, status=400, code="INVALID_TRADE_ORDER_ID", message="扣回回执非法")
        validate_proof(proof)
        entitled = revision % 2 == 0
        if state == ("REVERSED" if entitled else "DELIVERED"):
            raise CommerceError(400, "REVERSAL_STATE_MISMATCH", "扣回回执的结论与修订号不符")

        async def _op(_conn, cur) -> None:
            await cur.execute("SELECT status FROM oms_order WHERE order_sn=%s FOR UPDATE", (order_id,))
            if await cur.fetchone() is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "扣回订单不存在")
            await cur.execute("SELECT revision, status FROM commerce_reversal_intent WHERE order_sn=%s FOR UPDATE", (order_id,))
            intent = await cur.fetchone()
            if intent is None:
                raise CommerceError(404, "REVERSAL_NOT_FOUND", "扣回待办不存在")
            latest = int(intent["revision"])
            if revision > latest:
                raise CommerceError(409, "REVISION_TOO_NEW", "扣回回执的修订号超过商城记下的最新修订号")
            await cur.execute(
                "SELECT trade_order_id, state FROM commerce_reversal_receipt WHERE order_sn=%s AND revision=%s",
                (order_id, revision),
            )
            existing = await cur.fetchone()
            if existing is not None:
                if str(existing["trade_order_id"]) != trade_order_id or str(existing["state"]) != state:
                    raise CommerceError(409, "REVERSAL_CONFLICT", "同一修订号的扣回结论发生冲突")
                return
            await cur.execute(
                "INSERT INTO commerce_reversal_receipt(order_sn,revision,trade_order_id,state,proof) "
                "VALUES (%s,%s,%s,%s,%s)",
                (order_id, revision, trade_order_id, state, proof),
            )
            if revision < latest:
                return
            if str(intent["status"]) != "PENDING":
                raise CommerceError(503, "REVERSAL_ALREADY_FINAL", "扣回待办已终结但缺这个修订号的回执")
            await cur.execute(
                "UPDATE commerce_reversal_intent SET status=%s, updated_ms=%s "
                "WHERE order_sn=%s AND revision=%s AND status='PENDING'",
                (state, self._now_ms(), order_id, revision),
            )
            nxt = ORDER_STATUS_REVERSED if state == "REVERSED" else ORDER_STATUS_DELIVERED if state == "DELIVERED" else ORDER_STATUS_MANUAL
            if await cur.execute("UPDATE oms_order SET status=%s WHERE order_sn=%s AND status=8", (nxt, order_id)) != 1:
                raise CommerceError(503, "ORDER_REVERSING_INCONSISTENT", "扣回中的订单状态不一致")

        await self._run_tx(_op)

    # -- Catalog & Manual Admin -------------------------------------------------------------------------------

    async def _run_admin_cmd_sql(
        self,
        actor: str,
        request_id: str,
        kind: str,
        payload: dict[str, Any],
        mutate: Callable[[Any], Awaitable[dict[str, Any]]],
    ) -> dict[str, Any]:
        if not isinstance(actor, str) or not ACTOR_RE.fullmatch(actor) or not isinstance(request_id, str):
            raise CommerceError(400, "INVALID_ADMIN_REQ", "管理主体或请求号非法")
        validate_request_id(request_id)
        digest = _command_digest(kind, payload)
        actor_hex = _hex_ascii(actor)
        req_hex = _hex_ascii(request_id)
        pending = f"pending:{uuid.uuid4()}"

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute(
                "INSERT INTO commerce_admin_command(actor,request_id,command_type,request_digest,result_json,created_ms) "
                "VALUES (UNHEX(%s),UNHEX(%s),%s,%s,%s,%s) ON DUPLICATE KEY UPDATE request_id=request_id",
                (actor_hex, req_hex, kind, digest, pending, self._now_ms()),
            )
            await cur.execute(
                "SELECT command_type, request_digest, result_json FROM commerce_admin_command "
                "WHERE actor=UNHEX(%s) AND request_id=UNHEX(%s) FOR UPDATE",
                (actor_hex, req_hex),
            )
            row = await cur.fetchone()
            if row is None:
                raise CommerceError(503, "ADMIN_JOURNAL_MISSING", "管理请求原件缺失")
            if str(row["command_type"]) != kind or str(row["request_digest"]) != digest:
                raise CommerceError(409, "ADMIN_REQ_CONFLICT", "管理请求号已用于不同条款")
            if str(row["result_json"]) != pending:
                return json.loads(str(row["result_json"]))
            result = await mutate(cur)
            changed = await cur.execute(
                "UPDATE commerce_admin_command SET result_json=%s "
                "WHERE actor=UNHEX(%s) AND request_id=UNHEX(%s) AND result_json=%s",
                (_canonical_json(result), actor_hex, req_hex, pending),
            )
            if changed != 1:
                raise CommerceError(503, "ADMIN_SAVE_FAILED", "管理命令响应记录失败")
            return result

        try:
            return await self._run_tx(_op)
        except pymysql.MySQLError as exc:
            if _is_dup_key(exc):
                raise CommerceError(409, "ADMIN_CONFLICT", "SKU已存在或管理请求冲突") from None
            raise

    @staticmethod
    def _row_to_admin_sku(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "skuId": int(r["id"]),
            "revision": int(r["catalog_revision"]),
            "productId": int(r["product_id"]),
            "skuCode": str(r["sku_code"]),
            "priceMinor": int(r["price_minor"]),
            "currency": str(r["currency"]),
            "stock": int(r["stock"]),
            "lockedStock": int(r["lock_stock"]),
            "published": int(r["published"]) == 1,
            "itemConfigId": int(r["item_config_id"] or 0),
            "countPerUnit": int(r["count_per_unit"] or 0),
            "title": str(r["title"] or ""),
            "currencyType": int(r["currency_type"] or 0),
            "currencyAmount": int(r["currency_amount"] or 0),
        }

    async def _read_admin_sku(self, cur, sku_id: int) -> dict[str, Any]:
        await cur.execute(
            "SELECT s.*, g.item_config_id, g.count_per_unit, g.title, g.currency_type, g.currency_amount "
            "FROM pms_sku_stock s LEFT JOIN commerce_sku_grant g ON g.sku_id=s.id WHERE s.id=%s",
            (sku_id,),
        )
        row = await cur.fetchone()
        if row is None:
            raise CommerceError(503, "SKU_READ_FAILED", "SKU读回失败")
        return self._row_to_admin_sku(row)

    async def admin_list_skus(self, after_sku_id: int, limit: int) -> dict[str, Any]:
        if (
            not isinstance(after_sku_id, int) or isinstance(after_sku_id, bool) or after_sku_id < 0
            or not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100
        ):
            raise CommerceError(400, "INVALID_SKU_PAGE", "管理目录分页非法")

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute(
                "SELECT s.*, g.item_config_id, g.count_per_unit, g.title, g.currency_type, g.currency_amount "
                "FROM pms_sku_stock s LEFT JOIN commerce_sku_grant g ON g.sku_id=s.id "
                "WHERE s.id>%s ORDER BY s.id LIMIT %s",
                (after_sku_id, limit),
            )
            return {"skus": [self._row_to_admin_sku(r) for r in await cur.fetchall()]}

        return await self._run_tx(_op)

    async def admin_create_sku(
        self, actor: str, request_id: str, sku_id: int, terms: SkuTerms, stock: int
    ) -> dict[str, Any]:
        if (
            not isinstance(sku_id, int) or isinstance(sku_id, bool) or sku_id <= 0
            or not isinstance(stock, int) or isinstance(stock, bool) or stock < 0 or stock > INT32_MAX
        ):
            raise CommerceError(400, "INVALID_CREATE_SKU", "新增SKU参数非法")
        validate_sku_terms(terms)
        payload = {"requestId": request_id, "skuId": sku_id, "terms": terms.to_dict(), "stock": stock}

        async def _mutate(cur) -> dict[str, Any]:
            await cur.execute(
                "INSERT INTO pms_sku_stock(id,product_id,sku_code,price_minor,currency,stock,lock_stock,published,catalog_revision) "
                "VALUES (%s,%s,%s,%s,%s,%s,0,%s,1)",
                (sku_id, terms.product_id, terms.sku_code, terms.price_minor, terms.currency, stock, 1 if terms.published else 0),
            )
            await cur.execute(
                "INSERT INTO commerce_sku_grant(sku_id,item_config_id,count_per_unit,currency_type,currency_amount,title) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
                "item_config_id=VALUES(item_config_id),count_per_unit=VALUES(count_per_unit),"
                "currency_type=VALUES(currency_type),currency_amount=VALUES(currency_amount),title=VALUES(title)",
                (sku_id, terms.item_config_id, terms.count_per_unit, terms.currency_type, terms.currency_amount, terms.title),
            )
            return await self._read_admin_sku(cur, sku_id)

        return await self._run_admin_cmd_sql(actor, request_id, "create", payload, _mutate)

    async def admin_update_sku(
        self, actor: str, request_id: str, sku_id: int, expected_revision: int, terms: SkuTerms
    ) -> dict[str, Any]:
        if not isinstance(sku_id, int) or isinstance(sku_id, bool) or sku_id <= 0:
            raise CommerceError(400, "INVALID_UPDATE_SKU", "修改SKU参数非法")
        if not isinstance(expected_revision, int) or isinstance(expected_revision, bool) or expected_revision < 0 or expected_revision >= INT64_MAX:
            raise CommerceError(400, "INVALID_REVISION", "管理修订非法或已耗尽")
        validate_sku_terms(terms)
        payload = {"requestId": request_id, "skuId": sku_id, "expectedRevision": expected_revision, "terms": terms.to_dict()}

        async def _mutate(cur) -> dict[str, Any]:
            await cur.execute("SELECT catalog_revision FROM pms_sku_stock WHERE id=%s FOR UPDATE", (sku_id,))
            row = await cur.fetchone()
            if row is None:
                raise CommerceError(404, "SKU_NOT_FOUND", "SKU不存在")
            if int(row["catalog_revision"]) != expected_revision:
                raise CommerceError(409, "REVISION_CONFLICT", "管理目录修订已变化")
            changed = await cur.execute(
                "UPDATE pms_sku_stock SET product_id=%s,sku_code=%s,price_minor=%s,currency=%s,published=%s,"
                "catalog_revision=catalog_revision+1 WHERE id=%s AND catalog_revision=%s",
                (terms.product_id, terms.sku_code, terms.price_minor, terms.currency, 1 if terms.published else 0, sku_id, expected_revision),
            )
            if changed != 1:
                raise CommerceError(409, "REVISION_CONFLICT", "管理目录修订已变化")
            await cur.execute(
                "INSERT INTO commerce_sku_grant(sku_id,item_config_id,count_per_unit,currency_type,currency_amount,title) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
                "item_config_id=VALUES(item_config_id),count_per_unit=VALUES(count_per_unit),"
                "currency_type=VALUES(currency_type),currency_amount=VALUES(currency_amount),title=VALUES(title)",
                (sku_id, terms.item_config_id, terms.count_per_unit, terms.currency_type, terms.currency_amount, terms.title),
            )
            return await self._read_admin_sku(cur, sku_id)

        return await self._run_admin_cmd_sql(actor, request_id, "update", payload, _mutate)

    async def admin_adjust_stock(
        self, actor: str, request_id: str, sku_id: int, expected_revision: int, delta: int
    ) -> dict[str, Any]:
        if (
            not isinstance(sku_id, int) or isinstance(sku_id, bool) or sku_id <= 0
            or not isinstance(delta, int) or isinstance(delta, bool) or delta == 0
        ):
            raise CommerceError(400, "INVALID_STOCK_ADJ", "库存调整参数非法")
        if not isinstance(expected_revision, int) or isinstance(expected_revision, bool) or expected_revision < 0 or expected_revision >= INT64_MAX:
            raise CommerceError(400, "INVALID_REVISION", "管理修订非法或已耗尽")
        payload = {"requestId": request_id, "skuId": sku_id, "expectedRevision": expected_revision, "delta": delta}

        async def _mutate(cur) -> dict[str, Any]:
            await cur.execute("SELECT catalog_revision FROM pms_sku_stock WHERE id=%s FOR UPDATE", (sku_id,))
            row = await cur.fetchone()
            if row is None:
                raise CommerceError(404, "SKU_NOT_FOUND", "SKU不存在")
            if int(row["catalog_revision"]) != expected_revision:
                raise CommerceError(409, "REVISION_CONFLICT", "管理目录修订已变化")
            current = await self._read_admin_sku(cur, sku_id)
            after = current["stock"] + delta
            if after < current["lockedStock"] or after > INT32_MAX:
                raise CommerceError(409, "STOCK_OUT_OF_RANGE", "库存调整越界或侵占未付款预占")
            changed = await cur.execute(
                "UPDATE pms_sku_stock SET stock=stock+%s, catalog_revision=catalog_revision+1 "
                "WHERE id=%s AND catalog_revision=%s AND stock+%s>=lock_stock AND stock+%s<=%s",
                (delta, sku_id, expected_revision, delta, delta, INT32_MAX),
            )
            if changed != 1:
                raise CommerceError(409, "STOCK_CONFLICT", "库存或管理修订已变化")
            return await self._read_admin_sku(cur, sku_id)

        return await self._run_admin_cmd_sql(actor, request_id, "stock", payload, _mutate)

    async def admin_list_manual(
        self, after_order_id: str, after_listing_id: str, after_refund_cursor: str, limit: int
    ) -> dict[str, Any]:
        eff_limit = 50 if limit == 0 else limit
        if not isinstance(eff_limit, int) or isinstance(eff_limit, bool) or not 1 <= eff_limit <= 100:
            raise CommerceError(400, "INVALID_LIMIT", "manual list limit must be between 1 and 100")
        if after_order_id and not UUID_RE.fullmatch(after_order_id):
            raise CommerceError(400, "INVALID_CURSOR", "invalid afterOrderId cursor")
        if after_listing_id:
            validate_uint64_str(after_listing_id, status=400, code="INVALID_CURSOR", message="invalid afterListingId cursor")
        after_refund = _parse_refund_cursor(after_refund_cursor, error_msg="invalid afterRefundCursor")

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute(
                "SELECT o.order_sn,o.buyer_id,o.sku_id,o.quantity,o.pay_amount,o.currency,"
                "COALESCE(pr.revision,0) AS payment_revision,d.status AS delivery_status,"
                "v.revision AS reversal_revision,v.status AS reversal_status,"
                "co.listing_id,c.state AS consignment_state,"
                "i.net_fen,i.reserved_fen,i.paid_fen,i.forfeited_fen,i.available_ms "
                "FROM oms_order o "
                "LEFT JOIN commerce_payment_receipt pr ON pr.order_sn=o.order_sn "
                "LEFT JOIN commerce_delivery_intent d ON d.order_sn=o.order_sn "
                "LEFT JOIN commerce_reversal_intent v ON v.order_sn=o.order_sn "
                "LEFT JOIN commerce_consignment_order co ON co.order_sn=o.order_sn "
                "LEFT JOIN commerce_consignment c ON c.listing_id=co.listing_id "
                "LEFT JOIN commerce_seller_income i ON i.order_sn=o.order_sn "
                "WHERE o.status=7 AND o.order_sn>%s ORDER BY o.order_sn LIMIT %s",
                (after_order_id, eff_limit),
            )
            orders = [
                {
                    "orderId": str(r["order_sn"]),
                    "buyerId": str(r["buyer_id"]),
                    "skuId": int(r["sku_id"]),
                    "quantity": int(r["quantity"]),
                    "amountMinor": int(r["pay_amount"]),
                    "currency": str(r["currency"]),
                    "paymentRevision": int(r["payment_revision"]),
                    "deliveryStatus": r["delivery_status"],
                    "reversalRevision": int(r["reversal_revision"]) if r["reversal_revision"] is not None else None,
                    "reversalStatus": r["reversal_status"],
                    "consignmentListingId": r["listing_id"],
                    "consignmentState": r["consignment_state"],
                    "incomeNetFen": int(r["net_fen"]) if r["net_fen"] is not None else None,
                    "incomeReservedFen": int(r["reserved_fen"]) if r["reserved_fen"] is not None else None,
                    "incomePaidFen": int(r["paid_fen"]) if r["paid_fen"] is not None else None,
                    "incomeForfeitedFen": int(r["forfeited_fen"]) if r["forfeited_fen"] is not None else None,
                    "incomeAvailableMs": int(r["available_ms"]) if r["available_ms"] is not None else None,
                }
                for r in await cur.fetchall()
            ]

            if after_refund is None:
                await cur.execute(
                    "SELECT r.order_sn,p.channel,p.transaction_id,p.amount_minor,p.currency,r.source,r.close_reason,p.revision,p.fact "
                    "FROM commerce_refund_intent r "
                    "JOIN commerce_payment_receipt p ON p.channel=r.channel AND p.transaction_id=r.transaction_id "
                    "WHERE r.status='MANUAL' ORDER BY r.order_sn,r.channel,r.transaction_id LIMIT %s",
                    (eff_limit + 1,),
                )
            else:
                ch_hex = _hex_ascii(after_refund[1])
                tx_hex = _hex_ascii(after_refund[2])
                await cur.execute(
                    "SELECT r.order_sn,p.channel,p.transaction_id,p.amount_minor,p.currency,r.source,r.close_reason,p.revision,p.fact "
                    "FROM commerce_refund_intent r "
                    "JOIN commerce_payment_receipt p ON p.channel=r.channel AND p.transaction_id=r.transaction_id "
                    "WHERE r.status='MANUAL' AND (r.order_sn>%s OR (r.order_sn=%s AND "
                    "(r.channel>UNHEX(%s) OR (r.channel=UNHEX(%s) AND r.transaction_id>UNHEX(%s))))) "
                    "ORDER BY r.order_sn,r.channel,r.transaction_id LIMIT %s",
                    (after_refund[0], after_refund[0], ch_hex, ch_hex, tx_hex, eff_limit + 1),
                )
            ref_rows = await cur.fetchall()
            has_more_ref = len(ref_rows) > eff_limit
            ref_page = ref_rows[:eff_limit]
            refunds = [
                {
                    "orderId": str(r["order_sn"]),
                    "channel": _decode_ascii(r["channel"]),
                    "transactionId": _decode_ascii(r["transaction_id"]),
                    "amountMinor": int(r["amount_minor"]),
                    "currency": str(r["currency"]),
                    "reason": str(r["source"]),
                    "closeReason": r["close_reason"],
                    "paymentRevision": int(r["revision"]),
                    "paymentFact": str(r["fact"]),
                }
                for r in ref_page
            ]
            next_ref_cursor = ""
            if has_more_ref and refunds:
                last = refunds[-1]
                next_ref_cursor = f"r1:{last['orderId']}:{last['channel']}:{last['transactionId']}"

            await cur.execute(
                "SELECT c.listing_id,c.seller_player_id,c.market_zone,c.asset_kind,c.title,c.price_fen,c.current_order_sn,"
                "o.status AS order_status,i.net_fen,i.reserved_fen,i.paid_fen,i.forfeited_fen,i.available_ms "
                "FROM commerce_consignment c "
                "LEFT JOIN oms_order o ON o.order_sn=c.current_order_sn "
                "LEFT JOIN commerce_seller_income i ON i.order_sn=c.current_order_sn "
                "WHERE c.state='MANUAL' AND c.listing_id>%s ORDER BY c.listing_id LIMIT %s",
                (after_listing_id, eff_limit),
            )
            consignments = [
                {
                    "listingId": str(r["listing_id"]),
                    "sellerPlayerId": str(r["seller_player_id"]),
                    "marketZone": int(r["market_zone"]),
                    "assetKind": str(r["asset_kind"]),
                    "title": str(r["title"]),
                    "priceFen": int(r["price_fen"]),
                    "currentOrderId": r["current_order_sn"],
                    "orderState": order_state_name(int(r["order_status"])) if r["order_status"] is not None else None,
                    "incomeNetFen": int(r["net_fen"]) if r["net_fen"] is not None else None,
                    "incomeReservedFen": int(r["reserved_fen"]) if r["reserved_fen"] is not None else None,
                    "incomePaidFen": int(r["paid_fen"]) if r["paid_fen"] is not None else None,
                    "incomeForfeitedFen": int(r["forfeited_fen"]) if r["forfeited_fen"] is not None else None,
                    "incomeAvailableMs": int(r["available_ms"]) if r["available_ms"] is not None else None,
                }
                for r in await cur.fetchall()
            ]
            return {
                "orders": orders,
                "refunds": refunds,
                "nextRefundCursor": next_ref_cursor,
                "consignments": consignments,
            }

        return await self._run_tx(_op)

    async def admin_resolve_order(
        self, actor: str, request_id: str, order_id: str, action: str, reason: str
    ) -> dict[str, Any]:
        if not isinstance(order_id, str) or not UUID_RE.fullmatch(order_id) or action not in MANUAL_ORDER_ACTIONS:
            raise CommerceError(400, "INVALID_MANUAL_ORDER", "invalid manual order resolution request")
        cleaned_reason = _require_reason(reason)
        payload = {"requestId": request_id, "orderId": order_id, "action": action, "reason": reason}

        async def _mutate(cur) -> dict[str, Any]:
            await cur.execute("SELECT status, sku_id FROM oms_order WHERE order_sn=%s FOR UPDATE", (order_id,))
            ord_row = await cur.fetchone()
            if ord_row is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "order not found")
            if int(ord_row["sku_id"]) == 0:
                raise CommerceError(409, "CONSIGNMENT_ORDER", "consignment orders must be resolved through manual/consignments/resolve")
            if int(ord_row["status"]) != ORDER_STATUS_MANUAL:
                raise CommerceError(409, "NOT_MANUAL", "order is not in MANUAL state")
            now = self._now_ms()
            await cur.execute("SELECT revision, status FROM commerce_reversal_intent WHERE order_sn=%s FOR UPDATE", (order_id,))
            rev = await cur.fetchone()
            if action == "RETRY_REVERSAL":
                if rev is None or str(rev["status"]) != "MANUAL":
                    raise CommerceError(409, "NO_MANUAL_REVERSAL", "order has no MANUAL reversal intent to retry")
                r_num = int(rev["revision"])
                await cur.execute("DELETE FROM commerce_reversal_receipt WHERE order_sn=%s AND revision=%s AND state='MANUAL'", (order_id, r_num))
                await cur.execute("UPDATE commerce_reversal_intent SET status='PENDING', updated_ms=%s WHERE order_sn=%s AND revision=%s AND status='MANUAL'", (now, order_id, r_num))
                await cur.execute("UPDATE oms_order SET status=8 WHERE order_sn=%s AND status=7", (order_id,))
                res_state = order_state_name(ORDER_STATUS_REVERSING)
            elif action == "MARK_REVERSED":
                if rev is None or str(rev["status"]) != "MANUAL":
                    raise CommerceError(409, "NO_MANUAL_REVERSAL", "order has no MANUAL reversal intent")
                r_num = int(rev["revision"])
                if r_num % 2 == 0:
                    raise CommerceError(409, "EVEN_REVISION", "even payment revision is entitled and cannot be marked REVERSED")
                await cur.execute("UPDATE commerce_reversal_intent SET status='REVERSED', updated_ms=%s WHERE order_sn=%s AND revision=%s AND status='MANUAL'", (now, order_id, r_num))
                await cur.execute("UPDATE oms_order SET status=9 WHERE order_sn=%s AND status=7", (order_id,))
                res_state = order_state_name(ORDER_STATUS_REVERSED)
            elif action == "MARK_DELIVERED":
                if rev is not None:
                    if str(rev["status"]) != "MANUAL":
                        raise CommerceError(409, "REVERSAL_NOT_MANUAL", "order reversal intent is not in MANUAL state")
                    r_num = int(rev["revision"])
                    if r_num % 2 != 0:
                        raise CommerceError(409, "ODD_REVISION", "odd payment revision is revoked and cannot be marked DELIVERED")
                    await cur.execute("UPDATE commerce_reversal_intent SET status='DELIVERED', updated_ms=%s WHERE order_sn=%s AND revision=%s AND status='MANUAL'", (now, order_id, r_num))
                else:
                    await cur.execute("SELECT status FROM commerce_delivery_intent WHERE order_sn=%s FOR UPDATE", (order_id,))
                    d = await cur.fetchone()
                    if d is None or str(d["status"]) != "MANUAL":
                        raise CommerceError(409, "NO_MANUAL_DELIVERY", "order has no MANUAL delivery intent")
                    await cur.execute("UPDATE commerce_delivery_intent SET status='DELIVERED' WHERE order_sn=%s AND status='MANUAL'", (order_id,))
                await cur.execute("UPDATE oms_order SET status=2 WHERE order_sn=%s AND status=7", (order_id,))
                res_state = order_state_name(ORDER_STATUS_DELIVERED)
            elif action == "CREATE_REFUND":
                if rev is not None:
                    raise CommerceError(409, "REVERSAL_GOVERNED", "orders governed by payment reversal cannot create a new refund intent")
                await cur.execute("SELECT status FROM commerce_delivery_intent WHERE order_sn=%s FOR UPDATE", (order_id,))
                d = await cur.fetchone()
                if d is None or str(d["status"]) != "MANUAL":
                    raise CommerceError(409, "NO_MANUAL_DELIVERY", "CREATE_REFUND requires a MANUAL delivery intent")
                await cur.execute("SELECT channel, transaction_id, fact FROM commerce_payment_receipt WHERE order_sn=%s FOR UPDATE", (order_id,))
                pr = await cur.fetchone()
                if pr is None:
                    raise CommerceError(503, "RECEIPT_MISSING", "bound payment receipt missing for official order")
                if str(pr["fact"]) != FACT_PAID:
                    raise CommerceError(409, "PAYMENT_NOT_PAID", "bound payment is not PAID")
                ch_hex = bytes(pr["channel"]).hex()
                tx_hex = bytes(pr["transaction_id"]).hex()
                await cur.execute("SELECT status FROM commerce_refund_intent WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) FOR UPDATE", (ch_hex, tx_hex))
                if await cur.fetchone() is not None:
                    raise CommerceError(409, "REFUND_EXISTS", "refund intent already exists; resolve it through manual/refunds/resolve")
                await cur.execute(
                    "INSERT INTO commerce_refund_intent(channel,transaction_id,order_sn,source,status) VALUES (UNHEX(%s),UNHEX(%s),%s,%s,'PENDING')",
                    (ch_hex, tx_hex, order_id, REFUND_DELIVERY_REJECTED),
                )
                await cur.execute("UPDATE commerce_delivery_intent SET status='REFUND_PENDING' WHERE order_sn=%s AND status='MANUAL'", (order_id,))
                await cur.execute("UPDATE oms_order SET status=5 WHERE order_sn=%s AND status=7", (order_id,))
                res_state = order_state_name(ORDER_STATUS_REFUND_PENDING)
            else:
                raise CommerceError(400, "UNSUPPORTED_ACTION", "unsupported manual order action")
            return {
                "targetType": "ORDER",
                "targetId": order_id,
                "action": action,
                "resultingState": res_state,
                "reason": cleaned_reason,
                "resolvedAtMs": now,
            }

        return await self._run_admin_cmd_sql(actor, request_id, "manual_order", payload, _mutate)

    async def admin_resolve_refund(
        self,
        actor: str,
        request_id: str,
        order_id: str,
        channel: str,
        payment_transaction_id: str,
        action: str,
        external_refund_id: str | None,
        reason: str,
    ) -> dict[str, Any]:
        if (
            not isinstance(order_id, str) or not UUID_RE.fullmatch(order_id)
            or not isinstance(channel, str) or not CHANNEL_RE.fullmatch(channel)
            or not isinstance(payment_transaction_id, str) or not TXN_ID_RE.fullmatch(payment_transaction_id)
            or action not in MANUAL_REFUND_ACTIONS
        ):
            raise CommerceError(400, "INVALID_MANUAL_REFUND", "invalid manual refund resolution request")
        cleaned_reason = _require_reason(reason)
        if action == "MARK_REFUNDED":
            if not isinstance(external_refund_id, str) or not TXN_ID_RE.fullmatch(external_refund_id):
                raise CommerceError(400, "MISSING_EXTERNAL_REFUND_ID", "MARK_REFUNDED requires a valid externalRefundId")
        elif external_refund_id:
            raise CommerceError(400, "UNEXPECTED_EXTERNAL_REFUND_ID", "externalRefundId is only accepted for MARK_REFUNDED")

        target_id = f"{order_id}:{channel}:{payment_transaction_id}"
        payload = {
            "requestId": request_id,
            "orderId": order_id,
            "channel": channel,
            "paymentTransactionId": payment_transaction_id,
            "action": action,
            "externalRefundId": external_refund_id,
            "reason": reason,
        }
        ch_hex = _hex_ascii(channel)
        ptx_hex = _hex_ascii(payment_transaction_id)

        async def _mutate(cur) -> dict[str, Any]:
            await cur.execute("SELECT status FROM oms_order WHERE order_sn=%s FOR UPDATE", (order_id,))
            ord_row = await cur.fetchone()
            if ord_row is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "order not found")
            order_status = int(ord_row["status"])
            await cur.execute(
                "SELECT order_sn, source, status FROM commerce_refund_intent "
                "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) FOR UPDATE",
                (ch_hex, ptx_hex),
            )
            intent = await cur.fetchone()
            if intent is None or str(intent["order_sn"]) != order_id:
                raise CommerceError(404, "REFUND_NOT_FOUND", "refund intent not found")
            if str(intent["status"]) != "MANUAL":
                raise CommerceError(409, "REFUND_NOT_MANUAL", "refund intent is not in MANUAL state")
            await cur.execute(
                "SELECT order_sn, amount_minor, currency, fact FROM commerce_payment_receipt "
                "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) FOR UPDATE",
                (ch_hex, ptx_hex),
            )
            rec = await cur.fetchone()
            if rec is None:
                raise CommerceError(503, "RECEIPT_MISSING", "payment receipt missing for refund intent")
            own = refunds_orders_own_payment(str(intent["source"]))
            if own != (rec["order_sn"] == order_id):
                raise CommerceError(503, "REFUND_BINDING_MISMATCH", "refund intent source does not match payment binding")
            if own and order_status != ORDER_STATUS_MANUAL:
                raise CommerceError(409, "ORDER_NOT_MANUAL", "linked order is not in MANUAL state")

            now = self._now_ms()
            if action == "RETRY_REFUND":
                if str(rec["fact"]) != FACT_PAID:
                    raise CommerceError(409, "PAYMENT_NOT_PAID", "payment fact is not PAID and cannot be retried for refund")
                await cur.execute(
                    "UPDATE commerce_refund_intent SET status='PENDING', close_reason=NULL "
                    "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) AND status='MANUAL'",
                    (ch_hex, ptx_hex),
                )
                if own:
                    await cur.execute("UPDATE oms_order SET status=5 WHERE order_sn=%s AND status=7", (order_id,))
                res_state = "PENDING"
            elif action == "MARK_REFUNDED":
                assert external_refund_id is not None
                await cur.execute(
                    "SELECT transaction_id FROM commerce_refund_receipt "
                    "WHERE channel=UNHEX(%s) AND payment_transaction_id=UNHEX(%s)",
                    (ch_hex, ptx_hex),
                )
                if await cur.fetchone() is not None:
                    raise CommerceError(409, "RECEIPT_EXISTS", "refund receipt already recorded for this payment")
                await cur.execute(
                    "INSERT INTO commerce_refund_receipt(channel,payment_transaction_id,transaction_id,order_sn,amount_minor,currency) "
                    "VALUES (UNHEX(%s),UNHEX(%s),UNHEX(%s),%s,%s,%s)",
                    (ch_hex, ptx_hex, _hex_ascii(external_refund_id), order_id, int(rec["amount_minor"]), str(rec["currency"])),
                )
                await cur.execute(
                    "UPDATE commerce_refund_intent SET status='REFUNDED', close_reason='MANUAL_REFUNDED' "
                    "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) AND status='MANUAL'",
                    (ch_hex, ptx_hex),
                )
                if own:
                    await cur.execute("UPDATE oms_order SET status=6 WHERE order_sn=%s AND status=7", (order_id,))
                res_state = "REFUNDED"
            elif action == "MARK_REVOKED":
                await cur.execute(
                    "UPDATE commerce_refund_intent SET status='REVOKED', close_reason='MANUAL_REVOKED' "
                    "WHERE channel=UNHEX(%s) AND transaction_id=UNHEX(%s) AND status='MANUAL'",
                    (ch_hex, ptx_hex),
                )
                if own:
                    await cur.execute("UPDATE oms_order SET status=6 WHERE order_sn=%s AND status=7", (order_id,))
                res_state = "REVOKED"
            else:
                raise CommerceError(400, "UNSUPPORTED_ACTION", "unsupported manual refund action")

            return {
                "targetType": "REFUND",
                "targetId": target_id,
                "action": action,
                "resultingState": res_state,
                "reason": cleaned_reason,
                "resolvedAtMs": now,
            }

        return await self._run_admin_cmd_sql(actor, request_id, "manual_refund", payload, _mutate)

    async def admin_resolve_consignment(
        self, actor: str, request_id: str, listing_id: str, action: str, reason: str
    ) -> dict[str, Any]:
        if action not in MANUAL_CONSIGNMENT_ACTIONS:
            raise CommerceError(400, "INVALID_MANUAL_CONSIGNMENT", "invalid manual consignment resolution request")
        validate_uint64_str(listing_id, status=400, code="INVALID_LISTING_ID", message="人民币寄售或结算参数非法")
        cleaned_reason = _require_reason(reason)
        payload = {"requestId": request_id, "listingId": listing_id, "action": action, "reason": reason}

        async def _mutate(cur) -> dict[str, Any]:
            await cur.execute("SELECT current_order_sn FROM commerce_consignment WHERE listing_id=%s", (listing_id,))
            peek = await cur.fetchone()
            if peek is None:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "consignment listing not found")
            observed_order_id = peek["current_order_sn"]
            locked_status: int | None = None
            if observed_order_id is not None:
                await cur.execute("SELECT status, sku_id FROM oms_order WHERE order_sn=%s FOR UPDATE", (observed_order_id,))
                ord_row = await cur.fetchone()
                if ord_row is None or int(ord_row["sku_id"]) != 0:
                    raise CommerceError(503, "CONSIGNMENT_ORDER_BROKEN", "consignment current order record inconsistent")
                locked_status = int(ord_row["status"])
            await cur.execute("SELECT state, current_order_sn FROM commerce_consignment WHERE listing_id=%s FOR UPDATE", (listing_id,))
            cons = await cur.fetchone()
            if cons is None:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "consignment listing not found")
            if cons["current_order_sn"] != observed_order_id:
                raise CommerceError(409, "CONSIGNMENT_CHANGED", "consignment current order changed concurrently, please retry")
            if str(cons["state"]) != "MANUAL":
                raise CommerceError(409, "CONSIGNMENT_NOT_MANUAL", "consignment is not in MANUAL state")
            current_order_id = cons["current_order_sn"]
            now = self._now_ms()

            if action == "RETURN_TO_SELLER":
                if current_order_id is not None:
                    await cur.execute("SELECT order_sn FROM commerce_seller_income WHERE order_sn=%s", (current_order_id,))
                    if await cur.fetchone() is not None:
                        raise CommerceError(409, "INCOME_EXISTS", "asset already delivered and seller income recorded")
                    await cur.execute("SELECT status FROM commerce_delivery_intent WHERE order_sn=%s FOR UPDATE", (current_order_id,))
                    d = await cur.fetchone()
                    if d is not None:
                        if str(d["status"]) == "DELIVERED":
                            raise CommerceError(409, "ALREADY_DELIVERED", "consignment delivery already DELIVERED")
                        if str(d["status"]) in ("PENDING", "MANUAL"):
                            await cur.execute("UPDATE commerce_delivery_intent SET status='REVOKED' WHERE order_sn=%s", (current_order_id,))
                    if locked_status == ORDER_STATUS_MANUAL:
                        await cur.execute("UPDATE oms_order SET status=9 WHERE order_sn=%s AND status=7", (current_order_id,))
                await cur.execute("UPDATE commerce_consignment SET state='RETURNING' WHERE listing_id=%s AND state='MANUAL'", (listing_id,))
                res_state = "RETURNING"
            elif action == "UNFREEZE_INCOME":
                if current_order_id is None:
                    raise CommerceError(409, "NO_BOUND_ORDER", "consignment has no bound order for income unfreeze")
                await cur.execute("SELECT account FROM commerce_seller_income WHERE order_sn=%s", (current_order_id,))
                acc_row = await cur.fetchone()
                if acc_row is None:
                    raise CommerceError(409, "NO_INCOME", "consignment has no seller income record to unfreeze")
                await self._lock_seller_sql(cur, bytes(acc_row["account"]).hex())
                await cur.execute(
                    "SELECT net_fen,reserved_fen,paid_fen,forfeited_fen,available_ms FROM commerce_seller_income WHERE order_sn=%s FOR UPDATE",
                    (current_order_id,),
                )
                inc = await cur.fetchone()
                if inc is None:
                    raise CommerceError(503, "INCOME_MISSING", "seller income record missing under seller lock")
                if int(inc["available_ms"]) != INT64_MAX:
                    raise CommerceError(409, "NOT_FROZEN", "seller income is not frozen")
                if int(inc["net_fen"]) - int(inc["reserved_fen"]) - int(inc["paid_fen"]) - int(inc["forfeited_fen"]) <= 0:
                    raise CommerceError(409, "NO_BALANCE", "seller income has no remaining balance to unfreeze")
                await cur.execute(
                    "UPDATE commerce_seller_income SET available_ms=%s WHERE order_sn=%s AND available_ms=%s",
                    (now, current_order_id, INT64_MAX),
                )
                await cur.execute("UPDATE commerce_consignment SET state='SOLD' WHERE listing_id=%s AND state='MANUAL'", (listing_id,))
                if locked_status == ORDER_STATUS_MANUAL:
                    await cur.execute("UPDATE oms_order SET status=2 WHERE order_sn=%s AND status=7", (current_order_id,))
                res_state = "SOLD"
            elif action == "FORFEIT_INCOME":
                if current_order_id is None:
                    raise CommerceError(409, "NO_BOUND_ORDER", "consignment has no bound order for income forfeiture")
                await cur.execute("SELECT account FROM commerce_seller_income WHERE order_sn=%s", (current_order_id,))
                acc_row = await cur.fetchone()
                if acc_row is None:
                    raise CommerceError(409, "NO_INCOME", "consignment has no seller income record to forfeit")
                await self._lock_seller_sql(cur, bytes(acc_row["account"]).hex())
                await cur.execute(
                    "SELECT net_fen,reserved_fen,paid_fen,forfeited_fen FROM commerce_seller_income WHERE order_sn=%s FOR UPDATE",
                    (current_order_id,),
                )
                inc = await cur.fetchone()
                if inc is None:
                    raise CommerceError(503, "INCOME_MISSING", "seller income record missing under seller lock")
                if int(inc["reserved_fen"]) > 0:
                    raise CommerceError(409, "IN_FLIGHT_WITHDRAWAL", "in-flight withdrawal reserves this income")
                remaining = int(inc["net_fen"]) - int(inc["paid_fen"]) - int(inc["forfeited_fen"])
                if remaining < 0:
                    raise CommerceError(503, "ACCOUNTING_BROKEN", "seller income accounting invariant broken")
                await cur.execute(
                    "UPDATE commerce_seller_income SET forfeited_fen=forfeited_fen+%s, available_ms=%s "
                    "WHERE order_sn=%s AND reserved_fen=0 AND paid_fen+forfeited_fen+%s=net_fen",
                    (remaining, INT64_MAX, current_order_id, remaining),
                )
                await cur.execute("UPDATE commerce_consignment SET state='FORFEITED' WHERE listing_id=%s AND state='MANUAL'", (listing_id,))
                if locked_status == ORDER_STATUS_MANUAL:
                    await cur.execute("UPDATE oms_order SET status=9 WHERE order_sn=%s AND status=7", (current_order_id,))
                res_state = "FORFEITED"
            else:
                raise CommerceError(400, "UNSUPPORTED_ACTION", "unsupported manual consignment action")

            return {
                "targetType": "CONSIGNMENT",
                "targetId": listing_id,
                "action": action,
                "resultingState": res_state,
                "reason": cleaned_reason,
                "resolvedAtMs": now,
            }

        return await self._run_admin_cmd_sql(actor, request_id, "manual_consign", payload, _mutate)

    # -- RMB 寄售全部 14 个操作 --------------------------------------------------------------------------------

    def _row_to_consignment_view(self, row: dict[str, Any]) -> dict[str, Any]:
        state = str(row["state"])
        notice = int(row["notice_end_ms"])
        end = int(row["sale_end_ms"])
        now = self._now_ms()
        if state == "LISTED":
            phase = "PUBLIC_NOTICE" if now < notice else ("ON_SALE" if now < end else "ENDED")
        else:
            phase = state
        return {
            "listingId": str(row["listing_id"]),
            "sellerPlayerId": str(row["seller_player_id"]),
            "marketZone": int(row["market_zone"]),
            "assetKind": str(row["asset_kind"]),
            "title": str(row["title"]),
            "priceFen": int(row["price_fen"]),
            "feeBps": int(row["fee_bps"]),
            "coolingMs": int(row["cooling_ms"]),
            "noticeEndMs": notice,
            "saleEndMs": end,
            "state": state,
            "phase": phase,
            "orderId": row["current_order_sn"],
        }

    async def _lock_seller_sql(self, cur, account_hex: str) -> None:
        await cur.execute(
            "INSERT INTO commerce_seller_guard(account) VALUES (UNHEX(%s)) ON DUPLICATE KEY UPDATE account=account",
            (account_hex,),
        )
        await cur.execute("SELECT account FROM commerce_seller_guard WHERE account=UNHEX(%s) FOR UPDATE", (account_hex,))

    async def _consignment_cancelled_sql(self, cur, order_id: str) -> None:
        await cur.execute("SELECT listing_id FROM commerce_consignment_order WHERE order_sn=%s", (order_id,))
        co = await cur.fetchone()
        if co is None:
            raise CommerceError(503, "CONSIGNMENT_MAPPING_MISSING", "寄售订单关联缺失")
        listing_id = str(co["listing_id"])
        await cur.execute("SELECT state, current_order_sn, sale_end_ms FROM commerce_consignment WHERE listing_id=%s FOR UPDATE", (listing_id,))
        cons = await cur.fetchone()
        if cons is None or str(cons["state"]) != "LOCKED" or str(cons["current_order_sn"]) != order_id:
            raise CommerceError(503, "CONSIGNMENT_STATE_MISMATCH", "寄售当前订单原件与OMS不符")
        nxt = "RETURNING" if self._now_ms() >= int(cons["sale_end_ms"]) else "LISTED"
        await cur.execute(
            "UPDATE commerce_consignment SET state=%s, current_order_sn=NULL WHERE listing_id=%s AND current_order_sn=%s",
            (nxt, listing_id, order_id),
        )

    async def _consignment_paid_sql(self, cur, order_id: str) -> None:
        await cur.execute("SELECT listing_id FROM commerce_consignment_order WHERE order_sn=%s", (order_id,))
        co = await cur.fetchone()
        if co is None:
            raise CommerceError(503, "CONSIGNMENT_MAPPING_MISSING", "寄售订单关联缺失")
        listing_id = str(co["listing_id"])
        await cur.execute("SELECT state, current_order_sn FROM commerce_consignment WHERE listing_id=%s FOR UPDATE", (listing_id,))
        cons = await cur.fetchone()
        if cons is None or str(cons["state"]) != "LOCKED" or str(cons["current_order_sn"]) != order_id:
            raise CommerceError(503, "CONSIGNMENT_STATE_MISMATCH", "寄售当前订单原件与OMS不符")
        await cur.execute(
            "UPDATE commerce_consignment SET state='SOLD' WHERE listing_id=%s AND current_order_sn=%s",
            (listing_id, order_id),
        )

    async def _consignment_delivered_sql(self, cur, order_id: str, outcome: str) -> None:
        await cur.execute(
            "SELECT c.listing_id, c.seller_account, c.fee_bps, c.cooling_ms, o.pay_amount "
            "FROM commerce_consignment_order c JOIN oms_order o ON o.order_sn=c.order_sn WHERE c.order_sn=%s",
            (order_id,),
        )
        co = await cur.fetchone()
        if co is None:
            raise CommerceError(503, "CONSIGNMENT_OWNER_MISSING", "寄售交付原身份缺失")
        listing_id = str(co["listing_id"])
        await cur.execute("SELECT state, current_order_sn FROM commerce_consignment WHERE listing_id=%s FOR UPDATE", (listing_id,))
        cons = await cur.fetchone()
        if cons is None or str(cons["state"]) != "SOLD" or str(cons["current_order_sn"]) != order_id:
            raise CommerceError(503, "CONSIGNMENT_STATE_MISMATCH", "寄售当前订单原件与OMS不符")
        if outcome == "DELIVERED":
            gross = int(co["pay_amount"])
            fee = calc_consignment_fee(gross, int(co["fee_bps"]))
            available_ms = self._now_ms() + int(co["cooling_ms"])
            acc_hex = bytes(co["seller_account"]).hex()
            await self._lock_seller_sql(cur, acc_hex)
            await cur.execute(
                "INSERT INTO commerce_seller_income(order_sn,account,gross_fen,fee_fen,net_fen,available_ms) "
                "VALUES (%s,UNHEX(%s),%s,%s,%s,%s)",
                (order_id, acc_hex, gross, fee, gross - fee, available_ms),
            )
        elif outcome == "REFUND_PENDING":
            await cur.execute("UPDATE commerce_consignment SET state='RETURNING' WHERE listing_id=%s", (listing_id,))
        elif outcome == "MANUAL":
            await cur.execute("UPDATE commerce_consignment SET state='MANUAL' WHERE listing_id=%s", (listing_id,))
        else:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")

    async def consignment_register(
        self,
        listing_id: str,
        seller_player_id: str,
        seller_account: str,
        market_zone: int,
        asset_kind: str,
        asset_currency_type: int | None,
        escrow_reference: str,
        escrow_proof: str,
        price_fen: int,
        title: str,
    ) -> dict[str, Any]:
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_uint64_str(seller_player_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_uint64_str(escrow_reference, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_account(seller_account)
        validate_proof(escrow_proof)
        if (
            not isinstance(market_zone, int) or isinstance(market_zone, bool) or not 1 <= market_zone <= 0xFFFF_FFFF
            or asset_kind not in RMB_ASSET_KINDS
            or not isinstance(price_fen, int) or isinstance(price_fen, bool) or not 1 <= price_fen <= MAX_AMOUNT_MINOR
            or not isinstance(title, str) or not title.strip() or len(title) > 128
        ):
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        if asset_kind == "CURRENCY":
            if asset_currency_type is None or isinstance(asset_currency_type, bool) or asset_currency_type not in self.config.consignment_currencies:
                raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        elif asset_currency_type is not None:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")

        digest = _consignment_digest({
            "listingId": listing_id,
            "sellerPlayerId": seller_player_id,
            "sellerAccount": seller_account,
            "marketZone": market_zone,
            "assetKind": asset_kind,
            "assetCurrencyType": asset_currency_type,
            "escrowReference": escrow_reference,
            "escrowProof": escrow_proof,
            "priceFen": price_fen,
            "title": title,
        })
        now = self._now_ms()
        notice_end = now + self.config.consignment_notice_ms
        sale_end = notice_end + self.config.consignment_sale_ms

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute(
                "INSERT INTO commerce_consignment(listing_id,seller_player_id,seller_account,market_zone,asset_kind,"
                "asset_currency_type,escrow_reference,escrow_proof,request_digest,title,price_fen,fee_bps,cooling_ms,"
                "notice_end_ms,sale_end_ms,state,created_ms) "
                "VALUES (%s,%s,UNHEX(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'LISTED',%s) "
                "ON DUPLICATE KEY UPDATE listing_id=listing_id",
                (
                    listing_id, seller_player_id, _hex_utf8(seller_account), market_zone, asset_kind,
                    asset_currency_type, escrow_reference, escrow_proof, digest, title, price_fen,
                    self.config.consignment_fee_bps, self.config.consignment_cooling_ms, notice_end, sale_end, now,
                ),
            )
            await cur.execute("SELECT * FROM commerce_consignment WHERE listing_id=%s FOR UPDATE", (listing_id,))
            row = await cur.fetchone()
            if row is None or str(row["request_digest"]) != digest:
                raise CommerceError(409, "CONSIGNMENT_CONFLICT", "寄售编号或托管引用已用于不同条款")
            return self._row_to_consignment_view(row)

        try:
            return await self._run_tx(_op)
        except pymysql.MySQLError as exc:
            if _is_dup_key(exc):
                raise CommerceError(409, "ESCROW_REUSED", "托管合同已登记其它寄售") from None
            raise

    async def consignment_list(self, after_listing_id: str, limit: int) -> dict[str, Any]:
        if not isinstance(after_listing_id, str) or not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        if after_listing_id:
            validate_uint64_str(after_listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute(
                "SELECT * FROM commerce_consignment WHERE listing_id>%s AND state IN ('LISTED','LOCKED') "
                "AND sale_end_ms>%s ORDER BY listing_id LIMIT %s",
                (after_listing_id, self._now_ms(), limit),
            )
            return {"listings": [self._row_to_consignment_view(r) for r in await cur.fetchall()]}

        return await self._run_tx(_op)

    async def consignment_get(self, listing_id: str) -> dict[str, Any]:
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute("SELECT * FROM commerce_consignment WHERE listing_id=%s", (listing_id,))
            row = await cur.fetchone()
            if row is None:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "寄售不存在")
            return self._row_to_consignment_view(row)

        return await self._run_tx(_op)

    async def consignment_cancel(self, listing_id: str, seller_account: str) -> dict[str, Any]:
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_account(seller_account)

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute("SELECT * FROM commerce_consignment WHERE listing_id=%s FOR UPDATE", (listing_id,))
            row = await cur.fetchone()
            if row is None or _decode_utf8(row["seller_account"]) != seller_account:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "寄售不存在")
            state = str(row["state"])
            if state in ("RETURNING", "RETURNED"):
                return self._row_to_consignment_view(row)
            if state != "LISTED":
                raise CommerceError(409, "CONSIGNMENT_LOCKED", "交易中的寄售不能下架")
            await cur.execute("UPDATE commerce_consignment SET state='RETURNING' WHERE listing_id=%s AND state='LISTED'", (listing_id,))
            row["state"] = "RETURNING"
            return self._row_to_consignment_view(row)

        return await self._run_tx(_op)

    async def consignment_buy(
        self,
        listing_id: str,
        buyer_player_id: str,
        buyer_account: str,
        request_id: str,
        buyer_owner_revision: str,
    ) -> dict[str, Any]:
        validate_account(buyer_account)
        validate_uint64_str(buyer_player_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_revision(buyer_owner_revision)
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_request_id(request_id)

        acc_hex = _hex_utf8(buyer_account)
        req_hex = _hex_utf8(request_id)

        async def _ensure_guard(_conn, cur) -> None:
            await cur.execute(
                "INSERT INTO commerce_buyer_guard(buyer_id) VALUES (%s) ON DUPLICATE KEY UPDATE buyer_id=buyer_id",
                (buyer_player_id,),
            )

        await self._run_tx(_ensure_guard)

        async def _same_consign(cur, order_id: str) -> dict[str, Any]:
            await cur.execute(
                "SELECT listing_id, buyer_account, buyer_owner_revision FROM commerce_consignment_order WHERE order_sn=%s",
                (order_id,),
            )
            co = await cur.fetchone()
            if (
                co is None
                or str(co["listing_id"]) != listing_id
                or _decode_utf8(co["buyer_account"]) != buyer_account
                or _decode_ascii(co["buyer_owner_revision"]) != buyer_owner_revision
            ):
                raise CommerceError(409, "CONSIGNMENT_REQ_CONFLICT", "请求号已用于不同寄售或账号")
            ord_row = await self._load_order(cur, order_id, lock=False)
            if ord_row is None or str(ord_row["buyer_id"]) != buyer_player_id:
                raise CommerceError(409, "CONSIGNMENT_BUYER_CONFLICT", "购买请求号的角色原件不符")
            return await self._order_view_sql(cur, ord_row)

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute("SELECT buyer_id FROM commerce_buyer_guard WHERE buyer_id=%s FOR UPDATE", (buyer_player_id,))
            await cur.execute(
                "SELECT order_sn FROM commerce_consignment_order WHERE buyer_account=UNHEX(%s) AND request_id=UNHEX(%s)",
                (acc_hex, req_hex),
            )
            prior = await cur.fetchone()
            if prior is not None:
                return await _same_consign(cur, str(prior["order_sn"]))
            if await self._load_order_by_req(cur, buyer_player_id, request_id) is not None:
                raise CommerceError(409, "REQ_USED_OTHER_ORDER", "请求号已用于其它订单")
            await cur.execute("SELECT COUNT(*) AS cnt FROM oms_order WHERE buyer_id=%s AND status=0", (buyer_player_id,))
            if int((await cur.fetchone())["cnt"]) >= self.config.max_unpaid_orders:
                raise CommerceError(429, "TOO_MANY_UNPAID", "未支付订单数量已达上限")

            await cur.execute("SELECT * FROM commerce_consignment WHERE listing_id=%s FOR UPDATE", (listing_id,))
            cons = await cur.fetchone()
            if cons is None:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "寄售不存在")
            if _decode_utf8(cons["seller_account"]) == buyer_account:
                raise CommerceError(409, "SELF_BUY", "同账号不能购买自己的角色或资产")
            now = self._now_ms()
            if str(cons["state"]) != "LISTED" or now < int(cons["notice_end_ms"]) or now >= int(cons["sale_end_ms"]):
                raise CommerceError(409, "CONSIGNMENT_UNAVAILABLE", "寄售仍在公示、已结束或已被占用")

            order_id = str(uuid.uuid4())
            if await cur.execute(
                "UPDATE commerce_consignment SET state='LOCKED', current_order_sn=%s WHERE listing_id=%s AND state='LISTED'",
                (order_id, listing_id),
            ) != 1:
                raise CommerceError(409, "CONSIGNMENT_CHANGED", "寄售状态已变化")
            price_fen = int(cons["price_fen"])
            await cur.execute(
                "INSERT INTO commerce_consignment_order(order_sn,listing_id,buyer_account,buyer_owner_revision,request_id,"
                "seller_account,escrow_reference,escrow_proof,fee_bps,cooling_ms) "
                "VALUES (%s,%s,UNHEX(%s),UNHEX(%s),UNHEX(%s),UNHEX(%s),%s,%s,%s,%s)",
                (
                    order_id, listing_id, acc_hex, _hex_ascii(buyer_owner_revision), req_hex,
                    bytes(cons["seller_account"]).hex(), str(cons["escrow_reference"]), str(cons["escrow_proof"]),
                    int(cons["fee_bps"]), int(cons["cooling_ms"]),
                ),
            )
            await cur.execute(
                "INSERT INTO oms_order(order_sn,buyer_id,request_id,sku_id,quantity,pay_amount,currency,status,"
                "create_time_ms,payment_deadline_ms) VALUES (%s,%s,UNHEX(%s),0,1,%s,'CNY',0,%s,%s)",
                (order_id, buyer_player_id, _hex_ascii(request_id), price_fen, now, now + self.config.order_ttl_ms),
            )
            await cur.execute(
                "INSERT INTO oms_order_item(order_sn,product_id,product_sku_id,product_sku_code,product_quantity,product_price_minor) "
                "VALUES (%s,0,0,%s,1,%s)",
                (order_id, f"consignment-{listing_id}", price_fen),
            )
            created = await self._load_order(cur, order_id, lock=False)
            assert created is not None
            return await self._order_view_sql(cur, created)

        try:
            return await self._run_tx(_op)
        except pymysql.MySQLError as exc:
            if _is_dup_key(exc):
                async def _check(_conn, cur) -> dict[str, Any]:
                    await cur.execute(
                        "SELECT order_sn FROM commerce_consignment_order WHERE buyer_account=UNHEX(%s) AND request_id=UNHEX(%s)",
                        (acc_hex, req_hex),
                    )
                    prior = await cur.fetchone()
                    if prior is not None:
                        return await _same_consign(cur, str(prior["order_sn"]))
                    raise CommerceError(409, "CONSIGNMENT_BUY_CONFLICT", "寄售购买请求冲突，请使用原请求号重试")

                return await self._run_tx(_check)
            raise

    async def _consignment_owned_sql(self, cur, account: str, order_id: str, *, lock: bool) -> dict[str, Any]:
        validate_account(account)
        validate_order_id(order_id)
        order = await self._load_order(cur, order_id, lock=lock)
        await cur.execute("SELECT buyer_account FROM commerce_consignment_order WHERE order_sn=%s", (order_id,))
        co = await cur.fetchone()
        if order is None or co is None or _decode_utf8(co["buyer_account"]) != account:
            raise CommerceError(404, "CONSIGNMENT_ORDER_NOT_FOUND", "寄售订单不存在")
        return order

    async def consignment_order_get(self, account: str, order_id: str) -> dict[str, Any]:
        async def _op(_conn, cur) -> dict[str, Any]:
            row = await self._consignment_owned_sql(cur, account, order_id, lock=False)
            return await self._order_view_sql(cur, row)

        return await self._run_tx(_op)

    async def consignment_order_list(self, account: str, after_order_id: str, limit: int) -> list[dict[str, Any]]:
        if not isinstance(after_order_id, str) or not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_PAGE", "寄售订单分页非法")
        validate_account(account)

        async def _op(_conn, cur) -> list[dict[str, Any]]:
            after_id = 0
            if after_order_id:
                after_row = await self._consignment_owned_sql(cur, account, after_order_id, lock=False)
                after_id = int(after_row["id"])
            await cur.execute(
                "SELECT o.*, COALESCE(pr.revision, 0) AS payment_revision FROM oms_order o "
                "JOIN commerce_consignment_order c ON c.order_sn=o.order_sn "
                "LEFT JOIN commerce_payment_receipt pr ON pr.order_sn=o.order_sn "
                "WHERE c.buyer_account=UNHEX(%s) AND o.id>%s ORDER BY o.id LIMIT %s",
                (_hex_utf8(account), after_id, limit),
            )
            return [await self._order_view_sql(cur, r) for r in await cur.fetchall()]

        return await self._run_tx(_op)

    async def consignment_order_cancel(self, account: str, order_id: str) -> dict[str, Any]:
        async def _op(_conn, cur) -> dict[str, Any]:
            order = await self._consignment_owned_sql(cur, account, order_id, lock=True)
            return await self._cancel_order_row(cur, order)

        return await self._run_tx(_op)

    async def consignment_poll_deliveries(self, limit: int, cursor: str = "") -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        parsed = PendingScanCursor.parse(cursor)

        async def _op(_conn, cur) -> dict[str, Any]:
            nonlocal parsed
            if parsed is None:
                await cur.execute(
                    "SELECT d.created_ms, d.order_sn FROM commerce_delivery_intent d "
                    "JOIN commerce_consignment_order c ON c.order_sn=d.order_sn "
                    "WHERE d.status='PENDING' ORDER BY d.created_ms DESC, d.order_sn DESC LIMIT 1"
                )
                top = await cur.fetchone()
                if top is None:
                    return {"entries": []}
                parsed = PendingScanCursor(0, "", int(top["created_ms"]), str(top["order_sn"]))
            await cur.execute(
                "SELECT d.order_sn, d.buyer_id, d.created_ms, c.buyer_account, c.buyer_owner_revision, "
                "c.listing_id, c.escrow_reference, c.escrow_proof, o.pay_amount "
                "FROM commerce_delivery_intent d "
                "JOIN commerce_consignment_order c ON c.order_sn=d.order_sn "
                "JOIN oms_order o ON o.order_sn=d.order_sn "
                "WHERE d.status='PENDING' "
                "AND (d.created_ms>%s OR (d.created_ms=%s AND d.order_sn>%s)) "
                "AND (d.created_ms<%s OR (d.created_ms=%s AND d.order_sn<=%s)) "
                "ORDER BY d.created_ms, d.order_sn LIMIT %s",
                (parsed.after_ms, parsed.after_ms, parsed.after_id, parsed.upper_ms, parsed.upper_ms, parsed.upper_id, limit + 1),
            )
            rows = await cur.fetchall()
            has_more = len(rows) > limit
            page = rows[:limit]
            entries = []
            for idx, r in enumerate(page):
                acc = _decode_utf8(r["buyer_account"])
                rev = _decode_ascii(r["buyer_owner_revision"])
                validate_account(acc, status=503)
                validate_revision(rev, status=503)
                oid = str(r["order_sn"])
                cur_str = "" if (not has_more and idx == len(page) - 1) else parsed.after(int(r["created_ms"]), oid)
                entries.append({
                    "intent": {
                        "orderId": oid,
                        "buyerPlayerId": str(r["buyer_id"]),
                        "buyerAccount": acc,
                        "buyerOwnerRevision": rev,
                        "listingId": str(r["listing_id"]),
                        "escrowReference": str(r["escrow_reference"]),
                        "escrowProof": str(r["escrow_proof"]),
                        "amountFen": int(r["pay_amount"]),
                    },
                    "cursor": cur_str,
                })
            return {"entries": entries}

        return await self._run_tx(_op)

    async def consignment_poll_returns(self, limit: int, cursor: str = "") -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        after = ""
        upper = ""
        if cursor:
            parts = cursor.split(":")
            if len(parts) != 3 or parts[0] != "r1":
                raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
            validate_uint64_str(parts[1], status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
            validate_uint64_str(parts[2], status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
            after, upper = parts[1], parts[2]
            if after > upper:
                raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")

        async def _op(_conn, cur) -> dict[str, Any]:
            nonlocal upper
            now = self._now_ms()
            if not upper:
                await cur.execute(
                    "SELECT listing_id FROM commerce_consignment "
                    "WHERE state='RETURNING' OR (state='LISTED' AND sale_end_ms<=%s) "
                    "ORDER BY listing_id DESC LIMIT 1",
                    (now,),
                )
                top = await cur.fetchone()
                if top is None:
                    return {"entries": []}
                upper = str(top["listing_id"])
            await cur.execute(
                "SELECT listing_id, seller_player_id, seller_account, escrow_reference, escrow_proof "
                "FROM commerce_consignment WHERE listing_id>%s AND listing_id<=%s "
                "AND (state='RETURNING' OR (state='LISTED' AND sale_end_ms<=%s)) "
                "ORDER BY listing_id LIMIT %s",
                (after, upper, now, limit + 1),
            )
            rows = await cur.fetchall()
            has_more = len(rows) > limit
            page = rows[:limit]
            entries = []
            for idx, r in enumerate(page):
                lid = str(r["listing_id"])
                cur_str = "" if (not has_more and idx == len(page) - 1) else f"r1:{lid}:{upper}"
                entries.append({
                    "intent": {
                        "listingId": lid,
                        "sellerPlayerId": str(r["seller_player_id"]),
                        "sellerAccount": _decode_utf8(r["seller_account"]),
                        "escrowReference": str(r["escrow_reference"]),
                        "escrowProof": str(r["escrow_proof"]),
                    },
                    "cursor": cur_str,
                })
            return {"entries": entries}

        return await self._run_tx(_op)

    async def consignment_return_receipt(
        self, listing_id: str, escrow_reference: str, escrow_proof: str, return_proof: str
    ) -> None:
        validate_uint64_str(listing_id, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_uint64_str(escrow_reference, status=400, code="INVALID_RMB", message="人民币寄售或结算参数非法")
        validate_proof(escrow_proof)
        validate_proof(return_proof)

        async def _op(_conn, cur) -> None:
            await cur.execute("SELECT * FROM commerce_consignment WHERE listing_id=%s FOR UPDATE", (listing_id,))
            cons = await cur.fetchone()
            if cons is None:
                raise CommerceError(404, "CONSIGNMENT_NOT_FOUND", "寄售不存在")
            if str(cons["escrow_reference"]) != escrow_reference or str(cons["escrow_proof"]) != escrow_proof:
                raise CommerceError(409, "ESCROW_MISMATCH", "退回资产原合同不符")
            await cur.execute("SELECT return_proof FROM commerce_consignment_return WHERE listing_id=%s", (listing_id,))
            prior = await cur.fetchone()
            if prior is not None:
                if str(prior["return_proof"]) != return_proof:
                    raise CommerceError(409, "RETURN_CONFLICT", "退回终局原件冲突")
                return
            state = str(cons["state"])
            if state != "RETURNING" and not (state == "LISTED" and self._now_ms() >= int(cons["sale_end_ms"])):
                raise CommerceError(409, "NOT_RETURNING", "寄售不在待退回状态")
            await cur.execute(
                "INSERT INTO commerce_consignment_return(listing_id, return_proof) VALUES (%s, %s)",
                (listing_id, return_proof),
            )
            await cur.execute("UPDATE commerce_consignment SET state='RETURNED' WHERE listing_id=%s", (listing_id,))

        await self._run_tx(_op)

    async def consignment_incomes(self, account: str, after_order_id: str, limit: int) -> dict[str, Any]:
        if not isinstance(after_order_id, str) or not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        validate_account(account)
        if after_order_id:
            validate_order_id(after_order_id)

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute(
                "SELECT * FROM commerce_seller_income WHERE account=UNHEX(%s) AND order_sn>%s "
                "ORDER BY order_sn LIMIT %s",
                (_hex_utf8(account), after_order_id, limit),
            )
            incomes = [
                {
                    "orderId": str(r["order_sn"]),
                    "grossFen": int(r["gross_fen"]),
                    "feeFen": int(r["fee_fen"]),
                    "netFen": int(r["net_fen"]),
                    "reservedFen": int(r["reserved_fen"]),
                    "paidFen": int(r["paid_fen"]),
                    "availableMs": int(r["available_ms"]),
                    "forfeitedFen": int(r["forfeited_fen"]),
                }
                for r in await cur.fetchall()
            ]
            return {"incomes": incomes}

        return await self._run_tx(_op)

    @staticmethod
    def _row_to_withdrawal(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "withdrawalId": str(r["withdrawal_id"]),
            "amountFen": int(r["amount_fen"]),
            "state": str(r["state"]),
            "channel": _decode_opt_ascii(r["channel"]),
            "transactionId": _decode_opt_ascii(r["transaction_id"]),
        }

    async def consignment_withdraw_create(
        self, account: str, request_id: str, amount_fen: int
    ) -> dict[str, Any]:
        if not isinstance(amount_fen, int) or isinstance(amount_fen, bool) or not 1 <= amount_fen <= MAX_AMOUNT_MINOR:
            raise CommerceError(400, "INVALID_RMB", "人民币寄售或结算参数非法")
        validate_account(account)
        validate_request_id(request_id)
        acc_hex = _hex_utf8(account)
        req_hex = _hex_utf8(request_id)

        async def _op(_conn, cur) -> dict[str, Any]:
            await self._lock_seller_sql(cur, acc_hex)
            await cur.execute(
                "SELECT * FROM commerce_withdrawal WHERE account=UNHEX(%s) AND request_id=UNHEX(%s)",
                (acc_hex, req_hex),
            )
            prior = await cur.fetchone()
            if prior is not None:
                if int(prior["amount_fen"]) != amount_fen:
                    raise CommerceError(409, "WITHDRAW_AMOUNT_CONFLICT", "提现请求号已用于不同金额")
                return self._row_to_withdrawal(prior)

            now = self._now_ms()
            await cur.execute(
                "SELECT * FROM commerce_seller_income WHERE account=UNHEX(%s) AND available_ms<=%s "
                "AND net_fen-reserved_fen-paid_fen-forfeited_fen>0 ORDER BY order_sn LIMIT 100 FOR UPDATE",
                (acc_hex, now),
            )
            funds = await cur.fetchall()
            remaining = amount_fen
            for r in funds:
                avail = int(r["net_fen"]) - int(r["reserved_fen"]) - int(r["paid_fen"]) - int(r["forfeited_fen"])
                remaining -= min(remaining, avail)
            if remaining != 0:
                raise CommerceError(409, "INSUFFICIENT_INCOME", "可提现收入不足或本次分配超过100单上限")

            wid = str(uuid.uuid4())
            await cur.execute(
                "INSERT INTO commerce_withdrawal(withdrawal_id,account,request_id,amount_fen,state,created_ms) "
                "VALUES (%s,UNHEX(%s),UNHEX(%s),%s,'REQUESTED',%s)",
                (wid, acc_hex, req_hex, amount_fen, now),
            )
            remaining = amount_fen
            for r in funds:
                avail = int(r["net_fen"]) - int(r["reserved_fen"]) - int(r["paid_fen"]) - int(r["forfeited_fen"])
                take = min(remaining, avail)
                if take == 0:
                    break
                changed = await cur.execute(
                    "UPDATE commerce_seller_income SET reserved_fen=reserved_fen+%s "
                    "WHERE order_sn=%s AND net_fen-reserved_fen-paid_fen-forfeited_fen>=%s",
                    (take, r["order_sn"], take),
                )
                if changed != 1:
                    raise CommerceError(503, "RESERVATION_CONFLICT", "收入预留并发状态异常")
                await cur.execute(
                    "INSERT INTO commerce_withdrawal_allocation(withdrawal_id,order_sn,amount_fen) VALUES (%s,%s,%s)",
                    (wid, r["order_sn"], take),
                )
                remaining -= take

            await cur.execute("SELECT * FROM commerce_withdrawal WHERE withdrawal_id=%s", (wid,))
            return self._row_to_withdrawal(await cur.fetchone())

        return await self._run_tx(_op)

    async def consignment_withdraw_get(self, account: str, withdrawal_id: str) -> dict[str, Any]:
        validate_order_id(withdrawal_id)
        validate_account(account)

        async def _op(_conn, cur) -> dict[str, Any]:
            await cur.execute(
                "SELECT * FROM commerce_withdrawal WHERE withdrawal_id=%s AND account=UNHEX(%s)",
                (withdrawal_id, _hex_utf8(account)),
            )
            row = await cur.fetchone()
            if row is None:
                raise CommerceError(404, "WITHDRAWAL_NOT_FOUND", "提现单不存在")
            return self._row_to_withdrawal(row)

        return await self._run_tx(_op)

    async def _complete_withdrawal_in_cur(
        self, cur, withdrawal_id: str, channel: str, transaction_id: str, amount_fen: int, successful: bool
    ) -> dict[str, Any]:
        await cur.execute("SELECT account FROM commerce_withdrawal WHERE withdrawal_id=%s", (withdrawal_id,))
        owner_row = await cur.fetchone()
        if owner_row is None:
            raise CommerceError(404, "WITHDRAWAL_NOT_FOUND", "提现单不存在")
        owner_hex = bytes(owner_row["account"]).hex()
        await self._lock_seller_sql(cur, owner_hex)
        await cur.execute(
            "SELECT * FROM commerce_withdrawal WHERE withdrawal_id=%s AND account=UNHEX(%s) FOR UPDATE",
            (withdrawal_id, owner_hex),
        )
        cur_w = await cur.fetchone()
        if cur_w is None:
            raise CommerceError(404, "WITHDRAWAL_NOT_FOUND", "提现单不存在")
        if int(cur_w["amount_fen"]) != amount_fen:
            raise CommerceError(409, "WITHDRAW_AMOUNT_MISMATCH", "出款金额不符")
        ch_hex = _hex_ascii(channel)
        tx_hex = _hex_ascii(transaction_id)

        await cur.execute("SELECT * FROM commerce_withdrawal_receipt WHERE withdrawal_id=%s", (withdrawal_id,))
        prior = await cur.fetchone()
        if prior is not None:
            if (
                _decode_ascii(prior["channel"]) != channel
                or _decode_ascii(prior["transaction_id"]) != transaction_id
                or int(prior["successful"]) != (1 if successful else 0)
            ):
                raise CommerceError(409, "WITHDRAW_RECEIPT_CONFLICT", "出款终局回执冲突")
            return self._row_to_withdrawal(cur_w)
        if str(cur_w["state"]) != "REQUESTED":
            raise CommerceError(409, "WITHDRAW_NOT_REQUESTED", "提现单不在待出款状态")

        await cur.execute(
            "SELECT order_sn, amount_fen FROM commerce_withdrawal_allocation WHERE withdrawal_id=%s ORDER BY order_sn",
            (withdrawal_id,),
        )
        allocated = 0
        for alloc in await cur.fetchall():
            amt = int(alloc["amount_fen"])
            allocated += amt
            changed = await cur.execute(
                "UPDATE commerce_seller_income SET reserved_fen=reserved_fen-%s, paid_fen=paid_fen+%s "
                "WHERE order_sn=%s AND account=UNHEX(%s) AND reserved_fen>=%s",
                (amt, amt if successful else 0, alloc["order_sn"], owner_hex, amt),
            )
            if changed != 1:
                raise CommerceError(503, "INCOME_RESERVATION_BROKEN", "收入预留原件不符")
        if allocated != amount_fen:
            raise CommerceError(503, "ALLOCATION_SUM_MISMATCH", "出款分配总额不符")

        await cur.execute(
            "INSERT INTO commerce_withdrawal_receipt(withdrawal_id,channel,transaction_id,amount_fen,successful) "
            "VALUES (%s,UNHEX(%s),UNHEX(%s),%s,%s)",
            (withdrawal_id, ch_hex, tx_hex, amount_fen, 1 if successful else 0),
        )
        new_state = "PAID" if successful else "FAILED"
        await cur.execute(
            "UPDATE commerce_withdrawal SET state=%s, channel=UNHEX(%s), transaction_id=UNHEX(%s) "
            "WHERE withdrawal_id=%s AND state='REQUESTED'",
            (new_state, ch_hex, tx_hex, withdrawal_id),
        )
        await cur.execute("SELECT * FROM commerce_withdrawal WHERE withdrawal_id=%s", (withdrawal_id,))
        return self._row_to_withdrawal(await cur.fetchone())

    # -- Mock Payment & Payout --------------------------------------------------------------------------------

    async def mock_pay(self, order_id: str, request_id: str) -> dict[str, Any]:
        self.config.require_mock_payment()
        validate_order_id(order_id)
        validate_request_id(request_id)

        async def _create_fact(_conn, cur) -> dict[str, Any]:
            await cur.execute("SELECT status, pay_amount, currency FROM oms_order WHERE order_sn=%s FOR UPDATE", (order_id,))
            ord_row = await cur.fetchone()
            if ord_row is None:
                raise CommerceError(404, "ORDER_NOT_FOUND", "原订单不存在")
            await cur.execute("SELECT * FROM commerce_mock_transaction WHERE kind='PAYMENT' AND order_sn=%s FOR UPDATE", (order_id,))
            prior = await cur.fetchone()
            if prior is not None:
                if _decode_ascii(prior["request_id"]) != request_id:
                    raise CommerceError(409, "MOCK_REQ_CONFLICT", "原订单已绑定其他mock请求号")
                return prior
            await cur.execute("SELECT COUNT(*) AS cnt FROM commerce_payment_receipt WHERE order_sn=%s", (order_id,))
            if int(ord_row["status"]) not in (0, 4) or int((await cur.fetchone())["cnt"]) != 0:
                raise CommerceError(409, "ORDER_NOT_PAYABLE", "原订单已有付款或不可付款")
            txn = f"mock-pay-{uuid.uuid4()}"
            await cur.execute(
                "INSERT INTO commerce_mock_transaction(kind,order_sn,request_id,transaction_id,payment_transaction_id,"
                "amount_minor,currency,created_ms,applied) VALUES ('PAYMENT',%s,UNHEX(%s),UNHEX(%s),UNHEX(%s),%s,%s,%s,0)",
                (order_id, _hex_ascii(request_id), _hex_ascii(txn), _hex_ascii(txn), int(ord_row["pay_amount"]), str(ord_row["currency"]), self._now_ms()),
            )
            await cur.execute("SELECT * FROM commerce_mock_transaction WHERE kind='PAYMENT' AND order_sn=%s", (order_id,))
            return await cur.fetchone()

        try:
            fact = await self._run_tx(_create_fact)
        except pymysql.MySQLError as exc:
            if _is_dup_key(exc):
                raise CommerceError(409, "MOCK_REQ_REUSED", "mock请求号或原订单已绑定其他交易") from None
            raise

        txn_id = _decode_ascii(fact["transaction_id"])
        outcome = await self.record_payment(order_id, "mock", txn_id, int(fact["amount_minor"]), str(fact["currency"]))

        async def _mark(_conn, cur) -> None:
            await cur.execute(
                "UPDATE commerce_mock_transaction SET applied=1 WHERE kind='PAYMENT' AND order_sn=%s AND transaction_id=UNHEX(%s)",
                (order_id, _hex_ascii(txn_id)),
            )

        await self._run_tx(_mark)
        return outcome["order"]

    async def mock_reconcile(self) -> tuple[int, dict[str, Any]]:
        self.config.require_mock_payment()
        attempted = 0
        succeeded = 0
        failed = 0

        async def _list_pending_pays(_conn, cur) -> list[dict[str, Any]]:
            await cur.execute(
                "SELECT * FROM commerce_mock_transaction WHERE kind='PAYMENT' AND applied=0 ORDER BY order_sn LIMIT 20"
            )
            return list(await cur.fetchall())

        for fact in await self._run_tx(_list_pending_pays):
            attempted += 1
            try:
                txn_id = _decode_ascii(fact["transaction_id"])
                oid = str(fact["order_sn"])
                await self.record_payment(oid, "mock", txn_id, int(fact["amount_minor"]), str(fact["currency"]))
                async def _mark(_conn, cur) -> None:
                    await cur.execute(
                        "UPDATE commerce_mock_transaction SET applied=1 WHERE kind='PAYMENT' AND order_sn=%s AND transaction_id=UNHEX(%s)",
                        (oid, _hex_ascii(txn_id)),
                    )
                await self._run_tx(_mark)
                succeeded += 1
            except Exception:
                failed += 1

        async def _list_pending_refunds(_conn, cur) -> list[dict[str, Any]]:
            await cur.execute(
                "SELECT m.* FROM commerce_payment_receipt p "
                "JOIN commerce_mock_transaction m ON m.order_sn=p.order_sn AND m.kind='PAYMENT' "
                "AND m.transaction_id=p.transaction_id AND m.amount_minor=p.amount_minor AND m.currency=p.currency "
                "WHERE p.channel=UNHEX(%s) AND ("
                "EXISTS(SELECT 1 FROM commerce_refund_intent r WHERE r.channel=p.channel AND r.transaction_id=p.transaction_id AND r.status='PENDING') "
                "OR EXISTS(SELECT 1 FROM commerce_mock_transaction f WHERE f.kind='REFUND' AND f.order_sn=m.order_sn AND f.applied=0)) "
                "ORDER BY m.order_sn LIMIT 20",
                (_hex_ascii("mock"),),
            )
            return list(await cur.fetchall())

        for pf in await self._run_tx(_list_pending_refunds):
            if attempted >= 40:
                break
            attempted += 1
            oid = str(pf["order_sn"])
            p_txn = _decode_ascii(pf["transaction_id"])
            try:
                async def _ensure_refund_fact(_conn, cur) -> dict[str, Any]:
                    await cur.execute("SELECT * FROM commerce_mock_transaction WHERE kind='REFUND' AND order_sn=%s FOR UPDATE", (oid,))
                    rf = await cur.fetchone()
                    if rf is not None:
                        return rf
                    r_txn = f"mock-refund-{uuid.uuid4()}"
                    r_req = f"refund_{oid.replace('-', '')}"
                    await cur.execute(
                        "INSERT INTO commerce_mock_transaction(kind,order_sn,request_id,transaction_id,payment_transaction_id,"
                        "amount_minor,currency,created_ms,applied) VALUES ('REFUND',%s,UNHEX(%s),UNHEX(%s),UNHEX(%s),%s,%s,%s,0)",
                        (oid, _hex_ascii(r_req), _hex_ascii(r_txn), _hex_ascii(p_txn), int(pf["amount_minor"]), str(pf["currency"]), self._now_ms()),
                    )
                    await cur.execute("SELECT * FROM commerce_mock_transaction WHERE kind='REFUND' AND order_sn=%s", (oid,))
                    return await cur.fetchone()

                rf = await self._run_tx(_ensure_refund_fact)
                r_txn = _decode_ascii(rf["transaction_id"])
                await self.refund_receipt(oid, "mock", p_txn, r_txn, int(rf["amount_minor"]), str(rf["currency"]))
                async def _mark_r(_conn, cur) -> None:
                    await cur.execute(
                        "UPDATE commerce_mock_transaction SET applied=1 WHERE kind='REFUND' AND order_sn=%s AND transaction_id=UNHEX(%s)",
                        (oid, _hex_ascii(r_txn)),
                    )
                await self._run_tx(_mark_r)
                succeeded += 1
            except Exception:
                failed += 1

        body = {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "scanFailures": 0,
            "busy": False,
            "budgetExhausted": False,
        }
        return (503 if failed != 0 else 200), body

    async def mock_withdraw(self, withdrawal_id: str, request_id: str) -> dict[str, Any]:
        self.config.require_mock_payment()
        validate_order_id(withdrawal_id)
        validate_request_id(request_id)

        async def _create_fact(_conn, cur) -> dict[str, Any]:
            await cur.execute("SELECT amount_fen, state FROM commerce_withdrawal WHERE withdrawal_id=%s FOR UPDATE", (withdrawal_id,))
            w = await cur.fetchone()
            if w is None:
                raise CommerceError(404, "WITHDRAWAL_NOT_FOUND", "提现单不存在")
            await cur.execute("SELECT * FROM commerce_mock_payout WHERE withdrawal_id=%s FOR UPDATE", (withdrawal_id,))
            prior = await cur.fetchone()
            if prior is not None:
                if _decode_ascii(prior["request_id"]) != request_id:
                    raise CommerceError(409, "MOCK_PAYOUT_CONFLICT", "同一提现不能重建渠道出款请求")
                return prior
            if str(w["state"]) != "REQUESTED":
                raise CommerceError(409, "WITHDRAW_NOT_REQUESTED", "提现单不在待出款状态")
            txn = f"mock-payout-{uuid.uuid4()}"
            await cur.execute(
                "INSERT INTO commerce_mock_payout(withdrawal_id,request_id,transaction_id,amount_fen,successful,created_ms,applied) "
                "VALUES (%s,UNHEX(%s),UNHEX(%s),%s,1,%s,0)",
                (withdrawal_id, _hex_ascii(request_id), _hex_ascii(txn), int(w["amount_fen"]), self._now_ms()),
            )
            await cur.execute("SELECT * FROM commerce_mock_payout WHERE withdrawal_id=%s", (withdrawal_id,))
            return await cur.fetchone()

        try:
            fact = await self._run_tx(_create_fact)
        except pymysql.MySQLError as exc:
            if _is_dup_key(exc):
                raise CommerceError(409, "MOCK_PAYOUT_REQ_REUSED", "mock出款请求号已用于其它提现") from None
            raise

        txn_id = _decode_ascii(fact["transaction_id"])

        async def _apply(_conn, cur) -> dict[str, Any]:
            res = await self._complete_withdrawal_in_cur(
                cur, withdrawal_id, "mock", txn_id, int(fact["amount_fen"]), int(fact["successful"]) == 1
            )
            await cur.execute(
                "UPDATE commerce_mock_payout SET applied=1 WHERE withdrawal_id=%s AND transaction_id=UNHEX(%s)",
                (withdrawal_id, _hex_ascii(txn_id)),
            )
            return res

        try:
            return await self._run_tx(_apply)
        except pymysql.MySQLError as exc:
            if _is_dup_key(exc):
                raise CommerceError(409, "WITHDRAW_TXN_REUSED", "出款流水已用于其它提现") from None
            raise

    async def mock_withdrawals_reconcile(self) -> tuple[int, dict[str, Any]]:
        self.config.require_mock_payment()
        attempted = 0
        succeeded = 0
        failed = 0

        async def _list(_conn, cur) -> list[dict[str, Any]]:
            await cur.execute("SELECT * FROM commerce_mock_payout WHERE applied=0 ORDER BY withdrawal_id LIMIT 40")
            return list(await cur.fetchall())

        for fact in await self._run_tx(_list):
            attempted += 1
            wid = str(fact["withdrawal_id"])
            txn_id = _decode_ascii(fact["transaction_id"])
            try:
                async def _apply(_conn, cur) -> None:
                    await self._complete_withdrawal_in_cur(
                        cur, wid, "mock", txn_id, int(fact["amount_fen"]), int(fact["successful"]) == 1
                    )
                    await cur.execute(
                        "UPDATE commerce_mock_payout SET applied=1 WHERE withdrawal_id=%s AND transaction_id=UNHEX(%s)",
                        (wid, _hex_ascii(txn_id)),
                    )
                await self._run_tx(_apply)
                succeeded += 1
            except Exception:
                failed += 1

        body = {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "scanFailures": 0,
            "busy": False,
            "budgetExhausted": False,
        }
        return (503 if failed != 0 else 200), body
