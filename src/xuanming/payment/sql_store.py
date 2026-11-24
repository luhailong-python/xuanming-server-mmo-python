"""payment MySQL/TiDB 权威存储实现（SqlPaymentStore）。

与 Go ``go/payment/internal/data/repo.go`` 和 ``spend.go`` 逐字对齐：
- 常规读写均使用单条自动提交 CAS 语句（按自然键幂等，租约领取带旧值比对）；
- 未成年人额度预占与复核（``reserve_intent`` / ``record_minor_payment_txn`` / ``revive_minor_intent``）
  在 ``READ COMMITTED`` 隔离级别下执行行锁（``payment_spend`` + ``FOR UPDATE``）事务。
"""

from __future__ import annotations

import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path

import aiomysql
import pymysql

from .. import portal_sql
from .errors import DailyLimitError, MonthlyLimitError, RecordNotFoundError
from .model import (
    FINALIZE_NONE,
    FINALIZE_PENDING,
    INTENT_CLOSED,
    INTENT_CREATED,
    INTENT_PAID,
    MALL_BOUND,
    MALL_ORPHAN,
    MALL_PENDING,
    MALL_REFUND_QUEUED,
    MallSync,
    PaymentIntentRecord,
    PaymentTxnRecord,
    REFUND_FAILED,
    REFUND_NONE,
    REFUND_REPORTED,
    REFUND_REQUESTED,
    REFUND_SUCCEEDED,
    RecordMinorPaymentOutcome,
    RefundProgress,
    SpendLimit,
)
from .store import admit_minor_spend

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "payment_schema.sql"
DUP_ENTRY = 1062

INTENT_COLUMNS = (
    "`order_hi`,`order_lo`,`order_id`,`player_id`,`channel`,`amount_minor`,`currency`,`state`,"
    "`expire_ms`,`next_check_ms`,`check_count`,`checked_ms`,`created_ms`,`updated_ms`,"
    "`channel_ref`,`channel_ref_ms`,`sku_id`,`payer_id`,`spender_hi`,`spender_lo`,`paid_ms`,`minor_age`"
)

TXN_COLUMNS = (
    "`channel`,`txn_hi`,`txn_lo`,`transaction_id`,`order_hi`,`order_lo`,`out_trade_no`,`player_id`,"
    "`paid_amount_minor`,`paid_currency`,`paid_ms`,"
    "`mall_state`,`mall_refund_reason`,`mall_attempts`,`next_attempt_ms`,`mall_last_error`,"
    "`refund_state`,`out_refund_no`,`refund_id`,`refund_ms`,`refund_attempts`,`refund_next_ms`,"
    "`refund_last_error`,`created_ms`,`updated_ms`,"
    "`fact_revision`,`fact_observed_ms`,`mall_revision`,`mall_due`,`channel_token`,`channel_order_id`,"
    "`store_amount_minor`,`store_currency`,`finalize_state`,`finalize_next_ms`,`product_id`"
)

_INSERT_INTENT_SQL = f"INSERT INTO `payment_intent` ({INTENT_COLUMNS}) VALUES ({','.join(['%s'] * 22)})"
_INSERT_TXN_SQL = f"INSERT INTO `payment_txn` ({TXN_COLUMNS}) VALUES ({','.join(['%s'] * 36)})"

_REQUIRED_INDEXES: tuple[tuple[str, list[str]], ...] = (
    ("payment_intent", ["state", "next_check_ms"]),
    ("payment_intent", ["spender_hi", "spender_lo", "state", "paid_ms"]),
    ("payment_txn", ["mall_due", "next_attempt_ms"]),
    ("payment_txn", ["finalize_state", "finalize_next_ms"]),
)


def _mysql_errno(exc: BaseException) -> int | None:
    if isinstance(exc, pymysql.MySQLError) and exc.args:
        code = exc.args[0]
        if isinstance(code, int):
            return code
    return None


def _is_dup_key(exc: BaseException) -> bool:
    return _mysql_errno(exc) == DUP_ENTRY


def _to_str(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8")
    return str(value)


def _intent_args(rec: PaymentIntentRecord) -> tuple:
    return (
        rec.order_hi,
        rec.order_lo,
        rec.order_id,
        rec.player_id,
        rec.channel,
        rec.amount_minor,
        rec.currency,
        rec.state,
        rec.expire_ms,
        rec.next_check_ms,
        rec.check_count,
        rec.checked_ms,
        rec.created_ms,
        rec.updated_ms,
        rec.channel_ref,
        rec.channel_ref_ms,
        rec.sku_id,
        rec.payer_id,
        rec.spender_hi,
        rec.spender_lo,
        rec.paid_ms,
        rec.minor_age,
    )


def _scan_intent(row) -> PaymentIntentRecord:
    if row is None:
        raise RecordNotFoundError("payment intent not found")
    return PaymentIntentRecord(
        order_hi=int(row[0]),
        order_lo=int(row[1]),
        order_id=_to_str(row[2]),
        player_id=int(row[3]),
        channel=int(row[4]),
        amount_minor=int(row[5]),
        currency=_to_str(row[6]),
        state=int(row[7]),
        expire_ms=int(row[8]),
        next_check_ms=int(row[9]),
        check_count=int(row[10]),
        checked_ms=int(row[11]),
        created_ms=int(row[12]),
        updated_ms=int(row[13]),
        channel_ref=_to_str(row[14]),
        channel_ref_ms=int(row[15]),
        sku_id=int(row[16]),
        payer_id=_to_str(row[17]),
        spender_hi=int(row[18]),
        spender_lo=int(row[19]),
        paid_ms=int(row[20]),
        minor_age=int(row[21]),
    )


def _txn_args(rec: PaymentTxnRecord) -> tuple:
    return (
        rec.channel,
        rec.txn_hi,
        rec.txn_lo,
        rec.transaction_id,
        rec.order_hi,
        rec.order_lo,
        rec.out_trade_no,
        rec.player_id,
        rec.paid_amount_minor,
        rec.paid_currency,
        rec.paid_ms,
        rec.mall_state,
        rec.mall_refund_reason,
        rec.mall_attempts,
        rec.next_attempt_ms,
        rec.mall_last_error,
        rec.refund_state,
        rec.out_refund_no,
        rec.refund_id,
        rec.refund_ms,
        rec.refund_attempts,
        rec.refund_next_ms,
        rec.refund_last_error,
        rec.created_ms,
        rec.updated_ms,
        rec.fact_revision,
        rec.fact_observed_ms,
        rec.mall_revision,
        rec.mall_due,
        rec.channel_token,
        rec.channel_order_id,
        rec.store_amount_minor,
        rec.store_currency,
        rec.finalize_state,
        rec.finalize_next_ms,
        rec.product_id,
    )


def _scan_txn(row) -> PaymentTxnRecord:
    if row is None:
        raise RecordNotFoundError("payment txn not found")
    return PaymentTxnRecord(
        channel=int(row[0]),
        txn_hi=int(row[1]),
        txn_lo=int(row[2]),
        transaction_id=_to_str(row[3]),
        order_hi=int(row[4]),
        order_lo=int(row[5]),
        out_trade_no=_to_str(row[6]),
        player_id=int(row[7]),
        paid_amount_minor=int(row[8]),
        paid_currency=_to_str(row[9]),
        paid_ms=int(row[10]),
        mall_state=int(row[11]),
        mall_refund_reason=_to_str(row[12]),
        mall_attempts=int(row[13]),
        next_attempt_ms=int(row[14]),
        mall_last_error=_to_str(row[15]),
        refund_state=int(row[16]),
        out_refund_no=_to_str(row[17]),
        refund_id=_to_str(row[18]),
        refund_ms=int(row[19]),
        refund_attempts=int(row[20]),
        refund_next_ms=int(row[21]),
        refund_last_error=_to_str(row[22]),
        created_ms=int(row[23]),
        updated_ms=int(row[24]),
        fact_revision=int(row[25]),
        fact_observed_ms=int(row[26]),
        mall_revision=int(row[27]),
        mall_due=int(row[28]),
        channel_token=_to_str(row[29]),
        channel_order_id=_to_str(row[30]),
        store_amount_minor=int(row[31]),
        store_currency=_to_str(row[32]),
        finalize_state=int(row[33]),
        finalize_next_ms=int(row[34]),
        product_id=_to_str(row[35]),
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


class SqlPaymentStore:
    """基于 aiomysql 的支付独占库存储实现。"""

    def __init__(self, pool) -> None:
        self._pool = pool

    # -- 建表与启动验表（fail-closed） -------------------------------------------------------------

    async def bootstrap(self, schema: Path = SCHEMA_FILE) -> None:
        """幂等执行 ``config/payment_schema.sql`` 中的全部 4 张建表语句。"""
        text = "\n".join(line for line in schema.read_text("utf-8").splitlines() if not line.startswith("--"))
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for statement in re.split(r";\s*\n", text):
                sql = statement.strip().rstrip(";")
                if sql:
                    await cursor.execute(sql)

    async def verify_schema(self) -> None:
        """启动前核对全部 4 张表、列与必要二级索引均就位（fail-closed）。"""
        probes = (
            f"SELECT {INTENT_COLUMNS} FROM `payment_intent` LIMIT 0",
            "SELECT `spender_hi`,`spender_lo`,`updated_ms` FROM `payment_spend` LIMIT 0",
            f"SELECT {TXN_COLUMNS} FROM `payment_txn` LIMIT 0",
            "SELECT `channel`,`cursor_ms`,`lease_until_ms`,`lease_owner`,`updated_ms` "
            "FROM `payment_channel_cursor` LIMIT 0",
        )
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for probe in probes:
                await cursor.execute(probe)
            for table, expected_cols in _REQUIRED_INDEXES:
                await cursor.execute(
                    "SELECT index_name, column_name FROM information_schema.statistics "
                    "WHERE table_schema=DATABASE() AND table_name=%s "
                    "ORDER BY index_name, seq_in_index",
                    (table,),
                )
                indexes: dict[str, list[str]] = {}
                for idx_name, col_name in await cursor.fetchall():
                    indexes.setdefault(_to_str(idx_name), []).append(_to_str(col_name))
                if not any(cols[: len(expected_cols)] == expected_cols for cols in indexes.values()):
                    raise RuntimeError(
                        f"payment schema missing required index on {table}({','.join(expected_cols)})"
                    )

    @asynccontextmanager
    async def _spend_tx(self):
        """未成年人额度专用 READ COMMITTED 显式事务上下文。"""
        async with portal_sql.connection(self._pool) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                await conn.begin()
                try:
                    yield conn, cursor
                except BaseException:
                    await conn.rollback()
                    raise

    async def _lock_spender(self, cursor, spender_hi: int, spender_lo: int, now_ms: int) -> None:
        await cursor.execute(
            "INSERT INTO `payment_spend` (`spender_hi`,`spender_lo`,`updated_ms`) "
            "VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE `updated_ms`=%s",
            (spender_hi, spender_lo, now_ms, now_ms),
        )

    async def _check_spend(self, cursor, rec: PaymentIntentRecord, limit: SpendLimit) -> None:
        await cursor.execute(
            "SELECT `state`,`amount_minor`,`paid_ms` FROM `payment_intent` "
            "WHERE `spender_hi`=%s AND `spender_lo`=%s "
            "AND (`state`=%s OR (`state`=%s AND `paid_ms`>=%s)) "
            "AND NOT (`order_hi`=%s AND `order_lo`=%s) FOR UPDATE",
            (
                rec.spender_hi,
                rec.spender_lo,
                INTENT_CREATED,
                INTENT_PAID,
                limit.month_start_ms,
                rec.order_hi,
                rec.order_lo,
            ),
        )
        rows = await cursor.fetchall()
        held = [
            PaymentIntentRecord(state=int(r[0]), amount_minor=int(r[1]), paid_ms=int(r[2]))
            for r in rows
        ]
        admit_minor_spend(held, limit, rec.amount_minor)

    # -- payment_intent ----------------------------------------------------------------------------

    async def intent(self, order_hi: int, order_lo: int) -> PaymentIntentRecord:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {INTENT_COLUMNS} FROM `payment_intent` WHERE `order_hi`=%s AND `order_lo`=%s",
                (order_hi, order_lo),
            )
            return _scan_intent(await cursor.fetchone())

    async def insert_intent(self, rec: PaymentIntentRecord) -> tuple[PaymentIntentRecord, bool]:
        try:
            async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
                await cursor.execute(_INSERT_INTENT_SQL, _intent_args(rec))
            return rec, True
        except Exception as exc:
            if not _is_dup_key(exc):
                raise
        return await self.intent(rec.order_hi, rec.order_lo), False

    async def reserve_intent(
        self, rec: PaymentIntentRecord, limit: SpendLimit
    ) -> tuple[PaymentIntentRecord, bool]:
        try:
            async with self._spend_tx() as (conn, cursor):
                await self._lock_spender(cursor, rec.spender_hi, rec.spender_lo, rec.created_ms)
                await self._check_spend(cursor, rec, limit)
                try:
                    await cursor.execute(_INSERT_INTENT_SQL, _intent_args(rec))
                except Exception as exc:
                    if not _is_dup_key(exc):
                        raise
                    await conn.rollback()
                    return await self.intent(rec.order_hi, rec.order_lo), False
                await conn.commit()
                return rec, True
        except (MonthlyLimitError, DailyLimitError):
            raise

    async def record_minor_payment_txn(
        self,
        rec: PaymentTxnRecord,
        intent: PaymentIntentRecord,
        limit: SpendLimit,
        paid_ms: int,
        now_ms: int,
        over_limit_reason: str,
    ) -> RecordMinorPaymentOutcome:
        async with self._spend_tx() as (conn, cursor):
            await self._lock_spender(cursor, intent.spender_hi, intent.spender_lo, now_ms)
            await cursor.execute(
                "SELECT `state` FROM `payment_intent` WHERE `order_hi`=%s AND `order_lo`=%s FOR UPDATE",
                (intent.order_hi, intent.order_lo),
            )
            state_row = await cursor.fetchone()
            if state_row is None:
                raise RecordNotFoundError("payment intent not found")
            state = int(state_row[0])

            await cursor.execute(
                f"SELECT {TXN_COLUMNS} FROM `payment_txn` "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s FOR UPDATE",
                (rec.channel, rec.txn_hi, rec.txn_lo),
            )
            existing_row = await cursor.fetchone()
            if existing_row is not None:
                await conn.rollback()
                return RecordMinorPaymentOutcome(
                    stored=_scan_txn(existing_row),
                    inserted=False,
                    was_closed=(state == INTENT_CLOSED),
                )

            over_limit = False
            if state == INTENT_CREATED:
                await cursor.execute(
                    "UPDATE `payment_intent` SET `state`=%s,`paid_ms`=%s,`channel_ref`='',`updated_ms`=%s "
                    "WHERE `order_hi`=%s AND `order_lo`=%s AND `state`=%s",
                    (INTENT_PAID, paid_ms, now_ms, intent.order_hi, intent.order_lo, INTENT_CREATED),
                )
            elif state == INTENT_CLOSED:
                try:
                    await self._check_spend(cursor, intent, limit)
                except (MonthlyLimitError, DailyLimitError):
                    over_limit = True
                    rec.mall_state = MALL_ORPHAN
                    rec.mall_due = 0
                    rec.mall_last_error = over_limit_reason
                    rec.finalize_state = FINALIZE_NONE
                    rec.finalize_next_ms = 0
                else:
                    await cursor.execute(
                        "UPDATE `payment_intent` SET `state`=%s,`paid_ms`=%s,`channel_ref`='',`updated_ms`=%s "
                        "WHERE `order_hi`=%s AND `order_lo`=%s AND `state`=%s",
                        (INTENT_PAID, paid_ms, now_ms, intent.order_hi, intent.order_lo, INTENT_CLOSED),
                    )
            await cursor.execute(_INSERT_TXN_SQL, _txn_args(rec))
            await conn.commit()
            return RecordMinorPaymentOutcome(
                stored=rec,
                inserted=True,
                was_closed=(state == INTENT_CLOSED),
                over_limit=over_limit,
            )

    async def revive_minor_intent(
        self,
        intent: PaymentIntentRecord,
        limit: SpendLimit,
        paid_ms: int,
        now_ms: int,
        *,
        force: bool,
    ) -> bool:
        async with self._spend_tx() as (conn, cursor):
            await self._lock_spender(cursor, intent.spender_hi, intent.spender_lo, now_ms)
            await cursor.execute(
                "SELECT `state` FROM `payment_intent` WHERE `order_hi`=%s AND `order_lo`=%s FOR UPDATE",
                (intent.order_hi, intent.order_lo),
            )
            state_row = await cursor.fetchone()
            if state_row is None:
                raise RecordNotFoundError("payment intent not found")
            if int(state_row[0]) != INTENT_CLOSED:
                await conn.rollback()
                return True
            within = True
            try:
                await self._check_spend(cursor, intent, limit)
            except (MonthlyLimitError, DailyLimitError):
                within = False
            if not within and not force:
                await conn.rollback()
                return False
            await cursor.execute(
                "UPDATE `payment_intent` SET `state`=%s,`paid_ms`=%s,`channel_ref`='',`updated_ms`=%s "
                "WHERE `order_hi`=%s AND `order_lo`=%s AND `state`=%s",
                (INTENT_PAID, paid_ms, now_ms, intent.order_hi, intent.order_lo, INTENT_CLOSED),
            )
            await conn.commit()
            return within

    async def set_channel_ref(
        self, order_hi: int, order_lo: int, ref: str, ref_ms: int, payer_id: str, now_ms: int
    ) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_intent` SET `channel_ref`=%s,`channel_ref_ms`=%s,`payer_id`=%s,`updated_ms`=%s "
                "WHERE `order_hi`=%s AND `order_lo`=%s AND `state`=%s",
                (ref, ref_ms, payer_id, now_ms, order_hi, order_lo, INTENT_CREATED),
            )

    async def claim_active_query(self, rec: PaymentIntentRecord, now_ms: int) -> bool:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_intent` SET `checked_ms`=%s,`updated_ms`=%s "
                "WHERE `order_hi`=%s AND `order_lo`=%s AND `state`=%s AND `checked_ms`=%s",
                (now_ms, now_ms, rec.order_hi, rec.order_lo, INTENT_CREATED, rec.checked_ms),
            )
            return cursor.rowcount == 1

    async def due_intents(self, now_ms: int, limit: int) -> list[PaymentIntentRecord]:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {INTENT_COLUMNS} FROM `payment_intent` "
                "WHERE `state`=%s AND `next_check_ms`<=%s ORDER BY `next_check_ms` LIMIT %s",
                (INTENT_CREATED, now_ms, limit),
            )
            return [_scan_intent(row) for row in await cursor.fetchall()]

    async def claim_check(self, rec: PaymentIntentRecord, lease_ms: int, now_ms: int) -> bool:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_intent` SET `next_check_ms`=%s,`check_count`=`check_count`+1,"
                "`checked_ms`=%s,`updated_ms`=%s "
                "WHERE `order_hi`=%s AND `order_lo`=%s AND `state`=%s AND `next_check_ms`=%s",
                (lease_ms, now_ms, now_ms, rec.order_hi, rec.order_lo, INTENT_CREATED, rec.next_check_ms),
            )
            return cursor.rowcount == 1

    async def schedule_check(
        self, rec: PaymentIntentRecord, lease_ms: int, next_ms: int, now_ms: int
    ) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_intent` SET `next_check_ms`=%s,`updated_ms`=%s "
                "WHERE `order_hi`=%s AND `order_lo`=%s AND `state`=%s AND `next_check_ms`=%s",
                (next_ms, now_ms, rec.order_hi, rec.order_lo, INTENT_CREATED, lease_ms),
            )

    async def close_intent(self, order_hi: int, order_lo: int, now_ms: int) -> bool:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_intent` SET `state`=%s,`channel_ref`='',`updated_ms`=%s "
                "WHERE `order_hi`=%s AND `order_lo`=%s AND `state`=%s",
                (INTENT_CLOSED, now_ms, order_hi, order_lo, INTENT_CREATED),
            )
            return cursor.rowcount == 1

    async def mark_intent_paid(self, order_hi: int, order_lo: int, now_ms: int) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_intent` SET `state`=%s,`paid_ms`=GREATEST(%s,`created_ms`),"
                "`channel_ref`='',`updated_ms`=%s WHERE `order_hi`=%s AND `order_lo`=%s AND `state`<>%s",
                (INTENT_PAID, now_ms, now_ms, order_hi, order_lo, INTENT_PAID),
            )

    async def set_intent_state(self, order_hi: int, order_lo: int, state: int, now_ms: int) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_intent` SET "
                "`paid_ms`=CASE WHEN %s=%s THEN GREATEST(%s,`created_ms`) ELSE `paid_ms` END,"
                "`state`=%s,`channel_ref`='',`updated_ms`=%s WHERE `order_hi`=%s AND `order_lo`=%s",
                (state, INTENT_PAID, now_ms, state, now_ms, order_hi, order_lo),
            )

    # -- payment_txn -------------------------------------------------------------------------------

    async def txn(self, channel: int, txn_hi: int, txn_lo: int) -> PaymentTxnRecord:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {TXN_COLUMNS} FROM `payment_txn` "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s",
                (channel, txn_hi, txn_lo),
            )
            return _scan_txn(await cursor.fetchone())

    async def insert_txn(self, rec: PaymentTxnRecord) -> tuple[PaymentTxnRecord, bool]:
        try:
            async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
                await cursor.execute(_INSERT_TXN_SQL, _txn_args(rec))
            return rec, True
        except Exception as exc:
            if not _is_dup_key(exc):
                raise
        return await self.txn(rec.channel, rec.txn_hi, rec.txn_lo), False

    async def flip_fact(self, rec: PaymentTxnRecord, observed_ms: int, now_ms: int) -> bool:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_txn` SET "
                "`next_attempt_ms`=CASE WHEN `mall_due`=1 THEN `next_attempt_ms` ELSE %s END,"
                "`mall_due`=CASE WHEN `mall_state` IN (%s,%s,%s) THEN 1 ELSE `mall_due` END,"
                "`fact_revision`=`fact_revision`+1,`fact_observed_ms`=%s,`updated_ms`=%s "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s AND `fact_revision`=%s",
                (
                    now_ms,
                    MALL_PENDING,
                    MALL_BOUND,
                    MALL_REFUND_QUEUED,
                    observed_ms,
                    now_ms,
                    rec.channel,
                    rec.txn_hi,
                    rec.txn_lo,
                    rec.fact_revision,
                ),
            )
            return cursor.rowcount == 1

    async def touch_fact_observed(self, rec: PaymentTxnRecord, observed_ms: int, now_ms: int) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_txn` SET `fact_observed_ms`=%s,`updated_ms`=%s "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s "
                "AND `fact_revision`=%s AND `fact_observed_ms`<%s",
                (observed_ms, now_ms, rec.channel, rec.txn_hi, rec.txn_lo, rec.fact_revision, observed_ms),
            )

    async def oldest_mall_pending_ms(self) -> int:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute("SELECT COALESCE(MIN(`created_ms`), 0) FROM `payment_txn` WHERE `mall_due`=1")
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

    async def oldest_finalize_pending_ms(self) -> int:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "SELECT COALESCE(MIN(`created_ms`), 0) FROM `payment_txn` WHERE `finalize_state`=%s",
                (FINALIZE_PENDING,),
            )
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

    async def due_mall_txns(self, now_ms: int, limit: int) -> list[PaymentTxnRecord]:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {TXN_COLUMNS} FROM `payment_txn` "
                "WHERE `mall_due`=1 AND `next_attempt_ms`<=%s ORDER BY `next_attempt_ms` LIMIT %s",
                (now_ms, limit),
            )
            return [_scan_txn(row) for row in await cursor.fetchall()]

    async def claim_mall(self, rec: PaymentTxnRecord, lease_ms: int, now_ms: int) -> bool:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_txn` SET `next_attempt_ms`=%s,`mall_attempts`=`mall_attempts`+1,`updated_ms`=%s "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s AND `mall_due`=1 AND `next_attempt_ms`=%s",
                (lease_ms, now_ms, rec.channel, rec.txn_hi, rec.txn_lo, rec.next_attempt_ms),
            )
            return cursor.rowcount == 1

    async def settle_mall(
        self, rec: PaymentTxnRecord, lease_ms: int, sync: MallSync, now_ms: int
    ) -> None:
        keep = 1 if sync.keep_syncing else 0
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_txn` SET "
                "`mall_due`=CASE WHEN %s=1 AND `fact_revision`>%s THEN 1 ELSE 0 END,"
                "`mall_state`=%s,`mall_refund_reason`=%s,`mall_revision`=%s,"
                "`mall_last_error`=%s,`next_attempt_ms`=%s,`updated_ms`=%s "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s AND `mall_due`=1 AND `next_attempt_ms`=%s",
                (
                    keep,
                    sync.revision,
                    sync.state,
                    sync.refund_reason,
                    sync.revision,
                    sync.last_error,
                    now_ms,
                    now_ms,
                    rec.channel,
                    rec.txn_hi,
                    rec.txn_lo,
                    lease_ms,
                ),
            )

    async def defer_mall(
        self, rec: PaymentTxnRecord, lease_ms: int, next_ms: int, last_err: str, now_ms: int
    ) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_txn` SET `next_attempt_ms`=%s,`mall_last_error`=%s,`updated_ms`=%s "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s AND `mall_due`=1 AND `next_attempt_ms`=%s",
                (next_ms, last_err, now_ms, rec.channel, rec.txn_hi, rec.txn_lo, lease_ms),
            )

    async def claim_refund(
        self, rec: PaymentTxnRecord, out_refund_no: str, lease_ms: int, now_ms: int
    ) -> bool:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_txn` SET "
                "`refund_state`=CASE WHEN `refund_state`=%s THEN %s ELSE `refund_state` END,"
                "`out_refund_no`=CASE WHEN `out_refund_no`='' THEN %s ELSE `out_refund_no` END,"
                "`refund_next_ms`=%s,`refund_attempts`=`refund_attempts`+1,`updated_ms`=%s "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s "
                "AND `refund_state` IN (%s,%s,%s,%s,%s) AND `refund_next_ms`=%s",
                (
                    REFUND_NONE,
                    REFUND_REQUESTED,
                    out_refund_no,
                    lease_ms,
                    now_ms,
                    rec.channel,
                    rec.txn_hi,
                    rec.txn_lo,
                    REFUND_NONE,
                    REFUND_REQUESTED,
                    REFUND_SUCCEEDED,
                    REFUND_REPORTED,
                    REFUND_FAILED,
                    rec.refund_next_ms,
                ),
            )
            return cursor.rowcount == 1

    async def save_refund(
        self, rec: PaymentTxnRecord, lease_ms: int, progress: RefundProgress, now_ms: int
    ) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_txn` SET `refund_state`=%s,`out_refund_no`=%s,`refund_id`=%s,"
                "`refund_ms`=%s,`refund_next_ms`=%s,`refund_last_error`=%s,`updated_ms`=%s "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s AND `refund_next_ms`=%s",
                (
                    progress.state,
                    progress.out_refund_no,
                    progress.refund_id,
                    progress.refund_ms,
                    progress.next_ms,
                    progress.last_error,
                    now_ms,
                    rec.channel,
                    rec.txn_hi,
                    rec.txn_lo,
                    lease_ms,
                ),
            )

    async def due_finalize(self, now_ms: int, limit: int) -> list[PaymentTxnRecord]:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {TXN_COLUMNS} FROM `payment_txn` "
                "WHERE `finalize_state`=%s AND `finalize_next_ms`<=%s ORDER BY `finalize_next_ms` LIMIT %s",
                (FINALIZE_PENDING, now_ms, limit),
            )
            return [_scan_txn(row) for row in await cursor.fetchall()]

    async def claim_finalize(self, rec: PaymentTxnRecord, lease_ms: int, now_ms: int) -> bool:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_txn` SET `finalize_next_ms`=%s,`updated_ms`=%s "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s "
                "AND `finalize_state`=%s AND `finalize_next_ms`=%s",
                (lease_ms, now_ms, rec.channel, rec.txn_hi, rec.txn_lo, FINALIZE_PENDING, rec.finalize_next_ms),
            )
            return cursor.rowcount == 1

    async def save_finalize(
        self, rec: PaymentTxnRecord, lease_ms: int, state: int, next_ms: int, now_ms: int
    ) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_txn` SET `finalize_state`=%s,`finalize_next_ms`=%s,`updated_ms`=%s "
                "WHERE `channel`=%s AND `txn_hi`=%s AND `txn_lo`=%s AND `finalize_next_ms`=%s",
                (state, next_ms, now_ms, rec.channel, rec.txn_hi, rec.txn_lo, lease_ms),
            )

    # -- payment_channel_cursor --------------------------------------------------------------------

    async def claim_cursor(
        self, channel: int, owner: int, initial_ms: int, lease_ms: int, now_ms: int
    ) -> tuple[int, bool]:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "INSERT INTO `payment_channel_cursor` "
                    "(`channel`,`cursor_ms`,`lease_until_ms`,`lease_owner`,`updated_ms`) "
                    "VALUES (%s,%s,0,0,%s)",
                    (channel, initial_ms, now_ms),
                )
            except Exception as exc:
                if not _is_dup_key(exc):
                    raise
            await cursor.execute(
                "UPDATE `payment_channel_cursor` SET `lease_until_ms`=%s,`lease_owner`=%s,`updated_ms`=%s "
                "WHERE `channel`=%s AND `lease_until_ms`<%s",
                (lease_ms, owner, now_ms, channel, now_ms),
            )
            if cursor.rowcount != 1:
                return 0, False
            await cursor.execute(
                "SELECT `cursor_ms` FROM `payment_channel_cursor` WHERE `channel`=%s AND `lease_owner`=%s",
                (channel, owner),
            )
            row = await cursor.fetchone()
            if row is None:
                return 0, False
            return int(row[0]), True

    async def advance_cursor(self, channel: int, owner: int, cursor_ms: int, now_ms: int) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `payment_channel_cursor` SET "
                "`cursor_ms`=CASE WHEN `cursor_ms`<%s THEN %s ELSE `cursor_ms` END,"
                "`lease_until_ms`=0,`updated_ms`=%s WHERE `channel`=%s AND `lease_owner`=%s",
                (cursor_ms, cursor_ms, now_ms, channel, owner),
            )
