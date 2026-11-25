"""trade MySQL/TiDB 存储实现（SqlTradeStore）。

全部写事务运行在 ``READ COMMITTED`` 隔离级别下（对 1205/1213/9007 瞬态冲突做有界重试）；
全部二进制列通过 ``UNHEX(%s)`` 绑定，防止 utf8mb4 连接损坏原始字节；
严禁删除 ``trade_player_op_seq`` 行（保证两段锁序与流纪元单调性）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import TypeVar

import aiomysql
import pymysql
from google.protobuf.message import Message

from .. import portal_sql
from ..protocol import Protocol
from .economy_credit import (
    MAX_DAILY_PLAYER_TRADE_SCAN_ROWS,
    MAX_REFERENCE_SCAN_ROWS,
    MarketFillSample,
    SellerCreditKey,
    SellerCreditProfile,
    SellerReviewRecord,
    SellerViolationRecord,
    saturating_add_uint64,
    workflow_currency_outflow,
)
from .errors import (
    AssetOpLeaseLostError,
    AssetOpPoisonRowError,
    ListingNotFoundError,
    StoreUnavailableError,
    TradeError,
    TradeTooManyPendingError,
)
from .model import (
    ASSET_OP_MAX_PENDING,
    ASSET_OP_MAX_SPAN,
    ASSET_STREAM_TRADE_DEBIT,
    COMMERCE_ESCROWING,
    COMMERCE_KIND_MARKET_MATCH,
    COMMERCE_MANUAL,
    COMMERCE_SETTLED,
    DUE_STATUSES,
    FRESH_ATTEMPT_LIMIT,
    UINT64_MAX,
    LISTING_SORT_DEFAULT,
    LISTING_SORT_LEVEL_DESC,
    LISTING_SORT_PRICE_ASC,
    LISTING_SORT_PRICE_DESC,
    LISTING_SORT_REMAINING_ASC,
    LISTING_STATUS_LISTED,
    LISTING_STATUS_LOCKED,
    LISTING_TAB_ON_SALE,
    LISTING_TAB_PUBLIC_NOTICE,
    MAX_ROLE_PENDING_COMMERCE,
    OP_FINALIZABLE_STATUSES,
    OP_REF_KIND_ORDER,
    OP_STATUS_APPLIED,
    OP_STATUS_PENDING,
    OP_TERMINAL_STATUSES,
    ROLE_BLOCKER_ASSET_OP,
    ROLE_BLOCKER_COMMERCE,
    ROLE_BLOCKER_TOO_MANY_DEALS,
    CommerceCursor,
    LegOutcome,
    ListingQuery,
    PendingAssetOp,
    RoleFenceRow,
    on_book,
)
from .store import (
    RoleFenceDirectory,
    TradeStoreBase,
    TradeTx,
    _DuplicateKeyError,
    _TxRollbackReturn,
    _decode_workflow,
    _is_role_pending_commerce,
    _op_from_record,
)

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "trade_schema.sql"
REQUIRED_TABLES = (
    "trade_listing",
    "trade_favorite",
    "trade_player_op_seq",
    "trade_asset_op",
    "trade_commerce",
    "trade_commerce_guard",
    "role_fence",
)
SELLER_CREDIT_TABLES = (
    "trade_seller_credit",
    "trade_seller_completed_order",
    "trade_seller_review",
    "trade_seller_violation",
)
SELLER_CREDIT_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS `trade_seller_credit` (
  `seller_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `owner_account` MEDIUMTEXT COMMENT 'pb:3',
  `ownership_changed` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `ownership_changed_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `epoch_started_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `completed_orders` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `positive_reviews` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `neutral_reviews` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `negative_reviews` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `violation_count` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `env_credit_cleared` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `env_reset_count` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `last_env_changed_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `last_ip` MEDIUMTEXT COMMENT 'pb:15',
  `last_device_id` MEDIUMTEXT COMMENT 'pb:16',
  `known_devices_json` MEDIUMTEXT COMMENT 'pb:17',
  `credit_score` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `star_rating_tenths` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `positive_rate_bps` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `risk_tier` MEDIUMTEXT COMMENT 'pb:21',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:22',
  PRIMARY KEY (`seller_player_id`,`owner_revision`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='trade_seller_credit';

CREATE TABLE IF NOT EXISTS `trade_seller_completed_order` (
  `record_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `order_id` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:2',
  `seller_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`record_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_trade_seller_completed_order` (`order_id`),
  INDEX `idx_trade_seller_completed_order_0` (`seller_player_id`,`owner_revision`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='trade_seller_completed_order';

CREATE TABLE IF NOT EXISTS `trade_seller_review` (
  `review_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `order_id` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:2',
  `seller_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `seller_owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `buyer_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `buyer_account` MEDIUMTEXT COMMENT 'pb:6',
  `buyer_ip` MEDIUMTEXT COMMENT 'pb:7',
  `buyer_device_id` MEDIUMTEXT COMMENT 'pb:8',
  `rating` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `counted_for_score` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `comment` MEDIUMTEXT COMMENT 'pb:11',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  PRIMARY KEY (`review_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_trade_seller_review` (`order_id`),
  INDEX `idx_trade_seller_review_0` (`seller_player_id`,`seller_owner_revision`,`created_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='trade_seller_review';

CREATE TABLE IF NOT EXISTS `trade_seller_violation` (
  `violation_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `case_id` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:2',
  `order_id` MEDIUMTEXT COMMENT 'pb:3',
  `seller_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `seller_owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `seller_account` MEDIUMTEXT COMMENT 'pb:6',
  `reason` MEDIUMTEXT COMMENT 'pb:7',
  `recorded_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  PRIMARY KEY (`violation_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_trade_seller_violation` (`case_id`),
  INDEX `idx_trade_seller_violation_0` (`seller_player_id`,`seller_owner_revision`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='trade_seller_violation';
"""
_SELLER_CREDIT_COLUMNS = (
    "`seller_player_id`,`owner_revision`,`owner_account`,`ownership_changed`,`ownership_changed_at_ms`,"
    "`epoch_started_at_ms`,`completed_orders`,`positive_reviews`,`neutral_reviews`,`negative_reviews`,`violation_count`,"
    "`env_credit_cleared`,`env_reset_count`,`last_env_changed_at_ms`,`last_ip`,`last_device_id`,`known_devices_json`,"
    "`credit_score`,`star_rating_tenths`,`positive_rate_bps`,`risk_tier`,`updated_ms`"
)
DB_NOW_MS = "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS UNSIGNED)"
LOCK_ERRORS = (1205, 1213, 9007)
DUP_ENTRY = 1062
TX_MAX_ATTEMPTS = 3

_DUE_STATUSES_SQL = ",".join(str(s) for s in DUE_STATUSES)

_LISTING_SUMMARY_COLUMNS = (
    "`listing_id`,`seller_player_id`,`seller_account`,`market_zone`,`seller_zone_at_listing`,"
    "`category`,`subcategory`,`title`,`level`,`price_fen`,`status`,`summary`,`icon_key`,"
    "`notice_end_ms`,`sale_end_ms`,`created_ms`,`updated_ms`,`version`"
)
_LISTING_DETAIL_COLUMNS = _LISTING_SUMMARY_COLUMNS + ",`description`"

_COMMERCE_COLUMNS = (
    "`commerce_id`,`request_key`,`request_digest`,`kind`,`status`,`seller_player_id`,`buyer_player_id`,"
    "`market_id`,`item_config_id`,`side`,`unit_price`,`quantity`,`filled_quantity`,`currency_type`,"
    "`expires_ms`,`created_ms`,`updated_ms`,`version`,`workflow_proto`,`parent_id`,`counterparty_id`,`on_book`"
)

_ASSET_OP_COLUMNS = (
    "`op_id`,`player_id`,`stream`,`stream_epoch`,`seq`,`kind`,`status`,`durable`,"
    "`attempts`,`next_attempt_ms`,`deadline_ms`,`lease_until_ms`,`lease_token`,`tx_type`,"
    "`ref_kind`,`ref_id`,`payload`,`last_outcome`,`last_reason`,`created_ms`,`updated_ms`,"
    "`resolved_by`,`resolve_reason`,`last_send_ms`,`escrow_snapshot`,`receipt_ack_pending`,`business_consumed`"
)

_APPLY_ROLE_FENCE_SQL = (
    "INSERT INTO `role_fence`(player_id,account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms) "
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

_INSERT_FAVORITE_SQL = (
    "INSERT INTO `trade_favorite`(`player_id`,`listing_id`,`created_ms`) VALUES (%s,%s,%s) "
    "ON DUPLICATE KEY UPDATE `created_ms`=`created_ms`"
)
_DELETE_FAVORITE_SQL = "DELETE FROM `trade_favorite` WHERE `player_id`=%s AND `listing_id`=%s"

_SQL_LOCK_ASSET_OP = "SELECT `op_id` FROM `trade_asset_op` WHERE `op_id`=%s FOR UPDATE"
_SQL_CLAIM_ASSET_OP = (
    "UPDATE `trade_asset_op` SET "
    "`last_send_ms`=GREATEST(IF(`last_send_ms`=0,GREATEST(`created_ms`,`updated_ms`),`last_send_ms`),`lease_until_ms`),"
    "`lease_until_ms`=%s,`lease_token`=%s,`updated_ms`=%s "
    "WHERE `op_id`=%s AND `status`=%s AND `lease_until_ms`<%s"
)
_SQL_RESCHEDULE_ASSET_OP = (
    "UPDATE `trade_asset_op` SET "
    "`attempts`=`attempts`+1,`next_attempt_ms`=%s,`lease_until_ms`=0,"
    "`durable`=%s,`last_outcome`=%s,`last_reason`=%s,`updated_ms`=%s,"
    "`last_send_ms`=IF(%s,GREATEST(`last_send_ms`,%s),`last_send_ms`) "
    "WHERE `op_id`=%s AND `status`=%s AND `lease_token`=%s"
)
_SQL_POISON_ASSET_OP = (
    "UPDATE `trade_asset_op` SET `last_outcome`=0,`next_attempt_ms`=%s,`lease_until_ms`=0,`updated_ms`=%s "
    "WHERE `op_id`=%s AND `lease_token`=%s AND `status`=%s"
)
_SQL_FINALIZE_ASSET_OP = (
    "UPDATE `trade_asset_op` SET `status`=%s,`durable`=1,`last_outcome`=%s,`last_reason`=%s,"
    "`lease_until_ms`=0,`next_attempt_ms`=%s,`updated_ms`=%s "
    "WHERE `op_id`=%s AND `status`=%s"
)
_SQL_WAKE_COMMERCE = (
    f"UPDATE `trade_commerce` SET `updated_ms`=%s WHERE `commerce_id`=%s AND `updated_ms`>%s "
    f"AND `status` IN ({_DUE_STATUSES_SQL})"
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


def _mysql_errno(exc: BaseException) -> int | None:
    if isinstance(exc, pymysql.MySQLError) and exc.args:
        code = exc.args[0]
        return code if isinstance(code, int) else None
    return None


def _scan_role_fence(player_id: int, row) -> RoleFenceRow:
    return RoleFenceRow(
        player_id=player_id,
        account=_to_str(row[0]),
        owner_revision=int(row[1]),
        freeze_token=int(row[2]),
        freeze_final=bool(row[3]),
        version=int(row[4]),
        home_zone=int(row[5]),
        updated_ms=int(row[6]),
    )


def _scan_listing(protocol: Protocol, row, *, with_description: bool) -> Message:
    rec = protocol.message(
        "trade.TradeListingRecord",
        listing_id=int(row[0]),
        seller_player_id=int(row[1]),
        seller_account=_to_str(row[2]),
        market_zone=int(row[3]),
        seller_zone_at_listing=int(row[4]),
        category=int(row[5]),
        subcategory=int(row[6]),
        title=_to_str(row[7]),
        level=int(row[8]),
        price_fen=int(row[9]),
        status=int(row[10]),
        summary=_to_str(row[11]),
        icon_key=_to_str(row[12]),
        notice_end_ms=int(row[13]),
        sale_end_ms=int(row[14]),
        created_ms=int(row[15]),
        updated_ms=int(row[16]),
        version=int(row[17]),
    )
    if with_description:
        rec.description = _to_str(row[18])
    return rec


def _scan_commerce(protocol: Protocol, row) -> Message:
    return protocol.message(
        "trade.TradeCommerceRecord",
        commerce_id=int(row[0]),
        request_key=_to_str(row[1]),
        request_digest=_to_bytes(row[2]),
        kind=int(row[3]),
        status=int(row[4]),
        seller_player_id=int(row[5]),
        buyer_player_id=int(row[6]),
        market_id=int(row[7]),
        item_config_id=int(row[8]),
        side=int(row[9]),
        unit_price=int(row[10]),
        quantity=int(row[11]),
        filled_quantity=int(row[12]),
        currency_type=int(row[13]),
        expires_ms=int(row[14]),
        created_ms=int(row[15]),
        updated_ms=int(row[16]),
        version=int(row[17]),
        workflow_proto=_to_bytes(row[18]),
        parent_id=int(row[19]),
        counterparty_id=int(row[20]),
        on_book=bool(row[21]),
    )


def _scan_asset_op(protocol: Protocol, row) -> Message:
    return protocol.message(
        "trade.TradeAssetOpRecord",
        op_id=int(row[0]),
        player_id=int(row[1]),
        stream=int(row[2]),
        stream_epoch=int(row[3]),
        seq=int(row[4]),
        kind=int(row[5]),
        status=int(row[6]),
        durable=bool(row[7]),
        attempts=int(row[8]),
        next_attempt_ms=int(row[9]),
        deadline_ms=int(row[10]),
        lease_until_ms=int(row[11]),
        lease_token=int(row[12]),
        tx_type=int(row[13]),
        ref_kind=int(row[14]),
        ref_id=int(row[15]),
        payload=_to_bytes(row[16]),
        last_outcome=int(row[17]),
        last_reason=int(row[18]),
        created_ms=int(row[19]),
        updated_ms=int(row[20]),
        resolved_by=_to_str(row[21]),
        resolve_reason=_to_str(row[22]),
        last_send_ms=int(row[23]),
        escrow_snapshot=_to_bytes(row[24]),
        receipt_ack_pending=bool(row[25]),
        business_consumed=bool(row[26]),
    )


def _build_listing_filter(q: ListingQuery) -> tuple[str, list]:
    conds: list[str] = []
    args: list = []
    if q.tab == LISTING_TAB_PUBLIC_NOTICE:
        conds.extend(("`status`=%s", "`notice_end_ms`>%s"))
        args.extend((LISTING_STATUS_LISTED, q.now_ms))
    elif q.tab == LISTING_TAB_ON_SALE:
        conds.extend(("`status` IN (%s,%s)", "`notice_end_ms`<=%s", "`sale_end_ms`>%s"))
        args.extend((LISTING_STATUS_LISTED, LISTING_STATUS_LOCKED, q.now_ms, q.now_ms))
    else:
        raise StoreUnavailableError(f"unsupported listing tab {q.tab}")

    if q.market_zones:
        holders = ",".join(["%s"] * len(q.market_zones))
        conds.append(f"`market_zone` IN ({holders})")
        args.extend(q.market_zones)
    conds.append("`category`=%s")
    args.append(q.category)
    if q.subcategory != 0:
        conds.append("`subcategory`=%s")
        args.append(q.subcategory)
    if q.title_like_pattern:
        if q.search_listing_id != 0:
            conds.append("(`listing_id`=%s OR `title` LIKE %s ESCAPE '!')")
            args.extend((q.search_listing_id, q.title_like_pattern))
        else:
            conds.append("`title` LIKE %s ESCAPE '!'")
            args.append(q.title_like_pattern)
    if q.favorites_of != 0:
        conds.append(
            "EXISTS (SELECT 1 FROM `trade_favorite` f WHERE f.`player_id`=%s AND f.`listing_id`=`trade_listing`.`listing_id`)"
        )
        args.append(q.favorites_of)
    return " WHERE " + " AND ".join(conds), args


def _listing_order_by(sort: int, tab: int) -> str:
    if sort == LISTING_SORT_DEFAULT:
        return "`created_ms` DESC,`listing_id` DESC"
    if sort == LISTING_SORT_PRICE_ASC:
        return "`price_fen` ASC,`listing_id` ASC"
    if sort == LISTING_SORT_PRICE_DESC:
        return "`price_fen` DESC,`listing_id` DESC"
    if sort == LISTING_SORT_LEVEL_DESC:
        return "`level` DESC,`listing_id` DESC"
    if sort == LISTING_SORT_REMAINING_ASC:
        if tab == LISTING_TAB_PUBLIC_NOTICE:
            return "`notice_end_ms` ASC,`listing_id` ASC"
        return "`sale_end_ms` ASC,`listing_id` ASC"
    raise StoreUnavailableError(f"unsupported listing sort {sort}")


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
    """创建 aiomysql 连接池（严格模式、显式事务控制）。"""
    return await aiomysql.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        db=database,
        autocommit=False,
        charset="utf8mb4",
        maxsize=maxsize,
        minsize=minsize,
    )


class _SqlTx:
    """单次 READ COMMITTED 事务封装（全部二进制字段经 UNHEX(%s) 安全绑定）。"""

    def __init__(self, protocol: Protocol, cursor) -> None:
        self._protocol = protocol
        self._cur = cursor

    async def now_ms(self) -> int:
        await self._cur.execute(f"SELECT {DB_NOW_MS}")
        row = await self._cur.fetchone()
        return int(row[0])

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        await self._cur.execute(
            "SELECT `account`,`owner_revision`,`freeze_token`,`freeze_final`,`version`,`home_zone`,`updated_ms` "
            "FROM `role_fence` WHERE `player_id`=%s FOR UPDATE",
            (int(player_id),),
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_role_fence(int(player_id), row)

    async def upsert_role_fence(self, state: Message, now_ms: int) -> None:
        del now_ms
        await self._cur.execute(
            _APPLY_ROLE_FENCE_SQL,
            (
                int(state.player_id),
                str(state.account).encode("utf-8").hex(),
                int(state.owner_revision),
                int(state.freeze_token),
                1 if state.freeze_final else 0,
                int(state.version),
                int(state.home_zone),
            ),
        )

    async def role_pending_blocker(self, state: Message) -> str:
        pid = int(state.player_id)
        await self._cur.execute(
            "SELECT `commerce_id`,`kind`,`status`,`workflow_proto` FROM `trade_commerce` "
            "WHERE (`seller_player_id`=%s OR `buyer_player_id`=%s) "
            "AND (`status` NOT IN (7,11) OR `status` IS NULL) "
            "ORDER BY `commerce_id` LIMIT %s FOR UPDATE",
            (pid, pid, MAX_ROLE_PENDING_COMMERCE + 1),
        )
        rows = await self._cur.fetchall()
        if len(rows) > MAX_ROLE_PENDING_COMMERCE:
            return ROLE_BLOCKER_TOO_MANY_DEALS
        for row in rows:
            rec = self._protocol.message(
                "trade.TradeCommerceRecord",
                commerce_id=int(row[0]),
                kind=int(row[1]),
                status=int(row[2]),
                workflow_proto=_to_bytes(row[3]),
            )
            if _is_role_pending_commerce(self._protocol, rec, state):
                return ROLE_BLOCKER_COMMERCE
        await self._cur.execute(
            "SELECT `op_id` FROM `trade_asset_op` WHERE `player_id`=%s "
            "AND (`status` NOT IN (2,3,4,6) OR `status` IS NULL) LIMIT 1 FOR UPDATE",
            (pid,),
        )
        if await self._cur.fetchone() is not None:
            return ROLE_BLOCKER_ASSET_OP
        return ""

    async def purge_favorites(self, player_id: int) -> None:
        await self._cur.execute("DELETE FROM `trade_favorite` WHERE `player_id`=%s", (int(player_id),))

    async def lock_guard(self, guard_id: int) -> None:
        gid = int(guard_id)
        await self._cur.execute("INSERT IGNORE INTO `trade_commerce_guard`(`guard_id`) VALUES (%s)", (gid,))
        await self._cur.execute("SELECT `guard_id` FROM `trade_commerce_guard` WHERE `guard_id`=%s FOR UPDATE", (gid,))
        await self._cur.fetchone()

    async def lock_commerce(self, commerce_id: int) -> Message | None:
        await self._cur.execute(
            f"SELECT {_COMMERCE_COLUMNS} FROM `trade_commerce` WHERE `commerce_id`=%s FOR UPDATE",
            (int(commerce_id),),
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_commerce(self._protocol, row)

    async def insert_commerce(self, rec: Message) -> None:
        w = _decode_workflow(self._protocol, bytes(rec.workflow_proto))
        is_on_book = 1 if on_book(rec, w) else 0
        try:
            await self._cur.execute(
                f"INSERT INTO `trade_commerce`({_COMMERCE_COLUMNS}) VALUES "
                "(%s,%s,UNHEX(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),%s,%s,%s)",
                (
                    int(rec.commerce_id),
                    str(rec.request_key),
                    bytes(rec.request_digest).hex(),
                    int(rec.kind),
                    int(rec.status),
                    int(rec.seller_player_id),
                    int(rec.buyer_player_id),
                    int(rec.market_id),
                    int(rec.item_config_id),
                    int(rec.side),
                    int(rec.unit_price),
                    int(rec.quantity),
                    int(rec.filled_quantity),
                    int(rec.currency_type),
                    int(rec.expires_ms),
                    int(rec.created_ms),
                    int(rec.updated_ms),
                    int(rec.version),
                    bytes(rec.workflow_proto).hex(),
                    int(rec.parent_id),
                    int(rec.counterparty_id),
                    is_on_book,
                ),
            )
        except Exception as exc:
            if _mysql_errno(exc) == DUP_ENTRY:
                raise _DuplicateKeyError(str(exc)) from exc
            raise
        rec.on_book = bool(is_on_book)

    async def save_commerce(self, rec: Message, workflow: Message, now_ms: int) -> None:
        raw = workflow.SerializeToString(deterministic=True)
        is_on_book = 1 if on_book(rec, workflow) else 0
        await self._cur.execute(
            "UPDATE `trade_commerce` SET `status`=%s,`seller_player_id`=%s,`buyer_player_id`=%s,"
            "`filled_quantity`=%s,`expires_ms`=%s,`updated_ms`=%s,`version`=`version`+1,"
            "`workflow_proto`=UNHEX(%s),`on_book`=%s WHERE `commerce_id`=%s AND `version`=%s",
            (
                int(rec.status),
                int(rec.seller_player_id),
                int(rec.buyer_player_id),
                int(rec.filled_quantity),
                int(rec.expires_ms),
                int(now_ms),
                raw.hex(),
                is_on_book,
                int(rec.commerce_id),
                int(rec.version),
            ),
        )
        if self._cur.rowcount != 1:
            raise StoreUnavailableError("commerce version changed during save")
        rec.version = int(rec.version) + 1
        rec.workflow_proto = raw
        rec.updated_ms = int(now_ms)
        rec.on_book = bool(is_on_book)

    async def save_commerce_status(self, rec: Message, status: int, now_ms: int) -> None:
        await self._cur.execute(
            "UPDATE `trade_commerce` SET `status`=%s,`updated_ms`=%s,`version`=`version`+1 "
            "WHERE `commerce_id`=%s AND `version`=%s",
            (int(status), int(now_ms), int(rec.commerce_id), int(rec.version)),
        )
        if self._cur.rowcount != 1:
            raise StoreUnavailableError("commerce version changed during save_status")
        rec.status = int(status)
        rec.version = int(rec.version) + 1
        rec.updated_ms = int(now_ms)

    async def reschedule_commerce(self, commerce_id: int, version: int, due_ms: int) -> None:
        await self._cur.execute(
            "UPDATE `trade_commerce` SET `updated_ms`=%s WHERE `commerce_id`=%s AND `version`=%s",
            (int(due_ms), int(commerce_id), int(version)),
        )

    async def find_market_counterparty(self, taker: Message) -> int | None:
        return await _sql_find_market_counterparty(self._cur, taker)

    async def count_pending_market_fills(self, parent_id: int) -> int:
        pid = int(parent_id)
        await self._cur.execute(
            "SELECT COALESCE(SUM(`status` NOT IN (7,11)),0) FROM `trade_commerce` "
            "WHERE `kind`=4 AND `side`=0 AND (`parent_id`=%s OR `counterparty_id`=%s)",
            (pid, pid),
        )
        row = await self._cur.fetchone()
        return int(row[0] or 0)

    async def allocate_seqs(
        self, now_ms: int, keys: Sequence[tuple[int, int]]
    ) -> dict[tuple[int, int], tuple[int, int]]:
        ordered = sorted({(int(pid), int(stream)) for pid, stream in keys})
        for pid, stream in ordered:
            if pid == 0 or stream == 0:
                raise StoreUnavailableError(f"invalid seq key ({pid}, {stream})")
            await self._cur.execute(
                "SELECT 1 FROM `trade_player_op_seq` WHERE `player_id`=%s AND `stream`=%s",
                (pid, stream),
            )
            if await self._cur.fetchone() is None:
                raise StoreUnavailableError(f"trade_player_op_seq row missing in tx ({pid}, {stream})")
        out: dict[tuple[int, int], tuple[int, int]] = {}
        for pid, stream in ordered:
            await self._cur.execute(
                "SELECT `next_seq`,`epoch` FROM `trade_player_op_seq` "
                "WHERE `player_id`=%s AND `stream`=%s FOR UPDATE",
                (pid, stream),
            )
            row = await self._cur.fetchone()
            if row is None or int(row[0]) == 0 or int(row[1]) == 0:
                raise StoreUnavailableError(f"corrupt trade_player_op_seq ({pid}, {stream})")
            next_seq, epoch = int(row[0]), int(row[1])
            await self._cur.execute(
                "SELECT COUNT(*),COALESCE(MIN(`seq`),0) FROM `trade_asset_op` "
                "WHERE `player_id`=%s AND `stream`=%s AND `stream_epoch`=%s AND `status`=%s",
                (pid, stream, epoch, OP_STATUS_PENDING),
            )
            cnt_row = await self._cur.fetchone()
            pending_cnt, min_seq = int(cnt_row[0] or 0), int(cnt_row[1] or 0)
            if pending_cnt >= ASSET_OP_MAX_PENDING:
                raise TradeTooManyPendingError(f"player={pid} stream={stream} pending={pending_cnt}")
            if pending_cnt > 0 and next_seq - min_seq >= ASSET_OP_MAX_SPAN:
                raise TradeTooManyPendingError(f"player={pid} stream={stream} span exceeded")
            await self._cur.execute(
                "UPDATE `trade_player_op_seq` SET `next_seq`=`next_seq`+1,`updated_ms`=%s "
                "WHERE `player_id`=%s AND `stream`=%s",
                (int(now_ms), pid, stream),
            )
            out[(pid, stream)] = (next_seq, epoch)
        return out

    async def insert_asset_op(self, rec: Message) -> None:
        last_send = max(int(rec.last_send_ms), int(rec.created_ms))
        await self._cur.execute(
            f"INSERT INTO `trade_asset_op`({_ASSET_OP_COLUMNS}) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),%s,%s)",
            (
                int(rec.op_id),
                int(rec.player_id),
                int(rec.stream),
                int(rec.stream_epoch),
                int(rec.seq),
                int(rec.kind),
                int(rec.status),
                1 if rec.durable else 0,
                int(rec.attempts),
                int(rec.next_attempt_ms),
                int(rec.deadline_ms),
                int(rec.lease_until_ms),
                int(rec.lease_token),
                int(rec.tx_type),
                int(rec.ref_kind),
                int(rec.ref_id),
                bytes(rec.payload).hex(),
                int(rec.last_outcome),
                int(rec.last_reason),
                int(rec.created_ms),
                int(rec.updated_ms),
                str(rec.resolved_by),
                str(rec.resolve_reason),
                last_send,
                bytes(rec.escrow_snapshot).hex(),
                1 if rec.receipt_ack_pending else 0,
                1 if rec.business_consumed else 0,
            ),
        )

    async def read_leg_outcome(self, op_id: int) -> LegOutcome | None:
        return await _sql_read_leg_outcome(self._cur, op_id)

    async def expire_commerce_escrow_debits(self, commerce_id: int, now_ms: int) -> None:
        await self._cur.execute(
            "SELECT `op_id` FROM `trade_asset_op` WHERE `ref_kind`=2 AND `ref_id`=%s ORDER BY `op_id`",
            (int(commerce_id),),
        )
        op_ids = [int(r[0]) for r in await self._cur.fetchall()]
        for oid in op_ids:
            await self._cur.execute(_SQL_LOCK_ASSET_OP, (oid,))
            if await self._cur.fetchone() is None:
                continue
            await self._cur.execute(
                "UPDATE `trade_asset_op` SET `deadline_ms`=%s WHERE `op_id`=%s AND `stream`=3 AND `status`=1",
                (int(now_ms), oid),
            )

    async def mark_commerce_ops_consumed(self, commerce_id: int) -> None:
        await self._cur.execute(
            "SELECT `op_id` FROM `trade_asset_op` WHERE `ref_kind`=2 AND `ref_id`=%s ORDER BY `op_id`",
            (int(commerce_id),),
        )
        op_ids = [int(r[0]) for r in await self._cur.fetchall()]
        for oid in op_ids:
            await self._cur.execute(_SQL_LOCK_ASSET_OP, (oid,))
            if await self._cur.fetchone() is None:
                continue
            await self._cur.execute("UPDATE `trade_asset_op` SET `business_consumed`=1 WHERE `op_id`=%s", (oid,))


async def _sql_find_market_counterparty(cur, taker: Message) -> int | None:
    if int(taker.side) == 1:
        opposite, flt, order = 2, "`unit_price`<=%s", "`unit_price` ASC,`created_ms`,`commerce_id`"
    else:
        opposite, flt, order = 1, "`unit_price`>=%s", "`unit_price` DESC,`created_ms`,`commerce_id`"
    await cur.execute(
        f"SELECT `commerce_id` FROM `trade_commerce` WHERE `kind`=4 AND `side`=%s AND `market_id`=%s "
        f"AND `item_config_id`=%s AND `currency_type`=%s AND `status`=3 AND `on_book`=1 "
        f"AND `filled_quantity`<`quantity` AND `seller_player_id`<>%s AND {flt} ORDER BY {order} LIMIT 1",
        (
            opposite,
            int(taker.market_id),
            int(taker.item_config_id),
            int(taker.currency_type),
            int(taker.seller_player_id),
            int(taker.unit_price),
        ),
    )
    row = await cur.fetchone()
    return None if row is None else int(row[0])


async def _sql_read_leg_outcome(cur, op_id: int) -> LegOutcome | None:
    await cur.execute(
        "SELECT `status`,`escrow_snapshot`,`resolved_by`,`next_attempt_ms` FROM `trade_asset_op` WHERE `op_id`=%s",
        (int(op_id),),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    status = int(row[0])
    final_ms = int(row[3]) if status in OP_TERMINAL_STATUSES else 0
    return LegOutcome(
        status=status,
        snapshot=_to_bytes(row[1]),
        resolved_by=_to_str(row[2]),
        final_ms=final_ms,
    )


class SqlTradeStore(TradeStoreBase):
    """trade 独占库 mmorpg_trade 的 MySQL/TiDB 存储实现。"""

    def __init__(
        self,
        pool,
        protocol: Protocol,
        *,
        role_directory: RoleFenceDirectory | None = None,
        id_allocator: Callable[[], Awaitable[int]] | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        super().__init__(protocol, role_directory=role_directory, clock_ms=clock_ms)
        self._pool = pool
        self._id_allocator = id_allocator
        self._local_seq = 0

    async def _next_credit_id(self) -> int:
        if self._id_allocator is not None:
            return int(await self._id_allocator())
        self._local_seq += 1
        return (self.now_ms() << 16) | (self._local_seq & 0xFFFF)

    async def bootstrap(self) -> None:
        """幂等建表（读取 config/trade_schema.sql 并补建卖家信用 4 表）。"""
        ddl = SCHEMA_FILE.read_text(encoding="utf-8") + "\n" + SELLER_CREDIT_SCHEMA_DDL
        lines = [line for line in ddl.splitlines() if not line.lstrip().startswith("--")]
        statements = [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                for stmt in statements:
                    await cur.execute(stmt)
            await conn.commit()

    async def verify_schema(self) -> None:
        """启动期验表：校验 7 张必选表均存在，缺失即 fail-closed 拒启。"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SHOW TABLES")
                existing = {_to_str(row[0]).lower() for row in await cur.fetchall()}
            await conn.commit()
        missing = [t for t in REQUIRED_TABLES if t not in existing]
        if missing:
            raise RuntimeError(f"trade schema missing required tables: {missing}")

    async def run_tx(self, body: Callable[[TradeTx], Awaitable[T]]) -> T:
        for attempt in range(TX_MAX_ATTEMPTS):
            async with self._pool.acquire() as conn:
                try:
                    async with conn.cursor() as cur:
                        await cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        await conn.begin()
                        result = await body(_SqlTx(self.protocol, cur))
                    await conn.commit()
                    return result
                except _TxRollbackReturn as signal:
                    await conn.rollback()
                    return signal.value
                except Exception as exc:
                    await conn.rollback()
                    if _mysql_errno(exc) in LOCK_ERRORS and attempt + 1 < TX_MAX_ATTEMPTS:
                        await asyncio.sleep(0.015 * (attempt + 1) * (0.8 + 0.4 * random.random()))
                        continue
                    if isinstance(exc, (TradeError, _DuplicateKeyError)):
                        raise
                    raise StoreUnavailableError(f"trade sql tx failed: {exc}") from exc
        raise StoreUnavailableError("trade sql tx retry exhausted")

    async def _query(self, sql: str, args: Sequence = ()):
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, tuple(args))
                rows = await cur.fetchall()
            await conn.commit()
            return rows

    async def _query_one(self, sql: str, args: Sequence = ()):
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, tuple(args))
                row = await cur.fetchone()
            await conn.commit()
            return row

    async def get_role_fence_local(self, player_id: int) -> RoleFenceRow | None:
        row = await self._query_one(
            "SELECT `account`,`owner_revision`,`freeze_token`,`freeze_final`,`version`,`home_zone`,`updated_ms` "
            "FROM `role_fence` WHERE `player_id`=%s",
            (int(player_id),),
        )
        return None if row is None else _scan_role_fence(int(player_id), row)

    # -- P1 Catalog ---------------------------------------------------------------------------------

    async def count_listings(self, query: ListingQuery) -> int:
        where, args = _build_listing_filter(query)
        row = await self._query_one("SELECT COUNT(*) FROM `trade_listing`" + where, args)
        return int(row[0] or 0)

    async def query_listings(self, query: ListingQuery, offset: int, limit: int) -> list[Message]:
        where, args = _build_listing_filter(query)
        order_by = _listing_order_by(query.sort, query.tab)
        rows = await self._query(
            f"SELECT {_LISTING_SUMMARY_COLUMNS} FROM `trade_listing`{where} ORDER BY {order_by} LIMIT %s OFFSET %s",
            [*args, int(limit), int(offset)],
        )
        return [_scan_listing(self.protocol, r, with_description=False) for r in rows]

    async def count_seller_listings(self, seller_id: int) -> int:
        row = await self._query_one(
            "SELECT COUNT(*) FROM `trade_listing` WHERE `seller_player_id`=%s",
            (int(seller_id),),
        )
        return int(row[0] or 0)

    async def query_seller_listings(self, seller_id: int, offset: int, limit: int) -> list[Message]:
        rows = await self._query(
            f"SELECT {_LISTING_SUMMARY_COLUMNS} FROM `trade_listing` WHERE `seller_player_id`=%s "
            "ORDER BY `created_ms` DESC,`listing_id` DESC LIMIT %s OFFSET %s",
            (int(seller_id), int(limit), int(offset)),
        )
        return [_scan_listing(self.protocol, r, with_description=False) for r in rows]

    async def get_listing(self, listing_id: int) -> Message:
        row = await self._query_one(
            f"SELECT {_LISTING_DETAIL_COLUMNS} FROM `trade_listing` WHERE `listing_id`=%s",
            (int(listing_id),),
        )
        if row is None:
            raise ListingNotFoundError(f"listing {listing_id} not found")
        return _scan_listing(self.protocol, row, with_description=True)

    async def insert_listing(self, rec: Message) -> None:
        async def _tx(tx: TradeTx) -> None:
            assert isinstance(tx, _SqlTx)
            await tx._cur.execute(
                f"INSERT INTO `trade_listing`({_LISTING_DETAIL_COLUMNS}) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    int(rec.listing_id),
                    int(rec.seller_player_id),
                    str(rec.seller_account),
                    int(rec.market_zone),
                    int(rec.seller_zone_at_listing),
                    int(rec.category),
                    int(rec.subcategory),
                    str(rec.title),
                    int(rec.level),
                    int(rec.price_fen),
                    int(rec.status),
                    str(rec.summary),
                    str(rec.icon_key),
                    int(rec.notice_end_ms),
                    int(rec.sale_end_ms),
                    int(rec.created_ms),
                    int(rec.updated_ms),
                    int(rec.version),
                    str(rec.description),
                ),
            )

        await self.run_tx(_tx)

    async def favorite_ids(self, player_id: int, listing_ids: Sequence[int]) -> set[int]:
        if not listing_ids:
            return set()
        holders = ",".join(["%s"] * len(listing_ids))
        rows = await self._query(
            f"SELECT `listing_id` FROM `trade_favorite` WHERE `player_id`=%s AND `listing_id` IN ({holders})",
            [int(player_id), *(int(lid) for lid in listing_ids)],
        )
        return {int(r[0]) for r in rows}

    async def favorite_exists(self, player_id: int, listing_id: int) -> bool:
        row = await self._query_one(
            "SELECT 1 FROM `trade_favorite` WHERE `player_id`=%s AND `listing_id`=%s",
            (int(player_id), int(listing_id)),
        )
        return row is not None

    async def count_favorites(self, player_id: int) -> int:
        row = await self._query_one(
            "SELECT COUNT(*) FROM `trade_favorite` WHERE `player_id`=%s",
            (int(player_id),),
        )
        return int(row[0] or 0)

    async def insert_favorite(self, player_id: int, listing_id: int, created_ms: int) -> None:
        async def _tx(tx: TradeTx) -> None:
            assert isinstance(tx, _SqlTx)
            await tx._cur.execute(_INSERT_FAVORITE_SQL, (int(player_id), int(listing_id), int(created_ms)))

        await self.run_tx(_tx)

    async def delete_favorite(self, player_id: int, listing_id: int) -> None:
        async def _tx(tx: TradeTx) -> None:
            assert isinstance(tx, _SqlTx)
            await tx._cur.execute(_DELETE_FAVORITE_SQL, (int(player_id), int(listing_id)))

        await self.run_tx(_tx)

    # -- Commerce 查询与调度 ------------------------------------------------------------------------

    async def get_commerce(self, commerce_id: int) -> Message | None:
        row = await self._query_one(
            f"SELECT {_COMMERCE_COLUMNS} FROM `trade_commerce` WHERE `commerce_id`=%s",
            (int(commerce_id),),
        )
        return None if row is None else _scan_commerce(self.protocol, row)

    async def get_commerce_by_request(self, req_key: str) -> Message | None:
        row = await self._query_one(
            f"SELECT {_COMMERCE_COLUMNS} FROM `trade_commerce` WHERE `request_key`=%s",
            (str(req_key),),
        )
        return None if row is None else _scan_commerce(self.protocol, row)

    async def list_commerce_orders(self, player_id: int, after: CommerceCursor, limit: int) -> list[Message]:
        pid = int(player_id)
        rows = await self._query(
            f"SELECT {_COMMERCE_COLUMNS} FROM `trade_commerce` "
            "WHERE (`created_ms`>%s OR (`created_ms`=%s AND `commerce_id`>%s)) "
            "AND `kind`<>1 AND (`seller_player_id`=%s OR `buyer_player_id`=%s) "
            "ORDER BY `created_ms`,`commerce_id` LIMIT %s",
            (after.created_ms, after.created_ms, after.commerce_id, pid, pid, int(limit)),
        )
        return [_scan_commerce(self.protocol, r) for r in rows]

    async def list_market_orders(self, player_id: int, after: CommerceCursor, limit: int) -> list[Message]:
        rows = await self._query(
            f"SELECT {_COMMERCE_COLUMNS} FROM `trade_commerce` "
            "WHERE `kind`=4 AND `side`<>0 AND `seller_player_id`=%s "
            "AND (`created_ms`>%s OR (`created_ms`=%s AND `commerce_id`>%s)) "
            "ORDER BY `created_ms`,`commerce_id` LIMIT %s",
            (int(player_id), after.created_ms, after.created_ms, after.commerce_id, int(limit)),
        )
        return [_scan_commerce(self.protocol, r) for r in rows]

    async def query_market_depth(
        self, market_id: int, item_config_id: int, currency_type: int, limit: int
    ) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
        bids_rows = await self._query(
            "SELECT `unit_price`,SUM(`quantity`-`filled_quantity`) FROM `trade_commerce` "
            "WHERE `kind`=4 AND `side`=1 AND `market_id`=%s AND `item_config_id`=%s "
            "AND `currency_type`=%s AND `status`=3 AND `on_book`=1 AND `filled_quantity`<`quantity` "
            "GROUP BY `unit_price` ORDER BY `unit_price` DESC LIMIT %s",
            (int(market_id), int(item_config_id), int(currency_type), int(limit)),
        )
        asks_rows = await self._query(
            "SELECT `unit_price`,SUM(`quantity`-`filled_quantity`) FROM `trade_commerce` "
            "WHERE `kind`=4 AND `side`=2 AND `market_id`=%s AND `item_config_id`=%s "
            "AND `currency_type`=%s AND `status`=3 AND `on_book`=1 AND `filled_quantity`<`quantity` "
            "GROUP BY `unit_price` ORDER BY `unit_price` ASC LIMIT %s",
            (int(market_id), int(item_config_id), int(currency_type), int(limit)),
        )
        asks = [(int(r[0]), int(r[1])) for r in asks_rows]
        bids = [(int(r[0]), int(r[1])) for r in bids_rows]
        return asks, bids

    async def browse_commerce_offers(self, kind: int, after_id: int, now_ms: int, limit: int) -> list[Message]:
        rows = await self._query(
            f"SELECT {_COMMERCE_COLUMNS} FROM `trade_commerce` "
            "WHERE `kind`=%s AND `status`=3 AND `commerce_id`>%s AND `expires_ms`>%s "
            "ORDER BY `commerce_id` LIMIT %s",
            (int(kind), int(after_id), int(now_ms), int(limit)),
        )
        return [_scan_commerce(self.protocol, r) for r in rows]

    async def list_due_commerce(self, now_ms: int, limit: int) -> list[Message]:
        rows = await self._query(
            f"SELECT {_COMMERCE_COLUMNS} FROM `trade_commerce` "
            f"WHERE `status` IN ({_DUE_STATUSES_SQL}) AND `updated_ms`<=%s "
            "ORDER BY `updated_ms`,`commerce_id` LIMIT %s",
            (int(now_ms), int(limit)),
        )
        return [_scan_commerce(self.protocol, r) for r in rows]

    async def list_manual_commerce(self, after_updated_ms: int, after_id: int, limit: int) -> list[Message]:
        rows = await self._query(
            f"SELECT {_COMMERCE_COLUMNS} FROM `trade_commerce` "
            "WHERE `status`=10 AND (`updated_ms`>%s OR (`updated_ms`=%s AND `commerce_id`>%s)) "
            "ORDER BY `updated_ms`,`commerce_id` LIMIT %s",
            (int(after_updated_ms), int(after_updated_ms), int(after_id), int(limit)),
        )
        return [_scan_commerce(self.protocol, r) for r in rows]

    async def defer_commerce(self, commerce_id: int, version: int, until_ms: int) -> bool:
        async def _tx(tx: TradeTx) -> bool:
            assert isinstance(tx, _SqlTx)
            cur = await tx.lock_commerce(commerce_id)
            if cur is None or int(cur.version) != int(version):
                return False
            await tx._cur.execute(
                "UPDATE `trade_commerce` SET `updated_ms`=%s WHERE `commerce_id`=%s AND `version`=%s",
                (int(until_ms), int(commerce_id), int(version)),
            )
            return tx._cur.rowcount == 1

        return await self.run_tx(_tx)

    async def wake_commerce(self, commerce_id: int, at_ms: int) -> None:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_SQL_WAKE_COMMERCE, (int(at_ms), int(commerce_id), int(at_ms)))
            await conn.commit()

    async def find_market_counterparty(self, taker: Message) -> int | None:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                cid = await _sql_find_market_counterparty(cur, taker)
            await conn.commit()
            return cid

    async def read_leg_outcome(self, op_id: int) -> LegOutcome | None:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                outcome = await _sql_read_leg_outcome(cur, op_id)
            await conn.commit()
            return outcome

    async def ensure_seq_rows(self, now_ms: int, keys: Sequence[tuple[int, int]]) -> None:
        """短事务 A：按 (player_id, stream) 升序补建缺失 seq 行（永不删行）。"""
        ordered = sorted({(int(pid), int(stream)) for pid, stream in keys})
        if not ordered:
            return
        missing: list[tuple[int, int]] = []
        for pid, stream in ordered:
            if pid == 0 or stream == 0:
                raise StoreUnavailableError(f"invalid seq key ({pid}, {stream})")
            row = await self._query_one(
                "SELECT 1 FROM `trade_player_op_seq` WHERE `player_id`=%s AND `stream`=%s",
                (pid, stream),
            )
            if row is None:
                missing.append((pid, stream))
        if not missing:
            return
        epoch = max(int(now_ms), 1)

        async def _tx(tx: TradeTx) -> None:
            assert isinstance(tx, _SqlTx)
            for pid, stream in missing:
                await tx._cur.execute(
                    "INSERT IGNORE INTO `trade_player_op_seq`(`player_id`,`stream`,`next_seq`,`epoch`,`updated_ms`) "
                    "VALUES (%s,%s,1,%s,%s)",
                    (pid, stream, epoch, epoch),
                )

        await self.run_tx(_tx)

    # -- AssetOp Outbox -----------------------------------------------------------------------------

    async def _list_due_segment(
        self,
        now_ms: int,
        limit: int,
        fresh_only: bool,
        owned_shards: frozenset[int] | None,
    ) -> list[int]:
        cmp_op = "<" if fresh_only else ">="
        args: list = [OP_STATUS_PENDING, int(now_ms), int(now_ms), FRESH_ATTEMPT_LIMIT]
        shard_sql = ""
        if owned_shards is not None:
            if not owned_shards:
                return []
            holders = ",".join(["%s"] * len(owned_shards))
            shard_sql = f" AND MOD(`player_id`,64) IN ({holders})"
            args.extend(sorted(owned_shards))
        args.append(int(limit))
        rows = await self._query(
            f"SELECT `op_id` FROM `trade_asset_op` "
            f"WHERE `status`=%s AND `next_attempt_ms`<=%s AND `lease_until_ms`<%s "
            f"AND `attempts`{cmp_op}%s{shard_sql} "
            f"ORDER BY `next_attempt_ms` ASC,`op_id` ASC LIMIT %s",
            args,
        )
        return [int(r[0]) for r in rows]

    async def list_due_ops(
        self,
        now_ms: int,
        limit: int,
        *,
        owned_shards: frozenset[int] | None = None,
    ) -> list[int]:
        """两段式到期扫描（attempts < 3 优先），防止老行饿死新托管指令。"""
        if limit <= 0:
            return []
        fresh = await self._list_due_segment(now_ms, limit, True, owned_shards)
        if len(fresh) >= limit:
            return fresh
        aged = await self._list_due_segment(now_ms, limit - len(fresh), False, owned_shards)
        seen = set(fresh)
        for oid in aged:
            if oid not in seen:
                fresh.append(oid)
                seen.add(oid)
        return fresh

    async def claim_op(
        self,
        op_id: int,
        now_ms: int,
        lease_until_ms: int,
        poison_until_ms: int,
        token: int,
    ) -> PendingAssetOp | None:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    _SQL_CLAIM_ASSET_OP,
                    (int(lease_until_ms), int(token), int(now_ms), int(op_id), OP_STATUS_PENDING, int(now_ms)),
                )
                affected = cur.rowcount
            await conn.commit()
        if affected != 1:
            return None
        row = await self._query_one(
            f"SELECT {_ASSET_OP_COLUMNS} FROM `trade_asset_op` WHERE `op_id`=%s",
            (int(op_id),),
        )
        if row is None:
            return None
        rec = _scan_asset_op(self.protocol, row)
        try:
            return _op_from_record(self.protocol, rec, token)
        except AssetOpPoisonRowError:
            await self._mark_poison(op_id, token, now_ms, poison_until_ms)
            raise

    async def _mark_poison(self, op_id: int, token: int, now_ms: int, poison_until_ms: int) -> None:
        async def _tx(tx: TradeTx) -> None:
            assert isinstance(tx, _SqlTx)
            await tx._cur.execute(_SQL_LOCK_ASSET_OP, (int(op_id),))
            if await tx._cur.fetchone() is None:
                return
            await tx._cur.execute(
                _SQL_POISON_ASSET_OP,
                (int(poison_until_ms), int(now_ms), int(op_id), int(token), OP_STATUS_PENDING),
            )

        try:
            await self.run_tx(_tx)
        except Exception:
            logger.error("trade failed to postpone poison asset op op_id=%d", op_id, exc_info=True)

    async def reschedule_op(
        self,
        op: PendingAssetOp,
        next_attempt_ms: int,
        outcome: int,
        reason: int,
        durable: bool,
        local: bool,
        now_ms: int,
    ) -> None:
        async def _tx(tx: TradeTx) -> None:
            assert isinstance(tx, _SqlTx)
            await tx._cur.execute(_SQL_LOCK_ASSET_OP, (op.op_id,))
            if await tx._cur.fetchone() is None:
                raise AssetOpLeaseLostError(f"asset op vanished op_id={op.op_id}")
            await tx._cur.execute(
                _SQL_RESCHEDULE_ASSET_OP,
                (
                    int(next_attempt_ms),
                    1 if durable else 0,
                    int(outcome),
                    int(reason),
                    int(now_ms),
                    0 if local else 1,
                    int(now_ms),
                    op.op_id,
                    OP_STATUS_PENDING,
                    op.lease_token,
                ),
            )
            if tx._cur.rowcount == 0:
                raise AssetOpLeaseLostError(f"asset op lease lost op_id={op.op_id}")

        await self.run_tx(_tx)

    async def finalize_op(
        self,
        op: PendingAssetOp,
        final_status: int,
        outcome: int,
        reason: int,
        *,
        snapshot: Message | None,
        sql_committed: bool,
        durable: bool,
        now_ms: int,
    ) -> bool:
        if final_status not in OP_FINALIZABLE_STATUSES:
            raise StoreUnavailableError(f"invalid final asset op status: {final_status}")

        async def _tx(tx: TradeTx) -> tuple[bool, int]:
            assert isinstance(tx, _SqlTx)
            await tx._cur.execute(_SQL_LOCK_ASSET_OP, (op.op_id,))
            if await tx._cur.fetchone() is None:
                return False, 0
            await tx._cur.execute(
                "SELECT `ref_kind`,`ref_id`,`stream` FROM `trade_asset_op` WHERE `op_id`=%s",
                (op.op_id,),
            )
            meta = await tx._cur.fetchone()
            ref_kind, ref_id, stream = int(meta[0]), int(meta[1]), int(meta[2])
            if ref_kind == OP_REF_KIND_ORDER and (not sql_committed or not durable):
                raise StoreUnavailableError("commerce asset op finalize requires SQL-committed durable evidence")
            await tx._cur.execute(
                _SQL_FINALIZE_ASSET_OP,
                (
                    int(final_status),
                    int(outcome),
                    int(reason),
                    int(now_ms),
                    int(now_ms),
                    op.op_id,
                    OP_STATUS_PENDING,
                ),
            )
            if tx._cur.rowcount != 1:
                return False, 0
            if snapshot is not None and final_status == OP_STATUS_APPLIED and stream == ASSET_STREAM_TRADE_DEBIT:
                snap_bytes = snapshot.SerializeToString(deterministic=True)
                await tx._cur.execute(
                    "UPDATE `trade_asset_op` SET `escrow_snapshot`=UNHEX(%s),`receipt_ack_pending`=1 WHERE `op_id`=%s",
                    (snap_bytes.hex(), op.op_id),
                )
            wake_id = ref_id if ref_kind == OP_REF_KIND_ORDER else 0
            return True, wake_id

        finalized, wake_cid = await self.run_tx(_tx)
        if finalized and wake_cid:
            try:
                await self.wake_commerce(wake_cid, now_ms)
            except Exception:
                logger.error("trade post-finalize wake commerce failed cid=%d op_id=%d", wake_cid, op.op_id, exc_info=True)
        return finalized

    async def list_ack_due_ops(self, after_op_id: int, limit: int) -> list[PendingAssetOp | tuple[int, None]]:
        rows = await self._query(
            "SELECT `op_id`,`player_id`,`stream`,`stream_epoch`,`seq`,`tx_type`,`payload` "
            "FROM `trade_asset_op` WHERE `receipt_ack_pending`=1 AND `status`=2 AND `durable`=1 "
            "AND `op_id`>%s ORDER BY `op_id` LIMIT %s",
            (int(after_op_id), int(limit)),
        )
        out: list[PendingAssetOp | tuple[int, None]] = []
        for r in rows:
            rec = self.protocol.message(
                "trade.TradeAssetOpRecord",
                op_id=int(r[0]),
                player_id=int(r[1]),
                stream=int(r[2]),
                stream_epoch=int(r[3]),
                seq=int(r[4]),
                tx_type=int(r[5]),
                payload=_to_bytes(r[6]),
            )
            try:
                out.append(_op_from_record(self.protocol, rec, 0))
            except AssetOpPoisonRowError:
                out.append((int(r[0]), None))
        return out

    async def mark_op_acked(self, op_id: int) -> None:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE `trade_asset_op` SET `receipt_ack_pending`=0 WHERE `op_id`=%s AND `status`=2 AND `durable`=1",
                    (int(op_id),),
                )
            await conn.commit()

    # -- 卖家信用、近 7 日成交参考价与当面交易单日流出（对齐 Go seller_credit_sql.go / reference_price.go / player_trade_guard.go） --

    async def get_credit_profile(self, key: SellerCreditKey) -> SellerCreditProfile | None:
        row = await self._query_one(
            f"SELECT {_SELLER_CREDIT_COLUMNS} FROM `trade_seller_credit` "
            "WHERE `seller_player_id`=%s AND `owner_revision`=%s",
            (int(key.seller_player_id), int(key.owner_revision)),
        )
        if row is None:
            return None
        known_raw = _to_str(row[16])
        known: tuple[str, ...] = ()
        if known_raw:
            try:
                parsed = json.loads(known_raw)
                if isinstance(parsed, list):
                    known = tuple(str(x) for x in parsed)
            except Exception:
                known = ()
        return SellerCreditProfile(
            seller_player_id=int(row[0]),
            owner_revision=int(row[1]),
            owner_account=_to_str(row[2]),
            ownership_changed=bool(row[3]),
            ownership_changed_at_ms=int(row[4]),
            epoch_started_at_ms=int(row[5]),
            completed_orders=int(row[6]),
            positive_reviews=int(row[7]),
            neutral_reviews=int(row[8]),
            negative_reviews=int(row[9]),
            violation_count=int(row[10]),
            env_credit_cleared=bool(row[11]),
            env_reset_count=int(row[12]),
            last_env_changed_at_ms=int(row[13]),
            last_ip=_to_str(row[14]),
            last_device_id=_to_str(row[15]),
            known_devices=known,
            credit_score=int(row[17]),
            star_rating_tenths=int(row[18]),
            positive_rate_bps=int(row[19]),
            risk_tier=_to_str(row[20]),
        )

    async def has_prior_credit_epoch(self, seller_player_id: int, current_revision: int) -> bool:
        row = await self._query_one(
            "SELECT 1 FROM `trade_seller_credit` WHERE `seller_player_id`=%s AND `owner_revision`<%s LIMIT 1",
            (int(seller_player_id), int(current_revision)),
        )
        return row is not None

    async def save_credit_profile(self, profile: SellerCreditProfile) -> None:
        known_json = json.dumps(list(profile.known_devices), ensure_ascii=False) if profile.known_devices else "[]"
        now = self.now_ms()
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"INSERT INTO `trade_seller_credit` ({_SELLER_CREDIT_COLUMNS}) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE "
                    "`owner_account`=VALUES(`owner_account`),`ownership_changed`=VALUES(`ownership_changed`),"
                    "`ownership_changed_at_ms`=VALUES(`ownership_changed_at_ms`),"
                    "`epoch_started_at_ms`=VALUES(`epoch_started_at_ms`),"
                    "`completed_orders`=VALUES(`completed_orders`),`positive_reviews`=VALUES(`positive_reviews`),"
                    "`neutral_reviews`=VALUES(`neutral_reviews`),`negative_reviews`=VALUES(`negative_reviews`),"
                    "`violation_count`=VALUES(`violation_count`),`env_credit_cleared`=VALUES(`env_credit_cleared`),"
                    "`env_reset_count`=VALUES(`env_reset_count`),`last_env_changed_at_ms`=VALUES(`last_env_changed_at_ms`),"
                    "`last_ip`=VALUES(`last_ip`),`last_device_id`=VALUES(`last_device_id`),"
                    "`known_devices_json`=VALUES(`known_devices_json`),`credit_score`=VALUES(`credit_score`),"
                    "`star_rating_tenths`=VALUES(`star_rating_tenths`),`positive_rate_bps`=VALUES(`positive_rate_bps`),"
                    "`risk_tier`=VALUES(`risk_tier`),`updated_ms`=VALUES(`updated_ms`)",
                    (
                        int(profile.seller_player_id),
                        int(profile.owner_revision),
                        str(profile.owner_account),
                        1 if profile.ownership_changed else 0,
                        int(profile.ownership_changed_at_ms),
                        int(profile.epoch_started_at_ms),
                        int(profile.completed_orders),
                        int(profile.positive_reviews),
                        int(profile.neutral_reviews),
                        int(profile.negative_reviews),
                        int(profile.violation_count),
                        1 if profile.env_credit_cleared else 0,
                        int(profile.env_reset_count),
                        int(profile.last_env_changed_at_ms),
                        str(profile.last_ip),
                        str(profile.last_device_id),
                        known_json,
                        int(profile.credit_score),
                        int(profile.star_rating_tenths),
                        int(profile.positive_rate_bps),
                        str(profile.risk_tier),
                        int(now),
                    ),
                )
            await conn.commit()

    async def mark_seller_order_completed(self, order_id: str, key: SellerCreditKey) -> bool:
        rec_id = await self._next_credit_id()
        now = self.now_ms()
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT IGNORE INTO `trade_seller_completed_order` "
                    "(`record_id`,`order_id`,`seller_player_id`,`owner_revision`,`created_ms`) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (int(rec_id), str(order_id), int(key.seller_player_id), int(key.owner_revision), int(now)),
                )
                inserted = cur.rowcount == 1
            await conn.commit()
        return inserted

    async def get_seller_review_by_order(self, order_id: str) -> SellerReviewRecord | None:
        row = await self._query_one(
            "SELECT `order_id`,`seller_player_id`,`seller_owner_revision`,`buyer_player_id`,"
            "`buyer_account`,`buyer_ip`,`buyer_device_id`,`rating`,`counted_for_score`,`comment`,`created_ms` "
            "FROM `trade_seller_review` WHERE `order_id`=%s",
            (str(order_id),),
        )
        if row is None:
            return None
        return SellerReviewRecord(
            order_id=_to_str(row[0]),
            seller_player_id=int(row[1]),
            seller_owner_revision=int(row[2]),
            buyer_player_id=int(row[3]),
            buyer_account=_to_str(row[4]),
            buyer_ip=_to_str(row[5]),
            buyer_device_id=_to_str(row[6]),
            rating=int(row[7]),
            counted_for_score=bool(row[8]),
            comment=_to_str(row[9]),
            created_at_ms=int(row[10]),
        )

    async def count_pair_scored_reviews(
        self,
        key: SellerCreditKey,
        buyer_key: str,
        buyer_ip: str,
        buyer_device_id: str,
        since_ms: int,
        now_ms: int,
    ) -> int:
        row = await self._query_one(
            "SELECT COUNT(*) FROM `trade_seller_review` "
            "WHERE `seller_player_id`=%s AND `seller_owner_revision`=%s AND `counted_for_score`=1 "
            "AND `created_ms`>=%s AND `created_ms`<=%s "
            "AND (`buyer_account`=%s OR (%s<>'' AND `buyer_ip`=%s) OR (%s<>'' AND `buyer_device_id`=%s))",
            (
                int(key.seller_player_id),
                int(key.owner_revision),
                int(since_ms),
                int(now_ms),
                str(buyer_key),
                str(buyer_ip),
                str(buyer_ip),
                str(buyer_device_id),
                str(buyer_device_id),
            ),
        )
        return int(row[0] or 0) if row is not None else 0

    async def save_seller_review(self, rec: SellerReviewRecord) -> None:
        review_id = await self._next_credit_id()
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO `trade_seller_review` "
                    "(`review_id`,`order_id`,`seller_player_id`,`seller_owner_revision`,`buyer_player_id`,"
                    "`buyer_account`,`buyer_ip`,`buyer_device_id`,`rating`,`counted_for_score`,`comment`,`created_ms`) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        int(review_id),
                        str(rec.order_id),
                        int(rec.seller_player_id),
                        int(rec.seller_owner_revision),
                        int(rec.buyer_player_id),
                        str(rec.buyer_account),
                        str(rec.buyer_ip),
                        str(rec.buyer_device_id),
                        int(rec.rating),
                        1 if rec.counted_for_score else 0,
                        str(rec.comment),
                        int(rec.created_at_ms),
                    ),
                )
            await conn.commit()

    async def has_seller_violation(self, case_id: str) -> bool:
        row = await self._query_one(
            "SELECT 1 FROM `trade_seller_violation` WHERE `case_id`=%s LIMIT 1",
            (str(case_id),),
        )
        return row is not None

    async def save_seller_violation(self, rec: SellerViolationRecord) -> None:
        violation_id = await self._next_credit_id()
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO `trade_seller_violation` "
                    "(`violation_id`,`case_id`,`order_id`,`seller_player_id`,`seller_owner_revision`,"
                    "`seller_account`,`reason`,`recorded_ms`) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        int(violation_id),
                        str(rec.case_id),
                        str(rec.order_id),
                        int(rec.seller_player_id),
                        int(rec.seller_owner_revision),
                        str(rec.seller_account),
                        str(rec.reason),
                        int(rec.recorded_at_ms),
                    ),
                )
            await conn.commit()

    async def list_settled_market_fills(
        self,
        item_config_id: int,
        currency_type: int,
        floor_ms: int,
        now_ms: int,
        limit: int = MAX_REFERENCE_SCAN_ROWS,
    ) -> list[MarketFillSample]:
        rows = await self._query(
            "SELECT `unit_price`,`quantity`,`updated_ms`,`workflow_proto` FROM `trade_commerce` "
            "WHERE `kind`=4 AND `side`=0 AND `status`=%s AND `item_config_id`=%s AND `currency_type`=%s "
            "AND `updated_ms`>=%s AND `updated_ms`<=%s ORDER BY `updated_ms` DESC,`commerce_id` DESC LIMIT %s",
            (COMMERCE_SETTLED, int(item_config_id), int(currency_type), int(floor_ms), int(now_ms), int(limit)),
        )
        samples: list[MarketFillSample] = []
        for row in rows:
            raw_wf = _to_bytes(row[3])
            seller_acct = ""
            buyer_acct = ""
            if raw_wf:
                w = _decode_workflow(self.protocol, raw_wf)
                if w is not None:
                    if w.HasField("seller_identity"):
                        seller_acct = str(w.seller_identity.account)
                    if w.HasField("buyer_identity"):
                        buyer_acct = str(w.buyer_identity.account)
            samples.append(
                MarketFillSample(
                    item_config_id=int(item_config_id),
                    currency_type=int(currency_type),
                    unit_price=int(row[0]),
                    quantity=int(row[1]),
                    seller_account=seller_acct,
                    buyer_account=buyer_acct,
                    settled_at_ms=int(row[2]),
                )
            )
        return samples

    async def daily_player_trade_currency_outflow(
        self,
        player_id: int,
        currency_type: int,
        day_start_ms: int,
    ) -> int:
        async def _sum_side(col: str, *, as_seller: bool) -> int:
            rows = await self._query(
                f"SELECT `workflow_proto` FROM `trade_commerce` "
                f"WHERE `{col}`=%s AND `kind`=2 AND `status` IN (1,3,4,5,10,11) AND `created_ms`>=%s "
                "ORDER BY `commerce_id` DESC LIMIT %s",
                (int(player_id), int(day_start_ms), MAX_DAILY_PLAYER_TRADE_SCAN_ROWS),
            )
            if len(rows) >= MAX_DAILY_PLAYER_TRADE_SCAN_ROWS:
                return UINT64_MAX
            total = 0
            for row in rows:
                part = workflow_currency_outflow(
                    self.protocol, _to_bytes(row[0]), int(currency_type), as_seller=as_seller
                )
                total = saturating_add_uint64(total, part)
            return total

        seller_sum = await _sum_side("seller_player_id", as_seller=True)
        buyer_sum = await _sum_side("buyer_player_id", as_seller=False)
        return saturating_add_uint64(seller_sum, buyer_sum)
