"""mail MySQL/TiDB 存储实现（SqlMailStore）。

写事务固定运行在 ``READ-COMMITTED`` 隔离级别下（对 1205/1213/9007 瞬态冲突做有界重试）；
``ListMails`` 只读事务运行在 ``REPEATABLE-READ`` 下以保证计数与分页共享同一快照；
全部二进制列通过 ``UNHEX(%s)`` 绑定，防止 utf8mb4 连接损坏原始字节。
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
from .errors import (
    MailError,
    MailTooManyPendingError,
    StoreUnavailableError,
)
from .model import (
    CAMPAIGN_SEALED,
    CLAIM_PENDING,
    CLAIM_UNCLAIMED,
    FRESH_ATTEMPT_LIMIT,
    MailAssetOpRow,
    MailCampaignRow,
    MailCampaignShardRow,
    MailLimits,
    MailRow,
    MailStateRow,
    OP_PENDING,
    RoleFenceRow,
    valid_role_fence_state,
)
from .store import (
    GuildDirectory,
    MailStoreCore,
    RoleFenceDirectory,
    _DuplicateKeyError,
    _TxRollbackReturn,
)

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "mail_schema.sql"
DB_NOW_MS = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)"
LOCK_ERRORS = (1205, 1213, 9007)
DUP_ENTRY = 1062
TX_MAX_ATTEMPTS = 3

_MESSAGE_COLUMNS = (
    "`mail_id`,`owner_player_id`,`guild_id`,`kind`,`sender_kind`,`request_id`,"
    "`request_hash`,`title`,`body`,`attachments`,`sent_ms`,`expire_ms`,`campaign_id`,`manifest_generation`"
)
_CAMPAIGN_COLUMNS = (
    "`sender_kind`,`campaign_id`,`manifest_generation`,`mail_id`,`request_id`,`producer`,"
    "`request_hash`,`title`,`body`,`attachments`,`expire_ms`,`expected_recipient_count`,"
    "`expected_shard_count`,`manifest_hash`,`state`,`created_ms`,`sealed_ms`,`kind`,`guild_id`"
)
_STATE_COLUMNS = (
    "COALESCE(s.`read_ms`,0),COALESCE(s.`deleted_ms`,0),"
    "COALESCE(s.`claim_state`,IF(LENGTH(m.`attachments`)>0,1,0)),"
    "COALESCE(s.`current_op_id`,0),COALESCE(s.`claim_version`,0),"
    "COALESCE(s.`reason_tip_id`,0),COALESCE(s.`updated_ms`,0)"
)
_VISIBLE_PREDICATE = (
    "(m.`kind` IN (1,2,3) AND (m.`owner_player_id`=%s OR (m.`owner_player_id`=0 AND ("
    "(m.`campaign_id`>0 AND EXISTS (SELECT 1 FROM `mail_campaign_recipient` q "
    "WHERE q.`sender_kind`=m.`sender_kind` AND q.`campaign_id`=m.`campaign_id` "
    "AND q.`manifest_generation`=m.`manifest_generation` AND q.`player_id`=%s)) "
    "OR (m.`kind`=2 AND m.`campaign_id`=0) "
    "OR (m.`kind`=3 AND m.`campaign_id`=0 AND m.`guild_id`=%s)))))"
)
_SWEEP_ELIGIBLE = (
    "m.`expire_ms`>0 AND m.`expire_ms`<%s AND m.`campaign_id`=0 AND m.`kind` IN (1,2,3) "
    "AND NOT EXISTS(SELECT 1 FROM `mail_state` s WHERE s.`mail_id`=m.`mail_id` "
    "AND (s.`claim_state` IS NULL OR s.`claim_state` NOT IN (0,1,3))) "
    "AND NOT EXISTS(SELECT 1 FROM `mail_asset_op` o WHERE o.`mail_id`=m.`mail_id` "
    "AND (o.`status` IS NULL OR o.`status` NOT IN (2,3,5)))"
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


def _prefixed(columns: str, prefix: str) -> str:
    return ",".join(f"{prefix}{col}" for col in columns.split(","))


def _scan_message(row) -> MailRow:
    return MailRow(
        mail_id=int(row[0]),
        owner_player_id=int(row[1]),
        guild_id=int(row[2]),
        kind=int(row[3]),
        sender_kind=int(row[4]),
        request_id=int(row[5]),
        request_hash=_to_str(row[6]),
        title=_to_str(row[7]),
        body=_to_str(row[8]),
        attachments=_to_bytes(row[9]),
        sent_ms=int(row[10]),
        expire_ms=int(row[11]),
        campaign_id=int(row[12]),
        manifest_generation=int(row[13]),
    )


def _scan_campaign(row) -> MailCampaignRow:
    return MailCampaignRow(
        sender_kind=int(row[0]),
        campaign_id=int(row[1]),
        manifest_generation=int(row[2]),
        mail_id=int(row[3]),
        request_id=int(row[4]),
        producer=_to_str(row[5]),
        request_hash=_to_str(row[6]),
        title=_to_str(row[7]),
        body=_to_str(row[8]),
        attachments=_to_bytes(row[9]),
        expire_ms=int(row[10]),
        expected_recipient_count=int(row[11]),
        expected_shard_count=int(row[12]),
        manifest_hash=_to_str(row[13]),
        state=int(row[14]),
        created_ms=int(row[15]),
        sealed_ms=int(row[16]),
        kind=int(row[17]),
        guild_id=int(row[18]),
    )


def _scan_state(player_id: int, mail_id: int, row) -> MailStateRow:
    return MailStateRow(
        player_id=player_id,
        mail_id=mail_id,
        read_ms=int(row[0]),
        deleted_ms=int(row[1]),
        claim_state=int(row[2]),
        current_op_id=int(row[3]),
        claim_version=int(row[4]),
        reason_tip_id=int(row[5]),
        updated_ms=int(row[6]),
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


def _shard_sql(column: str, owned_shards: frozenset[int] | None) -> tuple[str, list[int]]:
    if owned_shards is None:
        return "1=1", []
    if not owned_shards:
        return "1=0", []
    ordered = sorted(owned_shards)
    marks = ",".join(["%s"] * len(ordered))
    return f"MOD({column}, 64) IN ({marks})", ordered


class _SqlTx:
    """单 MySQL/TiDB 事务内游标句柄（所有 bytes 均经 UNHEX(%s) 绑定）。"""

    def __init__(self, cursor) -> None:
        self._cur = cursor

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        await self._cur.execute(
            "SELECT account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms "
            "FROM `role_fence` WHERE player_id=%s FOR UPDATE",
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

    async def has_pending_asset_op(self, player_id: int) -> bool:
        await self._cur.execute(
            "SELECT `op_id` FROM `mail_asset_op` WHERE `player_id`=%s "
            "AND (`status` NOT IN (2,3,5) OR `status` IS NULL) LIMIT 1 FOR UPDATE",
            (player_id,),
        )
        return await self._cur.fetchone() is not None

    async def purge_player_rows(self, player_id: int) -> None:
        await self._cur.execute("SELECT `player_id` FROM `mail_box` WHERE `player_id`=%s FOR UPDATE", (player_id,))
        await self._cur.fetchone()
        steps = (
            ("SELECT `mail_id` FROM `mail_message` WHERE `owner_player_id`=%s ORDER BY `mail_id`",
             "DELETE FROM `mail_message` WHERE `mail_id`=%s"),
            ("SELECT `player_id`,`mail_id` FROM `mail_state` WHERE `player_id`=%s ORDER BY `mail_id`",
             "DELETE FROM `mail_state` WHERE `player_id`=%s AND `mail_id`=%s"),
            ("SELECT `player_id`,`stream` FROM `mail_player_op_seq` WHERE `player_id`=%s ORDER BY `stream`",
             "DELETE FROM `mail_player_op_seq` WHERE `player_id`=%s AND `stream`=%s"),
            ("SELECT q.`sender_kind`,q.`campaign_id`,q.`manifest_generation`,q.`player_id` "
             "FROM `mail_campaign` c JOIN `mail_campaign_recipient` q "
             "ON q.`sender_kind`=c.`sender_kind` AND q.`campaign_id`=c.`campaign_id` "
             "AND q.`manifest_generation`=c.`manifest_generation` AND q.`player_id`=%s "
             f"WHERE c.`state`={CAMPAIGN_SEALED} ORDER BY q.`sender_kind`,q.`campaign_id`,q.`manifest_generation`",
             "DELETE FROM `mail_campaign_recipient` WHERE `sender_kind`=%s AND `campaign_id`=%s "
             "AND `manifest_generation`=%s AND `player_id`=%s"),
            ("SELECT `player_id` FROM `mail_box` WHERE `player_id`=%s",
             "DELETE FROM `mail_box` WHERE `player_id`=%s"),
        )
        for select_sql, delete_sql in steps:
            await self._cur.execute(select_sql, (player_id,))
            keys = list(await self._cur.fetchall())
            for key in keys:
                await self._cur.execute(delete_sql, tuple(int(v) for v in key))

    async def ensure_mail_box(self, player_id: int, now_ms: int) -> None:
        await self._cur.execute(
            "INSERT IGNORE INTO `mail_box` (`player_id`,`created_ms`) VALUES (%s,%s)",
            (player_id, now_ms),
        )

    async def lock_mail_box(self, player_id: int) -> None:
        await self._cur.execute("SELECT `player_id` FROM `mail_box` WHERE `player_id`=%s FOR UPDATE", (player_id,))
        if await self._cur.fetchone() is None:
            raise StoreUnavailableError(f"mail_box missing player={player_id}")

    async def get_message_by_request(self, sender_kind: int, request_id: int) -> MailRow | None:
        await self._cur.execute(
            f"SELECT {_MESSAGE_COLUMNS} FROM `mail_message` WHERE `sender_kind`=%s AND `request_id`=%s",
            (sender_kind, request_id),
        )
        row = await self._cur.fetchone()
        return _scan_message(row) if row is not None else None

    async def get_message_by_id(self, mail_id: int) -> MailRow | None:
        await self._cur.execute(f"SELECT {_MESSAGE_COLUMNS} FROM `mail_message` WHERE `mail_id`=%s", (mail_id,))
        row = await self._cur.fetchone()
        return _scan_message(row) if row is not None else None

    async def get_visible_message(self, mail_id: int, player_id: int, guild_id: int) -> MailRow | None:
        cols = _prefixed(_MESSAGE_COLUMNS, "m.")
        await self._cur.execute(
            f"SELECT {cols} FROM `mail_message` m WHERE m.`mail_id`=%s AND {_VISIBLE_PREDICATE}",
            (mail_id, player_id, player_id, guild_id),
        )
        row = await self._cur.fetchone()
        return _scan_message(row) if row is not None else None

    async def count_personal_active_mails(self, player_id: int, now_ms: int) -> int:
        await self._cur.execute(
            "SELECT COUNT(*) FROM `mail_message` m JOIN `mail_state` s "
            "ON s.`mail_id`=m.`mail_id` AND s.`player_id`=m.`owner_player_id` "
            "WHERE m.`owner_player_id`=%s AND s.`deleted_ms`=0 "
            "AND (m.`expire_ms`>%s OR s.`claim_state` NOT IN (0,1,3))",
            (player_id, now_ms),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row else 0

    async def count_shared_active_mails(self, guild_id: int, now_ms: int) -> int:
        if guild_id == 0:
            await self._cur.execute(
                "SELECT COUNT(*) FROM `mail_message` WHERE `owner_player_id`=0 AND `guild_id`=0 "
                "AND `kind` IN (0,2) AND `expire_ms`>%s",
                (now_ms,),
            )
        else:
            await self._cur.execute(
                "SELECT COUNT(*) FROM `mail_message` WHERE `owner_player_id`=0 AND `guild_id`=%s "
                "AND `expire_ms`>%s",
                (guild_id, now_ms),
            )
        row = await self._cur.fetchone()
        return int(row[0]) if row else 0

    async def insert_message(self, row: MailRow) -> None:
        try:
            await self._cur.execute(
                f"INSERT INTO `mail_message` ({_MESSAGE_COLUMNS}) VALUES ("
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),%s,%s,%s,%s)",
                (
                    row.mail_id,
                    row.owner_player_id,
                    row.guild_id,
                    row.kind,
                    row.sender_kind,
                    row.request_id,
                    row.request_hash,
                    row.title,
                    row.body,
                    (row.attachments or b"").hex(),
                    row.sent_ms,
                    row.expire_ms,
                    row.campaign_id,
                    row.manifest_generation,
                ),
            )
        except pymysql.err.IntegrityError as exc:
            if _mysql_errno(exc) == DUP_ENTRY:
                raise _DuplicateKeyError("duplicate mail_message") from exc
            raise

    async def publish_campaign_message(self, mail_id: int, kind: int, sent_ms: int) -> None:
        await self._cur.execute(
            "UPDATE `mail_message` SET `kind`=%s,`sent_ms`=%s WHERE `mail_id`=%s AND `kind`=0",
            (kind, sent_ms, mail_id),
        )

    async def get_campaign_by_request(self, sender_kind: int, request_id: int) -> MailCampaignRow | None:
        await self._cur.execute(
            f"SELECT {_CAMPAIGN_COLUMNS} FROM `mail_campaign` WHERE `sender_kind`=%s AND `request_id`=%s LIMIT 1",
            (sender_kind, request_id),
        )
        row = await self._cur.fetchone()
        return _scan_campaign(row) if row is not None else None

    async def lock_campaign(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> MailCampaignRow | None:
        await self._cur.execute(
            f"SELECT {_CAMPAIGN_COLUMNS} FROM `mail_campaign` "
            "WHERE `sender_kind`=%s AND `campaign_id`=%s AND `manifest_generation`=%s FOR UPDATE",
            (sender_kind, campaign_id, manifest_generation),
        )
        row = await self._cur.fetchone()
        return _scan_campaign(row) if row is not None else None

    async def insert_campaign(self, row: MailCampaignRow) -> None:
        try:
            await self._cur.execute(
                f"INSERT INTO `mail_campaign` ({_CAMPAIGN_COLUMNS}) VALUES ("
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    row.sender_kind,
                    row.campaign_id,
                    row.manifest_generation,
                    row.mail_id,
                    row.request_id,
                    row.producer,
                    row.request_hash,
                    row.title,
                    row.body,
                    (row.attachments or b"").hex(),
                    row.expire_ms,
                    row.expected_recipient_count,
                    row.expected_shard_count,
                    row.manifest_hash,
                    row.state,
                    row.created_ms,
                    row.sealed_ms,
                    row.kind,
                    row.guild_id,
                ),
            )
        except pymysql.err.IntegrityError as exc:
            if _mysql_errno(exc) == DUP_ENTRY:
                raise _DuplicateKeyError("duplicate mail_campaign") from exc
            raise

    async def mark_campaign_sealed(
        self, sender_kind: int, campaign_id: int, manifest_generation: int, sealed_ms: int
    ) -> None:
        await self._cur.execute(
            f"UPDATE `mail_campaign` SET `state`={CAMPAIGN_SEALED},`sealed_ms`=%s "
            "WHERE `sender_kind`=%s AND `campaign_id`=%s AND `manifest_generation`=%s",
            (sealed_ms, sender_kind, campaign_id, manifest_generation),
        )

    async def get_campaign_shard(
        self, sender_kind: int, campaign_id: int, manifest_generation: int, shard_index: int
    ) -> MailCampaignShardRow | None:
        await self._cur.execute(
            "SELECT `shard_hash`,`recipient_count` FROM `mail_campaign_shard` "
            "WHERE `sender_kind`=%s AND `campaign_id`=%s AND `manifest_generation`=%s AND `shard_index`=%s",
            (sender_kind, campaign_id, manifest_generation, shard_index),
        )
        row = await self._cur.fetchone()
        if row is None:
            return None
        return MailCampaignShardRow(
            sender_kind=sender_kind,
            campaign_id=campaign_id,
            manifest_generation=manifest_generation,
            shard_index=shard_index,
            shard_hash=_to_str(row[0]),
            recipient_count=int(row[1]),
        )

    async def sum_campaign_shard_recipients(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> int:
        await self._cur.execute(
            "SELECT COALESCE(SUM(`recipient_count`),0) FROM `mail_campaign_shard` "
            "WHERE `sender_kind`=%s AND `campaign_id`=%s AND `manifest_generation`=%s",
            (sender_kind, campaign_id, manifest_generation),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row else 0

    async def list_campaign_shards(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> list[MailCampaignShardRow]:
        await self._cur.execute(
            "SELECT `shard_index`,`shard_hash`,`recipient_count` FROM `mail_campaign_shard` "
            "WHERE `sender_kind`=%s AND `campaign_id`=%s AND `manifest_generation`=%s ORDER BY `shard_index`",
            (sender_kind, campaign_id, manifest_generation),
        )
        return [
            MailCampaignShardRow(
                sender_kind=sender_kind,
                campaign_id=campaign_id,
                manifest_generation=manifest_generation,
                shard_index=int(r[0]),
                shard_hash=_to_str(r[1]),
                recipient_count=int(r[2]),
            )
            for r in await self._cur.fetchall()
        ]

    async def insert_campaign_recipients(
        self,
        sender_kind: int,
        campaign_id: int,
        manifest_generation: int,
        shard_index: int,
        player_ids: Sequence[int],
    ) -> None:
        if not player_ids:
            return
        values = ",".join(["(%s,%s,%s,%s,%s)"] * len(player_ids))
        args: list[int] = []
        for pid in player_ids:
            args.extend((sender_kind, campaign_id, manifest_generation, int(pid), shard_index))
        try:
            await self._cur.execute(
                "INSERT INTO `mail_campaign_recipient` "
                f"(`sender_kind`,`campaign_id`,`manifest_generation`,`player_id`,`shard_index`) VALUES {values}",
                tuple(args),
            )
        except pymysql.err.IntegrityError as exc:
            if _mysql_errno(exc) == DUP_ENTRY:
                raise _DuplicateKeyError("duplicate campaign recipient") from exc
            raise

    async def insert_campaign_shard(self, row: MailCampaignShardRow) -> None:
        try:
            await self._cur.execute(
                "INSERT INTO `mail_campaign_shard` "
                "(`sender_kind`,`campaign_id`,`manifest_generation`,`shard_index`,`shard_hash`,`recipient_count`) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    row.sender_kind,
                    row.campaign_id,
                    row.manifest_generation,
                    row.shard_index,
                    row.shard_hash,
                    row.recipient_count,
                ),
            )
        except pymysql.err.IntegrityError as exc:
            if _mysql_errno(exc) == DUP_ENTRY:
                raise _DuplicateKeyError("duplicate campaign shard") from exc
            raise

    async def count_campaign_recipients(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> int:
        await self._cur.execute(
            "SELECT COUNT(*) FROM `mail_campaign_recipient` "
            "WHERE `sender_kind`=%s AND `campaign_id`=%s AND `manifest_generation`=%s",
            (sender_kind, campaign_id, manifest_generation),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row else 0

    async def lock_mail_state(self, player_id: int, mail_id: int) -> MailStateRow | None:
        await self._cur.execute(
            "SELECT `read_ms`,`deleted_ms`,`claim_state`,`current_op_id`,`claim_version`,`reason_tip_id`,`updated_ms` "
            "FROM `mail_state` WHERE `player_id`=%s AND `mail_id`=%s FOR UPDATE",
            (player_id, mail_id),
        )
        row = await self._cur.fetchone()
        return _scan_state(player_id, mail_id, row) if row is not None else None

    async def insert_mail_state(self, row: MailStateRow) -> None:
        try:
            await self._cur.execute(
                "INSERT INTO `mail_state` (`player_id`,`mail_id`,`read_ms`,`deleted_ms`,`claim_state`,"
                "`current_op_id`,`claim_version`,`reason_tip_id`,`updated_ms`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    row.player_id,
                    row.mail_id,
                    row.read_ms,
                    row.deleted_ms,
                    row.claim_state,
                    row.current_op_id,
                    row.claim_version,
                    row.reason_tip_id,
                    row.updated_ms,
                ),
            )
        except pymysql.err.IntegrityError as exc:
            if _mysql_errno(exc) == DUP_ENTRY:
                raise _DuplicateKeyError("duplicate mail_state") from exc
            raise

    async def mark_mail_read(self, player_id: int, mail_id: int, now_ms: int) -> MailStateRow:
        await self._cur.execute(
            "UPDATE `mail_state` SET `read_ms`=IF(`read_ms`=0,%s,`read_ms`),`updated_ms`=%s "
            "WHERE `player_id`=%s AND `mail_id`=%s",
            (now_ms, now_ms, player_id, mail_id),
        )
        st = await self.lock_mail_state(player_id, mail_id)
        if st is None:
            raise StoreUnavailableError(f"mail_state disappeared player={player_id} mail={mail_id}")
        return st

    async def mark_mail_deleted(self, player_id: int, mail_id: int, now_ms: int) -> None:
        await self._cur.execute(
            "UPDATE `mail_state` SET `deleted_ms`=%s,`updated_ms`=%s WHERE `player_id`=%s AND `mail_id`=%s",
            (now_ms, now_ms, player_id, mail_id),
        )

    async def list_clearable_read_mail_ids(self, player_id: int, guild_id: int, limit: int) -> list[int]:
        await self._cur.execute(
            "SELECT m.`mail_id` FROM `mail_message` m JOIN `mail_state` s "
            "ON s.`mail_id`=m.`mail_id` AND s.`player_id`=%s "
            f"WHERE s.`read_ms`>0 AND s.`deleted_ms`=0 AND {_VISIBLE_PREDICATE} "
            "ORDER BY m.`mail_id` LIMIT %s",
            (player_id, player_id, player_id, guild_id, limit),
        )
        return [int(r[0]) for r in await self._cur.fetchall()]

    async def list_visible_counts(self, player_id: int, guild_id: int, now_ms: int) -> tuple[int, int]:
        base = (
            " FROM `mail_message` m LEFT JOIN `mail_state` s ON s.`player_id`=%s AND s.`mail_id`=m.`mail_id` "
            f"WHERE {_VISIBLE_PREDICATE} AND COALESCE(s.`deleted_ms`,0)=0 "
            "AND (m.`expire_ms`>%s OR s.`claim_state` NOT IN (0,1,3))"
        )
        await self._cur.execute(
            "SELECT COALESCE(SUM(COALESCE(s.`read_ms`,0)=0),0),"
            "COALESCE(SUM(m.`expire_ms`>%s AND COALESCE(s.`claim_state`,IF(LENGTH(m.`attachments`)>0,1,0))=1),0)"
            + base,
            (now_ms, player_id, player_id, player_id, guild_id, now_ms),
        )
        row = await self._cur.fetchone()
        return (int(row[0]), int(row[1])) if row else (0, 0)

    async def list_visible_page(
        self,
        player_id: int,
        guild_id: int,
        now_ms: int,
        before_sent_ms: int | None,
        before_mail_id: int | None,
        limit: int,
    ) -> list[tuple[MailRow, MailStateRow | None]]:
        cols = _prefixed(_MESSAGE_COLUMNS, "m.")
        query = (
            f"SELECT {cols},{_STATE_COLUMNS} FROM `mail_message` m "
            "LEFT JOIN `mail_state` s ON s.`player_id`=%s AND s.`mail_id`=m.`mail_id` "
            f"WHERE {_VISIBLE_PREDICATE} AND COALESCE(s.`deleted_ms`,0)=0 "
            "AND (m.`expire_ms`>%s OR s.`claim_state` NOT IN (0,1,3))"
        )
        args: list = [player_id, player_id, player_id, guild_id, now_ms]
        if before_sent_ms is not None and before_mail_id is not None:
            query += " AND (m.`sent_ms`<%s OR (m.`sent_ms`=%s AND m.`mail_id`<%s))"
            args.extend((before_sent_ms, before_sent_ms, before_mail_id))
        query += " ORDER BY m.`sent_ms` DESC,m.`mail_id` DESC LIMIT %s"
        args.append(limit)
        await self._cur.execute(query, tuple(args))
        out: list[tuple[MailRow, MailStateRow | None]] = []
        for row in await self._cur.fetchall():
            mail = _scan_message(row[:14])
            state = _scan_state(player_id, mail.mail_id, row[14:21])
            out.append((mail, state))
        return out

    async def ensure_seq_row(self, player_id: int, stream: int, now_ms: int) -> None:
        if now_ms == 0:
            raise StoreUnavailableError("seq epoch must be positive")
        await self._cur.execute(
            "INSERT IGNORE INTO `mail_player_op_seq` (`player_id`,`stream`,`next_seq`,`epoch`,`updated_ms`) "
            "VALUES (%s,%s,1,%s,%s)",
            (player_id, stream, now_ms, now_ms),
        )

    async def allocate_seq(
        self, player_id: int, stream: int, max_pending: int, max_span: int, now_ms: int
    ) -> tuple[int, int]:
        await self._cur.execute(
            "SELECT `next_seq`,`epoch` FROM `mail_player_op_seq` WHERE `player_id`=%s AND `stream`=%s FOR UPDATE",
            (player_id, stream),
        )
        row = await self._cur.fetchone()
        if row is None or int(row[1]) == 0:
            raise StoreUnavailableError(f"seq row missing or corrupt player={player_id} stream={stream}")
        next_seq, epoch = int(row[0]), int(row[1])
        # 普通读（非锁定），依靠 RC 每条语句新快照保证并发安全并消除二级索引死锁环。
        await self._cur.execute(
            "SELECT `seq` FROM `mail_asset_op` "
            "WHERE `player_id`=%s AND `stream`=%s AND `stream_epoch`=%s AND `status`=%s "
            "ORDER BY `seq` LIMIT %s",
            (player_id, stream, epoch, OP_PENDING, max_pending + 1),
        )
        pending = [int(r[0]) for r in await self._cur.fetchall()]
        if len(pending) >= max_pending:
            raise MailTooManyPendingError("too many pending ops")
        if pending and next_seq - pending[0] >= max_span:
            raise MailTooManyPendingError("pending op seq span exceeded")
        await self._cur.execute(
            "UPDATE `mail_player_op_seq` SET `next_seq`=`next_seq`+1,`updated_ms`=%s "
            "WHERE `player_id`=%s AND `stream`=%s",
            (now_ms, player_id, stream),
        )
        if self._cur.rowcount != 1:
            raise StoreUnavailableError("advance mail_player_op_seq affected != 1")
        return epoch, next_seq

    async def insert_asset_op(self, row: MailAssetOpRow) -> None:
        try:
            await self._cur.execute(
                "INSERT INTO `mail_asset_op` (`op_id`,`player_id`,`stream`,`seq`,`stream_epoch`,`mail_id`,"
                "`claim_version`,`status`,`payload`,`tx_type`,`attempts`,`next_attempt_ms`,`lease_until_ms`,"
                "`lease_token`,`last_outcome`,`last_reason`,`durable`,`reason_tip_id`,`created_ms`,`updated_ms`,"
                "`last_send_ms`,`resolved_by`,`resolve_reason`) VALUES ("
                "%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    row.op_id,
                    row.player_id,
                    row.stream,
                    row.seq,
                    row.stream_epoch,
                    row.mail_id,
                    row.claim_version,
                    row.status,
                    (row.payload or b"").hex(),
                    row.tx_type,
                    row.attempts,
                    row.next_attempt_ms,
                    row.lease_until_ms,
                    row.lease_token,
                    row.last_outcome,
                    row.last_reason,
                    row.durable,
                    row.reason_tip_id,
                    row.created_ms,
                    row.updated_ms,
                    row.last_send_ms,
                    row.resolved_by,
                    row.resolve_reason,
                ),
            )
        except pymysql.err.IntegrityError as exc:
            if _mysql_errno(exc) == DUP_ENTRY:
                raise _DuplicateKeyError("duplicate mail_asset_op") from exc
            raise

    async def cas_claim_state_pending(
        self, player_id: int, mail_id: int, op_id: int, new_version: int, old_version: int, now_ms: int
    ) -> bool:
        await self._cur.execute(
            f"UPDATE `mail_state` SET `claim_state`={CLAIM_PENDING},`current_op_id`=%s,`claim_version`=%s,"
            "`reason_tip_id`=0,`read_ms`=IF(`read_ms`=0,%s,`read_ms`),`updated_ms`=%s "
            f"WHERE `player_id`=%s AND `mail_id`=%s AND `claim_state`={CLAIM_UNCLAIMED} AND `claim_version`=%s",
            (op_id, new_version, now_ms, now_ms, player_id, mail_id, old_version),
        )
        return self._cur.rowcount == 1

    async def get_asset_op_mail_id(self, op_id: int) -> int | None:
        await self._cur.execute("SELECT `mail_id` FROM `mail_asset_op` WHERE `op_id`=%s", (op_id,))
        row = await self._cur.fetchone()
        return int(row[0]) if row is not None else None

    async def lock_asset_op(self, op_id: int) -> MailAssetOpRow | None:
        await self._cur.execute(
            "SELECT `op_id`,`player_id`,`stream`,`seq`,`stream_epoch`,`mail_id`,`claim_version`,`status`,"
            "`payload`,`tx_type`,`attempts`,`next_attempt_ms`,`lease_until_ms`,`lease_token`,`last_outcome`,"
            "`last_reason`,`durable`,`reason_tip_id`,`created_ms`,`updated_ms`,`last_send_ms`,`resolved_by`,"
            "`resolve_reason` FROM `mail_asset_op` WHERE `op_id`=%s FOR UPDATE",
            (op_id,),
        )
        row = await self._cur.fetchone()
        return _scan_asset_op(row) if row is not None else None

    async def update_asset_op_reschedule(
        self,
        op_id: int,
        next_attempt_ms: int,
        durable: int,
        last_outcome: int,
        last_reason: int,
        now_ms: int,
        last_send_ms: int,
    ) -> None:
        await self._cur.execute(
            "UPDATE `mail_asset_op` SET `attempts`=`attempts`+1,`next_attempt_ms`=%s,`lease_until_ms`=0,"
            "`durable`=%s,`last_outcome`=%s,`last_reason`=%s,`updated_ms`=%s,"
            "`last_send_ms`=GREATEST(`last_send_ms`,%s) WHERE `op_id`=%s",
            (next_attempt_ms, durable, last_outcome, last_reason, now_ms, last_send_ms, op_id),
        )

    async def update_asset_op_final(
        self,
        op_id: int,
        final_status: int,
        last_outcome: int,
        last_reason: int,
        now_ms: int,
        reason_tip_id: int,
    ) -> None:
        await self._cur.execute(
            "UPDATE `mail_asset_op` SET `status`=%s,`durable`=1,`last_outcome`=%s,`last_reason`=%s,"
            "`next_attempt_ms`=%s,`lease_until_ms`=0,`updated_ms`=%s,`reason_tip_id`=%s WHERE `op_id`=%s",
            (final_status, last_outcome, last_reason, now_ms, now_ms, reason_tip_id, op_id),
        )

    async def update_mail_state_final(
        self,
        player_id: int,
        mail_id: int,
        op_id: int,
        claim_version: int,
        new_claim_state: int,
        reason_tip_id: int,
        now_ms: int,
    ) -> None:
        await self._cur.execute(
            "UPDATE `mail_state` SET `claim_state`=%s,`reason_tip_id`=%s,`updated_ms`=%s "
            f"WHERE `player_id`=%s AND `mail_id`=%s AND `current_op_id`=%s AND `claim_version`=%s "
            f"AND `claim_state`={CLAIM_PENDING}",
            (new_claim_state, reason_tip_id, now_ms, player_id, mail_id, op_id, claim_version),
        )

    async def sweep_one_mail(self, mail_id: int, cutoff_ms: int, remaining: int) -> tuple[int, int]:
        await self._cur.execute(
            f"SELECT m.`mail_id` FROM `mail_message` m WHERE m.`mail_id`=%s AND {_SWEEP_ELIGIBLE} FOR UPDATE",
            (mail_id, cutoff_ms),
        )
        if await self._cur.fetchone() is None:
            return 0, 0
        await self._cur.execute(
            "DELETE FROM `mail_state` WHERE `mail_id`=%s AND `claim_state` IN (0,1,3) LIMIT %s",
            (mail_id, remaining),
        )
        states_del = int(self._cur.rowcount)
        msg_del = 0
        if states_del < remaining:
            await self._cur.execute(
                "DELETE FROM `mail_message` WHERE `mail_id`=%s AND `expire_ms`>0 AND `expire_ms`<%s "
                "AND `campaign_id`=0 AND NOT EXISTS(SELECT 1 FROM `mail_state` s WHERE s.`mail_id`=%s) "
                "AND NOT EXISTS(SELECT 1 FROM `mail_asset_op` o WHERE o.`mail_id`=%s "
                "AND (o.`status` IS NULL OR o.`status` NOT IN (2,3,5)))",
                (mail_id, cutoff_ms, mail_id, mail_id),
            )
            msg_del = int(self._cur.rowcount)
        return states_del, msg_del


def _scan_asset_op(row) -> MailAssetOpRow:
    return MailAssetOpRow(
        op_id=int(row[0]),
        player_id=int(row[1]),
        stream=int(row[2]),
        seq=int(row[3]),
        stream_epoch=int(row[4]),
        mail_id=int(row[5]),
        claim_version=int(row[6]),
        status=int(row[7]),
        payload=_to_bytes(row[8]),
        tx_type=int(row[9]),
        attempts=int(row[10]),
        next_attempt_ms=int(row[11]),
        lease_until_ms=int(row[12]),
        lease_token=int(row[13]),
        last_outcome=int(row[14]),
        last_reason=int(row[15]),
        durable=int(row[16]),
        reason_tip_id=int(row[17]),
        created_ms=int(row[18]),
        updated_ms=int(row[19]),
        last_send_ms=int(row[20]),
        resolved_by=_to_str(row[21]),
        resolve_reason=_to_str(row[22]),
    )


class _SqlDriver:
    """aiomysql 驱动：写事务固定 RC 重试，列表读事务固定 RR。"""

    def __init__(self, pool) -> None:
        self._pool = pool

    async def run_tx(self, work: Callable[[_SqlTx], Awaitable[T]]) -> T:
        for attempt in range(1, TX_MAX_ATTEMPTS + 1):
            try:
                return await self._run_tx_once(work, repeatable_read=False)
            except (MailError, _TxRollbackReturn):
                raise
            except pymysql.MySQLError as exc:
                if _mysql_errno(exc) in LOCK_ERRORS and attempt < TX_MAX_ATTEMPTS:
                    await asyncio.sleep(0.01 * attempt + random.uniform(0.0, 0.01))
                    continue
                raise StoreUnavailableError(f"mysql transaction failed: {exc}") from exc
        raise StoreUnavailableError("mysql transaction exhausted retries")

    async def run_rr_read_tx(self, work: Callable[[_SqlTx], Awaitable[T]]) -> T:
        try:
            return await self._run_tx_once(work, repeatable_read=True)
        except (MailError, _TxRollbackReturn):
            raise
        except pymysql.MySQLError as exc:
            raise StoreUnavailableError(f"mysql RR read transaction failed: {exc}") from exc

    async def _run_tx_once(self, work: Callable[[_SqlTx], Awaitable[T]], *, repeatable_read: bool) -> T:
        business_error: BaseException | None = None
        rollback_return: _TxRollbackReturn | None = None
        async with portal_sql.connection(self._pool) as conn:
            if repeatable_read:
                async with conn.cursor() as iso_cur:
                    await iso_cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            await conn.begin()
            try:
                async with conn.cursor() as cursor:
                    result = await work(_SqlTx(cursor))
                await conn.commit()
                return result
            except _TxRollbackReturn as ret:
                await conn.rollback()
                rollback_return = ret
            except MailError as exc:
                await conn.rollback()
                business_error = exc
        if rollback_return is not None:
            return rollback_return.value
        assert business_error is not None
        raise business_error

    async def read_role_fence(self, player_id: int) -> RoleFenceRow | None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "SELECT account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms "
                "FROM `role_fence` WHERE player_id=%s",
                (player_id,),
            )
            row = await cursor.fetchone()
            return _scan_role_fence(player_id, row) if row is not None else None

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

    async def ensure_mail_box_direct(self, player_id: int, now_ms: int) -> None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "INSERT IGNORE INTO `mail_box` (`player_id`,`created_ms`) VALUES (%s,%s)",
                (player_id, now_ms),
            )

    async def get_message_by_request_direct(self, sender_kind: int, request_id: int) -> MailRow | None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {_MESSAGE_COLUMNS} FROM `mail_message` WHERE `sender_kind`=%s AND `request_id`=%s",
                (sender_kind, request_id),
            )
            row = await cursor.fetchone()
            return _scan_message(row) if row is not None else None

    async def get_campaign_direct(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> MailCampaignRow | None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT {_CAMPAIGN_COLUMNS} FROM `mail_campaign` "
                "WHERE `sender_kind`=%s AND `campaign_id`=%s AND `manifest_generation`=%s",
                (sender_kind, campaign_id, manifest_generation),
            )
            row = await cursor.fetchone()
            return _scan_campaign(row) if row is not None else None

    async def list_due_ops(
        self, now_ms: int, limit: int, owned_shards: frozenset[int] | None = None
    ) -> list[int]:
        shard_expr, shard_args = _shard_sql("`player_id`", owned_shards)
        ids: list[int] = []
        seen: set[int] = set()
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for cmp_op in ("<", ">="):
                remaining = limit - len(ids)
                if remaining <= 0:
                    break
                await cursor.execute(
                    f"SELECT `op_id` FROM `mail_asset_op` WHERE `status`={OP_PENDING} "
                    f"AND `next_attempt_ms`<=%s AND `lease_until_ms`<%s AND `attempts` {cmp_op} %s "
                    f"AND {shard_expr} ORDER BY `next_attempt_ms`,`op_id` LIMIT %s",
                    (now_ms, now_ms, FRESH_ATTEMPT_LIMIT, *shard_args, remaining),
                )
                for row in await cursor.fetchall():
                    oid = int(row[0])
                    if oid not in seen:
                        seen.add(oid)
                        ids.append(oid)
        return ids

    async def claim_op_direct(
        self, op_id: int, now_ms: int, lease_until_ms: int, lease_token: int
    ) -> MailAssetOpRow | None:
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE `mail_asset_op` SET `last_send_ms`=GREATEST(`last_send_ms`,`lease_until_ms`),"
                "`lease_until_ms`=%s,`lease_token`=%s "
                f"WHERE `op_id`=%s AND `status`={OP_PENDING} AND `lease_until_ms`<%s",
                (lease_until_ms, lease_token, op_id, now_ms),
            )
            if cursor.rowcount == 0:
                return None
            await cursor.execute(
                "SELECT `op_id`,`player_id`,`stream`,`seq`,`stream_epoch`,`mail_id`,`claim_version`,`status`,"
                "`payload`,`tx_type`,`attempts`,`next_attempt_ms`,`lease_until_ms`,`lease_token`,`last_outcome`,"
                "`last_reason`,`durable`,`reason_tip_id`,`created_ms`,`updated_ms`,`last_send_ms`,`resolved_by`,"
                "`resolve_reason` FROM `mail_asset_op` WHERE `op_id`=%s",
                (op_id,),
            )
            row = await cursor.fetchone()
            return _scan_asset_op(row) if row is not None else None

    async def list_sweep_candidates(
        self, cutoff_ms: int, limit: int, owned_shards: frozenset[int] | None = None
    ) -> tuple[list[tuple[int, int]], int]:
        shard_expr, shard_args = _shard_sql("m.`mail_id`", owned_shards)
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT m.`mail_id`,m.`kind` FROM `mail_message` m WHERE {_SWEEP_ELIGIBLE} "
                f"AND {shard_expr} ORDER BY m.`expire_ms`,m.`mail_id` LIMIT %s",
                (cutoff_ms, *shard_args, limit),
            )
            candidates = [(int(r[0]), int(r[1])) for r in await cursor.fetchall()]
            await cursor.execute(
                f"SELECT COUNT(*) FROM (SELECT m.`expire_ms` FROM `mail_message` m WHERE {shard_expr} "
                "ORDER BY m.`mail_id` LIMIT %s) sample WHERE `expire_ms`=0",
                (*shard_args, limit),
            )
            inv_row = await cursor.fetchone()
            return candidates, (int(inv_row[0]) if inv_row else 0)


class SqlMailStore(MailStoreCore):
    """基于 MySQL/TiDB 的 mail 存储，带建表、结构验表与索引自检。"""

    def __init__(
        self,
        pool,
        protocol: Protocol,
        *,
        mail_ids: Callable[[], Awaitable[int]],
        op_ids: Callable[[], Awaitable[int]],
        item_exists: Callable[[int], bool],
        guilds: GuildDirectory | None = None,
        role_directory: RoleFenceDirectory | None = None,
        limits: MailLimits = MailLimits(),
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._pool = pool
        super().__init__(
            protocol,
            _SqlDriver(pool),
            mail_ids=mail_ids,
            op_ids=op_ids,
            item_exists=item_exists,
            guilds=guilds,
            role_directory=role_directory,
            limits=limits,
            clock_ms=clock_ms,
        )

    async def bootstrap(self, schema: Path = SCHEMA_FILE) -> None:
        """幂等执行 config/mail_schema.sql 中的全部建表语句。"""
        text = "\n".join(line for line in schema.read_text("utf-8").splitlines() if not line.startswith("--"))
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for statement in re.split(r";\s*\n", text):
                sql = statement.strip().rstrip(";")
                if sql:
                    await cursor.execute(sql)

    async def verify_schema(self) -> None:
        """启动前核对全部 8 张 mail_* 表、role_fence 结构与关键索引（fail-closed）。"""
        checks = (
            f"SELECT {_MESSAGE_COLUMNS} FROM `mail_message` LIMIT 0",
            f"SELECT {_CAMPAIGN_COLUMNS} FROM `mail_campaign` LIMIT 0",
            "SELECT `sender_kind`,`campaign_id`,`manifest_generation`,`shard_index`,`shard_hash`,`recipient_count` "
            "FROM `mail_campaign_shard` LIMIT 0",
            "SELECT `sender_kind`,`campaign_id`,`manifest_generation`,`player_id`,`shard_index` "
            "FROM `mail_campaign_recipient` LIMIT 0",
            "SELECT `player_id`,`created_ms` FROM `mail_box` LIMIT 0",
            "SELECT `player_id`,`mail_id`,`read_ms`,`deleted_ms`,`claim_state`,`current_op_id`,"
            "`claim_version`,`reason_tip_id`,`updated_ms` FROM `mail_state` LIMIT 0",
            "SELECT `player_id`,`stream`,`next_seq`,`updated_ms`,`epoch` FROM `mail_player_op_seq` LIMIT 0",
            "SELECT `op_id`,`player_id`,`stream`,`seq`,`stream_epoch`,`mail_id`,`claim_version`,`status`,"
            "`payload`,`tx_type`,`attempts`,`next_attempt_ms`,`lease_until_ms`,`lease_token`,`last_outcome`,"
            "`last_reason`,`durable`,`reason_tip_id`,`created_ms`,`updated_ms`,`last_send_ms`,`resolved_by`,"
            "`resolve_reason` FROM `mail_asset_op` LIMIT 0",
            "SELECT `player_id`,`account`,`owner_revision`,`freeze_token`,`freeze_final`,`version`,`home_zone`,"
            "`updated_ms` FROM `role_fence` LIMIT 0",
        )
        async with portal_sql.connection(self._pool) as conn, conn.cursor() as cursor:
            for sql in checks:
                await cursor.execute(sql)
            await cursor.execute(
                "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='role_fence' AND COLUMN_NAME='account'"
            )
            col = await cursor.fetchone()
            if col is None or str(col[0]).lower() != "varbinary" or int(col[1] or 0) != 191:
                raise RuntimeError("role_fence.account must be VARBINARY(191)")
            await cursor.execute(
                "SELECT index_name, column_name FROM information_schema.statistics "
                "WHERE table_schema=DATABASE() AND table_name='mail_asset_op' "
                "ORDER BY index_name, seq_in_index"
            )
            indexes: dict[str, list[str]] = {}
            for name, col_name in await cursor.fetchall():
                indexes.setdefault(str(name), []).append(str(col_name))
            if not any(
                cols == ["status", "next_attempt_ms", "op_id", "player_id"] for cols in indexes.values()
            ):
                raise RuntimeError("mail_asset_op missing 4-column idx_mail_asset_op_0")


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
