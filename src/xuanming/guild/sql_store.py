"""guild MySQL/TiDB 存储实现（SqlGuildStore）。

写事务固定运行在 ``READ-COMMITTED`` 隔离级别下（``innodb_lock_wait_timeout=1``，对 1213/9007 做有界重试，
1205 归一为 ``GuildWriteConflictError``）；全部二进制列通过 ``UNHEX(%s)`` 绑定，防止 utf8mb4 连接损坏原始字节；
``guild_member`` 上的锁定读与 UPDATE 固定带 ``FORCE INDEX (PRIMARY)``，与 Go 侧取锁计划完全一致。
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

from .. import portal_sql
from ..protocol import Protocol
from .errors import (
    GuildAssetPendingError,
    GuildWriteConflictError,
    StoreUnavailableError,
)
from .model import (
    ASSET_OP_FRESH_ATTEMPT_LIMIT,
    ASSET_OP_MAX_PENDING,
    ASSET_OP_MAX_SPAN,
    ASSET_STREAM_GUILD_CREDIT,
    ASSET_STREAM_GUILD_DEBIT,
    ActivityProgress,
    ActivityRewardStatus,
    ApplicantRow,
    ApplicationRow,
    AssetOpRow,
    COUNTER_KIND_ACTIVITY,
    GuildData,
    GuildTableCatalog,
    MemberData,
    MergedZones,
    OP_KIND_ACTIVITY_REWARD,
    OP_KIND_DONATE,
    OP_STATUS_ABANDONED,
    OP_STATUS_APPLIED,
    OP_STATUS_APPLIED_PARTIAL,
    OP_STATUS_PENDING,
    OP_STATUS_REJECTED,
    RANK_LEADER,
    REJECTED_REWARD_WINDOW_MS,
    ROLE_BLOCKER_ASSET_OP,
    ROLE_BLOCKER_GUILD_LEADER,
    ROLE_BLOCKER_GUILD_MEMBER,
    ROLE_BLOCKER_TRIAL_LAUNCH_ROLE,
    ROLE_BLOCKER_TRIAL_LOBBY_SLOT,
    ROLE_BLOCKER_TRIAL_REWARD_OWED,
    ROLE_BLOCKER_WAR_CREDIT_OWED,
    RoleFenceRow,
    TRIAL_BATTLE_STATE_STARTED,
    TRIAL_LAUNCH_ROLE_RELEASED,
    TRIAL_LAUNCH_STATE_PREPARED,
    TRIAL_LAUNCH_STATE_TERMINAL,
    TRIAL_LOBBY_STATE_PENDING,
    TrialBattleRecord,
    TrialLaunchRecord,
    TrialLaunchRoleRecord,
    TrialLobbyRecord,
    TrialLobbySlotRecord,
    TrialOwedRewardRecord,
    WarCreditOwed,
    WarCreditOwedCursor,
    is_removal_token,
    role_rank,
    valid_role_fence_state,
)
from .store import (
    GuildStoreCore,
    QuietWitness,
    RoleFenceDirectory,
    _DuplicateKeyError,
    _GuildLockRow,
    _TxCommitThen,
    _TxRollbackReturn,
)

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "guild_schema.sql"
DB_NOW_MS = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)"
RETRYABLE_ERRORS = (1213, 9007)
LOCK_WAIT_TIMEOUT_ERRNO = 1205
DUP_ENTRY_ERRNO = 1062
TX_MAX_ATTEMPTS = 3

# 全部核心表清单（启动期 verify_schema 校验）
REQUIRED_TABLES: tuple[str, ...] = (
    "role_fence",
    "guild",
    "guild_player_state",
    "guild_member",
    "guild_application",
    "guild_player_op_seq",
    "guild_asset_op",
    "guild_daily_counter",
    "guild_activity_progress",
    "guild_trial_lobby",
    "guild_trial_lobby_slot",
    "guild_trial_launch",
    "guild_trial_launch_role",
    "guild_trial_battle",
    "guild_trial_reward_owed",
    "guild_client_request",
    "guild_name_hold",
)

# SQL 常量（保留与 Go data 包一致的索引提示、列序与 UNHEX 绑定）
SQL_APPLY_ROLE_FENCE = (
    "INSERT INTO `role_fence`(player_id,account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms) "
    "VALUES (%s,UNHEX(%s),%s,%s,%s,%s,%s,%s) "
    "ON DUPLICATE KEY UPDATE "
    "account=IF(VALUES(version)>version,VALUES(account),account),"
    "owner_revision=IF(VALUES(version)>version,VALUES(owner_revision),owner_revision),"
    "freeze_token=IF(VALUES(version)>version,VALUES(freeze_token),freeze_token),"
    "freeze_final=IF(VALUES(version)>version,VALUES(freeze_final),freeze_final),"
    "home_zone=IF(VALUES(version)>version,VALUES(home_zone),home_zone),"
    "updated_ms=IF(VALUES(version)>version,VALUES(updated_ms),updated_ms),"
    "version=IF(VALUES(version)>version,VALUES(version),version)"
)

SQL_LOCK_MEMBER = (
    "SELECT `player_id`,`role`,`join_time_ms`,`last_active_ms`,`contribution_total`,`contribution_balance` "
    "FROM `guild_member` FORCE INDEX (PRIMARY) WHERE `guild_id`=%s AND `player_id`=%s FOR UPDATE"
)
SQL_UPDATE_MEMBER_ROLE = (
    "UPDATE `guild_member` FORCE INDEX (PRIMARY) SET `role`=%s WHERE `guild_id`=%s AND `player_id`=%s"
)
SQL_SET_MEMBER_CONTRIBUTION = (
    "UPDATE `guild_member` FORCE INDEX (PRIMARY) SET `contribution_total`=%s,`contribution_balance`=%s "
    "WHERE `guild_id`=%s AND `player_id`=%s"
)
SQL_DEBIT_MEMBER_CONTRIBUTION = (
    "UPDATE `guild_member` FORCE INDEX (PRIMARY) SET `contribution_balance`=`contribution_balance`-%s "
    "WHERE `guild_id`=%s AND `player_id`=%s AND `contribution_balance`>=%s"
)

SQL_ASSET_OP_COLUMNS = (
    "`op_id`,`player_id`,`stream`,`seq`,`guild_id`,`kind`,`status`,`durable`,"
    "`attempts`,`next_attempt_ms`,`deadline_ms`,`payload`,`ref_id`,`ref_count`,`period_key`,"
    "`contribution_delta`,`funds_delta`,`reason_tip_id`,`created_ms`,`updated_ms`,"
    "`lease_until_ms`,`lease_token`,`tx_type`,`last_outcome`,`last_reason`,`stream_epoch`,"
    "`resolved_by`,`resolve_reason`,`last_send_ms`"
)

SQL_INSERT_ASSET_OP = (
    f"INSERT INTO `guild_asset_op` ({SQL_ASSET_OP_COLUMNS}) VALUES ("
    "%s,%s,%s,%s,%s,%s,%s,%s,"
    "%s,%s,%s,UNHEX(%s),%s,%s,%s,"
    "%s,%s,%s,%s,%s,"
    "%s,%s,%s,%s,%s,%s,"
    "%s,%s,%s)"
)

SQL_UPSERT_COUNTER_WITH_LIMIT = (
    "INSERT INTO `guild_daily_counter` (`player_id`,`counter_kind`,`ref_id`,`period_key`,`used_count`,`updated_ms`) "
    "VALUES (%s,%s,%s,%s,%s,%s) "
    "ON DUPLICATE KEY UPDATE "
    "`updated_ms`=IF(`used_count`+%s<=%s,%s,`updated_ms`),"
    "`used_count`=IF(`used_count`+%s<=%s,`used_count`+%s,`used_count`)"
)

SQL_RESOLVE_ASSET_OP = (
    "UPDATE `guild_asset_op` SET `status`=%s,`resolved_by`=%s,`resolve_reason`=%s,"
    "`lease_until_ms`=0,`lease_token`=0,`next_attempt_ms`=%s,`updated_ms`=%s "
    "WHERE `op_id`=%s AND `status`=%s"
)
# 静默人工终结要求 lease_until_ms / last_send_ms / attempts 均与读证据时一致（对齐 Go asset_store.go）。
SQL_RESOLVE_ASSET_OP_QUIET = (
    SQL_RESOLVE_ASSET_OP + " AND `lease_until_ms`=%s AND `last_send_ms`=%s AND `attempts`=%s"
)
SQL_COUNT_WAR_CREDIT_OWED_CAPPED = (
    "SELECT COUNT(*) FROM (SELECT 1 FROM `guild_war_credit_owed` LIMIT %s) AS capped"
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


def _dup_key_name(exc: BaseException) -> str:
    text = str(exc)
    match = re.search(r"for key '([^']+)'", text)
    return match.group(1) if match else ""


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


def _scan_member(row) -> MemberData:
    return MemberData(
        player_id=int(row[0]),
        role=int(row[1]),
        join_time_ms=int(row[2]),
        last_active_ms=int(row[3]),
        contribution_total=int(row[4]),
        contribution_balance=int(row[5]),
    )


def _scan_asset_op(row) -> AssetOpRow:
    return AssetOpRow(
        op_id=int(row[0]),
        player_id=int(row[1]),
        stream=int(row[2]),
        seq=int(row[3]),
        guild_id=int(row[4]),
        kind=int(row[5]),
        status=int(row[6]),
        durable=int(row[7]),
        attempts=int(row[8]),
        next_attempt_ms=int(row[9]),
        deadline_ms=int(row[10]),
        payload=_to_bytes(row[11]),
        ref_id=int(row[12]),
        ref_count=int(row[13]),
        period_key=int(row[14]),
        contribution_delta=int(row[15]),
        funds_delta=int(row[16]),
        reason_tip_id=int(row[17]),
        created_ms=int(row[18]),
        updated_ms=int(row[19]),
        lease_until_ms=int(row[20]),
        lease_token=int(row[21]),
        tx_type=int(row[22]),
        last_outcome=int(row[23]),
        last_reason=int(row[24]),
        stream_epoch=int(row[25]),
        resolved_by=_to_str(row[26]),
        resolve_reason=_to_str(row[27]),
        last_send_ms=int(row[28]),
    )


def _scan_trial_lobby(row) -> TrialLobbyRecord:
    return TrialLobbyRecord(
        lobby_id=int(row[0]),
        guild_id=int(row[1]),
        activity_id=int(row[2]),
        initiator_player_id=int(row[3]),
        state=int(row[4]),
        view_proto=_to_bytes(row[5]),
        original_proto=_to_bytes(row[6]),
        consent_proto=_to_bytes(row[7]),
        expires_ms=int(row[8]),
        launch_deadline_ms=int(row[9]),
        battle_id=int(row[10]),
        created_ms=int(row[11]),
        updated_ms=int(row[12]),
    )


def _scan_trial_launch(row) -> TrialLaunchRecord:
    return TrialLaunchRecord(
        request_id=int(row[0]),
        battle_id=int(row[1]),
        state=int(row[2]),
        original_proto=_to_bytes(row[3]),
        request_digest=_to_bytes(row[4]),
        created_ms=int(row[5]),
        updated_ms=int(row[6]),
        cancel_receipt=_to_bytes(row[7]),
        next_check_ms=int(row[8]),
        check_attempts=int(row[9]),
        realtime_end_tip=_to_bytes(row[10]),
    )


def _scan_trial_battle(row) -> TrialBattleRecord:
    return TrialBattleRecord(
        battle_id=int(row[0]),
        guild_id=int(row[1]),
        activity_id=int(row[2]),
        period_key=int(row[3]),
        guild_period_key=int(row[4]),
        initiator_player_id=int(row[5]),
        state=int(row[6]),
        settle_result=int(row[7]),
        rewarded_count=int(row[8]),
        created_ms=int(row[9]),
        settled_ms=int(row[10]),
        launch_request_id=int(row[11]),
    )


def _scan_trial_reward_owed(row) -> TrialOwedRewardRecord:
    return TrialOwedRewardRecord(
        player_id=int(row[0]),
        battle_id=int(row[1]),
        guild_id=int(row[2]),
        activity_id=int(row[3]),
        period_key=int(row[4]),
        payload=_to_bytes(row[5]),
        attempts=int(row[6]),
        created_ms=int(row[7]),
    )


def load_schema_statements(schema_path: Path = SCHEMA_FILE) -> list[str]:
    """解析 config/guild_schema.sql 中的全部幂等 CREATE TABLE 语句。"""
    text = schema_path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("--")]
    cleaned = "\n".join(lines)
    return [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]


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
    """创建 aiomysql 连接池，固定 READ-COMMITTED 与 innodb_lock_wait_timeout=1，禁用 CLIENT_FOUND_ROWS。"""
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
        client_flag=0,
        init_command="SET SESSION transaction_isolation='READ-COMMITTED', innodb_lock_wait_timeout=1",
    )


class _SqlTx:
    """单 MySQL/TiDB 事务内游标句柄（所有 bytes 均经 UNHEX(%s) 绑定）。"""

    def __init__(self, cursor) -> None:
        self._cur = cursor

    # -- role_fence ------------------------------------------------------------------------------

    async def get_role_fence(self, player_id: int) -> RoleFenceRow | None:
        await self._cur.execute(
            "SELECT account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms "
            "FROM `role_fence` WHERE player_id=%s",
            (player_id,),
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_role_fence(player_id, row)

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        await self._cur.execute(
            "SELECT account,owner_revision,freeze_token,freeze_final,version,home_zone,updated_ms "
            "FROM `role_fence` WHERE player_id=%s FOR UPDATE",
            (player_id,),
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_role_fence(player_id, row)

    async def apply_role_fence(self, state: RoleFenceRow, now_ms: int) -> bool:
        await self._cur.execute(
            SQL_APPLY_ROLE_FENCE,
            (
                state.player_id,
                state.account.encode("utf-8").hex(),
                state.owner_revision,
                state.freeze_token,
                1 if state.freeze_final else 0,
                state.version,
                state.home_zone,
                now_ms,
            ),
        )
        return self._cur.rowcount > 0

    async def role_fence_pending_blocker(self, state: RoleFenceRow, *, dry_run: bool) -> str:
        pid = state.player_id
        await self._cur.execute("SELECT player_id FROM `guild_player_state` WHERE player_id=%s FOR UPDATE", (pid,))
        await self._cur.fetchone()

        await self._cur.execute("SELECT role FROM `guild_member` WHERE player_id=%s FOR UPDATE", (pid,))
        row = await self._cur.fetchone()
        if row is not None:
            role = int(row[0])
            if not is_removal_token(state.freeze_token):
                return ROLE_BLOCKER_GUILD_MEMBER
            if dry_run and role_rank(role) == RANK_LEADER:
                return ROLE_BLOCKER_GUILD_LEADER

        checks = (
            (
                ROLE_BLOCKER_ASSET_OP,
                "SELECT op_id FROM `guild_asset_op` WHERE player_id=%s AND (status NOT IN (2,3,4,6) OR status IS NULL) LIMIT 1 FOR UPDATE",
            ),
            (
                ROLE_BLOCKER_TRIAL_REWARD_OWED,
                "SELECT battle_id FROM `guild_trial_reward_owed` WHERE player_id=%s LIMIT 1 FOR UPDATE",
            ),
            (
                ROLE_BLOCKER_TRIAL_LAUNCH_ROLE,
                "SELECT r.request_id FROM `guild_trial_launch_role` r LEFT JOIN `guild_trial_launch` l "
                "ON l.request_id=r.request_id WHERE r.player_id=%s "
                "AND (r.state IS NULL OR r.state<>2 OR l.request_id IS NULL OR l.state IS NULL OR l.state<>3) "
                "LIMIT 1 FOR UPDATE",
            ),
            (
                ROLE_BLOCKER_TRIAL_LOBBY_SLOT,
                "SELECT s.player_id FROM `guild_trial_lobby_slot` s LEFT JOIN `guild_trial_lobby` l "
                "ON l.lobby_id=s.lobby_id WHERE s.player_id=%s "
                "AND (s.lobby_id IS NULL OR (s.lobby_id<>0 AND (l.lobby_id IS NULL OR l.state IS NULL OR l.state<>4))) "
                "LIMIT 1 FOR UPDATE",
            ),
            (
                ROLE_BLOCKER_WAR_CREDIT_OWED,
                "SELECT op_id FROM `guild_war_credit_owed` WHERE player_id=%s LIMIT 1 FOR UPDATE",
            ),
        )
        for blocker, sql in checks:
            await self._cur.execute(sql, (pid,))
            if await self._cur.fetchone() is not None:
                return blocker
        return ""

    # -- guild_player_state ----------------------------------------------------------------------

    async def ensure_player_state(self, player_id: int, now_ms: int) -> None:
        await self._cur.execute(
            "INSERT IGNORE INTO `guild_player_state` (`player_id`,`updated_ms`) VALUES (%s,%s)",
            (player_id, now_ms),
        )

    async def lock_player_state(self, player_id: int) -> bool:
        await self._cur.execute(
            "SELECT `player_id` FROM `guild_player_state` WHERE `player_id`=%s FOR UPDATE",
            (player_id,),
        )
        return await self._cur.fetchone() is not None

    async def delete_player_state(self, player_id: int) -> None:
        await self._cur.execute("DELETE FROM `guild_player_state` WHERE `player_id`=%s", (player_id,))

    # -- guild -----------------------------------------------------------------------------------

    async def load_guild(self, guild_id: int) -> GuildData | None:
        await self._cur.execute(
            "SELECT `guild_id`,`name`,`leader_id`,`level`,COALESCE(`announcement`,''),`create_time_ms`,"
            "`max_members`,`zone_id`,`score`,`funds`,`score_version`,`renamed_ms`,`name_norm` "
            "FROM `guild` WHERE `guild_id`=%s",
            (guild_id,),
        )
        row = await self._cur.fetchone()
        if row is None:
            return None
        members = await self.list_members(guild_id)
        return GuildData(
            guild_id=int(row[0]),
            name=_to_str(row[1]),
            leader_id=int(row[2]),
            level=int(row[3]),
            announcement=_to_str(row[4]),
            create_time_ms=int(row[5]),
            max_members=int(row[6]),
            zone_id=int(row[7]),
            score=int(row[8]),
            funds=int(row[9]),
            score_version=int(row[10]),
            renamed_ms=int(row[11]),
            name_norm=_to_str(row[12]),
            members=tuple(members),
        )

    async def lock_guild(self, guild_id: int) -> _GuildLockRow | None:
        await self._cur.execute(
            "SELECT `guild_id`,`name`,`name_norm`,`leader_id`,`level`,`max_members`,`zone_id`,"
            "`funds`,`score`,`score_version`,`renamed_ms` FROM `guild` WHERE `guild_id`=%s FOR UPDATE",
            (guild_id,),
        )
        row = await self._cur.fetchone()
        if row is None:
            return None
        return _GuildLockRow(
            guild_id=int(row[0]),
            name=_to_str(row[1]),
            name_norm=_to_str(row[2]),
            leader_id=int(row[3]),
            level=int(row[4]),
            max_members=int(row[5]),
            zone_id=int(row[6]),
            funds=int(row[7]),
            score=int(row[8]),
            score_version=int(row[9]),
            renamed_ms=int(row[10]),
        )

    async def get_guild_level(self, guild_id: int) -> int | None:
        await self._cur.execute("SELECT `level` FROM `guild` WHERE `guild_id`=%s", (guild_id,))
        row = await self._cur.fetchone()
        return None if row is None else int(row[0])

    async def insert_guild(self, guild: GuildData, name_norm: str) -> None:
        try:
            await self._cur.execute(
                "INSERT INTO `guild` (`guild_id`,`name`,`name_norm`,`leader_id`,`level`,`announcement`,"
                "`create_time_ms`,`max_members`,`zone_id`,`score`,`score_version`,`funds`,`renamed_ms`) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    guild.guild_id,
                    guild.name,
                    name_norm,
                    guild.leader_id,
                    guild.level,
                    guild.announcement,
                    guild.create_time_ms,
                    guild.max_members,
                    guild.zone_id,
                    guild.score,
                    guild.score_version,
                    guild.funds,
                    guild.renamed_ms,
                ),
            )
        except pymysql.MySQLError as exc:
            if _mysql_errno(exc) == DUP_ENTRY_ERRNO:
                raise _DuplicateKeyError(str(exc), _dup_key_name(exc)) from exc
            raise

    async def update_guild_announcement(self, guild_id: int, announcement: str) -> None:
        await self._cur.execute(
            "UPDATE `guild` SET `announcement`=%s WHERE `guild_id`=%s",
            (announcement, guild_id),
        )

    async def update_guild_name(self, guild_id: int, name: str, name_norm: str) -> None:
        try:
            await self._cur.execute(
                "UPDATE `guild` SET `name`=%s,`name_norm`=%s WHERE `guild_id`=%s",
                (name, name_norm, guild_id),
            )
        except pymysql.MySQLError as exc:
            if _mysql_errno(exc) == DUP_ENTRY_ERRNO:
                raise _DuplicateKeyError(str(exc), _dup_key_name(exc)) from exc
            raise

    async def deduct_guild_funds_and_stamp_rename(self, guild_id: int, cost_funds: int, now_ms: int) -> bool:
        await self._cur.execute(
            "UPDATE `guild` SET `funds`=`funds`-%s,`renamed_ms`=%s WHERE `guild_id`=%s AND `funds`>=%s",
            (cost_funds, now_ms, guild_id, cost_funds),
        )
        return self._cur.rowcount == 1

    async def update_guild_leader(self, guild_id: int, new_leader_id: int) -> None:
        await self._cur.execute(
            "UPDATE `guild` SET `leader_id`=%s WHERE `guild_id`=%s",
            (new_leader_id, guild_id),
        )

    async def update_guild_score(self, guild_id: int, score: int, new_version: int) -> None:
        await self._cur.execute(
            "UPDATE `guild` SET `score`=%s,`score_version`=%s WHERE `guild_id`=%s",
            (score, new_version, guild_id),
        )

    async def upgrade_guild_row(
        self, guild_id: int, cur_level: int, new_level: int, cost_funds: int, next_max_members: int
    ) -> bool:
        await self._cur.execute(
            "UPDATE `guild` SET `level`=%s,`funds`=`funds`-%s,`max_members`=%s "
            "WHERE `guild_id`=%s AND `level`=%s AND `funds`>=%s",
            (new_level, cost_funds, next_max_members, guild_id, cur_level, cost_funds),
        )
        return self._cur.rowcount == 1

    async def set_guild_funds(self, guild_id: int, new_funds: int) -> None:
        await self._cur.execute("UPDATE `guild` SET `funds`=%s WHERE `guild_id`=%s", (new_funds, guild_id))

    async def credit_guild_funds(self, guild_id: int, delta: int) -> bool:
        await self._cur.execute(
            "UPDATE `guild` SET `funds`=`funds`+%s WHERE `guild_id`=%s",
            (delta, guild_id),
        )
        return self._cur.rowcount == 1

    async def delete_guild(self, guild_id: int) -> bool:
        await self._cur.execute("DELETE FROM `guild` WHERE `guild_id`=%s", (guild_id,))
        return self._cur.rowcount == 1

    async def list_guild_zones(self) -> list[int]:
        await self._cur.execute("SELECT DISTINCT `zone_id` FROM `guild`")
        return [int(r[0]) for r in await self._cur.fetchall() if int(r[0]) > 0]

    async def list_rank_rows(self) -> list[tuple[int, int, int, int]]:
        await self._cur.execute("SELECT `guild_id`,`zone_id`,`score`,`score_version` FROM `guild`")
        return [(int(r[0]), int(r[1]), int(r[2]), int(r[3])) for r in await self._cur.fetchall()]

    # -- guild_name_hold -------------------------------------------------------------------------

    async def claim_name_hold(self, guild_id: int, name_norm: str, now_ms: int) -> bool:
        await self._cur.execute(
            "SELECT `hold_id`,`guild_id`,`held_until_ms` FROM `guild_name_hold` WHERE `name_norm`=%s FOR UPDATE",
            (name_norm,),
        )
        row = await self._cur.fetchone()
        if row is None:
            return True
        hold_id, holder_gid, held_until_ms = int(row[0]), int(row[1]), int(row[2])
        if holder_gid != guild_id and held_until_ms > now_ms:
            return False
        await self._cur.execute("DELETE FROM `guild_name_hold` WHERE `hold_id`=%s", (hold_id,))
        return True

    async def upsert_name_hold(self, guild_id: int, old_norm: str, held_until_ms: int, hold_id: int) -> None:
        await self._cur.execute(
            "SELECT `hold_id` FROM `guild_name_hold` WHERE `name_norm`=%s FOR UPDATE",
            (old_norm,),
        )
        row = await self._cur.fetchone()
        if row is not None:
            await self._cur.execute(
                "UPDATE `guild_name_hold` SET `guild_id`=%s,`held_until_ms`=%s WHERE `hold_id`=%s",
                (guild_id, held_until_ms, int(row[0])),
            )
            return
        await self._cur.execute(
            "INSERT INTO `guild_name_hold` (`hold_id`,`name_norm`,`guild_id`,`held_until_ms`) VALUES (%s,%s,%s,%s)",
            (hold_id, old_norm, guild_id, held_until_ms),
        )

    # -- guild_member ----------------------------------------------------------------------------

    async def get_player_guild_id(self, player_id: int) -> int:
        await self._cur.execute("SELECT `guild_id` FROM `guild_member` WHERE `player_id`=%s", (player_id,))
        row = await self._cur.fetchone()
        return 0 if row is None else int(row[0])

    async def get_member_role(self, guild_id: int, player_id: int) -> int | None:
        await self._cur.execute(
            "SELECT `role` FROM `guild_member` WHERE `guild_id`=%s AND `player_id`=%s",
            (guild_id, player_id),
        )
        row = await self._cur.fetchone()
        return None if row is None else int(row[0])

    async def get_member_contribution(self, guild_id: int, player_id: int) -> tuple[int, int] | None:
        await self._cur.execute(
            "SELECT `contribution_total`,`contribution_balance` FROM `guild_member` "
            "WHERE `guild_id`=%s AND `player_id`=%s",
            (guild_id, player_id),
        )
        row = await self._cur.fetchone()
        return None if row is None else (int(row[0]), int(row[1]))

    async def lock_member(self, guild_id: int, player_id: int) -> MemberData | None:
        await self._cur.execute(SQL_LOCK_MEMBER, (guild_id, player_id))
        row = await self._cur.fetchone()
        return None if row is None else _scan_member(row)

    async def list_member_ids(self, guild_id: int) -> list[int]:
        await self._cur.execute(
            "SELECT `player_id` FROM `guild_member` WHERE `guild_id`=%s ORDER BY `player_id`",
            (guild_id,),
        )
        return [int(r[0]) for r in await self._cur.fetchall()]

    async def list_members(self, guild_id: int) -> list[MemberData]:
        await self._cur.execute(
            "SELECT `player_id`,`role`,`join_time_ms`,`last_active_ms`,`contribution_total`,`contribution_balance` "
            "FROM `guild_member` WHERE `guild_id`=%s ORDER BY `player_id`",
            (guild_id,),
        )
        return [_scan_member(r) for r in await self._cur.fetchall()]

    async def count_members(self, guild_id: int) -> int:
        await self._cur.execute("SELECT COUNT(*) FROM `guild_member` WHERE `guild_id`=%s", (guild_id,))
        row = await self._cur.fetchone()
        return int(row[0]) if row else 0

    async def count_members_by_role(self, guild_id: int, role: int) -> int:
        await self._cur.execute(
            "SELECT COUNT(*) FROM `guild_member` WHERE `guild_id`=%s AND `role`=%s",
            (guild_id, role),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row else 0

    async def insert_member(self, guild_id: int, member: MemberData) -> None:
        try:
            await self._cur.execute(
                "INSERT INTO `guild_member` (`guild_id`,`player_id`,`role`,`join_time_ms`,`last_active_ms`,"
                "`contribution_total`,`contribution_balance`) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    guild_id,
                    member.player_id,
                    member.role,
                    member.join_time_ms,
                    member.last_active_ms,
                    member.contribution_total,
                    member.contribution_balance,
                ),
            )
        except pymysql.MySQLError as exc:
            if _mysql_errno(exc) == DUP_ENTRY_ERRNO:
                raise _DuplicateKeyError(str(exc), _dup_key_name(exc)) from exc
            raise

    async def update_member_role(self, guild_id: int, player_id: int, role: int) -> None:
        await self._cur.execute(SQL_UPDATE_MEMBER_ROLE, (role, guild_id, player_id))

    async def set_member_contribution(self, guild_id: int, player_id: int, total: int, balance: int) -> None:
        await self._cur.execute(SQL_SET_MEMBER_CONTRIBUTION, (total, balance, guild_id, player_id))

    async def credit_member_contribution(self, guild_id: int, player_id: int, delta: int) -> bool:
        await self._cur.execute(
            "UPDATE `guild_member` FORCE INDEX (PRIMARY) "
            "SET `contribution_total`=`contribution_total`+%s,`contribution_balance`=`contribution_balance`+%s "
            "WHERE `guild_id`=%s AND `player_id`=%s",
            (delta, delta, guild_id, player_id),
        )
        return self._cur.rowcount == 1

    async def refund_member_contribution(self, guild_id: int, player_id: int, delta: int) -> bool:
        await self._cur.execute(
            "UPDATE `guild_member` FORCE INDEX (PRIMARY) "
            "SET `contribution_balance`=`contribution_balance`+%s "
            "WHERE `guild_id`=%s AND `player_id`=%s",
            (delta, guild_id, player_id),
        )
        return self._cur.rowcount == 1

    async def debit_member_contribution(self, guild_id: int, player_id: int, cost: int) -> bool:
        await self._cur.execute(SQL_DEBIT_MEMBER_CONTRIBUTION, (cost, guild_id, player_id, cost))
        return self._cur.rowcount == 1

    async def delete_member(self, guild_id: int, player_id: int) -> bool:
        await self._cur.execute(
            "DELETE FROM `guild_member` WHERE `guild_id`=%s AND `player_id`=%s",
            (guild_id, player_id),
        )
        return self._cur.rowcount == 1

    async def get_trial_roster_members(self, guild_id: int, player_ids: Sequence[int]) -> dict[int, int]:
        ids = sorted({int(p) for p in player_ids if int(p) > 0})
        if not ids:
            return {}
        marks = ",".join(["%s"] * len(ids))
        await self._cur.execute(
            f"SELECT `player_id`,`join_time_ms` FROM `guild_member` WHERE `guild_id`=%s AND `player_id` IN ({marks})",
            (guild_id, *ids),
        )
        return {int(r[0]): int(r[1]) for r in await self._cur.fetchall()}

    # -- guild_application -----------------------------------------------------------------------

    async def lock_application_expire(self, guild_id: int, player_id: int) -> int | None:
        await self._cur.execute(
            "SELECT `expire_ms` FROM `guild_application` WHERE `guild_id`=%s AND `player_id`=%s FOR UPDATE",
            (guild_id, player_id),
        )
        row = await self._cur.fetchone()
        return None if row is None else int(row[0])

    async def insert_application(self, guild_id: int, player_id: int, apply_ms: int, expire_ms: int) -> bool:
        await self._cur.execute(
            "INSERT INTO `guild_application` (`guild_id`,`player_id`,`apply_ms`,`expire_ms`) "
            "VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE `apply_ms`=`apply_ms`",
            (guild_id, player_id, apply_ms, expire_ms),
        )
        return self._cur.rowcount == 1

    async def refresh_application(self, guild_id: int, player_id: int, apply_ms: int, expire_ms: int) -> None:
        await self._cur.execute(
            "UPDATE `guild_application` SET `apply_ms`=%s,`expire_ms`=%s WHERE `guild_id`=%s AND `player_id`=%s",
            (apply_ms, expire_ms, guild_id, player_id),
        )

    async def delete_application(self, guild_id: int, player_id: int) -> bool:
        await self._cur.execute(
            "DELETE FROM `guild_application` WHERE `guild_id`=%s AND `player_id`=%s",
            (guild_id, player_id),
        )
        return self._cur.rowcount == 1

    async def delete_expired_application(self, guild_id: int, player_id: int, now_ms: int) -> bool:
        await self._cur.execute(
            "DELETE FROM `guild_application` WHERE `guild_id`=%s AND `player_id`=%s AND `expire_ms`<=%s",
            (guild_id, player_id, now_ms),
        )
        return self._cur.rowcount == 1

    async def delete_live_application(self, guild_id: int, player_id: int, now_ms: int) -> bool:
        await self._cur.execute(
            "DELETE FROM `guild_application` WHERE `guild_id`=%s AND `player_id`=%s AND `expire_ms`>%s",
            (guild_id, player_id, now_ms),
        )
        return self._cur.rowcount == 1

    async def delete_applications_of_player(self, player_id: int) -> None:
        await self._cur.execute(
            "SELECT `guild_id` FROM `guild_application` WHERE `player_id`=%s ORDER BY `guild_id`",
            (player_id,),
        )
        gids = [int(r[0]) for r in await self._cur.fetchall()]
        for gid in gids:
            await self.delete_application(gid, player_id)

    async def delete_applications_of_guild(self, guild_id: int) -> None:
        await self._cur.execute(
            "SELECT `player_id` FROM `guild_application` WHERE `guild_id`=%s ORDER BY `player_id`",
            (guild_id,),
        )
        pids = [int(r[0]) for r in await self._cur.fetchall()]
        for pid in pids:
            await self.delete_application(guild_id, pid)

    async def purge_expired_applications_of_player(self, player_id: int, now_ms: int) -> None:
        await self._cur.execute(
            "SELECT `guild_id` FROM `guild_application` WHERE `player_id`=%s AND `expire_ms`<=%s ORDER BY `guild_id`",
            (player_id, now_ms),
        )
        gids = [int(r[0]) for r in await self._cur.fetchall()]
        for gid in gids:
            if await self.lock_application_expire(gid, player_id) is not None:
                await self.delete_expired_application(gid, player_id, now_ms)

    async def count_pending_applications_of_player(self, player_id: int, now_ms: int) -> int:
        await self._cur.execute(
            "SELECT COUNT(*) FROM `guild_application` WHERE `player_id`=%s AND `expire_ms`>%s",
            (player_id, now_ms),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row else 0

    async def count_live_applications_of_guild(self, guild_id: int, now_ms: int) -> int:
        await self._cur.execute(
            "SELECT COUNT(*) FROM `guild_application` a "
            "LEFT JOIN `guild_member` m ON m.`player_id`=a.`player_id` "
            "WHERE a.`guild_id`=%s AND a.`expire_ms`>%s AND m.`player_id` IS NULL",
            (guild_id, now_ms),
        )
        row = await self._cur.fetchone()
        return int(row[0]) if row else 0

    async def list_my_applications(self, player_id: int, now_ms: int, limit: int) -> list[ApplicationRow]:
        await self._cur.execute(
            "SELECT `guild_id`,`apply_ms`,`expire_ms` FROM `guild_application` "
            "WHERE `player_id`=%s AND `expire_ms`>%s ORDER BY `apply_ms` DESC, `guild_id` ASC LIMIT %s",
            (player_id, now_ms, limit),
        )
        return [ApplicationRow(guild_id=int(r[0]), apply_ms=int(r[1]), expire_ms=int(r[2])) for r in await self._cur.fetchall()]

    async def list_applicants(self, guild_id: int, now_ms: int, limit: int) -> list[ApplicantRow]:
        await self._cur.execute(
            "SELECT a.`player_id`,a.`apply_ms`,a.`expire_ms` FROM `guild_application` a "
            "LEFT JOIN `guild_member` m ON m.`player_id`=a.`player_id` "
            "WHERE a.`guild_id`=%s AND a.`expire_ms`>%s AND m.`player_id` IS NULL "
            "ORDER BY a.`apply_ms` ASC, a.`player_id` ASC LIMIT %s",
            (guild_id, now_ms, limit),
        )
        return [ApplicantRow(player_id=int(r[0]), apply_ms=int(r[1]), expire_ms=int(r[2])) for r in await self._cur.fetchall()]

    # -- asset ops & counters --------------------------------------------------------------------

    async def ensure_and_lock_seq(self, player_id: int, stream: int, now_ms: int) -> tuple[int, int]:
        await self._cur.execute(
            "INSERT IGNORE INTO `guild_player_op_seq` (`player_id`,`stream`,`next_seq`,`epoch`,`updated_ms`) "
            "VALUES (%s,%s,1,%s,%s)",
            (player_id, stream, now_ms, now_ms),
        )
        await self._cur.execute(
            "SELECT `next_seq`,`epoch` FROM `guild_player_op_seq` WHERE `player_id`=%s AND `stream`=%s FOR UPDATE",
            (player_id, stream),
        )
        row = await self._cur.fetchone()
        if row is None:
            raise StoreUnavailableError(f"guild_player_op_seq vanished player={player_id} stream={stream}")
        return int(row[0]), int(row[1])

    async def lock_seq_guard(self, player_id: int, stream: int) -> bool:
        await self._cur.execute(
            "SELECT `next_seq` FROM `guild_player_op_seq` WHERE `player_id`=%s AND `stream`=%s FOR UPDATE",
            (player_id, stream),
        )
        return await self._cur.fetchone() is not None

    async def allocate_seq(self, player_id: int, stream: int, now_ms: int) -> tuple[int, int]:
        next_seq, epoch = await self.ensure_and_lock_seq(player_id, stream, now_ms)
        await self._cur.execute(
            "SELECT COUNT(*), MIN(`seq`) FROM `guild_asset_op` "
            "WHERE `player_id`=%s AND `stream`=%s AND `stream_epoch`=%s AND `status`=%s",
            (player_id, stream, epoch, OP_STATUS_PENDING),
        )
        row = await self._cur.fetchone()
        pending_count = int(row[0]) if row else 0
        min_seq = int(row[1]) if row and row[1] is not None else 0
        if pending_count >= ASSET_OP_MAX_PENDING:
            raise GuildAssetPendingError("too many pending asset ops")
        if pending_count > 0 and (next_seq - min_seq) >= ASSET_OP_MAX_SPAN:
            raise GuildAssetPendingError("pending asset op seq span exceeded")
        await self._cur.execute(
            "UPDATE `guild_player_op_seq` SET `next_seq`=`next_seq`+1,`updated_ms`=%s "
            "WHERE `player_id`=%s AND `stream`=%s",
            (now_ms, player_id, stream),
        )
        return next_seq, epoch

    async def insert_asset_op(self, op: AssetOpRow) -> None:
        try:
            await self._cur.execute(
                SQL_INSERT_ASSET_OP,
                (
                    op.op_id,
                    op.player_id,
                    op.stream,
                    op.seq,
                    op.guild_id,
                    op.kind,
                    op.status,
                    op.durable,
                    op.attempts,
                    op.next_attempt_ms,
                    op.deadline_ms,
                    op.payload.hex(),
                    op.ref_id,
                    op.ref_count,
                    op.period_key,
                    op.contribution_delta,
                    op.funds_delta,
                    op.reason_tip_id,
                    op.created_ms,
                    op.updated_ms,
                    op.lease_until_ms,
                    op.lease_token,
                    op.tx_type,
                    op.last_outcome,
                    op.last_reason,
                    op.stream_epoch,
                    op.resolved_by,
                    op.resolve_reason,
                    op.last_send_ms,
                ),
            )
        except pymysql.MySQLError as exc:
            if _mysql_errno(exc) == DUP_ENTRY_ERRNO:
                raise _DuplicateKeyError(str(exc), _dup_key_name(exc)) from exc
            raise

    async def get_asset_op(self, op_id: int) -> AssetOpRow | None:
        await self._cur.execute(f"SELECT {SQL_ASSET_OP_COLUMNS} FROM `guild_asset_op` WHERE `op_id`=%s", (op_id,))
        row = await self._cur.fetchone()
        return None if row is None else _scan_asset_op(row)

    async def lock_asset_op(self, op_id: int) -> AssetOpRow | None:
        await self._cur.execute(
            f"SELECT {SQL_ASSET_OP_COLUMNS} FROM `guild_asset_op` WHERE `op_id`=%s FOR UPDATE", (op_id,)
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_asset_op(row)

    async def list_due_asset_ops(
        self, now_ms: int, limit: int, owned_shards: frozenset[int] | None = None
    ) -> list[int]:
        if limit <= 0:
            return []
        shard_clause = ""
        shard_args: tuple[int, ...] = ()
        if owned_shards is not None:
            if not owned_shards:
                return []
            marks = ",".join(["%s"] * len(owned_shards))
            shard_clause = f" AND (`player_id` % 64) IN ({marks})"
            shard_args = tuple(sorted(owned_shards))
        half = max(1, limit // 2)
        await self._cur.execute(
            "SELECT `op_id` FROM `guild_asset_op` "
            f"WHERE `status`=%s AND `next_attempt_ms`<=%s AND `lease_until_ms`<=%s AND `attempts`<%s{shard_clause} "
            "ORDER BY `next_attempt_ms` ASC, `op_id` ASC LIMIT %s",
            (OP_STATUS_PENDING, now_ms, now_ms, ASSET_OP_FRESH_ATTEMPT_LIMIT, *shard_args, half),
        )
        out = [int(r[0]) for r in await self._cur.fetchall()]
        rem = limit - len(out)
        if rem > 0:
            await self._cur.execute(
                "SELECT `op_id` FROM `guild_asset_op` "
                f"WHERE `status`=%s AND `next_attempt_ms`<=%s AND `lease_until_ms`<=%s AND `attempts`>=%s{shard_clause} "
                "ORDER BY `next_attempt_ms` ASC, `op_id` ASC LIMIT %s",
                (OP_STATUS_PENDING, now_ms, now_ms, ASSET_OP_FRESH_ATTEMPT_LIMIT, *shard_args, rem),
            )
            out.extend(int(r[0]) for r in await self._cur.fetchall())
        rem = limit - len(out)
        if rem > 0:
            await self._cur.execute(
                "SELECT `op_id` FROM `guild_asset_op` "
                f"WHERE `status`=%s AND `next_attempt_ms`<=%s AND `lease_until_ms`<=%s AND `attempts`<%s{shard_clause} "
                "ORDER BY `next_attempt_ms` ASC, `op_id` ASC LIMIT %s",
                (OP_STATUS_PENDING, now_ms, now_ms, ASSET_OP_FRESH_ATTEMPT_LIMIT, *shard_args, limit),
            )
            picked = set(out)
            for (oid,) in await self._cur.fetchall():
                oid_int = int(oid)
                if oid_int not in picked:
                    out.append(oid_int)
                    picked.add(oid_int)
                    if len(out) >= limit:
                        break
        return out

    async def claim_asset_op(
        self, op_id: int, now_ms: int, lease_until_ms: int, lease_token: int
    ) -> AssetOpRow | None:
        await self._cur.execute(
            "UPDATE `guild_asset_op` SET `lease_until_ms`=%s,`lease_token`=%s,`updated_ms`=%s "
            "WHERE `op_id`=%s AND `status`=%s AND `lease_until_ms`<=%s",
            (lease_until_ms, lease_token, now_ms, op_id, OP_STATUS_PENDING, now_ms),
        )
        if self._cur.rowcount != 1:
            return None
        return await self.get_asset_op(op_id)

    async def mark_asset_op_poison(self, op_id: int, lease_token: int, poison_until_ms: int, now_ms: int) -> None:
        await self._cur.execute(
            "UPDATE `guild_asset_op` SET `next_attempt_ms`=%s,`lease_until_ms`=0,`lease_token`=0,`updated_ms`=%s "
            "WHERE `op_id`=%s AND `status`=%s AND `lease_token`=%s",
            (poison_until_ms, now_ms, op_id, OP_STATUS_PENDING, lease_token),
        )

    async def reschedule_asset_op(
        self,
        op_id: int,
        lease_token: int,
        next_attempt_ms: int,
        outcome: int,
        reason: int,
        durable: bool,
        local: bool,
        now_ms: int,
    ) -> bool:
        if local:
            await self._cur.execute(
                "UPDATE `guild_asset_op` SET `attempts`=`attempts`+1,`next_attempt_ms`=%s,"
                "`last_outcome`=%s,`last_reason`=%s,`durable`=%s,`lease_until_ms`=0,`lease_token`=0,`updated_ms`=%s "
                "WHERE `op_id`=%s AND `status`=%s AND `lease_token`=%s",
                (
                    next_attempt_ms,
                    outcome,
                    reason,
                    1 if durable else 0,
                    now_ms,
                    op_id,
                    OP_STATUS_PENDING,
                    lease_token,
                ),
            )
        else:
            await self._cur.execute(
                "UPDATE `guild_asset_op` SET `attempts`=`attempts`+1,`next_attempt_ms`=%s,"
                "`last_outcome`=%s,`last_reason`=%s,`durable`=%s,`lease_until_ms`=0,`lease_token`=0,"
                "`last_send_ms`=%s,`updated_ms`=%s "
                "WHERE `op_id`=%s AND `status`=%s AND `lease_token`=%s",
                (
                    next_attempt_ms,
                    outcome,
                    reason,
                    1 if durable else 0,
                    now_ms,
                    now_ms,
                    op_id,
                    OP_STATUS_PENDING,
                    lease_token,
                ),
            )
        return self._cur.rowcount == 1

    async def finalize_asset_op_cas(
        self,
        op_id: int,
        final_status: int,
        last_outcome: int,
        last_reason: int,
        reason_tip_id: int,
        now_ms: int,
    ) -> bool:
        await self._cur.execute(
            "UPDATE `guild_asset_op` SET `status`=%s,`durable`=1,`last_outcome`=%s,`last_reason`=%s,"
            "`reason_tip_id`=%s,`next_attempt_ms`=%s,`lease_until_ms`=0,`lease_token`=0,`updated_ms`=%s "
            "WHERE `op_id`=%s AND `status`=%s",
            (
                final_status,
                last_outcome,
                last_reason,
                reason_tip_id,
                now_ms,
                now_ms,
                op_id,
                OP_STATUS_PENDING,
            ),
        )
        return self._cur.rowcount == 1

    async def abandon_asset_op_cas(self, op_id: int, lease_token: int, now_ms: int) -> bool:
        await self._cur.execute(
            "UPDATE `guild_asset_op` SET `status`=%s,`lease_until_ms`=0,`lease_token`=0,`updated_ms`=%s "
            "WHERE `op_id`=%s AND `status`=%s AND `lease_token`=%s",
            (OP_STATUS_ABANDONED, now_ms, op_id, OP_STATUS_PENDING, lease_token),
        )
        return self._cur.rowcount == 1

    async def resolve_asset_op_manual_cas(
        self,
        op_id: int,
        final_status: int,
        operator: str,
        reason: str,
        now_ms: int,
        witness: QuietWitness | None = None,
    ) -> bool:
        if witness is None:
            await self._cur.execute(
                SQL_RESOLVE_ASSET_OP,
                (final_status, operator, reason, now_ms, now_ms, op_id, OP_STATUS_PENDING),
            )
        else:
            await self._cur.execute(
                SQL_RESOLVE_ASSET_OP_QUIET,
                (
                    final_status,
                    operator,
                    reason,
                    now_ms,
                    now_ms,
                    op_id,
                    OP_STATUS_PENDING,
                    witness.lease_until_ms,
                    witness.last_send_ms,
                    witness.attempts,
                ),
            )
        return self._cur.rowcount == 1

    async def list_pending_ops(self, player_id: int, stream: int, limit: int) -> list[AssetOpRow]:
        await self._cur.execute(
            f"SELECT {SQL_ASSET_OP_COLUMNS} FROM `guild_asset_op` "
            "WHERE `player_id`=%s AND `stream`=%s AND `status`=%s ORDER BY `stream_epoch` ASC, `seq` ASC LIMIT %s",
            (player_id, stream, OP_STATUS_PENDING, limit),
        )
        return [_scan_asset_op(r) for r in await self._cur.fetchall()]

    async def list_recent_ops(self, player_id: int, stream: int, limit: int) -> list[AssetOpRow]:
        await self._cur.execute(
            f"SELECT {SQL_ASSET_OP_COLUMNS} FROM `guild_asset_op` "
            "WHERE `player_id`=%s AND `stream`=%s ORDER BY `stream_epoch` DESC, `seq` DESC LIMIT %s",
            (player_id, stream, limit),
        )
        return [_scan_asset_op(r) for r in await self._cur.fetchall()]

    async def accelerate_donation_deadlines(self, guild_id: int, player_ids: Sequence[int], now_ms: int) -> None:
        if not player_ids:
            return
        marks = ",".join(["%s"] * len(player_ids))
        await self._cur.execute(
            f"SELECT `op_id` FROM `guild_asset_op` WHERE `player_id` IN ({marks}) "
            "AND `stream`=%s AND `status`=%s AND `guild_id`=%s AND `kind`=%s AND `deadline_ms`>%s ORDER BY `op_id`",
            (*player_ids, ASSET_STREAM_GUILD_DEBIT, OP_STATUS_PENDING, guild_id, OP_KIND_DONATE, now_ms),
        )
        op_ids = [int(r[0]) for r in await self._cur.fetchall()]
        for op_id in op_ids:
            await self._cur.execute("SELECT `op_id` FROM `guild_asset_op` WHERE `op_id`=%s FOR UPDATE", (op_id,))
            if await self._cur.fetchone() is None:
                continue
            await self._cur.execute(
                "UPDATE `guild_asset_op` SET `deadline_ms`=%s,`next_attempt_ms`=LEAST(`next_attempt_ms`,%s),`updated_ms`=%s "
                "WHERE `op_id`=%s AND `status`=%s AND `deadline_ms`>%s",
                (now_ms, now_ms, now_ms, op_id, OP_STATUS_PENDING, now_ms),
            )

    async def get_client_request_op_id(self, player_id: int, client_request_id: int) -> int | None:
        await self._cur.execute(
            "SELECT `op_id` FROM `guild_client_request` WHERE `player_id`=%s AND `client_request_id`=%s",
            (player_id, client_request_id),
        )
        row = await self._cur.fetchone()
        return None if row is None else int(row[0])

    async def insert_client_request(self, player_id: int, client_request_id: int, op_id: int, now_ms: int) -> None:
        try:
            await self._cur.execute(
                "INSERT INTO `guild_client_request` (`player_id`,`client_request_id`,`op_id`,`created_ms`) "
                "VALUES (%s,%s,%s,%s)",
                (player_id, client_request_id, op_id, now_ms),
            )
        except pymysql.MySQLError as exc:
            if _mysql_errno(exc) == DUP_ENTRY_ERRNO:
                raise _DuplicateKeyError(str(exc), _dup_key_name(exc)) from exc
            raise

    async def upsert_counter_with_limit(
        self, player_id: int, counter_kind: int, ref_id: int, period_key_val: int, delta: int, limit: int, now_ms: int
    ) -> bool:
        if delta > limit:
            return False
        await self._cur.execute(
            SQL_UPSERT_COUNTER_WITH_LIMIT,
            (
                player_id,
                counter_kind,
                ref_id,
                period_key_val,
                delta,
                now_ms,
                delta,
                limit,
                now_ms,
                delta,
                limit,
                delta,
            ),
        )
        return self._cur.rowcount > 0

    async def refund_counter(
        self, player_id: int, counter_kind: int, ref_id: int, period_key_val: int, delta: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            "UPDATE `guild_daily_counter` SET `used_count`=IF(`used_count`>=%s,`used_count`-%s,0),`updated_ms`=%s "
            "WHERE `player_id`=%s AND `counter_kind`=%s AND `ref_id`=%s AND `period_key`=%s",
            (delta, delta, now_ms, player_id, counter_kind, ref_id, period_key_val),
        )

    async def get_counter_usage(
        self, player_id: int, counter_kind: int, period_keys: Sequence[int]
    ) -> dict[tuple[int, int], int]:
        if not period_keys:
            return {}
        marks = ",".join(["%s"] * len(period_keys))
        await self._cur.execute(
            f"SELECT `ref_id`,`period_key`,`used_count` FROM `guild_daily_counter` "
            f"WHERE `player_id`=%s AND `counter_kind`=%s AND `period_key` IN ({marks})",
            (player_id, counter_kind, *period_keys),
        )
        return {(int(r[0]), int(r[1])): int(r[2]) for r in await self._cur.fetchall()}

    async def get_activity_counter_value(self, player_id: int, activity_id: int, period_key_val: int) -> int:
        await self._cur.execute(
            "SELECT `used_count` FROM `guild_daily_counter` "
            "WHERE `player_id`=%s AND `counter_kind`=%s AND `ref_id`=%s AND `period_key`=%s",
            (player_id, COUNTER_KIND_ACTIVITY, activity_id, period_key_val),
        )
        row = await self._cur.fetchone()
        return 0 if row is None else int(row[0])

    async def list_applied_asset_ops_since(
        self,
        player_ids: Sequence[int],
        since_ms: int,
        after_op_id: int,
        limit: int,
        zones: Sequence[int],
    ) -> tuple[list[AssetOpRow], int]:
        p_marks = ",".join(["%s"] * len(player_ids))
        sql = (
            "SELECT o.`op_id`,o.`player_id`,o.`guild_id`,o.`stream`,o.`kind`,o.`status`,"
            "o.`funds_delta`,o.`contribution_delta`,o.`updated_ms` FROM `guild_asset_op` o"
        )
        args: list[int] = [OP_STATUS_APPLIED, OP_STATUS_APPLIED_PARTIAL, since_ms, *player_ids, after_op_id]
        if zones:
            sql += " LEFT JOIN `guild` g ON g.`guild_id`=o.`guild_id`"
        sql += f" WHERE o.`status` IN (%s,%s) AND o.`next_attempt_ms`>%s AND o.`player_id` IN ({p_marks}) AND o.`op_id`>%s"
        if zones:
            z_marks = ",".join(["%s"] * len(zones))
            sql += f" AND (g.`zone_id` IN ({z_marks}) OR g.`guild_id` IS NULL)"
            args.extend(zones)
        sql += " ORDER BY o.`op_id` ASC LIMIT %s"
        args.append(limit + 1)
        await self._cur.execute(sql, tuple(args))
        rows = await self._cur.fetchall()
        ops = [
            AssetOpRow(
                op_id=int(r[0]),
                player_id=int(r[1]),
                guild_id=int(r[2]),
                stream=int(r[3]),
                kind=int(r[4]),
                status=int(r[5]),
                funds_delta=int(r[6]),
                contribution_delta=int(r[7]),
                updated_ms=int(r[8]),
                seq=0,
                durable=1,
                attempts=0,
                next_attempt_ms=0,
                deadline_ms=0,
                payload=b"",
                ref_id=0,
                ref_count=0,
                period_key=0,
                reason_tip_id=0,
                created_ms=0,
                lease_until_ms=0,
                lease_token=0,
                tx_type=0,
                last_outcome=0,
                last_reason=0,
                stream_epoch=0,
            )
            for r in rows
        ]
        if len(ops) > limit:
            page = ops[:limit]
            return page, page[-1].op_id
        return ops, 0

    # -- activities & purge ----------------------------------------------------------------------

    async def get_activity_progress(
        self, guild_id: int, keys: Sequence[tuple[int, int]]
    ) -> dict[tuple[int, int], ActivityProgress]:
        out: dict[tuple[int, int], ActivityProgress] = {}
        for act_id, pk in keys:
            await self._cur.execute(
                "SELECT `progress_count`,`threshold_reached_ms`,`funds_granted` "
                "FROM `guild_activity_progress` WHERE `guild_id`=%s AND `activity_id`=%s AND `period_key`=%s",
                (guild_id, act_id, pk),
            )
            row = await self._cur.fetchone()
            if row is not None:
                out[(act_id, pk)] = ActivityProgress(
                    count=int(row[0]),
                    threshold_reached_ms=int(row[1]),
                    funds_granted=bool(row[2]),
                )
        return out

    async def upsert_and_lock_activity_progress(
        self, guild_id: int, activity_id: int, period_key_val: int, *, increment: bool, now_ms: int
    ) -> ActivityProgress:
        if increment:
            await self._cur.execute(
                "INSERT INTO `guild_activity_progress` "
                "(`guild_id`,`activity_id`,`period_key`,`progress_count`,`threshold_reached_ms`,`funds_granted`,`updated_ms`) "
                "VALUES (%s,%s,%s,1,0,0,%s) "
                "ON DUPLICATE KEY UPDATE `progress_count`=`progress_count`+1,`updated_ms`=%s",
                (guild_id, activity_id, period_key_val, now_ms, now_ms),
            )
        else:
            await self._cur.execute(
                "INSERT INTO `guild_activity_progress` "
                "(`guild_id`,`activity_id`,`period_key`,`progress_count`,`threshold_reached_ms`,`funds_granted`,`updated_ms`) "
                "VALUES (%s,%s,%s,0,0,0,%s) "
                "ON DUPLICATE KEY UPDATE `updated_ms`=%s",
                (guild_id, activity_id, period_key_val, now_ms, now_ms),
            )
        await self._cur.execute(
            "SELECT `progress_count`,`threshold_reached_ms`,`funds_granted` "
            "FROM `guild_activity_progress` WHERE `guild_id`=%s AND `activity_id`=%s AND `period_key`=%s FOR UPDATE",
            (guild_id, activity_id, period_key_val),
        )
        row = await self._cur.fetchone()
        assert row is not None
        return ActivityProgress(count=int(row[0]), threshold_reached_ms=int(row[1]), funds_granted=bool(row[2]))

    async def latch_activity_progress(
        self,
        guild_id: int,
        activity_id: int,
        period_key_val: int,
        threshold_reached_ms: int,
        funds_granted: bool,
        now_ms: int,
    ) -> None:
        await self._cur.execute(
            "UPDATE `guild_activity_progress` SET `threshold_reached_ms`=%s,`funds_granted`=%s,`updated_ms`=%s "
            "WHERE `guild_id`=%s AND `activity_id`=%s AND `period_key`=%s",
            (threshold_reached_ms, 1 if funds_granted else 0, now_ms, guild_id, activity_id, period_key_val),
        )

    async def set_trial_progress_count(
        self, guild_id: int, activity_id: int, period_key_val: int, count: int, now_ms: int
    ) -> None:
        await self._cur.execute(
            "INSERT INTO `guild_activity_progress` "
            "(`guild_id`,`activity_id`,`period_key`,`progress_count`,`threshold_reached_ms`,`funds_granted`,`updated_ms`) "
            "VALUES (%s,%s,%s,%s,0,0,%s) "
            "ON DUPLICATE KEY UPDATE `progress_count`=%s,`updated_ms`=%s",
            (guild_id, activity_id, period_key_val, count, now_ms, count, now_ms),
        )

    async def delete_activity_progress_of_guild(self, guild_id: int) -> None:
        await self._cur.execute(
            "SELECT `activity_id`,`period_key` FROM `guild_activity_progress` "
            "WHERE `guild_id`=%s ORDER BY `activity_id`,`period_key`",
            (guild_id,),
        )
        for act_id, pk in await self._cur.fetchall():
            await self._cur.execute(
                "DELETE FROM `guild_activity_progress` WHERE `guild_id`=%s AND `activity_id`=%s AND `period_key`=%s",
                (guild_id, int(act_id), int(pk)),
            )

    async def delete_started_trial_battles_of_guild(self, guild_id: int) -> None:
        await self._cur.execute(
            "SELECT `battle_id` FROM `guild_trial_battle` WHERE `guild_id`=%s AND `state`=1 ORDER BY `battle_id`",
            (guild_id,),
        )
        for (bid,) in await self._cur.fetchall():
            await self._cur.execute("DELETE FROM `guild_trial_battle` WHERE `battle_id`=%s", (int(bid),))

    async def get_activity_reward_statuses(self, player_id: int, now_ms: int) -> dict[int, ActivityRewardStatus]:
        pending_count: dict[int, int] = {}
        pending_reason: dict[int, int] = {}
        last_reject: dict[int, int] = {}
        await self._cur.execute(
            "SELECT `ref_id`,`last_reason` FROM `guild_asset_op` "
            "WHERE `player_id`=%s AND `stream`=%s AND `status`=%s AND `kind`=%s "
            "ORDER BY `stream_epoch` ASC, `seq` ASC LIMIT %s",
            (player_id, ASSET_STREAM_GUILD_CREDIT, OP_STATUS_PENDING, OP_KIND_ACTIVITY_REWARD, ASSET_OP_MAX_PENDING),
        )
        for ref_id, last_reason in await self._cur.fetchall():
            aid = int(ref_id)
            pending_count[aid] = pending_count.get(aid, 0) + 1
            if aid not in pending_reason and int(last_reason) != 0:
                pending_reason[aid] = int(last_reason)

        cutoff = max(0, now_ms - REJECTED_REWARD_WINDOW_MS)
        await self._cur.execute(
            "SELECT `ref_id`,`kind`,`reason_tip_id`,`next_attempt_ms` FROM `guild_asset_op` "
            "WHERE `player_id`=%s AND `stream`=%s AND `status`=%s ORDER BY `stream_epoch` DESC, `seq` DESC LIMIT 4",
            (player_id, ASSET_STREAM_GUILD_CREDIT, OP_STATUS_REJECTED),
        )
        for ref_id, kind, reason_tip, next_ms in await self._cur.fetchall():
            aid = int(ref_id)
            if int(kind) == OP_KIND_ACTIVITY_REWARD and int(next_ms) >= cutoff and aid not in last_reject:
                last_reject[aid] = int(reason_tip)

        all_ids = set(pending_count) | set(pending_reason) | set(last_reject)
        return {
            aid: ActivityRewardStatus(
                pending_count=pending_count.get(aid, 0),
                pending_reason_tip_id=pending_reason.get(aid, 0),
                last_reject_tip_id=last_reject.get(aid, 0),
            )
            for aid in all_ids
        }

    async def purge_player_personal_tables(self, player_id: int) -> None:
        # 按全库表间锁序 Q < C < R 主键升序逐行点删
        await self._cur.execute(
            "SELECT `stream` FROM `guild_player_op_seq` WHERE `player_id`=%s ORDER BY `stream`",
            (player_id,),
        )
        for (stream,) in await self._cur.fetchall():
            await self._cur.execute(
                "DELETE FROM `guild_player_op_seq` WHERE `player_id`=%s AND `stream`=%s",
                (player_id, int(stream)),
            )

        await self._cur.execute(
            "SELECT `counter_kind`,`ref_id`,`period_key` FROM `guild_daily_counter` "
            "WHERE `player_id`=%s ORDER BY `counter_kind`,`ref_id`,`period_key`",
            (player_id,),
        )
        for kind, ref_id, pk in await self._cur.fetchall():
            await self._cur.execute(
                "DELETE FROM `guild_daily_counter` WHERE `player_id`=%s AND `counter_kind`=%s "
                "AND `ref_id`=%s AND `period_key`=%s",
                (player_id, int(kind), int(ref_id), int(pk)),
            )

        await self._cur.execute(
            "SELECT `client_request_id` FROM `guild_client_request` WHERE `player_id`=%s ORDER BY `client_request_id`",
            (player_id,),
        )
        for (req_id,) in await self._cur.fetchall():
            await self._cur.execute(
                "DELETE FROM `guild_client_request` WHERE `player_id`=%s AND `client_request_id`=%s",
                (player_id, int(req_id)),
            )

        # 帮战个人数据：按锁序 14 -> 16 -> 24 清理守旗快照、个人帮战状态与待入账行（对齐 Go purgeWarRowsOfPlayer）。
        for table in ("guild_war_garrison", "guild_war_player_state"):
            await self._cur.execute(f"DELETE FROM `{table}` WHERE `player_id`=%s", (player_id,))
        await self._cur.execute(
            "SELECT `op_id` FROM `guild_war_credit_owed` WHERE `player_id`=%s ORDER BY `op_id`",
            (player_id,),
        )
        for (owed_op_id,) in await self._cur.fetchall():
            await self._cur.execute(
                "DELETE FROM `guild_war_credit_owed` WHERE `player_id`=%s AND `op_id`=%s",
                (player_id, int(owed_op_id)),
            )

    # -- guild_trial_lobby / slot / launch / launch_role / battle / reward_owed ------------------

    _TRIAL_LOBBY_COLS = (
        "`lobby_id`,`guild_id`,`activity_id`,`initiator_player_id`,`state`,"
        "`view_proto`,`original_proto`,`consent_proto`,`expires_ms`,`launch_deadline_ms`,"
        "`battle_id`,`created_ms`,`updated_ms`"
    )
    _TRIAL_LAUNCH_COLS = (
        "`request_id`,`battle_id`,`state`,`original_proto`,`request_digest`,"
        "`created_ms`,`updated_ms`,`cancel_receipt`,`next_check_ms`,`check_attempts`,`realtime_end_tip`"
    )
    _TRIAL_BATTLE_COLS = (
        "`battle_id`,`guild_id`,`activity_id`,`period_key`,`guild_period_key`,"
        "`initiator_player_id`,`state`,`settle_result`,`rewarded_count`,"
        "`created_ms`,`settled_ms`,`launch_request_id`"
    )

    async def get_trial_lobby(self, lobby_id: int) -> TrialLobbyRecord | None:
        await self._cur.execute(
            f"SELECT {self._TRIAL_LOBBY_COLS} FROM `guild_trial_lobby` WHERE `lobby_id`=%s", (lobby_id,)
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_trial_lobby(row)

    async def lock_trial_lobby(self, lobby_id: int) -> TrialLobbyRecord | None:
        await self._cur.execute(
            f"SELECT {self._TRIAL_LOBBY_COLS} FROM `guild_trial_lobby` WHERE `lobby_id`=%s FOR UPDATE", (lobby_id,)
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_trial_lobby(row)

    async def insert_trial_lobby(self, rec: TrialLobbyRecord) -> None:
        try:
            await self._cur.execute(
                f"INSERT INTO `guild_trial_lobby` ({self._TRIAL_LOBBY_COLS}) VALUES ("
                "%s,%s,%s,%s,%s,UNHEX(%s),UNHEX(%s),UNHEX(%s),%s,%s,%s,%s,%s)",
                (
                    rec.lobby_id,
                    rec.guild_id,
                    rec.activity_id,
                    rec.initiator_player_id,
                    rec.state,
                    rec.view_proto.hex(),
                    rec.original_proto.hex(),
                    rec.consent_proto.hex(),
                    rec.expires_ms,
                    rec.launch_deadline_ms,
                    rec.battle_id,
                    rec.created_ms,
                    rec.updated_ms,
                ),
            )
        except pymysql.MySQLError as exc:
            if _mysql_errno(exc) == DUP_ENTRY_ERRNO:
                raise _DuplicateKeyError(str(exc), _dup_key_name(exc)) from exc
            raise

    async def update_trial_lobby(
        self,
        lobby_id: int,
        state: int,
        view_proto: bytes,
        consent_proto: bytes,
        launch_deadline_ms: int,
        battle_id: int,
        updated_ms: int,
    ) -> None:
        await self._cur.execute(
            "UPDATE `guild_trial_lobby` SET `state`=%s,`view_proto`=UNHEX(%s),`consent_proto`=UNHEX(%s),"
            "`launch_deadline_ms`=%s,`battle_id`=%s,`updated_ms`=%s WHERE `lobby_id`=%s",
            (state, view_proto.hex(), consent_proto.hex(), launch_deadline_ms, battle_id, updated_ms, lobby_id),
        )

    async def list_expired_pending_lobbies(self, now_ms: int, limit: int) -> list[TrialLobbyRecord]:
        await self._cur.execute(
            f"SELECT {self._TRIAL_LOBBY_COLS} FROM `guild_trial_lobby` "
            "WHERE `state`=%s AND `expires_ms`<=%s ORDER BY `expires_ms` ASC, `lobby_id` ASC LIMIT %s",
            (TRIAL_LOBBY_STATE_PENDING, now_ms, limit),
        )
        return [_scan_trial_lobby(r) for r in await self._cur.fetchall()]

    async def get_trial_lobby_slot(self, player_id: int) -> TrialLobbySlotRecord | None:
        await self._cur.execute(
            "SELECT `player_id`,`lobby_id`,`cooldown_until_ms` FROM `guild_trial_lobby_slot` WHERE `player_id`=%s",
            (player_id,),
        )
        row = await self._cur.fetchone()
        if row is None:
            return None
        return TrialLobbySlotRecord(player_id=int(row[0]), lobby_id=int(row[1]), cooldown_until_ms=int(row[2]))

    async def lock_trial_lobby_slot(self, player_id: int) -> TrialLobbySlotRecord | None:
        await self._cur.execute(
            "SELECT `player_id`,`lobby_id`,`cooldown_until_ms` FROM `guild_trial_lobby_slot` "
            "WHERE `player_id`=%s FOR UPDATE",
            (player_id,),
        )
        row = await self._cur.fetchone()
        if row is None:
            return None
        return TrialLobbySlotRecord(player_id=int(row[0]), lobby_id=int(row[1]), cooldown_until_ms=int(row[2]))

    async def upsert_trial_lobby_slot(self, player_id: int, lobby_id: int, cooldown_until_ms: int) -> None:
        await self._cur.execute(
            "INSERT INTO `guild_trial_lobby_slot` (`player_id`,`lobby_id`,`cooldown_until_ms`) VALUES (%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE `lobby_id`=VALUES(`lobby_id`),`cooldown_until_ms`=VALUES(`cooldown_until_ms`)",
            (player_id, lobby_id, cooldown_until_ms),
        )

    async def get_trial_launch(self, request_id: int) -> TrialLaunchRecord | None:
        await self._cur.execute(
            f"SELECT {self._TRIAL_LAUNCH_COLS} FROM `guild_trial_launch` WHERE `request_id`=%s", (request_id,)
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_trial_launch(row)

    async def lock_trial_launch(self, request_id: int) -> TrialLaunchRecord | None:
        await self._cur.execute(
            f"SELECT {self._TRIAL_LAUNCH_COLS} FROM `guild_trial_launch` WHERE `request_id`=%s FOR UPDATE",
            (request_id,),
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_trial_launch(row)

    async def get_trial_launch_by_battle(self, battle_id: int) -> TrialLaunchRecord | None:
        await self._cur.execute(
            f"SELECT {self._TRIAL_LAUNCH_COLS} FROM `guild_trial_launch` WHERE `battle_id`=%s LIMIT 1",
            (battle_id,),
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_trial_launch(row)

    async def insert_trial_launch(self, rec: TrialLaunchRecord) -> None:
        try:
            await self._cur.execute(
                f"INSERT INTO `guild_trial_launch` ({self._TRIAL_LAUNCH_COLS}) VALUES ("
                "%s,%s,%s,UNHEX(%s),UNHEX(%s),%s,%s,UNHEX(%s),%s,%s,UNHEX(%s))",
                (
                    rec.request_id,
                    rec.battle_id,
                    rec.state,
                    rec.original_proto.hex(),
                    rec.request_digest.hex(),
                    rec.created_ms,
                    rec.updated_ms,
                    rec.cancel_receipt.hex(),
                    rec.next_check_ms,
                    rec.check_attempts,
                    rec.realtime_end_tip.hex(),
                ),
            )
        except pymysql.MySQLError as exc:
            if _mysql_errno(exc) == DUP_ENTRY_ERRNO:
                raise _DuplicateKeyError(str(exc), _dup_key_name(exc)) from exc
            raise

    async def update_trial_launch(
        self,
        request_id: int,
        battle_id: int,
        state: int,
        updated_ms: int,
        cancel_receipt: bytes | None = None,
    ) -> None:
        if cancel_receipt is None:
            await self._cur.execute(
                "UPDATE `guild_trial_launch` SET `battle_id`=%s,`state`=%s,`updated_ms`=%s WHERE `request_id`=%s",
                (battle_id, state, updated_ms, request_id),
            )
        else:
            await self._cur.execute(
                "UPDATE `guild_trial_launch` SET `battle_id`=%s,`state`=%s,`updated_ms`=%s,`cancel_receipt`=UNHEX(%s) "
                "WHERE `request_id`=%s",
                (battle_id, state, updated_ms, cancel_receipt.hex(), request_id),
            )

    async def record_trial_launch_end_tip(self, request_id: int, tip_bytes: bytes) -> None:
        await self._cur.execute(
            "UPDATE `guild_trial_launch` SET `realtime_end_tip`=UNHEX(%s) "
            "WHERE `request_id`=%s AND (`realtime_end_tip` IS NULL OR OCTET_LENGTH(`realtime_end_tip`)=0)",
            (tip_bytes.hex(), request_id),
        )

    async def expedite_trial_launch_check(self, request_id: int, now_ms: int) -> None:
        await self._cur.execute(
            "UPDATE `guild_trial_launch` SET `next_check_ms`=%s "
            "WHERE `request_id`=%s AND `state`=%s AND `next_check_ms`>%s",
            (now_ms, request_id, TRIAL_LAUNCH_STATE_PREPARED, now_ms),
        )

    async def list_due_trial_launches(self, now_ms: int, limit: int) -> list[TrialLaunchRecord]:
        await self._cur.execute(
            f"SELECT {self._TRIAL_LAUNCH_COLS} FROM `guild_trial_launch` "
            "WHERE `state`=%s AND `next_check_ms`<=%s ORDER BY `next_check_ms` ASC, `request_id` ASC LIMIT %s",
            (TRIAL_LAUNCH_STATE_PREPARED, now_ms, limit),
        )
        return [_scan_trial_launch(r) for r in await self._cur.fetchall()]

    async def claim_trial_launch(self, request_id: int, prev_next_check_ms: int, new_next_check_ms: int) -> bool:
        await self._cur.execute(
            "UPDATE `guild_trial_launch` SET `next_check_ms`=%s,`check_attempts`=`check_attempts`+1 "
            "WHERE `request_id`=%s AND `state`=%s AND `next_check_ms`=%s",
            (new_next_check_ms, request_id, TRIAL_LAUNCH_STATE_PREPARED, prev_next_check_ms),
        )
        return self._cur.rowcount == 1

    async def insert_trial_launch_role(self, rec: TrialLaunchRoleRecord) -> None:
        try:
            await self._cur.execute(
                "INSERT INTO `guild_trial_launch_role` (`player_id`,`request_id`,`account`,`owner_revision`,`state`) "
                "VALUES (%s,%s,%s,%s,%s)",
                (rec.player_id, rec.request_id, rec.account, rec.owner_revision, rec.state),
            )
        except pymysql.MySQLError as exc:
            if _mysql_errno(exc) == DUP_ENTRY_ERRNO:
                raise _DuplicateKeyError(str(exc), _dup_key_name(exc)) from exc
            raise

    async def lock_trial_launch_role(self, player_id: int, request_id: int) -> TrialLaunchRoleRecord | None:
        await self._cur.execute(
            "SELECT `player_id`,`request_id`,`account`,`owner_revision`,`state` "
            "FROM `guild_trial_launch_role` WHERE `player_id`=%s AND `request_id`=%s FOR UPDATE",
            (player_id, request_id),
        )
        row = await self._cur.fetchone()
        if row is None:
            return None
        return TrialLaunchRoleRecord(
            player_id=int(row[0]),
            request_id=int(row[1]),
            account=_to_str(row[2]),
            owner_revision=int(row[3]),
            state=int(row[4]),
        )

    async def has_active_trial_launch_role(self, player_id: int, exclude_request_id: int = 0) -> bool:
        await self._cur.execute(
            "SELECT r.`request_id` FROM `guild_trial_launch_role` r "
            "LEFT JOIN `guild_trial_launch` l ON l.`request_id`=r.`request_id` "
            "WHERE r.`player_id`=%s AND r.`request_id`<>%s "
            "AND (r.`state` IS NULL OR r.`state`<>%s OR l.`request_id` IS NULL OR l.`state` IS NULL OR l.`state`<>%s) "
            "LIMIT 1 FOR UPDATE",
            (player_id, exclude_request_id, TRIAL_LAUNCH_ROLE_RELEASED, TRIAL_LAUNCH_STATE_TERMINAL),
        )
        return await self._cur.fetchone() is not None

    async def update_trial_launch_role_state(self, player_id: int, request_id: int, state: int) -> None:
        await self._cur.execute(
            "UPDATE `guild_trial_launch_role` SET `state`=%s WHERE `player_id`=%s AND `request_id`=%s",
            (state, player_id, request_id),
        )

    async def get_trial_battle(self, battle_id: int) -> TrialBattleRecord | None:
        await self._cur.execute(
            f"SELECT {self._TRIAL_BATTLE_COLS} FROM `guild_trial_battle` WHERE `battle_id`=%s", (battle_id,)
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_trial_battle(row)

    async def lock_trial_battle(self, battle_id: int) -> TrialBattleRecord | None:
        await self._cur.execute(
            f"SELECT {self._TRIAL_BATTLE_COLS} FROM `guild_trial_battle` WHERE `battle_id`=%s FOR UPDATE",
            (battle_id,),
        )
        row = await self._cur.fetchone()
        return None if row is None else _scan_trial_battle(row)

    async def insert_trial_battle(self, rec: TrialBattleRecord) -> None:
        try:
            await self._cur.execute(
                f"INSERT INTO `guild_trial_battle` ({self._TRIAL_BATTLE_COLS}) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    rec.battle_id,
                    rec.guild_id,
                    rec.activity_id,
                    rec.period_key,
                    rec.guild_period_key,
                    rec.initiator_player_id,
                    rec.state,
                    rec.settle_result,
                    rec.rewarded_count,
                    rec.created_ms,
                    rec.settled_ms,
                    rec.launch_request_id,
                ),
            )
        except pymysql.MySQLError as exc:
            if _mysql_errno(exc) == DUP_ENTRY_ERRNO:
                raise _DuplicateKeyError(str(exc), _dup_key_name(exc)) from exc
            raise

    async def update_trial_battle(
        self, battle_id: int, state: int, settle_result: int, rewarded_count: int, settled_ms: int
    ) -> None:
        await self._cur.execute(
            "UPDATE `guild_trial_battle` SET `state`=%s,`settle_result`=%s,`rewarded_count`=%s,`settled_ms`=%s "
            "WHERE `battle_id`=%s",
            (state, settle_result, rewarded_count, settled_ms, battle_id),
        )

    async def list_overdue_trial_battles(self, created_before_ms: int, limit: int) -> list[TrialBattleRecord]:
        await self._cur.execute(
            f"SELECT {self._TRIAL_BATTLE_COLS} FROM `guild_trial_battle` "
            "WHERE `state`=%s AND `created_ms`<=%s ORDER BY `created_ms` ASC, `battle_id` ASC LIMIT %s",
            (TRIAL_BATTLE_STATE_STARTED, created_before_ms, limit),
        )
        return [_scan_trial_battle(r) for r in await self._cur.fetchall()]

    async def insert_trial_reward_owed(self, rec: TrialOwedRewardRecord) -> None:
        await self._cur.execute(
            "INSERT IGNORE INTO `guild_trial_reward_owed` "
            "(`player_id`,`battle_id`,`guild_id`,`activity_id`,`period_key`,`payload`,`attempts`,`created_ms`) "
            "VALUES (%s,%s,%s,%s,%s,UNHEX(%s),%s,%s)",
            (
                rec.player_id,
                rec.battle_id,
                rec.guild_id,
                rec.activity_id,
                rec.period_key,
                rec.payload.hex(),
                rec.attempts,
                rec.created_ms,
            ),
        )

    async def delete_trial_reward_owed(self, player_id: int, battle_id: int) -> bool:
        await self._cur.execute(
            "DELETE FROM `guild_trial_reward_owed` WHERE `player_id`=%s AND `battle_id`=%s",
            (player_id, battle_id),
        )
        return self._cur.rowcount == 1

    async def list_trial_rewards_owed(self, limit: int) -> list[TrialOwedRewardRecord]:
        await self._cur.execute(
            "SELECT `player_id`,`battle_id`,`guild_id`,`activity_id`,`period_key`,`payload`,`attempts`,`created_ms` "
            "FROM `guild_trial_reward_owed` ORDER BY `created_ms` ASC, `battle_id` ASC, `player_id` ASC LIMIT %s",
            (limit,),
        )
        return [_scan_trial_reward_owed(r) for r in await self._cur.fetchall()]

    async def count_trial_rewards_owed_by_player(self, player_id: int) -> dict[int, int]:
        await self._cur.execute(
            "SELECT `activity_id`, COUNT(*) FROM `guild_trial_reward_owed` WHERE `player_id`=%s GROUP BY `activity_id`",
            (player_id,),
        )
        return {int(r[0]): int(r[1]) for r in await self._cur.fetchall()}

    async def increment_trial_reward_owed_attempts(self, player_id: int, battle_id: int) -> None:
        await self._cur.execute(
            "UPDATE `guild_trial_reward_owed` SET `attempts`=`attempts`+1 WHERE `player_id`=%s AND `battle_id`=%s",
            (player_id, battle_id),
        )

    async def insert_war_credit_owed(self, rec: WarCreditOwed) -> None:
        await self._cur.execute(
            "INSERT INTO `guild_war_credit_owed` "
            "(`player_id`,`op_id`,`guild_id`,`kind`,`tx_type`,`ref_id`,`period_key`,`payload`,`source_id`,`created_ms`) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),%s,%s)",
            (
                rec.player_id,
                rec.op_id,
                rec.guild_id,
                rec.kind,
                rec.tx_type,
                rec.ref_id,
                rec.period_key,
                rec.payload.hex(),
                rec.source_id,
                rec.created_ms,
            ),
        )

    async def delete_war_credit_owed(self, player_id: int, op_id: int) -> bool:
        await self._cur.execute(
            "DELETE FROM `guild_war_credit_owed` WHERE `player_id`=%s AND `op_id`=%s",
            (player_id, op_id),
        )
        return self._cur.rowcount == 1

    async def war_credit_owed_exists(self, player_id: int, op_id: int) -> bool:
        await self._cur.execute(
            "SELECT 1 FROM `guild_war_credit_owed` WHERE `player_id`=%s AND `op_id`=%s",
            (player_id, op_id),
        )
        return await self._cur.fetchone() is not None

    async def list_war_credit_owed(self, after: WarCreditOwedCursor, limit: int) -> list[WarCreditOwed]:
        await self._cur.execute(
            "SELECT `player_id`,`op_id`,`guild_id`,`kind`,`tx_type`,`ref_id`,`period_key`,`payload`,`source_id`,`created_ms` "
            "FROM `guild_war_credit_owed` "
            "WHERE `created_ms`>%s OR (`created_ms`=%s AND (`player_id`>%s OR (`player_id`=%s AND `op_id`>%s))) "
            "ORDER BY `created_ms` ASC, `player_id` ASC, `op_id` ASC LIMIT %s",
            (after.created_ms, after.created_ms, after.player_id, after.player_id, after.op_id, limit),
        )
        out: list[WarCreditOwed] = []
        for row in await self._cur.fetchall():
            out.append(
                WarCreditOwed(
                    player_id=int(row[0]),
                    op_id=int(row[1]),
                    guild_id=int(row[2]),
                    kind=int(row[3]),
                    tx_type=int(row[4]),
                    ref_id=int(row[5]),
                    period_key=int(row[6]),
                    payload=_to_bytes(row[7]),
                    source_id=int(row[8]),
                    created_ms=int(row[9]),
                )
            )
        return out

    async def count_war_credit_owed(self, limit: int) -> int:
        if limit <= 0:
            return 0
        await self._cur.execute(SQL_COUNT_WAR_CREDIT_OWED_CAPPED, (int(limit),))
        row = await self._cur.fetchone()
        return int(row[0]) if row else 0


class SqlGuildStore(GuildStoreCore):
    """guild MySQL/TiDB 权威存储实现。"""

    def __init__(
        self,
        pool,
        protocol: Protocol,
        *,
        catalog: GuildTableCatalog,
        guild_ids: Callable[[], Awaitable[int]],
        op_ids: Callable[[], Awaitable[int]],
        role_directory: RoleFenceDirectory | None = None,
        merged_zones: MergedZones | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        super().__init__(
            protocol,
            catalog=catalog,
            guild_ids=guild_ids,
            op_ids=op_ids,
            role_directory=role_directory,
            merged_zones=merged_zones,
            clock_ms=clock_ms,
        )
        self._pool = pool

    async def bootstrap(self, schema_path: Path = SCHEMA_FILE) -> None:
        """幂等执行 config/guild_schema.sql 建表语句。"""
        statements = load_schema_statements(schema_path)
        async with portal_sql.connection(self._pool) as conn:
            async with conn.cursor() as cur:
                for stmt in statements:
                    await cur.execute(stmt)
            await conn.commit()

    async def verify_schema(self) -> None:
        """启动期核对核心表是否存在（缺失时 fail-closed 拒启）。"""
        async with portal_sql.connection(self._pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SHOW TABLES")
                existing = {_to_str(row[0]).lower() for row in await cur.fetchall()}
        missing = [name for name in REQUIRED_TABLES if name not in existing]
        if missing:
            raise StoreUnavailableError(f"guild schema missing required tables: {missing}")

    async def _run_tx(self, fn: Callable[[_SqlTx], Awaitable[T]], *, read_only: bool = False) -> T:
        if read_only:
            async with portal_sql.connection(self._pool) as conn:
                async with conn.cursor() as cur:
                    result = await fn(_SqlTx(cur))
                await conn.rollback()
                return result

        for attempt in range(1, TX_MAX_ATTEMPTS + 1):
            try:
                async with portal_sql.connection(self._pool) as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        await conn.begin()
                        try:
                            result = await fn(_SqlTx(cur))
                        except _TxCommitThen as commit_then:
                            await conn.commit()
                            raise commit_then.inner
                        except _TxRollbackReturn as rollback_ret:
                            await conn.rollback()
                            return rollback_ret.value
                        except BaseException:
                            await conn.rollback()
                            raise
                        await conn.commit()
                        return result
            except pymysql.MySQLError as exc:
                errno = _mysql_errno(exc)
                if errno == LOCK_WAIT_TIMEOUT_ERRNO:
                    raise GuildWriteConflictError(f"guild lock wait timeout: {exc}") from exc
                if errno in RETRYABLE_ERRORS:
                    if attempt >= TX_MAX_ATTEMPTS:
                        raise GuildWriteConflictError(f"guild tx retryable conflict after {attempt} attempts: {exc}") from exc
                    await asyncio.sleep(0.01 + random.random() * 0.04)
                    continue
                raise
        raise GuildWriteConflictError("guild tx retry exhausted")
