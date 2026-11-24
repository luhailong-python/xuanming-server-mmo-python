"""match 与 team MySQL/TiDB 权威存储实现（SqlMatchStore）。

所有二进制字段统一通过 ``UNHEX(%s)`` 绑定，避免 utf8mb4 连接下二进制字节损坏；
全部写事务运行在 ``READ COMMITTED`` 隔离级别下，对 1205/1213/9007 瞬态冲突做有界重试。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
import uuid
from pathlib import Path
from typing import Callable

import aiomysql
import pymysql
from google.protobuf.message import Message

from .. import portal_sql
from ..protocol import Protocol
from .errors import (
    BattleOutcomeOriginalError,
    ERR_MATCH_ALREADY_QUEUED,
    ERR_MATCH_BATTLE_REPORT_NOT_FOUND,
    ERR_MATCH_CANCEL_TOO_LATE,
    ERR_MATCH_CHALLENGE_EXPIRED,
    ERR_MATCH_CHALLENGE_NOT_TARGET,
    ERR_MATCH_CHALLENGE_PENDING,
    ERR_MATCH_CHALLENGE_SELF,
    ERR_MATCH_CHALLENGE_SELF_BUSY,
    ERR_MATCH_CHALLENGE_TARGET_BUSY,
    ERR_MATCH_IN_BATTLE,
    ERR_MATCH_MODE_NOT_OPEN,
    ERR_MATCH_SPECTATE_WHILE_QUEUED,
    ERR_MATCH_TICKET_MISMATCH,
    ERR_TEAM_APPLICATION_NOT_FOUND,
    ERR_TEAM_APPOINT_NOT_LEADER,
    ERR_TEAM_APPOINT_SELF,
    ERR_TEAM_CROSS_ZONE_DENIED,
    ERR_TEAM_DISBAND_NOT_LEADER,
    ERR_TEAM_IN_MATCH,
    ERR_TEAM_INVITE_LIMIT,
    ERR_TEAM_INVITE_NOT_FOUND,
    ERR_TEAM_KICK_NOT_LEADER,
    ERR_TEAM_KICK_SELF,
    ERR_TEAM_MEMBERS_FULL,
    ERR_TEAM_MEMBER_IN_TEAM,
    ERR_TEAM_MEMBER_NOT_IN_TEAM,
    ERR_TEAM_NOT_LEADER,
    ERR_TEAM_NO_TEAM,
    ERR_TEAM_PLAYER_ID,
    ERR_TEAM_SIZE_EXCEEDED,
    InvalidRoleFenceStateError,
    MatchBusinessError,
    OriginalRoleError,
    StoreUnavailableError,
    TeamBusinessError,
    UnterminableOriginalError,
)
from .model import (
    APPLICATION_TTL_MS,
    CHALLENGE_STATE_DECLINED,
    CHALLENGE_STATE_FORMED,
    CHALLENGE_STATE_PENDING,
    COMPLETION_SOURCE_BATTLE_RPC,
    FORMATION_STATE_ABORTED,
    FORMATION_STATE_CREATED,
    FORMATION_STATE_PERSISTED,
    FORMATION_STATE_PREPARING,
    FORMATION_STATE_UNKNOWN,
    INVITE_TTL_MS,
    MAX_APPLICATIONS_PER_TEAM,
    MAX_BATTLE_REPORTS,
    MAX_INVITES_PER_PLAYER,
    MAX_TEAM_SIZE,
    MODE_PVE_TEAM,
    MODE_PVP_CHALLENGE,
    OCCUPANCY_KIND_GUILD_TRIAL,
    OUTCOME_DECISION_ABORT,
    OUTCOME_DECISION_COMPLETE,
    OUTCOME_DECISION_UNKNOWN,
    ROLE_BLOCKER_OCCUPANCY,
    ROLE_BLOCKER_SETTLEMENT,
    ROLE_BLOCKER_SPECTATOR,
    ROLE_BLOCKER_TEAM,
    ROLE_BLOCKER_TEAM_LAUNCH,
    ROLE_BLOCKER_TICKET,
    SETTLEMENT_DISPATCH_APPLIED,
    SETTLEMENT_DISPATCH_PENDING,
    SPECTATOR_STATE_ACTIVE,
    SPECTATOR_STATE_CLOSED,
    SPECTATOR_STATE_PENDING_ADD,
    TEAM_IDLE_TTL_MS,
    TEAM_LAUNCH_CLOSED,
    TEAM_LAUNCH_CREATED,
    TEAM_LAUNCH_FORMING,
    TEAM_LAUNCH_QUEUED,
    TEAM_STATE_ACTIVE,
    TEAM_STATE_DISBANDED,
    TERMINAL_OUTCOME_GRACE_MS,
    TERMINAL_OUTCOME_NO_RETRY_MS,
    TICKET_STATE_DONE,
    TICKET_STATE_FORMING,
    TICKET_STATE_QUEUED,
    TICKET_STATE_READY,
    CallerIdentity,
    RoleFenceRow,
    build_battle_report_summary,
    classify_stranded_formation,
    completable_original,
    effective_deadline_ms,
    formation_digest,
    formation_outcome_decided,
    is_valid_uuid,
    preferred_region,
    uuid_to_pair,
    valid_role_fence_state,
    validate_terminal_against_formation,
    validate_terminal_payload,
)
from .store import (
    CompletionOutboxRow,
    IncomingInviteInfo,
    QueueStatusInfo,
    SettlementOutboxRow,
    StrandedFormationEntry,
    TeamMutationOutcome,
    TeamSnapshotResult,
)

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "match_schema.sql"
DB_NOW_MS = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)"
LOCK_ERRORS = (1205, 1213, 9007)
TX_MAX_ATTEMPTS = 3

REQUIRED_TABLES = (
    "team_authority",
    "team_membership",
    "team_invite_index",
    "team_application_index",
    "team_action_receipt",
    "team_launch",
    "team_launch_role",
    "match_network_profile",
    "match_authority_ticket",
    "match_challenge_invitation",
    "match_formation",
    "match_battle_completion",
    "match_battle_settlement_source",
    "match_spectator",
    "match_battle_report",
    "role_fence",
)

_APPLY_ROLE_FENCE_SQL = (
    "INSERT INTO `role_fence`(`player_id`,`account`,`owner_revision`,`freeze_token`,"
    "`freeze_final`,`version`,`home_zone`,`updated_ms`) "
    f"VALUES (%s,UNHEX(%s),%s,%s,%s,%s,%s,{DB_NOW_MS}) "
    "ON DUPLICATE KEY UPDATE "
    "`account`=IF(VALUES(`version`)>`version`,VALUES(`account`),`account`),"
    "`owner_revision`=IF(VALUES(`version`)>`version`,VALUES(`owner_revision`),`owner_revision`),"
    "`freeze_token`=IF(VALUES(`version`)>`version`,VALUES(`freeze_token`),`freeze_token`),"
    "`freeze_final`=IF(VALUES(`version`)>`version`,VALUES(`freeze_final`),`freeze_final`),"
    "`home_zone`=IF(VALUES(`version`)>`version`,VALUES(`home_zone`),`home_zone`),"
    "`updated_ms`=IF(VALUES(`version`)>`version`,VALUES(`updated_ms`),`updated_ms`),"
    "`version`=IF(VALUES(`version`)>`version`,VALUES(`version`),`version`)"
)

_FORMATION_COLS = (
    "`battle_id`,`operation_id`,`state`,`physical_region`,`target_node_id`,`target_instance_id`,"
    "`target_endpoint`,`mode`,`config_id`,`members`,`create_request`,`created_ms`,`deadline_ms`,"
    "`retry_at_ms`,`activity_context`,`requeue_on_failure`,`prepared`,`outcome_protocol_version`,"
    "`preflight_supported_version`,`preflight_instance_id`,`adopted_outcome_protocol_version`,"
    "`adopted_outcome_observed`,`release_track`,`ranked_context`,`ranked_season_id`"
)

_TICKET_COLS = (
    "`player_id`,`ticket`,`request_id`,`mode`,`config_id`,`team_size`,`state`,`enqueued_ms`,"
    "`not_before_ms`,`zone_id`,`rating_centi`,`formation_id`,`team_id`,`network_profile`,"
    "`expires_ms`,`selected_region`,`owner_account`,`owner_revision`,`pool_region`,`ranked_context`"
)


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


def _hex(data: bytes) -> str:
    return data.hex() if data else ""


def _mysql_errno(exc: BaseException) -> int:
    if isinstance(exc, pymysql.MySQLError) and exc.args:
        try:
            return int(exc.args[0])
        except (TypeError, ValueError):
            return 0
    return 0


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
    """打开 aiomysql 连接池（autocommit=True，由事务块显式控制 BEGIN/COMMIT）。"""
    return await aiomysql.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        db=database,
        minsize=minsize,
        maxsize=maxsize,
        autocommit=True,
        charset="utf8mb4",
    )


class SqlMatchStore:
    """基于 aiomysql 的 mmorpg_match 权威存储实现。"""

    def __init__(
        self,
        pool,
        protocol: Protocol,
        *,
        node_id: int = 1,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.pool = pool
        self.protocol = protocol
        self._node_id = node_id & 0xFFFF
        self._clock = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._seq = 0
        self._last_ms = 0
        self._id_lock = asyncio.Lock()
        # 运营赛事标记内存镜像（轻量非事务索引，与 SQL 活跃对局联动）
        self._featured: set[int] = set()

    async def now_ms(self) -> int:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT {DB_NOW_MS}")
                row = await cur.fetchone()
                return int(row[0]) if row else int(self._clock())

    async def next_id(self) -> int:
        """进程内单调雪花 ID（41 位毫秒 + 12 位节点 + 10 位序号，不与其他节点撞号）。"""
        async with self._id_lock:
            now = int(self._clock())
            if now <= self._last_ms:
                self._seq = (self._seq + 1) & 0x3FF
                if self._seq == 0:
                    self._last_ms += 1
            else:
                self._last_ms = now
                self._seq = 0
            return ((self._last_ms & 0x1FFFFFFFFFF) << 22) | ((self._node_id & 0xFFF) << 10) | self._seq

    async def bootstrap(self) -> None:
        """执行 config/match_schema.sql 幂等建表。"""
        sql_text = SCHEMA_FILE.read_text(encoding="utf-8")
        statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                for stmt in statements:
                    await cur.execute(stmt)

    async def verify_schema(self) -> None:
        """启动期核对全部必需权威表是否已存在。"""
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SHOW TABLES")
                existing = {_to_str(row[0]).lower() for row in await cur.fetchall()}
        missing = [t for t in REQUIRED_TABLES if t not in existing]
        if missing:
            raise StoreUnavailableError(f"missing required match schema tables: {missing}")

    async def _run_tx(self, work):
        """在 READ COMMITTED 隔离级别下执行事务，并对 1205/1213/9007 做有界重试。"""
        for attempt in range(TX_MAX_ATTEMPTS):
            try:
                async with portal_sql.connection(self.pool) as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        await conn.begin()
                        try:
                            result = await work(cur)
                        except BaseException:
                            try:
                                await conn.rollback()
                            except Exception:
                                pass
                            raise
                        await conn.commit()
                        return result
            except pymysql.MySQLError as exc:
                if _mysql_errno(exc) in LOCK_ERRORS and attempt + 1 < TX_MAX_ATTEMPTS:
                    await asyncio.sleep(0.02 * (attempt + 1) + random.uniform(0.0, 0.02))
                    continue
                raise StoreUnavailableError(f"sql transaction failed: {exc}") from exc

    async def _db_now(self, cur) -> int:
        await cur.execute(f"SELECT {DB_NOW_MS}")
        row = await cur.fetchone()
        return int(row[0])

    # -- 编解码辅助 -----------------------------------------------------------------------------------

    def _encode_members_blob(self, members) -> bytes:
        box = self.protocol.message("match.MatchFormationRecord")
        for m in members:
            box.members.add().CopyFrom(m)
        return box.SerializeToString(deterministic=True)

    def _encode_prepared_blob(self, prepared) -> bytes:
        box = self.protocol.message("match.MatchFormationRecord")
        for p in prepared:
            box.prepared.add().CopyFrom(p)
        return box.SerializeToString(deterministic=True)

    def _scan_formation(self, row) -> Message:
        f = self.protocol.message(
            "match.MatchFormationRecord",
            battle_id=int(row[0]),
            operation_id=_to_str(row[1]),
            state=int(row[2]),
            physical_region=int(row[3]),
            target_node_id=int(row[4]),
            target_instance_id=_to_str(row[5]),
            target_endpoint=_to_str(row[6]),
            mode=int(row[7]),
            config_id=int(row[8]),
            create_request=_to_bytes(row[10]),
            created_ms=int(row[11]),
            deadline_ms=int(row[12]),
            retry_at_ms=int(row[13]),
            activity_context=_to_bytes(row[14]),
            requeue_on_failure=bool(row[15]),
            outcome_protocol_version=int(row[17]),
            preflight_supported_version=int(row[18]),
            preflight_instance_id=_to_str(row[19]),
            adopted_outcome_protocol_version=int(row[20]),
            adopted_outcome_observed=bool(row[21]),
            release_track=_to_str(row[22]),
            ranked_context=_to_bytes(row[23]),
            ranked_season_id=int(row[24]),
        )
        raw_members = _to_bytes(row[9])
        if raw_members:
            box = self.protocol.parse("match.MatchFormationRecord", raw_members)
            f.members.extend(box.members)
        raw_prepared = _to_bytes(row[16])
        if raw_prepared:
            box = self.protocol.parse("match.MatchFormationRecord", raw_prepared)
            f.prepared.extend(box.prepared)
        return f

    def _scan_ticket(self, row) -> Message:
        return self.protocol.message(
            "match.MatchAuthorityTicketRecord",
            player_id=int(row[0]),
            ticket=_to_str(row[1]),
            request_id=_to_str(row[2]),
            mode=int(row[3]),
            config_id=int(row[4]),
            team_size=int(row[5]),
            state=int(row[6]),
            enqueued_ms=int(row[7]),
            not_before_ms=int(row[8]),
            zone_id=int(row[9]),
            rating_centi=int(row[10]),
            formation_id=int(row[11]),
            team_id=int(row[12]),
            network_profile=_to_bytes(row[13]),
            expires_ms=int(row[14]),
            selected_region=int(row[15]),
            owner_account=_to_str(row[16]),
            owner_revision=int(row[17]),
            pool_region=int(row[18]),
            ranked_context=_to_bytes(row[19]),
        )

    def _scan_fence(self, row) -> RoleFenceRow:
        return RoleFenceRow(
            player_id=int(row[0]),
            account=_to_str(row[1]),
            owner_revision=int(row[2]),
            freeze_token=int(row[3]),
            freeze_final=bool(row[4]),
            version=int(row[5]),
            home_zone=int(row[6]),
            updated_ms=int(row[7]),
        )

    async def _lock_fence(self, cur, player_id: int) -> RoleFenceRow | None:
        await cur.execute(
            "SELECT `player_id`,`account`,`owner_revision`,`freeze_token`,`freeze_final`,"
            "`version`,`home_zone`,`updated_ms` FROM `role_fence` WHERE `player_id`=%s FOR UPDATE",
            (player_id,),
        )
        row = await cur.fetchone()
        return self._scan_fence(row) if row else None

    async def _check_fence_tx(self, cur, player_id: int, account: str, owner_revision: int) -> None:
        fence = await self._lock_fence(cur, player_id)
        if fence is None:
            return
        if fence.account == "" or fence.freeze_token != 0 or fence.freeze_final:
            raise OriginalRoleError(f"role frozen or deleted player={player_id}")
        if account and fence.account != account:
            raise OriginalRoleError(f"role account mismatch player={player_id}")
        if owner_revision and fence.owner_revision != owner_revision:
            raise OriginalRoleError(f"role owner_revision mismatch player={player_id}")

    # -- RoleFence ------------------------------------------------------------------------------------

    async def get_role_fence(self, player_id: int) -> RoleFenceRow | None:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT `player_id`,`account`,`owner_revision`,`freeze_token`,`freeze_final`,"
                    "`version`,`home_zone`,`updated_ms` FROM `role_fence` WHERE `player_id`=%s",
                    (player_id,),
                )
                row = await cur.fetchone()
                return self._scan_fence(row) if row else None

    async def verify_original_owner(self, player_id: int, account: str, owner_revision: int) -> None:
        fence = await self.get_role_fence(player_id)
        if fence is None:
            return
        if fence.account == "" or fence.freeze_token != 0 or fence.freeze_final:
            raise OriginalRoleError(f"role frozen or deleted player={player_id}")
        if account and fence.account != account:
            raise OriginalRoleError(f"role account mismatch player={player_id}")
        if owner_revision and fence.owner_revision != owner_revision:
            raise OriginalRoleError(f"role owner_revision mismatch player={player_id}")

    async def _pending_blocker_tx(self, cur, player_id: int, now: int) -> str:
        await cur.execute(
            "SELECT `state` FROM `match_authority_ticket` WHERE `player_id`=%s AND `state` IN (1,2,3) LIMIT 1",
            (player_id,),
        )
        if await cur.fetchone():
            return ROLE_BLOCKER_TICKET
        await cur.execute("SELECT `team_id` FROM `team_membership` WHERE `player_id`=%s AND `team_id`>0 LIMIT 1", (player_id,))
        if await cur.fetchone():
            return ROLE_BLOCKER_TEAM
        await cur.execute(
            "SELECT 1 FROM `team_launch_role` WHERE `player_id`=%s AND `state`<>%s LIMIT 1",
            (player_id, TEAM_LAUNCH_CLOSED),
        )
        if await cur.fetchone():
            return ROLE_BLOCKER_TEAM_LAUNCH
        await cur.execute(
            "SELECT 1 FROM `match_spectator` WHERE `player_id`=%s AND `state`<>%s LIMIT 1",
            (player_id, SPECTATOR_STATE_CLOSED),
        )
        if await cur.fetchone():
            return ROLE_BLOCKER_SPECTATOR
        await cur.execute(
            "SELECT 1 FROM `match_role_occupancy` WHERE `player_id`=%s AND `expires_ms`>%s LIMIT 1",
            (player_id, now),
        )
        if await cur.fetchone():
            return ROLE_BLOCKER_OCCUPANCY
        await cur.execute(
            "SELECT 1 FROM `match_battle_settlement_source` WHERE `player_id`=%s AND `dispatch_state`=%s LIMIT 1",
            (player_id, SETTLEMENT_DISPATCH_PENDING),
        )
        if await cur.fetchone():
            return ROLE_BLOCKER_SETTLEMENT
        return ""

    async def prepare_role_freeze(self, state: Message, *, dry_run: bool = False) -> Message:
        if not valid_role_fence_state(state) or int(state.freeze_token) == 0 or bool(state.freeze_final):
            raise InvalidRoleFenceStateError("invalid RoleFenceState for PrepareRoleFreeze")
        pid = int(state.player_id)
        if dry_run:
            async with portal_sql.connection(self.pool) as conn:
                async with conn.cursor() as cur:
                    now = await self._db_now(cur)
                    blocker = await self._pending_blocker_tx(cur, pid, now)
                    return self.protocol.message(
                        "rolefence.PrepareRoleFreezeResponse", clean=(blocker == ""), blocker=blocker
                    )

        async def work(cur):
            now = await self._db_now(cur)
            await cur.execute(
                _APPLY_ROLE_FENCE_SQL,
                (
                    pid,
                    _hex(state.account.encode("utf-8")),
                    int(state.owner_revision),
                    int(state.freeze_token),
                    int(bool(state.freeze_final)),
                    int(state.version),
                    int(state.home_zone),
                ),
            )
            fence = await self._lock_fence(cur, pid)
            if fence is None:
                raise StoreUnavailableError("role_fence missing after apply")
            if fence.version != int(state.version) or fence.freeze_token != int(state.freeze_token):
                return self.protocol.message("rolefence.PrepareRoleFreezeResponse", stale=True)
            blocker = await self._pending_blocker_tx(cur, pid, now)
            if blocker:
                return self.protocol.message("rolefence.PrepareRoleFreezeResponse", blocker=blocker)
            return self.protocol.message("rolefence.PrepareRoleFreezeResponse", clean=True)

        return await self._run_tx(work)

    async def apply_role_fence(self, state: Message) -> Message:
        if not valid_role_fence_state(state):
            raise InvalidRoleFenceStateError("invalid RoleFenceState for ApplyRoleFence")

        async def work(cur):
            await cur.execute(
                _APPLY_ROLE_FENCE_SQL,
                (
                    int(state.player_id),
                    _hex(state.account.encode("utf-8")),
                    int(state.owner_revision),
                    int(state.freeze_token),
                    int(bool(state.freeze_final)),
                    int(state.version),
                    int(state.home_zone),
                ),
            )
            return self.protocol.message("rolefence.ApplyRoleFenceResponse")

        return await self._run_tx(work)

    async def purge_removed_role(self, state: Message) -> Message:
        if not valid_role_fence_state(state) or state.account != "" or int(state.freeze_token) != 0:
            raise InvalidRoleFenceStateError("PurgeRemovedRole requires tombstone state")
        pid = int(state.player_id)

        async def work(cur):
            await cur.execute(
                _APPLY_ROLE_FENCE_SQL,
                (pid, "", int(state.owner_revision), 0, int(bool(state.freeze_final)), int(state.version), int(state.home_zone)),
            )
            fence = await self._lock_fence(cur, pid)
            if fence is None or fence.account != "":
                raise StoreUnavailableError("role_fence is not tombstone")
            for table, col in (
                ("match_network_profile", "player_id"),
                ("match_authority_ticket", "player_id"),
                ("match_role_occupancy", "player_id"),
                ("match_spectator", "player_id"),
                ("match_player_battle_report", "player_id"),
                ("team_invite_index", "invitee_id"),
                ("team_application_index", "applicant_id"),
            ):
                await cur.execute(f"DELETE FROM `{table}` WHERE `{col}`=%s", (pid,))
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse")

        return await self._run_tx(work)

    # -- Team Helpers & CRUD --------------------------------------------------------------------------

    async def _bump_membership_tx(self, cur, player_id: int, team_id: int) -> int:
        await cur.execute("SELECT `epoch` FROM `team_membership` WHERE `player_id`=%s FOR UPDATE", (player_id,))
        row = await cur.fetchone()
        epoch = (int(row[0]) if row else 0) + 1
        await cur.execute(
            "INSERT INTO `team_membership`(`player_id`,`team_id`,`epoch`) VALUES (%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE `team_id`=VALUES(`team_id`),`epoch`=VALUES(`epoch`)",
            (player_id, team_id, epoch),
        )
        return epoch

    async def _epochs_tx(self, cur, player_ids: list[int]) -> dict[int, int]:
        out: dict[int, int] = {}
        for pid in set(player_ids):
            await cur.execute("SELECT `epoch` FROM `team_membership` WHERE `player_id`=%s", (pid,))
            row = await cur.fetchone()
            out[pid] = int(row[0]) if row else 0
        return out

    async def _load_team_for_update(self, cur, team_id: int) -> tuple[int, Message, int] | None:
        await cur.execute(
            "SELECT `version`,`payload`,`state` FROM `team_authority` WHERE `team_id`=%s FOR UPDATE",
            (team_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        data = self.protocol.parse("teampb.TeamAuthorityData", _to_bytes(row[1]))
        return int(row[0]), data, int(row[2])

    async def _save_team_tx(self, cur, team_id: int, version: int, data: Message, state: int, idle_expires_ms: int) -> None:
        raw = data.SerializeToString(deterministic=True)
        await cur.execute(
            "INSERT INTO `team_authority`(`team_id`,`version`,`payload`,`state`,`idle_expires_ms`) "
            "VALUES (%s,%s,UNHEX(%s),%s,%s) "
            "ON DUPLICATE KEY UPDATE `version`=VALUES(`version`),`payload`=VALUES(`payload`),"
            "`state`=VALUES(`state`),`idle_expires_ms`=VALUES(`idle_expires_ms`)",
            (team_id, version, _hex(raw), state, idle_expires_ms),
        )

    async def _get_membership_tx(self, cur, player_id: int, *, for_update: bool = False) -> tuple[int, int]:
        suffix = " FOR UPDATE" if for_update else ""
        await cur.execute(f"SELECT `team_id`,`epoch` FROM `team_membership` WHERE `player_id`=%s{suffix}", (player_id,))
        row = await cur.fetchone()
        return (int(row[0]), int(row[1])) if row else (0, 0)

    def _team_locked(self, rec: Message, now: int) -> bool:
        if not rec.match_lock_token:
            return False
        exp = int(rec.match_lock_expire_at_ms)
        return exp == 0 or exp > now

    async def _cancel_queued_team_launch_tx(self, cur, team_id: int) -> None:
        await cur.execute(
            "SELECT `actor_id`,`request_high`,`request_low` FROM `team_launch` WHERE `team_id`=%s AND `state`=%s FOR UPDATE",
            (team_id, TEAM_LAUNCH_QUEUED),
        )
        rows = await cur.fetchall()
        for a, h, l in rows:
            await cur.execute(
                "UPDATE `team_launch` SET `state`=%s WHERE `actor_id`=%s AND `request_high`=%s AND `request_low`=%s",
                (TEAM_LAUNCH_CLOSED, int(a), int(h), int(l)),
            )
            await cur.execute(
                "UPDATE `team_launch_role` SET `state`=%s WHERE `actor_id`=%s AND `request_high`=%s AND `request_low`=%s",
                (TEAM_LAUNCH_CLOSED, int(a), int(h), int(l)),
            )
        await cur.execute(
            "UPDATE `match_authority_ticket` SET `state`=%s WHERE `team_id`=%s AND `state`=%s",
            (TICKET_STATE_DONE, team_id, TICKET_STATE_QUEUED),
        )

    async def create_team(self, caller: CallerIdentity, zone_id: int, request_id: str) -> TeamMutationOutcome:
        team_id = await self.next_id()

        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            cur_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if cur_tid != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            now = await self._db_now(cur)
            data = self.protocol.message("teampb.TeamAuthorityData")
            rec = data.record
            rec.team_id = team_id
            rec.leader_id = caller.player_id
            rec.zone_id = zone_id
            rec.created_at_ms = now
            rec.next_join_seq = 2
            rec.members.add(player_id=caller.player_id, zone_id=zone_id, joined_at_ms=now, join_seq=1)
            data.members.add(player_id=caller.player_id, owner_account=caller.account, owner_revision=caller.owner_revision)
            await self._save_team_tx(cur, team_id, 1, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            epoch = await self._bump_membership_tx(cur, caller.player_id, team_id)
            return TeamMutationOutcome(
                team_id=team_id, version=1, record=rec, epochs={caller.player_id: epoch},
                reason=1, actor_id=caller.player_id, now_ms=now, joined=(caller.player_id,),
            )

        return await self._run_tx(work)

    async def get_my_team(
        self, player_id: int, *, match_request_id: str = "", caller: CallerIdentity | None = None
    ) -> TeamSnapshotResult:
        if caller is not None:
            await self.verify_original_owner(caller.player_id, caller.account, caller.owner_revision)
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                now = await self._db_now(cur)
                team_id, epoch = await self._get_membership_tx(cur, player_id)
                version = 0
                record = None
                if team_id:
                    await cur.execute(
                        "SELECT `version`,`payload`,`state` FROM `team_authority` WHERE `team_id`=%s",
                        (team_id,),
                    )
                    row = await cur.fetchone()
                    if row and int(row[2]) == TEAM_STATE_ACTIVE:
                        version = int(row[0])
                        record = self.protocol.parse("teampb.TeamAuthorityData", _to_bytes(row[1])).record
                    else:
                        team_id = 0
                l_state = l_bid = l_mode = l_cfg = l_target = l_enq = 0
                if match_request_id and is_valid_uuid(match_request_id):
                    h, l = uuid_to_pair(match_request_id)
                    await cur.execute(
                        "SELECT `state`,`battle_id`,`mode`,`config_id`,`target_size`,`enqueued_ms`,"
                        "`owner_account`,`owner_revision` FROM `team_launch` "
                        "WHERE `actor_id`=%s AND `request_high`=%s AND `request_low`=%s",
                        (player_id, h, l),
                    )
                    lr = await cur.fetchone()
                    if lr:
                        if caller is not None and (
                            _to_str(lr[6]) != caller.account or int(lr[7]) != caller.owner_revision
                        ):
                            l_state = 6
                        else:
                            l_state, l_bid, l_mode, l_cfg, l_target, l_enq = (
                                int(lr[0]), int(lr[1]), int(lr[2]), int(lr[3]), int(lr[4]), int(lr[5])
                            )
                return TeamSnapshotResult(
                    team_id=team_id, membership_epoch=epoch, version=version, record=record, now_ms=now,
                    launch_state=l_state, launch_battle_id=l_bid, launch_mode=l_mode,
                    launch_config_id=l_cfg, launch_target_size=l_target, launch_enqueued_ms=l_enq,
                )

    async def read_membership(
        self, player_id: int, *, owner_revision: int = 0
    ) -> tuple[int, int, int, Message | None]:
        await self.verify_original_owner(player_id, "", owner_revision)
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                team_id, epoch = await self._get_membership_tx(cur, player_id)
                if team_id == 0:
                    return 0, epoch, 0, None
                await cur.execute(
                    "SELECT `version`,`payload`,`state` FROM `team_authority` WHERE `team_id`=%s",
                    (team_id,),
                )
                row = await cur.fetchone()
                if not row or int(row[2]) != TEAM_STATE_ACTIVE:
                    return 0, epoch, 0, None
                data = self.protocol.parse("teampb.TeamAuthorityData", _to_bytes(row[1]))
                return team_id, epoch, int(row[0]), data.record

    async def apply_join_team(
        self, caller: CallerIdentity, target_player_id: int, zone_id: int, request_id: str, *, allow_cross_zone: bool
    ) -> TeamMutationOutcome:
        if target_player_id == 0 or target_player_id == caller.player_id:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)

        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            my_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if my_tid != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            target_tid, _ = await self._get_membership_tx(cur, target_player_id)
            if target_tid == 0:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            loaded = await self._load_team_for_update(cur, target_tid)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            version, data, _ = loaded
            rec = data.record
            now = await self._db_now(cur)
            if not allow_cross_zone and rec.zone_id and zone_id and int(rec.zone_id) != zone_id:
                raise TeamBusinessError(ERR_TEAM_CROSS_ZONE_DENIED)
            if len(rec.members) >= MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_MEMBERS_FULL)
            keep_apps = [a for a in rec.applications if int(a.expire_at_ms) > now]
            del rec.applications[:]
            rec.applications.extend(keep_apps)
            if len(rec.applications) >= MAX_APPLICATIONS_PER_TEAM and not any(
                int(a.player_id) == caller.player_id for a in rec.applications
            ):
                raise TeamBusinessError(ERR_TEAM_INVITE_LIMIT)
            expire_at = now + APPLICATION_TTL_MS
            existing = next((a for a in rec.applications if int(a.player_id) == caller.player_id), None)
            if existing is not None:
                existing.applied_at_ms = now
                existing.expire_at_ms = expire_at
            else:
                rec.applications.add(player_id=caller.player_id, zone_id=zone_id, applied_at_ms=now, expire_at_ms=expire_at)
            await cur.execute(
                "INSERT INTO `team_application_index`(`applicant_id`,`team_id`,`expires_ms`) VALUES (%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE `expires_ms`=VALUES(`expires_ms`)",
                (caller.player_id, target_tid, expire_at),
            )
            version += 1
            await self._save_team_tx(cur, target_tid, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            mids = [int(m.player_id) for m in rec.members]
            return TeamMutationOutcome(
                team_id=target_tid, version=version, record=rec,
                epochs=await self._epochs_tx(cur, mids + [caller.player_id]),
                reason=8, actor_id=caller.player_id, now_ms=now, kept=tuple(mids),
            )

        return await self._run_tx(work)

    async def handle_application(
        self, caller: CallerIdentity, applicant_id: int, approve: bool, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome:
        if applicant_id == 0:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)

        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            my_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if my_tid == 0 or (expected_team_id and my_tid != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            loaded = await self._load_team_for_update(cur, my_tid)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            version, data, _ = loaded
            rec = data.record
            now = await self._db_now(cur)
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_NOT_LEADER)
            app = next((a for a in rec.applications if int(a.player_id) == applicant_id and int(a.expire_at_ms) > now), None)
            if app is None:
                raise TeamBusinessError(ERR_TEAM_APPLICATION_NOT_FOUND)
            if not approve:
                keep = [a for a in rec.applications if int(a.player_id) != applicant_id]
                del rec.applications[:]
                rec.applications.extend(keep)
                await cur.execute("DELETE FROM `team_application_index` WHERE `applicant_id`=%s AND `team_id`=%s", (applicant_id, my_tid))
                version += 1
                await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
                mids = [int(m.player_id) for m in rec.members]
                return TeamMutationOutcome(
                    team_id=my_tid, version=version, record=rec, epochs=await self._epochs_tx(cur, mids),
                    reason=8, actor_id=caller.player_id, now_ms=now, kept=tuple(mids), rejected_applicant=applicant_id,
                )
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            if len(rec.members) >= MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_MEMBERS_FULL)
            app_tid, _ = await self._get_membership_tx(cur, applicant_id, for_update=True)
            if app_tid != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            await self._cancel_queued_team_launch_tx(cur, my_tid)
            kept = [int(m.player_id) for m in rec.members]
            seq = int(rec.next_join_seq) or (len(rec.members) + 1)
            rec.next_join_seq = seq + 1
            rec.members.add(player_id=applicant_id, zone_id=int(app.zone_id), joined_at_ms=now, join_seq=seq)
            keep = [a for a in rec.applications if int(a.player_id) != applicant_id]
            del rec.applications[:]
            rec.applications.extend(keep)
            await cur.execute("DELETE FROM `team_application_index` WHERE `applicant_id`=%s AND `team_id`=%s", (applicant_id, my_tid))
            await self._bump_membership_tx(cur, applicant_id, my_tid)
            version += 1
            await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            return TeamMutationOutcome(
                team_id=my_tid, version=version, record=rec, epochs=await self._epochs_tx(cur, kept + [applicant_id]),
                reason=2, actor_id=applicant_id, now_ms=now, joined=(applicant_id,), kept=tuple(kept),
            )

        return await self._run_tx(work)

    async def invite_to_team(
        self,
        caller: CallerIdentity,
        target_player_id: int,
        target_zone_id: int,
        expected_team_id: int,
        request_id: str,
        *,
        allow_cross_zone: bool,
    ) -> TeamMutationOutcome:
        if target_player_id == 0 or target_player_id == caller.player_id:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)

        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            my_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if my_tid == 0 or (expected_team_id and my_tid != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            loaded = await self._load_team_for_update(cur, my_tid)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            version, data, _ = loaded
            rec = data.record
            now = await self._db_now(cur)
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_NOT_LEADER)
            if len(rec.members) >= MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_MEMBERS_FULL)
            target_tid, _ = await self._get_membership_tx(cur, target_player_id)
            if target_tid != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            if not allow_cross_zone and rec.zone_id and target_zone_id and int(rec.zone_id) != target_zone_id:
                raise TeamBusinessError(ERR_TEAM_CROSS_ZONE_DENIED)
            await cur.execute(
                "SELECT COUNT(*) FROM `team_invite_index` WHERE `invitee_id`=%s AND `expires_ms`>%s",
                (target_player_id, now),
            )
            cnt = int((await cur.fetchone())[0])
            if cnt >= MAX_INVITES_PER_PLAYER:
                raise TeamBusinessError(ERR_TEAM_INVITE_LIMIT)
            expire_at = now + INVITE_TTL_MS
            existing = next((i for i in rec.invites if int(i.invitee_id) == target_player_id), None)
            if existing is not None:
                existing.inviter_id = caller.player_id
                existing.invited_at_ms = now
                existing.expire_at_ms = expire_at
            else:
                rec.invites.add(
                    invitee_id=target_player_id, inviter_id=caller.player_id,
                    zone_id=target_zone_id, invited_at_ms=now, expire_at_ms=expire_at,
                )
            await cur.execute(
                "INSERT INTO `team_invite_index`(`invitee_id`,`team_id`,`expires_ms`) VALUES (%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE `expires_ms`=VALUES(`expires_ms`)",
                (target_player_id, my_tid, expire_at),
            )
            version += 1
            await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            mids = [int(m.player_id) for m in rec.members]
            return TeamMutationOutcome(
                team_id=my_tid, version=version, record=rec, epochs=await self._epochs_tx(cur, mids),
                reason=9, actor_id=caller.player_id, now_ms=now, kept=tuple(mids), invited_player=target_player_id,
            )

        return await self._run_tx(work)

    async def respond_invite(
        self, caller: CallerIdentity, team_id: int, accept: bool, zone_id: int, request_id: str
    ) -> TeamMutationOutcome:
        if team_id == 0:
            raise TeamBusinessError(ERR_TEAM_INVITE_NOT_FOUND)

        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            loaded = await self._load_team_for_update(cur, team_id)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_INVITE_NOT_FOUND)
            version, data, _ = loaded
            rec = data.record
            now = await self._db_now(cur)
            inv = next((i for i in rec.invites if int(i.invitee_id) == caller.player_id and int(i.expire_at_ms) > now), None)
            if inv is None:
                raise TeamBusinessError(ERR_TEAM_INVITE_NOT_FOUND)
            keep = [i for i in rec.invites if int(i.invitee_id) != caller.player_id]
            del rec.invites[:]
            rec.invites.extend(keep)
            await cur.execute("DELETE FROM `team_invite_index` WHERE `invitee_id`=%s AND `team_id`=%s", (caller.player_id, team_id))
            if not accept:
                version += 1
                await self._save_team_tx(cur, team_id, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
                mids = [int(m.player_id) for m in rec.members]
                return TeamMutationOutcome(
                    team_id=team_id, version=version, record=rec, epochs=await self._epochs_tx(cur, mids + [caller.player_id]),
                    reason=9, actor_id=caller.player_id, now_ms=now, kept=tuple(mids),
                )
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            my_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if my_tid != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            if len(rec.members) >= MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_MEMBERS_FULL)
            await self._cancel_queued_team_launch_tx(cur, team_id)
            kept = [int(m.player_id) for m in rec.members]
            seq = int(rec.next_join_seq) or (len(rec.members) + 1)
            rec.next_join_seq = seq + 1
            rec.members.add(player_id=caller.player_id, zone_id=zone_id, joined_at_ms=now, join_seq=seq)
            data.members.add(player_id=caller.player_id, owner_account=caller.account, owner_revision=caller.owner_revision)
            await self._bump_membership_tx(cur, caller.player_id, team_id)
            version += 1
            await self._save_team_tx(cur, team_id, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            return TeamMutationOutcome(
                team_id=team_id, version=version, record=rec, epochs=await self._epochs_tx(cur, kept + [caller.player_id]),
                reason=2, actor_id=caller.player_id, now_ms=now, joined=(caller.player_id,), kept=tuple(kept),
            )

        return await self._run_tx(work)

    async def list_my_invites(self, player_id: int) -> tuple[list[IncomingInviteInfo], int]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                now = await self._db_now(cur)
                await cur.execute(
                    "SELECT `team_id`,`expires_ms` FROM `team_invite_index` "
                    "WHERE `invitee_id`=%s AND `expires_ms`>%s ORDER BY `expires_ms`,`team_id`",
                    (player_id, now),
                )
                rows = await cur.fetchall()
                out: list[IncomingInviteInfo] = []
                for tid, _ in rows:
                    await cur.execute(
                        "SELECT `payload`,`state` FROM `team_authority` WHERE `team_id`=%s",
                        (int(tid),),
                    )
                    tr = await cur.fetchone()
                    if not tr or int(tr[1]) != TEAM_STATE_ACTIVE:
                        continue
                    rec = self.protocol.parse("teampb.TeamAuthorityData", _to_bytes(tr[0])).record
                    inv = next((i for i in rec.invites if int(i.invitee_id) == player_id and int(i.expire_at_ms) > now), None)
                    if inv is not None:
                        out.append(
                            IncomingInviteInfo(
                                team_id=int(tid), inviter_id=int(inv.inviter_id), leader_id=int(rec.leader_id),
                                member_count=len(rec.members), zone_id=int(rec.zone_id), expire_at_ms=int(inv.expire_at_ms),
                            )
                        )
                return out, now

    async def leave_team(self, caller: CallerIdentity, expected_team_id: int, request_id: str) -> TeamMutationOutcome:
        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            my_tid, my_epoch = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            now = await self._db_now(cur)
            if my_tid == 0:
                return TeamMutationOutcome(
                    team_id=0, version=0, record=None, epochs={caller.player_id: my_epoch},
                    reason=3, actor_id=caller.player_id, now_ms=now,
                )
            if expected_team_id and my_tid != expected_team_id:
                loaded = await self._load_team_for_update(cur, my_tid)
                return TeamMutationOutcome(
                    team_id=my_tid, version=loaded[0] if loaded else 0,
                    record=loaded[1].record if loaded else None, epochs={caller.player_id: my_epoch},
                    reason=0, actor_id=caller.player_id, now_ms=now,
                )
            loaded = await self._load_team_for_update(cur, my_tid)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                ep = await self._bump_membership_tx(cur, caller.player_id, 0)
                return TeamMutationOutcome(
                    team_id=0, version=0, record=None, epochs={caller.player_id: ep},
                    reason=3, actor_id=caller.player_id, now_ms=now, left=(caller.player_id,),
                )
            version, data, _ = loaded
            rec = data.record
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            await self._cancel_queued_team_launch_tx(cur, my_tid)
            remaining = [m for m in rec.members if int(m.player_id) != caller.player_id]
            await self._bump_membership_tx(cur, caller.player_id, 0)
            version += 1
            if not remaining:
                del rec.members[:]
                await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_DISBANDED, now)
                return TeamMutationOutcome(
                    team_id=my_tid, version=version, record=None,
                    epochs=await self._epochs_tx(cur, [caller.player_id]),
                    reason=7, actor_id=caller.player_id, now_ms=now, left=(caller.player_id,),
                )
            remaining.sort(key=lambda m: (int(m.join_seq), int(m.player_id)))
            del rec.members[:]
            rec.members.extend(remaining)
            leader_changed = False
            if int(rec.leader_id) == caller.player_id:
                rec.leader_id = int(remaining[0].player_id)
                leader_changed = True
            await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            kept = [int(m.player_id) for m in rec.members]
            return TeamMutationOutcome(
                team_id=my_tid, version=version, record=rec,
                epochs=await self._epochs_tx(cur, kept + [caller.player_id]),
                reason=3, actor_id=caller.player_id, now_ms=now,
                kept=tuple(kept), left=(caller.player_id,), leader_changed=leader_changed,
            )

        return await self._run_tx(work)

    async def kick_member(
        self, caller: CallerIdentity, target_player_id: int, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome:
        if target_player_id == 0:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)
        if target_player_id == caller.player_id:
            raise TeamBusinessError(ERR_TEAM_KICK_SELF)

        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            my_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if my_tid == 0 or (expected_team_id and my_tid != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            loaded = await self._load_team_for_update(cur, my_tid)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            version, data, _ = loaded
            rec = data.record
            now = await self._db_now(cur)
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_KICK_NOT_LEADER)
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            if not any(int(m.player_id) == target_player_id for m in rec.members):
                raise TeamBusinessError(ERR_TEAM_MEMBER_NOT_IN_TEAM)
            await self._cancel_queued_team_launch_tx(cur, my_tid)
            remaining = [m for m in rec.members if int(m.player_id) != target_player_id]
            del rec.members[:]
            rec.members.extend(remaining)
            await self._bump_membership_tx(cur, target_player_id, 0)
            version += 1
            await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            kept = [int(m.player_id) for m in rec.members]
            return TeamMutationOutcome(
                team_id=my_tid, version=version, record=rec,
                epochs=await self._epochs_tx(cur, kept + [target_player_id]),
                reason=4, actor_id=caller.player_id, now_ms=now, kept=tuple(kept), left=(target_player_id,),
            )

        return await self._run_tx(work)

    async def transfer_leader(
        self, caller: CallerIdentity, target_player_id: int, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome:
        if target_player_id == 0:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)
        if target_player_id == caller.player_id:
            raise TeamBusinessError(ERR_TEAM_APPOINT_SELF)

        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            my_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if my_tid == 0 or (expected_team_id and my_tid != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            loaded = await self._load_team_for_update(cur, my_tid)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            version, data, _ = loaded
            rec = data.record
            now = await self._db_now(cur)
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_APPOINT_NOT_LEADER)
            if not any(int(m.player_id) == target_player_id for m in rec.members):
                raise TeamBusinessError(ERR_TEAM_MEMBER_NOT_IN_TEAM)
            rec.leader_id = target_player_id
            version += 1
            await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            mids = [int(m.player_id) for m in rec.members]
            return TeamMutationOutcome(
                team_id=my_tid, version=version, record=rec, epochs=await self._epochs_tx(cur, mids),
                reason=5, actor_id=target_player_id, now_ms=now, kept=tuple(mids), leader_changed=True,
            )

        return await self._run_tx(work)

    async def disband_team(
        self, caller: CallerIdentity, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome:
        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            my_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if my_tid == 0 or (expected_team_id and my_tid != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            loaded = await self._load_team_for_update(cur, my_tid)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            version, data, _ = loaded
            rec = data.record
            now = await self._db_now(cur)
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_DISBAND_NOT_LEADER)
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            await self._cancel_queued_team_launch_tx(cur, my_tid)
            mids = [int(m.player_id) for m in rec.members]
            revoked = [int(i.invitee_id) for i in rec.invites]
            for pid in sorted(mids):
                await self._bump_membership_tx(cur, pid, 0)
            await cur.execute("DELETE FROM `team_invite_index` WHERE `team_id`=%s", (my_tid,))
            await cur.execute("DELETE FROM `team_application_index` WHERE `team_id`=%s", (my_tid,))
            del rec.members[:]
            del rec.invites[:]
            del rec.applications[:]
            version += 1
            await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_DISBANDED, now)
            return TeamMutationOutcome(
                team_id=my_tid, version=version, record=None, epochs=await self._epochs_tx(cur, mids),
                reason=7, actor_id=caller.player_id, now_ms=now, left=tuple(mids), revoked_invitees=tuple(revoked),
            )

        return await self._run_tx(work)

    async def confirm_ranked_preset(
        self,
        caller: CallerIdentity,
        expected_team_id: int,
        char_preset: int,
        pet_preset: int,
        rule_version: int,
        request_id: str,
    ) -> TeamMutationOutcome:
        async def work(cur) -> TeamMutationOutcome:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            my_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if my_tid == 0 or (expected_team_id and my_tid != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            loaded = await self._load_team_for_update(cur, my_tid)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            version, data, _ = loaded
            rec = data.record
            now = await self._db_now(cur)
            found = next((m for m in rec.members if int(m.player_id) == caller.player_id), None)
            if found is None:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            found.ranked_character_preset_id = char_preset
            found.ranked_pet_preset_id = pet_preset
            found.ranked_rule_version = rule_version
            found.ranked_preset_confirmed = True
            version += 1
            await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            mids = [int(m.player_id) for m in rec.members]
            return TeamMutationOutcome(
                team_id=my_tid, version=version, record=rec, epochs=await self._epochs_tx(cur, mids),
                reason=17, actor_id=caller.player_id, now_ms=now, kept=tuple(mids),
            )

        return await self._run_tx(work)

    async def _insert_ticket_tx(self, cur, tk: Message) -> None:
        await cur.execute(
            f"REPLACE INTO `match_authority_ticket`({_TICKET_COLS}) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),%s,%s,%s,%s,%s,UNHEX(%s))",
            (
                int(tk.player_id), tk.ticket, tk.request_id, int(tk.mode), int(tk.config_id), int(tk.team_size),
                int(tk.state), int(tk.enqueued_ms), int(tk.not_before_ms), int(tk.zone_id), int(tk.rating_centi),
                int(tk.formation_id), int(tk.team_id), _hex(bytes(tk.network_profile)), int(tk.expires_ms),
                int(tk.selected_region), tk.owner_account, int(tk.owner_revision), int(tk.pool_region),
                _hex(bytes(tk.ranked_context)),
            ),
        )

    async def _insert_formation_tx(self, cur, f: Message) -> None:
        await cur.execute(
            f"INSERT INTO `match_formation`({_FORMATION_COLS}) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),UNHEX(%s),%s,%s,%s,UNHEX(%s),%s,UNHEX(%s),%s,%s,%s,%s,%s,%s,UNHEX(%s),%s)",
            (
                int(f.battle_id), f.operation_id, int(f.state), int(f.physical_region), int(f.target_node_id),
                f.target_instance_id, f.target_endpoint, int(f.mode), int(f.config_id),
                _hex(self._encode_members_blob(f.members)), _hex(bytes(f.create_request)),
                int(f.created_ms), int(f.deadline_ms), int(f.retry_at_ms), _hex(bytes(f.activity_context)),
                int(bool(f.requeue_on_failure)), _hex(self._encode_prepared_blob(f.prepared)),
                int(f.outcome_protocol_version), int(f.preflight_supported_version), f.preflight_instance_id,
                int(f.adopted_outcome_protocol_version), int(bool(f.adopted_outcome_observed)),
                f.release_track, _hex(bytes(f.ranked_context)), int(f.ranked_season_id),
            ),
        )

    async def start_team_match(
        self,
        caller: CallerIdentity,
        *,
        expected_team_id: int,
        battle_config_id: int,
        mode: int,
        target_team_size: int,
        request_id: str,
        member_zones: dict[int, int],
        immediate_battle_node: tuple[int, str, str, int, str] | None,
        battle_deadline_ms: int,
        queue_ttl_ms: int,
    ) -> tuple[TeamMutationOutcome, Message | None, Message]:
        req_uuid = request_id if is_valid_uuid(request_id) else str(uuid.uuid4())
        h, l = uuid_to_pair(req_uuid)
        battle_id_alloc = await self.next_id()

        async def work(cur):
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            my_tid, _ = await self._get_membership_tx(cur, caller.player_id, for_update=True)
            if my_tid == 0 or (expected_team_id and my_tid != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            loaded = await self._load_team_for_update(cur, my_tid)
            if loaded is None or loaded[2] != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            version, data, _ = loaded
            rec = data.record
            now = await self._db_now(cur)
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_NOT_LEADER)
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            mids = [int(m.player_id) for m in sorted(rec.members, key=lambda x: (int(x.join_seq), int(x.player_id)))]
            if target_team_size < len(mids) or target_team_size > MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_SIZE_EXCEEDED)

            lock_token = str(uuid.uuid4())
            is_immediate = mode == MODE_PVE_TEAM and target_team_size == len(mids)
            launch = self.protocol.message(
                "teampb.TeamLaunchRecord",
                actor_id=caller.player_id, request_high=h, request_low=l,
                team_id=my_tid, team_version=version + 1, lock_token=lock_token,
                config_id=battle_config_id, target_size=target_team_size, member_count=len(mids),
                state=TEAM_LAUNCH_FORMING if is_immediate else TEAM_LAUNCH_QUEUED,
                enqueued_ms=now, mode=mode, owner_account=caller.account, owner_revision=caller.owner_revision,
            )
            formation = None
            if is_immediate:
                b_node_id, b_uuid, b_endpoint, b_region, b_track = immediate_battle_node
                op_id = str(uuid.uuid4())
                launch.battle_id = battle_id_alloc
                rec.match_lock_token = lock_token
                rec.match_lock_expire_at_ms = 0
                del rec.match_lock_roster[:]
                rec.match_lock_roster.extend(mids)
                formation = self.protocol.message(
                    "match.MatchFormationRecord",
                    battle_id=battle_id_alloc, operation_id=op_id, state=FORMATION_STATE_PREPARING,
                    physical_region=b_region, target_node_id=b_node_id, target_instance_id=b_uuid,
                    target_endpoint=b_endpoint, mode=mode, config_id=battle_config_id,
                    created_ms=now, deadline_ms=now + battle_deadline_ms, retry_at_ms=now + 60_000,
                    requeue_on_failure=False, release_track=b_track,
                )
                for pid in mids:
                    tk_id = f"tk-{battle_id_alloc}-{pid}"
                    formation.members.add(player_id=pid, ticket=tk_id, team_index=0, captain=(pid == caller.player_id))
                    tk = self.protocol.message(
                        "match.MatchAuthorityTicketRecord",
                        player_id=pid, ticket=tk_id, request_id=req_uuid, mode=mode, config_id=battle_config_id,
                        team_size=target_team_size, state=TICKET_STATE_FORMING, enqueued_ms=now,
                        zone_id=member_zones.get(pid, int(rec.zone_id)), formation_id=battle_id_alloc,
                        team_id=my_tid, expires_ms=now + queue_ttl_ms, selected_region=b_region,
                        owner_account=caller.account if pid == caller.player_id else f"acct:{pid}",
                        owner_revision=caller.owner_revision if pid == caller.player_id else 1,
                    )
                    await self._insert_ticket_tx(cur, tk)
                await self._insert_formation_tx(cur, formation)
            else:
                for pid in mids:
                    tk = self.protocol.message(
                        "match.MatchAuthorityTicketRecord",
                        player_id=pid, ticket=f"tk-{req_uuid}-{pid}", request_id=req_uuid, mode=mode,
                        config_id=battle_config_id, team_size=target_team_size, state=TICKET_STATE_QUEUED,
                        enqueued_ms=now, zone_id=member_zones.get(pid, int(rec.zone_id)), team_id=my_tid,
                        expires_ms=now + queue_ttl_ms,
                        owner_account=caller.account if pid == caller.player_id else f"acct:{pid}",
                        owner_revision=caller.owner_revision if pid == caller.player_id else 1,
                    )
                    await self._insert_ticket_tx(cur, tk)

            await cur.execute(
                "INSERT INTO `team_launch`(`actor_id`,`request_high`,`request_low`,`team_id`,`team_version`,"
                "`lock_token`,`config_id`,`target_size`,`member_count`,`state`,`enqueued_ms`,`not_before_ms`,"
                "`battle_id`,`payload`,`owner_account`,`owner_revision`,`mode`,`pool_region`) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,UNHEX(%s),%s,%s,%s,0)",
                (
                    caller.player_id, h, l, my_tid, version + 1, lock_token, battle_config_id,
                    target_team_size, len(mids), int(launch.state), now, int(launch.battle_id), "",
                    caller.account, caller.owner_revision, mode,
                ),
            )
            for pid in mids:
                await cur.execute(
                    "INSERT INTO `team_launch_role`(`actor_id`,`request_high`,`request_low`,`player_id`,`state`) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (caller.player_id, h, l, pid, int(launch.state)),
                )
            version += 1
            await self._save_team_tx(cur, my_tid, version, data, TEAM_STATE_ACTIVE, now + TEAM_IDLE_TTL_MS)
            outcome = TeamMutationOutcome(
                team_id=my_tid, version=version, record=rec, epochs=await self._epochs_tx(cur, mids),
                reason=11 if is_immediate else 15, actor_id=caller.player_id, now_ms=now, kept=tuple(mids),
            )
            return outcome, formation, launch

        return await self._run_tx(work)

    # -- Network Profile ------------------------------------------------------------------------------

    async def save_network_plan(self, player_id: int, profile: Message, expires_ms: int) -> None:
        raw = profile.SerializeToString(deterministic=True)

        async def work(cur):
            await cur.execute(
                "REPLACE INTO `match_network_profile`(`player_id`,`payload`,`expires_ms`) VALUES (%s,UNHEX(%s),%s)",
                (player_id, _hex(raw), expires_ms),
            )

        await self._run_tx(work)

    async def commit_network_profile(
        self, player_id: int, plan_id: str, binding: str, receipts: list[Message], expires_ms: int
    ) -> None:
        async def work(cur):
            now = await self._db_now(cur)
            await cur.execute(
                "SELECT `payload` FROM `match_network_profile` WHERE `player_id`=%s FOR UPDATE",
                (player_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise OriginalRoleError("network probe plan missing")
            profile = self.protocol.parse("match.MatchNetworkProfile", _to_bytes(row[0]))
            if profile.plan_id != plan_id or profile.session_binding != binding:
                raise OriginalRoleError("network probe plan or binding mismatch")
            del profile.receipts[:]
            for r in receipts:
                profile.receipts.add().CopyFrom(r)
            raw = profile.SerializeToString(deterministic=True)
            await cur.execute(
                "UPDATE `match_network_profile` SET `payload`=UNHEX(%s),`expires_ms`=%s WHERE `player_id`=%s",
                (_hex(raw), expires_ms, player_id),
            )
            region = preferred_region(profile, now)
            await cur.execute(
                "UPDATE `match_authority_ticket` SET `network_profile`=UNHEX(%s),`pool_region`=%s "
                "WHERE `player_id`=%s AND `state`=%s",
                (_hex(raw), region, player_id, TICKET_STATE_QUEUED),
            )

        await self._run_tx(work)

    async def get_network_profile(self, player_id: int) -> tuple[Message | None, int]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT `payload`,`expires_ms` FROM `match_network_profile` WHERE `player_id`=%s",
                    (player_id,),
                )
                row = await cur.fetchone()
                if not row:
                    return None, 0
                return self.protocol.parse("match.MatchNetworkProfile", _to_bytes(row[0])), int(row[1])

    # -- Queue, Challenge, Formation ------------------------------------------------------------------

    async def join_queue(
        self,
        caller: CallerIdentity,
        *,
        mode: int,
        config_id: int,
        team_size: int,
        zone_id: int,
        request_id: str,
        request_payload: bytes,
        network_profile: bytes,
        pool_region: int,
        queue_ttl_ms: int,
    ) -> Message:
        req_uuid = request_id if is_valid_uuid(request_id) else str(uuid.uuid4())
        h, l = uuid_to_pair(req_uuid)
        seq_id = await self.next_id()

        async def work(cur) -> Message:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            now = await self._db_now(cur)
            await cur.execute(
                "SELECT `ticket_record`,`cancelled`,`owner_account`,`owner_revision` FROM `match_join_intent` "
                "WHERE `player_id`=%s AND `request_high`=%s AND `request_low`=%s FOR UPDATE",
                (caller.player_id, h, l),
            )
            ir = await cur.fetchone()
            if ir:
                if _to_str(ir[2]) != caller.account or int(ir[3]) != caller.owner_revision:
                    raise OriginalRoleError("join intent belongs to previous owner")
                if bool(ir[1]):
                    raise MatchBusinessError(ERR_MATCH_CANCEL_TOO_LATE)
                raw_tk = _to_bytes(ir[0])
                if raw_tk:
                    return self.protocol.parse("match.MatchAuthorityTicketRecord", raw_tk)

            await cur.execute(
                f"SELECT {_TICKET_COLS} FROM `match_authority_ticket` WHERE `player_id`=%s FOR UPDATE",
                (caller.player_id,),
            )
            tr = await cur.fetchone()
            if tr:
                existing = self._scan_ticket(tr)
                if int(existing.state) == TICKET_STATE_QUEUED and int(existing.expires_ms) > now:
                    raise MatchBusinessError(ERR_MATCH_ALREADY_QUEUED)
                if int(existing.state) in (TICKET_STATE_FORMING, TICKET_STATE_READY):
                    raise MatchBusinessError(ERR_MATCH_IN_BATTLE)

            tk = self.protocol.message(
                "match.MatchAuthorityTicketRecord",
                player_id=caller.player_id, ticket=f"q-{seq_id}-{caller.player_id}", request_id=req_uuid,
                mode=mode, config_id=config_id, team_size=team_size, state=TICKET_STATE_QUEUED,
                enqueued_ms=now, zone_id=zone_id, network_profile=network_profile, expires_ms=now + queue_ttl_ms,
                owner_account=caller.account, owner_revision=caller.owner_revision, pool_region=pool_region,
            )
            await self._insert_ticket_tx(cur, tk)
            await cur.execute(
                "INSERT INTO `match_join_intent`(`player_id`,`request_high`,`request_low`,`request_id`,"
                "`ticket_record`,`cancelled`,`owner_account`,`owner_revision`,`spectator_exit`,"
                "`exit_prepared`,`request_payload`,`state`) VALUES (%s,%s,%s,%s,UNHEX(%s),0,%s,%s,UNHEX(''),0,UNHEX(%s),2)",
                (
                    caller.player_id, h, l, req_uuid, _hex(tk.SerializeToString(deterministic=True)),
                    caller.account, caller.owner_revision, _hex(request_payload),
                ),
            )
            return tk

        return await self._run_tx(work)

    async def cancel_queue(self, caller: CallerIdentity, queue_ticket: str, request_id: str) -> None:
        async def work(cur):
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            if is_valid_uuid(request_id):
                h, l = uuid_to_pair(request_id)
                await cur.execute(
                    "INSERT INTO `match_join_intent`(`player_id`,`request_high`,`request_low`,`request_id`,"
                    "`ticket_record`,`cancelled`,`owner_account`,`owner_revision`,`spectator_exit`,"
                    "`exit_prepared`,`request_payload`,`state`) VALUES (%s,%s,%s,%s,UNHEX(''),1,%s,%s,UNHEX(''),0,UNHEX(''),3) "
                    "ON DUPLICATE KEY UPDATE `cancelled`=IF(`owner_account`=%s AND `owner_revision`=%s,1,`cancelled`),"
                    "`state`=IF(`owner_account`=%s AND `owner_revision`=%s,3,`state`)",
                    (
                        caller.player_id, h, l, request_id, caller.account, caller.owner_revision,
                        caller.account, caller.owner_revision, caller.account, caller.owner_revision,
                    ),
                )
            await cur.execute(
                f"SELECT {_TICKET_COLS} FROM `match_authority_ticket` WHERE `player_id`=%s FOR UPDATE",
                (caller.player_id,),
            )
            row = await cur.fetchone()
            if not row:
                return
            tk = self._scan_ticket(row)
            if int(tk.state) == TICKET_STATE_DONE:
                return
            if queue_ticket and tk.ticket != queue_ticket:
                raise MatchBusinessError(ERR_MATCH_TICKET_MISMATCH)
            if int(tk.state) in (TICKET_STATE_FORMING, TICKET_STATE_READY):
                raise MatchBusinessError(ERR_MATCH_CANCEL_TOO_LATE)
            if int(tk.team_id) != 0:
                await self._cancel_queued_team_launch_tx(cur, int(tk.team_id))
            await cur.execute("UPDATE `match_authority_ticket` SET `state`=%s WHERE `player_id`=%s", (TICKET_STATE_DONE, caller.player_id))

        await self._run_tx(work)

    async def get_queue_status(self, caller: CallerIdentity, request_id: str = "") -> QueueStatusInfo:
        await self.verify_original_owner(caller.player_id, caller.account, caller.owner_revision)
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                now = await self._db_now(cur)
                cancelled = False
                if is_valid_uuid(request_id):
                    h, l = uuid_to_pair(request_id)
                    await cur.execute(
                        "SELECT `cancelled`,`owner_account`,`owner_revision` FROM `match_join_intent` "
                        "WHERE `player_id`=%s AND `request_high`=%s AND `request_low`=%s",
                        (caller.player_id, h, l),
                    )
                    ir = await cur.fetchone()
                    if ir:
                        cancelled = bool(ir[0]) or _to_str(ir[1]) != caller.account or int(ir[2]) != caller.owner_revision
                await cur.execute(f"SELECT {_TICKET_COLS} FROM `match_authority_ticket` WHERE `player_id`=%s", (caller.player_id,))
                tr = await cur.fetchone()
                tk = self._scan_ticket(tr) if tr else None
                if tk is not None and int(tk.state) == TICKET_STATE_QUEUED and int(tk.expires_ms) <= now:
                    tk = None
                return QueueStatusInfo(ticket=tk, request_cancelled=cancelled, now_ms=now)

    async def list_queued_tickets(self, *, limit: int = 200) -> list[Message]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                now = await self._db_now(cur)
                await cur.execute(
                    f"SELECT {_TICKET_COLS} FROM `match_authority_ticket` "
                    "WHERE `state`=%s AND `team_id`=0 AND `expires_ms`>%s ORDER BY `enqueued_ms`,`player_id` LIMIT %s",
                    (TICKET_STATE_QUEUED, now, limit),
                )
                return [self._scan_ticket(r) for r in await cur.fetchall()]

    async def list_queued_team_launches(self, *, limit: int = 100) -> list[Message]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT `actor_id`,`request_high`,`request_low`,`team_id`,`team_version`,`lock_token`,"
                    "`config_id`,`target_size`,`member_count`,`state`,`enqueued_ms`,`battle_id`,"
                    "`owner_account`,`owner_revision`,`mode` FROM `team_launch` "
                    "WHERE `state`=%s ORDER BY `enqueued_ms`,`actor_id` LIMIT %s",
                    (TEAM_LAUNCH_QUEUED, limit),
                )
                out = []
                for r in await cur.fetchall():
                    out.append(
                        self.protocol.message(
                            "teampb.TeamLaunchRecord",
                            actor_id=int(r[0]), request_high=int(r[1]), request_low=int(r[2]), team_id=int(r[3]),
                            team_version=int(r[4]), lock_token=_to_str(r[5]), config_id=int(r[6]),
                            target_size=int(r[7]), member_count=int(r[8]), state=int(r[9]),
                            enqueued_ms=int(r[10]), battle_id=int(r[11]), owner_account=_to_str(r[12]),
                            owner_revision=int(r[13]), mode=int(r[14]),
                        )
                    )
                return out

    async def create_challenge(
        self, challenger: CallerIdentity, target_player_id: int, config_id: int, ttl_ms: int
    ) -> Message:
        if target_player_id == 0 or target_player_id == challenger.player_id:
            raise MatchBusinessError(ERR_MATCH_CHALLENGE_SELF)
        cid = await self.next_id()

        async def work(cur) -> Message:
            await self._check_fence_tx(cur, challenger.player_id, challenger.account, challenger.owner_revision)
            now = await self._db_now(cur)
            await cur.execute(
                "SELECT 1 FROM `match_challenge_invitation` WHERE `target_player_id`=%s AND `state`=%s AND `expires_ms`>%s LIMIT 1",
                (target_player_id, CHALLENGE_STATE_PENDING, now),
            )
            if await cur.fetchone():
                raise MatchBusinessError(ERR_MATCH_CHALLENGE_PENDING)
            inv = self.protocol.message(
                "match.MatchChallengeInvitationRecord",
                challenge_id=cid, challenger_player_id=challenger.player_id, target_player_id=target_player_id,
                config_id=config_id, challenger_account=challenger.account, challenger_revision=challenger.owner_revision,
                created_ms=now, expires_ms=now + ttl_ms, state=CHALLENGE_STATE_PENDING,
            )
            await cur.execute(
                "INSERT INTO `match_challenge_invitation`(`challenge_id`,`challenger_player_id`,`target_player_id`,"
                "`config_id`,`challenger_account`,`challenger_revision`,`target_account`,`target_revision`,"
                "`created_ms`,`expires_ms`,`state`,`battle_id`) VALUES (%s,%s,%s,%s,%s,%s,'',0,%s,%s,%s,0)",
                (cid, challenger.player_id, target_player_id, config_id, challenger.account, challenger.owner_revision, now, now + ttl_ms, CHALLENGE_STATE_PENDING),
            )
            return inv

        return await self._run_tx(work)

    async def respond_challenge(
        self,
        responder: CallerIdentity,
        challenge_id: int,
        accept: bool,
        *,
        battle_node: tuple[int, str, str, int, str] | None,
        battle_deadline_ms: int,
    ) -> tuple[Message, Message | None]:
        battle_id_alloc = await self.next_id()

        async def work(cur):
            await self._check_fence_tx(cur, responder.player_id, responder.account, responder.owner_revision)
            now = await self._db_now(cur)
            await cur.execute(
                "SELECT `challenge_id`,`challenger_player_id`,`target_player_id`,`config_id`,"
                "`challenger_account`,`challenger_revision`,`created_ms`,`expires_ms`,`state`,`battle_id` "
                "FROM `match_challenge_invitation` WHERE `challenge_id`=%s FOR UPDATE",
                (challenge_id,),
            )
            row = await cur.fetchone()
            if not row or int(row[8]) != CHALLENGE_STATE_PENDING or int(row[7]) <= now:
                raise MatchBusinessError(ERR_MATCH_CHALLENGE_EXPIRED)
            if int(row[2]) != responder.player_id:
                raise MatchBusinessError(ERR_MATCH_CHALLENGE_NOT_TARGET)
            inv = self.protocol.message(
                "match.MatchChallengeInvitationRecord",
                challenge_id=int(row[0]), challenger_player_id=int(row[1]), target_player_id=int(row[2]),
                config_id=int(row[3]), challenger_account=_to_str(row[4]), challenger_revision=int(row[5]),
                created_ms=int(row[6]), expires_ms=int(row[7]), state=int(row[8]), battle_id=int(row[9]),
            )
            if not accept:
                await cur.execute("UPDATE `match_challenge_invitation` SET `state`=%s WHERE `challenge_id`=%s", (CHALLENGE_STATE_DECLINED, challenge_id))
                inv.state = CHALLENGE_STATE_DECLINED
                return inv, None
            if battle_node is None:
                raise MatchBusinessError(ERR_MATCH_MODE_NOT_OPEN)
            b_node_id, b_uuid, b_endpoint, b_region, b_track = battle_node
            op_id = str(uuid.uuid4())
            formation = self.protocol.message(
                "match.MatchFormationRecord",
                battle_id=battle_id_alloc, operation_id=op_id, state=FORMATION_STATE_PREPARING,
                physical_region=b_region, target_node_id=b_node_id, target_instance_id=b_uuid,
                target_endpoint=b_endpoint, mode=MODE_PVP_CHALLENGE, config_id=int(inv.config_id),
                created_ms=now, deadline_ms=now + battle_deadline_ms, retry_at_ms=now + 60_000,
                requeue_on_failure=False, release_track=b_track,
            )
            for pid, side, acct, rev in (
                (int(inv.challenger_player_id), 0, inv.challenger_account, int(inv.challenger_revision)),
                (responder.player_id, 1, responder.account, responder.owner_revision),
            ):
                tk_id = f"ch-{battle_id_alloc}-{pid}"
                formation.members.add(player_id=pid, ticket=tk_id, team_index=side)
                tk = self.protocol.message(
                    "match.MatchAuthorityTicketRecord",
                    player_id=pid, ticket=tk_id, request_id=op_id, mode=MODE_PVP_CHALLENGE,
                    config_id=int(inv.config_id), team_size=1, state=TICKET_STATE_FORMING,
                    enqueued_ms=now, formation_id=battle_id_alloc, expires_ms=now + battle_deadline_ms,
                    selected_region=b_region, owner_account=acct, owner_revision=rev,
                )
                await self._insert_ticket_tx(cur, tk)
            await self._insert_formation_tx(cur, formation)
            await cur.execute(
                "UPDATE `match_challenge_invitation` SET `state`=%s,`battle_id`=%s WHERE `challenge_id`=%s",
                (CHALLENGE_STATE_FORMED, battle_id_alloc, challenge_id),
            )
            inv.state = CHALLENGE_STATE_FORMED
            inv.battle_id = battle_id_alloc
            return inv, formation

        return await self._run_tx(work)

    async def claim_formation(
        self,
        *,
        mode: int,
        config_id: int,
        members: list[tuple[int, str, int, bool]],
        physical_region: int,
        target_node_id: int,
        target_instance_id: str,
        target_endpoint: str,
        release_track: str,
        battle_deadline_ms: int,
        requeue_on_failure: bool,
        activity_context: bytes = b"",
        team_launch_keys: list[tuple[int, int, int]] | None = None,
    ) -> Message:
        battle_id = await self.next_id()

        async def work(cur) -> Message:
            now = await self._db_now(cur)
            for pid, tk_id, _, _ in sorted(members, key=lambda x: x[0]):
                await cur.execute(
                    "SELECT `ticket`,`state` FROM `match_authority_ticket` WHERE `player_id`=%s FOR UPDATE",
                    (pid,),
                )
                row = await cur.fetchone()
                if not row or _to_str(row[0]) != tk_id or int(row[1]) != TICKET_STATE_QUEUED:
                    raise StoreUnavailableError(f"ticket not claimable player={pid}")
            op_id = str(uuid.uuid4())
            formation = self.protocol.message(
                "match.MatchFormationRecord",
                battle_id=battle_id, operation_id=op_id, state=FORMATION_STATE_PREPARING,
                physical_region=physical_region, target_node_id=target_node_id, target_instance_id=target_instance_id,
                target_endpoint=target_endpoint, mode=mode, config_id=config_id,
                created_ms=now, deadline_ms=now + battle_deadline_ms, retry_at_ms=now + 60_000,
                activity_context=activity_context, requeue_on_failure=requeue_on_failure, release_track=release_track,
            )
            for pid, tk_id, side, captain in members:
                formation.members.add(player_id=pid, ticket=tk_id, team_index=side, captain=captain)
                await cur.execute(
                    "UPDATE `match_authority_ticket` SET `state`=%s,`formation_id`=%s,`selected_region`=%s "
                    "WHERE `player_id`=%s AND `ticket`=%s",
                    (TICKET_STATE_FORMING, battle_id, physical_region, pid, tk_id),
                )
            for a, h, l in team_launch_keys or ():
                await cur.execute(
                    "UPDATE `team_launch` SET `state`=%s,`battle_id`=%s WHERE `actor_id`=%s AND `request_high`=%s AND `request_low`=%s",
                    (TEAM_LAUNCH_FORMING, battle_id, a, h, l),
                )
                await cur.execute(
                    "UPDATE `team_launch_role` SET `state`=%s WHERE `actor_id`=%s AND `request_high`=%s AND `request_low`=%s",
                    (TEAM_LAUNCH_FORMING, a, h, l),
                )
            await self._insert_formation_tx(cur, formation)
            return formation

        return await self._run_tx(work)

    async def mark_formation_prepared(self, battle_id: int, player_id: int, endpoint: str) -> None:
        async def work(cur):
            await cur.execute(f"SELECT {_FORMATION_COLS} FROM `match_formation` WHERE `battle_id`=%s FOR UPDATE", (battle_id,))
            row = await cur.fetchone()
            if not row:
                return
            f = self._scan_formation(row)
            found = next((p for p in f.prepared if int(p.player_id) == player_id), None)
            if found is not None:
                found.endpoint = endpoint
            else:
                f.prepared.add(player_id=player_id, endpoint=endpoint)
            await cur.execute(
                "UPDATE `match_formation` SET `prepared`=UNHEX(%s) WHERE `battle_id`=%s",
                (_hex(self._encode_prepared_blob(f.prepared)), battle_id),
            )

        await self._run_tx(work)

    async def save_formation_intent(
        self, battle_id: int, create_request_bytes: bytes, preflight_instance_id: str
    ) -> Message:
        async def work(cur) -> Message:
            await cur.execute(f"SELECT {_FORMATION_COLS} FROM `match_formation` WHERE `battle_id`=%s FOR UPDATE", (battle_id,))
            row = await cur.fetchone()
            if not row:
                raise StoreUnavailableError(f"formation missing battle={battle_id}")
            f = self._scan_formation(row)
            if int(f.state) != FORMATION_STATE_PREPARING:
                raise StoreUnavailableError(f"formation not preparing battle={battle_id}")
            f.create_request = create_request_bytes
            f.outcome_protocol_version = 1
            f.preflight_supported_version = 1
            f.preflight_instance_id = preflight_instance_id
            try:
                completable_original(self.protocol, f)
            except ValueError as exc:
                raise UnterminableOriginalError(str(exc)) from exc
            f.state = FORMATION_STATE_PERSISTED
            await cur.execute(
                "UPDATE `match_formation` SET `state`=%s,`create_request`=UNHEX(%s),`outcome_protocol_version`=1,"
                "`preflight_supported_version`=1,`preflight_instance_id`=%s WHERE `battle_id`=%s",
                (FORMATION_STATE_PERSISTED, _hex(create_request_bytes), preflight_instance_id, battle_id),
            )
            return f

        return await self._run_tx(work)

    async def mark_formation_created(self, battle_id: int, adopted_version: int) -> Message:
        async def work(cur) -> Message:
            await cur.execute(f"SELECT {_FORMATION_COLS} FROM `match_formation` WHERE `battle_id`=%s FOR UPDATE", (battle_id,))
            row = await cur.fetchone()
            if not row:
                raise StoreUnavailableError(f"formation missing battle={battle_id}")
            f = self._scan_formation(row)
            if int(f.state) != FORMATION_STATE_ABORTED:
                f.state = FORMATION_STATE_CREATED
                f.adopted_outcome_protocol_version = adopted_version
                f.adopted_outcome_observed = True
                await cur.execute(
                    "UPDATE `match_formation` SET `state`=%s,`adopted_outcome_protocol_version`=%s,"
                    "`adopted_outcome_observed`=1 WHERE `battle_id`=%s",
                    (FORMATION_STATE_CREATED, adopted_version, battle_id),
                )
                await cur.execute(
                    "UPDATE `match_authority_ticket` SET `state`=%s WHERE `formation_id`=%s AND `state`=%s",
                    (TICKET_STATE_READY, battle_id, TICKET_STATE_FORMING),
                )
                await cur.execute(
                    "UPDATE `team_launch` SET `state`=%s WHERE `battle_id`=%s AND `state`<>%s",
                    (TEAM_LAUNCH_CREATED, battle_id, TEAM_LAUNCH_CLOSED),
                )
            return f

        return await self._run_tx(work)

    async def mark_formation_unknown(self, battle_id: int) -> None:
        async def work(cur):
            await cur.execute(
                "UPDATE `match_formation` SET `state`=%s WHERE `battle_id`=%s AND `state` IN (%s,%s)",
                (FORMATION_STATE_UNKNOWN, battle_id, FORMATION_STATE_PREPARING, FORMATION_STATE_PERSISTED),
            )

        await self._run_tx(work)

    async def abort_formation_pre_persist(self, battle_id: int, culprit_player_id: int = 0) -> None:
        async def work(cur):
            await cur.execute(f"SELECT {_FORMATION_COLS} FROM `match_formation` WHERE `battle_id`=%s FOR UPDATE", (battle_id,))
            row = await cur.fetchone()
            if not row:
                return
            f = self._scan_formation(row)
            await cur.execute("UPDATE `match_formation` SET `state`=%s WHERE `battle_id`=%s", (FORMATION_STATE_ABORTED, battle_id))
            for m in f.members:
                pid = int(m.player_id)
                if f.requeue_on_failure and pid != culprit_player_id:
                    await cur.execute(
                        "UPDATE `match_authority_ticket` SET `state`=%s,`formation_id`=0 WHERE `player_id`=%s AND `formation_id`=%s",
                        (TICKET_STATE_QUEUED, pid, battle_id),
                    )
                else:
                    await cur.execute(
                        "UPDATE `match_authority_ticket` SET `state`=%s WHERE `player_id`=%s AND `formation_id`=%s",
                        (TICKET_STATE_DONE, pid, battle_id),
                    )
            await self._close_team_launch_by_battle_tx(cur, battle_id)

        await self._run_tx(work)

    async def _close_team_launch_by_battle_tx(self, cur, battle_id: int) -> None:
        await cur.execute(
            "SELECT `actor_id`,`request_high`,`request_low`,`team_id`,`lock_token` FROM `team_launch` WHERE `battle_id`=%s FOR UPDATE",
            (battle_id,),
        )
        rows = await cur.fetchall()
        for a, h, l, tid, token in rows:
            await cur.execute(
                "UPDATE `team_launch` SET `state`=%s WHERE `actor_id`=%s AND `request_high`=%s AND `request_low`=%s",
                (TEAM_LAUNCH_CLOSED, int(a), int(h), int(l)),
            )
            await cur.execute(
                "UPDATE `team_launch_role` SET `state`=%s WHERE `actor_id`=%s AND `request_high`=%s AND `request_low`=%s",
                (TEAM_LAUNCH_CLOSED, int(a), int(h), int(l)),
            )
            loaded = await self._load_team_for_update(cur, int(tid))
            if loaded is not None and loaded[1].record.match_lock_token == _to_str(token):
                version, data, state = loaded
                data.record.match_lock_token = ""
                data.record.match_lock_expire_at_ms = 0
                del data.record.match_lock_roster[:]
                now = await self._db_now(cur)
                await self._save_team_tx(cur, int(tid), version + 1, data, state, now + TEAM_IDLE_TTL_MS)

    async def get_formation(self, battle_id: int) -> Message | None:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT {_FORMATION_COLS} FROM `match_formation` WHERE `battle_id`=%s", (battle_id,))
                row = await cur.fetchone()
                return self._scan_formation(row) if row else None

    async def list_active_formations(
        self, *, release_track: str = "stable", limit: int = 50
    ) -> list[tuple[Message, bool]]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                now = await self._db_now(cur)
                await cur.execute(
                    f"SELECT {_FORMATION_COLS} FROM `match_formation` "
                    "WHERE `state`=%s AND `deadline_ms`>%s AND `retry_at_ms`<>%s "
                    "ORDER BY `created_ms` DESC,`battle_id` DESC LIMIT %s",
                    (FORMATION_STATE_CREATED, now, TERMINAL_OUTCOME_NO_RETRY_MS, limit * 2),
                )
                items: list[tuple[Message, bool]] = []
                for row in await cur.fetchall():
                    f = self._scan_formation(row)
                    track = f.release_track or "stable"
                    if release_track == "stable" and track != "stable":
                        continue
                    items.append((f, int(f.battle_id) in self._featured))
                items.sort(key=lambda pair: (not pair[1], -int(pair[0].created_ms), int(pair[0].battle_id)))
                return items[:limit]

    async def set_formation_featured(self, battle_id: int, featured: bool) -> bool:
        f = await self.get_formation(battle_id)
        if f is None or int(f.state) != FORMATION_STATE_CREATED or formation_outcome_decided(f):
            return False
        if featured:
            self._featured.add(battle_id)
        else:
            self._featured.discard(battle_id)
        return True

    # -- Trial Occupancy & Activity Battles -----------------------------------------------------------

    async def acquire_trial_occupancy(
        self, owner_id: int, roster: list[tuple[int, str, int]], ttl_ms: int
    ) -> tuple[int, int]:
        async def work(cur) -> tuple[int, int]:
            now = await self._db_now(cur)
            for pid, acct, rev in sorted(roster, key=lambda x: x[0]):
                try:
                    await self._check_fence_tx(cur, pid, acct, rev)
                except OriginalRoleError:
                    return 2, pid
                await cur.execute(
                    "SELECT 1 FROM `match_authority_ticket` WHERE `player_id`=%s AND `state` IN (1,2,3) LIMIT 1",
                    (pid,),
                )
                if await cur.fetchone():
                    return 1, pid
                await cur.execute(
                    "SELECT `owner_kind`,`owner_id`,`expires_ms` FROM `match_role_occupancy` WHERE `player_id`=%s FOR UPDATE",
                    (pid,),
                )
                occ = await cur.fetchone()
                if occ and int(occ[2]) > now and (int(occ[0]) != OCCUPANCY_KIND_GUILD_TRIAL or int(occ[1]) != owner_id):
                    return 1, pid
            exp = now + ttl_ms
            for pid, acct, rev in roster:
                await cur.execute(
                    "INSERT INTO `match_role_occupancy`(`player_id`,`owner_kind`,`owner_id`,`account`,`owner_revision`,`expires_ms`) "
                    "VALUES (%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE `owner_kind`=VALUES(`owner_kind`),`owner_id`=VALUES(`owner_id`),"
                    "`account`=VALUES(`account`),`owner_revision`=VALUES(`owner_revision`),"
                    "`expires_ms`=GREATEST(`expires_ms`,VALUES(`expires_ms`))",
                    (pid, OCCUPANCY_KIND_GUILD_TRIAL, owner_id, acct or f"acct:{pid}", rev or 1, exp),
                )
            return 0, 0

        return await self._run_tx(work)

    async def release_trial_occupancy(self, owner_id: int, player_ids: list[int]) -> None:
        async def work(cur):
            for pid in player_ids:
                await cur.execute(
                    "DELETE FROM `match_role_occupancy` WHERE `player_id`=%s AND `owner_kind`=%s AND `owner_id`=%s",
                    (pid, OCCUPANCY_KIND_GUILD_TRIAL, owner_id),
                )

        await self._run_tx(work)

    async def start_activity_battle(
        self,
        *,
        request_id: int,
        battle_config_id: int,
        member_player_ids: list[int],
        activity_context_bytes: bytes,
        battle_node: tuple[int, str, str, int, str],
        battle_deadline_ms: int,
    ) -> tuple[Message, bool]:
        battle_id_alloc = await self.next_id()

        async def work(cur):
            now = await self._db_now(cur)
            b_node_id, b_uuid, b_endpoint, b_region, b_track = battle_node
            op_id = str(uuid.uuid4())
            formation = self.protocol.message(
                "match.MatchFormationRecord",
                battle_id=battle_id_alloc, operation_id=op_id, state=FORMATION_STATE_PREPARING,
                physical_region=b_region, target_node_id=b_node_id, target_instance_id=b_uuid,
                target_endpoint=b_endpoint, mode=MODE_PVE_TEAM, config_id=battle_config_id,
                created_ms=now, deadline_ms=now + battle_deadline_ms, retry_at_ms=now + 60_000,
                activity_context=activity_context_bytes, requeue_on_failure=False, release_track=b_track,
            )
            for idx, pid in enumerate(member_player_ids):
                tk_id = f"act-{battle_id_alloc}-{pid}"
                formation.members.add(player_id=pid, ticket=tk_id, team_index=0, captain=(idx == 0))
                tk = self.protocol.message(
                    "match.MatchAuthorityTicketRecord",
                    player_id=pid, ticket=tk_id, request_id=op_id, mode=MODE_PVE_TEAM,
                    config_id=battle_config_id, team_size=len(member_player_ids), state=TICKET_STATE_FORMING,
                    enqueued_ms=now, formation_id=battle_id_alloc, expires_ms=now + battle_deadline_ms,
                    selected_region=b_region, owner_account=f"acct:{pid}", owner_revision=1,
                )
                await self._insert_ticket_tx(cur, tk)
            await self._insert_formation_tx(cur, formation)
            return formation, True

        return await self._run_tx(work)

    async def cancel_activity_battle(self, request_id: int) -> tuple[int, bool, int, int]:
        return 0, True, 0, OUTCOME_DECISION_UNKNOWN

    # -- Battle Outcome, Terminal Decision & Outbox ---------------------------------------------------

    async def commit_battle_outcome(self, req: Message) -> int:
        battle_id = int(req.battle_id)
        if not battle_id or not req.placement_operation_id or not req.target_instance_id:
            raise BattleOutcomeOriginalError("incomplete CommitBattleOutcomeRequest")
        raw_payload = bytes(req.raw_result_payload)
        try:
            event = validate_terminal_payload(self.protocol, raw_payload)
            orig_err = None
        except ValueError as exc:
            event = None
            orig_err = exc
        digest = hashlib.sha256(raw_payload).digest()

        async def work(cur) -> int:
            await cur.execute(
                "SELECT `decision`,`raw_payload`,`payload_digest`,`placement_operation_id`,`target_instance_id` "
                "FROM `match_battle_completion` WHERE `battle_id`=%s FOR UPDATE",
                (battle_id,),
            )
            old = await cur.fetchone()
            if old and int(old[0]) != OUTCOME_DECISION_UNKNOWN:
                dec = int(old[0])
                if dec == OUTCOME_DECISION_ABORT:
                    return OUTCOME_DECISION_ABORT
                if (
                    dec == OUTCOME_DECISION_COMPLETE
                    and _to_str(old[3]) == req.placement_operation_id
                    and _to_str(old[4]) == req.target_instance_id
                    and _to_bytes(old[1]) == raw_payload
                    and _to_bytes(old[2]) == digest
                ):
                    return OUTCOME_DECISION_COMPLETE
                raise BattleOutcomeOriginalError("replayed outcome differs from committed COMPLETE")

            if orig_err is not None or event is None:
                raise BattleOutcomeOriginalError(str(orig_err))

            await cur.execute(f"SELECT {_FORMATION_COLS} FROM `match_formation` WHERE `battle_id`=%s FOR UPDATE", (battle_id,))
            frow = await cur.fetchone()
            if not frow:
                raise BattleOutcomeOriginalError("formation not found")
            f = self._scan_formation(frow)
            if (
                f.operation_id != req.placement_operation_id
                or f.target_instance_id != req.target_instance_id
                or int(f.state) == FORMATION_STATE_ABORTED
            ):
                raise BattleOutcomeOriginalError("formation identity or state mismatch")
            try:
                create_req = validate_terminal_against_formation(self.protocol, f, event)
            except ValueError as exc:
                raise BattleOutcomeOriginalError(str(exc)) from exc

            owners: dict[int, tuple[str, int, str]] = {}
            for m in sorted(f.members, key=lambda x: int(x.player_id)):
                pid = int(m.player_id)
                await cur.execute(f"SELECT {_TICKET_COLS} FROM `match_authority_ticket` WHERE `player_id`=%s FOR UPDATE", (pid,))
                trow = await cur.fetchone()
                if not trow:
                    raise BattleOutcomeOriginalError(f"ticket missing player={pid}")
                tk = self._scan_ticket(trow)
                if (
                    tk.ticket != m.ticket
                    or int(tk.formation_id) != battle_id
                    or int(tk.state) not in (TICKET_STATE_FORMING, TICKET_STATE_READY)
                    or not tk.owner_account
                    or int(tk.owner_revision) == 0
                ):
                    raise BattleOutcomeOriginalError(f"ticket invalid player={pid}")
                await self._check_fence_tx(cur, pid, tk.owner_account, int(tk.owner_revision))
                owners[pid] = (tk.owner_account, int(tk.owner_revision), tk.ticket)

            now = await self._db_now(cur)
            await cur.execute(
                "INSERT INTO `match_battle_completion`(`battle_id`,`raw_payload`,`payload_digest`,`format_version`,"
                "`archived_ms`,`source_topic`,`source_partition`,`source_offset`,`decision`,`source_kind`,"
                "`placement_operation_id`,`target_instance_id`,`outcome_protocol_version`,`result_published`,`next_retry_ms`) "
                "VALUES (%s,UNHEX(%s),UNHEX(%s),1,%s,'',0,0,%s,%s,%s,%s,1,0,%s)",
                (
                    battle_id, _hex(raw_payload), _hex(digest), now, OUTCOME_DECISION_COMPLETE,
                    COMPLETION_SOURCE_BATTLE_RPC, f.operation_id, f.target_instance_id, now,
                ),
            )
            for settlement in event.settlements:
                pid = int(settlement.player_id)
                body = settlement.SerializeToString(deterministic=True)
                p_digest = hashlib.sha256(body).digest()
                acct, rev, _ = owners[pid]
                await cur.execute(
                    "INSERT INTO `match_battle_settlement_source`(`battle_id`,`player_id`,`settlement_proto`,"
                    "`payload_digest`,`archived_ms`,`owner_account`,`owner_revision`,`issued_at_ms`,"
                    "`inbox_seq`,`inbox_state`,`dispatch_state`,`next_retry_ms`) "
                    "VALUES (%s,%s,UNHEX(%s),UNHEX(%s),%s,%s,%s,%s,0,0,%s,%s)",
                    (battle_id, pid, _hex(body), _hex(p_digest), now, acct, rev, now, SETTLEMENT_DISPATCH_PENDING, now),
                )

            has_replay = req.HasField("replay") and bool(req.replay.payload)
            member_ids = [int(p.player_id) for p in create_req.players]
            if has_replay:
                env_bytes = req.replay.SerializeToString(deterministic=True)
                await cur.execute(
                    "REPLACE INTO `match_battle_report`(`battle_id`,`created_ms`,`match_mode`,`battle_config_id`,"
                    "`envelope`,`envelope_digest`,`ref_count`) VALUES (%s,%s,%s,%s,UNHEX(%s),UNHEX(%s),%s)",
                    (battle_id, now, int(f.mode), int(f.config_id), _hex(env_bytes), _hex(hashlib.sha256(env_bytes).digest()), len(member_ids)),
                )
            summary_bytes = build_battle_report_summary(
                self.protocol, create_req, event, has_replay=has_replay
            ).SerializeToString(deterministic=True)
            for pid in member_ids:
                await cur.execute(
                    "REPLACE INTO `match_player_battle_report`(`player_id`,`created_ms`,`battle_id`,`summary`) "
                    "VALUES (%s,%s,%s,UNHEX(%s))",
                    (pid, now, battle_id, _hex(summary_bytes)),
                )

            await cur.execute(
                "UPDATE `match_formation` SET `state`=%s,`retry_at_ms`=%s WHERE `battle_id`=%s",
                (FORMATION_STATE_CREATED, TERMINAL_OUTCOME_NO_RETRY_MS, battle_id),
            )
            await cur.execute(
                "UPDATE `match_authority_ticket` SET `state`=%s WHERE `formation_id`=%s AND `state`=%s",
                (TICKET_STATE_READY, battle_id, TICKET_STATE_FORMING),
            )
            return OUTCOME_DECISION_COMPLETE

        return await self._run_tx(work)

    async def commit_battle_abort(
        self, battle_id: int, source_kind: int, *, manual_audit: dict | None = None
    ) -> int:
        async def work(cur) -> int:
            await cur.execute("SELECT `decision` FROM `match_battle_completion` WHERE `battle_id`=%s FOR UPDATE", (battle_id,))
            old = await cur.fetchone()
            if old and int(old[0]) != OUTCOME_DECISION_UNKNOWN:
                return int(old[0])
            await cur.execute(f"SELECT {_FORMATION_COLS} FROM `match_formation` WHERE `battle_id`=%s FOR UPDATE", (battle_id,))
            frow = await cur.fetchone()
            if not frow:
                raise StoreUnavailableError(f"formation missing battle={battle_id}")
            f = self._scan_formation(frow)
            now = await self._db_now(cur)
            await cur.execute(
                "INSERT INTO `match_battle_completion`(`battle_id`,`raw_payload`,`payload_digest`,`format_version`,"
                "`archived_ms`,`source_topic`,`source_partition`,`source_offset`,`decision`,`source_kind`,"
                "`placement_operation_id`,`target_instance_id`,`outcome_protocol_version`,`result_published`,`next_retry_ms`) "
                "VALUES (%s,UNHEX(''),UNHEX(''),0,%s,'',0,0,%s,%s,%s,%s,%s,1,0)",
                (battle_id, now, OUTCOME_DECISION_ABORT, source_kind, f.operation_id, f.target_instance_id, int(f.outcome_protocol_version)),
            )
            await cur.execute(
                "UPDATE `match_formation` SET `state`=%s,`retry_at_ms`=%s WHERE `battle_id`=%s",
                (FORMATION_STATE_ABORTED, TERMINAL_OUTCOME_NO_RETRY_MS, battle_id),
            )
            await cur.execute(
                "UPDATE `match_authority_ticket` SET `state`=%s WHERE `formation_id`=%s",
                (TICKET_STATE_DONE, battle_id),
            )
            await self._close_team_launch_by_battle_tx(cur, battle_id)
            if manual_audit is not None:
                await cur.execute(
                    "INSERT INTO `match_manual_resolution`(`battle_id`,`action`,`formation_class`,`failed_check`,"
                    "`operator_name`,`reason`,`resolved_ms`,`formation_digest_before`,`formation_before`,"
                    "`tickets_before`,`effective_deadline_ms`,`replica_instance`) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(%s),UNHEX(%s),%s,%s)",
                    (
                        battle_id, manual_audit.get("action", 3), manual_audit.get("formation_class", 0),
                        manual_audit.get("failed_check", ""), manual_audit.get("operator_name", ""),
                        manual_audit.get("reason", ""), now, manual_audit.get("formation_digest_before", ""),
                        _hex(manual_audit.get("formation_before", b"")), _hex(manual_audit.get("tickets_before", b"")),
                        manual_audit.get("effective_deadline_ms", 0), manual_audit.get("replica_instance", ""),
                    ),
                )
            return OUTCOME_DECISION_ABORT

        return await self._run_tx(work)

    async def get_battle_completion(self, battle_id: int) -> tuple[int, bytes]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT `decision`,`raw_payload` FROM `match_battle_completion` WHERE `battle_id`=%s",
                    (battle_id,),
                )
                row = await cur.fetchone()
                if not row or int(row[0]) == OUTCOME_DECISION_UNKNOWN:
                    return OUTCOME_DECISION_UNKNOWN, b""
                dec = int(row[0])
                return dec, _to_bytes(row[1]) if dec == OUTCOME_DECISION_COMPLETE else b""

    async def get_battle_terminal_state(self, battle_id: int) -> tuple[int, bool]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT `decision` FROM `match_battle_completion` WHERE `battle_id`=%s", (battle_id,))
                row = await cur.fetchone()
                if row and int(row[0]) != OUTCOME_DECISION_UNKNOWN:
                    return int(row[0]), False
                await cur.execute(f"SELECT {_FORMATION_COLS} FROM `match_formation` WHERE `battle_id`=%s", (battle_id,))
                frow = await cur.fetchone()
                if not frow:
                    return OUTCOME_DECISION_UNKNOWN, False
                f = self._scan_formation(frow)
                if int(f.state) not in (FORMATION_STATE_PERSISTED, FORMATION_STATE_UNKNOWN, FORMATION_STATE_CREATED):
                    return OUTCOME_DECISION_UNKNOWN, False
                try:
                    completable_original(self.protocol, f)
                    return OUTCOME_DECISION_UNKNOWN, True
                except ValueError:
                    return OUTCOME_DECISION_UNKNOWN, False

    async def due_outcome_results(self, stamp_ms: int, limit: int) -> list[CompletionOutboxRow]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT `battle_id`,`raw_payload`,`payload_digest`,`next_retry_ms` FROM `match_battle_completion` "
                    "WHERE `decision`=1 AND `result_published`=0 AND `next_retry_ms`<=%s "
                    "ORDER BY `next_retry_ms`,`battle_id` LIMIT %s",
                    (stamp_ms, limit),
                )
                return [
                    CompletionOutboxRow(int(r[0]), _to_bytes(r[1]), _to_bytes(r[2]), int(r[3]))
                    for r in await cur.fetchall()
                ]

    async def claim_outcome_result(self, battle_id: int, prev_retry_ms: int, lease_ms: int) -> bool:
        async def work(cur) -> bool:
            await cur.execute(
                "UPDATE `match_battle_completion` SET `next_retry_ms`=%s "
                "WHERE `battle_id`=%s AND `decision`=1 AND `result_published`=0 AND `next_retry_ms`=%s",
                (lease_ms, battle_id, prev_retry_ms),
            )
            return cur.rowcount > 0

        return await self._run_tx(work)

    async def mark_outcome_result_published(self, battle_id: int, lease_ms: int) -> None:
        async def work(cur):
            await cur.execute(
                "UPDATE `match_battle_completion` SET `result_published`=1 "
                "WHERE `battle_id`=%s AND `decision`=1 AND `result_published`=0 AND `next_retry_ms`=%s",
                (battle_id, lease_ms),
            )

        await self._run_tx(work)

    async def due_settlement_sources(
        self, stamp_ms: int, first_limit: int, waiting_limit: int
    ) -> list[SettlementOutboxRow]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                out: list[SettlementOutboxRow] = []
                for cond, lim in (("=0", first_limit), (">0", waiting_limit)):
                    await cur.execute(
                        "SELECT `battle_id`,`player_id`,`settlement_proto`,`payload_digest`,`owner_account`,"
                        "`owner_revision`,`issued_at_ms`,`inbox_seq`,`inbox_state`,`dispatch_state`,`next_retry_ms` "
                        f"FROM `match_battle_settlement_source` WHERE `dispatch_state`=1 AND `inbox_seq`{cond} "
                        "AND `next_retry_ms`<=%s ORDER BY `next_retry_ms`,`battle_id`,`player_id` LIMIT %s",
                        (stamp_ms, lim),
                    )
                    for r in await cur.fetchall():
                        out.append(
                            SettlementOutboxRow(
                                int(r[0]), int(r[1]), _to_bytes(r[2]), _to_bytes(r[3]), _to_str(r[4]),
                                int(r[5]), int(r[6]), int(r[7]), int(r[8]), int(r[9]), int(r[10]),
                            )
                        )
                return out

    async def claim_settlement_source(
        self, battle_id: int, player_id: int, prev_retry_ms: int, lease_ms: int
    ) -> bool:
        async def work(cur) -> bool:
            await cur.execute(
                "UPDATE `match_battle_settlement_source` SET `next_retry_ms`=%s "
                "WHERE `battle_id`=%s AND `player_id`=%s AND `dispatch_state`=1 AND `next_retry_ms`=%s",
                (lease_ms, battle_id, player_id, prev_retry_ms),
            )
            return cur.rowcount > 0

        return await self._run_tx(work)

    async def update_settlement_source(
        self,
        battle_id: int,
        player_id: int,
        lease_ms: int,
        *,
        inbox_seq: int,
        inbox_state: int,
        dispatch_state: int,
        next_retry_ms: int,
    ) -> None:
        async def work(cur):
            await cur.execute(
                "UPDATE `match_battle_settlement_source` SET `inbox_seq`=%s,`inbox_state`=%s,"
                "`dispatch_state`=%s,`next_retry_ms`=%s WHERE `battle_id`=%s AND `player_id`=%s "
                "AND `dispatch_state`=1 AND `next_retry_ms`=%s AND (`inbox_seq`=0 OR `inbox_seq`=%s)",
                (inbox_seq, inbox_state, dispatch_state, next_retry_ms, battle_id, player_id, lease_ms, inbox_seq),
            )

        await self._run_tx(work)

    async def release_settled_tickets(self, battle_id: int, player_id: int) -> bool:
        async def work(cur) -> bool:
            await cur.execute(
                "SELECT `owner_account`,`owner_revision`,`dispatch_state` FROM `match_battle_settlement_source` "
                "WHERE `battle_id`=%s AND `player_id`=%s",
                (battle_id, player_id),
            )
            row = await cur.fetchone()
            if not row or int(row[2]) != SETTLEMENT_DISPATCH_APPLIED:
                return False
            await cur.execute(
                "UPDATE `match_authority_ticket` SET `state`=%s WHERE `player_id`=%s AND `formation_id`=%s "
                "AND `state`=%s AND `owner_account`=%s AND `owner_revision`=%s",
                (TICKET_STATE_DONE, player_id, battle_id, TICKET_STATE_READY, _to_str(row[0]), int(row[1])),
            )
            released = cur.rowcount > 0
            if released:
                await cur.execute(
                    "SELECT COUNT(*) FROM `match_authority_ticket` WHERE `formation_id`=%s AND `state`<>%s",
                    (battle_id, TICKET_STATE_DONE),
                )
                remaining = int((await cur.fetchone())[0])
                if remaining == 0:
                    await self._close_team_launch_by_battle_tx(cur, battle_id)
            return released

        return await self._run_tx(work)

    # -- Battle Reports -------------------------------------------------------------------------------

    async def list_battle_reports(self, player_id: int) -> list[Message]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT `summary` FROM `match_player_battle_report` WHERE `player_id`=%s "
                    "ORDER BY `created_ms` DESC,`battle_id` DESC LIMIT %s",
                    (player_id, MAX_BATTLE_REPORTS),
                )
                return [self.protocol.parse("match.BattleReportSummary", _to_bytes(r[0])) for r in await cur.fetchall()]

    async def get_battle_report(self, player_id: int, battle_id: int) -> Message:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM `match_player_battle_report` WHERE `player_id`=%s AND `battle_id`=%s LIMIT 1",
                    (player_id, battle_id),
                )
                if not await cur.fetchone():
                    raise MatchBusinessError(ERR_MATCH_BATTLE_REPORT_NOT_FOUND)
                await cur.execute("SELECT `envelope` FROM `match_battle_report` WHERE `battle_id`=%s", (battle_id,))
                row = await cur.fetchone()
                if not row or not _to_bytes(row[0]):
                    raise MatchBusinessError(ERR_MATCH_BATTLE_REPORT_NOT_FOUND)
                return self.protocol.parse("BattleReplayEnvelope", _to_bytes(row[0]))

    # -- Spectator ------------------------------------------------------------------------------------

    async def get_spectator(self, player_id: int) -> Message | None:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT `player_id`,`operation_id`,`battle_id`,`target_endpoint`,`target_instance_id`,"
                    "`state`,`request_payload`,`owner_account`,`owner_revision`,`add_request`,`updated_ms`,"
                    "`request_high`,`request_low` FROM `match_spectator` WHERE `player_id`=%s",
                    (player_id,),
                )
                r = await cur.fetchone()
                if not r:
                    return None
                return self.protocol.message(
                    "match.MatchSpectatorRecord",
                    player_id=int(r[0]), operation_id=_to_str(r[1]), battle_id=int(r[2]),
                    target_endpoint=_to_str(r[3]), target_instance_id=_to_str(r[4]), state=int(r[5]),
                    request_payload=_to_bytes(r[6]), owner_account=_to_str(r[7]), owner_revision=int(r[8]),
                    add_request=_to_bytes(r[9]), updated_ms=int(r[10]), request_high=int(r[11]), request_low=int(r[12]),
                )

    async def prepare_spectator(
        self,
        caller: CallerIdentity,
        battle_id: int,
        operation_id: str,
        target_endpoint: str,
        target_instance_id: str,
        request_payload: bytes,
        add_request: bytes,
        request_id: str,
    ) -> Message:
        h, l = uuid_to_pair(request_id) if is_valid_uuid(request_id) else (0, 0)

        async def work(cur) -> Message:
            await self._check_fence_tx(cur, caller.player_id, caller.account, caller.owner_revision)
            await cur.execute(
                "SELECT 1 FROM `match_authority_ticket` WHERE `player_id`=%s AND `state` IN (1,2,3) LIMIT 1",
                (caller.player_id,),
            )
            if await cur.fetchone():
                raise MatchBusinessError(ERR_MATCH_SPECTATE_WHILE_QUEUED)
            now = await self._db_now(cur)
            await cur.execute(
                "REPLACE INTO `match_spectator`(`player_id`,`operation_id`,`battle_id`,`target_endpoint`,"
                "`target_instance_id`,`state`,`request_payload`,`owner_account`,`owner_revision`,"
                "`add_request`,`updated_ms`,`request_high`,`request_low`) "
                "VALUES (%s,%s,%s,%s,%s,%s,UNHEX(%s),%s,%s,UNHEX(%s),%s,%s,%s)",
                (
                    caller.player_id, operation_id, battle_id, target_endpoint, target_instance_id,
                    SPECTATOR_STATE_PENDING_ADD, _hex(request_payload), caller.account, caller.owner_revision,
                    _hex(add_request), now, h, l,
                ),
            )
            return self.protocol.message(
                "match.MatchSpectatorRecord",
                player_id=caller.player_id, operation_id=operation_id, battle_id=battle_id,
                target_endpoint=target_endpoint, target_instance_id=target_instance_id,
                state=SPECTATOR_STATE_PENDING_ADD, request_payload=request_payload,
                owner_account=caller.account, owner_revision=caller.owner_revision,
                add_request=add_request, updated_ms=now, request_high=h, request_low=l,
            )

        return await self._run_tx(work)

    async def commit_spectator(self, player_id: int, operation_id: str) -> None:
        async def work(cur):
            now = await self._db_now(cur)
            await cur.execute(
                "UPDATE `match_spectator` SET `state`=%s,`updated_ms`=%s WHERE `player_id`=%s AND `operation_id`=%s",
                (SPECTATOR_STATE_ACTIVE, now, player_id, operation_id),
            )

        await self._run_tx(work)

    async def close_spectator(self, player_id: int, operation_id: str) -> None:
        async def work(cur):
            now = await self._db_now(cur)
            if operation_id:
                await cur.execute(
                    "UPDATE `match_spectator` SET `state`=%s,`updated_ms`=%s WHERE `player_id`=%s AND `operation_id`=%s",
                    (SPECTATOR_STATE_CLOSED, now, player_id, operation_id),
                )
            else:
                await cur.execute(
                    "UPDATE `match_spectator` SET `state`=%s,`updated_ms`=%s WHERE `player_id`=%s",
                    (SPECTATOR_STATE_CLOSED, now, player_id),
                )

        await self._run_tx(work)

    # -- Stranded Formations --------------------------------------------------------------------------

    async def _build_stranded_sql(self, cur, f: Message, now: int, *, include_tickets: bool) -> StrandedFormationEntry:
        f_class, failed_check, create_req = classify_stranded_formation(self.protocol, f)
        eff_dl = effective_deadline_ms(f, create_req)
        await cur.execute(
            "SELECT `decision`,`source_kind` FROM `match_battle_completion` WHERE `battle_id`=%s",
            (int(f.battle_id),),
        )
        crow = await cur.fetchone()
        has_term = crow is not None
        term_dec = int(crow[0]) if crow else OUTCOME_DECISION_UNKNOWN
        term_src = int(crow[1]) if crow else 0
        refusal = ""
        if has_term and term_dec != OUTCOME_DECISION_UNKNOWN:
            refusal = "already_resolved"
        elif f_class == 1:
            refusal = "auto_recoverable"
        elif now < eff_dl + TERMINAL_OUTCOME_GRACE_MS:
            refusal = "deadline_not_elapsed"
        tickets: list[Message] = []
        if include_tickets:
            for m in f.members:
                await cur.execute(f"SELECT {_TICKET_COLS} FROM `match_authority_ticket` WHERE `player_id`=%s", (int(m.player_id),))
                trow = await cur.fetchone()
                if trow:
                    tickets.append(self._scan_ticket(trow))
        return StrandedFormationEntry(
            formation=f, formation_class=f_class, failed_check=failed_check, create_req=create_req,
            effective_deadline_ms=eff_dl, has_terminal_row=has_term, terminal_decision=term_dec,
            terminal_source_kind=term_src, digest=formation_digest(f), refusal=refusal, tickets=tuple(tickets),
        )

    async def list_stranded_formations(
        self, after_battle_id: int, limit: int
    ) -> tuple[list[StrandedFormationEntry], int, int]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                now = await self._db_now(cur)
                await cur.execute(
                    f"SELECT {_FORMATION_COLS} FROM `match_formation` "
                    "WHERE `battle_id`>%s AND `state` IN (2,3,4) AND `retry_at_ms`<>%s AND LENGTH(`create_request`)>0 "
                    "ORDER BY `battle_id` ASC LIMIT %s",
                    (after_battle_id, TERMINAL_OUTCOME_NO_RETRY_MS, limit * 2 + 1),
                )
                entries: list[StrandedFormationEntry] = []
                for row in await cur.fetchall():
                    f = self._scan_formation(row)
                    entry = await self._build_stranded_sql(cur, f, now, include_tickets=False)
                    if now >= entry.effective_deadline_ms + TERMINAL_OUTCOME_GRACE_MS:
                        entries.append(entry)
                        if len(entries) >= limit + 1:
                            break
                has_more = len(entries) > limit
                page = entries[:limit]
                next_bid = int(page[-1].formation.battle_id) if (has_more and page) else 0
                return page, next_bid, now

    async def inspect_stranded_formation(self, battle_id: int) -> tuple[StrandedFormationEntry | None, int]:
        async with portal_sql.connection(self.pool) as conn:
            async with conn.cursor() as cur:
                now = await self._db_now(cur)
                await cur.execute(f"SELECT {_FORMATION_COLS} FROM `match_formation` WHERE `battle_id`=%s", (battle_id,))
                row = await cur.fetchone()
                if not row:
                    return None, now
                return await self._build_stranded_sql(cur, self._scan_formation(row), now, include_tickets=True), now
