"""MySQL/TiDB store for the data_service main path (single logical shard).

Every SQL statement is the Go one (``go/data_service/internal/store``) with ``?`` turned into ``%s``.
Transactions run through ``portal_sql.connection`` (bounded acquire, a failed connection is discarded).
The pool must use READ-COMMITTED and autocommit; ``open_pool`` sets both.
"""

import asyncio
import hashlib
import logging
import random
import re
from pathlib import Path

import aiomysql
import pymysql

from .. import portal_sql
from .errors import (Conflict, DirectoryError, Full, Invalid, Missing, NameConflict, NameInUse, NameOutsideWindow,
                     NotCommitted, RetryLater, TxNeverBegan, ZoneConflict)
from .model import (
    AccountDirectoryRead,
    DELETION_DELETING,
    DELETION_LEASE_MS,
    DELETION_NONE,
    DELETION_OUTCOME_BLOCKED,
    DELETION_OUTCOME_NOT_FOUND,
    DELETION_OUTCOME_OK,
    DELETION_OUTCOME_TOO_LATE,
    DELETION_PENDING,
    DELETION_SOURCE_ADMIN,
    DELETION_SOURCE_USER,
    INITIAL_OWNER_EPOCH,
    INITIAL_SAVE_SEQ,
    MAX_APPEARANCE_ID_BYTES,
    MAX_DELETION_CLAIM,
    MAX_DELETION_COOLING_OFF_MS,
    MAX_ROLE_BYTES,
    MAX_UINT64,
    NAME_BATCH_LIMIT,
    RENAME_COOLDOWN,
    RENAME_FROZEN,
    RENAME_INVALID,
    RENAME_NOT_FOUND,
    RENAME_OK,
    RENAME_SAME_NAME,
    RENAME_STALE_SEQ,
    RENAME_STATE_COMMITTED,
    RENAME_STATE_PENDING,
    RENAME_STATE_REJECTED,
    RENAME_STATE_REJECTED_DIRTY,
    RENAME_STATE_SETTLED,
    RENAME_TAKEN,
    ROUTE_BATCH_LIMIT,
    ROUTE_SHARDS,
    SCHEMA_GEN,
    STAGE1_TRADABLE_MATERIAL_LIFETIME_LIMIT,
    Ownership,
    valid_account,
    valid_pi,
)
from . import names
from .sql_audit import _SqlAuditMixin
from .sql_trade import _SqlTradeMixin

logger = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "data_service_schema.sql"
DB_NOW_MS = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)"   # route_store.go dbNowMsSQL
OWNERSHIP_COLUMNS = "account,role_proto,owner_revision,trade_listing_id,prepare_token"
ROUTE_COLUMNS = "home_zone, storage_id, placement_version, rev, updated_by, created_at_ms, updated_at_ms"
MAX_REF_ROWS = 100            # account_role_ref.go maxRefRows
REF_PENDING, REF_ACTIVE = 1, 2
LOCK_ERRORS = (1205, 1213, 9007)  # lock wait timeout, deadlock, TiDB write conflict
DUP_ENTRY = 1062
DEAD_HOLDER_ACK_DIGEST = hashlib.sha256(b"account-role-dead-holder-v1").digest()  # account_role_dead_holder.go
ID_LIMIT = 1 << 57  # counter value mask of the Go issuer (region bits above it stay 0)
NAME_ATTEMPTS = 3             # playerNameReserveAttempts / playerNameReleaseAttempts

LOCK_OWNERSHIP = f"SELECT {OWNERSHIP_COLUMNS} FROM account_role WHERE player_id=%s FOR UPDATE"
ROUTE_LOCK_ROW = f"SELECT {ROUTE_COLUMNS} FROM player_route WHERE route_shard = %s AND player_id = %s FOR UPDATE"
ROUTE_READ_ROW = f"SELECT {ROUTE_COLUMNS} FROM player_route WHERE route_shard = %s AND player_id = %s"
ROUTE_INSERT_NEW = (
    "INSERT INTO player_route (route_shard, player_id, home_zone, storage_id, placement_version,"
    f" rev, updated_by, created_at_ms, updated_at_ms) VALUES (%s, %s, %s, %s, %s, 1, %s, {DB_NOW_MS}, {DB_NOW_MS})"
    " ON DUPLICATE KEY UPDATE player_id = player_id")
ROUTE_FILL_HOME = (f"UPDATE player_route SET home_zone = %s, rev = rev + 1, updated_by = %s, updated_at_ms = {DB_NOW_MS}"
                   " WHERE route_shard = %s AND player_id = %s AND rev = %s AND home_zone IS NULL")
ROUTE_FILL_HOME_PLACEMENT = (
    "UPDATE player_route SET home_zone = %s, storage_id = %s, placement_version = 1, rev = rev + 1,"
    f" updated_by = %s, updated_at_ms = {DB_NOW_MS}"
    " WHERE route_shard = %s AND player_id = %s AND rev = %s AND home_zone IS NULL AND storage_id IS NULL")
WRITE_PENDING_REF = (
    "INSERT INTO account_role_ref(account,player_id,state,tx_id,listing_id,home_zone,nonce,created_ms)"
    f" VALUES (%s,%s,%s,%s,%s,%s,%s,{DB_NOW_MS}) ON DUPLICATE KEY UPDATE state=VALUES(state),tx_id=VALUES(tx_id),"
    "listing_id=VALUES(listing_id),home_zone=VALUES(home_zone),nonce=VALUES(nonce),created_ms=VALUES(created_ms),"
    "held_ms=0,held_reason=''")
ACTIVATE_REF = (
    "INSERT INTO account_role_ref(account,player_id,state,tx_id,listing_id,home_zone,nonce,created_ms)"
    f" VALUES (%s,%s,%s,0,0,0,%s,{DB_NOW_MS}) ON DUPLICATE KEY UPDATE state=IF(tx_id IN (0,%s),VALUES(state),state),"
    "listing_id=IF(tx_id IN (0,%s),0,listing_id),nonce=IF(tx_id IN (0,%s),VALUES(nonce),nonce),"
    "held_ms=IF(tx_id IN (0,%s),0,held_ms),held_reason=IF(tx_id IN (0,%s),'',held_reason),"
    "tx_id=IF(tx_id IN (0,%s),0,tx_id)")
NAME_RESERVE = ("INSERT INTO player_name (player_id, name, name_norm, created_ms) VALUES (%s, %s, %s, %s)"
                " ON DUPLICATE KEY UPDATE player_id = player_id")
NAME_OWNER_UPSERT = ("INSERT INTO player_name_owner (player_id, name_norm, name) VALUES (%s, %s, %s)"
                     " ON DUPLICATE KEY UPDATE player_id = player_id")
NAME_OWNER_DELETE = "DELETE FROM player_name_owner WHERE player_id = %s AND name_norm = %s"
NAME_RELEASE = ("DELETE player_name FROM player_name FORCE INDEX (PRIMARY)"
                " WHERE player_id = %s AND name_norm = %s AND created_ms >= %s AND bound_ms = 0")
NAME_TAKE_OVER = ("UPDATE player_name SET player_id = %s, name = %s, created_ms = %s, bound_ms = 0, held_until_ms = 0"
                  " WHERE name_norm = %s AND player_id = %s AND held_until_ms = %s AND held_until_ms <= %s")
NAME_BIND = "UPDATE player_name SET bound_ms = %s WHERE name_norm = %s AND player_id = %s AND bound_ms = 0"
NAME_HOLDER = "SELECT player_id, held_until_ms FROM player_name WHERE name_norm = %s"
NAME_ROW_STATE = "SELECT created_ms, bound_ms FROM player_name WHERE player_id = %s AND name_norm = %s"
ACCOUNT_ROLE_TRADABLE_QUOTA_TABLE = "account_role_tradable_quota"
ACCOUNT_ROLE_TRADABLE_QUOTA_DDL = (
    "CREATE TABLE IF NOT EXISTS account_role_tradable_quota ("
    "account VARBINARY(191) NOT NULL COMMENT 'pb:1',"
    "player_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:2',"
    "confirmed_units INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:3',"
    "updated_ms BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:4',"
    "PRIMARY KEY(account,player_id)) ENGINE=InnoDB"
)


def _chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _marks(count: int) -> str:
    return ",".join(["%s"] * count)


class SqlStore(_SqlTradeMixin, _SqlAuditMixin):
    def __init__(self, pool, protocol, *, max_roles: int = 5, storage_pools=None, writer: str = "data_service@py",
                 generation: str = "py1") -> None:
        self.pool, self.protocol, self.max_roles = pool, protocol, max_roles
        self.storage_pools = storage_pools or {}  # storage_id -> pool; also the storage allow-list
        self.writer, self.generation = writer, generation
        self.role_type = protocol.message_class("AccountSimplePlayer")
        self.database_type = protocol.message_class("player_database")

    # -- plumbing ------------------------------------------------------------------------------------
    async def bootstrap(self, schema: Path = SCHEMA_FILE) -> None:
        """Apply the idempotent DDL and seed route_meta (Go: no ON DUPLICATE KEY, a duplicate means it exists)."""
        text = "\n".join(line for line in schema.read_text("utf-8").splitlines() if not line.startswith("--"))
        async with portal_sql.connection(self.pool) as conn, conn.cursor() as cursor:
            for statement in re.split(r";\s*\n", text):
                if statement.strip():
                    await cursor.execute(statement)
            await cursor.execute(ACCOUNT_ROLE_TRADABLE_QUOTA_DDL)
            await cursor.execute("SHOW COLUMNS FROM account_directory LIKE 'stage1_tradable_material_used'")
            if await cursor.fetchone() is None:
                await cursor.execute(
                    "ALTER TABLE account_directory "
                    "ADD COLUMN stage1_tradable_material_used INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:9'"
                )
            await cursor.execute("SELECT writable FROM route_meta WHERE id = 1")
            if await cursor.fetchone() is None:
                try:
                    await cursor.execute("INSERT INTO route_meta (id, generation, writable) VALUES (1, %s, 1)",
                                         (self.generation,))
                except pymysql.err.IntegrityError:
                    pass  # seeded concurrently

    async def _tx(self, work, pool=None):
        """Run ``work(cursor)`` in one transaction. Business outcomes roll back cleanly and keep the connection."""
        failure, began = None, False
        try:
            async with portal_sql.connection(pool or self.pool) as conn:
                await conn.begin()
                began = True
                try:
                    async with conn.cursor() as cursor:
                        result = await work(cursor)
                except DirectoryError as error:
                    await conn.rollback()
                    failure = error
                else:
                    await conn.commit()
                    return result
        except Exception as error:
            if not began:
                raise TxNeverBegan(str(error)) from error
            raise
        raise failure

    async def _one(self, sql, args=(), *, pool=None):
        """One auto-committed statement; returns ``(rowcount, rows)``."""
        async with portal_sql.connection(pool or self.pool) as conn, conn.cursor() as cursor:
            await cursor.execute(sql, args)
            return cursor.rowcount, await cursor.fetchall()

    async def _locked(self, sql, args=()):
        """Retry a single statement after lock errors (1205/1213/9007) with jitter, like the name store."""
        for attempt in range(1, NAME_ATTEMPTS + 1):
            try:
                return await self._one(sql, args)
            except pymysql.err.MySQLError as error:
                if error.args[0] not in LOCK_ERRORS or attempt == NAME_ATTEMPTS:
                    raise
                await asyncio.sleep(random.uniform(0.01, 0.05))

    @staticmethod
    def _decode(player: int, account: bytes, raw: bytes, rev: int, listing: int, prepare: int, role_type):
        role = role_type()
        if len(raw) > MAX_ROLE_BYTES:
            raise RuntimeError("角色归属原件越界")
        role.ParseFromString(raw)
        account = bytes(account).decode()
        if (role.player_id != player or not player or not rev or (account and not valid_account(account))
                or (not account and (listing or prepare)) or (listing and prepare)):
            raise RuntimeError("角色归属原件损坏")
        return Ownership(account, bytes(raw), rev, listing, prepare)

    # -- account directory ---------------------------------------------------------------------------
    async def _read_account_detail_in(self, cursor, account: str):
        await cursor.execute(
            "SELECT account,deletion_state,deletion_execute_after_ms,real_name_pi "
            "FROM account_directory WHERE account=%s", (account,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None, []
        await cursor.execute(
            "SELECT player_id,held_ms FROM account_role_ref WHERE account=%s "
            "ORDER BY player_id LIMIT %s", (account, MAX_REF_ROWS + 1),
        )
        refs = [int(r[0]) for r in await cursor.fetchall()]
        if len(refs) > MAX_REF_ROWS:
            raise RuntimeError("账号角色目录超过硬上限")
        detail = {
            "deletion_state": int(row[1]),
            "deletion_execute_after_ms": int(row[2]),
            "real_name_pi": bytes(row[3] or b"").decode("utf-8"),
        }
        return detail, refs

    async def _read_account(self, account: str):
        """无锁读取目录行存在性与有序 ref 列表。"""
        detail, refs = await self._tx(lambda cursor: self._read_account_detail_in(cursor, account))
        return detail is not None, refs

    @staticmethod
    async def _lock_directory(cursor, account: str, create: bool) -> int:
        if not valid_account(account):
            raise Invalid("account")
        if create:
            await cursor.execute(
                "INSERT INTO account_directory(account,created_ms) VALUES (%s,UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000)"
                " ON DUPLICATE KEY UPDATE account=account", (account,),
            )
        await cursor.execute("SELECT account,deletion_state FROM account_directory WHERE account=%s FOR UPDATE",
                             (account,))
        row = await cursor.fetchone()
        if row is None:
            raise Missing(account)
        return int(row[1])

    async def _roles_of_refs(self, account: str, refs: list[int]) -> list[bytes]:
        """按真相 account_role.account 过滤 ref，返回有序角色序列化字节列表。"""
        if not refs:
            return []
        _, rows = await self._one(f"SELECT player_id,{OWNERSHIP_COLUMNS} FROM account_role "
                                  f"WHERE player_id IN ({_marks(len(refs))})", refs)
        owners = {row[0]: self._decode(*row, self.role_type) for row in rows}
        return [owners[pid].role for pid in refs if pid in owners and owners[pid].account == account]

    async def _ensure_account_with_receipt(self, account: str) -> int:
        """建账号行并查询是否曾注销过（返回 previous_deleted_ms）。"""
        digest = hashlib.sha256(account.encode("utf-8")).digest()

        async def work(cursor):
            await self._lock_directory(cursor, account, True)
            await cursor.execute(
                "SELECT deleted_ms FROM account_deletion_receipt WHERE account_digest=UNHEX(%s)",
                (digest.hex(),),
            )
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

        return await self._tx(work)

    async def read_account_with_lifecycle(self, account: str, create: bool) -> AccountDirectoryRead:
        """同一次读取账号角色列表、注销生命周期与实名 pi。"""
        if not valid_account(account):
            raise Invalid("account")
        detail, refs = await self._tx(lambda cursor: self._read_account_detail_in(cursor, account))
        previous_deleted_ms = 0
        if detail is None:
            if not create:
                raise Missing(account)
            previous_deleted_ms = await self._ensure_account_with_receipt(account)
            detail, refs = await self._tx(lambda cursor: self._read_account_detail_in(cursor, account))
            if detail is None:
                detail = {"deletion_state": 0, "deletion_execute_after_ms": 0, "real_name_pi": ""}
        roles = await self._roles_of_refs(account, refs)
        return AccountDirectoryRead(
            roles=roles,
            deletion_state=detail["deletion_state"],
            deletion_execute_after_ms=detail["deletion_execute_after_ms"],
            previous_deleted_ms=previous_deleted_ms,
            real_name_pi=detail["real_name_pi"],
        )

    async def read_account(self, account: str, create: bool) -> list[bytes]:
        return (await self.read_account_with_lifecycle(account, create)).roles

    async def account_deletion_state(self, account: str) -> int:
        """点读账号注销状态；账号行不存在返回 DELETION_NONE(0)。"""
        if not valid_account(account):
            raise Invalid("account")
        _, rows = await self._one("SELECT deletion_state FROM account_directory WHERE account=%s", (account,))
        return int(rows[0][0]) if rows else DELETION_NONE

    async def _read_owner(self, player: int) -> Ownership | None:
        _, rows = await self._one(f"SELECT {OWNERSHIP_COLUMNS},initial_player_commit FROM account_role "
                                  "WHERE player_id=%s", (player,))
        if not rows:
            return None
        account, raw, rev, listing, prepare, initial = rows[0]
        owned = self._decode(player, account, raw, rev, listing, prepare, self.role_type)
        return Ownership(owned.account, owned.role, rev, listing, prepare, initial == 1)

    async def lookup_role(self, player_id: int) -> Ownership:
        if not player_id:
            raise Invalid("player_id")
        owned = await self._read_owner(player_id)
        if owned is None or not owned.account:  # a tombstone counts as missing
            raise Missing(player_id)
        return Ownership(owned.account, owned.role, owned.owner_revision, owned.trade_listing_id, 0,
                         owned.initial_player_commit)

    def _check_replay(self, old: Ownership, account: str, role) -> None:
        """checkCreateReplay: same account, same role, no trade token, not a tombstone."""
        if (not old.account or old.trade_listing_id or old.prepare_token or old.account != account
                or self.role_type.FromString(old.role) != role):
            raise Conflict("role exists with a different owner or content")

    async def create_role(self, account: str, role) -> list[bytes]:
        """The four create steps of Go ``createRoleWithInitialRows`` (single shard)."""
        if not valid_account(account) or not role.player_id or role.ByteSize() > MAX_ROLE_BYTES:
            raise Invalid("role")
        raw = role.SerializeToString()
        try:
            old = await self._read_owner(role.player_id)
        except Exception as error:
            raise NotCommitted(str(error)) from error
        if old is not None:  # replay of a committed create: no store writes, errors here are not "not committed"
            self._check_replay(old, account, role)
            await self._bind_name(role)
            await self._activate_ref(account, role.player_id)
            return await self._role_list(account)
        try:
            storage = await self._initial_storage(role)
            ref = (role.player_id, role.zone_id, random.getrandbits(63))
            try:
                await self._reserve_ref(account, *ref)
            except Full:
                raise
            except Exception:
                await self._release_own_ref(account, ref)  # maybe written: delete by our nonce
                raise
            try:
                await self._write_initial_rows(storage, role)
            except Exception:
                await self._release_own_ref(account, ref)
                raise
        except (Conflict, Full, Invalid):
            raise
        except Exception as error:
            raise NotCommitted(str(error)) from error
        try:
            await self._commit_role(account, role, raw, storage)
        except NotCommitted:
            await self._release_own_ref(account, ref)
            raise
        except TxNeverBegan as error:  # BEGIN itself failed: no role row was written
            await self._release_own_ref(account, ref)
            raise NotCommitted(str(error)) from error
        await self._bind_name(role)
        await self._activate_ref(account, role.player_id)
        return await self._role_list(account)

    async def _role_list(self, account: str) -> list[bytes]:
        _, refs = await self._read_account(account)
        return await self._roles_of_refs(account, refs)

    async def _initial_storage(self, role) -> int:
        if not role.zone_id:
            raise RuntimeError("role has no home zone")
        _, rows = await self._one(ROUTE_READ_ROW, (role.player_id % ROUTE_SHARDS, role.player_id))
        if not rows or rows[0][0] is None:
            raise RuntimeError(f"player {role.player_id} has no route")
        home, storage, version, rev, *_ = rows[0]
        if home != role.zone_id or not rev or storage is None:
            raise Conflict("route does not match the role")
        if storage not in self.storage_pools:
            raise RuntimeError(f"storage {storage} is not allowed")
        return storage

    async def _reserve_ref(self, account: str, player: int, zone: int, nonce: int) -> None:
        """reserveRef: lock the directory row, check the per-account cap, write a PENDING ref.
        shortcut: no stale-ref eviction (Go reapRefs) when the account is full, add it with the background sweep."""
        async def work(cursor):
            deletion = await self._lock_directory(cursor, account, True)
            await cursor.execute("SELECT player_id,held_ms FROM account_role_ref WHERE account=%s "
                                 "ORDER BY player_id LIMIT %s", (account, MAX_REF_ROWS + 1))
            refs = [row[0] for row in await cursor.fetchall()]
            if player not in refs and (len(refs) >= self.max_roles or deletion):
                raise Full(account)
            await cursor.execute(WRITE_PENDING_REF, (account, player, REF_PENDING, 0, 0, zone, nonce))

        await self._tx(work)

    async def _release_own_ref(self, account: str, ref: tuple) -> None:
        player, _, nonce = ref
        try:
            async with asyncio.timeout(0.5):
                await self._one("DELETE FROM account_role_ref WHERE account=%s AND player_id=%s AND nonce=%s",
                                (account, player, nonce))
        except Exception:
            logger.exception("[account-role-ref] release failed, the slot stays until swept: player=%d", player)

    async def _activate_ref(self, account: str, player: int) -> None:
        try:
            await self._one(ACTIVATE_REF, (account, player, REF_ACTIVE, random.getrandbits(63), *[0] * 6))
        except Exception:
            logger.exception("[account-role-ref] activate failed: player=%d", player)

    async def _bind_name(self, role) -> None:
        """Best effort after commit: reserved -> bound. shortcut: no Restore path when the name row is gone."""
        if not role.name:
            return
        try:
            _, norm, verdict = names.normalize(role.name)
            owned = await self._read_owner(role.player_id)
            if verdict == names.OK and owned is not None and self.role_type.FromString(owned.role).name == role.name:
                await self._one(NAME_BIND, (await self._now_ms(), norm, role.player_id))
        except Exception:
            logger.exception("[player-name] name of a committed role is not bound: player=%d", role.player_id)

    async def _now_ms(self) -> int:
        _, rows = await self._one(f"SELECT {DB_NOW_MS}")
        return rows[0][0]

    async def _write_initial_rows(self, storage: int, role) -> None:
        """upsertInitialPlayerStoreRows: two player_store rows, ODKU no-op, read back and compare."""
        database = self.database_type(uint32_pb_component={"class": role.class_id},
                                      profile_component={"name": role.name, "appearance_id": role.appearance_id,
                                                         "gender": role.gender})
        rows = [(0, 1, b""), (4, 0, database.SerializeToString(deterministic=True))]  # OWNER released, PROFILE
        values = ",".join(["(%s,%s,%s,%s,%s,'',%s,UNHEX(%s))"] * len(rows))
        args = []
        for kind, released, payload in rows:
            args += [role.player_id, kind, INITIAL_OWNER_EPOCH, INITIAL_SAVE_SEQ, SCHEMA_GEN, released, payload.hex()]

        async def work(cursor):
            await cursor.execute("INSERT INTO player_store (player_id,kind,owner_epoch,save_seq,schema_gen,"
                                 f"holder_instance,released,payload) VALUES {values} "
                                 "ON DUPLICATE KEY UPDATE player_id=player_id", args)
            await cursor.execute("SELECT kind,owner_epoch,save_seq,schema_gen,holder_instance,released,payload "
                                 "FROM player_store WHERE player_id=%s ORDER BY kind FOR UPDATE", (role.player_id,))
            existing = await cursor.fetchall()
            same = len(existing) == len(rows) and all(
                (got[0], got[1], got[2], got[5], len(got[4] or b""), bytes(got[6])) ==
                (kind, INITIAL_OWNER_EPOCH, INITIAL_SAVE_SEQ, released, 0, payload) and got[3] <= SCHEMA_GEN
                for got, (kind, released, payload) in zip(existing, rows))
            if not same:
                raise Conflict("player_store already holds other rows")

        await self._tx(work, self.storage_pools[storage])

    async def _commit_role(self, account: str, role, raw: bytes, storage: int) -> None:
        """The commit point: route row check, then a strict INSERT into account_role."""
        async def work(cursor):
            await self._require_writable(cursor)
            await cursor.execute(ROUTE_LOCK_ROW, (role.player_id % ROUTE_SHARDS, role.player_id))
            route = await cursor.fetchone()
            if route is None:
                raise Conflict("route row missing")
            home, route_storage, _, rev, *_ = route
            if home != role.zone_id or not rev or route_storage != storage:
                raise Conflict("route changed")
            await cursor.execute(LOCK_OWNERSHIP, (role.player_id,))
            row = await cursor.fetchone()
            if row is not None:
                owned = self._decode(role.player_id, row[0], row[1], row[2], row[3], row[4], self.role_type)
                return self._check_replay(owned, account, role)
            try:
                await cursor.execute(
                    "INSERT INTO account_role(player_id,account,role_proto,owner_revision,updated_ms,"
                    "initial_player_commit,fence_version) VALUES (%s,%s,UNHEX(%s),1,"
                    "UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000,1,1)", (role.player_id, account, raw.hex()))
            except pymysql.err.IntegrityError as error:
                if error.args[0] == DUP_ENTRY:
                    raise Conflict("role row already exists") from error
                raise

        await self._tx(work)

    @staticmethod
    async def _require_writable(cursor) -> None:
        await cursor.execute("SELECT writable FROM route_meta WHERE id = 1")
        row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("route_meta: row id=1 missing; apply config/data_service_schema.sql")
        if row[0] != 1:
            raise RuntimeError(f"route_meta: writable!=1, route writers are frozen (writable={row[0]})")

    # -- role admissions -----------------------------------------------------------------------------
    @staticmethod
    async def _lock_admission_role(cursor, player: int, revision: int) -> Ownership:
        await cursor.execute(LOCK_OWNERSHIP, (player,))
        row = await cursor.fetchone()
        if row is None:
            raise Missing(player)
        return Ownership(bytes(row[0]).decode(), bytes(row[1]), row[2], row[3], row[4])

    @staticmethod
    async def _read_admission(cursor, identity_type, player: int, instance: str):
        await cursor.execute(
            "SELECT highest_generation,closed_through,identity_proto,request_digest,required_owner_epoch,"
            "required_save_seq,close_digest,updated_ms FROM account_role_admission "
            "WHERE player_id=%s AND scene_instance_id=%s FOR UPDATE", (player, instance))
        row = await cursor.fetchone()
        if row is None:
            return None
        highest, closed, raw, digest, epoch, seq, close_digest, _ = row
        identity = identity_type.FromString(bytes(raw))
        if (len(raw) > 1024 or identity.player_id != player or identity.scene_instance_id != instance
                or identity.generation != highest or closed > highest or len(digest) != 32
                or len(close_digest or b"") not in (0, 32) or (epoch == 0) != (seq == 0)):
            raise RuntimeError("角色入场登记原件损坏")
        return dict(highest=highest, closed=closed, identity=identity, digest=bytes(digest), epoch=epoch,
                    seq=seq, close_digest=bytes(close_digest or b""))

    async def register_admission(self, identity, digest: bytes) -> int:
        """Go ``AccountRoleAdmissionStore.Register``: one player-shard transaction, idempotent per generation."""
        raw = identity.SerializeToString()

        async def work(cursor):
            owned = await self._lock_admission_role(cursor, identity.player_id, identity.owner_revision)
            if owned.owner_revision != identity.owner_revision or owned.trade_listing_id:
                raise Conflict("owner revision changed or role is frozen")
            row = await self._read_admission(cursor, type(identity), identity.player_id, identity.scene_instance_id)
            if row is None:
                await cursor.execute(
                    "INSERT INTO account_role_admission(player_id,scene_instance_id,highest_generation,closed_through,"
                    "identity_proto,request_digest,required_owner_epoch,required_save_seq,close_digest,updated_ms) "
                    f"VALUES (%s,%s,%s,0,UNHEX(%s),UNHEX(%s),0,0,'',{DB_NOW_MS})",
                    (identity.player_id, identity.scene_instance_id, identity.generation, raw.hex(), digest.hex()))
                return 0
            if identity.generation <= row["closed"] or identity.generation < row["highest"]:
                raise Conflict("generation is closed or stale")
            if identity.generation == row["highest"]:
                if row["digest"] != digest:
                    raise Conflict("same generation, different request")
                return row["closed"]
            old = row["identity"]
            if old.scene_node_id != identity.scene_node_id or old.zone_id != identity.zone_id:
                raise Conflict("scene instance moved to another node or zone")
            await cursor.execute(
                f"UPDATE account_role_admission SET highest_generation=%s,identity_proto=UNHEX(%s),request_digest=UNHEX(%s),"
                f"updated_ms={DB_NOW_MS} WHERE player_id=%s AND scene_instance_id=%s",
                (identity.generation, raw.hex(), digest.hex(), identity.player_id, identity.scene_instance_id))
            return row["closed"]

        return await self._tx(work)

    async def close_admissions(self, request, digest: bytes) -> int:
        """Go ``CloseAdmissions``."""
        async def work(cursor):
            await cursor.execute(LOCK_OWNERSHIP, (request.player_id,))
            role = await cursor.fetchone()
            if role is None:
                raise Missing(request.player_id)
            if role[2] > request.owner_revision:  # transferred away: nothing left to close
                return request.through_generation
            if role[2] != request.owner_revision:
                raise Conflict("owner revision differs")
            identity_type = self.protocol.message_class("common.base.RoleAdmissionIdentity")
            row = await self._read_admission(cursor, identity_type, request.player_id, request.scene_instance_id)
            if row is None:
                raise Missing("admission")
            if (row["identity"].owner_revision != request.owner_revision
                    or request.through_generation > row["highest"] or request.through_generation < row["closed"]):
                raise Conflict("close range")
            if request.through_generation == row["closed"]:
                if row["close_digest"] not in (digest, DEAD_HOLDER_ACK_DIGEST):
                    raise Conflict("closed with a different request")
            else:
                epoch, seq = row["epoch"], row["seq"]
                if (request.required_owner_epoch, request.required_save_seq) > (epoch, seq):
                    epoch, seq = request.required_owner_epoch, request.required_save_seq
                await cursor.execute(
                    f"UPDATE account_role_admission SET closed_through=%s,required_owner_epoch=%s,required_save_seq=%s,"
                    f"close_digest=UNHEX(%s),updated_ms={DB_NOW_MS} WHERE player_id=%s AND scene_instance_id=%s",
                    (request.through_generation, epoch, seq, digest.hex(), request.player_id,
                     request.scene_instance_id))
            return request.through_generation

        return await self._tx(work)

    # -- routing -------------------------------------------------------------------------------------
    async def register_zone(self, player_id: int, home: int, storage: int) -> None:
        """Go ``RegisterHome`` (W1). Never overwrites a mapped home; conflict raises ``ZoneConflict``."""
        shard = player_id % ROUTE_SHARDS

        async def work(cursor):
            await self._require_writable(cursor)
            await cursor.execute(ROUTE_INSERT_NEW, (shard, player_id, home, storage, 1, self.writer))
            if cursor.rowcount == 1:
                return
            await cursor.execute(ROUTE_LOCK_ROW, (shard, player_id))
            row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("player_route: row vanished between ON DUPLICATE KEY and the locking read")
            existing, existing_storage, _, rev, *_ = row
            if existing == home:
                return
            if existing is not None:
                raise ZoneConflict(existing)
            if existing_storage is None:
                await cursor.execute(ROUTE_FILL_HOME_PLACEMENT, (home, storage, self.writer, shard, player_id, rev))
            else:  # the stored placement points at where the data really is: keep it
                logger.error("placement already present in player_route, kept: player=%d storage=%d",
                             player_id, storage)
                await cursor.execute(ROUTE_FILL_HOME, (home, self.writer, shard, player_id, rev))
            if cursor.rowcount != 1:
                raise RuntimeError("player_route: rev CAS lost under a row lock")

        await self._tx(work)

    async def routes(self, player_ids: list[int]) -> dict[int, int]:
        """player_id -> raw home zone; ids without a mapped home are absent. Any chunk failure fails the batch."""
        keys = sorted({(pid % ROUTE_SHARDS, pid) for pid in player_ids if pid})
        homes = {}
        for chunk in _chunks(keys, ROUTE_BATCH_LIMIT):
            sql = (f"SELECT route_shard, player_id, {ROUTE_COLUMNS} FROM player_route WHERE (route_shard, player_id) "
                   f"IN ({','.join(['(%s, %s)'] * len(chunk))}) ORDER BY route_shard, player_id")
            _, rows = await self._one(sql, [value for key in chunk for value in key])
            for shard, pid, home, *_ in rows:
                if shard != pid % ROUTE_SHARDS:
                    raise RuntimeError(f"player_route: stored route_shard mismatch for player {pid}")
                if home is not None:
                    homes[pid] = home
        return homes

    async def merged_zones(self) -> dict[int, int]:
        _, rows = await self._one("SELECT src_zone, dst_zone, run_id, merged_at_ms FROM zone_merged_into "
                                  "ORDER BY src_zone")
        return {src: dst for src, dst, *_ in rows}

    async def route_detail(self, player_id: int) -> tuple[int, int] | None:
        """读取原始 (home_zone, storage_id)，不做合服折叠；缺失任一字段返回 None。"""
        shard = player_id % ROUTE_SHARDS
        _, rows = await self._one(ROUTE_READ_ROW, (shard, player_id))
        if not rows:
            return None
        home, storage = rows[0][0], rows[0][1]
        if home is None or storage is None:
            return None
        return int(home), int(storage)

    async def count_zone_roles(self, zones: list[int]) -> int:
        if not zones:
            return 0
        _, rows = await self._one(
            f"SELECT COUNT(*) FROM player_route WHERE home_zone IN ({_marks(len(zones))})",
            zones,
        )
        return int(rows[0][0]) if rows else 0

    async def role_fence_state(self, player_id: int) -> dict | None:
        """读取角色护栏状态（含已删角墓碑，account 为空串）。"""
        _, rows = await self._one(
            "SELECT account, owner_revision, trade_listing_id, prepare_token, fence_version "
            "FROM account_role WHERE player_id=%s",
            (player_id,),
        )
        if not rows:
            return None
        account_raw, owner_rev, listing_id, prepare_token, fence_ver = rows[0]
        route = await self.route_detail(player_id)
        home_zone = route[0] if route is not None else 0
        freeze_token = int(listing_id or prepare_token or 0)
        return {
            "player_id": player_id,
            "account": bytes(account_raw).decode("utf-8"),
            "owner_revision": int(owner_rev),
            "freeze_token": freeze_token,
            "version": int(fence_ver or 1),
            "home_zone": home_zone,
            "freeze_final": bool(listing_id),
        }

    async def verify_admission(self, identity) -> bool:
        """强读校验该实例入场登记是否为当前未关闭最高世代且原件一致。"""
        owned = await self._read_owner(identity.player_id)
        if owned is None or not owned.account or owned.owner_revision != identity.owner_revision:
            return False
        _, rows = await self._one(
            "SELECT highest_generation, closed_through, identity_proto "
            "FROM account_role_admission WHERE player_id=%s AND scene_instance_id=%s",
            (identity.player_id, identity.scene_instance_id),
        )
        if not rows:
            return False
        highest, closed, raw_identity = rows[0]
        if int(identity.generation) != int(highest) or int(identity.generation) <= int(closed):
            return False
        stored = type(identity).FromString(bytes(raw_identity))
        return stored == identity

    async def read_store_row(self, player_id: int, kind: int) -> tuple[bool, bytes]:
        """读取玩家落盘表 player_store 的指定 kind payload，用于收件箱账本与物品封禁查询。"""
        found, _released, payload = await self.read_store_row_with_release(player_id, kind)
        return found, payload

    async def read_store_row_with_release(self, player_id: int, kind: int) -> tuple[bool, bool, bytes]:
        """读取玩家写权行释放状态及指定 kind payload。"""
        route = await self.route_detail(player_id)
        if route is None:
            return False, False, b""
        pool = self.storage_pools.get(route[1])
        if pool is None:
            raise RuntimeError(f"storage pool unavailable: {route[1]}")
        _, owner_rows = await self._one(
            "SELECT released FROM player_store WHERE player_id=%s AND kind=0", (player_id,), pool=pool,
        )
        if not owner_rows:
            return False, False, b""
        released = bool(owner_rows[0][0])
        _, rows = await self._one(
            "SELECT payload FROM player_store WHERE player_id=%s AND kind=%s", (player_id, kind), pool=pool,
        )
        if not rows or rows[0][0] is None:
            return True, released, b""
        return True, released, bytes(rows[0][0])

    async def player_displays(self, player_ids: list[int]) -> dict[int, dict]:
        """批量查询角色基础展示信息（名字/职业/性别/外观及默认等级）。"""
        if not player_ids:
            return {}
        _, rows = await self._one(
            f"SELECT player_id, {OWNERSHIP_COLUMNS} FROM account_role WHERE player_id IN ({_marks(len(player_ids))})",
            player_ids,
        )
        result: dict[int, dict] = {}
        for row in rows:
            pid = int(row[0])
            owned = self._decode(*row, self.role_type)
            if not owned.account:
                continue
            role = self.role_type.FromString(owned.role)
            result[pid] = {
                "name": role.name,
                "level": 0,
                "class_id": int(role.class_id),
                "gender": int(role.gender),
                "appearance_id": role.appearance_id,
            }
        return result

    async def upsert_channel_identity(self, account: str, channel: str, open_id: str) -> None:
        """在账号分片登记或刷新第三方登录渠道标识与 open_id。"""
        if not valid_account(account) or not channel or len(channel) > 32 or len(open_id) > 191:
            raise Invalid("channel identity")
        await self._one(
            f"INSERT INTO account_channel_identity(account,channel,open_id,updated_ms) "
            f"VALUES (%s,%s,%s,{DB_NOW_MS}) ON DUPLICATE KEY UPDATE "
            "channel=VALUES(channel),open_id=VALUES(open_id),updated_ms=VALUES(updated_ms)",
            (account, channel, open_id),
        )

    async def player_channel_identity(self, player_id: int) -> dict | None:
        """读取角色所属账号、已持久化渠道身份、建号时间与角色等级，用于聊天与内容审核画像。"""
        owned = await self._read_owner(player_id)
        if owned is None or not owned.account:
            return None
        _, rows = await self._one(
            "SELECT created_ms FROM account_directory WHERE account=%s", (owned.account,),
        )
        created_ms = int(rows[0][0]) if rows and rows[0][0] is not None else 0
        _, ch_rows = await self._one(
            "SELECT channel,open_id FROM account_channel_identity WHERE account=%s", (owned.account,),
        )
        channel, open_id = (str(ch_rows[0][0]), str(ch_rows[0][1])) if ch_rows else ("", "")
        displays = await self.player_displays([player_id])
        level = int(displays[player_id]["level"]) if player_id in displays else 0
        return {
            "account": owned.account,
            "channel": channel,
            "open_id": open_id,
            "account_created_ms": created_ms,
            "player_level": level,
        }

    # -- real name -----------------------------------------------------------------------------------
    async def set_real_name(self, account: str, pi: str) -> str:
        """只写一次实名 pi；已有不同 pi 时不改并返回库里现存值，账号不存在抛 Missing。"""
        if not valid_account(account) or not valid_pi(pi):
            raise Invalid("real_name")

        async def work(cursor):
            await cursor.execute(
                "UPDATE account_directory SET real_name_pi=%s WHERE account=%s AND real_name_pi=''",
                (pi, account),
            )
            await cursor.execute("SELECT real_name_pi FROM account_directory WHERE account=%s", (account,))
            row = await cursor.fetchone()
            if row is None:
                raise Missing(account)
            return bytes(row[0] or b"").decode("utf-8")

        return await self._tx(work)

    async def reset_real_name(self, account: str) -> bool:
        """客服重置实名：清空账号行上的 real_name_pi，返回重置前是否存在实名。"""
        if not valid_account(account):
            raise Invalid("account")

        async def work(cursor):
            await cursor.execute(
                "SELECT real_name_pi FROM account_directory WHERE account=%s FOR UPDATE", (account,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise Missing(account)
            had = len(bytes(row[0] or b"")) > 0
            if had:
                await cursor.execute("UPDATE account_directory SET real_name_pi='' WHERE account=%s", (account,))
            return had

        return await self._tx(work)

    async def get_real_name_of_player(self, player_id: int) -> tuple[bool, str]:
        """按玩家查所属账号的实名 pi；角色不存在/已删或账号不存在返回 (False, '')。"""
        if not player_id:
            raise Invalid("player_id")
        owned = await self._read_owner(player_id)
        if owned is None or not owned.account:
            return False, ""
        _, rows = await self._one("SELECT real_name_pi FROM account_directory WHERE account=%s", (owned.account,))
        if not rows:
            return False, ""
        return True, bytes(rows[0][0] or b"").decode("utf-8")

    async def sync_account_stage1_tradable(self, player_id: int, desired_char_total: int) -> tuple[bool, int, int]:
        """在账号分片单片事务内幂等确认单角色阶段一可交易材料份数并累加到账号终身 60 份硬上限（删角不重置）。"""
        if not player_id or desired_char_total > STAGE1_TRADABLE_MATERIAL_LIFETIME_LIMIT:
            raise Invalid("stage1_tradable")
        owned = await self._read_owner(player_id)
        if owned is None or not owned.account:
            return False, 0, 0
        account_bytes = owned.account.encode("utf-8")

        async def work(cursor):
            await cursor.execute(
                "SELECT stage1_tradable_material_used FROM account_directory WHERE account=%s FOR UPDATE",
                (account_bytes,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise Missing(owned.account)
            used = min(int(row[0] or 0), STAGE1_TRADABLE_MATERIAL_LIFETIME_LIMIT)
            await cursor.execute(
                f"SELECT confirmed_units FROM {ACCOUNT_ROLE_TRADABLE_QUOTA_TABLE} "
                "WHERE account=%s AND player_id=%s FOR UPDATE",
                (account_bytes, player_id),
            )
            qrow = await cursor.fetchone()
            prev_confirmed = int(qrow[0] or 0) if qrow is not None else 0
            confirmed_char = prev_confirmed
            account_used = used
            if desired_char_total > prev_confirmed and used < STAGE1_TRADABLE_MATERIAL_LIFETIME_LIMIT:
                want = desired_char_total - prev_confirmed
                remaining = STAGE1_TRADABLE_MATERIAL_LIFETIME_LIMIT - used
                grant = min(want, remaining)
                if grant > 0:
                    confirmed_char = prev_confirmed + grant
                    account_used = used + grant
                    await cursor.execute(
                        f"INSERT INTO {ACCOUNT_ROLE_TRADABLE_QUOTA_TABLE}"
                        f"(account,player_id,confirmed_units,updated_ms) VALUES (%s,%s,%s,{DB_NOW_MS}) "
                        "ON DUPLICATE KEY UPDATE confirmed_units=VALUES(confirmed_units),updated_ms=VALUES(updated_ms)",
                        (account_bytes, player_id, confirmed_char),
                    )
                    await cursor.execute(
                        "UPDATE account_directory SET stage1_tradable_material_used=%s WHERE account=%s",
                        (account_used, account_bytes),
                    )
            return True, confirmed_char, account_used

        try:
            return await self._tx(work)
        except Missing:
            return False, 0, 0

    # -- repair & remove roles -----------------------------------------------------------------------
    def _merge_role(self, old_raw: bytes, patch):
        merged = self.role_type.FromString(old_raw)
        if patch.class_id:
            if merged.class_id and merged.class_id != patch.class_id:
                raise Conflict("class_id mismatch")
            merged.class_id = patch.class_id
        if patch.gender:
            if merged.gender and merged.gender != patch.gender:
                raise Conflict("gender mismatch")
            merged.gender = patch.gender
        if patch.appearance_id:
            if merged.appearance_id and merged.appearance_id != patch.appearance_id:
                raise Conflict("appearance_id mismatch")
            merged.appearance_id = patch.appearance_id
        if patch.name:
            if merged.name and merged.name != patch.name:
                raise Conflict("name mismatch")
            merged.name = patch.name
        return merged

    async def repair_role(self, account: str, role, expected_owner_revision: int) -> list[bytes]:
        """仅补齐缺失的身份字段，不改变归属或 owner_revision。"""
        if (not valid_account(account) or not role.player_id or role.ByteSize() > MAX_ROLE_BYTES
                or not expected_owner_revision):
            raise Invalid("repair")

        async def work(cursor):
            await cursor.execute(LOCK_OWNERSHIP, (role.player_id,))
            row = await cursor.fetchone()
            if row is None:
                raise Missing(role.player_id)
            owned = self._decode(role.player_id, *row, self.role_type)
            if (owned.account != account or owned.owner_revision != expected_owner_revision
                    or owned.trade_listing_id != 0 or owned.prepare_token != 0):
                raise Conflict("ownership changed or frozen")
            merged = self._merge_role(owned.role, role)
            raw = merged.SerializeToString()
            if raw != owned.role:
                await cursor.execute(
                    f"UPDATE account_role SET role_proto=UNHEX(%s),updated_ms={DB_NOW_MS} "
                    "WHERE player_id=%s AND account=%s AND owner_revision=%s",
                    (raw.hex(), role.player_id, account, expected_owner_revision),
                )

        await self._tx(work)
        return await self._role_list(account)

    async def remove_roles(self, account: str, roles: list[tuple[int, int]]) -> None:
        """批量删角（逐个落墓碑并清 ref 与名字登记；存在开放入场登记/冻结/进行中改名时拒绝）。"""
        if not valid_account(account) or not roles or len(roles) > MAX_REF_ROWS:
            raise Invalid("remove")
        ordered = sorted(roles, key=lambda item: item[0])
        seen: set[int] = set()
        for pid, rev in ordered:
            if not pid or not rev or pid in seen:
                raise Invalid("remove role item")
            seen.add(pid)

        for pid, rev in ordered:
            await self._remove_one_role(account, pid, rev)

    async def _remove_one_role(self, account: str, player_id: int, revision: int) -> None:
        async def work(cursor):
            await cursor.execute(LOCK_OWNERSHIP, (player_id,))
            row = await cursor.fetchone()
            if row is None:
                return ""
            owned = self._decode(player_id, *row, self.role_type)
            if not owned.account:
                return self.role_type.FromString(owned.role).name
            if (owned.account != account or owned.owner_revision != revision
                    or owned.trade_listing_id != 0 or owned.prepare_token != 0):
                raise Conflict("role ownership changed or frozen")
            if owned.owner_revision == MAX_UINT64:
                raise RuntimeError("owner_revision exhausted")
            if await self._rename_open_tx(cursor, player_id):
                raise RetryLater("rename in progress")
            await cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM account_role_admission "
                "WHERE player_id=%s AND closed_through<highest_generation)",
                (player_id,),
            )
            if int((await cursor.fetchone())[0]):
                raise Conflict("open role admission exists")
            await cursor.execute(
                f"UPDATE account_role SET account='',owner_revision=owner_revision+1,trade_listing_id=0,"
                f"prepare_token=0,fence_version=fence_version+1,updated_ms={DB_NOW_MS} WHERE player_id=%s",
                (player_id,),
            )
            return self.role_type.FromString(owned.role).name

        role_name = await self._tx(work)
        await self._one("DELETE FROM account_role_ref WHERE account=%s AND player_id=%s", (account, player_id))
        if role_name:
            _, norm, _ = names.normalize(role_name)
            if norm:
                await self._one("DELETE FROM player_name WHERE player_id=%s AND name_norm=%s", (player_id, norm))
                await self._one(NAME_OWNER_DELETE, (player_id, norm))

    # -- account deletion (0 -> 1 -> 2 -> deleted + receipt) -----------------------------------------
    async def request_deletion(self, account: str, source: int, cooling_off_ms: int) -> dict:
        """申请账号注销：预检全部角色无寄售/准备冻结后 0 -> 1；已是 1/2 幂等原样回。"""
        if (not valid_account(account) or source not in (DELETION_SOURCE_USER, DELETION_SOURCE_ADMIN)
                or not 0 <= cooling_off_ms <= MAX_DELETION_COOLING_OFF_MS):
            raise Invalid("request_deletion")
        detail, refs = await self._tx(lambda cursor: self._read_account_detail_in(cursor, account))
        if detail is None:
            return {"outcome": DELETION_OUTCOME_NOT_FOUND, "lifecycle": None, "blockers": []}
        if detail["deletion_state"] != DELETION_NONE:
            return {
                "outcome": DELETION_OUTCOME_OK,
                "lifecycle": {
                    "deletion_state": detail["deletion_state"],
                    "deletion_execute_after_ms": detail["deletion_execute_after_ms"],
                },
                "blockers": [],
            }
        blockers: list[tuple[int, str]] = []
        if refs:
            _, rows = await self._one(
                f"SELECT player_id,{OWNERSHIP_COLUMNS} FROM account_role WHERE player_id IN ({_marks(len(refs))})",
                refs,
            )
            for row in rows:
                pid = int(row[0])
                owned = self._decode(*row, self.role_type)
                if owned.account != account:
                    continue
                if owned.trade_listing_id:
                    blockers.append((pid, "role_listed"))
                elif owned.prepare_token:
                    blockers.append((pid, "role_preparing"))
        if blockers:
            return {"outcome": DELETION_OUTCOME_BLOCKED, "lifecycle": {"deletion_state": 0, "deletion_execute_after_ms": 0},
                    "blockers": blockers}

        async def work(cursor):
            state = await self._lock_directory(cursor, account, False)
            if state == DELETION_NONE:
                await cursor.execute(
                    f"UPDATE account_directory SET deletion_state=%s,deletion_requested_ms={DB_NOW_MS},"
                    f"deletion_execute_after_ms={DB_NOW_MS}+%s,deletion_lease_until_ms=0,deletion_source=%s "
                    "WHERE account=%s",
                    (DELETION_PENDING, cooling_off_ms, source, account),
                )
            await cursor.execute(
                "SELECT deletion_state,deletion_execute_after_ms FROM account_directory WHERE account=%s",
                (account,),
            )
            st, exec_after = await cursor.fetchone()
            return {"deletion_state": int(st), "deletion_execute_after_ms": int(exec_after)}

        try:
            lifecycle = await self._tx(work)
        except Missing:
            return {"outcome": DELETION_OUTCOME_NOT_FOUND, "lifecycle": None, "blockers": []}
        return {"outcome": DELETION_OUTCOME_OK, "lifecycle": lifecycle, "blockers": []}

    async def cancel_deletion(self, account: str) -> dict:
        """撤销注销：1 -> 0，0 原样回，2 回 TOO_LATE。"""
        if not valid_account(account):
            raise Invalid("account")

        async def work(cursor):
            state = await self._lock_directory(cursor, account, False)
            outcome = DELETION_OUTCOME_OK
            if state == DELETION_PENDING:
                await cursor.execute(
                    "UPDATE account_directory SET deletion_state=0,deletion_requested_ms=0,"
                    "deletion_execute_after_ms=0,deletion_lease_until_ms=0,deletion_source=0 WHERE account=%s",
                    (account,),
                )
            elif state == DELETION_DELETING:
                outcome = DELETION_OUTCOME_TOO_LATE
            await cursor.execute(
                "SELECT deletion_state,deletion_execute_after_ms FROM account_directory WHERE account=%s",
                (account,),
            )
            st, exec_after = await cursor.fetchone()
            return {
                "outcome": outcome,
                "lifecycle": {"deletion_state": int(st), "deletion_execute_after_ms": int(exec_after)},
                "blockers": [],
            }

        try:
            return await self._tx(work)
        except Missing:
            return {"outcome": DELETION_OUTCOME_NOT_FOUND, "lifecycle": None, "blockers": []}

    async def claim_deletions(self, limit: int) -> list[str]:
        """认领冷静期满的 1 与租约过期的 2，条件置为 2 并写租约（不可撤销）。"""
        if not 1 <= limit <= MAX_DELETION_CLAIM:
            raise Invalid("limit")
        _, due_rows = await self._one(
            f"SELECT account FROM account_directory WHERE deletion_state IN (1,2) "
            f"AND deletion_execute_after_ms<={DB_NOW_MS} AND deletion_lease_until_ms<={DB_NOW_MS} "
            "ORDER BY deletion_execute_after_ms LIMIT %s",
            (limit,),
        )
        claimed: list[str] = []
        for (account_b,) in due_rows:
            acct = bytes(account_b).decode("utf-8")

            async def claim_one(cursor, target=acct):
                await cursor.execute(
                    f"UPDATE account_directory SET deletion_state=%s,deletion_lease_until_ms={DB_NOW_MS}+%s "
                    f"WHERE account=%s AND deletion_state IN (1,2) "
                    f"AND deletion_execute_after_ms<={DB_NOW_MS} AND deletion_lease_until_ms<={DB_NOW_MS}",
                    (DELETION_DELETING, DELETION_LEASE_MS, target),
                )
                return cursor.rowcount == 1

            if await self._tx(claim_one):
                claimed.append(acct)
        return claimed

    async def finish_deletion(self, account: str) -> tuple[bool, int]:
        """收尾账号注销：清陈旧 ref，状态为 2 且无剩余 ref 时写回执并删账号行。"""
        if not valid_account(account):
            raise Invalid("account")
        detail, refs = await self._tx(lambda cursor: self._read_account_detail_in(cursor, account))
        if detail is None:
            return True, 0
        if detail["deletion_state"] != DELETION_DELETING:
            raise Conflict("account is not in deleting state")
        # 清理可证明陈旧的 ref（角色不存在、已删墓碑或归属已转走）
        if refs:
            _, rows = await self._one(
                f"SELECT player_id,{OWNERSHIP_COLUMNS} FROM account_role WHERE player_id IN ({_marks(len(refs))})",
                refs,
            )
            live = {int(r[0]) for r in rows if bytes(r[1]).decode("utf-8") == account}
            for pid in refs:
                if pid not in live:
                    await self._one("DELETE FROM account_role_ref WHERE account=%s AND player_id=%s", (account, pid))

        digest = hashlib.sha256(account.encode("utf-8")).digest()

        async def work(cursor):
            try:
                state = await self._lock_directory(cursor, account, False)
            except Missing:
                return True, 0
            if state != DELETION_DELETING:
                raise Conflict("account is not in deleting state")
            await cursor.execute("SELECT player_id FROM account_role_ref WHERE account=%s", (account,))
            left = await cursor.fetchall()
            if left:
                return False, len(left)
            await cursor.execute(
                "SELECT deletion_requested_ms,deletion_source FROM account_directory WHERE account=%s", (account,),
            )
            requested_ms, source = await cursor.fetchone()
            await cursor.execute(
                f"INSERT INTO account_deletion_receipt(account_digest,requested_ms,deleted_ms,source) "
                f"VALUES (UNHEX(%s),%s,{DB_NOW_MS},%s) ON DUPLICATE KEY UPDATE "
                f"requested_ms=%s,deleted_ms={DB_NOW_MS},source=%s",
                (digest.hex(), int(requested_ms), int(source), int(requested_ms), int(source)),
            )
            await cursor.execute("DELETE FROM account_directory WHERE account=%s", (account,))
            return True, 0

        return await self._tx(work)

    # -- role appearance & rename --------------------------------------------------------------------
    async def update_role_appearance(self, player_id: int, appearance_id: str) -> bool:
        """同步角色永久外观到 account_role.role_proto，不改动 owner_revision 与 fence_version。"""
        if not player_id or not appearance_id or len(appearance_id.encode("utf-8")) > MAX_APPEARANCE_ID_BYTES:
            raise Invalid("appearance")

        async def work(cursor):
            await cursor.execute(LOCK_OWNERSHIP, (player_id,))
            row = await cursor.fetchone()
            if row is None:
                return False
            owned = self._decode(player_id, *row, self.role_type)
            if not owned.account:
                return False
            role = self.role_type.FromString(owned.role)
            if role.appearance_id == appearance_id:
                return False
            role.appearance_id = appearance_id
            raw = role.SerializeToString()
            await cursor.execute(
                f"UPDATE account_role SET role_proto=UNHEX(%s),updated_ms={DB_NOW_MS} WHERE player_id=%s",
                (raw.hex(), player_id),
            )
            return True

        return await self._tx(work)

    @staticmethod
    async def _rename_open_tx(cursor, player_id: int) -> bool:
        await cursor.execute("SELECT state FROM account_role_rename WHERE player_id=%s", (player_id,))
        row = await cursor.fetchone()
        return row is not None and int(row[0]) in (
            RENAME_STATE_PENDING, RENAME_STATE_COMMITTED, RENAME_STATE_REJECTED_DIRTY,
        )

    async def rename_role(
        self, player_id: int, seq: int, display: str, norm: str, *,
        invalid: bool, cooldown_ms: int, hold_ms: int, now_ms: int,
    ) -> dict:
        """角色改名（按 seq 幂等与防倒流，提交点改 account_role 与 player_name_owner，随后绑定新名并保护旧名）。"""
        if not player_id or not seq or cooldown_ms < 0 or hold_ms < 0:
            raise Invalid("rename")

        def replay(rec) -> dict:
            r_seq, state, result, new_name, _new_norm, _old_norm, renamed_ms, _held = rec
            if state in (RENAME_STATE_COMMITTED, RENAME_STATE_SETTLED):
                return {"result": RENAME_OK, "current_seq": r_seq, "name": new_name,
                        "renamed_ms": renamed_ms, "next_allowed_ms": 0}
            next_ms = (renamed_ms + cooldown_ms) if result == RENAME_COOLDOWN else 0
            return {"result": result, "current_seq": r_seq, "name": "", "renamed_ms": 0, "next_allowed_ms": next_ms}

        async def begin_tx(cursor):
            await cursor.execute(LOCK_OWNERSHIP, (player_id,))
            row = await cursor.fetchone()
            if row is None:
                return "done", {"result": RENAME_NOT_FOUND, "current_seq": 0, "name": "", "renamed_ms": 0, "next_allowed_ms": 0}
            owned = self._decode(player_id, *row, self.role_type)
            if not owned.account:
                return "done", {"result": RENAME_NOT_FOUND, "current_seq": 0, "name": "", "renamed_ms": 0, "next_allowed_ms": 0}
            await cursor.execute(
                "SELECT seq,state,result,new_name,new_norm,old_norm,renamed_ms,held_until_ms "
                "FROM account_role_rename WHERE player_id=%s FOR UPDATE",
                (player_id,),
            )
            rec = await cursor.fetchone()
            prev_renamed_ms = 0
            if rec is not None:
                r_seq, r_state, r_res, r_new_name, r_new_norm, r_old_norm, r_renamed_ms, r_held = (
                    int(rec[0]), int(rec[1]), int(rec[2]), str(rec[3]), str(rec[4]), str(rec[5]), int(rec[6]), int(rec[7]),
                )
                prev_renamed_ms = r_renamed_ms
                if r_seq > seq or (r_seq == seq and r_new_norm != norm):
                    return "done", {"result": RENAME_STALE_SEQ, "current_seq": r_seq, "name": "", "renamed_ms": 0, "next_allowed_ms": 0}
                if r_seq == seq:
                    if r_state == RENAME_STATE_PENDING:
                        return "proceed", None
                    return "done", replay((r_seq, r_state, r_res, r_new_name, r_new_norm, r_old_norm, r_renamed_ms, r_held))
            role = self.role_type.FromString(owned.role)
            _, cur_norm, _ = names.normalize(role.name)
            verdict_res = 0
            next_allowed = 0
            if invalid:
                verdict_res = RENAME_INVALID
            elif owned.trade_listing_id or owned.prepare_token:
                verdict_res = RENAME_FROZEN
            elif cur_norm == norm:
                verdict_res = RENAME_SAME_NAME
            elif prev_renamed_ms and now_ms < prev_renamed_ms + cooldown_ms:
                verdict_res = RENAME_COOLDOWN
                next_allowed = prev_renamed_ms + cooldown_ms
            state = RENAME_STATE_REJECTED if verdict_res else RENAME_STATE_PENDING
            await cursor.execute(
                "INSERT INTO account_role_rename(player_id,seq,state,result,new_name,new_norm,old_norm,"
                "renamed_ms,held_until_ms,updated_ms) VALUES (%s,%s,%s,%s,%s,%s,'',0,0,%s) "
                "ON DUPLICATE KEY UPDATE seq=VALUES(seq),state=VALUES(state),result=VALUES(result),"
                "new_name=VALUES(new_name),new_norm=VALUES(new_norm),old_norm='',held_until_ms=0,updated_ms=VALUES(updated_ms)",
                (player_id, seq, state, verdict_res, display, norm, now_ms),
            )
            if state == RENAME_STATE_REJECTED:
                return "done", {"result": verdict_res, "current_seq": seq, "name": "", "renamed_ms": 0, "next_allowed_ms": next_allowed}
            return "proceed", None

        step, out = await self._tx(begin_tx)
        if step == "done":
            return out

        # 预留新名（只动 player_name 表）
        outcome, _owner = await self._reserve_row(player_id, display, norm, now_ms)
        if outcome == names.RESERVE_TAKEN:
            async def reject_tx(cursor):
                await cursor.execute(
                    "UPDATE account_role_rename SET state=%s,result=%s,updated_ms=%s "
                    "WHERE player_id=%s AND seq=%s AND state=%s",
                    (RENAME_STATE_REJECTED, RENAME_TAKEN, now_ms, player_id, seq, RENAME_STATE_PENDING),
                )
            await self._tx(reject_tx)
            return {"result": RENAME_TAKEN, "current_seq": seq, "name": "", "renamed_ms": 0, "next_allowed_ms": 0}

        # 提交点：更新 account_role.role_proto、player_name_owner 与改名记录
        async def commit_tx(cursor):
            await cursor.execute(LOCK_OWNERSHIP, (player_id,))
            row = await cursor.fetchone()
            if row is None:
                raise Missing(player_id)
            owned = self._decode(player_id, *row, self.role_type)
            if not owned.account or owned.trade_listing_id or owned.prepare_token:
                raise Conflict("role removed or frozen during rename")
            role = self.role_type.FromString(owned.role)
            _, old_norm, _ = names.normalize(role.name)
            role.name = display
            raw = role.SerializeToString()
            held_until = now_ms + hold_ms
            await cursor.execute(
                "UPDATE account_role SET role_proto=UNHEX(%s),updated_ms=%s WHERE player_id=%s",
                (raw.hex(), now_ms, player_id),
            )
            await cursor.execute(
                "INSERT INTO player_name_owner(player_id,name_norm,name) VALUES (%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE name_norm=VALUES(name_norm),name=VALUES(name)",
                (player_id, norm, display),
            )
            await cursor.execute(
                "UPDATE account_role_rename SET state=%s,old_norm=%s,renamed_ms=%s,held_until_ms=%s,updated_ms=%s "
                "WHERE player_id=%s AND seq=%s AND state=%s",
                (RENAME_STATE_COMMITTED, old_norm, now_ms, held_until, now_ms, player_id, seq, RENAME_STATE_PENDING),
            )
            return old_norm, held_until

        old_norm, held_until = await self._tx(commit_tx)
        # 收尾：绑定新名，旧名置保护期，记录置已收尾
        await self._one(NAME_BIND, (now_ms, norm, player_id))
        if old_norm and old_norm != norm:
            await self._one(
                "UPDATE player_name SET held_until_ms=%s WHERE name_norm=%s AND player_id=%s",
                (max(1, held_until), old_norm, player_id),
            )
        await self._one(
            "UPDATE account_role_rename SET state=%s,updated_ms=%s WHERE player_id=%s AND seq=%s AND state=%s",
            (RENAME_STATE_SETTLED, now_ms, player_id, seq, RENAME_STATE_COMMITTED),
        )
        return {"result": RENAME_OK, "current_seq": seq, "name": display, "renamed_ms": now_ms, "next_allowed_ms": 0}

    # -- player_store dead holder read / release -----------------------------------------------------
    async def read_player_store_holder(self, player_id: int) -> tuple[int, bytes, int] | None:
        """读取 player_store 写权行 (owner_epoch, holder_instance, claim_seq)；无路由或写权行返回 None。"""
        if not player_id:
            raise Invalid("player_id")
        route = await self.route_detail(player_id)
        if route is None:
            return None
        pool = self.storage_pools.get(route[1])
        if pool is None:
            raise RuntimeError(f"storage pool unavailable: {route[1]}")
        _, rows = await self._one(
            "SELECT owner_epoch,holder_instance,claim_seq FROM player_store WHERE player_id=%s AND kind=0",
            (player_id,), pool=pool,
        )
        if not rows:
            return None
        return int(rows[0][0]), bytes(rows[0][1] or b""), int(rows[0][2])

    async def release_dead_player_store_holder(
        self, player_id: int, owner_epoch: int, seen: tuple[int, bytes, int] | None = None,
    ) -> None:
        """判死释放 player_store 写权行：抬代次至 owner_epoch+1、清 holder_instance、置 released=1。"""
        if not player_id or not owner_epoch or owner_epoch == MAX_UINT64:
            raise Invalid("release_dead_holder")
        route = await self.route_detail(player_id)
        if route is None:
            return
        pool = self.storage_pools.get(route[1])
        if pool is None:
            raise RuntimeError(f"storage pool unavailable: {route[1]}")

        async def work(cursor):
            await cursor.execute(
                "SELECT owner_epoch,holder_instance,claim_seq FROM player_store "
                "WHERE player_id=%s AND kind=0 FOR UPDATE",
                (player_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return
            cur = (int(row[0]), bytes(row[1] or b""), int(row[2]))
            if cur[0] > owner_epoch:
                return
            if seen is not None and cur != seen:
                raise Conflict("player store holder changed since read")
            await cursor.execute(
                "UPDATE player_store SET owner_epoch=%s,holder_instance='',released=1 "
                "WHERE player_id=%s AND kind=0",
                (owner_epoch + 1, player_id),
            )

        await self._tx(work, pool=pool)

    # -- names ---------------------------------------------------------------------------------------
    async def _owned_norm(self, player_id: int):
        _, rows = await self._one("SELECT name_norm FROM player_name_owner WHERE player_id = %s", (player_id,))
        return rows[0][0] if rows else None

    async def reserve_name(self, player_id: int, name: str, norm: str, now_ms: int) -> tuple[int, int]:
        """Go ``PlayerNameStore.Reserve``: the unique key on name_norm decides; returns (result, owner)."""
        owned = await self._owned_norm(player_id)
        if owned is not None and owned != norm:
            _, rows = await self._one(NAME_HOLDER, (owned,))
            if rows and rows[0][0] == player_id:
                raise NameConflict(f"player {player_id} already holds {owned!r}, refused {norm!r}")
            await self._locked(NAME_OWNER_DELETE, (player_id, owned))
            owned = None
        outcome, owner = await self._reserve_row(player_id, name, norm, now_ms)
        if outcome == names.RESERVE_TAKEN:
            if owned is not None:
                await self._locked(NAME_OWNER_DELETE, (player_id, norm))
            return names.RESERVE_TAKEN, owner
        count, _ = await self._locked(NAME_OWNER_UPSERT, (player_id, norm, name))
        if count != 1:  # the owner row existed already: compare
            other = await self._owned_norm(player_id)
            if other is None:
                raise RuntimeError("player_name_owner: duplicate row vanished (concurrent release)")
            if other != norm:
                if outcome == "inserted":
                    await self._locked(NAME_RELEASE, (player_id, norm, now_ms))
                raise NameConflict(f"player {player_id} already holds {other!r}, refused {norm!r}")
        return names.RESERVE_OK, 0

    async def _reserve_row(self, player_id: int, name: str, norm: str, now_ms: int):
        for _ in range(NAME_ATTEMPTS):
            count, _ = await self._locked(NAME_RESERVE, (player_id, name, norm, now_ms))
            if count == 1:
                return "inserted", 0
            _, rows = await self._one(NAME_HOLDER, (norm,))
            if not rows:  # the row vanished (concurrent release): insert again
                continue
            owner, held_until = rows[0]
            if owner == player_id:
                return "owned", 0
            if held_until == 0 or held_until > now_ms:
                return names.RESERVE_TAKEN, owner
            taken, _ = await self._locked(NAME_TAKE_OVER, (player_id, name, now_ms, norm, owner, held_until, now_ms))
            if taken == 1:
                return "inserted", 0
        raise RuntimeError(f"player_name reserve: player_id={player_id} exhausted {NAME_ATTEMPTS} attempts")

    async def _name_state(self, player_id: int, norm: str):
        _, rows = await self._one(NAME_ROW_STATE, (player_id, norm))
        return rows[0] if rows else None

    async def release_name(self, player_id: int, norm: str, min_created_ms: int) -> None:
        """Go ``logic.ReleasePlayerName`` (non-admin): read, prove the role never commits, conditional delete."""
        def check(state):
            if state is None:
                return False
            created, bound = state
            if bound:
                raise NameInUse(f"player {player_id}: name is bound to a role")
            if created < min_created_ms:
                raise NameOutsideWindow(f"player {player_id}: registered before the release window")
            return True

        if not check(await self._name_state(player_id, norm)):
            return
        if await self._seal_uncommitted_role(player_id) == "committed":
            raise NameInUse(f"player {player_id}: name belongs to a committed role")
        for _ in range(NAME_ATTEMPTS):
            count, _ = await self._locked(NAME_RELEASE, (player_id, norm, min_created_ms))
            if count > 0 or not check(await self._name_state(player_id, norm)):
                break  # deleted, or gone already
        else:
            raise RuntimeError(f"player_name release: player_id={player_id} exhausted {NAME_ATTEMPTS} attempts")
        await self._locked(NAME_OWNER_DELETE, (player_id, norm))

    async def _seal_uncommitted_role(self, player_id: int) -> str:
        """Go ``sealRoleTx``: route row lock, then role row lock; an unowned routed id gets a tombstone."""
        async def work(cursor):
            await cursor.execute(ROUTE_LOCK_ROW, (player_id % ROUTE_SHARDS, player_id))
            route = await cursor.fetchone()
            await cursor.execute(LOCK_OWNERSHIP, (player_id,))
            row = await cursor.fetchone()
            if row is not None:
                return "committed" if bytes(row[0]) else "removed"
            if route is None or route[0] is None:
                return "unrouted"
            tombstone = self.role_type(player_id=player_id, zone_id=route[0]).SerializeToString()
            await cursor.execute(
                "INSERT INTO account_role(player_id,account,role_proto,owner_revision,updated_ms,"
                f"initial_player_commit,fence_version) VALUES (%s,'',UNHEX(%s),1,{DB_NOW_MS},0,1)",
                (player_id, tombstone.hex()))
            return "sealed"

        for attempt in (1, 2):  # a concurrent tombstone makes the INSERT fail once; the retry reads it
            try:
                return await self._tx(work)
            except pymysql.err.IntegrityError as error:
                if error.args[0] != DUP_ENTRY or attempt == 2:
                    raise

    async def player_names(self, player_ids: list[int]) -> dict[int, str]:
        if len(player_ids) > NAME_BATCH_LIMIT:
            raise Invalid("too many ids")
        if not player_ids:
            return {}
        _, rows = await self._one(f"SELECT player_id, name FROM player_name_owner "
                                  f"WHERE player_id IN ({_marks(len(player_ids))})", player_ids)
        return dict(rows)

    # -- id segments ---------------------------------------------------------------------------------
    async def allocate(self, step: int) -> tuple[int, int]:
        """One shared counter in ``data_service_id_counter`` (Go: etcd issuer with region bits and double records)."""
        async def work(cursor):
            await cursor.execute("INSERT INTO data_service_id_counter(id,next_id) VALUES (0,1) ON DUPLICATE KEY UPDATE id=id")
            await cursor.execute("SELECT next_id FROM data_service_id_counter WHERE id=0 FOR UPDATE")
            lo = (await cursor.fetchone())[0]
            if lo + step > ID_LIMIT:
                raise OverflowError("id space exhausted")
            await cursor.execute("UPDATE data_service_id_counter SET next_id=%s WHERE id=0", (lo + step,))
            return lo, lo + step

        return await self._tx(work)


async def open_pool(*, host, port, user, password, database, maxsize=8, minsize=1):
    """Pool with the session the Go route store probes for: READ-COMMITTED, autocommit, utf8mb4."""
    return await aiomysql.create_pool(
        host=host, port=port, user=user, password=password, db=database, minsize=minsize, maxsize=maxsize,
        connect_timeout=3, autocommit=True, charset="utf8mb4",
        init_command="SET SESSION transaction_isolation='READ-COMMITTED'")
