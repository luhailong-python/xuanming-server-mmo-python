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
                     NotCommitted, TxNeverBegan, ZoneConflict)
from .model import (INITIAL_OWNER_EPOCH, INITIAL_SAVE_SEQ, MAX_ROLE_BYTES, NAME_BATCH_LIMIT,
                    ROUTE_BATCH_LIMIT, ROUTE_SHARDS, SCHEMA_GEN, Ownership, valid_account)
from . import names

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


def _chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _marks(count: int) -> str:
    return ",".join(["%s"] * count)


class SqlStore:
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
    async def _read_account(self, account: str):
        """Directory row existence and the ref player ids (ordered), without row locks."""
        def work(cursor):
            return self._read_account_in(cursor, account)

        return await self._tx(work)

    async def _read_account_in(self, cursor, account: str):
        await cursor.execute("SELECT account,deletion_state,deletion_execute_after_ms,real_name_pi "
                             "FROM account_directory WHERE account=%s", (account,))
        if await cursor.fetchone() is None:
            return False, []
        await cursor.execute("SELECT player_id,held_ms FROM account_role_ref WHERE account=%s "
                             "ORDER BY player_id LIMIT %s", (account, MAX_REF_ROWS + 1))
        refs = [row[0] for row in await cursor.fetchall()]
        if len(refs) > MAX_REF_ROWS:
            raise RuntimeError("账号角色目录超过硬上限")
        return True, refs

    @staticmethod
    async def _lock_directory(cursor, account: str, create: bool) -> int:
        if not valid_account(account):
            raise Invalid("account")
        if create:
            await cursor.execute(
                "INSERT INTO account_directory(account,created_ms) VALUES (%s,UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000)"
                " ON DUPLICATE KEY UPDATE account=account", (account,))
        await cursor.execute("SELECT account,deletion_state FROM account_directory WHERE account=%s FOR UPDATE",
                             (account,))
        row = await cursor.fetchone()
        if row is None:
            raise Missing(account)
        return row[1]

    async def _roles_of_refs(self, account: str, refs: list[int]) -> list[bytes]:
        """Roles whose truth row (account_role.account) is this account, ordered by player id."""
        if not refs:
            return []
        _, rows = await self._one(f"SELECT player_id,{OWNERSHIP_COLUMNS} FROM account_role "
                                  f"WHERE player_id IN ({_marks(len(refs))})", refs)
        owners = {row[0]: self._decode(*row, self.role_type) for row in rows}
        return [owners[pid].role for pid in refs if pid in owners and owners[pid].account == account]

    async def read_account(self, account: str, create: bool) -> list[bytes]:
        if not valid_account(account):
            raise Invalid("account")
        exists, refs = await self._read_account(account)
        if not exists:
            if not create:
                raise Missing(account)
            # shortcut: the account-deletion receipt lookup (previous_deleted_ms) is skipped, no deletion in v1.
            await self._tx(lambda cursor: self._lock_directory(cursor, account, True))
            _, refs = await self._read_account(account)
        return await self._roles_of_refs(account, refs)

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
