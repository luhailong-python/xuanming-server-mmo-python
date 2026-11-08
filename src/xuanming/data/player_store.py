"""Scene 与玩家存储之间的事务、围栏和存盘重试边界。"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import random
import sqlite3
import time
from collections.abc import Awaitable, Callable, Mapping
from contextlib import closing
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Protocol
from weakref import WeakValueDictionary

from google.protobuf.message import DecodeError, Message

from .player_rows import PlayerRowKind

_MAX_UINT64 = (1 << 64) - 1
_MAX_WIRE_BYTES = (4 * 1024 - 64) * 1024
_MAX_SAVE_BYTES = 3 << 20
_MAX_ROW_BYTES = 1 << 20
_STORE = "/playerstore.PlayerStore"
_DETACHED_SETTLED = {
    "SAVE_PLAYER_COMMITTED", "SAVE_PLAYER_EQUAL", "SAVE_PLAYER_NO_OWNER",
    "SAVE_PLAYER_UNCLAIMED", "SAVE_PLAYER_STALE_OWNER", "SAVE_PLAYER_NOT_HOLDER",
    "SAVE_PLAYER_OLDER_SEQ",
}


class StoreRejected(RuntimeError):
    """服务明确拒绝了认领或存盘。"""

    def __init__(self, result: str) -> None:
        self.result = result
        super().__init__(result)


class StoreUnavailable(RuntimeError):
    """存储请求在预算内未获得结论。"""


class SaveInDoubt(StoreUnavailable):
    """同一个存盘请求仍需原样重试。"""


@dataclass(frozen=True, slots=True)
class PlayerSnapshot:
    player_id: int
    owner: bytes
    owner_epoch: int
    save_seq: int
    rows: Mapping[int, bytes]
    storage_id: int = 1
    schema_gen: int = 1
    released: bool = False
    has_saved: bool = False
    _revision: _Revision | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "rows", MappingProxyType(dict(self.rows)))


@dataclass(slots=True, weakref_slot=True)
class _Revision:
    save_seq: int
    deferred_since: float | None = None


class PlayerStore(Protocol):
    async def load_and_claim(
        self, player_id: int, owner: bytes, owner_epoch: int = 0
    ) -> PlayerSnapshot: ...

    async def commit(
        self,
        snapshot: PlayerSnapshot,
        rows: Mapping[int, bytes],
        *,
        release: bool = False,
        force: bool = False,
        periodic: bool = False,
    ) -> PlayerSnapshot: ...

    async def release(self, snapshot: PlayerSnapshot) -> PlayerSnapshot: ...

    async def release_detached(self, player_id: int, owner: bytes, owner_epoch: int) -> bool: ...


class _SnapshotStore:
    def __init__(self, *, piggyback_fallback: float = 300.0) -> None:
        if piggyback_fallback < 0:
            raise ValueError("piggyback_fallback 不能为负")
        self._piggyback_fallback = piggyback_fallback
        self._locks: WeakValueDictionary[int, asyncio.Lock] = WeakValueDictionary()
        self._active: WeakValueDictionary[int, _Revision] = WeakValueDictionary()

    def _lock(self, player_id: int) -> asyncio.Lock:
        lock = self._locks.get(player_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[player_id] = lock
        return lock

    def _claimed(self, snapshot: PlayerSnapshot) -> PlayerSnapshot:
        revision = _Revision(snapshot.save_seq)
        self._active[snapshot.player_id] = revision
        return replace(snapshot, _revision=revision)

    def _revision(self, snapshot: PlayerSnapshot) -> _Revision:
        revision = self._active.get(snapshot.player_id)
        if revision is None or revision is not snapshot._revision:
            raise StoreRejected("SAVE_PLAYER_STALE_OWNER")
        if revision.save_seq != snapshot.save_seq:
            raise StoreRejected("SAVE_PLAYER_OLDER_SEQ")
        return revision

    async def release(self, snapshot: PlayerSnapshot) -> PlayerSnapshot:
        return await self.commit(snapshot, snapshot.rows, release=True)

    def _changes(
        self,
        snapshot: PlayerSnapshot,
        revision: _Revision,
        rows: Mapping[int, bytes],
        *,
        release: bool,
        force: bool,
        periodic: bool,
    ) -> dict[int, bytes] | None:
        changes = {
            kind: rows.get(kind, b"")
            for kind in sorted(set(rows) | set(snapshot.rows))
            if snapshot.rows.get(kind, b"") != rows.get(kind, b"")
        }
        if release or force or snapshot.released or not snapshot.has_saved:
            return changes
        if not changes:
            revision.deferred_since = None
            return None
        if periodic and set(changes) == {PlayerRowKind.LOCATION}:
            now = time.monotonic()
            if revision.deferred_since is None:
                revision.deferred_since = now
            if now - revision.deferred_since < self._piggyback_fallback:
                return None
        return changes

    def _saved(
        self,
        snapshot: PlayerSnapshot,
        revision: _Revision,
        changes: Mapping[int, bytes],
        save_seq: int,
        release: bool,
    ) -> PlayerSnapshot:
        revision.deferred_since = None
        revision.save_seq = save_seq
        return replace(
            snapshot,
            rows={**snapshot.rows, **changes},
            save_seq=save_seq,
            released=release,
            has_saved=True,
        )


class SqlitePlayerStore(_SnapshotStore):
    """本地配置使用 SQLite 保存相同的行和写权边界。"""

    def __init__(
        self, path: str | Path, *, auto_create: bool = True, piggyback_fallback: float = 300.0
    ) -> None:
        super().__init__(piggyback_fallback=piggyback_fallback)
        self._path = Path(path)
        if self._path == Path(":memory:"):
            raise ValueError("SQLite 本地档需要持久化文件路径")
        self._auto_create = auto_create

    def _connect(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._path, timeout=10.0, isolation_level=None)
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS player_owner (
                    player_id TEXT PRIMARY KEY, owner_epoch TEXT NOT NULL,
                    holder BLOB NOT NULL, save_seq TEXT NOT NULL,
                    released INTEGER NOT NULL, schema_gen INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS player_row (
                    player_id TEXT NOT NULL, kind INTEGER NOT NULL, payload BLOB NOT NULL,
                    PRIMARY KEY (player_id, kind)
                )
                """
            )
        except BaseException:
            connection.close()
            raise
        return connection

    async def load_and_claim(
        self, player_id: int, owner: bytes, owner_epoch: int = 0
    ) -> PlayerSnapshot:
        _check_owner(player_id, owner, owner_epoch, allow_zero=True)
        async with self._lock(player_id):
            try:
                snapshot = await asyncio.to_thread(self._claim, player_id, owner, owner_epoch)
            except (sqlite3.Error, OSError) as exc:
                raise StoreUnavailable("SQLite 玩家认领暂不可用") from exc
            return self._claimed(snapshot)

    async def release_detached(self, player_id: int, owner: bytes, owner_epoch: int) -> bool:
        _check_owner(player_id, owner, owner_epoch)
        async with self._lock(player_id):
            try:
                await asyncio.to_thread(self._release_detached, player_id, owner, owner_epoch)
            except (sqlite3.Error, OSError, StoreRejected):
                return False
            return True

    def _release_detached(self, player_id: int, owner: bytes, owner_epoch: int) -> None:
        key = str(player_id)
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT owner_epoch, holder, save_seq, schema_gen FROM player_owner WHERE player_id = ?",
                (key,),
            ).fetchone()
            if current is None:
                return
            epoch, holder, sequence, generation = current
            if owner_epoch < int(epoch) or holder != owner:
                return
            if generation != 1:
                raise StoreRejected("SAVE_PLAYER_SCHEMA_REGRESSED")
            connection.execute(
                "UPDATE player_owner SET owner_epoch = ?, save_seq = ?, released = 1 WHERE player_id = ?",
                (str(owner_epoch), str(_next(int(sequence))), key),
            )

    def _claim(self, player_id: int, owner: bytes, owner_epoch: int) -> PlayerSnapshot:
        key = str(player_id)
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT owner_epoch, holder, save_seq, schema_gen
                FROM player_owner WHERE player_id = ?
                """,
                (key,),
            ).fetchone()
            if row is None:
                if not self._auto_create:
                    raise StoreRejected("LOAD_AND_CLAIM_NO_OWNER")
                epoch = owner_epoch or 1
                connection.execute(
                    "INSERT INTO player_owner VALUES (?, ?, ?, '0', 0, 1)",
                    (key, str(epoch), owner),
                )
                return PlayerSnapshot(player_id, owner, epoch, 0, {})
            stored_epoch, holder, save_seq, schema_gen = row
            stored_epoch = int(stored_epoch)
            epoch = owner_epoch or (stored_epoch if holder == owner else _next(stored_epoch))
            if epoch < stored_epoch:
                raise StoreRejected("LOAD_AND_CLAIM_STALE_OWNER")
            if epoch == stored_epoch and holder and holder != owner:
                raise StoreRejected("LOAD_AND_CLAIM_NOT_HOLDER")
            if schema_gen > 1:
                raise StoreRejected("LOAD_AND_CLAIM_SCHEMA_TOO_NEW")
            rows = dict(
                connection.execute(
                    "SELECT kind, payload FROM player_row WHERE player_id = ?", (key,)
                ).fetchall()
            )
            rows = _validate_rows(rows)
            connection.execute(
                "UPDATE player_owner SET owner_epoch = ?, holder = ?, released = 0 WHERE player_id = ?",
                (str(epoch), owner, key),
            )
            return PlayerSnapshot(player_id, owner, epoch, int(save_seq), rows, schema_gen=schema_gen)

    async def commit(
        self,
        snapshot: PlayerSnapshot,
        rows: Mapping[int, bytes],
        *,
        release: bool = False,
        force: bool = False,
        periodic: bool = False,
    ) -> PlayerSnapshot:
        rows = _validate_rows(rows)
        _check_owner(snapshot.player_id, snapshot.owner, snapshot.owner_epoch)
        async with self._lock(snapshot.player_id):
            revision = self._revision(snapshot)
            changes = self._changes(
                snapshot, revision, rows, release=release, force=force, periodic=periodic
            )
            if changes is None:
                return snapshot
            _check_save_size(changes)
            save_seq = _next(snapshot.save_seq)
            try:
                await asyncio.to_thread(self._write, snapshot, changes, save_seq, release)
            except (sqlite3.Error, OSError) as exc:
                raise StoreUnavailable("SQLite 玩家存档未获确认") from exc
            return self._saved(snapshot, revision, changes, save_seq, release)

    def _write(
        self, snapshot: PlayerSnapshot, changes: Mapping[int, bytes], save_seq: int, release: bool
    ) -> None:
        key = str(snapshot.player_id)
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT owner_epoch, holder, save_seq, schema_gen, released
                FROM player_owner WHERE player_id = ?
                """,
                (key,),
            ).fetchone()
            if row is None:
                raise StoreRejected("SAVE_PLAYER_NO_OWNER")
            epoch, holder, old_seq, schema_gen, was_released = row
            if snapshot.owner_epoch < int(epoch):
                raise StoreRejected("SAVE_PLAYER_STALE_OWNER")
            if holder != snapshot.owner:
                result = "SAVE_PLAYER_NOT_HOLDER" if snapshot.owner_epoch == int(epoch) else "SAVE_PLAYER_UNCLAIMED"
                raise StoreRejected(result)
            if save_seq == int(old_seq):
                # 同一序号只能重放同一批内容，拒绝过期快照带来的序号碰撞。
                existing = dict(connection.execute(
                    "SELECT kind, payload FROM player_row WHERE player_id = ?", (key,)
                ).fetchall())
                expected = {**snapshot.rows, **changes}
                if existing == expected and bool(was_released) == release:
                    return
            if save_seq <= int(old_seq):
                raise StoreRejected("SAVE_PLAYER_OLDER_SEQ")
            if snapshot.schema_gen != schema_gen:
                raise StoreRejected("SAVE_PLAYER_SCHEMA_REGRESSED")
            connection.executemany(
                """
                INSERT INTO player_row (player_id, kind, payload) VALUES (?, ?, ?)
                ON CONFLICT (player_id, kind) DO UPDATE SET payload = excluded.payload
                """,
                ((key, kind, payload) for kind, payload in changes.items()),
            )
            connection.execute(
                "UPDATE player_owner SET owner_epoch = ?, save_seq = ?, released = ? WHERE player_id = ?",
                (str(snapshot.owner_epoch), str(save_seq), int(release), key),
            )


class DataServicePlayerStore(_SnapshotStore):
    """通过签名 gRPC 请求调用 Go DB 节点上的 PlayerStore。"""

    def __init__(
        self,
        protocol,
        call: Callable[[str, bytes], Awaitable[bytes]],
        secret: str | bytes,
        *,
        schema_gen: int = 1,
        retry_budget: float = 30.0,
        piggyback_fallback: float = 300.0,
    ) -> None:
        super().__init__(piggyback_fallback=piggyback_fallback)
        self._protocol = protocol
        self._call = call
        self._secret = (secret.encode() if isinstance(secret, str) else secret).strip()
        if len(self._secret) < 32:
            raise ValueError("MMORPG_PLAYER_STORE_SECRET_SCENE 至少需要 32 字节")
        if retry_budget <= 0:
            raise ValueError("retry_budget 必须大于 0")
        if not 1 <= schema_gen <= (1 << 32) - 1:
            raise ValueError("schema_gen 必须是非零 uint32")
        self._schema_gen = schema_gen
        self._retry_budget = retry_budget
        self._unsettled: dict[int, bytes] = {}
        self._issued_seq = 0

    async def load_and_claim(
        self, player_id: int, owner: bytes, owner_epoch: int = 0
    ) -> PlayerSnapshot:
        _check_owner(player_id, owner, owner_epoch)
        async with self._lock(player_id):
            if player_id in self._unsettled:
                raise SaveInDoubt("玩家仍有结果未明的存盘")
            request = self._protocol.message(
                "playerstore.LoadAndClaimRequest",
                player_id=player_id,
                owner_epoch=owner_epoch,
                instance_id=owner,
                binary_schema_gen=self._schema_gen,
            )
            result = await self._request("LoadAndClaim", request)
            self._check_result(result, {"LOAD_AND_CLAIM_CLAIMED"})
            if (
                result.owner_epoch != owner_epoch or result.storage_id == 0
                or not 1 <= result.schema_gen <= self._schema_gen
            ):
                raise StoreUnavailable("认领响应的写权代次、数据世代或落点无效")
            rows: dict[int, bytes] = {}
            for row in result.rows:
                if row.kind in rows:
                    raise StoreUnavailable("认领响应含重复玩家数据行")
                rows[row.kind] = row.payload
            return self._claimed(PlayerSnapshot(
                player_id, owner, result.owner_epoch, result.save_seq, _validate_rows(rows),
                result.storage_id, result.schema_gen,
            ))

    async def release_detached(self, player_id: int, owner: bytes, owner_epoch: int) -> bool:
        _check_owner(player_id, owner, owner_epoch)
        async with self._lock(player_id):
            if player_id in self._unsettled or self._issued_seq == _MAX_UINT64:
                return False
            self._issued_seq = max(time.time_ns() // 1000, self._issued_seq + 1)
            request = self._protocol.message(
                "playerstore.SavePlayerRequest", player_id=player_id, owner_epoch=owner_epoch,
                instance_id=owner, save_seq=self._issued_seq, schema_gen=self._schema_gen, release=True,
            )
            path = f"{_STORE}/SavePlayer"
            try:
                # 无实体释放由下一次 LeaveScene 重试，本调用只发送一遍。
                async with asyncio.timeout(min(5.0, self._retry_budget)):
                    body = await self._call(path, _sign_request(request, path, self._secret))
                result = self._protocol.parse("playerstore.SavePlayerResponse", body)
            except (OSError, DecodeError):
                return False
            except Exception as exc:
                if callable(getattr(exc, "code", None)):
                    return False
                raise
            enum = result.DESCRIPTOR.fields_by_name["result"].enum_type
            value = enum.values_by_number.get(result.result)
            return value is not None and value.name in _DETACHED_SETTLED

    async def commit(
        self,
        snapshot: PlayerSnapshot,
        rows: Mapping[int, bytes],
        *,
        release: bool = False,
        force: bool = False,
        periodic: bool = False,
    ) -> PlayerSnapshot:
        rows = _validate_rows(rows)
        _check_owner(snapshot.player_id, snapshot.owner, snapshot.owner_epoch)
        async with self._lock(snapshot.player_id):
            revision = self._revision(snapshot)
            previous = self._unsettled.get(snapshot.player_id)
            changes = self._changes(
                snapshot, revision, rows,
                release=release, force=force or previous is not None, periodic=periodic,
            )
            if changes is None:
                return snapshot
            request = self._protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=snapshot.player_id,
                storage_id=snapshot.storage_id,
                owner_epoch=snapshot.owner_epoch,
                schema_gen=self._schema_gen,
                instance_id=snapshot.owner,
                release=release,
            )
            for kind, payload in changes.items():
                request.rows.add(kind=kind, payload=payload)
            if previous is None:
                request.save_seq = max(
                    time.time_ns() // 1000, _next(snapshot.save_seq), _next(self._issued_seq)
                )
                self._issued_seq = request.save_seq
            else:
                sent = self._protocol.parse("playerstore.SavePlayerRequest", previous)
                request.save_seq = sent.save_seq
                if request != sent:
                    raise SaveInDoubt("请先用相同的快照和提交选项重试上一笔存盘")
            if request.ByteSize() > _MAX_WIRE_BYTES:
                raise StoreRejected("SAVE_PLAYER_TOO_LARGE")
            self._unsettled[snapshot.player_id] = request.SerializeToString(deterministic=True)
            try:
                result = await self._request("SavePlayer", request, prior_unknown=previous is not None)
            except StoreRejected:
                self._unsettled.pop(snapshot.player_id, None)
                raise
            except StoreUnavailable as exc:
                raise SaveInDoubt(f"玩家 {snapshot.player_id} 存盘结果待定") from exc
            self._check_result(result, {"SAVE_PLAYER_COMMITTED", "SAVE_PLAYER_EQUAL"}, snapshot.player_id)
            return replace(
                self._saved(snapshot, revision, changes, request.save_seq, release),
                schema_gen=self._schema_gen,
            )

    def _check_result(self, result: Message, accepted: set[str], player_id: int = 0) -> None:
        enum = result.DESCRIPTOR.fields_by_name["result"].enum_type
        value = enum.values_by_number.get(result.result)
        if value is None or result.result == 0:
            error = SaveInDoubt if player_id else StoreUnavailable
            raise error("玩家存储响应结果未知")
        if player_id:
            self._unsettled.pop(player_id, None)
        if value.name not in accepted:
            raise StoreRejected(value.name)

    async def _request(
        self, method: str, request: Message, *, prior_unknown: bool = False
    ) -> Message:
        path = f"{_STORE}/{method}"
        delay = 0.5
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._retry_budget
        try:
            async with asyncio.timeout_at(deadline):
                while True:
                    try:
                        body = _sign_request(request, path, self._secret)
                        reply = await self._call(path, body)
                        response = self._protocol.parse(f"playerstore.{method}Response", reply)
                        enum = response.DESCRIPTOR.fields_by_name["result"].enum_type
                        if response.result and response.result in enum.values_by_number:
                            return response
                    except (OSError, DecodeError):
                        pass
                    except Exception as exc:
                        if not callable(getattr(exc, "code", None)):
                            raise
                        status = exc.code().name
                        if status in {"PERMISSION_DENIED", "UNAUTHENTICATED", "UNIMPLEMENTED"}:
                            if prior_unknown:
                                raise StoreUnavailable(f"{path} 重试被拒，先前请求结果待定") from exc
                            raise StoreRejected(status) from exc
                    prior_unknown = True
                    await asyncio.sleep(random.uniform(delay / 2, delay))
                    delay = min(delay * 2, 5.0)
        except TimeoutError as exc:
            raise StoreUnavailable(f"{path} 超出重试预算") from exc


def _sign_request(request: Message, path: str, secret: bytes) -> bytes:
    signed = type(request)()
    signed.CopyFrom(request)
    signed.ClearField("auth")
    digest = hashlib.sha256(signed.SerializeToString(deterministic=True)).hexdigest()
    now_ms = time.time_ns() // 1_000_000
    content = f"v1\n{path}\nscene\n{now_ms}\n{digest}".encode()
    signed.auth.caller = "scene"
    signed.auth.timestamp_ms = now_ms
    signed.auth.signature_hex = hmac.digest(secret, content, "sha256").hex()
    return signed.SerializeToString(deterministic=True)


def _validate_rows(rows: Mapping[int, bytes]) -> dict[int, bytes]:
    result = dict(rows)
    for kind, payload in result.items():
        if not isinstance(kind, int) or kind not in PlayerRowKind._value2member_map_ or not isinstance(payload, bytes):
            raise ValueError(f"玩家数据行无效: {kind}")
    return result


def _check_save_size(changes: Mapping[int, bytes]) -> None:
    sizes = [len(payload) for payload in changes.values()]
    if sum(sizes) > _MAX_SAVE_BYTES or any(size > _MAX_ROW_BYTES for size in sizes):
        raise StoreRejected("SAVE_PLAYER_TOO_LARGE")


def _check_owner(player_id: int, owner: bytes, epoch: int, *, allow_zero: bool = False) -> None:
    if type(player_id) is not int or not 1 <= player_id <= _MAX_UINT64:
        raise ValueError("player_id 必须是非零 uint64")
    if not isinstance(owner, bytes) or not 1 <= len(owner) <= 64:
        raise ValueError("owner 必须是 1 至 64 字节")
    if type(epoch) is not int or not int(not allow_zero) <= epoch <= _MAX_UINT64:
        raise ValueError("owner_epoch 超出有效范围")


def _next(value: int) -> int:
    if value >= _MAX_UINT64:
        raise StoreRejected("SAVE_SEQ_EXHAUSTED")
    return value + 1
