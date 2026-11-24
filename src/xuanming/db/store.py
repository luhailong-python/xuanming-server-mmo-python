"""PlayerStore 仓储接口（Repository 模式）与确定性内存实现。

采用 Repository 模式隔离 gRPC 服务层与 MySQL/TiDB 存储实现：
- 生产环境注入 ``SqlPlayerStore``；
- 单元测试注入 ``MemoryPlayerStore``，以完全相同的围栏判定与审计组装逻辑实现无外部依赖回归验证。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol
from weakref import WeakValueDictionary

from .model import (
    DEFAULT_KNOWN_GEN,
    ClaimOutcome,
    ClaimResult,
    OwnerRow,
    SaveLimits,
    SaveOutcome,
    SaveResult,
    StorageUnavailableError,
    audit_shard_of,
    decide_claim,
    decide_save,
    prepare_snapshot_records,
    prepare_transaction_log_records,
    validate_claim_request,
    validate_save_request,
)


class RouteResolver(Protocol):
    """按 player_id 解析玩家所属地域存档库编号（storage_id）。"""

    async def resolve_storage(self, player_id: int) -> int:
        """返回非零 ``storage_id``；不存在或下游不可用时抛出 ``StorageUnavailableError``。"""


class PlayerStoreBackend(Protocol):
    """玩家存档库事务接口。"""

    async def load_and_claim(
        self,
        storage_id: int,
        player_id: int,
        owner_epoch: int,
        instance_id: bytes,
        binary_schema_gen: int,
    ) -> ClaimOutcome:
        """在同一事务内锁定写权行、执行认领判定、递增 ``claim_seq`` 并读出全部数据行。"""

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
        """在同一事务内锁定写权行、执行存盘围栏判定、写入脏行与可选审计发件箱。"""


@dataclass(slots=True)
class MemoryOwnerRecord:
    """内存仓储中的写权行状态（含 ``released`` 与 ``claim_seq`` 供单测断言）。"""

    owner_epoch: int
    save_seq: int
    schema_gen: int
    holder: bytes
    released: int
    claim_seq: int


@dataclass(frozen=True, slots=True)
class MemoryAuditRow:
    """内存仓储中的 ``player_audit_outbox`` 行快照。"""

    shard: int
    kind: int
    created_ms: int
    player_id: int
    record_id: int
    save_seq: int
    payload: bytes


class _MemoryStorage:
    __slots__ = ("owners", "rows", "audit_outbox")

    def __init__(self) -> None:
        self.owners: dict[int, MemoryOwnerRecord] = {}
        self.rows: dict[int, dict[int, bytes]] = {}
        self.audit_outbox: dict[tuple[int, int, int, int, int], MemoryAuditRow] = {}


class MemoryPlayerStore:
    """``PlayerStoreBackend`` 的内存孪生实现，用于确定性单测。"""

    def __init__(
        self,
        protocol,
        *,
        known_gen: int = DEFAULT_KNOWN_GEN,
        limits: SaveLimits | None = None,
        available_storages: Sequence[int] | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._protocol = protocol
        self._known_gen = known_gen
        self._limits = limits or SaveLimits()
        self._limits.validate()
        self._available = None if available_storages is None else set(available_storages)
        self._clock_ms = clock_ms
        self._storages: dict[int, _MemoryStorage] = {}
        self._locks: WeakValueDictionary[tuple[int, int], asyncio.Lock] = WeakValueDictionary()

    def _lock(self, storage_id: int, player_id: int) -> asyncio.Lock:
        key = (storage_id, player_id)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def _storage(self, storage_id: int) -> _MemoryStorage:
        if self._available is not None and storage_id not in self._available:
            raise StorageUnavailableError(f"storage_id={storage_id} is not available")
        bucket = self._storages.get(storage_id)
        if bucket is None:
            bucket = _MemoryStorage()
            self._storages[storage_id] = bucket
        return bucket

    def seed_player(
        self,
        storage_id: int,
        player_id: int,
        *,
        owner_epoch: int = 1,
        save_seq: int = 0,
        schema_gen: int = 1,
        holder: bytes = b"",
        released: int = 1,
        claim_seq: int = 0,
        rows: Mapping[int, bytes] | None = None,
    ) -> None:
        """为测试预置角色初始写权行与数据行（模拟 ``CreateAccountRole`` 建角落盘）。"""
        bucket = self._storages.setdefault(storage_id, _MemoryStorage())
        bucket.owners[player_id] = MemoryOwnerRecord(
            owner_epoch=owner_epoch,
            save_seq=save_seq,
            schema_gen=schema_gen,
            holder=bytes(holder),
            released=int(released),
            claim_seq=claim_seq,
        )
        bucket.rows[player_id] = {int(k): bytes(v) for k, v in (rows or {}).items()}

    def get_owner(self, storage_id: int, player_id: int) -> MemoryOwnerRecord | None:
        bucket = self._storages.get(storage_id)
        return None if bucket is None else bucket.owners.get(player_id)

    def get_rows(self, storage_id: int, player_id: int) -> dict[int, bytes]:
        bucket = self._storages.get(storage_id)
        if bucket is None:
            return {}
        return dict(bucket.rows.get(player_id, {}))

    def get_audit_rows(self, storage_id: int, player_id: int | None = None) -> list[MemoryAuditRow]:
        bucket = self._storages.get(storage_id)
        if bucket is None:
            return []
        items = list(bucket.audit_outbox.values())
        if player_id is not None:
            items = [row for row in items if row.player_id == player_id]
        items.sort(key=lambda r: (r.shard, r.kind, r.created_ms, r.player_id, r.record_id))
        return items

    async def load_and_claim(
        self,
        storage_id: int,
        player_id: int,
        owner_epoch: int,
        instance_id: bytes,
        binary_schema_gen: int,
    ) -> ClaimOutcome:
        bucket = self._storage(storage_id)
        invalid = validate_claim_request(player_id, owner_epoch, instance_id)
        if invalid != ClaimResult.UNSPECIFIED:
            return ClaimOutcome(result=invalid)
        async with self._lock(storage_id, player_id):
            record = bucket.owners.get(player_id)
            row = (
                None
                if record is None
                else OwnerRow(record.owner_epoch, record.save_seq, record.schema_gen, record.holder)
            )
            decision = decide_claim(row, owner_epoch, instance_id, binary_schema_gen, self._known_gen)
            if decision != ClaimResult.CLAIMED:
                if row is None:
                    return ClaimOutcome(result=decision)
                return ClaimOutcome(
                    result=decision,
                    owner_epoch=row.owner_epoch,
                    save_seq=row.save_seq,
                    schema_gen=row.schema_gen,
                )
            assert record is not None
            record.owner_epoch = owner_epoch
            record.holder = bytes(instance_id)
            record.released = 0
            record.claim_seq += 1
            data_rows = tuple(sorted(bucket.rows.get(player_id, {}).items()))
            return ClaimOutcome(
                result=ClaimResult.CLAIMED,
                owner_epoch=owner_epoch,
                save_seq=record.save_seq,
                schema_gen=record.schema_gen,
                rows=data_rows,
            )

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
        bucket = self._storage(storage_id)
        invalid = validate_save_request(player_id, owner_epoch, save_seq, instance_id, rows, self._limits)
        if invalid != SaveResult.UNSPECIFIED:
            return SaveOutcome(result=invalid)
        async with self._lock(storage_id, player_id):
            record = bucket.owners.get(player_id)
            row = (
                None
                if record is None
                else OwnerRow(record.owner_epoch, record.save_seq, record.schema_gen, record.holder)
            )
            decision = decide_save(
                row, owner_epoch, save_seq, schema_gen, len(rows) > 0, instance_id, self._known_gen
            )
            if decision != SaveResult.COMMITTED:
                return SaveOutcome(
                    result=decision,
                    row_owner_epoch=0 if row is None else row.owner_epoch,
                    row_save_seq=0 if row is None else row.save_seq,
                    audit_outbox_written=(decision == SaveResult.EQUAL),
                )
            assert record is not None
            record.owner_epoch = owner_epoch
            record.save_seq = save_seq
            # 与 SQL 的 GREATEST(schema_gen, VALUES(schema_gen)) 保持一致：写权行世代只升不降。
            record.schema_gen = max(record.schema_gen, schema_gen)
            record.holder = bytes(instance_id)
            record.released = 1 if release else 0
            player_rows = bucket.rows.setdefault(player_id, {})
            for kind, payload in rows:
                player_rows[int(kind)] = bytes(payload)

            if transaction_logs or audit_snapshots:
                now_ms = int(self._clock_ms())
                audit_records = prepare_transaction_log_records(player_id, save_seq, transaction_logs)
                if audit_snapshots:
                    sorted_rows = sorted(player_rows.items())
                    audit_records.extend(
                        prepare_snapshot_records(
                            self._protocol,
                            player_id,
                            save_seq,
                            schema_gen,
                            audit_snapshots,
                            sorted_rows,
                            now_ms // 1000,
                        )
                    )
                shard = audit_shard_of(player_id)
                for item in audit_records:
                    key = (shard, item.kind, now_ms, player_id, item.record_id)
                    # 与 SQL 的 ON DUPLICATE KEY UPDATE record_id=record_id 一致：主键冲突保留原行。
                    bucket.audit_outbox.setdefault(
                        key,
                        MemoryAuditRow(
                            shard=shard,
                            kind=item.kind,
                            created_ms=now_ms,
                            player_id=player_id,
                            record_id=item.record_id,
                            save_seq=save_seq,
                            payload=item.payload,
                        ),
                    )
            return SaveOutcome(
                result=SaveResult.COMMITTED,
                row_owner_epoch=owner_epoch,
                row_save_seq=save_seq,
                audit_outbox_written=True,
            )
