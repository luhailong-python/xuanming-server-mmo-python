"""friend 核心存储引擎、出站 data_service 适配协议与内存双胞胎（MemoryFriendStore）。

本模块收敛全部写事务锁序与领域不变量，由 ``SqlFriendStore`` 与 ``MemoryFriendStore`` 共享：
- 全局锁序：``role_fence``（按 player_id 升序 FOR UPDATE）-> ``friend_capacity``（升序 FOR UPDATE）-> 单行主键点锁；
- 角色护栏自愈：事务内发现缺失/修订落后/冻结时回滚并在事务外经 ``GetRoleFenceState`` 自愈再试一次；
- ``request_id`` 两阶段领号：事务外调用 ``AllocateIdSegment("friend_request")`` + ``mix_guid``，绝不持容量锁等网络。
"""

from __future__ import annotations

import asyncio
import copy
import logging
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Protocol as TypingProtocol, TypeVar

from google.protobuf.message import Message

from ..data.id_segments import IdSegments
from ..grpc_runtime import sign_service_call
from ..protocol import Protocol
from .errors import (
    AcceptorFriendsFullError,
    AlreadyFriendsError,
    BlockListFullError,
    BlockedError,
    FriendError,
    InvalidRoleFenceStateError,
    OriginalRoleError,
    RequestNotFoundError,
    RoleFenceUnavailableError,
    SenderFriendsFullError,
    StoreUnavailableError,
    TargetInboxFullError,
    TooManyPendingError,
)
from .model import (
    ActorIdentity,
    AddFriendLimits,
    BlockEffect,
    BlockEntry,
    FriendEntry,
    FriendRequestEntry,
    FriendRules,
    PlayerBrief,
    RECOMMEND_ANCHOR_WINDOW,
    REQUEST_ID_BIZ_TAG,
    REQUEST_STATUS_ACCEPTED,
    REQUEST_STATUS_PENDING,
    REQUEST_STATUS_REJECTED,
    ROLE_FENCE_TIMEOUT_SEC,
    RecommendCandidate,
    RequestChange,
    RoleFenceExpect,
    RoleFenceRow,
    ascending_unique_ids,
    friend_list_cache_key,
    pending_requests_cache_key,
    valid_account,
    valid_role_fence_state,
)
from .redis_client import CacheInvalidator

logger = logging.getLogger(__name__)

_DATA_SERVICE_PREFIX = "/data_service.DataService/"
_ROLE_FENCE_CALLER = "friend"
CAPACITY_GUARD_MAX_ATTEMPTS = 3

T = TypeVar("T")


class _TxRollbackReturn(Exception):
    """在事务块内显式回滚并返回结果的内部控制流信号（只读事务或幂等无变化）。"""

    def __init__(self, value) -> None:
        super().__init__("rollback_return")
        self.value = value


class _RoleFenceStale(Exception):
    """副本行缺失、修订号落后或处于冻结，需回滚后在事务外自愈再试一次。"""

    def __init__(self, players: tuple[int, ...]) -> None:
        super().__init__("role_fence_stale")
        self.players = players


class _CapacityRowsMissing(Exception):
    """容量守卫缺行（如与 sweep 回收竞态），需回滚后重新 ensure 并有界重试。"""


class _RequestIdRequired(Exception):
    """需要向发号器申请新的 request_id：先回滚纯判定事务，在事务外领号后重试。"""


class FriendDataServiceClient(TypingProtocol):
    """出站 data_service 依赖协议（生产 gRPC 与单测替身共用）。"""

    async def get_role_fence_state(self, player_id: int) -> Message | None: ...

    async def allocate_request_id(self) -> int: ...

    async def batch_get_player_display(self, player_ids: Sequence[int]) -> dict[int, PlayerBrief]: ...

    async def batch_get_player_name(self, player_ids: Sequence[int]) -> dict[int, str]: ...

    async def batch_get_player_home_zone(self, player_ids: Sequence[int]) -> dict[int, int]: ...


class GrpcFriendDataServiceClient:
    """生产出站 data_service 客户端：GetRoleFenceState 以 friend 签名，发号走 friend_request + mix_guid。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        role_fence_secret: bytes,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        id_segment_size: int = 256,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._role_fence_secret = role_fence_secret
        self._clock_ms = clock_ms
        self._request_ids = IdSegments.remote(protocol, call, REQUEST_ID_BIZ_TAG)
        self._request_ids.size = id_segment_size

    async def get_role_fence_state(self, player_id: int) -> Message | None:
        path = _DATA_SERVICE_PREFIX + "GetRoleFenceState"
        req = self._protocol.message("data_service.GetRoleFenceStateRequest", player_id=player_id)
        sign_service_call(path, req, _ROLE_FENCE_CALLER, self._clock_ms(), self._role_fence_secret)
        try:
            raw = await self._call(path, req.SerializeToString(deterministic=True), timeout=ROLE_FENCE_TIMEOUT_SEC)
        except Exception as exc:
            raise RoleFenceUnavailableError(f"GetRoleFenceState failed player={player_id}: {exc}") from exc
        resp = self._protocol.parse("data_service.GetRoleFenceStateResponse", raw)
        if resp.retry:
            raise RoleFenceUnavailableError(f"GetRoleFenceState requested retry player={player_id}")
        if not resp.found:
            return None
        state = resp.state
        if int(state.player_id) != player_id or not valid_role_fence_state(state):
            raise RoleFenceUnavailableError(f"GetRoleFenceState returned invalid state player={player_id}")
        return state

    async def allocate_request_id(self) -> int:
        try:
            value = int(await self._request_ids())
        except Exception as exc:
            raise StoreUnavailableError(f"AllocateIdSegment(friend_request) failed: {exc}") from exc
        if value <= 0:
            raise StoreUnavailableError("AllocateIdSegment(friend_request) returned zero id")
        return value

    async def batch_get_player_display(self, player_ids: Sequence[int]) -> dict[int, PlayerBrief]:
        ids = [int(pid) for pid in dict.fromkeys(player_ids) if int(pid) > 0]
        if not ids:
            return {}
        req = self._protocol.message("data_service.BatchGetPlayerDisplayRequest", player_ids=ids)
        raw = await self._call(
            _DATA_SERVICE_PREFIX + "BatchGetPlayerDisplay", req.SerializeToString(deterministic=True)
        )
        resp = self._protocol.parse("data_service.BatchGetPlayerDisplayResponse", raw)
        return {
            int(pid): PlayerBrief(
                name=entry.name,
                level=int(entry.level),
                class_id=int(entry.class_id),
                gender=int(entry.gender),
                appearance_id=entry.appearance_id,
            )
            for pid, entry in resp.players.items()
        }

    async def batch_get_player_name(self, player_ids: Sequence[int]) -> dict[int, str]:
        ids = [int(pid) for pid in dict.fromkeys(player_ids) if int(pid) > 0]
        if not ids:
            return {}
        req = self._protocol.message("data_service.BatchGetPlayerNameRequest", player_ids=ids)
        raw = await self._call(
            _DATA_SERVICE_PREFIX + "BatchGetPlayerName", req.SerializeToString(deterministic=True)
        )
        resp = self._protocol.parse("data_service.BatchGetPlayerNameResponse", raw)
        return {int(pid): str(name) for pid, name in resp.names.items()}

    async def batch_get_player_home_zone(self, player_ids: Sequence[int]) -> dict[int, int]:
        ids = [int(pid) for pid in dict.fromkeys(player_ids) if int(pid) > 0]
        if not ids:
            return {}
        req = self._protocol.message("data_service.BatchGetPlayerHomeZoneRequest", player_ids=ids)
        raw = await self._call(
            _DATA_SERVICE_PREFIX + "BatchGetPlayerHomeZone", req.SerializeToString(deterministic=True)
        )
        resp = self._protocol.parse("data_service.BatchGetPlayerHomeZoneResponse", raw)
        return {int(pid): int(zone) for pid, zone in resp.player_zone_map.items()}


class FriendTxHandle(TypingProtocol):
    """单事务内底层存储原语协议（SqlDriver 与 MemoryDriver 共享）。"""

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None: ...

    async def apply_role_fence(self, state: Message, now_ms: int) -> bool: ...

    async def count_authoritative_friends(self, player_id: int) -> int: ...

    async def ensure_capacity_row(self, player_id: int, count: int, now_ms: int) -> None: ...

    async def lock_capacity_row(self, player_id: int) -> int | None: ...

    async def lock_block_row_exists(self, player_id: int, blocked_player_id: int) -> bool: ...

    async def lock_friend_edge_exists(self, player_id: int, friend_player_id: int) -> bool: ...

    async def lock_request_row(self, from_player_id: int, to_player_id: int) -> tuple[int, int, int] | None: ...

    async def count_outgoing_pending(self, from_player_id: int) -> int: ...

    async def count_incoming_pending(self, to_player_id: int) -> int: ...

    async def upsert_pending_request(
        self, from_player_id: int, to_player_id: int, now_ms: int, request_id: int
    ) -> None: ...

    async def backfill_pending_request_id(self, from_player_id: int, to_player_id: int, request_id: int) -> int: ...

    async def find_request_by_id(self, request_id: int) -> list[tuple[int, int]]: ...

    async def update_request_status_cas(
        self, from_player_id: int, to_player_id: int, new_status: int, now_ms: int, request_id: int
    ) -> int: ...

    async def resolve_reverse_pending(
        self, from_player_id: int, to_player_id: int, new_status: int, now_ms: int
    ) -> int: ...

    async def insert_friend_edge_ignore(self, player_id: int, friend_player_id: int, now_ms: int) -> int: ...

    async def increment_friend_count(self, player_id: int) -> None: ...

    async def delete_friend_edge(self, player_id: int, friend_player_id: int) -> int: ...

    async def decrement_friend_count(self, player_id: int, now_ms: int) -> int: ...

    async def friend_edge_exists_plain(self, a: int, b: int) -> bool: ...

    async def pair_blocked_either_way_plain(self, a: int, b: int) -> bool: ...

    async def pair_mutual_friends_plain(self, a: int, b: int) -> bool: ...

    async def query_friend_list(self, player_id: int, limit: int) -> list[FriendEntry]: ...

    async def query_pending_requests(self, player_id: int, limit: int) -> list[FriendRequestEntry]: ...

    async def probe_block_count(self, player_id: int, target_player_id: int) -> tuple[int, int]: ...

    async def count_blocks(self, player_id: int) -> int: ...

    async def insert_block_odku(self, player_id: int, target_player_id: int, now_ms: int) -> None: ...

    async def delete_block(self, player_id: int, target_player_id: int) -> int: ...

    async def query_blocks(self, player_id: int, limit: int) -> list[BlockEntry]: ...

    async def query_recommend_mutual(
        self, player_id: int, exclude: Sequence[int], limit: int
    ) -> list[RecommendCandidate]: ...

    async def query_friend_id_range(self) -> tuple[int | None, int | None]: ...

    async def query_recommend_anchor(
        self, player_id: int, exclude: Sequence[int], pivot: int, limit: int
    ) -> list[RecommendCandidate]: ...

    async def query_purge_peers(self, player_id: int) -> list[int]: ...

    async def delete_request_row(self, from_player_id: int, to_player_id: int) -> None: ...

    async def delete_capacity_row(self, player_id: int) -> None: ...


class FriendDriver(TypingProtocol):
    """事务运行器协议（SqlDriver 与 MemoryDriver 共享）。"""

    async def run_tx(self, work: Callable[[FriendTxHandle], Awaitable[T]]) -> T: ...

    async def apply_role_fence_direct(self, state: Message, now_ms: int) -> bool: ...


def _dedupe_positive_ids(ids: Sequence[int]) -> list[int]:
    """去重并校验全为正整数玩家 ID。"""
    seen: set[int] = set()
    out: list[int] = []
    for raw in ids:
        pid = int(raw)
        if pid <= 0:
            raise ValueError("player id 0 not allowed")
        if pid in seen:
            continue
        seen.add(pid)
        out.append(pid)
    return out


def _check_role_fence(row: RoleFenceRow | None, expect: RoleFenceExpect) -> str:
    """与 Go rolefence.Check 一致：返回 ``ok`` / ``stale`` / ``reject``。"""
    if row is None:
        return "stale"
    if expect.owner_revision != 0:
        if row.owner_revision > expect.owner_revision:
            return "reject"
        if row.owner_revision < expect.owner_revision:
            return "stale"
        if row.account != expect.account:
            return "reject"
    if row.freeze_token != 0 and row.freeze_token != expect.allow_token:
        return "stale"
    if row.account == "":
        return "reject"
    return "ok"


async def _blocked_either_way(tx: FriendTxHandle, a: int, b: int) -> bool:
    """两次独立完整主键点锁（禁止 OR 扫描二级索引，与 Go blockedEitherWay 一致）。"""
    for left, right in ((a, b), (b, a)):
        if await tx.lock_block_row_exists(left, right):
            return True
    return False


async def _friend_edge_exists_for_update(tx: FriendTxHandle, a: int, b: int) -> bool:
    """两次独立完整主键点锁（与 Go friendEdgeExistsForUpdate 一致）。"""
    for left, right in ((a, b), (b, a)):
        if await tx.lock_friend_edge_exists(left, right):
            return True
    return False


async def _delete_friend_edges(tx: FriendTxHandle, a: int, b: int, now_ms: int) -> int:
    """删除双向好友边，并按实际删除行数减 friend_count 同时刷新 created_ms（与 Go deleteFriendEdges 一致）。"""
    removed = 0
    for left, right in ((a, b), (b, a)):
        deleted = await tx.delete_friend_edge(left, right)
        removed += deleted
        if deleted == 1:
            updated = await tx.decrement_friend_count(left, now_ms)
            if updated == 0:
                logger.error(
                    "friend_count underflow prevented player=%d edge deleted but count already 0",
                    left,
                )
    return removed


class FriendStoreCore:
    """friend 权威存储核心引擎：统一承载全部业务事务与 RoleFenceParticipant。"""

    def __init__(
        self,
        protocol: Protocol,
        driver: FriendDriver,
        data_service: FriendDataServiceClient,
        rules: FriendRules,
        *,
        cache_invalidator: CacheInvalidator | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        rng: random.Random | None = None,
    ) -> None:
        self._protocol = protocol
        self._driver = driver
        self._data_service = data_service
        self._rules = rules
        self._cache_invalidator = cache_invalidator
        self._clock_ms = clock_ms
        self._rng = rng if rng is not None else random.Random()

    @property
    def rules(self) -> FriendRules:
        return self._rules

    @property
    def data_service(self) -> FriendDataServiceClient:
        return self._data_service

    # -- 角色护栏前缀锁与自愈 -----------------------------------------------------------------------

    def _validate_actor(self, actor: ActorIdentity) -> None:
        if actor.player_id <= 0 or actor.owner_revision <= 0 or not valid_account(actor.account):
            raise OriginalRoleError("incomplete actor identity")

    async def _lock_role_prefix(
        self,
        tx: FriendTxHandle,
        actor: ActorIdentity,
        players: Sequence[int],
        absent: set[int],
    ) -> None:
        ordered = ascending_unique_ids(players if players else (actor.player_id,))
        expects: list[RoleFenceExpect] = []
        included = False
        for pid in ordered:
            if pid <= 0:
                raise OriginalRoleError("zero player_id in role fence lock")
            if pid == actor.player_id:
                included = True
                expects.append(
                    RoleFenceExpect(
                        player_id=actor.player_id,
                        account=actor.account,
                        owner_revision=actor.owner_revision,
                    )
                )
            elif pid not in absent:
                expects.append(RoleFenceExpect(player_id=pid))
        if not included:
            raise OriginalRoleError("actor missing from role fence expectations")

        stale_players: list[int] = []
        for expect in expects:
            row = await tx.lock_role_fence(expect.player_id)
            verdict = _check_role_fence(row, expect)
            if verdict == "reject":
                raise OriginalRoleError(f"role fence rejected player={expect.player_id}")
            if verdict == "stale":
                stale_players.append(expect.player_id)
        if stale_players:
            raise _RoleFenceStale(tuple(stale_players))

    async def _heal_roles(self, players: Sequence[int]) -> list[int]:
        """在事务外向 data_service 拉取权威角色状态并写入本库 role_fence，返回目录中不存在的玩家列表。"""
        missing: list[int] = []
        now_ms = self._clock_ms()
        for pid in players:
            state = await self._data_service.get_role_fence_state(pid)
            if state is None:
                missing.append(pid)
                continue
            await self._driver.apply_role_fence_direct(state, now_ms)
        return missing

    async def _run_role_tx(
        self,
        actor: ActorIdentity,
        players: Sequence[int],
        work: Callable[[FriendTxHandle], Awaitable[T]],
        *,
        read_only: bool = False,
    ) -> T:
        """在严格 ``role_fence`` 前缀锁下执行事务；按 Go Guard.RunTx + beginGuardedTx 语义自愈与跳过目录缺席对端。"""
        self._validate_actor(actor)
        absent: set[int] = set()

        async def _attempt() -> T:
            async def _tx_body(tx: FriendTxHandle) -> T:
                await self._lock_role_prefix(tx, actor, players, absent)
                result = await work(tx)
                if read_only:
                    raise _TxRollbackReturn(result)
                return result

            return await self._driver.run_tx(_tx_body)

        healed: set[int] = set()
        for _ in range(2):
            try:
                return await _attempt()
            except _RoleFenceStale as stale:
                to_heal = tuple(pid for pid in stale.players if pid not in healed)
                if not to_heal:
                    raise OriginalRoleError("role fence rejected after heal") from stale
                missing = await self._heal_roles(to_heal)
                healed.update(to_heal)
                if actor.player_id in missing:
                    raise OriginalRoleError(f"actor role not found player={actor.player_id}")
                if missing:
                    absent.update(missing)
        try:
            return await _attempt()
        except _RoleFenceStale as exc:
            raise OriginalRoleError("role fence rejected after heal") from exc

    async def _ensure_bound_capacity(self, actor: ActorIdentity, player_a: int, player_b: int) -> None:
        """在主写事务外独立事务补齐双方 friend_capacity 行（避免与主事务形成 insert-intention 死锁）。"""
        ids = ascending_unique_ids((player_a, player_b))

        async def _work(tx: FriendTxHandle) -> None:
            now_ms = self._clock_ms()
            for pid in ids:
                count = await tx.count_authoritative_friends(pid)
                await tx.ensure_capacity_row(pid, count, now_ms)

        await self._run_role_tx(actor, ids, _work, read_only=False)

    async def _run_guarded_write(
        self,
        actor: ActorIdentity,
        player_a: int,
        player_b: int,
        body: Callable[[FriendTxHandle, dict[int, int]], Awaitable[T]],
    ) -> T:
        """容量守卫写事务唯一骨架：ensure -> (role_fence -> friend_capacity -> body -> commit)，缺行最多 3 遍。"""
        ids = ascending_unique_ids((player_a, player_b))
        last_exc: Exception | None = None
        for attempt in range(1, CAPACITY_GUARD_MAX_ATTEMPTS + 1):
            await self._ensure_bound_capacity(actor, player_a, player_b)

            async def _work(tx: FriendTxHandle) -> T:
                counts: dict[int, int] = {}
                for pid in ids:
                    cnt = await tx.lock_capacity_row(pid)
                    if cnt is None:
                        raise _CapacityRowsMissing(f"friend_capacity missing player={pid}")
                    counts[pid] = cnt
                return await body(tx, counts)

            try:
                return await self._run_role_tx(actor, ids, _work, read_only=False)
            except _CapacityRowsMissing as exc:
                last_exc = exc
                if attempt < CAPACITY_GUARD_MAX_ATTEMPTS:
                    logger.warning(
                        "friend capacity guard row missing; re-ensuring players=(%d,%d) attempt=%d/%d",
                        player_a,
                        player_b,
                        attempt,
                        CAPACITY_GUARD_MAX_ATTEMPTS,
                    )
        raise StoreUnavailableError(f"friend capacity rows missing after retries: {last_exc}")

    async def _run_request_write(
        self,
        actor: ActorIdentity,
        from_id: int,
        to_id: int,
        body: Callable[[FriendTxHandle, dict[int, int], int], Awaitable[T]],
    ) -> T:
        """两阶段领号写事务：先不领号判定，确实需要新号时回滚并在事务外调用 data_service 领号后重试。"""
        candidate = 0
        for _ in range(2):
            try:
                return await self._run_guarded_write(
                    actor, from_id, to_id, lambda tx, counts: body(tx, counts, candidate)
                )
            except _RequestIdRequired:
                candidate = await self._data_service.allocate_request_id()
                if candidate <= 0:
                    raise StoreUnavailableError("friend request id allocator returned zero")
        raise StoreUnavailableError("friend request write still required id after allocation")

    async def _invalidate_caches(self, *keys: str) -> None:
        if self._cache_invalidator is not None and keys:
            await self._cache_invalidator.invalidate_after_commit(*keys)

    async def _find_request(self, actor: ActorIdentity, request_id: int) -> RequestChange:
        """按 request_id 普通索引定位完整主键（严格忽略请求体里的 from_player_id）。"""
        if request_id <= 0 or actor.player_id <= 0:
            raise RequestNotFoundError("invalid request_id or accepter")

        async def _work(tx: FriendTxHandle) -> RequestChange:
            rows = await tx.find_request_by_id(request_id)
            if len(rows) > 1:
                raise StoreUnavailableError(f"duplicate non-zero request_id={request_id}")
            if not rows:
                raise RequestNotFoundError("friend request not found")
            from_id, to_id = rows[0]
            if to_id != actor.player_id or from_id <= 0 or from_id == actor.player_id:
                raise RequestNotFoundError("friend request recipient mismatch")
            return RequestChange(request_id=request_id, from_player_id=from_id, to_player_id=to_id)

        return await self._run_role_tx(actor, (actor.player_id,), _work, read_only=True)

    # -- 业务方法 -----------------------------------------------------------------------------------

    async def create_request(
        self, actor: ActorIdentity, target_player_id: int, limits: AddFriendLimits
    ) -> RequestChange:
        from_id = actor.player_id
        to_id = int(target_player_id)
        if from_id <= 0 or to_id <= 0 or from_id == to_id:
            raise StoreUnavailableError(f"invalid player pair ({from_id}, {to_id})")

        async def _body(tx: FriendTxHandle, counts: dict[int, int], candidate: int) -> RequestChange:
            if await _blocked_either_way(tx, from_id, to_id):
                raise BlockedError("blocked between players")
            if await _friend_edge_exists_for_update(tx, from_id, to_id):
                raise AlreadyFriendsError("already friends")

            existing = await tx.lock_request_row(from_id, to_id)
            if existing is not None:
                existing_status, existing_id, _existing_time = existing
                if existing_status == REQUEST_STATUS_PENDING:
                    if existing_id != 0:
                        return RequestChange(
                            request_id=existing_id, from_player_id=from_id, to_player_id=to_id, changed=False
                        )
                    if candidate == 0:
                        raise _RequestIdRequired()
                    affected = await tx.backfill_pending_request_id(from_id, to_id, candidate)
                    if affected != 1:
                        raise StoreUnavailableError("failed to backfill legacy pending request_id")
                    return RequestChange(
                        request_id=candidate, from_player_id=from_id, to_player_id=to_id, changed=True
                    )

            if limits.max_pending_requests > 0:
                outgoing = await tx.count_outgoing_pending(from_id)
                if outgoing >= limits.max_pending_requests:
                    raise TooManyPendingError("too many outgoing friend requests")
            if limits.max_incoming_requests > 0:
                incoming = await tx.count_incoming_pending(to_id)
                if incoming >= limits.max_incoming_requests:
                    raise TargetInboxFullError("target friend request inbox full")
            if limits.max_friends > 0:
                if counts[from_id] >= limits.max_friends:
                    raise SenderFriendsFullError("sender friend list full")
                if counts[to_id] >= limits.max_friends:
                    raise AcceptorFriendsFullError("acceptor friend list full")

            if candidate == 0:
                raise _RequestIdRequired()
            now_ms = self._clock_ms()
            await tx.upsert_pending_request(from_id, to_id, now_ms, candidate)
            return RequestChange(request_id=candidate, from_player_id=from_id, to_player_id=to_id, changed=True)

        change = await self._run_request_write(actor, from_id, to_id, _body)
        if change.changed:
            await self._invalidate_caches(pending_requests_cache_key(to_id))
        return change

    async def accept_request(self, actor: ActorIdentity, request_id: int, max_friends: int) -> RequestChange:
        found = await self._find_request(actor, request_id)
        from_id, to_id = found.from_player_id, found.to_player_id

        async def _body(tx: FriendTxHandle, counts: dict[int, int]) -> RequestChange:
            row = await tx.lock_request_row(from_id, to_id)
            if row is None:
                raise RequestNotFoundError("friend request missing under lock")
            status, current_id, _req_time = row
            if current_id != request_id:
                raise RequestNotFoundError("friend request generation superseded")
            if status == REQUEST_STATUS_ACCEPTED:
                return RequestChange(
                    request_id=request_id, from_player_id=from_id, to_player_id=to_id, changed=False
                )
            if status != REQUEST_STATUS_PENDING:
                raise RequestNotFoundError("friend request not pending")
            if await _blocked_either_way(tx, from_id, to_id):
                raise BlockedError("blocked between players")
            if max_friends > 0:
                if counts[from_id] >= max_friends:
                    raise SenderFriendsFullError("sender friend list full")
                if counts[to_id] >= max_friends:
                    raise AcceptorFriendsFullError("acceptor friend list full")

            now_ms = self._clock_ms()
            affected = await tx.update_request_status_cas(
                from_id, to_id, REQUEST_STATUS_ACCEPTED, now_ms, request_id
            )
            if affected != 1:
                raise RequestNotFoundError("friend request accept CAS missed")
            # 反向 pending 一并收敛为 accepted
            await tx.resolve_reverse_pending(to_id, from_id, REQUEST_STATUS_ACCEPTED, now_ms)

            for left, right in ((from_id, to_id), (to_id, from_id)):
                inserted = await tx.insert_friend_edge_ignore(left, right, now_ms)
                if inserted > 1:
                    raise StoreUnavailableError(f"unexpected friend edge insert count={inserted}")
                if inserted == 1:
                    await tx.increment_friend_count(left)

            return RequestChange(request_id=request_id, from_player_id=from_id, to_player_id=to_id, changed=True)

        change = await self._run_guarded_write(actor, from_id, to_id, _body)
        if change.changed:
            await self._invalidate_caches(
                friend_list_cache_key(from_id),
                friend_list_cache_key(to_id),
                pending_requests_cache_key(to_id),
                pending_requests_cache_key(from_id),
            )
        return change

    async def reject_request(self, actor: ActorIdentity, request_id: int) -> RequestChange:
        found = await self._find_request(actor, request_id)
        from_id, to_id = found.from_player_id, found.to_player_id

        async def _body(tx: FriendTxHandle, _counts: dict[int, int]) -> RequestChange:
            row = await tx.lock_request_row(from_id, to_id)
            if row is None:
                raise RequestNotFoundError("friend request missing under lock")
            status, current_id, _req_time = row
            if current_id != request_id:
                raise RequestNotFoundError("friend request generation superseded")
            if status == REQUEST_STATUS_REJECTED:
                return RequestChange(
                    request_id=request_id, from_player_id=from_id, to_player_id=to_id, changed=False
                )
            if status != REQUEST_STATUS_PENDING:
                raise RequestNotFoundError("friend request not pending")
            affected = await tx.update_request_status_cas(
                from_id, to_id, REQUEST_STATUS_REJECTED, self._clock_ms(), request_id
            )
            if affected != 1:
                raise RequestNotFoundError("friend request reject CAS missed")
            return RequestChange(request_id=request_id, from_player_id=from_id, to_player_id=to_id, changed=True)

        change = await self._run_guarded_write(actor, from_id, to_id, _body)
        if change.changed:
            await self._invalidate_caches(pending_requests_cache_key(to_id))
        return change

    async def remove_friend(self, actor: ActorIdentity, target_player_id: int) -> bool:
        self._validate_actor(actor)
        me = actor.player_id
        target = int(target_player_id)
        if me == target:
            return False
        if target <= 0:
            raise StoreUnavailableError(f"invalid player pair ({me}, {target})")

        # F2-15：非好友前置普通读快速返回 False，绝不为任意 target 凭空创建 friend_capacity 行。
        async def _probe(tx: FriendTxHandle) -> bool:
            return await tx.friend_edge_exists_plain(me, target)

        exists = await self._run_role_tx(actor, (me,), _probe, read_only=True)
        if not exists:
            return False

        async def _body(tx: FriendTxHandle, _counts: dict[int, int]) -> int:
            return await _delete_friend_edges(tx, me, target, self._clock_ms())

        deleted = await self._run_guarded_write(actor, me, target, _body)
        await self._invalidate_caches(friend_list_cache_key(me), friend_list_cache_key(target))
        return deleted > 0

    async def get_friend_list(self, actor: ActorIdentity) -> list[FriendEntry]:
        async def _work(tx: FriendTxHandle) -> list[FriendEntry]:
            return await tx.query_friend_list(actor.player_id, self._rules.list_read_hard_limit)

        return await self._run_role_tx(actor, (actor.player_id,), _work, read_only=True)

    async def _ensure_pending_request(self, actor: ActorIdentity, from_id: int, to_id: int) -> None:
        async def _body(tx: FriendTxHandle, _counts: dict[int, int], candidate: int) -> None:
            row = await tx.lock_request_row(from_id, to_id)
            if row is None:
                return
            status, existing_id, _req_time = row
            if status != REQUEST_STATUS_PENDING or existing_id != 0:
                return
            if candidate == 0:
                raise _RequestIdRequired()
            affected = await tx.backfill_pending_request_id(from_id, to_id, candidate)
            if affected != 1:
                raise StoreUnavailableError("legacy pending request backfill affected != 1")

        await self._run_request_write(actor, from_id, to_id, _body)

    async def get_pending_requests(self, actor: ActorIdentity) -> list[FriendRequestEntry]:
        async def _query(tx: FriendTxHandle) -> list[FriendRequestEntry]:
            return await tx.query_pending_requests(actor.player_id, self._rules.list_read_hard_limit)

        requests = await self._run_role_tx(actor, (actor.player_id,), _query, read_only=True)
        backfilled = False
        for req in requests:
            if req.request_id == 0:
                await self._ensure_pending_request(actor, req.from_player_id, actor.player_id)
                backfilled = True
        if not backfilled:
            return requests
        # 补号后另取一次完整快照，拒绝返回任何 request_id=0 的混版申请。
        fresh = await self._run_role_tx(actor, (actor.player_id,), _query, read_only=True)
        for req in fresh:
            if req.request_id == 0:
                raise StoreUnavailableError("zero request_id remained after backfill")
        return fresh

    async def block(self, actor: ActorIdentity, target_player_id: int, max_blocks: int) -> BlockEffect:
        me = actor.player_id
        target = int(target_player_id)
        if me <= 0 or target <= 0 or me == target:
            raise StoreUnavailableError(f"invalid player pair ({me}, {target})")

        # 事务外快速名额探针：已满且非重复拉黑直接拒，不创建 friend_capacity 行。
        if max_blocks > 0:
            async def _probe(tx: FriendTxHandle) -> tuple[int, int]:
                return await tx.probe_block_count(me, target)

            blocked, already = await self._run_role_tx(actor, (me,), _probe, read_only=True)
            if already == 0 and blocked >= max_blocks:
                raise BlockListFullError("block list full")

        async def _body(tx: FriendTxHandle, _counts: dict[int, int]) -> BlockEffect:
            now_ms = self._clock_ms()
            already_blocked = await tx.lock_block_row_exists(me, target)
            if not already_blocked:
                if max_blocks > 0:
                    blocked_count = await tx.count_blocks(me)
                    if blocked_count >= max_blocks:
                        raise BlockListFullError("block list full")
                await tx.insert_block_odku(me, target, now_ms)

            removed_edges = await _delete_friend_edges(tx, me, target, now_ms)
            outbound_rejected = await tx.resolve_reverse_pending(me, target, REQUEST_STATUS_REJECTED, now_ms)
            await tx.resolve_reverse_pending(target, me, REQUEST_STATUS_REJECTED, now_ms)
            return BlockEffect(edge_removed=removed_edges > 0, outbound_pending_rejected=outbound_rejected > 0)

        effect = await self._run_guarded_write(actor, me, target, _body)
        await self._invalidate_caches(
            friend_list_cache_key(me),
            friend_list_cache_key(target),
            pending_requests_cache_key(me),
            pending_requests_cache_key(target),
        )
        return effect

    async def unblock(self, actor: ActorIdentity, target_player_id: int) -> None:
        me = actor.player_id
        target = int(target_player_id)
        if me <= 0 or target <= 0 or me == target:
            raise StoreUnavailableError(f"invalid player pair ({me}, {target})")

        async def _work(tx: FriendTxHandle) -> None:
            await tx.delete_block(me, target)

        await self._run_role_tx(actor, (me, target), _work, read_only=False)

    async def list_blocks(self, actor: ActorIdentity) -> list[BlockEntry]:
        async def _work(tx: FriendTxHandle) -> list[BlockEntry]:
            return await tx.query_blocks(actor.player_id, self._rules.list_read_hard_limit)

        return await self._run_role_tx(actor, (actor.player_id,), _work, read_only=True)

    async def recommend_by_mutual(
        self, actor: ActorIdentity, exclude: Sequence[int], limit: int
    ) -> list[RecommendCandidate]:
        async def _work(tx: FriendTxHandle) -> list[RecommendCandidate]:
            return await tx.query_recommend_mutual(actor.player_id, exclude, limit)

        return await self._run_role_tx(actor, (actor.player_id,), _work, read_only=True)

    async def recommend_random(
        self, actor: ActorIdentity, exclude: Sequence[int], limit: int
    ) -> list[RecommendCandidate]:
        async def _range_work(tx: FriendTxHandle) -> tuple[int | None, int | None]:
            return await tx.query_friend_id_range()

        min_id, max_id = await self._run_role_tx(actor, (actor.player_id,), _range_work, read_only=True)
        if min_id is None or max_id is None or max_id == 0:
            return []
        pivot = min_id if max_id <= min_id else self._rng.randint(min_id, max_id)

        async def _anchor_work(tx: FriendTxHandle) -> list[RecommendCandidate]:
            return await tx.query_recommend_anchor(actor.player_id, exclude, pivot, limit)

        return await self._run_role_tx(actor, (actor.player_id,), _anchor_work, read_only=True)

    async def batch_check_social_relations(
        self, side_a: Sequence[int], side_b: Sequence[int]
    ) -> tuple[bool, bool]:
        """内部只读批量关系检查（供 match 生死状校验两队拉黑与好友关系，不加容量守卫、不创建任何行）。"""
        uniq_a = _dedupe_positive_ids(side_a)
        uniq_b = _dedupe_positive_ids(side_b)
        if not uniq_a or not uniq_b or len(uniq_a) > 5 or len(uniq_b) > 5:
            raise ValueError(f"invalid team sizes for social relation check: {len(uniq_a)} vs {len(uniq_b)}")
        if set(uniq_a) & set(uniq_b):
            raise ValueError("overlapping player id across sides")

        async def _work(tx: FriendTxHandle) -> tuple[bool, bool]:
            all_friends = True
            for a in uniq_a:
                for b in uniq_b:
                    if await tx.pair_blocked_either_way_plain(a, b):
                        raise _TxRollbackReturn((True, False))
                    if all_friends and not await tx.pair_mutual_friends_plain(a, b):
                        all_friends = False
            raise _TxRollbackReturn((False, all_friends))

        return await self._driver.run_tx(_work)

    # -- RoleFenceParticipant（NoPending + RolePurge）-----------------------------------------------

    async def prepare_role_freeze(self, state: Message, *, dry_run: bool = False) -> Message:
        if not valid_role_fence_state(state) or int(state.freeze_token) == 0 or bool(state.freeze_final):
            raise InvalidRoleFenceStateError("invalid PrepareRoleFreeze state")
        if dry_run:
            # friend 无挂起全集（NoPending）
            return self._protocol.message("rolefence.PrepareRoleFreezeResponse", clean=True)

        async def _work(tx: FriendTxHandle) -> Message:
            now_ms = self._clock_ms()
            await tx.apply_role_fence(state, now_ms)
            fence = await tx.lock_role_fence(int(state.player_id))
            if fence is None or fence.version != int(state.version) or fence.freeze_token != int(state.freeze_token):
                raise _TxRollbackReturn(
                    self._protocol.message("rolefence.PrepareRoleFreezeResponse", stale=True)
                )
            return self._protocol.message("rolefence.PrepareRoleFreezeResponse", clean=True)

        return await self._driver.run_tx(_work)

    async def apply_role_fence(self, state: Message) -> Message:
        if not valid_role_fence_state(state):
            raise InvalidRoleFenceStateError("invalid ApplyRoleFence state")
        await self._driver.apply_role_fence_direct(state, self._clock_ms())
        return self._protocol.message("rolefence.ApplyRoleFenceResponse")

    async def purge_removed_role(self, state: Message) -> Message:
        if not valid_role_fence_state(state) or state.account != "" or int(state.freeze_token) != 0:
            raise InvalidRoleFenceStateError("PurgeRemovedRole requires tombstone state (empty account, zero token)")

        player_id = int(state.player_id)

        async def _work(tx: FriendTxHandle) -> Message:
            now_ms = self._clock_ms()
            await tx.apply_role_fence(state, now_ms)
            fence = await tx.lock_role_fence(player_id)
            if fence is None or fence.account != "":
                raise StoreUnavailableError(f"role_fence replica is not tombstone player={player_id}")

            peers = ascending_unique_ids(await tx.query_purge_peers(player_id))
            # 与写路径同一锁序：role_fence -> friend_capacity(本角色 + 全部对端升序点锁) -> 主键点删。
            for guard_id in ascending_unique_ids((player_id, *peers)):
                await tx.lock_capacity_row(guard_id)
            for peer in peers:
                await _delete_friend_edges(tx, player_id, peer, now_ms)
                for left, right in ((player_id, peer), (peer, player_id)):
                    await tx.delete_request_row(left, right)
                    await tx.delete_block(left, right)
            await tx.delete_capacity_row(player_id)
            return self._protocol.message("rolefence.PurgeRemovedRoleResponse")

        return await self._driver.run_tx(_work)


# ---------------------------------------------------------------------------
# 确定性内存双胞胎（MemoryFriendStore）
# ---------------------------------------------------------------------------


@dataclass
class _MemRequestRow:
    from_player_id: int
    to_player_id: int
    request_time_ms: int
    status: int
    updated_ms: int
    request_id: int


@dataclass
class _MemCapacityRow:
    player_id: int
    friend_count: int
    created_ms: int


@dataclass
class _MemState:
    friends: dict[tuple[int, int], int]  # (player_id, friend_player_id) -> since_ms
    requests: dict[tuple[int, int], _MemRequestRow]
    capacities: dict[int, _MemCapacityRow]
    blocks: dict[tuple[int, int], int]  # (player_id, blocked_player_id) -> since_ms
    role_fences: dict[int, RoleFenceRow]


class _MemoryTx:
    """单事务内存句柄：基于 _MemState 快照执行，回滚时自动恢复。"""

    def __init__(self, state: _MemState, rng: random.Random) -> None:
        self._s = state
        self._rng = rng

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        return self._s.role_fences.get(player_id)

    async def apply_role_fence(self, state: Message, now_ms: int) -> bool:
        pid = int(state.player_id)
        existing = self._s.role_fences.get(pid)
        if existing is not None and int(state.version) <= existing.version:
            return False
        self._s.role_fences[pid] = RoleFenceRow(
            player_id=pid,
            account=str(state.account),
            owner_revision=int(state.owner_revision),
            freeze_token=int(state.freeze_token),
            freeze_final=bool(state.freeze_final),
            version=int(state.version),
            home_zone=int(state.home_zone),
            updated_ms=now_ms,
        )
        return True

    async def count_authoritative_friends(self, player_id: int) -> int:
        return sum(1 for (p, _) in self._s.friends if p == player_id)

    async def ensure_capacity_row(self, player_id: int, count: int, now_ms: int) -> None:
        if player_id not in self._s.capacities:
            self._s.capacities[player_id] = _MemCapacityRow(
                player_id=player_id, friend_count=count, created_ms=now_ms
            )

    async def lock_capacity_row(self, player_id: int) -> int | None:
        row = self._s.capacities.get(player_id)
        return row.friend_count if row is not None else None

    async def lock_block_row_exists(self, player_id: int, blocked_player_id: int) -> bool:
        return (player_id, blocked_player_id) in self._s.blocks

    async def lock_friend_edge_exists(self, player_id: int, friend_player_id: int) -> bool:
        return (player_id, friend_player_id) in self._s.friends

    async def lock_request_row(self, from_player_id: int, to_player_id: int) -> tuple[int, int, int] | None:
        row = self._s.requests.get((from_player_id, to_player_id))
        if row is None:
            return None
        return (row.status, row.request_id, row.request_time_ms)

    async def count_outgoing_pending(self, from_player_id: int) -> int:
        return sum(
            1
            for row in self._s.requests.values()
            if row.from_player_id == from_player_id and row.status == REQUEST_STATUS_PENDING
        )

    async def count_incoming_pending(self, to_player_id: int) -> int:
        return sum(
            1
            for row in self._s.requests.values()
            if row.to_player_id == to_player_id and row.status == REQUEST_STATUS_PENDING
        )

    async def upsert_pending_request(
        self, from_player_id: int, to_player_id: int, now_ms: int, request_id: int
    ) -> None:
        self._s.requests[(from_player_id, to_player_id)] = _MemRequestRow(
            from_player_id=from_player_id,
            to_player_id=to_player_id,
            request_time_ms=now_ms,
            status=REQUEST_STATUS_PENDING,
            updated_ms=now_ms,
            request_id=request_id,
        )

    async def backfill_pending_request_id(self, from_player_id: int, to_player_id: int, request_id: int) -> int:
        row = self._s.requests.get((from_player_id, to_player_id))
        if row is None or row.status != REQUEST_STATUS_PENDING or row.request_id != 0:
            return 0
        row.request_id = request_id
        return 1

    async def find_request_by_id(self, request_id: int) -> list[tuple[int, int]]:
        hits: list[tuple[int, int]] = []
        for row in self._s.requests.values():
            if row.request_id == request_id:
                hits.append((row.from_player_id, row.to_player_id))
                if len(hits) >= 2:
                    break
        return hits

    async def update_request_status_cas(
        self, from_player_id: int, to_player_id: int, new_status: int, now_ms: int, request_id: int
    ) -> int:
        row = self._s.requests.get((from_player_id, to_player_id))
        if row is None or row.status != REQUEST_STATUS_PENDING or row.request_id != request_id:
            return 0
        row.status = new_status
        row.updated_ms = now_ms
        return 1

    async def resolve_reverse_pending(
        self, from_player_id: int, to_player_id: int, new_status: int, now_ms: int
    ) -> int:
        row = self._s.requests.get((from_player_id, to_player_id))
        if row is None or row.status != REQUEST_STATUS_PENDING:
            return 0
        row.status = new_status
        row.updated_ms = now_ms
        return 1

    async def insert_friend_edge_ignore(self, player_id: int, friend_player_id: int, now_ms: int) -> int:
        key = (player_id, friend_player_id)
        if key in self._s.friends:
            return 0
        self._s.friends[key] = now_ms
        return 1

    async def increment_friend_count(self, player_id: int) -> None:
        row = self._s.capacities.get(player_id)
        if row is not None:
            row.friend_count += 1

    async def delete_friend_edge(self, player_id: int, friend_player_id: int) -> int:
        return 1 if self._s.friends.pop((player_id, friend_player_id), None) is not None else 0

    async def decrement_friend_count(self, player_id: int, now_ms: int) -> int:
        row = self._s.capacities.get(player_id)
        if row is None or row.friend_count <= 0:
            return 0
        row.friend_count -= 1
        row.created_ms = now_ms
        return 1

    async def friend_edge_exists_plain(self, a: int, b: int) -> bool:
        return (a, b) in self._s.friends or (b, a) in self._s.friends

    async def pair_blocked_either_way_plain(self, a: int, b: int) -> bool:
        return (a, b) in self._s.blocks or (b, a) in self._s.blocks

    async def pair_mutual_friends_plain(self, a: int, b: int) -> bool:
        return (a, b) in self._s.friends and (b, a) in self._s.friends

    async def query_friend_list(self, player_id: int, limit: int) -> list[FriendEntry]:
        items = sorted(
            (FriendEntry(friend_player_id=fid, since_ms=since) for (pid, fid), since in self._s.friends.items() if pid == player_id),
            key=lambda e: e.friend_player_id,
        )
        return items[:limit]

    async def query_pending_requests(self, player_id: int, limit: int) -> list[FriendRequestEntry]:
        items = [
            FriendRequestEntry(
                request_id=row.request_id,
                from_player_id=row.from_player_id,
                to_player_id=row.to_player_id,
                request_time_ms=row.request_time_ms,
                status=row.status,
            )
            for row in self._s.requests.values()
            if row.to_player_id == player_id and row.status == REQUEST_STATUS_PENDING
        ]
        items.sort(key=lambda r: (r.from_player_id, r.to_player_id))
        return items[:limit]

    async def probe_block_count(self, player_id: int, target_player_id: int) -> tuple[int, int]:
        total = sum(1 for (pid, _) in self._s.blocks if pid == player_id)
        already = 1 if (player_id, target_player_id) in self._s.blocks else 0
        return total, already

    async def count_blocks(self, player_id: int) -> int:
        return sum(1 for (pid, _) in self._s.blocks if pid == player_id)

    async def insert_block_odku(self, player_id: int, target_player_id: int, now_ms: int) -> None:
        self._s.blocks.setdefault((player_id, target_player_id), now_ms)

    async def delete_block(self, player_id: int, target_player_id: int) -> int:
        return 1 if self._s.blocks.pop((player_id, target_player_id), None) is not None else 0

    async def query_blocks(self, player_id: int, limit: int) -> list[BlockEntry]:
        items = sorted(
            (BlockEntry(blocked_player_id=bid, since_ms=since) for (pid, bid), since in self._s.blocks.items() if pid == player_id),
            key=lambda b: b.blocked_player_id,
        )
        return items[:limit]

    def _is_excluded(self, player_id: int, candidate_id: int, exclude_set: set[int]) -> bool:
        if candidate_id == player_id or candidate_id in exclude_set:
            return True
        if (player_id, candidate_id) in self._s.friends:
            return True
        if (player_id, candidate_id) in self._s.blocks or (candidate_id, player_id) in self._s.blocks:
            return True
        r_out = self._s.requests.get((player_id, candidate_id))
        if r_out is not None and r_out.status == REQUEST_STATUS_PENDING:
            return True
        r_in = self._s.requests.get((candidate_id, player_id))
        if r_in is not None and r_in.status == REQUEST_STATUS_PENDING:
            return True
        return False

    async def query_recommend_mutual(
        self, player_id: int, exclude: Sequence[int], limit: int
    ) -> list[RecommendCandidate]:
        my_friends = [fid for (pid, fid) in self._s.friends if pid == player_id]
        mutual_counts: dict[int, int] = {}
        for f1 in my_friends:
            for (pid, f2) in self._s.friends:
                if pid == f1 and f2 != player_id:
                    mutual_counts[f2] = mutual_counts.get(f2, 0) + 1
        ranked = [(cid, cnt, self._rng.random()) for cid, cnt in mutual_counts.items()]
        ranked.sort(key=lambda item: (-item[1], item[2]))
        window = ranked[:RECOMMEND_ANCHOR_WINDOW]
        exclude_set = set(int(x) for x in exclude)
        out: list[RecommendCandidate] = []
        for cid, cnt, _ in window:
            if self._is_excluded(player_id, cid, exclude_set):
                continue
            out.append(RecommendCandidate(candidate_player_id=cid, mutual_friends=cnt))
            if len(out) >= limit:
                break
        return out

    async def query_friend_id_range(self) -> tuple[int | None, int | None]:
        if not self._s.friends:
            return None, None
        pids = [pid for (pid, _) in self._s.friends]
        return min(pids), max(pids)

    async def query_recommend_anchor(
        self, player_id: int, exclude: Sequence[int], pivot: int, limit: int
    ) -> list[RecommendCandidate]:
        distinct_pids = sorted({pid for (pid, _) in self._s.friends if pid >= pivot})[:RECOMMEND_ANCHOR_WINDOW]
        exclude_set = set(int(x) for x in exclude)
        my_friends = {fid for (pid, fid) in self._s.friends if pid == player_id}
        out: list[RecommendCandidate] = []
        for cid in distinct_pids:
            if self._is_excluded(player_id, cid, exclude_set):
                continue
            mutual = sum(1 for fid in my_friends if (cid, fid) in self._s.friends)
            out.append(RecommendCandidate(candidate_player_id=cid, mutual_friends=mutual))
            if len(out) >= limit:
                break
        return out

    async def query_purge_peers(self, player_id: int) -> list[int]:
        peers: set[int] = set()
        for p, f in self._s.friends:
            if p == player_id:
                peers.add(f)
            if f == player_id:
                peers.add(p)
        for f, t in self._s.requests:
            if f == player_id:
                peers.add(t)
            if t == player_id:
                peers.add(f)
        for p, b in self._s.blocks:
            if p == player_id:
                peers.add(b)
            if b == player_id:
                peers.add(p)
        return sorted(peers)

    async def delete_request_row(self, from_player_id: int, to_player_id: int) -> None:
        self._s.requests.pop((from_player_id, to_player_id), None)

    async def delete_capacity_row(self, player_id: int) -> None:
        self._s.capacities.pop(player_id, None)


class _MemoryDriver:
    """基于异步锁与状态快照的内存事务驱动。"""

    def __init__(self, rng: random.Random) -> None:
        self._lock = asyncio.Lock()
        self._rng = rng
        self.state = _MemState(friends={}, requests={}, capacities={}, blocks={}, role_fences={})

    async def run_tx(self, work: Callable[[FriendTxHandle], Awaitable[T]]) -> T:
        async with self._lock:
            snapshot = copy.deepcopy(self.state)
            try:
                result = await work(_MemoryTx(snapshot, self._rng))
            except _TxRollbackReturn as ret:
                return ret.value
            except BaseException:
                raise
            self.state = snapshot
            return result

    async def apply_role_fence_direct(self, state: Message, now_ms: int) -> bool:
        async with self._lock:
            return await _MemoryTx(self.state, self._rng).apply_role_fence(state, now_ms)


class MemoryFriendStore(FriendStoreCore):
    """确定性内存双胞胎存储，供端到端单测与本地测试复用。"""

    def __init__(
        self,
        protocol: Protocol,
        data_service: FriendDataServiceClient,
        rules: FriendRules | None = None,
        *,
        cache_invalidator: CacheInvalidator | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        seed: int = 42,
    ) -> None:
        rng = random.Random(seed)
        self._driver_impl = _MemoryDriver(rng)
        super().__init__(
            protocol,
            self._driver_impl,
            data_service,
            rules if rules is not None else FriendRules(),
            cache_invalidator=cache_invalidator,
            clock_ms=clock_ms,
            rng=rng,
        )

    @property
    def mem_state(self) -> _MemState:
        """暴露内存表快照供单测做不变量断言（例如验证容量行未被凭空创建）。"""
        return self._driver_impl.state
