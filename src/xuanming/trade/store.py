"""trade 核心存储抽象（TradeStoreBase）、外部目录适配与确定性内存双胞胎（MemoryTradeStore）。"""

from __future__ import annotations

import asyncio
import copy
import logging
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import replace
from typing import Protocol as TypingProtocol, TypeVar

from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import sign_service_call
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
    InvalidRoleFenceStateError,
    ListingNotFoundError,
    RoleFenceStaleError,
    RoleFenceUnavailableError,
    RoleNotFoundError,
    StoreUnavailableError,
    TradeRoleConflictError,
    TradeTooManyPendingError,
)
from .model import (
    ASSET_OP_MAX_PENDING,
    ASSET_OP_MAX_SPAN,
    ASSET_STREAM_TRADE_DEBIT,
    COMMERCE_CANCELLED,
    COMMERCE_DELIVERED,
    COMMERCE_DELIVERING,
    COMMERCE_ESCROWING,
    COMMERCE_KIND_MARKET_MATCH,
    COMMERCE_KIND_OFFICIAL,
    COMMERCE_KIND_PLAYER_TRADE,
    COMMERCE_KIND_RMB_CONSIGNMENT,
    COMMERCE_MANUAL,
    COMMERCE_REFUND_PENDING,
    COMMERCE_SETTLED,
    COMMERCE_WAITING,
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
    MergedZones,
    OP_FINALIZABLE_STATUSES,
    OP_REF_KIND_ORDER,
    OP_ROLE_CLEAR_STATUSES,
    OP_STATUS_APPLIED,
    OP_STATUS_PENDING,
    OP_TERMINAL_STATUSES,
    ROLE_BLOCKER_ASSET_OP,
    ROLE_BLOCKER_COMMERCE,
    ROLE_BLOCKER_TOO_MANY_DEALS,
    ROLE_FENCE_CALLER,
    CommerceCursor,
    LegOutcome,
    ListingQuery,
    PendingAssetOp,
    RoleFenceExpect,
    RoleFenceRow,
    RoleOriginal,
    check_role_fence,
    on_book,
    valid_role_fence_state,
)

logger = logging.getLogger(__name__)

_DATA_SERVICE_PREFIX = "/data_service.DataService/"
_ROLE_FENCE_TIMEOUT_SEC = 2.0
_HOME_ZONE_TIMEOUT_SEC = 1.5
_ID_ALLOC_TIMEOUT_SEC = 2.0
_MERGED_ZONES_TIMEOUT_SEC = 2.0

T = TypeVar("T")


class _TxRollbackReturn(Exception):
    """在事务块内显式回滚并返回业务结果的内部控制流信号。"""

    def __init__(self, value) -> None:
        super().__init__("rollback_return")
        self.value = value


class _DuplicateKeyError(Exception):
    """唯一键或主键冲突（对应 MySQL 1062）。"""


# -- 外部目录与发号协议及 gRPC 适配器 -------------------------------------------------------------------

class RoleFenceDirectory(TypingProtocol):
    """查询 data_service 权威角色护栏状态的协议。"""

    async def get_role_fence_state(self, player_id: int) -> Message | None: ...


class HomeZoneDirectory(TypingProtocol):
    """查询玩家归属区（0 表示未映射）。"""

    async def home_zone(self, player_id: int) -> int: ...


class MergedZonesDirectory(TypingProtocol):
    """查询全服合服映射表。"""

    async def get_merged_zones(self) -> MergedZones: ...


class GrpcRoleFenceDirectory:
    """经 data_service.DataService/GetRoleFenceState 读取权威护栏状态（caller="trade"）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        role_fence_secret: bytes,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._secret = role_fence_secret
        self._clock_ms = clock_ms

    async def get_role_fence_state(self, player_id: int) -> Message | None:
        path = _DATA_SERVICE_PREFIX + "GetRoleFenceState"
        req = self._protocol.message("data_service.GetRoleFenceStateRequest", player_id=player_id)
        sign_service_call(path, req, ROLE_FENCE_CALLER, self._clock_ms(), self._secret)
        try:
            raw = await self._call(path, req.SerializeToString(deterministic=True), timeout=_ROLE_FENCE_TIMEOUT_SEC)
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


class GrpcHomeZoneDirectory:
    """经 data_service.DataService/BatchGetPlayerHomeZone 查询玩家归属区。"""

    def __init__(self, protocol: Protocol, call: Callable[..., Awaitable[bytes]]) -> None:
        self._protocol = protocol
        self._call = call

    async def home_zone(self, player_id: int) -> int:
        if player_id == 0:
            return 0
        req = self._protocol.message("data_service.BatchGetPlayerHomeZoneRequest", player_ids=[player_id])
        try:
            raw = await self._call(
                _DATA_SERVICE_PREFIX + "BatchGetPlayerHomeZone",
                req.SerializeToString(deterministic=True),
                timeout=_HOME_ZONE_TIMEOUT_SEC,
            )
        except Exception as exc:
            raise StoreUnavailableError(f"BatchGetPlayerHomeZone failed player={player_id}: {exc}") from exc
        resp = self._protocol.parse("data_service.BatchGetPlayerHomeZoneResponse", raw)
        return int(resp.player_zone_map.get(player_id, 0))


class GrpcMergedZonesDirectory:
    """经 data_service.DataService/GetMergedZones 拉取合服映射表。"""

    def __init__(self, protocol: Protocol, call: Callable[..., Awaitable[bytes]]) -> None:
        self._protocol = protocol
        self._call = call

    async def get_merged_zones(self) -> MergedZones:
        req = self._protocol.message("google.protobuf.Empty")
        try:
            raw = await self._call(
                _DATA_SERVICE_PREFIX + "GetMergedZones",
                req.SerializeToString(deterministic=True),
                timeout=_MERGED_ZONES_TIMEOUT_SEC,
            )
        except Exception as exc:
            raise StoreUnavailableError(f"GetMergedZones failed: {exc}") from exc
        resp = self._protocol.parse("data_service.GetMergedZonesResponse", raw)
        return MergedZones({int(k): int(v) for k, v in resp.merged_into.items()})


class IdSegmentAllocator:
    """经 data_service.DataService/AllocateIdSegment 本地缓段发号（trade_listing / trade_asset_op）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        biz_tag: str,
        *,
        step: int = 1000,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._biz_tag = biz_tag
        self._step = step
        self._lock = asyncio.Lock()
        self._next = 0
        self._hi = 0

    async def __call__(self) -> int:
        async with self._lock:
            if self._next < self._hi:
                value = self._next
                self._next += 1
                return value
            req = self._protocol.message(
                "data_service.AllocateIdSegmentRequest",
                biz_tag=self._biz_tag,
                step=self._step,
            )
            try:
                raw = await self._call(
                    _DATA_SERVICE_PREFIX + "AllocateIdSegment",
                    req.SerializeToString(deterministic=True),
                    timeout=_ID_ALLOC_TIMEOUT_SEC,
                )
            except Exception as exc:
                raise StoreUnavailableError(f"AllocateIdSegment failed tag={self._biz_tag}: {exc}") from exc
            resp = self._protocol.parse("data_service.AllocateIdSegmentResponse", raw)
            if resp.error_code != 0 or not 0 < resp.lo < resp.hi:
                raise StoreUnavailableError(
                    f"AllocateIdSegment rejected tag={self._biz_tag} code={resp.error_code} lo={resp.lo} hi={resp.hi}"
                )
            self._next = int(resp.lo) + 1
            self._hi = int(resp.hi)
            return int(resp.lo)


# -- 事务接口与公共存储基类 -----------------------------------------------------------------------------

class TradeTx(TypingProtocol):
    """单次 READ COMMITTED 事务接口（Memory 与 SQL 共享全部业务引擎逻辑）。"""

    async def now_ms(self) -> int: ...
    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None: ...
    async def upsert_role_fence(self, state: Message, now_ms: int) -> None: ...
    async def role_pending_blocker(self, state: Message) -> str: ...
    async def purge_favorites(self, player_id: int) -> None: ...
    async def lock_guard(self, guard_id: int) -> None: ...
    async def lock_commerce(self, commerce_id: int) -> Message | None: ...
    async def insert_commerce(self, rec: Message) -> None: ...
    async def save_commerce(self, rec: Message, workflow: Message, now_ms: int) -> None: ...
    async def save_commerce_status(self, rec: Message, status: int, now_ms: int) -> None: ...
    async def reschedule_commerce(self, commerce_id: int, version: int, due_ms: int) -> None: ...
    async def find_market_counterparty(self, taker: Message) -> int | None: ...
    async def count_pending_market_fills(self, parent_id: int) -> int: ...
    async def allocate_seqs(
        self, now_ms: int, keys: Sequence[tuple[int, int]]
    ) -> dict[tuple[int, int], tuple[int, int]]: ...
    async def insert_asset_op(self, rec: Message) -> None: ...
    async def read_leg_outcome(self, op_id: int) -> LegOutcome | None: ...
    async def expire_commerce_escrow_debits(self, commerce_id: int, now_ms: int) -> None: ...
    async def mark_commerce_ops_consumed(self, commerce_id: int) -> None: ...


class TradeStoreBase:
    """trade 存储公共层：统一承载 RoleFence 自愈、OwnershipInterceptor 预检与 RoleFenceParticipant 事务。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        role_directory: RoleFenceDirectory | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.role_directory = role_directory
        self._clock_ms = clock_ms

    def now_ms(self) -> int:
        return int(self._clock_ms())

    async def run_tx(self, body: Callable[[TradeTx], Awaitable[T]]) -> T:
        raise NotImplementedError

    async def get_role_fence_local(self, player_id: int) -> RoleFenceRow | None:
        raise NotImplementedError

    async def merge_role_fence(self, state: Message, now_ms: int) -> None:
        async def _tx(tx: TradeTx) -> None:
            await tx.upsert_role_fence(state, now_ms)

        await self.run_tx(_tx)

    async def _heal_players(self, player_ids: Sequence[int]) -> None:
        if self.role_directory is None:
            raise RoleFenceUnavailableError("role fence directory not configured")
        now = self.now_ms()
        for pid in sorted({int(p) for p in player_ids if int(p) != 0}):
            state = await self.role_directory.get_role_fence_state(pid)
            if state is None:
                raise RoleNotFoundError(f"role {pid} not found in account directory")
            await self.merge_role_fence(state, now)

    async def run_role_tx(self, body: Callable[[TradeTx], Awaitable[T]]) -> T:
        """执行带角色护栏自愈的事务：首次遇到副本缺行/落后/冻结时回滚并拉取权威状态后重跑一次。"""
        healed = False
        while True:
            try:
                return await self.run_tx(body)
            except RoleFenceStaleError as stale:
                if healed or not stale.player_ids:
                    raise (stale.reject or TradeRoleConflictError()) from stale
                await self._heal_players(stale.player_ids)
                healed = True

    async def verify_original_owner(self, original: RoleOriginal) -> None:
        """客户端入口归属预检（OwnershipInterceptor）：只核原账号与修订号，不看冻结。"""
        expect = RoleFenceExpect(
            player_id=original.player_id,
            account=original.account,
            owner_revision=original.owner_revision,
            reject=TradeRoleConflictError("role ownership changed"),
        )
        fence = await self.get_role_fence_local(original.player_id)
        verdict = check_role_fence(fence, expect, check_freeze=False)
        if verdict == "ok":
            return
        if verdict == "reject":
            raise TradeRoleConflictError("role ownership rejected by local fence")
        try:
            await self._heal_players((original.player_id,))
        except RoleNotFoundError as exc:
            raise TradeRoleConflictError("role not found in directory") from exc
        fence = await self.get_role_fence_local(original.player_id)
        verdict = check_role_fence(fence, expect, check_freeze=False)
        if verdict != "ok":
            raise TradeRoleConflictError("role ownership rejected after self-heal")

    async def lock_roles(self, tx: TradeTx, expects: Sequence[RoleFenceExpect]) -> dict[int, RoleFenceRow]:
        """按 player_id 升序锁角色护栏副本并逐一核验；所有需要自愈的 player_id 合并一次抛 StaleError。"""
        merged: dict[int, list[RoleFenceExpect]] = {}
        for exp in expects:
            if exp.player_id:
                merged.setdefault(int(exp.player_id), []).append(exp)
        rows: dict[int, RoleFenceRow] = {}
        stale_ids: list[int] = []
        first_reject: Exception | None = None
        for pid in sorted(merged):
            row = await tx.lock_role_fence(pid)
            for exp in merged[pid]:
                verdict = check_role_fence(row, exp, check_freeze=True)
                if verdict == "reject":
                    raise (exp.reject or TradeRoleConflictError())
                if verdict == "stale":
                    if pid not in stale_ids:
                        stale_ids.append(pid)
                    if first_reject is None and exp.reject is not None:
                        first_reject = exp.reject
            if row is not None:
                rows[pid] = row
        if stale_ids:
            raise RoleFenceStaleError(tuple(stale_ids), reject=first_reject)
        return rows

    async def read_ownership(self, tx: TradeTx, player_id: int) -> RoleFenceRow:
        if player_id == 0:
            raise TradeRoleConflictError("zero player_id")
        row = await tx.lock_role_fence(player_id)
        if row is None:
            raise RoleFenceStaleError(player_id, reject=TradeRoleConflictError())
        if row.account == "":
            raise TradeRoleConflictError("role deleted")
        return row

    # -- RoleFenceParticipant 公共事务（PrepareRoleFreeze / ApplyRoleFence / PurgeRemovedRole） -----------

    async def prepare_role_freeze(self, state: Message, *, dry_run: bool = False) -> Message:
        if not valid_role_fence_state(state):
            raise InvalidRoleFenceStateError("invalid role fence state in PrepareRoleFreeze")
        if state.account == "":
            raise InvalidRoleFenceStateError("PrepareRoleFreeze cannot accept tombstone state")
        if not dry_run and int(state.freeze_token) == 0:
            raise InvalidRoleFenceStateError("PrepareRoleFreeze requires non-zero freeze_token")

        now = self.now_ms()

        async def _tx(tx: TradeTx) -> Message:
            cur = await tx.lock_role_fence(int(state.player_id))
            if cur is not None and cur.version > int(state.version):
                raise _TxRollbackReturn(self.protocol.message("rolefence.PrepareRoleFreezeResponse", stale=True))
            if not dry_run:
                await tx.upsert_role_fence(state, now)
            blocker = await tx.role_pending_blocker(state)
            if blocker:
                raise _TxRollbackReturn(
                    self.protocol.message("rolefence.PrepareRoleFreezeResponse", clean=False, blocker=blocker)
                )
            resp = self.protocol.message("rolefence.PrepareRoleFreezeResponse", clean=True)
            if dry_run:
                raise _TxRollbackReturn(resp)
            return resp

        return await self.run_tx(_tx)

    async def apply_role_fence(self, state: Message) -> Message:
        if not valid_role_fence_state(state):
            raise InvalidRoleFenceStateError("invalid role fence state in ApplyRoleFence")
        await self.merge_role_fence(state, self.now_ms())
        return self.protocol.message("rolefence.ApplyRoleFenceResponse")

    async def purge_removed_role(self, state: Message) -> Message:
        if not valid_role_fence_state(state) or state.account != "":
            raise InvalidRoleFenceStateError("PurgeRemovedRole requires valid tombstone state")
        now = self.now_ms()

        async def _tx(tx: TradeTx) -> Message:
            await tx.upsert_role_fence(state, now)
            cur = await tx.lock_role_fence(int(state.player_id))
            if cur is None or cur.account != "":
                return self.protocol.message("rolefence.PurgeRemovedRoleResponse", retry=True)
            await tx.purge_favorites(int(state.player_id))
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse", retry=False)

        return await self.run_tx(_tx)


# -- 确定性内存双胞胎（MemoryTradeStore） ----------------------------------------------------------------

def _like_to_regex(pattern: str) -> re.Pattern[str]:
    """将带 ESCAPE '!' 的 SQL LIKE 模式转成大小写不敏感正则（与 MySQL utf8mb4_unicode_ci 一致）。"""
    out = ["^"]
    i = 0
    n = len(pattern)
    while i < n:
        ch = pattern[i]
        if ch == "!" and i + 1 < n:
            out.append(re.escape(pattern[i + 1]))
            i += 2
            continue
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
        i += 1
    out.append("$")
    return re.compile("".join(out), re.IGNORECASE | re.DOTALL)


def _clone_msg(protocol: Protocol, name: str, msg: Message) -> Message:
    cloned = protocol.message(name)
    cloned.CopyFrom(msg)
    return cloned


def _decode_workflow(protocol: Protocol, raw: bytes) -> Message | None:
    w = protocol.message("trade.TradeCommerceWorkflow")
    try:
        w.ParseFromString(raw)
        return w
    except DecodeError:
        return None


def _is_role_pending_commerce(protocol: Protocol, rec: Message, state: Message) -> bool:
    """判定单条未结案合同是否阻塞角色冻结（与 Go commerce.rolePendingCommerce 完全一致）。"""
    status = int(rec.status)
    if status in (COMMERCE_CANCELLED, COMMERCE_SETTLED):
        return False
    raw = bytes(rec.workflow_proto)
    if len(raw) > (1 << 20):
        return True
    w = _decode_workflow(protocol, raw)
    if w is None:
        return True
    kind = int(rec.kind)
    if kind == COMMERCE_KIND_OFFICIAL and status == COMMERCE_REFUND_PENDING and w.stage == "official_refund":
        return False
    if (
        int(rec.commerce_id) == int(state.freeze_token)
        and kind == COMMERCE_KIND_RMB_CONSIGNMENT
        and status == COMMERCE_ESCROWING
        and w.HasField("rmb")
        and int(w.rmb.character_player_id) == int(state.player_id)
        and w.rmb.seller_account == state.account
        and int(w.rmb.seller_owner_revision) == int(state.owner_revision)
        and w.stage == "rmb_freezing"
    ):
        return False
    return True


def _op_from_record(protocol: Protocol, rec: Message, lease_token: int) -> PendingAssetOp:
    bundle = protocol.message("AssetBundle")
    if rec.payload:
        try:
            bundle.ParseFromString(bytes(rec.payload))
        except DecodeError as exc:
            raise AssetOpPoisonRowError(f"cannot decode payload for op_id={rec.op_id}") from exc
    return PendingAssetOp(
        op_id=int(rec.op_id),
        player_id=int(rec.player_id),
        stream=int(rec.stream),
        stream_epoch=int(rec.stream_epoch),
        seq=int(rec.seq),
        correlation_id=int(rec.op_id),
        tx_type=int(rec.tx_type),
        bundle=bundle,
        attempts=int(rec.attempts),
        deadline_ms=int(rec.deadline_ms),
        lease_token=lease_token,
        last_reason=int(rec.last_reason),
        created_ms=int(rec.created_ms),
        last_send_ms=int(rec.last_send_ms),
    )


class _MemoryState:
    def __init__(self) -> None:
        self.listings: dict[int, Message] = {}
        self.favorites: dict[tuple[int, int], int] = {}  # (player_id, listing_id) -> created_ms
        self.op_seqs: dict[tuple[int, int], list[int]] = {}  # (player_id, stream) -> [next_seq, epoch, updated_ms]
        self.asset_ops: dict[int, Message] = {}  # op_id -> TradeAssetOpRecord
        self.commerce: dict[int, Message] = {}  # commerce_id -> TradeCommerceRecord
        self.commerce_by_req: dict[str, int] = {}  # request_key -> commerce_id
        self.guards: set[int] = set()
        self.role_fences: dict[int, RoleFenceRow] = {}
        self.credit_profiles: dict[SellerCreditKey, SellerCreditProfile] = {}
        self.credit_completed_orders: dict[str, SellerCreditKey] = {}
        self.credit_reviews_by_order: dict[str, SellerReviewRecord] = {}
        self.credit_reviews: list[SellerReviewRecord] = []
        self.credit_violations: dict[str, SellerViolationRecord] = {}


class _MemoryTx:
    def __init__(self, store: "MemoryTradeStore", state: _MemoryState) -> None:
        self._store = store
        self._state = state
        self._protocol = store.protocol

    async def now_ms(self) -> int:
        return self._store.now_ms()

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        return self._state.role_fences.get(int(player_id))

    async def upsert_role_fence(self, state: Message, now_ms: int) -> None:
        pid = int(state.player_id)
        cur = self._state.role_fences.get(pid)
        if cur is not None and cur.version >= int(state.version):
            return
        self._state.role_fences[pid] = RoleFenceRow(
            player_id=pid,
            account=str(state.account),
            owner_revision=int(state.owner_revision),
            freeze_token=int(state.freeze_token),
            freeze_final=bool(state.freeze_final),
            version=int(state.version),
            home_zone=int(state.home_zone),
            updated_ms=now_ms,
        )

    async def role_pending_blocker(self, state: Message) -> str:
        pid = int(state.player_id)
        active = sorted(
            (
                rec
                for rec in self._state.commerce.values()
                if (int(rec.seller_player_id) == pid or int(rec.buyer_player_id) == pid)
                and int(rec.status) not in (COMMERCE_CANCELLED, COMMERCE_SETTLED)
            ),
            key=lambda r: int(r.commerce_id),
        )
        if len(active) > MAX_ROLE_PENDING_COMMERCE:
            return ROLE_BLOCKER_TOO_MANY_DEALS
        for rec in active:
            if _is_role_pending_commerce(self._protocol, rec, state):
                return ROLE_BLOCKER_COMMERCE
        for op in self._state.asset_ops.values():
            if int(op.player_id) == pid and int(op.status) not in OP_ROLE_CLEAR_STATUSES:
                return ROLE_BLOCKER_ASSET_OP
        return ""

    async def purge_favorites(self, player_id: int) -> None:
        pid = int(player_id)
        for key in [k for k in self._state.favorites if k[0] == pid]:
            del self._state.favorites[key]

    async def lock_guard(self, guard_id: int) -> None:
        self._state.guards.add(int(guard_id))

    async def lock_commerce(self, commerce_id: int) -> Message | None:
        rec = self._state.commerce.get(int(commerce_id))
        if rec is None:
            return None
        return _clone_msg(self._protocol, "trade.TradeCommerceRecord", rec)

    async def insert_commerce(self, rec: Message) -> None:
        req_key = str(rec.request_key)
        if req_key in self._state.commerce_by_req:
            raise _DuplicateKeyError(f"duplicate request_key={req_key}")
        cid = int(rec.commerce_id)
        if cid in self._state.commerce:
            raise _DuplicateKeyError(f"duplicate commerce_id={cid}")
        w = _decode_workflow(self._protocol, bytes(rec.workflow_proto))
        rec.on_book = on_book(rec, w)
        stored = _clone_msg(self._protocol, "trade.TradeCommerceRecord", rec)
        self._state.commerce[cid] = stored
        self._state.commerce_by_req[req_key] = cid

    async def save_commerce(self, rec: Message, workflow: Message, now_ms: int) -> None:
        cid = int(rec.commerce_id)
        cur = self._state.commerce.get(cid)
        if cur is None or int(cur.version) != int(rec.version):
            raise StoreUnavailableError("commerce version changed")
        raw = workflow.SerializeToString(deterministic=True)
        rec.workflow_proto = raw
        rec.updated_ms = now_ms
        rec.version = int(rec.version) + 1
        rec.on_book = on_book(rec, workflow)
        self._state.commerce[cid] = _clone_msg(self._protocol, "trade.TradeCommerceRecord", rec)

    async def save_commerce_status(self, rec: Message, status: int, now_ms: int) -> None:
        cid = int(rec.commerce_id)
        cur = self._state.commerce.get(cid)
        if cur is None or int(cur.version) != int(rec.version):
            raise StoreUnavailableError("commerce version changed")
        rec.status = status
        rec.updated_ms = now_ms
        rec.version = int(rec.version) + 1
        self._state.commerce[cid] = _clone_msg(self._protocol, "trade.TradeCommerceRecord", rec)

    async def reschedule_commerce(self, commerce_id: int, version: int, due_ms: int) -> None:
        cur = self._state.commerce.get(int(commerce_id))
        if cur is not None and int(cur.version) == int(version):
            cur.updated_ms = due_ms

    async def find_market_counterparty(self, taker: Message) -> int | None:
        return _memory_find_market_counterparty(self._state, taker)

    async def count_pending_market_fills(self, parent_id: int) -> int:
        pid = int(parent_id)
        return sum(
            1
            for rec in self._state.commerce.values()
            if int(rec.kind) == COMMERCE_KIND_MARKET_MATCH
            and int(rec.side) == 0
            and (int(rec.parent_id) == pid or int(rec.counterparty_id) == pid)
            and int(rec.status) not in (COMMERCE_CANCELLED, COMMERCE_SETTLED)
        )

    async def allocate_seqs(
        self, now_ms: int, keys: Sequence[tuple[int, int]]
    ) -> dict[tuple[int, int], tuple[int, int]]:
        ordered = sorted({(int(pid), int(stream)) for pid, stream in keys})
        for pid, stream in ordered:
            if pid == 0 or stream == 0:
                raise StoreUnavailableError(f"invalid seq key ({pid}, {stream})")
            if (pid, stream) not in self._state.op_seqs:
                raise StoreUnavailableError(f"missing seq row for ({pid}, {stream})")
        out: dict[tuple[int, int], tuple[int, int]] = {}
        for pid, stream in ordered:
            row = self._state.op_seqs[(pid, stream)]
            next_seq, epoch = row[0], row[1]
            if epoch == 0 or next_seq == 0:
                raise StoreUnavailableError(f"corrupt seq row for ({pid}, {stream})")
            pending = [
                int(op.seq)
                for op in self._state.asset_ops.values()
                if int(op.player_id) == pid
                and int(op.stream) == stream
                and int(op.stream_epoch) == epoch
                and int(op.status) == OP_STATUS_PENDING
            ]
            if len(pending) >= ASSET_OP_MAX_PENDING:
                raise TradeTooManyPendingError(f"player={pid} stream={stream} pending={len(pending)}")
            if pending and next_seq - min(pending) >= ASSET_OP_MAX_SPAN:
                raise TradeTooManyPendingError(f"player={pid} stream={stream} span exceeded")
            row[0] = next_seq + 1
            row[2] = now_ms
            out[(pid, stream)] = (next_seq, epoch)
        return out

    async def insert_asset_op(self, rec: Message) -> None:
        oid = int(rec.op_id)
        if oid in self._state.asset_ops:
            raise _DuplicateKeyError(f"duplicate op_id={oid}")
        stored = _clone_msg(self._protocol, "trade.TradeAssetOpRecord", rec)
        stored.last_send_ms = max(int(stored.last_send_ms), int(stored.created_ms))
        self._state.asset_ops[oid] = stored

    async def read_leg_outcome(self, op_id: int) -> LegOutcome | None:
        return _memory_read_leg_outcome(self._state, op_id)

    async def expire_commerce_escrow_debits(self, commerce_id: int, now_ms: int) -> None:
        cid = int(commerce_id)
        for oid in sorted(self._state.asset_ops):
            op = self._state.asset_ops[oid]
            if (
                int(op.ref_kind) == OP_REF_KIND_ORDER
                and int(op.ref_id) == cid
                and int(op.stream) == ASSET_STREAM_TRADE_DEBIT
                and int(op.status) == OP_STATUS_PENDING
            ):
                op.deadline_ms = now_ms

    async def mark_commerce_ops_consumed(self, commerce_id: int) -> None:
        cid = int(commerce_id)
        for oid in sorted(self._state.asset_ops):
            op = self._state.asset_ops[oid]
            if int(op.ref_kind) == OP_REF_KIND_ORDER and int(op.ref_id) == cid:
                op.business_consumed = True


def _memory_find_market_counterparty(state: _MemoryState, taker: Message) -> int | None:
    opposite = 2 if int(taker.side) == 1 else 1
    candidates = [
        rec
        for rec in state.commerce.values()
        if int(rec.kind) == COMMERCE_KIND_MARKET_MATCH
        and int(rec.side) == opposite
        and int(rec.market_id) == int(taker.market_id)
        and int(rec.item_config_id) == int(taker.item_config_id)
        and int(rec.currency_type) == int(taker.currency_type)
        and int(rec.status) == COMMERCE_ESCROWING
        and bool(rec.on_book)
        and int(rec.filled_quantity) < int(rec.quantity)
        and int(rec.seller_player_id) != int(taker.seller_player_id)
        and (
            int(rec.unit_price) <= int(taker.unit_price)
            if int(taker.side) == 1
            else int(rec.unit_price) >= int(taker.unit_price)
        )
    ]
    if not candidates:
        return None
    if int(taker.side) == 1:
        candidates.sort(key=lambda r: (int(r.unit_price), int(r.created_ms), int(r.commerce_id)))
    else:
        candidates.sort(key=lambda r: (-int(r.unit_price), int(r.created_ms), int(r.commerce_id)))
    return int(candidates[0].commerce_id)


def _memory_read_leg_outcome(state: _MemoryState, op_id: int) -> LegOutcome | None:
    op = state.asset_ops.get(int(op_id))
    if op is None:
        return None
    status = int(op.status)
    final_ms = int(op.next_attempt_ms) if status in OP_TERMINAL_STATUSES else 0
    return LegOutcome(
        status=status,
        snapshot=bytes(op.escrow_snapshot),
        resolved_by=str(op.resolved_by),
        final_ms=final_ms,
    )


class MemoryTradeStore(TradeStoreBase):
    """确定性内存双胞胎：单测与本地联调复用，事务回滚与 SQL 语义一致。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        role_directory: RoleFenceDirectory | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        super().__init__(protocol, role_directory=role_directory, clock_ms=clock_ms)
        self._state = _MemoryState()
        self._lock = asyncio.Lock()

    async def run_tx(self, body: Callable[[TradeTx], Awaitable[T]]) -> T:
        async with self._lock:
            snapshot = copy.deepcopy(self._state)
            tx = _MemoryTx(self, snapshot)
            try:
                result = await body(tx)
            except _TxRollbackReturn as signal:
                return signal.value
            except BaseException:
                raise
            self._state = snapshot
            return result

    async def get_role_fence_local(self, player_id: int) -> RoleFenceRow | None:
        return self._state.role_fences.get(int(player_id))

    # -- P1 Catalog ---------------------------------------------------------------------------------

    def _filter_listings(self, q: ListingQuery) -> list[Message]:
        like_re = _like_to_regex(q.title_like_pattern) if q.title_like_pattern else None
        zones_set = set(q.market_zones)
        out: list[Message] = []
        for rec in self._state.listings.values():
            st = int(rec.status)
            if q.tab == LISTING_TAB_PUBLIC_NOTICE:
                if st != LISTING_STATUS_LISTED or int(rec.notice_end_ms) <= q.now_ms:
                    continue
            elif q.tab == LISTING_TAB_ON_SALE:
                if (
                    st not in (LISTING_STATUS_LISTED, LISTING_STATUS_LOCKED)
                    or int(rec.notice_end_ms) > q.now_ms
                    or int(rec.sale_end_ms) <= q.now_ms
                ):
                    continue
            else:
                raise StoreUnavailableError(f"unsupported listing tab {q.tab}")
            if zones_set and int(rec.market_zone) not in zones_set:
                continue
            if int(rec.category) != q.category:
                continue
            if q.subcategory != 0 and int(rec.subcategory) != q.subcategory:
                continue
            if like_re is not None:
                id_match = q.search_listing_id != 0 and int(rec.listing_id) == q.search_listing_id
                title_match = bool(like_re.match(str(rec.title)))
                if not (id_match or title_match):
                    continue
            if q.favorites_of != 0 and (q.favorites_of, int(rec.listing_id)) not in self._state.favorites:
                continue
            out.append(rec)
        return out

    @staticmethod
    def _sort_listings(rows: list[Message], sort: int, tab: int) -> list[Message]:
        if sort == LISTING_SORT_DEFAULT:
            return sorted(rows, key=lambda r: (-int(r.created_ms), -int(r.listing_id)))
        if sort == LISTING_SORT_PRICE_ASC:
            return sorted(rows, key=lambda r: (int(r.price_fen), int(r.listing_id)))
        if sort == LISTING_SORT_PRICE_DESC:
            return sorted(rows, key=lambda r: (-int(r.price_fen), -int(r.listing_id)))
        if sort == LISTING_SORT_LEVEL_DESC:
            return sorted(rows, key=lambda r: (-int(r.level), -int(r.listing_id)))
        if sort == LISTING_SORT_REMAINING_ASC:
            if tab == LISTING_TAB_PUBLIC_NOTICE:
                return sorted(rows, key=lambda r: (int(r.notice_end_ms), int(r.listing_id)))
            return sorted(rows, key=lambda r: (int(r.sale_end_ms), int(r.listing_id)))
        raise StoreUnavailableError(f"unsupported listing sort {sort}")

    async def count_listings(self, query: ListingQuery) -> int:
        return len(self._filter_listings(query))

    async def query_listings(self, query: ListingQuery, offset: int, limit: int) -> list[Message]:
        ordered = self._sort_listings(self._filter_listings(query), query.sort, query.tab)
        return [_clone_msg(self.protocol, "trade.TradeListingRecord", r) for r in ordered[offset : offset + limit]]

    async def count_seller_listings(self, seller_id: int) -> int:
        return sum(1 for r in self._state.listings.values() if int(r.seller_player_id) == seller_id)

    async def query_seller_listings(self, seller_id: int, offset: int, limit: int) -> list[Message]:
        rows = [r for r in self._state.listings.values() if int(r.seller_player_id) == seller_id]
        rows.sort(key=lambda r: (-int(r.created_ms), -int(r.listing_id)))
        return [_clone_msg(self.protocol, "trade.TradeListingRecord", r) for r in rows[offset : offset + limit]]

    async def get_listing(self, listing_id: int) -> Message:
        rec = self._state.listings.get(int(listing_id))
        if rec is None:
            raise ListingNotFoundError(f"listing {listing_id} not found")
        return _clone_msg(self.protocol, "trade.TradeListingRecord", rec)

    async def insert_listing(self, rec: Message) -> None:
        lid = int(rec.listing_id)
        if lid in self._state.listings:
            raise StoreUnavailableError(f"duplicate listing_id={lid}")
        self._state.listings[lid] = _clone_msg(self.protocol, "trade.TradeListingRecord", rec)

    async def favorite_ids(self, player_id: int, listing_ids: Sequence[int]) -> set[int]:
        pid = int(player_id)
        return {int(lid) for lid in listing_ids if (pid, int(lid)) in self._state.favorites}

    async def favorite_exists(self, player_id: int, listing_id: int) -> bool:
        return (int(player_id), int(listing_id)) in self._state.favorites

    async def count_favorites(self, player_id: int) -> int:
        pid = int(player_id)
        return sum(1 for k in self._state.favorites if k[0] == pid)

    async def insert_favorite(self, player_id: int, listing_id: int, created_ms: int) -> None:
        key = (int(player_id), int(listing_id))
        self._state.favorites.setdefault(key, created_ms)

    async def delete_favorite(self, player_id: int, listing_id: int) -> None:
        self._state.favorites.pop((int(player_id), int(listing_id)), None)

    # -- Commerce 查询与调度 ------------------------------------------------------------------------

    async def get_commerce(self, commerce_id: int) -> Message | None:
        rec = self._state.commerce.get(int(commerce_id))
        return None if rec is None else _clone_msg(self.protocol, "trade.TradeCommerceRecord", rec)

    async def get_commerce_by_request(self, req_key: str) -> Message | None:
        cid = self._state.commerce_by_req.get(req_key)
        if cid is None:
            return None
        return await self.get_commerce(cid)

    async def list_commerce_orders(self, player_id: int, after: CommerceCursor, limit: int) -> list[Message]:
        pid = int(player_id)
        rows = [
            rec
            for rec in self._state.commerce.values()
            if int(rec.kind) != COMMERCE_KIND_OFFICIAL
            and (int(rec.seller_player_id) == pid or int(rec.buyer_player_id) == pid)
            and (
                int(rec.created_ms) > after.created_ms
                or (int(rec.created_ms) == after.created_ms and int(rec.commerce_id) > after.commerce_id)
            )
        ]
        rows.sort(key=lambda r: (int(r.created_ms), int(r.commerce_id)))
        return [_clone_msg(self.protocol, "trade.TradeCommerceRecord", r) for r in rows[:limit]]

    async def list_market_orders(self, player_id: int, after: CommerceCursor, limit: int) -> list[Message]:
        pid = int(player_id)
        rows = [
            rec
            for rec in self._state.commerce.values()
            if int(rec.kind) == COMMERCE_KIND_MARKET_MATCH
            and int(rec.side) != 0
            and int(rec.seller_player_id) == pid
            and (
                int(rec.created_ms) > after.created_ms
                or (int(rec.created_ms) == after.created_ms and int(rec.commerce_id) > after.commerce_id)
            )
        ]
        rows.sort(key=lambda r: (int(r.created_ms), int(r.commerce_id)))
        return [_clone_msg(self.protocol, "trade.TradeCommerceRecord", r) for r in rows[:limit]]

    async def query_market_depth(
        self, market_id: int, item_config_id: int, currency_type: int, limit: int
    ) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
        bids_map: dict[int, int] = {}
        asks_map: dict[int, int] = {}
        for rec in self._state.commerce.values():
            if (
                int(rec.kind) == COMMERCE_KIND_MARKET_MATCH
                and int(rec.market_id) == market_id
                and int(rec.item_config_id) == item_config_id
                and int(rec.currency_type) == currency_type
                and int(rec.status) == COMMERCE_ESCROWING
                and bool(rec.on_book)
                and int(rec.filled_quantity) < int(rec.quantity)
            ):
                rem = int(rec.quantity) - int(rec.filled_quantity)
                price = int(rec.unit_price)
                if int(rec.side) == 1:
                    bids_map[price] = bids_map.get(price, 0) + rem
                elif int(rec.side) == 2:
                    asks_map[price] = asks_map.get(price, 0) + rem
        asks = sorted(asks_map.items(), key=lambda kv: kv[0])[:limit]
        bids = sorted(bids_map.items(), key=lambda kv: -kv[0])[:limit]
        return asks, bids

    async def browse_commerce_offers(self, kind: int, after_id: int, now_ms: int, limit: int) -> list[Message]:
        rows = [
            rec
            for rec in self._state.commerce.values()
            if int(rec.kind) == kind
            and int(rec.status) == COMMERCE_ESCROWING
            and int(rec.commerce_id) > after_id
            and int(rec.expires_ms) > now_ms
        ]
        rows.sort(key=lambda r: int(r.commerce_id))
        return [_clone_msg(self.protocol, "trade.TradeCommerceRecord", r) for r in rows[:limit]]

    async def list_due_commerce(self, now_ms: int, limit: int) -> list[Message]:
        rows = [
            rec
            for rec in self._state.commerce.values()
            if int(rec.status) in DUE_STATUSES and int(rec.updated_ms) <= now_ms
        ]
        rows.sort(key=lambda r: (int(r.updated_ms), int(r.commerce_id)))
        return [_clone_msg(self.protocol, "trade.TradeCommerceRecord", r) for r in rows[:limit]]

    async def list_manual_commerce(self, after_updated_ms: int, after_id: int, limit: int) -> list[Message]:
        rows = [
            rec
            for rec in self._state.commerce.values()
            if int(rec.status) == COMMERCE_MANUAL
            and (
                int(rec.updated_ms) > after_updated_ms
                or (int(rec.updated_ms) == after_updated_ms and int(rec.commerce_id) > after_id)
            )
        ]
        rows.sort(key=lambda r: (int(r.updated_ms), int(r.commerce_id)))
        return [_clone_msg(self.protocol, "trade.TradeCommerceRecord", r) for r in rows[:limit]]

    async def defer_commerce(self, commerce_id: int, version: int, until_ms: int) -> bool:
        async with self._lock:
            rec = self._state.commerce.get(int(commerce_id))
            if rec is None or int(rec.version) != int(version):
                return False
            rec.updated_ms = until_ms
            return True

    async def wake_commerce(self, commerce_id: int, at_ms: int) -> None:
        async with self._lock:
            rec = self._state.commerce.get(int(commerce_id))
            if rec is not None and int(rec.status) in DUE_STATUSES and int(rec.updated_ms) > at_ms:
                rec.updated_ms = at_ms

    async def find_market_counterparty(self, taker: Message) -> int | None:
        return _memory_find_market_counterparty(self._state, taker)

    async def read_leg_outcome(self, op_id: int) -> LegOutcome | None:
        return _memory_read_leg_outcome(self._state, op_id)

    async def ensure_seq_rows(self, now_ms: int, keys: Sequence[tuple[int, int]]) -> None:
        ordered = sorted({(int(pid), int(stream)) for pid, stream in keys})
        async with self._lock:
            for pid, stream in ordered:
                if pid == 0 or stream == 0:
                    raise StoreUnavailableError(f"invalid seq key ({pid}, {stream})")
                if (pid, stream) not in self._state.op_seqs:
                    self._state.op_seqs[(pid, stream)] = [1, max(now_ms, 1), now_ms]

    # -- AssetOp Outbox -----------------------------------------------------------------------------

    async def list_due_ops(
        self,
        now_ms: int,
        limit: int,
        *,
        owned_shards: frozenset[int] | None = None,
    ) -> list[int]:
        if limit <= 0:
            return []

        def _eligible(op: Message) -> bool:
            if int(op.status) != OP_STATUS_PENDING:
                return False
            if int(op.next_attempt_ms) > now_ms or int(op.lease_until_ms) >= now_ms:
                return False
            if owned_shards is not None and (int(op.player_id) % 64) not in owned_shards:
                return False
            return True

        fresh = sorted(
            (op for op in self._state.asset_ops.values() if _eligible(op) and int(op.attempts) < FRESH_ATTEMPT_LIMIT),
            key=lambda o: (int(o.next_attempt_ms), int(o.op_id)),
        )
        ids = [int(o.op_id) for o in fresh[:limit]]
        if len(ids) >= limit:
            return ids
        seen = set(ids)
        aged = sorted(
            (op for op in self._state.asset_ops.values() if _eligible(op) and int(op.attempts) >= FRESH_ATTEMPT_LIMIT),
            key=lambda o: (int(o.next_attempt_ms), int(o.op_id)),
        )
        for op in aged:
            oid = int(op.op_id)
            if oid not in seen:
                ids.append(oid)
                seen.add(oid)
                if len(ids) >= limit:
                    break
        return ids

    async def claim_op(
        self,
        op_id: int,
        now_ms: int,
        lease_until_ms: int,
        poison_until_ms: int,
        token: int,
    ) -> PendingAssetOp | None:
        async with self._lock:
            op = self._state.asset_ops.get(int(op_id))
            if op is None or int(op.status) != OP_STATUS_PENDING or int(op.lease_until_ms) >= now_ms:
                return None
            base_last = int(op.last_send_ms) or max(int(op.created_ms), int(op.updated_ms))
            op.last_send_ms = max(base_last, int(op.lease_until_ms))
            op.lease_until_ms = lease_until_ms
            op.lease_token = token
            op.updated_ms = now_ms
            try:
                return _op_from_record(self.protocol, op, token)
            except AssetOpPoisonRowError:
                op.last_outcome = 0
                op.next_attempt_ms = poison_until_ms
                op.lease_until_ms = 0
                op.updated_ms = now_ms
                raise

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
        async with self._lock:
            rec = self._state.asset_ops.get(op.op_id)
            if rec is None or int(rec.status) != OP_STATUS_PENDING or int(rec.lease_token) != op.lease_token:
                raise AssetOpLeaseLostError(f"lease lost op_id={op.op_id}")
            rec.attempts = int(rec.attempts) + 1
            rec.next_attempt_ms = next_attempt_ms
            rec.lease_until_ms = 0
            rec.durable = bool(durable)
            rec.last_outcome = outcome
            rec.last_reason = reason
            rec.updated_ms = now_ms
            if not local:
                rec.last_send_ms = max(int(rec.last_send_ms), now_ms)

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
        wake_cid = 0
        async with self._lock:
            rec = self._state.asset_ops.get(op.op_id)
            if rec is None or int(rec.status) != OP_STATUS_PENDING:
                return False
            if int(rec.ref_kind) == OP_REF_KIND_ORDER and (not sql_committed or not durable):
                raise StoreUnavailableError("commerce asset op finalize requires SQL-committed durable evidence")
            rec.status = final_status
            rec.durable = True
            rec.last_outcome = outcome
            rec.last_reason = reason
            rec.lease_until_ms = 0
            rec.next_attempt_ms = now_ms
            rec.updated_ms = now_ms
            if (
                snapshot is not None
                and final_status == OP_STATUS_APPLIED
                and int(rec.stream) == ASSET_STREAM_TRADE_DEBIT
            ):
                rec.escrow_snapshot = snapshot.SerializeToString(deterministic=True)
                rec.receipt_ack_pending = True
            if int(rec.ref_kind) == OP_REF_KIND_ORDER and int(rec.ref_id) != 0:
                wake_cid = int(rec.ref_id)
        if wake_cid:
            await self.wake_commerce(wake_cid, now_ms)
        return True

    async def list_ack_due_ops(self, after_op_id: int, limit: int) -> list[PendingAssetOp | tuple[int, None]]:
        rows = [
            rec
            for rec in self._state.asset_ops.values()
            if bool(rec.receipt_ack_pending)
            and int(rec.status) == OP_STATUS_APPLIED
            and bool(rec.durable)
            and int(rec.op_id) > after_op_id
        ]
        rows.sort(key=lambda r: int(r.op_id))
        out: list[PendingAssetOp | tuple[int, None]] = []
        for rec in rows[:limit]:
            try:
                out.append(_op_from_record(self.protocol, rec, 0))
            except AssetOpPoisonRowError:
                out.append((int(rec.op_id), None))
        return out

    async def mark_op_acked(self, op_id: int) -> None:
        async with self._lock:
            rec = self._state.asset_ops.get(int(op_id))
            if rec is not None and int(rec.status) == OP_STATUS_APPLIED and bool(rec.durable):
                rec.receipt_ack_pending = False

    def get_asset_op_record(self, op_id: int) -> Message | None:
        """测试辅助：按主键读取内存中的 TradeAssetOpRecord 副本。"""
        rec = self._state.asset_ops.get(int(op_id))
        return None if rec is None else _clone_msg(self.protocol, "trade.TradeAssetOpRecord", rec)

    def set_commerce_raw_workflow(self, commerce_id: int, raw: bytes) -> None:
        """测试辅助：向指定合同写入原始字节以验证坏数据隔离。"""
        rec = self._state.commerce.get(int(commerce_id))
        if rec is not None:
            rec.workflow_proto = raw

    # -- 卖家信用、近 7 日成交参考价与当面交易单日流出 ---------------------------------------------------

    async def get_credit_profile(self, key: SellerCreditKey) -> SellerCreditProfile | None:
        async with self._lock:
            prof = self._state.credit_profiles.get(key)
            return None if prof is None else replace(prof)

    async def has_prior_credit_epoch(self, seller_player_id: int, current_revision: int) -> bool:
        pid = int(seller_player_id)
        rev = int(current_revision)
        async with self._lock:
            return any(
                k.seller_player_id == pid and k.owner_revision < rev
                for k in self._state.credit_profiles
            )

    async def save_credit_profile(self, profile: SellerCreditProfile) -> None:
        key = SellerCreditKey(
            seller_player_id=int(profile.seller_player_id),
            owner_revision=int(profile.owner_revision),
        )
        async with self._lock:
            self._state.credit_profiles[key] = replace(profile)

    async def mark_seller_order_completed(self, order_id: str, key: SellerCreditKey) -> bool:
        async with self._lock:
            if order_id in self._state.credit_completed_orders:
                return False
            self._state.credit_completed_orders[order_id] = key
            return True

    async def get_seller_review_by_order(self, order_id: str) -> SellerReviewRecord | None:
        async with self._lock:
            rec = self._state.credit_reviews_by_order.get(order_id)
            return None if rec is None else replace(rec)

    async def count_pair_scored_reviews(
        self,
        key: SellerCreditKey,
        buyer_key: str,
        buyer_ip: str,
        buyer_device_id: str,
        since_ms: int,
        now_ms: int,
    ) -> int:
        async with self._lock:
            count = 0
            for r in self._state.credit_reviews:
                if (
                    r.seller_player_id != key.seller_player_id
                    or r.seller_owner_revision != key.owner_revision
                    or not r.counted_for_score
                    or r.created_at_ms < since_ms
                    or r.created_at_ms > now_ms
                ):
                    continue
                same_buyer = (
                    r.buyer_account == buyer_key
                    or (bool(buyer_ip) and bool(r.buyer_ip) and r.buyer_ip == buyer_ip)
                    or (bool(buyer_device_id) and bool(r.buyer_device_id) and r.buyer_device_id == buyer_device_id)
                )
                if same_buyer:
                    count += 1
            return count

    async def save_seller_review(self, rec: SellerReviewRecord) -> None:
        async with self._lock:
            cloned = replace(rec)
            self._state.credit_reviews_by_order[cloned.order_id] = cloned
            self._state.credit_reviews.append(cloned)

    async def has_seller_violation(self, case_id: str) -> bool:
        async with self._lock:
            return case_id in self._state.credit_violations

    async def save_seller_violation(self, rec: SellerViolationRecord) -> None:
        async with self._lock:
            self._state.credit_violations[rec.case_id] = replace(rec)

    async def list_settled_market_fills(
        self,
        item_config_id: int,
        currency_type: int,
        floor_ms: int,
        now_ms: int,
        limit: int = MAX_REFERENCE_SCAN_ROWS,
    ) -> list[MarketFillSample]:
        item_id = int(item_config_id)
        cur = int(currency_type)
        lo = int(floor_ms)
        hi = int(now_ms)
        async with self._lock:
            matched = [
                rec
                for rec in self._state.commerce.values()
                if int(rec.kind) == COMMERCE_KIND_MARKET_MATCH
                and int(rec.side) == 0
                and int(rec.status) == COMMERCE_SETTLED
                and int(rec.item_config_id) == item_id
                and int(rec.currency_type) == cur
                and lo <= int(rec.updated_ms) <= hi
            ]
            matched.sort(key=lambda r: (int(r.updated_ms), int(r.commerce_id)), reverse=True)
            samples: list[MarketFillSample] = []
            for rec in matched[: int(limit)]:
                seller_acct = ""
                buyer_acct = ""
                if rec.workflow_proto:
                    w = _decode_workflow(self.protocol, bytes(rec.workflow_proto))
                    if w is not None:
                        if w.HasField("seller_identity"):
                            seller_acct = str(w.seller_identity.account)
                        if w.HasField("buyer_identity"):
                            buyer_acct = str(w.buyer_identity.account)
                samples.append(
                    MarketFillSample(
                        item_config_id=item_id,
                        currency_type=cur,
                        unit_price=int(rec.unit_price),
                        quantity=int(rec.quantity),
                        seller_account=seller_acct,
                        buyer_account=buyer_acct,
                        settled_at_ms=int(rec.updated_ms),
                    )
                )
            return samples

    async def daily_player_trade_currency_outflow(
        self,
        player_id: int,
        currency_type: int,
        day_start_ms: int,
    ) -> int:
        pid = int(player_id)
        cur = int(currency_type)
        since = int(day_start_ms)
        active_statuses = (
            COMMERCE_WAITING,
            COMMERCE_ESCROWING,
            COMMERCE_DELIVERING,
            COMMERCE_DELIVERED,
            COMMERCE_MANUAL,
            COMMERCE_SETTLED,
        )

        def _sum_side(as_seller: bool) -> int:
            rows = [
                rec
                for rec in self._state.commerce.values()
                if int(rec.kind) == COMMERCE_KIND_PLAYER_TRADE
                and int(rec.status) in active_statuses
                and int(rec.created_ms) >= since
                and (int(rec.seller_player_id) == pid if as_seller else int(rec.buyer_player_id) == pid)
            ]
            rows.sort(key=lambda r: int(r.commerce_id), reverse=True)
            rows = rows[:MAX_DAILY_PLAYER_TRADE_SCAN_ROWS]
            if len(rows) >= MAX_DAILY_PLAYER_TRADE_SCAN_ROWS:
                return UINT64_MAX
            total = 0
            for rec in rows:
                part = workflow_currency_outflow(
                    self.protocol, bytes(rec.workflow_proto), cur, as_seller=as_seller
                )
                total = saturating_add_uint64(total, part)
            return total

        async with self._lock:
            return saturating_add_uint64(_sum_side(True), _sum_side(False))
