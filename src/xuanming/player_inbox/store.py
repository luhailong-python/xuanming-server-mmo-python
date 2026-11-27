"""player_inbox 核心存储引擎、外部目录适配协议与内存双胞胎（MemoryInboxStore）。"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Protocol as TypingProtocol, TypeVar

import grpc
from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import sign_service_call
from ..protocol import Protocol
from .errors import (
    BattleSettlementOriginalError,
    ERR_IDEMPOTENCY_CONFLICT,
    ERR_INVALID_PARAMETER,
    ERR_ISSUED_OUT_OF_WINDOW,
    ERR_ITEM_NOT_FOUND,
    ERR_ITEM_STATE_CONFLICT,
    ERR_PLAYER_NOT_FOUND,
    InboxError,
    InvalidRoleFenceStateError,
    OriginalRoleError,
    RoleFenceStaleError,
    RoleFenceUnavailableError,
    RoleNotFoundError,
    StoreUnavailableError,
)
from .model import (
    ACK_CONFLICT,
    ACK_FINALIZED,
    ACK_UNKNOWN_SEQ,
    AUDIT_FORCE_VOID,
    AUDIT_VOID,
    ACK_MAX_DECISIONS,
    AuditInput,
    AuditRow,
    DELIVERY_WARN_THRESHOLD,
    DIRECTIVE_NONE,
    DIRECTIVE_VOID,
    DOMAIN_BATTLE_SETTLEMENT,
    DOMAIN_UNSPECIFIED,
    EnqueueParams,
    EnqueueResult,
    FENCE_EPOCH_REQUIRED,
    FENCE_NO_STREAM,
    FETCH_DEFAULT_ITEMS,
    FETCH_MAX_ITEMS,
    FINALIZER_ADMIN,
    FINALIZER_CONSUMER,
    FORCE_VOID_PROVEN,
    HOME_ZONE_TIMEOUT_SEC,
    ISSUED_AT_MAX_FUTURE_MS,
    ItemRow,
    LEDGER_READ_TIMEOUT_SEC,
    LIST_DEFAULT_LIMIT,
    LIST_MAX_LIMIT,
    MSG_BATTLE_SETTLEMENT,
    MSG_GM_BLOCK_ITEM_GAIN,
    MSG_GM_UNBLOCK_ITEM_GAIN,
    MAX_OPERATOR_BYTES,
    MAX_REASON_BYTES,
    NOTIFY_TIMEOUT_SEC,
    OUTCOME_APPLIED,
    PRODUCER_MATCH,
    RECONCILE_DIFF_UNSPECIFIED,
    ROLE_BLOCKER_ITEM,
    ROLE_FENCE_TIMEOUT_SEC,
    RoleFenceRow,
    STATE_PENDING,
    STATE_SETTLED_UNKNOWN,
    STATE_UNSPECIFIED,
    STATE_VOIDED,
    StreamRow,
    TERMINAL_STATES,
    VALID_PRODUCERS,
    VALID_STATES,
    VALID_VOID_MODES,
    VALID_VOID_REASONS,
    VOID_MODE_FORCE_MISMATCH,
    VoidParams,
    ack_outcome_reason,
    build_audit_row,
    clamp_limit,
    content_sha256,
    decide_ack,
    decide_enqueue_locked,
    decide_fence,
    decide_force_void,
    decide_void,
    decided_through,
    fence_allows,
    fence_reject_tip,
    fence_writes,
    find_cursor,
    issued_within_window,
    next_seq,
    raise_next_seq,
    reconcile_diff,
    result_for_existing,
    row_shard,
    subtract_open_count,
    valid_account,
    valid_admission,
    valid_ledger,
    valid_role_fence_state,
    validate_ack_reports,
    validate_text,
    void_outcome,
)
from .notify import InboxNotifier, NotifyResult
from .registry import InboxRegistry

logger = logging.getLogger(__name__)

_DATA_SERVICE_PREFIX = "/data_service.DataService/"
_UNMAPPED_HOME_ZONE_TEXT = "no home zone mapping"
_ITEM_GAIN_SANCTION_TYPES = (MSG_GM_BLOCK_ITEM_GAIN, MSG_GM_UNBLOCK_ITEM_GAIN)

# 启动期表与核心列自检清单（与 Go player_inbox/internal/inbox/store.go VerifySchema 严格对齐）。
INBOX_REQUIRED_SCHEMA: dict[str, tuple[str, ...]] = {
    "player_inbox_stream": ("shard", "player_id", "domain", "next_seq", "fence_epoch"),
    "player_inbox_item": ("shard", "player_id", "domain", "seq", "state", "owner_account", "owner_revision"),
    "player_inbox_audit": ("shard", "player_id", "domain", "seq", "operator"),
}


async def verify_inbox_schema(cursor) -> None:
    """启动期只读核验 player_inbox 三张权威表与核心列存在性（缺表缺列立即拒启，提示先跑 -migrate）。"""
    await cursor.execute(
        "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() "
        "AND TABLE_NAME IN ('player_inbox_stream', 'player_inbox_item', 'player_inbox_audit')"
    )
    found: dict[str, set[str]] = {}
    for table_name, column_name in await cursor.fetchall():
        tname = table_name.decode("utf-8") if isinstance(table_name, (bytes, bytearray)) else str(table_name)
        cname = column_name.decode("utf-8") if isinstance(column_name, (bytes, bytearray)) else str(column_name)
        found.setdefault(tname.lower(), set()).add(cname.lower())

    missing_tables = [t for t in INBOX_REQUIRED_SCHEMA if t not in found]
    if missing_tables:
        raise StoreUnavailableError(
            f"player_inbox schema check failed (run player_inbox -migrate first): missing tables {missing_tables}"
        )
    for table_name, required_cols in INBOX_REQUIRED_SCHEMA.items():
        cols = found[table_name]
        missing_cols = [c for c in required_cols if c not in cols]
        if missing_cols:
            raise StoreUnavailableError(
                f"player_inbox schema check failed (run player_inbox -migrate first): "
                f"table {table_name} missing columns {missing_cols}"
            )

T = TypeVar("T")


class _TxRollbackReturn(Exception):
    """在事务块内显式回滚并返回业务结果的内部控制流信号。"""

    def __init__(self, value) -> None:
        super().__init__("rollback_return")
        self.value = value


class DataServiceDirectory(TypingProtocol):
    """出站 data_service 依赖协议（供单测与生产 gRPC 客户端共用）。"""

    async def player_exists(self, player_id: int) -> bool: ...

    async def get_role_fence_state(self, player_id: int) -> Message | None: ...

    async def verify_role_admission(self, admission: Message) -> bool: ...

    async def read_inbox_ledger(self, player_id: int) -> tuple[Message | None, bool]: ...

    async def read_item_gain_block(self, player_id: int) -> tuple[Message | None, bool]: ...


class GrpcDataServiceDirectory:
    """生产出站 data_service 客户端，对 GetRoleFenceState / VerifyRoleAdmission 以 player_inbox 签名。"""

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
        self._role_fence_secret = role_fence_secret
        self._clock_ms = clock_ms

    async def player_exists(self, player_id: int) -> bool:
        req = self._protocol.message("data_service.GetPlayerHomeZoneRequest", player_id=player_id)
        try:
            raw = await self._call(
                _DATA_SERVICE_PREFIX + "GetPlayerHomeZone",
                req.SerializeToString(deterministic=True),
                timeout=HOME_ZONE_TIMEOUT_SEC,
            )
        except grpc.aio.AioRpcError as exc:
            if exc.code() == grpc.StatusCode.NOT_FOUND:
                return False
            if exc.code() == grpc.StatusCode.UNKNOWN and _UNMAPPED_HOME_ZONE_TEXT in (exc.details() or ""):
                return False
            raise StoreUnavailableError(f"GetPlayerHomeZone failed player={player_id}: {exc}") from exc
        resp = self._protocol.parse("data_service.GetPlayerHomeZoneResponse", raw)
        return int(resp.home_zone_id) != 0

    async def get_role_fence_state(self, player_id: int) -> Message | None:
        path = _DATA_SERVICE_PREFIX + "GetRoleFenceState"
        req = self._protocol.message("data_service.GetRoleFenceStateRequest", player_id=player_id)
        sign_service_call(path, req, "player_inbox", self._clock_ms(), self._role_fence_secret)
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

    async def verify_role_admission(self, admission: Message) -> bool:
        path = _DATA_SERVICE_PREFIX + "VerifyRoleAdmission"
        req = self._protocol.message("data_service.VerifyRoleAdmissionRequest", admission=admission)
        sign_service_call(path, req, "player_inbox", self._clock_ms(), self._role_fence_secret)
        try:
            raw = await self._call(path, req.SerializeToString(deterministic=True), timeout=ROLE_FENCE_TIMEOUT_SEC)
        except Exception as exc:
            raise RoleFenceUnavailableError(f"VerifyRoleAdmission failed: {exc}") from exc
        resp = self._protocol.parse("data_service.VerifyRoleAdmissionResponse", raw)
        if resp.retry:
            raise RoleFenceUnavailableError("VerifyRoleAdmission requested retry")
        return bool(resp.valid)

    async def read_inbox_ledger(self, player_id: int) -> tuple[Message | None, bool]:
        req = self._protocol.message("data_service.GetPlayerInboxLedgerRequest", player_id=player_id)
        try:
            raw = await self._call(
                _DATA_SERVICE_PREFIX + "GetPlayerInboxLedger",
                req.SerializeToString(deterministic=True),
                timeout=LEDGER_READ_TIMEOUT_SEC,
            )
        except Exception as exc:
            raise StoreUnavailableError(f"GetPlayerInboxLedger failed player={player_id}: {exc}") from exc
        resp = self._protocol.parse("data_service.GetPlayerInboxLedgerResponse", raw)
        if not resp.found:
            return None, False
        return resp.ledger, True

    async def read_item_gain_block(self, player_id: int) -> tuple[Message | None, bool]:
        req = self._protocol.message("data_service.GetPlayerItemGainBlockRequest", player_id=player_id)
        try:
            raw = await self._call(
                _DATA_SERVICE_PREFIX + "GetPlayerItemGainBlock",
                req.SerializeToString(deterministic=True),
                timeout=LEDGER_READ_TIMEOUT_SEC,
            )
        except Exception as exc:
            raise StoreUnavailableError(f"GetPlayerItemGainBlock failed player={player_id}: {exc}") from exc
        resp = self._protocol.parse("data_service.GetPlayerItemGainBlockResponse", raw)
        if not resp.found:
            return None, False
        return resp.block, True


class TxHandle(TypingProtocol):
    """单事务内底层存储原语协议（SqlDriver 与 MemoryDriver 共享同一接口）。"""

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None: ...

    async def apply_role_fence(self, state: Message, now_ms: int) -> bool: ...

    async def has_pending_item_any_domain(self, shard: int, player_id: int) -> bool: ...

    async def has_mismatched_pending_original(
        self, shard: int, player_id: int, account: str, owner_revision: int
    ) -> bool: ...

    async def ensure_stream(self, shard: int, player_id: int, domain: int, now_ms: int) -> None: ...

    async def lock_stream(self, shard: int, player_id: int, domain: int) -> StreamRow | None: ...

    async def update_stream_enqueue(
        self, shard: int, player_id: int, domain: int, next_seq: int, open_count: int, notify_due_ms: int, now_ms: int
    ) -> None: ...

    async def update_stream_fetch(
        self, shard: int, player_id: int, domain: int, fence_epoch: int, next_seq: int, now_ms: int
    ) -> None: ...

    async def update_stream_ack(
        self, shard: int, player_id: int, domain: int, open_count: int, fence_epoch: int, now_ms: int
    ) -> None: ...

    async def update_stream_open_count(
        self, shard: int, player_id: int, domain: int, open_count: int, now_ms: int
    ) -> None: ...

    async def read_item_by_request(
        self, shard: int, player_id: int, producer_kind: int, request_id: int
    ) -> ItemRow | None: ...

    async def read_item(self, shard: int, player_id: int, domain: int, seq: int) -> ItemRow | None: ...

    async def read_items_by_seq(
        self, shard: int, player_id: int, domain: int, seqs: tuple[int, ...]
    ) -> dict[int, ItemRow]: ...

    async def query_pending_items(self, shard: int, player_id: int, domain: int, limit: int) -> list[ItemRow]: ...

    async def insert_item(self, row: ItemRow) -> None: ...

    async def mark_item_delivered(
        self, shard: int, player_id: int, domain: int, seq: int, epoch: int, now_ms: int
    ) -> None: ...

    async def finalize_item(
        self,
        shard: int,
        player_id: int,
        domain: int,
        seq: int,
        new_state: int,
        finalizer: int,
        decided_epoch: int,
        outcome_reason: int,
        now_ms: int,
    ) -> None: ...

    async def void_item_direct(
        self, shard: int, player_id: int, domain: int, seq: int, void_reason: int, now_ms: int
    ) -> None: ...

    async def attach_void_directive(
        self, shard: int, player_id: int, domain: int, seq: int, void_reason: int, now_ms: int
    ) -> None: ...

    async def force_void_item(
        self, shard: int, player_id: int, domain: int, seq: int, void_reason: int, request_id: int, now_ms: int
    ) -> None: ...

    async def insert_audit(self, row: AuditRow) -> None: ...

    async def purge_player_rows(self, shard: int, player_id: int) -> None: ...


class InboxDriver(TypingProtocol):
    """存储驱动协议：提供事务封装与非事务点读/簿记更新。"""

    async def run_tx(self, work: Callable[[TxHandle], Awaitable[T]]) -> T: ...

    async def apply_role_fence_direct(self, state: Message, now_ms: int) -> bool: ...

    async def clear_notify_due(self, shard: int, player_id: int, domain: int, expect_next_seq: int) -> bool: ...

    async def read_streams_for_player(self, shard: int, player_id: int) -> list[StreamRow]: ...

    async def read_stream(self, shard: int, player_id: int, domain: int) -> StreamRow | None: ...

    async def read_pending_aggregates(self, shard: int, player_id: int) -> dict[int, tuple[int, int]]: ...

    async def list_items(
        self, shard: int, player_id: int, domain: int, state: int, after_seq: int, limit: int
    ) -> list[ItemRow]: ...

    async def read_item_by_request_direct(
        self, shard: int, player_id: int, producer_kind: int, request_id: int
    ) -> ItemRow | None: ...

    async def list_player_items_for_reconcile(self, shard: int, player_id: int, domain: int) -> list[ItemRow]: ...

    async def read_pending_item_gain_sanctions(
        self, shard: int, player_id: int, domain: int, msg_types: tuple[int, ...], limit: int
    ) -> list[ItemRow]: ...


class InboxStore(TypingProtocol):
    """收件箱业务存储协议（MemoryInboxStore 与 SqlInboxStore 的公共契约）。"""

    registry: InboxRegistry

    async def enqueue(self, params: EnqueueParams, *, audit: AuditInput | None = None) -> EnqueueResult: ...

    async def enqueue_battle_settlement(self, req: Message) -> Message: ...

    async def lookup_battle_settlement(self, req: Message) -> Message: ...

    async def fetch(self, req: Message, *, peer_addr: str = "unknown") -> Message: ...

    async def ack(self, req: Message, *, peer_addr: str = "unknown") -> Message: ...

    async def void_item(self, params: VoidParams) -> Message: ...

    async def summary(self, player_id: int) -> Message: ...

    async def list_items(self, req: Message) -> Message: ...

    async def lookup_by_request(self, req: Message) -> Message: ...

    async def reconcile_player(self, req: Message) -> Message: ...

    async def item_gain_blocks(self, player_id: int) -> Message: ...

    async def prepare_role_freeze(self, state: Message, *, dry_run: bool = False) -> Message: ...

    async def apply_role_fence(self, state: Message) -> Message: ...

    async def purge_removed_role(self, state: Message) -> Message: ...


def _set_tip(resp: Message, code: int) -> None:
    if code:
        resp.error_message.id = code


def _require_admission_original(admission: Message, fence: RoleFenceRow) -> None:
    if int(admission.player_id) != fence.player_id or int(admission.owner_revision) != fence.owner_revision:
        raise OriginalRoleError("admission owner_revision mismatch")


def _require_item_original(row: ItemRow | None, fence: RoleFenceRow) -> None:
    if row is None:
        return
    if (
        row.player_id != fence.player_id
        or row.owner_revision == 0
        or row.owner_revision != fence.owner_revision
        or row.owner_account.encode("utf-8") != fence.account.encode("utf-8")
    ):
        raise OriginalRoleError("item original owner mismatch")


def _battle_original_matches(req: Message, fence: RoleFenceRow) -> bool:
    return (
        int(req.player_id) == fence.player_id
        and req.expected_owner_account == fence.account
        and int(req.expected_owner_revision) == fence.owner_revision
    )


class InboxStoreCore:
    """收件箱业务编排核心：统一驱动入箱、消费、确认、作废、对账与角色护栏。"""

    def __init__(
        self,
        protocol: Protocol,
        registry: InboxRegistry,
        directory: DataServiceDirectory,
        driver: InboxDriver,
        *,
        allowed_types: frozenset[int] = frozenset(),
        notifier: InboxNotifier | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        registry.check_allowlist(allowed_types)
        self.protocol = protocol
        self.registry = registry
        self.directory = directory
        self.driver = driver
        self.allowed_types = frozenset(allowed_types)
        self.notifier = notifier
        self._clock_ms = clock_ms

    def _now_ms(self) -> int:
        ms = int(self._clock_ms())
        return ms if ms > 0 else 0

    def _item_view(self, row: ItemRow) -> Message:
        return self.protocol.message(
            "player_inbox.PlayerInboxItemView",
            domain=row.domain,
            seq=row.seq,
            msg_type=row.msg_type,
            producer_kind=row.producer_kind,
            request_id=row.request_id,
            payload=row.payload,
            state=row.state,
            directive=row.directive,
            void_reason=row.void_reason,
            delivery_count=row.delivery_count,
            last_delivered_epoch=row.last_delivered_epoch,
            last_delivered_ms=row.last_delivered_ms,
            outcome_reason=row.outcome_reason,
            finalized_by=row.finalized_by,
            decided_epoch=row.decided_epoch,
            issued_at_ms=row.issued_at_ms,
            created_ms=row.created_ms,
            updated_ms=row.updated_ms,
            operator_name=row.operator_name,
            note=row.note,
        )

    async def _heal_role_fence(self, players: tuple[int, ...]) -> None:
        missing: list[int] = []
        now_ms = self._now_ms()
        for player_id in players:
            state = await self.directory.get_role_fence_state(player_id)
            if state is None:
                missing.append(player_id)
                continue
            await self.driver.apply_role_fence_direct(state, now_ms)
        if missing:
            raise RoleNotFoundError(f"roles not found in directory: {missing}")

    async def _with_player_tx(
        self,
        player_id: int,
        action: Callable[[TxHandle, RoleFenceRow], Awaitable[T]],
    ) -> T:
        """锁 role_fence 副本并执行事务；副本缺行或冻结时回滚后向目录自愈并重试一次。"""
        if player_id <= 0:
            raise OriginalRoleError("player_id must be non-zero")

        async def run_once() -> T:
            async def tx_work(tx: TxHandle) -> T:
                fence = await tx.lock_role_fence(player_id)
                if fence is None:
                    raise RoleFenceStaleError((player_id,))
                if fence.freeze_token != 0:
                    raise RoleFenceStaleError((player_id,))
                if not fence.account or not valid_account(fence.account) or fence.owner_revision == 0:
                    raise OriginalRoleError("role deleted or invalid ownership in role_fence")
                return await action(tx, fence)

            return await self.driver.run_tx(tx_work)

        try:
            return await run_once()
        except RoleFenceStaleError as stale:
            try:
                await self._heal_role_fence(stale.players)
            except RoleNotFoundError as exc:
                raise OriginalRoleError("role not found in directory") from exc
            try:
                return await run_once()
            except RoleFenceStaleError as exc:
                raise OriginalRoleError("role fence still frozen or missing after heal") from exc

    async def _notify_and_clear(self, player_id: int, domain: int, expect_next_seq: int) -> NotifyResult:
        if self.notifier is None:
            return NotifyResult.ERROR
        try:
            async with asyncio.timeout(NOTIFY_TIMEOUT_SEC):
                result = await self.notifier.notify(player_id, domain)
        except Exception:
            logger.info("player_inbox post-commit notify timed out player=%d domain=%d", player_id, domain)
            return NotifyResult.ERROR
        if not result.settled:
            return result
        try:
            await self.driver.clear_notify_due(row_shard(player_id), player_id, domain, expect_next_seq)
        except Exception:
            logger.error(
                "player_inbox notify settled but clearing notify_due failed player=%d domain=%d",
                player_id,
                domain,
                exc_info=True,
            )
            return NotifyResult.ERROR
        return result

    # -- 入箱与 Battle 生产面 -------------------------------------------------------------------------

    async def enqueue(self, params: EnqueueParams, *, audit: AuditInput | None = None) -> EnqueueResult:
        if (
            params.player_id == 0
            or params.request_id == 0
            or params.issued_at_ms == 0
            or params.producer_kind not in VALID_PRODUCERS
        ):
            return EnqueueResult(code=ERR_INVALID_PARAMETER)
        is_battle = params.producer_kind == PRODUCER_MATCH or params.msg_type == MSG_BATTLE_SETTLEMENT
        if is_battle != (params.battle_original is not None):
            return EnqueueResult(code=ERR_INVALID_PARAMETER)

        entry, code = self.registry.validate_payload(params.msg_type, self.allowed_types, params.payload or b"")
        if code != 0 or entry is None:
            return EnqueueResult(code=code)
        code = validate_text(params.operator, MAX_OPERATOR_BYTES, required=True)
        if code != 0:
            return EnqueueResult(code=code)
        code = validate_text(params.note, MAX_REASON_BYTES, required=False)
        if code != 0:
            return EnqueueResult(code=code)

        domain = entry.domain
        domain_spec = self.registry.domain_spec(domain)
        if domain_spec is None:
            raise StoreUnavailableError(f"domain {domain} spec missing")

        exists = await self.directory.player_exists(params.player_id)
        if not exists:
            return EnqueueResult(code=ERR_PLAYER_NOT_FOUND)

        shard = row_shard(params.player_id)
        payload = params.payload or b""
        digest = content_sha256(params.msg_type, params.issued_at_ms, payload)

        async def work(tx: TxHandle, fence: RoleFenceRow) -> EnqueueResult:
            if params.battle_original is not None and not _battle_original_matches(params.battle_original, fence):
                raise BattleSettlementOriginalError("battle original account or revision mismatch")
            existing = await tx.read_item_by_request(shard, params.player_id, params.producer_kind, params.request_id)
            if existing is not None:
                try:
                    _require_item_original(existing, fence)
                except OriginalRoleError as exc:
                    if params.battle_original is not None:
                        raise BattleSettlementOriginalError("battle permanent receipt owner mismatch") from exc
                    raise
                # 幂等判重先于时间窗与容量检查（IB-2）。
                return result_for_existing(existing, digest)

            now_ms = self._now_ms()
            if params.battle_original is None:
                if not issued_within_window(params.issued_at_ms, now_ms):
                    return EnqueueResult(code=ERR_ISSUED_OUT_OF_WINDOW)
            elif params.issued_at_ms > now_ms + ISSUED_AT_MAX_FUTURE_MS:
                return EnqueueResult(code=ERR_ISSUED_OUT_OF_WINDOW)

            if await tx.has_mismatched_pending_original(shard, params.player_id, fence.account, fence.owner_revision):
                raise OriginalRoleError("existing pending items belong to another original owner")

            await tx.ensure_stream(shard, params.player_id, domain, now_ms)
            stream = await tx.lock_stream(shard, params.player_id, domain)
            if stream is None:
                raise StoreUnavailableError("stream row vanished after ensure_stream")

            existing = await tx.read_item_by_request(shard, params.player_id, params.producer_kind, params.request_id)
            _require_item_original(existing, fence)
            decided, proceed = decide_enqueue_locked(existing, digest, stream.open_count, domain_spec.max_pending)
            if not proceed:
                return decided

            tx_now_ms = self._now_ms()
            seq = next_seq(stream.next_seq, tx_now_ms)
            item = ItemRow(
                shard=shard,
                player_id=params.player_id,
                domain=domain,
                seq=seq,
                msg_type=params.msg_type,
                producer_kind=params.producer_kind,
                request_id=params.request_id,
                content_sha256=digest,
                payload=payload,
                state=STATE_PENDING,
                directive=DIRECTIVE_NONE,
                void_reason=0,
                delivery_count=0,
                last_delivered_epoch=0,
                last_delivered_ms=0,
                outcome_reason=0,
                finalized_by=0,
                decided_epoch=0,
                issued_at_ms=params.issued_at_ms,
                created_ms=tx_now_ms,
                updated_ms=tx_now_ms,
                operator_name=params.operator,
                note=params.note,
                owner_account=fence.account,
                owner_revision=fence.owner_revision,
            )
            await tx.insert_item(item)
            await tx.update_stream_enqueue(
                shard, params.player_id, domain, seq + 1, stream.open_count + 1, tx_now_ms, tx_now_ms
            )
            if audit is not None:
                await tx.insert_audit(build_audit_row(item, audit, tx_now_ms))
            return EnqueueResult(seq=seq, created=True, state=STATE_PENDING)

        res = await self._with_player_tx(params.player_id, work)
        if res.created:
            notify_res = await self._notify_and_clear(params.player_id, domain, res.seq + 1)
            if not notify_res.settled:
                logger.info(
                    "player_inbox post-commit notify not settled player=%d domain=%d seq=%d result=%s",
                    params.player_id,
                    domain,
                    res.seq,
                    notify_res.name,
                )
        return res

    async def enqueue_battle_settlement(self, req: Message) -> Message:
        self.registry.validate_battle_original(req)
        params = EnqueueParams(
            player_id=int(req.player_id),
            msg_type=MSG_BATTLE_SETTLEMENT,
            producer_kind=PRODUCER_MATCH,
            request_id=int(req.battle_id),
            issued_at_ms=int(req.issued_at_ms),
            payload=bytes(req.settlement_payload),
            operator="match",
            battle_original=req,
        )
        res = await self.enqueue(params)
        if res.code == ERR_IDEMPOTENCY_CONFLICT:
            raise BattleSettlementOriginalError("battle settlement idempotency conflict")
        resp = self.protocol.message(
            "player_inbox.BattleSettlementInboxResponse",
            player_id=int(req.player_id),
            battle_id=int(req.battle_id),
            seq=res.seq,
            state=res.state,
            content_sha256=content_sha256(MSG_BATTLE_SETTLEMENT, int(req.issued_at_ms), bytes(req.settlement_payload)),
        )
        _set_tip(resp, res.code)
        return resp

    async def lookup_battle_settlement(self, req: Message) -> Message:
        self.registry.validate_battle_original(req)
        player_id = int(req.player_id)
        battle_id = int(req.battle_id)
        payload = bytes(req.settlement_payload)
        issued_at_ms = int(req.issued_at_ms)
        digest = content_sha256(MSG_BATTLE_SETTLEMENT, issued_at_ms, payload)
        shard = row_shard(player_id)

        async def work(tx: TxHandle, fence: RoleFenceRow) -> tuple[int, int, bytes]:
            if not _battle_original_matches(req, fence):
                raise BattleSettlementOriginalError("battle original account or revision mismatch")
            row = await tx.read_item_by_request(shard, player_id, PRODUCER_MATCH, battle_id)
            if row is None:
                return 0, STATE_UNSPECIFIED, b""
            try:
                _require_item_original(row, fence)
            except OriginalRoleError as exc:
                raise BattleSettlementOriginalError("battle permanent receipt owner mismatch") from exc
            if (
                row.msg_type != MSG_BATTLE_SETTLEMENT
                or row.content_sha256 != digest
                or row.payload != payload
                or row.issued_at_ms != issued_at_ms
            ):
                raise BattleSettlementOriginalError("battle permanent receipt content mismatch")
            return row.seq, row.state, row.content_sha256

        seq, state, stored_digest = await self._with_player_tx(player_id, work)
        return self.protocol.message(
            "player_inbox.BattleSettlementInboxResponse",
            player_id=player_id,
            battle_id=battle_id,
            seq=seq,
            state=state,
            content_sha256=stored_digest,
        )

    # -- Scene 消费与确认 -----------------------------------------------------------------------------

    async def _verify_scene_admission(self, player_id: int, req_epoch: int, admission: Message) -> None:
        if int(admission.player_id) != player_id or int(admission.owner_epoch) != req_epoch:
            raise OriginalRoleError("admission identity does not match request")
        if not valid_admission(admission):
            raise OriginalRoleError("admission identity fields invalid")
        if not await self.directory.verify_role_admission(admission):
            raise OriginalRoleError("role admission rejected by directory")

    async def fetch(self, req: Message, *, peer_addr: str = "unknown") -> Message:
        player_id = int(req.player_id)
        domain = int(req.domain)
        correlation_id = int(req.correlation_id)
        resp = self.protocol.message(
            "player_inbox.FetchPlayerInboxResponse",
            player_id=player_id,
            domain=domain,
            correlation_id=correlation_id,
        )
        if self.registry.domain_spec(domain) is None or player_id == 0:
            _set_tip(resp, ERR_INVALID_PARAMETER)
            return resp
        req_epoch = int(req.owner_epoch)
        if req_epoch == 0:
            resp.fence = FENCE_EPOCH_REQUIRED
            _set_tip(resp, fence_reject_tip(FENCE_EPOCH_REQUIRED))
            logger.error(
                "player_inbox fetch rejected EPOCH_REQUIRED player=%d domain=%d peer=%s scene_node_id=%d",
                player_id,
                domain,
                peer_addr,
                int(req.scene_node_id),
            )
            return resp

        await self._verify_scene_admission(player_id, req_epoch, req.admission)
        shard = row_shard(player_id)
        max_items = clamp_limit(int(req.max_items), FETCH_DEFAULT_ITEMS, FETCH_MAX_ITEMS)
        decided_through_seq = int(req.consumer_decided_through)

        async def work(tx: TxHandle, fence: RoleFenceRow):
            _require_admission_original(req.admission, fence)
            if await tx.has_mismatched_pending_original(shard, player_id, fence.account, fence.owner_revision):
                raise OriginalRoleError("pending item owner mismatch during fetch")
            stream = await tx.lock_stream(shard, player_id, domain)
            if stream is None:
                return FENCE_NO_STREAM, 0, [], False, False, 0

            locked_fence = stream.fence_epoch
            verdict = decide_fence(req_epoch, locked_fence, req_epoch)
            if not fence_allows(verdict):
                return verdict, locked_fence, [], False, False, 0

            now_ms = self._now_ms()
            new_next_seq, raised = raise_next_seq(stream.next_seq, decided_through_seq)
            rows = await tx.query_pending_items(shard, player_id, domain, max_items + 1)
            has_more = len(rows) > max_items
            page = rows[:max_items]
            delivered: list[Message] = []
            for row in page:
                _require_item_original(row, fence)
                await tx.mark_item_delivered(shard, player_id, domain, row.seq, req_epoch, now_ms)
                item_msg = self.protocol.message(
                    "player_inbox.PlayerInboxItem",
                    seq=row.seq,
                    msg_type=row.msg_type,
                    # directive != NONE 时剥离载荷，确保作废路径不解码毒载荷（§7.9）
                    payload=row.payload if row.directive == DIRECTIVE_NONE else b"",
                    directive=row.directive,
                    void_reason=row.void_reason,
                    request_id=row.request_id,
                    delivery_count=row.delivery_count + 1,
                    created_ms=row.created_ms,
                )
                delivered.append(item_msg)

            if fence_writes(verdict) or raised:
                new_fence = req_epoch if fence_writes(verdict) else locked_fence
                await tx.update_stream_fetch(shard, player_id, domain, new_fence, new_next_seq, now_ms)
            return verdict, locked_fence, delivered, has_more, raised, new_next_seq

        verdict, locked_fence, items, has_more, cursor_ahead, raised_to = await self._with_player_tx(player_id, work)
        resp.fence = verdict
        if verdict == FENCE_NO_STREAM:
            return resp
        if not fence_allows(verdict):
            _set_tip(resp, fence_reject_tip(verdict))
            logger.info(
                "player_inbox fetch fence rejected verdict=%d player=%d domain=%d req_epoch=%d fence=%d peer=%s",
                verdict,
                player_id,
                domain,
                req_epoch,
                locked_fence,
                peer_addr,
            )
            return resp
        for item in items:
            resp.items.append(item)
            if item.delivery_count >= DELIVERY_WARN_THRESHOLD:
                logger.error(
                    "player_inbox item delivered repeatedly count=%d player=%d domain=%d seq=%d msg_type=%d",
                    item.delivery_count,
                    player_id,
                    domain,
                    item.seq,
                    item.msg_type,
                )
        resp.has_more = has_more
        if cursor_ahead:
            logger.error(
                "player_inbox cursor ahead player=%d domain=%d consumer_decided_through=%d raised_next_seq=%d",
                player_id,
                domain,
                decided_through_seq,
                raised_to,
            )
        return resp

    async def ack(self, req: Message, *, peer_addr: str = "unknown") -> Message:
        player_id = int(req.player_id)
        domain = int(req.domain)
        correlation_id = int(req.correlation_id)
        resp = self.protocol.message(
            "player_inbox.AckPlayerInboxResponse",
            player_id=player_id,
            domain=domain,
            correlation_id=correlation_id,
        )
        if (
            self.registry.domain_spec(domain) is None
            or player_id == 0
            or not validate_ack_reports(req.decisions, ACK_MAX_DECISIONS)
        ):
            _set_tip(resp, ERR_INVALID_PARAMETER)
            return resp
        req_epoch = int(req.owner_epoch)
        if req_epoch == 0:
            resp.fence = FENCE_EPOCH_REQUIRED
            _set_tip(resp, fence_reject_tip(FENCE_EPOCH_REQUIRED))
            logger.error(
                "player_inbox ack rejected EPOCH_REQUIRED player=%d domain=%d peer=%s scene_node_id=%d",
                player_id,
                domain,
                peer_addr,
                int(req.scene_node_id),
            )
            return resp

        await self._verify_scene_admission(player_id, req_epoch, req.admission)
        shard = row_shard(player_id)
        seqs = tuple(int(report.seq) for report in req.decisions)

        async def work(tx: TxHandle, fence: RoleFenceRow):
            _require_admission_original(req.admission, fence)
            if await tx.has_mismatched_pending_original(shard, player_id, fence.account, fence.owner_revision):
                raise OriginalRoleError("pending item owner mismatch during ack")
            stream = await tx.lock_stream(shard, player_id, domain)
            if stream is None:
                unknown_results = [
                    self.protocol.message(
                        "player_inbox.PlayerInboxAckItem",
                        seq=int(report.seq),
                        result=ACK_UNKNOWN_SEQ,
                    )
                    for report in req.decisions
                ]
                return FENCE_NO_STREAM, 0, unknown_results

            locked_fence = stream.fence_epoch
            verdict = decide_fence(req_epoch, locked_fence, req_epoch)
            if not fence_allows(verdict):
                return verdict, locked_fence, []

            rows = await tx.read_items_by_seq(shard, player_id, domain, seqs)
            now_ms = self._now_ms()
            finalized = 0
            results: list[Message] = []
            for report in req.decisions:
                row = rows.get(int(report.seq))
                _require_item_original(row, fence)
                # Battle 永久原收据只认 APPLIED，拒绝 REJECTED/SKIPPED/UNKNOWN 销账。
                if row is not None and row.msg_type == MSG_BATTLE_SETTLEMENT and int(report.outcome) != OUTCOME_APPLIED:
                    results.append(
                        self.protocol.message(
                            "player_inbox.PlayerInboxAckItem",
                            seq=int(report.seq),
                            result=ACK_CONFLICT,
                            state=row.state,
                        )
                    )
                    continue
                decision = decide_ack(row, report)
                if decision.finalize:
                    await tx.finalize_item(
                        shard,
                        player_id,
                        domain,
                        int(report.seq),
                        decision.new_state,
                        FINALIZER_CONSUMER,
                        req_epoch,
                        ack_outcome_reason(report),
                        now_ms,
                    )
                    finalized += 1
                results.append(
                    self.protocol.message(
                        "player_inbox.PlayerInboxAckItem",
                        seq=int(report.seq),
                        result=decision.result,
                        state=decision.state,
                    )
                )

            if finalized > 0 or fence_writes(verdict):
                new_open, underflow = subtract_open_count(stream.open_count, finalized)
                if underflow:
                    logger.error(
                        "player_inbox open_count underflow player=%d domain=%d open_count=%d finalized=%d",
                        player_id,
                        domain,
                        stream.open_count,
                        finalized,
                    )
                new_fence = req_epoch if fence_writes(verdict) else locked_fence
                await tx.update_stream_ack(shard, player_id, domain, new_open, new_fence, now_ms)
            return verdict, locked_fence, results

        verdict, locked_fence, results = await self._with_player_tx(player_id, work)
        resp.fence = verdict
        if verdict == FENCE_NO_STREAM:
            for item in results:
                resp.results.append(item)
            return resp
        if not fence_allows(verdict):
            _set_tip(resp, fence_reject_tip(verdict))
            logger.info(
                "player_inbox ack fence rejected verdict=%d player=%d domain=%d req_epoch=%d fence=%d peer=%s",
                verdict,
                player_id,
                domain,
                req_epoch,
                locked_fence,
                peer_addr,
            )
            return resp
        for item in results:
            resp.results.append(item)
            if item.result == ACK_FINALIZED and item.state == STATE_SETTLED_UNKNOWN:
                logger.error(
                    "player_inbox item settled as SETTLED_UNKNOWN player=%d domain=%d seq=%d epoch=%d peer=%s",
                    player_id,
                    domain,
                    item.seq,
                    req_epoch,
                    peer_addr,
                )
        return resp

    # -- 运维接口 -------------------------------------------------------------------------------------

    async def void_item(self, params: VoidParams) -> Message:
        resp = self.protocol.message("player_inbox.VoidPlayerInboxItemResponse")
        # Battle 结算域永不可作废，只允许消费方真实 APPLIED 终结。
        if params.domain == DOMAIN_BATTLE_SETTLEMENT:
            _set_tip(resp, ERR_ITEM_STATE_CONFLICT)
            return resp
        if (
            self.registry.domain_spec(params.domain) is None
            or params.player_id == 0
            or params.seq == 0
            or params.reason not in VALID_VOID_REASONS
            or params.mode not in VALID_VOID_MODES
        ):
            _set_tip(resp, ERR_INVALID_PARAMETER)
            return resp
        code = validate_text(params.operator, MAX_OPERATOR_BYTES, required=True)
        if code != 0:
            _set_tip(resp, code)
            return resp
        code = validate_text(params.note, MAX_REASON_BYTES, required=False)
        if code != 0:
            _set_tip(resp, code)
            return resp
        if params.mode == VOID_MODE_FORCE_MISMATCH:
            return await self._force_void(params)

        shard = row_shard(params.player_id)

        async def work(tx: TxHandle, fence: RoleFenceRow) -> tuple[str, int]:
            if await tx.has_mismatched_pending_original(shard, params.player_id, fence.account, fence.owner_revision):
                raise OriginalRoleError("pending item owner mismatch during void")
            stream = await tx.lock_stream(shard, params.player_id, params.domain)
            if stream is None:
                return "not_found", STATE_UNSPECIFIED
            row = await tx.read_item(shard, params.player_id, params.domain, params.seq)
            _require_item_original(row, fence)
            action = decide_void(row)
            if action == "not_found":
                return action, STATE_UNSPECIFIED
            if action == "terminal":
                return action, row.state

            now_ms = self._now_ms()
            if action == "direct":
                await tx.void_item_direct(shard, params.player_id, params.domain, params.seq, params.reason, now_ms)
                new_open, _ = subtract_open_count(stream.open_count, 1)
                await tx.update_stream_open_count(shard, params.player_id, params.domain, new_open, now_ms)
                state = STATE_VOIDED
            elif action == "directive":
                await tx.attach_void_directive(
                    shard, params.player_id, params.domain, params.seq, params.reason, now_ms
                )
                state = STATE_PENDING
            else:
                state = STATE_PENDING

            await tx.insert_audit(
                build_audit_row(
                    row,
                    AuditInput(
                        action=AUDIT_VOID,
                        operator=params.operator,
                        peer_addr=params.peer_addr,
                        reason=params.note,
                        void_reason=params.reason,
                    ),
                    now_ms,
                )
            )
            return action, state

        action, state = await self._with_player_tx(params.player_id, work)
        tip, resp_state, void_requested = void_outcome(action, state)
        _set_tip(resp, tip)
        resp.state = resp_state
        resp.void_requested = void_requested
        return resp

    async def _force_void(self, params: VoidParams) -> Message:
        resp = self.protocol.message("player_inbox.VoidPlayerInboxItemResponse")
        code = validate_text(params.note, MAX_REASON_BYTES, required=True)
        if code != 0:
            _set_tip(resp, code)
            return resp
        # 事务外读取已落盘账本，不在持行锁时等待 data_service。
        ledger, found = await self.directory.read_inbox_ledger(params.player_id)
        shard = row_shard(params.player_id)

        async def work(tx: TxHandle, fence: RoleFenceRow):
            if await tx.has_mismatched_pending_original(shard, params.player_id, fence.account, fence.owner_revision):
                raise OriginalRoleError("pending item owner mismatch during force void")
            stream = await tx.lock_stream(shard, params.player_id, params.domain)
            if stream is None:
                return "not_found", STATE_UNSPECIFIED, 0
            row = await tx.read_item(shard, params.player_id, params.domain, params.seq)
            _require_item_original(row, fence)
            action, proof = decide_force_void(row, ledger, found)
            if action != "forced":
                return action, (row.state if row is not None else STATE_UNSPECIFIED), proof.verdict

            now_ms = self._now_ms()
            await tx.force_void_item(
                shard, params.player_id, params.domain, params.seq, params.reason, row.request_id, now_ms
            )
            new_open, _ = subtract_open_count(stream.open_count, 1)
            await tx.update_stream_open_count(shard, params.player_id, params.domain, new_open, now_ms)
            await tx.insert_audit(
                build_audit_row(
                    row,
                    AuditInput(
                        action=AUDIT_FORCE_VOID,
                        operator=params.operator,
                        peer_addr=params.peer_addr,
                        reason=params.note,
                        void_reason=params.reason,
                    ),
                    now_ms,
                )
            )
            return "forced", STATE_VOIDED, FORCE_VOID_PROVEN

        action, state, verdict = await self._with_player_tx(params.player_id, work)
        tip, resp_state, void_requested = void_outcome(action, state)
        _set_tip(resp, tip)
        resp.state = resp_state
        resp.void_requested = void_requested
        resp.force_verdict = verdict
        return resp

    async def summary(self, player_id: int) -> Message:
        resp = self.protocol.message("player_inbox.GetPlayerInboxSummaryResponse", player_id=player_id)
        if player_id == 0:
            _set_tip(resp, ERR_INVALID_PARAMETER)
            return resp
        shard = row_shard(player_id)
        streams = await self.driver.read_streams_for_player(shard, player_id)
        aggs = await self.driver.read_pending_aggregates(shard, player_id)
        max_fence = 0
        for st in streams:
            delivered_unacked, oldest_created = aggs.get(st.domain, (0, 0))
            resp.streams.add(
                domain=st.domain,
                open_count=st.open_count,
                next_seq=st.next_seq,
                fence_epoch=st.fence_epoch,
                delivered_unacked_count=delivered_unacked,
                oldest_pending_created_ms=oldest_created,
                notify_due_ms=st.notify_due_ms,
            )
            if st.fence_epoch > max_fence:
                max_fence = st.fence_epoch
        resp.max_fence_epoch = max_fence
        return resp

    async def list_items(self, req: Message) -> Message:
        resp = self.protocol.message("player_inbox.ListPlayerInboxItemsResponse")
        player_id = int(req.player_id)
        domain = int(req.domain)
        state = int(req.state)
        if self.registry.domain_spec(domain) is None or player_id == 0:
            _set_tip(resp, ERR_INVALID_PARAMETER)
            return resp
        if state != STATE_UNSPECIFIED and state not in VALID_STATES:
            _set_tip(resp, ERR_INVALID_PARAMETER)
            return resp
        limit = clamp_limit(int(req.limit), LIST_DEFAULT_LIMIT, LIST_MAX_LIMIT)
        rows = await self.driver.list_items(
            row_shard(player_id), player_id, domain, state, int(req.after_seq), limit + 1
        )
        resp.has_more = len(rows) > limit
        for row in rows[:limit]:
            resp.items.append(self._item_view(row))
        return resp

    async def lookup_by_request(self, req: Message) -> Message:
        resp = self.protocol.message("player_inbox.LookupPlayerInboxResponse")
        player_id = int(req.player_id)
        producer_kind = int(req.producer_kind)
        request_id = int(req.request_id)
        if player_id == 0 or request_id == 0 or producer_kind not in VALID_PRODUCERS:
            _set_tip(resp, ERR_INVALID_PARAMETER)
            return resp
        row = await self.driver.read_item_by_request_direct(row_shard(player_id), player_id, producer_kind, request_id)
        if row is None:
            _set_tip(resp, ERR_ITEM_NOT_FOUND)
            return resp
        resp.item.CopyFrom(self._item_view(row))
        return resp

    async def reconcile_player(self, req: Message) -> Message:
        resp = self.protocol.message("player_inbox.ReconcilePlayerInboxResponse")
        player_id = int(req.player_id)
        domain = int(req.domain)
        if player_id == 0 or (domain != DOMAIN_UNSPECIFIED and self.registry.domain_spec(domain) is None):
            _set_tip(resp, ERR_INVALID_PARAMETER)
            return resp
        ledger, found = await self.directory.read_inbox_ledger(player_id)
        resp.ledger_found = found
        if not found:
            return resp
        if not valid_ledger(ledger):
            raise StoreUnavailableError(f"persisted inbox ledger structurally invalid for player={player_id}")

        shard = row_shard(player_id)
        rows = await self.driver.list_player_items_for_reconcile(shard, player_id, domain)
        for row in rows:
            diff_kind = reconcile_diff(row, find_cursor(ledger, row.domain))
            if diff_kind != RECONCILE_DIFF_UNSPECIFIED:
                resp.diffs.add(
                    domain=row.domain,
                    seq=row.seq,
                    kind=diff_kind,
                    state=row.state,
                    request_id=row.request_id,
                )

        domains = (domain,) if domain != DOMAIN_UNSPECIFIED else self.registry.domains()
        for dom in domains:
            stream = await self.driver.read_stream(shard, player_id, dom)
            dt = decided_through(ledger, dom)
            if stream is None and dt == 0:
                continue
            resp.cursors.add(
                domain=dom,
                ledger_decided_through=dt,
                inbox_next_seq=stream.next_seq if stream is not None else 0,
            )
        return resp

    async def item_gain_blocks(self, player_id: int) -> Message:
        resp = self.protocol.message("player_inbox.GetPlayerItemGainBlocksResponse", player_id=player_id)
        if player_id == 0:
            _set_tip(resp, ERR_INVALID_PARAMETER)
            return resp
        entry = self.registry.type_entry(MSG_GM_BLOCK_ITEM_GAIN)
        if entry is None:
            raise StoreUnavailableError("item gain sanction message type not registered")
        domain = entry.domain
        spec = self.registry.domain_spec(domain)
        if spec is None:
            raise StoreUnavailableError("item gain sanction domain spec missing")

        # 先读收件箱 PENDING，再读已落盘存档，避免在两次读之间发生落盘并确认导致漏检。
        rows = await self.driver.read_pending_item_gain_sanctions(
            row_shard(player_id), player_id, domain, _ITEM_GAIN_SANCTION_TYPES, spec.max_pending + 1
        )
        if len(rows) > spec.max_pending:
            raise StoreUnavailableError(
                f"player={player_id} pending item gain sanctions exceed max_pending={spec.max_pending}"
            )
        block, found = await self.directory.read_item_gain_block(player_id)
        resp.found = found
        if found and block is not None:
            resp.blocked_item_config_ids.extend(block.item_config_ids)
        for row in rows:
            item_config_id = 0
            try:
                parsed = self.protocol.parse("player_inbox.GmItemGainPayload", row.payload)
                item_config_id = int(parsed.item_config_id)
            except DecodeError:
                item_config_id = 0
            if item_config_id == 0:
                logger.error(
                    "player_inbox GetItemGainBlocks undecodable payload player=%d domain=%d seq=%d",
                    player_id,
                    row.domain,
                    row.seq,
                )
            resp.pending.add(
                seq=row.seq,
                msg_type=row.msg_type,
                item_config_id=item_config_id,
                request_id=row.request_id,
                directive=row.directive,
                delivery_count=row.delivery_count,
                created_ms=row.created_ms,
                operator_name=row.operator_name,
                note=row.note,
            )
        return resp

    # -- RoleFenceParticipant -------------------------------------------------------------------------

    async def prepare_role_freeze(self, state: Message, *, dry_run: bool = False) -> Message:
        if not valid_role_fence_state(state) or int(state.freeze_token) == 0 or bool(state.freeze_final):
            raise InvalidRoleFenceStateError("invalid RoleFenceState for PrepareRoleFreeze")
        player_id = int(state.player_id)
        shard = row_shard(player_id)
        now_ms = self._now_ms()

        async def work(tx: TxHandle) -> Message:
            if dry_run:
                has_pending = await tx.has_pending_item_any_domain(shard, player_id)
                blocker = ROLE_BLOCKER_ITEM if has_pending else ""
                raise _TxRollbackReturn(
                    self.protocol.message(
                        "rolefence.PrepareRoleFreezeResponse",
                        clean=(blocker == ""),
                        blocker=blocker,
                    )
                )
            await tx.apply_role_fence(state, now_ms)
            fence = await tx.lock_role_fence(player_id)
            if fence is None:
                raise StoreUnavailableError(f"role_fence missing after apply player={player_id}")
            if fence.version != int(state.version) or fence.freeze_token != int(state.freeze_token):
                raise _TxRollbackReturn(self.protocol.message("rolefence.PrepareRoleFreezeResponse", stale=True))
            if await tx.has_pending_item_any_domain(shard, player_id):
                raise _TxRollbackReturn(
                    self.protocol.message("rolefence.PrepareRoleFreezeResponse", blocker=ROLE_BLOCKER_ITEM)
                )
            return self.protocol.message("rolefence.PrepareRoleFreezeResponse", clean=True)

        return await self.driver.run_tx(work)

    async def apply_role_fence(self, state: Message) -> Message:
        if not valid_role_fence_state(state):
            raise InvalidRoleFenceStateError("invalid RoleFenceState for ApplyRoleFence")
        await self.driver.apply_role_fence_direct(state, self._now_ms())
        return self.protocol.message("rolefence.ApplyRoleFenceResponse")

    async def purge_removed_role(self, state: Message) -> Message:
        if not valid_role_fence_state(state) or state.account != "" or int(state.freeze_token) != 0:
            raise InvalidRoleFenceStateError("PurgeRemovedRole requires tombstone state")
        player_id = int(state.player_id)
        shard = row_shard(player_id)
        now_ms = self._now_ms()

        async def work(tx: TxHandle) -> Message:
            await tx.apply_role_fence(state, now_ms)
            fence = await tx.lock_role_fence(player_id)
            if fence is None or fence.account != "":
                raise StoreUnavailableError(f"role_fence is not tombstone player={player_id}")
            await tx.purge_player_rows(shard, player_id)
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse")

        return await self.driver.run_tx(work)


# ── 内存存储驱动与双胞胎实现 ────────────────────────────────────────────────────────────────────────────


class _MemoryState:
    """内存表快照容器，用于事务内原子提交与异常回滚。"""

    def __init__(self) -> None:
        self.role_fences: dict[int, RoleFenceRow] = {}
        self.streams: dict[tuple[int, int, int], StreamRow] = {}
        self.items: dict[tuple[int, int, int, int], ItemRow] = {}
        self.items_by_req: dict[tuple[int, int, int, int], tuple[int, int, int, int]] = {}
        self.audits: dict[tuple[int, int, int, int, int], AuditRow] = {}

    def clone(self) -> "_MemoryState":
        copy = _MemoryState()
        copy.role_fences = dict(self.role_fences)
        copy.streams = dict(self.streams)
        copy.items = dict(self.items)
        copy.items_by_req = dict(self.items_by_req)
        copy.audits = dict(self.audits)
        return copy


class _MemoryTx:
    """基于不可变 dataclass 行的内存事务句柄。"""

    def __init__(self, state: _MemoryState) -> None:
        self._s = state

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        return self._s.role_fences.get(player_id)

    async def apply_role_fence(self, state: Message, now_ms: int) -> bool:
        pid = int(state.player_id)
        existing = self._s.role_fences.get(pid)
        if existing is not None and int(state.version) <= existing.version:
            return False
        self._s.role_fences[pid] = RoleFenceRow(
            player_id=pid,
            account=state.account,
            owner_revision=int(state.owner_revision),
            freeze_token=int(state.freeze_token),
            freeze_final=bool(state.freeze_final),
            version=int(state.version),
            home_zone=int(state.home_zone),
            updated_ms=now_ms,
        )
        return True

    async def has_pending_item_any_domain(self, shard: int, player_id: int) -> bool:
        return any(
            row.shard == shard and row.player_id == player_id and row.state not in TERMINAL_STATES
            for row in self._s.items.values()
        )

    async def has_mismatched_pending_original(
        self, shard: int, player_id: int, account: str, owner_revision: int
    ) -> bool:
        want_acct = account.encode("utf-8")
        for row in self._s.items.values():
            if row.shard != shard or row.player_id != player_id:
                continue
            if row.state in TERMINAL_STATES:
                continue
            if (
                row.state != STATE_PENDING
                or row.owner_account.encode("utf-8") != want_acct
                or row.owner_revision != owner_revision
            ):
                return True
        return False

    async def ensure_stream(self, shard: int, player_id: int, domain: int, now_ms: int) -> None:
        key = (shard, player_id, domain)
        if key not in self._s.streams:
            self._s.streams[key] = StreamRow(shard, player_id, domain, 1, 0, 0, 0, now_ms, now_ms)

    async def lock_stream(self, shard: int, player_id: int, domain: int) -> StreamRow | None:
        return self._s.streams.get((shard, player_id, domain))

    async def update_stream_enqueue(
        self, shard: int, player_id: int, domain: int, next_seq: int, open_count: int, notify_due_ms: int, now_ms: int
    ) -> None:
        key = (shard, player_id, domain)
        st = self._s.streams.get(key)
        if st is None:
            raise StoreUnavailableError("stream row missing on update_stream_enqueue")
        self._s.streams[key] = StreamRow(
            st.shard, st.player_id, st.domain, next_seq, st.fence_epoch, open_count, notify_due_ms, st.created_ms, now_ms
        )

    async def update_stream_fetch(
        self, shard: int, player_id: int, domain: int, fence_epoch: int, next_seq: int, now_ms: int
    ) -> None:
        key = (shard, player_id, domain)
        st = self._s.streams.get(key)
        if st is None:
            raise StoreUnavailableError("stream row missing on update_stream_fetch")
        self._s.streams[key] = StreamRow(
            st.shard, st.player_id, st.domain, next_seq, fence_epoch, st.open_count, st.notify_due_ms, st.created_ms, now_ms
        )

    async def update_stream_ack(
        self, shard: int, player_id: int, domain: int, open_count: int, fence_epoch: int, now_ms: int
    ) -> None:
        key = (shard, player_id, domain)
        st = self._s.streams.get(key)
        if st is None:
            raise StoreUnavailableError("stream row missing on update_stream_ack")
        self._s.streams[key] = StreamRow(
            st.shard, st.player_id, st.domain, st.next_seq, fence_epoch, open_count, st.notify_due_ms, st.created_ms, now_ms
        )

    async def update_stream_open_count(
        self, shard: int, player_id: int, domain: int, open_count: int, now_ms: int
    ) -> None:
        key = (shard, player_id, domain)
        st = self._s.streams.get(key)
        if st is None:
            raise StoreUnavailableError("stream row missing on update_stream_open_count")
        self._s.streams[key] = StreamRow(
            st.shard, st.player_id, st.domain, st.next_seq, st.fence_epoch, open_count, st.notify_due_ms, st.created_ms, now_ms
        )

    async def read_item_by_request(
        self, shard: int, player_id: int, producer_kind: int, request_id: int
    ) -> ItemRow | None:
        pk = self._s.items_by_req.get((shard, player_id, producer_kind, request_id))
        return self._s.items.get(pk) if pk is not None else None

    async def read_item(self, shard: int, player_id: int, domain: int, seq: int) -> ItemRow | None:
        return self._s.items.get((shard, player_id, domain, seq))

    async def read_items_by_seq(
        self, shard: int, player_id: int, domain: int, seqs: tuple[int, ...]
    ) -> dict[int, ItemRow]:
        out: dict[int, ItemRow] = {}
        for seq in seqs:
            row = self._s.items.get((shard, player_id, domain, seq))
            if row is not None:
                out[seq] = row
        return out

    async def query_pending_items(self, shard: int, player_id: int, domain: int, limit: int) -> list[ItemRow]:
        rows = [
            row
            for row in self._s.items.values()
            if row.shard == shard and row.player_id == player_id and row.domain == domain and row.state == STATE_PENDING
        ]
        rows.sort(key=lambda r: r.seq)
        return rows[:limit]

    async def insert_item(self, row: ItemRow) -> None:
        pk = (row.shard, row.player_id, row.domain, row.seq)
        uk = (row.shard, row.player_id, row.producer_kind, row.request_id)
        if pk in self._s.items or uk in self._s.items_by_req:
            raise StoreUnavailableError("duplicate key while role fence is held")
        self._s.items[pk] = row
        self._s.items_by_req[uk] = pk

    def _replace_item(self, pk: tuple[int, int, int, int], **changes) -> None:
        row = self._s.items.get(pk)
        if row is None:
            raise StoreUnavailableError("item row missing during update")
        data = {field: getattr(row, field) for field in ItemRow.__slots__}
        data.update(changes)
        self._s.items[pk] = ItemRow(**data)

    async def mark_item_delivered(
        self, shard: int, player_id: int, domain: int, seq: int, epoch: int, now_ms: int
    ) -> None:
        pk = (shard, player_id, domain, seq)
        row = self._s.items.get(pk)
        if row is None or row.state != STATE_PENDING:
            raise StoreUnavailableError("mark_item_delivered state changed under stream lock")
        self._replace_item(
            pk,
            delivery_count=row.delivery_count + 1,
            last_delivered_epoch=epoch,
            last_delivered_ms=now_ms,
            updated_ms=now_ms,
        )

    async def finalize_item(
        self,
        shard: int,
        player_id: int,
        domain: int,
        seq: int,
        new_state: int,
        finalizer: int,
        decided_epoch: int,
        outcome_reason: int,
        now_ms: int,
    ) -> None:
        pk = (shard, player_id, domain, seq)
        row = self._s.items.get(pk)
        if row is None or row.state != STATE_PENDING:
            raise StoreUnavailableError("finalize_item state changed under stream lock")
        self._replace_item(
            pk,
            state=new_state,
            finalized_by=finalizer,
            decided_epoch=decided_epoch,
            outcome_reason=outcome_reason,
            updated_ms=now_ms,
        )

    async def void_item_direct(
        self, shard: int, player_id: int, domain: int, seq: int, void_reason: int, now_ms: int
    ) -> None:
        pk = (shard, player_id, domain, seq)
        row = self._s.items.get(pk)
        if row is None or row.state != STATE_PENDING or row.delivery_count != 0:
            raise StoreUnavailableError("void_item_direct precondition failed")
        self._replace_item(
            pk,
            state=STATE_VOIDED,
            finalized_by=FINALIZER_ADMIN,
            void_reason=void_reason,
            updated_ms=now_ms,
        )

    async def attach_void_directive(
        self, shard: int, player_id: int, domain: int, seq: int, void_reason: int, now_ms: int
    ) -> None:
        pk = (shard, player_id, domain, seq)
        row = self._s.items.get(pk)
        if row is None or row.state != STATE_PENDING:
            raise StoreUnavailableError("attach_void_directive precondition failed")
        self._replace_item(pk, directive=DIRECTIVE_VOID, void_reason=void_reason, updated_ms=now_ms)

    async def force_void_item(
        self, shard: int, player_id: int, domain: int, seq: int, void_reason: int, request_id: int, now_ms: int
    ) -> None:
        pk = (shard, player_id, domain, seq)
        row = self._s.items.get(pk)
        if row is None or row.state != STATE_PENDING or row.delivery_count <= 0 or row.request_id != request_id:
            raise StoreUnavailableError("force_void_item precondition failed")
        self._replace_item(
            pk,
            state=STATE_VOIDED,
            finalized_by=FINALIZER_ADMIN,
            void_reason=void_reason,
            updated_ms=now_ms,
        )

    async def insert_audit(self, row: AuditRow) -> None:
        key = (row.shard, row.player_id, row.domain, row.seq, row.action)
        if key not in self._s.audits:
            self._s.audits[key] = row

    async def purge_player_rows(self, shard: int, player_id: int) -> None:
        for key in [k for k in self._s.streams if k[0] == shard and k[1] == player_id]:
            del self._s.streams[key]
        for pk in [k for k in self._s.items if k[0] == shard and k[1] == player_id]:
            row = self._s.items.pop(pk)
            self._s.items_by_req.pop((row.shard, row.player_id, row.producer_kind, row.request_id), None)


class _MemoryDriver:
    """线程/协程安全、支持原子回滚的内存存储驱动。"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.state = _MemoryState()

    async def run_tx(self, work: Callable[[TxHandle], Awaitable[T]]) -> T:
        async with self._lock:
            staging = self.state.clone()
            tx = _MemoryTx(staging)
            try:
                result = await work(tx)
            except _TxRollbackReturn as ret:
                return ret.value
            self.state = staging
            return result

    async def apply_role_fence_direct(self, state: Message, now_ms: int) -> bool:
        async with self._lock:
            tx = _MemoryTx(self.state)
            return await tx.apply_role_fence(state, now_ms)

    async def clear_notify_due(self, shard: int, player_id: int, domain: int, expect_next_seq: int) -> bool:
        async with self._lock:
            key = (shard, player_id, domain)
            st = self.state.streams.get(key)
            if st is None or st.next_seq != expect_next_seq or st.notify_due_ms == 0:
                return False
            self.state.streams[key] = StreamRow(
                st.shard, st.player_id, st.domain, st.next_seq, st.fence_epoch, st.open_count, 0, st.created_ms, st.updated_ms
            )
            return True

    async def read_streams_for_player(self, shard: int, player_id: int) -> list[StreamRow]:
        async with self._lock:
            rows = [st for st in self.state.streams.values() if st.shard == shard and st.player_id == player_id]
            rows.sort(key=lambda s: s.domain)
            return rows

    async def read_stream(self, shard: int, player_id: int, domain: int) -> StreamRow | None:
        async with self._lock:
            return self.state.streams.get((shard, player_id, domain))

    async def read_pending_aggregates(self, shard: int, player_id: int) -> dict[int, tuple[int, int]]:
        async with self._lock:
            delivered: dict[int, int] = {}
            oldest: dict[int, int] = {}
            for row in self.state.items.values():
                if row.shard != shard or row.player_id != player_id or row.state != STATE_PENDING:
                    continue
                if row.delivery_count > 0:
                    delivered[row.domain] = delivered.get(row.domain, 0) + 1
                prev = oldest.get(row.domain, 0)
                if prev == 0 or row.created_ms < prev:
                    oldest[row.domain] = row.created_ms
            domains = set(delivered) | set(oldest)
            return {dom: (delivered.get(dom, 0), oldest.get(dom, 0)) for dom in domains}

    async def list_items(
        self, shard: int, player_id: int, domain: int, state: int, after_seq: int, limit: int
    ) -> list[ItemRow]:
        async with self._lock:
            rows = [
                row
                for row in self.state.items.values()
                if row.shard == shard
                and row.player_id == player_id
                and row.domain == domain
                and row.seq > after_seq
                and (state == STATE_UNSPECIFIED or row.state == state)
            ]
            rows.sort(key=lambda r: r.seq)
            return rows[:limit]

    async def read_item_by_request_direct(
        self, shard: int, player_id: int, producer_kind: int, request_id: int
    ) -> ItemRow | None:
        async with self._lock:
            pk = self.state.items_by_req.get((shard, player_id, producer_kind, request_id))
            return self.state.items.get(pk) if pk is not None else None

    async def list_player_items_for_reconcile(self, shard: int, player_id: int, domain: int) -> list[ItemRow]:
        async with self._lock:
            rows = [
                row
                for row in self.state.items.values()
                if row.shard == shard
                and row.player_id == player_id
                and (domain == DOMAIN_UNSPECIFIED or row.domain == domain)
            ]
            rows.sort(key=lambda r: (r.domain, r.seq))
            return rows

    async def read_pending_item_gain_sanctions(
        self, shard: int, player_id: int, domain: int, msg_types: tuple[int, ...], limit: int
    ) -> list[ItemRow]:
        async with self._lock:
            allowed = set(msg_types)
            rows = [
                row
                for row in self.state.items.values()
                if row.shard == shard
                and row.player_id == player_id
                and row.domain == domain
                and row.state == STATE_PENDING
                and row.msg_type in allowed
            ]
            rows.sort(key=lambda r: r.seq)
            return rows[:limit]


class MemoryInboxStore(InboxStoreCore):
    """纯内存双胞胎收件箱存储，支持完整事务回滚与单测状态检查。"""

    def __init__(
        self,
        protocol: Protocol,
        registry: InboxRegistry,
        directory: DataServiceDirectory,
        *,
        allowed_types: frozenset[int] = frozenset(),
        notifier: InboxNotifier | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._mem = _MemoryDriver()
        super().__init__(
            protocol,
            registry,
            directory,
            self._mem,
            allowed_types=allowed_types,
            notifier=notifier,
            clock_ms=clock_ms,
        )

    @property
    def mem_state(self) -> _MemoryState:
        return self._mem.state
