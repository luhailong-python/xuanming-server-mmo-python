"""payment 存储抽象协议（PaymentStore）与确定性内存孪生实现（MemoryPaymentStore）。

与 Go ``go/payment/internal/core/service.go:Store`` 和 ``internal/data/repo.go``、``spend.go`` 语义一一对应。
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Protocol

from .errors import DailyLimitError, MonthlyLimitError, RecordNotFoundError
from .model import (
    FINALIZE_NONE,
    FINALIZE_PENDING,
    INTENT_CLOSED,
    INTENT_CREATED,
    INTENT_PAID,
    MALL_BOUND,
    MALL_ORPHAN,
    MALL_PENDING,
    MALL_REFUND_QUEUED,
    MallSync,
    PaymentIntentRecord,
    PaymentTxnRecord,
    REFUND_FAILED,
    REFUND_NONE,
    REFUND_REPORTED,
    REFUND_REQUESTED,
    REFUND_SUCCEEDED,
    RecordMinorPaymentOutcome,
    RefundProgress,
    SpendLimit,
)

_MALL_SYNCABLE_STATES = (MALL_PENDING, MALL_BOUND, MALL_REFUND_QUEUED)
_REFUND_CLAIMABLE_STATES = (
    REFUND_NONE,
    REFUND_REQUESTED,
    REFUND_SUCCEEDED,
    REFUND_REPORTED,
    REFUND_FAILED,
)


def admit_minor_spend(
    held_intents: list[PaymentIntentRecord],
    limit: SpendLimit,
    amount_minor: int,
) -> None:
    """未成年人月/日额度准入校验（与 Go data/spend.go:admit 逐字对齐）。"""
    inflight = 0
    month = 0
    day = 0
    for item in held_intents:
        if item.state == INTENT_CREATED:
            inflight += item.amount_minor
        elif item.state == INTENT_PAID and item.paid_ms >= limit.month_start_ms:
            month += item.amount_minor
            if item.paid_ms >= limit.day_start_ms:
                day += item.amount_minor
    if amount_minor + inflight + month > limit.monthly_fen:
        raise MonthlyLimitError("payment: minor monthly spending limit reached")
    if limit.daily_fen > 0 and amount_minor + inflight + day > limit.daily_fen:
        raise DailyLimitError("payment: minor daily spending limit reached")


class PaymentStore(Protocol):
    """支付服务对独占库 mmorpg_payment 的全部读写依赖。"""

    # payment_intent
    async def intent(self, order_hi: int, order_lo: int) -> PaymentIntentRecord: ...
    async def insert_intent(self, rec: PaymentIntentRecord) -> tuple[PaymentIntentRecord, bool]: ...
    async def reserve_intent(
        self, rec: PaymentIntentRecord, limit: SpendLimit
    ) -> tuple[PaymentIntentRecord, bool]: ...
    async def record_minor_payment_txn(
        self,
        rec: PaymentTxnRecord,
        intent: PaymentIntentRecord,
        limit: SpendLimit,
        paid_ms: int,
        now_ms: int,
        over_limit_reason: str,
    ) -> RecordMinorPaymentOutcome: ...
    async def revive_minor_intent(
        self,
        intent: PaymentIntentRecord,
        limit: SpendLimit,
        paid_ms: int,
        now_ms: int,
        *,
        force: bool,
    ) -> bool: ...
    async def set_channel_ref(
        self, order_hi: int, order_lo: int, ref: str, ref_ms: int, payer_id: str, now_ms: int
    ) -> None: ...
    async def claim_active_query(self, rec: PaymentIntentRecord, now_ms: int) -> bool: ...
    async def due_intents(self, now_ms: int, limit: int) -> list[PaymentIntentRecord]: ...
    async def claim_check(self, rec: PaymentIntentRecord, lease_ms: int, now_ms: int) -> bool: ...
    async def schedule_check(
        self, rec: PaymentIntentRecord, lease_ms: int, next_ms: int, now_ms: int
    ) -> None: ...
    async def close_intent(self, order_hi: int, order_lo: int, now_ms: int) -> bool: ...
    async def mark_intent_paid(self, order_hi: int, order_lo: int, now_ms: int) -> None: ...
    async def set_intent_state(self, order_hi: int, order_lo: int, state: int, now_ms: int) -> None: ...

    # payment_txn
    async def txn(self, channel: int, txn_hi: int, txn_lo: int) -> PaymentTxnRecord: ...
    async def insert_txn(self, rec: PaymentTxnRecord) -> tuple[PaymentTxnRecord, bool]: ...
    async def flip_fact(self, rec: PaymentTxnRecord, observed_ms: int, now_ms: int) -> bool: ...
    async def touch_fact_observed(self, rec: PaymentTxnRecord, observed_ms: int, now_ms: int) -> None: ...
    async def oldest_mall_pending_ms(self) -> int: ...
    async def oldest_finalize_pending_ms(self) -> int: ...
    async def due_mall_txns(self, now_ms: int, limit: int) -> list[PaymentTxnRecord]: ...
    async def claim_mall(self, rec: PaymentTxnRecord, lease_ms: int, now_ms: int) -> bool: ...
    async def settle_mall(
        self, rec: PaymentTxnRecord, lease_ms: int, sync: MallSync, now_ms: int
    ) -> None: ...
    async def defer_mall(
        self, rec: PaymentTxnRecord, lease_ms: int, next_ms: int, last_err: str, now_ms: int
    ) -> None: ...
    async def claim_refund(
        self, rec: PaymentTxnRecord, out_refund_no: str, lease_ms: int, now_ms: int
    ) -> bool: ...
    async def save_refund(
        self, rec: PaymentTxnRecord, lease_ms: int, progress: RefundProgress, now_ms: int
    ) -> None: ...
    async def due_finalize(self, now_ms: int, limit: int) -> list[PaymentTxnRecord]: ...
    async def claim_finalize(self, rec: PaymentTxnRecord, lease_ms: int, now_ms: int) -> bool: ...
    async def save_finalize(
        self, rec: PaymentTxnRecord, lease_ms: int, state: int, next_ms: int, now_ms: int
    ) -> None: ...

    # payment_channel_cursor
    async def claim_cursor(
        self, channel: int, owner: int, initial_ms: int, lease_ms: int, now_ms: int
    ) -> tuple[int, bool]: ...
    async def advance_cursor(self, channel: int, owner: int, cursor_ms: int, now_ms: int) -> None: ...


class MemoryPaymentStore:
    """单进程确定性内存存储（与 SqlPaymentStore 语义严格对齐，用于单元测试与本地联调）。"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._intents: dict[tuple[int, int], PaymentIntentRecord] = {}
        self._txns: dict[tuple[int, int, int], PaymentTxnRecord] = {}
        self._cursors: dict[int, tuple[int, int, int, int]] = {}  # channel -> (cursor_ms, lease_until_ms, owner, updated_ms)
        self._spends: dict[tuple[int, int], int] = {}

    def _held_intents_unlocked(self, rec: PaymentIntentRecord, month_start_ms: int) -> list[PaymentIntentRecord]:
        key = (rec.order_hi, rec.order_lo)
        out: list[PaymentIntentRecord] = []
        for ikey, row in self._intents.items():
            if ikey == key:
                continue
            if row.spender_hi != rec.spender_hi or row.spender_lo != rec.spender_lo:
                continue
            if row.state == INTENT_CREATED or (row.state == INTENT_PAID and row.paid_ms >= month_start_ms):
                out.append(row)
        return out

    # -- payment_intent ----------------------------------------------------------------------------

    async def intent(self, order_hi: int, order_lo: int) -> PaymentIntentRecord:
        async with self._lock:
            row = self._intents.get((order_hi, order_lo))
            if row is None:
                raise RecordNotFoundError("payment intent not found")
            return replace(row)

    async def insert_intent(self, rec: PaymentIntentRecord) -> tuple[PaymentIntentRecord, bool]:
        async with self._lock:
            key = (rec.order_hi, rec.order_lo)
            existing = self._intents.get(key)
            if existing is not None:
                return replace(existing), False
            stored = replace(rec)
            self._intents[key] = stored
            return replace(stored), True

    async def reserve_intent(
        self, rec: PaymentIntentRecord, limit: SpendLimit
    ) -> tuple[PaymentIntentRecord, bool]:
        async with self._lock:
            self._spends[(rec.spender_hi, rec.spender_lo)] = rec.created_ms
            held = self._held_intents_unlocked(rec, limit.month_start_ms)
            admit_minor_spend(held, limit, rec.amount_minor)
            key = (rec.order_hi, rec.order_lo)
            existing = self._intents.get(key)
            if existing is not None:
                return replace(existing), False
            stored = replace(rec)
            self._intents[key] = stored
            return replace(stored), True

    async def record_minor_payment_txn(
        self,
        rec: PaymentTxnRecord,
        intent: PaymentIntentRecord,
        limit: SpendLimit,
        paid_ms: int,
        now_ms: int,
        over_limit_reason: str,
    ) -> RecordMinorPaymentOutcome:
        async with self._lock:
            self._spends[(intent.spender_hi, intent.spender_lo)] = now_ms
            ikey = (intent.order_hi, intent.order_lo)
            cur_intent = self._intents.get(ikey)
            if cur_intent is None:
                raise RecordNotFoundError("payment intent not found")
            tkey = (rec.channel, rec.txn_hi, rec.txn_lo)
            existing_txn = self._txns.get(tkey)
            if existing_txn is not None:
                return RecordMinorPaymentOutcome(
                    stored=replace(existing_txn),
                    inserted=False,
                    was_closed=(cur_intent.state == INTENT_CLOSED),
                )
            to_insert = replace(rec)
            was_closed = cur_intent.state == INTENT_CLOSED
            over_limit = False
            if cur_intent.state == INTENT_CREATED:
                cur_intent.state = INTENT_PAID
                cur_intent.paid_ms = paid_ms
                cur_intent.channel_ref = ""
                cur_intent.updated_ms = now_ms
            elif cur_intent.state == INTENT_CLOSED:
                held = self._held_intents_unlocked(cur_intent, limit.month_start_ms)
                try:
                    admit_minor_spend(held, limit, cur_intent.amount_minor)
                except (MonthlyLimitError, DailyLimitError):
                    over_limit = True
                    to_insert.mall_state = MALL_ORPHAN
                    to_insert.mall_due = 0
                    to_insert.mall_last_error = over_limit_reason
                    to_insert.finalize_state = FINALIZE_NONE
                    to_insert.finalize_next_ms = 0
                else:
                    cur_intent.state = INTENT_PAID
                    cur_intent.paid_ms = paid_ms
                    cur_intent.channel_ref = ""
                    cur_intent.updated_ms = now_ms
            self._txns[tkey] = to_insert
            return RecordMinorPaymentOutcome(
                stored=replace(to_insert),
                inserted=True,
                was_closed=was_closed,
                over_limit=over_limit,
            )

    async def revive_minor_intent(
        self,
        intent: PaymentIntentRecord,
        limit: SpendLimit,
        paid_ms: int,
        now_ms: int,
        *,
        force: bool,
    ) -> bool:
        async with self._lock:
            self._spends[(intent.spender_hi, intent.spender_lo)] = now_ms
            cur = self._intents.get((intent.order_hi, intent.order_lo))
            if cur is None:
                raise RecordNotFoundError("payment intent not found")
            if cur.state != INTENT_CLOSED:
                return True
            held = self._held_intents_unlocked(cur, limit.month_start_ms)
            within = True
            try:
                admit_minor_spend(held, limit, cur.amount_minor)
            except (MonthlyLimitError, DailyLimitError):
                within = False
            if not within and not force:
                return False
            cur.state = INTENT_PAID
            cur.paid_ms = paid_ms
            cur.channel_ref = ""
            cur.updated_ms = now_ms
            return within

    async def set_channel_ref(
        self, order_hi: int, order_lo: int, ref: str, ref_ms: int, payer_id: str, now_ms: int
    ) -> None:
        async with self._lock:
            row = self._intents.get((order_hi, order_lo))
            if row is not None and row.state == INTENT_CREATED:
                row.channel_ref = ref
                row.channel_ref_ms = ref_ms
                row.payer_id = payer_id
                row.updated_ms = now_ms

    async def claim_active_query(self, rec: PaymentIntentRecord, now_ms: int) -> bool:
        async with self._lock:
            row = self._intents.get((rec.order_hi, rec.order_lo))
            if row is None or row.state != INTENT_CREATED or row.checked_ms != rec.checked_ms:
                return False
            row.checked_ms = now_ms
            row.updated_ms = now_ms
            return True

    async def due_intents(self, now_ms: int, limit: int) -> list[PaymentIntentRecord]:
        async with self._lock:
            candidates = [
                replace(row)
                for row in self._intents.values()
                if row.state == INTENT_CREATED and row.next_check_ms <= now_ms
            ]
            candidates.sort(key=lambda r: (r.next_check_ms, r.order_hi, r.order_lo))
            return candidates[:limit]

    async def claim_check(self, rec: PaymentIntentRecord, lease_ms: int, now_ms: int) -> bool:
        async with self._lock:
            row = self._intents.get((rec.order_hi, rec.order_lo))
            if row is None or row.state != INTENT_CREATED or row.next_check_ms != rec.next_check_ms:
                return False
            row.next_check_ms = lease_ms
            row.check_count += 1
            row.checked_ms = now_ms
            row.updated_ms = now_ms
            return True

    async def schedule_check(
        self, rec: PaymentIntentRecord, lease_ms: int, next_ms: int, now_ms: int
    ) -> None:
        async with self._lock:
            row = self._intents.get((rec.order_hi, rec.order_lo))
            if row is not None and row.state == INTENT_CREATED and row.next_check_ms == lease_ms:
                row.next_check_ms = next_ms
                row.updated_ms = now_ms

    async def close_intent(self, order_hi: int, order_lo: int, now_ms: int) -> bool:
        async with self._lock:
            row = self._intents.get((order_hi, order_lo))
            if row is None or row.state != INTENT_CREATED:
                return False
            row.state = INTENT_CLOSED
            row.channel_ref = ""
            row.updated_ms = now_ms
            return True

    async def mark_intent_paid(self, order_hi: int, order_lo: int, now_ms: int) -> None:
        async with self._lock:
            row = self._intents.get((order_hi, order_lo))
            if row is not None and row.state != INTENT_PAID:
                row.state = INTENT_PAID
                row.paid_ms = max(now_ms, row.created_ms)
                row.channel_ref = ""
                row.updated_ms = now_ms

    async def set_intent_state(self, order_hi: int, order_lo: int, state: int, now_ms: int) -> None:
        async with self._lock:
            row = self._intents.get((order_hi, order_lo))
            if row is not None:
                if state == INTENT_PAID:
                    row.paid_ms = max(now_ms, row.created_ms)
                row.state = state
                row.channel_ref = ""
                row.updated_ms = now_ms

    # -- payment_txn -------------------------------------------------------------------------------

    async def txn(self, channel: int, txn_hi: int, txn_lo: int) -> PaymentTxnRecord:
        async with self._lock:
            row = self._txns.get((channel, txn_hi, txn_lo))
            if row is None:
                raise RecordNotFoundError("payment txn not found")
            return replace(row)

    async def insert_txn(self, rec: PaymentTxnRecord) -> tuple[PaymentTxnRecord, bool]:
        async with self._lock:
            key = (rec.channel, rec.txn_hi, rec.txn_lo)
            existing = self._txns.get(key)
            if existing is not None:
                return replace(existing), False
            stored = replace(rec)
            self._txns[key] = stored
            return replace(stored), True

    async def flip_fact(self, rec: PaymentTxnRecord, observed_ms: int, now_ms: int) -> bool:
        async with self._lock:
            row = self._txns.get((rec.channel, rec.txn_hi, rec.txn_lo))
            if row is None or row.fact_revision != rec.fact_revision:
                return False
            if row.mall_due != 1:
                row.next_attempt_ms = now_ms
            if row.mall_state in _MALL_SYNCABLE_STATES:
                row.mall_due = 1
            row.fact_revision += 1
            row.fact_observed_ms = observed_ms
            row.updated_ms = now_ms
            return True

    async def touch_fact_observed(self, rec: PaymentTxnRecord, observed_ms: int, now_ms: int) -> None:
        async with self._lock:
            row = self._txns.get((rec.channel, rec.txn_hi, rec.txn_lo))
            if (
                row is not None
                and row.fact_revision == rec.fact_revision
                and row.fact_observed_ms < observed_ms
            ):
                row.fact_observed_ms = observed_ms
                row.updated_ms = now_ms

    async def oldest_mall_pending_ms(self) -> int:
        async with self._lock:
            pending = [row.created_ms for row in self._txns.values() if row.mall_due == 1]
            return min(pending) if pending else 0

    async def oldest_finalize_pending_ms(self) -> int:
        async with self._lock:
            pending = [row.created_ms for row in self._txns.values() if row.finalize_state == FINALIZE_PENDING]
            return min(pending) if pending else 0

    async def due_mall_txns(self, now_ms: int, limit: int) -> list[PaymentTxnRecord]:
        async with self._lock:
            candidates = [
                replace(row)
                for row in self._txns.values()
                if row.mall_due == 1 and row.next_attempt_ms <= now_ms
            ]
            candidates.sort(key=lambda r: (r.next_attempt_ms, r.channel, r.txn_hi, r.txn_lo))
            return candidates[:limit]

    async def claim_mall(self, rec: PaymentTxnRecord, lease_ms: int, now_ms: int) -> bool:
        async with self._lock:
            row = self._txns.get((rec.channel, rec.txn_hi, rec.txn_lo))
            if row is None or row.mall_due != 1 or row.next_attempt_ms != rec.next_attempt_ms:
                return False
            row.next_attempt_ms = lease_ms
            row.mall_attempts += 1
            row.updated_ms = now_ms
            return True

    async def settle_mall(
        self, rec: PaymentTxnRecord, lease_ms: int, sync: MallSync, now_ms: int
    ) -> None:
        async with self._lock:
            row = self._txns.get((rec.channel, rec.txn_hi, rec.txn_lo))
            if row is None or row.mall_due != 1 or row.next_attempt_ms != lease_ms:
                return
            row.mall_due = 1 if (sync.keep_syncing and row.fact_revision > sync.revision) else 0
            row.mall_state = sync.state
            row.mall_refund_reason = sync.refund_reason
            row.mall_revision = sync.revision
            row.mall_last_error = sync.last_error
            row.next_attempt_ms = now_ms
            row.updated_ms = now_ms

    async def defer_mall(
        self, rec: PaymentTxnRecord, lease_ms: int, next_ms: int, last_err: str, now_ms: int
    ) -> None:
        async with self._lock:
            row = self._txns.get((rec.channel, rec.txn_hi, rec.txn_lo))
            if row is None or row.mall_due != 1 or row.next_attempt_ms != lease_ms:
                return
            row.next_attempt_ms = next_ms
            row.mall_last_error = last_err
            row.updated_ms = now_ms

    async def claim_refund(
        self, rec: PaymentTxnRecord, out_refund_no: str, lease_ms: int, now_ms: int
    ) -> bool:
        async with self._lock:
            row = self._txns.get((rec.channel, rec.txn_hi, rec.txn_lo))
            if (
                row is None
                or row.refund_state not in _REFUND_CLAIMABLE_STATES
                or row.refund_next_ms != rec.refund_next_ms
            ):
                return False
            if row.refund_state == REFUND_NONE:
                row.refund_state = REFUND_REQUESTED
            if not row.out_refund_no:
                row.out_refund_no = out_refund_no
            row.refund_next_ms = lease_ms
            row.refund_attempts += 1
            row.updated_ms = now_ms
            return True

    async def save_refund(
        self, rec: PaymentTxnRecord, lease_ms: int, progress: RefundProgress, now_ms: int
    ) -> None:
        async with self._lock:
            row = self._txns.get((rec.channel, rec.txn_hi, rec.txn_lo))
            if row is None or row.refund_next_ms != lease_ms:
                return
            row.refund_state = progress.state
            row.out_refund_no = progress.out_refund_no
            row.refund_id = progress.refund_id
            row.refund_ms = progress.refund_ms
            row.refund_next_ms = progress.next_ms
            row.refund_last_error = progress.last_error
            row.updated_ms = now_ms

    async def due_finalize(self, now_ms: int, limit: int) -> list[PaymentTxnRecord]:
        async with self._lock:
            candidates = [
                replace(row)
                for row in self._txns.values()
                if row.finalize_state == FINALIZE_PENDING and row.finalize_next_ms <= now_ms
            ]
            candidates.sort(key=lambda r: (r.finalize_next_ms, r.channel, r.txn_hi, r.txn_lo))
            return candidates[:limit]

    async def claim_finalize(self, rec: PaymentTxnRecord, lease_ms: int, now_ms: int) -> bool:
        async with self._lock:
            row = self._txns.get((rec.channel, rec.txn_hi, rec.txn_lo))
            if (
                row is None
                or row.finalize_state != FINALIZE_PENDING
                or row.finalize_next_ms != rec.finalize_next_ms
            ):
                return False
            row.finalize_next_ms = lease_ms
            row.updated_ms = now_ms
            return True

    async def save_finalize(
        self, rec: PaymentTxnRecord, lease_ms: int, state: int, next_ms: int, now_ms: int
    ) -> None:
        async with self._lock:
            row = self._txns.get((rec.channel, rec.txn_hi, rec.txn_lo))
            if row is None or row.finalize_next_ms != lease_ms:
                return
            row.finalize_state = state
            row.finalize_next_ms = next_ms
            row.updated_ms = now_ms

    # -- payment_channel_cursor --------------------------------------------------------------------

    async def claim_cursor(
        self, channel: int, owner: int, initial_ms: int, lease_ms: int, now_ms: int
    ) -> tuple[int, bool]:
        async with self._lock:
            entry = self._cursors.get(channel)
            if entry is None:
                entry = (initial_ms, 0, 0, now_ms)
                self._cursors[channel] = entry
            cursor_ms, lease_until_ms, _, _ = entry
            if not (lease_until_ms < now_ms):
                return 0, False
            self._cursors[channel] = (cursor_ms, lease_ms, owner, now_ms)
            return cursor_ms, True

    async def advance_cursor(self, channel: int, owner: int, cursor_ms: int, now_ms: int) -> None:
        async with self._lock:
            entry = self._cursors.get(channel)
            if entry is None or entry[2] != owner:
                return
            cur_ms = entry[0]
            next_cursor = cursor_ms if cur_ms < cursor_ms else cur_ms
            self._cursors[channel] = (next_cursor, 0, owner, now_ms)
