"""payment 后台异步工作器（仅在 release_track == 'stable' 副本运行）。

包含 5 条后台循环（与 Go ``go/payment/internal/core/workers.go`` 对齐）：
1. ``sweep_intents``：到点查单与到期关单；
2. ``push_mall``：付款事实首次记账与 ``fact_revision`` 修订同步（支持 ``kick_event`` 即时唤醒）；
3. ``process_refunds``：分页拉取商城退款待办、按 ``ClaimRefund`` 原子预置 ``REQUESTED`` + ``out_refund_no`` 后执行渠道退款并回执/转人工；
4. ``finalize``：渠道侧收尾动作（如 Google Play consume）；
5. ``poll_history``：按时间窗租约补拉渠道历史事件。
"""

from __future__ import annotations

import asyncio
import logging
import random
import secrets

from .channels import FinalizerChannel, HistoryPollerChannel, PaymentChannel
from .errors import (
    ChannelCallError,
    MallConflictError,
    MallInvalidError,
    QueryUnsupportedError,
    RecordNotFoundError,
    error_class,
)
from .model import (
    ChannelRefundRequest,
    ChannelRefundState,
    FACT_PAID,
    FACT_REVOKED,
    FINALIZE_DONE,
    FINALIZE_FAILED,
    FINALIZE_PENDING,
    INTENT_CLOSED,
    MALL_BOUND,
    MALL_PENDING,
    MALL_REFUND_QUEUED,
    MALL_REJECTED,
    MallPayment,
    MallPendingRefund,
    MallRefund,
    MallRefundClosure,
    MallRevision,
    MallSync,
    PaymentIntentRecord,
    PaymentTxnRecord,
    REFUND_FAILED,
    REFUND_NONE,
    REFUND_REPORTED,
    REFUND_REQUESTED,
    REFUND_SUCCEEDED,
    RefundProgress,
    TradeState,
    channel_name_of,
    close_reason,
    is_revoked,
    order_id_from_key,
    out_refund_no,
    out_trade_no,
    refunds_orders_own_payment,
    tail,
)
from .service import PaymentCore

logger = logging.getLogger(__name__)

REFUND_POLL_MAX_PAGES = 20
HISTORY_OVERLAP_MS = 5 * 60 * 1000
HISTORY_LEASE_MS = 90 * 1000
ONE_MINUTE_MS = 60 * 1000
ONE_HOUR_MS = 3600 * 1000


def backoff_ms(attempt: int, max_ms: int) -> int:
    """指数退避（1 秒起翻倍，封顶 max_ms，±20% 抖动）。"""
    clamped = min(max(attempt, 0), 20)
    wait = min(1000 << clamped, max_ms)
    jitter = random.randint(0, wait // 5)
    return max(1, wait - jitter if random.randint(0, 1) == 0 else wait + jitter)


class PaymentWorkers:
    """后台工作器集合：既支持 ``run(stop_event)`` 常驻循环，也暴露单轮方法供确定性测试。"""

    def __init__(self, core: PaymentCore) -> None:
        self.core = core

    async def run(self, stop_event: asyncio.Event) -> None:
        opts = self.core.options
        tasks = [
            asyncio.create_task(
                self._periodic(stop_event, opts.interval_sec, self.sweep_intents_once, None),
                name="payment-sweep-intents",
            ),
            asyncio.create_task(
                self._periodic(stop_event, opts.interval_sec, self.push_mall_once, self.core.kick_event),
                name="payment-push-mall",
            ),
            asyncio.create_task(
                self._periodic(stop_event, opts.refund_poll_interval_sec, self.process_refunds_once, None),
                name="payment-process-refunds",
            ),
            asyncio.create_task(
                self._periodic(stop_event, opts.interval_sec, self.finalize_once, None),
                name="payment-finalize",
            ),
            asyncio.create_task(
                self._periodic(stop_event, opts.history_interval_sec, self.poll_history_once, None),
                name="payment-poll-history",
            ),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _periodic(
        self,
        stop_event: asyncio.Event,
        interval_sec: float,
        tick,
        kick_event: asyncio.Event | None,
    ) -> None:
        while not stop_event.is_set():
            if kick_event is not None:
                wait_stop = asyncio.create_task(stop_event.wait())
                wait_kick = asyncio.create_task(kick_event.wait())
                try:
                    done, pending = await asyncio.wait(
                        {wait_stop, wait_kick},
                        timeout=interval_sec,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in pending:
                        task.cancel()
                    if stop_event.is_set():
                        return
                    if kick_event.is_set():
                        kick_event.clear()
                finally:
                    for task in (wait_stop, wait_kick):
                        if not task.done():
                            task.cancel()
            else:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
                    return
                except asyncio.TimeoutError:
                    pass
            if stop_event.is_set():
                return
            try:
                await tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("payment background worker tick failed fn=%s: %s", tick.__name__, exc)

    # ---------------------------------------------------------------------------
    # 1. 查单与到期关单（sweep_intents）
    # ---------------------------------------------------------------------------

    async def sweep_intents_once(self) -> int:
        opts = self.core.options
        due = await self.core.store.due_intents(self.core.now_ms(), opts.batch)
        processed = 0
        for intent in due:
            try:
                await self._check_intent(intent)
                processed += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("payment check_intent failed order=%s: %s", intent.order_id, exc)
        return processed

    async def _check_intent(self, intent: PaymentIntentRecord) -> None:
        opts = self.core.options
        now_ms = self.core.now_ms()
        lease_ms = now_ms + opts.lease_ms
        claimed = await self.core.store.claim_check(intent, lease_ms, now_ms)
        if not claimed:
            return

        deadline = intent.expire_ms + opts.close_grace_ms
        give_up = intent.expire_ms + opts.give_up_after_ms
        ch = self.core.channel_by_id(intent.channel)
        if ch is None:
            if now_ms >= give_up:
                await self.core.store.close_intent(intent.order_hi, intent.order_lo, now_ms)
            else:
                await self.core.store.schedule_check(intent, lease_ms, now_ms + ONE_HOUR_MS, now_ms)
            return

        query_err: Exception | None = None
        unsupported = False
        result = None
        try:
            async with asyncio.timeout(opts.background_call_timeout_sec):
                result = await self.core.query_channel(ch, intent)
        except QueryUnsupportedError as exc:
            query_err = exc
            unsupported = True
        except Exception as exc:
            query_err = exc

        if query_err is None and result is not None:
            if result.state == TradeState.PAID and result.payment is not None:
                await self.core.record(ch, result.payment, refunded_outside=False)
                return
            if result.state == TradeState.REFUNDED_OUTSIDE and result.payment is not None:
                await self.core.record(ch, result.payment, refunded_outside=True)
                return
            if result.state == TradeState.CLOSED:
                await self.core.store.close_intent(intent.order_hi, intent.order_lo, now_ms)
                return

        if now_ms >= deadline and (query_err is None or unsupported):
            if not unsupported:
                try:
                    async with asyncio.timeout(opts.background_call_timeout_sec):
                        await ch.close_trade(out_trade_no(intent.order_id))
                except Exception:
                    await self.core.store.schedule_check(
                        intent,
                        lease_ms,
                        now_ms + backoff_ms(intent.check_count, opts.check_interval_ms),
                        now_ms,
                    )
                    return
                try:
                    async with asyncio.timeout(opts.background_call_timeout_sec):
                        final = await self.core.query_channel(ch, intent)
                except Exception:
                    await self.core.store.schedule_check(
                        intent,
                        lease_ms,
                        now_ms + backoff_ms(intent.check_count, opts.check_interval_ms),
                        now_ms,
                    )
                    return
                if final.state == TradeState.PAID and final.payment is not None:
                    await self.core.record(ch, final.payment, refunded_outside=False)
                    return
                if final.state == TradeState.REFUNDED_OUTSIDE and final.payment is not None:
                    await self.core.record(ch, final.payment, refunded_outside=True)
                    return
            await self.core.store.close_intent(intent.order_hi, intent.order_lo, now_ms)
            return

        if query_err is not None and not unsupported and now_ms >= give_up:
            logger.error(
                "payment giving up query order=%s channel=%s: %s",
                intent.order_id,
                ch.name,
                query_err,
            )
            await self.core.store.close_intent(intent.order_hi, intent.order_lo, now_ms)
            return

        if query_err is not None and not unsupported:
            next_ms = now_ms + backoff_ms(intent.check_count, opts.check_interval_ms)
        else:
            next_ms = now_ms + opts.check_interval_ms
        if unsupported or (now_ms < deadline and next_ms > deadline):
            next_ms = max(deadline, now_ms + 1)
        await self.core.store.schedule_check(intent, lease_ms, next_ms, now_ms)

    # ---------------------------------------------------------------------------
    # 2. 推进商城（push_mall）
    # ---------------------------------------------------------------------------

    async def push_mall_once(self) -> int:
        opts = self.core.options
        due = await self.core.store.due_mall_txns(self.core.now_ms(), opts.batch)
        processed = 0
        for txn in due:
            try:
                await self._push_one(txn)
                processed += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("payment push_mall failed tx_tail=%s: %s", tail(txn.transaction_id), exc)
        return processed

    async def _push_one(self, txn: PaymentTxnRecord) -> None:
        opts = self.core.options
        now_ms = self.core.now_ms()
        lease_ms = now_ms + opts.lease_ms
        claimed = await self.core.store.claim_mall(txn, lease_ms, now_ms)
        if not claimed:
            return

        ch_name = channel_name_of(txn.channel)
        order_id = order_id_from_key(txn.order_hi, txn.order_lo)

        async def retry(reason: str) -> None:
            wait = backoff_ms(txn.mall_attempts + 1, opts.mall_backoff_max_ms)
            await self.core.store.defer_mall(txn, lease_ms, now_ms + wait, reason, now_ms)

        if txn.mall_state == MALL_PENDING:
            try:
                async with asyncio.timeout(opts.background_call_timeout_sec):
                    outcome = await self.core.mall.record_payment(
                        MallPayment(
                            order_id=order_id,
                            channel=ch_name,
                            transaction_id=txn.transaction_id,
                            amount_minor=txn.paid_amount_minor,
                            currency=txn.paid_currency,
                        )
                    )
            except (MallConflictError, MallInvalidError) as exc:
                logger.error(
                    "payment mall rejected payment order=%s channel=%s tx_tail=%s: %s",
                    order_id,
                    ch_name,
                    tail(txn.transaction_id),
                    exc,
                )
                await self.core.store.settle_mall(
                    txn,
                    lease_ms,
                    MallSync(
                        state=MALL_REJECTED,
                        revision=txn.mall_revision,
                        keep_syncing=False,
                        last_error=error_class(exc),
                    ),
                    now_ms,
                )
                return
            except Exception as exc:
                await retry(error_class(exc))
                return

            if not outcome.refund_reason:
                await self.core.store.settle_mall(
                    txn,
                    lease_ms,
                    MallSync(state=MALL_BOUND, revision=0, keep_syncing=True),
                    now_ms,
                )
                # 若首次报给商城前已被渠道撤销，在此补关意图释放未成年人额度
                if is_revoked(txn.fact_revision) and (txn.order_hi != 0 or txn.order_lo != 0):
                    await self.core.apply_flip(txn.order_hi, txn.order_lo, INTENT_CLOSED, now_ms)
                return

            if outcome.refund_reason in ("AMOUNT_MISMATCH", "CURRENCY_MISMATCH"):
                logger.error(
                    "payment mall queued mismatch refund order=%s channel=%s reason=%s",
                    order_id,
                    ch_name,
                    outcome.refund_reason,
                )
            await self.core.store.settle_mall(
                txn,
                lease_ms,
                MallSync(
                    state=MALL_REFUND_QUEUED,
                    refund_reason=outcome.refund_reason,
                    revision=0,
                    keep_syncing=True,
                ),
                now_ms,
            )
            if (
                is_revoked(txn.fact_revision)
                and refunds_orders_own_payment(outcome.refund_reason)
                and (txn.order_hi != 0 or txn.order_lo != 0)
            ):
                await self.core.apply_flip(txn.order_hi, txn.order_lo, INTENT_CLOSED, now_ms)
            return

        # 已落账，推进更大的 fact_revision
        fact = FACT_REVOKED if is_revoked(txn.fact_revision) else FACT_PAID
        try:
            async with asyncio.timeout(opts.background_call_timeout_sec):
                acked = await self.core.mall.push_revision(
                    MallRevision(
                        order_id=order_id,
                        channel=ch_name,
                        transaction_id=txn.transaction_id,
                        revision=txn.fact_revision,
                        fact=fact,
                    )
                )
        except MallInvalidError:
            logger.error(
                "payment mall rejected revision rev=%d tx_tail=%s",
                txn.fact_revision,
                tail(txn.transaction_id),
            )
            await self.core.store.settle_mall(
                txn,
                lease_ms,
                MallSync(
                    state=txn.mall_state,
                    refund_reason=txn.mall_refund_reason,
                    revision=txn.mall_revision,
                    keep_syncing=False,
                    last_error="mall_invalid_revision",
                ),
                now_ms,
            )
            return
        except Exception as exc:
            await retry(error_class(exc))
            return

        if acked >= txn.fact_revision:
            await self.core.store.settle_mall(
                txn,
                lease_ms,
                MallSync(
                    state=txn.mall_state,
                    refund_reason=txn.mall_refund_reason,
                    revision=txn.fact_revision,
                    keep_syncing=True,
                ),
                now_ms,
            )
        else:
            logger.error(
                "payment mall revision behind expected=%d acked=%d tx_tail=%s",
                txn.fact_revision,
                acked,
                tail(txn.transaction_id),
            )
            await retry("mall_revision_behind")

    # ---------------------------------------------------------------------------
    # 3. 执行商城退款待办（process_refunds）
    # ---------------------------------------------------------------------------

    async def process_refunds_once(self) -> int:
        opts = self.core.options
        cursor = ""
        processed = 0
        for _ in range(REFUND_POLL_MAX_PAGES):
            async with asyncio.timeout(opts.background_call_timeout_sec):
                page = await self.core.mall.pending_refunds(cursor)
            for item in page.entries:
                ch = self.core.channel_by_name(item.channel)
                if ch is None:
                    continue
                try:
                    await self._refund_one(ch, item)
                    processed += 1
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error(
                        "payment refund_one failed channel=%s tx_tail=%s: %s",
                        ch.name,
                        tail(item.transaction_id),
                        exc,
                    )
            if not page.next_cursor or not page.entries:
                return processed
            cursor = page.next_cursor
        return processed

    async def _refund_one(self, ch: PaymentChannel, item: MallPendingRefund) -> None:
        from .model import txn_key

        opts = self.core.options
        tx_hi, tx_lo = txn_key(item.transaction_id)
        try:
            txn = await self.core.store.txn(ch.channel_id, tx_hi, tx_lo)
        except RecordNotFoundError:
            logger.error(
                "payment refund target missing channel=%s tx_tail=%s",
                ch.name,
                tail(item.transaction_id),
            )
            async with asyncio.timeout(opts.background_call_timeout_sec):
                await self._close_refund(ch, item, "TXN_NOT_FOUND")
            return

        if (
            txn.transaction_id != item.transaction_id
            or txn.paid_amount_minor != item.amount_minor
            or txn.paid_currency != item.currency
        ):
            logger.error(
                "payment refund fact mismatch channel=%s tx_tail=%s",
                ch.name,
                tail(item.transaction_id),
            )
            async with asyncio.timeout(opts.background_call_timeout_sec):
                await self._close_refund(ch, item, "TXN_FACT_MISMATCH")
            return

        # 渠道侧已被用户撤销且本服务从未发起退款：等待修订同步，不发起退款
        if is_revoked(txn.fact_revision) and txn.refund_state == REFUND_NONE:
            return

        now_ms = self.core.now_ms()
        if txn.refund_next_ms > now_ms:
            return

        refund_no = txn.out_refund_no or out_refund_no(ch.name, txn.transaction_id)
        lease_ms = now_ms + opts.lease_ms
        # 原子置为 REFUND_REQUESTED 并落盘 out_refund_no，防止己方退款回调竞态误翻 fact_revision
        claimed = await self.core.store.claim_refund(txn, refund_no, lease_ms, now_ms)
        if not claimed:
            return

        retry_at = now_ms + backoff_ms(txn.refund_attempts + 1, opts.refund_backoff_max_ms)
        cur_state = REFUND_REQUESTED if txn.refund_state == REFUND_NONE else txn.refund_state
        refund_id = txn.refund_id
        refund_ms = txn.refund_ms
        last_err = txn.refund_last_error

        if txn.refund_state == REFUND_REPORTED:
            try:
                async with asyncio.timeout(opts.background_call_timeout_sec):
                    await self._report_refund(ch, item, txn, refund_id)
            except MallConflictError:
                pass
            except Exception as exc:
                await self.core.store.save_refund(
                    txn,
                    lease_ms,
                    RefundProgress(
                        state=cur_state,
                        out_refund_no=refund_no,
                        refund_id=refund_id,
                        refund_ms=refund_ms,
                        next_ms=retry_at,
                        last_error=error_class(exc),
                    ),
                    now_ms,
                )
                return
            await self.core.store.save_refund(
                txn,
                lease_ms,
                RefundProgress(
                    state=cur_state,
                    out_refund_no=refund_no,
                    refund_id=refund_id,
                    refund_ms=refund_ms,
                    next_ms=now_ms + ONE_MINUTE_MS,
                    last_error=last_err,
                ),
                now_ms,
            )
            return

        if txn.refund_state == REFUND_FAILED:
            try:
                async with asyncio.timeout(opts.background_call_timeout_sec):
                    await self._close_refund(ch, item, last_err)
            except MallConflictError:
                pass
            except Exception:
                await self.core.store.save_refund(
                    txn,
                    lease_ms,
                    RefundProgress(
                        state=REFUND_FAILED,
                        out_refund_no=refund_no,
                        refund_id=refund_id,
                        refund_ms=refund_ms,
                        next_ms=retry_at,
                        last_error=last_err,
                    ),
                    now_ms,
                )
                return
            await self.core.store.save_refund(
                txn,
                lease_ms,
                RefundProgress(
                    state=REFUND_FAILED,
                    out_refund_no=refund_no,
                    refund_id=refund_id,
                    refund_ms=refund_ms,
                    next_ms=now_ms + ONE_MINUTE_MS,
                    last_error=last_err,
                ),
                now_ms,
            )
            return

        if cur_state != REFUND_SUCCEEDED:
            try:
                async with asyncio.timeout(opts.background_call_timeout_sec):
                    res = await ch.refund(
                        ChannelRefundRequest(
                            out_trade_no=txn.out_trade_no,
                            transaction_id=txn.transaction_id,
                            out_refund_no=refund_no,
                            amount_minor=txn.paid_amount_minor,
                            currency=txn.paid_currency,
                            reason=opts.refund_reason,
                            channel_token=txn.channel_token,
                            channel_order_id=txn.channel_order_id,
                        )
                    )
            except Exception as exc:
                await self.core.store.save_refund(
                    txn,
                    lease_ms,
                    RefundProgress(
                        state=cur_state,
                        out_refund_no=refund_no,
                        refund_id=refund_id,
                        refund_ms=refund_ms,
                        next_ms=retry_at,
                        last_error=error_class(exc),
                    ),
                    now_ms,
                )
                return

            if res.state == ChannelRefundState.PROCESSING:
                await self.core.store.save_refund(
                    txn,
                    lease_ms,
                    RefundProgress(
                        state=REFUND_REQUESTED,
                        out_refund_no=refund_no,
                        refund_id=refund_id,
                        refund_ms=refund_ms,
                        next_ms=retry_at,
                        last_error=res.detail,
                    ),
                    now_ms,
                )
                return

            if res.state == ChannelRefundState.FAILED:
                logger.error(
                    "payment channel refund failed channel=%s tx_tail=%s detail=%s",
                    ch.name,
                    tail(txn.transaction_id),
                    res.detail,
                )
                next_ms = now_ms + ONE_MINUTE_MS
                try:
                    async with asyncio.timeout(opts.background_call_timeout_sec):
                        await self._close_refund(ch, item, res.detail)
                except MallConflictError:
                    pass
                except Exception:
                    next_ms = retry_at
                await self.core.store.save_refund(
                    txn,
                    lease_ms,
                    RefundProgress(
                        state=REFUND_FAILED,
                        out_refund_no=refund_no,
                        refund_id=refund_id,
                        refund_ms=refund_ms,
                        next_ms=next_ms,
                        last_error=res.detail,
                    ),
                    now_ms,
                )
                return

            # SUCCEEDED：先落盘 REFUND_SUCCEEDED（保留 lease_ms），再释放意图并回执商城
            refund_id = res.refund_id
            refund_ms = res.at_ms if res.at_ms > 0 else self.core.now_ms()
            cur_state = REFUND_SUCCEEDED
            await self.core.store.save_refund(
                txn,
                lease_ms,
                RefundProgress(
                    state=REFUND_SUCCEEDED,
                    out_refund_no=refund_no,
                    refund_id=refund_id,
                    refund_ms=refund_ms,
                    next_ms=lease_ms,
                    last_error="",
                ),
                now_ms,
            )

        await self.core.release_refunded(txn, item.reason, now_ms)

        try:
            async with asyncio.timeout(opts.background_call_timeout_sec):
                await self._report_refund(ch, item, txn, refund_id)
        except MallConflictError:
            logger.error(
                "payment mall refund receipt conflict tx_tail=%s",
                tail(txn.transaction_id),
            )
            await self.core.store.save_refund(
                txn,
                lease_ms,
                RefundProgress(
                    state=REFUND_REPORTED,
                    out_refund_no=refund_no,
                    refund_id=refund_id,
                    refund_ms=refund_ms,
                    next_ms=now_ms + ONE_HOUR_MS,
                    last_error="mall_conflict",
                ),
                now_ms,
            )
            return
        except Exception as exc:
            await self.core.store.save_refund(
                txn,
                lease_ms,
                RefundProgress(
                    state=REFUND_SUCCEEDED,
                    out_refund_no=refund_no,
                    refund_id=refund_id,
                    refund_ms=refund_ms,
                    next_ms=retry_at,
                    last_error=error_class(exc),
                ),
                now_ms,
            )
            return

        await self.core.store.save_refund(
            txn,
            lease_ms,
            RefundProgress(
                state=REFUND_REPORTED,
                out_refund_no=refund_no,
                refund_id=refund_id,
                refund_ms=refund_ms,
                next_ms=0,
                last_error="",
            ),
            now_ms,
        )

    async def _report_refund(
        self, ch: PaymentChannel, item: MallPendingRefund, txn: PaymentTxnRecord, refund_id: str
    ) -> None:
        await self.core.mall.report_refund(
            MallRefund(
                order_id=item.order_id,
                channel=ch.name,
                payment_transaction_id=txn.transaction_id,
                transaction_id=refund_id,
                amount_minor=txn.paid_amount_minor,
                currency=txn.paid_currency,
            )
        )

    async def _close_refund(self, ch: PaymentChannel, item: MallPendingRefund, reason: str) -> None:
        await self.core.mall.close_refund(
            MallRefundClosure(
                order_id=item.order_id,
                channel=ch.name,
                payment_transaction_id=item.transaction_id,
                reason=close_reason(reason),
            )
        )

    # ---------------------------------------------------------------------------
    # 4. 渠道侧收尾（finalize）
    # ---------------------------------------------------------------------------

    async def finalize_once(self) -> int:
        opts = self.core.options
        due = await self.core.store.due_finalize(self.core.now_ms(), opts.batch)
        processed = 0
        for txn in due:
            try:
                await self._finalize_one(txn)
                processed += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("payment finalize failed tx_tail=%s: %s", tail(txn.transaction_id), exc)
        return processed

    async def _finalize_one(self, txn: PaymentTxnRecord) -> None:
        opts = self.core.options
        now_ms = self.core.now_ms()
        lease_ms = now_ms + opts.lease_ms
        claimed = await self.core.store.claim_finalize(txn, lease_ms, now_ms)
        if not claimed:
            return

        if is_revoked(txn.fact_revision):
            await self.core.store.save_finalize(txn, lease_ms, FINALIZE_FAILED, 0, now_ms)
            return

        ch = self.core.channel_by_id(txn.channel)
        if ch is None or not isinstance(ch, FinalizerChannel):
            await self.core.store.save_finalize(txn, lease_ms, FINALIZE_PENDING, now_ms + ONE_HOUR_MS, now_ms)
            return

        try:
            async with asyncio.timeout(opts.background_call_timeout_sec):
                await ch.finalize(txn.product_id, txn.channel_token)
        except ChannelCallError as exc:
            if exc.status in (400, 404, 410):
                logger.error(
                    "payment finalize permanently rejected channel=%s tx_tail=%s status=%d",
                    ch.name,
                    tail(txn.transaction_id),
                    exc.status,
                )
                await self.core.store.save_finalize(txn, lease_ms, FINALIZE_FAILED, 0, now_ms)
                return
            attempts = min((now_ms - txn.created_ms) // 60_000, 10) if now_ms > txn.created_ms else 0
            await self.core.store.save_finalize(
                txn, lease_ms, FINALIZE_PENDING, now_ms + backoff_ms(attempts, 10 * ONE_MINUTE_MS), now_ms
            )
            return
        except Exception:
            attempts = min((now_ms - txn.created_ms) // 60_000, 10) if now_ms > txn.created_ms else 0
            await self.core.store.save_finalize(
                txn, lease_ms, FINALIZE_PENDING, now_ms + backoff_ms(attempts, 10 * ONE_MINUTE_MS), now_ms
            )
            return

        await self.core.store.save_finalize(txn, lease_ms, FINALIZE_DONE, 0, now_ms)

    # ---------------------------------------------------------------------------
    # 5. 补拉渠道历史（poll_history）
    # ---------------------------------------------------------------------------

    async def poll_history_once(self) -> int:
        processed = 0
        for ch in self.core.all_channels():
            if not isinstance(ch, HistoryPollerChannel):
                continue
            try:
                await self._poll_one(ch)
                processed += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("payment poll_history failed channel=%s: %s", ch.name, exc)
        return processed

    async def _poll_one(self, ch: PaymentChannel) -> None:
        assert isinstance(ch, HistoryPollerChannel)
        opts = self.core.options
        owner = (secrets.randbits(63) << 1) | 1
        now_ms = self.core.now_ms()
        initial_ms = max(0, now_ms - opts.history_lookback_ms)
        cursor_ms, claimed = await self.core.store.claim_cursor(
            ch.channel_id, owner, initial_ms, now_ms + HISTORY_LEASE_MS, now_ms
        )
        if not claimed:
            return

        since_ms = max(0, cursor_ms - HISTORY_OVERLAP_MS)
        try:
            async with asyncio.timeout(25.0):
                notes, through_ms = await ch.poll_history(since_ms)
                for note in notes:
                    await self.core.handle_notification(ch, note)
        except Exception:
            await self.core.store.advance_cursor(ch.channel_id, owner, cursor_ms, self.core.now_ms())
            raise
        await self.core.store.advance_cursor(ch.channel_id, owner, through_ms, self.core.now_ms())
