"""payment 核心领域服务（PaymentCore）与 gRPC 协议适配层（PaymentGrpcService）。

- ``PaymentCore`` 严格对齐 Go ``go/payment/internal/core/service.go`` 与 ``minor.go`` 的全部状态机不变量；
- ``PaymentGrpcService`` 挂载 ``paymentpb.ClientPlayerPayment``（4 个 RPC），使用 Gate Ed25519
  fail-closed 鉴权，并将业务/存储结果封装为 gRPC OK + ``TipInfoMessage`` + ``server_now_ms``。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Iterable, Mapping

import grpc

from ..login.caller_auth import AuthError, CallerVerifier, SESSION_DETAIL, parse_details
from ..protocol import Protocol
from .channels import PaymentChannel, ReceiptChannel
from .errors import (
    ChannelMismatchError,
    ChannelUnavailableError,
    CurrencyUnsupportedError,
    DailyLimitError,
    FactConflictError,
    InvalidKeyError,
    InvalidRequestError,
    LoginRequiredError,
    MallInvalidError,
    MallNotFoundError,
    MinorDailyLimitError,
    MinorMonthlyLimitError,
    MinorNotAllowedError,
    MinorSingleLimitError,
    MonthlyLimitError,
    OrderExpiredError,
    OrderNotFoundError,
    OrderNotPayableError,
    OrderPaidError,
    PaymentBusyError,
    PaymentError,
    QueryUnsupportedError,
    RealNameRequiredError,
    RecordNotFoundError,
    StoreUnavailableError,
    ChannelLoginRequiredError,
    tip_id_for,
)
from .mall_client import MallClient
from .model import (
    ChannelNotification,
    ChannelOrder,
    ChannelPayment,
    ChannelPrepared,
    ChannelQueryRequest,
    ChannelQueryResult,
    ChannelReceipt,
    ChannelRef,
    ChannelRevocation,
    CreateIntentCommand,
    CreateIntentResult,
    FINALIZE_PENDING,
    INTENT_CLOSED,
    INTENT_CREATED,
    INTENT_PAID,
    LIMIT_CURRENCY,
    MALL_BOUND,
    MALL_ORPHAN,
    MALL_PENDING,
    MALL_REFUND_QUEUED,
    MINOR_LATE_OVER_LIMIT_REASON,
    ORDER_STATE_AWAITING_PAYMENT,
    PAY_CHANNEL_APPLE,
    PAY_CHANNEL_GOOGLE,
    PaymentIntentRecord,
    PaymentOptions,
    PaymentTxnRecord,
    ProductMapping,
    REFUND_NONE,
    REFUND_REPORTED,
    REFUND_SUCCEEDED,
    REVOCATION_TOMBSTONE_REASON,
    SubmitPurchaseCommand,
    SubmitPurchaseResult,
    TestPolicy,
    TradeState,
    age_at,
    birth_date_from_pi,
    is_platform_managed_channel,
    is_revoked,
    is_valid_platform,
    minor_age_rule,
    order_id_from_out_trade_no,
    order_key,
    out_refund_no,
    out_trade_no,
    refunds_orders_own_payment,
    spend_limit_for_age,
    tail,
    txn_key,
    valid_transaction_id,
)
from .store import PaymentStore

logger = logging.getLogger(__name__)

SERVICE_CLIENT = "paymentpb.ClientPlayerPayment"
CODE = grpc.StatusCode


def is_minor_intent(intent: PaymentIntentRecord) -> bool:
    """判断意图是否绑定了未成年人额度主体。"""
    return intent.spender_hi != 0 or intent.spender_lo != 0


def counts_for_intent(fact: PaymentTxnRecord, intent: PaymentIntentRecord | None) -> bool:
    """判断这笔付款事实是否算数（可驱动意图转 PAID）：非人工、未被本服务退款，且金额币种与意图一致。"""
    if fact.mall_state == MALL_ORPHAN or fact.refund_state in (REFUND_SUCCEEDED, REFUND_REPORTED):
        return False
    return intent is None or (
        fact.paid_amount_minor == intent.amount_minor and fact.paid_currency == intent.currency
    )


def drives_intent(txn: PaymentTxnRecord) -> bool:
    """判断一笔付款的事实翻转是否应驱动意图状态变更（仅商城确认归属订单的那笔）。"""
    if txn.mall_state == MALL_BOUND:
        return True
    if txn.mall_state == MALL_REFUND_QUEUED:
        return refunds_orders_own_payment(txn.mall_refund_reason)
    return False


def same_fact(a: PaymentTxnRecord, b: PaymentTxnRecord) -> bool:
    """比对同一交易号两次报告是否一致（与 Go core/service.go:sameFact 逐字一致）。"""
    if a.transaction_id != b.transaction_id or a.order_hi != b.order_hi or a.order_lo != b.order_lo:
        return False
    if a.store_amount_minor != 0 and b.store_amount_minor != 0 and a.store_amount_minor != b.store_amount_minor:
        return False
    if a.paid_amount_minor != 0 and b.paid_amount_minor != 0 and a.paid_amount_minor != b.paid_amount_minor:
        return False
    if a.paid_currency and b.paid_currency and a.paid_currency.lower() != b.paid_currency.lower():
        return False
    return True


class PaymentCore:
    """支付核心领域服务：意图创建/查询、商店回执核实、回调事实落库与修订翻转。"""

    def __init__(
        self,
        store: PaymentStore,
        mall: MallClient,
        channels: Iterable[PaymentChannel],
        platforms: Mapping[int, Iterable[int]],
        *,
        options: PaymentOptions | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.store = store
        self.mall = mall
        self.options = options or PaymentOptions()
        self._clock_ms = clock_ms
        self.kick_event = asyncio.Event()

        self._channels: dict[int, PaymentChannel] = {}
        self._by_name: dict[str, PaymentChannel] = {}
        for ch in channels:
            if ch.channel_id in self._channels:
                raise ValueError(f"payment: channel registered twice: {ch.name}")
            self._channels[ch.channel_id] = ch
            self._by_name[ch.name] = ch

        self._platforms: dict[int, tuple[int, ...]] = {}
        for platform, ch_list in platforms.items():
            ids = tuple(ch_list)
            for cid in ids:
                if cid not in self._channels:
                    raise ValueError(f"payment: platform {platform} lists disabled channel {cid}")
            self._platforms[platform] = ids

        self._products: dict[int, ProductMapping] = {}
        self._by_sku: dict[str, dict[str, int]] = {}
        for prod in self.options.products:
            if prod.sku_id <= 0:
                raise ValueError("payment: product mapping has sku 0")
            if prod.sku_id in self._products:
                raise ValueError(f"payment: sku mapped twice: {prod.sku_id}")
            self._products[prod.sku_id] = prod
            for ch_name, prod_id in prod.by_channel:
                ch_map = self._by_sku.setdefault(ch_name, {})
                if prod_id in ch_map:
                    raise ValueError(f"payment: product id {prod_id} mapped to two skus on {ch_name}")
                ch_map[prod_id] = prod.sku_id

        self._test_policies: dict[str, TestPolicy] = dict(self.options.test_policies)

    def now_ms(self) -> int:
        return self._clock_ms()

    def kick(self) -> None:
        """唤醒后台 push_mall 任务立即推进商城。"""
        self.kick_event.set()

    def channel_by_id(self, channel_id: int) -> PaymentChannel | None:
        return self._channels.get(channel_id)

    def channel_by_name(self, name: str) -> PaymentChannel | None:
        return self._by_name.get(name)

    def all_channels(self) -> tuple[PaymentChannel, ...]:
        return tuple(self._channels.values())

    def _allowed_on_platform(self, platform: int, channel_id: int) -> bool:
        return channel_id in self._platforms.get(platform, ())

    def _product_for(self, sku_id: int, channel_name: str) -> str:
        prod = self._products.get(sku_id)
        return prod.product_for(channel_name) if prod is not None else ""

    def _accepts_test(self, channel_name: str, intent: PaymentIntentRecord | None) -> bool:
        policy = self._test_policies.get(channel_name)
        if policy is None or not policy.accept:
            return False
        if not policy.players:
            return True
        return intent is not None and intent.player_id in policy.players

    # -- 客户端业务接口 ----------------------------------------------------------------------------

    def list_channels(self, platform: int) -> tuple[int, ...]:
        if not is_valid_platform(platform):
            raise InvalidRequestError("payment: invalid platform")
        return self._platforms.get(platform, ())

    async def create_intent(self, cmd: CreateIntentCommand) -> CreateIntentResult:
        if (
            cmd.player_id <= 0
            or not is_valid_platform(cmd.platform)
            or len(cmd.login_code) > 256
            or len(cmd.device_os) > 16
        ):
            raise InvalidRequestError("payment: invalid create intent request")
        try:
            hi, lo = order_key(cmd.order_id)
        except InvalidKeyError as exc:
            raise InvalidRequestError("payment: invalid order_id") from exc

        ch = self._channels.get(cmd.channel)
        if ch is None or not self._allowed_on_platform(cmd.platform, cmd.channel):
            raise ChannelUnavailableError("payment: channel is not available for this platform")

        now_ms = self.now_ms()
        try:
            intent = await self.store.intent(hi, lo)
        except RecordNotFoundError:
            intent = await self._open_intent(cmd.player_id, cmd.order_id, hi, lo, ch, now_ms)
        except PaymentError:
            raise
        except Exception as exc:
            logger.error("payment intent lookup failed order=%s: %s", cmd.order_id, exc)
            raise StoreUnavailableError("payment: store lookup failed") from exc

        # 别人的订单一律表现为不存在（31001），不泄露订单存在性
        if intent.player_id != cmd.player_id:
            raise OrderNotFoundError("payment: order not found")

        if intent.channel != cmd.channel:
            raise ChannelMismatchError("payment: order already uses another channel", intent=intent)

        if intent.state == INTENT_PAID:
            return CreateIntentResult(intent=intent)
        if intent.state == INTENT_CLOSED:
            raise OrderExpiredError("payment: order intent is closed", intent=intent)

        if not isinstance(ch, ReceiptChannel) and now_ms + self.options.min_pay_window_ms >= intent.expire_ms:
            raise OrderExpiredError("payment: order payment window has passed", intent=intent)

        product = self._product_for(intent.sku_id, ch.name)
        if ch.needs_product() and not product:
            raise ChannelUnavailableError("payment: sku has no mapped product for channel", intent=intent)

        try:
            prepared = await self._prepare(ch, intent, product, cmd, now_ms)
        except OrderPaidError:
            # 渠道告知订单已付：当场查单记账，返回最新已付视图而非报错
            refreshed = await self.active_query(intent, force=True)
            return CreateIntentResult(intent=refreshed)
        except ChannelLoginRequiredError as exc:
            raise LoginRequiredError("payment: fresh mini-game login required", intent=intent) from exc
        except StoreUnavailableError as exc:
            raise StoreUnavailableError(str(exc), intent=intent) from exc
        except Exception as exc:
            logger.error("payment prepare failed channel=%s order=%s: %s", ch.name, cmd.order_id, exc)
            raise PaymentBusyError("payment: channel prepare temporarily failed", intent=intent) from exc

        return CreateIntentResult(intent=intent, params=prepared.params)

    async def _open_intent(
        self,
        player_id: int,
        order_id: str,
        hi: int,
        lo: int,
        ch: PaymentChannel,
        now_ms: int,
    ) -> PaymentIntentRecord:
        try:
            async with asyncio.timeout(self.options.request_call_timeout_sec):
                order = await self.mall.order_view(order_id)
        except (MallNotFoundError, MallInvalidError) as exc:
            raise OrderNotFoundError("payment: order not found") from exc
        except Exception as exc:
            logger.error("payment mall order_view failed order=%s: %s", order_id, exc)
            raise PaymentBusyError("payment: mall temporarily unreachable") from exc

        is_receipt = isinstance(ch, ReceiptChannel)
        if order.buyer_id != str(player_id):
            raise OrderNotFoundError("payment: order not found")
        if order.state != ORDER_STATE_AWAITING_PAYMENT or order.amount_minor <= 0:
            raise OrderNotPayableError("payment: order is not awaiting payment")
        if not ch.supports(order.currency):
            raise CurrencyUnsupportedError("payment: channel does not support currency")
        if ch.needs_product() and not self._product_for(order.sku_id, ch.name):
            raise ChannelUnavailableError("payment: sku not configured for product-priced channel")
        if not is_receipt and order.payment_deadline_ms <= now_ms + self.options.min_pay_window_ms:
            raise OrderExpiredError("payment: order payment deadline passed")

        expire_ms = order.payment_deadline_ms
        if is_receipt:
            expire_ms = order.payment_deadline_ms + self.options.receipt_window_ms

        rec = PaymentIntentRecord(
            order_hi=hi,
            order_lo=lo,
            order_id=order_id,
            player_id=player_id,
            channel=ch.channel_id,
            amount_minor=order.amount_minor,
            currency=order.currency,
            state=INTENT_CREATED,
            expire_ms=expire_ms,
            next_check_ms=now_ms + self.options.first_check_delay_ms,
            created_ms=now_ms,
            updated_ms=now_ms,
            sku_id=order.sku_id,
        )
        await self._gate_buyer(rec, now_ms)
        return await self._insert_intent(rec, now_ms)

    async def _gate_buyer(self, rec: PaymentIntentRecord, now_ms: int) -> None:
        if self.options.real_names is None:
            return
        try:
            async with asyncio.timeout(self.options.request_call_timeout_sec):
                pi, found = await self.options.real_names.real_name_of(rec.player_id)
        except Exception as exc:
            logger.error("payment real_name lookup failed order=%s: %s", rec.order_id, exc)
            raise PaymentBusyError("payment: real-name service temporarily unreachable") from exc

        if not found:
            logger.error("payment account directory missing buyer order=%s", rec.order_id)
            raise RealNameRequiredError("payment: real-name verification required")
        if not pi and is_platform_managed_channel(rec.channel):
            return
        if not pi:
            raise RealNameRequiredError("payment: real-name verification required")

        birth = birth_date_from_pi(pi)
        if birth is None:
            logger.error("payment malformed real-name pi order=%s", rec.order_id)
            raise PaymentBusyError("payment: invalid real-name record")

        age = age_at(birth, now_ms)
        rule = minor_age_rule(age)
        if rule.unlimited:
            return
        if rule.single_fen == 0:
            raise MinorNotAllowedError("payment: buyers under 8 may not pay")
        if rec.currency != LIMIT_CURRENCY:
            raise CurrencyUnsupportedError("payment: minor spending limit requires CNY")
        if rec.amount_minor > rule.single_fen:
            raise MinorSingleLimitError("payment: exceeds minor single-payment limit")

        from .model import spender_key

        rec.spender_hi, rec.spender_lo = spender_key(pi)
        rec.minor_age = age

    async def _insert_intent(self, rec: PaymentIntentRecord, now_ms: int) -> PaymentIntentRecord:
        try:
            if not is_minor_intent(rec):
                stored, _ = await self.store.insert_intent(rec)
                return stored
            limit = spend_limit_for_age(rec.minor_age, now_ms)
            stored, _ = await self.store.reserve_intent(rec, limit)
            return stored
        except MonthlyLimitError as exc:
            raise MinorMonthlyLimitError("payment: minor monthly spending limit reached") from exc
        except DailyLimitError as exc:
            raise MinorDailyLimitError("payment: minor daily spending limit reached") from exc
        except Exception as exc:
            logger.error("payment insert_intent failed order=%s: %s", rec.order_id, exc)
            raise StoreUnavailableError("payment: store insert_intent failed") from exc

    async def _prepare(
        self,
        ch: PaymentChannel,
        intent: PaymentIntentRecord,
        product: str,
        cmd: CreateIntentCommand,
        now_ms: int,
    ) -> ChannelPrepared:
        trade_no = out_trade_no(intent.order_id)
        prod = self._products.get(intent.sku_id)
        title = prod.title if prod is not None else ""
        order = ChannelOrder(
            order_id=intent.order_id,
            out_trade_no=trade_no,
            player_id=intent.player_id,
            amount_minor=intent.amount_minor,
            currency=intent.currency,
            expire_ms=intent.expire_ms,
            product_id=product,
            title=title,
            login_code=cmd.login_code,
            device_os=cmd.device_os,
        )
        async with asyncio.timeout(self.options.request_call_timeout_sec):
            prepared = await ch.prepare(order, intent.channel_ref, intent.channel_ref_ms)

        if prepared.ref.value or prepared.payer_id:
            ref_val = prepared.ref.value or intent.channel_ref
            payer = prepared.payer_id or intent.payer_id
            try:
                await self.store.set_channel_ref(
                    intent.order_hi,
                    intent.order_lo,
                    ref_val,
                    prepared.ref.at_ms,
                    payer,
                    now_ms,
                )
            except Exception as exc:
                logger.error("payment set_channel_ref failed order=%s: %s", intent.order_id, exc)
                raise StoreUnavailableError("payment: saving channel ref failed") from exc
        return prepared

    async def get_intent(self, player_id: int, order_id: str) -> PaymentIntentRecord:
        if player_id <= 0:
            raise InvalidRequestError("payment: invalid player_id")
        try:
            hi, lo = order_key(order_id)
        except InvalidKeyError as exc:
            raise InvalidRequestError("payment: invalid order_id") from exc

        try:
            intent = await self.store.intent(hi, lo)
        except RecordNotFoundError as exc:
            raise OrderNotFoundError("payment: order not found") from exc
        except Exception as exc:
            logger.error("payment get_intent store failed order=%s: %s", order_id, exc)
            raise StoreUnavailableError("payment: store lookup failed") from exc

        if intent.player_id != player_id:
            raise OrderNotFoundError("payment: order not found")

        if intent.state == INTENT_CREATED:
            intent = await self.active_query(intent, force=False)
        return intent

    async def active_query(self, intent: PaymentIntentRecord, *, force: bool) -> PaymentIntentRecord:
        ch = self._channels.get(intent.channel)
        if ch is None or isinstance(ch, ReceiptChannel):
            return intent
        now_ms = self.now_ms()
        if not force:
            if now_ms < intent.checked_ms + self.options.active_query_interval_ms:
                return intent
            try:
                claimed = await self.store.claim_active_query(intent, now_ms)
            except Exception as exc:
                logger.warning("payment claim_active_query failed order=%s: %s", intent.order_id, exc)
                return intent
            if not claimed:
                return intent

        try:
            async with asyncio.timeout(self.options.request_call_timeout_sec):
                result = await self.query_channel(ch, intent)
        except Exception:
            return intent

        if result.payment is None or result.state not in (TradeState.PAID, TradeState.REFUNDED_OUTSIDE):
            return intent

        try:
            await self.record(ch, result.payment, refunded_outside=(result.state == TradeState.REFUNDED_OUTSIDE))
        except Exception as exc:
            logger.warning("payment active_query record failed order=%s: %s", intent.order_id, exc)
            return intent

        try:
            return await self.store.intent(intent.order_hi, intent.order_lo)
        except Exception:
            return intent

    async def query_channel(self, ch: PaymentChannel, intent: PaymentIntentRecord) -> ChannelQueryResult:
        trade_no = out_trade_no(intent.order_id)
        req = ChannelQueryRequest(
            out_trade_no=trade_no,
            payer_id=intent.payer_id,
            ref=ChannelRef(value=intent.channel_ref, at_ms=intent.channel_ref_ms),
            amount_minor=intent.amount_minor,
            currency=intent.currency,
        )
        result = await ch.query(req)
        if result.state in (TradeState.PAID, TradeState.REFUNDED_OUTSIDE) and result.payment is None:
            raise RuntimeError("payment: channel reported paid without payment details")
        return result

    async def submit_purchase(self, cmd: SubmitPurchaseCommand) -> SubmitPurchaseResult:
        if (
            cmd.player_id <= 0
            or len(cmd.apple_transaction_id) > 64
            or len(cmd.google_purchase_token) > 512
            or len(cmd.google_product_id) > 128
        ):
            raise InvalidRequestError("payment: invalid submit_purchase request")
        ch = self._channels.get(cmd.channel)
        if ch is None:
            raise ChannelUnavailableError("payment: channel is not enabled")
        if not isinstance(ch, ReceiptChannel) or (not cmd.apple_transaction_id and not cmd.google_purchase_token):
            raise InvalidRequestError("payment: channel does not accept receipt submission or token empty")

        receipt = ChannelReceipt(
            transaction_id=cmd.apple_transaction_id,
            purchase_token=cmd.google_purchase_token,
            product_id=cmd.google_product_id,
        )
        try:
            async with asyncio.timeout(self.options.request_call_timeout_sec):
                result = await ch.verify(receipt)
        except Exception as exc:
            logger.error("payment verify failed channel=%s: %s", ch.name, exc)
            owned = await self._owned_intent(cmd.player_id, cmd.order_id)
            raise PaymentBusyError("payment: receipt verification temporarily failed", intent=owned) from exc

        order_id = cmd.order_id
        finish = True
        if result.state in (TradeState.PAID, TradeState.REFUNDED_OUTSIDE):
            if result.payment is None:
                owned = await self._owned_intent(cmd.player_id, order_id)
                raise PaymentBusyError("payment: channel verify returned empty payment", intent=owned)
            if result.payment.order_id and result.payment.order_id != order_id:
                logger.info(
                    "payment submitted purchase bound to different order channel=%s submitted=%s bound=%s",
                    ch.name,
                    order_id,
                    result.payment.order_id,
                )
            try:
                stored = await self.record(
                    ch,
                    result.payment,
                    refunded_outside=(result.state == TradeState.REFUNDED_OUTSIDE),
                )
            except FactConflictError:
                # Google Play 冲突交易不 finish（保留 3 天自动退款）；App Store 必须 finish 防止锁死复购
                finish = ch.channel_id != PAY_CHANNEL_GOOGLE
            except Exception as exc:
                owned = await self._owned_intent(cmd.player_id, order_id)
                raise StoreUnavailableError("payment: recording submitted purchase failed", intent=owned) from exc
            else:
                if ch.channel_id == PAY_CHANNEL_GOOGLE:
                    # Google 仅当进入商城记账链路（PENDING/BOUND/REFUND_QUEUED）才允许客户端确认/消耗
                    finish = stored.mall_state in (MALL_PENDING, MALL_BOUND, MALL_REFUND_QUEUED)
                if result.state == TradeState.PAID and result.payment.order_id:
                    order_id = result.payment.order_id
        elif result.state == TradeState.NOT_PAID:
            finish = False
        elif result.state == TradeState.CLOSED:
            if result.payment is not None:
                try:
                    await self.revoke(
                        ch,
                        ChannelRevocation(
                            transaction_id=result.payment.transaction_id,
                            revoked=True,
                            observed_ms=self.now_ms(),
                            detail="verify",
                        ),
                    )
                except FactConflictError:
                    pass
                except Exception as exc:
                    raise StoreUnavailableError("payment: revoking closed purchase failed") from exc

        owned = await self._owned_intent(cmd.player_id, order_id)
        return SubmitPurchaseResult(intent=owned, finish_transaction=finish)

    async def _owned_intent(self, player_id: int, order_id: str) -> PaymentIntentRecord | None:
        try:
            hi, lo = order_key(order_id)
            intent = await self.store.intent(hi, lo)
        except Exception:
            return None
        return intent if intent.player_id == player_id else None

    # -- 回调处理、事实落库与修订翻转 --------------------------------------------------------------

    async def handle_notification(self, ch: PaymentChannel, note: ChannelNotification) -> None:
        errors: list[Exception] = []
        if note.payment is not None:
            try:
                await self.record(ch, note.payment, refunded_outside=False)
            except FactConflictError:
                pass
            except Exception as exc:
                errors.append(exc)
        for revocation in note.revocations:
            try:
                await self.revoke(ch, revocation)
            except FactConflictError:
                pass
            except Exception as exc:
                errors.append(exc)
        if isinstance(ch, ReceiptChannel):
            for receipt in note.receipts:
                try:
                    await self._verify_receipt(ch, receipt)
                except Exception as exc:
                    errors.append(exc)
        if errors:
            raise errors[0]

    async def _verify_receipt(self, ch: PaymentChannel, receipt: ChannelReceipt) -> None:
        assert isinstance(ch, ReceiptChannel)
        async with asyncio.timeout(self.options.background_call_timeout_sec):
            result = await ch.verify(receipt)
        if result.state in (TradeState.PAID, TradeState.REFUNDED_OUTSIDE) and result.payment is not None:
            await self.record(ch, result.payment, refunded_outside=(result.state == TradeState.REFUNDED_OUTSIDE))
        elif result.state == TradeState.CLOSED and result.payment is not None:
            await self.revoke(
                ch,
                ChannelRevocation(
                    transaction_id=result.payment.transaction_id,
                    revoked=True,
                    observed_ms=self.now_ms(),
                    detail="recheck",
                ),
            )

    async def record_payment(self, ch: PaymentChannel, payment: ChannelPayment) -> PaymentTxnRecord:
        return await self.record(ch, payment, refunded_outside=False)

    async def record(
        self,
        ch: PaymentChannel,
        payment: ChannelPayment,
        *,
        refunded_outside: bool,
    ) -> PaymentTxnRecord:
        now_ms = self.now_ms()
        paid_ms = payment.paid_ms if payment.paid_ms > 0 else now_ms
        tx_hi, tx_lo = txn_key(payment.transaction_id)

        rec = PaymentTxnRecord(
            channel=ch.channel_id,
            txn_hi=tx_hi,
            txn_lo=tx_lo,
            transaction_id=payment.transaction_id,
            out_trade_no=payment.out_trade_no,
            paid_amount_minor=payment.amount_minor,
            paid_currency=payment.currency,
            paid_ms=paid_ms,
            mall_state=MALL_PENDING,
            mall_due=1,
            next_attempt_ms=now_ms,
            created_ms=now_ms,
            updated_ms=now_ms,
            channel_token=payment.channel_token,
            channel_order_id=payment.channel_order_id,
            store_amount_minor=payment.store_amount_minor,
            store_currency=payment.store_currency,
            product_id=payment.product_id,
            fact_observed_ms=paid_ms,
        )

        order_id = payment.order_id
        if not order_id and payment.out_trade_no:
            try:
                order_id = order_id_from_out_trade_no(payment.out_trade_no)
            except InvalidKeyError:
                order_id = ""
        if not rec.out_trade_no and order_id:
            try:
                rec.out_trade_no = out_trade_no(order_id)
            except InvalidKeyError:
                pass

        def mark_orphan(reason: str) -> None:
            if rec.mall_state != MALL_ORPHAN:
                rec.mall_state = MALL_ORPHAN
                rec.mall_due = 0
                rec.mall_last_error = reason

        intent: PaymentIntentRecord | None = None
        hi = lo = 0
        key_valid = False
        try:
            hi, lo = order_key(order_id)
            key_valid = True
        except InvalidKeyError:
            pass

        if not valid_transaction_id(payment.transaction_id):
            mark_orphan("transaction id cannot be reported to the mall")
        elif not key_valid:
            mark_orphan("the payment is not bound to an order issued by this service")
        else:
            rec.order_hi, rec.order_lo = hi, lo
            try:
                intent = await self.store.intent(hi, lo)
                rec.player_id = intent.player_id
            except RecordNotFoundError:
                intent = None

        if rec.mall_state == MALL_PENDING:
            self._screen(ch, payment, intent, rec, mark_orphan)

        if refunded_outside:
            rec.fact_revision = 1
            mark_orphan("refunded at the channel before this service recorded it; not delivered")

        if payment.needs_finalize and rec.mall_state == MALL_PENDING:
            rec.finalize_state = FINALIZE_PENDING
            rec.finalize_next_ms = now_ms

        intent_settled = False
        if (
            intent is not None
            and is_minor_intent(intent)
            and counts_for_intent(rec, intent)
            and not refunded_outside
        ):
            minor_paid_ms = max(now_ms, intent.created_ms)
            limit = spend_limit_for_age(intent.minor_age, minor_paid_ms)
            outcome = await self.store.record_minor_payment_txn(
                rec, intent, limit, minor_paid_ms, now_ms, MINOR_LATE_OVER_LIMIT_REASON
            )
            stored, inserted = outcome.stored, outcome.inserted
            if inserted:
                intent_settled = True
        else:
            stored, inserted = await self.store.insert_txn(rec)

        # 退款回调先于付款回调到达时留下的墓碑行：直接按已撤销重复吞掉，不报冲突、不发货
        if (
            not inserted
            and stored.fact_revision > 0
            and is_revoked(stored.fact_revision)
            and stored.mall_last_error == REVOCATION_TOMBSTONE_REASON
            and stored.transaction_id == rec.transaction_id
        ):
            return stored

        if not inserted and not same_fact(stored, rec):
            logger.error(
                "payment fact conflict channel=%s tx_tail=%s",
                ch.name,
                tail(payment.transaction_id),
            )
            raise FactConflictError("payment: channel reported a conflicting transaction")

        if (
            not intent_settled
            and (stored.order_hi != 0 or stored.order_lo != 0)
            and (inserted or not is_revoked(stored.fact_revision))
        ):
            await self._settle_intent(ch, stored, intent, inserted, refunded_outside, now_ms)

        if inserted and stored.mall_state == MALL_ORPHAN:
            logger.error(
                "payment orphan recorded channel=%s tx_tail=%s order=%s reason=%s",
                ch.name,
                tail(payment.transaction_id),
                order_id,
                stored.mall_last_error,
            )

        self.kick()
        return stored

    def _screen(
        self,
        ch: PaymentChannel,
        payment: ChannelPayment,
        intent: PaymentIntentRecord | None,
        rec: PaymentTxnRecord,
        mark_orphan: Callable[[str], None],
    ) -> None:
        if payment.anomaly:
            mark_orphan(payment.anomaly)
            return
        if payment.store_priced:
            if intent is None:
                mark_orphan("store-priced payment has no payment intent to take the order amount from")
                return
            rec.paid_amount_minor = intent.amount_minor
            rec.paid_currency = intent.currency
            if (
                payment.store_amount_minor > 0
                and payment.store_currency == intent.currency
                and payment.store_amount_minor != intent.amount_minor
            ):
                mark_orphan("store price differs from the order price")
                return
        if (
            intent is not None
            and payment.product_id
            and ch.needs_product()
            and self._product_for(intent.sku_id, ch.name) != payment.product_id
        ):
            mark_orphan("product does not match the order sku")
            return
        if (
            isinstance(ch, ReceiptChannel)
            and intent is not None
            and payment.account_ref
            and ch.account_ref(intent.player_id) != payment.account_ref
        ):
            mark_orphan("account binding does not match the order buyer")
            return
        if payment.test and not self._accepts_test(ch.name, intent):
            mark_orphan("test transaction is not accepted in this environment")

    async def _settle_intent(
        self,
        ch: PaymentChannel,
        fact: PaymentTxnRecord,
        intent: PaymentIntentRecord | None,
        inserted: bool,
        refunded_outside: bool,
        now_ms: int,
    ) -> None:
        counts = counts_for_intent(fact, intent)
        if refunded_outside or (not counts and not isinstance(ch, ReceiptChannel)):
            await self.store.close_intent(fact.order_hi, fact.order_lo, now_ms)
            return
        if not counts:
            return
        if not inserted and intent is not None and intent.state == INTENT_CLOSED and is_minor_intent(intent):
            await self.recount_minor(intent, now_ms)
            return
        await self.store.mark_intent_paid(fact.order_hi, fact.order_lo, now_ms)

    async def recount_minor(self, intent: PaymentIntentRecord, now_ms: int) -> None:
        paid_ms = max(now_ms, intent.created_ms)
        limit = spend_limit_for_age(intent.minor_age, paid_ms)
        within = await self.store.revive_minor_intent(intent, limit, paid_ms, now_ms, force=True)
        if not within:
            logger.error(
                "payment compliance minor recounted over limit order=%s",
                intent.order_id,
            )

    async def apply_flip(self, order_hi: int, order_lo: int, state: int, now_ms: int) -> None:
        if state == INTENT_PAID:
            try:
                intent = await self.store.intent(order_hi, order_lo)
            except RecordNotFoundError:
                intent = None
            if intent is not None and is_minor_intent(intent):
                await self.recount_minor(intent, now_ms)
                return
        await self.store.set_intent_state(order_hi, order_lo, state, now_ms)

    async def release_refunded(self, txn: PaymentTxnRecord, reason: str, now_ms: int) -> None:
        if not refunds_orders_own_payment(reason) or (txn.order_hi == 0 and txn.order_lo == 0):
            return
        await self.store.set_intent_state(txn.order_hi, txn.order_lo, INTENT_CLOSED, now_ms)

    async def revoke(self, ch: PaymentChannel, revocation: ChannelRevocation) -> None:
        tx_hi, tx_lo = txn_key(revocation.transaction_id)
        observed_ms = revocation.observed_ms if revocation.observed_ms > 0 else self.now_ms()

        for _ in range(3):
            try:
                txn = await self.store.txn(ch.channel_id, tx_hi, tx_lo)
            except RecordNotFoundError:
                if revocation.revoked and revocation.payment is not None:
                    await self.record(ch, revocation.payment, refunded_outside=True)
                    return
                if revocation.revoked:
                    if revocation.out_refund_no and revocation.out_refund_no == out_refund_no(
                        ch.name, revocation.transaction_id
                    ):
                        return
                    # 退款回调先于付款回调到达：写 fact_revision=1 墓碑行拦截晚到的付款通知
                    order_hi = order_lo = 0
                    if revocation.out_trade_no:
                        try:
                            oid = order_id_from_out_trade_no(revocation.out_trade_no)
                            order_hi, order_lo = order_key(oid)
                        except InvalidKeyError:
                            pass
                    now_ms = self.now_ms()
                    tomb = PaymentTxnRecord(
                        channel=ch.channel_id,
                        txn_hi=tx_hi,
                        txn_lo=tx_lo,
                        transaction_id=revocation.transaction_id,
                        order_hi=order_hi,
                        order_lo=order_lo,
                        out_trade_no=revocation.out_trade_no,
                        mall_state=MALL_ORPHAN,
                        mall_due=0,
                        mall_last_error=REVOCATION_TOMBSTONE_REASON,
                        fact_revision=1,
                        fact_observed_ms=observed_ms,
                        created_ms=now_ms,
                        updated_ms=now_ms,
                    )
                    _, inserted = await self.store.insert_txn(tomb)
                    if not inserted:
                        continue
                    logger.info(
                        "payment revocation tombstone recorded channel=%s tx_tail=%s",
                        ch.name,
                        tail(revocation.transaction_id),
                    )
                    return
                logger.info(
                    "payment revocation ignored for unknown txn channel=%s tx_tail=%s",
                    ch.name,
                    tail(revocation.transaction_id),
                )
                return

            if txn.transaction_id != revocation.transaction_id:
                raise FactConflictError("payment: transaction hash collision")

            # 本服务自己发起的退款回调（已置 refund_state!=NONE 或匹配 out_refund_no）绝不翻转 fact_revision
            if txn.refund_state != REFUND_NONE or (
                revocation.out_refund_no
                and revocation.out_refund_no == out_refund_no(ch.name, txn.transaction_id)
            ):
                return

            # 乱序观测保护：过时观测不翻转事实
            if (txn.fact_revision > 0 and observed_ms <= txn.fact_observed_ms) or (
                txn.fact_revision == 0 and observed_ms < txn.fact_observed_ms
            ):
                return

            if is_revoked(txn.fact_revision) == revocation.revoked:
                if observed_ms > txn.fact_observed_ms:
                    await self.store.touch_fact_observed(txn, observed_ms, self.now_ms())
                return

            # 仅 App Store 允许撤销后再撤回（REVOKED -> PAID）
            if not revocation.revoked and ch.channel_id != PAY_CHANNEL_APPLE:
                return

            now_ms = self.now_ms()
            flipped = await self.store.flip_fact(txn, observed_ms, now_ms)
            if not flipped:
                continue

            if (txn.order_hi != 0 or txn.order_lo != 0) and drives_intent(txn):
                next_state = INTENT_CLOSED if revocation.revoked else INTENT_PAID
                await self.apply_flip(txn.order_hi, txn.order_lo, next_state, now_ms)

            logger.info(
                "payment fact flipped channel=%s tx_tail=%s revision=%d detail=%s",
                ch.name,
                tail(revocation.transaction_id),
                txn.fact_revision + 1,
                revocation.detail,
            )
            self.kick()
            return

        raise RuntimeError("payment: transaction fact kept changing while applying revocation")


# ---------------------------------------------------------------------------
# gRPC 适配层（paymentpb.ClientPlayerPayment）
# ---------------------------------------------------------------------------


def _has_session_metadata(context) -> bool:
    if context is None:
        return False
    for key, _ in context.invocation_metadata() or ():
        if key.lower() == SESSION_DETAIL:
            return True
    return False


def _fill_intent_view(target, rec: PaymentIntentRecord) -> None:
    target.order_id = rec.order_id
    target.channel = rec.channel
    target.state = rec.state
    target.amount_minor = rec.amount_minor
    target.currency = rec.currency
    target.expire_ms = rec.expire_ms


class PaymentGrpcService:
    """挂载 ``paymentpb.ClientPlayerPayment`` 的 gRPC 适配层（Gate Ed25519 fail-closed 鉴权）。"""

    def __init__(
        self,
        protocol: Protocol,
        core: PaymentCore,
        *,
        caller_verifier: CallerVerifier | None,
    ) -> None:
        self.protocol = protocol
        self.core = core
        self._caller_verifier = caller_verifier

    def services(self) -> dict[str, dict[str, Callable]]:
        return {
            SERVICE_CLIENT: {
                "ListPayChannels": self.list_pay_channels,
                "CreatePayIntent": self.create_pay_intent,
                "GetPayIntent": self.get_pay_intent,
                "SubmitPurchase": self.submit_purchase,
            }
        }

    async def _admit_client(self, context, path: str) -> int:
        if not _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, "client payment RPC requires verified gate session")
        if self._caller_verifier is None:
            await context.abort(CODE.UNAVAILABLE, "gate caller verifier not configured")
        try:
            raw = self._caller_verifier.authenticate(context.invocation_metadata(), path)
            if raw is None:
                await context.abort(CODE.PERMISSION_DENIED, "missing gate session identity")
            details = parse_details(self.protocol, raw)
        except AuthError as exc:
            await context.abort(CODE.UNAUTHENTICATED, f"invalid gate session signature: {exc}")
        account = str(details.authenticated_account).strip()
        if not details.player_id or not details.owner_revision or not account:
            await context.abort(CODE.PERMISSION_DENIED, "incomplete online role identity in gate session")
        return int(details.player_id)

    async def list_pay_channels(self, request, context, path: str):
        await self._admit_client(context, path)
        resp = self.protocol.message("paymentpb.ListPayChannelsResponse")
        try:
            channels = self.core.list_channels(int(request.platform))
            resp.channels.extend(channels)
        except Exception as exc:
            resp.error_message.id = tip_id_for(exc)
        return resp

    async def create_pay_intent(self, request, context, path: str):
        player_id = await self._admit_client(context, path)
        resp = self.protocol.message("paymentpb.CreatePayIntentResponse")
        try:
            created = await self.core.create_intent(
                CreateIntentCommand(
                    player_id=player_id,
                    order_id=str(request.order_id),
                    channel=int(request.channel),
                    platform=int(request.platform),
                    login_code=str(request.login_code),
                    device_os=str(request.device_os),
                )
            )
            if created.intent is not None:
                _fill_intent_view(resp.intent, created.intent)
            params = created.params
            if params.devpay is not None:
                resp.devpay.out_trade_no = params.devpay["out_trade_no"]
            if params.wechat_app is not None:
                for field_name, val in params.wechat_app.items():
                    setattr(resp.wechat_app, field_name, val)
            if params.alipay_app is not None:
                resp.alipay_app.order_string = params.alipay_app["order_string"]
            if params.wechat_minigame is not None:
                for field_name, val in params.wechat_minigame.items():
                    setattr(resp.wechat_minigame, field_name, val)
            if params.douyin_minigame is not None:
                for field_name, val in params.douyin_minigame.items():
                    setattr(resp.douyin_minigame, field_name, val)
            if params.apple is not None:
                for field_name, val in params.apple.items():
                    setattr(resp.apple, field_name, val)
            if params.google is not None:
                for field_name, val in params.google.items():
                    setattr(resp.google, field_name, val)
        except Exception as exc:
            resp.error_message.id = tip_id_for(exc)
            if isinstance(exc, PaymentError) and isinstance(exc.intent, PaymentIntentRecord):
                _fill_intent_view(resp.intent, exc.intent)
        resp.server_now_ms = self.core.now_ms()
        return resp

    async def get_pay_intent(self, request, context, path: str):
        player_id = await self._admit_client(context, path)
        resp = self.protocol.message("paymentpb.GetPayIntentResponse")
        try:
            intent = await self.core.get_intent(player_id, str(request.order_id))
            _fill_intent_view(resp.intent, intent)
        except Exception as exc:
            resp.error_message.id = tip_id_for(exc)
        resp.server_now_ms = self.core.now_ms()
        return resp

    async def submit_purchase(self, request, context, path: str):
        player_id = await self._admit_client(context, path)
        resp = self.protocol.message("paymentpb.SubmitPurchaseResponse")
        try:
            submitted = await self.core.submit_purchase(
                SubmitPurchaseCommand(
                    player_id=player_id,
                    channel=int(request.channel),
                    order_id=str(request.order_id),
                    apple_transaction_id=str(request.apple_transaction_id),
                    google_purchase_token=str(request.google_purchase_token),
                    google_product_id=str(request.google_product_id),
                )
            )
            if submitted.intent is not None:
                _fill_intent_view(resp.intent, submitted.intent)
            resp.finish_transaction = submitted.finish_transaction
        except Exception as exc:
            resp.error_message.id = tip_id_for(exc)
            resp.finish_transaction = False
            if isinstance(exc, PaymentError) and isinstance(exc.intent, PaymentIntentRecord):
                _fill_intent_view(resp.intent, exc.intent)
        resp.server_now_ms = self.core.now_ms()
        return resp
