"""人民币寄售（物品/宠物/货币/角色）合同协调器（与 Go trade/internal/commerce/rmb.go 对齐）。

复用 ``trade_commerce`` 与 ``trade_asset_op`` outbox：
- 所有外部 HTTP/gRPC 请求全部在 SQL 事务之外执行；
- 角色寄售经 ``RoleTradeAuthority``（DataService）+ ``RoleSealer``（SceneNodeGrpc）完成冻结/过户/回退；
- 后台 ``tick()`` 独立拉取商城交付队列与回退队列，单条故障隔离。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any

from google.protobuf.message import Message

from ..protocol import Protocol
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_LOGIN_ACCOUNT_PLAYER_FULL,
    ERR_TRADE_ASSET_NOT_TRADABLE,
    ERR_TRADE_CANNOT_TRADE_SELF,
    ERR_TRADE_ORDER_NOT_FOUND,
    ERR_TRADE_ORDER_STATE_CONFLICT,
    ERR_TRADE_REQUEST_CONFLICT,
    RoleDirectoryConflictError,
    RoleDirectoryFullError,
    RoleFenceStaleError,
    StoreUnavailableError,
    TradeCorruptContractError,
    TradeRejection,
    TradeRoleConflictError,
    TradeStaleObservationError,
)
from .mall_adapter import (
    MALL_ORDER_ID_RE,
    MALL_POLL_LIMIT,
    MallAdapter,
    canonical_uint64,
    validate_mall_page,
)
from .model import (
    ASSET_STREAM_TRADE_DEBIT,
    COMMERCE_CANCELLED,
    COMMERCE_CANCEL_PENDING,
    COMMERCE_DELIVERING,
    COMMERCE_ESCROWING,
    COMMERCE_KIND_RMB_CONSIGNMENT,
    COMMERCE_MANUAL,
    COMMERCE_REFUND_PENDING,
    COMMERCE_SETTLED,
    COMMERCE_WAITING,
    CURRENCY_BIND_DIAMOND,
    CURRENCY_GOLD,
    MAX_RMB_PRICE_FEN,
    MAX_RMB_TITLE_UTF16,
    QUARANTINE_DETAIL_MAX_BYTES,
    RoleOriginal,
    TradePolicy,
    build_asset_bundle,
    transfer_bundle,
    truncate_utf8,
    valid_account,
)
from .role_trade import (
    ROLE_TRADE_STATE_CANCELLED,
    ROLE_TRADE_STATE_CANCEL_PENDING,
    ROLE_TRADE_STATE_FROZEN,
    ROLE_TRADE_STATE_TRANSFERRED,
    RoleSealer,
    RoleTradeAuthority,
)

logger = logging.getLogger(__name__)

MANUAL_REASON_RMB_RETURN = "rmb_return_failed"
_RMB_DEFER_MS = 30_000
_SEAL_BATCH_LIMIT = 8
_SEAL_CALL_TIMEOUT_SEC = 0.35

_RMB_CANCELLABLE_STAGES = frozenset((
    "rmb_escrow",
    "rmb_freezing",
    "rmb_ready",
    "rmb_registering",
    "rmb_listed",
    "rmb_return",
))


def rmb_cancellable(stage: str) -> bool:
    """人民币寄售可撤阶段白名单（买家付款后、人工单与终态一律拒绝撤单）。"""
    return stage in _RMB_CANCELLABLE_STAGES


def _utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _find_leg(w: Message, role: str) -> Message | None:
    for item in w.legs:
        if item.role == role:
            return item
    return None


def _current_leg(w: Message, role: str) -> Message | None:
    cur = _find_leg(w, role)
    seen: set[str] = set()
    while cur is not None and cur.superseded_by:
        if cur.superseded_by in seen:
            break
        seen.add(cur.superseded_by)
        nxt = _find_leg(w, cur.superseded_by)
        if nxt is None:
            break
        cur = nxt
    return cur


def _add_leg(w: Message, role: str, player_id: int, stream: int, bundle: Message) -> Message:
    existing = _find_leg(w, role)
    if existing is not None:
        return existing
    leg = w.legs.add()
    leg.role = role
    leg.player_id = int(player_id)
    leg.stream = int(stream)
    leg.bundle.CopyFrom(bundle)
    return leg


def _add_escrow_leg(w: Message, role: str, to_player: int, owner_player: int, bundle: Message) -> Message:
    existing = _find_leg(w, role)
    if existing is not None:
        return existing
    leg = _add_leg(w, role, to_player, 4, bundle)
    leg.escrow_owner_id = int(owner_player)
    return leg


def _transfer_leg(protocol: Protocol, w: Message, role: str, to_player: int, from_leg: Message) -> None:
    snap = from_leg.snapshot if from_leg.HasField("snapshot") else None
    transferred = transfer_bundle(protocol, from_leg.bundle, snap)
    _add_escrow_leg(w, role, to_player, int(from_leg.player_id), transferred)


def _leg_done(leg: Message | None) -> bool:
    return leg is not None and int(leg.outcome) != 0


def _leg_applied(leg: Message | None) -> bool:
    return leg is not None and int(leg.outcome) == 2


def plan_rmb(protocol: Protocol, policy: TradePolicy, rec: Message, w: Message, now_ms: int) -> None:
    """人民币寄售非角色资产（物品/宠物/货币）状态机推演（由 CommerceEngine._plan 调用）。"""
    if not w.HasField("rmb"):
        raise ValueError("rmb contract missing terms")
    stage = str(w.stage)
    if stage == "rmb_escrow":
        _add_leg(
            w,
            "seller_debit",
            int(rec.seller_player_id),
            ASSET_STREAM_TRADE_DEBIT,
            build_asset_bundle(protocol, w.seller_assets),
        )
        debit = _find_leg(w, "seller_debit")
        if not _leg_done(debit):
            return
        if not _leg_applied(debit):
            w.stage = "rmb_returned"
            rec.status = COMMERCE_CANCELLED
            return
        snap = debit.snapshot if debit.HasField("snapshot") else None
        if w.cancel_requested or not policy.allow_snapshot(snap):
            w.stage = "rmb_return"
            rec.status = COMMERCE_CANCEL_PENDING
            _transfer_leg(protocol, w, "seller_return", int(rec.seller_player_id), debit)
            return
        w.stage = "rmb_ready"
        rec.status = COMMERCE_ESCROWING
        return

    if stage == "rmb_delivery":
        if int(w.rmb.character_player_id) != 0:
            return
        debit = _find_leg(w, "seller_debit")
        if not _leg_applied(debit):
            raise ValueError("rmb delivery missing applied seller_debit")
        _transfer_leg(protocol, w, "rmb_buyer_credit", int(rec.buyer_player_id), debit)
        credit = _find_leg(w, "rmb_buyer_credit")
        if _leg_done(credit):
            if _leg_applied(credit):
                w.stage = "rmb_delivered"
                rec.status = COMMERCE_SETTLED
            else:
                w.stage = "rmb_refund"
                rec.status = COMMERCE_REFUND_PENDING
        return

    if stage == "rmb_return":
        if int(w.rmb.character_player_id) != 0:
            return
        debit = _find_leg(w, "seller_debit")
        if _leg_applied(debit):
            _transfer_leg(protocol, w, "seller_return", int(rec.seller_player_id), debit)
        returned = _current_leg(w, "seller_return")
        if returned is None or _leg_done(returned):
            if returned is None or _leg_applied(returned):
                w.stage = "rmb_returned"
                rec.status = COMMERCE_CANCELLED
            else:
                if w.stage != "manual":
                    w.quarantine.stage = w.stage
                    w.quarantine.status = int(rec.status)
                    w.quarantine.reason = MANUAL_REASON_RMB_RETURN
                    w.quarantine.detail = truncate_utf8("rmb consignment return failed", QUARANTINE_DETAIL_MAX_BYTES)
                    w.quarantine.at_ms = int(now_ms)
                rec.status = COMMERCE_MANUAL
                w.stage = "manual"


def local_rmb_view(protocol: Protocol, rec: Message, w: Message) -> Message:
    """将尚未在商城登记或已回退的本地人民币合同投影为 RmbConsignmentView。"""
    status = int(rec.status)
    state = "ESCROWING"
    if status == COMMERCE_CANCEL_PENDING:
        state = "CANCEL_PENDING"
    elif status == COMMERCE_CANCELLED:
        state = "RETURNED"
    elif status == COMMERCE_SETTLED:
        state = "SOLD"
    elif status == COMMERCE_MANUAL:
        state = "MANUAL"
    return protocol.message(
        "trade.RmbConsignmentView",
        listing_id=int(rec.commerce_id),
        seller_player_id=int(rec.seller_player_id),
        market_zone=int(w.rmb.market_zone),
        asset_kind=str(w.rmb.asset_kind),
        title=str(w.rmb.title),
        price_fen=int(w.rmb.price_fen),
        state=state,
        phase=str(w.stage),
        designated_only=bool(str(w.rmb.designated_buyer_account)),
    )


def parse_return_cursor(raw: str) -> tuple[str, str]:
    """解析并校验人民币回退分页游标：r1:<listingId>:<upperListingId>。"""
    if not raw:
        return "", ""
    parts = raw.split(":")
    if len(raw) > 100 or len(parts) != 3 or parts[0] != "r1":
        raise ValueError(f"invalid rmb return cursor: {raw!r}")
    canonical_uint64(parts[1])
    canonical_uint64(parts[2])
    if parts[1] > parts[2]:
        raise ValueError(f"rmb return cursor exceeds upper bound: {raw!r}")
    return parts[1], parts[2]


def _role_directory_error(exc: Exception) -> Exception:
    if isinstance(exc, RoleDirectoryConflictError):
        return TradeRoleConflictError(str(exc))
    return exc


def _transfer_rejected(exc: Exception) -> bool:
    return isinstance(exc, (RoleDirectoryFullError, RoleDirectoryConflictError))


class RmbConsignments:
    """人民币寄售协调器（物品/宠物/货币 + 角色交易 + 商城交付/回退轮询）。"""

    def __init__(
        self,
        protocol: Protocol,
        engine,
        mall: MallAdapter,
        directory: RoleTradeAuthority,
        sealer: RoleSealer,
        *,
        credits=None,
        currency_types: tuple[int, ...] = (CURRENCY_GOLD,),
    ) -> None:
        self.protocol = protocol
        self.engine = engine
        self.mall = mall
        self.directory = directory
        self.sealer = sealer
        self.credits = credits
        self.currency_types = tuple(currency_types) if currency_types else (CURRENCY_GOLD,)
        self.delivery_cursor: str = ""
        self.return_cursor: str = ""
        self._seal_after: dict[int, int] = {}

    def _classify_selection(self, req: Message) -> str:
        sel = req.assets if req.HasField("assets") else None
        has_items = sel is not None and (len(sel.items) + len(sel.item_uuids) > 0)
        has_pet = sel is not None and int(sel.pet_id) != 0
        has_currency = sel is not None and int(sel.currency_amount) != 0
        count = sum(1 for present in (has_items, has_pet, has_currency) if present)
        char_id = int(req.character_player_id)
        if char_id != 0:
            if count != 0 or (sel is not None and int(sel.currency_type) != 0):
                raise TradeRejection(ERR_INVALID_PARAMETER, "character consignment cannot carry asset selection")
            return "CHARACTER"
        if count != 1 or sel is None:
            raise TradeRejection(ERR_INVALID_PARAMETER, "rmb consignment requires exactly one asset category")
        if int(sel.currency_type) == CURRENCY_BIND_DIAMOND:
            raise TradeRejection(ERR_TRADE_ASSET_NOT_TRADABLE, "bind diamond is not tradable")
        rmb_policy = TradePolicy(
            items=self.engine.policy.items,
            item_titles=self.engine.policy.item_titles,
            pets=self.engine.policy.pets,
            currencies=self.currency_types,
            allow_instances=self.engine.policy.allow_instances,
        )
        rmb_policy.validate_selection(sel)
        if has_items:
            return "ITEMS"
        if has_pet:
            return "PET"
        return "CURRENCY"

    async def create(
        self,
        original: RoleOriginal,
        account: str,
        player_id: int,
        owner_revision: int,
        zone: int,
        req: Message,
    ) -> Message:
        title = str(req.title)
        price_fen = int(req.price_fen)
        if (
            not valid_account(account)
            or player_id <= 0
            or owner_revision <= 0
            or zone <= 0
            or price_fen <= 0
            or price_fen > MAX_RMB_PRICE_FEN
            or not title.strip()
            or _utf16_len(title) > MAX_RMB_TITLE_UTF16
        ):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid rmb consignment parameters")
        asset_kind = self._classify_selection(req)
        designated_buyer_account = ""
        target_id = int(req.designated_buyer_player_id)
        if target_id != 0:
            if target_id == player_id:
                raise TradeRejection(ERR_TRADE_CANNOT_TRADE_SELF, "cannot designate self as rmb buyer")
            target_owned = await self.directory.lookup(target_id)
            if target_owned is None or not valid_account(str(target_owned.account)):
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid designated rmb buyer account")
            if str(target_owned.account) == account:
                raise TradeRejection(ERR_TRADE_CANNOT_TRADE_SELF, "cannot designate own account as rmb buyer")
            designated_buyer_account = str(target_owned.account)
        stage = "rmb_escrow"
        char_id = int(req.character_player_id)
        if asset_kind == "CHARACTER":
            stage = "rmb_freezing"
            if player_id != char_id:
                raise TradeRejection(ERR_INVALID_PARAMETER, "character consignment must match target character")

        w = self.protocol.message("trade.TradeCommerceWorkflow", stage=stage)
        if req.HasField("assets"):
            w.seller_assets.CopyFrom(req.assets)
        w.rmb.seller_account = account
        w.rmb.seller_owner_revision = owner_revision
        w.rmb.character_player_id = char_id
        w.rmb.title = title
        w.rmb.asset_kind = asset_kind
        w.rmb.market_zone = zone
        w.rmb.price_fen = price_fen
        w.rmb.client_request_id = str(req.client_request_id)
        w.rmb.designated_buyer_account = designated_buyer_account

        input_clone = self.protocol.message("trade.TradeCommerceWorkflow")
        input_clone.CopyFrom(w)
        rec = self.protocol.message(
            "trade.TradeCommerceRecord",
            kind=COMMERCE_KIND_RMB_CONSIGNMENT,
            status=COMMERCE_ESCROWING,
            seller_player_id=player_id,
            unit_price=price_fen,
            expires_ms=self.engine.now_ms() + 15 * 60 * 1000,
        )
        return await self.engine.create_contract(original, player_id, str(req.client_request_id), input_clone, rec, w)

    async def _save(self, old: Message, body, *, original: RoleOriginal | None = None) -> None:
        async def _tx(tx) -> None:
            await self.engine.lock_roles(
                tx, original, int(old.commerce_id), int(old.seller_player_id), int(old.buyer_player_id)
            )
            rec = await tx.lock_commerce(int(old.commerce_id))
            if rec is None:
                raise TradeCorruptContractError(f"rmb commerce {old.commerce_id} vanished")
            if int(rec.version) != int(old.version):
                raise TradeStaleObservationError("rmb commerce version changed before lock")
            w = self.protocol.message("trade.TradeCommerceWorkflow")
            w.ParseFromString(bytes(rec.workflow_proto))
            if not w.HasField("rmb"):
                raise TradeCorruptContractError("rmb commerce missing terms")
            res = body(tx, rec, w)
            if asyncio.iscoroutine(res):
                await res
            await tx.save_commerce(rec, w, self.engine.now_ms())

        await self.engine.store.run_role_tx(_tx)

    async def cancel(self, original: RoleOriginal, account: str, listing_id: int) -> None:
        rec = await self.engine.store.get_commerce(listing_id)
        if rec is None:
            raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "rmb consignment not found")
        w = self.protocol.message("trade.TradeCommerceWorkflow")
        w.ParseFromString(bytes(rec.workflow_proto))
        if int(rec.kind) != COMMERCE_KIND_RMB_CONSIGNMENT or not w.HasField("rmb") or str(w.rmb.seller_account) != account:
            raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "rmb consignment not owned by seller")
        if int(rec.status) == COMMERCE_CANCELLED:
            return
        if not rmb_cancellable(str(w.stage)):
            raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "rmb consignment cannot be cancelled in current stage")

        async def _mutate(tx, cur: Message, cw: Message) -> None:
            if not rmb_cancellable(str(cw.stage)):
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "rmb consignment cannot be cancelled")
            cw.cancel_requested = True
            cur.status = COMMERCE_CANCEL_PENDING
            await tx.expire_commerce_escrow_debits(int(cur.commerce_id), self.engine.now_ms())

        await self._save(rec, _mutate, original=original)

    async def _buyer_account_full(self, terms: Message, account: str) -> bool:
        if int(terms.character_player_id) == 0:
            return False
        return await self.directory.role_slot_full(account)

    async def check_buyer_slot(self, account: str, listing_id: int) -> None:
        rec = await self.engine.store.get_commerce(listing_id)
        if rec is None or int(rec.kind) != COMMERCE_KIND_RMB_CONSIGNMENT:
            return
        w = self.protocol.message("trade.TradeCommerceWorkflow")
        try:
            w.ParseFromString(bytes(rec.workflow_proto))
        except Exception:
            return
        if not w.HasField("rmb"):
            return
        try:
            full = await self._buyer_account_full(w.rmb, account)
        except Exception as exc:
            logger.info("trade buyer role slot precheck skipped listing=%d err=%s", listing_id, exc)
            return
        if full:
            raise TradeRejection(ERR_LOGIN_ACCOUNT_PLAYER_FULL, "buyer account role slots full")

    # -- 合同后台推进（由 CommerceEngine._step 分派 kind=6） ---------------------------------------------------

    async def _check_asset_identity(self, rec: Message, w: Message) -> None:
        t = w.rmb
        owned = await self.directory.lookup(int(rec.seller_player_id))
        if (
            str(owned.account) != str(t.seller_account)
            or int(owned.owner_revision) != int(t.seller_owner_revision)
            or int(owned.trade_listing_id) != 0
        ):
            raise TradeRoleConflictError("rmb seller role identity changed or frozen")
        if str(w.stage) == "rmb_delivery":
            buyer = await self.directory.lookup(int(rec.buyer_player_id))
            if (
                str(buyer.account) != str(t.buyer_account)
                or int(buyer.owner_revision) != int(t.buyer_owner_revision)
                or int(buyer.trade_listing_id) != 0
            ):
                raise TradeRoleConflictError("rmb buyer role identity changed or frozen")

    async def _seal_holders(self, receipt: Message) -> None:
        holders = list(receipt.holders)
        if not holders:
            return
        lid = int(receipt.listing_id)
        start = self._seal_after.get(lid, 0) % len(holders)
        first_err: Exception | None = None
        for step_idx in range(min(len(holders), _SEAL_BATCH_LIMIT)):
            idx = (start + step_idx) % len(holders)
            holder = holders[idx]
            if not holder.drained:
                try:
                    async with asyncio.timeout(_SEAL_CALL_TIMEOUT_SEC):
                        await self.sealer.seal(holder.seal)
                except Exception as exc:
                    if first_err is None:
                        first_err = exc
            self._seal_after[lid] = (idx + 1) % len(holders)
        if first_err is not None:
            raise first_err

    async def _cancel_role(self, rec: Message, w: Message) -> None:
        t = w.rmb
        req = self.protocol.message(
            "data_service.CancelAccountRoleTradeRequest",
            listing_id=int(rec.commerce_id),
            player_id=int(t.character_player_id),
            seller_account=str(t.seller_account),
            expected_owner_revision=int(t.seller_owner_revision),
        )
        try:
            receipt = await self.directory.cancel(req)
        except Exception as exc:
            raise _role_directory_error(exc) from exc
        if int(receipt.state) != ROLE_TRADE_STATE_CANCELLED:
            await self._seal_holders(receipt)
            raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "role trade cancel still draining holders")
        self._seal_after.pop(int(rec.commerce_id), None)

        def _finish(_tx, cur: Message, cw: Message) -> None:
            if str(cw.stage) in ("rmb_delivery", "rmb_delivered"):
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "cannot cancel role already in delivery")
            cw.stage = "rmb_returned"
            cur.status = COMMERCE_CANCELLED

        await self._save(rec, _finish)

    async def _settle_role_delivery(self, rec: Message) -> None:
        self._seal_after.pop(int(rec.commerce_id), None)
        transferred_player = 0
        new_buyer_account = ""
        next_revision = 0

        def _finish(_tx, cur: Message, cw: Message) -> None:
            nonlocal transferred_player, new_buyer_account, next_revision
            if str(cw.stage) != "rmb_delivery":
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "rmb role delivery stage mismatch")
            transferred_player = int(cw.rmb.character_player_id)
            new_buyer_account = str(cw.rmb.buyer_account)
            next_revision = int(cw.rmb.seller_owner_revision) + 1
            cw.stage = "rmb_delivered"
            cur.status = COMMERCE_SETTLED

        await self._save(rec, _finish)
        if self.credits is not None and transferred_player and new_buyer_account and next_revision:
            try:
                await self.credits.on_character_transferred(
                    transferred_player, new_buyer_account, next_revision, self.engine.now_ms()
                )
            except Exception as reset_exc:
                logger.error(
                    "trade seller credit reset after character transfer failed player_id=%d next_rev=%d err=%s",
                    transferred_player,
                    next_revision,
                    reset_exc,
                )

    async def _refund_role(self, rec: Message, w: Message, transfer_req: Message, cause: Exception) -> None:
        t = w.rmb
        cancel_req = self.protocol.message(
            "data_service.CancelAccountRoleTradeRequest",
            listing_id=int(rec.commerce_id),
            player_id=int(t.character_player_id),
            seller_account=str(t.seller_account),
            expected_owner_revision=int(t.seller_owner_revision),
        )
        try:
            receipt = await self.directory.cancel(cancel_req)
        except RoleDirectoryConflictError as cancel_exc:
            # Cancel 被拒可能意味着过户已提交：用原 tx_id 重放 Transfer 消歧
            try:
                await self.directory.transfer(transfer_req)
                await self._settle_role_delivery(rec)
                return
            except Exception as replay_exc:
                if not _transfer_rejected(replay_exc):
                    raise replay_exc
                raise TradeRoleConflictError(
                    f"role transfer rejected ({cause}) and cancel rejected ({cancel_exc})"
                ) from cancel_exc

        if int(receipt.state) != ROLE_TRADE_STATE_CANCELLED:
            await self._seal_holders(receipt)
            raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "role refund cancel awaiting holder drain")
        self._seal_after.pop(int(rec.commerce_id), None)
        logger.info("trade role transfer cannot finish, refunding buyer commerce_id=%d cause=%s", rec.commerce_id, cause)

        def _mark_refund(_tx, cur: Message, cw: Message) -> None:
            if str(cw.stage) != "rmb_delivery":
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "rmb role refund stage mismatch")
            cw.stage = "rmb_refund"
            cur.status = COMMERCE_REFUND_PENDING

        await self._save(rec, _mark_refund)

    async def _transfer_role(self, rec: Message, w: Message) -> None:
        t = w.rmb
        if int(t.transfer_tx_id) == 0 or not valid_account(str(t.buyer_account)):
            raise TradeCorruptContractError("role transfer missing transfer_tx_id or buyer_account")
        req = self.protocol.message(
            "data_service.TransferAccountRoleRequest",
            tx_id=int(t.transfer_tx_id),
            listing_id=int(rec.commerce_id),
            player_id=int(t.character_player_id),
            seller_account=str(t.seller_account),
            buyer_account=str(t.buyer_account),
            expected_owner_revision=int(t.seller_owner_revision),
        )
        try:
            await self.directory.transfer(req)
        except Exception as exc:
            if _transfer_rejected(exc):
                await self._refund_role(rec, w, req, exc)
                return
            raise
        await self._settle_role_delivery(rec)

    async def step(self, rec: Message) -> None:
        w = self.protocol.message("trade.TradeCommerceWorkflow")
        try:
            w.ParseFromString(bytes(rec.workflow_proto))
        except Exception as exc:
            raise TradeCorruptContractError(f"rmb workflow decode failed: {exc}") from exc
        if not w.HasField("rmb"):
            raise TradeCorruptContractError("rmb contract missing terms")
        t = w.rmb
        stage = str(w.stage)

        if stage in ("rmb_escrow", "rmb_delivery", "rmb_return"):
            if int(t.character_player_id) == 0:
                await self._check_asset_identity(rec, w)
                await self.engine.advance_contract(rec)
                return
            if stage == "rmb_return":
                await self._cancel_role(rec, w)
                return
            if stage == "rmb_delivery":
                await self._transfer_role(rec, w)
                return
            raise TradeCorruptContractError("rmb character contract cannot be in rmb_escrow")

        if stage == "rmb_freezing":
            if w.cancel_requested or (int(rec.expires_ms) != 0 and int(rec.expires_ms) <= self.engine.now_ms()):
                await self._cancel_role(rec, w)
                return
            begin_req = self.protocol.message(
                "data_service.BeginAccountRoleTradeRequest",
                listing_id=int(rec.commerce_id),
                player_id=int(t.character_player_id),
                seller_account=str(t.seller_account),
                expected_owner_revision=int(t.seller_owner_revision),
            )
            try:
                first = await self.directory.begin(begin_req)
            except Exception as exc:
                raise _role_directory_error(exc) from exc
            if w.cancel_requested or int(first.state) in (ROLE_TRADE_STATE_CANCEL_PENDING, ROLE_TRADE_STATE_CANCELLED):
                await self._cancel_role(rec, w)
                return
            if int(first.state) == ROLE_TRADE_STATE_TRANSFERRED:
                raise TradeCorruptContractError("listing contract already has transferred role receipt")
            await self._seal_holders(first)
            confirm_req = self.protocol.message(
                "data_service.ConfirmAccountRoleTradeFreezeRequest",
                listing_id=int(rec.commerce_id),
                player_id=int(t.character_player_id),
                seller_account=str(t.seller_account),
                expected_owner_revision=int(t.seller_owner_revision),
            )
            confirmed = await self.directory.confirm(confirm_req)
            if int(confirmed.state) != ROLE_TRADE_STATE_FROZEN or not confirmed.HasField("freeze_evidence"):
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "role freeze not yet confirmed")
            proof_bytes = confirmed.SerializeToString(deterministic=True)

            def _mark_ready(_tx, _cur: Message, cw: Message) -> None:
                if str(cw.stage) == "rmb_freezing":
                    cw.rmb.freeze_receipt = proof_bytes
                    cw.stage = "rmb_ready"

            await self._save(rec, _mark_ready)
            return

        if stage == "rmb_ready":
            def _prepare_register(_tx, cur: Message, cw: Message) -> None:
                if str(cw.stage) != "rmb_ready":
                    return
                if cw.cancel_requested:
                    cw.stage = "rmb_return"
                    cur.status = COMMERCE_CANCEL_PENDING
                    return
                raw = bytes(cw.rmb.freeze_receipt)
                if int(cw.rmb.character_player_id) == 0:
                    debit = _find_leg(cw, "seller_debit")
                    if not _leg_applied(debit):
                        raise TradeCorruptContractError("rmb ready missing applied seller_debit")
                    raw = debit.SerializeToString(deterministic=True)
                digest = hashlib.sha256(bytes(cur.request_digest) + raw).hexdigest()
                cw.rmb.escrow_proof = digest
                cw.rmb.escrow_reference = f"trade:{int(cur.commerce_id)}"
                cw.stage = "rmb_registering"

            await self._save(rec, _prepare_register)
            return

        if stage == "rmb_registering":
            listing_view = await self.mall.rmb_register(rec, w)

            def _mark_listed(_tx, cur: Message, cw: Message) -> None:
                if str(cw.stage) == "rmb_registering":
                    cw.rmb.registered_listing.CopyFrom(listing_view)
                    cw.stage = "rmb_listed"
                    cur.status = COMMERCE_WAITING

            await self._save(rec, _mark_listed)
            return

        if stage == "rmb_listed":
            if not w.cancel_requested:
                await self.engine.store.defer_commerce(
                    int(rec.commerce_id), int(rec.version), self.engine.now_ms() + _RMB_DEFER_MS
                )
                return
            cancelled_view = await self.mall.rmb_cancel(str(t.seller_account), int(rec.commerce_id))
            if str(cancelled_view.state) not in ("RETURNING", "RETURNED"):
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "mall cancel did not enter returning state")

            def _mark_return(_tx, cur: Message, cw: Message) -> None:
                if str(cw.stage) == "rmb_listed":
                    cw.stage = "rmb_return"
                    cur.status = COMMERCE_CANCEL_PENDING

            await self._save(rec, _mark_return)
            return

        if stage == "rmb_refund":
            await self.engine.store.defer_commerce(
                int(rec.commerce_id), int(rec.version), self.engine.now_ms() + _RMB_DEFER_MS
            )
            return

        if stage in ("rmb_delivered", "rmb_returned", "manual"):
            return

        raise TradeCorruptContractError(f"unknown rmb commerce stage: {stage!r}")

    # -- 商城交付意图与回退意图拉取/回报（与 Go rmb.go deliver/returned/Tick 对齐） -----------------------------

    async def _send_delivery_receipt(self, rec: Message, order_id: str, state: str) -> None:
        proof = hashlib.sha256(bytes(rec.workflow_proto)).hexdigest()
        await self.mall.call(
            "delivery",
            "trade",
            "POST",
            "/internal/deliveries/receipt",
            {"orderId": order_id, "tradeOrderId": str(int(rec.commerce_id)), "state": state, "proof": proof},
            expect_json=False,
        )

    async def deliver(self, intent: dict[str, Any]) -> None:
        try:
            listing_id = canonical_uint64(intent.get("listingId"))
            buyer_id = canonical_uint64(intent.get("buyerPlayerId"))
            owner_rev = canonical_uint64(intent.get("buyerOwnerRevision"))
        except ValueError as exc:
            raise TradeRejection(ERR_INVALID_PARAMETER, str(exc)) from exc
        order_id = intent.get("orderId")
        buyer_account = str(intent.get("buyerAccount", ""))
        reference = str(intent.get("escrowReference", ""))
        proof = str(intent.get("escrowProof", ""))
        amount_fen = int(intent.get("amountFen", 0) or 0)
        if not isinstance(order_id, str) or not MALL_ORDER_ID_RE.match(order_id) or not valid_account(buyer_account):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid rmb delivery intent")

        rec = await self.engine.store.get_commerce(listing_id)
        if rec is None:
            raise StoreUnavailableError(f"rmb listing {listing_id} not found")
        w = self.protocol.message("trade.TradeCommerceWorkflow")
        w.ParseFromString(bytes(rec.workflow_proto))
        t = w.rmb if w.HasField("rmb") else None
        if (
            int(rec.kind) != COMMERCE_KIND_RMB_CONSIGNMENT
            or t is None
            or reference != str(t.escrow_reference)
            or proof != str(t.escrow_proof)
            or amount_fen != int(t.price_fen)
        ):
            raise TradeRejection(ERR_TRADE_REQUEST_CONFLICT, "rmb delivery intent terms mismatch")

        digest = hashlib.sha256(
            json.dumps(intent, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ).digest()
        if bytes(t.delivery_digest):
            if bytes(t.delivery_digest) != digest:
                raise TradeRejection(ERR_TRADE_REQUEST_CONFLICT, "rmb delivery digest conflict")
        else:
            owned = await self.directory.lookup(buyer_id)
            full = await self._buyer_account_full(t, buyer_account)
            self_purchase = buyer_account == str(t.seller_account)
            if (
                self_purchase
                or full
                or str(owned.account) != buyer_account
                or int(owned.owner_revision) != owner_rev
                or int(owned.trade_listing_id) != 0
            ):
                def _persist_refund(_tx, cur: Message, cw: Message) -> None:
                    if bytes(cw.rmb.delivery_digest):
                        if bytes(cw.rmb.delivery_digest) != digest:
                            raise TradeRejection(ERR_TRADE_REQUEST_CONFLICT, "rmb delivery digest conflict")
                        return
                    if str(cw.stage) != "rmb_listed":
                        raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "rmb listing not listed for refund")
                    cw.rmb.delivery_digest = digest
                    cw.rmb.buyer_account = buyer_account
                    cw.rmb.mall_order_id = order_id
                    cw.stage = "rmb_refund"
                    cur.status = COMMERCE_REFUND_PENDING

                await self._save(rec, _persist_refund)
                committed = await self.engine.store.get_commerce(listing_id)
                if committed is None:
                    raise StoreUnavailableError(f"rmb listing {listing_id} vanished after refund commit")
                await self._send_delivery_receipt(committed, order_id, "REFUND_PENDING")
                return

            tx_id = await self.engine.next_commerce_id() if int(t.character_player_id) != 0 else 0

            async def _tx(tx) -> None:
                await self.engine.lock_roles(tx, None, int(rec.commerce_id), int(rec.seller_player_id), buyer_id)
                cur = await tx.lock_commerce(listing_id)
                if cur is None:
                    raise StoreUnavailableError(f"rmb listing {listing_id} vanished")
                cw = self.protocol.message("trade.TradeCommerceWorkflow")
                cw.ParseFromString(bytes(cur.workflow_proto))
                if not cw.HasField("rmb"):
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "rmb terms missing")
                ct = cw.rmb
                if bytes(ct.delivery_digest):
                    if bytes(ct.delivery_digest) != digest:
                        raise TradeRejection(ERR_TRADE_REQUEST_CONFLICT, "rmb delivery digest conflict")
                    return
                if str(cw.stage) != "rmb_listed" or buyer_account == str(ct.seller_account):
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "rmb listing not deliverable")
                locked_buyer = await self.engine.store.read_ownership(tx, buyer_id)
                if locked_buyer.owner_revision < owner_rev:
                    raise RoleFenceStaleError(buyer_id, reject=TradeRoleConflictError())
                trade_listing_id = locked_buyer.freeze_token if locked_buyer.freeze_final else 0
                if (
                    locked_buyer.account != buyer_account
                    or locked_buyer.owner_revision != owner_rev
                    or trade_listing_id != 0
                ):
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "buyer role ownership conflict")
                ct.buyer_account = buyer_account
                ct.buyer_owner_revision = locked_buyer.owner_revision
                cw.buyer_identity.account = buyer_account
                cw.buyer_identity.owner_revision = owner_rev
                ct.mall_order_id = order_id
                ct.delivery_digest = digest
                ct.transfer_tx_id = tx_id
                cw.cancel_requested = False
                cur.buyer_player_id = buyer_id
                cw.stage = "rmb_delivery"
                cur.status = COMMERCE_DELIVERING
                await tx.save_commerce(cur, cw, self.engine.now_ms())

            await self.engine.store.run_role_tx(_tx)
            rec = await self.engine.store.get_commerce(listing_id)
            if rec is None:
                raise StoreUnavailableError(f"rmb listing {listing_id} vanished after delivery accept")

        status = int(rec.status)
        state = ""
        if status == COMMERCE_SETTLED:
            state = "DELIVERED"
        elif status == COMMERCE_REFUND_PENDING:
            state = "REFUND_PENDING"
        elif status == COMMERCE_MANUAL:
            state = "MANUAL"
        if not state:
            return
        await self._send_delivery_receipt(rec, order_id, state)

    async def returned(self, intent: dict[str, Any]) -> None:
        try:
            listing_id = canonical_uint64(intent.get("listingId"))
        except ValueError as exc:
            raise TradeRejection(ERR_INVALID_PARAMETER, str(exc)) from exc
        rec = await self.engine.store.get_commerce(listing_id)
        if rec is None:
            raise StoreUnavailableError(f"rmb listing {listing_id} not found for return")
        w = self.protocol.message("trade.TradeCommerceWorkflow")
        w.ParseFromString(bytes(rec.workflow_proto))
        t = w.rmb if w.HasField("rmb") else None
        if (
            int(rec.kind) != COMMERCE_KIND_RMB_CONSIGNMENT
            or t is None
            or str(intent.get("sellerPlayerId", "")) != str(int(rec.seller_player_id))
            or str(intent.get("sellerAccount", "")) != str(t.seller_account)
            or str(intent.get("escrowReference", "")) != str(t.escrow_reference)
            or str(intent.get("escrowProof", "")) != str(t.escrow_proof)
        ):
            raise TradeRejection(ERR_TRADE_REQUEST_CONFLICT, "rmb return intent terms mismatch")

        if int(rec.status) != COMMERCE_CANCELLED:
            if str(w.stage) not in ("rmb_listed", "rmb_return", "rmb_refund"):
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "rmb listing cannot return in current stage")

            def _mark_return(_tx, cur: Message, cw: Message) -> None:
                if str(cw.stage) in ("rmb_listed", "rmb_refund"):
                    cw.stage = "rmb_return"
                    cw.cancel_requested = True
                    cur.status = COMMERCE_CANCEL_PENDING

            await self._save(rec, _mark_return)
            return

        return_proof = hashlib.sha256(bytes(rec.workflow_proto)).hexdigest()
        await self.mall.consignment_call(
            "returns/receipt",
            {
                "listingId": str(intent["listingId"]),
                "escrowReference": str(intent["escrowReference"]),
                "escrowProof": str(intent["escrowProof"]),
                "returnProof": return_proof,
            },
            expect_json=False,
        )

    async def poll_deliveries(self) -> list[Exception]:
        data = await self.mall.consignment_call(
            "deliveries/poll", {"cursor": self.delivery_cursor, "limit": MALL_POLL_LIMIT}
        )
        entries = data.get("entries") if isinstance(data, dict) else None
        validate_mall_page(entries, self.delivery_cursor, order_id_getter=lambda item: item.get("orderId"))
        if not entries:
            self.delivery_cursor = ""
            return []
        errors: list[Exception] = []
        for entry in entries:
            intent = entry["intent"]
            try:
                await self.deliver(intent)
            except Exception as exc:
                logger.warning("trade rmb deliver intent failed order_id=%s err=%s", intent.get("orderId"), exc)
                errors.append(exc)
            self.delivery_cursor = entry["cursor"]
        return errors

    async def poll_returns(self) -> list[Exception]:
        data = await self.mall.consignment_call(
            "returns/poll", {"cursor": self.return_cursor, "limit": MALL_POLL_LIMIT}
        )
        entries = data.get("entries") if isinstance(data, dict) else None
        if not isinstance(entries, list) or len(entries) > MALL_POLL_LIMIT:
            raise ValueError("invalid rmb return poll page")
        previous, upper = parse_return_cursor(self.return_cursor)
        for idx, entry in enumerate(entries):
            if not isinstance(entry, dict):
                raise ValueError("rmb return entry is not an object")
            intent = entry.get("intent")
            cursor_raw = entry.get("cursor")
            if not isinstance(intent, dict) or not isinstance(cursor_raw, str):
                raise ValueError("rmb return entry missing intent or cursor")
            canonical_uint64(intent.get("listingId"))
            if cursor_raw == "":
                if idx != len(entries) - 1:
                    raise ValueError("rmb return cursor ended early")
                continue
            nxt, bound = parse_return_cursor(cursor_raw)
            if nxt != intent["listingId"] or nxt <= previous or (upper and upper != bound):
                raise ValueError("rmb return cursor watermark invalid")
            previous, upper = nxt, bound
        if not entries:
            self.return_cursor = ""
            return []
        errors: list[Exception] = []
        for entry in entries:
            intent = entry["intent"]
            try:
                await self.returned(intent)
            except Exception as exc:
                logger.warning("trade rmb return intent failed listing=%s err=%s", intent.get("listingId"), exc)
                errors.append(exc)
            self.return_cursor = entry["cursor"]
        return errors

    async def tick(self) -> list[Exception]:
        """推进一轮人民币寄售商城交付队列与回退队列（各具独立子预算）。"""
        errors: list[Exception] = []
        for poll_fn in (self.poll_deliveries, self.poll_returns):
            try:
                async with asyncio.timeout(1.2):
                    errors.extend(await poll_fn())
            except Exception as exc:
                logger.warning("trade rmb poll channel failed fn=%s err=%s", poll_fn.__name__, exc)
                errors.append(exc)
        return errors
