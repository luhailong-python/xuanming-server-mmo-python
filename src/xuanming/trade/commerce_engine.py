"""trade 核心交易状态机（CommerceEngine）：P2P、寄售、材料限价盘口、拍卖、收据 ACK 与人工处置。"""

from __future__ import annotations

import hashlib
import logging
import random
import struct
import time
import unicodedata
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from google.protobuf.message import DecodeError, Message

from ..protocol import Protocol
from .asset_loop import AssetOpCaller, SqlCommittedLedgerReader, acknowledge_receipt
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_TRADE_AUCTION_ENDED,
    ERR_TRADE_BID_TOO_LOW,
    ERR_TRADE_CANNOT_TRADE_SELF,
    ERR_TRADE_ORDER_NOT_FOUND,
    ERR_TRADE_ORDER_STATE_CONFLICT,
    ERR_TRADE_REQUEST_CONFLICT,
    ManualNotAllowedError,
    ManualNotFoundError,
    ManualVersionChangedError,
    RoleFenceStaleError,
    RoleNotFoundError,
    TradeCorruptContractError,
    TradeRejection,
    TradeRoleConflictError,
    TradeStaleObservationError,
    TradeTooManyPendingError,
)
from .model import (
    ASSET_STREAM_TRADE_CREDIT,
    ASSET_STREAM_TRADE_DEBIT,
    COMMERCE_CANCELLED,
    COMMERCE_CANCEL_PENDING,
    COMMERCE_DELIVERING,
    COMMERCE_ESCROWING,
    COMMERCE_KIND_AUCTION_LOT,
    COMMERCE_KIND_BID,
    COMMERCE_KIND_CONSIGNMENT,
    COMMERCE_KIND_MARKET_MATCH,
    COMMERCE_KIND_OFFICIAL,
    COMMERCE_KIND_PLAYER_TRADE,
    COMMERCE_KIND_RMB_CONSIGNMENT,
    COMMERCE_MANUAL,
    COMMERCE_SETTLED,
    COMMERCE_WAITING,
    INT64_MAX,
    MANUAL_OPERATOR_MAX_BYTES,
    MANUAL_REASON_MAX_BYTES,
    MAX_NEW_LEGS_PER_STEP,
    OP_KIND_DELIVER_CREDIT,
    OP_KIND_ESCROW_DEBIT,
    OP_REF_KIND_ORDER,
    OP_STATUS_APPLIED,
    OP_STATUS_APPLIED_PARTIAL,
    OP_STATUS_PENDING,
    QUARANTINE_DETAIL_MAX_BYTES,
    STAGE_OFFICIAL_DELIVER,
    LegOutcome,
    RoleFenceExpect,
    RoleFenceRow,
    RoleOriginal,
    TradePolicy,
    build_asset_bundle,
    empty_bundle,
    is_official_reverse_leg,
    leg_tx_type,
    market_remainder,
    request_digest,
    request_key,
    transfer_bundle,
    truncate_utf8,
    valid_account,
)
from .mall_adapter import MANUAL_REASON_PAYMENT_REVERSAL, plan_official
from .rmb import plan_rmb
from .store import TradeStoreBase, TradeTx, _DuplicateKeyError

logger = logging.getLogger(__name__)

# 转人工原因类别（与 Go commerce/manual.go 对齐）
MANUAL_REASON_CORRUPT = "corrupt"
MANUAL_REASON_BAD_COUNTERPARTY = "bad_counterparty"
MANUAL_REASON_APPLIED_PARTIAL = "applied_partial"
MANUAL_REASON_FINAL_CREDIT = "final_credit_failed"
MANUAL_REASON_RMB_RETURN = "rmb_return_failed"
MANUAL_REASON_OWNER_CHANGED = "owner_changed"

MANUAL_ACTIONS = ("resume", "reissue", "refund", "partial", "redirect", "void-auction")
_UNWOUND_BY = "unwound"

# 调度间隔常量（毫秒，与 Go commerce/schedule.go & tick_isolation.go 对齐）
_TICK_DUE_LIMIT = 100
_TICK_ACK_LIMIT = 100
_IDLE_RECHECK_MS = 3_600_000
_IN_FLIGHT_RECHECK_MS = 30_000
_BID_HELD_RECHECK_MS = 30_000
_BID_HELD_MAX_WAIT_MS = 3_600_000
_TICK_RETRY_BACKOFF_MS = 10_000
_TICK_PENDING_BACKOFF_MS = 60_000
_TICK_ROLE_CONFLICT_BACKOFF_MS = 300_000


# -- 合同与腿纯辅助函数 ---------------------------------------------------------------------------------

def decode_workflow(protocol: Protocol, rec: Message) -> Message:
    w = protocol.message("trade.TradeCommerceWorkflow")
    try:
        w.ParseFromString(bytes(rec.workflow_proto))
    except DecodeError as exc:
        raise TradeCorruptContractError(f"commerce {rec.commerce_id} workflow decode failed: {exc}") from exc
    return w


def find_leg(w: Message, role: str) -> Message | None:
    for item in w.legs:
        if item.role == role:
            return item
    return None


def add_leg(w: Message, role: str, player_id: int, stream: int, bundle: Message) -> Message:
    existing = find_leg(w, role)
    if existing is not None:
        return existing
    leg = w.legs.add()
    leg.role = role
    leg.player_id = int(player_id)
    leg.stream = int(stream)
    leg.bundle.CopyFrom(bundle)
    return leg


def add_escrow_leg(w: Message, role: str, to_player: int, owner_player: int, bundle: Message) -> Message:
    existing = find_leg(w, role)
    if existing is not None:
        return existing
    leg = add_leg(w, role, to_player, ASSET_STREAM_TRADE_CREDIT, bundle)
    leg.escrow_owner_id = int(owner_player)
    return leg


def transfer_leg(protocol: Protocol, w: Message, role: str, to_player: int, from_leg: Message) -> None:
    snap = from_leg.snapshot if from_leg.HasField("snapshot") else None
    transferred = transfer_bundle(protocol, from_leg.bundle, snap)
    add_escrow_leg(w, role, to_player, int(from_leg.player_id), transferred)


def leg_done(leg: Message | None) -> bool:
    return leg is not None and int(leg.outcome) != 0


def leg_applied(leg: Message | None) -> bool:
    return leg is not None and int(leg.outcome) == OP_STATUS_APPLIED


def current_leg(w: Message, role: str) -> Message | None:
    cur = find_leg(w, role)
    seen: set[str] = set()
    while cur is not None and cur.superseded_by:
        if cur.superseded_by in seen:
            break
        seen.add(cur.superseded_by)
        nxt = find_leg(w, cur.superseded_by)
        if nxt is None:
            break
        cur = nxt
    return cur


def is_seller_proceeds(leg: Message) -> bool:
    return str(leg.role).split("#", 1)[0] == "seller_receive"


def held_for_buyer_first(w: Message, leg: Message) -> bool:
    """先买家、后卖家：卖家所得腿必须等买家收货腿到账后才发出。"""
    if not is_seller_proceeds(leg):
        return False
    buyer = current_leg(w, "buyer_receive")
    return buyer is not None and not leg_applied(buyer)


def leg_ready_to_send(w: Message, leg: Message) -> bool:
    return int(leg.op_id) == 0 and not leg.superseded_by and not held_for_buyer_first(w, leg)


def final_credits(w: Message) -> tuple[bool, bool]:
    for leg in w.legs:
        if int(leg.stream) == ASSET_STREAM_TRADE_CREDIT and not leg.superseded_by:
            if not leg_done(leg):
                return False, False
            if not leg_applied(leg):
                return True, False
    return True, True


def mark_manual(rec: Message, w: Message, reason: str, detail: str, now_ms: int) -> None:
    if w.stage != "manual":
        w.quarantine.stage = w.stage
        w.quarantine.status = int(rec.status)
        w.quarantine.reason = reason
        w.quarantine.detail = truncate_utf8(detail, QUARANTINE_DETAIL_MAX_BYTES)
        w.quarantine.at_ms = int(now_ms)
    rec.status = COMMERCE_MANUAL
    w.stage = "manual"


def unwindable(w: Message) -> bool:
    buyer = find_leg(w, "buyer_receive")
    seller = find_leg(w, "seller_receive")
    return (
        buyer is not None
        and seller is not None
        and not buyer.superseded_by
        and not seller.superseded_by
        and leg_done(buyer)
        and not leg_applied(buyer)
        and int(buyer.outcome) != OP_STATUS_APPLIED_PARTIAL
        and int(seller.op_id) == 0
    )


def settle_buyer_first_delivery(protocol: Protocol, rec: Message, w: Message, now_ms: int) -> None:
    buyer = current_leg(w, "buyer_receive")
    if buyer is None or not leg_done(buyer) or leg_applied(buyer):
        return
    seller = current_leg(w, "seller_receive")
    if seller is None or int(seller.op_id) != 0:
        return
    if not unwindable(w):
        mark_manual(rec, w, MANUAL_REASON_FINAL_CREDIT, "buyer_receive failed while seller_receive held", now_ms)
        return
    if find_leg(w, "seller_return") is not None or find_leg(w, "buyer_return") is not None:
        raise ValueError("auto-unwind requested but return legs already exist")
    buyer.superseded_by = _UNWOUND_BY
    seller.superseded_by = _UNWOUND_BY
    buyer_bundle = protocol.message("AssetBundle")
    buyer_bundle.CopyFrom(buyer.bundle)
    add_escrow_leg(w, "seller_return", int(seller.player_id), int(buyer.escrow_owner_id), buyer_bundle)
    if int(rec.kind) != COMMERCE_KIND_AUCTION_LOT:
        seller_bundle = protocol.message("AssetBundle")
        seller_bundle.CopyFrom(seller.bundle)
        add_escrow_leg(w, "buyer_return", int(buyer.player_id), int(seller.escrow_owner_id), seller_bundle)
    w.stage = "return"
    rec.status = COMMERCE_CANCEL_PENDING


def held_bid_keeps_funds(lot_w: Message, bid_id: int) -> bool:
    if int(lot_w.winning_bid_id) != bid_id:
        return True
    proceeds = find_leg(lot_w, "seller_receive")
    if proceeds is not None and int(proceeds.op_id) != 0:
        return False
    for leg in lot_w.legs:
        if str(leg.role).split("#", 1)[0] == "buyer_receive" and int(leg.op_id) != 0:
            if not leg_done(leg) or leg_applied(leg) or int(leg.outcome) == OP_STATUS_APPLIED_PARTIAL:
                return False
    return True


def record_leg_outcome(protocol: Protocol, leg: Message, out: LegOutcome) -> None:
    if not out.is_final:
        raise ValueError(f"leg {leg.role} op_id={leg.op_id} has invalid non-final status {out.status}")
    leg.outcome = int(out.status)
    if out.snapshot:
        snap = protocol.message("AssetSnapshot")
        try:
            snap.ParseFromString(out.snapshot)
        except DecodeError as exc:
            raise ValueError(f"leg {leg.role} op_id={leg.op_id} corrupt escrow snapshot: {exc}") from exc
        leg.snapshot.CopyFrom(snap)


def owns_record(original: RoleOriginal | None, rec: Message | None, w: Message | None) -> bool:
    """用持久原账号/修订筛选私有历史：角色过户后新主看不到旧主的合同。"""
    if original is None or rec is None or w is None:
        return False
    parties = (
        (int(rec.seller_player_id), w.seller_identity if w.HasField("seller_identity") else None),
        (int(rec.buyer_player_id), w.buyer_identity if w.HasField("buyer_identity") else None),
    )
    for pid, ident in parties:
        if (
            pid == original.player_id
            and ident is not None
            and ident.account == original.account
            and int(ident.owner_revision) == original.owner_revision
        ):
            return True
    return False


def _next_due_ms(rec: Message, w: Message, now_ms: int) -> int:
    if int(rec.kind) == COMMERCE_KIND_RMB_CONSIGNMENT:
        return now_ms
    in_flight = any(int(l.op_id) != 0 and int(l.outcome) == 0 for l in w.legs)
    if not in_flight and int(rec.kind) == COMMERCE_KIND_MARKET_MATCH and int(rec.side) != 0:
        in_flight = w.stage == "market_wait" or (w.stage == "market_open" and int(rec.filled_quantity) == int(rec.quantity))
    if in_flight:
        return now_ms + _IN_FLIGHT_RECHECK_MS
    waits_time = (
        w.stage in ("open", "lot_open", "official_refund")
        or (w.stage == "confirm" and int(rec.kind) == COMMERCE_KIND_PLAYER_TRADE)
        or (w.stage == "market_open" and int(rec.side) != 0 and int(rec.filled_quantity) < int(rec.quantity))
    )
    if not waits_time:
        return now_ms
    due = now_ms + _IDLE_RECHECK_MS
    if w.stage in ("open", "lot_open", "confirm") and int(rec.expires_ms) != 0 and int(rec.expires_ms) < due:
        due = int(rec.expires_ms)
    return max(due, now_ms)


def _filled_parents_to_wake(before: Message, after: Message) -> tuple[int, ...]:
    if int(after.kind) != COMMERCE_KIND_MARKET_MATCH or int(after.side) != 0:
        return ()
    settled_set = (COMMERCE_CANCELLED, COMMERCE_SETTLED)
    if int(before.status) in settled_set or int(after.status) not in settled_set:
        return ()
    return tuple(pid for pid in (int(after.parent_id), int(after.counterparty_id)) if pid != 0)


def _finished_lot_bid(before: Message, after: Message, w: Message) -> int:
    settled_set = (COMMERCE_CANCELLED, COMMERCE_SETTLED)
    if int(after.kind) != COMMERCE_KIND_AUCTION_LOT:
        return 0
    if int(before.status) in settled_set or int(after.status) not in settled_set:
        return 0
    return int(w.winning_bid_id)


def _owner_changed(fence: RoleFenceRow, ident: Message) -> bool:
    return (
        fence.owner_revision > int(ident.owner_revision)
        or (fence.owner_revision == int(ident.owner_revision) and fence.account != ident.account)
    )


def _contract_parties(rec: Message, w: Message) -> list[tuple[str, int, Message | None]]:
    out: list[tuple[str, int, Message | None]] = []
    if int(rec.seller_player_id) != 0:
        out.append(("seller", int(rec.seller_player_id), w.seller_identity if w.HasField("seller_identity") else None))
    if int(rec.buyer_player_id) != 0:
        out.append(("buyer", int(rec.buyer_player_id), w.buyer_identity if w.HasField("buyer_identity") else None))
    out.sort(key=lambda item: item[1])
    return out


# -- 人工处置数据结构与计划函数（对应 Go manual.go & manual_plan.go） ------------------------------------

@dataclass(frozen=True, slots=True)
class PartialMissing:
    instances: tuple[int, ...] = ()
    pet: bool = False
    currencies: tuple[int, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.instances and not self.pet and not self.currencies


@dataclass(frozen=True, slots=True)
class PartialPartSpec:
    leg: str
    missing: PartialMissing


@dataclass(frozen=True, slots=True)
class ManualRequest:
    commerce_id: int
    expect_version: int
    action: str
    operator: str
    reason: str
    parts: tuple[PartialPartSpec, ...] = ()
    party: str = ""
    to_player_id: int = 0
    linked_commerce_id: int = 0
    linked_version: int = 0


@dataclass
class ManualResult:
    version_before: int = 0
    quarantine: Message | None = None
    stage: str = ""
    status: int = 0
    new_legs: list[str] = field(default_factory=list)
    superseded_legs: list[str] = field(default_factory=list)
    party: str = ""
    from_player: int = 0
    to_player: int = 0
    identity_before: Message | None = None
    identity_after: Message | None = None
    retargeted_legs: list[str] = field(default_factory=list)
    partial_parts: list[Message] = field(default_factory=list)
    linked_id: int = 0
    linked: "ManualResult | None" = None
    audit: Message | None = None


@dataclass(slots=True)
class _RoleFact:
    fence: RoleFenceRow | None = None
    deleted: bool = False
    unknown: bool = False


def parse_partial_part(raw: str) -> PartialPartSpec:
    if "=" not in raw:
        raise ManualNotAllowedError(f"partial spec must be <leg>=<missing>, got {raw!r}")
    leg_name, spec = raw.split("=", 1)
    leg_name = leg_name.strip()
    spec = spec.strip()
    if not leg_name or not spec:
        raise ManualNotAllowedError(f"partial spec missing leg or items: {raw!r}")
    instances: list[int] = []
    currencies: list[int] = []
    seen_inst: set[int] = set()
    seen_cur: set[int] = set()
    pet = False
    for token in (part.strip() for part in spec.split(",")):
        if token == "pet":
            if pet:
                raise ManualNotAllowedError("duplicate pet in partial spec")
            pet = True
        elif token.startswith("instance:"):
            val = token[len("instance:") :]
            if not val.isdigit() or int(val) == 0:
                raise ManualNotAllowedError(f"invalid instance guid: {token!r}")
            uid = int(val)
            if uid in seen_inst:
                raise ManualNotAllowedError(f"duplicate instance {uid} in partial spec")
            seen_inst.add(uid)
            instances.append(uid)
        elif token.startswith("currency:"):
            val = token[len("currency:") :]
            if not val.isdigit():
                raise ManualNotAllowedError(f"invalid currency type: {token!r}")
            ctype = int(val)
            if ctype in seen_cur:
                raise ManualNotAllowedError(f"duplicate currency {ctype} in partial spec")
            seen_cur.add(ctype)
            currencies.append(ctype)
        else:
            raise ManualNotAllowedError(f"unknown partial token {token!r}")
    return PartialPartSpec(
        leg=leg_name,
        missing=PartialMissing(instances=tuple(instances), pet=pet, currencies=tuple(currencies)),
    )


def _validate_manual_request(req: ManualRequest) -> None:
    if req.commerce_id == 0 or req.expect_version == 0:
        raise ManualNotAllowedError("commerce_id and expect_version are required")
    if req.action not in MANUAL_ACTIONS:
        raise ManualNotAllowedError(f"unknown action {req.action!r}")
    for name, value, limit in (
        ("operator", req.operator, MANUAL_OPERATOR_MAX_BYTES),
        ("reason", req.reason, MANUAL_REASON_MAX_BYTES),
    ):
        if not value.strip() or len(value.encode("utf-8")) > limit:
            raise ManualNotAllowedError(f"{name} must be non-empty and <= {limit} bytes")
        if any(unicodedata.category(ch).startswith("C") for ch in value):
            raise ManualNotAllowedError(f"{name} must not contain control characters")
    if req.action == "partial":
        if not 1 <= len(req.parts) <= MAX_NEW_LEGS_PER_STEP:
            raise ManualNotAllowedError(f"partial requires 1..{MAX_NEW_LEGS_PER_STEP} parts")
        seen = set()
        for part in req.parts:
            if not part.leg or part.missing.is_empty or part.leg in seen:
                raise ManualNotAllowedError("invalid or duplicate partial leg")
            seen.add(part.leg)
    elif req.parts:
        raise ManualNotAllowedError("parts only allowed for partial action")
    if req.action == "redirect":
        if req.party not in ("seller", "buyer") or req.to_player_id == 0:
            raise ManualNotAllowedError("redirect requires party in {seller,buyer} and non-zero to_player_id")
    elif req.party or req.to_player_id != 0:
        raise ManualNotAllowedError("party/to_player_id only allowed for redirect action")
    if req.action == "void-auction":
        if req.linked_commerce_id == 0 or req.linked_version == 0 or req.linked_commerce_id == req.commerce_id:
            raise ManualNotAllowedError("void-auction requires linked_commerce_id and linked_version")
    elif req.linked_commerce_id != 0 or req.linked_version != 0:
        raise ManualNotAllowedError("linked_commerce_id/linked_version only allowed for void-auction")


def _reissue_role(w: Message, role: str) -> str:
    base, num = role, 1
    if "#" in role:
        head, tail = role.rsplit("#", 1)
        if tail.isdigit() and int(tail) >= 2:
            base, num = head, int(tail)
    k = num + 1
    while True:
        cand = f"{base}#{k}"
        if find_leg(w, cand) is None:
            return cand
        k += 1


def _failed_credits(w: Message) -> list[Message]:
    return [
        l
        for l in w.legs
        if int(l.stream) == ASSET_STREAM_TRADE_CREDIT
        and not l.superseded_by
        and leg_done(l)
        and not leg_applied(l)
        and int(l.outcome) != OP_STATUS_APPLIED_PARTIAL
    ]


def _failed_official_reversal_legs(w: Message) -> list[Message]:
    return [
        l
        for l in w.legs
        if not l.superseded_by
        and leg_done(l)
        and not leg_applied(l)
        and int(l.outcome) != OP_STATUS_APPLIED_PARTIAL
        and (
            int(l.stream) == ASSET_STREAM_TRADE_CREDIT
            or (int(l.stream) == ASSET_STREAM_TRADE_DEBIT and is_official_reverse_leg(str(l.role)))
        )
    ]


def _reissuable_quarantine(q: Message) -> bool:
    if q.reason == MANUAL_REASON_FINAL_CREDIT:
        return q.stage in ("deliver", "return")
    if q.reason == MANUAL_REASON_RMB_RETURN:
        return True
    if q.reason == MANUAL_REASON_PAYMENT_REVERSAL:
        return q.stage == STAGE_OFFICIAL_DELIVER
    return False


def _is_refund_leg(role: str) -> bool:
    return role.split("#", 1)[0].endswith("/refund")


def _refund_recipient(rec: Message, debit: Message) -> int:
    party = int(rec.buyer_player_id) if str(debit.role).startswith("buyer_debit") else int(rec.seller_player_id)
    return party if party != 0 else int(debit.player_id)


def _credit_shortfall(protocol: Protocol, bundle: Message, missing: PartialMissing) -> Message:
    if missing.is_empty:
        raise ManualNotAllowedError("missing shortfall is empty")
    if len(bundle.item_uuids) > 0 or int(bundle.pet_id) != 0:
        raise ManualNotAllowedError("credit leg carries debit-only fields")
    order: list[tuple[str, int]] = []
    for it in bundle.item_instances:
        order.append(("instance", int(it.item_uuid)))
    if bundle.HasField("pet_instance") and int(bundle.pet_instance.pet_id) != 0:
        order.append(("pet", 0))
    for cur in bundle.currencies:
        order.append(("currency", int(cur.currency_type)))
    pos: dict[tuple[str, int], int] = {}
    for idx, unit in enumerate(order):
        if unit in pos:
            raise ManualNotAllowedError(f"duplicate unit {unit} in bundle")
        pos[unit] = idx
    wanted: set[tuple[str, int]] = set()
    for uid in missing.instances:
        wanted.add(("instance", uid))
    if missing.pet:
        wanted.add(("pet", 0))
    for ctype in missing.currencies:
        wanted.add(("currency", ctype))
    first = len(order)
    for unit in wanted:
        if unit not in pos:
            raise ManualNotAllowedError(f"unit {unit} not found in bundle")
        first = min(first, pos[unit])
    for idx in range(first, len(order)):
        if order[idx] not in wanted:
            raise ManualNotAllowedError("shortfall must be a suffix in scene credit order")
    if first == 0 and len(bundle.items) == 0:
        raise ManualNotAllowedError("entire leg missing; use reissue instead of partial")
    out = protocol.message("AssetBundle")
    for it in bundle.item_instances:
        if ("instance", int(it.item_uuid)) in wanted:
            out.item_instances.add().CopyFrom(it)
    if ("pet", 0) in wanted:
        out.pet_instance.CopyFrom(bundle.pet_instance)
    for cur in bundle.currencies:
        if ("currency", int(cur.currency_type)) in wanted:
            out.currencies.add().CopyFrom(cur)
    return out


# -- CommerceEngine -------------------------------------------------------------------------------------

class CommerceEngine:
    """交易合同状态机与人工处置引擎。"""

    def __init__(
        self,
        protocol: Protocol,
        store: TradeStoreBase,
        *,
        commerce_ids: Callable[[], Awaitable[int]],
        op_ids: Callable[[], Awaitable[int]],
        policy: TradePolicy,
        asset_caller: AssetOpCaller | None = None,
        ledger_reader: SqlCommittedLedgerReader | None = None,
        rmb=None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self._commerce_ids = commerce_ids
        self._op_ids = op_ids
        self.policy = policy
        self._asset_caller = asset_caller
        self._ledger_reader = ledger_reader
        self.rmb = rmb
        self._clock_ms = clock_ms
        self._ack_after_op_id = 0

    def now_ms(self) -> int:
        return int(self._clock_ms())

    async def next_commerce_id(self) -> int:
        return int(await self._commerce_ids())

    async def lock_roles(
        self,
        tx: TradeTx,
        original: RoleOriginal | None,
        allow_listing: int,
        *players: int,
    ) -> dict[int, RoleFenceRow]:
        return await self._lock_roles(tx, original, allow_listing, *players)

    async def create_contract(
        self,
        original: RoleOriginal | None,
        player_id: int,
        client_req_id: str,
        input_msg: Message,
        rec: Message,
        w: Message,
    ) -> Message:
        return await self._create(original, player_id, client_req_id, input_msg, rec, w)

    async def advance_contract(self, rec: Message) -> None:
        await self._advance(rec)

    async def apply_payment_revision(self, commerce_id: int, revision: int) -> Message:
        """记录官方商城报来的付款修订号（只增不减），并让非人工合同回到收敛阶段。"""
        rev = int(revision)

        async def _tx(tx: TradeTx) -> Message:
            rec = await tx.lock_commerce(int(commerce_id))
            if rec is None:
                raise TradeCorruptContractError(f"official commerce {commerce_id} vanished")
            w = decode_workflow(self.protocol, rec)
            if int(rec.kind) != COMMERCE_KIND_OFFICIAL or int(w.payment_revision) >= rev:
                return rec
            w.payment_revision = rev
            w.payment_revoked = (rev % 2 == 1)
            if int(rec.status) != COMMERCE_MANUAL:
                w.stage = STAGE_OFFICIAL_DELIVER
                rec.status = COMMERCE_DELIVERING
            await tx.save_commerce(rec, w, self.now_ms())
            return rec

        return await self.store.run_tx(_tx)

    # -- 内部角色锁与校验 ---------------------------------------------------------------------------

    async def _lock_roles(
        self,
        tx: TradeTx,
        original: RoleOriginal | None,
        allow_listing: int,
        *players: int,
    ) -> dict[int, RoleFenceRow]:
        expects: list[RoleFenceExpect] = []
        for pid in players:
            if pid:
                expects.append(RoleFenceExpect(player_id=int(pid), allow_token=allow_listing, reject=TradeRoleConflictError()))
        if original is not None:
            expects.append(
                RoleFenceExpect(
                    player_id=original.player_id,
                    account=original.account,
                    owner_revision=original.owner_revision,
                    allow_token=allow_listing,
                    reject=TradeRoleConflictError(),
                )
            )
        return await self.store.lock_roles(tx, expects)

    async def _check_contract_identities(self, tx: TradeTx, rec: Message, w: Message) -> None:
        for _, pid, ident in _contract_parties(rec, w):
            if ident is None or not valid_account(ident.account) or int(ident.owner_revision) == 0:
                raise TradeCorruptContractError("contract missing original role identity")
            fence = await self.store.read_ownership(tx, pid)
            if fence.owner_revision < int(ident.owner_revision):
                raise RoleFenceStaleError(pid, reject=TradeRoleConflictError())
            if fence.account != ident.account or fence.owner_revision != int(ident.owner_revision):
                raise TradeRoleConflictError("contract party role changed owner")

    async def _verify_replay_actor(self, original: RoleOriginal | None, old: Message) -> None:
        if original is None:
            return
        allow = 0
        if int(old.kind) == COMMERCE_KIND_RMB_CONSIGNMENT:
            w = decode_workflow(self.protocol, old)
            if w.HasField("rmb") and int(w.rmb.character_player_id) != 0:
                allow = int(old.commerce_id)

        async def _tx(tx: TradeTx) -> None:
            await self._lock_roles(tx, original, allow)

        await self.store.run_role_tx(_tx)

    async def _prior_request(
        self, original: RoleOriginal | None, player_id: int, client_req_id: str, input_msg: Message
    ) -> Message | None:
        if player_id == 0 or not 8 <= len(client_req_id) <= 80:
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid client_request_id")
        rkey = request_key("player", player_id, client_req_id)
        old = await self.store.get_commerce_by_request(rkey)
        if old is None:
            return None
        if bytes(old.request_digest) != request_digest(self.protocol, input_msg, original):
            raise TradeRejection(ERR_TRADE_REQUEST_CONFLICT, "idempotent request digest mismatch")
        await self._verify_replay_actor(original, old)
        return old

    async def _create(
        self,
        original: RoleOriginal | None,
        player_id: int,
        client_req_id: str,
        input_msg: Message,
        rec: Message,
        w: Message,
    ) -> Message:
        prior = await self._prior_request(original, player_id, client_req_id, input_msg)
        if prior is not None:
            return prior
        rkey = request_key("player", player_id, client_req_id)
        rdigest = request_digest(self.protocol, input_msg, original)
        cid = await self._commerce_ids()
        now = self.now_ms()
        rec.commerce_id = cid
        rec.request_key = rkey
        rec.request_digest = rdigest
        rec.created_ms = now
        rec.updated_ms = now
        rec.version = 1

        async def _tx(tx: TradeTx) -> None:
            await self._lock_roles(tx, original, 0, int(rec.seller_player_id), int(rec.buyer_player_id))
            if int(rec.kind) == COMMERCE_KIND_RMB_CONSIGNMENT:
                if not w.HasField("rmb"):
                    raise TradeRejection(ERR_INVALID_PARAMETER, "rmb contract missing terms")
                seller_fence = await self.store.read_ownership(tx, int(rec.seller_player_id))
                if seller_fence.owner_revision < int(w.rmb.seller_owner_revision):
                    raise RoleFenceStaleError(int(rec.seller_player_id), reject=TradeRoleConflictError())
                trade_listing_id = seller_fence.freeze_token if seller_fence.freeze_final else 0
                if (
                    seller_fence.account != str(w.rmb.seller_account)
                    or seller_fence.owner_revision != int(w.rmb.seller_owner_revision)
                    or trade_listing_id != 0
                ):
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "rmb seller role ownership conflict")
            if int(rec.seller_player_id) != 0:
                fence = await self.store.read_ownership(tx, int(rec.seller_player_id))
                w.seller_identity.account = fence.account
                w.seller_identity.owner_revision = fence.owner_revision
            if int(rec.buyer_player_id) != 0:
                fence = await self.store.read_ownership(tx, int(rec.buyer_player_id))
                w.buyer_identity.account = fence.account
                w.buyer_identity.owner_revision = fence.owner_revision
            rec.workflow_proto = w.SerializeToString(deterministic=True)
            await tx.insert_commerce(rec)

        try:
            await self.store.run_role_tx(_tx)
        except _DuplicateKeyError:
            old = await self.store.get_commerce_by_request(rkey)
            if old is None:
                raise
            if bytes(old.request_digest) != rdigest:
                raise TradeRejection(ERR_TRADE_REQUEST_CONFLICT, "idempotent request digest conflict")
            await self._verify_replay_actor(original, old)
            return old
        return rec

    # -- 客户端写/读入口 ----------------------------------------------------------------------------

    async def create_player_trade(self, original: RoleOriginal, req: Message) -> Message:
        me = original.player_id
        buyer = int(req.buyer_player_id)
        if buyer == 0:
            raise TradeRejection(ERR_INVALID_PARAMETER, "buyer_player_id required")
        if buyer == me:
            raise TradeRejection(ERR_TRADE_CANNOT_TRADE_SELF, "cannot trade with self")
        self.policy.validate_selection(req.seller_assets if req.HasField("seller_assets") else None)
        self.policy.validate_selection(req.buyer_assets if req.HasField("buyer_assets") else None)
        rec = self.protocol.message(
            "trade.TradeCommerceRecord",
            kind=COMMERCE_KIND_PLAYER_TRADE,
            status=COMMERCE_ESCROWING,
            seller_player_id=me,
            buyer_player_id=buyer,
            expires_ms=self.now_ms() + 15 * 60 * 1000,
        )
        w = self.protocol.message("trade.TradeCommerceWorkflow", stage="offer_escrow")
        w.seller_assets.CopyFrom(req.seller_assets)
        w.buyer_assets.CopyFrom(req.buyer_assets)
        return await self._create(original, me, str(req.client_request_id), req, rec, w)

    async def create_consignment(self, original: RoleOriginal, req: Message) -> Message:
        me = original.player_id
        self.policy.validate_selection(req.assets if req.HasField("assets") else None)
        price = int(req.price)
        cur = int(req.currency_type)
        dur = int(req.duration_ms)
        if not self.policy.valid_price(price, cur) or dur < 60_000 or dur > 30 * 24 * 3600 * 1000:
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid consignment price or duration")
        rec = self.protocol.message(
            "trade.TradeCommerceRecord",
            kind=COMMERCE_KIND_CONSIGNMENT,
            status=COMMERCE_ESCROWING,
            seller_player_id=me,
            unit_price=price,
            currency_type=cur,
            expires_ms=self.now_ms() + dur,
        )
        w = self.protocol.message("trade.TradeCommerceWorkflow", stage="escrow")
        w.seller_assets.CopyFrom(req.assets)
        return await self._create(original, me, str(req.client_request_id), req, rec, w)

    async def create_auction_lot(self, original: RoleOriginal, req: Message) -> Message:
        me = original.player_id
        self.policy.validate_selection(req.assets if req.HasField("assets") else None)
        opening = int(req.opening_price)
        min_inc = int(req.minimum_increment)
        cur = int(req.currency_type)
        dur = int(req.duration_ms)
        if (
            not self.policy.valid_price(opening, cur)
            or min_inc == 0
            or min_inc > INT64_MAX
            or dur < 60_000
            or dur > 7 * 24 * 3600 * 1000
        ):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid auction lot parameters")
        rec = self.protocol.message(
            "trade.TradeCommerceRecord",
            kind=COMMERCE_KIND_AUCTION_LOT,
            status=COMMERCE_ESCROWING,
            seller_player_id=me,
            unit_price=opening,
            currency_type=cur,
            expires_ms=self.now_ms() + dur,
        )
        w = self.protocol.message("trade.TradeCommerceWorkflow", stage="escrow", minimum_increment=min_inc)
        w.seller_assets.CopyFrom(req.assets)
        return await self._create(original, me, str(req.client_request_id), req, rec, w)

    async def place_market_order(self, original: RoleOriginal, req: Message) -> Message:
        me = original.player_id
        qty = int(req.quantity)
        market_id = int(req.market_id)
        item_cfg = int(req.item_config_id)
        side = int(req.side)
        unit_price = int(req.unit_price)
        cur = int(req.currency_type)
        if (
            qty == 0
            or qty > 4096
            or market_id != 1
            or not self.policy.currencies
            or cur != self.policy.currencies[0]
            or item_cfg not in self.policy.items
            or not self.policy.valid_price(unit_price, cur)
            or unit_price > INT64_MAX // qty
            or side not in (1, 2)
        ):
            raise TradeRejection(ERR_INVALID_PARAMETER, "invalid market order parameters")
        sel = self.protocol.message("trade.TradeAssetSelection")
        if side == 1:
            sel.currency_type = cur
            sel.currency_amount = unit_price * qty
        else:
            sel.items[item_cfg] = qty
        rec = self.protocol.message(
            "trade.TradeCommerceRecord",
            kind=COMMERCE_KIND_MARKET_MATCH,
            status=COMMERCE_ESCROWING,
            seller_player_id=me,
            market_id=market_id,
            item_config_id=item_cfg,
            side=side,
            unit_price=unit_price,
            quantity=qty,
            currency_type=cur,
        )
        w = self.protocol.message("trade.TradeCommerceWorkflow", stage="escrow")
        w.seller_assets.CopyFrom(sel)
        return await self._create(original, me, str(req.client_request_id), req, rec, w)

    async def place_auction_bid(self, original: RoleOriginal, lot_id: int, req: Message) -> Message:
        me = original.player_id
        lot = await self.store.get_commerce(lot_id)
        if lot is None:
            raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "auction lot not found")
        prior = await self._prior_request(original, me, str(req.client_request_id), req)
        if prior is not None:
            return prior
        w = decode_workflow(self.protocol, lot)
        if int(lot.kind) != COMMERCE_KIND_AUCTION_LOT or w.stage != "lot_open" or int(lot.expires_ms) <= self.now_ms():
            raise TradeRejection(ERR_TRADE_AUCTION_ENDED, "auction ended")
        if me == int(lot.seller_player_id):
            raise TradeRejection(ERR_TRADE_CANNOT_TRADE_SELF, "cannot bid on own auction")
        amount = int(req.amount)
        if amount < int(lot.unit_price) or amount > INT64_MAX:
            raise TradeRejection(ERR_TRADE_BID_TOO_LOW, "bid below opening price")
        if int(w.highest_bid) > 0 and (
            int(w.minimum_increment) > INT64_MAX - int(w.highest_bid)
            or amount < int(w.highest_bid) + int(w.minimum_increment)
        ):
            raise TradeRejection(ERR_TRADE_BID_TOO_LOW, "bid below minimum increment")
        rec = self.protocol.message(
            "trade.TradeCommerceRecord",
            kind=COMMERCE_KIND_BID,
            status=COMMERCE_ESCROWING,
            seller_player_id=me,
            parent_id=lot_id,
            unit_price=amount,
            currency_type=int(lot.currency_type),
            expires_ms=int(lot.expires_ms),
        )
        bw = self.protocol.message("trade.TradeCommerceWorkflow", stage="bid_freeze")
        bw.seller_assets.currency_type = int(lot.currency_type)
        bw.seller_assets.currency_amount = amount
        return await self._create(original, me, str(req.client_request_id), req, rec, bw)

    async def mutate(self, original: RoleOriginal, commerce_id: int, action: str, client_request_id: str = "") -> Message:
        me = original.player_id
        before = await self.store.get_commerce(commerce_id)
        if before is None:
            raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "commerce order not found")

        async def _tx(tx: TradeTx) -> Message:
            await self._lock_roles(tx, original, 0, me, int(before.seller_player_id), int(before.buyer_player_id))
            rec = await tx.lock_commerce(commerce_id)
            if rec is None:
                raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "commerce order not found")
            w = decode_workflow(self.protocol, rec)
            if (
                int(rec.seller_player_id) != int(before.seller_player_id)
                or int(rec.buyer_player_id) != int(before.buyer_player_id)
            ):
                raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "contract participants changed")
            await self._check_contract_identities(tx, rec, w)
            now = await tx.now_ms()

            if action == "confirm":
                if int(rec.kind) != COMMERCE_KIND_PLAYER_TRADE or w.stage != "confirm":
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "cannot confirm in current stage")
                if int(rec.expires_ms) <= now or w.cancel_requested:
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "trade expired or cancelling")
                if me == int(rec.seller_player_id):
                    if not w.buyer_confirmed or not leg_applied(find_leg(w, "buyer_debit")):
                        raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "buyer must confirm and escrow first")
                    w.seller_confirmed = True
                elif me == int(rec.buyer_player_id):
                    if not leg_applied(find_leg(w, "seller_debit")):
                        raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "seller escrow not ready")
                    w.buyer_confirmed = True
                    if not leg_applied(find_leg(w, "buyer_debit")):
                        w.stage = "buyer_escrow"
                        rec.status = COMMERCE_ESCROWING
                else:
                    raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "not a party to trade")
                if w.seller_confirmed and w.buyer_confirmed:
                    w.stage = "escrow"
                    rec.status = COMMERCE_ESCROWING

            elif action == "buy":
                if not 8 <= len(client_request_id) <= 80:
                    raise TradeRejection(ERR_INVALID_PARAMETER, "invalid client_request_id")
                buy_key = request_key("consignment_buy", me, client_request_id)
                for l in w.legs:
                    if str(l.role).endswith("_" + buy_key):
                        if int(rec.buyer_player_id) == me and w.stage in ("pay", "deliver", "done"):
                            return rec
                        raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "buy request conflict")
                if (
                    int(rec.kind) == COMMERCE_KIND_CONSIGNMENT
                    and int(rec.buyer_player_id) == me
                    and w.stage in ("pay", "deliver", "done")
                ):
                    return rec
                if (
                    int(rec.kind) != COMMERCE_KIND_CONSIGNMENT
                    or w.stage != "open"
                    or int(rec.expires_ms) <= now
                    or int(rec.buyer_player_id) != 0
                ):
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "consignment not open")
                if me == int(rec.seller_player_id):
                    raise TradeRejection(ERR_TRADE_CANNOT_TRADE_SELF, "cannot buy own consignment")
                if int(w.quantity_reserved) >= 32:
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "consignment reservation limit")
                rec.buyer_player_id = me
                buyer_fence = await self.store.read_ownership(tx, me)
                w.buyer_identity.account = buyer_fence.account
                w.buyer_identity.owner_revision = buyer_fence.owner_revision
                w.buyer_assets.currency_type = int(rec.currency_type)
                w.buyer_assets.currency_amount = int(rec.unit_price)
                w.quantity_reserved = int(w.quantity_reserved) + 1
                add_leg(
                    w,
                    f"buyer_debit_{w.quantity_reserved}_{buy_key}",
                    me,
                    ASSET_STREAM_TRADE_DEBIT,
                    build_asset_bundle(self.protocol, w.buyer_assets),
                )
                w.stage = "pay"
                rec.status = COMMERCE_ESCROWING

            elif action == "cancel":
                if me != int(rec.seller_player_id) and me != int(rec.buyer_player_id):
                    raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "not a party to order")
                if int(rec.status) == COMMERCE_CANCELLED:
                    return rec
                if int(rec.kind) == COMMERCE_KIND_PLAYER_TRADE and w.stage in ("confirm", "offer_escrow", "buyer_escrow"):
                    w.cancel_requested = True
                    rec.status = COMMERCE_CANCEL_PENDING
                elif me == int(rec.seller_player_id) and (
                    w.stage in ("open", "market_open")
                    or (w.stage == "lot_open" and int(w.highest_bidder) == 0)
                    or (
                        w.stage == "escrow"
                        and int(rec.kind) in (COMMERCE_KIND_CONSIGNMENT, COMMERCE_KIND_MARKET_MATCH, COMMERCE_KIND_AUCTION_LOT)
                    )
                ):
                    w.cancel_requested = True
                    rec.status = COMMERCE_CANCEL_PENDING
                else:
                    raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "cannot cancel in current stage")
                await tx.expire_commerce_escrow_debits(int(rec.commerce_id), now)
            else:
                raise TradeRejection(ERR_INVALID_PARAMETER, f"unknown mutate action {action!r}")

            await tx.save_commerce(rec, w, self.now_ms())
            return rec

        return await self.store.run_role_tx(_tx)

    async def get(self, original: RoleOriginal | None, commerce_id: int) -> Message:
        rec = await self.store.get_commerce(commerce_id)
        if rec is None:
            raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "commerce order not found")
        me = original.player_id if original is not None else 0
        is_public = int(rec.kind) in (COMMERCE_KIND_CONSIGNMENT, COMMERCE_KIND_AUCTION_LOT)
        if not is_public and int(rec.seller_player_id) != me and int(rec.buyer_player_id) != me:
            raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "order not visible to caller")
        if original is not None and not is_public:
            w = decode_workflow(self.protocol, rec)
            if not owns_record(original, rec, w):
                raise TradeRejection(ERR_TRADE_ORDER_NOT_FOUND, "order not owned by original role identity")
        return rec

    # -- 状态机推进（plan / advance / match_market / accept_bid / settle_held_bid / Tick） ---------------

    def _plan(self, rec: Message, w: Message, now_ms: int) -> None:
        kind = int(rec.kind)
        if kind == COMMERCE_KIND_RMB_CONSIGNMENT:
            plan_rmb(self.protocol, self.policy, rec, w, now_ms)
            return
        if w.stage == STAGE_OFFICIAL_DELIVER:
            plan_official(rec, w, now_ms)
            return
        if w.stage == "offer_escrow":
            add_leg(w, "seller_debit", int(rec.seller_player_id), ASSET_STREAM_TRADE_DEBIT, build_asset_bundle(self.protocol, w.seller_assets))
            a = find_leg(w, "seller_debit")
            if not leg_done(a):
                return
            if not leg_applied(a):
                w.stage = "done"
                rec.status = COMMERCE_CANCELLED
                return
            if not self.policy.allow_snapshot(a.snapshot if a.HasField("snapshot") else None):
                w.cancel_requested = True
            w.stage = "confirm"
            rec.status = COMMERCE_WAITING

        if w.stage == "buyer_escrow":
            add_leg(w, "buyer_debit", int(rec.buyer_player_id), ASSET_STREAM_TRADE_DEBIT, build_asset_bundle(self.protocol, w.buyer_assets))
            b = find_leg(w, "buyer_debit")
            if not leg_done(b):
                return
            if not leg_applied(b) or not self.policy.allow_snapshot(b.snapshot if b.HasField("snapshot") else None):
                w.cancel_requested = True
            w.stage = "confirm"
            rec.status = COMMERCE_WAITING

        if kind == COMMERCE_KIND_PLAYER_TRADE and w.stage == "confirm" and (w.cancel_requested or int(rec.expires_ms) <= now_ms):
            w.stage = "return"
            rec.status = COMMERCE_CANCEL_PENDING
            a = find_leg(w, "seller_debit")
            if leg_applied(a):
                transfer_leg(self.protocol, w, "seller_return", int(rec.seller_player_id), a)
            b = find_leg(w, "buyer_debit")
            if leg_applied(b):
                transfer_leg(self.protocol, w, "buyer_return", int(rec.buyer_player_id), b)

        if w.stage == "bid_freeze":
            add_leg(w, "bid_debit", int(rec.seller_player_id), ASSET_STREAM_TRADE_DEBIT, build_asset_bundle(self.protocol, w.seller_assets))
            b = find_leg(w, "bid_debit")
            if leg_done(b):
                if leg_applied(b):
                    w.stage = "bid_ready"
                else:
                    w.stage = "done"
                    rec.status = COMMERCE_CANCELLED

        if w.stage == "bid_refund":
            b = find_leg(w, "bid_debit")
            if not leg_applied(b):
                raise ValueError("bid_refund missing applied bid_debit")
            transfer_leg(self.protocol, w, "bid_return", int(rec.seller_player_id), b)
            w.stage = "return"
            rec.status = COMMERCE_CANCEL_PENDING

        if w.stage == "escrow":
            add_leg(w, "seller_debit", int(rec.seller_player_id), ASSET_STREAM_TRADE_DEBIT, build_asset_bundle(self.protocol, w.seller_assets))
            if kind == COMMERCE_KIND_PLAYER_TRADE:
                add_leg(w, "buyer_debit", int(rec.buyer_player_id), ASSET_STREAM_TRADE_DEBIT, build_asset_bundle(self.protocol, w.buyer_assets))
            a = find_leg(w, "seller_debit")
            b = find_leg(w, "buyer_debit")
            if not leg_done(a) or (kind == COMMERCE_KIND_PLAYER_TRADE and not leg_done(b)):
                return
            bad_a = leg_applied(a) and not self.policy.allow_snapshot(a.snapshot if a.HasField("snapshot") else None)
            bad_b = leg_applied(b) and not self.policy.allow_snapshot(b.snapshot if b.HasField("snapshot") else None)
            if not leg_applied(a) or (kind == COMMERCE_KIND_PLAYER_TRADE and not leg_applied(b)) or bad_a or bad_b:
                w.stage = "return"
                rec.status = COMMERCE_CANCEL_PENDING
                if leg_applied(a):
                    transfer_leg(self.protocol, w, "seller_return", int(rec.seller_player_id), a)
                if leg_applied(b):
                    transfer_leg(self.protocol, w, "buyer_return", int(rec.buyer_player_id), b)
            elif kind == COMMERCE_KIND_PLAYER_TRADE:
                w.stage = "deliver"
                rec.status = COMMERCE_DELIVERING
                transfer_leg(self.protocol, w, "seller_receive", int(rec.seller_player_id), b)
                transfer_leg(self.protocol, w, "buyer_receive", int(rec.buyer_player_id), a)
            elif kind == COMMERCE_KIND_MARKET_MATCH:
                w.stage = "market_open"
            elif kind == COMMERCE_KIND_AUCTION_LOT:
                w.stage = "lot_open"
            else:
                w.stage = "open"

        if w.stage == "pay":
            prefix = f"buyer_debit_{int(w.quantity_reserved)}"
            b = find_leg(w, prefix)
            for l in w.legs:
                if str(l.role).startswith(prefix + "_"):
                    b = l
                    break
            if b is None:
                b = add_leg(w, prefix, int(rec.buyer_player_id), ASSET_STREAM_TRADE_DEBIT, build_asset_bundle(self.protocol, w.buyer_assets))
            if not leg_done(b):
                return
            if not leg_applied(b):
                w.stage = "open"
                rec.buyer_player_id = 0
            else:
                w.stage = "deliver"
                rec.status = COMMERCE_DELIVERING
                transfer_leg(self.protocol, w, "buyer_receive", int(rec.buyer_player_id), find_leg(w, "seller_debit"))
                transfer_leg(self.protocol, w, "seller_receive", int(rec.seller_player_id), b)

        if w.stage == "lot_open" and int(rec.expires_ms) <= now_ms and int(w.highest_bidder) != 0:
            w.stage = "deliver"
            rec.status = COMMERCE_DELIVERING
            transfer_leg(self.protocol, w, "buyer_receive", int(w.highest_bidder), find_leg(w, "seller_debit"))
            add_leg(w, "seller_receive", int(rec.seller_player_id), ASSET_STREAM_TRADE_CREDIT, build_asset_bundle(self.protocol, w.buyer_assets))

        if w.stage in ("open", "lot_open") and int(rec.expires_ms) <= now_ms:
            w.cancel_requested = True

        if w.cancel_requested and (
            w.stage in ("open", "market_open")
            or (w.stage == "lot_open" and int(w.highest_bidder) == 0)
        ):
            w.stage = "return"
            rec.status = COMMERCE_CANCEL_PENDING
            s_debit = find_leg(w, "seller_debit")
            if kind == COMMERCE_KIND_MARKET_MATCH:
                ret_bundle = market_remainder(self.protocol, rec)
            else:
                snap = s_debit.snapshot if s_debit.HasField("snapshot") else None
                ret_bundle = transfer_bundle(self.protocol, s_debit.bundle, snap)
            if not empty_bundle(ret_bundle):
                add_escrow_leg(w, "seller_return", int(rec.seller_player_id), int(s_debit.player_id), ret_bundle)

        if w.stage == "deliver":
            settle_buyer_first_delivery(self.protocol, rec, w, now_ms)

        if w.stage in ("deliver", "return"):
            ready, ok = final_credits(w)
            if ready and not ok:
                mark_manual(rec, w, MANUAL_REASON_FINAL_CREDIT, "final credit leg failed", now_ms)
            elif ready:
                w.stage = "done"
                rec.status = COMMERCE_CANCELLED if int(rec.status) == COMMERCE_CANCEL_PENDING else COMMERCE_SETTLED

    async def _market_barrier(self, tx: TradeTx, rec: Message, w: Message) -> None:
        if int(rec.kind) != COMMERCE_KIND_MARKET_MATCH or int(rec.side) == 0:
            return
        pending = await tx.count_pending_market_fills(int(rec.commerce_id))
        if w.stage == "done" and pending > 0:
            w.stage = "market_wait"
            rec.status = COMMERCE_CANCEL_PENDING
        if w.stage == "market_wait" and pending == 0:
            w.stage = "done"
            rec.status = COMMERCE_CANCELLED
        if w.stage == "market_open" and int(rec.quantity) == int(rec.filled_quantity) and pending == 0:
            w.stage = "done"
            rec.status = COMMERCE_SETTLED

    async def _advance(self, old: Message) -> None:
        w0 = decode_workflow(self.protocol, old)
        seq_keys: list[tuple[int, int]] = []
        for pid in (int(old.seller_player_id), int(old.buyer_player_id), int(w0.highest_bidder)):
            if pid > 0:
                seq_keys.append((pid, ASSET_STREAM_TRADE_DEBIT))
                seq_keys.append((pid, ASSET_STREAM_TRADE_CREDIT))
        await self.store.ensure_seq_rows(self.now_ms(), seq_keys)
        pre_op_ids = [await self._op_ids() for _ in range(MAX_NEW_LEGS_PER_STEP)]

        parents_to_wake: list[int] = []
        bid_to_wake = 0

        async def _tx(tx: TradeTx) -> None:
            nonlocal parents_to_wake, bid_to_wake
            parents_to_wake = []
            bid_to_wake = 0
            await self._lock_roles(tx, None, 0, int(old.seller_player_id), int(old.buyer_player_id), int(w0.highest_bidder))
            rec = await tx.lock_commerce(int(old.commerce_id))
            if rec is None:
                raise TradeCorruptContractError(f"commerce {old.commerce_id} vanished")
            w = decode_workflow(self.protocol, rec)
            locked_rec = self.protocol.message("trade.TradeCommerceRecord")
            locked_rec.CopyFrom(rec)
            locked_w = self.protocol.message("trade.TradeCommerceWorkflow")
            locked_w.CopyFrom(w)
            if (
                int(rec.seller_player_id) != int(old.seller_player_id)
                or int(rec.buyer_player_id) != int(old.buyer_player_id)
            ):
                raise TradeStaleObservationError("participants changed after due read")
            await self._check_contract_identities(tx, rec, w)

            for leg in w.legs:
                if int(leg.op_id) == 0 or int(leg.outcome) != 0:
                    continue
                out = await tx.read_leg_outcome(int(leg.op_id))
                if out is None:
                    raise TradeCorruptContractError(f"leg {leg.role} op_id={leg.op_id} missing")
                if out.is_pending:
                    continue
                if not out.is_final:
                    raise TradeCorruptContractError(f"leg {leg.role} op_id={leg.op_id} invalid status={out.status}")
                if out.status == OP_STATUS_APPLIED_PARTIAL:
                    await tx.expire_commerce_escrow_debits(int(rec.commerce_id), self.now_ms())
                    mark_manual(rec, w, MANUAL_REASON_APPLIED_PARTIAL, f"leg {leg.role} op_id={leg.op_id} partial", self.now_ms())
                    parents_to_wake = list(_filled_parents_to_wake(locked_rec, rec))
                    await tx.save_commerce(rec, w, self.now_ms())
                    return
                try:
                    record_leg_outcome(self.protocol, leg, out)
                except ValueError as exc:
                    raise TradeCorruptContractError(str(exc)) from exc
                if int(leg.stream) == ASSET_STREAM_TRADE_DEBIT and leg_applied(leg):
                    snap = leg.snapshot if leg.HasField("snapshot") else None
                    try:
                        transfer_bundle(self.protocol, leg.bundle, snap)
                    except ValueError as exc:
                        raise TradeCorruptContractError(f"leg {leg.role} escrow snapshot invalid: {exc}") from exc

            db_now = await tx.now_ms()
            try:
                self._plan(rec, w, db_now)
            except ValueError as exc:
                raise TradeCorruptContractError(str(exc)) from exc
            await self._market_barrier(tx, rec, w)

            pending_legs = [l for l in w.legs if leg_ready_to_send(w, l)]
            pending_legs.sort(key=lambda l: (int(l.player_id), int(l.stream), str(l.role)))
            if len(pending_legs) > len(pre_op_ids):
                raise TradeCorruptContractError("more than 8 new legs in one step")
            now_proc = self.now_ms()
            for idx, leg in enumerate(pending_legs):
                allocs = await tx.allocate_seqs(now_proc, [(int(leg.player_id), int(leg.stream))])
                seq_num, epoch = allocs[(int(leg.player_id), int(leg.stream))]
                raw_bundle = leg.bundle.SerializeToString(deterministic=True)
                op_kind = OP_KIND_ESCROW_DEBIT if int(leg.stream) == ASSET_STREAM_TRADE_DEBIT else OP_KIND_DELIVER_CREDIT
                tx_type = leg_tx_type(leg)
                leg.op_id = pre_op_ids[idx]
                deadline = 0
                if int(leg.stream) == ASSET_STREAM_TRADE_DEBIT:
                    deadline = db_now if w.cancel_requested else int(rec.expires_ms)
                op_rec = self.protocol.message(
                    "trade.TradeAssetOpRecord",
                    op_id=int(leg.op_id),
                    player_id=int(leg.player_id),
                    stream=int(leg.stream),
                    stream_epoch=epoch,
                    seq=seq_num,
                    kind=op_kind,
                    status=OP_STATUS_PENDING,
                    next_attempt_ms=now_proc,
                    deadline_ms=deadline,
                    tx_type=tx_type,
                    ref_kind=OP_REF_KIND_ORDER,
                    ref_id=int(rec.commerce_id),
                    payload=raw_bundle,
                    created_ms=now_proc,
                    updated_ms=now_proc,
                )
                await tx.insert_asset_op(op_rec)

            if w.stage in ("done", "official_reversed", "rmb_delivered", "rmb_returned"):
                await tx.mark_commerce_ops_consumed(int(rec.commerce_id))

            if (
                rec.SerializeToString(deterministic=True) == locked_rec.SerializeToString(deterministic=True)
                and w.SerializeToString(deterministic=True) == locked_w.SerializeToString(deterministic=True)
            ):
                await tx.reschedule_commerce(int(rec.commerce_id), int(rec.version), _next_due_ms(rec, w, now_proc))
                return
            parents_to_wake = list(_filled_parents_to_wake(locked_rec, rec))
            bid_to_wake = _finished_lot_bid(locked_rec, rec, w)
            await tx.save_commerce(rec, w, now_proc)

        await self.store.run_role_tx(_tx)
        now = self.now_ms()
        for parent_id in parents_to_wake:
            await self.store.wake_commerce(parent_id, now)
        if bid_to_wake:
            await self.store.wake_commerce(bid_to_wake, now)

    async def _match_market(self, taker_id: int) -> None:
        taker = await self.store.get_commerce(taker_id)
        if (
            taker is None
            or int(taker.kind) != COMMERCE_KIND_MARKET_MATCH
            or int(taker.side) == 0
            or not taker.on_book
            or int(taker.filled_quantity) >= int(taker.quantity)
        ):
            return
        observed_id = await self.store.find_market_counterparty(taker)
        if observed_id is None:
            return
        observed = await self.store.get_commerce(observed_id)
        if observed is None:
            return
        match_id = await self._commerce_ids()
        market_hash = hashlib.sha256(
            f"market:{taker.market_id}:{taker.item_config_id}:{taker.currency_type}".encode("utf-8")
        ).digest()
        guard_id = struct.unpack(">Q", market_hash[:8])[0] | (1 << 63)

        async def _tx(tx: TradeTx) -> None:
            await self._lock_roles(tx, None, 0, int(taker.seller_player_id), int(observed.seller_player_id))
            await tx.lock_guard(guard_id)
            other_id = await tx.find_market_counterparty(taker)
            if other_id is None:
                return
            if other_id != observed_id:
                raise TradeStaleObservationError("best market counterparty changed")
            first, second = (taker_id, other_id) if taker_id < other_id else (other_id, taker_id)
            a = await tx.lock_commerce(first)
            b = await tx.lock_commerce(second)
            if a is None or b is None:
                return
            other = a if int(a.commerce_id) == other_id else b
            try:
                ow = decode_workflow(self.protocol, other)
                await self._check_contract_identities(tx, other, ow)
            except (TradeCorruptContractError, TradeRoleConflictError) as defect:
                await self._quarantine_locked_tx(tx, other, MANUAL_REASON_BAD_COUNTERPARTY, str(defect))
                return
            aw = decode_workflow(self.protocol, a)
            bw = decode_workflow(self.protocol, b)
            await self._check_contract_identities(tx, a, aw)
            await self._check_contract_identities(tx, b, bw)
            if (
                aw.stage != "market_open"
                or bw.stage != "market_open"
                or aw.cancel_requested
                or bw.cancel_requested
                or int(a.filled_quantity) >= int(a.quantity)
                or int(b.filled_quantity) >= int(b.quantity)
            ):
                return
            buy, sell, buy_w, sell_w = (a, b, aw, bw) if int(a.side) == 1 else (b, a, bw, aw)
            if int(buy.unit_price) < int(sell.unit_price) or int(buy.seller_player_id) == int(sell.seller_player_id):
                return
            maker = a
            if int(b.created_ms) < int(a.created_ms) or (
                int(b.created_ms) == int(a.created_ms) and int(b.commerce_id) < int(a.commerce_id)
            ):
                maker = b
            price = int(maker.unit_price)
            qty = min(int(buy.quantity) - int(buy.filled_quantity), int(sell.quantity) - int(sell.filled_quantity))
            now = self.now_ms()
            fill = self.protocol.message(
                "trade.TradeCommerceRecord",
                commerce_id=match_id,
                request_key=f"{market_hash.hex()}:{match_id}",
                kind=COMMERCE_KIND_MARKET_MATCH,
                status=COMMERCE_DELIVERING,
                seller_player_id=int(sell.seller_player_id),
                buyer_player_id=int(buy.seller_player_id),
                market_id=int(buy.market_id),
                item_config_id=int(buy.item_config_id),
                unit_price=price,
                quantity=qty,
                currency_type=int(buy.currency_type),
                created_ms=now,
                updated_ms=now,
                version=1,
                parent_id=int(buy.commerce_id),
                counterparty_id=int(sell.commerce_id),
            )
            mw = self.protocol.message("trade.TradeCommerceWorkflow", stage="deliver")
            if not buy_w.HasField("seller_identity") or not sell_w.HasField("seller_identity"):
                raise TradeCorruptContractError("market parent order missing seller_identity")
            mw.seller_identity.CopyFrom(sell_w.seller_identity)
            mw.buyer_identity.CopyFrom(buy_w.seller_identity)
            sell_debit = find_leg(sell_w, "seller_debit")
            buy_debit = find_leg(buy_w, "seller_debit")
            if sell_debit is None or buy_debit is None:
                raise TradeCorruptContractError("market parent order missing seller_debit leg")

            item_b = self.protocol.message("AssetBundle")
            item_g = item_b.items.add()
            item_g.config_id = int(buy.item_config_id)
            item_g.count = qty
            add_escrow_leg(mw, "buyer_receive", int(buy.seller_player_id), int(sell_debit.player_id), item_b)

            pay_b = self.protocol.message("AssetBundle")
            pay_c = pay_b.currencies.add()
            pay_c.currency_type = int(buy.currency_type)
            pay_c.amount = qty * price
            add_escrow_leg(mw, "seller_receive", int(sell.seller_player_id), int(buy_debit.player_id), pay_b)

            if int(buy.unit_price) > price:
                sur_b = self.protocol.message("AssetBundle")
                sur_c = sur_b.currencies.add()
                sur_c.currency_type = int(buy.currency_type)
                sur_c.amount = qty * (int(buy.unit_price) - price)
                add_escrow_leg(mw, "buyer_surplus", int(buy.seller_player_id), int(buy_debit.player_id), sur_b)

            fill.workflow_proto = mw.SerializeToString(deterministic=True)
            await tx.insert_commerce(fill)
            a.filled_quantity = int(a.filled_quantity) + qty
            b.filled_quantity = int(b.filled_quantity) + qty
            if int(a.filled_quantity) == int(a.quantity):
                a.status = COMMERCE_DELIVERING
            if int(b.filled_quantity) == int(b.quantity):
                b.status = COMMERCE_DELIVERING
            await tx.save_commerce(a, aw, now)
            await tx.save_commerce(b, bw, now)

        await self.store.run_role_tx(_tx)

    async def _accept_bid(self, bid_id: int) -> None:
        candidate = await self.store.get_commerce(bid_id)
        if candidate is None:
            return
        parent = await self.store.get_commerce(int(candidate.parent_id))
        parent_seller = int(parent.seller_player_id) if parent is not None else 0
        parent_buyer = int(parent.buyer_player_id) if parent is not None else 0

        async def _tx(tx: TradeTx) -> None:
            await self._lock_roles(tx, None, 0, int(candidate.seller_player_id), parent_seller, parent_buyer)
            await tx.lock_guard(int(candidate.parent_id))
            lot = await tx.lock_commerce(int(candidate.parent_id))
            lw = None
            if lot is not None:
                if int(lot.seller_player_id) != parent_seller or int(lot.buyer_player_id) != parent_buyer:
                    raise TradeStaleObservationError("lot participants changed")
                try:
                    lw = decode_workflow(self.protocol, lot)
                except TradeCorruptContractError:
                    lw = None
            bid = await tx.lock_commerce(bid_id)
            if bid is None:
                return
            bw = decode_workflow(self.protocol, bid)
            if bw.stage != "bid_ready":
                return
            await self._check_contract_identities(tx, bid, bw)
            if lw is not None:
                try:
                    await self._check_contract_identities(tx, lot, lw)
                except (TradeCorruptContractError, TradeRoleConflictError):
                    lw = None
            db_now = await tx.now_ms()
            eligible = (
                lw is not None
                and lw.stage == "lot_open"
                and not lw.cancel_requested
                and int(lot.expires_ms) > db_now
                and int(bid.unit_price) >= int(lot.unit_price)
                and (
                    int(lw.highest_bid) == 0
                    or (
                        int(lw.minimum_increment) <= INT64_MAX - int(lw.highest_bid)
                        and int(bid.unit_price) >= int(lw.highest_bid) + int(lw.minimum_increment)
                    )
                )
            )
            save_ms = self.now_ms()
            if not eligible:
                bw.stage = "bid_refund"
                bid.status = COMMERCE_CANCEL_PENDING
                await tx.save_commerce(bid, bw, save_ms)
                return
            if int(lw.winning_bid_id) != 0:
                old_bid = await tx.lock_commerce(int(lw.winning_bid_id))
                if old_bid is None:
                    raise RuntimeError("previous winning bid missing")
                ow = decode_workflow(self.protocol, old_bid)
                if ow.stage != "bid_held":
                    raise RuntimeError("previous winning bid not in bid_held")
                ow.stage = "bid_refund"
                old_bid.status = COMMERCE_CANCEL_PENDING
                await tx.save_commerce(old_bid, ow, save_ms)
            if not bw.HasField("seller_identity"):
                raise TradeCorruptContractError("bid missing seller_identity")
            lw.highest_bid = int(bid.unit_price)
            lw.highest_bidder = int(bid.seller_player_id)
            lw.winning_bid_id = int(bid.commerce_id)
            lw.buyer_assets.CopyFrom(bw.seller_assets)
            lot.buyer_player_id = int(bid.seller_player_id)
            lw.buyer_identity.CopyFrom(bw.seller_identity)
            bw.stage = "bid_held"
            await tx.save_commerce(lot, lw, save_ms)
            await tx.save_commerce(bid, bw, save_ms)

        await self.store.run_role_tx(_tx)

    async def _settle_held_bid(self, bid: Message) -> None:
        parent = await self.store.get_commerce(int(bid.parent_id))
        if parent is None:
            raise TradeCorruptContractError(f"held bid {bid.commerce_id} parent lot {bid.parent_id} missing")
        if int(parent.status) == COMMERCE_CANCELLED:
            lw = decode_workflow(self.protocol, parent)
            if not held_bid_keeps_funds(lw, int(bid.commerce_id)):
                raise TradeCorruptContractError(f"cancelled lot {parent.commerce_id} already released funds")
        elif int(parent.status) != COMMERCE_SETTLED:
            now = self.now_ms()
            until = min(max(int(parent.expires_ms), now + _BID_HELD_RECHECK_MS), now + _BID_HELD_MAX_WAIT_MS)
            await self.store.defer_commerce(int(bid.commerce_id), int(bid.version), until)
            return

        async def _tx(tx: TradeTx) -> None:
            cur = await tx.lock_commerce(int(bid.commerce_id))
            if cur is None:
                return
            cw = decode_workflow(self.protocol, cur)
            if cw.stage != "bid_held":
                return
            if int(parent.status) == COMMERCE_CANCELLED:
                cw.stage = "bid_refund"
                cur.status = COMMERCE_CANCEL_PENDING
                await tx.save_commerce(cur, cw, self.now_ms())
                return
            cur.status = COMMERCE_SETTLED
            cw.stage = "done"
            await tx.mark_commerce_ops_consumed(int(cur.commerce_id))
            await tx.save_commerce(cur, cw, self.now_ms())

        await self.store.run_tx(_tx)

    async def _quarantine_locked_tx(self, tx: TradeTx, rec: Message, reason: str, detail: str) -> None:
        now = self.now_ms()
        await tx.expire_commerce_escrow_debits(int(rec.commerce_id), now)
        try:
            w = decode_workflow(self.protocol, rec)
        except TradeCorruptContractError:
            await tx.save_commerce_status(rec, COMMERCE_MANUAL, now)
            return
        mark_manual(rec, w, reason, detail, now)
        await tx.save_commerce(rec, w, now)

    async def _quarantine(self, rec: Message, reason: str, detail: str) -> bool:
        async def _tx(tx: TradeTx) -> bool:
            cur = await tx.lock_commerce(int(rec.commerce_id))
            if cur is None or int(cur.version) != int(rec.version) or int(cur.status) == COMMERCE_MANUAL:
                return False
            await self._quarantine_locked_tx(tx, cur, reason, detail)
            return True

        return await self.store.run_tx(_tx)

    async def _try_quarantine_owner_changed(self, rec: Message) -> bool:
        if int(rec.kind) == COMMERCE_KIND_RMB_CONSIGNMENT:
            return False
        try:
            w = decode_workflow(self.protocol, rec)
        except TradeCorruptContractError:
            return False

        async def _tx(tx: TradeTx) -> bool:
            changes: list[str] = []
            for party_name, pid, ident in _contract_parties(rec, w):
                if ident is None:
                    continue
                fence = await tx.lock_role_fence(pid)
                if fence is None:
                    continue
                if fence.account == "":
                    changes.append(f"{party_name} player={pid} role deleted")
                elif _owner_changed(fence, ident):
                    changes.append(
                        f"{party_name} player={pid} owner_revision={fence.owner_revision} (contract {ident.owner_revision})"
                    )
            if not changes:
                return False
            cur = await tx.lock_commerce(int(rec.commerce_id))
            if cur is None or int(cur.version) != int(rec.version) or int(cur.status) == COMMERCE_MANUAL:
                return False
            await self._quarantine_locked_tx(tx, cur, MANUAL_REASON_OWNER_CHANGED, "; ".join(changes))
            return True

        return await self.store.run_tx(_tx)

    async def _step(self, rec: Message) -> None:
        if int(rec.kind) == COMMERCE_KIND_RMB_CONSIGNMENT:
            if self.rmb is None:
                raise TradeCorruptContractError("rmb consignment coordinator not wired")
            await self.rmb.step(rec)
            return
        w = decode_workflow(self.protocol, rec)
        if w.stage == "bid_ready":
            await self._accept_bid(int(rec.commerce_id))
            return
        if w.stage == "bid_held":
            await self._settle_held_bid(rec)
            return
        await self._advance(rec)
        if int(rec.kind) == COMMERCE_KIND_MARKET_MATCH and int(rec.side) != 0:
            try:
                await self._match_market(int(rec.commerce_id))
            except Exception as exc:
                logger.info("trade market match deferred taker_id=%d err=%s", rec.commerce_id, exc)
                await self.store.wake_commerce(int(rec.commerce_id), self.now_ms())

    async def _defer_failure(self, rec: Message, exc: Exception) -> None:
        if isinstance(exc, TradeStaleObservationError):
            return
        if isinstance(exc, TradeCorruptContractError):
            if await self._quarantine(rec, MANUAL_REASON_CORRUPT, str(exc)):
                logger.error("trade contract quarantined as manual commerce_id=%d err=%s", rec.commerce_id, exc)
                return
        if isinstance(exc, TradeRoleConflictError):
            if await self._try_quarantine_owner_changed(rec):
                logger.error("trade contract quarantined owner_changed commerce_id=%d", rec.commerce_id)
                return
            base = _TICK_ROLE_CONFLICT_BACKOFF_MS
        elif isinstance(exc, TradeTooManyPendingError):
            base = _TICK_PENDING_BACKOFF_MS
        else:
            base = _TICK_RETRY_BACKOFF_MS
        until = self.now_ms() + int(base * (0.8 + 0.4 * random.random()))
        await self.store.defer_commerce(int(rec.commerce_id), int(rec.version), until)

    async def ack_receipts(self) -> int:
        """分页轮转确认已提交托管快照的 Scene 收据释放（AckReceipts）。"""
        if self._asset_caller is None or self._ledger_reader is None:
            return 0
        ops = await self.store.list_ack_due_ops(self._ack_after_op_id, _TICK_ACK_LIMIT)
        acked = 0
        for item in ops:
            if isinstance(item, tuple):
                self._ack_after_op_id = item[0]
                logger.error("trade ack_receipt skipped poison op_id=%d", item[0])
                continue
            try:
                done = await acknowledge_receipt(self.protocol, self._asset_caller, self._ledger_reader, item)
                if done:
                    await self.store.mark_op_acked(item.op_id)
                    acked += 1
            except Exception as exc:
                logger.error("trade ack_receipt failed op_id=%d err=%s", item.op_id, exc)
            self._ack_after_op_id = item.op_id
        if len(ops) < _TICK_ACK_LIMIT:
            self._ack_after_op_id = 0
        return acked

    async def tick(self) -> int:
        """推进一轮到期合同并轮转收据 ACK（单行隔离，不因单行故障阻断全服）。"""
        rows = await self.store.list_due_commerce(self.now_ms(), _TICK_DUE_LIMIT)
        processed = 0
        for rec in rows:
            try:
                await self._step(rec)
                processed += 1
            except Exception as exc:
                logger.warning("trade commerce step failed commerce_id=%d err=%s", rec.commerce_id, exc)
                try:
                    await self._defer_failure(rec, exc)
                except Exception:
                    logger.error("trade defer failure error commerce_id=%d", rec.commerce_id, exc_info=True)
        await self.ack_receipts()
        return processed

    # -- 人工处置（ShowManual / ResolveManual，供 TradeAdmin 调用） ----------------------------------------

    async def show_manual(self, commerce_id: int) -> dict:
        rec = await self.store.get_commerce(commerce_id)
        if rec is None:
            raise ManualNotFoundError(f"commerce {commerce_id} not found")
        report: dict = {"record": rec, "linked": None, "legs": [], "parties": [], "actions": [], "decode_err": ""}
        try:
            w = decode_workflow(self.protocol, rec)
        except TradeCorruptContractError as exc:
            report["decode_err"] = str(exc)
            return report
        for l in w.legs:
            out = await self.store.read_leg_outcome(int(l.op_id)) if int(l.op_id) != 0 else None
            report["legs"].append({
                "leg": l,
                "op_status": out.status if out is not None else 0,
                "final_ms": out.final_ms if out is not None else 0,
                "read_failed": int(l.op_id) != 0 and out is None,
            })
        for party_name, pid, ident in _contract_parties(rec, w):
            fence = await self.store.get_role_fence_local(pid)
            deleted = fence is not None and fence.account == ""
            unknown = fence is None
            changed = deleted or (fence is not None and ident is not None and _owner_changed(fence, ident))
            report["parties"].append({
                "party": party_name,
                "player_id": pid,
                "contract_owner_revision": int(ident.owner_revision) if ident is not None else 0,
                "current_owner_revision": 0 if (deleted or unknown) else fence.owner_revision,
                "same_account": bool(fence is not None and not deleted and ident is not None and fence.account == ident.account),
                "deleted": deleted,
                "frozen": bool(fence is not None and fence.freeze_token != 0),
                "unknown": unknown,
                "changed": changed,
            })
        linked_id = int(rec.parent_id) if int(rec.kind) == COMMERCE_KIND_BID else (int(w.winning_bid_id) if int(rec.kind) == COMMERCE_KIND_AUCTION_LOT else 0)
        if linked_id:
            report["linked"] = await self.store.get_commerce(linked_id)
        for action in MANUAL_ACTIONS:
            if action == "partial":
                partial_legs = [
                    item["leg"].role
                    for item in report["legs"]
                    if not item["leg"].superseded_by
                    and (
                        int(item["leg"].outcome) == OP_STATUS_APPLIED_PARTIAL
                        or (int(item["leg"].outcome) == 0 and item["op_status"] == OP_STATUS_APPLIED_PARTIAL)
                    )
                ]
                report["actions"].append((action, bool(partial_legs), f"partial legs: {','.join(partial_legs)}" if partial_legs else "no partial legs"))
            elif action == "redirect":
                changed_p = [f"{p['party']}={p['player_id']}" for p in report["parties"] if p["changed"]]
                report["actions"].append((action, bool(changed_p), f"changed: {','.join(changed_p)}" if changed_p else "no changed party"))
            else:
                linked = report["linked"]
                if action == "void-auction" and (int(rec.kind) != COMMERCE_KIND_AUCTION_LOT or int(w.winning_bid_id) == 0 or linked is None):
                    report["actions"].append((action, False, "not an auction lot with bid"))
                    continue
                req = ManualRequest(
                    commerce_id=int(rec.commerce_id),
                    expect_version=int(rec.version),
                    action=action,
                    operator="show",
                    reason="show",
                    linked_commerce_id=int(linked.commerce_id) if action == "void-auction" and linked is not None else 0,
                    linked_version=int(linked.version) if action == "void-auction" and linked is not None else 0,
                )
                try:
                    res = await self.resolve_manual(req, execute=False)
                    report["actions"].append((action, True, f"stage={res.stage} status={res.status}"))
                except Exception as exc:
                    report["actions"].append((action, False, str(exc)))
        return report

    async def resolve_manual(self, req: ManualRequest, *, execute: bool) -> ManualResult:
        _validate_manual_request(req)
        pre = await self.store.get_commerce(req.commerce_id)
        if pre is None:
            raise ManualNotFoundError(f"commerce {req.commerce_id} not found")
        if int(pre.version) != req.expect_version:
            raise ManualVersionChangedError(f"commerce {req.commerce_id} version={pre.version} expected={req.expect_version}")

        players = [int(pre.seller_player_id), int(pre.buyer_player_id)]
        lot_id = 0
        if int(pre.kind) == COMMERCE_KIND_BID:
            lot_id = int(pre.parent_id)
        elif req.action == "void-auction" and int(pre.kind) == COMMERCE_KIND_AUCTION_LOT:
            lot_id = int(pre.commerce_id)
            bid_pre = await self.store.get_commerce(req.linked_commerce_id)
            if bid_pre is None:
                raise ManualNotAllowedError(f"linked bid {req.linked_commerce_id} not found")
            if int(bid_pre.version) != req.linked_version:
                raise ManualVersionChangedError(f"linked bid version={bid_pre.version} expected={req.linked_version}")
            players.extend((int(bid_pre.seller_player_id), int(bid_pre.buyer_player_id)))
        if req.action == "redirect":
            players.append(req.to_player_id)

        now_ms = self.now_ms()

        async def _tx(tx: TradeTx) -> ManualResult:
            facts: dict[int, _RoleFact] = {}
            missing_pids: list[int] = []
            for pid in sorted({p for p in players if p != 0}):
                fence = await tx.lock_role_fence(pid)
                if fence is None:
                    missing_pids.append(pid)
                    facts[pid] = _RoleFact(unknown=True)
                elif fence.account == "":
                    facts[pid] = _RoleFact(deleted=True)
                else:
                    facts[pid] = _RoleFact(fence=fence)
            if missing_pids:
                raise RoleFenceStaleError(tuple(missing_pids), reject=ManualNotAllowedError("missing role fence"))

            target_fence: RoleFenceRow | None = None
            if req.action == "redirect":
                tf = facts.get(req.to_player_id)
                if tf is None or tf.deleted or tf.fence is None:
                    raise ManualNotAllowedError(f"redirect target {req.to_player_id} unavailable or deleted")
                if tf.fence.freeze_token != 0:
                    raise RoleFenceStaleError(req.to_player_id, reject=ManualNotAllowedError("redirect target frozen"))
                target_fence = tf.fence

            lot = None
            if lot_id != 0:
                await tx.lock_guard(lot_id)
                lot = await tx.lock_commerce(lot_id)

            rec = lot if lot_id == req.commerce_id else await tx.lock_commerce(req.commerce_id)
            if rec is None:
                raise ManualNotFoundError(f"commerce {req.commerce_id} not found")
            if int(rec.version) != req.expect_version:
                raise ManualVersionChangedError(f"commerce version={rec.version} expected={req.expect_version}")
            try:
                w = decode_workflow(self.protocol, rec)
            except TradeCorruptContractError as exc:
                raise ManualNotAllowedError(f"workflow undecodable: {exc}") from exc

            bid = None
            bw = None
            if req.action == "void-auction" and int(rec.kind) == COMMERCE_KIND_AUCTION_LOT:
                bid = await tx.lock_commerce(req.linked_commerce_id)
                if bid is None:
                    raise ManualNotAllowedError(f"linked bid {req.linked_commerce_id} not found")
                if int(bid.version) != req.linked_version:
                    raise ManualVersionChangedError(f"linked bid version={bid.version} expected={req.linked_version}")
                bw = decode_workflow(self.protocol, bid)
                await self._settle_manual_legs(tx, bw, partial_legs=set(), unissued_ok=False)

            partial_names = {p.leg for p in req.parts} if req.action == "partial" else set()
            await self._settle_manual_legs(tx, w, partial_legs=partial_names, unissued_ok=(req.action == "redirect"))

            lot_w = None
            if int(rec.kind) == COMMERCE_KIND_BID and lot is not None:
                try:
                    lot_w = decode_workflow(self.protocol, lot)
                except TradeCorruptContractError:
                    lot_w = None

            version_before = int(rec.version)
            res = self._plan_manual(rec, w, req, facts, target_fence, lot, lot_w, bid, bw)
            res.version_before = version_before
            res.audit = self._build_manual_audit(req, res, now_ms)
            if bid is not None and res.linked is not None:
                res.linked.version_before = req.linked_version
                res.linked.audit = self.protocol.message(
                    "trade.TradeCommerceManualAction",
                    action=req.action,
                    operator=req.operator,
                    reason=req.reason,
                    at_ms=now_ms,
                    version_before=req.linked_version,
                    linked_commerce_id=int(rec.commerce_id),
                )
                if res.linked.quarantine is not None:
                    res.linked.audit.quarantine.CopyFrom(res.linked.quarantine)

            if execute:
                w.manual_actions.add().CopyFrom(res.audit)
                await tx.save_commerce(rec, w, now_ms)
                if bid is not None and bw is not None and res.linked is not None and res.linked.audit is not None:
                    bw.manual_actions.add().CopyFrom(res.linked.audit)
                    await tx.save_commerce(bid, bw, now_ms)
            return res

        try:
            return await self.store.run_role_tx(_tx)
        except RoleNotFoundError as exc:
            raise ManualNotAllowedError(f"role not found: {exc}") from exc

    async def _settle_manual_legs(self, tx: TradeTx, w: Message, *, partial_legs: set[str], unissued_ok: bool) -> None:
        for l in w.legs:
            if l.superseded_by or int(l.outcome) != 0:
                continue
            if int(l.op_id) == 0:
                if unissued_ok or is_seller_proceeds(l):
                    continue
                raise ManualNotAllowedError(f"leg {l.role} not issued yet")
            out = await tx.read_leg_outcome(int(l.op_id))
            if out is None:
                raise ManualNotAllowedError(f"leg {l.role} op_id={l.op_id} missing")
            if out.is_pending:
                raise ManualNotAllowedError(f"leg {l.role} op_id={l.op_id} still pending")
            if out.status == OP_STATUS_APPLIED_PARTIAL and l.role not in partial_legs:
                raise ManualNotAllowedError(f"leg {l.role} is partial; use partial action")
            try:
                record_leg_outcome(self.protocol, l, out)
            except ValueError as exc:
                raise ManualNotAllowedError(str(exc)) from exc

    def _plan_manual(
        self,
        rec: Message,
        w: Message,
        req: ManualRequest,
        facts: dict[int, _RoleFact],
        target_fence: RoleFenceRow | None,
        lot: Message | None,
        lot_w: Message | None,
        bid: Message | None,
        bw: Message | None,
    ) -> ManualResult:
        if int(rec.status) != COMMERCE_MANUAL or w.stage != "manual":
            raise ManualNotAllowedError(f"contract not in manual state (status={rec.status} stage={w.stage!r})")
        if not w.HasField("quarantine") or not w.quarantine.stage:
            raise ManualNotAllowedError("contract missing quarantine record")
        q = w.quarantine
        changed_parties = [
            (name, pid, ident)
            for name, pid, ident in _contract_parties(rec, w)
            if ident is not None
            and pid in facts
            and (facts[pid].deleted or (facts[pid].fence is not None and _owner_changed(facts[pid].fence, ident)))
        ]
        if req.action != "redirect" and changed_parties:
            raise ManualNotAllowedError("contract party changed owner; redirect first")

        q_copy = self.protocol.message("trade.TradeCommerceQuarantine")
        q_copy.CopyFrom(q)
        res = ManualResult(quarantine=q_copy)

        if req.action == "resume":
            if _reissuable_quarantine(q):
                raise ManualNotAllowedError("failed delivery or reversal leg; use reissue or refund")
            w.stage = q.stage
            rec.status = int(q.status)

        elif req.action == "reissue":
            if not _reissuable_quarantine(q):
                raise ManualNotAllowedError(f"reissue not allowed for reason={q.reason}")
            if q.reason == MANUAL_REASON_PAYMENT_REVERSAL:
                failed = _failed_official_reversal_legs(w)
            else:
                failed = _failed_credits(w)
            if not failed:
                raise ManualNotAllowedError("no failed credit or reversal legs to reissue")
            for l in failed:
                new_role = _reissue_role(w, l.role)
                l.superseded_by = new_role
                b_copy = self.protocol.message("AssetBundle")
                b_copy.CopyFrom(l.bundle)
                if q.reason == MANUAL_REASON_PAYMENT_REVERSAL:
                    add_leg(w, new_role, int(l.player_id), int(l.stream), b_copy)
                else:
                    add_escrow_leg(w, new_role, int(l.player_id), int(l.escrow_owner_id), b_copy)
                res.new_legs.append(new_role)
                res.superseded_legs.append(l.role)
            w.stage = q.stage
            rec.status = int(q.status)

        elif req.action == "refund":
            self._check_refund_allowed(rec, w, lot, lot_w)
            returns = self._compute_refund_returns(rec, w)
            self._apply_refund(rec, w, returns, res)

        elif req.action == "partial":
            if int(rec.kind) in (COMMERCE_KIND_OFFICIAL, COMMERCE_KIND_RMB_CONSIGNMENT):
                raise ManualNotAllowedError("partial not supported for official/rmb contracts")
            named = set()
            credit_fixes = []
            for part in req.parts:
                l = find_leg(w, part.leg)
                if l is None or l.superseded_by or int(l.outcome) != OP_STATUS_APPLIED_PARTIAL:
                    raise ManualNotAllowedError(f"leg {part.leg} is not an active partial leg")
                named.add(l.role)
                if int(l.stream) != ASSET_STREAM_TRADE_CREDIT:
                    raise ManualNotAllowedError(f"partial debit recovery not automated for {l.role}")
                shortfall = _credit_shortfall(self.protocol, l.bundle, part.missing)
                credit_fixes.append((l, shortfall))
                pp = self.protocol.message("trade.TradeCommercePartialPart", leg=l.role)
                pp.missing.CopyFrom(shortfall)
                res.partial_parts.append(pp)
            for l in w.legs:
                if not l.superseded_by and int(l.outcome) == OP_STATUS_APPLIED_PARTIAL and l.role not in named:
                    raise ManualNotAllowedError(f"all partial legs must be included; missing {l.role}")
            for l, shortfall in credit_fixes:
                new_role = _reissue_role(w, l.role)
                l.superseded_by = new_role
                add_escrow_leg(w, new_role, int(l.player_id), int(l.escrow_owner_id), shortfall)
                res.new_legs.append(new_role)
                res.superseded_legs.append(l.role)
            w.stage = q.stage
            rec.status = int(q.status)

        elif req.action == "redirect":
            if int(rec.kind) in (COMMERCE_KIND_OFFICIAL, COMMERCE_KIND_RMB_CONSIGNMENT):
                raise ManualNotAllowedError("redirect not allowed for official/rmb contracts")
            match = [p for p in changed_parties if p[0] == req.party]
            if not match:
                raise ManualNotAllowedError(f"party {req.party} has not changed owner")
            party_name, old_pid, old_ident = match[0]
            if target_fence is None or target_fence.account != old_ident.account:
                raise ManualNotAllowedError("redirect target must belong to original account")
            for other_name, other_pid, _ in _contract_parties(rec, w):
                if other_name != party_name and other_pid == req.to_player_id:
                    raise ManualNotAllowedError("redirect target cannot be the counterparty")
            after_ident = self.protocol.message(
                "trade.TradeRoleIdentity", account=old_ident.account, owner_revision=target_fence.owner_revision
            )
            before_ident = self.protocol.message("trade.TradeRoleIdentity")
            before_ident.CopyFrom(old_ident)
            res.party = party_name
            res.from_player = old_pid
            res.to_player = req.to_player_id
            res.identity_before = before_ident
            res.identity_after = after_ident
            if party_name == "seller":
                rec.seller_player_id = req.to_player_id
                w.seller_identity.CopyFrom(after_ident)
            else:
                rec.buyer_player_id = req.to_player_id
                w.buyer_identity.CopyFrom(after_ident)
                if int(rec.kind) == COMMERCE_KIND_AUCTION_LOT and int(w.highest_bidder) == old_pid:
                    w.highest_bidder = req.to_player_id
            if req.to_player_id != old_pid:
                for l in list(w.legs):
                    if l.superseded_by or int(l.player_id) != old_pid:
                        continue
                    if int(l.op_id) == 0:
                        l.player_id = req.to_player_id
                        res.retargeted_legs.append(l.role)
                    elif int(l.stream) == ASSET_STREAM_TRADE_CREDIT and leg_done(l) and not leg_applied(l) and int(l.outcome) != OP_STATUS_APPLIED_PARTIAL:
                        new_role = _reissue_role(w, l.role)
                        l.superseded_by = new_role
                        b_copy = self.protocol.message("AssetBundle")
                        b_copy.CopyFrom(l.bundle)
                        add_escrow_leg(w, new_role, req.to_player_id, int(l.escrow_owner_id), b_copy)
                        res.new_legs.append(new_role)
                        res.superseded_legs.append(l.role)
            if len(changed_parties) == 1:
                w.stage = q.stage
                rec.status = int(q.status)

        elif req.action == "void-auction":
            if int(rec.kind) != COMMERCE_KIND_AUCTION_LOT or int(w.highest_bidder) == 0 or int(w.winning_bid_id) == 0:
                raise ManualNotAllowedError("void-auction requires auction lot with winning bid")
            if bid is None or bw is None or int(w.winning_bid_id) != int(bid.commerce_id):
                raise ManualNotAllowedError("linked bid mismatch")
            held = bw.stage == "bid_held" or (
                int(bid.status) == COMMERCE_MANUAL and bw.stage == "manual" and bw.quarantine.stage == "bid_held"
            )
            if not held or not leg_applied(find_leg(bw, "bid_debit")):
                raise ManualNotAllowedError("linked bid not in refundable bid_held state")
            self._check_refund_common(w)
            s_debit = find_leg(w, "seller_debit")
            if not leg_applied(s_debit):
                raise ManualNotAllowedError("auction lot seller_debit not applied")
            snap = s_debit.snapshot if s_debit.HasField("snapshot") else None
            back = transfer_bundle(self.protocol, s_debit.bundle, snap)
            self._apply_refund(
                rec,
                w,
                [(f"{s_debit.role}/refund", _refund_recipient(rec, s_debit), int(s_debit.player_id), back)],
                res,
            )
            bid_q = None
            if int(bid.status) == COMMERCE_MANUAL and bw.HasField("quarantine"):
                bid_q = self.protocol.message("trade.TradeCommerceQuarantine")
                bid_q.CopyFrom(bw.quarantine)
            bw.stage = "bid_refund"
            bid.status = COMMERCE_CANCEL_PENDING
            res.linked_id = int(bid.commerce_id)
            res.linked = ManualResult(quarantine=bid_q, stage=bw.stage, status=int(bid.status))

        if len(res.new_legs) > MAX_NEW_LEGS_PER_STEP:
            raise ManualNotAllowedError("too many new legs")
        res.stage = w.stage
        res.status = int(rec.status)
        return res

    def _check_refund_common(self, w: Message) -> None:
        for l in w.legs:
            if int(l.stream) == ASSET_STREAM_TRADE_CREDIT and int(l.outcome) == OP_STATUS_APPLIED_PARTIAL:
                raise ManualNotAllowedError(f"credit leg {l.role} partially applied; use reissue")
            if int(l.stream) != ASSET_STREAM_TRADE_CREDIT or l.superseded_by:
                continue
            if leg_applied(l):
                raise ManualNotAllowedError(f"credit leg {l.role} already applied; use reissue")
            if _is_refund_leg(l.role):
                raise ManualNotAllowedError(f"contract already refunded ({l.role}); use reissue")

    def _check_refund_allowed(self, rec: Message, w: Message, lot: Message | None, lot_w: Message | None) -> None:
        kind = int(rec.kind)
        if kind in (COMMERCE_KIND_OFFICIAL, COMMERCE_KIND_RMB_CONSIGNMENT):
            raise ManualNotAllowedError("official/rmb contracts have dedicated refund flows")
        if kind == COMMERCE_KIND_BID:
            if lot is not None and (lot_w is None or int(lot_w.winning_bid_id) == int(rec.commerce_id)):
                raise ManualNotAllowedError("winning bid referenced by lot; use void-auction or resume")
        if kind == COMMERCE_KIND_MARKET_MATCH:
            if int(rec.side) == 0:
                raise ManualNotAllowedError("market fill cannot be refunded alone; use reissue")
            if int(rec.filled_quantity) >= int(rec.quantity):
                raise ManualNotAllowedError("market order fully filled; use resume")
        if kind == COMMERCE_KIND_AUCTION_LOT and int(w.highest_bidder) != 0:
            raise ManualNotAllowedError("auction has bids; use void-auction")
        self._check_refund_common(w)

    def _compute_refund_returns(self, rec: Message, w: Message) -> list[tuple[str, int, int, Message]]:
        out: list[tuple[str, int, int, Message]] = []
        for l in w.legs:
            if int(l.stream) != ASSET_STREAM_TRADE_DEBIT or l.superseded_by or not leg_applied(l):
                continue
            if int(rec.kind) == COMMERCE_KIND_MARKET_MATCH and int(rec.side) != 0 and l.role == "seller_debit":
                b = market_remainder(self.protocol, rec)
            else:
                snap = l.snapshot if l.HasField("snapshot") else None
                try:
                    b = transfer_bundle(self.protocol, l.bundle, snap)
                except ValueError as exc:
                    raise ManualNotAllowedError(f"cannot transfer escrow for {l.role}: {exc}") from exc
            if not empty_bundle(b):
                out.append((f"{l.role}/refund", _refund_recipient(rec, l), int(l.player_id), b))
        return out

    def _apply_refund(
        self, rec: Message, w: Message, returns: list[tuple[str, int, int, Message]], res: ManualResult
    ) -> None:
        for l in _failed_credits(w):
            l.superseded_by = "refund"
            res.superseded_legs.append(l.role)
        for l in w.legs:
            if int(l.stream) == ASSET_STREAM_TRADE_CREDIT and not l.superseded_by and int(l.op_id) == 0 and is_seller_proceeds(l):
                l.superseded_by = "refund"
                res.superseded_legs.append(l.role)
        for role, player_id, owner_id, bundle in returns:
            add_escrow_leg(w, role, player_id, owner_id, bundle)
            res.new_legs.append(role)
        w.stage = "return"
        rec.status = COMMERCE_CANCEL_PENDING

    def _build_manual_audit(self, req: ManualRequest, res: ManualResult, now_ms: int) -> Message:
        audit = self.protocol.message(
            "trade.TradeCommerceManualAction",
            action=req.action,
            operator=req.operator,
            reason=req.reason,
            at_ms=now_ms,
            version_before=res.version_before,
            new_legs=res.new_legs,
            superseded_legs=res.superseded_legs,
            party=res.party,
            from_player_id=res.from_player,
            to_player_id=res.to_player,
            retargeted_legs=res.retargeted_legs,
            linked_commerce_id=res.linked_id,
        )
        if res.quarantine is not None:
            audit.quarantine.CopyFrom(res.quarantine)
        if res.identity_before is not None:
            audit.identity_before.CopyFrom(res.identity_before)
        if res.identity_after is not None:
            audit.identity_after.CopyFrom(res.identity_after)
        for pp in res.partial_parts:
            audit.partial_parts.add().CopyFrom(pp)
        return audit
