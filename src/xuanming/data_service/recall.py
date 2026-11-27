"""GM 批量回收计划、账本证据判定与人工终结校验的纯业务逻辑（对齐 Go ``recall_plan.go`` / ``recall_logic.go``）。"""

from dataclasses import dataclass, field
import hashlib
import re

from .model import (
    MAX_UINT64,
    RECALL_KIND_CURRENCY,
    RECALL_KIND_ITEM,
    RECALL_OP_ABORTED,
    RECALL_OP_APPLIED,
    RECALL_OP_CANCELLED,
    RECALL_OP_PARTIAL,
    RECALL_OP_PENDING,
    RECALL_OP_PLANNED,
    RECALL_OP_REJECTED,
    RECALL_OP_ROLE_GONE,
)

BATCH_RECALL_QUERY_LIMIT = 10_000
MAX_RECALL_PLAYERS = 1000
MAX_RECALL_SOURCE_ROWS = BATCH_RECALL_QUERY_LIMIT
MAX_RECALL_HINTS_PER_OP = 64
MAX_RECALL_OPS_PER_PLAYER = 16
RECALL_OPERATOR_MAX_RUNES = 64
RECALL_REASON_MAX_RUNES = 191
RECALL_AUDIT_TYPE = 4
RECALL_STREAM_GM_RECALL = 6          # ASSET_OP_STREAM_GM_RECALL
RECALL_TX_TYPE = 9                   # TransactionType.TX_BATCH_RECALL (兜底值，优先从协议枚举读取)
CREDIT_STREAMS = frozenset((2, 4, 5))  # GUILD_CREDIT / TRADE_CREDIT / SYSTEM_CREDIT

# RecallReviewReason 枚举值（data_service.proto）
REVIEW_PROVEN = 0
REVIEW_NO_CORRELATION = 1
REVIEW_NO_PLAYER_STORE = 2
REVIEW_NOT_IN_LEDGER = 3
REVIEW_LEDGER_AMBIGUOUS = 4
REVIEW_LEDGER_REJECTED = 5
REVIEW_LEDGER_PARTIAL = 6
REVIEW_LEDGER_NOT_APPLIED = 7
REVIEW_ROWS_DISAGREE = 8
VALID_REVIEW_REASONS = frozenset((2, 3, 4, 5, 6, 7, 8))

# 账本位图窗口常量（对齐 Go shared/assetop/classify.go）
_WINDOW_BITS = 1024
_WINDOW_WORDS = 16
_MAX_SEQ_JUMP = 1024
_DEFAULT_QUIET_GAP_MS = 60_000

_STATUS_LABELS = {
    RECALL_OP_PENDING: "PENDING",
    RECALL_OP_APPLIED: "APPLIED",
    RECALL_OP_REJECTED: "REJECTED",
    RECALL_OP_ABORTED: "ABORTED",
    RECALL_OP_PARTIAL: "PARTIAL",
    RECALL_OP_PLANNED: "PLANNED",
    RECALL_OP_CANCELLED: "CANCELLED",
    RECALL_OP_ROLE_GONE: "ROLE_GONE",
}
MANUAL_FINAL_STATUSES = frozenset((RECALL_OP_REJECTED, RECALL_OP_ABORTED, RECALL_OP_PARTIAL))

# 双人复核批准记录校验（对齐 Go shared/dualcontrol/dualcontrol.go）。
APPROVAL_SUBJECT_PATTERN = re.compile(r"^[A-Za-z0-9._@-]{1,64}$")
APPROVAL_ID_PATTERN = re.compile(r"^[1-9][0-9]{0,18}$")


def validate_admin_approval(
    approval,
    operator: str,
    *,
    request_id: int | None = None,
) -> tuple[str | None, str | None]:
    """核对高风险管理请求的 AdminApproval 双人复核批准记录（对齐 Go dualcontrol.Require/RequireForRequest）。

    返回 `(kind, detail)`：
    - `(None, None)`：批准记录合格；
    - `("missing", ...)`：未携带批准记录（仅在 admin_allow_unapproved=True 时可放行并告警）；
    - `("invalid", ...)`：批准记录格式错误、发起人与批准人相同、发起人与操作人不符或批准号与 request_id 不符（一律拒审）。
    """
    if approval is None or (hasattr(approval, "ByteSize") and approval.ByteSize() == 0):
        return "missing", "dual-control approval record is missing"
    approval_id = str(getattr(approval, "approval_id", "") or "")
    proposer = str(getattr(approval, "proposer", "") or "")
    approver = str(getattr(approval, "approver", "") or "")
    approved_at_ms = int(getattr(approval, "approved_at_ms", 0) or 0)
    if not APPROVAL_ID_PATTERN.match(approval_id):
        return "invalid", "dual-control approval record is invalid: approval_id must be a positive decimal number"
    if not APPROVAL_SUBJECT_PATTERN.match(proposer):
        return "invalid", "dual-control approval record is invalid: proposer is not a valid operator identifier"
    if not APPROVAL_SUBJECT_PATTERN.match(approver):
        return "invalid", "dual-control approval record is invalid: approver is not a valid operator identifier"
    if approved_at_ms <= 0:
        return "invalid", "dual-control approval record is invalid: approved_at_ms is missing"
    if proposer == approver:
        return "invalid", "dual-control approval record is invalid: proposer and approver are the same person"
    if proposer != operator:
        return "invalid", "dual-control approval record is invalid: proposer differs from the request operator"
    if request_id is not None and approval_id != str(int(request_id)):
        return "invalid", "dual-control approval record is invalid: request_id must be the approval id"
    return None, None


def approval_trail(approval) -> str:
    """格式化双人复核留痕片段（对齐 Go recall_logic.go approvalTrail）。"""
    aid = str(getattr(approval, "approval_id", "") or "") if approval is not None else ""
    approver = str(getattr(approval, "approver", "") or "") if approval is not None else ""
    return f'approval_id="{aid}" approver="{approver}"'


def with_approval_trail(reason: str, approval) -> str:
    """将双人复核批准记录拼接到落库理由文本末尾（不改变入参长度校验）。"""
    return f"{reason} [{approval_trail(approval)}]"


class RecallPlanTooLarge(ValueError):
    """计划超过单批玩家、单玩家指令或总金额硬上限。"""


@dataclass(frozen=True, slots=True)
class RecallFilter:
    kind: int
    player_ids: tuple[int, ...] = ()
    item_config_id: int = 0
    currency_type: int = 0
    time_start: int = 0
    time_end: int = 0
    tx_types: tuple[int, ...] = ()

    def digest(self) -> bytes:
        players = sorted(self.player_ids)
        types = sorted(self.tx_types)
        p_str = "[" + " ".join(str(x) for x in players) + "]"
        t_str = "[" + " ".join(str(x) for x in types) + "]"
        text = f"v1|{self.kind}|{self.item_config_id}|{self.currency_type}|{self.time_start}|{self.time_end}|{p_str}|{t_str}"
        return hashlib.sha256(text.encode("utf-8")).digest()


@dataclass(slots=True)
class PlannedRecallOp:
    player_id: int
    currency_type: int
    item_config_id: int
    requested: int = 0
    hints: list[int] = field(default_factory=list)
    source_tx_ids: list[int] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class RecallReview:
    row: dict
    reason: int


@dataclass(slots=True)
class RecallPlan:
    ops: list[PlannedRecallOp] = field(default_factory=list)
    reviews: list[RecallReview] = field(default_factory=list)
    source_rows: int = 0
    player_count: int = 0
    amount_requested: int = 0
    digest: bytes = b""

    @property
    def digest_hex(self) -> str:
        return self.digest.hex()


def status_label(status: int) -> str:
    return _STATUS_LABELS.get(status, f"STATUS_{status}")


def valid_recall_text(text: str, max_runes: int) -> bool:
    return bool(text) and len(text) <= max_runes


def valid_recall_asset(kind: int, item_config_id: int, currency_type: int, known_currencies: set[int]) -> bool:
    if kind == RECALL_KIND_ITEM:
        return item_config_id != 0 and currency_type == 0
    if kind == RECALL_KIND_CURRENCY:
        return item_config_id == 0 and currency_type in known_currencies
    return False


def recall_gain_of(flt: RecallFilter, row: dict) -> int:
    """计算一条流水给 to_player 带来的收益，非收益返回 0。"""
    to_player = int(row.get("to_player", 0))
    if not to_player:
        return 0
    if flt.player_ids and to_player not in flt.player_ids:
        return 0
    if flt.kind == RECALL_KIND_ITEM:
        if int(row.get("item_config_id", 0)) != flt.item_config_id:
            return 0
        return int(row.get("item_quantity", 0))
    if (int(row.get("item_config_id", 0)) != 0
            or int(row.get("currency_type", 0)) != flt.currency_type
            or int(row.get("currency_delta", 0)) <= 0):
        return 0
    return int(row["currency_delta"])


def distinct_gains(flt: RecallFilter, rows: list[dict]) -> list[dict]:
    seen: set[int] = set()
    out: list[dict] = []
    for row in rows:
        tx_id = int(row["tx_id"])
        if tx_id in seen or recall_gain_of(flt, row) == 0:
            continue
        seen.add(tx_id)
        out.append(row)
    out.sort(key=lambda r: (int(r["to_player"]), int(r["tx_id"])))
    return out


def recall_instruction_ids(flt: RecallFilter, rows: list[dict]) -> dict[int, list[int]]:
    seen: set[tuple[int, int]] = set()
    out: dict[int, list[int]] = {}
    for row in rows:
        pid, corr = int(row.get("to_player", 0)), int(row.get("correlation_id", 0))
        if not corr or (pid, corr) in seen or recall_gain_of(flt, row) == 0:
            continue
        seen.add((pid, corr))
        out.setdefault(pid, []).append(corr)
    for ids in out.values():
        ids.sort()
    return out


def recall_source_tx_ids(flt: RecallFilter, rows: list[dict]) -> list[int]:
    return [int(r["tx_id"]) for r in rows if recall_gain_of(flt, r) > 0]


def _bit_at(words: list[int], index: int) -> bool:
    word = index // 64
    if word >= len(words):
        return False
    return (int(words[word]) & (1 << (index % 64))) != 0


def classify_seq(stream_ledger, epoch: int, seq: int) -> str:
    """判定 (epoch, seq) 在单条流账本中的状态（对齐 Go shared/assetop/classify.go）。"""
    if seq == 0 or epoch == 0:
        return "invalid"
    ledger_epoch = int(stream_ledger.stream_epoch) if stream_ledger is not None else 0
    if epoch < ledger_epoch:
        return "stale_epoch"
    if epoch > ledger_epoch:
        return "unseen" if seq <= _WINDOW_BITS else "jump_too_far"
    seen = list(stream_ledger.seen_bits)
    applied = list(stream_ledger.applied_bits)
    if len(seen) != _WINDOW_WORDS or len(applied) != _WINDOW_WORDS:
        return "invalid"
    watermark = int(stream_ledger.watermark)
    if watermark > MAX_UINT64 - _WINDOW_BITS:
        return "invalid"
    if seq <= watermark:
        return "behind_window"
    max_seq = int(stream_ledger.max_seq)
    if max_seq <= MAX_UINT64 - _MAX_SEQ_JUMP and seq > max_seq + _MAX_SEQ_JUMP:
        return "jump_too_far"
    if seq > watermark + _WINDOW_BITS:
        return "ahead_of_window"
    idx = seq - watermark - 1
    if not _bit_at(seen, idx):
        return "unseen"
    if _bit_at(applied, idx):
        return "applied"
    return "rejected"


def instruction_verdict(ledger_snapshot: tuple[bool, bool, object] | None, correlation_id: int) -> int:
    """用已提交资产账本核对指令是否恰好应用一次；返回 REVIEW_PROVEN(0) 或待核对原因码。"""
    if ledger_snapshot is None:
        return REVIEW_NO_PLAYER_STORE
    found, _released, ledger = ledger_snapshot
    if not found or ledger is None:
        return REVIEW_NO_PLAYER_STORE
    matched_stream = None
    matched_receipt = None
    matches = 0
    seen_streams: set[int] = set()
    for candidate in ledger.streams:
        stream_id = int(candidate.stream)
        if stream_id not in CREDIT_STREAMS:
            continue
        if stream_id in seen_streams:
            return REVIEW_LEDGER_AMBIGUOUS
        seen_streams.add(stream_id)
        for receipt in candidate.receipts:
            if int(receipt.correlation_id) == correlation_id:
                matched_stream, matched_receipt = candidate, receipt
                matches += 1
    if matches == 0:
        return REVIEW_NOT_IN_LEDGER
    if matches > 1:
        return REVIEW_LEDGER_AMBIGUOUS
    state = classify_seq(matched_stream, int(matched_stream.stream_epoch), int(matched_receipt.seq))
    if state == "rejected":
        return REVIEW_LEDGER_REJECTED
    if state != "applied":
        return REVIEW_LEDGER_NOT_APPLIED
    if int(matched_receipt.seq) in set(int(s) for s in matched_stream.partial_seqs):
        return REVIEW_LEDGER_PARTIAL
    return REVIEW_PROVEN


def _one_application(flt: RecallFilter, rows: list[dict]) -> tuple[int, int, list[int]] | None:
    first = rows[0]
    amount = recall_gain_of(flt, first)
    is_item = flt.kind == RECALL_KIND_ITEM
    hint = int(first.get("item_uuid", 0)) if is_item else 0
    source_ids: list[int] = []
    for row in rows:
        if recall_gain_of(flt, row) != amount:
            return None
        if is_item and len(rows) > 1 and (int(row.get("item_uuid", 0)) == 0 or int(row.get("item_uuid", 0)) != hint):
            return None
        source_ids.append(int(row["tx_id"]))
    return amount, hint, source_ids


def _plan_one_player(flt: RecallFilter, player_id: int, instructions: list[tuple[int, int, list[int]]]) -> list[PlannedRecallOp]:
    limit = (1 << 63) - 1 if flt.kind == RECALL_KIND_CURRENCY else (1 << 32) - 1
    current = PlannedRecallOp(player_id=player_id, currency_type=flt.currency_type, item_config_id=flt.item_config_id)
    ops = [current]
    for amount, hint, source_tx_ids in instructions:
        if amount > limit:
            raise RecallPlanTooLarge("instruction amount exceeds limit")
        new_hint = hint != 0 and hint not in current.hints
        if current.requested > limit - amount or (new_hint and len(current.hints) == MAX_RECALL_HINTS_PER_OP):
            if flt.kind == RECALL_KIND_CURRENCY:
                raise RecallPlanTooLarge("currency recall total exceeds limit")
            current = PlannedRecallOp(player_id=player_id, currency_type=flt.currency_type, item_config_id=flt.item_config_id)
            ops.append(current)
            new_hint = hint != 0
        current.requested += amount
        current.source_tx_ids.extend(source_tx_ids)
        if new_hint:
            current.hints.append(hint)
    if len(ops) > MAX_RECALL_OPS_PER_PLAYER:
        raise RecallPlanTooLarge("player recall ops exceed limit")
    return ops


def _plan_digest(flt: RecallFilter, ops: list[PlannedRecallOp], reviews: list[RecallReview]) -> bytes:
    parts = [flt.digest().hex()]
    for op in ops:
        hints_s = "[" + " ".join(str(x) for x in op.hints) + "]"
        txs_s = "[" + " ".join(str(x) for x in op.source_tx_ids) + "]"
        parts.append(f"{op.player_id}:{op.currency_type}:{op.item_config_id}:{op.requested}:{hints_s}:{txs_s}")
    for rev in reviews:
        parts.append(f"review:{int(rev.row['tx_id'])}:{rev.reason}")
    return hashlib.sha256("\n".join(parts).encode("utf-8")).digest()


def build_recall_plan(
    flt: RecallFilter,
    rows: list[dict],
    siblings: list[dict],
    ledgers: dict[int, tuple[bool, bool, object]],
    consumed: set[int],
) -> RecallPlan:
    """纯函数计算批量回收计划与待人工核对清单，预览和执行共用同一摘要口径。"""
    plan = RecallPlan()
    groups: dict[tuple[int, int], list[dict]] = {}
    for row in distinct_gains(flt, rows):
        corr = int(row.get("correlation_id", 0))
        if corr == 0:
            if int(row["tx_id"]) not in consumed:
                plan.reviews.append(RecallReview(row=row, reason=REVIEW_NO_CORRELATION))
            continue
        key = (int(row["to_player"]), corr)
        groups.setdefault(key, []).append(row)

    for row in distinct_gains(flt, siblings):
        key = (int(row["to_player"]), int(row.get("correlation_id", 0)))
        existing = groups.get(key)
        if existing is not None and not any(int(r["tx_id"]) == int(row["tx_id"]) for r in existing):
            existing.append(row)

    accepted: dict[int, list[tuple[int, int, list[int]]]] = {}
    for key in sorted(groups):
        grp_rows = sorted(groups[key], key=lambda r: int(r["tx_id"]))
        if any(int(r["tx_id"]) in consumed for r in grp_rows):
            continue
        reason = instruction_verdict(ledgers.get(key[0]), key[1])
        if reason != REVIEW_PROVEN:
            plan.reviews.extend(RecallReview(row=r, reason=reason) for r in grp_rows)
            continue
        inst = _one_application(flt, grp_rows)
        if inst is None:
            plan.reviews.extend(RecallReview(row=r, reason=REVIEW_ROWS_DISAGREE) for r in grp_rows)
            continue
        accepted.setdefault(key[0], []).append(inst)

    plan.reviews.sort(key=lambda r: (int(r.row["to_player"]), int(r.row["tx_id"])))
    for pid in sorted(accepted):
        plan.ops.extend(_plan_one_player(flt, pid, accepted[pid]))
        plan.player_count += 1
    for op in plan.ops:
        if plan.amount_requested > MAX_UINT64 - op.requested:
            raise RecallPlanTooLarge("total requested amount overflow")
        plan.amount_requested += op.requested
        plan.source_rows += len(op.source_tx_ids)
    plan.digest = _plan_digest(flt, plan.ops, plan.reviews)
    return plan


def judge_manual_resolve(
    ledger_snapshot: tuple[bool, bool, object],
    op_state: dict,
    now_ms: int,
    quiet_gap_ms: int = _DEFAULT_QUIET_GAP_MS,
) -> tuple[str, str]:
    """根据已提交资产账本判定单条卡死回收指令的人工终结证据（对齐 Go JudgeManual）。"""
    found, released, ledger = ledger_snapshot
    if not found or ledger is None:
        return "none", "player_store has no owner row for this player"
    stream_id = int(op_state["stream"])
    stream_epoch = int(op_state["stream_epoch"])
    seq = int(op_state["seq"])
    op_id = int(op_state["op_id"])
    stream = next((s for s in ledger.streams if int(s.stream) == stream_id), None)
    state = classify_seq(stream, stream_epoch, seq)
    if state in ("applied", "rejected"):
        receipt = next((r for r in stream.receipts if int(r.seq) == seq), None) if stream is not None else None
        if receipt is None or int(receipt.correlation_id) != op_id:
            return "none", f"committed ledger has {state} for seq={seq}, but receipt does not match op_id={op_id}"
        if state == "rejected":
            return "rejected", f"committed ledger recorded REJECTED for seq={seq}"
        if seq in set(int(s) for s in stream.partial_seqs):
            return "partial", f"committed ledger recorded PARTIAL for seq={seq}"
        return "applied", f"committed ledger recorded APPLIED for seq={seq}"
    if state in ("unseen", "ahead_of_window"):
        if not released:
            return "none", "committed ledger has not seen this seq, but owner row is still held by a scene"
        lease_until = int(op_state.get("lease_until_ms", 0))
        last_send = int(op_state.get("last_send_ms", 0))
        if lease_until > now_ms or last_send == 0 or now_ms < last_send + quiet_gap_ms:
            return "none", "owner row released and seq unseen, but op has not been quiet long enough"
        return "never_applied", "owner row released, committed ledger unseen, and op was quiet past quiet gap"
    return "none", f"committed ledger cannot tell ({state})"


def manual_resolve_allows(verdict: str, final_status: int) -> bool:
    """回收人工终结放行对表（对齐 Go recallManualAllows）。"""
    if verdict == "rejected":
        return final_status in (RECALL_OP_REJECTED, RECALL_OP_ABORTED)
    if verdict == "never_applied":
        return final_status == RECALL_OP_ABORTED
    if verdict == "none":
        return final_status == RECALL_OP_PARTIAL
    return False


def format_recall_evidence(verdict: str, detail: str) -> str:
    accepts = [status_label(st) for st in (RECALL_OP_REJECTED, RECALL_OP_ABORTED, RECALL_OP_PARTIAL)
               if manual_resolve_allows(verdict, st)]
    label = "|".join(accepts) if accepts else "none (redelivery loop finalizes it from receipt)"
    return f"recall_accepts={label}; verdict={verdict}; {detail}"
