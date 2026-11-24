"""player_inbox 领域常量、行模型与无 I/O 纯判定函数（与 Go shard/seq/fence/decide/rolefence 对齐）。"""

from __future__ import annotations

import hashlib
import re
import struct
from dataclasses import dataclass

from .errors import (
    ERR_EPOCH_AHEAD,
    ERR_EPOCH_REQUIRED,
    ERR_FULL,
    ERR_IDEMPOTENCY_CONFLICT,
    ERR_INVALID_PARAMETER,
    ERR_ITEM_NOT_FOUND,
    ERR_ITEM_STATE_CONFLICT,
    ERR_STALE_OWNER,
    ERR_TEXT_TOO_LONG,
)

# 节点与库表常量
PLAYER_INBOX_NODE_TYPE = 30  # common.base.eNodeType.PlayerInboxNodeService
DATABASE_NAME = "mmorpg_player_inbox"
SHARD_COUNT = 64
UINT64_MASK = (1 << 64) - 1
INT64_MAX = (1 << 63) - 1

# seq 分配常量（IB-6）
SEQ_CLOCK_SHIFT = 12
MAX_SEQ_CLOCK_MS = 1 << (63 - SEQ_CLOCK_SHIFT)

# 输入与窗口限值（与 Go constants.go 对齐）
MAX_OPERATOR_BYTES = 64
MAX_REASON_BYTES = 256
MAX_ACCOUNT_BYTES = 191
MAX_PAYLOAD_BYTES_HARD_CAP = 16 * 1024
MAX_DEBT_EXPIRES_AT_SECONDS = 253402300799
ISSUED_AT_MAX_PAST_MS = 7 * 24 * 60 * 60 * 1000
ISSUED_AT_MAX_FUTURE_MS = 5 * 60 * 1000
FETCH_DEFAULT_ITEMS = 16
FETCH_MAX_ITEMS = 16
ACK_MAX_DECISIONS = 32
LIST_DEFAULT_LIMIT = 50
LIST_MAX_LIMIT = 200
LEDGER_RECENT_CAPACITY = 32
DELIVERY_WARN_THRESHOLD = 5

# 外部调用预算（秒）
NOTIFY_TIMEOUT_SEC = 0.2
HOME_ZONE_TIMEOUT_SEC = 1.5
LEDGER_READ_TIMEOUT_SEC = 2.0
ROLE_FENCE_TIMEOUT_SEC = 2.0

# 角色护栏阻塞标签与 Kafka 通知事件号
ROLE_BLOCKER_ITEM = "player_inbox_item"
PLAYER_INBOX_NOTIFY_EVENT_ID = 49

# Proto 枚举值常量
CLASS_DURABLE = 1

DOMAIN_UNSPECIFIED = 0
DOMAIN_GM_SANCTION = 1
DOMAIN_BATTLE_SETTLEMENT = 2

PRODUCER_UNSPECIFIED = 0
PRODUCER_GM = 1
PRODUCER_MATCH = 2
VALID_PRODUCERS = frozenset((PRODUCER_GM, PRODUCER_MATCH))

MSG_UNSPECIFIED = 0
MSG_GM_BLOCK_CURRENCY_GAIN = 1
MSG_GM_UNBLOCK_CURRENCY_GAIN = 2
MSG_GM_ATTACH_CURRENCY_DEBT = 3
MSG_BATTLE_SETTLEMENT = 4
MSG_GM_BLOCK_ITEM_GAIN = 5
MSG_GM_UNBLOCK_ITEM_GAIN = 6

STATE_UNSPECIFIED = 0
STATE_PENDING = 1
STATE_APPLIED = 2
STATE_REJECTED = 3
STATE_VOIDED = 4
STATE_SETTLED_UNKNOWN = 5
VALID_STATES = frozenset((STATE_PENDING, STATE_APPLIED, STATE_REJECTED, STATE_VOIDED, STATE_SETTLED_UNKNOWN))
TERMINAL_STATES = frozenset((STATE_APPLIED, STATE_REJECTED, STATE_VOIDED, STATE_SETTLED_UNKNOWN))

DIRECTIVE_NONE = 0
DIRECTIVE_VOID = 1

VOID_UNSPECIFIED = 0
VOID_CANCELLED = 1
VOID_QUARANTINED = 2
VALID_VOID_REASONS = frozenset((VOID_CANCELLED, VOID_QUARANTINED))

VOID_MODE_NORMAL = 0
VOID_MODE_FORCE_MISMATCH = 1
VALID_VOID_MODES = frozenset((VOID_MODE_NORMAL, VOID_MODE_FORCE_MISMATCH))

FORCE_VOID_UNSPECIFIED = 0
FORCE_VOID_PROVEN = 1
FORCE_VOID_NOT_DELIVERED = 2
FORCE_VOID_LEDGER_ABSENT = 3
FORCE_VOID_NOT_DECIDED = 4
FORCE_VOID_SEQ_NOT_IN_RING = 5
FORCE_VOID_REQUEST_IN_LEDGER = 6
FORCE_VOID_LEDGER_INVALID = 7

FINALIZER_UNSPECIFIED = 0
FINALIZER_CONSUMER = 1
FINALIZER_ADMIN = 2

OUTCOME_UNSPECIFIED = 0
OUTCOME_APPLIED = 1
OUTCOME_REJECTED = 2
OUTCOME_SKIPPED = 3
OUTCOME_UNKNOWN = 4
RECORDABLE_OUTCOMES = frozenset((OUTCOME_APPLIED, OUTCOME_REJECTED, OUTCOME_SKIPPED))

FENCE_UNSPECIFIED = 0
FENCE_NO_STREAM = 1
FENCE_FIRST = 2
FENCE_MATCH = 3
FENCE_ADVANCE = 4
FENCE_STALE = 5
FENCE_EPOCH_REQUIRED = 6
FENCE_EPOCH_AHEAD = 7

ACK_UNSPECIFIED = 0
ACK_FINALIZED = 1
ACK_ALREADY_SAME = 2
ACK_CONFLICT = 3
ACK_UNKNOWN_SEQ = 4
ACK_NOT_DELIVERED = 5
ACK_SKIP_NOT_REQUESTED = 6
ACK_MISMATCH = 7

AUDIT_SANCTION = 1
AUDIT_VOID = 2
AUDIT_FORCE_VOID = 3

RECONCILE_DIFF_UNSPECIFIED = 0
RECONCILE_FINAL_NOT_IN_LEDGER = 1
RECONCILE_PENDING_BUT_DECIDED = 2

_INSTANCE_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


@dataclass(frozen=True, slots=True)
class RoleFenceRow:
    """本库 role_fence 副本行。"""

    player_id: int
    account: str
    owner_revision: int
    freeze_token: int
    freeze_final: bool
    version: int
    home_zone: int
    updated_ms: int = 0


@dataclass(frozen=True, slots=True)
class StreamRow:
    """player_inbox_stream 行。"""

    shard: int
    player_id: int
    domain: int
    next_seq: int
    fence_epoch: int
    open_count: int
    notify_due_ms: int
    created_ms: int
    updated_ms: int


@dataclass(frozen=True, slots=True)
class ItemRow:
    """player_inbox_item 行。"""

    shard: int
    player_id: int
    domain: int
    seq: int
    msg_type: int
    producer_kind: int
    request_id: int
    content_sha256: bytes
    payload: bytes
    state: int
    directive: int
    void_reason: int
    delivery_count: int
    last_delivered_epoch: int
    last_delivered_ms: int
    outcome_reason: int
    finalized_by: int
    decided_epoch: int
    issued_at_ms: int
    created_ms: int
    updated_ms: int
    operator_name: str
    note: str
    owner_account: str
    owner_revision: int


@dataclass(frozen=True, slots=True)
class AuditRow:
    """player_inbox_audit 行。"""

    shard: int
    player_id: int
    domain: int
    seq: int
    action: int
    operator: str
    peer_addr: str
    reason: str
    created_ms: int
    msg_type: int
    request_id: int
    issued_at_ms: int
    content_sha256: bytes
    payload: bytes
    void_reason: int = 0


@dataclass(frozen=True, slots=True)
class AuditInput:
    """调用方在事务内写审计行所需的最小字段（条目内容列由存储层从条目行原样复制）。"""

    action: int
    operator: str
    peer_addr: str
    reason: str
    void_reason: int = 0


@dataclass(frozen=True, slots=True)
class EnqueueParams:
    """入箱参数。"""

    player_id: int
    msg_type: int
    producer_kind: int
    request_id: int
    issued_at_ms: int
    payload: bytes
    operator: str
    note: str = ""
    battle_original: object | None = None


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    """入箱结果。"""

    code: int = 0
    seq: int = 0
    created: bool = False
    state: int = STATE_UNSPECIFIED


@dataclass(frozen=True, slots=True)
class VoidParams:
    """运维作废参数。"""

    player_id: int
    domain: int
    seq: int
    reason: int
    operator: str
    note: str = ""
    peer_addr: str = "unknown"
    mode: int = VOID_MODE_NORMAL


@dataclass(frozen=True, slots=True)
class AckDecision:
    """单条 Ack 报告的纯判定结果。"""

    result: int
    finalize: bool = False
    new_state: int = STATE_UNSPECIFIED
    state: int = STATE_UNSPECIFIED


@dataclass(frozen=True, slots=True)
class ForceVoidProof:
    """强制作废的账本证明结论。"""

    verdict: int
    decided_through: int = 0
    ring_request_id: int = 0


def mix64(x: int) -> int:
    """splitmix64 终结函数（Stafford Mix13），与 Go shared/inboxshard.Mix64 逐位一致。"""
    x &= UINT64_MASK
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & UINT64_MASK
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & UINT64_MASK
    return (x ^ (x >> 31)) & UINT64_MASK


def row_shard(player_id: int) -> int:
    """唯一分片计算入口：splitmix64(player_id) % 64。"""
    return mix64(player_id) % SHARD_COUNT


def content_sha256(msg_type: int, issued_at_ms: int, payload: bytes | None) -> bytes:
    """计算内容摘要：sha256(be32(msg_type) || be64(issued_at_ms) || payload)（IB-2）。"""
    header = struct.pack(">IQ", msg_type, issued_at_ms)
    return hashlib.sha256(header + (payload or b"")).digest()


def next_seq(current_next_seq: int, now_ms: int) -> int:
    """流行锁下分配下一个 seq：max(next_seq, now_ms << 12, 1)（IB-6）。"""
    if now_ms <= 0 or now_ms >= MAX_SEQ_CLOCK_MS:
        raise ValueError(f"system clock unusable for seq allocation: now_ms={now_ms}")
    return max(current_next_seq, now_ms << SEQ_CLOCK_SHIFT, 1)


def is_cursor_ahead(current_next_seq: int, consumer_decided_through: int) -> bool:
    """判定消费方水位是否已领先流的 next_seq（库恢复场景）。"""
    return consumer_decided_through >= current_next_seq


def raise_next_seq(current_next_seq: int, consumer_decided_through: int) -> tuple[int, bool]:
    """处理 CursorAhead：若水位 >= next_seq，将 next_seq 抬升到水位 + 1。"""
    if consumer_decided_through < current_next_seq:
        return current_next_seq, False
    return consumer_decided_through + 1, True


def decide_fence(req_epoch: int, fence: int, upper_bound: int) -> int:
    """owner_epoch 围栏纯判定函数（与 Go inbox.DecideFence 一致）。"""
    if req_epoch == 0:
        return FENCE_EPOCH_REQUIRED
    if req_epoch < fence:
        return FENCE_STALE
    if req_epoch == fence:
        return FENCE_MATCH
    if req_epoch > upper_bound:
        return FENCE_EPOCH_AHEAD
    if fence == 0:
        return FENCE_FIRST
    return FENCE_ADVANCE


def fence_allows(verdict: int) -> bool:
    return verdict in (FENCE_MATCH, FENCE_FIRST, FENCE_ADVANCE)


def fence_writes(verdict: int) -> bool:
    return verdict in (FENCE_FIRST, FENCE_ADVANCE)


def fence_reject_tip(verdict: int) -> int:
    if verdict == FENCE_STALE:
        return ERR_STALE_OWNER
    if verdict == FENCE_EPOCH_REQUIRED:
        return ERR_EPOCH_REQUIRED
    if verdict == FENCE_EPOCH_AHEAD:
        return ERR_EPOCH_AHEAD
    return 0


def is_terminal_state(state: int) -> bool:
    return state in TERMINAL_STATES


def outcome_to_state(outcome: int) -> int:
    mapping = {
        OUTCOME_APPLIED: STATE_APPLIED,
        OUTCOME_REJECTED: STATE_REJECTED,
        OUTCOME_SKIPPED: STATE_VOIDED,
        OUTCOME_UNKNOWN: STATE_SETTLED_UNKNOWN,
    }
    return mapping.get(outcome, STATE_UNSPECIFIED)


def same_outcome(state: int, state_reason: int, outcome: int, reason: int) -> bool:
    """终态与报告结局一致的唯一判定（Ack ALREADY_SAME 与对账共用）。"""
    want = outcome_to_state(outcome)
    if want == STATE_UNSPECIFIED or state != want:
        return False
    return want != STATE_REJECTED or state_reason == reason


def ack_outcome_reason(report) -> int:
    if report.outcome in (OUTCOME_REJECTED, OUTCOME_SKIPPED):
        return int(report.reason)
    return 0


def decide_ack(row: ItemRow | None, report) -> AckDecision:
    """按设计 §7.8 Ack 决策表判定单条报告。"""
    if row is None:
        return AckDecision(result=ACK_UNKNOWN_SEQ)
    if row.request_id != report.request_id:
        return AckDecision(result=ACK_MISMATCH, state=row.state)
    want = outcome_to_state(report.outcome)
    if row.state == STATE_PENDING:
        if row.delivery_count == 0:
            return AckDecision(result=ACK_NOT_DELIVERED, state=row.state)
        if report.outcome == OUTCOME_SKIPPED and row.directive != DIRECTIVE_VOID:
            return AckDecision(result=ACK_SKIP_NOT_REQUESTED, state=row.state)
        return AckDecision(result=ACK_FINALIZED, finalize=True, new_state=want, state=want)
    if same_outcome(row.state, row.outcome_reason, report.outcome, report.reason):
        return AckDecision(result=ACK_ALREADY_SAME, state=row.state)
    return AckDecision(result=ACK_CONFLICT, state=row.state)


def validate_ack_reports(reports, max_reports: int = ACK_MAX_DECISIONS) -> bool:
    if not reports or len(reports) > max_reports:
        return False
    prev = 0
    for report in reports:
        if report.seq == 0 or report.seq <= prev:
            return False
        if outcome_to_state(report.outcome) == STATE_UNSPECIFIED:
            return False
        prev = report.seq
    return True


def decide_existing(stored_digest: bytes, requested_digest: bytes) -> str:
    """同幂等键判重：摘要一致为 duplicate，否则为 conflict（IB-2）。"""
    if len(stored_digest) == 32 and stored_digest == requested_digest:
        return "duplicate"
    return "conflict"


def result_for_existing(row: ItemRow, digest: bytes) -> EnqueueResult:
    if decide_existing(row.content_sha256, digest) == "duplicate":
        return EnqueueResult(seq=row.seq, created=False, state=row.state)
    return EnqueueResult(code=ERR_IDEMPOTENCY_CONFLICT)


def decide_enqueue_locked(
    existing: ItemRow | None,
    digest: bytes,
    open_count: int,
    max_pending: int,
) -> tuple[EnqueueResult, bool]:
    """流行锁内权威判定：先判重、后判满箱（顺序是不变量）。"""
    if existing is not None:
        return result_for_existing(existing, digest), False
    if open_count >= max_pending:
        return EnqueueResult(code=ERR_FULL), False
    return EnqueueResult(), True


def decide_void(row: ItemRow | None) -> str:
    """普通模式作废动作判定（IB-5）。"""
    if row is None:
        return "not_found"
    if row.state != STATE_PENDING:
        return "terminal"
    if row.delivery_count == 0:
        return "direct"
    if row.directive == DIRECTIVE_VOID:
        return "already_requested"
    return "directive"


def void_outcome(action: str, state: int) -> tuple[int, int, bool]:
    """将作废动作折为 (tip_code, resp_state, void_requested)。"""
    if action == "terminal":
        return ERR_ITEM_STATE_CONFLICT, state, False
    if action == "direct":
        return 0, state, False
    if action in ("directive", "already_requested"):
        return 0, state, True
    if action == "forced":
        return 0, state, False
    if action == "force_refused":
        return ERR_ITEM_STATE_CONFLICT, state, False
    return ERR_ITEM_NOT_FOUND, STATE_UNSPECIFIED, False


def valid_ledger(ledger) -> bool:
    """整份校验已落盘账本结构，与 Go validLedger / C++ ValidateLedger 同口径。"""
    if ledger is None:
        return False
    seen: set[int] = set()
    for cursor in ledger.cursors:
        if cursor.domain == 0 or cursor.domain in seen or len(cursor.recent) > LEDGER_RECENT_CAPACITY:
            return False
        seen.add(cursor.domain)
        prev = 0
        for entry in cursor.recent:
            if (
                entry.seq == 0
                or entry.seq <= prev
                or entry.seq > cursor.decided_through_seq
                or entry.outcome not in RECORDABLE_OUTCOMES
                or entry.request_id == 0
            ):
                return False
            prev = entry.seq
    return True


def find_cursor(ledger, domain: int):
    if ledger is None:
        return None
    for cursor in ledger.cursors:
        if cursor.domain == domain:
            return cursor
    return None


def decided_through(ledger, domain: int) -> int:
    cursor = find_cursor(ledger, domain)
    return int(cursor.decided_through_seq) if cursor is not None else 0


def prove_never_executed(row: ItemRow, ledger, found: bool) -> ForceVoidProof:
    """用已落盘账本证明一条已投递 PENDING 行在当前消费方谱系上从未执行（IB-4 / IB-9）。"""
    if row.delivery_count == 0:
        return ForceVoidProof(verdict=FORCE_VOID_NOT_DELIVERED)
    if not found or ledger is None:
        return ForceVoidProof(verdict=FORCE_VOID_LEDGER_ABSENT)
    if not valid_ledger(ledger):
        return ForceVoidProof(verdict=FORCE_VOID_LEDGER_INVALID)
    cursor = find_cursor(ledger, row.domain)
    if cursor is None:
        return ForceVoidProof(verdict=FORCE_VOID_NOT_DECIDED)
    dt = int(cursor.decided_through_seq)
    if dt < row.seq:
        return ForceVoidProof(verdict=FORCE_VOID_NOT_DECIDED, decided_through=dt)
    seq_decided = False
    ring_req_id = 0
    for entry in cursor.recent:
        if entry.seq == row.seq:
            seq_decided = True
            ring_req_id = int(entry.request_id)
        if entry.request_id == row.request_id:
            return ForceVoidProof(
                verdict=FORCE_VOID_REQUEST_IN_LEDGER,
                decided_through=dt,
                ring_request_id=ring_req_id,
            )
    if not seq_decided:
        return ForceVoidProof(verdict=FORCE_VOID_SEQ_NOT_IN_RING, decided_through=dt)
    return ForceVoidProof(verdict=FORCE_VOID_PROVEN, decided_through=dt, ring_request_id=ring_req_id)


def decide_force_void(row: ItemRow | None, ledger, found: bool) -> tuple[str, ForceVoidProof]:
    if row is None:
        return "not_found", ForceVoidProof(verdict=FORCE_VOID_UNSPECIFIED)
    if row.state != STATE_PENDING:
        return "terminal", ForceVoidProof(verdict=FORCE_VOID_UNSPECIFIED)
    proof = prove_never_executed(row, ledger, found)
    if proof.verdict != FORCE_VOID_PROVEN:
        return "force_refused", proof
    return "forced", proof


def finalized_ledger_gap(row: ItemRow, cursor) -> str:
    """对账核心谓词：判断消费方终结的行在已落盘账本里是否存在缺口。"""
    if row.finalized_by != FINALIZER_CONSUMER or not is_terminal_state(row.state):
        return "none"
    dt = int(cursor.decided_through_seq) if cursor is not None else 0
    if row.seq > dt:
        return "beyond_watermark"
    if row.state == STATE_SETTLED_UNKNOWN:
        return "none"
    recent = cursor.recent if cursor is not None else ()
    if len(recent) >= LEDGER_RECENT_CAPACITY and row.seq < recent[0].seq:
        return "none"
    for entry in recent:
        if entry.seq != row.seq:
            continue
        if (
            entry.request_id != row.request_id
            or entry.msg_type != row.msg_type
            or not same_outcome(row.state, row.outcome_reason, entry.outcome, entry.reason)
        ):
            return "ring_mismatch"
        return "none"
    return "missing_in_ring"


def reconcile_diff(row: ItemRow, cursor) -> int:
    """判定单条收件箱行与已落盘账本游标的差异类型。"""
    if row.state == STATE_PENDING:
        dt = int(cursor.decided_through_seq) if cursor is not None else 0
        if row.seq <= dt:
            return RECONCILE_PENDING_BUT_DECIDED
        return RECONCILE_DIFF_UNSPECIFIED
    if finalized_ledger_gap(row, cursor) != "none":
        return RECONCILE_FINAL_NOT_IN_LEDGER
    return RECONCILE_DIFF_UNSPECIFIED


def issued_within_window(issued_at_ms: int, now_ms: int) -> bool:
    """校验 GM 签发时刻位于 [now-7d, now+5min]。"""
    if issued_at_ms > now_ms + ISSUED_AT_MAX_FUTURE_MS:
        return False
    return now_ms < ISSUED_AT_MAX_PAST_MS or issued_at_ms >= now_ms - ISSUED_AT_MAX_PAST_MS


def validate_text(text: str, max_bytes: int, *, required: bool) -> int:
    """校验自由文本字段的必填性、UTF-8 合法性与字节上限。"""
    if not text:
        return ERR_INVALID_PARAMETER if required else 0
    try:
        encoded = text.encode("utf-8")
    except UnicodeEncodeError:
        return ERR_TEXT_TOO_LONG
    if len(encoded) > max_bytes:
        return ERR_TEXT_TOO_LONG
    return 0


def valid_account(account: str) -> bool:
    try:
        return 0 < len(account.encode("utf-8")) <= MAX_ACCOUNT_BYTES
    except UnicodeEncodeError:
        return False


def _valid_node_id(node_id: str) -> bool:
    try:
        raw = node_id.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return 0 < len(raw) <= 96 and not any(ch in node_id for ch in "\x00\r\n\t ")


def valid_admission(identity) -> bool:
    """与 Go accountdirectory.ValidAdmission / Python data_service.model.valid_admission 一致。"""
    if identity is None:
        return False
    return (
        identity.player_id > 0
        and _INSTANCE_RE.fullmatch(identity.scene_instance_id) is not None
        and identity.generation > 0
        and identity.owner_revision > 0
        and identity.owner_epoch > 0
        and _valid_node_id(identity.scene_node_id)
        and identity.scene_id > 0
        and identity.zone_id > 0
        and identity.session_id > 0
        and identity.login_generation > 0
    )


def valid_role_fence_state(state) -> bool:
    """与 Go rolefence.ValidState 一致。"""
    if state is None:
        return False
    if state.player_id == 0 or state.owner_revision == 0 or state.version == 0 or state.home_zone == 0:
        return False
    if state.freeze_final and state.freeze_token == 0:
        return False
    if state.account == "":
        return state.freeze_token == 0
    return valid_account(state.account)


def clamp_limit(requested: int, default: int, maximum: int) -> int:
    if requested <= 0:
        return default
    return min(int(requested), maximum)


def subtract_open_count(open_count: int, finalized: int) -> tuple[int, bool]:
    if finalized > open_count:
        return 0, True
    return open_count - finalized, False


def build_audit_row(item: ItemRow, audit: AuditInput, now_ms: int) -> AuditRow:
    """从条目行复制不可变内容列组装审计行（评审 L9）。"""
    return AuditRow(
        shard=row_shard(item.player_id),
        player_id=item.player_id,
        domain=item.domain,
        seq=item.seq,
        action=audit.action,
        operator=audit.operator,
        peer_addr=audit.peer_addr,
        reason=audit.reason,
        created_ms=now_ms,
        msg_type=item.msg_type,
        request_id=item.request_id,
        issued_at_ms=item.issued_at_ms,
        content_sha256=item.content_sha256 or b"",
        payload=item.payload or b"",
        void_reason=audit.void_reason,
    )
