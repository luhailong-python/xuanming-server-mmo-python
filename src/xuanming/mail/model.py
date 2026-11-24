"""mail 领域常量、不可变数据模型与纯校验/转换函数（零 I/O）。"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from google.protobuf.message import DecodeError, Message

from ..protocol import Protocol
from .errors import MailAttachmentError, MailInvalidError

DATABASE_NAME = "mmorpg_mail"
MAIL_NODE_TYPE = 8  # common.base.eNodeType.MailNodeService

# MailKind（对应 mail.proto）
MAIL_KIND_UNSPECIFIED = 0
MAIL_KIND_PERSONAL = 1
MAIL_KIND_SYSTEM = 2
MAIL_KIND_GUILD = 3

# MailClaimState（对应 mail.proto）
CLAIM_NONE = 0
CLAIM_UNCLAIMED = 1
CLAIM_PENDING = 2
CLAIM_CLAIMED = 3
CLAIM_MANUAL = 4

# MailAssetOpStatus（对应 mail_table.proto）
OP_UNSPECIFIED = 0
OP_PENDING = 1
OP_APPLIED = 2
OP_REJECTED = 3
OP_MANUAL = 4
OP_ABANDONED = 5
OP_TERMINAL_STATUSES = frozenset((OP_APPLIED, OP_REJECTED, OP_ABANDONED))

# 活动状态（mail_campaign.state）
CAMPAIGN_IMPORTING = 1
CAMPAIGN_SEALED = 2

# 资产通道契约常量
ASSET_STREAM_SYSTEM_CREDIT = 5
TX_MAIL_ATTACHMENT = 2
ASSET_OP_MAX_PENDING = 16
ASSET_OP_MAX_SPAN = 512
FRESH_ATTEMPT_LIMIT = 3
ASSET_OP_CALLER = "mail"
ROLE_FENCE_CALLER = "mail"
ROLE_BLOCKER_ASSET_OP = "mail_asset_op"

# 发件人 caller -> sender_kind 固定白名单映射
SENDER_KINDS: dict[str, int] = {
    "admin": 1,
    "guild": 2,
    "rank": 3,
    "trade": 4,
    "battle": 5,
}

# 附件与内容边界常量
KNOWN_CURRENCIES = frozenset((0, 1, 2))
MAX_CURRENCIES = 4
MAX_ITEM_LINES = 16
MAX_TOTAL_ITEMS = 4096
MAX_TITLE_RUNES = 128
MAX_BODY_RUNES = 4096
MAX_CLAIM_BATCH = 100
MAX_DELETE_BATCH = 100
MAX_CLEAR_READ_SCAN = 1000
LIST_DEFAULT_LIMIT = 50
LIST_MAX_LIMIT = 100
MAX_SHARD_RECIPIENTS = 1000
MAX_CAMPAIGN_RECIPIENTS = 1_000_000
MAX_CAMPAIGN_SHARDS = 10_000
MAX_ACCOUNT_BYTES = 191
MS_PER_DAY = 86_400_000
UINT64_MAX = (1 << 64) - 1
INT64_MAX = (1 << 63) - 1

SWEEP_MODE_REPORT_ONLY = "report_only"
SWEEP_MODE_DELETE = "delete"

_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class MailLimits:
    """邮件容量与有效期限额（与 Go data.Limits 一致）。"""

    max_personal_mails_per_player: int = 200
    max_active_system_mails: int = 100
    max_personal_mail_ttl_days: int = 30
    max_system_mail_ttl_days: int = 30
    max_active_guild_mails_per_guild: int = 20

    def validate(self) -> None:
        if (
            not 1 <= self.max_personal_mails_per_player <= 1000
            or not 1 <= self.max_active_system_mails <= 500
            or not 1 <= self.max_personal_mail_ttl_days <= 365
            or not 1 <= self.max_system_mail_ttl_days <= 365
        ):
            raise ValueError("Mail capacity or TTL out of allowed range")
        if not 1 <= self.max_active_guild_mails_per_guild <= 100:
            raise ValueError("Mail.max_active_guild_mails_per_guild must be within [1, 100]")


@dataclass(frozen=True, slots=True)
class RoleOriginal:
    """Gate 验签原件提取的在线角色原身份。"""

    player_id: int
    account: str
    owner_revision: int


@dataclass(frozen=True, slots=True)
class RoleFenceRow:
    """本库 role_fence 副本的一行快照。"""

    player_id: int
    account: str
    owner_revision: int
    freeze_token: int
    freeze_final: bool
    version: int
    home_zone: int
    updated_ms: int = 0


@dataclass(frozen=True, slots=True)
class MailRow:
    """mail_message 表记录。"""

    mail_id: int
    owner_player_id: int
    guild_id: int
    kind: int
    sender_kind: int
    request_id: int
    request_hash: str
    title: str
    body: str
    attachments: bytes
    sent_ms: int
    expire_ms: int
    campaign_id: int = 0
    manifest_generation: int = 0


@dataclass(frozen=True, slots=True)
class MailStateRow:
    """mail_state 表记录。"""

    player_id: int
    mail_id: int
    read_ms: int = 0
    deleted_ms: int = 0
    claim_state: int = CLAIM_NONE
    current_op_id: int = 0
    claim_version: int = 0
    reason_tip_id: int = 0
    updated_ms: int = 0


@dataclass(frozen=True, slots=True)
class MailCampaignRow:
    """mail_campaign 表记录。"""

    sender_kind: int
    campaign_id: int
    manifest_generation: int
    mail_id: int
    request_id: int
    producer: str
    request_hash: str
    title: str
    body: str
    attachments: bytes
    expire_ms: int
    expected_recipient_count: int
    expected_shard_count: int
    manifest_hash: str
    state: int
    created_ms: int
    sealed_ms: int
    kind: int
    guild_id: int


@dataclass(frozen=True, slots=True)
class MailCampaignShardRow:
    """mail_campaign_shard 表记录。"""

    sender_kind: int
    campaign_id: int
    manifest_generation: int
    shard_index: int
    shard_hash: str
    recipient_count: int


@dataclass(frozen=True, slots=True)
class MailAssetOpRow:
    """mail_asset_op 表记录。"""

    op_id: int
    player_id: int
    stream: int
    seq: int
    stream_epoch: int
    mail_id: int
    claim_version: int
    status: int
    payload: bytes
    tx_type: int
    attempts: int
    next_attempt_ms: int
    lease_until_ms: int
    lease_token: int
    last_outcome: int
    last_reason: int
    durable: int
    reason_tip_id: int
    created_ms: int
    updated_ms: int
    last_send_ms: int
    resolved_by: str = ""
    resolve_reason: str = ""


@dataclass(frozen=True, slots=True)
class PendingAssetOp:
    """重投循环领取到的一笔待投递资产指令。"""

    op_id: int
    player_id: int
    stream: int
    seq: int
    stream_epoch: int
    correlation_id: int
    tx_type: int
    bundle: Message
    attempts: int
    lease_token: int
    last_reason: int
    created_ms: int
    last_send_ms: int
    deadline_ms: int = 0


@dataclass(frozen=True, slots=True)
class SweepStats:
    """单类表过期清理统计。"""

    candidates: int = 0
    deleted: int = 0
    invalid: int = 0


@dataclass(frozen=True, slots=True)
class SweepReport:
    """单轮过期清理汇总。"""

    personal: SweepStats = SweepStats()
    system: SweepStats = SweepStats()
    system_state: SweepStats = SweepStats()


def sender_kind_for(caller: str) -> int:
    """按固定白名单将内部调用方名字映射为 sender_kind；未知返回 0。"""
    return SENDER_KINDS.get(caller, 0)


def has_attachments(payload: bytes) -> bool:
    return len(payload) > 0


def is_protected_claim_state(claim_state: int) -> bool:
    """PENDING(2)、MANUAL(4) 及未知状态受保护，不因过期隐藏或被删除。"""
    return claim_state not in (CLAIM_NONE, CLAIM_UNCLAIMED, CLAIM_CLAIMED)


def effective_claim_state(attachments: bytes, state: MailStateRow | None) -> int:
    if state is not None:
        return state.claim_state
    return CLAIM_UNCLAIMED if has_attachments(attachments) else CLAIM_NONE


def _has_unknown_fields(message: Message) -> bool:
    try:
        return len(message.UnknownFields()) > 0
    except Exception:
        return False


def validate_send_content(request: Message | None) -> None:
    """校验发件基础字段（对应 Go validateContent）。"""
    if request is None:
        raise MailInvalidError("empty send mail request")
    if (
        request.sender_kind == 0
        or request.request_id == 0
        or not 1 <= len(request.title) <= MAX_TITLE_RUNES
        or len(request.body) > MAX_BODY_RUNES
        or _has_unknown_fields(request)
    ):
        raise MailInvalidError("invalid mail header or body")


def build_asset_bundle(
    protocol: Protocol,
    attachments: Message | None,
    item_exists: Callable[[int], bool] | None,
) -> bytes:
    """将 MailAttachments 严格校验并确定性编码为 AssetBundle 字节（对应 Go mailgrant.Bundle）。"""
    if attachments is None:
        return b""
    if _has_unknown_fields(attachments):
        raise MailAttachmentError("attachments carries unknown fields")
    if len(attachments.currencies) > MAX_CURRENCIES or len(attachments.items) > MAX_ITEM_LINES:
        raise MailAttachmentError("attachments count exceeds limit")

    bundle = protocol.message("AssetBundle")
    seen_currencies: set[int] = set()
    for currency in attachments.currencies:
        if (
            _has_unknown_fields(currency)
            or currency.currency_type not in KNOWN_CURRENCIES
            or not 1 <= currency.amount <= INT64_MAX
            or currency.currency_type in seen_currencies
        ):
            raise MailAttachmentError("invalid mail currency attachment")
        seen_currencies.add(int(currency.currency_type))
        bundle.currencies.add(currency_type=currency.currency_type, amount=currency.amount)

    seen_items: set[int] = set()
    total_items = 0
    for item in attachments.items:
        if (
            _has_unknown_fields(item)
            or item.config_id == 0
            or item.count == 0
            or item.config_id in seen_items
            or item_exists is None
            or not item_exists(int(item.config_id))
        ):
            raise MailAttachmentError("invalid mail item attachment")
        total_items += int(item.count)
        if total_items > MAX_TOTAL_ITEMS:
            raise MailAttachmentError("total attachment item count exceeds 4096")
        seen_items.add(int(item.config_id))
        bundle.items.add(config_id=item.config_id, count=item.count)

    if not bundle.currencies and not bundle.items:
        return b""
    return bundle.SerializeToString(deterministic=True)


def bundle_to_attachments_view(protocol: Protocol, payload: bytes) -> Message:
    """将存储的 AssetBundle 字节还原为客户端展示的 MailAttachments（对应 Go mailgrant.View）。"""
    view = protocol.message("mailpb.MailAttachments")
    if not payload:
        return view
    try:
        bundle = protocol.parse("AssetBundle", payload)
    except DecodeError as exc:
        raise MailAttachmentError("corrupt AssetBundle payload") from exc
    if (
        _has_unknown_fields(bundle)
        or len(bundle.item_uuids) > 0
        or bundle.pet_id != 0
        or len(bundle.item_instances) > 0
        or bundle.HasField("pet_instance")
    ):
        raise MailAttachmentError("unsupported AssetBundle shape in mail")
    for currency in bundle.currencies:
        view.currencies.add(currency_type=currency.currency_type, amount=currency.amount)
    for item in bundle.items:
        view.items.add(config_id=item.config_id, count=item.count)
    return view


def build_mail_view(protocol: Protocol, mail: MailRow, state: MailStateRow | None) -> Message:
    """构造客户端可见的 mailpb.MailView。"""
    attachments = bundle_to_attachments_view(protocol, mail.attachments)
    claim_state = effective_claim_state(mail.attachments, state)
    view = protocol.message(
        "mailpb.MailView",
        mail_id=mail.mail_id,
        kind=mail.kind,
        title=mail.title,
        body=mail.body,
        sent_ms=mail.sent_ms,
        expire_ms=mail.expire_ms,
        is_read=bool(state is not None and state.read_ms != 0),
        claim_state=claim_state,
        attachments=attachments,
        asset_op_id=state.current_op_id if state is not None else 0,
    )
    if state is not None and state.reason_tip_id != 0:
        view.claim_reason.id = state.reason_tip_id
    return view


def valid_manifest(request: Message) -> bool:
    """校验活动冻结名单元信息（对应 Go validManifest）。"""
    return (
        request.recipient_player_id == 0
        and request.campaign_id > 0
        and request.manifest_generation > 0
        and 0 < request.expected_recipient_count <= MAX_CAMPAIGN_RECIPIENTS
        and 0 < request.expected_shard_count <= MAX_CAMPAIGN_SHARDS
        and bool(_LOWER_SHA256_RE.fullmatch(request.manifest_hash))
    )


def shard_hash(player_ids: Sequence[int]) -> tuple[str, tuple[int, ...]]:
    """计算名单分片摘要：去重、按 uint64 升序拼接十进制行后做小写 SHA256。"""
    if not player_ids or len(player_ids) > MAX_SHARD_RECIPIENTS:
        raise MailInvalidError("invalid shard recipient count")
    ordered = sorted(int(pid) for pid in player_ids)
    unique: list[int] = []
    for pid in ordered:
        if pid <= 0 or pid > UINT64_MAX:
            raise MailInvalidError("invalid recipient player_id in shard")
        if not unique or unique[-1] != pid:
            unique.append(pid)
    encoded = "".join(f"{pid}\n" for pid in unique).encode("ascii")
    return hashlib.sha256(encoded).hexdigest(), tuple(unique)


def manifest_hash(shards: Sequence[tuple[int, str, int]]) -> str:
    """按 shard_index 升序计算冻结名单总摘要（v1 跨语言契约）。"""
    lines = ["v1\n"]
    for idx, digest, count in shards:
        lines.append(f"{idx}:{digest}:{count}\n")
    return hashlib.sha256("".join(lines).encode("ascii")).hexdigest()


def valid_account(account: str) -> bool:
    try:
        return 0 < len(account.encode("utf-8")) <= MAX_ACCOUNT_BYTES
    except UnicodeEncodeError:
        return False


def valid_role_fence_state(state) -> bool:
    """校验 RoleFenceState 字段合法性（对应 Go rolefence.ValidState）。"""
    if state is None:
        return False
    if state.player_id == 0 or state.owner_revision == 0 or state.version == 0 or state.home_zone == 0:
        return False
    if state.freeze_final and state.freeze_token == 0:
        return False
    if state.account == "":
        return state.freeze_token == 0
    return valid_account(state.account)


def check_role_fence(fence: RoleFenceRow | None, expect: RoleOriginal) -> str:
    """按 Go rolefence.Check 语义判定副本行：返回 ``ok`` / ``stale`` / ``reject``。"""
    if fence is None:
        return "stale"
    if expect.owner_revision != 0:
        if fence.owner_revision > expect.owner_revision:
            return "reject"
        if fence.owner_revision < expect.owner_revision:
            return "stale"
        if fence.account != expect.account:
            return "reject"
    if fence.freeze_token != 0:
        return "stale"
    if fence.account == "":
        return "reject"
    return "ok"
