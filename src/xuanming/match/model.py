"""match 与 team 领域常量、行模型、签名与无 I/O 纯校验函数。"""

from __future__ import annotations

import hashlib
import hmac
import re
import struct
import uuid
from dataclasses import dataclass
from typing import Iterable

from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import service_digest, sign_service_call, verify_service_call
from ..login.caller_auth import AuthError, CallerVerifier, SESSION_DETAIL, parse_details
from ..protocol import Protocol

# -- 节点类型与全局协议常量 --------------------------------------------------------------------------

TEAM_NODE_TYPE = 10   # common.base.eNodeType.TeamNodeService
MATCH_NODE_TYPE = 16  # common.base.eNodeType.MatchNodeService
SCENE_NODE_TYPE = 3   # common.base.eNodeType.SceneNodeService
GATE_NODE_TYPE = 4    # common.base.eNodeType.GateNodeService
BATTLE_NODE_TYPE = 28 # common.base.eNodeType.BattleNodeService

MAX_TEAM_SIZE = 5
MAX_INVITES_PER_PLAYER = 20
MAX_APPLICATIONS_PER_TEAM = 20
INVITE_TTL_MS = 300_000
APPLICATION_TTL_MS = 300_000
TEAM_IDLE_TTL_MS = 24 * 3_600_000
RECALL_COOLDOWN_MS = 15_000
VALID_TEAM_FOLLOW_FORMATIONS = frozenset((0, 1, 2))
MAX_PROBE_REGIONS = 16
MAX_BATTLE_REPORTS = 100
MAX_COMPLETION_BYTES = 960 * 1024
MAX_INBOX_BATTLE_PAYLOAD_BYTES = 16 * 1024
TERMINAL_OUTCOME_NO_RETRY_MS = (1 << 63) - 1
TERMINAL_OUTCOME_GRACE_MS = 60_000
MAX_OPERATOR_BYTES = 64
MAX_REASON_BYTES = 191

# MatchMode 枚举值
MODE_UNSPECIFIED = 0
MODE_5V5 = 1
MODE_3V3 = 2
MODE_1V1 = 3
MODE_PVE_SOLO = 4
MODE_PVE_TEAM = 5
MODE_PVP_CHALLENGE = 6
MODE_PVP_TEAM = 7
MODE_POLARITY_5V5 = 8
MODE_GUILD_WAR_PVP = 9
MODE_GUILD_WAR_SIEGE = 10
MODE_SEASON_TOURNAMENT = 11
MODE_JIANGHU_DUEL = 12
BATTLE_ACTIVITY_KIND_GUILD_WAR = 3
GUILD_WAR_MODES = frozenset((MODE_GUILD_WAR_PVP, MODE_GUILD_WAR_SIEGE))
PVE_MODES = frozenset((MODE_PVE_SOLO, MODE_PVE_TEAM))
PVP_MODES = frozenset((
    MODE_1V1, MODE_3V3, MODE_5V5, MODE_PVP_CHALLENGE, MODE_PVP_TEAM,
    MODE_POLARITY_5V5, MODE_GUILD_WAR_PVP, MODE_SEASON_TOURNAMENT, MODE_JIANGHU_DUEL,
))

# 五行试炼志 V3 全部四项自选誓约合法位掩码（bit0 禁控 / bit1 单宝 / bit2 无宠 / bit3 紧缩回合）
TRIAL_VOW_MASK_ALL = 0x0F

# QueueState 枚举值
QUEUE_STATE_QUEUED = 1
QUEUE_STATE_MATCHED = 2
QUEUE_STATE_READY = 3
QUEUE_STATE_ENTERING = 4
QUEUE_STATE_NOT_QUEUED = 5

# Ticket 状态（match_authority_ticket.state）
TICKET_STATE_QUEUED = 1
TICKET_STATE_FORMING = 2
TICKET_STATE_READY = 3
TICKET_STATE_DONE = 4

# Formation 状态（match_formation.state）
FORMATION_STATE_PREPARING = 1
FORMATION_STATE_PERSISTED = 2
FORMATION_STATE_UNKNOWN = 3
FORMATION_STATE_CREATED = 4
FORMATION_STATE_ABORTED = 5

# TeamAuthority 状态
TEAM_STATE_ACTIVE = 1
TEAM_STATE_DISBANDED = 2

# TeamLaunch 状态
TEAM_LAUNCH_PENDING = 1
TEAM_LAUNCH_QUEUED = 2
TEAM_LAUNCH_FORMING = 3
TEAM_LAUNCH_CREATED = 4
TEAM_LAUNCH_CLOSED = 5

# Challenge 状态
CHALLENGE_STATE_PENDING = 1
CHALLENGE_STATE_FORMED = 2
CHALLENGE_STATE_DECLINED = 3
CHALLENGE_STATE_ACCEPTING = 4
CHALLENGE_STATE_CLOSED = 5

# BattleOutcomeDecision
OUTCOME_DECISION_UNKNOWN = 0
OUTCOME_DECISION_COMPLETE = 1
OUTCOME_DECISION_ABORT = 2

# MatchBattleCompletionSourceKind
COMPLETION_SOURCE_KAFKA = 1
COMPLETION_SOURCE_BATTLE_RPC = 2
COMPLETION_SOURCE_DEADLINE_ABORT = 3
COMPLETION_SOURCE_NO_ROOM_ABORT = 4
COMPLETION_SOURCE_MANUAL_ABORT = 5
COMPLETION_SOURCE_REJECTED_ORIGINAL_ABORT = 6
COMPLETION_SOURCE_INSTANCE_GONE_ABORT = 7

# Settlement dispatch_state
SETTLEMENT_DISPATCH_LEGACY = 0
SETTLEMENT_DISPATCH_PENDING = 1
SETTLEMENT_DISPATCH_APPLIED = 2

# PlayerInbox 协议常量（与 player_inbox.model 严格一致）
INBOX_MSG_BATTLE_SETTLEMENT = 4
INBOX_ITEM_STATE_PENDING = 1
INBOX_ITEM_STATE_APPLIED = 2

# Spectate 常量
SPECTATE_VISIBILITY_PRIVATE = 1
SPECTATE_VISIBILITY_PUBLIC = 2
SPECTATE_DELIVERY_NONE = 0
SPECTATE_DELIVERY_DIRECT = 1
SPECTATE_DELIVERY_LOBBY = 2
SPECTATE_TIER_DIRECT = 1
SPECTATE_TIER_RELAY = 2
SPECTATE_TIER_BROADCAST = 3

SPECTATOR_STATE_PENDING_ADD = 1
SPECTATOR_STATE_ACTIVE = 2
SPECTATOR_STATE_PENDING_CANCEL = 3
SPECTATOR_STATE_UNKNOWN = 4
SPECTATOR_STATE_CLOSED = 5

# TrialOccupancy owner_kind
OCCUPANCY_KIND_GUILD_TRIAL = 1

# Probe 签名域（必须与 nodes/battle_tcp.py:28-29 及 Go authority/probe.go 逐字节一致）
PROBE_GRANT_DOMAIN = b"xuanming.match.probe.grant.v1\x00"
PROBE_RECEIPT_DOMAIN = b"xuanming.match.probe.receipt.v1\x00"

# BattleNode 不可分配跨语言契约消息（go/match/internal/logic/gather.go:67）
BATTLE_NOT_ALLOCATABLE_MESSAGE = "battle_not_allocatable"

# RoleFence 阻塞标签
ROLE_BLOCKER_TICKET = "match_ticket"
ROLE_BLOCKER_TEAM = "team_membership"
ROLE_BLOCKER_TEAM_LAUNCH = "team_launch"
ROLE_BLOCKER_SPECTATOR = "match_spectator"
ROLE_BLOCKER_OCCUPANCY = "match_role_occupancy"
ROLE_BLOCKER_SETTLEMENT = "match_battle_settlement"

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


# -- 不可变数据模型 ----------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CallerIdentity:
    """经 Gate Ed25519 验签通过的权威玩家会话身份。"""

    player_id: int
    account: str
    owner_revision: int
    session_id: int
    gate_node_id: int
    gate_instance_id: str
    session_incarnation: int
    release_track: str = "stable"


@dataclass(frozen=True, slots=True)
class RoleFenceRow:
    """role_fence 表行模型（不可变值对象）。"""

    player_id: int
    account: str
    owner_revision: int
    freeze_token: int
    freeze_final: bool
    version: int
    home_zone: int
    updated_ms: int


@dataclass(frozen=True, slots=True)
class BattleNodeEntry:
    """服务发现中的 Battle 节点条目快照。"""

    node_id: int
    node_uuid: str
    grpc_endpoint: str
    physical_region: int
    client_host: str = "127.0.0.1"
    client_port: int = 0
    client_ws_url: str = ""
    release_track: str = "stable"
    admitting: bool = True


@dataclass(frozen=True, slots=True)
class PlayerPresence:
    """玩家实时路由与展示信息（来自定位/会话目录，只读咨询性数据）。"""

    player_id: int
    online: bool = False
    in_battle: bool = False
    zone_id: int = 1
    scene_node_id: int = 0
    scene_instance_id: str = ""
    scene_grpc_endpoint: str = ""
    session_id: int = 0
    gate_node_id: int = 0
    gate_instance_id: str = ""
    name: str = ""
    level: int = 1
    class_id: int = 0
    gender: int = 0
    appearance_id: str = ""
    hosted: bool = False


# -- UUID 与文本校验工具 -----------------------------------------------------------------------------


def is_valid_uuid(text: str) -> bool:
    if not text or not _UUID_RE.fullmatch(text):
        return False
    return uuid.UUID(text).int != 0


def uuid_to_pair(text: str) -> tuple[int, int]:
    """将标准非零 UUID 拆为 (high64, low64) 无符号整数，匹配 SQL 主键约定。"""
    if not is_valid_uuid(text):
        raise ValueError(f"invalid uuid: {text!r}")
    val = uuid.UUID(text).int
    return (val >> 64) & ((1 << 64) - 1), val & ((1 << 64) - 1)


def pair_to_uuid(high: int, low: int) -> str:
    return str(uuid.UUID(int=((high & ((1 << 64) - 1)) << 64) | (low & ((1 << 64) - 1))))


def valid_operator_text(text: str, max_bytes: int, *, required: bool = True) -> bool:
    if required and not text:
        return False
    raw = text.encode("utf-8")
    if len(raw) > max_bytes:
        return False
    return not any(ord(ch) < 32 or ord(ch) == 127 for ch in text)


def valid_role_fence_state(state: Message) -> bool:
    if state is None or int(state.player_id) == 0 or int(state.version) == 0:
        return False
    if len(state.account.encode("utf-8")) > 191:
        return False
    if state.account and int(state.owner_revision) == 0:
        return False
    return True


# -- Gate / 内部服务鉴权辅助函数 ---------------------------------------------------------------------


def metadata_dict(context) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    if context is None:
        return result
    for key, value in context.invocation_metadata() or ():
        k = key.lower()
        v = value.decode("utf-8", errors="ignore") if isinstance(value, (bytes, bytearray)) else str(value)
        result.setdefault(k, []).append(v)
    return result


def has_session_metadata(context) -> bool:
    """判定入站请求是否携带 Gate 客户端会话头（内部服务必须 fail-closed 拒绝）。"""
    return SESSION_DETAIL in metadata_dict(context)


def authenticate_gate_caller(
    protocol: Protocol,
    verifier: CallerVerifier | None,
    context,
    method: str,
) -> CallerIdentity:
    """验签 Gate Ed25519 会话身份，强制要求完整 player_id / owner_revision / authenticated_account。"""
    if verifier is None:
        raise AuthError("verifier_not_configured")
    raw = verifier.authenticate(context.invocation_metadata() if context is not None else (), method)
    if raw is None:
        raise AuthError("missing_session")
    details = parse_details(protocol, raw)
    if (
        not details.player_id
        or not details.owner_revision
        or not details.authenticated_account
    ):
        raise AuthError("incomplete_identity")
    md = metadata_dict(context)
    track = (md.get("x-release-track") or ["stable"])[0].strip() or "stable"
    if track not in ("stable", "canary"):
        raise AuthError("invalid_release_track")
    return CallerIdentity(
        player_id=int(details.player_id),
        account=str(details.authenticated_account),
        owner_revision=int(details.owner_revision),
        session_id=int(details.session_id),
        gate_node_id=int(details.gate_node_id),
        gate_instance_id=str(details.gate_instance_id),
        session_incarnation=int(details.session_incarnation),
        release_track=track,
    )


def verify_internal_hmac(path: str, request: Message, caller: str, secret: bytes, now_ms: int) -> bool:
    if not secret:
        return False
    return verify_service_call(path, request, caller, now_ms, secret)


def sign_internal_reply(
    path: str,
    request: Message,
    reply: Message,
    caller: str,
    secret: bytes,
    now_ms: int,
) -> None:
    """写入 request_digest 并以 ``path + '/reply'`` 签名内部响应。"""
    reply.request_digest = service_digest(request)
    sign_service_call(path + "/reply", reply, caller, now_ms, secret)


# -- 地域探测 HMAC 与会话绑定（与 nodes/battle_tcp.py 及 Go authority/probe.go 逐字节一致）----------


def session_binding(caller: CallerIdentity) -> str:
    """按长度前缀计算会话绑定摘要，防止跨会话/换主复用探测回执。"""
    if (
        not caller.player_id
        or not caller.gate_instance_id
        or not caller.session_incarnation
        or not caller.owner_revision
        or not caller.account
    ):
        return ""
    gate_bytes = caller.gate_instance_id.encode("utf-8")
    acct_bytes = caller.account.encode("utf-8")
    buf = bytearray()
    buf += struct.pack(">Q", len(gate_bytes)) + gate_bytes
    buf += struct.pack(">QQ", caller.session_incarnation, caller.player_id)
    buf += struct.pack(">Q", len(acct_bytes)) + acct_bytes
    buf += struct.pack(">Q", caller.owner_revision)
    return hashlib.sha256(bytes(buf)).hexdigest()


def sign_probe_message(secret: bytes, domain: bytes, message: Message) -> tuple[bytes, bytes]:
    if len(secret) < 32:
        raise ValueError("probe secret must be at least 32 bytes")
    payload = message.SerializeToString(deterministic=True)
    sig = hmac.digest(secret, domain + payload, "sha256")
    return payload, sig


def verify_probe_message(
    protocol: Protocol,
    secret: bytes,
    domain: bytes,
    signed: Message,
    message_type: str,
) -> Message:
    if len(secret) < 32 or signed is None:
        raise ValueError("invalid probe signature configuration")
    payload = bytes(signed.payload)
    signature = bytes(signed.signature)
    if not payload or len(payload) > 8192:
        raise ValueError("invalid probe payload size")
    expected = hmac.digest(secret, domain + payload, "sha256")
    if not hmac.compare_digest(expected, signature):
        raise ValueError("probe signature mismatch")
    try:
        return protocol.parse(message_type, payload)
    except DecodeError as exc:
        raise ValueError("probe payload decode failed") from exc


def probe_latencies(profile: Message, now_ms: int, hard_ms: int) -> dict[int, int]:
    out: dict[int, int] = {}
    for receipt in profile.receipts:
        grant = receipt.grant
        if (
            not grant.physical_region
            or receipt.expires_ms if hasattr(receipt, "expires_ms") else False
        ):
            continue
        if (
            receipt.expires_at_ms <= now_ms
            or grant.expires_at_ms < receipt.measured_at_ms
            or receipt.measured_at_ms > now_ms
            or receipt.sample_count < 3
            or receipt.rtt_ms == 0
            or receipt.rtt_ms > hard_ms
        ):
            continue
        out[int(grant.physical_region)] = int(receipt.rtt_ms)
    return out


def preferred_region(profile: Message, now_ms: int, hard_ms: int = (1 << 32) - 1) -> int:
    best_region = 0
    best_rtt = (1 << 32) - 1
    for region, rtt in probe_latencies(profile, now_ms, hard_ms).items():
        if best_region == 0 or rtt < best_rtt or (rtt == best_rtt and region < best_region):
            best_region, best_rtt = region, rtt
    return best_region


# -- 观战订阅凭证 HMAC（与 nodes/gate_spectate.py:88-94 逐字节一致）----------------------------------


def sign_spectate_ticket(
    protocol: Protocol,
    secret: bytes,
    *,
    battle_id: int,
    player_id: int,
    battle_instance_id: str,
    deadline_ms: int,
    expire_at_ms: int,
) -> bytes:
    if len(secret) < 32:
        raise ValueError("spectate token secret must be at least 32 bytes")
    if not battle_id or not player_id or not battle_instance_id or not deadline_ms or not expire_at_ms:
        raise ValueError("spectate ticket payload incomplete")
    payload_msg = protocol.message(
        "SpectateTicketPayload",
        battle_id=battle_id,
        player_id=player_id,
        battle_instance_id=battle_instance_id,
        deadline_ms=deadline_ms,
        expire_at_ms=expire_at_ms,
    )
    raw = payload_msg.SerializeToString(deterministic=True)
    sig = hmac.new(secret, raw, "sha256").hexdigest().encode("ascii")
    ticket = protocol.message("SpectateTicket", payload=raw, signature=sig)
    return ticket.SerializeToString(deterministic=True)


def verify_spectate_ticket(protocol: Protocol, secret: bytes, raw_ticket: bytes) -> Message | None:
    if len(secret) < 32 or not raw_ticket:
        return None
    try:
        ticket = protocol.parse("SpectateTicket", raw_ticket)
        expected = hmac.new(secret, bytes(ticket.payload), "sha256").hexdigest().encode("ascii")
        if not hmac.compare_digest(expected, bytes(ticket.signature)):
            return None
        return protocol.parse("SpectateTicketPayload", bytes(ticket.payload))
    except DecodeError:
        return None


# -- PlayerInbox 结算内容摘要（与 player_inbox/model.py:312-315 逐字节一致）--------------------------


def inbox_content_sha256(msg_type: int, issued_at_ms: int, payload: bytes) -> bytes:
    """计算收件箱条目内容摘要：sha256(be32(msg_type) || be64(issued_at_ms) || payload)。"""
    return hashlib.sha256(struct.pack(">IQ", msg_type, issued_at_ms) + payload).digest()


def pending_settlement_delay_ms(issued_ms: int, now_ms: int) -> int:
    """按原签发时间计算 5 秒至 5 分钟的有界退避。"""
    maximum = 5 * 60 * 1000
    delay = 5000
    if now_ms <= issued_ms:
        return delay
    age = now_ms - issued_ms
    while delay < maximum and age >= delay:
        age -= delay
        delay *= 2
    return min(delay, maximum)


# -- 终局原件与成局核对纯函数 ------------------------------------------------------------------------


# 道友切磋坊自定义规则白名单（与 docs/design/wuxing-gameplay-expansions-v1.md 对齐）
VALID_CUSTOM_DUEL_ROUND_LIMITS = frozenset((0, 8, 12, 20))
DEFAULT_CUSTOM_DUEL_CHARACTER_PRESET_ID = 101
DEFAULT_CUSTOM_DUEL_PET_PRESET_ID = 203


def has_custom_duel_rules(rules: Message | None) -> bool:
    """判定 CustomDuelRules 是否勾选了任意非默认自定义规则。"""
    if rules is None:
        return False
    return bool(
        getattr(rules, "use_free_preset", False)
        or getattr(rules, "enable_polarity_rotation", False)
        or int(getattr(rules, "round_limit_override", 0))
        or getattr(rules, "ban_control_skills", False)
        or getattr(rules, "allow_public_spectate", False)
        or int(getattr(rules, "challenger_character_preset_id", 0))
        or int(getattr(rules, "challenger_pet_preset_id", 0))
        or int(getattr(rules, "skill_ban_slots_per_team", 0))
    )


def validate_custom_duel_rules(rules: Message | None) -> bool:
    """校验道友切磋坊自定义规则白名单（回合上限 0/8/12/20；禁招槽位仅 0 或 3 且与免费预设互斥）。"""
    if rules is None:
        return True
    if int(getattr(rules, "round_limit_override", 0)) not in VALID_CUSTOM_DUEL_ROUND_LIMITS:
        return False
    ban_slots = int(getattr(rules, "skill_ban_slots_per_team", 0))
    if ban_slots not in (0, 3):
        return False
    if ban_slots > 0 and bool(getattr(rules, "use_free_preset", False)):
        return False
    return True


def spectate_visibility_for(
    mode: int,
    has_activity_context: bool,
    *,
    allow_public_spectate: bool = False,
) -> int:
    """判定开局的观战可见性：帮战、锦标赛与天墉生死状公开；自定义切磋按 allow_public_spectate 冻结；活动与单人 PVE 私密。"""
    if mode in GUILD_WAR_MODES or mode in (MODE_SEASON_TOURNAMENT, MODE_JIANGHU_DUEL):
        return SPECTATE_VISIBILITY_PUBLIC
    if mode == MODE_PVP_CHALLENGE:
        return SPECTATE_VISIBILITY_PUBLIC if allow_public_spectate else SPECTATE_VISIBILITY_PRIVATE
    if has_activity_context or mode == MODE_PVE_SOLO:
        return SPECTATE_VISIBILITY_PRIVATE
    return SPECTATE_VISIBILITY_PUBLIC


def record_replay_for(mode: int, *, trial_journal_entry_id: int = 0) -> bool:
    """PVP（含天墉生死状）默认录制战报回放；试炼志 V3 有格 PVE 受控开放录制。"""
    if mode in PVP_MODES:
        return True
    if mode in (MODE_PVE_SOLO, MODE_PVE_TEAM) and int(trial_journal_entry_id) > 0:
        return True
    return False


def valid_trial_vow_mask(mask: int) -> bool:
    """校验试炼誓约位掩码仅包含合法的 bit0..3（0..0x0F）。"""
    return (int(mask) & ~TRIAL_VOW_MASK_ALL) == 0


def is_permanent_boss_trial_kind(boss_trial_kind: int) -> bool:
    """判定 Dungeon.boss_trial_kind 是否属于支持试炼誓约的常驻首领试炼（卷一/卷二/卷四）。"""
    kind = int(boss_trial_kind)
    return (1 <= kind <= 4) or kind == 8 or (12 <= kind <= 14)


def formation_outcome_decided(formation: Message) -> bool:
    return int(formation.state) == FORMATION_STATE_ABORTED or (
        int(formation.state) == FORMATION_STATE_CREATED
        and int(formation.retry_at_ms) == TERMINAL_OUTCOME_NO_RETRY_MS
    )


def original_capability_check(formation: Message | None) -> str:
    if formation is None:
        return "formation_missing"
    if int(formation.outcome_protocol_version) != 1:
        return "outcome_protocol"
    if int(formation.preflight_supported_version) != 1:
        return "preflight_version"
    if not formation.target_instance_id:
        return "target_instance_missing"
    if formation.preflight_instance_id != formation.target_instance_id:
        return "preflight_instance_mismatch"
    if formation.adopted_outcome_observed and int(formation.adopted_outcome_protocol_version) != 1:
        return "adopted_legacy"
    if not formation.adopted_outcome_observed and int(formation.adopted_outcome_protocol_version) != 0:
        return "adopted_invalid"
    return ""


def decode_outcome_original(protocol: Protocol, formation: Message | None) -> tuple[Message | None, str]:
    if formation is None:
        return None, "formation_missing"
    if not formation.create_request:
        return None, "create_missing"
    try:
        req = protocol.parse("CreateBattleRequest", bytes(formation.create_request))
    except DecodeError:
        return None, "create_undecodable"
    if int(req.outcome_protocol_version) != 1:
        return req, "create_protocol"
    if int(req.battle_id) != int(formation.battle_id):
        return req, "battle_id_mismatch"
    if req.placement_operation_id != formation.operation_id:
        return req, "operation_mismatch"
    if req.target_instance_id != formation.target_instance_id:
        return req, "target_instance_mismatch"
    if int(req.battle_config_id) != int(formation.config_id):
        return req, "config_mismatch"
    if int(req.match_mode) != int(formation.mode):
        return req, "mode_mismatch"
    if int(req.deadline_ms) == 0:
        return req, "deadline_missing"
    if int(req.deadline_ms) != int(formation.deadline_ms):
        return req, "deadline_mismatch"
    if int(req.created_at_ms) < int(formation.created_ms):
        return req, "created_before_formation"
    if int(req.deadline_ms) <= int(req.created_at_ms):
        return req, "created_after_deadline"
    human_players = [p for p in req.players if not getattr(p, "ai_controlled", False)]
    if len(human_players) != len(formation.members):
        return req, "players_count_mismatch"
    return req, ""


def completable_original(protocol: Protocol, formation: Message) -> tuple[Message, dict[int, int]]:
    """校验成局行与持久 CreateBattleRequest 能否被 COMPLETE 裁决，返回 (CreateBattleRequest, player_id -> team_index)。"""
    cap = original_capability_check(formation)
    if cap:
        raise ValueError(f"capability check failed: {cap}")
    req, check = decode_outcome_original(protocol, formation)
    if check or req is None:
        raise ValueError(f"create original mismatch: {check}")
    teams: dict[int, int] = {}
    for player in req.players:
        if getattr(player, "ai_controlled", False):
            continue
        pid = int(player.player_id)
        if pid == 0 or pid in teams:
            raise ValueError("duplicate or empty player in create original")
        teams[pid] = int(player.team_index)
    if len(teams) != len(formation.members):
        raise ValueError("member count mismatch")
    seen: set[int] = set()
    for member in formation.members:
        pid = int(member.player_id)
        if pid not in teams or pid in seen:
            raise ValueError("formation member missing from create original")
        seen.add(pid)
    return req, teams


def validate_terminal_payload(protocol: Protocol, raw_payload: bytes) -> Message:
    """不碰存储的终局载荷纯校验（960KiB、v1 语义、个人结算与逃跑/阵亡索引一致、单人 <= 16KiB）。"""
    if not raw_payload or len(raw_payload) > MAX_COMPLETION_BYTES:
        raise ValueError("raw_result_payload empty or exceeds 960KiB")
    try:
        event = protocol.parse("contracts.kafka.BattleResultEvent", raw_payload)
    except DecodeError as exc:
        raise ValueError("cannot decode BattleResultEvent") from exc
    if int(event.settlement_format_version) != 1:
        raise ValueError("settlement_format_version must be 1")
    if int(event.battle_id) == 0 or not 1 <= int(event.outcome) <= 3:
        raise ValueError("invalid battle_id or outcome")
    if int(event.finished_at_ms) == 0:
        raise ValueError("finished_at_ms must be non-zero")

    members: dict[int, int] = {}
    teams_seen: set[int] = set()
    for team in event.teams:
        tid = int(team.team_index)
        if tid in teams_seen or not team.player_ids:
            raise ValueError("duplicate or empty team in BattleResultEvent")
        teams_seen.add(tid)
        for pid in team.player_ids:
            p = int(pid)
            if p == 0 or p in members:
                raise ValueError("duplicate or empty player in BattleResultEvent")
            members[p] = tid

    if not members or len(event.settlements) != len(members):
        raise ValueError("settlements must cover every participant")

    seen: set[int] = set()
    want_fled: set[int] = set()
    want_dead: set[int] = set()
    for s in event.settlements:
        pid = int(s.player_id)
        if pid == 0 or pid in seen or pid not in members:
            raise ValueError("settlement player invalid or duplicate")
        if (
            int(s.battle_id) != int(event.battle_id)
            or int(s.outcome) != int(event.outcome)
            or int(s.player_team_index) != members[pid]
            or int(s.total_rounds) != int(event.total_rounds)
        ):
            raise ValueError("settlement header disagrees with BattleResultEvent")
        body = s.SerializeToString(deterministic=True)
        if len(body) > MAX_INBOX_BATTLE_PAYLOAD_BYTES:
            raise ValueError("personal settlement exceeds 16KiB inbox cap")
        seen.add(pid)
        if s.fled:
            want_fled.add(pid)
        if s.is_dead:
            want_dead.add(pid)

    got_fled = [int(x) for x in event.fled_player_ids]
    got_dead = [int(x) for x in event.dead_player_ids]
    if len(got_fled) != len(set(got_fled)) or set(got_fled) != want_fled:
        raise ValueError("fled_player_ids mismatch")
    if len(got_dead) != len(set(got_dead)) or set(got_dead) != want_dead:
        raise ValueError("dead_player_ids mismatch")
    return event


def validate_terminal_against_formation(
    protocol: Protocol,
    formation: Message,
    event: Message,
) -> Message:
    """将已通过纯校验的 BattleResultEvent 与持久成局原件逐项核对，返回 CreateBattleRequest。"""
    create_req, member_teams = completable_original(protocol, formation)
    if (
        int(event.battle_id) != int(create_req.battle_id)
        or int(event.battle_config_id) != int(create_req.battle_config_id)
        or int(event.match_mode) != int(create_req.match_mode)
    ):
        raise ValueError("result metadata disagrees with create original")
    if event.activity_context.SerializeToString(deterministic=True) != create_req.activity_context.SerializeToString(
        deterministic=True
    ):
        raise ValueError("result activity_context disagrees with create original")
    if len(event.settlements) != len(member_teams):
        raise ValueError("result player set disagrees with create original")
    for s in event.settlements:
        pid = int(s.player_id)
        if member_teams.get(pid) != int(s.player_team_index):
            raise ValueError("result player team disagrees with create original")
    return create_req


def formation_digest(formation: Message) -> str:
    return hashlib.sha256(formation.SerializeToString(deterministic=True)).hexdigest()


def effective_deadline_ms(formation: Message, create_req: Message | None) -> int:
    values = [int(formation.deadline_ms)]
    if create_req is not None:
        values.append(int(create_req.deadline_ms))
        values.append(int(create_req.admit_before_ms))
    return max(values)


def classify_stranded_formation(
    protocol: Protocol,
    formation: Message,
) -> tuple[int, str, Message | None]:
    """判定滞留成局分类：(StrandedFormationClass, failed_check, decoded_create_req)。"""
    cap = original_capability_check(formation)
    create_req, check = decode_outcome_original(protocol, formation)
    if cap:
        return 3, cap, create_req  # SHAPE_INVALID
    if check:
        return 2, check, create_req  # ORIGINAL_MISMATCH
    try:
        completable_original(protocol, formation)
    except ValueError:
        return 2, "member_mismatch", create_req
    return 1, "", create_req  # AUTO


def build_battle_report_summary(
    protocol: Protocol,
    create_req: Message,
    event: Message,
    *,
    has_replay: bool,
) -> Message:
    summary = protocol.message(
        "match.BattleReportSummary",
        battle_id=int(event.battle_id),
        mode=int(event.match_mode),
        battle_config_id=int(event.battle_config_id),
        finished_at_ms=int(event.finished_at_ms),
        outcome=int(event.outcome),
        total_rounds=int(event.total_rounds),
        has_replay=has_replay,
    )
    ordered = sorted(create_req.players, key=lambda p: (int(p.team_index), int(p.player_id)))
    for player in ordered:
        summary.members.add(
            player_id=int(player.player_id),
            team_index=int(player.team_index),
            name=player.player_name,
            level=int(player.level),
            class_id=int(player.class_id),
        )
    return summary


def unique_ints(values: Iterable[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for v in values:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out
