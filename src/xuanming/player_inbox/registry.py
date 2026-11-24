"""基于 proto 描述符扩展的玩家收件箱类型注册表与载荷校验（与 Go registry.go 对齐）。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from google.protobuf.message import DecodeError, Message

from ..data.asset_inventory import carries_unknown
from ..protocol import Protocol
from .errors import (
    BattleSettlementOriginalError,
    ERR_DISABLED,
    ERR_PAYLOAD_INVALID,
    ERR_PAYLOAD_TOO_LARGE,
    ERR_UNKNOWN_TYPE,
)
from .model import (
    CLASS_DURABLE,
    INT64_MAX,
    MAX_ACCOUNT_BYTES,
    MAX_DEBT_EXPIRES_AT_SECONDS,
    MAX_OPERATOR_BYTES,
    MAX_PAYLOAD_BYTES_HARD_CAP,
    MAX_REASON_BYTES,
    MSG_BATTLE_SETTLEMENT,
    MSG_GM_ATTACH_CURRENCY_DEBT,
    MSG_GM_BLOCK_CURRENCY_GAIN,
    MSG_GM_BLOCK_ITEM_GAIN,
    MSG_GM_UNBLOCK_CURRENCY_GAIN,
    MSG_GM_UNBLOCK_ITEM_GAIN,
    MSG_UNSPECIFIED,
    validate_text,
)

# v1 禁止放入 EnqueueTypes 白名单的消息类型（欠款合并与撤销语义未就绪）。
V1_DISABLED_ENQUEUE_TYPES: dict[int, str] = {
    MSG_GM_ATTACH_CURRENCY_DEBT: "debt merge semantics and undo path are not ready in v1",
}

# BATTLE_OUTCOME_VOID 允许携带的展示/身份字段白名单，禁止携带任何业务效果字段。
VOID_SETTLEMENT_FIELDS: frozenset[str] = frozenset((
    "battle_id",
    "player_id",
    "outcome",
    "player_team_index",
    "finished_at_ms",
    "match_mode",
    "battle_config_id",
))

_BATTLE_OUTCOME_SIDE_A_WIN = 1
_BATTLE_OUTCOME_DRAW = 3
_BATTLE_OUTCOME_VOID = 4


@dataclass(frozen=True, slots=True)
class DomainSpec:
    domain: int
    max_pending: int
    requires_out_of_battle: bool


@dataclass(frozen=True, slots=True)
class TypeEntry:
    msg_type: int
    domain: int
    klass: int
    payload_message: str
    max_payload_bytes: int
    message_cls: type[Message]
    validator: Callable[[Message, frozenset[int]], bool]


def _validate_gain_payload(msg: Message, currency_types: frozenset[int]) -> bool:
    return int(msg.currency_type) in currency_types


def _validate_item_gain_payload(msg: Message, _currency_types: frozenset[int]) -> bool:
    return int(msg.item_config_id) != 0


def _validate_debt_payload(msg: Message, currency_types: frozenset[int]) -> bool:
    if int(msg.currency_type) not in currency_types:
        return False
    if not 0 < int(msg.owe_amount) <= INT64_MAX:
        return False
    if validate_text(msg.reason, MAX_REASON_BYTES, required=False) != 0:
        return False
    if validate_text(msg.gm_operator, MAX_OPERATOR_BYTES, required=True) != 0:
        return False
    return int(msg.expires_at) <= MAX_DEBT_EXPIRES_AT_SECONDS


def void_settlement_has_no_effects(msg: Message) -> bool:
    """作废结算只能携带身份、结局和展示字段，任何白名单外字段一律拒绝。"""
    return all(field.name in VOID_SETTLEMENT_FIELDS for field, _ in msg.ListFields())


def _validate_battle_payload(msg: Message, _currency_types: frozenset[int]) -> bool:
    if int(msg.player_id) == 0 or int(msg.battle_id) == 0 or carries_unknown(msg):
        return False
    outcome = int(msg.outcome)
    if _BATTLE_OUTCOME_SIDE_A_WIN <= outcome <= _BATTLE_OUTCOME_DRAW:
        return True
    if outcome == _BATTLE_OUTCOME_VOID:
        return void_settlement_has_no_effects(msg)
    return False


_PAYLOAD_RULES: dict[int, tuple[str, Callable[[Message, frozenset[int]], bool]]] = {
    MSG_GM_BLOCK_CURRENCY_GAIN: ("player_inbox.GmCurrencyGainPayload", _validate_gain_payload),
    MSG_GM_UNBLOCK_CURRENCY_GAIN: ("player_inbox.GmCurrencyGainPayload", _validate_gain_payload),
    MSG_GM_ATTACH_CURRENCY_DEBT: ("player_inbox.GmCurrencyDebtPayload", _validate_debt_payload),
    MSG_BATTLE_SETTLEMENT: ("BattleSettlementData", _validate_battle_payload),
    MSG_GM_BLOCK_ITEM_GAIN: ("player_inbox.GmItemGainPayload", _validate_item_gain_payload),
    MSG_GM_UNBLOCK_ITEM_GAIN: ("player_inbox.GmItemGainPayload", _validate_item_gain_payload),
}


def _enum_option(protocol: Protocol, enum_value, ext_descriptor):
    opts = enum_value.GetOptions()
    cls = protocol.message_class(opts.DESCRIPTOR.full_name)
    parsed = cls.FromString(opts.SerializeToString())
    return parsed.Extensions[ext_descriptor] if parsed.HasExtension(ext_descriptor) else None


class InboxRegistry:
    """只读收件箱类型注册表，直接从 schema.pb 的 proto 选项自检构建。"""

    def __init__(self, protocol: Protocol) -> None:
        self.protocol = protocol
        currency_enum = protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
        self.currency_types: frozenset[int] = frozenset(currency_enum.values_by_number)

        dom_ext = protocol.pool.FindExtensionByName("player_inbox.OptionPlayerInboxDomain")
        type_ext = protocol.pool.FindExtensionByName("player_inbox.OptionPlayerInboxType")
        dom_enum = protocol.pool.FindEnumTypeByName("player_inbox.PlayerInboxDomain")
        type_enum = protocol.pool.FindEnumTypeByName("player_inbox.PlayerInboxMessageType")

        self._type_name_to_num: dict[str, int] = {v.name: v.number for v in type_enum.values}
        self._domains: dict[int, DomainSpec] = {}
        for value in dom_enum.values:
            if value.number == 0:
                continue
            spec = _enum_option(protocol, value, dom_ext)
            if spec is None or spec.max_pending == 0:
                raise ValueError(f"inbox registry: domain {value.name} missing valid OptionPlayerInboxDomain")
            self._domains[value.number] = DomainSpec(
                domain=value.number,
                max_pending=int(spec.max_pending),
                requires_out_of_battle=bool(spec.requires_out_of_battle),
            )

        self._types: dict[int, TypeEntry] = {}
        for value in type_enum.values:
            if value.number == 0:
                continue
            spec = _enum_option(protocol, value, type_ext)
            if spec is None:
                raise ValueError(f"inbox registry: type {value.name} missing OptionPlayerInboxType")
            if spec.domain not in self._domains:
                raise ValueError(f"inbox registry: type {value.name} references unregistered domain {spec.domain}")
            if spec.klass != CLASS_DURABLE:
                raise ValueError(f"inbox registry: type {value.name} must be DURABLE")
            if not 0 < spec.max_payload_bytes <= MAX_PAYLOAD_BYTES_HARD_CAP:
                raise ValueError(f"inbox registry: type {value.name} invalid max_payload_bytes={spec.max_payload_bytes}")
            msg_cls = protocol.message_class(spec.payload_message)
            rule = _PAYLOAD_RULES.get(value.number)
            if rule is None:
                raise ValueError(f"inbox registry: type {value.name} has no payload validator")
            expected_name, validator = rule
            if expected_name != msg_cls.DESCRIPTOR.full_name:
                raise ValueError(
                    f"inbox registry: type {value.name} expects {expected_name}, spec has {msg_cls.DESCRIPTOR.full_name}"
                )
            self._types[value.number] = TypeEntry(
                msg_type=value.number,
                domain=int(spec.domain),
                klass=int(spec.klass),
                payload_message=spec.payload_message,
                max_payload_bytes=int(spec.max_payload_bytes),
                message_cls=msg_cls,
                validator=validator,
            )

        for msg_type in _PAYLOAD_RULES:
            if msg_type not in self._types:
                raise ValueError(f"inbox registry: payload validator registered for unknown type {msg_type}")
        if not self._types:
            raise ValueError("inbox registry: no message types registered")

    def domain_spec(self, domain: int) -> DomainSpec | None:
        return self._domains.get(domain)

    def domains(self) -> tuple[int, ...]:
        return tuple(sorted(self._domains))

    def type_entry(self, msg_type: int) -> TypeEntry | None:
        return self._types.get(msg_type)

    def parse_enqueue_types(self, names: tuple[str, ...] | list[str]) -> frozenset[int]:
        """解析 EnqueueTypes 白名单：拒绝未知名、UNSPECIFIED、重复及 v1 禁用的类型。"""
        out: set[int] = set()
        for name in names:
            num = self._type_name_to_num.get(name)
            if num is None:
                raise ValueError(f"unknown PlayerInboxMessageType in enqueue_types: {name!r}")
            if num == MSG_UNSPECIFIED:
                raise ValueError("enqueue_types must not contain PLAYER_INBOX_MSG_UNSPECIFIED")
            if num in V1_DISABLED_ENQUEUE_TYPES:
                raise ValueError(f"enqueue_types must not contain {name}: {V1_DISABLED_ENQUEUE_TYPES[num]}")
            if num in out:
                raise ValueError(f"duplicate enqueue_types entry: {name!r}")
            if num not in self._types:
                raise ValueError(f"unregistered type in enqueue_types: {name!r}")
            out.add(num)
        return frozenset(out)

    def check_allowlist(self, allowed: frozenset[int]) -> None:
        for msg_type in allowed:
            if msg_type in V1_DISABLED_ENQUEUE_TYPES:
                raise ValueError(f"type {msg_type} is disabled in v1: {V1_DISABLED_ENQUEUE_TYPES[msg_type]}")
            if msg_type not in self._types:
                raise ValueError(f"unregistered type {msg_type} in allowlist")

    def decode(self, entry: TypeEntry, payload: bytes) -> Message:
        """按类型规格解码并拒绝未知字段（fail-closed）。"""
        msg = entry.message_cls()
        msg.ParseFromString(payload)
        if carries_unknown(msg):
            raise DecodeError(f"payload {entry.payload_message} carries unknown fields")
        return msg

    def validate_payload(
        self,
        msg_type: int,
        allowed: frozenset[int],
        payload: bytes,
    ) -> tuple[TypeEntry | None, int]:
        """入箱注册表校验：已登记持久类 -> 白名单 -> 大小 -> 解码（无未知字段） -> 业务规则。"""
        entry = self._types.get(msg_type)
        if entry is None or entry.klass != CLASS_DURABLE:
            return None, ERR_UNKNOWN_TYPE
        if msg_type not in allowed:
            return None, ERR_DISABLED
        if len(payload) > entry.max_payload_bytes:
            return None, ERR_PAYLOAD_TOO_LARGE
        try:
            msg = self.decode(entry, payload)
        except DecodeError:
            return None, ERR_PAYLOAD_INVALID
        if not entry.validator(msg, self.currency_types):
            return None, ERR_PAYLOAD_INVALID
        return entry, 0

    def validate_battle_original(self, req: Message) -> None:
        """校验 Match 结算原件完整性与 BattleSettlementData 语义，不通过抛 BattleSettlementOriginalError。"""
        entry = self._types.get(MSG_BATTLE_SETTLEMENT)
        if entry is None or req is None:
            raise BattleSettlementOriginalError("battle settlement spec unavailable")
        if (
            int(req.player_id) == 0
            or int(req.battle_id) == 0
            or validate_text(req.expected_owner_account, MAX_ACCOUNT_BYTES, required=True) != 0
            or int(req.expected_owner_revision) == 0
            or int(req.issued_at_ms) == 0
            or len(req.settlement_payload) == 0
            or len(req.settlement_payload) > entry.max_payload_bytes
            or carries_unknown(req)
        ):
            raise BattleSettlementOriginalError("battle settlement original incomplete")
        try:
            settlement = self.decode(entry, req.settlement_payload)
        except DecodeError as exc:
            raise BattleSettlementOriginalError("battle settlement payload undecodable") from exc
        if int(settlement.player_id) != int(req.player_id) or int(settlement.battle_id) != int(req.battle_id):
            raise BattleSettlementOriginalError("battle settlement payload identity mismatch")
        if not _validate_battle_payload(settlement, self.currency_types):
            raise BattleSettlementOriginalError("battle settlement payload invalid")
