"""玩家域 Redis 键构造器，与 Go ``shared/redisslot`` 及 ``player_locator/internal/logic/keys.go`` 逐字节一致。

所有会话、租约与 gate 索引键按玩家所在 Redis Cluster slot 对齐，确保多键 Lua 同槽执行。
"""

from __future__ import annotations

from dataclasses import dataclass

from ..login.keys import SLOT_COUNT, crc16, slot_tag

SESSION_STATE_UNKNOWN = 0
SESSION_STATE_ONLINE = 1
SESSION_STATE_DISCONNECTING = 2
SESSION_STATE_OFFLINE = 3

LEASE_KEY_PREFIX = "player:leases:"
GATE_INDEX_KEY_PREFIX = "player:gate_index:"


@dataclass(frozen=True, slots=True)
class LeaseKeys:
    """单个 Redis Cluster slot 上的四把租约键，共用同一个 slot-tag。"""

    ready: str
    processing: str
    claim_tokens: str
    claim_payloads: str


def hash_slot(key: str) -> int:
    """按 Redis Cluster hash-tag 规范计算键的 slot。"""
    open_idx = key.find("{")
    if open_idx >= 0:
        close_idx = key.find("}", open_idx + 1)
        if close_idx > open_idx + 1:
            return crc16(key[open_idx + 1:close_idx]) & (SLOT_COUNT - 1)
    return crc16(key) & (SLOT_COUNT - 1)


def player_slot(player_id: int) -> int:
    """计算玩家 ID 对应的 Redis Cluster slot。"""
    return crc16(str(player_id)) & (SLOT_COUNT - 1)


def slot_index_tag(slot: int) -> str:
    """返回含花括号的 slot-tag，与该 slot 下所有 {<pid>} 键同槽。"""
    return f"{{{slot_tag(slot)}}}"


def session_key(player_id: int) -> str:
    return f"player:session:{{{player_id}}}"


def location_key(player_id: int) -> str:
    return f"player:{{{player_id}}}:location"


def afk_pass_key(player_id: int) -> str:
    return f"player:afk_pass:{{{player_id}}}"


def hosted_presence_key(player_id: int) -> str:
    """离线托管角色在场心跳键（与 Go redisslot.HostedPresenceKey 逐字一致）。"""
    return f"player:{{{player_id}}}:hosted_presence"


def hosted_return_key(player_id: int) -> str:
    """离线托管归位信息键（与 Go redisslot.HostedReturnKey 逐字一致）。"""
    return f"player:{{{player_id}}}:hosted_return"


def lease_keys_for_slot(slot: int) -> LeaseKeys:
    ready = LEASE_KEY_PREFIX + slot_index_tag(slot)
    return LeaseKeys(
        ready=ready,
        processing=f"{ready}:processing",
        claim_tokens=f"{ready}:claim_tokens",
        claim_payloads=f"{ready}:claim_payloads",
    )


def lease_keys_for_player(player_id: int) -> LeaseKeys:
    return lease_keys_for_slot(player_slot(player_id))


def gate_registry_key(slot: int) -> str:
    return f"{GATE_INDEX_KEY_PREFIX}{slot_index_tag(slot)}:gates"


def gate_session_index_key(slot: int, gate_uuid: str) -> str:
    return f"{GATE_INDEX_KEY_PREFIX}{slot_index_tag(slot)}:sessions:{gate_uuid}"


def gate_index_key_or_placeholder(slot: int, gate_uuid: str) -> str:
    """无索引一侧用同槽登记集占位，避免空键名落到 slot 0 触发 CROSSSLOT。"""
    return gate_registry_key(slot) if not gate_uuid else gate_session_index_key(slot, gate_uuid)


def indexed_gate(session) -> str:
    """不变量 I1：仅 ONLINE 且 gate_instance_id 非空的会话才属于 gate 索引。"""
    if session is None or session.state != SESSION_STATE_ONLINE:
        return ""
    return session.gate_instance_id


def parse_index_member(member: str) -> int | None:
    """严格解析规范十进制非零 uint64 玩家 ID（拒绝前导零与 0）。"""
    if not member or not member.isascii() or not member.isdigit() or member[0] == "0":
        return None
    value = int(member)
    if value <= 0 or value >= (1 << 64):
        return None
    return value


def session_lifecycle_keys(player_id: int) -> tuple[str, str, str, str, str]:
    lease = lease_keys_for_player(player_id)
    return (session_key(player_id), lease.ready, lease.processing, lease.claim_tokens, lease.claim_payloads)


def replace_session_keys(player_id: int, previous_gate: str, next_gate: str) -> tuple[str, ...]:
    slot = player_slot(player_id)
    return (
        *session_lifecycle_keys(player_id),
        gate_registry_key(slot),
        gate_index_key_or_placeholder(slot, previous_gate),
        gate_index_key_or_placeholder(slot, next_gate),
    )


def set_disconnecting_keys(player_id: int, gate: str) -> tuple[str, ...]:
    slot = player_slot(player_id)
    return (*session_lifecycle_keys(player_id), gate_registry_key(slot), gate_index_key_or_placeholder(slot, gate))


def delete_session_keys(player_id: int, gate: str) -> tuple[str, ...]:
    return set_disconnecting_keys(player_id, gate)


def claim_expired_leases_keys(slot: int) -> tuple[str, str, str]:
    lease = lease_keys_for_slot(slot)
    return (lease.ready, lease.processing, lease.claim_tokens)


def pin_lease_claim_payload_keys(player_id: int) -> tuple[str, str, str]:
    lease = lease_keys_for_player(player_id)
    return (session_key(player_id), lease.claim_tokens, lease.claim_payloads)


def ack_lease_claim_keys(player_id: int) -> tuple[str, str, str]:
    lease = lease_keys_for_player(player_id)
    return (lease.processing, lease.claim_tokens, lease.claim_payloads)


def renew_lease_claims_keys(slot: int) -> tuple[str, str]:
    lease = lease_keys_for_slot(slot)
    return (lease.processing, lease.claim_tokens)


def enqueue_offline_cleanup_keys(player_id: int) -> tuple[str, str, str]:
    lease = lease_keys_for_player(player_id)
    return (session_key(player_id), lease.ready, lease.claim_payloads)


def prune_gate_index_member_keys(player_id: int, gate: str) -> tuple[str, str]:
    return (session_key(player_id), gate_session_index_key(player_slot(player_id), gate))


def release_gate_index_keys(slot: int, gate: str) -> tuple[str, str]:
    return (gate_session_index_key(slot, gate), gate_registry_key(slot))
