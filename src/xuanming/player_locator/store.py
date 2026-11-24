"""player_locator 存储抽象：RedisSessionStore（10 段 Lua 原子脚本）与 MemorySessionStore（同构内存孪生）。"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import IntEnum
from typing import Protocol

from . import keys, lua

logger = logging.getLogger(__name__)

_LEASE_PROBE_SLOTS_PER_PIPELINE = 64


class ReplaceSessionResult(IntEnum):
    CONFLICT = 0
    APPLIED = 1
    CLEANUP_PENDING = 2


class CommitLeaseResult(IntEnum):
    CLAIM_LOST = 0
    COMMITTED = 1
    SESSION_REPLACED = 2


class RearmLeaseResult(IntEnum):
    CLAIM_LOST = 0
    REARMED = 1
    SESSION_REPLACED = 2


class AfkPassState(IntEnum):
    INACTIVE = 0
    ACTIVE = 1
    UNKNOWN = 2


@dataclass(frozen=True, slots=True)
class LeaseClaim:
    """已认领并钉住载荷的到期租约条目。"""

    player_id: int
    token: str
    payload: bytes


class SessionStore(Protocol):
    async def get_session_bytes(self, player_id: int) -> bytes | None: ...
    async def get_session_and_location_bytes(self, player_id: int) -> tuple[bytes | None, bytes | None]: ...
    async def get_location_bytes(self, player_id: int) -> bytes | None: ...
    async def get_afk_pass_bytes(self, player_id: int) -> bytes | None: ...
    async def get_hosted_presence(self, player_id: int) -> bytes | None: ...
    async def get_hosted_return(self, player_id: int) -> bytes | None: ...
    async def delete_hosted_return_if_value(self, player_id: int, expected: bytes | str) -> bool: ...
    async def expire_hosted_return_if_value(self, player_id: int, expected: bytes | str, ttl_seconds: int) -> bool: ...
    async def list_registered_gates(self, slots: Sequence[int]) -> list[tuple[int, str]]: ...
    async def scan_gate_index_members(
        self, slot: int, gate_uuid: str, cursor: int = 0, count: int = 100,
    ) -> tuple[int, list[str]]: ...
    async def mget_sessions_bytes(self, player_ids: Sequence[int]) -> list[bytes | None]: ...
    async def remove_bad_gate_index_member(self, slot: int, gate_uuid: str, member: str) -> bool: ...
    async def prune_gate_index_member(
        self, player_id: int, gate_uuid: str, member: str, expected: bytes | None,
    ) -> bool: ...
    async def release_gate_index_if_empty(self, slot: int, gate_uuid: str) -> bool: ...
    async def replace_session_and_cancel_lease(
        self, player_id: int, expected: bytes | None, updated: bytes, ttl_ms: int,
        previous_gate: str, next_gate: str,
    ) -> ReplaceSessionResult: ...
    async def transition_to_disconnecting(
        self, player_id: int, expected: bytes, updated: bytes, deadline_unix_sec: int, previous_gate: str,
    ) -> bool: ...
    async def delete_session_if_unchanged(
        self, player_id: int, expected: bytes | None, gate: str,
    ) -> bool: ...
    async def enqueue_offline_cleanup(self, player_id: int, payload: bytes, ready_at_unix_sec: int) -> bool: ...
    async def probe_due_lease_slots(self, slots: Sequence[int], now_unix_sec: int) -> list[int]: ...
    async def claim_expired_leases(
        self, slot: int, now_unix_sec: int, claim_deadline_unix_sec: int, batch_size: int,
        batch_token: str | None = None,
    ) -> list[LeaseClaim]: ...
    async def pin_lease_claim_payload(self, player_id: int, token: str) -> bytes | None: ...
    async def commit_lease_expiry(self, claim: LeaseClaim) -> CommitLeaseResult: ...
    async def ack_lease_claim(self, claim: LeaseClaim) -> bool: ...
    async def renew_lease_claims(self, claims: Sequence[LeaseClaim], deadline_unix_sec: int) -> int: ...
    async def rearm_lease_claim(self, claim: LeaseClaim, deadline_unix_sec: int) -> RearmLeaseResult: ...


def open_player_redis(url: str, *, cluster: bool = False):
    """打开玩家域 Redis 异步客户端（decode_responses=False，保留原始 protobuf 字节）。"""
    from redis.asyncio import Redis, RedisCluster

    redis_cls = RedisCluster if cluster else Redis
    return redis_cls.from_url(url, decode_responses=False, socket_connect_timeout=3, socket_timeout=3)


def _as_bytes_or_none(value) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value).encode("utf-8")


def _as_text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


class RedisSessionStore:
    """基于 redis.asyncio 与 10 段核心 Lua 脚本的玩家域会话与租约存储。"""

    def __init__(self, redis_client) -> None:
        self.redis = redis_client
        self._replace_script = redis_client.register_script(lua.REPLACE_SESSION_AND_CANCEL_LEASE_LUA)
        self._set_disconnecting_script = redis_client.register_script(lua.SET_DISCONNECTING_LUA)
        self._delete_script = redis_client.register_script(lua.DELETE_SESSION_IF_UNCHANGED_LUA)
        self._claim_script = redis_client.register_script(lua.CLAIM_EXPIRED_LEASES_LUA)
        self._pin_script = redis_client.register_script(lua.PIN_LEASE_CLAIM_PAYLOAD_LUA)
        self._commit_script = redis_client.register_script(lua.COMMIT_LEASE_EXPIRY_LUA)
        self._ack_script = redis_client.register_script(lua.ACK_LEASE_CLAIM_LUA)
        self._renew_script = redis_client.register_script(lua.RENEW_LEASE_CLAIMS_LUA)
        self._rearm_script = redis_client.register_script(lua.REARM_LEASE_CLAIM_LUA)
        self._enqueue_script = redis_client.register_script(lua.ENQUEUE_OFFLINE_CLEANUP_LUA)
        self._extra_scripts: dict[str, object] = {}

    def _lazy_script(self, name: str, source: str):
        script = self._extra_scripts.get(name)
        if script is None:
            script = self.redis.register_script(source)
            self._extra_scripts[name] = script
        return script

    async def get_session_bytes(self, player_id: int) -> bytes | None:
        return _as_bytes_or_none(await self.redis.get(keys.session_key(player_id)))

    async def get_session_and_location_bytes(self, player_id: int) -> tuple[bytes | None, bytes | None]:
        values = await self.redis.mget(keys.session_key(player_id), keys.location_key(player_id))
        if len(values) != 2:
            raise RuntimeError(f"unexpected mget reply length {len(values)} for player {player_id}")
        return _as_bytes_or_none(values[0]), _as_bytes_or_none(values[1])

    async def get_location_bytes(self, player_id: int) -> bytes | None:
        return _as_bytes_or_none(await self.redis.get(keys.location_key(player_id)))

    async def get_afk_pass_bytes(self, player_id: int) -> bytes | None:
        return _as_bytes_or_none(await self.redis.get(keys.afk_pass_key(player_id)))

    async def get_hosted_presence(self, player_id: int) -> bytes | None:
        return _as_bytes_or_none(await self.redis.get(keys.hosted_presence_key(player_id)))

    async def get_hosted_return(self, player_id: int) -> bytes | None:
        return _as_bytes_or_none(await self.redis.get(keys.hosted_return_key(player_id)))

    async def delete_hosted_return_if_value(self, player_id: int, expected: bytes | str) -> bool:
        script = self._lazy_script("delete_hosted_return", lua.DELETE_HOSTED_RETURN_IF_VALUE_LUA)
        expected_bytes = expected.encode("utf-8") if isinstance(expected, str) else expected
        result = await script(keys=[keys.hosted_return_key(player_id)], args=[expected_bytes])
        return int(result) == 1

    async def expire_hosted_return_if_value(self, player_id: int, expected: bytes | str, ttl_seconds: int) -> bool:
        script = self._lazy_script("expire_hosted_return", lua.EXPIRE_HOSTED_RETURN_IF_VALUE_LUA)
        expected_bytes = expected.encode("utf-8") if isinstance(expected, str) else expected
        result = await script(keys=[keys.hosted_return_key(player_id)], args=[expected_bytes, int(ttl_seconds)])
        return int(result) == 1

    async def list_registered_gates(self, slots: Sequence[int]) -> list[tuple[int, str]]:
        pairs: list[tuple[int, str]] = []
        for start in range(0, len(slots), _LEASE_PROBE_SLOTS_PER_PIPELINE):
            chunk = slots[start:start + _LEASE_PROBE_SLOTS_PER_PIPELINE]
            pipe = self.redis.pipeline(transaction=False)
            for slot in chunk:
                pipe.smembers(keys.gate_registry_key(slot))
            replies = await pipe.execute(raise_on_error=False)
            for slot, reply in zip(chunk, replies):
                if isinstance(reply, Exception):
                    if "WRONGTYPE" in str(reply):
                        logger.error("skipped gate registry slot=%d reason=wrongtype error=%s", slot, reply)
                        continue
                    raise reply
                for raw_gate in reply:
                    gate = _as_text(raw_gate)
                    if gate:
                        pairs.append((slot, gate))
        return pairs

    async def scan_gate_index_members(
        self, slot: int, gate_uuid: str, cursor: int = 0, count: int = 100,
    ) -> tuple[int, list[str]]:
        next_cursor, raw_members = await self.redis.sscan(
            keys.gate_session_index_key(slot, gate_uuid), cursor=cursor, count=count,
        )
        return int(next_cursor), [_as_text(m) for m in raw_members]

    async def mget_sessions_bytes(self, player_ids: Sequence[int]) -> list[bytes | None]:
        if not player_ids:
            return []
        result: list[bytes | None] = [None] * len(player_ids)
        by_slot: dict[int, list[int]] = defaultdict(list)
        for idx, pid in enumerate(player_ids):
            by_slot[keys.player_slot(pid)].append(idx)
        for indices in by_slot.values():
            req_keys = [keys.session_key(player_ids[i]) for i in indices]
            values = await self.redis.mget(*req_keys)
            if len(values) != len(indices):
                raise RuntimeError(f"unexpected mget reply length {len(values)} want {len(indices)}")
            for i, val in zip(indices, values):
                result[i] = _as_bytes_or_none(val)
        return result

    async def remove_bad_gate_index_member(self, slot: int, gate_uuid: str, member: str) -> bool:
        removed = await self.redis.srem(keys.gate_session_index_key(slot, gate_uuid), member)
        return int(removed) == 1

    async def prune_gate_index_member(
        self, player_id: int, gate_uuid: str, member: str, expected: bytes | None,
    ) -> bool:
        script = self._lazy_script("prune_gate_index_member", lua.PRUNE_GATE_INDEX_MEMBER_LUA)
        mode = "missing" if expected is None else "present"
        result = await script(
            keys=list(keys.prune_gate_index_member_keys(player_id, gate_uuid)),
            args=[mode, b"" if expected is None else expected, member],
        )
        return int(result) == 1

    async def release_gate_index_if_empty(self, slot: int, gate_uuid: str) -> bool:
        script = self._lazy_script("release_gate_index_if_empty", lua.RELEASE_GATE_INDEX_IF_EMPTY_LUA)
        result = await script(
            keys=list(keys.release_gate_index_keys(slot, gate_uuid)),
            args=[gate_uuid],
        )
        return int(result) == 1

    async def replace_session_and_cancel_lease(
        self, player_id: int, expected: bytes | None, updated: bytes, ttl_ms: int,
        previous_gate: str, next_gate: str,
    ) -> ReplaceSessionResult:
        mode = "missing" if expected is None else "present"
        result = await self._replace_script(
            keys=list(keys.replace_session_keys(player_id, previous_gate, next_gate)),
            args=[
                mode,
                b"" if expected is None else expected,
                updated,
                ttl_ms,
                str(player_id),
                previous_gate,
                next_gate,
            ],
        )
        return ReplaceSessionResult(int(result))

    async def transition_to_disconnecting(
        self, player_id: int, expected: bytes, updated: bytes, deadline_unix_sec: int, previous_gate: str,
    ) -> bool:
        result = await self._set_disconnecting_script(
            keys=list(keys.set_disconnecting_keys(player_id, previous_gate)),
            args=[expected, updated, 0, deadline_unix_sec, str(player_id), previous_gate],
        )
        return int(result) == 1

    async def delete_session_if_unchanged(
        self, player_id: int, expected: bytes | None, gate: str,
    ) -> bool:
        mode = "missing" if expected is None else "present"
        result = await self._delete_script(
            keys=list(keys.delete_session_keys(player_id, gate)),
            args=[mode, b"" if expected is None else expected, str(player_id), gate],
        )
        return int(result) == 1

    async def enqueue_offline_cleanup(self, player_id: int, payload: bytes, ready_at_unix_sec: int) -> bool:
        result = await self._enqueue_script(
            keys=list(keys.enqueue_offline_cleanup_keys(player_id)),
            args=[ready_at_unix_sec, str(player_id), payload],
        )
        return int(result) == 1

    async def probe_due_lease_slots(self, slots: Sequence[int], now_unix_sec: int) -> list[int]:
        due: list[int] = []
        failed = 0
        first_error: Exception | None = None
        for start in range(0, len(slots), _LEASE_PROBE_SLOTS_PER_PIPELINE):
            chunk = slots[start:start + _LEASE_PROBE_SLOTS_PER_PIPELINE]
            pipe = self.redis.pipeline(transaction=False)
            for slot in chunk:
                lease = keys.lease_keys_for_slot(slot)
                pipe.zcount(lease.ready, "-inf", now_unix_sec)
                pipe.zcount(lease.processing, "-inf", now_unix_sec)
            replies = await pipe.execute(raise_on_error=False)
            for idx, slot in enumerate(chunk):
                ready_reply = replies[idx * 2]
                proc_reply = replies[idx * 2 + 1]
                if isinstance(ready_reply, Exception) or isinstance(proc_reply, Exception):
                    failed += 1
                    if first_error is None:
                        first_error = ready_reply if isinstance(ready_reply, Exception) else proc_reply
                    continue
                if int(ready_reply) > 0 or int(proc_reply) > 0:
                    due.append(slot)
        if first_error is not None:
            logger.error("probe due lease slots failed count=%d first_error=%s", failed, first_error)
        return due

    async def claim_expired_leases(
        self, slot: int, now_unix_sec: int, claim_deadline_unix_sec: int, batch_size: int,
        batch_token: str | None = None,
    ) -> list[LeaseClaim]:
        token_prefix = batch_token or str(uuid.uuid4())
        raw_pairs = await self._claim_script(
            keys=list(keys.claim_expired_leases_keys(slot)),
            args=[now_unix_sec, claim_deadline_unix_sec, batch_size, token_prefix],
        )
        if not raw_pairs:
            return []
        if len(raw_pairs) % 2 != 0:
            raise RuntimeError(f"lease claim returned malformed tuple count {len(raw_pairs)}")

        claims: list[LeaseClaim] = []
        for idx in range(0, len(raw_pairs), 2):
            member = _as_text(raw_pairs[idx])
            player_id = keys.parse_index_member(member)
            if player_id is None or keys.player_slot(player_id) != slot:
                logger.error("malformed lease member=%r in slot=%d left in place", member, slot)
                continue
            token = _as_text(raw_pairs[idx + 1])
            try:
                payload = await self.pin_lease_claim_payload(player_id, token)
            except Exception as exc:
                logger.error("pin lease claim payload failed player=%d error=%s", player_id, exc)
                if "WRONGTYPE" in str(exc):
                    continue
                return claims
            if payload is None:
                continue
            claims.append(LeaseClaim(player_id=player_id, token=token, payload=payload))
        return claims

    async def pin_lease_claim_payload(self, player_id: int, token: str) -> bytes | None:
        raw = await self._pin_script(
            keys=list(keys.pin_lease_claim_payload_keys(player_id)),
            args=[str(player_id), token],
        )
        return _as_bytes_or_none(raw)

    async def commit_lease_expiry(self, claim: LeaseClaim) -> CommitLeaseResult:
        result = await self._commit_script(
            keys=list(keys.session_lifecycle_keys(claim.player_id)),
            args=[str(claim.player_id), claim.token, claim.payload],
        )
        return CommitLeaseResult(int(result))

    async def ack_lease_claim(self, claim: LeaseClaim) -> bool:
        result = await self._ack_script(
            keys=list(keys.ack_lease_claim_keys(claim.player_id)),
            args=[str(claim.player_id), claim.token],
        )
        return int(result) == 1

    async def renew_lease_claims(self, claims: Sequence[LeaseClaim], deadline_unix_sec: int) -> int:
        if not claims:
            return 0
        by_slot: dict[int, list[LeaseClaim]] = defaultdict(list)
        for claim in claims:
            by_slot[keys.player_slot(claim.player_id)].append(claim)
        renewed = 0
        for slot, group in by_slot.items():
            args: list[object] = [deadline_unix_sec]
            for claim in group:
                args.extend((str(claim.player_id), claim.token))
            try:
                count = await self._renew_script(keys=list(keys.renew_lease_claims_keys(slot)), args=args)
                renewed += int(count)
            except Exception as exc:
                logger.error("renew lease claims failed slot=%d error=%s", slot, exc)
        return renewed

    async def rearm_lease_claim(self, claim: LeaseClaim, deadline_unix_sec: int) -> RearmLeaseResult:
        result = await self._rearm_script(
            keys=list(keys.session_lifecycle_keys(claim.player_id)),
            args=[str(claim.player_id), claim.token, claim.payload, deadline_unix_sec, 0],
        )
        return RearmLeaseResult(int(result))


class MemorySessionStore:
    """与 RedisSessionStore 10 段 Lua 语义逐一等价的内存孪生，用于确定性单测。"""

    def __init__(self) -> None:
        self.strings: dict[str, bytes] = {}
        self.ttls: dict[str, int] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.hashes: dict[str, dict[str, bytes]] = {}
        self.sets: dict[str, set[str]] = {}
        self.fail_afk_probe: bool = False
        self.fail_session_read: bool = False
        self.on_before_cas: Callable[[str, int], None] | None = None

    def set_location_bytes(self, player_id: int, raw: bytes) -> None:
        self.strings[keys.location_key(player_id)] = raw

    def delete_location(self, player_id: int) -> None:
        self.strings.pop(keys.location_key(player_id), None)

    def set_afk_pass(self, player_id: int, expiry_unix_sec: int | str | bytes) -> None:
        raw = expiry_unix_sec if isinstance(expiry_unix_sec, bytes) else str(expiry_unix_sec).encode("utf-8")
        self.strings[keys.afk_pass_key(player_id)] = raw

    def delete_afk_pass(self, player_id: int) -> None:
        self.strings.pop(keys.afk_pass_key(player_id), None)

    def set_hosted_presence(self, player_id: int, value: bytes | str = b"scene-1") -> None:
        raw = value if isinstance(value, bytes) else value.encode("utf-8")
        self.strings[keys.hosted_presence_key(player_id)] = raw

    def delete_hosted_presence(self, player_id: int) -> None:
        self.strings.pop(keys.hosted_presence_key(player_id), None)

    def set_hosted_return(
        self, player_id: int, value: bytes | str, ttl_seconds: int | None = None,
    ) -> None:
        raw = value if isinstance(value, bytes) else value.encode("utf-8")
        key = keys.hosted_return_key(player_id)
        self.strings[key] = raw
        if ttl_seconds is not None:
            self.ttls[key] = int(ttl_seconds)

    def delete_hosted_return(self, player_id: int) -> None:
        key = keys.hosted_return_key(player_id)
        self.strings.pop(key, None)
        self.ttls.pop(key, None)

    def _zadd(self, key: str, member: str, score: float) -> None:
        self.zsets.setdefault(key, {})[member] = float(score)

    def _zrem(self, key: str, member: str) -> None:
        zset = self.zsets.get(key)
        if zset is not None:
            zset.pop(member, None)
            if not zset:
                self.zsets.pop(key, None)

    def _hset(self, key: str, field: str, value: bytes) -> None:
        self.hashes.setdefault(key, {})[field] = value

    def _hget(self, key: str, field: str) -> bytes | None:
        h = self.hashes.get(key)
        return None if h is None else h.get(field)

    def _hdel(self, key: str, field: str) -> None:
        h = self.hashes.get(key)
        if h is not None:
            h.pop(field, None)
            if not h:
                self.hashes.pop(key, None)

    def _sadd(self, key: str, member: str) -> None:
        self.sets.setdefault(key, set()).add(member)

    def _srem(self, key: str, member: str) -> None:
        s = self.sets.get(key)
        if s is not None:
            s.discard(member)
            if not s:
                self.sets.pop(key, None)

    def _scard(self, key: str) -> int:
        s = self.sets.get(key)
        return 0 if s is None else len(s)

    def _zrangebyscore(self, key: str, max_score: float, limit: int) -> list[str]:
        zset = self.zsets.get(key)
        if not zset or limit <= 0:
            return []
        matched = sorted((score, member) for member, score in zset.items() if score <= max_score)
        return [member for _, member in matched[:limit]]

    async def get_session_bytes(self, player_id: int) -> bytes | None:
        if self.fail_session_read:
            raise RuntimeError("simulated redis session read failure")
        return self.strings.get(keys.session_key(player_id))

    async def get_session_and_location_bytes(self, player_id: int) -> tuple[bytes | None, bytes | None]:
        return self.strings.get(keys.session_key(player_id)), self.strings.get(keys.location_key(player_id))

    async def get_location_bytes(self, player_id: int) -> bytes | None:
        return self.strings.get(keys.location_key(player_id))

    async def get_afk_pass_bytes(self, player_id: int) -> bytes | None:
        if self.fail_afk_probe:
            raise RuntimeError("simulated redis afk pass failure")
        return self.strings.get(keys.afk_pass_key(player_id))

    async def get_hosted_presence(self, player_id: int) -> bytes | None:
        return self.strings.get(keys.hosted_presence_key(player_id))

    async def get_hosted_return(self, player_id: int) -> bytes | None:
        return self.strings.get(keys.hosted_return_key(player_id))

    async def delete_hosted_return_if_value(self, player_id: int, expected: bytes | str) -> bool:
        key = keys.hosted_return_key(player_id)
        expected_bytes = expected.encode("utf-8") if isinstance(expected, str) else expected
        if self.strings.get(key) != expected_bytes:
            return False
        self.strings.pop(key, None)
        self.ttls.pop(key, None)
        return True

    async def expire_hosted_return_if_value(self, player_id: int, expected: bytes | str, ttl_seconds: int) -> bool:
        key = keys.hosted_return_key(player_id)
        expected_bytes = expected.encode("utf-8") if isinstance(expected, str) else expected
        if self.strings.get(key) != expected_bytes:
            return False
        if ttl_seconds <= 0:
            self.strings.pop(key, None)
            self.ttls.pop(key, None)
        else:
            self.ttls[key] = int(ttl_seconds)
        return True

    async def list_registered_gates(self, slots: Sequence[int]) -> list[tuple[int, str]]:
        pairs: list[tuple[int, str]] = []
        for slot in slots:
            gates = self.sets.get(keys.gate_registry_key(slot))
            if not gates:
                continue
            for gate in sorted(gates):
                if gate:
                    pairs.append((slot, gate))
        return pairs

    async def scan_gate_index_members(
        self, slot: int, gate_uuid: str, cursor: int = 0, count: int = 100,
    ) -> tuple[int, list[str]]:
        members = sorted(self.sets.get(keys.gate_session_index_key(slot, gate_uuid), ()))
        if not members or cursor >= len(members):
            return 0, []
        step = max(1, count)
        end = cursor + step
        page = members[cursor:end]
        next_cursor = end if end < len(members) else 0
        return next_cursor, page

    async def mget_sessions_bytes(self, player_ids: Sequence[int]) -> list[bytes | None]:
        return [self.strings.get(keys.session_key(pid)) for pid in player_ids]

    async def remove_bad_gate_index_member(self, slot: int, gate_uuid: str, member: str) -> bool:
        idx_key = keys.gate_session_index_key(slot, gate_uuid)
        s = self.sets.get(idx_key)
        if s is None or member not in s:
            return False
        self._srem(idx_key, member)
        return True

    async def prune_gate_index_member(
        self, player_id: int, gate_uuid: str, member: str, expected: bytes | None,
    ) -> bool:
        current = self.strings.get(keys.session_key(player_id))
        if expected is None:
            if current is not None:
                return False
        elif current is None or current != expected:
            return False
        idx_key = keys.gate_session_index_key(keys.player_slot(player_id), gate_uuid)
        s = self.sets.get(idx_key)
        if s is None or member not in s:
            return False
        self._srem(idx_key, member)
        return True

    async def release_gate_index_if_empty(self, slot: int, gate_uuid: str) -> bool:
        idx_key = keys.gate_session_index_key(slot, gate_uuid)
        if self._scard(idx_key) > 0:
            return False
        self.sets.pop(idx_key, None)
        self._srem(keys.gate_registry_key(slot), gate_uuid)
        return True

    async def replace_session_and_cancel_lease(
        self, player_id: int, expected: bytes | None, updated: bytes, ttl_ms: int,
        previous_gate: str, next_gate: str,
    ) -> ReplaceSessionResult:
        if self.on_before_cas is not None:
            self.on_before_cas("replace", player_id)
        s_key = keys.session_key(player_id)
        lease = keys.lease_keys_for_player(player_id)
        member = str(player_id)
        current = self.strings.get(s_key)

        if expected is None:
            if current is not None:
                return ReplaceSessionResult.CONFLICT
            if (
                member in self.zsets.get(lease.processing, {})
                or self._hget(lease.claim_tokens, member) is not None
                or self._hget(lease.claim_payloads, member) is not None
            ):
                return ReplaceSessionResult.CLEANUP_PENDING
        else:
            if current is None or current != expected:
                return ReplaceSessionResult.CONFLICT

        slot = keys.player_slot(player_id)
        reg_key = keys.gate_registry_key(slot)
        if previous_gate and previous_gate != next_gate:
            prev_idx = keys.gate_session_index_key(slot, previous_gate)
            self._srem(prev_idx, member)
            if self._scard(prev_idx) == 0:
                self._srem(reg_key, previous_gate)
        if next_gate:
            next_idx = keys.gate_session_index_key(slot, next_gate)
            self._sadd(next_idx, member)
            self._sadd(reg_key, next_gate)

        self._zrem(lease.ready, member)
        self._zrem(lease.processing, member)
        self._hdel(lease.claim_tokens, member)
        self._hdel(lease.claim_payloads, member)
        self.strings[s_key] = updated
        return ReplaceSessionResult.APPLIED

    async def transition_to_disconnecting(
        self, player_id: int, expected: bytes, updated: bytes, deadline_unix_sec: int, previous_gate: str,
    ) -> bool:
        if self.on_before_cas is not None:
            self.on_before_cas("disconnect", player_id)
        s_key = keys.session_key(player_id)
        if self.strings.get(s_key) != expected:
            return False
        lease = keys.lease_keys_for_player(player_id)
        member = str(player_id)
        self._zadd(lease.ready, member, deadline_unix_sec)
        self._zrem(lease.processing, member)
        self._hdel(lease.claim_tokens, member)
        self._hdel(lease.claim_payloads, member)
        if previous_gate:
            slot = keys.player_slot(player_id)
            prev_idx = keys.gate_session_index_key(slot, previous_gate)
            self._srem(prev_idx, member)
            if self._scard(prev_idx) == 0:
                self._srem(keys.gate_registry_key(slot), previous_gate)
        self.strings[s_key] = updated
        return True

    async def delete_session_if_unchanged(
        self, player_id: int, expected: bytes | None, gate: str,
    ) -> bool:
        if self.on_before_cas is not None:
            self.on_before_cas("delete", player_id)
        s_key = keys.session_key(player_id)
        current = self.strings.get(s_key)
        if expected is None:
            if current is not None:
                return False
        elif current is None or current != expected:
            return False

        lease = keys.lease_keys_for_player(player_id)
        member = str(player_id)
        self._zrem(lease.ready, member)
        self._zrem(lease.processing, member)
        self._hdel(lease.claim_tokens, member)
        self._hdel(lease.claim_payloads, member)
        if gate:
            slot = keys.player_slot(player_id)
            idx_key = keys.gate_session_index_key(slot, gate)
            self._srem(idx_key, member)
            if self._scard(idx_key) == 0:
                self._srem(keys.gate_registry_key(slot), gate)
        self.strings.pop(s_key, None)
        return True

    async def enqueue_offline_cleanup(self, player_id: int, payload: bytes, ready_at_unix_sec: int) -> bool:
        s_key = keys.session_key(player_id)
        if s_key in self.strings:
            return False
        lease = keys.lease_keys_for_player(player_id)
        member = str(player_id)
        self._zadd(lease.ready, member, ready_at_unix_sec)
        self._hset(lease.claim_payloads, member, payload)
        return True

    async def probe_due_lease_slots(self, slots: Sequence[int], now_unix_sec: int) -> list[int]:
        due: list[int] = []
        for slot in slots:
            lease = keys.lease_keys_for_slot(slot)
            ready = self.zsets.get(lease.ready)
            proc = self.zsets.get(lease.processing)
            if (ready and any(score <= now_unix_sec for score in ready.values())) or (
                proc and any(score <= now_unix_sec for score in proc.values())
            ):
                due.append(slot)
        return due

    async def claim_expired_leases(
        self, slot: int, now_unix_sec: int, claim_deadline_unix_sec: int, batch_size: int,
        batch_token: str | None = None,
    ) -> list[LeaseClaim]:
        lease = keys.lease_keys_for_slot(slot)
        token_prefix = batch_token or str(uuid.uuid4())

        for member in self._zrangebyscore(lease.processing, now_unix_sec, batch_size):
            self._zrem(lease.processing, member)
            self._zadd(lease.ready, member, now_unix_sec)
            self._hdel(lease.claim_tokens, member)

        claimed_pairs: list[tuple[str, str]] = []
        for member in self._zrangebyscore(lease.ready, now_unix_sec, batch_size):
            self._zrem(lease.ready, member)
            self._zadd(lease.processing, member, claim_deadline_unix_sec)
            token = f"{token_prefix}:{member}"
            self._hset(lease.claim_tokens, member, token.encode("utf-8"))
            claimed_pairs.append((member, token))

        claims: list[LeaseClaim] = []
        for member, token in claimed_pairs:
            player_id = keys.parse_index_member(member)
            if player_id is None or keys.player_slot(player_id) != slot:
                logger.error("malformed lease member=%r in slot=%d left in place", member, slot)
                continue
            payload = await self.pin_lease_claim_payload(player_id, token)
            if payload is None:
                continue
            claims.append(LeaseClaim(player_id=player_id, token=token, payload=payload))
        return claims

    async def pin_lease_claim_payload(self, player_id: int, token: str) -> bytes | None:
        lease = keys.lease_keys_for_player(player_id)
        member = str(player_id)
        if self._hget(lease.claim_tokens, member) != token.encode("utf-8"):
            return None
        current = self.strings.get(keys.session_key(player_id))
        if current is not None:
            self._hset(lease.claim_payloads, member, current)
            return current
        pinned = self._hget(lease.claim_payloads, member)
        if pinned is not None:
            return pinned
        return b""

    async def commit_lease_expiry(self, claim: LeaseClaim) -> CommitLeaseResult:
        lease = keys.lease_keys_for_player(claim.player_id)
        member = str(claim.player_id)
        if self._hget(lease.claim_tokens, member) != claim.token.encode("utf-8"):
            return CommitLeaseResult.CLAIM_LOST
        s_key = keys.session_key(claim.player_id)
        current = self.strings.get(s_key)
        if current is not None and current != claim.payload:
            self._zrem(lease.processing, member)
            self._hdel(lease.claim_tokens, member)
            self._hdel(lease.claim_payloads, member)
            return CommitLeaseResult.SESSION_REPLACED
        self.strings.pop(s_key, None)
        return CommitLeaseResult.COMMITTED

    async def ack_lease_claim(self, claim: LeaseClaim) -> bool:
        lease = keys.lease_keys_for_player(claim.player_id)
        member = str(claim.player_id)
        if self._hget(lease.claim_tokens, member) != claim.token.encode("utf-8"):
            return False
        self._zrem(lease.processing, member)
        self._hdel(lease.claim_tokens, member)
        self._hdel(lease.claim_payloads, member)
        return True

    async def renew_lease_claims(self, claims: Sequence[LeaseClaim], deadline_unix_sec: int) -> int:
        renewed = 0
        for claim in claims:
            lease = keys.lease_keys_for_player(claim.player_id)
            member = str(claim.player_id)
            if self._hget(lease.claim_tokens, member) == claim.token.encode("utf-8"):
                self._zadd(lease.processing, member, deadline_unix_sec)
                renewed += 1
        return renewed

    async def rearm_lease_claim(self, claim: LeaseClaim, deadline_unix_sec: int) -> RearmLeaseResult:
        lease = keys.lease_keys_for_player(claim.player_id)
        member = str(claim.player_id)
        if self._hget(lease.claim_tokens, member) != claim.token.encode("utf-8"):
            return RearmLeaseResult.CLAIM_LOST
        current = self.strings.get(keys.session_key(claim.player_id))
        if current is None or current != claim.payload:
            self._zrem(lease.processing, member)
            self._hdel(lease.claim_tokens, member)
            self._hdel(lease.claim_payloads, member)
            return RearmLeaseResult.SESSION_REPLACED
        self._zadd(lease.ready, member, deadline_unix_sec)
        self._zrem(lease.processing, member)
        self._hdel(lease.claim_tokens, member)
        self._hdel(lease.claim_payloads, member)
        return RearmLeaseResult.REARMED
