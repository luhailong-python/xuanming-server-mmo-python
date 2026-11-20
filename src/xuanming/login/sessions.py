"""Player sessions via player_locator plus login generations (Go ``pkg/sessionmanager``)."""

import time
from enum import IntEnum

from . import keys

STATE_ONLINE, STATE_DISCONNECTING = 1, 2
LOGIN_FIRST, LOGIN_REPLACE, LOGIN_RECONNECT = 1, 2, 3
DISCONNECT_LEASE_SECONDS = 30  # mirrors C++ kLoginReconnectLease
LOGIN_GENERATION_TTL_SECONDS = 7 * 24 * 3600
IDEMPOTENCY_TTL_SECONDS = 300
_PATH = "/playerlocator.PlayerLocator/"

MINT_LOGIN_GENERATION = """
local nxt = (tonumber(redis.call('GET', KEYS[1])) or 0) + 1
local nowMs = tonumber(ARGV[1])
if nowMs > nxt then nxt = nowMs end
local floor = tonumber(ARGV[2]) + 1
if floor > nxt then nxt = floor end
local s = string.format('%.0f', nxt)
redis.call('SET', KEYS[1], s, 'EX', ARGV[3])
return s
"""


class Decision(IntEnum):
    FIRST = 0
    RECONNECT = 1
    REPLACE = 2

    @property
    def enter_gs_type(self) -> int:
        return (LOGIN_FIRST, LOGIN_RECONNECT, LOGIN_REPLACE)[self]


class SessionChanged(Exception):
    """player_locator answered ABORTED: the record changed after it was read; nothing was written."""


def decide(existing, account: str) -> Decision:
    if existing is None:
        return Decision.FIRST
    if existing.state == STATE_DISCONNECTING and existing.account == account:
        return Decision.RECONNECT
    return Decision.REPLACE


def _code(error) -> str:
    code = getattr(error, "code", None)
    return code().name if callable(code) else ""


class PlayerLocator:
    def __init__(self, protocol, call) -> None:
        self.protocol = protocol
        self.call = call

    async def _rpc(self, method: str, request_type: str, response_type: str | None, **fields):
        request = self.protocol.message(f"playerlocator.{request_type}", **fields)
        raw = await self.call(_PATH + method, request.SerializeToString())
        return self.protocol.parse(f"playerlocator.{response_type}", raw) if response_type else None

    async def get(self, player_id: int):
        response = await self._rpc("GetSession", "GetSessionRequest", "GetSessionResponse", player_id=player_id)
        return response.session if response.found else None

    async def set(self, session) -> None:
        try:
            await self._rpc("SetSession", "SetSessionRequest", None, session=session)
        except Exception as error:
            if _code(error) == "ABORTED":
                raise SessionChanged(str(error)) from error
            raise

    async def set_disconnecting(self, player_id: int, session_id: int, gate_instance_id: str, incarnation: int) -> None:
        await self._rpc("SetDisconnecting", "SetDisconnectingRequest", None,
                        player_id=player_id, session_id=session_id, lease_ttl_seconds=DISCONNECT_LEASE_SECONDS,
                        gate_instance_id=gate_instance_id, session_incarnation=incarnation)

    async def reconnect(self, *, player_id, session_id, gate_id, gate_instance_id, incarnation, account,
                        request_id, login_generation):
        response = await self._rpc(
            "Reconnect", "ReconnectRequest", "ReconnectResponse", player_id=player_id, new_session_id=session_id,
            gate_id=gate_id, gate_instance_id=gate_instance_id, account=account, request_id=request_id,
            login_generation=login_generation, session_incarnation=incarnation,
        )
        if not response.success:
            raise RuntimeError(f"reconnect failed: {response.error_message}")
        return response.session

    async def delete(self, player_id: int, expected_session_id: int, gate_instance_id: str) -> None:
        """MarkOffline only the exact session; a late request from a replaced connection is a no-op."""
        current = await self.get(player_id)
        if current is None or current.session_id != expected_session_id:
            return
        await self._rpc("MarkOffline", "MarkOfflineRequest", None, player_id=player_id,
                        expected_session_id=expected_session_id, expected_session_version=current.session_version,
                        expected_gate_instance_id=gate_instance_id)

    def online_session(self, existing, *, player_id, session_id, gate_id, gate_instance_id, incarnation, account,
                       request_id, login_generation):
        return self.protocol.message(
            "playerlocator.PlayerSession", player_id=player_id, session_id=session_id, gate_id=gate_id,
            gate_instance_id=gate_instance_id, session_incarnation=incarnation, account=account,
            state=STATE_ONLINE, last_active_ts=time.time_ns() // 1_000_000, request_id=request_id,
            login_generation=login_generation,
            session_version=1 if existing is None else existing.session_version + 1,
        )


async def mint_login_generation(redis, player_id: int, floor: int, now_ms: int | None = None) -> int:
    if not player_id:
        raise ValueError("mint login generation: player_id is 0")
    now_ms = time.time_ns() // 1_000_000 if now_ms is None else now_ms
    raw = await redis.eval(MINT_LOGIN_GENERATION, 1, keys.login_generation_key(player_id),
                           now_ms, floor, LOGIN_GENERATION_TTL_SECONDS)
    generation = int(raw)
    if generation <= floor:
        raise RuntimeError(f"minted generation {generation} is not above session floor {floor}")
    return generation


async def set_idempotency(redis, player_id: int, request_id: str) -> None:
    if request_id:
        await redis.set(keys.login_idempotent_key(player_id, request_id), "1", ex=IDEMPOTENCY_TTL_SECONDS)
