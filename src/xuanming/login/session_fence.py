"""Per gate-connection session fence (Go ``pkg/sessionfence``); scripts copied verbatim."""

from enum import Enum

from . import keys

SESSION_TTL_MS = 6 * 60_000  # enterGameChainBudget (5m) + 1m
BOUND_RECORD_TTL_MS = 7 * 24 * 3600_000

COMMIT_LOGIN = """
if redis.call('HGET', KEYS[1], 'closed') == '1' then
  return 0
end
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[2])
if redis.call('HEXISTS', KEYS[1], 'player') == 1 then
  redis.call('HSET', KEYS[1], 'account', ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
end
return 1
"""

CLOSE = """
redis.call('HSET', KEYS[1], 'closed', '1')
redis.call('PEXPIRE', KEYS[1], ARGV[1])
local p = redis.call('HGET', KEYS[1], 'player')
if p then
  return p
end
return '0'
"""

CHECK_ENTER_GAME = """
redis.call('HSETNX', KEYS[1], 'gen', '0')
local v = redis.call('HMGET', KEYS[1], 'closed', 'gen', 'player')
if v[3] and v[1] ~= '1' then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return {v[1] or '', v[2] or '0', v[3] or ''}
"""

BIND_PLAYER = """
local v = redis.call('HMGET', KEYS[1], 'closed', 'gen', 'player')
if v[1] == '1' then
  return 'closed'
end
local gen = v[2] or '0'
if gen ~= ARGV[1] then
  return 'left'
end
if v[3] and v[3] ~= ARGV[2] then
  return 'other_player'
end
redis.call('HSET', KEYS[1], 'player', ARGV[2])
local account = redis.call('GET', KEYS[2])
if account then
  redis.call('HSET', KEYS[1], 'account', account)
else
  redis.call('HDEL', KEYS[1], 'account')
end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 'ok'
"""

LEAVE = """
redis.call('HINCRBY', KEYS[1], 'gen', 1)
local p = redis.call('HGET', KEYS[1], 'player')
if p then
  redis.call('HSET', KEYS[1], 'left_player', p)
  redis.call('HDEL', KEYS[1], 'player', 'account')
else
  p = redis.call('HGET', KEYS[1], 'left_player')
end
redis.call('PEXPIRE', KEYS[1], ARGV[1])
if p then
  return p
end
return '0'
"""

RENEW = """
local v = redis.call('HMGET', KEYS[1], 'closed', 'player', 'account')
if v[1] == '1' then
  return {0, '', 0}
end
local kept = 0
local restored = 0
local account = redis.call('GET', KEYS[2])
if account then
  redis.call('PEXPIRE', KEYS[2], ARGV[1])
  kept = 1
end
if v[2] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  kept = 1
  if not account and v[3] and v[3] ~= '' then
    redis.call('SET', KEYS[2], v[3], 'PX', ARGV[1])
    account = v[3]
    restored = 1
  end
end
return {kept, account or '', restored}
"""


class Outcome(Enum):
    OK = "ok"
    CLOSED = "closed"
    LEFT = "left"
    OTHER_PLAYER = "other_player"


def connection_of(details) -> str | None:
    """Fence/login-session identity of a gate connection, or None if it has no global identity."""
    if details is None or not details.session_incarnation or not details.gate_instance_id:
        return None
    return keys.connection_id(details.gate_instance_id, details.session_incarnation)


def _text(value) -> str:
    return value.decode() if isinstance(value, bytes) else "" if value is None else str(value)


def _player(value) -> int:
    raw = _text(value)
    return int(raw) if raw else 0


async def _eval(redis, script, keys_, *args):
    return await redis.eval(script, len(keys_), *keys_, *args)


async def commit_login(redis, conn: str, account: str, session_ttl_ms: int) -> bool:
    """Write login_session unless the connection is closed; False means closed (nothing written)."""
    if session_ttl_ms <= 0:
        raise ValueError("session fence commit login: non-positive session ttl")
    result = await _eval(redis, COMMIT_LOGIN, [keys.session_fence_key(conn), keys.login_session_key(conn)],
                         account, session_ttl_ms, BOUND_RECORD_TTL_MS)
    return int(result) == 1


async def close(redis, conn: str, ttl_ms: int = SESSION_TTL_MS) -> int:
    return _player(await _eval(redis, CLOSE, [keys.session_fence_key(conn)], ttl_ms))


async def check_enter_game(redis, conn: str, player_id: int, ttl_ms: int = SESSION_TTL_MS) -> tuple[Outcome, int]:
    values = await _eval(redis, CHECK_ENTER_GAME, [keys.session_fence_key(conn)], ttl_ms, BOUND_RECORD_TTL_MS)
    if len(values) != 3:
        raise RuntimeError(f"session fence check: unexpected reply length {len(values)}")
    if _text(values[0]) == "1":
        return Outcome.CLOSED, 0
    gen, bound = _player(values[1]), _player(values[2])
    if bound and bound != player_id:
        return Outcome.OTHER_PLAYER, gen
    return Outcome.OK, gen


async def bind_player(redis, conn: str, player_id: int, expect_gen: int) -> Outcome:
    raw = _text(await _eval(redis, BIND_PLAYER, [keys.session_fence_key(conn), keys.login_session_key(conn)],
                            str(expect_gen), str(player_id), BOUND_RECORD_TTL_MS))
    try:
        return Outcome(raw)
    except ValueError:
        return Outcome.CLOSED  # an unknown reply means the script was tampered with: fail closed


async def recheck(redis, conn: str, expect_gen: int) -> Outcome:
    closed, gen = await redis.hmget(keys.session_fence_key(conn), "closed", "gen")
    if _text(closed) == "1":
        return Outcome.CLOSED
    return Outcome.OK if _player(gen) == expect_gen else Outcome.LEFT


async def leave(redis, conn: str, ttl_ms: int = SESSION_TTL_MS) -> int:
    return _player(await _eval(redis, LEAVE, [keys.session_fence_key(conn)], ttl_ms))


async def renew(redis, conn: str, session_ttl_ms: int) -> tuple[bool, str, bool]:
    """Returns (kept, account, restored)."""
    if session_ttl_ms <= 0:
        raise ValueError("session fence renew: non-positive session ttl")
    values = await _eval(redis, RENEW, [keys.session_fence_key(conn), keys.login_session_key(conn)],
                         session_ttl_ms, BOUND_RECORD_TTL_MS)
    if len(values) != 3 or not isinstance(values[0], int) or not isinstance(values[2], int):
        raise RuntimeError("session fence renew: unexpected reply")
    return values[0] == 1, _text(values[1]), values[2] == 1
