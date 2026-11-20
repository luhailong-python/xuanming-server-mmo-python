"""Access/refresh token families in the login-domain Redis (Go ``pkg/token``).

Token string = ``<account_tag>.<base64url(32 bytes)>``; bytes[0:16] are the family id and the top
bit of byte 16 marks an access token. The Lua scripts are copied verbatim from Go ``family.go`` so a
Go and a Python login can share the same Redis.
"""

import asyncio
import base64
import json
import logging
import secrets
import time
from dataclasses import dataclass

from redis.exceptions import ResponseError

from . import keys

logger = logging.getLogger(__name__)

_FAMILY_BYTES = 16
_TOKEN_BYTES = 32
_ACCESS_BIT = 0x80
_MAX_REFRESH_PER_ACCOUNT = 32
MAX_REUSE_GRACE_MS = 60_000
_REPLAY_ATTEMPTS = 3

ISSUE_FAMILY = """
local kind = redis.call("TYPE", KEYS[3]).ok
if kind ~= "none" and kind ~= "zset" then
    return redis.error_reply("invalid account refresh index")
end
if redis.call("EXISTS", KEYS[1], KEYS[2], KEYS[4]) ~= 0 then
    if redis.call("TYPE", KEYS[1]).ok == "string" and redis.call("GET", KEYS[1]) == ARGV[1] and
       redis.call("TYPE", KEYS[2]).ok == "string" and redis.call("GET", KEYS[2]) == ARGV[1] and
       redis.call("TYPE", KEYS[4]).ok == "hash" and redis.call("HGET", KEYS[4], "head") == ARGV[8] and
       redis.call("HGET", KEYS[4], "account") == ARGV[2] then
        return 1
    end
    return redis.error_reply("token identity collision")
end
local oldTTL = redis.call("PTTL", KEYS[3])
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[4])
redis.call("SET", KEYS[2], ARGV[1], "PX", ARGV[5])
redis.call("HSET", KEYS[4], "account", ARGV[2], "head", ARGV[8], "prev", "", "created_at", ARGV[3], "rotated_at", ARGV[3])
redis.call("PEXPIRE", KEYS[4], ARGV[5])
redis.call("ZREMRANGEBYSCORE", KEYS[3], "-inf", ARGV[6])
redis.call("ZADD", KEYS[3], ARGV[7], ARGV[8])
redis.call("PEXPIRE", KEYS[3], math.max(oldTTL, tonumber(ARGV[5])))
return 1
"""

ROTATE_REFRESH = """
if redis.call("GET", KEYS[1]) ~= ARGV[1] or redis.call("PTTL", KEYS[1]) <= 0 then
    return 0
end
local kind = redis.call("TYPE", KEYS[2]).ok
if kind ~= "none" and kind ~= "zset" then
    return redis.error_reply("invalid account refresh index")
end
if kind ~= "zset" or not redis.call("ZSCORE", KEYS[2], ARGV[2]) then
    return 0
end
if redis.call("EXISTS", KEYS[3], KEYS[4], KEYS[6]) ~= 0 then
    return redis.error_reply("token identity collision")
end
local familyKind = redis.call("TYPE", KEYS[5]).ok
if familyKind == "none" then
    return 2
end
if familyKind ~= "hash" or redis.call("HGET", KEYS[5], "account") ~= ARGV[9] or redis.call("PTTL", KEYS[5]) <= 0 then
    return redis.error_reply("invalid refresh family")
end
if redis.call("HGET", KEYS[5], "head") ~= ARGV[2] then
    return 3
end
local oldTTL = redis.call("PTTL", KEYS[2])
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("SET", KEYS[3], ARGV[3], "PX", ARGV[4])
redis.call("SET", KEYS[4], ARGV[3], "PX", ARGV[5])
redis.call("HSET", KEYS[5], "head", ARGV[8], "prev", ARGV[2], "rotated_at", ARGV[10])
redis.call("PEXPIRE", KEYS[5], ARGV[5])
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[6])
redis.call("ZADD", KEYS[2], ARGV[7], ARGV[8])
redis.call("PEXPIRE", KEYS[2], math.max(oldTTL, tonumber(ARGV[5])))
if tonumber(ARGV[11]) > 0 then
    redis.call("HSET", KEYS[6], "access", ARGV[12], "refresh", ARGV[8], "access_expire", ARGV[13], "refresh_expire", ARGV[7], "rotated_at", ARGV[10])
    redis.call("PEXPIRE", KEYS[6], ARGV[11])
end
return 1
"""

REPLAY_REFRESH = """
local recordKind = redis.call("TYPE", KEYS[2]).ok
if recordKind ~= "none" and recordKind ~= "hash" then
    return {-1}
end
local familyKind = redis.call("TYPE", KEYS[1]).ok
if familyKind == "none" then
    if recordKind == "hash" then
        return {2}
    end
    return {3}
end
if familyKind ~= "hash" or redis.call("PTTL", KEYS[1]) <= 0 then
    return {-1}
end
local account = redis.call("HGET", KEYS[1], "account")
local head = redis.call("HGET", KEYS[1], "head")
if not account or not head then
    return {-1}
end
if head == ARGV[1] then
    return {3}
end
if recordKind == "hash" then
    local ttl = redis.call("PTTL", KEYS[2])
    if ttl < 0 or ttl > tonumber(ARGV[3]) then
        return {-1}
    end
    if ttl > tonumber(ARGV[2]) then
        return {3}
    end
    local successor = redis.call("HGET", KEYS[2], "refresh")
    if not successor then
        return {-1}
    end
    if head == successor then
        local successorTTL = redis.call("PTTL", ARGV[4] .. successor)
        if successorTTL == -1 then
            return {-1}
        end
        if successorTTL == -2 then
            return {0}
        end
        return {1, account, redis.call("HGET", KEYS[2], "access"), successor,
            redis.call("HGET", KEYS[2], "access_expire"), redis.call("HGET", KEYS[2], "refresh_expire"),
            redis.call("HGET", KEYS[2], "rotated_at")}
    end
end
local replayed = "older"
if redis.call("HGET", KEYS[1], "prev") == ARGV[1] then
    replayed = "predecessor"
end
local rotatedAt = redis.call("HGET", KEYS[1], "rotated_at") or "0"
local createdAt = redis.call("HGET", KEYS[1], "created_at") or "0"
if ARGV[6] == "1" then
    return {5, account, replayed, rotatedAt, createdAt}
end
local index = ARGV[5] .. account
redis.call("DEL", ARGV[4] .. head, KEYS[1])
if redis.call("TYPE", index).ok == "zset" then
    redis.call("ZREM", index, head)
end
return {4, account, replayed, rotatedAt, createdAt}
"""

VALIDATE_FAMILY_ACCESS = """
if redis.call("GET", KEYS[1]) ~= ARGV[1] or redis.call("PTTL", KEYS[1]) <= 0 or
   redis.call("TYPE", KEYS[2]).ok ~= "hash" or
   redis.call("HGET", KEYS[2], "account") ~= ARGV[2] or
   redis.call("PTTL", KEYS[2]) <= 0 then
    return 0
end
local head = redis.call("HGET", KEYS[2], "head")
if not head or #head ~= 43 or not string.match(head, "^[%w_-]+$") or
   redis.call("TYPE", KEYS[3]).ok ~= "zset" or
   not redis.call("ZSCORE", KEYS[3], head) then
    return 0
end
if redis.call("PTTL", ARGV[4] .. head) <= 0 then return 0 end
local raw = redis.call("GET", ARGV[4] .. head)
if not raw then return 0 end
local ok, data = pcall(cjson.decode, raw)
if not ok or type(data) ~= "table" or data.account ~= ARGV[2] or data.family_id ~= ARGV[3] then
    return 0
end
return 1
"""

RETIRE_REFRESH = """
local kind = redis.call("TYPE", KEYS[1]).ok
if kind == "none" then return 0 end
if kind ~= "zset" then return redis.error_reply("invalid account refresh index") end
local count = redis.call("ZCARD", KEYS[1])
local keep = tonumber(ARGV[2])
if count <= keep then return 0 end
local candidates = redis.call("ZRANGE", KEYS[1], 0, count - keep)
local tokens = {}
for _, token in ipairs(candidates) do
    if token ~= ARGV[5] and #tokens < count - keep then
        tokens[#tokens + 1] = token
    end
end
local families = {}
for i, token in ipairs(tokens) do
    local raw = redis.call("GET", ARGV[3] .. token)
    if raw then
        local ok, data = pcall(cjson.decode, raw)
        if not ok or type(data) ~= "table" or data.account ~= ARGV[1] then
            return redis.error_reply("invalid refresh ownership")
        end
        local family = data.family_id
        if type(family) ~= "string" or #family ~= 22 or not string.match(family, "^[%w_-]+$") or
           not string.match(string.sub(family, -1), "^[AQgw]$") then
            return redis.error_reply("invalid refresh family identity")
        end
        if redis.call("PTTL", ARGV[3] .. token) <= 0 then
            return redis.error_reply("invalid refresh expiration")
        end
        local key = ARGV[4] .. family
        local familyType = redis.call("TYPE", key).ok
        if familyType ~= "none" then
            if familyType ~= "hash" or redis.call("HGET", key, "account") ~= ARGV[1] or redis.call("PTTL", key) <= 0 then
                return redis.error_reply("invalid refresh family ownership")
            end
            local head = redis.call("HGET", key, "head")
            if not head or #head ~= 43 or not string.match(head, "^[%w_-]+$") or
               not redis.call("ZSCORE", KEYS[1], head) or redis.call("PTTL", ARGV[3] .. head) <= 0 then
                return redis.error_reply("invalid refresh family head")
            end
            local headRaw = redis.call("GET", ARGV[3] .. head)
            local headOK, headData = false, nil
            if headRaw then headOK, headData = pcall(cjson.decode, headRaw) end
            if not headOK or type(headData) ~= "table" or headData.account ~= ARGV[1] or headData.family_id ~= family then
                return redis.error_reply("invalid refresh family head ownership")
            end
            if head == token then families[i] = key end
        end
    end
end
for i, token in ipairs(tokens) do
    redis.call("DEL", ARGV[3] .. token)
    if families[i] then redis.call("DEL", families[i]) end
    redis.call("ZREM", KEYS[1], token)
end
return #tokens
"""


class AccessRejected(Exception):
    """The access token cannot authenticate (as opposed to a store failure)."""


class RefreshRejected(Exception):
    """The refresh token cannot be rotated."""


@dataclass(frozen=True, slots=True)
class TokenData:
    account: str
    auth_type: str
    family_id: str
    device_id: str = ""
    created_at: int = 0


@dataclass(frozen=True, slots=True)
class TokenPair:
    access_token: str
    refresh_token: str
    access_expire: int
    refresh_expire: int


@dataclass(frozen=True, slots=True)
class _Parsed:
    tag: str
    secret: str
    family_id: str
    access: bool


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _parse(raw: str) -> _Parsed | None:
    tag, dot, secret = raw.partition(".")
    if not dot or not keys.valid_account_tag(tag):
        return None
    try:
        decoded = base64.urlsafe_b64decode(secret + "=" * (-len(secret) % 4))
    except ValueError:
        return None
    # Re-encode and compare: one 32-byte value has exactly one accepted spelling.
    if len(decoded) != _TOKEN_BYTES or _b64(decoded) != secret:
        return None
    return _Parsed(tag, secret, _b64(decoded[:_FAMILY_BYTES]), bool(decoded[_FAMILY_BYTES] & _ACCESS_BIT))


def _new_secret(family: bytes, access: bool) -> str:
    tail = bytearray(secrets.token_bytes(_TOKEN_BYTES - _FAMILY_BYTES))
    tail[0] = tail[0] | _ACCESS_BIT if access else tail[0] & ~_ACCESS_BIT
    return _b64(family + bytes(tail))


def _go_json(data: dict) -> bytes:
    # encoding/json escapes HTML characters and line separators; keep stored payloads identical.
    text = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    for char, escaped in (("<", "\\u003c"), (">", "\\u003e"), ("&", "\\u0026"),
                          ("\u2028", "\\u2028"), ("\u2029", "\\u2029")):
        text = text.replace(char, escaped)
    return text.encode()


def _payload(account: str, auth_type: str, device_id: str, created_at: int, family_id: str) -> bytes:
    data = {"account": account, "auth_type": auth_type}
    if device_id:
        data["device_id"] = device_id
    data |= {"created_at": created_at, "family_id": family_id}
    return _go_json(data)


def _text(value) -> str:
    return value.decode() if isinstance(value, bytes) else "" if value is None else str(value)


class TokenManager:
    def __init__(self, redis, *, access_ttl_ms: int = 2 * 3600_000, refresh_ttl_ms: int = 720 * 3600_000,
                 reuse_grace_ms: int = 30_000, observe_only: bool = False) -> None:
        if access_ttl_ms <= 0 or refresh_ttl_ms < access_ttl_ms or not 0 <= reuse_grace_ms <= MAX_REUSE_GRACE_MS:
            raise ValueError("token TTL 配置无效")
        self.redis = redis
        self.access_ttl_ms = access_ttl_ms
        self.refresh_ttl_ms = refresh_ttl_ms
        self.reuse_grace_ms = reuse_grace_ms
        self.observe_only = observe_only

    async def _eval(self, script: str, keys_: list[str], *args):
        return await self.redis.eval(script, len(keys_), *keys_, *args)

    async def issue(self, account: str, auth_type: str, device_id: str = "") -> TokenPair:
        if not account or not auth_type:
            raise ValueError("token 账号与认证类型不能为空")
        now = int(time.time())
        family = secrets.token_bytes(_FAMILY_BYTES)
        family_id = _b64(family)
        access, refresh = _new_secret(family, True), _new_secret(family, False)
        refresh_expire = now + self.refresh_ttl_ms // 1000
        tag = keys.account_tag(account)
        await self._eval(
            ISSUE_FAMILY,
            [keys.access_token_key(tag, access), keys.refresh_token_key(tag, refresh),
             keys.account_refresh_key(account), keys.refresh_family_key(tag, family_id)],
            _payload(account, auth_type, device_id, now, family_id), account, now,
            self.access_ttl_ms, self.refresh_ttl_ms, now - 1, refresh_expire, refresh,
        )
        await self._trim(account, refresh)
        return TokenPair(f"{tag}.{access}", f"{tag}.{refresh}", now + self.access_ttl_ms // 1000, refresh_expire)

    async def validate_access(self, token: str) -> TokenData:
        parsed = _parse(token)
        if parsed is None or not parsed.access:
            raise AccessRejected("token malformed")
        key = keys.access_token_key(parsed.tag, parsed.secret)
        raw, data = await self._load(parsed, key, AccessRejected)
        valid = await self._eval(
            VALIDATE_FAMILY_ACCESS,
            [key, keys.refresh_family_key(parsed.tag, parsed.family_id), keys.account_refresh_key(data.account)],
            raw, data.account, parsed.family_id, keys.refresh_token_key_prefix(parsed.tag),
        )
        if int(valid) != 1:
            raise AccessRejected("token family 已失效或不匹配")
        return data

    async def refresh(self, token: str) -> TokenPair:
        parsed = _parse(token)
        if parsed is None or parsed.access:
            raise RefreshRejected("token malformed")
        try:
            raw, data = await self._load(parsed, keys.refresh_token_key(parsed.tag, parsed.secret), None)
        except LookupError:
            return await self._replay(parsed)
        now = int(time.time())
        family = base64.urlsafe_b64decode(parsed.family_id + "==")
        access, refresh = _new_secret(family, True), _new_secret(family, False)
        access_expire, refresh_expire = now + self.access_ttl_ms // 1000, now + self.refresh_ttl_ms // 1000
        result = int(await self._eval(
            ROTATE_REFRESH,
            [keys.refresh_token_key(parsed.tag, parsed.secret), keys.account_refresh_key(data.account),
             keys.access_token_key(parsed.tag, access), keys.refresh_token_key(parsed.tag, refresh),
             keys.refresh_family_key(parsed.tag, parsed.family_id), keys.refresh_rotated_key(parsed.tag, parsed.secret)],
            raw, parsed.secret, _payload(data.account, data.auth_type, data.device_id, now, parsed.family_id),
            self.access_ttl_ms, self.refresh_ttl_ms, now - 1, refresh_expire, refresh, data.account, now,
            self.reuse_grace_ms, access, access_expire,
        ))
        if result == 0:
            # A concurrent rotation consumed it between GET and the script: judge it as a replay.
            return await self._replay(parsed)
        if result == 2:
            raise RefreshRejected("token family 已吊销或结束")
        if result != 1:
            raise RuntimeError(f"live refresh token is not its family head (result={result})")
        await self._trim(data.account, refresh)
        return TokenPair(f"{parsed.tag}.{access}", f"{parsed.tag}.{refresh}", access_expire, refresh_expire)

    async def _replay(self, parsed: _Parsed) -> TokenPair:
        script_keys = [keys.refresh_family_key(parsed.tag, parsed.family_id),
                       keys.refresh_rotated_key(parsed.tag, parsed.secret)]
        args = (parsed.secret, self.reuse_grace_ms, MAX_REUSE_GRACE_MS, keys.refresh_token_key_prefix(parsed.tag),
                keys.account_refresh_key_prefix(parsed.tag), "1" if self.observe_only else "0")
        for attempt in range(_REPLAY_ATTEMPTS):
            try:
                reply = await self._eval(REPLAY_REFRESH, script_keys, *args)
                break
            except ResponseError:
                raise
            except Exception:
                # The script may have run with its reply lost; it is idempotent, so retry within budget.
                if attempt + 1 == _REPLAY_ATTEMPTS:
                    raise
                logger.exception("refresh 重放判定第 %d 次失败，结果未知", attempt + 1)
                await asyncio.sleep(0.1 * (1 << attempt))
        code = int(reply[0]) if reply and isinstance(reply[0], int) else -1
        if code == 1 and len(reply) == 7:
            access, refresh = _text(reply[2]), _text(reply[3])
            try:
                access_expire, refresh_expire = int(_text(reply[4])), int(_text(reply[5]))
            except ValueError:
                access = ""
            if access and refresh:
                return TokenPair(f"{parsed.tag}.{access}", f"{parsed.tag}.{refresh}", access_expire, refresh_expire)
            raise RuntimeError("refresh rotation record damaged")
        if code in (4, 5):
            logger.error("SECURITY refresh token reuse detected: account=%s replayed=%s revoked=%s",
                         _text(reply[1]) if len(reply) > 1 else "", _text(reply[2]) if len(reply) > 2 else "",
                         code == 4)
            raise RefreshRejected("refresh token reused")
        if code in (0, 2, 3):
            raise RefreshRejected(f"refresh rejected (code={code})")
        raise RuntimeError("token family state damaged")

    async def _trim(self, account: str, protect: str) -> None:
        tag = keys.account_tag(account)
        try:
            await self._eval(RETIRE_REFRESH, [keys.account_refresh_key(account)], account, _MAX_REFRESH_PER_ACCOUNT,
                             keys.refresh_token_key_prefix(tag), keys.refresh_family_key_prefix(tag), protect)
        except Exception:
            logger.exception("trim refresh families failed")

    async def _load(self, parsed: _Parsed, key: str, rejected) -> tuple[bytes, TokenData]:
        raw = await self.redis.get(key)
        if raw is None:
            if rejected is None:
                raise LookupError(key)
            raise rejected("token not found or expired")
        try:
            data = json.loads(raw)
            token = TokenData(account=data["account"], auth_type=data["auth_type"], family_id=data["family_id"],
                              device_id=data.get("device_id", ""), created_at=data["created_at"])
            ok = (isinstance(token.account, str) and token.account and isinstance(token.auth_type, str)
                  and token.auth_type and isinstance(token.created_at, int) and token.created_at > 0
                  and token.family_id == parsed.family_id and keys.account_tag(token.account) == parsed.tag)
        except (ValueError, KeyError, TypeError):
            ok = False
        if not ok:
            if rejected is None:
                raise RuntimeError("token record invalid")
            raise rejected("token record invalid")
        return raw, token
