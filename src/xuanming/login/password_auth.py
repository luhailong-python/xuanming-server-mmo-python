"""生产口令认证、失败配额限流与开发口令提供器（对应 Go ``pkg/auth/password_*.go``）。

优先使用标准库 ``hashlib.scrypt`` 生成与校验 PHC 格式口令哈希，同时在可选依赖 ``argon2`` 可用时兼容
Go ``$argon2id$v=19$...`` 哈希；全部哈希比对均使用常数时间 ``hmac.compare_digest``。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
import secrets
import time
from dataclasses import dataclass

from . import keys

logger = logging.getLogger(__name__)

# 口令与账号形状边界（与 Go password_provider.go 一致）
PASSWORD_ACCOUNT_MAX_CHARS = 191
PASSWORD_MAX_BYTES = 1024

DEFAULT_KDF_CONCURRENCY = 2
MAX_KDF_CONCURRENCY = 8
DEFAULT_KDF_WAIT_TIMEOUT_S = 0.5
MAX_KDF_WAIT_TIMEOUT_S = 5.0

PASSWORD_QUOTA_BUDGET_S = 0.25
PASSWORD_ATTEMPT_LEASE_MS = 30_000

# scrypt 默认参数与安全上下界（N=2^14, r=8, p=1 -> ~16MiB 工作集）
_DEFAULT_SCRYPT_LN = 14
_DEFAULT_SCRYPT_R = 8
_DEFAULT_SCRYPT_P = 1
_DEFAULT_SALT_BYTES = 16
_DEFAULT_KEY_BYTES = 32

KNOWN_ACCOUNT = "known"
UNKNOWN_ACCOUNT = "unknown"

OUTCOME_SUCCESS = "success"
OUTCOME_MISMATCH = "mismatch"
OUTCOME_ABORT = "abort"

try:  # 可选：若运行环境已安装 argon2-cffi，则同时支持校验 Go 写入的 Argon2id 哈希
    from argon2.low_level import Type as _Argon2Type, hash_secret_raw as _argon2_hash_raw
except Exception:  # pragma: no cover
    _Argon2Type = None
    _argon2_hash_raw = None

# Lua 脚本与 Go ``password_quota.go`` 逐字对齐，实现单账号租约 + 失败窗 + 锁窗原子控制
_PASSWORD_QUOTA_STATE = """
local key, fp = KEYS[1], ARGV[1]
local limit, window, lock = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local token = ARGV[5]
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local function integer(n) return n and n >= 0 and n <= 9007199254740991 and n == math.floor(n) end
local function validToken(t) return t == '' or (#t == 32 and string.match(t, '^[0-9a-f]+$')) end
if not integer(limit) or limit < 1 or limit > 100 or not integer(window) or window < 1000 or window > 3600000
  or not integer(lock) or lock < 1000 or lock > 3600000 or not validToken(token) or token == '' then return {5, 0} end
local exists = redis.call('EXISTS', key) == 1
local s = {n=0, w=0, l=0, t='', e=0, d='', r='', u=0}
if exists then
  if redis.call('TYPE', key).ok ~= 'hash' or redis.call('HLEN', key) ~= 10 or redis.call('PTTL', key) < 0 then return {5, 0} end
  local values = redis.call('HMGET', key, 'v', 'fp', 'n', 'w', 'l', 't', 'e', 'd', 'r', 'u')
  for i=1,10 do if values[i] == false then return {5, 0} end end
  if values[1] ~= '1' or values[2] ~= fp then return {5, 0} end
  s = {n=tonumber(values[3]), w=tonumber(values[4]), l=tonumber(values[5]), t=values[6],
       e=tonumber(values[7]), d=values[8], r=values[9], u=tonumber(values[10])}
  if not integer(s.n) or s.n >= limit or not integer(s.w) or not integer(s.l) or not integer(s.e) or not integer(s.u)
    or not validToken(s.t) or not validToken(s.d) or ((s.n == 0) ~= (s.w == 0))
    or (s.l > 0 and (s.n ~= 0 or s.w ~= 0)) or ((s.t == '') ~= (s.e == 0))
    or (s.t ~= '' and s.d ~= '') or ((s.d == '') ~= (s.u == 0))
    or (s.d == '' and s.r ~= '') or (s.d ~= '' and s.r ~= 'success' and s.r ~= 'mismatch' and s.r ~= 'abort') then return {5, 0} end
end
local function expireWindows()
  if s.w > 0 and s.w <= now then s.n=0; s.w=0 end
  if s.l > 0 and s.l <= now then s.l=0 end
end
local function save()
  local deadline = math.max(s.w, s.l, s.e, s.u)
  redis.call('HSET', key, 'v', '1', 'fp', fp, 'n', s.n, 'w', s.w, 'l', s.l,
    't', s.t, 'e', s.e, 'd', s.d, 'r', s.r, 'u', s.u)
  redis.call('PEXPIREAT', key, deadline)
end
"""

PASSWORD_QUOTA_BEGIN = _PASSWORD_QUOTA_STATE + """
expireWindows()
if s.l > now then return {2, 0} end
if s.t ~= '' then
  if s.t == token then
    if s.e > now then return {1, s.e} else return {4, 0} end
  elseif s.e > now then return {3, 0} end
end
if s.d == token then return {4, 0} end
s.t=token; s.e=now+30000
s.d=''; s.r=''; s.u=0
save()
return {1, s.e}
"""

PASSWORD_QUOTA_FINISH = _PASSWORD_QUOTA_STATE + """
local outcome = ARGV[6]
if outcome ~= 'success' and outcome ~= 'mismatch' and outcome ~= 'abort' then return {6, 0} end
if not exists then return {4, 0} end
if s.d == token then
  if s.u <= now then return {4, 0} end
  if s.r ~= outcome then return {6, 0} end
  return {1, 0}
end
if s.t ~= token or s.e <= now then return {4, 0} end
expireWindows()
if outcome == 'mismatch' then
  if s.n == 0 then s.w=now+window end
  s.n=s.n+1
  if s.n >= limit then s.n=0; s.w=0; s.l=now+lock end
elseif outcome == 'success' then s.n=0; s.w=0; s.l=0 end
s.d=token; s.r=outcome; s.u=s.e
s.t=''; s.e=0
save()
return {1, 0}
"""


class PasswordAuthError(PermissionError):
    """口令认证拒绝基类（登录入口对外统一映射为 2000，防止账号枚举）。"""


class InvalidCredentialsError(PasswordAuthError):
    """账号或口令无效。"""


class PasswordQuotaDeniedError(PasswordAuthError):
    """触发失败限流锁定或在途并发互斥。"""


class PasswordQuotaUnavailableError(PasswordAuthError):
    """口令配额存储不可用（fail-closed 拒绝认证）。"""


class PasswordKdfCapacityError(PasswordAuthError):
    """KDF 并发槽已满或等待超时。"""


class PasswordCredentialUnavailableError(PasswordAuthError):
    """账号哈希记录损坏、空哈希或后端库故障。"""


@dataclass(frozen=True, slots=True)
class PasswordAttempt:
    key: str
    token: str
    deadline_monotonic: float


def validate_password_account(account: str) -> bool:
    """账号形状门禁：非空、无首尾空白、长度不超过 VARCHAR(191)。"""
    return bool(account) and account == account.strip() and len(account) <= PASSWORD_ACCOUNT_MAX_CHARS


def _b64_encode(raw: bytes) -> str:
    return base64.b64encode(raw).rstrip(b"=").decode("ascii")


def _b64_decode(encoded: str) -> bytes:
    return base64.b64decode(encoded + "=" * (-len(encoded) % 4), validate=True)


def hash_password(password: str, *, ln: int = _DEFAULT_SCRYPT_LN, r: int = _DEFAULT_SCRYPT_R,
                  p: int = _DEFAULT_SCRYPT_P) -> str:
    """使用标准库 ``hashlib.scrypt`` 生成 PHC 编码口令哈希。"""
    if not password:
        raise ValueError("password must not be empty")
    raw = password.encode("utf-8")
    if len(raw) > PASSWORD_MAX_BYTES:
        raise ValueError(f"password exceeds {PASSWORD_MAX_BYTES} bytes")
    salt = secrets.token_bytes(_DEFAULT_SALT_BYTES)
    n = 1 << ln
    maxmem = max(64 * 1024 * 1024, 256 * n * r)
    digest = hashlib.scrypt(raw, salt=salt, n=n, r=r, p=p, maxmem=maxmem, dklen=_DEFAULT_KEY_BYTES)
    return f"$scrypt$ln={ln},r={r},p={p}${_b64_encode(salt)}${_b64_encode(digest)}"


def verify_password_hash(password: str, encoded: str) -> bool:
    """校验 PHC 格式口令哈希（支持 ``$scrypt$`` 与可选 ``$argon2id$v=19$``），常数时间比较。"""
    if encoded.startswith("$scrypt$"):
        return _verify_scrypt(password, encoded)
    if encoded.startswith("$argon2id$v=19$"):
        return _verify_argon2id(password, encoded)
    raise ValueError("unsupported password hash format")


def _verify_scrypt(password: str, encoded: str) -> bool:
    parts = encoded.split("$")
    if len(parts) != 5 or parts[0] != "" or parts[1] != "scrypt":
        raise ValueError("unsupported scrypt hash format")
    params = _parse_kv(parts[2], ("ln", "r", "p"))
    ln, r, p = params["ln"], params["r"], params["p"]
    # 设定上下界：拒绝弱参数，同时防止异常库记录诱导超大内存分配
    if not (10 <= ln <= 18 and 1 <= r <= 16 and 1 <= p <= 8):
        raise ValueError("scrypt parameters outside policy")
    try:
        salt = _b64_decode(parts[3])
        expected = _b64_decode(parts[4])
    except ValueError as error:
        raise ValueError("invalid scrypt base64 payload") from error
    if not (16 <= len(salt) <= 64 and 16 <= len(expected) <= 64):
        raise ValueError("invalid scrypt salt or digest length")
    n = 1 << ln
    maxmem = max(64 * 1024 * 1024, 256 * n * r)
    actual = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=n, r=r, p=p, maxmem=maxmem, dklen=len(expected))
    return hmac.compare_digest(actual, expected)


def _verify_argon2id(password: str, encoded: str) -> bool:
    if _argon2_hash_raw is None or _Argon2Type is None:
        raise ValueError("argon2id verifier not installed")
    parts = encoded.split("$")
    if len(parts) != 6 or parts[0] != "" or parts[1] != "argon2id" or parts[2] != "v=19":
        raise ValueError("unsupported argon2id hash format")
    params = _parse_kv(parts[3], ("m", "t", "p"))
    memory, iterations, parallelism = params["m"], params["t"], params["p"]
    if not (19 * 1024 <= memory <= 256 * 1024 and 2 <= iterations <= 10 and 1 <= parallelism <= 16):
        raise ValueError("argon2id parameters outside policy")
    try:
        salt = _b64_decode(parts[4])
        expected = _b64_decode(parts[5])
    except ValueError as error:
        raise ValueError("invalid argon2id base64 payload") from error
    if not (16 <= len(salt) <= 64 and 16 <= len(expected) <= 64):
        raise ValueError("invalid argon2id salt or digest length")
    actual = _argon2_hash_raw(
        secret=password.encode("utf-8"),
        salt=salt,
        time_cost=iterations,
        memory_cost=memory,
        parallelism=parallelism,
        hash_len=len(expected),
        type=_Argon2Type.ID,
    )
    return hmac.compare_digest(actual, expected)


def _parse_kv(raw: str, expected_keys: tuple[str, ...]) -> dict[str, int]:
    fields = raw.split(",")
    if len(fields) != len(expected_keys):
        raise ValueError("invalid KDF parameter count")
    values: dict[str, int] = {}
    for field in fields:
        key, sep, val = field.partition("=")
        if not sep or key in values or not val.isdigit():
            raise ValueError("invalid KDF parameter entry")
        values[key] = int(val)
    if set(values) != set(expected_keys):
        raise ValueError("missing KDF parameter")
    return values


# -- 口令哈希存储（内存测试实现 + SQL 只读实现）------------------------------------------------------


class MemoryPasswordHashStore:
    """内存版口令哈希表（供单测与本地调试）。"""

    def __init__(self, accounts: dict[str, str] | None = None) -> None:
        self.accounts: dict[str, str] = dict(accounts or {})

    async def lookup_password_hash(self, account: str) -> tuple[str, str, bool]:
        if account not in self.accounts:
            return "", "", False
        return account, self.accounts[account], True


class SQLPasswordHashStore:
    """只读 ``user_accounts(account, password)`` SQL 存储（兼容 sqlite3 / DB-API 连接）。"""

    def __init__(self, connection, *, placeholder: str = "?") -> None:
        self._conn = connection
        self._query = f"SELECT account, password FROM user_accounts WHERE account = {placeholder} LIMIT 2"

    async def lookup_password_hash(self, account: str) -> tuple[str, str, bool]:
        return await asyncio.to_thread(self._query_sync, account)

    def _query_sync(self, account: str) -> tuple[str, str, bool]:
        cursor = self._conn.cursor()
        try:
            cursor.execute(self._query, (account,))
            rows = cursor.fetchall()
        finally:
            cursor.close()
        if not rows:
            return "", "", False
        if len(rows) > 1:
            raise RuntimeError(f"duplicate authoritative rows for account {account!r}")
        canonical, encoded = rows[0][0], rows[0][1]
        return str(canonical or ""), str(encoded or ""), True


# -- Redis 失败配额限流（Go ``RedisPasswordFailureQuota``）------------------------------------------


class RedisPasswordFailureQuota:
    """基于 Redis Lua 的单账号口令失败配额与并发租约，故障一律 fail-closed。"""

    def __init__(self, redis, *, limit: int = 5, window_ms: int = 300_000, lock_ms: int = 900_000,
                 clock=time.monotonic) -> None:
        if redis is None or not (1 <= limit <= 100) or not (1000 <= window_ms <= 3_600_000) or not (
            1000 <= lock_ms <= 3_600_000
        ):
            raise ValueError("口令配额要求有效 Redis、1..100 阈值与 1s..1h 窗口/锁窗")
        self.redis = redis
        self.limit = limit
        self.window_ms = window_ms
        self.lock_ms = lock_ms
        self.fingerprint = f"v1:{limit}:{window_ms}:{lock_ms}:{PASSWORD_ATTEMPT_LEASE_MS}"
        self.clock = clock

    async def begin(self, kind: str, identity: str) -> PasswordAttempt:
        if kind not in (KNOWN_ACCOUNT, UNKNOWN_ACCOUNT) or not identity:
            raise PasswordQuotaUnavailableError("invalid quota identity")
        token = secrets.token_hex(16)
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        key = keys.password_quota_key(f"{kind}:{digest}")
        attempt = PasswordAttempt(key=key, token=token, deadline_monotonic=self.clock() + PASSWORD_ATTEMPT_LEASE_MS / 1000)
        code = await self._execute("begin", PASSWORD_QUOTA_BEGIN, attempt, "")
        if code != 1:
            self._raise_rejected("begin", code)
        if self.clock() >= attempt.deadline_monotonic:
            self._raise_rejected("begin", 4)
        return attempt

    async def finish(self, attempt: PasswordAttempt, outcome: str) -> None:
        if not attempt.key or not attempt.token or outcome not in (OUTCOME_SUCCESS, OUTCOME_MISMATCH, OUTCOME_ABORT):
            raise PasswordQuotaUnavailableError("invalid quota finish input")
        if self.clock() >= attempt.deadline_monotonic:
            self._raise_rejected("finish", 4)
        code = await self._execute("finish", PASSWORD_QUOTA_FINISH, attempt, outcome)
        if code != 1:
            self._raise_rejected("finish", code)

    async def _execute(self, operation: str, script: str, attempt: PasswordAttempt, outcome: str) -> int:
        try:
            async with asyncio.timeout(PASSWORD_QUOTA_BUDGET_S):
                reply = await self.redis.eval(
                    script, 1, attempt.key, self.fingerprint,
                    self.limit, self.window_ms, self.lock_ms, attempt.token, outcome,
                )
        except Exception as error:
            logger.error("Password failure quota rejected authentication operation=%s reason=redis", operation)
            raise PasswordQuotaUnavailableError("password quota unavailable") from error
        if not isinstance(reply, (list, tuple)) or len(reply) != 2:
            logger.error("Password failure quota rejected authentication operation=%s reason=reply", operation)
            raise PasswordQuotaUnavailableError("password quota reply invalid")
        return int(reply[0])

    @staticmethod
    def _raise_rejected(operation: str, code: int) -> None:
        if code in (2, 3):
            raise PasswordQuotaDeniedError("password quota denied")
        if code in (4, 6):
            raise PasswordQuotaUnavailableError(f"password quota state rejected op={operation} code={code}")
        logger.error("Password failure quota rejected authentication operation=%s reason=state code=%d", operation, code)
        raise PasswordQuotaUnavailableError("password quota state damaged")


# -- 认证提供器（生产 + 开发 + 组合）----------------------------------------------------------------


class DevPasswordProvider:
    """开发环境共享口令认证器（仅接受指定前缀账号，常数时间比对）。"""

    def __init__(self, shared_secret: str, prefixes: tuple[str, ...] = ("robot_", "dev_")) -> None:
        self.shared_secret = shared_secret
        self.prefixes = prefixes

    async def validate_password(self, account: str, password: str) -> str:
        if (
            self.shared_secret
            and validate_password_account(account)
            and account.startswith(self.prefixes)
            and hmac.compare_digest(password.encode("utf-8"), self.shared_secret.encode("utf-8"))
        ):
            return account
        raise InvalidCredentialsError("invalid dev credentials")


class ProductionPasswordProvider:
    """生产口令认证器：权威表查哈希 + 失败配额限流 + 有界 KDF 并发 + 未知账号等耗时防枚举。"""

    def __init__(
        self,
        store,
        quota: RedisPasswordFailureQuota,
        *,
        concurrency: int = DEFAULT_KDF_CONCURRENCY,
        wait_timeout_s: float = DEFAULT_KDF_WAIT_TIMEOUT_S,
        dummy_hash: str | None = None,
        verify_fn=verify_password_hash,
    ) -> None:
        if store is None or quota is None:
            raise ValueError("production password auth requires store and quota")
        if not 1 <= concurrency <= MAX_KDF_CONCURRENCY:
            raise ValueError(f"password KDF concurrency must be in 1..{MAX_KDF_CONCURRENCY}")
        if not 0 < wait_timeout_s <= MAX_KDF_WAIT_TIMEOUT_S:
            raise ValueError(f"password KDF wait timeout must be in (0, {MAX_KDF_WAIT_TIMEOUT_S}]")
        self.store = store
        self.quota = quota
        self.wait_timeout_s = wait_timeout_s
        self._semaphore = asyncio.Semaphore(concurrency)
        self._verify_fn = verify_fn
        self._dummy_hash = dummy_hash or hash_password("invalid-account-dummy-password")

    async def validate_password(self, account: str, password: str) -> str:
        if not validate_password_account(account) or not password or len(password.encode("utf-8")) > PASSWORD_MAX_BYTES:
            raise InvalidCredentialsError("invalid credentials")
        try:
            canonical, encoded_hash, found = await self.store.lookup_password_hash(account)
        except Exception as error:
            logger.error("Password credential store unavailable: %s", error)
            raise PasswordCredentialUnavailableError("password store unavailable") from error
        if found and not canonical:
            raise PasswordCredentialUnavailableError("canonical account empty")

        kind, identity = (KNOWN_ACCOUNT, canonical) if found else (UNKNOWN_ACCOUNT, account)
        attempt = await self.quota.begin(kind, identity)
        finish_attempted = False
        try:
            target_hash = encoded_hash if (found and encoded_hash) else self._dummy_hash
            try:
                matched = await self._verify_with_limit(password, target_hash)
            except PasswordKdfCapacityError:
                raise
            except Exception as error:
                # 损坏哈希仍补跑一次 dummy KDF 抹平时序，随后按凭据不可用拒绝
                await self._verify_with_limit(password, self._dummy_hash)
                raise PasswordCredentialUnavailableError("corrupt password hash") from error
            if found and not encoded_hash:
                raise PasswordCredentialUnavailableError("empty password hash")
            outcome = OUTCOME_SUCCESS if (found and matched) else OUTCOME_MISMATCH
            finish_attempted = True
            await self.quota.finish(attempt, outcome)
            if outcome != OUTCOME_SUCCESS:
                raise InvalidCredentialsError("invalid credentials")
            return canonical
        finally:
            if not finish_attempted:
                try:
                    await self.quota.finish(attempt, OUTCOME_ABORT)
                except Exception:
                    pass

    async def _verify_with_limit(self, password: str, encoded: str) -> bool:
        try:
            async with asyncio.timeout(self.wait_timeout_s):
                await self._semaphore.acquire()
        except TimeoutError as error:
            raise PasswordKdfCapacityError("password KDF capacity exhausted") from error
        try:
            return await asyncio.to_thread(self._verify_fn, password, encoded)
        finally:
            self._semaphore.release()


class CompositePasswordProvider:
    """组合认证器：命中开发前缀且配置了开发密钥时走开发通道，否则走生产口令提供器。"""

    def __init__(self, production: ProductionPasswordProvider, dev: DevPasswordProvider | None = None) -> None:
        self.production = production
        self.dev = dev

    async def validate_password(self, account: str, password: str) -> str:
        if self.dev is not None and self.dev.shared_secret and account.startswith(self.dev.prefixes):
            return await self.dev.validate_password(account, password)
        return await self.production.validate_password(account, password)
