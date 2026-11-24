"""friend 的 Redis 适配层：会话在线读取、配额限流、落地同步摘要 bump 与缓存失效。

严格区分两类 Redis 句柄（与 Go config.ControlRedis / PlayerRedis 契约一致）：
- ControlRedis：friend 私有键（申请配额、在线目录配额、私有列表缓存失效）；
- PlayerRedis：玩家域跨运行时契约键（``player:session:{<id>}`` 与 ``player:{<id>}:sync_digest``）。
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Protocol as TypingProtocol

from google.protobuf.message import DecodeError, Message

from ..protocol import Protocol
from .errors import StoreUnavailableError
from .model import (
    DIRECTORY_QUOTA_PER_MINUTE,
    DIRECTORY_SCAN_COUNT,
    OnlineStatus,
    REQUEST_QUOTA_WINDOW_SEC,
    SESSION_KEY_PATTERN,
    SESSION_STATE_ONLINE,
    STANDALONE_SCAN_DIGEST,
    SYNC_DIGEST_TTL_SEC,
    ScanCursor,
    ScanPageResult,
    directory_quota_key,
    player_session_key,
    request_quota_key,
    sync_digest_key,
)

logger = logging.getLogger(__name__)

DEFAULT_SESSION_BATCH_SIZE = 256

# 与 Go logic/rate_quota.go 逐字一致：单键原子 INCR + 首次/无 TTL 补设 EXPIRE。
REQUEST_QUOTA_SCRIPT = """local n = redis.call("INCR", KEYS[1])
if n == 1 or redis.call("TTL", KEYS[1]) == -1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return n"""

# 与 Go shared/playersync/digest.go 逐字一致：首次以 Redis TIME 起种 g，再逐域 HINCRBY 并续期 30 天。
SYNC_DIGEST_BUMP_SCRIPT = """if redis.call('HEXISTS', KEYS[1], 'g') == 0 then
  local t = redis.call('TIME')
  redis.call('HSET', KEYS[1], 'g', tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000))
end
for i = 2, #ARGV do
  redis.call('HINCRBY', KEYS[1], ARGV[i], 1)
end
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
"""

# 与 Go data/friend_repo.go 一致：同槽原子递增 generation 并删除数据键。
INVALIDATE_CACHE_SCRIPT = """redis.call("INCR", KEYS[1])
redis.call("DEL", KEYS[2])
return 1
"""


class SessionStore(TypingProtocol):
    """玩家域会话读取协议（供 GetFriendList / RecommendFriends / OnlineDirectory / Push 共用）。"""

    async def batch_online_status(self, player_ids: Sequence[int]) -> dict[int, OnlineStatus]: ...

    async def fill_online_status(self, player_ids: Sequence[int]) -> dict[int, OnlineStatus]: ...

    async def load_session(self, player_id: int) -> Message | None: ...

    async def scan_session_page(
        self, cursor: ScanCursor, *, count: int = DIRECTORY_SCAN_COUNT
    ) -> ScanPageResult: ...

    async def mget_sessions_raw(self, keys: Sequence[str]) -> list[bytes | None]: ...


class QuotaLimiter(TypingProtocol):
    """控制面 Redis 配额限流协议。"""

    async def allow_friend_request(self, player_id: int, limit: int) -> bool: ...

    async def allow_directory_page(
        self, player_id: int, limit: int = DIRECTORY_QUOTA_PER_MINUTE
    ) -> bool: ...


class SyncDigestBumper(TypingProtocol):
    """玩家域落地恢复同步摘要 bump 协议（提交后调用，失败只打日志不抛异常）。"""

    async def bump_all(self, player_ids: Sequence[int], *domains: int) -> None: ...


class CacheInvalidator(TypingProtocol):
    """写事务提交后的私有缓存失效协议（失败只打日志不抛异常）。"""

    async def invalidate_after_commit(self, *keys: str) -> None: ...


def _to_bytes(value) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


def _to_str(value) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="ignore")
    return str(value)


def _unique_nonzero(player_ids: Sequence[int]) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for pid in player_ids:
        val = int(pid)
        if val <= 0 or val in seen:
            continue
        seen.add(val)
        out.append(val)
    return out


class RedisSessionStore:
    """基于 PlayerRedis 读取跨运行时 ``player:session:{<id>}`` 的会话读取器。"""

    def __init__(self, redis_client, protocol: Protocol, *, batch_size: int = DEFAULT_SESSION_BATCH_SIZE) -> None:
        self._redis = redis_client
        self._protocol = protocol
        self._batch_size = batch_size if batch_size > 0 else DEFAULT_SESSION_BATCH_SIZE

    async def mget_sessions_raw(self, keys: Sequence[str]) -> list[bytes | None]:
        if not keys:
            return []
        if self._redis is None:
            raise StoreUnavailableError("player redis handle is not configured")
        try:
            values = await self._redis.mget(list(keys))
        except Exception as exc:
            raise StoreUnavailableError(f"mget player sessions failed: {exc}") from exc
        if len(values) != len(keys):
            raise StoreUnavailableError(f"mget player sessions returned {len(values)} values for {len(keys)} keys")
        return [_to_bytes(item) for item in values]

    async def _batch_online_internal(
        self, player_ids: Sequence[int]
    ) -> tuple[dict[int, OnlineStatus], Exception | None]:
        ids = _unique_nonzero(player_ids)
        if not ids:
            return {}, None
        if self._redis is None:
            return {}, StoreUnavailableError("player redis handle is not configured")

        states: dict[int, OnlineStatus] = {}
        first_err: Exception | None = None
        for start in range(0, len(ids), self._batch_size):
            chunk = ids[start : start + self._batch_size]
            keys = [player_session_key(pid) for pid in chunk]
            try:
                values = await self.mget_sessions_raw(keys)
            except Exception as exc:
                logger.error("friend online status mget failed count=%d error=%s", len(chunk), exc)
                if first_err is None:
                    first_err = exc
                continue
            for pid, raw in zip(chunk, values):
                if not raw:
                    continue
                try:
                    sess = self._protocol.parse("playerlocator.PlayerSession", raw)
                except DecodeError as exc:
                    err = StoreUnavailableError(f"corrupt PlayerSession payload player={pid}")
                    logger.error("friend online status decode failed player=%d error=%s", pid, exc)
                    if first_err is None:
                        first_err = err
                    continue
                if int(sess.player_id) != pid:
                    err = StoreUnavailableError(f"PlayerSession identity mismatch player={pid}")
                    logger.error(
                        "friend online status identity mismatch expected=%d actual=%d",
                        pid,
                        int(sess.player_id),
                    )
                    if first_err is None:
                        first_err = err
                    continue
                if int(sess.state) != SESSION_STATE_ONLINE:
                    continue
                states[pid] = OnlineStatus(online=True, last_active_ms=int(sess.last_active_ts))
        return states, first_err

    async def batch_online_status(self, player_ids: Sequence[int]) -> dict[int, OnlineStatus]:
        """可靠在线状态查询（GetFriendList 专用）：Redis / 解码 / 身份任一异常均 fail-closed 抛错。"""
        states, err = await self._batch_online_internal(player_ids)
        if err is not None:
            raise err
        return states

    async def fill_online_status(self, player_ids: Sequence[int]) -> dict[int, OnlineStatus]:
        """可降级在线状态查询（普通 RecommendFriends 专用）：忽略局部故障并返回健康条目。"""
        states, _ = await self._batch_online_internal(player_ids)
        return states

    async def load_session(self, player_id: int) -> Message | None:
        """读取单个玩家会话供 S2C 推送路由；键不存在返回 None，解码失败抛错。"""
        if self._redis is None:
            raise StoreUnavailableError("player redis handle is not configured")
        raw = _to_bytes(await self._redis.get(player_session_key(player_id)))
        if not raw:
            return None
        try:
            return self._protocol.parse("playerlocator.PlayerSession", raw)
        except DecodeError as exc:
            raise StoreUnavailableError(f"corrupt PlayerSession payload player={player_id}") from exc

    async def scan_session_page(
        self, cursor: ScanCursor, *, count: int = DIRECTORY_SCAN_COUNT
    ) -> ScanPageResult:
        """分页 SCAN 玩家会话键（单实例 digest 固定为 00000000，与 Go clusterredis.ScanPage 对齐）。"""
        if self._redis is None:
            raise StoreUnavailableError("player redis handle is not configured")
        restarted = False
        current = cursor
        if cursor.is_start or cursor.digest != STANDALONE_SCAN_DIGEST or cursor.master != 0:
            restarted = not cursor.is_start
            current = ScanCursor(digest=STANDALONE_SCAN_DIGEST, master=0, node=0)
        try:
            next_node, raw_keys = await self._redis.scan(
                cursor=current.node, match=SESSION_KEY_PATTERN, count=count
            )
        except Exception as exc:
            raise StoreUnavailableError(f"scan player sessions failed: {exc}") from exc
        keys = tuple(_to_str(k) for k in raw_keys)
        next_cursor = (
            ScanCursor(digest=STANDALONE_SCAN_DIGEST, master=0, node=int(next_node))
            if int(next_node) != 0
            else ScanCursor()
        )
        return ScanPageResult(keys=keys, current=current, next=next_cursor, restarted=restarted)


class RedisQuotaLimiter:
    """基于 ControlRedis 的每分钟固定窗口配额限流器。"""

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    async def allow_friend_request(self, player_id: int, limit: int) -> bool:
        """好友申请限流：与 Go rate_quota.go 一致采用 fail-open（Redis 故障不阻断全服加好友）。"""
        if limit <= 0:
            return True
        if self._redis is None:
            logger.warning("friend control redis handle nil; request quota bypassed player=%d", player_id)
            return True
        try:
            raw = await self._redis.eval(
                REQUEST_QUOTA_SCRIPT, 1, request_quota_key(player_id), str(REQUEST_QUOTA_WINDOW_SEC)
            )
        except Exception as exc:
            logger.error("friend request quota eval failed; fail-open player=%d error=%s", player_id, exc)
            return True
        if isinstance(raw, bool) or not isinstance(raw, int):
            logger.error("friend request quota unexpected reply type=%s player=%d", type(raw).__name__, player_id)
            return True
        if raw > limit:
            logger.info("friend request rate limit reached player=%d count=%d limit=%d", player_id, raw, limit)
            return False
        return True

    async def allow_directory_page(
        self, player_id: int, limit: int = DIRECTORY_QUOTA_PER_MINUTE
    ) -> bool:
        """在线目录分页限流：与 Go online_directory.go 一致采用 fail-closed（Redis 故障回 1003）。"""
        if self._redis is None:
            raise StoreUnavailableError("online directory limiter unavailable")
        try:
            raw = await self._redis.eval(
                REQUEST_QUOTA_SCRIPT, 1, directory_quota_key(player_id), str(REQUEST_QUOTA_WINDOW_SEC)
            )
        except Exception as exc:
            logger.error("friend online directory quota eval failed player=%d error=%s", player_id, exc)
            raise StoreUnavailableError(f"online directory limiter unavailable: {exc}") from exc
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise StoreUnavailableError("online directory limiter returned non-integer reply")
        return raw <= limit


class RedisSyncDigestBumper:
    """基于 PlayerRedis 的落地同步摘要 Lua bump 器（与 Go shared/playersync 对齐）。"""

    def __init__(self, redis_client, *, ttl_sec: int = SYNC_DIGEST_TTL_SEC) -> None:
        self._redis = redis_client
        self._ttl_arg = str(int(ttl_sec))

    async def bump_all(self, player_ids: Sequence[int], *domains: int) -> None:
        if self._redis is None:
            return
        targets = _unique_nonzero(player_ids)
        if not targets:
            return
        domain_args: list[str] = []
        seen_domains: set[int] = set()
        for domain in domains:
            val = int(domain)
            if val <= 0:
                logger.error("playersync invalid sync domain ignored service=friend domain=%d", val)
                continue
            if val in seen_domains:
                continue
            seen_domains.add(val)
            domain_args.append(str(val))
        if not domain_args:
            return
        args = [self._ttl_arg, *domain_args]
        for pid in targets:
            try:
                await self._redis.eval(SYNC_DIGEST_BUMP_SCRIPT, 1, sync_digest_key(pid), *args)
            except Exception as exc:
                logger.warning(
                    "playersync digest bump failed service=friend player=%d domains=%s error=%s",
                    pid,
                    domain_args,
                    exc,
                )


class RedisCacheInvalidator:
    """写事务提交后尽力而为的私有列表缓存失效器（失败只打日志，不影响提交结果）。"""

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    async def invalidate_after_commit(self, *keys: str) -> None:
        if self._redis is None or not keys:
            return
        for key in keys:
            try:
                await self._redis.eval(INVALIDATE_CACHE_SCRIPT, 2, f"{key}:generation", key)
            except Exception as exc:
                logger.error(
                    "friend cache invalidation failed after commit key=%s error=%s",
                    key,
                    exc,
                )
