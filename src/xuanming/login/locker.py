"""SETNX lock with compare-and-delete release (Go ``pkg/locker``)."""

import asyncio
import logging
import time
import uuid

logger = logging.getLogger(__name__)

RELEASE = """
if redis.call("get", KEYS[1]) == ARGV[1] then
\treturn redis.call("del", KEYS[1])
else
\treturn 0
end
"""

RENEW = """
if redis.call("get", KEYS[1]) == ARGV[1] then
\treturn redis.call("pexpire", KEYS[1], ARGV[2])
else
\treturn 0
end
"""


class RedisLock:
    def __init__(self, redis, key: str, ttl_ms: int) -> None:
        self.redis = redis
        self.key = key
        self.ttl_ms = ttl_ms
        self.value = str(uuid.uuid4())

    async def acquire(self) -> bool:
        if await self.redis.set(self.key, self.value, nx=True, px=self.ttl_ms):
            return True
        # A replayed SETNX whose first attempt won sees our own value: the lock is ours.
        current = await self.redis.get(self.key)
        return current is not None and current.decode() == self.value

    async def release(self) -> bool:
        return int(await self.redis.eval(RELEASE, 1, self.key, self.value)) == 1

    async def renew(self) -> bool:
        return int(await self.redis.eval(RENEW, 1, self.key, self.value, self.ttl_ms)) == 1

    async def still_held(self) -> bool:
        current = await self.redis.get(self.key)
        return current is not None and current.decode() == self.value

    async def heartbeat(self, on_lost) -> None:
        """Renew every TTL/3 until cancelled; call ``on_lost`` once if ownership is lost."""
        interval = self.ttl_ms / 3000
        last_ok = time.monotonic()
        while True:
            await asyncio.sleep(interval)
            sent = time.monotonic()
            try:
                async with asyncio.timeout(min(2, interval)):
                    held = await self.renew()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("锁续期失败 key=%s", self.key)
                # shortcut: self-fence after two missed beats (Go derives this from the client's worst-case call).
                if time.monotonic() - last_ok < 2 * interval:
                    continue
                held = False
            if not held:
                on_lost()
                return
            last_ok = sent
