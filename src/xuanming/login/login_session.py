"""Per-connection login state and the account device set (Go ``pkg/loginsession``)."""

import logging

from . import keys, session_fence

logger = logging.getLogger(__name__)


async def register_device(redis, account: str, conn: str, ttl_ms: int) -> None:
    device_set = keys.account_device_set_key(account)
    async with redis.pipeline(transaction=True) as pipe:
        pipe.sadd(device_set, conn)
        pipe.pexpire(device_set, ttl_ms)
        await pipe.execute()


async def renew(redis, conn: str, ttl_ms: int) -> bool:
    kept, account, restored = await session_fence.renew(redis, conn, ttl_ms)
    if account:
        await register_device(redis, account, conn, ttl_ms)
    if restored:
        logger.info("登录态已按绑定记录重建 session=%s", conn)
    return kept


async def prune_stale_devices(redis, account: str) -> int:
    """Drop device-set members whose login_session is gone; read failures prune nothing."""
    device_set = keys.account_device_set_key(account)
    try:
        members = await redis.smembers(device_set)
        # shortcut: one GET per member (members are on different slots), fine for MaxDevicesPerAccount-sized sets.
        stale = [member for member in members
                 if await redis.get(keys.login_session_key(member.decode() if isinstance(member, bytes) else member))
                 is None]
        if stale:
            await redis.srem(device_set, *stale)
        return len(stale)
    except Exception:
        logger.exception("清理设备集失败 key=%s", device_set)
        return 0


async def get_account(redis, conn: str) -> str | None:
    account = await redis.get(keys.login_session_key(conn))
    return account.decode() if isinstance(account, bytes) else account


async def cleanup(redis, conn: str) -> None:
    """Best effort like Go: leftovers expire with the session TTL."""
    try:
        account = await get_account(redis, conn)
        if account is not None:
            await redis.srem(keys.account_device_set_key(account), conn)
        await redis.delete(keys.login_session_key(conn))
    except Exception:
        logger.exception("清理登录态失败 session=%s", conn)
