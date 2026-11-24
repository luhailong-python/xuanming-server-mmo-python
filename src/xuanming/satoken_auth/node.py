"""satoken_auth 节点生命周期：配置校验 -> 打开 MySQL/Redis 或内存存储 -> 启动 HTTP 服务 -> 优雅 SIGTERM 关闭。"""

from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import AsyncExitStack

from aiohttp import web

from ..settings import Settings
from .app import READY_STATE_KEY, create_auth_app
from .config import SaTokenAuthConfig
from .store import (
    AuthStore,
    MemoryAuthStore,
    MemoryTokenSessionStore,
    RedisTokenSessionStore,
    SqlAuthStore,
    TokenSessionStore,
)

logger = logging.getLogger(__name__)


async def _close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def _close_redis(client) -> None:
    await client.aclose()


async def run(settings: Settings, *, environ=None) -> None:
    """运行 Sa-Token HTTP 认证节点直到收到停止信号。"""
    config = SaTokenAuthConfig.from_settings(settings, environ=environ)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    previous_sigint = signal.getsignal(signal.SIGINT)

    def _request_shutdown(*_args) -> None:
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    try:
        async with AsyncExitStack() as stack:
            user_store: AuthStore
            token_store: TokenSessionStore

            if config.use_memory_store:
                user_store = MemoryAuthStore()
            else:
                import aiomysql

                pool = await aiomysql.create_pool(
                    host=config.mysql_host,
                    port=config.mysql_port,
                    user=config.mysql_user,
                    password=config.mysql_password,
                    db=config.mysql_database,
                    minsize=config.min_open_conn,
                    maxsize=config.max_open_conn,
                    connect_timeout=min(config.sql_timeout_sec, 5.0),
                    autocommit=True,
                    charset="utf8mb4",
                )
                stack.push_async_callback(_close_pool, pool)
                user_store = SqlAuthStore(pool)

            if config.use_memory_store or not config.redis_url:
                token_store = MemoryTokenSessionStore()
            else:
                import redis.asyncio as aioredis

                redis_client = aioredis.from_url(config.redis_url, decode_responses=True)
                stack.push_async_callback(_close_redis, redis_client)
                token_store = RedisTokenSessionStore(redis_client)

            app = create_auth_app(user_store, token_store, config, ready=True)
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            stack.push_async_callback(runner.cleanup)

            site = web.TCPSite(runner, config.host, config.port)
            await site.start()
            logger.info(
                "satoken_auth started host=%s port=%d profile=%s dev_login_enabled=%s",
                config.host,
                config.port,
                config.profile,
                config.allow_dev_login,
            )
            await stop_event.wait()
            app[READY_STATE_KEY]["ready"] = False
            logger.info("satoken_auth shutting down gracefully")
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)


__all__ = ["run"]
