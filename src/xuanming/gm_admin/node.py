"""gm_admin 节点生命周期：配置校验 -> 打开存储与下游客户端 -> 启动 HTTP 服务 -> 优雅 SIGTERM 关闭。"""

from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import AsyncExitStack

from aiohttp import web

from ..protocol import Protocol
from ..settings import Settings
from .app import READY_STATE_KEY, create_gm_app
from .clients import GameServicesClient
from .config import GmAdminConfig
from .journal import BaseGmJournal, MemoryGmJournal, SqlGmJournal, open_pool

logger = logging.getLogger(__name__)


async def _close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def run(settings: Settings, *, environ=None) -> None:
    """运行 gm_admin HTTP 管理节点直到收到停止信号。"""
    config = GmAdminConfig.from_settings(settings, environ=environ)
    protocol = Protocol()
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
            journal: BaseGmJournal
            if config.use_memory_journal:
                journal = MemoryGmJournal()
            else:
                pool = await open_pool(
                    host=config.mysql_host,
                    port=config.mysql_port,
                    user=config.mysql_user,
                    password=config.mysql_password,
                    database=config.mysql_database,
                    minsize=config.min_open_conn,
                    maxsize=config.max_open_conn,
                    connect_timeout=min(config.sql_timeout_sec, 5.0),
                )
                stack.push_async_callback(_close_pool, pool)
                sql_journal = SqlGmJournal(pool)
                if config.bootstrap_schema:
                    await sql_journal.bootstrap()
                await sql_journal.verify_schema()
                journal = sql_journal

            services = GameServicesClient(protocol, config)
            stack.push_async_callback(services.close)

            app = create_gm_app(journal, services, config, ready=True)
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            stack.push_async_callback(runner.cleanup)

            site = web.TCPSite(runner, config.host, config.port)
            await site.start()
            logger.info(
                "gm_admin started host=%s port=%d mail_enabled=%s player_commands_enabled=%s",
                config.host,
                config.port,
                services.mail_enabled,
                services.player_commands_enabled,
            )
            await stop_event.wait()
            app[READY_STATE_KEY]["ready"] = False
            logger.info("gm_admin shutting down gracefully")
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)
