"""mall_game 节点生命周期：配置校验 -> 建池验表 -> 启动 aiohttp HTTP 服务与后台清理协程 -> 响应 SIGTERM 优雅下线。"""

from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import AsyncExitStack

from aiohttp import web

from ..settings import Settings
from .app import create_mall_app
from .config import MallGameConfig
from .sql_store import SqlMallStore, open_pool
from .store import MallStore

logger = logging.getLogger(__name__)

_NONCE_PRUNE_INTERVAL_SEC = 10.0
_ORDER_EXPIRE_INTERVAL_SEC = 15.0


async def _close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def _background_maintenance(store: MallStore, stop: asyncio.Event) -> None:
    """后台定时清理过期 nonce 与超时未支付订单（与 Java @Scheduled 任务对齐）。"""
    nonce_elapsed = 0.0
    order_elapsed = 0.0
    step = 1.0
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=step)
            break
        except TimeoutError:
            pass
        nonce_elapsed += step
        order_elapsed += step
        if nonce_elapsed >= _NONCE_PRUNE_INTERVAL_SEC:
            nonce_elapsed = 0.0
            try:
                await store.prune_expired_nonces()
            except Exception as exc:
                logger.warning("scheduled nonce cleanup failed: err=%s", exc)
        if order_elapsed >= _ORDER_EXPIRE_INTERVAL_SEC:
            order_elapsed = 0.0
            try:
                expired = await store.expire_overdue_orders()
                if expired > 0:
                    logger.info("expired overdue unpaid orders: count=%d", expired)
            except Exception as exc:
                logger.warning("scheduled unpaid order expiration failed: err=%s", exc)


async def run(settings: Settings) -> None:
    """运行 mall_game HTTP 节点直至收到 SIGTERM。"""
    config = MallGameConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            pool = await open_pool(
                host=config.mysql_host,
                port=config.mysql_port,
                user=config.mysql_user,
                password=config.mysql_password,
                database=config.mysql_database,
                require_tidb=config.require_tidb,
                minsize=config.mysql_min_conn,
                maxsize=config.mysql_max_conn,
            )
            stack.push_async_callback(_close_pool, pool)

            store = SqlMallStore(pool, config)
            if config.bootstrap_schema:
                await store.bootstrap()
            await store.verify_schema()

            app = create_mall_app(store, config)
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            stack.push_async_callback(runner.cleanup)

            site = web.TCPSite(runner, host=config.host, port=config.port)
            await site.start()

            maint_task = asyncio.create_task(_background_maintenance(store, stop), name="mall-game-maintenance")
            stack.callback(maint_task.cancel)

            logger.info(
                "mall_game node started host=%s port=%d run_mode=%s require_tidb=%s",
                config.host,
                config.port,
                config.run_mode,
                config.require_tidb,
            )
            await stop.wait()
            logger.info("mall_game node shutting down")
    finally:
        signal.signal(signal.SIGTERM, previous)
