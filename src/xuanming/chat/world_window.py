"""世界频道近窗快照与单飞重读器（与 Go authority/world_window.go 对齐）。

每个副本持有一份最新若干行的快照：
- 未过期直接命中；
- 过期时同一时刻只允许一次重读在途（single-flight），后台任务独立于发起方取消；
- 本地世界频道写入或撤回后调用 ``invalidate()`` 递增 epoch，在途旧重读不回填缓存，保证读己之写；
- 重读失败绝不写入缓存。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .errors import StoreUnavailableError
from .model import HistoryRow

logger = logging.getLogger(__name__)

# 与 Go worldWindowLoadTimeout=3s 一致：小于 4s RPC 预算，防止慢 SQL 拖挂等待方。
WORLD_WINDOW_LOAD_TIMEOUT_SEC = 3.0


def _silence_unretrieved(fut: asyncio.Future) -> None:
    """所有等待方都提前取消时吞掉未取异常，避免 asyncio 输出噪音告警。"""
    if not fut.cancelled():
        fut.exception()


@dataclass(frozen=True, slots=True)
class WorldSnapshot:
    """一次 SQL 合并读取的世界频道近窗快照。"""

    rows: tuple[HistoryRow, ...]
    db_now_ms: int
    taken_at: float = 0.0

    def now_ms(self, at: float) -> int:
        """按单调时钟将数据库时间推进到当前时刻，避免依赖本机墙钟。"""
        age_sec = at - self.taken_at
        if age_sec < 0.0:
            age_sec = 0.0
        return self.db_now_ms + int(age_sec * 1000)


class WorldWindow:
    """世界频道近窗单飞缓存。"""

    def __init__(
        self,
        max_age_sec: float,
        load: Callable[[], Awaitable[WorldSnapshot]],
        *,
        monotonic_clock: Callable[[], float] = time.monotonic,
        load_timeout_sec: float = WORLD_WINDOW_LOAD_TIMEOUT_SEC,
    ) -> None:
        self._max_age = max_age_sec
        self._load = load
        self._now = monotonic_clock
        self._load_timeout = load_timeout_sec
        self._snap: WorldSnapshot | None = None
        self._inflight: asyncio.Future[WorldSnapshot] | None = None
        self._epoch: int = 0
        self._tasks: set[asyncio.Task] = set()

    def now(self) -> float:
        """返回注入的单调时钟当前读数（秒）。"""
        return self._now()

    async def get(self) -> WorldSnapshot:
        """返回未过期快照；过期时加入或启动一次独立单飞重读。"""
        snap = self._snap
        if snap is not None and (self._now() - snap.taken_at) < self._max_age:
            return snap

        fut = self._inflight
        if fut is None:
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            fut.add_done_callback(_silence_unretrieved)
            self._inflight = fut
            epoch = self._epoch
            # 重读在独立 Task 内执行，发起请求取消不会连带中止其他等待方。
            task = asyncio.create_task(self._run(fut, epoch), name="chat-world-window-load")
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        return await asyncio.shield(fut)

    async def _run(self, fut: asyncio.Future[WorldSnapshot], epoch: int) -> None:
        # 在读库之前记录单调时刻：推算的数据库时间偏保守，年龄只会略大不会偏小。
        started = self._now()
        result: WorldSnapshot | None = None
        error: BaseException | None = None
        try:
            async with asyncio.timeout(self._load_timeout):
                loaded = await self._load()
            if loaded is None:
                error = StoreUnavailableError("world window snapshot load returned None")
            else:
                result = WorldSnapshot(
                    rows=tuple(loaded.rows),
                    db_now_ms=int(loaded.db_now_ms),
                    taken_at=started,
                )
        except asyncio.CancelledError as exc:
            error = exc
            raise
        except Exception as exc:
            error = exc
            logger.warning("world window snapshot reload failed: %s", exc)
        finally:
            if self._inflight is fut:
                self._inflight = None
            if error is None and result is not None and self._epoch == epoch:
                self._snap = result
            if not fut.done():
                if error is None and result is not None:
                    fut.set_result(result)
                else:
                    fut.set_exception(error or StoreUnavailableError("world window snapshot load aborted"))

    def invalidate(self) -> None:
        """本地世界频道写入或撤回成功后作废快照并推进 epoch。"""
        self._snap = None
        self._inflight = None
        self._epoch += 1

    async def close(self) -> None:
        """关闭前取消并等待仍在途的后台快照加载任务。"""
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._inflight = None
        self._snap = None
