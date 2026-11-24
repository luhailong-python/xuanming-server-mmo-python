"""mail 过期邮件后台清理循环（MailExpirySweeper，与 Go maintenance/sweep.go 及 data/sweep.go 对齐）。"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable
from typing import Protocol as TypingProtocol

from .model import (
    MS_PER_DAY,
    SWEEP_MODE_DELETE,
    SWEEP_MODE_REPORT_ONLY,
    SweepReport,
)

logger = logging.getLogger(__name__)

DEFAULT_SWEEP_RETENTION_DAYS = 7
DEFAULT_SWEEP_BATCH_LIMIT = 1000
DEFAULT_SWEEP_INTERVAL_SEC = 300.0
DEFAULT_SWEEP_ROUND_BUDGET_SEC = 30.0


class SweepStore(TypingProtocol):
    """过期清理所需的存储接口（SqlMailStore 与 MemoryMailStore 均实现该协议）。"""

    async def sweep(
        self,
        cutoff_ms: int,
        limit: int,
        delete_rows: bool,
        *,
        owned_shards: frozenset[int] | None = None,
    ) -> SweepReport: ...


class MailExpirySweeper:
    """有界过期邮件清理器：只回收超过保留期且未受保护的非活动邮件，绝不碰审计表与在途记录。"""

    def __init__(
        self,
        store: SweepStore,
        *,
        mode: str = SWEEP_MODE_REPORT_ONLY,
        retention_days: int = DEFAULT_SWEEP_RETENTION_DAYS,
        batch_limit: int = DEFAULT_SWEEP_BATCH_LIMIT,
        interval_sec: float = DEFAULT_SWEEP_INTERVAL_SEC,
        round_budget_sec: float = DEFAULT_SWEEP_ROUND_BUDGET_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        owned_shards: Callable[[], frozenset[int] | None] | None = None,
        rnd: Callable[[], float] | None = None,
    ) -> None:
        if mode not in (SWEEP_MODE_REPORT_ONLY, SWEEP_MODE_DELETE):
            raise ValueError(f"mail sweep mode invalid: {mode!r}")
        if not 1 <= retention_days <= 36500:
            raise ValueError(f"mail sweep retention_days must be within [1, 36500], got {retention_days}")
        if not 1 <= batch_limit <= 10000:
            raise ValueError(f"mail sweep batch_limit must be within [1, 10000], got {batch_limit}")
        if not 1.0 <= interval_sec <= 86400.0:
            raise ValueError(f"mail sweep interval_sec must be within [1, 86400], got {interval_sec}")
        if round_budget_sec <= 0:
            raise ValueError(f"mail sweep round_budget_sec must be > 0, got {round_budget_sec}")

        self._store = store
        self.mode = mode
        self.retention_days = int(retention_days)
        self.batch_limit = int(batch_limit)
        self.interval_sec = float(interval_sec)
        self._round_budget_sec = float(round_budget_sec)
        self._clock_ms = clock_ms
        self._owned_shards = owned_shards
        self._rnd = rnd if rnd is not None else random.random
        self.last_report: SweepReport | None = None
        self.last_run_unixtime: float = 0.0

    @property
    def delete_rows(self) -> bool:
        return self.mode == SWEEP_MODE_DELETE

    async def sweep_once(self, *, owned_shards: frozenset[int] | None = None) -> SweepReport | None:
        """执行单轮有界清理；单轮异常仅打错误日志并返回 None，不击穿后台循环。"""
        now_ms = self._clock_ms()
        cutoff_ms = now_ms - self.retention_days * MS_PER_DAY
        if cutoff_ms <= 0:
            report = SweepReport()
            self.last_report = report
            self.last_run_unixtime = now_ms / 1000.0
            return report

        shards = owned_shards
        if shards is None and self._owned_shards is not None:
            shards = self._owned_shards()

        budget_sec = min(self._round_budget_sec, self.interval_sec)
        try:
            async with asyncio.timeout(budget_sec):
                report = await self._store.sweep(
                    cutoff_ms,
                    self.batch_limit,
                    self.delete_rows,
                    owned_shards=shards,
                )
        except Exception as exc:
            logger.error(
                "mail bounded sweep failed mode=%s reason=%s",
                self.mode,
                type(exc).__name__,
                exc_info=True,
            )
            self.last_run_unixtime = self._clock_ms() / 1000.0
            return None

        self.last_report = report
        self.last_run_unixtime = self._clock_ms() / 1000.0
        logger.info(
            "mail sweep finished mode=%s personal_cand=%d personal_del=%d system_cand=%d system_del=%d state_del=%d invalid=%d",
            self.mode,
            report.personal.candidates,
            report.personal.deleted,
            report.system.candidates,
            report.system.deleted,
            report.system_state.deleted,
            report.personal.invalid,
        )
        return report

    async def run(self, *, stop_event: asyncio.Event | None = None) -> None:
        """带启动随机打散的周期清理循环，直至收到 stop_event 或取消信号。"""
        self.last_run_unixtime = self._clock_ms() / 1000.0
        try:
            # 启动抖动打散多副本首轮扫描时间，与 Go maintenance.Run 一致。
            jitter = min(max(float(self._rnd()), 0.0), 1.0) * self.interval_sec
            if jitter > 0:
                if await self._wait(jitter, stop_event):
                    return
            while stop_event is None or not stop_event.is_set():
                await self.sweep_once()
                if await self._wait(self.interval_sec, stop_event):
                    return
        finally:
            self.last_run_unixtime = 0.0

    @staticmethod
    async def _wait(seconds: float, stop_event: asyncio.Event | None) -> bool:
        if stop_event is None:
            await asyncio.sleep(seconds)
            return False
        if stop_event.is_set():
            return True
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False
