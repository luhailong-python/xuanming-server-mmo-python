"""玩家实体上的资产收益窗口及原有 Kafka 告警协议。"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from inspect import isawaitable
from typing import Literal

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AssetGain:
    category: Literal["currency", "item"]
    sub_key: int
    amount: int


@dataclass(frozen=True, slots=True)
class AnomalyThreshold:
    count: int = 50
    amount: int = 100_000
    window_seconds: int = 600


@dataclass(slots=True)
class _GainBucket:
    events: deque[tuple[int, int]] = field(default_factory=deque)
    amount: int = 0


class AssetAnomaly:
    def __init__(self, is_current, emit=None, *, clock=time.time) -> None:
        self.default_threshold = AnomalyThreshold()
        self.currency_thresholds: dict[int, AnomalyThreshold] = {}
        self.item_thresholds: dict[int, AnomalyThreshold] = {}
        self.is_current = is_current
        self.emit = emit
        self.clock = clock
        self._pending = asyncio.Queue(maxsize=2048)
        self._worker: asyncio.Task | None = None
        self._closed = False

    def now(self) -> int:
        return int(self.clock())

    def record(self, player, gain: AssetGain, timestamp: int) -> bool:
        if player is None or not self.is_current(player):
            return False
        thresholds = self.currency_thresholds if gain.category == "currency" else self.item_thresholds
        threshold = thresholds.get(gain.sub_key, self.default_threshold)
        if not (threshold.count or threshold.amount):
            return False
        if player.asset_anomaly is None:
            player.asset_anomaly = {}
        key = gain.category, gain.sub_key
        bucket = player.asset_anomaly.get(key)
        if bucket is None:
            bucket = player.asset_anomaly[key] = _GainBucket()
        start = max(0, timestamp - threshold.window_seconds)
        while bucket.events and bucket.events[0][0] < start:
            _, expired = bucket.events.popleft()
            bucket.amount -= expired
        bucket.events.append((timestamp, gain.amount))
        bucket.amount += gain.amount
        count = len(bucket.events)
        if count <= threshold.count and bucket.amount <= threshold.amount:
            return False
        player_id = player.player_id
        logger.warning(
            "资产收益超出窗口阈值 player_id=%s category=%s sub_key=%s count=%s total_amount=%s "
            "threshold_count=%s threshold_amount=%s window_seconds=%s",
            player_id, gain.category, gain.sub_key, count, bucket.amount,
            threshold.count, threshold.amount, threshold.window_seconds,
        )
        if self.emit is not None:
            payload = json.dumps({
                "player_id": player_id, "category": gain.category, "sub_key": gain.sub_key,
                "count": count, "total_amount": bucket.amount, "threshold_count": threshold.count,
                "threshold_amount": threshold.amount, "window_seconds": threshold.window_seconds,
                "timestamp": timestamp,
            }, separators=(",", ":")).encode()
            self._enqueue(player_id, payload)
        return True

    def currency_gain_count(self, player, kind: int) -> int:
        if player is None or not self.is_current(player) or player.asset_anomaly is None:
            return 0
        bucket = player.asset_anomaly.get(("currency", kind))
        if bucket is None:
            return 0
        threshold = self.currency_thresholds.get(kind, self.default_threshold)
        start = max(0, self.now() - threshold.window_seconds)
        return sum(timestamp >= start for timestamp, _ in bucket.events)

    def clear_player(self, player) -> None:
        if player is not None and self.is_current(player):
            player.asset_anomaly = None

    def _enqueue(self, player_id: int, payload: bytes) -> None:
        if self._closed or self._pending.full():
            logger.error("资产告警队列不可用 player_id=%s", player_id)
            return
        self._pending.put_nowait((player_id, payload))
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._publish())

    async def _publish(self) -> None:
        while not self._pending.empty():
            player_id, payload = self._pending.get_nowait()
            try:
                async with asyncio.timeout(5):
                    sent = self.emit(player_id, payload)
                    if isawaitable(sent):
                        await sent
            except Exception:
                logger.exception("发布资产收益告警失败 player_id=%s", player_id)

    async def close(self) -> None:
        self._closed = True
        if self._worker is None:
            return
        try:
            await asyncio.wait_for(self._worker, timeout=5)
        except TimeoutError:
            logger.error("停止时资产告警仍在发布，剩余 %s 条", self._pending.qsize())
