"""玩家实体上的资产收益窗口及原有 Kafka 告警协议。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
from pathlib import Path
import tempfile
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from inspect import isawaitable
from typing import Literal

from filelock import FileLock

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


class _AnomalyJournal:
    """Atomic JSON journal for rolling windows and a retryable alert outbox."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def _locked(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return FileLock(str(self.path) + ".lock", timeout=30)

    def _read(self) -> dict:
        if not self.path.exists():
            return {"format_version": 1, "players": {}, "outbox": []}
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("资产告警 JSON 日志损坏或不可读") from exc
        if (not isinstance(document, dict) or document.get("format_version") != 1
                or not isinstance(document.get("players"), dict) or not isinstance(document.get("outbox"), list)):
            raise RuntimeError("资产告警 JSON 日志格式无效")
        return document

    def _write(self, document: dict) -> None:
        encoded = json.dumps(document, separators=(",", ":"), sort_keys=True).encode("utf-8")
        fd, temporary = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "wb") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.path)
            if os.name != "nt":
                directory_fd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def record(self, player_id: int, gain: AssetGain, timestamp: int, threshold: AnomalyThreshold,
               *, enqueue_alert: bool) -> tuple[list[tuple[int, int]], int, int, bytes | None]:
        with self._locked():
            document = self._read()
            if timestamp - int(document.get("last_sweep", 0)) >= 60:
                for stored_player_id, buckets in tuple(document["players"].items()):
                    if not isinstance(buckets, dict):
                        del document["players"][stored_player_id]
                        continue
                    for stored_key, stored_bucket in tuple(buckets.items()):
                        if isinstance(stored_bucket, dict):
                            window = max(0, int(stored_bucket.get("window_seconds", 600)))
                            old_events = stored_bucket.get("events", [])
                        else:
                            window, old_events = 600, stored_bucket
                        start = max(0, timestamp - window)
                        live = [row for row in old_events if isinstance(row, list) and len(row) == 2
                                and int(row[0]) >= start]
                        if live:
                            buckets[stored_key] = {"window_seconds": window, "events": live}
                        else:
                            del buckets[stored_key]
                    if not buckets:
                        del document["players"][stored_player_id]
                document["last_sweep"] = timestamp
            player = document["players"].setdefault(str(player_id), {})
            key = f"{gain.category}:{gain.sub_key}"
            stored_bucket = player.get(key, [])
            if isinstance(stored_bucket, dict):
                old_events = stored_bucket.get("events", [])
            else:
                old_events = stored_bucket
            if not isinstance(old_events, list):
                raise RuntimeError("资产收益窗口记录格式无效")
            start = max(0, timestamp - threshold.window_seconds)
            events = [(int(row[0]), int(row[1])) for row in old_events
                      if isinstance(row, list) and len(row) == 2 and int(row[0]) >= start]
            events.append((timestamp, gain.amount))
            total = sum(amount for _, amount in events)
            player[key] = {"window_seconds": threshold.window_seconds, "events": events}
            exceeded = len(events) > threshold.count or total > threshold.amount
            payload = None
            if exceeded and enqueue_alert:
                alert = {
                    "player_id": player_id, "category": gain.category, "sub_key": gain.sub_key,
                    "count": len(events), "total_amount": total, "threshold_count": threshold.count,
                    "threshold_amount": threshold.amount, "window_seconds": threshold.window_seconds,
                    "timestamp": timestamp,
                }
                payload = json.dumps(alert, separators=(",", ":")).encode()
                alert_id = hashlib.sha256(uuid.uuid4().bytes + payload).hexdigest()
                document["outbox"].append({
                    "id": alert_id, "payload": base64.b64encode(payload).decode("ascii"),
                    "attempts": 0, "lease_owner": "", "lease_until": 0,
                })
            self._write(document)
            return events, len(events), total, payload

    def has_outbox(self) -> bool:
        with self._locked():
            return bool(self._read()["outbox"])

    def claim(self, worker_id: str, now: int) -> tuple[str, bytes] | None:
        with self._locked():
            document = self._read()
            kept = []
            claimed = None
            dirty = False
            for item in document["outbox"]:
                if claimed is not None:
                    kept.append(item)
                    continue
                if (not isinstance(item, dict) or not isinstance(item.get("id"), str)
                        or not isinstance(item.get("payload"), str)):
                    logger.error("丢弃损坏的持久资产告警条目: %r", item)
                    dirty = True
                    continue
                try:
                    lease_until = int(item.get("lease_until", 0))
                except (TypeError, ValueError):
                    logger.error("丢弃租约无效的持久资产告警 id=%s", item["id"])
                    dirty = True
                    continue
                if lease_until > now:
                    kept.append(item)
                    continue
                try:
                    payload = base64.b64decode(item["payload"], validate=True)
                    parsed = json.loads(payload)
                    if not isinstance(parsed, dict) or "player_id" not in parsed:
                        raise ValueError("missing player_id")
                except (ValueError, TypeError, UnicodeDecodeError):
                    logger.error("丢弃载荷损坏的持久资产告警 id=%s", item["id"])
                    dirty = True
                    continue
                item["lease_owner"] = worker_id
                item["lease_until"] = now + 30
                kept.append(item)
                claimed = (item["id"], payload)
                dirty = True
            if dirty:
                document["outbox"] = kept
                self._write(document)
            return claimed

    def acknowledge(self, alert_id: str, worker_id: str) -> None:
        with self._locked():
            document = self._read()
            document["outbox"] = [item for item in document["outbox"]
                                  if not (item.get("id") == alert_id and item.get("lease_owner") == worker_id)]
            self._write(document)

    def retry(self, alert_id: str, worker_id: str, now: int) -> float:
        with self._locked():
            document = self._read()
            delay = 1.0
            for item in document["outbox"]:
                if item.get("id") == alert_id and item.get("lease_owner") == worker_id:
                    item["attempts"] = int(item.get("attempts", 0)) + 1
                    delay = min(60.0, float(2 ** min(item["attempts"], 6)))
                    item["lease_until"] = now + int(delay)
                    item["lease_owner"] = ""
                    break
            self._write(document)
            return delay


class AssetAnomaly:
    def __init__(self, is_current, emit=None, *, clock=time.time, journal_path: str | Path | None = None) -> None:
        self.default_threshold = AnomalyThreshold()
        self.currency_thresholds: dict[int, AnomalyThreshold] = {}
        self.item_thresholds: dict[int, AnomalyThreshold] = {}
        self.is_current = is_current
        self.emit = emit
        self.clock = clock
        self._pending = asyncio.Queue(maxsize=2048)
        self._worker: asyncio.Task | None = None
        self._closed = False
        self._journal = _AnomalyJournal(journal_path) if journal_path is not None else None
        self._wake = asyncio.Event()
        self._worker_id = uuid.uuid4().hex
        self._started = self._journal is None

    async def start(self) -> None:
        self._started = True
        if self.emit is not None and self._journal is not None:
            try:
                if await asyncio.to_thread(self._journal.has_outbox):
                    self._wake.set()
                    self._ensure_worker()
            except Exception:
                logger.exception("读取持久资产告警队列失败")

    def now(self) -> int:
        return int(self.clock())

    async def record(self, player, gain: AssetGain, timestamp: int) -> bool:
        if player is None or not self.is_current(player):
            return False
        thresholds = self.currency_thresholds if gain.category == "currency" else self.item_thresholds
        threshold = thresholds.get(gain.sub_key, self.default_threshold)
        if not (threshold.count or threshold.amount):
            return False
        key = gain.category, gain.sub_key
        if self._journal is None:
            if player.asset_anomaly is None:
                player.asset_anomaly = {}
            bucket = player.asset_anomaly.get(key)
            if bucket is None:
                bucket = player.asset_anomaly[key] = _GainBucket()
            start = max(0, timestamp - threshold.window_seconds)
            while bucket.events and bucket.events[0][0] < start:
                _, expired = bucket.events.popleft()
                bucket.amount -= expired
            bucket.events.append((timestamp, gain.amount))
            bucket.amount += gain.amount
            count, total = len(bucket.events), bucket.amount
            payload = self._payload(player.player_id, gain, count, total, threshold, timestamp)
        else:
            try:
                events, count, total, payload = await asyncio.to_thread(
                    self._journal.record, player.player_id, gain, timestamp, threshold,
                    enqueue_alert=self.emit is not None,
                )
            except Exception:
                logger.exception("持久化资产收益窗口失败 player_id=%s", player.player_id)
                return self._record_memory(player, gain, timestamp, threshold)
            if player.asset_anomaly is None:
                player.asset_anomaly = {}
            player.asset_anomaly[key] = _GainBucket(deque(events), total)
        if count <= threshold.count and total <= threshold.amount:
            return False
        player_id = player.player_id
        logger.warning(
            "资产收益超出窗口阈值 player_id=%s category=%s sub_key=%s count=%s total_amount=%s "
            "threshold_count=%s threshold_amount=%s window_seconds=%s",
            player_id, gain.category, gain.sub_key, count, total,
            threshold.count, threshold.amount, threshold.window_seconds,
        )
        if self.emit is not None and payload is not None:
            self._enqueue(player_id, payload, persisted=self._journal is not None)
        return True

    def _record_memory(self, player, gain, timestamp, threshold) -> bool:
        if player.asset_anomaly is None:
            player.asset_anomaly = {}
        bucket = player.asset_anomaly.setdefault((gain.category, gain.sub_key), _GainBucket())
        start = max(0, timestamp - threshold.window_seconds)
        while bucket.events and bucket.events[0][0] < start:
            _, expired = bucket.events.popleft()
            bucket.amount -= expired
        bucket.events.append((timestamp, gain.amount))
        bucket.amount += gain.amount
        if len(bucket.events) <= threshold.count and bucket.amount <= threshold.amount:
            return False
        payload = self._payload(player.player_id, gain, len(bucket.events), bucket.amount, threshold, timestamp)
        if self.emit is not None:
            self._enqueue(player.player_id, payload)
        return True

    @staticmethod
    def _payload(player_id, gain, count, total, threshold, timestamp) -> bytes:
        return json.dumps({
            "player_id": player_id, "category": gain.category, "sub_key": gain.sub_key,
            "count": count, "total_amount": total, "threshold_count": threshold.count,
            "threshold_amount": threshold.amount, "window_seconds": threshold.window_seconds,
            "timestamp": timestamp,
        }, separators=(",", ":")).encode()

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

    def _enqueue(self, player_id: int, payload: bytes, *, persisted: bool = False) -> None:
        if persisted:
            if self._started:
                self._wake.set()
                self._ensure_worker()
            return
        if self._closed or self._pending.full():
            logger.error("资产告警队列不可用 player_id=%s", player_id)
            return
        self._pending.put_nowait((player_id, payload))
        self._wake.set()
        self._ensure_worker()

    def _ensure_worker(self) -> None:
        if not self._closed and (self._worker is None or self._worker.done()):
            self._worker = asyncio.create_task(self._publish())
            self._worker.add_done_callback(self._worker_finished)

    def _worker_finished(self, task) -> None:
        if self._worker is task:
            self._worker = None
        if self._closed or self.emit is None:
            return
        if not self._pending.empty() or (self._journal is not None and self._started and self._wake.is_set()):
            self._ensure_worker()

    async def _drain_pending(self) -> None:
        while not self._pending.empty():
            player_id, payload = self._pending.get_nowait()
            try:
                async with asyncio.timeout(5):
                    sent = self.emit(player_id, payload)
                    if isawaitable(sent):
                        await sent
            except Exception:
                logger.exception("发布资产收益告警失败 player_id=%s", player_id)

    async def _wait(self, seconds: float) -> None:
        if self._closed or not self._pending.empty():
            return
        self._wake.clear()
        if self._closed or not self._pending.empty():
            return
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def _publish(self) -> None:
        await self._drain_pending()
        if self._journal is not None and self._started:
            await self._publish_journal()
        await self._drain_pending()

    async def _publish_journal(self) -> None:
        while not self._closed:
            await self._drain_pending()
            try:
                item = await asyncio.to_thread(self._journal.claim, self._worker_id, int(time.time()))
            except Exception:
                logger.exception("读取持久资产告警失败")
                await self._drain_pending()
                await self._wait(1)
                continue
            if item is None:
                if not self._pending.empty():
                    continue
                try:
                    if not await asyncio.to_thread(self._journal.has_outbox):
                        return
                except Exception:
                    logger.exception("读取持久资产告警状态失败")
                await self._wait(1)
                continue
            alert_id, payload = item
            try:
                async with asyncio.timeout(5):
                    sent = self.emit(json.loads(payload)["player_id"], payload)
                    if isawaitable(sent):
                        await sent
                await asyncio.to_thread(self._journal.acknowledge, alert_id, self._worker_id)
            except Exception:
                logger.exception("发布持久资产收益告警失败 id=%s", alert_id)
                try:
                    await asyncio.to_thread(self._journal.retry, alert_id, self._worker_id, int(time.time()))
                except Exception:
                    logger.exception("更新持久资产告警重试状态失败 id=%s", alert_id)
                    await self._wait(1)

    async def close(self) -> None:
        self._closed = True
        self._wake.set()
        if self._worker is None:
            return
        try:
            await asyncio.wait_for(self._worker, timeout=5)
        except TimeoutError:
            logger.error("停止时资产告警仍在发布，剩余 %s 条", self._pending.qsize())
