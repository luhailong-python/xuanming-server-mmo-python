import asyncio
import json
from types import SimpleNamespace

from xuanming.cluster import KafkaCommands
from xuanming.data.asset_anomaly import AnomalyThreshold, AssetAnomaly, AssetGain


def test_window_uses_inclusive_boundary_and_reports_each_excess():
    async def exercise():
        player = SimpleNamespace(player_id=7, asset_anomaly=None)
        emitted = []
        now = 100

        async def send_and_wait(topic, payload, *, key):
            emitted.append((topic, key, json.loads(payload)))

        kafka = KafkaCommands("unused", partitions=32, generation=9)
        kafka.producer = SimpleNamespace(send_and_wait=send_and_wait)
        detector = AssetAnomaly(lambda candidate: candidate is player, kafka.publish_anomaly, clock=lambda: now)
        assert detector.default_threshold == AnomalyThreshold(50, 100_000, 600)
        detector.currency_thresholds[1] = AnomalyThreshold(2, 10, 10)
        assert detector.currency_gain_count(player, 1) == 0
        assert player.asset_anomaly is None
        assert not await detector.record(player, AssetGain("currency", 1, 6), now)
        now = 110
        assert not await detector.record(player, AssetGain("currency", 1, 4), now)
        assert detector.currency_gain_count(player, 1) == 2
        assert await detector.record(player, AssetGain("currency", 1, 1), now)
        now = 111
        assert await detector.record(player, AssetGain("currency", 1, 5), now)
        now = 121
        assert detector.currency_gain_count(player, 1) == 1
        assert not await detector.record(player, AssetGain("currency", 1, 5), now)
        assert not await detector.record(player, AssetGain("item", 1, 11), now)
        await detector.close()

        assert emitted == [
            ("anomaly_alert_topic", b"7", {
                "player_id": 7, "category": "currency", "sub_key": 1, "count": 3,
                "total_amount": amount, "threshold_count": 2, "threshold_amount": 10,
                "window_seconds": 10, "timestamp": stamp,
            })
            for amount, stamp in ((11, 110), (10, 111))
        ]

    asyncio.run(exercise())


def test_window_is_owned_by_current_player_and_reset_is_idempotent():
    original = SimpleNamespace(player_id=7, asset_anomaly=None)
    other = SimpleNamespace(player_id=8, asset_anomaly=None)
    active = {7: original, 8: other}
    detector = AssetAnomaly(lambda player: active.get(player.player_id) is player, clock=lambda: 100)
    detector.item_thresholds[1] = AnomalyThreshold(0, 0, 600)
    async def exercise():
        assert not await detector.record(original, AssetGain("item", 1, 120_000), 100)
        gain = AssetGain("currency", 1, 2)
        assert not await detector.record(original, gain, 100)
        assert not await detector.record(other, gain, 100)
        detector.clear_player(original)
        detector.clear_player(original)
        assert detector.currency_gain_count(original, 1) == 0
        assert detector.currency_gain_count(other, 1) == 1

        replacement = active[7] = SimpleNamespace(player_id=7, asset_anomaly=None)
        assert not await detector.record(original, gain, 100)
        assert replacement.asset_anomaly is None
        assert not await detector.record(replacement, gain, 100)
        detector.clear_player(original)
        assert detector.currency_gain_count(replacement, 1) == 1

    asyncio.run(exercise())


def test_persistent_windows_and_alert_outbox_survive_restart(tmp_path):
    async def exercise():
        path = tmp_path / "asset-anomaly.json"
        player = SimpleNamespace(player_id=7, asset_anomaly=None)
        attempted = asyncio.Event()
        now = 100
        threshold = AnomalyThreshold(1, 1_000, 600)

        async def fail_once(_player_id, _payload):
            attempted.set()
            raise OSError("Kafka unavailable")

        first = AssetAnomaly(lambda candidate: candidate is player, fail_once, clock=lambda: now,
                             journal_path=path)
        first.currency_thresholds[1] = threshold
        await first.start()
        gain = AssetGain("currency", 1, 6)
        assert not await first.record(player, gain, now)
        assert await first.record(player, gain, now)
        await asyncio.wait_for(attempted.wait(), 2)
        await first.close()

        sent = []
        delivered = asyncio.Event()

        async def succeed(player_id, payload):
            sent.append((player_id, json.loads(payload)))
            delivered.set()

        replacement = SimpleNamespace(player_id=7, asset_anomaly=None)
        second = AssetAnomaly(lambda candidate: candidate is replacement, succeed, clock=lambda: now,
                              journal_path=path)
        second.currency_thresholds[1] = threshold
        await second.start()
        assert await second.record(replacement, gain, now)
        assert second.currency_gain_count(replacement, 1) == 3
        await asyncio.wait_for(delivered.wait(), 3)
        async with asyncio.timeout(3):
            while await asyncio.to_thread(second._journal.has_outbox):
                await asyncio.sleep(0.01)
        await second.close()
        assert [row[1]["count"] for row in sent] == [2, 3]

    asyncio.run(exercise())
