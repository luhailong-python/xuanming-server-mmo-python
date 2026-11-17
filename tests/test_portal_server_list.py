import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
import grpc
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from xuanming import portal_server_list as module
from xuanming.portal_server_list import Health, HealthReader, HealthSnapshot, _decode_health, make_list, register_routes
from xuanming.protocol import Protocol


def test_recommendation_health_and_manual_status():
    now = datetime(2026, 11, 17)
    rows = [(1, "busy", 0, 100, "", None, True, 1, False, now),
            (2, "free", 0, 100, "", None, False, 2, False, now - timedelta(days=8)),
            (3, "maintenance", 1, 100, "update", None, True, 3, False, now)]
    snapshot = HealthSnapshot({1: Health("HEALTHY", 90, 90), 2: Health("DEGRADED", 20, 90)}, frozenset(), 0)
    zones = make_list(rows, snapshot, now)["zones"]
    assert zones[0]["load_level"] == "FULL" and not zones[0]["recommended"]
    assert zones[1]["load_level"] == "SMOOTH" and zones[1]["recommended"] and not zones[1]["is_new"]
    assert zones[2]["status"] == "MAINTENANCE" and zones[2]["maintenance_msg"] == "update"


def test_missing_health_and_mysql_bits():
    now = datetime(2026, 11, 17)
    rows = [(1, "one", 0, 100, "", None, b"\x01", 1, b"\x00", None)]
    def zone(snapshot): return make_list(rows, snapshot, now)["zones"][0]
    assert zone(None)["status"] == "OPEN" and "load_level" not in zone(None)
    live_login = HealthSnapshot({}, frozenset({1}), 0)
    assert zone(live_login)["status"] == "OPEN" and zone(live_login)["recommended"]
    assert zone(HealthSnapshot({}, frozenset(), 0))["status"] == "MAINTENANCE"
    assert zone(HealthSnapshot({1: Health("DOWN", 0, 10)}, frozenset({1}), 0))["status"] == "MAINTENANCE"
    assert zone(None)["create_role_closed"] is False


@pytest.mark.parametrize("key,value", [
    (b"/mmorpg/zone_health/zone/0/publisher/a", b"{}"),
    (b"/mmorpg/zone_health/zone/1/publisher/a", b'{"zoneId":2,"status":"HEALTHY","onlineCount":1,"capacity":2}'),
    (b"/mmorpg/zone_health/zone/1/publisher/a", b'{"zoneId":1,"status":"UNKNOWN","onlineCount":1,"capacity":2}'),
    (b"/mmorpg/zone_health/zone/1/publisher/a", b'{"zoneId":1,"status":"HEALTHY","onlineCount":true,"capacity":2}'),
    (b"/mmorpg/zone_health/zone/1/publisher/a", b'{"zoneId":1,"status":"HEALTHY","onlineCount":1,"capacity":-1}'),
])
def test_bad_health_invalidates_snapshot(key, value):
    with pytest.raises(ValueError):
        _decode_health(key, value)


def test_latest_revision_atomic_snapshot_failures_and_expiry():
    async def run():
        now = [10.0]
        def entry(key, value, rev): return SimpleNamespace(key=key, value=value, mod_revision=rev)
        def health(publisher, online, rev):
            return entry(f"/mmorpg/zone_health/zone/1/publisher/{publisher}".encode(),
                         f'{{"zoneId":1,"status":"HEALTHY","onlineCount":{online},"capacity":100}}'.encode(), rev)

        class Directory:
            def _info(self, data): return SimpleNamespace(node_type=5, zone_id=int(data))
            def _service_key(self, info): return f"LoginNodeService.rpc/zone/{info.zone_id}".encode()

        directory = Directory()
        reader = HealthReader(directory, clock=lambda: now[0])
        reader.health._apply(health("a", 5, 1))
        reader.health._apply(health("b", 70, 2))
        reader.logins._apply(entry(b"LoginNodeService.rpc/zone/2", b"2", 1))
        reader.health.synced = reader.logins.synced = True
        assert reader.publish()
        good = reader.current()
        assert good.by_zone[1].online == 70 and good.login_zones == {2}
        # A malformed health record or registration never publishes a partial snapshot.
        malformed = entry(b"/mmorpg/zone_health/zone/2/publisher/a", b"broken", 3)
        reader.health._apply(malformed)
        assert not reader.publish()
        assert reader.current() is good
        reader.health._apply(malformed, deleted=True)
        invalid_login = entry(b"LoginNodeService.rpc/zone/bad", b"2", 1)
        reader.logins._apply(invalid_login)
        assert not reader.publish()
        assert reader.current() is good
        reader.logins._apply(invalid_login, deleted=True)
        reader.logins.synced = False
        assert not reader.publish()
        now[0] += 15
        assert reader.current() is good
        now[0] += .01
        assert reader.current() is None

    asyncio.run(run())


def test_concurrent_http_requests_share_periodic_health_snapshot(monkeypatch):
    async def run():
        protocol = Protocol()

        class Watch:
            created = False
            async def write(self, data): pass
            async def read(self):
                if not self.created:
                    self.created = True
                    return protocol.message("etcdserverpb.WatchResponse", created=True).SerializeToString()
                await asyncio.Event().wait()
            def cancel(self): pass

        class Channel:
            def stream_stream(self, path): return Watch

        class Directory:
            calls = 0
            protocol = None
            client = SimpleNamespace(channel=Channel())
            async def _rpc(self, service, method, **kwargs):
                self.calls += 1
                return SimpleNamespace(kvs=[], header=SimpleNamespace(revision=1))

        class DB:
            calls = 0
            def acquire(self): return self
            def cursor(self): return self
            async def __aenter__(self): return self
            async def __aexit__(self, *_): pass
            async def execute(self, sql): self.calls += 1
            async def fetchall(self): return [(1, "one", 0, 100, "", None, True, 1, False, None)]
            async def rollback(self): pass

        directory, db = Directory(), DB()
        directory.protocol = protocol
        app = web.Application()
        register_routes(app, SimpleNamespace(directory=directory, db=db))
        async with TestClient(TestServer(app)) as client:
            async with asyncio.timeout(1):
                while directory.calls < 2:
                    await asyncio.sleep(0)
                # Wait until the watcher creation acknowledgements have been
                # incorporated into a published snapshot.
                while True:
                    first = await client.get("/api/server-list")
                    if (await first.json())["zones"][0]["status"] == "MAINTENANCE":
                        break
                    await asyncio.sleep(.005)
            responses = await asyncio.gather(*(client.get("/api/server-list") for _ in range(20)))
            assert all(response.status == 200 for response in responses)
            assert directory.calls == 2 and db.calls == 1
            assert (await responses[0].json())["zones"][0]["status"] == "MAINTENANCE"
    monkeypatch.setattr(module, "_POLL_INTERVAL", .01)
    asyncio.run(run())


@pytest.mark.parametrize("compacted", [True, False])
def test_watch_resumes_after_compaction_or_disconnect(compacted):
    async def run():
        protocol, queue = Protocol(), asyncio.Queue()
        key = b"/mmorpg/zone_health/zone/1/publisher/a"
        def value(online):
            return f'{{"zoneId":1,"status":"HEALTHY","onlineCount":{online},"capacity":100}}'.encode()
        watches, revisions = [], []

        class Watch:
            def __init__(self):
                self.index = len(watches)
                self.first = True
                self.cancelled = False
                watches.append(self)
            async def write(self, data):
                revisions.append(protocol.parse("etcdserverpb.WatchRequest", data).create_request.start_revision)
            async def read(self):
                if self.first:
                    self.first = False
                    return protocol.message("etcdserverpb.WatchResponse", created=True).SerializeToString()
                if self.index == 0:
                    return await queue.get()
                await asyncio.Event().wait()
            def cancel(self): self.cancelled = True

        class Directory:
            calls = 0
            client = SimpleNamespace(channel=SimpleNamespace(stream_stream=lambda _: Watch))
            protocol = None
            async def _rpc(self, service, method, **kwargs):
                self.calls += 1
                number = 10 if self.calls == 1 else 20
                item = SimpleNamespace(key=key, value=value(1 if self.calls == 1 else 3), mod_revision=number)
                return SimpleNamespace(kvs=[item], header=SimpleNamespace(revision=number))

        directory = Directory()
        directory.protocol = protocol
        mirror = module._PrefixMirror(directory, module._HEALTH_PREFIX, module._decode_health)
        task = asyncio.create_task(mirror.run())
        try:
            async with asyncio.timeout(2):
                while not mirror.synced:
                    await asyncio.sleep(.001)
            # A put after Range but before watch creation is delivered from
            # revision+1 and must not be lost.
            event = protocol.message("etcdserverpb.WatchResponse", events=[{"kv": {
                "key": key, "value": value(2), "mod_revision": 11}}])
            await queue.put(event.SerializeToString())
            async with asyncio.timeout(2):
                while mirror.entries[key][0][1].online != 2:
                    await asyncio.sleep(.001)
            ended = (protocol.message("etcdserverpb.WatchResponse", canceled=True, compact_revision=19).SerializeToString()
                     if compacted else grpc.aio.EOF)
            await queue.put(ended)
            async with asyncio.timeout(3):
                while directory.calls < 2 or not mirror.synced:
                    await asyncio.sleep(.001)
            assert mirror.entries[key][0][1].online == 3
            assert revisions == [11, 21] and watches[0].cancelled
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert watches[-1].cancelled

    asyncio.run(run())


def test_compaction_during_watch_creation_restarts_from_new_range():
    async def run():
        protocol = Protocol()
        calls, starts = [], []

        class Watch:
            def __init__(self): self.index = len(calls) - 1
            async def write(self, data):
                starts.append(protocol.parse("etcdserverpb.WatchRequest", data).create_request.start_revision)
            async def read(self):
                if self.index == 0:
                    return protocol.message("etcdserverpb.WatchResponse", canceled=True,
                                            compact_revision=15).SerializeToString()
                if self.index == 1:
                    self.index = 2
                    return protocol.message("etcdserverpb.WatchResponse", created=True).SerializeToString()
                await asyncio.Event().wait()
            def cancel(self): pass

        class Directory:
            client = SimpleNamespace(channel=SimpleNamespace(stream_stream=lambda _: Watch))
            protocol = None
            async def _rpc(self, *args, **kwargs):
                revision = 10 if not calls else 20
                calls.append(revision)
                return SimpleNamespace(kvs=[], header=SimpleNamespace(revision=revision))

        directory = Directory()
        directory.protocol = protocol
        mirror = module._PrefixMirror(directory, module._HEALTH_PREFIX, module._decode_health)
        task = asyncio.create_task(mirror.run())
        try:
            async with asyncio.timeout(2):
                while not mirror.synced:
                    await asyncio.sleep(.001)
            assert calls == [10, 20] and starts == [11, 21]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(run())


def test_silent_watch_is_periodically_relisted(monkeypatch):
    async def run():
        protocol = Protocol()
        watches = []

        class Watch:
            def __init__(self): self.first, self.cancelled = True, False; watches.append(self)
            async def write(self, data): pass
            async def read(self):
                if self.first:
                    self.first = False
                    return protocol.message("etcdserverpb.WatchResponse", created=True).SerializeToString()
                await asyncio.Event().wait()
            def cancel(self): self.cancelled = True

        class Directory:
            client = SimpleNamespace(channel=SimpleNamespace(stream_stream=lambda _: Watch))
            protocol = None
            calls = 0
            async def _rpc(self, *args, **kwargs):
                self.calls += 1
                return SimpleNamespace(kvs=[], header=SimpleNamespace(revision=self.calls))

        directory = Directory()
        directory.protocol = protocol
        mirror = module._PrefixMirror(directory, module._HEALTH_PREFIX, module._decode_health)
        task = asyncio.create_task(mirror.run())
        try:
            async with asyncio.timeout(2):
                while directory.calls < 2 or not mirror.synced:
                    await asyncio.sleep(.001)
            assert watches[0].cancelled
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    monkeypatch.setattr(module, "_MAX_MIRROR_AGE", .05)
    asyncio.run(run())
