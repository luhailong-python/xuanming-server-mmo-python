import asyncio
import base64
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from xuanming.portal_login import Limiter, client_ip, register_login_routes
from xuanming.protocol import Protocol


def settings(**overrides):
    return SimpleNamespace(**({"rate_limit_enabled": False, "trusted_proxies": [], "ip_rps": 2,
                               "ip_burst": 10, "zone_rps": 500, "zone_burst": 1000,
                               "account_cooldown_ms": 5000} | overrides))


class Connection:
    def __init__(self, status):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    def cursor(self):
        return self

    async def execute(self, sql, params):
        assert "manual_status" in sql and params == (1,)

    async def fetchone(self):
        return self.status

    async def rollback(self):
        pass


class Database:
    def __init__(self, status):
        self.status = status

    def acquire(self):
        return Connection(self.status)


class Rpc:
    def __init__(self, protocol):
        self.protocol, self.calls = protocol, []

    async def call(self, zone, service, method, request, response, **kwargs):
        self.calls.append((zone, service, method, request, kwargs))
        if method == "Login":
            return self.protocol.message("loginpb.LoginResponse", access_token="access", refresh_token="refresh")
        if method == "AssignGate":
            return self.protocol.message("loginpb.AssignGateResponse", ip="127.0.0.1", port=8610,
                                         token_payload=b"payload", token_signature=b"signature", token_deadline=12)
        if method == "QueryQueueStatus":
            return self.protocol.message("loginpb.QueryQueueStatusResponse", status=1, queue_rank=2)
        return self.protocol.message("loginpb.RefreshTokenResponse", access_token="new", refresh_token="rotated")


def test_login_admission_queue_and_rotation():
    async def run():
        protocol = Protocol()
        rpc = Rpc(protocol)
        db = Database((0,))
        limiter = Limiter(None, settings())
        app = web.Application()
        register_login_routes(app, SimpleNamespace(protocol=protocol, rpc=rpc, db=db, limiter=limiter))
        async with TestClient(TestServer(app)) as client:
            response = await client.post("/api/login", json={"zone_id": 1, "account": "alice", "password": "pw"})
            assert (await response.json())["access_token"] == "access"
            response = await client.post("/api/assign-gate", json={"zone_id": 1, "account": "forged",
                                                                    "access_token": "access", "device_id": "device"})
            result = await response.json()
            assert result["code"] == 0 and base64.b64decode(result["token_payload"]) == b"payload"
            assert rpc.calls[-1][3].access_token == "access"
            assert "account" not in rpc.calls[-1][3].DESCRIPTOR.fields_by_name
            response = await client.post("/api/queue-status", json={"zone_id": 1, "queue_token": "queue"})
            assert (await response.json())["queue_token"] == "queue"
            response = await client.post("/api/refresh-token", json={"refresh_token": "refresh"})
            assert (await response.json())["refresh_token"] == "rotated"
            assert rpc.calls[-1][-1] == {"zoned": False}
            count = len(rpc.calls)
            db.status = (1,)
            response = await client.post("/api/assign-gate", json={"zone_id": 1})
            assert (await response.json())["error"] == "zone_maintenance" and len(rpc.calls) == count
            db.status = None
            response = await client.post("/api/queue-status", json={"zone_id": 1, "queue_token": "queue"})
            assert (await response.json())["error"] == "zone_not_found" and len(rpc.calls) == count

    asyncio.run(run())


def test_client_ip_only_trusts_configured_proxy_hops():
    import ipaddress

    request = SimpleNamespace(remote="192.0.2.2", headers={"X-Forwarded-For": "198.51.100.5, 10.0.0.3"})
    assert client_ip(request, ()) == "192.0.2.2"
    request.remote = "10.0.0.2"
    assert client_ip(request, (ipaddress.ip_network("10.0.0.0/8"),)) == "198.51.100.5"
    request.headers = {"X-Forwarded-For": "bad, 10.0.0.3"}
    assert client_ip(request, (ipaddress.ip_network("10.0.0.0/8"),)) == "10.0.0.2"


def test_wave_schedule_and_separate_account_cooldowns(monkeypatch):
    class Redis:
        async def eval(self, *_):
            return [0, 0]

    async def run():
        config = settings(rate_limit_enabled=True, wave_enabled=True, wave_start_epoch_seconds=100,
                          wave_steps=[{"offset_seconds": 0, "allow_zones": [1]},
                                      {"offset_seconds": 30, "allow_zones": [-1]}])
        limiter = Limiter(Redis(), config)
        assert await limiter.check(2, "192.0.2.1", "alice", "login") == ("wave", 20000)
        assert await limiter.check(1, "192.0.2.1", "alice", "login") is None
        assert await limiter.check(1, "192.0.2.1", "alice", "login") == ("deny", "ACCOUNT_COOLDOWN")
        assert await limiter.check(1, "192.0.2.1", "alice", "assign-gate") is None

    monkeypatch.setattr("xuanming.portal_login.time.time", lambda: 110)
    asyncio.run(run())


def test_redis_failure_allows_only_one_half_open_probe():
    class Redis:
        def __init__(self):
            self.calls = 0
            self.started, self.release = asyncio.Event(), asyncio.Event()

        async def eval(self, *_):
            self.calls += 1
            if self.calls == 1:
                raise OSError("redis unavailable")
            self.started.set()
            await self.release.wait()
            return [1, 20]  # A healthy bucket can still reject the request.

    async def run():
        import time
        redis = Redis()
        limiter = Limiter(redis, settings(rate_limit_enabled=True))
        assert await limiter.check(0, "192.0.2.1", "", "login") is None
        assert limiter.bypass_until > time.monotonic()
        assert await limiter.check(0, "192.0.2.1", "", "login") is None
        assert redis.calls == 1
        limiter.bypass_until = time.monotonic() - 1
        probe = asyncio.create_task(limiter.check(0, "192.0.2.1", "", "login"))
        await redis.started.wait()
        bypassed = await asyncio.gather(*(limiter.check(0, "192.0.2.2", "", "login") for _ in range(10)))
        assert bypassed == [None] * 10 and redis.calls == 2
        redis.release.set()
        assert await probe == ("deny", "IP_RATE_LIMIT")
        assert limiter.bypass_until == 0

    asyncio.run(run())


def test_inflight_failure_does_not_get_erased_by_older_successful_probe():
    class Redis:
        def __init__(self):
            self.calls = 0
            self.old_started, self.probe_started = asyncio.Event(), asyncio.Event()
            self.old_release, self.probe_release = asyncio.Event(), asyncio.Event()

        async def eval(self, *_):
            self.calls += 1
            if self.calls == 1:
                self.old_started.set()
                await self.old_release.wait()
                raise OSError("late failure")
            if self.calls == 2:
                raise OSError("initial failure")
            self.probe_started.set()
            await self.probe_release.wait()
            return [0, 0]

    async def run():
        import time
        redis = Redis()
        limiter = Limiter(redis, settings(rate_limit_enabled=True))
        old = asyncio.create_task(limiter.check(0, "192.0.2.1", "", "login"))
        await redis.old_started.wait()
        assert await limiter.check(0, "192.0.2.2", "", "login") is None
        limiter.bypass_until = time.monotonic() - 1
        probe = asyncio.create_task(limiter.check(0, "192.0.2.3", "", "login"))
        await redis.probe_started.wait()
        redis.old_release.set()
        assert await old is None
        redis.probe_release.set()
        assert await probe is None
        assert limiter.bypass_until > time.monotonic()

    asyncio.run(run())
