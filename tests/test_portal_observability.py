import asyncio
import re
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from xuanming.portal_login import Limiter, LoginRpc
from xuanming.portal_observability import Metrics, create_management_app, trace_metadata
from xuanming.protocol import Protocol


class Database:
    size, freesize, maxsize = 3, 2, 10

    def __init__(self):
        self.fail = False

    def acquire(self):
        return self

    def cursor(self):
        return self

    async def __aenter__(self):
        if self.fail:
            raise OSError("offline")
        return self

    async def __aexit__(self, *_):
        pass

    async def execute(self, sql):
        assert sql == "SELECT 1"

    async def fetchone(self):
        return (1,)

    async def rollback(self):
        pass


def test_management_health_checks_real_dependencies_and_tracks_lifecycle():
    class Redis:
        fail = False

        async def ping(self):
            if self.fail:
                raise OSError("offline")
            return True

    async def run():
        db, redis, metrics = Database(), Redis(), Metrics()
        app = create_management_app(SimpleNamespace(db=db, redis=redis, metrics=metrics))
        async with TestClient(TestServer(app)) as client:
            response = await client.get("/actuator/health/liveness")
            assert response.status == 200 and (await response.json())["status"] == "UP"
            assert (await client.get("/actuator/health/readiness")).status == 503
            metrics.ready = True
            assert (await client.get("/actuator/health/readiness")).status == 200
            assert (await client.get("/actuator/health")).status == 200
            db.fail = True
            assert (await client.get("/actuator/health")).status == 503
            # Dependency failures do not make the process unhealthy or unready.
            assert (await client.get("/actuator/health/liveness")).status == 200
            assert (await client.get("/actuator/health/readiness")).status == 200
            db.fail, redis.fail = False, True
            assert (await client.get("/actuator/health")).status == 503
            text = await (await client.get("/actuator/prometheus")).text()
            assert 'portal_mysql_pool_connections{state="size"} 3' in text
            assert 'portal_mysql_pool_connections{state="free"} 2' in text
            assert "hikaricp" not in text

    asyncio.run(run())


def test_public_trace_is_not_trusted_and_go_login_receives_new_trace(monkeypatch):
    async def run():
        protocol, metrics = Protocol(), Metrics()
        sent = []

        class Client:
            def __init__(self, *_args, **_kwargs):
                pass

            async def call(self, path, body, *, metadata):
                sent.append((path, dict(metadata)))
                await asyncio.sleep(0)
                return protocol.message("loginpb.LoginResponse").SerializeToString()

            async def close(self):
                pass

        monkeypatch.setattr("xuanming.portal_login.GrpcClient", Client)
        rpc = LoginRpc(protocol, {"1": "127.0.0.1:1234"})
        app = web.Application(middlewares=[metrics.instrument])

        async def login(_request):
            await rpc.call(1, "ClientPlayerLogin", "Login", protocol.message("loginpb.LoginRequest"), "LoginResponse")
            return web.json_response({"ok": True})

        app.router.add_post("/api/login", login)
        attacker_trace = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
        async with TestClient(TestServer(app)) as client:
            responses = await asyncio.gather(*(client.post("/api/login", headers={"traceparent": attacker_trace,
                                              "b3": "a" * 32 + "-" + "b" * 16 + "-1"}) for _ in range(2)))
            assert all(item.status == 200 for item in responses)
        traces = [metadata["traceparent"] for _, metadata in sent]
        assert all(re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-01", value) for value in traces)
        assert all(value[3:35] != "a" * 32 for value in traces)
        assert traces[0][3:35] != traces[1][3:35]
        assert trace_metadata() == ()
        output = metrics.render(Database())
        assert 'portal_http_requests_total{method="POST",route="/api/login",status="200"} 2' in output
        await rpc.close()

    asyncio.run(run())


def test_real_rate_limit_decisions_are_exported():
    class Redis:
        async def eval(self, *_):
            return [1, 1]

    async def run():
        metrics = Metrics()
        config = SimpleNamespace(rate_limit_enabled=True, trusted_proxies=[], ip_rps=1, ip_burst=1,
                                 zone_rps=1, zone_burst=1, account_cooldown_ms=5000)
        limiter = Limiter(Redis(), config, metrics=metrics)
        assert await limiter.check(1, "127.0.0.1", "", "login") == ("deny", "IP_RATE_LIMIT")
        assert 'gateway_rate_limit_decisions_total{outcome="ip_deny"} 1' in metrics.render(Database())

    asyncio.run(run())
