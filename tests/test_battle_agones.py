import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import grpc
import pytest

from xuanming.grpc_runtime import GrpcClient
from xuanming.nodes import battle_agones
from xuanming.nodes.battle_agones import BattleAgones
from xuanming.runtime import Runtime, grpc_address
from xuanming.settings import NodeSettings, Settings

web = pytest.importorskip("aiohttp.web")


@asynccontextmanager
async def sdk(handler):
    app = web.Application()
    app.router.add_route("*", "/{path}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        yield runner.addresses[0][1]
    finally:
        await runner.cleanup()


async def wait_until(condition):
    async with asyncio.timeout(2):
        while not condition():
            await asyncio.sleep(0.005)


def test_agones_advertises_sdk_endpoint_and_fences_allocation_during_drain(monkeypatch):
    monkeypatch.setattr(battle_agones, "_READY_INITIAL_BACKOFF", 0.01)
    monkeypatch.setattr(battle_agones, "_DRAIN_INTERVAL", 0.02)
    monkeypatch.setattr(battle_agones, "_HEALTH_INTERVAL", 0.02)

    async def exercise():
        calls = []
        metadata = {"labels": {"mmorpg.io/drain": "false"}}

        async def handler(request):
            calls.append(request.path)
            if request.path == "/gameserver":
                assert request.method == "GET"
                return web.json_response({"objectMeta": metadata, "status": {"address": "192.0.2.15", "ports": [
                    {"name": "grpc", "port": 18080}, {"name": "client", "port": 28080},
                ]}})
            assert request.method == "POST" and await request.json() == {}
            if request.path == "/ready" and calls.count("/ready") == 1:
                return web.Response(status=503)
            return web.json_response({})

        async with sdk(handler) as port:
            monkeypatch.setenv("AGONES_SDK_HTTP_PORT", str(port))
            monkeypatch.setenv("AGONES_ENABLED", "false")
            assert BattleAgones.from_env() is None
            monkeypatch.delenv("AGONES_ENABLED")
            monkeypatch.delenv("AGONES_SDK_HOST", raising=False)
            lifecycle = BattleAgones.from_env()
            try:
                assert await lifecycle.client_endpoint() == ("192.0.2.15", 28080)
                async with lifecycle.permit() as granted:
                    assert not granted
                lifecycle.start()
                await wait_until(lambda: "/health" in calls)
                assert calls[:4] == ["/gameserver", "/ready", "/ready", "/gameserver"]
                async with lifecycle.permit() as granted:
                    assert not granted

                metadata = None
                await asyncio.sleep(0.06)
                assert not lifecycle.admitting
                metadata = {"labels": {}}
                await wait_until(lambda: lifecycle.admitting)

                first, second, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()

                async def allocate(entered):
                    async with lifecycle.permit() as granted:
                        assert granted
                        entered.set()
                        await finish.wait()

                tasks = (asyncio.create_task(allocate(first)), asyncio.create_task(allocate(second)))
                try:
                    await asyncio.wait_for(first.wait(), 1)
                    await asyncio.wait_for(second.wait(), 1)
                    assert calls.count("/allocate") == 1
                    lifecycle.rooms_changed(1)
                    metadata = {"labels": {"mmorpg.io/drain": "true"}}
                    await wait_until(lambda: not lifecycle.admitting)
                    lifecycle.rooms_changed(0)
                    await asyncio.sleep(0.04)
                    assert calls.count("/ready") == 2
                finally:
                    finish.set()
                    await asyncio.gather(*tasks)
                await wait_until(lambda: calls.count("/ready") == 3)
                assert not lifecycle.admitting
            finally:
                await lifecycle.close()
            assert "/shutdown" not in calls

    asyncio.run(exercise())


def test_battle_runtime_publishes_sdk_admission_and_gates_grpc(monkeypatch):
    monkeypatch.setattr(battle_agones, "_DRAIN_INTERVAL", 0.01)
    monkeypatch.setenv("MMORPG_BATTLE_TOKEN_SECRET", "b" * 32)
    monkeypatch.setenv("MMORPG_MATCH_OUTCOME_SECRET_BATTLE", "m" * 32)
    monkeypatch.setenv("CLIENT_ENDPOINT_SOURCE", "agones")
    monkeypatch.setenv("CLIENT_ENDPOINT_HOST", "battle.example.test")
    for key in ("AGONES_ENABLED", "AGONES_SDK_HOST", "CLIENT_ENDPOINT_PORT", "GLOBAL_MATCH_ENABLED", "MATCH_PROBE_SECRET"):
        monkeypatch.delenv(key, raising=False)

    async def exercise():
        states, requests = [], []
        drain = True

        async def handler(request):
            requests.append(request.path)
            if request.path == "/gameserver":
                return web.json_response({
                    "objectMeta": {"labels": {"mmorpg.io/drain": "true"} if drain else {}},
                    "status": {"address": "192.0.2.10", "ports": [{"name": "client", "port": 17654}]},
                })
            return web.json_response({})

        async def publish(info):
            states.append(info.admission)

        async def idle(*args, **kwargs):
            await asyncio.Future()

        async with sdk(handler) as port:
            monkeypatch.setenv("AGONES_SDK_HTTP_PORT", str(port))
            runtime = Runtime(Settings(battle=NodeSettings(301), match="127.0.0.1:1"), "battle")
            runtime.directory = SimpleNamespace(lease=1, register=publish, update=publish, maintain=idle)
            try:
                await runtime._battle()
                info = runtime.battle_info
                assert states == [2]
                assert (info.client_endpoint.ip, info.client_endpoint.port) == ("battle.example.test", 17654)
                assert (runtime.battle.host, runtime.battle.port) == ("battle.example.test", 17654)
                p = runtime.protocol
                client = GrpcClient(grpc_address(info.grpc_endpoint.ip, info.grpc_endpoint.port))
                fields = {"battle_id": 77, "target_instance_id": info.node_uuid, "placement_operation_id": "77"}
                create = p.message("CreateBattleRequest", **fields).SerializeToString()
                query = p.message("QueryBattleCreationRequest", **fields).SerializeToString()
                try:
                    with pytest.raises(grpc.aio.AioRpcError) as refused:
                        await client.call("/BattleNode/CreateBattle", create)
                    assert refused.value.code() == grpc.StatusCode.UNAVAILABLE
                    assert refused.value.details() == "battle_not_allocatable"
                    drain = False
                    await wait_until(lambda: states == [2, 1])
                    reply = p.parse("QueryBattleCreationResponse", await client.call(
                        "/BattleNode/QueryBattleCreation", query,
                    ))
                    assert not reply.rejects_new_battles
                    await client.call("/BattleNode/CreateBattle", create)
                    assert "/allocate" in requests
                finally:
                    await client.close()
            finally:
                await runtime.__aexit__(None, None, None)
            assert states[-1] == 2 and "/shutdown" not in requests

    asyncio.run(exercise())


def test_agones_returns_to_ready_after_late_allocate_and_stops_pending_permits(monkeypatch):
    monkeypatch.setattr(battle_agones, "_ALLOCATE_WAIT", 0.025)
    monkeypatch.setattr(battle_agones, "_HEALTH_INTERVAL", 0.025)
    monkeypatch.setattr(battle_agones, "_STALE_HEARTBEAT", 0.04)

    async def exercise():
        calls = []
        allocating = asyncio.Event()
        finish = asyncio.Event()

        async def handler(request):
            calls.append(request.path)
            if request.path == "/allocate":
                allocating.set()
                await finish.wait()
            return web.json_response({"object_meta": {}})

        async with sdk(handler) as port:
            lifecycle = BattleAgones("127.0.0.1", port)
            lifecycle.start()
            await wait_until(lambda: lifecycle.admitting)
            try:
                async with lifecycle.permit() as granted:
                    assert not granted
                assert allocating.is_set() and calls.count("/allocate") == 1
                finish.set()
                await wait_until(lambda: calls.count("/ready") == 2)

                await asyncio.sleep(0.06)
                health_count = calls.count("/health")
                await asyncio.sleep(0.06)
                assert calls.count("/health") == health_count
                lifecycle.touch_heartbeat()
                await wait_until(lambda: calls.count("/health") > health_count)

                allocating.clear()
                finish.clear()
                monkeypatch.setattr(battle_agones, "_ALLOCATE_WAIT", 2)

                async def acquire():
                    async with lifecycle.permit() as granted:
                        return granted

                permit = asyncio.create_task(acquire())
                await asyncio.wait_for(allocating.wait(), 1)
                await lifecycle.close()
                assert not await asyncio.wait_for(permit, 0.2)
                assert not lifecycle.admitting
            finally:
                finish.set()
                await lifecycle.close()
            assert calls.count("/ready") == 2 and "/shutdown" not in calls

    asyncio.run(exercise())
