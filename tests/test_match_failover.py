import asyncio

import grpc
import pytest

from xuanming.cluster import LocalDirectory
from xuanming.match_client import MatchClient, MatchUnavailable
from xuanming.protocol import Protocol


PATH = "/match.MatchInternal/CommitBattleOutcome"


async def _server(handler):
    server = grpc.aio.server()
    method = grpc.unary_unary_rpc_method_handler(handler, request_deserializer=lambda value: value,
                                                  response_serializer=lambda value: value)
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        "match.MatchInternal", {"CommitBattleOutcome": method}),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, port


def _register(directory, protocol, node_id, port):
    info = protocol.message("NodeInfo", node_type=16, node_id=node_id, zone_id=1,
                            node_uuid=f"match-{node_id}", protocol_type=1)
    info.grpc_endpoint.ip, info.grpc_endpoint.port = "127.0.0.1", port
    directory.publish(info)


def test_real_grpc_replicas_failover_and_registry_removal(monkeypatch):
    async def run():
        requests = []

        async def unavailable(data, context):
            requests.append(("first", data))
            await context.abort(grpc.StatusCode.UNAVAILABLE, "replica unavailable")

        async def healthy(data, context):
            requests.append(("second", data))
            return b"committed"

        first, first_port = await _server(unavailable)
        second, second_port = await _server(healthy)
        directory, protocol = LocalDirectory(), Protocol()
        _register(directory, protocol, 1, first_port)
        _register(directory, protocol, 2, second_port)
        first_address = f"127.0.0.1:{first_port}"
        monkeypatch.setattr("xuanming.match_client.random.shuffle", lambda values: values.sort(key=lambda v: v != first_address))
        client = MatchClient(directory, "127.0.0.1:1")
        try:
            assert await client.call(PATH, b"same-frozen-original") == b"committed"
            assert requests == [("first", b"same-frozen-original"), ("second", b"same-frozen-original")]
            del directory.nodes[16, 1, 1]
            client._expires = 0
            assert await client.call(PATH, b"new-original") == b"committed"
            assert first_address not in client.clients
            assert requests[-1] == ("second", b"new-original")
            with pytest.raises(ValueError):
                await client.call("/match.MatchInternal/NonIdempotent", b"")
        finally:
            await client.close()
            await asyncio.gather(first.stop(None), second.stop(None))

    asyncio.run(run())


def test_fixed_address_fallback_and_nonretryable_status():
    async def run():
        calls = []

        async def denied(data, context):
            calls.append(data)
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "denied")

        server, port = await _server(denied)
        client = MatchClient(LocalDirectory(), f"127.0.0.1:{port}")
        try:
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await client.call(PATH, b"original")
            assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
            assert calls == [b"original"]
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(run())


def test_fallback_remains_available_during_discovery_outage():
    async def run():
        class FailedDirectory:
            calls = 0

            async def candidates(self, *_):
                self.calls += 1
                raise OSError("etcd unavailable")

        async def accepted(data, context):
            return data

        server, port = await _server(accepted)
        directory = FailedDirectory()
        client = MatchClient(directory, f"127.0.0.1:{port}")
        try:
            assert await client.call(PATH, b"first") == b"first"
            assert await client.call(PATH, b"second") == b"second"
            assert directory.calls == 1
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(run())


def test_canary_and_unknown_tracks_do_not_use_fixed_fallback():
    async def run():
        directory, protocol = LocalDirectory(), Protocol()
        _register(directory, protocol, 1, 11001)
        _register(directory, protocol, 2, 11002)
        directory.nodes[16, 1, 1].release_track = "canary"
        directory.nodes[16, 1, 2].release_track = "invalid"
        client = MatchClient(directory, "127.0.0.1:1")
        try:
            with pytest.raises(MatchUnavailable) as error:
                await client.call(PATH, b"original")
            assert error.value.code() == grpc.StatusCode.UNAVAILABLE
            assert client.clients == {}
            directory.nodes[16, 1, 1].release_track = "stable"
            client._expires = 0
            assert await client._addresses() == ["127.0.0.1:11001"]
        finally:
            await client.close()

    asyncio.run(run())


def test_removing_replica_does_not_close_an_inflight_call():
    async def run():
        started, release = asyncio.Event(), asyncio.Event()

        async def delayed(data, context):
            started.set()
            await release.wait()
            return data

        async def ready(data, context):
            return data

        first, first_port = await _server(delayed)
        second, second_port = await _server(ready)
        directory, protocol = LocalDirectory(), Protocol()
        _register(directory, protocol, 1, first_port)
        client = MatchClient(directory, "127.0.0.1:1")
        first_call = asyncio.create_task(client.call(PATH, b"inflight"))
        try:
            await asyncio.wait_for(started.wait(), 2)
            del directory.nodes[16, 1, 1]
            _register(directory, protocol, 2, second_port)
            client._expires = 0
            assert await client.call(PATH, b"replacement") == b"replacement"
            release.set()
            assert await first_call == b"inflight"
        finally:
            release.set()
            await asyncio.gather(first_call, return_exceptions=True)
            await client.close()
            await asyncio.gather(first.stop(None), second.stop(None))

    asyncio.run(run())


def test_ready_replicas_share_requests(monkeypatch):
    async def run():
        calls = []

        async def first_handler(data, context):
            calls.append("first")
            return b"ok"

        async def second_handler(data, context):
            calls.append("second")
            return b"ok"

        first, first_port = await _server(first_handler)
        second, second_port = await _server(second_handler)
        directory, protocol = LocalDirectory(), Protocol()
        _register(directory, protocol, 1, first_port)
        _register(directory, protocol, 2, second_port)
        client = MatchClient(directory, "127.0.0.1:1")
        order = 0

        def alternate(values):
            nonlocal order
            order += 1
            values.sort(reverse=bool(order % 2))

        monkeypatch.setattr("xuanming.match_client.random.shuffle", alternate)
        try:
            assert await client.call(PATH, b"warmup") == b"ok"
            async with asyncio.timeout(3):
                while (len(client.clients) != 2 or any(
                    rpc.channel.get_state(True) != grpc.ChannelConnectivity.READY
                    for rpc in client.clients.values()
                )):
                    await asyncio.sleep(.02)
            calls.clear()
            for _ in range(8):
                assert await client.call(PATH, b"original") == b"ok"
            assert calls.count("first") == 4
            assert calls.count("second") == 4
        finally:
            await client.close()
            await asyncio.gather(first.stop(None), second.stop(None))

    asyncio.run(run())
