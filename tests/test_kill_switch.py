import asyncio
from types import SimpleNamespace

import grpc
import pytest

from test_scene_assets import _scene
from test_transport import tcp_pair
from xuanming.grpc_runtime import GrpcClient, serve_grpc
from xuanming.kill_switch import EtcdKillSwitch, KillSwitch, Rule, parse_rule
from xuanming.nodes.battle_tcp import BattleClients
from xuanming.nodes.gate import GateNode, Session
from xuanming.protocol import Protocol
from xuanming.transport import RpcChannel, RpcError, RpcErrorCode


def _switch(protocol, node_type=3, *, clock=lambda: 100.0):
    return KillSwitch(protocol, protocol.message("NodeInfo", node_type=node_type, node_uuid="node-a"), clock=clock)


def test_rules_precedence_protection_and_stale_fail_open():
    protocol = Protocol()
    now = [10.0]
    switch = _switch(protocol, clock=lambda: now[0])
    prefix = b"/mmorpg/killswitch/"
    switch.replace(10, [
        (prefix + b"*", b"true"),
        (prefix + b"type/SceneNodeService/SceneBagClientPlayer/*", b"false"),
        (prefix + b"instance/node-a/SceneBagClientPlayer/GetBag", b'{"deny":true,"code":9}'),
    ])
    assert switch.check("SceneBagClientPlayer", "GetBag") == Rule(True, "", 9)
    assert switch.check("SceneBagClientPlayer", "SortBag") is None
    assert switch.check("ScenePlayer", "ExitGame") is None
    switch.replace(11, [(prefix + b"*", b"true"), (prefix + b"ScenePlayer/ExitGame", b"deny")])
    assert switch.check("ScenePlayer", "ExitGame").deny
    switch.replace(9, [])  # an older range renews freshness without reverting state
    assert switch.check("ScenePlayer", "ExitGame").deny
    now[0] += 61
    assert switch.check("SceneBagClientPlayer", "GetBag") is None
    assert parse_rule(b'{"deny":"true"}') is None
    assert parse_rule(b'{"deny":true,"code":-1}') is None
    assert parse_rule(b'') == Rule()


def test_watch_updates_same_revision_and_malformed_rules():
    protocol = Protocol()
    switch = _switch(protocol)
    prefix = b"/mmorpg/killswitch/"
    switch.replace(10, [(prefix + b"*", b"true")])
    def event(key, value, revision, *, delete=False):
        return SimpleNamespace(type=int(delete), kv=SimpleNamespace(key=key, value=value, mod_revision=revision))
    switch.apply_events([event(prefix + b"SceneBagClientPlayer/GetBag", b"false", 11)])
    switch.apply_events([event(prefix + b"SceneBagClientPlayer/SortBag", b"false", 11)])
    assert switch.check("SceneBagClientPlayer", "GetBag") is None
    assert switch.check("SceneBagClientPlayer", "SortBag") is None
    switch.apply_events([event(prefix + b"SceneBagClientPlayer/GetBag", b"garbage", 12)])
    assert switch.check("SceneBagClientPlayer", "GetBag").deny
    switch.apply_events([event(prefix + b"*", b"", 13, delete=True)])
    assert switch.check("SceneBagClientPlayer", "GetBag") is None
    switch.apply_events([event(prefix + b"*", b"true", 0)], header_revision=14)
    assert switch.check("SceneBagClientPlayer", "GetBag").deny


def test_etcd_range_then_watch_uses_next_revision():
    async def exercise():
        protocol = Protocol()
        prefix = b"/mmorpg/killswitch/"
        started, publish, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def get_range(data, context):
            request = protocol.parse("etcdserverpb.RangeRequest", data)
            assert request.key == prefix and request.range_end == prefix[:-1] + b"0"
            return protocol.message("etcdserverpb.RangeResponse", header={"revision": 10},
                                    kvs=[{"key": prefix + b"*", "value": b"true"}]).SerializeToString()
        async def watch(requests, context):
            request = protocol.parse("etcdserverpb.WatchRequest", await anext(requests))
            assert request.create_request.start_revision == 11
            started.set()
            await publish.wait()
            yield protocol.message("etcdserverpb.WatchResponse", header={"revision": 11}, events=[{
                "kv": {"key": prefix + b"SceneBagClientPlayer/GetBag", "value": b"false", "mod_revision": 11},
            }]).SerializeToString()
            await stop.wait()
        server = grpc.aio.server()
        server.add_generic_rpc_handlers((
            grpc.method_handlers_generic_handler("etcdserverpb.KV", {
                "Range": grpc.unary_unary_rpc_method_handler(get_range),
            }),
            grpc.method_handlers_generic_handler("etcdserverpb.Watch", {
                "Watch": grpc.stream_stream_rpc_method_handler(watch),
            }),
        ))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        switch = _switch(protocol)
        sync = EtcdKillSwitch(protocol, f"127.0.0.1:{port}", switch)
        sync.start()
        try:
            await asyncio.wait_for(started.wait(), 5)
            assert switch.check("SceneBagClientPlayer", "GetBag").deny
            publish.set()
            async def changed():
                while switch.revision < 11:
                    await asyncio.sleep(.01)
            await asyncio.wait_for(changed(), 5)
            assert switch.check("SceneBagClientPlayer", "GetBag") is None
            assert switch.check("SceneBagClientPlayer", "SortBag").deny
        finally:
            stop.set()
            await sync.close()
            await server.stop(0)
    asyncio.run(exercise())


def test_etcd_compacted_watch_ranges_again_before_resuming():
    async def exercise():
        protocol = Protocol()
        ranges, revisions = [], []
        resumed = asyncio.Event()
        async def get_range(data, context):
            ranges.append(1)
            revision = 10 if len(ranges) == 1 else 20
            return protocol.message("etcdserverpb.RangeResponse", header={"revision": revision}).SerializeToString()
        async def watch(requests, context):
            request = protocol.parse("etcdserverpb.WatchRequest", await anext(requests))
            revisions.append(request.create_request.start_revision)
            if len(revisions) == 1:
                yield protocol.message("etcdserverpb.WatchResponse", canceled=True,
                                       compact_revision=15).SerializeToString()
            else:
                resumed.set()
                await asyncio.Event().wait()
        server = grpc.aio.server()
        server.add_generic_rpc_handlers((
            grpc.method_handlers_generic_handler("etcdserverpb.KV", {
                "Range": grpc.unary_unary_rpc_method_handler(get_range),
            }),
            grpc.method_handlers_generic_handler("etcdserverpb.Watch", {
                "Watch": grpc.stream_stream_rpc_method_handler(watch),
            }),
        ))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        switch = _switch(protocol)
        sync = EtcdKillSwitch(protocol, f"127.0.0.1:{port}", switch)
        sync.start()
        try:
            await asyncio.wait_for(resumed.wait(), 5)
            assert len(ranges) >= 2 and revisions[:2] == [11, 21]
        finally:
            await sync.close()
            await server.stop(0)
    asyncio.run(exercise())
def test_client_gate_scene_and_battle_dispatch(tmp_path):
    async def exercise():
        protocol, scene, _, _ = await _scene(tmp_path)
        gate_info = protocol.message("NodeInfo", node_type=4, node_id=1, zone_id=1, node_uuid="gate")
        gate = GateNode(protocol, gate_info, b"s" * 32, lambda *args: None)
        gate.kill_switch = _switch(protocol, 4)
        scene.kill_switch = _switch(protocol, 3)
        request_id = protocol.message_ids["SceneBagClientPlayerGetBag"]
        gate.kill_switch.replace(1, [(b"/mmorpg/killswitch/SceneBagClientPlayer/GetBag", b"true")])
        request = protocol.message("ClientRequest", id=4, message_id=request_id)
        assert (await gate._dispatch(Session(1, 1, None), 1, request)).error_message.id == 1003
        scene.kill_switch.replace(1, [(b"/mmorpg/killswitch/SceneBagClientPlayer/GetBag", b"true")])
        result = await scene.process(protocol.message("ProcessClientPlayerMessageRequest", session_id=9, player_id=1,
                                     message_content=protocol.content(request_id, 5)), "gate")
        assert result.message_content.error_message.id == 1003
        battle_switch = _switch(protocol, 28)
        battle_switch.replace(1, [(b"/mmorpg/killswitch/BattleClientPlayer/GetBattleState", b"true")])
        edge = BattleClients(protocol, SimpleNamespace(tables=None, kill_switch=battle_switch))
        result = edge._dispatch(None, protocol.message("ClientRequest", id=6, message_id=140), 1, {})
        assert result.error_message.id == 1003
        await gate.close()
        await scene.close()
    asyncio.run(exercise())


def test_rpc0_and_grpc_denial_before_handler():
    async def exercise():
        protocol = Protocol()
        calls = []
        async def handler(body):
            calls.append(body)
            return body
        async with tcp_pair() as (client_stream, server_stream):
            envelope = protocol.message_class("GameRpcMessage")
            server = RpcChannel(*server_stream, envelope, {7: handler}, deny=lambda mid: mid == 7)
            client = RpcChannel(*client_stream, envelope)
            try:
                with pytest.raises(RpcError) as error:
                    await client.call(7, b"x")
                assert error.value.code == RpcErrorCode.KILL_SWITCH_DENIED and not calls
            finally:
                await client.aclose()
                await server.aclose()
        switch = _switch(protocol, 28)
        switch.replace(1, [(b"/mmorpg/killswitch/BattleNode/CreateBattle", b'{"deny":true,"code":9}')])
        server, port = await serve_grpc(protocol, "127.0.0.1:0", {"BattleNode": {"CreateBattle": handler}},
                                        kill_switch=switch)
        client = GrpcClient(f"127.0.0.1:{port}")
        try:
            request = protocol.message("CreateBattleRequest")
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await client.call("/BattleNode/CreateBattle", request.SerializeToString())
            assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION and not calls
        finally:
            await client.close()
            await server.stop(0)
    asyncio.run(exercise())
