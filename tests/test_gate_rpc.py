import asyncio
import hmac
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

import xuanming.nodes.gate_rpc as gate_rpc
from xuanming.cluster import GATE, SCENE, LocalDirectory
from xuanming.node_link import GateLinks, SceneLinks
from xuanming.nodes.gate import GateNode, Session
from xuanming.nodes.gate_rpc import GateRpcService, GmRequestVerifier
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.settings import Settings
from xuanming.smoke import connect
from xuanming.transport import RpcChannel, RpcDisconnected, RpcError, RpcErrorCode


_SECRET = b"gate-admin-test-signing-key"
_METHOD = "Gate.GmGracefulShutdown"


def _node(protocol, kind, node_id, uuid, *, zone=1, address="127.0.0.1"):
    return protocol.message(
        "NodeInfo", node_type=kind, zone_id=zone, node_id=node_id, node_uuid=uuid,
        endpoint={"ip": address, "port": 17100},
    )


def _gate(protocol):
    return GateNode(protocol, _node(protocol, GATE, 7, "gate"), b"gate-ticket-test-key" * 2, None)


def _session(gate, number, player_id, *, scene_id=10, verified=True):
    writer = SimpleNamespace(closed=False)
    writer.is_closing = lambda: writer.closed
    writer.transport = SimpleNamespace(abort=lambda: setattr(writer, "closed", True))
    sid = gate.info.node_id << 17 | number
    session = Session(sid, number, writer, verified=verified, player_id=player_id, scene_id=scene_id,
                      scene_instance_id="scene")
    gate.sessions[sid] = session
    return session


def _operator(timestamp, nonce, reason, *, method=_METHOD, node=7, secret=_SECRET, operator="运维"):
    canonical = "\n".join((method, str(node), operator, timestamp, nonce, reason)).encode()
    sig = hmac.digest(secret, canonical, "sha256").hex()
    return "|".join((operator, timestamp, nonce, sig))


async def _dial(protocol, server):
    reader, writer = await asyncio.open_connection(*server.sockets[0].getsockname())
    return RpcChannel(reader, writer, protocol.message_class("GameRpcMessage"))


async def _call(channel, protocol, method, request):
    return await channel.call(protocol.message_ids["Gate" + method], request.SerializeToString(), timeout=2)


def test_native_gm_auth_binds_all_fields_and_expires_nonces(monkeypatch):
    monkeypatch.setenv("GATE_GM_AUTH_SKEW_SECONDS", "30")
    clock = [100]
    verifier = GmRequestVerifier(_SECRET, clock=lambda: clock[0])
    operator = _operator("000130", "nonce", "维护\n完成")
    assert verifier.verify(_METHOD, 7, operator, "changed") == (None, "signature_mismatch")
    assert verifier.verify(_METHOD, 8, operator, "维护\n完成") == (None, "signature_mismatch")
    assert verifier.verify("Scene.GmGracefulShutdown", 7, operator, "维护\n完成") == (None, "signature_mismatch")
    assert verifier.verify(_METHOD, 7, operator, "维护\n完成") == ("运维", "ok")
    clock[0] = 160
    assert verifier.verify(_METHOD, 7, operator, "维护\n完成") == (None, "replayed_nonce")
    clock[0] = 161
    assert verifier.verify(_METHOD, 7, operator, "维护\n完成") == (None, "timestamp_out_of_window")
    updated = _operator("161", "nonce", "maintenance")
    assert verifier.verify(_METHOD, 7, updated, "maintenance") == ("运维", "ok")
    assert GmRequestVerifier(b"   ").verify(_METHOD, 7, updated, "maintenance") == (None, "secret_not_configured")


def test_gate_rpc0_verifies_scene_and_shares_kafka_broadcast_delivery(monkeypatch):
    monkeypatch.setattr(gate_rpc, "_DISCOVERY_REFRESH", 0)

    async def exercise():
        p, directory = Protocol(), LocalDirectory()
        gate = _gate(p)
        service = GateRpcService(gate, directory, lambda: None, gm_secret=_SECRET)
        scene = _node(p, SCENE, 3, "scene")
        directory.publish(scene)
        first = _session(gate, 4, 40)
        second = _session(gate, 7, 70, scene_id=20)
        unbound = _session(gate, 10, 0)
        content = p.content(31, 9, body=b"broadcast-content")
        unicast = p.message("NodeRouteMessageRequest", message_content=content,
                            header={"session_id": first.session_id, "target_player_id": 40})

        async with await asyncio.start_server(service.serve, "127.0.0.1", 0) as server:
            channel = await _dial(p, server)
            try:
                with pytest.raises(RpcError) as failure:
                    await _call(channel, p, "SendMessageToPlayer", unicast)
                assert failure.value.code == RpcErrorCode.NO_SERVICE
                request = p.message("NodeHandshakeRequest", self_node=scene, expected_peer_uuid="wrong")
                reply = await _call(channel, p, "NodeHandshake", request)
                assert p.parse("NodeHandshakeResponse", reply).error_message.id == 1018
                request.expected_peer_uuid = gate.info.node_uuid
                request.self_node.endpoint.ip = "127.0.0.2"
                reply = await _call(channel, p, "NodeHandshake", request)
                assert p.parse("NodeHandshakeResponse", reply).error_message.id == 1018
                directory.publish(request.self_node)
                reply = await _call(channel, p, "NodeHandshake", request)
                assert p.parse("NodeHandshakeResponse", reply).error_message.id == 1018
                directory.publish(scene)
                request.self_node.CopyFrom(scene)
                reply = p.parse("NodeHandshakeResponse", await _call(channel, p, "NodeHandshake", request))
                assert not reply.error_message.id and reply.peer_node == gate.info
                await _call(channel, p, "SendMessageToPlayer", unicast)
                assert first.outbound.get_nowait() == content
                unicast.header.target_player_id = 70
                await _call(channel, p, "SendMessageToPlayer", unicast)
                assert first.outbound.empty()

                kick = p.message("NodeRouteMessageRequest", message_content=p.content(
                    p.message_ids["SceneClientPlayerCommonKickPlayer"],
                ), header={"session_id": first.session_id, "target_player_id": 40})
                first.scene_instance_id = "new-scene"
                await _call(channel, p, "SendMessageToPlayer", kick)
                assert first.outbound.empty()

                method = "BroadcastToPlayers"
                fields = dict(
                    message_content=content, session_bitmap_base=first.session_id,
                    session_bitmap=bytes([0b1001]), player_list=[0, 999],
                )
                await _call(channel, p, method, p.message(method + "Request", **fields))
                assert first.outbound.get_nowait() == content and second.outbound.empty()
                event = p.message(f"contracts.kafka.{method}Event", **fields)
                command = p.message(
                    "contracts.kafka.GateCommand", target_gate_id=gate.info.node_id,
                    target_instance_id=gate.info.node_uuid, event_id=p.event_ids[f"ContractsKafka{method}Event"],
                    payload=event.SerializeToString(),
                )
                await gate.command(command.SerializeToString())
                assert first.outbound.get_nowait() == content and second.outbound.empty()
                fields["player_list"] = [40]
                await _call(channel, p, method, p.message(method + "Request", **fields))
                assert first.outbound.empty() and second.outbound.empty()
                await _call(channel, p, "BroadcastToScene", p.message(
                    "BroadcastToSceneRequest", scene_id=20, message_content=content,
                ))
                assert second.outbound.get_nowait() == content and first.outbound.empty()
                await _call(channel, p, "BroadcastToAll", p.message("BroadcastToAllRequest"))
                assert first.outbound.empty() and second.outbound.empty()
                await _call(channel, p, "BroadcastToAll", p.message("BroadcastToAllRequest", message_content=content))
                assert first.outbound.get_nowait() == second.outbound.get_nowait() == content
                assert unbound.outbound.empty()
                directory.publish(_node(p, SCENE, 3, "replacement"))
                with pytest.raises(RpcError) as failure:
                    await _call(channel, p, "SendMessageToPlayer", unicast)
                assert failure.value.code == RpcErrorCode.NO_SERVICE
            finally:
                await channel.aclose()
                await service.close()

    asyncio.run(exercise())


def test_gate_rpc0_connection_bounds_and_signed_gm_without_handshake():
    async def exercise():
        p, stopped = Protocol(), asyncio.Event()
        gate = _gate(p)
        active = _session(gate, 1, 0, verified=False)
        service = GateRpcService(gate, LocalDirectory(), stopped.set, gm_secret=_SECRET,
                                 max_connections=1, handshake_timeout=.5)
        service.gm_auth.clock = lambda: 100
        async with await asyncio.start_server(service.serve, "127.0.0.1", 0) as server:
            idle = await _dial(p, server)
            over_limit = await _dial(p, server)
            try:
                await asyncio.wait_for(over_limit.wait_closed(), 1)
                await asyncio.wait_for(idle.wait_closed(), 1)
            finally:
                await over_limit.aclose()
                await idle.aclose()

            admin = await _dial(p, server)
            try:
                request = p.message("GmGracefulShutdownRequest", operator="unsigned", reason="maintenance")
                with pytest.raises(RpcError) as failure:
                    await _call(admin, p, "GmGracefulShutdown", request)
                assert failure.value.code == RpcErrorCode.HANDLER_FAILED and not stopped.is_set()
                setattr(request, "operator", _operator("100", "unique-nonce", "maintenance"))
                response = await _call(admin, p, "GmGracefulShutdown", request)
                assert p.parse("GmGracefulShutdownResponse", response).affected_count == 1
                assert stopped.is_set() and active.writer.closed
                with pytest.raises(RpcError) as failure:
                    await _call(admin, p, "GmGracefulShutdown", request)
                assert failure.value.code == RpcErrorCode.HANDLER_FAILED
                with pytest.raises(RpcDisconnected):
                    late = await _dial(p, server)
                    try:
                        await _call(late, p, "GmGracefulShutdown", request)
                    finally:
                        await late.aclose()
            finally:
                await admin.aclose()
                await service.close()

    asyncio.run(exercise())


def test_verified_scene_reverse_link_can_broadcast():
    async def exercise():
        p, directory = Protocol(), LocalDirectory()
        gate = _gate(p)
        client = _session(gate, 3, 88)
        scene_info = _node(p, SCENE, 6, "scene")
        scene = SceneLinks(p, scene_info, directory)
        content = p.content(31, 5, body=b"reverse-push")
        directory.publish(gate.info)

        async def enter(request, gate_uuid):
            peer = scene._peers[gate_uuid]
            await _call(peer.channel, p, "BroadcastToAll", p.message("BroadcastToAllRequest", message_content=content))
            return p.message("Empty")

        scene.bind(SimpleNamespace(enter=enter))
        links = GateLinks(p, gate.info, directory, gate.push, source_aware=True, on_broadcast=gate.broadcast)
        async with await asyncio.start_server(scene.serve, "127.0.0.1", 0) as server:
            scene_info.endpoint.ip, scene_info.endpoint.port = server.sockets[0].getsockname()
            directory.publish(scene_info)
            try:
                request = p.message("PlayerEnterGameNodeRequest", player_id=88, session_id=client.session_id)
                await links.call(6, p.message_ids["ScenePlayerEnterGameNode"], request, timeout=2)
                assert client.outbound.get_nowait() == content
            finally:
                await links.close()
                await scene.close()

    asyncio.run(exercise())


def test_runtime_advertises_distinct_gate_ports_and_flushes_admin_reply(tmp_path, monkeypatch):
    monkeypatch.setenv("GATE_GM_ADMIN_SECRET", _SECRET.decode())

    async def exercise():
        defaults = Settings()
        settings = replace(
            defaults, database=tmp_path / "gate-rpc.sqlite3",
            gate=replace(defaults.gate, port=0, rpc_port=0),
            scene=replace(defaults.scene, port=0, grpc_port=0),
            battle=replace(defaults.battle, port=0, grpc_port=0),
        )
        started = asyncio.get_running_loop().create_future()

        async def runtime_lifetime():
            async with Runtime(settings, "local", dev_token="test-only") as runtime:
                started.set_result(runtime)
                await runtime.wait()

        serving = asyncio.create_task(runtime_lifetime())
        try:
            async with asyncio.timeout(5):
                runtime = await asyncio.shield(started)
                p, gate = runtime.protocol, runtime.gate
                info = gate.info
                assert info.client_endpoint.port and info.endpoint.port != info.client_endpoint.port
                async with connect(p, info.client_endpoint) as client:
                    await client.send(gate.issue_ticket())
                    assert (await client.receive("ClientTokenVerifyResponse")).success
                    reader, writer = await asyncio.open_connection(info.endpoint.ip, info.endpoint.port)
                    admin = RpcChannel(reader, writer, p.message_class("GameRpcMessage"))
                    try:
                        request = p.message(
                            "GmGracefulShutdownRequest", reason="maintenance",
                            operator=_operator(str(int(time.time())), "runtime-ack", "maintenance", node=info.node_id),
                        )
                        reply = await _call(admin, p, "GmGracefulShutdown", request)
                        assert p.parse("GmGracefulShutdownResponse", reply).affected_count == 1
                        await serving
                    finally:
                        await admin.aclose()
        finally:
            if not serving.done():
                serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)

    asyncio.run(exercise())
