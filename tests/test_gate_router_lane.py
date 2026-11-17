import asyncio
import base64
import socket

import grpc
import pytest

from xuanming.grpc_runtime import GrpcClient
from xuanming.nodes import gate as gate_module
from xuanming.nodes.gate import GateNode, RouterForwarder, Session
from xuanming.protocol import Protocol
from xuanming.transport import Packet


class Writer:
    def __init__(self):
        self.closed = False
        self.transport = self

    def close(self): self.closed = True
    def abort(self): self.close()
    def is_closing(self): return self.closed


def setup(forward=None, **session_fields):
    protocol = Protocol()
    gate = GateNode(protocol, protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate"),
                    b"x" * 32, forward)
    return protocol, gate, Session(131073, 1, Writer(), verified=True, **session_fields)


def request(protocol, method, request_id):
    return protocol.message("ClientRequest", id=request_id, message_id=protocol.message_ids[method])


def test_router_depth_counts_inflight_and_rejects_without_illegal_packet():
    protocol, gate, session = setup()
    session.lane_busy = True
    for request_id in range(1, 33):
        gate._admit_router(session, request(protocol, "ClientPlayerChatSendChat", request_id), 0)
    assert session.requests.qsize() == 31
    error = session.outbound.get_nowait()
    assert (error.id, error.error_message.id, session.illegal_packets) == (32, 1008, 0)


def test_expired_and_rebound_requests_do_not_reach_router():
    async def run():
        calls = []

        async def forward(*args):
            calls.append(args)

        protocol, gate, session = setup(forward, player_id=42, account="old", owner_revision=1)
        gate._admit_router(session, request(protocol, "ClientPlayerChatSendChat", 1),
                           gate_module.time.monotonic() - 6)
        gate._admit_router(session, request(protocol, "ClientPlayerChatPullChatHistory", 2),
                           gate_module.time.monotonic())
        session.account, session.owner_revision = "new", 2
        worker = asyncio.create_task(gate._lane(session))
        try:
            first = await asyncio.wait_for(session.outbound.get(), 1)
            second = await asyncio.wait_for(session.outbound.get(), 1)
            assert (first.id, first.error_message.id) == (1, 1003)
            assert (second.id, second.error_message.id) == (2, 2028)
            assert not calls
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    asyncio.run(run())


def test_queued_leave_blocks_scene_and_local_gate_requests(monkeypatch):
    async def run():
        protocol, gate, session = setup(player_id=42, account="acct", owner_revision=1, logged_in=True)
        gate._admit_router(session, request(protocol, "ClientPlayerLoginLeaveGame", 1),
                           gate_module.time.monotonic())
        received = iter((
            Packet.from_message(request(protocol, "SceneBagClientPlayerGetBag", 2)),
            Packet.from_message(request(protocol, "GateClientSpectateSubscribeSpectate", 3)),
            None,
        ))

        async def fake_read(_reader):
            return next(received)

        monkeypatch.setattr(gate_module, "read_packet", fake_read)
        await gate._receive(None, session)
        first, second = session.outbound.get_nowait(), session.outbound.get_nowait()
        assert (first.id, first.error_message.id) == (2, 1003)
        assert (second.id, second.error_message.id) == (3, 1003)
        assert session.requests.qsize() == 1

    asyncio.run(run())


def test_leave_barrier_remains_during_scene_exit_and_clears_on_send(monkeypatch):
    async def run():
        started, release, sent = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def forward(_gate, _session, _route, frame, _seq):
            sent.set()
            return protocol.content(frame.message_id, frame.id)

        protocol, gate, session = setup(forward, player_id=42, account="acct", owner_revision=1,
                                         logged_in=True, scene_node_id=6)

        async def delayed_exit(_session, _reason):
            started.set()
            await release.wait()

        gate.exit_scene = delayed_exit
        gate._admit_router(session, request(protocol, "ClientPlayerLoginLeaveGame", 1),
                           gate_module.time.monotonic())
        worker = asyncio.create_task(gate._lane(session))
        try:
            await asyncio.wait_for(started.wait(), 1)
            received = iter((Packet.from_message(request(protocol, "SceneBagClientPlayerGetBag", 2)), None))

            async def fake_read(_reader): return next(received)
            monkeypatch.setattr(gate_module, "read_packet", fake_read)
            await gate._receive(None, session)
            error = session.outbound.get_nowait()
            assert (error.id, error.error_message.id) == (2, 1003)
            release.set()
            await asyncio.wait_for(sent.wait(), 1)
            assert session.queued_leave == 0
        finally:
            release.set()
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    asyncio.run(run())


def test_router_leave_accepts_cpp_scene_empty_response_and_forwards_to_login():
    async def run():
        calls = []

        async def forward(_gate, _session, route, frame, _seq):
            calls.append(route.method)
            return protocol.content(frame.message_id, frame.id)

        protocol, gate, session = setup(forward, player_id=42, account="acct", owner_revision=1,
                                         logged_in=True, scene_node_id=6, scene_instance_id="scene-a")

        class CppScene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid):
                assert (node, expected_uuid, one_way, payload.player_id) == (6, "scene-a", False, 42)
                assert message_id == protocol.message_ids["SceneProcessClientPlayerMessage"]
                assert protocol.parse("GameNodeExitGameRequest", payload.message_content.serialized_message).reason == 1
                calls.append("SceneExit")
                return b""  # C++ sends no content for the Empty inner response.

        gate.scene = CppScene()
        leave = request(protocol, "ClientPlayerLoginLeaveGame", 1)
        response = await gate._dispatch(session, 1, leave)
        assert calls == ["SceneExit", "LeaveGame"]
        assert (response.message_id, response.id, response.error_message.id) == (leave.message_id, 1, 0)
        assert (session.player_id, session.scene_node_id, session.account) == (0, 0, "")

    asyncio.run(run())


def test_router_leave_with_cpp_empty_response_over_rpc0_and_grpc(tmp_path):
    from test_scene_enter_fence import _edge

    async def run():
        async with _edge(tmp_path) as (protocol, scene, links, enter):
            sid = (5 << 17) | 1
            await enter(sid, 8)
            original_process = scene.process

            async def cpp_response(request, gate_uuid):
                response = await original_process(request, gate_uuid)
                if request.message_content.message_id == protocol.message_ids["ScenePlayerExitGame"]:
                    return protocol.message("ProcessClientPlayerMessageResponse")
                return response

            scene.process = cpp_response
            seen = []

            async def router(raw, context):
                incoming = protocol.parse("client_rpc_router.ForwardRequest", raw).request
                seen.append(incoming.message_id)
                await context.send_initial_metadata((("x-session-detail-bin", dict(context.invocation_metadata())["x-session-detail-bin"]),))
                return protocol.content(incoming.message_id, incoming.id).SerializeToString()

            server = grpc.aio.server()
            server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("client_rpc_router.ClientRpcRouter", {
                "Forward": grpc.unary_unary_rpc_method_handler(router),
            }),))
            port = server.add_insecure_port("127.0.0.1:0")
            await server.start()
            client = GrpcClient(f"127.0.0.1:{port}", timeout=2, enable_retries=False)
            info = protocol.message("NodeInfo", node_id=5, zone_id=1, node_uuid="enter-gate")
            gate = GateNode(protocol, info, b"x" * 32, RouterForwarder(protocol, client, b"y" * 32))
            gate.scene = links
            session = Session(sid, 1, Writer(), verified=True, logged_in=True, player_id=1,
                              account="alice", owner_revision=1, login_generation=8,
                              scene_node_id=6, scene_id=1, scene_instance_id="enter-scene")
            try:
                leave = request(protocol, "ClientPlayerLoginLeaveGame", 1)
                response = await gate._dispatch(session, 1, leave)
                assert not response.error_message.id
                assert seen == [leave.message_id] and not scene.sessions and 1 not in scene.locations
                assert (session.player_id, session.scene_node_id) == (0, 0)
            finally:
                await client.close()
                await server.stop(0)

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["wrong_session", "retry_later", "wrong_content", "error", "route_changed"])
def test_router_leave_rejects_invalid_scene_response(failure):
    async def run():
        calls = []

        async def forward(*_args):
            calls.append("Login")

        protocol, gate, session = setup(forward, player_id=42, account="acct", owner_revision=1,
                                         logged_in=True, scene_node_id=6, scene_instance_id="scene-a")

        class Scene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid):
                reply = protocol.message("ProcessClientPlayerMessageResponse")
                if failure == "wrong_session":
                    reply.session_id = session.session_id + 1
                elif failure == "retry_later":
                    reply.dispatch_verdict = 1
                elif failure == "wrong_content":
                    reply.session_id = session.session_id
                    reply.message_content.CopyFrom(protocol.content(123, 0))
                elif failure == "error":
                    reply.session_id = session.session_id
                    reply.message_content.CopyFrom(protocol.content(payload.message_content.message_id, 0, error=1003))
                else:
                    session.scene_instance_id = "replacement"
                return reply.SerializeToString()

        gate.scene = Scene()
        leave = request(protocol, "ClientPlayerLoginLeaveGame", 1)
        response = await gate._dispatch(session, 1, leave)
        assert response.error_message.id == 1003
        assert not calls
        assert session.player_id == 42

    asyncio.run(run())


def test_request_received_behind_leave_clears_account_snapshot():
    protocol, gate, session = setup(player_id=42, account="old", owner_revision=7, logged_in=True)
    gate._admit_router(session, request(protocol, "ClientPlayerLoginLeaveGame", 1), gate_module.time.monotonic())
    gate._admit_router(session, request(protocol, "ClientPlayerLoginLogin", 2), gate_module.time.monotonic())
    leave, login = session.requests.get_nowait(), session.requests.get_nowait()
    assert leave.details.authenticated_account == "old" and leave.details.owner_revision == 7
    assert login.details.player_id == 42
    assert login.details.authenticated_account == "" and login.details.owner_revision == 0


def test_router_signs_admission_snapshot_not_later_session_identity():
    async def run():
        protocol = Protocol()
        observed = []

        class Call:
            def __init__(self, metadata, response):
                self.metadata, self.response = metadata, response
            def __await__(self):
                async def result(): return self.response
                return result().__await__()
            async def initial_metadata(self): return (self.metadata[0],)

        class Client:
            def request(self, path, payload, *, metadata):
                details = protocol.parse("SessionDetails", base64.b64decode(metadata[0][1]))
                observed.append(details)
                request = protocol.parse("client_rpc_router.ForwardRequest", payload).request
                result = protocol.message("loginpb.LoginResponse")
                response = protocol.content(request.message_id, request.id, body=result.SerializeToString())
                return Call(metadata, response.SerializeToString())

        gate = GateNode(protocol, protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate"),
                        b"x" * 32, RouterForwarder(protocol, Client(), b"y" * 32))
        session = Session(131073, 1, Writer(), verified=True)
        gate._admit_router(session, request(protocol, "ClientPlayerLoginLogin", 1), gate_module.time.monotonic())
        session.player_id, session.account, session.owner_revision = 42, "later", 9
        worker = asyncio.create_task(gate._lane(session))
        try:
            assert (await asyncio.wait_for(session.outbound.get(), 1)).id == 1
            assert len(observed) == 1
            assert (observed[0].player_id, observed[0].authenticated_account, observed[0].owner_revision) == (0, "", 0)
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    asyncio.run(run())


def test_router_enter_game_parse_and_bound_role_fences():
    async def run():
        calls = []

        async def forward(_gate, _session, _route, frame, _seq):
            calls.append(frame.body)
            return protocol.content(frame.message_id, frame.id, error=1014)

        protocol, gate, session = setup(forward, logged_in=True, player_id=42)
        method = "ClientPlayerLoginEnterGame"
        malformed = request(protocol, method, 1)
        malformed.body = b"\x08\x80"
        reply = await gate._dispatch(session, 1, malformed)
        assert (reply.id, reply.error_message.id) == (1, 1014) and not calls
        assert not session.enter_requested

        switched = request(protocol, method, 2)
        switched.body = protocol.message("loginpb.EnterGameRequest", player_id=43).SerializeToString()
        reply = await gate._dispatch(session, 2, switched)
        assert (reply.id, reply.error_message.id) == (2, 2005) and not calls
        assert not session.enter_requested

        session.player_id = 0
        reply = await gate._dispatch(session, 3, malformed)
        assert (reply.id, reply.error_message.id) == (1, 1014)
        assert calls == [b"\x08\x80"] and session.enter_requested and session.enter_player_id == 0

    asyncio.run(run())


def test_leave_unreachable_router_is_unsent_and_retry_preserves_then_releases():
    async def run():
        protocol, gate, session = setup(player_id=42, account="alice", owner_revision=5,
                                        logged_in=True, scene_node_id=6, scene_id=9,
                                        scene_instance_id="scene-a")
        gate.sessions[session.session_id] = session
        calls, exits = [], []
        class Scene:
            async def call(self, node, message_id, payload, **kwargs):
                exits.append((node, payload.player_id))
                return protocol.message("ProcessClientPlayerMessageResponse", session_id=session.session_id,
                                        message_content=protocol.content(payload.message_content.message_id)).SerializeToString()
        gate.scene = Scene()
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        client = GrpcClient(f"127.0.0.1:{port}", timeout=0.2, enable_retries=False)
        gate.forward = RouterForwarder(protocol, client, b"x" * 32)
        leave = request(protocol, "ClientPlayerLoginLeaveGame", 1)
        try:
            gate._admit_router(session, leave, gate_module.time.monotonic())
            worker = asyncio.create_task(gate._lane(session))
            assert (await asyncio.wait_for(session.outbound.get(), 2)).error_message.id == 1003
            assert (session.player_id, session.account, session.owner_revision, session.scene_node_id) == (42, "alice", 5, 6)
            assert not exits
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            await client.close()

            async def forward(raw, context):
                incoming = protocol.parse("client_rpc_router.ForwardRequest", raw).request
                calls.append(incoming.id)
                await context.send_initial_metadata((("x-session-detail-bin", dict(context.invocation_metadata())["x-session-detail-bin"]),))
                return protocol.content(incoming.message_id, incoming.id).SerializeToString()
            server = grpc.aio.server()
            server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("client_rpc_router.ClientRpcRouter", {
                "Forward": grpc.unary_unary_rpc_method_handler(forward),
            }),))
            port = server.add_insecure_port("127.0.0.1:0")
            await server.start()
            client = GrpcClient(f"127.0.0.1:{port}", timeout=2, enable_retries=False)
            gate.forward = RouterForwarder(protocol, client, b"x" * 32)
            try:
                gate._admit_router(session, leave, gate_module.time.monotonic())
                worker = asyncio.create_task(gate._lane(session))
                assert not (await asyncio.wait_for(session.outbound.get(), 3)).error_message.id
                assert calls == [1] and exits == [(6, 42)]
                assert (session.player_id, session.account, session.scene_node_id) == (0, "", 0)
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            finally:
                await client.close()
                await server.stop(0)
        finally:
            if not worker.done():
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            await client.close()

    asyncio.run(run())


def test_leave_router_received_request_then_lost_reply_does_not_restore_session():
    async def run():
        protocol, gate, session = setup(player_id=42, account="alice", owner_revision=5, logged_in=True)
        seen = []
        async def forward(raw, context):
            seen.append(protocol.parse("client_rpc_router.ForwardRequest", raw).request.id)
            await context.abort(grpc.StatusCode.UNAVAILABLE, "response lost after dispatch")
        server = grpc.aio.server()
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("client_rpc_router.ClientRpcRouter", {
            "Forward": grpc.unary_unary_rpc_method_handler(forward),
        }),))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        client = GrpcClient(f"127.0.0.1:{port}", timeout=2, enable_retries=False)
        gate.forward = RouterForwarder(protocol, client, b"x" * 32)
        try:
            reply = await gate._dispatch(session, 1, request(protocol, "ClientPlayerLoginLeaveGame", 1))
            assert reply.error_message.id == 1003 and seen == [1]
            assert (session.player_id, session.account, session.owner_revision) == (0, "", 0)
        finally:
            await client.close()
            await server.stop(0)

    asyncio.run(run())
