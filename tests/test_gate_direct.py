import asyncio
import base64
import hashlib
import time

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.cluster import EtcdDirectory, LocalDirectory
from xuanming.nodes.gate import GateNode, Session
from xuanming.nodes.gate_direct import DirectForwarder
from xuanming.protocol import Protocol
from xuanming.transport import Packet, read_packet, write_packet

SECRET = b"x" * 32


def node(protocol, *, kind, zone=1, number=5, port=1234, track="", grpc_type=1):
    return protocol.message(
        "NodeInfo", node_type=kind, zone_id=zone, node_id=number, protocol_type=grpc_type,
        node_uuid=f"{kind}-{zone}-{number}", release_track=track,
        grpc_endpoint={"ip": "127.0.0.1", "port": port},
    )


def request(protocol, name, call=19, body=b""):
    return protocol.message("ClientRequest", message_id=protocol.message_ids[name], id=call, body=body)


def gate_session(protocol, forward, *, track="stable"):
    gate = GateNode(protocol, node(protocol, kind=4, number=7, track=track), SECRET, forward)
    session = Session(917505, 91, None)
    gate.sessions[session.session_id] = session
    return gate, session


def test_typed_direct_login_auth_and_reply_without_echo():
    async def exercise():
        protocol = Protocol()
        directory = LocalDirectory()
        forward = DirectForwarder(protocol, directory, SECRET, discovery_ttl=0)
        gate, session = gate_session(protocol, forward)
        service = "loginpb.ClientPlayerLogin"
        calls = []

        async def login(raw, context):
            calls.append(raw)
            typed = protocol.parse("loginpb.LoginRequest", raw)
            # gRPC decodes one binary metadata layer; Go verifies the application layer.
            metadata = dict(context.invocation_metadata())
            subject = base64.b64decode(metadata["x-session-detail-bin"], validate=True)
            details = protocol.parse("SessionDetails", subject)
            assert (details.session_id, details.session_incarnation, details.gate_call_seq) == (917505, 91, 0)
            assert (details.gate_node_id, details.gate_instance_id) == (7, "4-1-7")
            assert metadata["x-caller-id"] == "gate"
            assert abs(int(metadata["x-caller-ts"]) - time.time_ns() // 1_000_000) < 3000
            canonical = "\n".join((
                "mmorpg-gate-identity/v2", "gate", f"/{service}/Login",
                hashlib.sha256(subject).hexdigest(), metadata["x-caller-ts"], metadata["x-caller-nonce"],
            )).encode()
            assert len(metadata["x-caller-nonce"]) == 32
            assert len(metadata["x-caller-sig"]) == 128
            Ed25519PrivateKey.from_private_bytes(SECRET).public_key().verify(
                bytes.fromhex(metadata["x-caller-sig"]), canonical
            )
            if typed.account == "refused":
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "native error")
            assert typed.account == "native"
            return protocol.message("loginpb.LoginResponse", access_token="ok").SerializeToString()

        async def disconnect(raw, context):
            assert protocol.parse("loginpb.LoginNodeDisconnectRequest", raw).session_id == session.session_id
            calls.append(raw)
            return b""

        server = grpc.aio.server()
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(service, {
            "Login": grpc.unary_unary_rpc_method_handler(login),
            "Disconnect": grpc.unary_unary_rpc_method_handler(disconnect),
        }),))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            login_request = "ClientPlayerLoginLogin"
            bad = request(protocol, login_request, body=b"\x0a\xff")
            reply = await gate._dispatch(session, 1, bad)
            assert (reply.id, reply.message_id, reply.error_message.id) == (bad.id, bad.message_id, 1014)
            assert not session.login_started

            # A foreign Login cannot serve this Gate. A same-zone one ignores the track.
            directory.publish(node(protocol, kind=5, zone=2, port=port))
            good = request(protocol, login_request, body=b"\x0a\x03old\x0a\x06native")
            assert (await gate._dispatch(session, 2, good)).error_message.id == 1003
            assert not session.login_started and not calls
            directory.publish(node(protocol, kind=5, number=0, port=port, track="canary"))
            reply = await gate._dispatch(session, 3, good)
            assert (reply.id, reply.message_id, reply.error_message.id) == (good.id, good.message_id, 0)
            assert protocol.parse("loginpb.LoginResponse", reply.serialized_message).access_token == "ok"
            assert session.logged_in and session.login_started
            assert calls == [b"\x0a\x06native"]

            refused = request(protocol, login_request, call=20, body=b"\x0a\x07refused")
            reply = await gate._dispatch(session, 4, refused)
            assert (reply.id, reply.message_id, reply.error_message.id) == (refused.id, refused.message_id, 1003)
            assert session.logged_in and calls.count(refused.body) == 1

            notice = request(protocol, "ClientPlayerLoginDisconnect", call=0, body=protocol.message(
                "loginpb.LoginNodeDisconnectRequest", session_id=session.session_id,
            ).SerializeToString())
            assert await forward(gate, session, protocol.routes[notice.message_id], notice, 0) is None
            assert calls[-1] == notice.body
        finally:
            await forward.close()
            await server.stop(0)

    asyncio.run(exercise())


def test_direct_track_zone_and_single_attempt():
    async def exercise():
        protocol = Protocol()
        directory = LocalDirectory()
        calls = []

        class Client:
            def __init__(self, address, *, timeout, enable_retries):
                assert enable_retries is False
                self.address, self.timeout = address, timeout

            async def call(self, path, body, *, metadata):
                calls.append((self.address, path, self.timeout))
                if self.address.endswith(":2222"):
                    raise OSError("connection lost")
                return b""

            async def close(self):
                pass

        forward = DirectForwarder(protocol, directory, SECRET, client_factory=Client, discovery_ttl=0)
        stable, session = gate_session(protocol, forward)
        canary, canary_session = gate_session(protocol, forward, track="canary")
        queue = request(protocol, "MatchServiceJoinQueue")
        cancel = request(protocol, "MatchServiceCancelQueue", call=20)
        chat = request(protocol, "ClientPlayerChatSendChat")
        team = request(protocol, "ClientPlayerTeamCreateTeam")
        try:
            directory.publish(node(protocol, kind=16, zone=8, number=0, port=1111))
            directory.publish(node(protocol, kind=16, zone=8, number=6, port=2222, track="canary"))
            directory.publish(node(protocol, kind=16, zone=8, number=7, port=3333, track="CANARY"))
            directory.publish(node(protocol, kind=16, zone=8, number=9, port=8888, grpc_type=0))
            directory.publish(node(protocol, kind=9, zone=1, port=5555))
            directory.publish(node(protocol, kind=10, zone=1, port=1111))

            assert not (await stable._dispatch(session, 1, queue)).error_message.id
            assert (await canary._dispatch(canary_session, 1, queue)).error_message.id == 1003
            # An unavailable write on the canary was attempted once. Removing that
            # instance permits subsequent canary requests to pick a stable peer.
            del directory.nodes[16, 8, 6]
            assert not (await canary._dispatch(canary_session, 2, queue)).error_message.id
            assert (await stable._dispatch(session, 2, chat)).error_message.id == 1003
            assert (await stable._dispatch(session, 3, team)).error_message.id == 1003
            # Go CancelQueue returns Empty but remains a two-way client request.
            result = await stable._dispatch(session, 4, cancel)
            assert (result.id, result.message_id) == (cancel.id, cancel.message_id)
            assert not result.error_message.id

            assert calls == [
                ("127.0.0.1:1111", "/match.MatchService/JoinQueue", 7),
                ("127.0.0.1:2222", "/match.MatchService/JoinQueue", 7),
                ("127.0.0.1:1111", "/match.MatchService/JoinQueue", 7),
                ("127.0.0.1:1111", "/match.MatchService/CancelQueue", 7),
            ]
        finally:
            await forward.close()

    asyncio.run(exercise())


def test_etcd_direct_ranges_require_matching_registration_key():
    async def exercise():
        protocol = Protocol()
        directory = EtcdDirectory(protocol, "127.0.0.1:1")
        first = node(protocol, kind=16, number=0)
        second = node(protocol, kind=16, zone=2)
        ranges = []

        async def scan(service, method, **fields):
            assert (service, method) == ("KV", "Range")
            ranges.append((fields["key"], fields["range_end"]))
            return protocol.message("etcdserverpb.RangeResponse", kvs=[
                {"key": directory._service_key(first), "value": directory._serialized(first)},
                {"key": directory._service_key(second), "value": directory._serialized(second)},
                {"key": directory._service_key(first) + b"9", "value": directory._serialized(first)},
                {"key": directory._service_key(first) + b"8", "value": b"invalid-json"},
            ])

        directory._rpc = scan
        try:
            assert await directory.candidates(16, None) == [first, second]
            assert await directory.candidates(16, 1) == [first]
            assert ranges == [
                (b"MatchNodeService.rpc/zone/", b"MatchNodeService.rpc/zone0"),
                (b"MatchNodeService.rpc/zone/1/node_type/16/node_id/",
                 b"MatchNodeService.rpc/zone/1/node_type/16/node_id0"),
            ]
        finally:
            await directory.close()

    asyncio.run(exercise())


def test_discovery_retires_removed_peer_after_in_flight_reply():
    async def exercise():
        protocol = Protocol()
        directory = LocalDirectory()
        started, finish = asyncio.Event(), asyncio.Event()
        closed = []

        class Client:
            def __init__(self, address, *, timeout, enable_retries):
                self.address = address

            async def call(self, path, body, *, metadata):
                if self.address.endswith(":1111"):
                    started.set()
                    await finish.wait()
                return b""

            async def close(self):
                closed.append(self.address)

        forward = DirectForwarder(protocol, directory, SECRET, client_factory=Client, discovery_ttl=0)
        gate, session = gate_session(protocol, forward)
        second = Session(917506, 92, None)
        gate.sessions[second.session_id] = second
        incoming = request(protocol, "MatchServiceGetQueueStatus")
        directory.publish(node(protocol, kind=16, port=1111))
        in_flight = asyncio.create_task(gate._dispatch(session, 1, incoming))
        try:
            await asyncio.wait_for(started.wait(), timeout=1)
            directory.publish(node(protocol, kind=16, port=2222))
            assert not (await gate._dispatch(second, 1, incoming)).error_message.id
            assert not closed
            finish.set()
            assert not (await in_flight).error_message.id
            assert closed == ["127.0.0.1:1111"]
        finally:
            finish.set()
            await asyncio.gather(in_flight, return_exceptions=True)
            await forward.close()
        assert closed == ["127.0.0.1:1111", "127.0.0.1:2222"]

    asyncio.run(exercise())


def test_direct_calls_are_concurrent_and_keep_receive_time_identity():
    class Writer:
        def is_closing(self): return False
        def close(self): pass

    async def exercise():
        protocol = Protocol()
        started, finish = asyncio.Event(), asyncio.Event()
        seen = []

        class Forward:
            direct_mode = True

            def validate_request(self, route, frame):
                return protocol.parse(route.request_type, frame.body)

            async def __call__(self, gate, session, route, frame, seq, *, details, on_send):
                seen.append((frame.id, details.player_id, details.authenticated_account, details.owner_revision))
                if len(seen) == 2:
                    started.set()
                await finish.wait()
                return protocol.content(frame.message_id, frame.id)

        gate, session = gate_session(protocol, Forward())
        session.writer = Writer()
        session.verified = True
        session.player_id, session.account, session.owner_revision = 41, "first", 1
        a = request(protocol, "MatchServiceJoinQueue", call=1)
        b = request(protocol, "MatchServiceCancelQueue", call=2)
        gate._admit_direct(session, a, protocol.routes[a.message_id])
        session.player_id, session.account, session.owner_revision = 42, "second", 2
        gate._admit_direct(session, b, protocol.routes[b.message_id])
        try:
            await asyncio.wait_for(started.wait(), 1)
            assert sorted(seen) == [(1, 41, "first", 1), (2, 42, "second", 2)]
            assert session.requests.empty() and len(session.direct_tasks) == 2
            finish.set()
            assert {(await asyncio.wait_for(session.outbound.get(), 1)).id,
                    (await asyncio.wait_for(session.outbound.get(), 1)).id} == {1, 2}
        finally:
            finish.set()
            await asyncio.gather(*tuple(session.direct_tasks), return_exceptions=True)

    asyncio.run(exercise())


def test_direct_pending_login_does_not_admit_early_enter_and_fences_old_completion():
    class Writer:
        def is_closing(self): return False
        def close(self): pass

    async def exercise():
        protocol = Protocol()
        started, finish = asyncio.Event(), asyncio.Event()

        class Forward:
            direct_mode = True
            def validate_request(self, route, frame): return protocol.parse(route.request_type, frame.body)
            async def __call__(self, gate, session, route, frame, seq, *, details, on_send):
                started.set()
                await finish.wait()
                return protocol.content(frame.message_id, frame.id, body=protocol.message(
                    "loginpb.LoginResponse").SerializeToString())

        gate, session = gate_session(protocol, Forward())
        session.writer, session.verified = Writer(), True
        login = request(protocol, "ClientPlayerLoginLogin", call=1)
        gate._admit_direct(session, login, protocol.routes[login.message_id])
        enter = request(protocol, "ClientPlayerLoginEnterGame", call=2,
                        body=protocol.message("loginpb.EnterGameRequest", player_id=42).SerializeToString())
        gate._admit_direct(session, enter, protocol.routes[enter.message_id])
        error = session.outbound.get_nowait()
        assert (error.id, error.error_message.id) == (2, 2028)
        await asyncio.wait_for(started.wait(), 1)
        replacement = Session(session.session_id, session.incarnation + 1, Writer(), verified=True)
        gate.sessions[session.session_id] = replacement
        finish.set()
        await asyncio.gather(*tuple(session.direct_tasks))
        assert not session.logged_in and not replacement.logged_in and replacement.outbound.empty()

    asyncio.run(exercise())


def test_direct_enter_game_parse_and_bound_role_fences():
    class Writer:
        def is_closing(self): return False
        def close(self): pass

    async def exercise():
        protocol = Protocol()

        class Forward:
            direct_mode = True
            def validate_request(self, route, frame): return protocol.parse(route.request_type, frame.body)
            async def __call__(self, gate, session, route, frame, seq, *, details, on_send):
                raise AssertionError("rejected input must not be forwarded")

        gate, session = gate_session(protocol, Forward())
        session.writer, session.verified, session.logged_in, session.player_id = Writer(), True, True, 42
        malformed = request(protocol, "ClientPlayerLoginEnterGame", call=1, body=b"\x08\x80")
        gate._admit_direct(session, malformed, protocol.routes[malformed.message_id])
        assert session.outbound.get_nowait().error_message.id == 1014 and not session.enter_requested
        switched = request(protocol, "ClientPlayerLoginEnterGame", call=2,
                           body=protocol.message("loginpb.EnterGameRequest", player_id=43).SerializeToString())
        gate._admit_direct(session, switched, protocol.routes[switched.message_id])
        assert session.outbound.get_nowait().error_message.id == 2005 and not session.enter_requested
        session.player_id = 0
        gate._admit_direct(session, malformed, protocol.routes[malformed.message_id])
        assert session.outbound.get_nowait().error_message.id == 1014 and session.enter_requested
        assert not session.direct_tasks

    asyncio.run(exercise())


def test_direct_leave_resets_before_concurrent_dispatch_and_captures_scene_route():
    class Writer:
        def is_closing(self): return False
        def close(self): pass

    async def exercise():
        protocol = Protocol()
        started, finish = asyncio.Event(), asyncio.Event()
        seen, exits = [], []

        class Scene:
            async def call(self, node, mid, payload, *, expected_uuid):
                exits.append((node, expected_uuid, payload.player_id,
                              protocol.parse("GameNodeExitGameRequest", payload.message_content.serialized_message).reason))
                return protocol.message("ProcessClientPlayerMessageResponse").SerializeToString()

        class Forward:
            direct_mode = True
            def validate_request(self, route, frame): return protocol.parse(route.request_type, frame.body)
            async def __call__(self, gate, session, route, frame, seq, *, details, on_send):
                seen.append((route.method, details.player_id, details.authenticated_account, details.owner_revision))
                if len(seen) == 2: started.set()
                await finish.wait()
                return protocol.content(frame.message_id, frame.id)

        gate, session = gate_session(protocol, Forward())
        gate.scene = Scene()
        session.writer, session.verified, session.logged_in = Writer(), True, True
        session.player_id, session.account, session.owner_revision = 42, "alice", 5
        session.scene_node_id, session.scene_instance_id, session.scene_id = 6, "scene-a", 9
        leave = request(protocol, "ClientPlayerLoginLeaveGame", call=1)
        gate._admit_direct(session, leave, protocol.routes[leave.message_id])
        assert (session.player_id, session.account, session.scene_node_id) == (0, "", 0)
        create = request(protocol, "ClientPlayerLoginCreatePlayer", call=2)
        gate._admit_direct(session, create, protocol.routes[create.message_id])
        try:
            await asyncio.wait_for(started.wait(), 1)
            assert sorted(seen) == [("CreatePlayer", 42, "", 0), ("LeaveGame", 42, "", 0)]
            assert exits == [(6, "scene-a", 42, 1)]
        finally:
            finish.set()
            await asyncio.gather(*tuple(session.direct_tasks), return_exceptions=True)
            await asyncio.gather(*tuple(gate._logout_exits), return_exceptions=True)

    asyncio.run(exercise())


def test_direct_metadata_retains_role_until_new_binding_after_leave():
    protocol = Protocol()
    forward = DirectForwarder(protocol, LocalDirectory(), SECRET)
    gate, session = gate_session(protocol, forward)
    session.left_player_id = 42
    details = forward._details(gate, session)
    assert (details.player_id, details.authenticated_account, details.owner_revision) == (42, "", 0)
    session.player_id, session.account, session.owner_revision = 43, "new", 7
    details = forward._details(gate, session)
    assert (details.player_id, details.authenticated_account, details.owner_revision) == (43, "new", 7)


def test_direct_logout_finishes_before_same_session_reentry_over_real_rpc0(tmp_path):
    from test_scene_enter_fence import _edge

    class Writer:
        closed = False
        def is_closing(self): return self.closed
        def close(self): self.closed = True

    async def exercise():
        async with _edge(tmp_path) as (protocol, scene, links, enter):
            sid = (5 << 17) | 1
            await enter(sid, 8)
            started, release = asyncio.Event(), asyncio.Event()

            class DelayedLinks:
                def __getattr__(self, name): return getattr(links, name)
                async def call(self, *args, **kwargs):
                    started.set()
                    await release.wait()
                    return await links.call(*args, **kwargs)

            info = protocol.message("NodeInfo", node_id=5, zone_id=1, node_uuid="enter-gate")
            gate = GateNode(protocol, info, SECRET, None)
            gate.scene = DelayedLinks()
            session = Session(sid, 1, Writer(), verified=True, logged_in=True, player_id=1,
                              login_generation=8, scene_node_id=6, scene_id=1,
                              scene_instance_id="enter-scene")
            gate.sessions[sid] = session
            gate._direct_leave(session)
            await asyncio.wait_for(started.wait(), 2)
            session.enter_requested, session.enter_player_id = True, 1
            session.player_id, session.login_generation, session.owner_revision = 1, 9, 1
            session.account, session.enter_type = "alice", 1
            event = protocol.message("contracts.kafka.RoutePlayerEvent", session_id=sid, player_id=1,
                                     scene_id=1, target_node_id=6, owner_revision=1,
                                     owner_epoch=1, login_generation=9)
            await gate.route(event, 0, background=True)
            entry = session.pending_route.task
            try:
                await asyncio.sleep(0.05)
                assert not entry.done() and scene.sessions[("enter-gate", sid)].generation == 8
                release.set()
                await asyncio.wait_for(entry, 5)
                assert scene.sessions[("enter-gate", sid)].generation == 9
                assert session.scene_node_id == 6 and not session.writer.closed
            finally:
                release.set()
                await asyncio.gather(*tuple(gate._logout_exits), return_exceptions=True)
                if not entry.done():
                    entry.cancel()
                    await asyncio.gather(entry, return_exceptions=True)

    asyncio.run(exercise())


def test_direct_uncertain_logout_closes_session_before_reentry():
    class Writer:
        closed = False
        def is_closing(self): return self.closed
        def close(self): self.closed = True

    async def exercise():
        protocol = Protocol()
        gate, session = gate_session(protocol, None)
        session.writer, session.verified, session.player_id = Writer(), True, 42
        session.scene_node_id, session.scene_instance_id = 6, "scene-a"
        class Scene:
            async def call(self, *args, **kwargs):
                raise TimeoutError("result unknown after write")
        gate.scene = Scene()
        gate._direct_leave(session)
        await asyncio.gather(*tuple(gate._logout_exits))
        assert session.entry_failed and session.writer.closed

    asyncio.run(exercise())


def test_direct_slow_discovery_signs_original_identity():
    async def exercise():
        protocol = Protocol()
        started, release = asyncio.Event(), asyncio.Event()
        seen = []

        class Directory:
            async def candidates(self, kind, zone):
                started.set()
                await release.wait()
                return [node(protocol, kind=16)]

        class Client:
            def __init__(self, *_args, **_kwargs): pass
            async def call(self, path, body, *, metadata):
                seen.append(protocol.parse("SessionDetails", base64.b64decode(dict(metadata)["x-session-detail-bin"])))
                return b""
            async def close(self): pass

        forward = DirectForwarder(protocol, Directory(), SECRET, client_factory=Client)
        gate, session = gate_session(protocol, forward)
        session.player_id, session.account, session.owner_revision = 41, "first", 1
        frame = request(protocol, "MatchServiceJoinQueue")
        task = asyncio.create_task(forward(gate, session, protocol.routes[frame.message_id], frame, 0))
        try:
            await asyncio.wait_for(started.wait(), 1)
            session.player_id, session.account, session.owner_revision = 42, "second", 2
            release.set()
            assert not (await task).error_message.id
            assert (seen[0].player_id, seen[0].authenticated_account, seen[0].owner_revision) == (41, "first", 1)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            await forward.close()

    asyncio.run(exercise())


def test_direct_receive_bypasses_router_lane_even_after_redirect(monkeypatch):
    from xuanming.nodes import gate as gate_module

    class Writer:
        def is_closing(self): return False
        def close(self): pass

    async def exercise():
        protocol = Protocol()
        finish = asyncio.Event()
        seen = []

        class Forward:
            direct_mode = True
            def validate_request(self, route, frame): return protocol.parse(route.request_type, frame.body)
            async def __call__(self, gate, session, route, frame, seq, *, details, on_send):
                seen.append(frame.id)
                await finish.wait()
                return protocol.content(frame.message_id, frame.id)

        gate, session = gate_session(protocol, Forward())
        session.writer, session.verified, session.redirected = Writer(), True, True
        packets = iter((Packet.from_message(request(protocol, "MatchServiceJoinQueue", call=1)),
                        Packet.from_message(request(protocol, "MatchServiceCancelQueue", call=2)), None))

        async def read(_reader): return next(packets)
        monkeypatch.setattr(gate_module, "read_packet", read)
        await gate._receive(None, session)
        try:
            async with asyncio.timeout(1):
                while len(seen) != 2:
                    await asyncio.sleep(0)
            assert session.requests.empty() and session.outbound.empty()
        finally:
            finish.set()
            await asyncio.gather(*tuple(session.direct_tasks), return_exceptions=True)

    asyncio.run(exercise())


def test_direct_tcp_disconnect_cancels_pending_calls():
    async def exercise():
        protocol = Protocol()
        started, cancelled = asyncio.Event(), asyncio.Event()

        class Forward:
            direct_mode = True
            def validate_request(self, route, frame): return protocol.parse(route.request_type, frame.body)
            async def __call__(self, gate, session, route, frame, seq, *, details, on_send):
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        gate = GateNode(protocol, node(protocol, kind=4, number=7), SECRET, Forward())
        server = await asyncio.start_server(gate.handle_client, "127.0.0.1", 0)
        try:
            host, port = server.sockets[0].getsockname()[:2]
            reader, writer = await asyncio.open_connection(host, port)
            await write_packet(writer, Packet.from_message(gate.issue_ticket()))
            assert protocol.parse("ClientTokenVerifyResponse", (await read_packet(reader)).payload).success
            await write_packet(writer, Packet.from_message(request(protocol, "MatchServiceJoinQueue")))
            await asyncio.wait_for(started.wait(), 2)
            writer.close()
            await writer.wait_closed()
            await asyncio.wait_for(cancelled.wait(), 2)
            async with asyncio.timeout(2):
                while gate.sessions:
                    await asyncio.sleep(0)
        finally:
            server.close()
            await server.wait_closed()
            await gate.close()

    asyncio.run(exercise())


def test_direct_tcp_disconnect_does_not_cancel_an_already_sent_grpc_call():
    async def exercise():
        protocol, directory = Protocol(), LocalDirectory()
        started, finish, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        cancelled = []

        async def join(raw, context):
            started.set()
            try:
                await finish.wait()
                completed.set()
                return protocol.message("match.JoinQueueResponse").SerializeToString()
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        grpc_server = grpc.aio.server()
        grpc_server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("match.MatchService", {
            "JoinQueue": grpc.unary_unary_rpc_method_handler(join),
        }),))
        grpc_port = grpc_server.add_insecure_port("127.0.0.1:0")
        await grpc_server.start()
        directory.publish(node(protocol, kind=16, port=grpc_port))
        forward = DirectForwarder(protocol, directory, SECRET)
        gate = GateNode(protocol, node(protocol, kind=4, number=7), SECRET, forward)
        server = await asyncio.start_server(gate.handle_client, "127.0.0.1", 0)
        writer = None
        try:
            host, port = server.sockets[0].getsockname()[:2]
            reader, writer = await asyncio.open_connection(host, port)
            await write_packet(writer, Packet.from_message(gate.issue_ticket()))
            assert protocol.parse("ClientTokenVerifyResponse", (await read_packet(reader)).payload).success
            await write_packet(writer, Packet.from_message(request(protocol, "MatchServiceJoinQueue")))
            await asyncio.wait_for(started.wait(), 2)
            writer.close()
            await writer.wait_closed()
            async with asyncio.timeout(2):
                while gate.sessions:
                    await asyncio.sleep(0)
            assert gate._detached_direct and not cancelled
            finish.set()
            await asyncio.wait_for(completed.wait(), 2)
            async with asyncio.timeout(2):
                while gate._detached_direct:
                    await asyncio.sleep(0)
            assert not cancelled
        finally:
            finish.set()
            if writer is not None:
                writer.close()
                await writer.wait_closed()
            server.close()
            await server.wait_closed()
            await gate.close()
            await forward.close()
            await grpc_server.stop(0)

    asyncio.run(exercise())


def test_direct_tcp_dispatches_real_grpc_calls_without_waiting_for_each_other():
    async def exercise():
        protocol, directory = Protocol(), LocalDirectory()
        login_started, release_login = asyncio.Event(), asyncio.Event()
        join_started, cancel_started, release_match = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def login(raw, context):
            login_started.set()
            await release_login.wait()
            return protocol.message("loginpb.LoginResponse", access_token="ok").SerializeToString()

        async def disconnect(raw, context): return b""

        async def join(raw, context):
            join_started.set()
            await release_match.wait()
            return protocol.message("match.JoinQueueResponse").SerializeToString()

        async def cancel(raw, context):
            cancel_started.set()
            await release_match.wait()
            return b""

        grpc_server = grpc.aio.server()
        grpc_server.add_generic_rpc_handlers((
            grpc.method_handlers_generic_handler("loginpb.ClientPlayerLogin", {
                "Login": grpc.unary_unary_rpc_method_handler(login),
                "Disconnect": grpc.unary_unary_rpc_method_handler(disconnect),
            }),
            grpc.method_handlers_generic_handler("match.MatchService", {
                "JoinQueue": grpc.unary_unary_rpc_method_handler(join),
                "CancelQueue": grpc.unary_unary_rpc_method_handler(cancel),
            }),
        ))
        port = grpc_server.add_insecure_port("127.0.0.1:0")
        await grpc_server.start()
        directory.publish(node(protocol, kind=5, port=port))
        directory.publish(node(protocol, kind=16, port=port))
        forward = DirectForwarder(protocol, directory, SECRET)
        gate = GateNode(protocol, node(protocol, kind=4, number=7), SECRET, forward)
        server = await asyncio.start_server(gate.handle_client, "127.0.0.1", 0)
        writer = None
        try:
            host, gate_port = server.sockets[0].getsockname()[:2]
            reader, writer = await asyncio.open_connection(host, gate_port)
            await write_packet(writer, Packet.from_message(gate.issue_ticket()))
            assert protocol.parse("ClientTokenVerifyResponse", (await read_packet(reader)).payload).success
            await write_packet(writer, Packet.from_message(request(protocol, "ClientPlayerLoginLogin", call=1)))
            await asyncio.wait_for(login_started.wait(), 2)
            enter = request(protocol, "ClientPlayerLoginEnterGame", call=2,
                            body=protocol.message("loginpb.EnterGameRequest", player_id=42).SerializeToString())
            await write_packet(writer, Packet.from_message(enter))
            rejected = protocol.parse("MessageContent", (await asyncio.wait_for(read_packet(reader), 2)).payload)
            assert (rejected.id, rejected.error_message.id) == (2, 2028)
            release_login.set()
            accepted = protocol.parse("MessageContent", (await asyncio.wait_for(read_packet(reader), 2)).payload)
            assert accepted.id == 1 and not accepted.error_message.id
            await write_packet(writer, Packet.from_message(request(protocol, "MatchServiceJoinQueue", call=3)))
            await asyncio.wait_for(join_started.wait(), 2)
            await write_packet(writer, Packet.from_message(request(protocol, "MatchServiceCancelQueue", call=4)))
            await asyncio.wait_for(cancel_started.wait(), 2)
            release_match.set()
            replies = [protocol.parse("MessageContent", (await asyncio.wait_for(read_packet(reader), 2)).payload)
                       for _ in range(2)]
            assert {item.id for item in replies} == {3, 4}
            assert all(not item.error_message.id for item in replies)
        finally:
            release_login.set()
            release_match.set()
            if writer is not None:
                writer.close()
                await writer.wait_closed()
            server.close()
            await server.wait_closed()
            await gate.close()
            await forward.close()
            await grpc_server.stop(0)

    asyncio.run(exercise())


def test_direct_forwarder_retains_healthy_peers_on_transient_discovery_timeout():
    async def exercise():
        protocol = Protocol()
        fail = False
        calls = []

        class Directory:
            async def candidates(self, kind, zone):
                if fail:
                    raise TimeoutError("etcd timeout")
                return [node(protocol, kind=16, port=1111)]

        class Client:
            def __init__(self, address, **_kwargs):
                self.address = address

            async def call(self, path, body, *, metadata):
                calls.append(self.address)
                return b""

            async def close(self):
                pass

        forward = DirectForwarder(protocol, Directory(), SECRET, client_factory=Client, discovery_ttl=0)
        gate, session = gate_session(protocol, forward)
        queue = request(protocol, "MatchServiceJoinQueue")
        try:
            assert not (await gate._dispatch(session, 1, queue)).error_message.id
            fail = True
            assert not (await gate._dispatch(session, 2, queue)).error_message.id
            assert calls == ["127.0.0.1:1111", "127.0.0.1:1111"]
        finally:
            await forward.close()

    asyncio.run(exercise())
