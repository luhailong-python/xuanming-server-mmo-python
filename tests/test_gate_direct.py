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
