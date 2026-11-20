import asyncio
import base64
from types import SimpleNamespace

from xuanming.nodes.gate import GateNode, RouterForwarder, Session
from xuanming.nodes.gate_login_renewal import LoginRenewal, RENEW_ID, RENEW_PATH, next_interval
from xuanming.protocol import Protocol


def test_renewal_retry_relogin_and_reused_session():
    async def exercise():
        now = [100.0]
        gate = SimpleNamespace(sessions={})
        entered, release = asyncio.Event(), asyncio.Event()
        replies = [OSError("offline"), b"\x08\x01\x10\x3c", b"", b"\x08\x01\x10\x3c"]

        async def sender(_, session):
            entered.set()
            await release.wait()
            result = replies.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        renewal = LoginRenewal(gate, sender, clock=lambda: now[0])
        first = SimpleNamespace(session_id=10, incarnation=1)
        gate.sessions[10] = first

        async def start():
            entered.clear()
            release.clear()
            renewal.tick()
            await entered.wait()

        async def finish():
            release.set()
            await asyncio.gather(*tuple(renewal._inflight))

        try:
            # Drive ticks explicitly to make backoff and in-flight ordering deterministic.
            renewal._runner = asyncio.current_task()
            renewal.login_succeeded(first)
            await start()
            await finish()
            assert renewal._due[0][0] == 130.0
            now[0] = 130.0
            await start()
            await finish()
            assert renewal._due[0][0] == 130.0 + next_interval(60, 10)
            now[0] = renewal._due[0][0]
            await start()
            renewal.login_succeeded(first)
            await finish()
            assert renewal._due[0][0] == now[0]
            await start()
            replacement = SimpleNamespace(session_id=10, incarnation=2)
            gate.sessions[10] = replacement
            await finish()
            assert not renewal._due
        finally:
            renewal._runner = None
            await renewal.close()

    asyncio.run(exercise())


def test_renewal_interval_clamp_and_spread():
    assert next_interval(0, 0) == 5
    assert next_interval(60, 10) == 50
    assert next_interval(1000000, 1) == 86399


def test_router_renewal_uses_internal_message_and_signed_session():
    async def exercise():
        protocol = Protocol()
        response = b"\x08\x01\x10\x3c"
        info = protocol.message("NodeInfo", node_id=1, zone_id=2, node_uuid="gate-a")
        session = Session((1 << 17) | 3, 7, None)

        class Call:
            def __init__(self, payload, metadata):
                self.payload, self.metadata = payload, metadata

            def __await__(self):
                async def result():
                    return protocol.content(RENEW_ID, body=response).SerializeToString()
                return result().__await__()

            async def initial_metadata(self):
                return (self.metadata[0],)

        class Client:
            def request(self, path, payload, *, metadata):
                request = protocol.parse("client_rpc_router.ForwardRequest", payload)
                assert request.zone_id == 2 and request.request.message_id == RENEW_ID
                assert request.request.id == 0 and request.request.body == b""
                details = protocol.parse("SessionDetails", base64.b64decode(metadata[0][1]))
                assert details.session_id == session.session_id and details.session_incarnation == 7
                return Call(payload, metadata)

        forward = RouterForwarder(protocol, Client(), b"x" * 32)
        gate = GateNode(protocol, info, b"x" * 32, forward)
        assert RENEW_PATH.endswith("/RenewSession")
        assert await forward.renew_login(gate, session) == response
        await gate.close()

    asyncio.run(exercise())
