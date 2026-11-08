import asyncio
from types import SimpleNamespace

from xuanming.nodes.local_login import LocalLogin
from xuanming.protocol import Protocol


class _Session:
    def __init__(self, sid, incarnation):
        self.session_id = sid
        self.incarnation = incarnation
        self.ticket_player_id = 0
        self.player_id = 0
        self.scene_node_id = 0
        self.version = 0
        self.closed = False

    def close(self):
        self.closed = True


class _Gate:
    def __init__(self, protocol):
        self.protocol = protocol
        self.info = SimpleNamespace(zone_id=7)
        self.sessions = {}
        self.exits = []
        self.routes = []

    async def bind(self, event):
        session = self.sessions[event.session_id]
        assert session.incarnation == event.session_incarnation
        session.player_id = event.player_id
        session.version = event.session_version

    async def route(self, event, enter_type):
        self.routes.append((event, enter_type))
        self.sessions[event.session_id].scene_node_id = event.target_node_id

    async def exit_scene(self, session, reason):
        self.exits.append((session.player_id, reason))

    def _error(self, request, error):
        return self.protocol.content(request.message_id, request.id, error=error)


def test_local_login_requires_token_and_routes_with_bound_player():
    async def exercise():
        protocol = Protocol()
        gate = _Gate(protocol)
        scene = SimpleNamespace(node_id=2, zone_id=7)
        login = LocalLogin(protocol, scene, access_token="test-dev-token")
        alice = gate.sessions[11] = _Session(11, 21)
        bob = gate.sessions[12] = _Session(12, 22)

        async def request(session, method, **fields):
            message_id = protocol.message_ids[f"ClientPlayerLogin{method}"]
            route = protocol.routes[message_id]
            body = protocol.message_class(route.request_type)(**fields).SerializeToString()
            content = await login(
                gate, session, route,
                protocol.message("ClientRequest", id=19, message_id=message_id, body=body),
                19,
            )
            return protocol.parse(route.response_type, content.serialized_message)

        reply = await request(alice, "Login", account="local", password="wrong")
        assert reply.error_message.id == 2000
        assert (await request(alice, "EnterGame", player_id=1)).error_message.id == 2004
        reply = await request(alice, "Login", account="local", password="test-dev-token")
        assert [slot.player.player_id for slot in reply.players] == [1]
        assert not reply.access_token

        reply = await request(alice, "CreatePlayer", name="Second", gender=2)
        assert len(reply.players) == 2
        assert reply.players[1].player.name == "Second"
        assert (await request(alice, "EnterGame", player_id=1)).player_id == 1
        assert len(gate.routes) == 1
        assert gate.routes[0][0].owner_epoch == 0
        assert gate.routes[0][0].scene_id == 1
        assert gate.routes[0][1] == 1

        reply = await request(bob, "Login", account="other", password="test-dev-token")
        other_id = reply.players[0].player.player_id
        assert other_id != 1
        assert (await request(bob, "EnterGame", player_id=1)).error_message.id == 2006
        assert (await request(bob, "EnterGame", player_id=other_id)).player_id == other_id

        await request(alice, "LeaveGame")
        assert gate.exits == [(1, 1)]
        assert not alice.player_id
        assert (await request(alice, "EnterGame", player_id=1)).error_message.id == 2004

    asyncio.run(exercise())
