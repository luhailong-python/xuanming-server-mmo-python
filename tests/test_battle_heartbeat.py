import asyncio
from types import SimpleNamespace

from xuanming.nodes.battle_tcp import BattleClients
from xuanming.protocol import Protocol
from xuanming.transport import Packet, decode_packet, encode_packet, read_packet, write_packet


class BattleStub:
    max_connections = 10
    tables = None

    def __init__(self):
        self.connection = None

    def authorize(self, payload, signature):
        return SimpleNamespace(battle_id=42)

    def attach(self, ticket, emit, close, *, is_live):
        self.connection = close

    def owns_connection(self, ticket, close):
        return self.connection is close

    def on_verified(self, ticket):
        pass

    def detach(self, ticket, close):
        if self.connection is close:
            self.connection = None


def test_tcp_heartbeat_rate_limit_and_duplicate_verify():
    async def exercise():
        protocol = Protocol()
        battle = BattleStub()
        clients = BattleClients(protocol, battle)
        clients._illegal_threshold = 2
        server = await asyncio.start_server(clients.serve, "127.0.0.1", 0)
        async with server:
            reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
            verify = protocol.message("BattleTokenVerifyRequest")
            await write_packet(writer, Packet.from_message(verify))
            packet = await read_packet(reader)
            response = protocol.parse(packet.type_name, packet.payload)
            assert response.success and response.battle_id == 42
            assert (response.heartbeat_interval_ms, response.heartbeat_timeout_ms) == (10_000, 30_000)
            for seq in range(4):
                await write_packet(writer, Packet.from_message(protocol.message("ClientHeartbeatRequest", seq=seq)))
            await write_packet(writer, Packet.from_message(verify))
            for seq in range(3):
                packet = await asyncio.wait_for(read_packet(reader), 1)
                assert packet.type_name == "ClientHeartbeatResponse"
                assert protocol.parse(packet.type_name, packet.payload).seq == seq
            packet = await asyncio.wait_for(read_packet(reader), 1)
            response = protocol.parse(packet.type_name, packet.payload)
            assert packet.type_name == "BattleTokenVerifyResponse"
            assert (response.heartbeat_interval_ms, response.heartbeat_timeout_ms) == (10_000, 30_000)
            await write_packet(writer, Packet.from_message(protocol.message("ClientHeartbeatRequest", seq=4)))
            assert await asyncio.wait_for(read_packet(reader), 1) is None
            writer.close()
            await writer.wait_closed()
        await clients.close()
        assert battle.connection is None

    asyncio.run(exercise())


def test_idle_timeout_resets_on_inbound_frame(monkeypatch):
    import xuanming.nodes.battle_tcp as battle_tcp

    async def exercise():
        monkeypatch.setattr(battle_tcp, "HEARTBEAT_TIMEOUT", 0.2)
        protocol = Protocol()
        battle = BattleStub()
        clients = BattleClients(protocol, battle)
        server = await asyncio.start_server(clients.serve, "127.0.0.1", 0)
        async with server:
            reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
            await write_packet(writer, Packet.from_message(protocol.message("BattleTokenVerifyRequest")))
            assert (await read_packet(reader)).type_name == "BattleTokenVerifyResponse"
            for seq in range(2):
                await asyncio.sleep(0.12)
                await write_packet(writer, Packet.from_message(protocol.message("ClientHeartbeatRequest", seq=seq)))
                packet = await asyncio.wait_for(read_packet(reader), 1)
                assert protocol.parse(packet.type_name, packet.payload).seq == seq
            assert await asyncio.wait_for(read_packet(reader), 1) is None
            writer.close()
            await writer.wait_closed()
        await clients.close()
        assert battle.connection is None

    asyncio.run(exercise())


def test_websocket_heartbeat_uses_the_same_wire_protocol():
    async def exercise():
        from websockets.asyncio.client import connect
        from websockets.asyncio.server import serve

        protocol = Protocol()
        battle = BattleStub()
        clients = BattleClients(protocol, battle)
        async with serve(clients.serve_websocket, "127.0.0.1", 0, compression=None) as server:
            port = server.sockets[0].getsockname()[1]
            async with connect(f"ws://127.0.0.1:{port}", compression=None) as websocket:
                await websocket.send(encode_packet(Packet.from_message(protocol.message("BattleTokenVerifyRequest"))))
                packet = decode_packet(await websocket.recv())
                assert protocol.parse(packet.type_name, packet.payload).heartbeat_timeout_ms == 30_000
                await websocket.send(encode_packet(Packet.from_message(protocol.message("ClientHeartbeatRequest", seq=123))))
                packet = decode_packet(await websocket.recv())
                assert packet.type_name == "ClientHeartbeatResponse"
                assert protocol.parse(packet.type_name, packet.payload).seq == 123
        await clients.close()
        assert battle.connection is None

    asyncio.run(exercise())
