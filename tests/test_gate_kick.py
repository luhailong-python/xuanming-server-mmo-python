import asyncio
from contextlib import suppress

from xuanming.nodes.gate import GateNode, issue_gate_ticket
from xuanming.protocol import Protocol
from xuanming.transport import Packet, decode_packet, encode_packet, read_packet, write_packet


def test_kick_identity_fence_and_notification_over_tcp():
    async def exercise():
        protocol = Protocol()

        async def forward(*args):
            pass

        info = protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate-kick")
        gate = GateNode(protocol, info, b"x" * 32, forward)
        # (bound player, bound generation, event player, event generation, kicks)
        cases = (
            (42, 4, 42, 5, True),
            (42, 5, 42, 5, False),
            (42, 6, 42, 5, False),
            (42, 4, 43, 5, False),
            (0, 0, 42, 5, False),
            ((1 << 64) - 1, 0, (1 << 64) - 1, 1, False),
            (42, 99, 0, 1, True),  # legacy event bypasses generation comparison
            (0, 0, 0, 0, True),
            (42, 99, 42, 0, True),  # legacy generation is not compared
        )
        async with await asyncio.start_server(gate.handle_client, "127.0.0.1", 0) as server:
            host, port = server.sockets[0].getsockname()[:2]
            for player, generation, target, kicker, expected in cases:
                reader, writer = await asyncio.open_connection(host, port)
                try:
                    async with asyncio.timeout(3):
                        await write_packet(writer, Packet.from_message(gate.issue_ticket()))
                        handshake = await read_packet(reader)
                        assert protocol.parse(handshake.type_name, handshake.payload).success
                        session = next(s for s in gate.sessions.values() if s.writer.get_extra_info("peername") == writer.get_extra_info("sockname"))
                        session.player_id, session.login_generation = player, generation
                        event = protocol.message("contracts.kafka.KickPlayerEvent", session_id=session.session_id,
                                                 player_id=target, login_generation=kicker)
                        command = protocol.message(
                            "contracts.kafka.GateCommand", target_gate_id=info.node_id,
                            target_instance_id=info.node_uuid,
                            event_id=protocol.event_ids["ContractsKafkaKickPlayerEvent"],
                            payload=event.SerializeToString(),
                        )
                        await gate.command(command.SerializeToString())
                        assert session.writer.is_closing() is expected
                        if expected:
                            frame = await read_packet(reader)
                            content = protocol.parse(frame.type_name, frame.payload)
                            assert content.message_id == protocol.message_ids["SceneClientPlayerCommonKickPlayer"]
                            assert protocol.parse("GameKickPlayerRequest", content.serialized_message).reason.id == 2017
                            assert await read_packet(reader) is None
                finally:
                    writer.close()
                    with suppress(ConnectionError):
                        await writer.wait_closed()
        await gate.close()

    asyncio.run(exercise())


def test_redirected_session_kick_closes_without_another_login_notice():
    async def exercise():
        protocol = Protocol()
        async def forward(*args): pass
        info = protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate-kick")
        gate = GateNode(protocol, info, b"x" * 32, forward)
        async with await asyncio.start_server(gate.handle_client, "127.0.0.1", 0) as server:
            reader, writer = await asyncio.open_connection(*server.sockets[0].getsockname()[:2])
            try:
                await write_packet(writer, Packet.from_message(gate.issue_ticket()))
                assert protocol.parse("ClientTokenVerifyResponse", (await read_packet(reader)).payload).success
                session = next(iter(gate.sessions.values()))
                session.player_id, session.login_generation, session.redirected = 42, 4, True
                event = protocol.message("contracts.kafka.KickPlayerEvent", session_id=session.session_id,
                                         player_id=42, login_generation=5)
                command = protocol.message("contracts.kafka.GateCommand", target_gate_id=info.node_id,
                                           target_instance_id=info.node_uuid,
                                           event_id=protocol.event_ids["ContractsKafkaKickPlayerEvent"],
                                           payload=event.SerializeToString())
                await gate.command(command.SerializeToString())
                assert await asyncio.wait_for(read_packet(reader), 2) is None
            finally:
                writer.close()
                with suppress(ConnectionError): await writer.wait_closed()
        await gate.close()

    asyncio.run(exercise())


def test_gate_verify_rejects_ticket_for_different_zone():
    async def exercise():
        protocol = Protocol()
        secret = b"x" * 32

        async def forward(*args):
            pass

        info = protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate-zone-1")
        gate = GateNode(protocol, info, secret, forward)
        async with await asyncio.start_server(gate.handle_client, "127.0.0.1", 0) as server:
            host, port = server.sockets[0].getsockname()[:2]
            for ticket_zone, expected in ((2, False), (0, True), (1, True)):
                reader, writer = await asyncio.open_connection(host, port)
                try:
                    ticket = issue_gate_ticket(protocol, info.node_id, ticket_zone, secret)
                    await write_packet(writer, Packet.from_message(ticket))
                    reply = await asyncio.wait_for(read_packet(reader), 2)
                    assert protocol.parse(reply.type_name, reply.payload).success is expected
                finally:
                    writer.close()
                    with suppress(ConnectionError):
                        await writer.wait_closed()
        await gate.close()

    asyncio.run(exercise())


def test_kick_flushes_notice_before_closing_websocket():
    async def exercise():
        from websockets.asyncio.client import connect
        from websockets.asyncio.server import serve
        from websockets.exceptions import ConnectionClosedOK

        protocol = Protocol()

        async def forward(*args):
            pass

        info = protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate-ws-kick")
        gate = GateNode(protocol, info, b"x" * 32, forward)
        async with serve(gate.handle_websocket, "127.0.0.1", 0, compression=None) as server:
            port = server.sockets[0].getsockname()[1]
            async with connect(f"ws://127.0.0.1:{port}", compression=None) as websocket:
                await websocket.send(encode_packet(Packet.from_message(gate.issue_ticket())))
                reply = decode_packet(await asyncio.wait_for(websocket.recv(), 2))
                assert protocol.parse(reply.type_name, reply.payload).success
                session = next(iter(gate.sessions.values()))
                session.player_id, session.login_generation = 42, 4
                event = protocol.message("contracts.kafka.KickPlayerEvent", session_id=session.session_id,
                                         player_id=42, login_generation=5)
                command = protocol.message(
                    "contracts.kafka.GateCommand", target_gate_id=info.node_id,
                    target_instance_id=info.node_uuid,
                    event_id=protocol.event_ids["ContractsKafkaKickPlayerEvent"],
                    payload=event.SerializeToString(),
                )
                await gate.command(command.SerializeToString())
                frame = decode_packet(await asyncio.wait_for(websocket.recv(), 2))
                content = protocol.parse(frame.type_name, frame.payload)
                assert content.message_id == protocol.message_ids["SceneClientPlayerCommonKickPlayer"]
                assert protocol.parse("GameKickPlayerRequest", content.serialized_message).reason.id == 2017
                try:
                    await asyncio.wait_for(websocket.recv(), 2)
                except ConnectionClosedOK:
                    pass
                else:
                    raise AssertionError("expected websocket to close after kick notice")
        await gate.close()

    asyncio.run(exercise())
