import asyncio
from contextlib import suppress

from xuanming.nodes.gate import GateNode
from xuanming.protocol import Protocol
from xuanming.transport import Packet, read_packet, write_packet


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
