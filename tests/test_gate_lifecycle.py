import asyncio
from contextlib import suppress

from xuanming.nodes.gate import GateNode, Session
from xuanming.nodes.gate_chat import WorldChatFanout
from xuanming.protocol import Protocol
from xuanming.transport import Packet, read_packet, write_packet


class Writer:
    def __init__(self):
        self.transport = self
        self.closed = False

    def close(self):
        self.closed = True

    def abort(self):
        self.close()

    def is_closing(self):
        return self.closed


def test_lease_expiry_fences_player_and_uses_immediate_scene_exit():
    async def run():
        protocol = Protocol()
        disconnects = []
        exits = []

        async def forward(gate, session, route, request, seq):
            disconnects.append((route.method, session.player_id, seq))

        class Scene:
            async def call(self, node_id, message_id, request, *, one_way, expected_uuid):
                content = request.message_content
                assert one_way and (node_id, expected_uuid) == (6, "scene-a")
                assert message_id == protocol.message_ids["SceneProcessClientPlayerMessage"]
                assert content.message_id == protocol.message_ids["ScenePlayerExitGame"]
                reason = protocol.parse("GameNodeExitGameRequest", content.serialized_message).reason
                exits.append((request.session_id, request.player_id, reason))

        info = protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate-a")
        gate = GateNode(protocol, info, b"x" * 32, forward)
        gate.scene = Scene()
        session = Session(1 << 17 | 1, 1, Writer(), verified=True, player_id=42,
                          scene_node_id=6, scene_instance_id="scene-a")
        gate.sessions[session.session_id] = session

        async def expire(player_id, *, target_instance="gate-a"):
            event = protocol.message("contracts.kafka.PlayerLeaseExpiredEvent", session_id=session.session_id,
                                     player_id=player_id)
            command = protocol.message(
                "contracts.kafka.GateCommand", target_gate_id=info.node_id, target_instance_id=target_instance,
                event_id=protocol.event_ids["ContractsKafkaPlayerLeaseExpiredEvent"],
                payload=event.SerializeToString(),
            )
            await gate.command(command.SerializeToString())

        await expire(42, target_instance="retired-gate")
        await expire(99)
        assert not session.writer.closed
        await expire(42)
        assert session.writer.closed
        await gate._disconnect(session)
        assert exits == [(session.session_id, 42, 3)]
        assert disconnects == [("Disconnect", 42, 0)]

    asyncio.run(run())


def test_world_chat_only_pushes_to_bound_sessions_and_caps_node_fanout():
    async def run():
        protocol = Protocol()
        info = protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate-a")
        gate = GateNode(protocol, info, b"x" * 32, None)
        now = 10.0
        fanout = WorldChatFanout(protocol, gate._broadcast_all, clock=lambda: now)
        sessions = [Session(1 << 17 | index, index, Writer(), verified=True, player_id=player_id)
                    for index, player_id in enumerate((41, 42, 0), 1)]
        gate.sessions.update((session.session_id, session) for session in sessions)

        message_id = protocol.message_ids["ClientPlayerChatNotifyWorldChat"]
        chat = protocol.message("chatpb.ChatMessage", sender_player_id=41, channel=1, content="你好", send_time_ms=1)
        content = protocol.content(message_id, body=chat.SerializeToString())
        valid = protocol.message("contracts.kafka.BroadcastToAllEvent", message_content=content).SerializeToString()
        kick = protocol.content(protocol.message_ids["SceneClientPlayerCommonKickPlayer"])
        wrong_id = protocol.message("contracts.kafka.BroadcastToAllEvent", message_content=kick).SerializeToString()
        await fanout(b"\x80")
        await fanout(wrong_id)
        for _ in range(21):
            await fanout(valid)

        now += 0.25
        for _ in range(3):
            await fanout(valid)
        for session in sessions[:2]:
            assert session.outbound.qsize() == 22
            while not session.outbound.empty():
                assert session.outbound.get_nowait() == content
        assert sessions[2].outbound.empty()

        forged_request = protocol.message("ClientRequest", id=7, message_id=message_id)
        result = await gate._dispatch(sessions[0], 1, forged_request)
        assert result.id == 7 and result.error_message.id == 1005

    asyncio.run(run())


def test_native_rate_table_and_other_illegal_requests_share_close_threshold(monkeypatch):
    monkeypatch.setenv("GATE_ILLEGAL_PACKET_THRESHOLD", "4")
    monkeypatch.delenv("GATE_RUN_MODE", raising=False)
    protocol = Protocol()
    gate = GateNode(protocol, protocol.message("NodeInfo", node_id=1), b"x" * 32, None)
    session = Session(1 << 17 | 1, 1, Writer(), verified=True)

    def request(method, *, size=16, now=10.0, call_id=1):
        frame = protocol.message("ClientRequest", id=call_id, message_id=protocol.message_ids[method])
        return gate._admit_request(session, frame, size, now)

    for _ in range(15):
        assert request("SceneMovementClientPlayerMoveStart", call_id=0)
    assert not request("SceneMovementClientPlayerMoveStart", call_id=0)
    rate_error = session.outbound.get_nowait()
    assert rate_error.id == 0 and rate_error.error_message.id == 1008
    assert request("SceneMovementClientPlayerMoveStart", now=11.1, call_id=0)

    assert not request("SceneCurrencyClientPlayerGetCurrencyList", size=1025, call_id=2)
    oversized = session.outbound.get_nowait()
    assert oversized.id == 2 and oversized.error_message.id == 1010
    assert not request("ClientPlayerLoginDisconnect", call_id=3)
    assert session.outbound.empty() and not session.writer.closed
    assert not request("SceneCurrencyClientPlayerGmAddCurrency", call_id=4)
    gm_error = session.outbound.get_nowait()
    assert gm_error.id == 4 and gm_error.error_message.id == 1006
    assert session.illegal_packets == 4 and session.writer.closed


def test_heartbeat_abuse_force_closes_verified_tcp(monkeypatch):
    monkeypatch.setenv("GATE_ILLEGAL_PACKET_THRESHOLD", "1")

    async def run():
        protocol = Protocol()
        info = protocol.message("NodeInfo", node_id=1, zone_id=1)
        gate = GateNode(protocol, info, b"x" * 32, None)
        async with await asyncio.start_server(gate.handle_client, "127.0.0.1", 0) as server:
            host, port = server.sockets[0].getsockname()[:2]
            reader, writer = await asyncio.open_connection(host, port)
            try:
                async with asyncio.timeout(3):
                    await write_packet(writer, Packet.from_message(gate.issue_ticket()))
                    handshake = await read_packet(reader)
                    assert protocol.parse(handshake.type_name, handshake.payload).success
                    for seq in range(4):
                        heartbeat = protocol.message("ClientHeartbeatRequest", seq=seq)
                        await write_packet(writer, Packet.from_message(heartbeat))
                        if seq < 3:
                            response = await read_packet(reader)
                            assert protocol.parse(response.type_name, response.payload).seq == seq
                    try:
                        assert await read_packet(reader) is None
                    except ConnectionResetError:
                        pass
            finally:
                writer.close()
                with suppress(ConnectionError):
                    await writer.wait_closed()
        await gate.close()

    asyncio.run(run())
