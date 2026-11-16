import asyncio
import hmac
import time
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from xuanming.nodes.battle import BattleError, BattleNode, _Mt19937_64
from xuanming.nodes.battle_control import BattleControl, service_digest, sign_service_call, verify_service_call
from xuanming.nodes.battle_replay import decode_replay
from xuanming.nodes.battle_spectate import spectate_kafka_route
from xuanming.nodes.battle_tcp import BattleClients
from xuanming.nodes.scene_battle import SceneBattle
from xuanming.data import ConfigTables
from xuanming.grpc_runtime import serve_grpc
from xuanming.protocol import Protocol
from xuanming.transport import Packet, read_packet, write_packet
from xuanming.transport import decode_packet, encode_packet


@dataclass
class Clock:
    now: int = 1000

    def __call__(self) -> int:
        return self.now


@dataclass
class Socket:
    messages: list = field(default_factory=list)
    closed: int = 0

    def emit(self, message_id, message):
        self.messages.append((message_id, message))

    def close(self):
        self.closed += 1


@pytest.fixture
def runtime():
    protocol = Protocol()
    clock = Clock()
    outcomes = []
    published = []
    node = BattleNode(
        protocol, node_id=4, instance_id="battle-process", host="127.0.0.1", port=8000,
        ticket_secret=b"battle-ticket-keys-for-test-only", physical_region=2, now_ms=clock, steady_ms=clock,
        on_outcome=lambda *values: outcomes.append(values), on_lobby_push=lambda *values: published.append(values),
    )
    return protocol, node, clock, outcomes, published


def create_request(protocol, *, health=80, match_mode=7):
    request = protocol.message(
        "CreateBattleRequest", battle_id=10, match_mode=match_mode, seed=5489,
        deadline_ms=100_000, admit_before_ms=31_000, placement_operation_id="first-placement",
        target_instance_id="battle-process", required_physical_region=2, outcome_protocol_version=1,
        spectate_visibility=2,
    )
    for player_id, side, speed in ((101, 0, 10), (202, 1, 5)):
        player = request.players.add(player_id=player_id, team_index=side, max_health=health, physical_attack=30)
        player.base_attributes.health = health
        player.base_attributes.speed = speed
        player.routing.gate_instance_id = "gate-process"
        player.routing.scene_instance_id = "scene-process"
    request.players[0].items.add(item_table_id=9, count=4)
    request.players[0].combat.physical_crit_rate = 100
    return request


def issue(protocol, node, player_id, operation=""):
    request = protocol.message("IssueBattleTicketRequest", battle_id=10, player_id=player_id,
                               target_instance_id=node.instance_id, spectator_operation_id=operation)
    response = node.issue_ticket(request)
    assert not response.error_message.id
    return response.assignment


def test_mt19937_64_matches_cpp_reference_seed():
    generator = _Mt19937_64(5489)
    assert [generator.next() for _ in range(4)] == [
        14514284786278117030, 4620546740167642908, 13109570281517897720, 17462938647148434322,
    ]


def test_create_idempotency_and_wire_ticket_fence(runtime):
    protocol, node, clock, _, pushes = runtime
    request = create_request(protocol)
    response = node.create(request)
    assert response.room_count == 1
    assert response.adopted_outcome_protocol_version == 1
    assert node.create(request) == response
    assert [message_id for _, _, message_id, _ in pushes] == [177, 143, 177, 143]
    request.seed += 1
    assert node.create(request).error_message.id == 1005

    assigned = issue(protocol, node, 101)
    digest = hmac.new(node.ticket_secret, assigned.token_payload, "sha256")
    assert assigned.token_signature == digest.hexdigest().encode("ascii")
    ticket = node.authorize(assigned.token_payload, assigned.token_signature)
    assert (ticket.battle_id, ticket.player_id, ticket.role) == (10, 101, 1)
    with pytest.raises(BattleError, match="signature"):
        node.authorize(assigned.token_payload, digest.digest())
    clock.now = assigned.expire_at_ms
    with pytest.raises(BattleError, match="expired"):
        node.authorize(assigned.token_payload, assigned.token_signature)


def test_battle_websocket_carries_the_existing_battle_ticket_frame(runtime):
    async def exercise():
        from websockets.asyncio.client import connect
        from websockets.asyncio.server import serve

        protocol, node, _, _, _ = runtime
        assert not node.create(create_request(protocol)).error_message.id
        assignment = node._assignment(node.rooms[10], 101, 1)
        clients = BattleClients(protocol, node)
        async with serve(clients.serve_websocket, "127.0.0.1", 0, compression=None) as server:
            port = server.sockets[0].getsockname()[1]
            async with connect(f"ws://127.0.0.1:{port}", compression=None) as websocket:
                verify = protocol.message(
                    "BattleTokenVerifyRequest", payload=assignment.token_payload,
                    signature=assignment.token_signature,
                )
                await websocket.send(encode_packet(Packet.from_message(verify)))
                packet = decode_packet(await websocket.recv())
                response = protocol.parse(packet.type_name, packet.payload)
                assert packet.type_name == "BattleTokenVerifyResponse" and response.success
        await clients.close()

    asyncio.run(exercise())


def test_reconnect_fences_late_close_and_turn_waits_for_sql(runtime):
    protocol, node, _, outcomes, _ = runtime
    assert not node.create(create_request(protocol, health=20)).error_message.id
    a = issue(protocol, node, 101)
    b = issue(protocol, node, 202)
    participant_a = node.authorize(a.token_payload, a.token_signature)
    participant_b = node.authorize(b.token_payload, b.token_signature)
    first, current, opponent = Socket(), Socket(), Socket()
    first_close, current_close, opponent_close = first.close, current.close, opponent.close
    node.attach(participant_a, first.emit, first_close)
    node.attach(participant_a, current.emit, current_close)
    node.detach(participant_a, first_close)
    node.attach(participant_b, opponent.emit, opponent_close)
    assert first.closed == 1 and node.owns_connection(participant_a, current_close)

    query = protocol.message("GetBattleStateRequest", battle_id=10)
    initial = node.dispatch(participant_a, 140, query)
    assert list(initial.pending_actor_ids) == [101, 202]
    assert [(item.item_table_id, item.count) for item in initial.self_items] == [(9, 4)]
    assert not any(actor.HasField("combat") for actor in initial.actors)
    assert not node.dispatch(participant_b, 140, query).self_items

    action = protocol.message("SubmitBattleActionRequest", battle_id=10)
    action.action.action_type = 1
    action.action.target_id = 202
    assert not node.dispatch(participant_a, 149, action).error_message.id
    action.action.target_id = 101
    assert not node.dispatch(participant_b, 149, action).error_message.id
    assert outcomes == [(10, 1)]
    assert not current.messages and not opponent.messages
    assert node.dispatch(participant_b, 140, query).actors[1].attributes.health == 20

    assert node.decide_outcome(10, committed=True)
    assert not node.decide_outcome(10, committed=True)
    assert [message_id for message_id, _ in current.messages] == [139]
    turn = current.messages[0][1]
    assert turn.round_index == 1 and list(turn.action_order) == [101, 202]
    assert [event.event_type for event in turn.events] == [1, 3, 8]
    assert turn.state.actors[1].is_dead
    assert current.closed == opponent.closed == 1


def test_admission_tombstones_block_late_create_and_old_observer_ticket(runtime):
    protocol, node, _, _, _ = runtime
    create = create_request(protocol)
    node.destroy(protocol.message("DestroyBattleRequest", battle_id=10,
                                  placement_operation_id=create.placement_operation_id,
                                  target_instance_id=node.instance_id))
    assert node.create(create).error_message.id == 1005
    create.battle_id = 11
    assert not node.create(create).error_message.id
    operation = "01900000-0000-7000-8000-000000000001"
    add = protocol.message("AddObserverRequest", battle_id=11, observer_player_id=303,
                           spectator_operation_id=operation, target_instance_id=node.instance_id,
                           admit_before_ms=15_000)
    add.routing.gate_instance_id = "gate-process"
    assert not node.add_observer(add).error_message.id
    assigned = node.issue_ticket(protocol.message("IssueBattleTicketRequest", battle_id=11, player_id=303,
                                                  spectator_operation_id=operation,
                                                  target_instance_id=node.instance_id)).assignment
    ticket = node.authorize(assigned.token_payload, assigned.token_signature)
    assert ticket.role == 2
    remove = protocol.message("RemoveObserverRequest", battle_id=11, observer_player_id=303,
                              spectator_operation_id=operation, admit_before_ms=15_000,
                              target_instance_id=node.instance_id)
    assert node.remove_observer(remove).cancelled
    assert node.add_observer(add).error_message.id == 1005
    with pytest.raises(BattleError, match="not in this battle"):
        node.authorize(assigned.token_payload, assigned.token_signature)


def test_direct_spectator_node_quota_counts_seats_and_snapshot_counts_live_connections(runtime):
    protocol, original, clock, _, _ = runtime
    node = BattleNode(protocol, node_id=original.node_id, instance_id=original.instance_id,
                      host=original.host, port=original.port, ticket_secret=original.ticket_secret,
                      physical_region=2, now_ms=clock, steady_ms=clock, max_observers=2, max_connections=12,
                      on_outcome=lambda *_: None)
    for battle_id in (10, 11):
        request = create_request(protocol)
        request.battle_id = battle_id
        assert not node.create(request).error_message.id

    def add(battle_id, player_id, operation_id):
        request = protocol.message(
            "AddObserverRequest", battle_id=battle_id, observer_player_id=player_id,
            spectator_operation_id=f"01900000-0000-7000-8000-{operation_id:012x}",
            target_instance_id=node.instance_id, admit_before_ms=clock.now + 14_000,
        )
        request.routing.gate_instance_id = "gate-process"
        return request

    first, second, other = add(10, 301, 1), add(10, 302, 2), add(11, 303, 3)
    for request in (first, second, other):
        assert not node.add_observer(request).error_message.id
    assert node.add_observer(add(10, 304, 4)).error_message.id == 16021
    assert node.add_observer(add(11, 304, 5)).error_message.id == 16021

    socket = Socket()
    assignment = issue(protocol, node, 301, first.spectator_operation_id)
    ticket = node.authorize(assignment.token_payload, assignment.token_signature)
    node.attach(ticket, socket.emit, socket.close)
    other_socket = Socket()
    assignment = issue(protocol, node, 302, second.spectator_operation_id)
    second_ticket = node.authorize(assignment.token_payload, assignment.token_signature)
    node.attach(second_ticket, other_socket.emit, other_socket.close, is_live=lambda: not other_socket.closed)
    node.on_verified(ticket)
    assert socket.messages[-1][1].observer_count == 2
    other_socket.close()
    assert not node.add_observer(first).error_message.id
    assert socket.messages[-1][1].observer_count == 1

    # A still-live seat plus waiting and reconnecting seats fill the node's quota.
    close = other_socket.close
    node.attach(second_ticket, other_socket.emit, close)
    node.detach(second_ticket, close)
    assert node.add_observer(add(11, 304, 6)).error_message.id == 16021
    clock.now += 30_000
    node.tick()
    assert not node.add_observer(add(11, 304, 7)).error_message.id
    assert node.add_observer(first).error_message.id == 1005


def test_observer_reconnect_grace_uses_elapsed_time_across_wall_clock_adjustments(runtime):
    protocol, node, wall, _, _ = runtime
    steady = Clock(5000)
    node._steady_ms = steady
    assert not node.create(create_request(protocol)).error_message.id
    operation = "01900000-0000-7000-8000-000000000001"
    request = protocol.message("AddObserverRequest", battle_id=10, observer_player_id=303,
                               spectator_operation_id=operation, target_instance_id=node.instance_id,
                               admit_before_ms=15_000, routing={"gate_instance_id": "gate-process"})
    assert not node.add_observer(request).error_message.id
    assignment = issue(protocol, node, 303, operation)
    ticket = node.authorize(assignment.token_payload, assignment.token_signature)
    socket = Socket()
    close = socket.close
    node.attach(ticket, socket.emit, close)
    node.detach(ticket, close)

    wall.now = 40_000
    node.tick()
    assert 303 in node.rooms[10].observers
    wall.now = 500
    steady.now += 29_999
    node.tick()
    assert 303 in node.rooms[10].observers
    steady.now += 1
    node.tick()
    assert 303 not in node.rooms[10].observers
    with pytest.raises(BattleError, match="not in this battle"):
        node.authorize(assignment.token_payload, assignment.token_signature)


def test_pending_outcome_retries_and_abandonment_ignore_wall_clock_adjustments(runtime):
    protocol, node, wall, attempts, _ = runtime
    steady = Clock(5000)
    node._steady_ms = steady
    assert not node.create(create_request(protocol, health=20)).error_message.id
    for player_id in (101, 202):
        assignment = issue(protocol, node, player_id)
        ticket = node.authorize(assignment.token_payload, assignment.token_signature)
        action = protocol.message("SubmitBattleActionRequest", battle_id=10, action={"action_type": 1})
        assert not node.dispatch(ticket, 149, action).error_message.id
    original = node.outcome_request(10).SerializeToString(deterministic=True)
    result = protocol.parse("contracts.kafka.BattleResultEvent", node.outcome_request(10).raw_result_payload)
    assert result.finished_at_ms == wall.now

    wall.now += 40 * 3_600_000
    node.tick()
    assert attempts == [(10, 1)] and 10 in node.rooms
    wall.now = 1
    steady.now += 5000
    node.tick()
    assert attempts == [(10, 1), (10, 1)]
    assert node.outcome_request(10).SerializeToString(deterministic=True) == original
    steady.now = 5000 + 6 * 3_600_000
    node.tick()
    assert len(attempts) == 3
    steady.now += 59_999
    node.tick()
    assert len(attempts) == 3
    steady.now += 1
    node.tick()
    assert len(attempts) == 4
    steady.now = 5000 + 30 * 3_600_000
    node.tick()
    assert 10 not in node.rooms and len(attempts) == 4


def test_pve_spawns_from_config_tables_and_freezes_seeded_rewards(runtime):
    protocol, original, clock, _, _ = runtime
    node = BattleNode(protocol, node_id=original.node_id, instance_id=original.instance_id,
                      host=original.host, port=original.port, ticket_secret=original.ticket_secret,
                      physical_region=2, now_ms=clock, steady_ms=clock, on_outcome=lambda *_: None,
                      tables=ConfigTables())
    request = create_request(protocol, match_mode=4)
    del request.players[1:]
    request.players[0].physical_attack = 100_000
    assert not node.create(request).error_message.id
    node.tables.rewards[0].exp_gain = 9999
    node.tables.rewards[0].ClearField("random_items")
    node.tables.rewards[0].ClearField("currencies")
    assignment = issue(protocol, node, 101)
    ticket = node.authorize(assignment.token_payload, assignment.token_signature)
    snapshot = node.dispatch(ticket, 140, protocol.message("GetBattleStateRequest", battle_id=10))
    assert len(snapshot.actors) == 2 and snapshot.actors[1].actor_type == 2
    assert snapshot.actors[1].attributes.health == 300
    action = protocol.message("SubmitBattleActionRequest", battle_id=10)
    action.action.action_type = 1
    assert not node.dispatch(ticket, 149, action).error_message.id
    result = protocol.parse("contracts.kafka.BattleResultEvent", node.outcome_request(10).raw_result_payload)
    assert result.outcome == 1 and result.settlement_format_version == 1
    settlement = result.settlements[0]
    assert settlement.player_id == 101 and settlement.exp_gain == 100
    assert sum(item.count for item in settlement.items_gained) == 2
    assert [(currency.currency_type, currency.amount) for currency in settlement.currency_gains] == [(0, 100), (1, 10), (2, 20)]


def test_live_grpc_match_receipt_must_authenticate_and_replays_same_result(runtime):
    protocol, node, clock, _, _ = runtime
    path = "/match.MatchInternal/CommitBattleOutcome"
    secret = b"separate-match-outcome-signing-key-for-test"

    async def exercise():
        digests = []
        result_bytes = []
        commands = []

        async def match(request):
            now = time.time_ns() // 1_000_000
            assert verify_service_call(path, request, "battle", now, secret)
            digest = service_digest(request)
            digests.append(digest)
            result_bytes.append(request.raw_result_payload)
            replay = decode_replay(protocol, request.replay)
            assert replay.end_reason == 1 and len(replay.rounds) == 1 and replay.outcome == 1
            response = protocol.message("match.CommitBattleOutcomeResponse", decision=1, request_digest=digest)
            signing_key = b"first-reply-uses-invalid-secret" if len(digests) == 1 else secret
            sign_service_call(path + "/reply", response, "match", now, signing_key)
            return response

        async def publish(*values):
            commands.append(values)

        grpc_server, port = await serve_grpc(protocol, "127.0.0.1:0", {
            "match.MatchInternal": {"CommitBattleOutcome": match},
        })
        control = BattleControl(protocol, match_endpoint=f"127.0.0.1:{port}", outcome_secret=secret,
                                publish=publish)
        try:
            control.bind(node)
            node.on_outcome = control.outcome
            node.on_lobby_push = control.lobby
            node.on_confirm = control.confirm
            request = create_request(protocol, health=20)
            request.record_replay = True
            for player in request.players:
                player.routing.gate_node_id = 12
                player.routing.scene_node_id = 13
                player.routing.session_id = 14
            assert not node.create(request).error_message.id
            for player_id in (101, 202):
                assignment = issue(protocol, node, player_id)
                ticket = node.authorize(assignment.token_payload, assignment.token_signature)
                action = protocol.message("SubmitBattleActionRequest", battle_id=10)
                action.action.action_type = 1
                assert not node.dispatch(ticket, 149, action).error_message.id
            await asyncio.wait_for(asyncio.gather(*tuple(control._pending.values())), 3)
            assert 10 in node.rooms
            clock.now += 5000
            node.tick()
            await asyncio.wait_for(asyncio.gather(*tuple(control._pending.values())), 3)
            assert 10 not in node.rooms
            assert len(digests) == 2 and digests[0] == digests[1]
            assert result_bytes[0] == result_bytes[1]
            result = protocol.parse("contracts.kafka.BattleResultEvent", result_bytes[0])
            assert [(entry.player_id, entry.health) for entry in result.settlements] == [(101, 20), (202, 0)]
        finally:
            await control.close()
            await grpc_server.stop(0)

        assert [(role, target, player) for role, target, player, _ in commands] == [
            ("gate", 12, 101), ("gate", 12, 101), ("scene", 13, 101),
            ("gate", 12, 202), ("gate", 12, 202), ("scene", 13, 202),
        ]
        assert commands[0][3].event_id == 39 and commands[2][3].event_id == 45

    asyncio.run(exercise())


def test_shutdown_retries_pending_match_outcome_and_drains_spectator_frames(runtime, monkeypatch):
    protocol, node, clock, _, _ = runtime
    steady = Clock(clock.now)
    node._steady_ms = steady
    path = "/match.MatchInternal/CommitBattleOutcome"
    secret = b"separate-match-outcome-signing-key-for-test"
    monkeypatch.setattr("xuanming.nodes.battle_control._SHUTDOWN_RETRY_SECONDS", 0.01)

    async def exercise():
        attempts, frames, heartbeats = [], [], []
        first_started, finish_first = asyncio.Event(), asyncio.Event()

        async def match(request):
            now = time.time_ns() // 1_000_000
            assert verify_service_call(path, request, "battle", now, secret)
            digest = service_digest(request)
            attempts.append((request.battle_id, digest, request.raw_result_payload))
            if len(attempts) == 1:
                first_started.set()
                await finish_first.wait()
            response = protocol.message("match.CommitBattleOutcomeResponse",
                                        decision=3 if len(attempts) == 1 else 1, request_digest=digest)
            sign_service_call(path + "/reply", response, "match", now, secret)
            return response

        async def publish(role, _gate, battle_id, frame):
            assert role == "relay"
            envelope = protocol.parse("MessageContent", frame.client_frame)
            if envelope.message_id == 166:
                end = protocol.parse("SpectateEndS2C", envelope.serialized_message)
                frames.append((battle_id, end.reason, end.outcome))
            elif envelope.message_id == 161:
                heartbeats.append((battle_id, protocol.parse("SpectateStateS2C", envelope.serialized_message)))

        server, port = await serve_grpc(protocol, "127.0.0.1:0", {
            "match.MatchInternal": {"CommitBattleOutcome": match},
        })
        control = BattleControl(protocol, match_endpoint=f"127.0.0.1:{port}", outcome_secret=secret, publish=publish)
        control.bind(node)
        node.on_outcome = control.outcome
        node.on_spectate_frame = control.spectate
        try:
            for battle_id in (10, 11):
                request = create_request(protocol, health=20)
                request.battle_id = battle_id
                assert not node.create(request).error_message.id
                relay = protocol.message("AttachRelayGateRequest", battle_id=battle_id, gate_node_id=33,
                                         gate_instance_id="gate-a", target_instance_id=node.instance_id)
                assert not node.attach_relay_gate(relay).error_message.id
            for player_id in (101, 202):
                assignment = issue(protocol, node, player_id)
                ticket = node.authorize(assignment.token_payload, assignment.token_signature)
                action = protocol.message("SubmitBattleActionRequest", battle_id=10, action={"action_type": 1})
                assert not node.dispatch(ticket, 149, action).error_message.id
            await asyncio.wait_for(first_started.wait(), 2)

            draining = asyncio.create_task(control.close())
            await asyncio.sleep(0.03)
            assert len(attempts) == 1 and 10 in node.rooms
            assert not node.admitting
            assert len(heartbeats) == 2
            steady.now += 10_000
            async with asyncio.timeout(1):
                while len(heartbeats) < 3:
                    await asyncio.sleep(0.01)
            battle_id, heartbeat = heartbeats[-1]
            assert battle_id == 10 and heartbeat.state.outcome == 0
            assert all(not actor.is_dead for actor in heartbeat.state.actors)
            clock.now += 200_000
            node.tick()
            assert node.outcome_request(11) is None
            finish_first.set()
            await asyncio.wait_for(draining, 2)
            assert len(attempts) == 2 and attempts[0] == attempts[1]
            assert 10 not in node.rooms and 11 in node.rooms
            assert frames == [(11, 4, 0), (10, 1, 1)]
            before = len(heartbeats)
            steady.now += 10_000
            node.pump_spectate_keepalive()
            assert len(heartbeats) == before
        finally:
            finish_first.set()
            await control.close()
            await server.stop(0)

    asyncio.run(exercise())


def test_observer_stop_ack_is_sent_before_tcp_eof(runtime):
    protocol, node, _, _, _ = runtime

    async def exercise():
        clients = BattleClients(protocol, node)
        server = await asyncio.start_server(clients.serve, "127.0.0.1", 0)
        node.port = server.sockets[0].getsockname()[1]
        try:
            assert not node.create(create_request(protocol)).error_message.id
            operation = "01900000-0000-7000-8000-000000000001"
            add = protocol.message("AddObserverRequest", battle_id=10, observer_player_id=303,
                                   spectator_operation_id=operation, target_instance_id=node.instance_id,
                                   admit_before_ms=15_000)
            add.routing.gate_instance_id = "gate-process"
            assert not node.add_observer(add).error_message.id
            assignment = issue(protocol, node, 303, operation)
            reader, writer = await asyncio.open_connection("127.0.0.1", node.port)
            try:
                async with asyncio.timeout(3):
                    verify = protocol.message("BattleTokenVerifyRequest", payload=assignment.token_payload,
                                              signature=assignment.token_signature)
                    await write_packet(writer, Packet.from_message(verify))
                    packet = await read_packet(reader)
                    assert packet.type_name == "BattleTokenVerifyResponse"
                    assert protocol.parse(packet.type_name, packet.payload).success
                    packet = await read_packet(reader)
                    assert protocol.parse(packet.type_name, packet.payload).message_id == 161
                    request = protocol.message("StopWatchBattleRequest", battle_id=10)
                    envelope = protocol.message("ClientRequest", id=12, message_id=165, body=request.SerializeToString())
                    await write_packet(writer, Packet.from_message(envelope))
                    packet = await read_packet(reader)
                    response = protocol.parse(packet.type_name, packet.payload)
                    assert response.id == 12 and response.message_id == 165 and not response.error_message.id
                    assert not protocol.parse("StopWatchBattleResponse", response.serialized_message).error_message.id
                    assert await read_packet(reader) is None
            finally:
                writer.close()
                await writer.wait_closed()
        finally:
            server.close()
            await clients.close()
            await server.wait_closed()

    asyncio.run(exercise())


def test_shutdown_timeout_keeps_unknown_result_and_stops_spectate_keepalive(runtime, monkeypatch):
    p, node, clock, _, _ = runtime
    monkeypatch.setattr("xuanming.nodes.battle_control._SHUTDOWN_DRAIN_SECONDS", 0.05)
    monkeypatch.setattr("xuanming.nodes.battle_control._SHUTDOWN_RETRY_SECONDS", 0.01)

    class UnavailableMatch:
        closed = False

        async def call(self, *_):
            await asyncio.Future()

        async def close(self):
            self.closed = True

    async def run():
        frames = []

        async def publish(*args):
            frames.append(args[-1])

        control = BattleControl(p, match_endpoint="127.0.0.1:1", outcome_secret=b"m" * 32, publish=publish)
        await control.match.close()
        control.match = UnavailableMatch()
        control.bind(node)
        node.on_outcome = control.outcome
        node.on_spectate_frame = control.spectate
        try:
            assert not node.create(create_request(p, health=20)).error_message.id
            assert not node.promote_spectate_broadcast(p.message(
                "PromoteSpectateBroadcastRequest", battle_id=10, target_instance_id=node.instance_id,
            )).error_message.id
            for player_id in (101, 202):
                assignment = issue(p, node, player_id)
                ticket = node.authorize(assignment.token_payload, assignment.token_signature)
                action = p.message("SubmitBattleActionRequest", battle_id=10, action={"action_type": 1})
                assert not node.dispatch(ticket, 149, action).error_message.id
            original = node.outcome_request(10).SerializeToString(deterministic=True)
            clock.now += 10_000
            await control.close()
            assert [frame.kind for frame in frames] == [1, 1]
            assert control.match.closed and not control._pending
            assert node.outcome_request(10).SerializeToString(deterministic=True) == original

            extra = []
            node.on_spectate_frame = lambda *record: extra.append(record)
            clock.now += 10_000
            node.pump_spectate_keepalive()
            assert not extra
        finally:
            await control.close()

    asyncio.run(run())


@pytest.mark.parametrize("overflow", [False, True])
def test_battle_tcp_connection_limit_and_output_drain(runtime, monkeypatch, overflow):
    protocol, node, _, _, _ = runtime
    node.max_connections = 1

    async def exercise():
        clients = BattleClients(protocol, node)
        server = await asyncio.start_server(clients.serve, "127.0.0.1", 0)
        node.port = server.sockets[0].getsockname()[1]
        request = create_request(protocol)
        assert not node.create(request).error_message.id
        operation = "01900000-0000-7000-8000-000000000001"
        add = protocol.message("AddObserverRequest", battle_id=10, observer_player_id=303,
                               spectator_operation_id=operation, target_instance_id=node.instance_id,
                               admit_before_ms=15_000, routing={"gate_instance_id": "gate-process"})
        assert not node.add_observer(add).error_message.id
        ticket = issue(protocol, node, 303, operation)
        reader, writer = await asyncio.open_connection("127.0.0.1", node.port)
        try:
            async with asyncio.timeout(3):
                verify = protocol.message("BattleTokenVerifyRequest", payload=ticket.token_payload,
                                          signature=ticket.token_signature)
                await write_packet(writer, Packet.from_message(verify))
                response = await read_packet(reader)
                assert protocol.parse(response.type_name, response.payload).success
                initial = await read_packet(reader)
                assert protocol.parse(initial.type_name, initial.payload).message_id == 161
                excess, excess_writer = await asyncio.open_connection("127.0.0.1", node.port)
                try:
                    assert await read_packet(excess) is None
                finally:
                    excess_writer.close()
                    await excess_writer.wait_closed()

                if overflow:
                    monkeypatch.setattr("xuanming.nodes.battle_tcp._MAX_BUFFERED_OUTPUT", 1)
                    node.on_verified(node.authorize(ticket.token_payload, ticket.token_signature))
                    assert await read_packet(reader) is None
                    return

                node.begin_shutdown()
                server.close()
                await clients.close()
                last = await read_packet(reader)
                envelope = protocol.parse(last.type_name, last.payload)
                assert envelope.message_id == 166
                assert protocol.parse("SpectateEndS2C", envelope.serialized_message).reason == 4
                assert await read_packet(reader) is None
        finally:
            writer.close()
            await writer.wait_closed()
            server.close()
            await clients.close()
            await server.wait_closed()

    asyncio.run(exercise())


@pytest.mark.parametrize("threshold", [0, 3])
def test_battle_tcp_honors_shared_illegal_threshold_and_native_frame_limit(runtime, monkeypatch, threshold):
    monkeypatch.setenv("GATE_ILLEGAL_PACKET_THRESHOLD", str(threshold))
    protocol, node, _, _, _ = runtime

    async def exercise():
        clients = BattleClients(protocol, node)
        async with await asyncio.start_server(clients.serve, "127.0.0.1", 0) as server:
            node.port = server.sockets[0].getsockname()[1]
            assert not node.create(create_request(protocol)).error_message.id
            assignment = issue(protocol, node, 101)
            reader, writer = await asyncio.open_connection("127.0.0.1", node.port)
            try:
                async with asyncio.timeout(3):
                    verify = protocol.message("BattleTokenVerifyRequest", payload=assignment.token_payload,
                                              signature=assignment.token_signature)
                    await write_packet(writer, Packet.from_message(verify))
                    reply = await read_packet(reader)
                    assert protocol.parse(reply.type_name, reply.payload).success
                    # A frame above 4 KiB still gets the native 1 KiB envelope error.
                    oversized = protocol.message("ClientRequest", id=1, message_id=140, body=b"x" * 5000)
                    await write_packet(writer, Packet.from_message(oversized))
                    reply = await read_packet(reader)
                    assert protocol.parse(reply.type_name, reply.payload).error_message.id == 1010
                    count = threshold - 1 if threshold else 55
                    for index in range(count):
                        request = protocol.message("ClientRequest", id=index + 2, message_id=999999)
                        await write_packet(writer, Packet.from_message(request))
                        reply = await read_packet(reader)
                        if threshold and index == count - 1:
                            assert reply is None
                        else:
                            assert protocol.parse(reply.type_name, reply.payload).error_message.id == 1005
                    if not threshold:
                        query = protocol.message("GetBattleStateRequest", battle_id=10)
                        request = protocol.message("ClientRequest", id=99, message_id=140, body=query.SerializeToString())
                        await write_packet(writer, Packet.from_message(request))
                        reply = await read_packet(reader)
                        envelope = protocol.parse(reply.type_name, reply.payload)
                        assert envelope.id == 99 and not envelope.error_message.id
                        assert protocol.parse("BattleStateS2C", envelope.serialized_message).battle_id == 10
            finally:
                writer.close()
                await writer.wait_closed()
                await clients.close()

    asyncio.run(exercise())


def test_battle_probe_requires_grant_for_this_instance_and_fresh_echoes():
    async def exercise():
        p = Protocol()
        key = b"test-network-probe-key-32-characters"
        battle = SimpleNamespace(node_id=3, instance_id="battle-instance", physical_region=2,
                                 tables=None, max_connections=4096)
        clients = BattleClients(p, battle, probe_secret=key)
        server = await asyncio.start_server(clients.serve, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        async def send(writer, name, **fields):
            await write_packet(writer, Packet.from_message(p.message(f"match.{name}", **fields)))

        async def receive(reader, name):
            packet = await read_packet(reader)
            assert packet is not None and packet.type_name == f"match.{name}"
            return p.parse(packet.type_name, packet.payload)

        try:
            async with asyncio.timeout(5):
                for instance_id, replay in (("previous-instance", False), (battle.instance_id, True),
                                            (battle.instance_id, False)):
                    reader, writer = await asyncio.open_connection("127.0.0.1", port)
                    try:
                        grant = p.message("match.NetworkProbeGrant", plan_id="plan", player_id=42,
                                          session_binding="current-session", physical_region=2, target_node_id=3,
                                          target_instance_id=instance_id, expires_at_ms=time.time_ns() // 1_000_000 + 5000)
                        payload = grant.SerializeToString()
                        signature = hmac.digest(key, b"xuanming.match.probe.grant.v1\0" + payload, "sha256")
                        await send(writer, "NetworkProbeStart", grant={"payload": payload, "signature": signature})
                        if instance_id != battle.instance_id:
                            assert await read_packet(reader) is None
                            continue
                        first = await receive(reader, "NetworkProbeChallenge")
                        await send(writer, "NetworkProbeEcho", nonce=first.nonce, sample_index=0)
                        second = await receive(reader, "NetworkProbeChallenge")
                        await send(writer, "NetworkProbeEcho", nonce=first.nonce if replay else second.nonce, sample_index=1)
                        if replay:
                            assert await read_packet(reader) is None
                            continue
                        third = await receive(reader, "NetworkProbeChallenge")
                        await send(writer, "NetworkProbeEcho", nonce=third.nonce, sample_index=2)
                        complete = await receive(reader, "NetworkProbeComplete")
                        signed = complete.receipt
                        assert signed.signature == hmac.digest(
                            key, b"xuanming.match.probe.receipt.v1\0" + signed.payload, "sha256",
                        )
                        receipt = p.parse("match.NetworkProbeReceipt", signed.payload)
                        assert receipt.grant == grant and receipt.sample_count == 3 and receipt.rtt_ms >= 1
                        assert await read_packet(reader) is None
                    finally:
                        writer.close()
                        await writer.wait_closed()
        finally:
            server.close()
            await clients.close()
            await server.wait_closed()

    asyncio.run(exercise())


def test_spectate_promotion_keeps_turn_and_end_on_same_broadcast_partition(runtime):
    p, node, clock, _, _ = runtime
    frames = []
    node.on_spectate_frame = lambda *record: frames.append(record)
    request = create_request(p, health=20)
    assert not node.create(request).error_message.id
    attach = p.message("AttachRelayGateRequest", battle_id=10, gate_node_id=33,
                       gate_instance_id="gate-a", target_instance_id=node.instance_id)
    assert not node.attach_relay_gate(attach).error_message.id
    assert not node.attach_relay_gate(attach).error_message.id
    assert len(frames) == 1
    assert frames[0][0:3] == ("relay", 33, 10)
    relay = frames[0][3]
    assert relay.kind == 1 and relay.target_gate_id == 33 and relay.target_instance_id == "gate-a"
    assert spectate_kafka_route("relay", 33, 10, 7, 32) == ("gate-spectate_g7", 1, b"10")

    clock.now += 10_000
    node.tick()
    assert len(frames) == 2 and frames[1][3].kind == 1
    promote = p.message("PromoteSpectateBroadcastRequest", battle_id=10, target_instance_id=node.instance_id)
    assert not node.promote_spectate_broadcast(promote).error_message.id
    assert node.attach_relay_gate(attach).broadcast
    assert frames[-1][0:3] == ("broadcast", 0, 10)
    for player in (101, 202):
        assignment = issue(p, node, player)
        ticket = node.authorize(assignment.token_payload, assignment.token_signature)
        action = p.message("SubmitBattleActionRequest", battle_id=10)
        action.action.action_type = 1
        assert not node.dispatch(ticket, 149, action).error_message.id
    assert len(frames) == 3
    node.decide_outcome(10, committed=True)
    assert [frame.kind for _, _, _, frame in frames] == [1, 1, 1, 2, 3]
    for channel, gate, battle, frame in frames[-3:]:
        assert spectate_kafka_route(channel, gate, battle, 7, 32) == ("spectate-bcast_g7", 10, b"10")
        assert not frame.target_gate_id and not frame.target_instance_id
        envelope = p.parse("MessageContent", frame.client_frame)
        assert envelope.message_id == {1: 161, 2: 158, 3: 166}[frame.kind]


def test_spectate_keepalive_uses_elapsed_time_and_retires_expired_channels(runtime):
    p, node, wall, _, _ = runtime
    steady, frames = Clock(5000), []
    node._steady_ms = steady
    node.on_spectate_frame = lambda *record: frames.append(record)
    assert not node.create(create_request(p)).error_message.id

    def attach(gate_id):
        request = p.message("AttachRelayGateRequest", battle_id=10, gate_node_id=gate_id,
                            gate_instance_id=f"gate-{gate_id}", target_instance_id=node.instance_id)
        assert not node.attach_relay_gate(request).error_message.id

    attach(33)
    assert len(frames) == 1
    wall.now += 9000
    node.pump_spectate_keepalive()
    assert len(frames) == 1
    wall.now = 500
    steady.now += 9999
    node.tick()
    assert len(frames) == 1
    steady.now += 1
    node.tick()
    assert len(frames) == 2
    assert frames[-1][:3] == ("relay", 33, 10) and frames[-1][3].kind == 1

    steady.now += 9999
    attach(34)
    assert len(frames) == 3
    steady.now += 1
    node.tick()
    assert [(role, gate) for role, gate, _, _ in frames[-2:]] == [("relay", 33), ("relay", 34)]

    promote = p.message("PromoteSpectateBroadcastRequest", battle_id=10, target_instance_id=node.instance_id)
    assert not node.promote_spectate_broadcast(promote).error_message.id
    steady.now += 10_000
    node.tick()
    assert frames[-1][:3] == ("broadcast", 0, 10) and frames[-1][3].kind == 1

    before = len(frames)
    wall.now = node.rooms[10].deadline_ms + 60_000
    steady.now += 10_000
    node.pump_spectate_keepalive()
    assert len(frames) == before
    wall.now = 500
    steady.now += 10_000
    node.pump_spectate_keepalive()
    assert len(frames) == before


def test_scene_prepare_confirm_cannot_be_cancelled_by_late_compensation(runtime):
    protocol, _, clock, _, _ = runtime

    async def exercise():
        data = protocol.message("player_database", player_id=101,
                                derived_attributes_component={"health": 50, "mana": 10},
                                level_component={"level": 8})
        player = SimpleNamespace(player_id=101, snapshot=data, gate_uuid="gate-instance", session_id=(7 << 17) + 3,
                                 disconnect_at=None, motion=SimpleNamespace(velocity=(1.0, 0.0, 0.0)))
        lock = asyncio.Lock()

        async def flush(_):
            return None

        async def commit(player, saved):
            player.snapshot = saved

        scene = SimpleNamespace(protocol=protocol, rows=SimpleNamespace(assemble=lambda snapshot: snapshot),
                                _lock=lambda _: lock, locations={101: SimpleNamespace(players={101: player})},
                                mutable=lambda _: True, flush_player_rows=flush, commit_player_data=commit,
                                info=SimpleNamespace(node_id=10, node_uuid="scene-instance", zone_id=1))
        tables = ConfigTables()
        medicine = next(row.id for row in tables["Item"].rows() if row.battle_usable)
        data.bag_component.items.add(item_uuid=1, config_id=medicine, stack_size=2)
        extended = data.bag_component.items.add(instance={"item_id": 2, "config_id": medicine, "size": 9})
        extended.instance.MergeFromString(b"\x98\x06\x01")
        battle = SceneBattle(scene, tables, now_ms=clock)
        prepare = protocol.message("PrepareBattleRequest", player_id=101, battle_id=40, battle_node_id=2,
                                   deadline_ms=8000, prepare_deadline_ms=3000)
        result = await battle.prepare(prepare)
        assert not result.error_message.id
        assert result.snapshot.base_attributes.speed > 0 and result.snapshot.level == 8
        assert result.table_fingerprint == result.snapshot.table_fingerprint == "19ee31807f2393e449dcebebb5cca233"
        assert result.snapshot.routing.gate_node_id == 7 and result.snapshot.routing.scene_node_id == 10
        assert [(item.item_table_id, item.count) for item in result.snapshot.items] == [(medicine, 2)]
        assert [(h.hold_id, [(line.config_id, line.count) for line in h.lines]) for h in data.item_holds.holds] == [
            (40, [(medicine, 2)])
        ]
        assert battle.frozen(101) and player.motion.velocity == (0.0, 0.0, 0.0)
        assert (await battle.prepare(prepare)).error_message.id == 1006
        cancel = protocol.message("CancelBattlePrepareRequest", player_id=101, battle_id=41)
        await battle.cancel(cancel)
        assert battle.frozen(101)
        await battle.confirm(protocol.message("BattleConfirmedEvent", player_id=101, battle_id=40, deadline_ms=8000))
        cancel.battle_id = 40
        await battle.cancel(cancel)
        clock.now = 3000
        assert battle.frozen(101)
        clock.now = 128_000
        assert battle.frozen(101)
        clock.now += 1
        assert not battle.frozen(101)

    asyncio.run(exercise())
