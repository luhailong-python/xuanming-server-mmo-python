import asyncio
import hmac
import time
from types import SimpleNamespace

import pytest

from xuanming.nodes.gate import Session
from xuanming.nodes.gate_spectate import GateSpectate, SpectateEntry
from xuanming.protocol import Protocol


def test_tiered_spectate_ack_snapshot_dedupe_and_terminal():
    async def run():
        protocol = Protocol()
        messages = []
        session = SimpleNamespace(session_id=33, account="player", player_id=91,
                                  emit=messages.append, outbound=asyncio.Queue(128), buffered_output_bytes=0)
        gate = SimpleNamespace(protocol=protocol, sessions={33: session},
                               info=SimpleNamespace(node_id=9, node_uuid="gate-9"))
        secret = b"test-spectate-hmac-key-with-scope!"
        hub = GateSpectate(gate, secret)
        now = time.time_ns() // 1_000_000

        async def frame(kind, round_index, message_id, payload, **target):
            wrapped = protocol.content(message_id, body=payload.SerializeToString())
            event = protocol.message("contracts.kafka.SpectateFrameEvent", battle_id=42,
                                     battle_instance_id="battle-42", round_index=round_index,
                                     kind=kind, client_frame=wrapped.SerializeToString(), **target)
            topic = "gate-spectate_g1" if target else "spectate-bcast_g1"
            await hub.frame(topic, event.SerializeToString())

        state = protocol.message("SpectateStateS2C", state={"battle_id": 42, "round_index": 1})
        await frame(1, 1, 161, state, target_gate_id=9, target_instance_id="stale-gate")
        assert not hub.entries
        await frame(1, 1, 161, state, target_gate_id=9, target_instance_id="gate-9")
        raw = protocol.message("SpectateTicketPayload", battle_id=42, player_id=91,
                               battle_instance_id="battle-42", expire_at_ms=now + 60000).SerializeToString()
        ticket = protocol.message("SpectateTicket", payload=raw,
                                  signature=hmac.new(secret, raw, "sha256").hexdigest().encode())
        sub = protocol.message("SubscribeSpectateRequest", battle_id=42, spectate_ticket=ticket.SerializeToString())
        number = protocol.message_ids["GateClientSpectateSubscribeSpectate"]
        request = protocol.message("ClientRequest", id=1, message_id=number, body=sub.SerializeToString())
        assert hub.request(session, protocol.routes[number], request) is None
        assert [(m.message_id, m.id) for m in messages] == [(number, 1), (161, 0)]

        turn = protocol.message("TurnResultS2C", battle_id=42, round_index=1,
                                state={"battle_id": 42, "round_index": 2})
        await frame(2, 1, 158, turn)
        await frame(2, 1, 158, turn, target_gate_id=9, target_instance_id="gate-9")
        await frame(3, 1, 166, protocol.message("SpectateEndS2C", battle_id=42, reason=1))
        assert [m.message_id for m in messages] == [number, 161, 158, 166]
        assert not hub.subscriptions

    asyncio.run(run())


@pytest.mark.parametrize("cached", [False, True])
def test_spectator_first_turn_recovers_from_backpressure_with_snapshot_only(cached):
    async def run():
        protocol = Protocol()
        secret = b"test-spectate-hmac-key-with-scope!"
        session = Session(33, 1, SimpleNamespace(is_closing=lambda: False), account="player", player_id=91)
        gate = SimpleNamespace(protocol=protocol, sessions={33: session},
                               info=SimpleNamespace(node_id=9, node_uuid="gate-9"))
        hub = GateSpectate(gate, secret)

        async def frame(kind, round_index):
            if kind == 1:
                message_id = 161
                payload = protocol.message("SpectateStateS2C", state={"battle_id": 42, "round_index": round_index})
            else:
                message_id = 158
                payload = protocol.message("TurnResultS2C", battle_id=42, round_index=round_index,
                                           state={"battle_id": 42, "round_index": round_index + 1})
            event = protocol.message(
                "contracts.kafka.SpectateFrameEvent", battle_id=42, battle_instance_id="battle-42",
                kind=kind, round_index=round_index,
                client_frame=protocol.content(message_id, body=payload.SerializeToString()).SerializeToString(),
            )
            await hub.frame("spectate-bcast_g1", event.SerializeToString())

        if cached:
            await frame(1, 1)
        for _ in range(2):
            session.emit(protocol.content(1, body=b"x" * 140_000))
        raw = protocol.message("SpectateTicketPayload", battle_id=42, player_id=91,
                               battle_instance_id="battle-42", expire_at_ms=time.time_ns() // 1_000_000 + 60_000)
        raw = raw.SerializeToString()
        ticket = protocol.message("SpectateTicket", payload=raw,
                                  signature=hmac.new(secret, raw, "sha256").hexdigest().encode())
        sub = protocol.message("SubscribeSpectateRequest", battle_id=42, spectate_ticket=ticket.SerializeToString())
        number = protocol.message_ids["GateClientSpectateSubscribeSpectate"]
        request = protocol.message("ClientRequest", id=1, message_id=number, body=sub.SerializeToString())
        assert hub.request(session, protocol.routes[number], request) is None
        assert session.outbound.qsize() == 3
        assert not hub.entries[42].subscribers[session.session_id]

        await frame(2, 1)
        assert session.outbound.qsize() == 3
        assert not hub.entries[42].subscribers[session.session_id]
        while not session.outbound.empty():
            session.outbound.get_nowait()

        await frame(2, 2)
        assert session.outbound.qsize() == 1
        first = session.outbound.get_nowait()
        assert first.message_id == 161
        assert protocol.parse("SpectateStateS2C", first.serialized_message).state.round_index == 3
        assert hub.entries[42].subscribers[session.session_id]
        await frame(2, 2)
        assert session.outbound.empty()
        await frame(2, 3)
        turn = session.outbound.get_nowait()
        assert turn.message_id == 158
        assert protocol.parse("TurnResultS2C", turn.serialized_message).round_index == 3

    asyncio.run(run())


def test_spectator_terminal_bypasses_soft_watermark_without_first_snapshot():
    async def run():
        protocol = Protocol()
        session = Session(33, 1, SimpleNamespace(is_closing=lambda: False), account="player", player_id=91)
        gate = SimpleNamespace(protocol=protocol, sessions={33: session},
                               info=SimpleNamespace(node_id=9, node_uuid="gate-9"))
        hub = GateSpectate(gate, b"test-spectate-hmac-key-with-scope!")
        hub.entries[42] = entry = SpectateEntry("battle-42", time.monotonic(), 0, subscribers={33: False})
        hub.subscriptions[33] = 42
        for _ in range(2):
            session.emit(protocol.content(1, body=b"x" * 140_000))
        end = protocol.message("SpectateEndS2C", battle_id=42, reason=1)
        event = protocol.message("contracts.kafka.SpectateFrameEvent", battle_id=42,
                                 battle_instance_id="battle-42", kind=3,
                                 client_frame=protocol.content(166, body=end.SerializeToString()).SerializeToString())
        await hub.frame("spectate-bcast_g1", event.SerializeToString())
        assert session.outbound.qsize() == 3
        assert not entry.subscribers and not hub.subscriptions
        while session.outbound.qsize() > 1:
            session.outbound.get_nowait()
        assert session.outbound.get_nowait().message_id == 166

    asyncio.run(run())


@pytest.mark.parametrize("queued_count,body_size,socket_bytes,expected", [
    (70, 1, 0, True),
    (1, 270_000, 0, False),
    (0, 0, 256 * 1024, True),
    (0, 0, 256 * 1024 + 1, False),
])
def test_spectator_soft_watermark_counts_frame_bytes_and_preserves_reliable_replies(
    queued_count, body_size, socket_bytes, expected,
):
    async def run():
        p = Protocol()
        transport = SimpleNamespace(get_write_buffer_size=lambda: socket_bytes)
        writer = SimpleNamespace(is_closing=lambda: False, transport=transport)
        session = Session(33, 1, writer, account="player", player_id=91)
        gate = SimpleNamespace(protocol=p, sessions={33: session}, info=SimpleNamespace(node_id=9, node_uuid="gate-9"))
        hub = GateSpectate(gate, b"test-spectate-hmac-key-with-scope!")
        hub.entries[42] = SpectateEntry("battle-42", time.monotonic(), 0, subscribers={33: True})
        hub.subscriptions[33] = 42
        for _ in range(queued_count):
            session.emit(p.content(1, body=b"x" * body_size))
        turn = p.message("TurnResultS2C", battle_id=42, round_index=1, state={"battle_id": 42, "round_index": 2})
        event = p.message("contracts.kafka.SpectateFrameEvent", battle_id=42, battle_instance_id="battle-42", kind=2,
                          round_index=1, client_frame=p.content(158, body=turn.SerializeToString()).SerializeToString())
        await hub.frame("spectate-bcast_g1", event.SerializeToString())
        assert session.outbound.qsize() == queued_count + int(expected)

        reliable = p.content(5, 77, body=b"committed-save-response")
        session.emit(reliable)
        pending = [session.outbound.get_nowait() for _ in range(session.outbound.qsize())]
        assert pending[-1] is reliable
        assert [message.message_id for message in pending[queued_count:-1]] == ([158] if expected else [])
        assert session.buffered_output_bytes == socket_bytes

    asyncio.run(run())
