import asyncio
import hmac
import time
from types import SimpleNamespace

from xuanming.nodes.gate_spectate import GateSpectate
from xuanming.protocol import Protocol


def test_tiered_spectate_ack_snapshot_dedupe_and_terminal():
    async def run():
        protocol = Protocol()
        messages = []
        session = SimpleNamespace(session_id=33, account="player", player_id=91,
                                  emit=messages.append, outbound=asyncio.Queue(128))
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
