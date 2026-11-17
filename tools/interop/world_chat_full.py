"""Exercise the original world-chat envelope through real Kafka and Gate TCP."""

import asyncio
import time

from login_full import connect_assigned_gate
from xuanming.transport import read_packet


async def verify_world_chat(session, gates):
    gate = gates[1]
    protocol, client = gate.protocol, session.client
    producer = gate.kafka.producer
    topic = f"chat-world-bcast_g{gate.settings.kafka_generation}"
    message_id = protocol.message_ids["ClientPlayerChatNotifyWorldChat"]
    sender_key = str(session.player_id).encode("ascii")

    async def publish(partition, content):
        # Match the original Go Chat world's payload, independent of GateCommand.
        event = protocol.message("contracts.kafka.BroadcastToAllEvent", message_content=content)
        await producer.send_and_wait(topic, event.SerializeToString(), key=sender_key, partition=partition)

    # Use a harmless invalid push followed by a valid one in the same partition.
    # Once the latter arrives, any erroneously forwarded earlier push is observable.
    tip = protocol.message("TipInfoMessage", id=98765).SerializeToString()
    unexpected = protocol.content(protocol.message_ids["SceneClientPlayerCommonSendTipToClient"], body=tip)
    endpoint = gate.gate.info.client_endpoint
    async with connect_assigned_gate(protocol, endpoint, session.account, access_token=session.access_token, ingress="python") as unbound:
        await publish(0, unexpected)
        bodies = set()
        for partition in range(4):
            message = protocol.message("chatpb.ChatMessage", sender_player_id=session.player_id, channel=1,
                                       content=f"world-chat partition {partition}", send_time_ms=time.time_ns() // 1_000_000)
            body = message.SerializeToString()
            await publish(partition, protocol.content(message_id, body=body))
            bodies.add(body)

        for _ in range(4):
            event = await client.content(message_id)
            bodies.remove(event.serialized_message)
        assert not bodies
        assert not any(event == unexpected for event in client.pending)

        try:
            async with asyncio.timeout(0.3):
                unexpected_packet = await read_packet(unbound.reader)
        except TimeoutError:
            pass
        else:
            raise AssertionError(f"verified but unbound Gate session received {unexpected_packet}")

    print("PASS world chat from all four native Kafka partitions reached bound Gate TCP only.", flush=True)
    return {"native_broker": True, "original_go_chat_envelope": True, "partitions_delivered": 4,
            "wrong_message_id_rejected": True, "verified_unbound_excluded": True}
