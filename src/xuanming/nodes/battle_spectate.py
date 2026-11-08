"""Original Battle-to-Gate L1 and L2 spectator frame envelopes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from google.protobuf.message import Message


STATE, TURN, END = 1, 2, 3
_CLIENT_MESSAGES = {STATE: 161, TURN: 158, END: 166}
KEEPALIVE_MS = 10_000
DEADLINE_GRACE_MS = 60_000

SpectateSink = Callable[[str, int, int, Message], None]


def spectate_kafka_route(channel: str, gate_id: int, battle_id: int, generation: int, partitions: int):
    if generation < 1 or partitions < 1 or battle_id < 1:
        raise ValueError("invalid spectate route")
    if channel == "relay" and gate_id > 0:
        return f"gate-spectate_g{generation}", gate_id % partitions, str(battle_id).encode("ascii")
    if channel == "broadcast" and not gate_id:
        return f"spectate-bcast_g{generation}", battle_id % 16, str(battle_id).encode("ascii")
    raise ValueError("invalid spectate route")


@dataclass(slots=True)
class SpectateChannels:
    relay_gates: dict[int, str] = field(default_factory=dict)
    broadcast: bool = False
    last_frame_ms: int | None = None

    def due(self, now_ms: int, deadline_ms: int) -> bool:
        return self.last_frame_ms is not None and (now_ms - self.last_frame_ms >= KEEPALIVE_MS
                                                    and now_ms < deadline_ms + DEADLINE_GRACE_MS)

    def frame(self, protocol, *, battle_id: int, instance_id: str, deadline_ms: int,
              kind: int, round_index: int, message: Message) -> Message:
        envelope = protocol.content(_CLIENT_MESSAGES[kind], body=message.SerializeToString())
        return protocol.message("contracts.kafka.SpectateFrameEvent", battle_id=battle_id,
                                battle_instance_id=instance_id, round_index=round_index, kind=kind,
                                client_frame=envelope.SerializeToString(), deadline_ms=deadline_ms)

    def attach(self, gate_id: int, instance_id: str, now_ms: int) -> bool:
        if self.broadcast or self.relay_gates.get(gate_id) == instance_id:
            return False
        self.relay_gates[gate_id] = instance_id
        if self.last_frame_ms is None:
            self.last_frame_ms = now_ms
        return True

    def targets(self) -> tuple[tuple[str, int, str], ...]:
        if self.broadcast:
            return (("broadcast", 0, ""),)
        return tuple(("relay", gate_id, uuid) for gate_id, uuid in self.relay_gates.items())
