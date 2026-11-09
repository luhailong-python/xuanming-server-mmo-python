"""大厅观战订阅、快照补帧与按来源实例隔离的扇出。"""

import asyncio
import hmac
import time
from dataclasses import dataclass, field

from google.protobuf.message import DecodeError

KINDS = {1: (161, "SpectateStateS2C"), 2: (158, "TurnResultS2C"), 3: (166, "SpectateEndS2C")}
_SOFT_WATERMARK_BYTES = 256 * 1024


@dataclass(slots=True)
class SpectateEntry:
    instance_id: str
    touched: float
    deadline_ms: int
    cached_round: int = -1
    last_turn: int = -1
    first: object = None
    terminal: object = None
    subscribers: dict[int, bool] = field(default_factory=dict)


class GateSpectate:
    def __init__(self, gate, secret: bytes) -> None:
        self.gate = gate
        self.protocol = gate.protocol
        self.secret = secret
        self.entries: dict[int, SpectateEntry] = {}
        self.subscriptions: dict[int, int] = {}
        self._fanout_lock = asyncio.Lock()

    def close_session(self, session_id: int, battle_id: int | None = None) -> None:
        current = self.subscriptions.get(session_id)
        if current is not None and battle_id in (None, current):
            self.subscriptions.pop(session_id)
            entry = self.entries.get(current)
            if entry:
                entry.subscribers.pop(session_id, None)

    def request(self, session, route, request):
        p = self.protocol
        try:
            message = p.parse(route.request_type, request.body)
        except DecodeError:
            return p.content(request.message_id, request.id, error=1014)
        if route.method == "UnsubscribeSpectate":
            self.close_session(session.session_id, message.battle_id)
            return p.content(request.message_id, request.id)
        if route.method != "SubscribeSpectate":
            return p.content(request.message_id, request.id, error=1003)
        ticket = self._ticket(session, message)
        if ticket is None:
            return p.content(request.message_id, request.id, error=16024)
        if ticket.battle_id not in self.entries and len(self.entries) >= 16384:
            return p.content(request.message_id, request.id, error=1003)
        session.emit(p.content(request.message_id, request.id))
        battle = ticket.battle_id
        session_id = session.session_id
        self.close_session(session_id)
        now = time.monotonic()
        entry = self.entries.get(battle)
        if entry is None or entry.instance_id != ticket.battle_instance_id:
            old = entry.subscribers if entry is not None else {}
            entry = self.entries[battle] = SpectateEntry(ticket.battle_instance_id, now, ticket.deadline_ms)
            entry.subscribers.update(dict.fromkeys(old, False))
        if entry.terminal is not None:
            session.emit(entry.terminal)
        else:
            entry.deadline_ms = ticket.deadline_ms or entry.deadline_ms
            self.subscriptions[session_id] = battle
            entry.subscribers[session_id] = False
            self._send_first(session, entry)
        return None

    @staticmethod
    def _send_first(session, entry) -> None:
        if entry.first is not None and GateSpectate._can_send_state(session):
            session.emit(entry.first)
            entry.subscribers[session.session_id] = True

    @staticmethod
    def _can_send_state(session) -> bool:
        return not session.outbound.full() and session.buffered_output_bytes <= _SOFT_WATERMARK_BYTES

    def _ticket(self, session, request):
        try:
            ticket = self.protocol.parse("SpectateTicket", request.spectate_ticket)
            expected = hmac.new(self.secret, ticket.payload, "sha256").hexdigest().encode()
            if not hmac.compare_digest(expected, ticket.signature):
                return None
            payload = self.protocol.parse("SpectateTicketPayload", ticket.payload)
        except DecodeError:
            return None
        if (not session.account or not session.player_id or payload.player_id != session.player_id
                or not payload.battle_id or payload.battle_id != request.battle_id or not payload.battle_instance_id
                or payload.expire_at_ms <= time.time_ns() // 1_000_000):
            return None
        return payload

    async def frame(self, topic: str, body: bytes) -> None:
        async with self._fanout_lock:
            await self._frame(topic, body)

    async def _frame(self, topic: str, body: bytes) -> None:
        p = self.protocol
        try:
            event = p.parse("contracts.kafka.SpectateFrameEvent", body)
            info = self.gate.info
            if topic.startswith("gate-spectate_"):
                if (event.target_gate_id, event.target_instance_id) != (info.node_id, info.node_uuid):
                    return
            elif event.target_gate_id or event.target_instance_id:
                return
            schema = KINDS.get(event.kind)
            if not schema or not event.battle_id or not event.battle_instance_id or len(event.client_frame) > (1 << 20):
                return
            message = p.parse("MessageContent", event.client_frame)
            if message.message_id != schema[0] or message.id or message.error_message.id:
                return
            payload = p.parse(schema[1], message.serialized_message)
        except DecodeError:
            return
        battle = event.battle_id
        if event.kind == 3:
            if payload.battle_id != battle:
                return
        elif not payload.HasField("state") or payload.state.battle_id != battle:
            return
        now = time.monotonic()
        entry = self.entries.get(battle)
        if entry is None:
            if len(self.entries) >= 16384:
                return
            entry = self.entries[battle] = SpectateEntry(event.battle_instance_id, now, event.deadline_ms)
        if entry.instance_id != event.battle_instance_id or entry.terminal is not None:
            return
        entry.touched = now
        entry.deadline_ms = event.deadline_ms or entry.deadline_ms
        if event.kind == 1:
            if event.round_index < entry.cached_round:
                return
            entry.cached_round, entry.first = event.round_index, message
            await self._fanout(battle, entry, first_only=True)
        elif event.kind == 2:
            if event.round_index <= entry.last_turn:
                return
            entry.last_turn = event.round_index
            if event.round_index + 1 >= entry.cached_round:
                entry.cached_round = event.round_index + 1
                first = p.message("SpectateStateS2C", state=payload.state)
                entry.first = p.content(161, body=first.SerializeToString())
            await self._fanout(battle, entry, turn=message)
        else:
            entry.terminal, entry.first = message, None
            await self._fanout(battle, entry)

    async def _fanout(self, battle, entry, *, first_only=False, turn=None):
        for index, session_id in enumerate(tuple(entry.subscribers)):
            if index and index % 512 == 0:
                await asyncio.sleep(0)
            if self.entries.get(battle) is not entry:
                return
            session = self.gate.sessions.get(session_id)
            if not session or not session.account or self.subscriptions.get(session_id) != battle:
                self.close_session(session_id, battle)
                continue
            if entry.terminal is not None:
                session.emit(entry.terminal)
                self.close_session(session_id, battle)
                continue
            if not entry.subscribers.get(session_id):
                self._send_first(session, entry)
                continue
            if not first_only and turn and self._can_send_state(session):
                session.emit(turn)

    async def sweep(self) -> None:
        while True:
            await asyncio.sleep(1)
            async with self._fanout_lock:
                now, stamp = time.monotonic(), time.time_ns() // 1_000_000
                for battle, entry in tuple(self.entries.items()):
                    if entry.terminal is not None and now - entry.touched >= 600:
                        self.entries.pop(battle)
                    elif entry.terminal is None and (
                        now - entry.touched >= 30 or entry.deadline_ms and stamp >= entry.deadline_ms + 60_000
                    ):
                        if entry.subscribers:
                            end = self.protocol.message("SpectateEndS2C", battle_id=battle, reason=4)
                            entry.terminal = self.protocol.content(166, body=end.SerializeToString())
                            await self._fanout(battle, entry)
                        if self.entries.get(battle) is entry:
                            self.entries.pop(battle)
