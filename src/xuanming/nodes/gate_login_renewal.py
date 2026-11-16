"""Renew Login's connection lease while its Gate session remains live."""

import asyncio
import heapq
import logging
import time

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

_LOG = logging.getLogger(__name__)
RENEW_ID = 394
RENEW_PATH = "/loginpb.ClientPlayerLogin/RenewSession"
_RETRY = 30
_MAX_PER_TICK = 200
_MAX_IN_FLIGHT = 400

# This is the existing C++/Go internal response wire contract. It is kept
# separate from the client's protocol snapshot and never exposed to clients.
_file = descriptor_pb2.FileDescriptorProto(name="gate_login_renewal_internal.proto", syntax="proto3")
_response = _file.message_type.add(name="RenewSessionResponse")
_response.field.add(name="renewed", number=1, label=1, type=8)
_response.field.add(name="renew_after_seconds", number=2, label=1, type=13)
_pool = descriptor_pool.DescriptorPool()
_pool.AddSerializedFile(_file.SerializeToString())
_Response = message_factory.GetMessageClass(_pool.FindMessageTypeByName("RenewSessionResponse"))


def parse_response(data: bytes):
    return _Response.FromString(data)


def next_interval(seconds: int, session_id: int) -> int:
    base = max(5, min(86400, seconds))
    return base - session_id % (base // 5 + 1)


class LoginRenewal:
    def __init__(self, gate, sender, *, clock=time.monotonic):
        self.gate, self.sender, self.clock = gate, sender, clock
        self._due = []
        self._states = {}
        self._inflight = set()
        self._runner = None
        self._closed = False

    def _enqueue(self, session, when):
        self._states[session.session_id] = (session, "queued")
        heapq.heappush(self._due, (when, session.incarnation, session.session_id))

    def login_succeeded(self, session):
        if self._closed:
            return
        state = self._states.get(session.session_id)
        if state is None or state[0] is not session:
            self._enqueue(session, self.clock())
        elif state[1] == "inflight":
            self._states[session.session_id] = (session, "relogged")
        if self._runner is None:
            self._runner = asyncio.create_task(self._run(), name="gate-login-renewal")

    def forget(self, session):
        state = self._states.get(session.session_id)
        if state is not None and state[0] is session:
            del self._states[session.session_id]

    async def _run(self):
        try:
            while not self._closed:
                self.tick()
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    def tick(self):
        now = self.clock()
        budget = _MAX_PER_TICK
        while budget and len(self._inflight) < _MAX_IN_FLIGHT and self._due and self._due[0][0] <= now:
            _, incarnation, session_id = heapq.heappop(self._due)
            state = self._states.get(session_id)
            if (state is None or state[1] != "queued" or state[0].incarnation != incarnation
                    or self.gate.sessions.get(session_id) is not state[0]):
                continue
            session = state[0]
            self._states[session_id] = (session, "inflight")
            task = asyncio.create_task(self._renew(session), name=f"login-renew-{session_id}")
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)
            budget -= 1

    async def _renew(self, session):
        try:
            response = parse_response(await self.sender(self.gate, session))
        except Exception:
            response = None
            _LOG.debug("Login session renewal failed session=%s", session.session_id, exc_info=True)
        state = self._states.get(session.session_id)
        if (self._closed or self.gate.sessions.get(session.session_id) is not session
                or state is None or state[0] is not session):
            return
        if response is None:
            self._enqueue(session, self.clock() + _RETRY)
        elif response.renewed:
            self._enqueue(session, self.clock() + next_interval(response.renew_after_seconds, session.session_id))
        elif state[1] == "relogged":
            self._enqueue(session, self.clock())
        else:
            del self._states[session.session_id]

    async def close(self):
        self._closed = True
        tasks = tuple(self._inflight)
        if self._runner is not None:
            tasks += (self._runner,)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._states.clear()
        self._due.clear()
