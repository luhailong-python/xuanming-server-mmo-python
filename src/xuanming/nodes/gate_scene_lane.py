"""Bounded, ordered client-to-Scene handoff lane for one Gate session."""

import asyncio
import time
from collections import deque
from dataclasses import dataclass

from google.protobuf.message import DecodeError
import grpc

from ..transport import RpcError, RpcNotSent

MAX_DEPTH = 64
MAX_BYPASS = 64
MAX_WAIT = 2.0
RETRY_INTERVAL = 0.2


@dataclass(slots=True)
class Item:
    request: object
    one_way: bool
    admitted_at: float
    route: tuple
    identity: tuple
    retry_at: float = 0.0


class SceneLane:
    def __init__(self, gate, session):
        self.gate, self.session = gate, session
        self.queue = deque()
        # Async writes can block, so bound the fast-path backlog separately.
        self.bypass = deque()
        self.changed = asyncio.Event()
        self.bypass_space = asyncio.Event()
        self.bypass_space.set()
        self.inflight = None
        self.sent = False

    @property
    def idle(self):
        return not self.queue and not self.bypass and self.inflight is None

    def admit(self, request, one_way):
        session = self.session
        item = Item(request, one_way, time.monotonic(), self.gate._scene_route(session),
                    self.gate._client_identity(session))
        return self._admit(item)

    def _admit(self, item):
        if item.one_way and self.queue and self.queue[0].retry_at:
            if len(self.bypass) >= MAX_BYPASS:
                return False
            self.bypass.append(item)
            self.changed.set()
            return True
        if len(self.queue) + (self.inflight is not None) >= MAX_DEPTH:
            if not item.one_way:
                self.session.emit(self.gate._error(item.request, 1008))
            return True
        self.queue.append(item)
        self.changed.set()
        return True

    async def admit_wait(self, request, one_way):
        """Backpressure the client when direct one-way writes are congested."""
        session = self.session
        item = Item(request, one_way, time.monotonic(), self.gate._scene_route(session),
                    self.gate._client_identity(session))
        while not self._admit(item):
            self.bypass_space.clear()
            if len(self.bypass) < MAX_BYPASS or not self.queue or not self.queue[0].retry_at:
                continue
            await self.bypass_space.wait()

    def redirect(self):
        self.bypass.clear()
        self.bypass_space.set()
        while self.queue:
            item = self.queue.popleft()
            self._reply_error(item, 1003)
        self.changed.set()

    def _reply_error(self, item, error):
        reply = self.gate._error(item.request, error)
        if reply is not None:
            self.session.emit(reply)

    def _current(self, item):
        return (self.gate.sessions.get(self.session.session_id) is self.session
                and not self.session.writer.is_closing()
                and self.gate._client_identity(self.session) == item.identity)

    async def _send(self, item, sent=None):
        session = self.session
        if not self._current(item) or session.redirected or session.entry_failed:
            self._reply_error(item, 1003)
            return
        switch = getattr(self.gate, "kill_switch", None)
        if switch is not None and switch.check_id(item.request.message_id):
            self._reply_error(item, 1003)
            return
        if not session.player_id or not session.scene_node_id:
            self._reply_error(item, 1003)
            return
        while True:
            target = self.gate._scene_identity(session)
            sent_this_attempt = False

            def mark_sent():
                nonlocal sent_this_attempt
                sent_this_attempt = True
                if sent is not None:
                    sent()

            try:
                result = await self.gate._scene_request(
                    session, item.request, one_way=item.one_way,
                    previous_route=item.route != self.gate._scene_route(session),
                    on_sent=mark_sent if sent is not None else None,
                    can_send=lambda: self._current(item) and not session.redirected
                        and self.gate._scene_identity(session) == target,
                    lane_response=True,
                )
                if item.one_way or not self._current(item):
                    return
                if result.HasField("message_content"):
                    content = result.message_content
                    if content.message_id != item.request.message_id or content.id != item.request.id:
                        self._reply_error(item, 1003)
                    else:
                        session.emit(content)
                elif result.dispatch_verdict == 1 and not session.redirected and time.monotonic() - item.admitted_at < MAX_WAIT:
                    item.retry_at = time.monotonic() + RETRY_INTERVAL
                    self.queue.appendleft(item)
                else:
                    self._reply_error(item, 1003)
                return
            except RpcNotSent:
                # _write rejected before writing any bytes. Only route churn, with
                # the same live identity, permits another attempt on the new route.
                if (not sent_this_attempt and self._current(item) and not session.redirected
                        and not session.entry_failed and self.gate._scene_identity(session) != target
                        and time.monotonic() - item.admitted_at < MAX_WAIT):
                    continue
                if self._current(item):
                    self._reply_error(item, 1003)
                return
            except (OSError, ValueError, RpcError, DecodeError, grpc.RpcError, RuntimeError, TimeoutError):
                if self._current(item):
                    self._reply_error(item, 1003)
                return

    async def _call(self, item):
        try:
            await self._send(item, self._mark_sent)
        finally:
            self.inflight = None
            self.sent = False
            self.changed.set()

    def _mark_sent(self):
        self.sent = True
        self.changed.set()

    async def run(self):
        try:
            while True:
                self.changed.clear()
                now = time.monotonic()
                item = None
                timeout = None
                if self.queue:
                    head = self.queue[0]
                    if head.retry_at:
                        # Only one-ways can pass the head held for retry.
                        item = next((entry for entry in self.queue if entry.one_way), None)
                        if item is not None:
                            self.queue.remove(item)
                        elif self.bypass:
                            item = self.bypass.popleft()
                            self.bypass_space.set()
                        elif head.retry_at > now:
                            timeout = min(head.retry_at, head.admitted_at + MAX_WAIT) - now
                        elif self.inflight is None:
                            item = self.queue.popleft()
                    elif self.inflight is None or self.sent and head.one_way:
                        item = self.queue.popleft()
                elif self.bypass and (self.inflight is None or self.sent):
                    item = self.bypass.popleft()
                    self.bypass_space.set()
                if item is not None:
                    if time.monotonic() - item.admitted_at >= MAX_WAIT:
                        self._reply_error(item, 1003)
                    elif item.one_way:
                        await self._send(item)
                    else:
                        self.sent = False
                        self.inflight = asyncio.create_task(self._call(item))
                    continue
                if timeout is not None and timeout <= 0:
                    # Expire the held head even if its next retry is later.
                    self._reply_error(self.queue.popleft(), 1003)
                    continue
                try:
                    if timeout is None:
                        await self.changed.wait()
                    else:
                        await asyncio.wait_for(self.changed.wait(), timeout)
                except TimeoutError:
                    pass
        finally:
            if self.inflight is not None:
                self.inflight.cancel()
                await asyncio.gather(self.inflight, return_exceptions=True)
