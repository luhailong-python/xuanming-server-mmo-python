"""Move eligible sessions when Login grants this Gate a drain lease."""

import asyncio
import logging
import time

_LOG = logging.getLogger(__name__)
_RETRY = 10.0
_LINGER = 60.0
_ENTER_SETTLE = 300.0


class GateDrainMigration:
    def __init__(self, gate, sender, *, clock=time.monotonic):
        self.gate, self.sender, self.clock = gate, sender, clock
        self.deadline = 0.0
        self.rate = 0
        self._runner = None
        self._inflight = {}
        self._retry = {}
        self._linger = {}
        self._closed = False

    def lease(self, event):
        now = self.clock()
        self.rate = event.rate_per_second if event.lease_ms else 0
        self.deadline = now + event.lease_ms / 1000 if self.rate else 0.0
        if self._runner is None and not self._closed:
            self._runner = asyncio.create_task(self._run(), name="gate-drain-migration")

    def redirected(self, session):
        self._linger.setdefault((session.session_id, session.incarnation), self.clock() + _LINGER)
        if self._runner is None and not self._closed:
            self._runner = asyncio.create_task(self._run(), name="gate-drain-migration")

    def forget(self, session):
        key = session.session_id, session.incarnation
        self._retry.pop(key, None)
        self._linger.pop(key, None)

    def classify(self, session, now):
        if (not session.verified or not session.logged_in or session.redirected or session.entry_failed
                or session.writer.is_closing() or session.lane_busy or not session.requests.empty()
                or session.pending_route is not None or session.routing.locked()):
            return None
        if session.player_id == (1 << 64) - 1:
            return None
        if session.player_id:
            return "game" if session.scene_id and session.scene_node_id else None
        if session.last_enter_at is not None and now - session.last_enter_at < _ENTER_SETTLE:
            return None
        return "select"

    def tick(self):
        now = self.clock()
        for key, deadline in tuple(self._linger.items()):
            if deadline <= now:
                del self._linger[key]
                session = self.gate.sessions.get(key[0])
                if session is not None and session.incarnation == key[1]:
                    session.close()
        if not self.rate or now >= self.deadline or self.sender is None:
            return
        budget = self.rate
        for session in tuple(self.gate.sessions.values()):
            if not budget or len(self._inflight) >= 2 * self.rate:
                break
            key = session.session_id, session.incarnation
            kind = self.classify(session, now)
            if kind is None or key in self._inflight or self._retry.get(key, 0) > now:
                continue
            task = asyncio.create_task(self._request(session, kind, session.player_id if kind == "game" else 0))
            self._inflight[key] = task
            task.add_done_callback(lambda done, key=key: self._inflight.pop(key, None)
                                   if self._inflight.get(key) is done else None)
            budget -= 1

    async def _request(self, session, kind, player_id):
        key = session.session_id, session.incarnation
        try:
            raw = await self.sender(self.gate, session)
            response = self.gate.protocol.parse("loginpb.GateMigrationResponse", raw)
        except Exception:
            _LOG.debug("Gate migration ticket request failed session=%s", session.session_id, exc_info=True)
            response = None
        if self._closed or self.gate.sessions.get(session.session_id) is not session:
            return
        now = self.clock()
        if response is None or response.result not in (1, 2, 3, 4):
            self._retry[key] = now + _RETRY
            return
        if response.result in (2, 4):
            self.rate = 0
            return
        if response.result == 3:
            self._retry[key] = now + _RETRY
            return
        if self.classify(session, now) != kind or (kind == "game" and session.player_id != player_id):
            return
        if not self.gate._drain_redirect(session, response, player_id):
            self._retry[key] = now + _RETRY

    async def _run(self):
        try:
            while not self._closed:
                self.tick()
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    async def close(self):
        self._closed = True
        tasks = tuple(self._inflight.values()) + ((self._runner,) if self._runner else ())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._inflight.clear()
        self._linger.clear()
        self._retry.clear()
