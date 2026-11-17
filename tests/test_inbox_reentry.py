import asyncio
from types import SimpleNamespace

import pytest

from xuanming.nodes.player_inbox import GM, SceneInbox
from xuanming.protocol import Protocol


@pytest.mark.parametrize("phase,outcome", [
    ("fetch", "stale"), ("fetch", "error"), ("fetch", "invalid"),
    ("ack", "stale"), ("ack", "error"), ("ack", "invalid"), ("ack", "more"),
    ("halted_ack", "success"), ("halted_ack", "stale"),
    ("halted_ack", "invalid"), ("halted_ack", "error"), ("halted_ack", "cancel"),
])
def test_old_poll_does_not_mutate_reentered_player_state(phase, outcome):
    async def run():
        protocol = Protocol()
        started, release = asyncio.Event(), asyncio.Event()
        old = SimpleNamespace(player_id=1, entity=11, session_id=21, generation=1,
                              snapshot=SimpleNamespace(owner_epoch=1))
        new = SimpleNamespace(player_id=1, entity=12, session_id=22, generation=2,
                              snapshot=SimpleNamespace(owner_epoch=2))
        world = SimpleNamespace(players={1: old})
        lock = asyncio.Lock()

        class Scene:
            locations = {1: world}
            battle = None
            rows = SimpleNamespace(assemble=lambda snapshot: protocol.message("player_database"))

            def __init__(self):
                self.protocol = protocol

            def _lock(self, player_id):
                return lock

            def mutable(self, player):
                return True

            async def flush_player_rows(self, player):
                pass

        class Peer:
            async def respond(self, method, player, domain, correlation, reports=()):
                if phase == method or phase == "halted_ack" and method == "ack":
                    started.set()
                    await release.wait()
                    if outcome == "error":
                        raise OSError("disconnected")
                fence = 5 if outcome == "stale" and started.is_set() else 3
                invalid = outcome == "invalid" and started.is_set()
                if method == "fetch":
                    return protocol.message("player_inbox.FetchPlayerInboxResponse", fence=6 if invalid else fence,
                                            items=[{"seq": 1, "request_id": 1, "msg_type": 1}],
                                            has_more=outcome == "more")
                return protocol.message("player_inbox.AckPlayerInboxResponse", fence=fence,
                                        error_message={"id": 1005 if invalid else 0},
                                        results=[{"seq": x.seq, "result": 1, "state": x.outcome + 1}
                                                 for x in reports])

            async def fetch(self, player, domain, correlation, cursor):
                return await self.respond("fetch", player, domain, correlation)

            async def ack(self, player, domain, correlation, reports):
                return await self.respond("ack", player, domain, correlation, reports)

        scene = Scene()
        inbox = SceneInbox(scene, Peer(), None, None, clock=lambda: 100)
        report = protocol.message("player_inbox.PlayerInboxDecisionReport", seq=1, request_id=1, outcome=1)

        async def consume(player, domain, item):
            return report

        inbox._consume = consume
        key = (1, GM)
        if phase == "halted_ack":
            inbox._halted.add(key)
            inbox._pending_ack[key] = (old, inbox._identity(old), (report,))
        task = asyncio.create_task(inbox._poll(*key))
        await asyncio.wait_for(started.wait(), 5)
        inbox.departed(old)
        world.players[1] = new
        inbox.landed(new)
        # A newer generation may already have installed its own pending prefix.
        pending = (new, inbox._identity(new), (report,))
        if phase == "halted_ack":
            inbox._pending_ack[key] = pending
        if outcome == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            release.set()
            await asyncio.wait_for(task, 5)
        assert inbox._due[key] == 100
        assert key not in inbox._halted
        if phase == "halted_ack":
            assert inbox._pending_ack[key] is pending

    asyncio.run(run())
