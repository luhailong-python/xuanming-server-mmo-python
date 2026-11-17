import asyncio
from types import SimpleNamespace

import pytest

from xuanming.data import PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes import scene as scene_module
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_agones import SceneAgones
from xuanming.nodes.scene_planned_drain import RelocationPacer, budget_from_env, planned_drain
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.settings import Settings


def test_relocation_pacer_shares_tokens_across_worlds_and_caps_burst(monkeypatch):
    monkeypatch.setenv("SCENE_PLANNED_DRAIN_RELOCATIONS_PER_SECOND", "2")
    pacer = RelocationPacer.from_env()
    assert not pacer.allow(10)
    assert not pacer.allow(10.25)
    assert pacer.allow(10.5)  # world A
    assert not pacer.allow(10.5)  # world B shares the same node budget
    assert pacer.allow(11.0)
    assert pacer.allow(20.0) and pacer.allow(20.0) and not pacer.allow(20.0)
    monkeypatch.setenv("SCENE_PLANNED_DRAIN_RELOCATIONS_PER_SECOND", "0")
    unlimited = RelocationPacer.from_env()
    assert all(unlimited.allow(0) for _ in range(300))
    monkeypatch.setenv("SCENE_PLANNED_DRAIN_RELOCATIONS_PER_SECOND", "invalid")
    assert RelocationPacer.from_env().rate == 200


def test_drain_stops_admission_retries_registration_and_evacuates(monkeypatch):
    monkeypatch.setenv("SCENE_PLANNED_DRAIN_SELF_EVACUATE_AFTER_SECONDS", "0")

    async def exercise():
        p = Protocol()
        info = p.message("NodeInfo", node_type=3, zone_id=1, node_id=1, node_uuid="old")
        scene = SceneNode(p, info, None, None, lambda *_: None)
        scene.create(p.message("CreateSceneRequest", config_id=1, scene_id=1))
        scene.locations[7] = scene.worlds[1]
        peer = p.message("NodeInfo", node_type=3, zone_id=1, node_id=2, admission=1)

        class Directory:
            calls = 0

            async def update(self, updated):
                assert updated.admission == 2
                assert scene._admission_closed
                with pytest.raises(ValueError):
                    scene.create(p.message("CreateSceneRequest", config_id=1, scene_id=2))
                self.calls += 1
                if self.calls == 1:
                    raise ConnectionError("temporary registry outage")

            async def candidates(self, *_):
                return [info, peer]

        directory = Directory()

        def evacuate():
            assert directory.calls >= 1
            scene.locations.clear()

        scene.evacuate_remaining = evacuate
        await planned_drain(scene, directory, 4)
        assert directory.calls == 2
        assert not scene.locations

    asyncio.run(exercise())


@pytest.mark.parametrize("value", ["-1", " 1", "1.5", "86401", "１２"])
def test_invalid_drain_budget_is_rejected_at_startup(monkeypatch, value):
    monkeypatch.setenv("MMORPG_PLANNED_DRAIN_BUDGET_SECONDS", value)
    with pytest.raises(ValueError):
        budget_from_env()


def test_zero_drain_budget_disables_migration(monkeypatch):
    monkeypatch.setenv("MMORPG_PLANNED_DRAIN_BUDGET_SECONDS", "0")
    assert budget_from_env() == 0
    async def exercise():
        class Scene:
            info = SimpleNamespace(admission=1)
            def begin_planned_drain(self):
                self.closed = True
        scene = Scene()
        async def update(info):
            assert scene.closed and info.admission == 2
        await planned_drain(scene, SimpleNamespace(update=update), 0)
    asyncio.run(exercise())


def test_shutdown_cancels_blocked_entry_before_snapshot(monkeypatch):
    monkeypatch.setattr(scene_module, "SHUTDOWN_SAVE_BUDGET", .05)

    async def exercise():
        p = Protocol()
        scene = SceneNode(p, p.message("NodeInfo", node_id=1), None, None, lambda *_: None)
        scene.create(p.message("CreateSceneRequest", config_id=1, scene_id=1))
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def blocked(*_args, **_kwargs):
            started.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        scene._load_player = blocked
        request = p.message("PlayerEnterGameNodeRequest", player_id=1, scene_id=1,
                            session_id=131073, owner_revision=1)
        entry = asyncio.create_task(scene.enter(request, "gate"))
        await started.wait()
        await scene.close()
        await asyncio.gather(entry, return_exceptions=True)
        assert cancelled.is_set() and entry.cancelled()
        assert not scene.locations and not scene._pending_sessions

    asyncio.run(exercise())


def test_blocked_entry_does_not_prevent_existing_player_save(tmp_path, monkeypatch):
    monkeypatch.setattr(scene_module, "SHUTDOWN_SAVE_BUDGET", .1)

    async def exercise():
        p = Protocol()
        scene = SceneNode(p, p.message("NodeInfo", node_id=1, node_uuid="scene"),
                          SqlitePlayerStore(tmp_path / "players.sqlite3"), PlayerRowCodec(p), lambda *_: None)
        scene.create(p.message("CreateSceneRequest", config_id=1, scene_id=1))
        def request(player):
            return p.message("PlayerEnterGameNodeRequest", player_id=player, scene_id=1,
                             session_id=131073 + player, owner_revision=1)
        await scene.enter(request(1), "gate")
        actor = scene.locations[1].players[1]
        started = asyncio.Event()

        async def blocked(*_args, **_kwargs):
            started.set()
            await asyncio.Future()

        scene._load_player = blocked
        entry = asyncio.create_task(scene.enter(request(2), "gate"))
        await started.wait()
        await scene.close()
        await asyncio.gather(entry, return_exceptions=True)
        assert actor.snapshot.released and not scene.locations
        assert entry.cancelled()

    asyncio.run(exercise())


def test_shutdown_waits_for_fenced_handoff_before_removing_player(monkeypatch):
    monkeypatch.setattr(scene_module, "SHUTDOWN_SAVE_RETRY", .01)

    async def exercise():
        scene = SceneNode(Protocol(), SimpleNamespace(), None, None, lambda *_: None)
        player = SimpleNamespace(player_id=1)
        world = SimpleNamespace(players={1: player})
        pending = True
        removed = []
        scene.travel = SimpleNamespace(frozen=lambda _: pending, published=lambda _: False)

        async def remove(_world, _player):
            removed.append(True)
            del world.players[1]

        scene._remove = remove
        task = asyncio.create_task(scene._shutdown_player(world, player))
        await asyncio.sleep(.03)
        assert not removed and world.players[1] is player
        pending = False
        await asyncio.wait_for(task, 1)
        assert removed == [True]

    asyncio.run(exercise())


def test_late_entry_never_publishes_actor_after_shutdown(monkeypatch):
    monkeypatch.setattr(scene_module, "SHUTDOWN_SAVE_BUDGET", .05)

    async def exercise():
        p = Protocol()
        saved = SimpleNamespace(released=False)

        async def release(snapshot):
            snapshot.released = True
            return snapshot

        scene = SceneNode(p, p.message("NodeInfo", node_id=1), SimpleNamespace(release=release), None, lambda *_: None)
        scene.create(p.message("CreateSceneRequest", config_id=1, scene_id=1))
        started = asyncio.Event()

        async def load(*_args, **_kwargs):
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                return SimpleNamespace(snapshot=saved, pending_save=None, admission=None)

        scene._load_player = load
        request = p.message("PlayerEnterGameNodeRequest", player_id=1, scene_id=1,
                            session_id=131073, owner_revision=1)
        entry = asyncio.create_task(scene.enter(request, "gate"))
        await started.wait()
        await scene.close()
        result = await asyncio.gather(entry, return_exceptions=True)
        assert isinstance(result[0], ValueError)
        assert saved.released and not scene.locations and not scene.worlds[1].players

    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["drain", "scene", "agones"])
def test_runtime_always_closes_stack_and_background_when_shutdown_raises(monkeypatch, failure):
    import xuanming.runtime as runtime_module

    async def exercise():
        runtime = Runtime(Settings(), "scene")
        runtime.directory = SimpleNamespace(lease=1)
        runtime._scene_drain_budget = 1
        runtime.request_shutdown()
        cleaned = []

        async def close_scene():
            if failure == "scene":
                raise RuntimeError("scene")

        async def close_agones():
            if failure == "agones":
                raise RuntimeError("agones")

        async def drain(*_args):
            if failure == "drain":
                raise RuntimeError("drain")

        async def cleanup():
            cleaned.append(True)

        monkeypatch.setattr(runtime_module, "planned_drain", drain)
        runtime.scene = SimpleNamespace(close=close_scene)
        runtime._scene_agones = SimpleNamespace(close=close_agones)
        runtime._stack.push_async_callback(cleanup)
        task = asyncio.create_task(asyncio.sleep(60))
        runtime._background.append(task)
        with pytest.raises(RuntimeError, match=failure):
            await runtime.__aexit__(None, None, None)
        assert task.cancelled() and cleaned == [True]

    asyncio.run(exercise())


def test_agones_label_requests_planned_shutdown_and_signals_sidecar_once():
    async def exercise():
        calls = []
        lifecycle = SceneAgones("127.0.0.1", 9)

        async def request(path):
            calls.append(path)
            if path == "gameserver":
                return {"objectMeta": {"labels": {"mmorpg.io/drain": "true"}}}

        lifecycle._request = request
        requested = asyncio.Event()
        await lifecycle._poll_drain()
        watcher = asyncio.create_task(lifecycle.watch_drain(requested.set))
        await asyncio.wait_for(requested.wait(), 1)
        assert not watcher.done()
        await lifecycle.close()
        await lifecycle.close()
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
        assert calls.count("shutdown") == 1

        # SIGTERM without a label must not recursively request Pod deletion.
        lifecycle = SceneAgones("127.0.0.1", 9)
        lifecycle._request = request
        await lifecycle.close()
        assert calls.count("shutdown") == 1

    asyncio.run(exercise())
