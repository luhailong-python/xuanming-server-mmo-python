"""Lightweight checks of the isolated drain harness; no cluster is started."""

import asyncio
from contextlib import AsyncExitStack
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

from xuanming.settings import Settings


@pytest.mark.parametrize("incorrect", [None, "source_retained", "wrong_gate", "lost_wallet"])
def test_drain_harness_verifies_handoff_and_rejects_incomplete_results(monkeypatch, tmp_path, incorrect):
    async def exercise():
        player_id = 42
        old_player = SimpleNamespace(snapshot=SimpleNamespace(owner_epoch=4))
        new_player = SimpleNamespace(admission=object(), snapshot=SimpleNamespace(owner_epoch=5, owner=b"new", released=False))
        old_world = SimpleNamespace(info=SimpleNamespace(scene_config_id=8), players={player_id: old_player})
        old_scene = SimpleNamespace(info=SimpleNamespace(zone_id=1, node_id=201),
                                    locations={player_id: old_world})
        gate_session = SimpleNamespace(player_id=player_id, scene_node_id=201, scene_instance_id="old")
        gate = SimpleNamespace(gate=SimpleNamespace(sessions={1: gate_session}))
        session = SimpleNamespace(player_id=player_id, client=object())
        state = SimpleNamespace(target=None, requested=False, closed=False)

        class Lifecycle:
            async def aclose(self):
                assert state.requested and state.target is not None
                state.closed = True
                if incorrect != "source_retained":
                    old_scene.locations.clear()
                if incorrect != "wrong_gate":
                    gate_session.scene_node_id, gate_session.scene_instance_id = 203, "new"

        source = SimpleNamespace(scene=old_scene, settings=Settings(), _interop_lifecycle=Lifecycle(),
                                 request_shutdown=lambda: setattr(state, "requested", True))

        class Target:
            def __init__(self, settings, role):
                assert role == "scene" and settings.scene.node_id == 203
                self.scene = SimpleNamespace(info=SimpleNamespace(zone_id=1, node_id=203, node_uuid="new"),
                                             locations={player_id: SimpleNamespace(
                                                 info=SimpleNamespace(scene_id=20, scene_config_id=8),
                                                 players={player_id: new_player})})
                state.target = self
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass

        notices = iter((10, 20))  # An earlier scene-change notification is still buffered.
        async def read_push(client, name):
            assert state.closed and client is session.client and name == "SceneSceneClientPlayerNotifyEnterScene"
            return name, SimpleNamespace(scene_info=SimpleNamespace(scene_id=next(notices)))
        async def wallet(_client, request_id):
            return 8 if incorrect == "lost_wallet" and request_id == 182 else 9
        async def epoch(_player): return 5
        async def saved(_player):
            return SimpleNamespace(owner=b"new", owner_epoch=5, released=False, save_seq=7)

        scene_full = ModuleType("scene_full")
        scene_full._read_push, scene_full.redis_epoch, scene_full.wallet = read_push, epoch, wallet
        data_full = ModuleType("data_full")
        data_full.read_native_player = saved
        smoke_original = ModuleType("smoke_original")
        smoke_original.RUN = tmp_path
        for name, module in (("scene_full", scene_full), ("data_full", data_full), ("smoke_original", smoke_original)):
            monkeypatch.setitem(sys.modules, name, module)
        spec = importlib.util.spec_from_file_location("isolated_drain_full", Path(__file__).with_name("drain_full.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        monkeypatch.setattr(module, "Runtime", Target)
        monkeypatch.setenv("SCENE_PLANNED_DRAIN_SELF_EVACUATE_AFTER_SECONDS", "17")
        async with AsyncExitStack() as stack:
            if incorrect:
                with pytest.raises(AssertionError):
                    await module.verify_drain(stack, session, gate, source)
            else:
                target, player, result = await module.verify_drain(stack, session, gate, source)
                assert target is state.target and player is new_player
                assert result["source_shutdown"] and result["gate_routed"] and result["target_epoch"] == 5
        assert state.closed
        assert module.os.environ["SCENE_PLANNED_DRAIN_SELF_EVACUATE_AFTER_SECONDS"] == "17"

    asyncio.run(exercise())
