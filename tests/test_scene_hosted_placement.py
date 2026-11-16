import asyncio
from types import SimpleNamespace

import pytest

from xuanming.data import PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.scene import SceneNode
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.data import ConfigTables
from xuanming.nodes import scene_travel as travel_module
from xuanming.nodes.scene_travel import SceneTravel


@pytest.mark.parametrize("restore_succeeds", [True, False])
def test_same_node_hosted_failure_restores_admission_or_evicts(tmp_path, restore_succeeds):
    async def exercise():
        protocol = Protocol()
        scene = SceneNode(protocol, protocol.message("NodeInfo", node_id=2, zone_id=1, node_uuid="scene-a"),
                          SqlitePlayerStore(tmp_path / "same-node.sqlite"), PlayerRowCodec(protocol), lambda *_: None)
        for scene_id in (1, 2):
            scene.create(protocol.message("CreateSceneRequest", scene_id=scene_id, config_id=scene_id))
        request = protocol.message("PlayerEnterGameNodeRequest", player_id=1, scene_id=1, owner_revision=7,
                                   session_id=(1 << 17) | 9, login_generation=12)
        await scene.enter(request, "gate")
        actor = scene.locations[1].players[1]
        actor.disconnect_at = 1.0
        assert scene.connection_holds.start(actor)
        former, restored = object(), object()
        actor.admission = former
        calls, retired = [], []

        class Travel:
            def sealed(self, _):
                return False

            def published(self, _):
                return False

            async def admit(self, incoming):
                calls.append(incoming.scene_id)
                if len(calls) == 1 or not restore_succeeds:
                    raise RuntimeError("registration unavailable")
                return restored

            async def confirm(self, admission):
                assert admission is restored

            async def retire(self, admission, *, transferred=False):
                retired.append((admission, transferred))

        scene.travel = Travel()
        request.scene_id, request.owner_epoch = 2, actor.snapshot.owner_epoch
        try:
            with pytest.raises(RuntimeError, match="registration unavailable"):
                await scene.enter(request, "", hosted=True)
            assert calls == [2, 1]
            if restore_succeeds:
                assert scene.locations[1].players[1] is actor and actor.admission is restored
                assert retired == [(former, True)]
            else:
                assert 1 not in scene.locations
        finally:
            scene.travel = None
            if 1 in scene.locations:
                actor.admission = None
            await scene.close()

    asyncio.run(exercise())


def test_hosted_event_loads_without_binding_session_and_rejects_live_replacement(tmp_path):
    async def exercise():
        protocol, sent = Protocol(), []
        scene = SceneNode(protocol, protocol.message("NodeInfo", node_id=2, zone_id=1, node_uuid="scene-a"),
                          SqlitePlayerStore(tmp_path / "hosted.sqlite"), PlayerRowCodec(protocol),
                          lambda player, content: sent.append(content))
        scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))
        runtime = SimpleNamespace(protocol=protocol, scene=scene)

        def command(event, *, instance="scene-a"):
            return protocol.message("contracts.kafka.SceneCommand", player_id=event.player_id,
                                    target_scene_id=2, target_instance_id=instance,
                                    event_id=protocol.event_ids["PlayerHostedPlacementEvent"],
                                    payload=event.SerializeToString()).SerializeToString()

        event = protocol.message("PlayerHostedPlacementEvent", player_id=1, scene_id=1, home_zone_id=1,
                                 owner_epoch=1, session_id=(1 << 17) | 9, login_generation=12, owner_revision=7)
        try:
            await Runtime._scene_command(runtime, command(event, instance="old-node"))
            assert 1 not in scene.locations
            await Runtime._scene_command(runtime, command(event))
            actor = scene.locations[1].players[1]
            assert actor.disconnect_at is not None and scene.connection_holds.held(actor)
            assert not scene.sessions and not sent
            await Runtime._scene_command(runtime, command(event))
            assert scene.locations[1].players[1] is actor and not scene.sessions

            await scene.enter(protocol.message("PlayerEnterGameNodeRequest", player_id=2, scene_id=1,
                                              owner_revision=7, login_generation=14, session_id=(1 << 17) | 10), "gate")
            live = scene.locations[2].players[2]
            with pytest.raises(ValueError, match="在线角色"):
                await scene.enter(protocol.message("PlayerEnterGameNodeRequest", player_id=2, scene_id=1,
                                                   owner_revision=7, login_generation=14,
                                                   session_id=(1 << 17) | 10), "", hosted=True)
            assert scene.locations[2].players[2] is live and scene.sessions[("gate", live.session_id)] is live
        finally:
            await scene.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("stale,failures,race", [(False, 1, False), (False, 3, False),
                                                 (True, 1, False), (False, 1, True)])
def test_hosted_retry_requires_authoritative_placement(monkeypatch, stale, failures, race):
    async def exercise():
        protocol = Protocol()
        calls, presence = [], []
        scene = SimpleNamespace(protocol=protocol, info=protocol.message("NodeInfo", node_id=2, zone_id=1,
                                                                        node_uuid="scene-a"),
                                locations={}, worlds={1: SimpleNamespace(draining=False)}, _closing=False,
                                register=lambda *_: None)

        async def enter(request, gate, *, hosted):
            calls.append(request.player_id)
            if len(calls) <= failures:
                raise OSError("temporary storage failure")
            scene.locations[request.player_id] = scene.worlds[1]

        scene.enter = enter

        class Redis:
            moved = stale

            async def eval(self, script, key_count, *keys):
                assert script.startswith("#!lua\n") and key_count == 3
                if script == travel_module._HOSTED_WATCH_PRESENCE:
                    if race:
                        self.moved = True
                        return 0
                    presence.append((keys[2], keys[5], 60))
                    return 1
                location = protocol.message("storage.PlayerLocation", scene_id=1, node_id="3" if self.moved else "2",
                                            zone_id=1, owner_epoch=8, session_id=131081, login_generation=12)
                return b"8", location.SerializeToString(), None

            async def set(self, key, value, *, ex):
                presence.append((key, value, ex))

            async def aclose(self):
                pass

        class Peer:
            async def close(self):
                pass

        monkeypatch.setattr(travel_module, "_HOSTED_RETRY_DELAYS", (.001, .001, .001))
        travel = SceneTravel(scene, ConfigTables(), Peer(), Redis(), Peer(), b"x" * 32)
        request = protocol.message("PlayerEnterGameNodeRequest", player_id=42, scene_id=1, owner_epoch=8,
                                   session_id=131081, login_generation=12, owner_revision=7)
        try:
            await travel.hosted_placement(request)
            await asyncio.gather(*tuple(travel._hosted_watches.values()))
            assert len(calls) == (0 if stale else 1 if race else failures + 1)
            assert (42 in scene.locations) is not (stale or race)
            assert presence == ([] if stale or race else [("player:{42}:hosted_presence", "scene-a", 60)])
        finally:
            await travel.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("replace", [False, True])
def test_stale_hosted_event_keeps_existing_watch_and_valid_new_event_can_replace(monkeypatch, replace):
    async def exercise():
        protocol = Protocol()
        attempted = asyncio.Event()
        calls = []
        scene = SimpleNamespace(protocol=protocol, info=protocol.message("NodeInfo", node_id=2, zone_id=1,
                                                                        node_uuid="scene-a"),
                                locations={}, worlds={1: SimpleNamespace(draining=False)}, _closing=False,
                                register=lambda *_: None)

        async def enter(request, gate, *, hosted):
            calls.append(request.owner_epoch)
            attempted.set()
            if request.owner_epoch == 8 and calls.count(8) == 1:
                raise OSError("temporary load failure")
            scene.locations[request.player_id] = scene.worlds[1]

        scene.enter = enter

        class Redis:
            epoch = 8
            fail_read = False

            async def eval(self, script, key_count, *keys):
                if script == travel_module._HOSTED_WATCH_PRESENCE:
                    return 1
                if self.fail_read:
                    self.fail_read = False
                    raise OSError("temporary Redis failure")
                location = protocol.message("storage.PlayerLocation", scene_id=1, node_id="2", zone_id=1,
                                            owner_epoch=self.epoch, session_id=131081, login_generation=12)
                return str(self.epoch).encode(), location.SerializeToString(), None

            async def aclose(self):
                pass

        class Peer:
            async def close(self):
                pass

        monkeypatch.setattr(travel_module, "_HOSTED_RETRY_DELAYS", (.05, .05, .05))
        monkeypatch.setattr(travel_module, "_HOSTED_READ_RETRY", .001)
        redis = Redis()
        travel = SceneTravel(scene, ConfigTables(), Peer(), redis, Peer(), b"x" * 32)
        request = protocol.message("PlayerEnterGameNodeRequest", player_id=42, scene_id=1, owner_epoch=8,
                                   session_id=131081, login_generation=12, owner_revision=7)
        try:
            await travel.hosted_placement(request)
            original_watch = travel._hosted_watches[42]
            await asyncio.wait_for(attempted.wait(), 1)
            await travel.hosted_placement(request)
            assert travel._hosted_watches[42] is original_watch
            stale = protocol.message("PlayerEnterGameNodeRequest", player_id=42, scene_id=1, owner_epoch=7,
                                     session_id=131081, login_generation=12, owner_revision=7)
            await travel.hosted_placement(stale)
            await asyncio.gather(*tuple(travel._hosted_candidates.values()))
            assert travel._hosted_watches[42] is original_watch and not original_watch.cancelled()
            if replace:
                redis.epoch, redis.fail_read = 9, True
                newer = protocol.message("PlayerEnterGameNodeRequest", player_id=42, scene_id=1, owner_epoch=9,
                                         session_id=131081, login_generation=12, owner_revision=7)
                await travel.hosted_placement(newer)
                await asyncio.gather(*tuple(travel._hosted_candidates.values()))
                assert travel._hosted_watches.get(42) is not original_watch
                await asyncio.gather(*tuple(travel._hosted_watches.values()))
                assert calls == [8, 9]
            else:
                await original_watch
                assert calls == [8, 8]
            assert 42 in scene.locations
        finally:
            await travel.close()

    asyncio.run(exercise())
