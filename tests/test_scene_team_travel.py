import asyncio
from types import SimpleNamespace

import pytest

from xuanming.data import ConfigTables, PlayerSnapshot
from xuanming.nodes.scene_travel import SceneTravel
from xuanming.protocol import Protocol


class _UnusedPeer:
    async def close(self):
        pass

    async def aclose(self):
        pass


@pytest.mark.parametrize("outcome", (0, 17, 18, "unavailable"))
def test_automatic_team_scene_change_never_upgrades_to_ownership_handoff(outcome):
    async def exercise():
        protocol = Protocol()
        player = SimpleNamespace(
            player_id=42, session_id=1 << 17 | 9, generation=12, owner_revision=7, gate_uuid="gate-a",
            admission=object(), disconnect_at=None, motion=SimpleNamespace(velocity=(2., 0., 0.)),
            snapshot=PlayerSnapshot(42, b"scene-a", 8, 3, {}),
        )
        world = SimpleNamespace(players={42: player}, info=protocol.message("SceneInfoComp", scene_id=23))
        called, respond = asyncio.Event(), asyncio.Event()
        calls, notifications, handoffs = [], [], []

        class Manager(_UnusedPeer):
            async def call(self, path, payload):
                assert path == "/scene_manager.SceneManager/EnterScene"
                request = protocol.parse("scene_manager.EnterSceneRequest", payload)
                calls.append(request)
                called.set()
                await respond.wait()
                if outcome == "unavailable":
                    raise OSError("simulated manager transport failure")
                return protocol.message(
                    "scene_manager.EnterSceneResponse", player_id=request.player_id,
                    correlation_id=request.correlation_id, error_code=outcome,
                ).SerializeToString()

        scene = SimpleNamespace(
            protocol=protocol, info=protocol.message("NodeInfo", node_id=2, zone_id=1, node_uuid="scene-a"),
            locations={42: world}, battle=None, register=lambda *_: None,
            _push=lambda *_: notifications.append("push"),
        )
        travel = SceneTravel(scene, ConfigTables(), Manager(), _UnusedPeer(), _UnusedPeer(), b"x" * 32)

        async def handoff(trip, failure):
            handoffs.append((trip, failure))

        travel._handoff = handoff
        try:
            snapshot = player.snapshot
            travel.follow_scene(player, 24)
            await asyncio.wait_for(called.wait(), 1)
            request = calls[0]
            assert (request.player_id, request.scene_id, request.zone_id) == (42, 24, 1)
            assert (request.session_id, request.gate_id, request.gate_instance_id) == (player.session_id, "1", "gate-a")
            assert (request.login_generation, request.owner_revision) == (12, 7)
            assert request.correlation_id and not request.client_channel_pick and not request.request_id
            assert not travel.frozen(42)
            travel.follow_scene(player, 25)
            rejected = travel.change_scene(world, player, None, protocol.message(
                "EnterSceneC2SRequest", scene_info={"scene_id": 26},
            ))
            assert rejected.error_message.id == 3014
            assert len(calls) == 1
            respond.set()
            await asyncio.gather(*tuple(travel._tasks))
            assert not handoffs and not notifications and not travel.frozen(42)
            assert player.snapshot is snapshot and player.motion.velocity == (2., 0., 0.)
            assert scene.locations[42] is world and world.players[42] is player
            assert 42 not in travel._trips

            called.clear()
            travel.follow_scene(player, 25)
            await asyncio.gather(*tuple(travel._tasks))
            assert len(calls) == 2 and calls[1].scene_id == 25
            assert calls[1].correlation_id != request.correlation_id
            assert not handoffs and not notifications
        finally:
            respond.set()
            await travel.close()

    asyncio.run(exercise())


def test_player_requested_scene_change_still_hands_off_after_manager_18():
    async def exercise():
        protocol = Protocol()
        player = SimpleNamespace(
            player_id=42, session_id=1 << 17 | 9, generation=12, owner_revision=7, gate_uuid="gate-a",
            admission=object(), disconnect_at=None, motion=SimpleNamespace(velocity=(2., 0., 0.)),
            snapshot=PlayerSnapshot(42, b"scene-a", 8, 3, {}),
        )
        world = SimpleNamespace(players={42: player}, info=protocol.message("SceneInfoComp", scene_id=23))
        lock = asyncio.Lock()
        handoffs = []

        class Manager(_UnusedPeer):
            async def call(self, path, payload):
                request = protocol.parse("scene_manager.EnterSceneRequest", payload)
                assert request.scene_id == 24 and request.client_channel_pick
                return protocol.message(
                    "scene_manager.EnterSceneResponse", player_id=42,
                    correlation_id=request.correlation_id, error_code=18,
                ).SerializeToString()

        scene = SimpleNamespace(
            protocol=protocol, info=protocol.message("NodeInfo", node_id=2, zone_id=1, node_uuid="scene-a"),
            locations={42: world}, battle=None, register=lambda *_: None, _lock=lambda _: lock,
        )
        travel = SceneTravel(scene, ConfigTables(), Manager(), _UnusedPeer(), _UnusedPeer(), b"x" * 32)

        async def handoff(trip, failure):
            assert trip.frozen and trip.zone == 1 and trip.scene_id == 24 and not trip.silent
            assert failure == 3023 and player.motion.velocity == (0., 0., 0.)
            handoffs.append(trip)

        travel._handoff = handoff
        try:
            response = travel.change_scene(world, player, None, protocol.message(
                "EnterSceneC2SRequest", scene_info={"scene_id": 24},
            ))
            assert not response.error_message.id
            await asyncio.gather(*tuple(travel._tasks))
            assert len(handoffs) == 1 and 42 not in travel._trips
        finally:
            await travel.close()

    asyncio.run(exercise())
