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
def test_automatic_team_scene_change_hands_off_only_when_required(outcome):
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
            _push=lambda *_: notifications.append("push"), _lock=lambda _: asyncio.Lock(),
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
            assert len(handoffs) == int(outcome == 18) and not notifications and not travel.frozen(42)
            assert player.snapshot is snapshot
            assert player.motion.velocity == ((0., 0., 0.) if outcome == 18 else (2., 0., 0.))
            assert scene.locations[42] is world and world.players[42] is player
            assert 42 not in travel._trips

            called.clear()
            travel.follow_scene(player, 25)
            await asyncio.gather(*tuple(travel._tasks))
            assert len(calls) == 2 and calls[1].scene_id == 25
            assert calls[1].correlation_id != request.correlation_id
            assert len(handoffs) == 2 * int(outcome == 18) and not notifications
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


def test_hosted_team_follow_uses_offline_placement_and_handoff():
    async def exercise():
        protocol = Protocol()
        player = SimpleNamespace(
            player_id=42, session_id=1 << 17 | 9, generation=12, owner_revision=7, gate_uuid="gate-a",
            admission=object(), disconnect_at=100., motion=SimpleNamespace(velocity=(2., 0., 0.)),
            snapshot=PlayerSnapshot(42, b"scene-a", 8, 3, {}),
        )
        world = SimpleNamespace(players={42: player})
        eligible = [True]
        requests, handoffs, presence = [], [], []

        class Manager(_UnusedPeer):
            async def call(self, path, payload):
                request = protocol.parse("scene_manager.EnterSceneRequest", payload)
                requests.append(request)
                return protocol.message("scene_manager.EnterSceneResponse", player_id=42,
                                        correlation_id=request.correlation_id, error_code=18).SerializeToString()

        class Redis(_UnusedPeer):
            async def set(self, key, value, *, ex):
                presence.append((key, value, ex))

        scene = SimpleNamespace(
            protocol=protocol, info=protocol.message("NodeInfo", node_id=2, zone_id=1, node_uuid="scene-a"),
            locations={42: world}, battle=None, register=lambda *_: None, _lock=lambda _: asyncio.Lock(),
            connection_holds=SimpleNamespace(placement_eligible=lambda actor: eligible[0],
                                             placement_expiry=lambda actor: 250, unix_clock=lambda: 98),
            _releasing=lambda actor: False,
        )
        travel = SceneTravel(scene, ConfigTables(), Manager(), Redis(), _UnusedPeer(), b"x" * 32)

        async def handoff(trip, failure):
            assert trip.hosted and trip.frozen and travel._trip_ready(trip)
            handoffs.append(trip)

        travel._handoff = handoff
        try:
            eligible[0] = False
            travel.follow_scene(player, 24)
            assert not travel._tasks
            eligible[0] = True
            travel.refresh_hosted_presence(player)
            await asyncio.gather(*tuple(travel._presence_tasks.values()))
            assert presence == [("player:{42}:hosted_presence", "scene-a", 60),
                                ("player:{42}:hosted_return", f"v1:1:7:{player.session_id}:12", 152)]
            travel.follow_scene(player, 24)
            await asyncio.gather(*tuple(travel._tasks))
            assert len(handoffs) == 1 and len(requests) == 1
            request = requests[0]
            assert request.hosted_placement and not request.gate_id and not request.gate_instance_id
            assert (request.session_id, request.login_generation, request.owner_revision) == (player.session_id, 12, 7)
            assert player.motion.velocity == (0., 0., 0.)
        finally:
            await travel.close()

    asyncio.run(exercise())
