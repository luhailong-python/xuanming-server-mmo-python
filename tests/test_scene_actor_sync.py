import asyncio
from types import SimpleNamespace

from xuanming.cluster import GATE, SCENE, LocalDirectory
from xuanming.node_link import GateLinks, SceneLinks
from xuanming.nodes.scene import Motion, Transform
from xuanming.nodes.scene_actor_sync import SceneActorSync
from xuanming.protocol import Protocol


def test_actor_delta_waits_for_visible_observers_and_coalesces_latest_fields():
    protocol = Protocol()
    actor = SimpleNamespace(entity=71, player_id=902, transform=Transform(), motion=Motion())
    watcher = object()
    world = SimpleNamespace(actors={actor.entity: actor})
    observers = []
    delivered = []
    scene = SimpleNamespace(protocol=protocol, locations={902: world}, travel=None, buffs=None,
                            watchers=lambda *_: iter(observers), emit=lambda *args: delivered.append(args))
    sync = SceneActorSync(scene)
    sync.mark(actor, "transform")
    sync.tick()
    assert not delivered

    observers.append(watcher)
    actor.transform.position = (4.0, 5.0, 6.0)
    actor.motion.velocity = (1.0, 2.0, 3.0)
    sync.mark(actor, "velocity", "transform")
    sync.tick()
    assert not delivered
    actor.motion.velocity = (3.0, 2.0, 1.0)
    sync.tick()

    assert [receiver for receiver, _ in delivered] == [watcher]
    content = delivered[0][1]
    assert content.message_id == 66
    message = protocol.parse("ActorBaseAttributesS2C", content.serialized_message)
    assert message.entity_id == 902
    assert message.transform.location.z == 6.0 and message.velocity.x == 3.0
    assert not message.HasField("combat_state_flags")
    sync.tick()
    sync.tick()
    assert len(delivered) == 1


def test_actor_delta_honors_source_freeze_and_discards_stale_entities():
    protocol = Protocol()
    actor = SimpleNamespace(entity=71, player_id=902, transform=Transform(), motion=Motion())
    npc = SimpleNamespace(entity=72, transform=Transform())
    world = SimpleNamespace(actors={71: actor, 72: npc})
    npc.world = world
    frozen = {902}
    flags = {1: True}
    delivered = []
    scene = SimpleNamespace(
        protocol=protocol, locations={902: world}, travel=SimpleNamespace(frozen=frozen.__contains__),
        buffs=SimpleNamespace(state_flags=lambda _: flags), watchers=lambda *_: [object()],
        emit=lambda _, content: delivered.append(protocol.parse("ActorBaseAttributesS2C", content.serialized_message)),
    )
    sync = SceneActorSync(scene)
    sync.mark(actor, "velocity")
    sync.mark(npc, "combat_state_flags")
    sync.tick()
    assert len(delivered) == 1 and delivered[0].entity_id == 0
    assert dict(delivered[0].combat_state_flags.state_flags) == {1: True}

    flags.clear()
    sync.mark(npc, "combat_state_flags")
    frozen.clear()
    sync.tick()
    sync.tick()
    assert [message.entity_id for message in delivered] == [0, 902, 0]
    assert delivered[1].HasField("velocity") and not delivered[1].HasField("transform")
    assert delivered[2].HasField("combat_state_flags") and not delivered[2].combat_state_flags.state_flags

    sync.mark(npc, "transform")
    del world.actors[npc.entity]
    sync.tick()
    sync.tick()
    assert len(delivered) == 3 and not sync._dirty


def test_actor_delta_fanout_groups_by_authenticated_gate_and_preserves_target_identity():
    async def exercise():
        p, directory = Protocol(), LocalDirectory()
        scene_info = p.message("NodeInfo", node_type=SCENE, node_id=6, zone_id=1, node_uuid="scene")
        links = SceneLinks(p, scene_info, directory)

        async def enter(*_):
            return p.message("Empty")

        def gate_info(node_id):
            return p.message("NodeInfo", node_type=GATE, node_id=node_id, zone_id=1, node_uuid=f"gate-{node_id}")

        def player(gate, sequence, player_id):
            return SimpleNamespace(gate_uuid=gate.node_uuid, session_id=(gate.node_id << 17) + sequence,
                                   player_id=player_id)

        links.bind(SimpleNamespace(enter=enter))
        gates = [gate_info(3), gate_info(4)]
        requests = [asyncio.Queue(), asyncio.Queue()]
        peers = []
        for gate, pushes in zip(gates, requests):
            directory.publish(gate)
            peers.append(GateLinks(p, gate, directory, lambda _: None,
                                   on_broadcast=lambda *args, pushes=pushes: pushes.put_nowait(args)))
        async with await asyncio.start_server(links.serve, "127.0.0.1", 0) as server:
            scene_info.endpoint.ip, scene_info.endpoint.port = server.sockets[0].getsockname()
            directory.publish(scene_info)
            try:
                for gate, peer in zip(gates, peers):
                    target = player(gate, 1, 1)
                    await peer.call(6, p.message_ids["ScenePlayerEnterGameNode"], p.message(
                        "PlayerEnterGameNodeRequest", session_id=target.session_id, player_id=target.player_id,
                    ), timeout=2)

                dense = [player(gates[0], index, index + 100) for index in range(1, 33)]
                sparse = [player(gates[1], 59, 501), player(gates[1], 2, 502)]
                duplicate = player(gates[0], 1, 999)
                crossed = SimpleNamespace(gate_uuid=gates[0].node_uuid, session_id=sparse[0].session_id,
                                          player_id=800)
                actor = SimpleNamespace(entity=71, player_id=902, transform=Transform())
                world = SimpleNamespace(actors={71: actor})
                scene = SimpleNamespace(protocol=p, locations={902: world}, travel=None,
                                        watchers=lambda *_: [*reversed(dense), *sparse, duplicate, crossed])
                sync = SceneActorSync(scene, emit_many=links.emit_many)
                sync.mark(actor, "transform")
                sync.tick()
                async with asyncio.timeout(2):
                    method1, req1 = await requests[0].get()
                    method2, req2 = await requests[1].get()
                assert method1 == method2 == "BroadcastToPlayers"
                assert not req1.session_list and req1.session_bitmap == b"\xff" * 4
                assert req1.session_bitmap_base == dense[0].session_id
                assert list(req1.player_list) == [item.player_id for item in dense]
                assert not req2.session_bitmap
                assert list(req2.session_list) == [item.session_id for item in reversed(sparse)]
                assert list(req2.player_list) == [item.player_id for item in reversed(sparse)]
                assert req1.message_content == req2.message_content
                assert req1.message_content.message_id == 66
                assert p.parse("ActorBaseAttributesS2C", req1.message_content.serialized_message).entity_id == 902
            finally:
                await asyncio.gather(*(peer.close() for peer in peers))
                await links.close()

    asyncio.run(exercise())
