"""Two real Scene RPC0 links and a gRPC coordinator without external services."""

import asyncio
from contextlib import AsyncExitStack

import grpc

from test_scene_travel import _Data, _RecordingRedis
from xuanming.cluster import GATE, SCENE, LocalDirectory
from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.grpc_runtime import GrpcClient
from xuanming.node_link import GateLinks, SceneLinks
from xuanming.nodes import _handoff_lua as lua
from xuanming.nodes.gate import GateNode, Session
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_travel import SceneTravel, _keys
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.settings import Settings


class Writer:
    def is_closing(self): return False
    def close(self): pass


def test_planned_shutdown_moves_role_between_scenes_over_rpc0_and_grpc(tmp_path, monkeypatch):
    monkeypatch.setenv("SCENE_PLANNED_DRAIN_SELF_EVACUATE_AFTER_SECONDS", "0")
    monkeypatch.setenv("SCENE_PLANNED_DRAIN_RELOCATIONS_PER_SECOND", "0")

    async def exercise():
        p = Protocol()
        player_id, sid = 42, (5 << 17) | 1
        calls, received = [], []

        class Directory(LocalDirectory):
            lease = 1
            async def update(self, info): self.publish(info)

        directory = Directory()
        gate_info = p.message("NodeInfo", node_type=GATE, zone_id=1, node_id=5, node_uuid="gate")
        source_info = p.message("NodeInfo", node_type=SCENE, zone_id=1, node_id=6, node_uuid="source", admission=1)
        target_info = p.message("NodeInfo", node_type=SCENE, zone_id=1, node_id=7, node_uuid="target", admission=1)
        directory.publish(gate_info)
        class MemoryRedis(_RecordingRedis):
            async def eval(self, script, key_count, *arguments):
                if script == lua.RELEASE:
                    return 1
                return await super().eval(script, key_count, *arguments)

        redis = MemoryRedis(calls)
        await redis.set(_keys(player_id)[0], b"1")
        data = _Data(p, calls)
        gate = GateNode(p, gate_info, b"x" * 32, None)
        gate_links = GateLinks(p, gate_info, directory, gate.push, source_aware=True)
        gate.scene = gate_links
        session = Session(sid, 1, Writer(), verified=True, logged_in=True, player_id=player_id,
                          account="alice", owner_revision=1, login_generation=3, enter_requested=True,
                          enter_player_id=player_id, scene_node_id=6, scene_id=10,
                          scene_instance_id="source", owner_epoch=1)
        gate.sessions[sid] = session

        async def manager(raw, _context):
            request = p.parse("scene_manager.EnterSceneRequest", raw)
            received.append(request)
            assert request.evacuation and (request.scene_id, request.scene_conf_id) == (10, 1)
            assert (request.player_id, request.session_id, request.login_generation) == (player_id, sid, 3)
            await redis.set(_keys(player_id)[0], b"2")
            location = p.message("storage.PlayerLocation", scene_id=20, node_id="7", zone_id=1,
                                 owner_epoch=2, session_id=sid, login_generation=3)
            await redis.set(_keys(player_id)[3], location.SerializeToString())
            event = p.message("contracts.kafka.RoutePlayerEvent", session_id=sid, player_id=player_id,
                              scene_id=20, target_node_id=7, owner_revision=1, owner_epoch=2,
                              login_generation=3, home_zone_id=1)
            await gate.route(event, 0)
            return p.message("scene_manager.EnterSceneResponse", player_id=player_id,
                             correlation_id=request.correlation_id).SerializeToString()

        async with AsyncExitStack() as stack:
            server = grpc.aio.server()
            server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("scene_manager.SceneManager", {
                "EnterScene": grpc.unary_unary_rpc_method_handler(manager),
            }),))
            port = server.add_insecure_port("127.0.0.1:0")
            await server.start()
            stack.push_async_callback(server.stop, 0)
            coordinator = GrpcClient(f"127.0.0.1:{port}", timeout=4, enable_retries=False)
            stack.push_async_callback(coordinator.close)
            stack.push_async_callback(gate_links.close)
            scenes = []
            for info, scene_id in ((source_info, 10), (target_info, 20)):
                links = SceneLinks(p, info, directory)
                scene = SceneNode(p, info, SqlitePlayerStore(tmp_path / "players.sqlite"), PlayerRowCodec(p), links.emit)
                scene.create(p.message("CreateSceneRequest", scene_id=scene_id, config_id=1))
                links.bind(scene)
                scene.travel = SceneTravel(scene, ConfigTables(), coordinator, redis, data, b"x" * 32)
                listener = await stack.enter_async_context(await asyncio.start_server(links.serve, "127.0.0.1", 0))
                info.endpoint.ip, info.endpoint.port = listener.sockets[0].getsockname()
                directory.publish(info)
                stack.push_async_callback(links.close)
                scenes.append(scene)
            source, target = scenes
            await gate_links.call(6, p.message_ids["ScenePlayerEnterGameNode"], p.message(
                "PlayerEnterGameNodeRequest", player_id=player_id, session_id=sid, scene_id=10,
                owner_epoch=1, owner_revision=1, login_generation=3,
            ))
            source.locations[player_id].players[player_id].transform.position = (181., 202., 0.)

            runtime = Runtime(Settings(), "scene")
            runtime.directory, runtime.scene, runtime._scene_drain_budget = directory, source, 8
            runtime._spawn(runtime._scene_loop(), "scene-tick")
            runtime.request_shutdown()
            await asyncio.wait_for(runtime.__aexit__(None, None, None), 12)
            assert len(received) == 1 and not source.locations
            player = target.locations[player_id].players[player_id]
            assert player.snapshot.owner_epoch == 2 and player.snapshot.owner == b"target"
            assert player.transform.position == (181., 202., 0.)
            assert (session.scene_node_id, session.scene_instance_id, session.owner_epoch) == (7, "target", 2)
            await target.close()

    asyncio.run(exercise())
