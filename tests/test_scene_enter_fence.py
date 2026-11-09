import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from xuanming.cluster import GATE, SCENE, LocalDirectory
from xuanming.data import PlayerRowCodec, SqlitePlayerStore
from xuanming.node_link import GateLinks, SceneLinks
from xuanming.nodes.scene import SceneNode
from xuanming.protocol import Protocol
from xuanming.transport import RpcError


@asynccontextmanager
async def _edge(tmp_path):
    p, directory = Protocol(), LocalDirectory()
    gate_info = p.message("NodeInfo", node_type=GATE, zone_id=1, node_id=5, node_uuid="enter-gate")
    scene_info = p.message("NodeInfo", node_type=SCENE, zone_id=1, node_id=6, node_uuid="enter-scene")
    directory.publish(gate_info)
    links = SceneLinks(p, scene_info, directory)
    scene = SceneNode(p, scene_info, SqlitePlayerStore(tmp_path / "entry.sqlite3"), PlayerRowCodec(p), links.emit)
    for scene_id in (1, 2):
        scene.create(p.message("CreateSceneRequest", scene_id=scene_id, config_id=1))
    links.bind(scene)
    gate = GateLinks(p, gate_info, directory, lambda *_: None)

    async def enter(session_id, generation, *, scene_id=1, epoch=1, revision=1):
        request = p.message(
            "PlayerEnterGameNodeRequest", player_id=1, session_id=session_id, scene_id=scene_id,
            login_generation=generation, owner_epoch=epoch, owner_revision=revision,
        )
        await gate.call(6, p.message_ids["ScenePlayerEnterGameNode"], request)
        return scene.locations[1].players[1]

    async with await asyncio.start_server(links.serve, "127.0.0.1", 0) as server:
        scene_info.endpoint.ip, scene_info.endpoint.port = server.sockets[0].getsockname()
        directory.publish(scene_info)
        try:
            yield p, scene, gate, enter
        finally:
            scene.travel = None
            await gate.close()
            await links.close()
            await scene.close()


def test_same_session_late_entry_moves_scene_without_lowering_login_fence(tmp_path):
    async def exercise():
        async with _edge(tmp_path) as (p, scene, gate, enter):
            original, replacement = (5 << 17) | 1, (5 << 17) | 2
            actor = await enter(original, 8)
            entity, snapshot = actor.entity, actor.snapshot
            assert await enter(original, 5, scene_id=2) is actor
            assert scene.locations[1] is scene.worlds[2]
            assert actor.generation == 8 and actor.entity == entity and actor.snapshot is snapshot
            assert 1 not in scene.worlds[1].players
            assert scene.sessions == {("enter-gate", original): actor}

            # 低代次重试不能把最高代次降到 5，给另一个会话的代次 6 开门。
            for generation in (6, 8):
                with pytest.raises(RpcError):
                    await enter(replacement, generation, scene_id=2)
                assert actor.generation == 8 and actor.session_id == original
                assert scene.locations[1] is scene.worlds[2]
            assert await enter(original, 3, scene_id=2) is actor
            assert actor.generation == 8

            assert await enter(replacement, 9, scene_id=2) is actor
            assert scene.sessions == {("enter-gate", replacement): actor}
            with pytest.raises(RpcError):
                await enter(original, 8)
            assert await enter(replacement, 7) is actor
            assert actor.generation == 9 and actor.session_id == replacement
            assert scene.locations[1] is scene.worlds[1]

            request = p.message(
                "ProcessClientPlayerMessageRequest", player_id=1, session_id=replacement,
                message_content=p.content(p.message_ids["SceneSceneClientPlayerSceneInfoC2S"], 7,
                                          body=p.message("SceneInfoRequest").SerializeToString()),
            )
            response = p.parse("ProcessClientPlayerMessageResponse", await gate.call(
                6, p.message_ids["SceneProcessClientPlayerMessage"], request,
            ))
            assert response.message_content.error_message.id == 0

    asyncio.run(exercise())


def test_same_session_late_entry_cannot_resume_a_disconnected_session(tmp_path):
    async def exercise():
        async with _edge(tmp_path) as (p, scene, gate, enter):
            session = (5 << 17) | 1
            actor = await enter(session, 8)
            await gate.call(6, p.message_ids["SceneProcessClientPlayerMessage"], p.message(
                "ProcessClientPlayerMessageRequest", player_id=1, session_id=session,
                message_content=p.content(p.message_ids["ScenePlayerExitGame"], 1, body=p.message(
                    "GameNodeExitGameRequest", reason=2,
                ).SerializeToString()),
            ))
            assert scene.connection_holds.held(actor) and not scene.sessions
            for generation in (7, 8):
                with pytest.raises(RpcError):
                    await enter(session, generation)
                assert scene.connection_holds.held(actor) and actor.generation == 8 and not scene.sessions
            assert await enter(session, 9) is actor
            assert actor.generation == 9 and not scene.connection_holds.held(actor)
            assert scene.sessions["enter-gate", session] is actor

    asyncio.run(exercise())


def test_entry_fence_rejects_before_published_handoff_has_any_side_effect(tmp_path):
    async def exercise():
        async with _edge(tmp_path) as (_, scene, _, enter):
            session = (5 << 17) | 1
            actor = await enter(session, 8)
            snapshot, world = actor.snapshot, scene.locations[1]
            calls = []
            authority = SimpleNamespace(revision=1, confirmed=True)
            admitted = SimpleNamespace(identity=None)

            async def retire(*args, **kwargs):
                calls.append("retire")

            async def admit(request):
                calls.append("admit")
                if request.owner_revision != authority.revision:
                    raise ValueError("账号修订未获授权")
                return admitted

            async def confirm(admission):
                assert admission is admitted
                calls.append("confirm")
                if not authority.confirmed:
                    raise ValueError("新入场资格已失效")

            scene.travel = SimpleNamespace(
                sealed=lambda _: False, published=lambda _: True,
                superseded=lambda _: calls.append("superseded"), retire=retire, admit=admit, confirm=confirm,
            )
            # 更高归属 epoch 不能绕过登录围栏；仍在本代的请求继续校验账号 revision。
            for incoming, generation, epoch, revision in (
                (session + 1, 7, 2, 1),
                (session + 1, 8, 2, 1),
                (session + 1, 9, 1, 2),
                (session, 8, 1, 2),
                (session, 0, 2, 1),
                (session, 8, 0, 1),
            ):
                with pytest.raises(RpcError):
                    await enter(incoming, generation, epoch=epoch, revision=revision)
                assert calls == []
                assert scene.locations[1] is world and world.players[1] is actor
                assert scene.sessions == {("enter-gate", session): actor}
                assert actor.snapshot is snapshot and actor.generation == 8

            with pytest.raises(RpcError):
                await enter(session + 1, 9, epoch=2, revision=2)
            assert calls == ["admit"]
            assert scene.locations[1] is world and world.players[1] is actor
            assert scene.sessions == {("enter-gate", session): actor} and actor.snapshot is snapshot

            calls.clear()
            authority.revision, authority.confirmed = 2, False
            with pytest.raises(RpcError):
                await enter(session + 1, 9, epoch=2, revision=2)
            assert calls == ["admit", "confirm", "retire"]
            assert scene.locations[1] is world and world.players[1] is actor
            assert scene.sessions == {("enter-gate", session): actor} and actor.snapshot is snapshot

            # 只有新账号修订获权威确认后才废黜；载入复用同一 admission，不二次注册。
            calls.clear()
            authority.confirmed = True
            replacement = await enter(session + 1, 9, epoch=2, revision=2)
            assert replacement is not actor and replacement.generation == 9
            assert replacement.owner_revision == 2 and replacement.snapshot.owner_epoch == 2
            assert calls == ["admit", "confirm", "superseded", "retire", "confirm", "confirm"]
            assert scene.sessions == {("enter-gate", session + 1): replacement}

    asyncio.run(exercise())
