import asyncio
import hmac
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

import xuanming.node_link as node_link
from xuanming.data import PlayerRowCodec, PlayerRowKind, PlayerSnapshot
from xuanming.nodes.scene import Motion, PendingSave, Player, Transform
from xuanming.nodes.scene_admin import MAX_DESCRIPTION_BYTES, MAX_VALUE_BYTES, SceneAdmin
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.settings import Settings
from xuanming.smoke import connect
from xuanming.transport import RpcChannel, RpcError, RpcErrorCode

_SECRET = b"scene-admin-test-signing-key"
_TOKEN = "scene-admin-login-test"


def _settings(tmp_path):
    defaults = Settings()
    return replace(
        defaults, database=tmp_path / "scene-admin.sqlite3",
        gate=replace(defaults.gate, port=0), scene=replace(defaults.scene, port=0, grpc_port=0),
        battle=replace(defaults.battle, port=0, grpc_port=0),
    )


def _sign(request, node_id, method, nonce, *, timestamp=None):
    stamp = str(int(time.time()) if timestamp is None else timestamp)
    canonical = "\n".join((method, str(node_id), "测试运维", stamp, nonce, request.reason)).encode()
    signature = hmac.digest(_SECRET, canonical, "sha256").hex()
    setattr(request, "operator", "|".join(("测试运维", stamp, nonce, signature)))
    return request


async def _admin_channel(runtime):
    reader, writer = await asyncio.open_connection(runtime.scene.info.endpoint.ip, runtime.scene.info.endpoint.port)
    return RpcChannel(reader, writer, runtime.protocol.message_class("GameRpcMessage"))


async def _enter(runtime, client):
    p = runtime.protocol
    await client.send(runtime.gate.issue_ticket())
    assert (await client.receive("ClientTokenVerifyResponse")).success
    assert not (await client.rpc(p.message_ids["ClientPlayerLoginLogin"], 1, p.message(
        "loginpb.LoginRequest", account="local", password=_TOKEN,
    ))).error_message.id
    assert (await client.rpc(p.message_ids["ClientPlayerLoginEnterGame"], 2, p.message(
        "loginpb.EnterGameRequest", player_id=1, request_id="scene-admin-entry",
    ))).player_id == 1
    await client.content(p.message_ids["SceneSceneClientPlayerNotifyEnterScene"])
    return runtime.scene.locations[1].players[1]


def test_scene_admin_description_verifies_target_and_keeps_state_read_only(tmp_path, monkeypatch):
    monkeypatch.setenv("GATE_GM_ADMIN_SECRET", _SECRET.decode())

    async def exercise():
        async with Runtime(_settings(tmp_path), "local", dev_token=_TOKEN) as runtime:
            p, scene = runtime.protocol, runtime.scene
            async with connect(p, runtime.gate.info.endpoint) as client:
                player = await _enter(runtime, client)
                monkeypatch.setattr(node_link, "_HANDSHAKE_TIMEOUT", .15)
                admin = await _admin_channel(runtime)
                message_id = p.message_ids["SceneSceneGmDescribeEntity"]
                request = p.message("GmDescribeEntityRequest", player_id=1, reason="1")
                before, motion = player.snapshot, player.motion
                position = tuple(value + .1 for value in player.transform.position)
                scene._move_position(scene.locations[1], player, position)
                try:
                    with pytest.raises(RpcError) as rejected:
                        await admin.call(message_id, request.SerializeToString())
                    assert rejected.value.code == RpcErrorCode.HANDLER_FAILED
                    _sign(request, scene.info.node_id, "Gate.GmDescribeEntity", "cross-method")
                    with pytest.raises(RpcError):
                        await admin.call(message_id, request.SerializeToString())
                    _sign(request, scene.info.node_id + 1, "Scene.GmDescribeEntity", "cross-node")
                    with pytest.raises(RpcError):
                        await admin.call(message_id, request.SerializeToString())
                    _sign(request, scene.info.node_id, "Scene.GmDescribeEntity", "expired", timestamp=int(time.time()) - 1000)
                    with pytest.raises(RpcError):
                        await admin.call(message_id, request.SerializeToString())

                    _sign(request, scene.info.node_id, "Scene.GmDescribeEntity", "types")
                    description = p.parse("GmDescribeEntityResponse", await admin.call(
                        message_id, request.SerializeToString(),
                    )).description
                    assert f"entity={player.entity} valid=1 components=[Player,Motion," in description
                    assert "Transform" in description and "LevelComp" in description and "\n" not in description
                    assert player.snapshot is before and player.pending_save is None and player.motion is motion
                    with pytest.raises(RpcError) as rejected:
                        await admin.call(message_id, request.SerializeToString())
                    assert rejected.value.code == RpcErrorCode.HANDLER_FAILED

                    await asyncio.sleep(.2)
                    request.include_values = True
                    _sign(request, scene.info.node_id, "Scene.GmDescribeEntity", "values")
                    description = p.parse("GmDescribeEntityResponse", await admin.call(
                        message_id, request.SerializeToString(),
                    )).description
                    transform_line = next(line for line in description.splitlines() if line.startswith("Transform="))
                    assert "location" in transform_line and f"{position[0]}" in transform_line
                    assert player.snapshot is before and player.pending_save is None
                    assert runtime._shutdown_requested.is_set() is False
                    with pytest.raises(RpcError) as rejected:
                        await admin.call(p.message_ids["SceneCreateScene"], p.message(
                            "CreateSceneRequest", scene_id=998, config_id=1,
                        ).SerializeToString())
                    assert rejected.value.code == RpcErrorCode.NO_SERVICE and 998 not in scene.worlds

                    request.player_id = 2
                    _sign(request, scene.info.node_id, "Scene.GmDescribeEntity", "bad-target")
                    with pytest.raises(RpcError) as rejected:
                        await admin.call(message_id, request.SerializeToString())
                    assert rejected.value.code == RpcErrorCode.HANDLER_FAILED
                    request.reason = "2"
                    _sign(request, scene.info.node_id, "Scene.GmDescribeEntity", "missing")
                    response = await admin.call(message_id, request.SerializeToString())
                    assert p.parse("GmDescribeEntityResponse", response).description == "player not on this node"
                finally:
                    await admin.aclose()

    asyncio.run(exercise())


def test_scene_admin_description_bounds_unicode_and_omits_transient_credentials():
    p = Protocol()
    data = p.message("player_database", profile_component={"name": "界" * 4000})
    scene = SimpleNamespace(protocol=p, rows=SimpleNamespace(assemble=lambda _snapshot: data))
    actor = Player(
        17, 1, 9, "private-gate-instance-marker", 2, 1, PlayerSnapshot(1, b"scene", 1, 0, {}),
        Transform((1., 2., 3.)), motion=Motion(), admission=SimpleNamespace(secret="private-admission-marker"),
    )
    description, _ = SceneAdmin(scene, lambda: None)._description(actor, True)
    assert len(description.encode()) <= MAX_DESCRIPTION_BYTES
    assert "private-admission-marker" not in description and "private-gate-instance-marker" not in description
    value = next(line.removeprefix("PlayerProfileComp=") for line in description.splitlines()
                 if line.startswith("PlayerProfileComp="))
    assert len(value.encode()) <= MAX_VALUE_BYTES and "界" in value


def test_scene_admin_description_merges_pending_partial_rows_without_saving():
    p = Protocol()
    rows = PlayerRowCodec(p)
    data = p.message("player_database", profile_component={"name": "已有角色"}, currency={"values": [10, 0, 0]})
    snapshot = PlayerSnapshot(1, b"scene", 1, 5, rows.project(data))
    pending = rows.project(p.message("player_database", currency={"values": [99, 0, 0]}))
    actor = Player(17, 1, 9, "gate", 2, 1, snapshot, Transform((1., 2., 3.)))
    actor.pending_save = PendingSave({PlayerRowKind.WALLET: pending[PlayerRowKind.WALLET]}, False, False, False)
    scene = SimpleNamespace(protocol=p, rows=rows)
    description, _ = SceneAdmin(scene, lambda: None)._description(actor, True)
    assert "PlayerProfileComp=name: \"已有角色\"" in description
    assert "CurrencyComp=values: 99 values: 0 values: 0" in description
    assert actor.snapshot is snapshot and actor.snapshot.save_seq == 5
    assert rows.assemble(snapshot).currency.values[0] == 10
    assert len(actor.pending_save.rows) == 1


def test_scene_admin_shutdown_replies_then_persists_held_actor(tmp_path, monkeypatch):
    monkeypatch.setenv("GATE_GM_ADMIN_SECRET", _SECRET.decode())

    async def exercise():
        started = asyncio.get_running_loop().create_future()

        async def lifetime():
            async with Runtime(_settings(tmp_path), "local", dev_token=_TOKEN) as runtime:
                started.set_result(runtime)
                await runtime.wait()

        serving = asyncio.create_task(lifetime())
        try:
            async with asyncio.timeout(8):
                runtime = await asyncio.shield(started)
                p, scene = runtime.protocol, runtime.scene
                async with connect(p, runtime.gate.info.endpoint) as client:
                    player = await _enter(runtime, client)
                    scene._move_position(scene.locations[1], player, (181., 200., 0.))
                while not scene.connection_holds.held(player):
                    await asyncio.sleep(.01)
                admin = await _admin_channel(runtime)
                try:
                    request = p.message("GmGracefulShutdownRequest", reason="maintenance")
                    _sign(request, scene.info.node_id, "Gate.GmGracefulShutdown", "wrong-method")
                    with pytest.raises(RpcError) as rejected:
                        await admin.call(p.message_ids["SceneSceneGmGracefulShutdown"], request.SerializeToString())
                    assert rejected.value.code == RpcErrorCode.HANDLER_FAILED
                    assert not runtime._shutdown_requested.is_set()
                    _sign(request, scene.info.node_id, "Scene.GmGracefulShutdown", "shutdown")
                    reply = await admin.call(p.message_ids["SceneSceneGmGracefulShutdown"], request.SerializeToString())
                    assert p.parse("GmGracefulShutdownResponse", reply).affected_count == 1
                    await serving
                    assert player.snapshot.released and not scene.locations
                    snapshot = await scene.store.load_and_claim(1, b"admin-verifier")
                    assert scene.rows.assemble(snapshot).transform.location.x == 181.
                    await scene.store.release(snapshot)
                finally:
                    await admin.aclose()
        finally:
            if not serving.done():
                serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)

    asyncio.run(exercise())
