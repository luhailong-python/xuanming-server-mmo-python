import asyncio
from dataclasses import replace

import xuanming.nodes.scene_channels as channels_module
from xuanming.runtime import Runtime
from xuanming.settings import Settings
from xuanming.smoke import connect

_TOKEN = "channel-directory-test"


class _ControlRedis:
    def __init__(self):
        self.value = None
        self.calls = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    async def get(self, key):
        self.calls.append(key)
        self.started.set()
        await self.release.wait()
        if isinstance(self.value, Exception):
            raise self.value
        return self.value


def _settings(tmp_path):
    defaults = Settings()
    return replace(
        defaults, database=tmp_path / "channels.sqlite3",
        gate=replace(defaults.gate, port=0), scene=replace(defaults.scene, port=0, grpc_port=0),
        battle=replace(defaults.battle, port=0, grpc_port=0),
    )


async def _enter(runtime, client):
    p = runtime.protocol
    await client.send(runtime.gate.issue_ticket())
    assert (await client.receive("ClientTokenVerifyResponse")).success
    assert not (await client.rpc(p.message_ids["ClientPlayerLoginLogin"], 1, p.message(
        "loginpb.LoginRequest", account="local", password=_TOKEN,
    ))).error_message.id
    assert (await client.rpc(p.message_ids["ClientPlayerLoginEnterGame"], 2, p.message(
        "loginpb.EnterGameRequest", player_id=1, request_id="channel-entry",
    ))).player_id == 1
    await client.content(p.message_ids["SceneSceneClientPlayerNotifyEnterScene"])


async def _query(client, *, with_directory=True):
    p = client.protocol
    await client.rpc(p.message_ids["SceneSceneClientPlayerSceneInfoC2S"], 0,
                     p.message("SceneInfoRequest", with_channel_directory=with_directory))
    content = await client.content(p.message_ids["SceneSceneClientPlayerNotifySceneInfo"])
    return p.parse("SceneInfoS2C", content.serialized_message)


async def _change_scene(client, scene_id, call_id):
    p = client.protocol
    response = await client.rpc(p.message_ids["SceneSceneClientPlayerEnterScene"], call_id, p.message(
        "EnterSceneC2SRequest", scene_info={"scene_id": scene_id},
    ))
    assert not response.error_message.id
    content = await client.content(p.message_ids["SceneSceneClientPlayerNotifyEnterScene"])
    assert p.parse("EnterSceneS2C", content.serialized_message).scene_info.scene_id == scene_id


def test_real_tcp_scene_info_forwards_original_directory_only_when_requested(tmp_path):
    async def exercise():
        async with Runtime(_settings(tmp_path), "local", dev_token=_TOKEN) as runtime:
            p, scene, redis = runtime.protocol, runtime.scene, _ControlRedis()
            scene.channels.control_redis = redis
            # 本用例连续检验多种 Redis 返回值，单独放宽测试实例的查询额度。
            runtime.gate._message_limits[p.message_ids["SceneSceneClientPlayerSceneInfoC2S"]] = (100, 1)
            directory = p.message(
                "SceneChannelDirectory", zone_id=1, scene_config_id=1, updated_at_ms=1700000000123,
                switch_cooldown_seconds=10, max_players_per_channel=99, switch_enabled=True,
                channels=[{"scene_id": 1, "channel_no": 1, "player_count": 30, "state": 1},
                          {"scene_id": 9007199254740995, "channel_no": 2, "player_count": 99, "state": 3}],
            )
            redis.value = directory.SerializeToString() + b"\xf8\x07\x01"
            async with connect(p, runtime.gate.info.endpoint) as client:
                await _enter(runtime, client)
                before = scene.locations[1].players[1].snapshot
                response = await _query(client, with_directory=False)
                assert response.scene_info[0].scene_id == 1
                assert not response.HasField("channel_directory") and not redis.calls
                response = await _query(client)
                assert redis.calls == ["world_channel_directory:zone:1:1"]
                assert response.channel_directory.SerializeToString() == redis.value
                assert scene.locations[1].players[1].snapshot is before

                for raw in (None, b"\xff", "not-bytes", {"invalid": "hash"}, OSError("redis unavailable"),
                            p.message("SceneChannelDirectory", scene_config_id=2).SerializeToString()):
                    redis.value = raw
                    response = await _query(client)
                    assert response.scene_info[0].scene_id == 1 and not response.HasField("channel_directory")
                reads = len(redis.calls)
                scene.channels.control_redis = None
                assert not (await _query(client)).HasField("channel_directory")
                assert len(redis.calls) == reads
                scene.channels.control_redis = redis
                for scene_id, fields in ((2, {"dungeon_config_id": 1}), (3, {"mirror_config_id": 1})):
                    scene.create(p.message("CreateSceneRequest", scene_id=scene_id, config_id=1, **fields))
                    await _change_scene(client, scene_id, scene_id + 10)
                    response = await _query(client)
                    assert response.scene_info[0].scene_id == scene_id and not response.HasField("channel_directory")
                    assert len(redis.calls) == reads

    asyncio.run(exercise())


def test_delayed_directory_does_not_lock_player_or_push_after_scene_change(tmp_path):
    async def exercise():
        async with Runtime(_settings(tmp_path), "local", dev_token=_TOKEN) as runtime:
            p, scene, redis = runtime.protocol, runtime.scene, _ControlRedis()
            scene.channels.control_redis = redis
            redis.release.clear()
            redis.value = p.message("SceneChannelDirectory", scene_config_id=1).SerializeToString()
            scene.create(p.message("CreateSceneRequest", scene_id=2, config_id=1))
            async with connect(p, runtime.gate.info.endpoint) as client:
                await _enter(runtime, client)
                sent, emit = [], scene.emit

                def record(player, content):
                    sent.append(content.message_id)
                    emit(player, content)

                scene.emit = record
                await client.rpc(p.message_ids["SceneSceneClientPlayerSceneInfoC2S"], 0,
                                 p.message("SceneInfoRequest", with_channel_directory=True))
                await asyncio.wait_for(redis.started.wait(), 1)
                await _change_scene(client, 2, 3)
                redis.release.set()
                await asyncio.gather(*tuple(scene.channels._pending))
                assert p.message_ids["SceneSceneClientPlayerNotifySceneInfo"] not in sent
                assert (await _query(client)).scene_info[0].scene_id == 2

                redis.started.clear()
                redis.release.clear()
                await client.rpc(p.message_ids["SceneSceneClientPlayerSceneInfoC2S"], 0,
                                 p.message("SceneInfoRequest", with_channel_directory=True))
                await asyncio.wait_for(redis.started.wait(), 1)
                tasks = tuple(scene.channels._pending)
                await scene.channels.close()
                assert tasks and all(task.cancelled() for task in tasks)

    asyncio.run(exercise())


def test_directory_timeout_still_pushes_base_scene_info(tmp_path, monkeypatch):
    monkeypatch.setattr(channels_module, "IO_TIMEOUT_SECONDS", .02)

    async def exercise():
        async with Runtime(_settings(tmp_path), "local", dev_token=_TOKEN) as runtime:
            p, scene, redis = runtime.protocol, runtime.scene, _ControlRedis()
            scene.channels.control_redis = redis
            redis.release.clear()
            async with connect(p, runtime.gate.info.endpoint) as client:
                await _enter(runtime, client)
                response = await _query(client)
                assert response.scene_info[0].scene_id == 1 and not response.HasField("channel_directory")
                assert redis.calls == ["world_channel_directory:zone:1:1"]

    asyncio.run(exercise())
