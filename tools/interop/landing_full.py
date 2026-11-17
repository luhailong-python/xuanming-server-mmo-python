"""Verify the original landing digest and wake contract over Redis and client TCP."""

import asyncio

from redis.asyncio import Redis

from smoke_original import endpoint


async def subscribe(stack):
    redis = Redis.from_url(f"redis://{endpoint('redis')}/0")
    stack.push_async_callback(redis.aclose)
    pubsub = await stack.enter_async_context(redis.pubsub())
    await pubsub.subscribe("assetop:wake")
    async with asyncio.timeout(4):
        while not (event := await pubsub.get_message(timeout=0.2)) or event["type"] != "subscribe":
            pass
    return redis, pubsub


async def verify_landing(session, peers):
    redis, pubsub = peers
    player_id = session.player_id
    async with asyncio.timeout(4):
        while True:
            wake = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
            if wake and wake["data"] == str(player_id).encode():
                assert wake["channel"] == b"assetop:wake"
                break

    client, key = session.client, f"player:{{{player_id}}}:sync_digest"
    p = client.protocol

    async def digest():
        await client.rpc(p.message_ids["SceneSceneClientPlayerGetSyncDigest"], 0, p.message("GetSyncDigestRequest"))
        content = await client.content(p.message_ids["SceneSceneClientPlayerNotifySyncDigest"])
        return p.parse("SyncDigestS2C", content.serialized_message)

    empty = await digest()
    assert empty.status == 0 and not empty.generation and not empty.entries
    await redis.hset(key, mapping={"g": 110, "1": 4, "4": 12, "03": "not a known domain", "999": "future"})
    answer = await digest()
    assert answer.status == 0 and answer.generation == 110
    assert {row.domain: row.version for row in answer.entries} == {1: 4, 4: 12}
    await redis.hset(key, "4", "-1")
    unavailable = await digest()
    assert unavailable.status == 1 and not unavailable.generation and not unavailable.entries
    await redis.delete(key)
    print("PASS Scene landing digest over TCP and native Redis asset wake", flush=True)
    return {"asset_wake": True, "missing_digest": True, "known_domains": [1, 4], "malformed_digest_fallback": True}


async def verify_role_panels(session, runtime):
    from data_full import read_native_player

    client, player_id = session.client, session.player_id
    p = client.protocol
    attribute = await client.rpc(p.message_ids["SceneAttributeClientPlayerGetAttributePanel"], 75,
                                 p.message("GetAttributePanelRequest"))
    pets = await client.rpc(p.message_ids["ScenePetClientPlayerGetPetList"], 76, p.message("GetPetListRequest"))
    assert not attribute.error_message.id and attribute.HasField("panel")
    assert not pets.error_message.id and pets.HasField("pets")
    saved = runtime.scene.rows.assemble(await read_native_player(player_id))
    assert saved.attribute_component.active_scheme_id == 1 and len(saved.attribute_component.schemes) == 1
    assert not saved.pet_component.pets
    print("PASS native account read the Python attribute and pet panels over TCP", flush=True)
    return {"attribute_panel": True, "pet_list": True, "native_sql_default_scheme": 1}


async def verify_channel_directory(session, runtime, peers):
    client, redis = session.client, peers[0]
    protocol, scene = client.protocol, runtime.scene
    world = scene.locations[session.player_id]
    config_id = world.info.scene_config_id
    key = f"world_channel_directory:{{ctl:{scene.info.zone_id}}}:{config_id}"
    async with asyncio.timeout(15):
        while not await redis.get(key):
            await asyncio.sleep(0.2)
    await client.rpc(protocol.message_ids["SceneSceneClientPlayerSceneInfoC2S"], 0,
                     protocol.message("SceneInfoRequest", with_channel_directory=True))
    content = await client.content(protocol.message_ids["SceneSceneClientPlayerNotifySceneInfo"])
    info = protocol.parse("SceneInfoS2C", content.serialized_message)
    assert info.HasField("channel_directory")
    directory = info.channel_directory
    assert directory.zone_id == scene.info.zone_id and directory.scene_config_id == config_id
    assert world.info.scene_id in {channel.scene_id for channel in directory.channels}
    assert directory.max_players_per_channel > 0
    print("PASS native Go SceneManager directory via real Redis and Python Scene TCP", flush=True)
    return {"native_scene_manager": True, "redis_key": key, "scene_config_id": config_id,
            "channels": len(directory.channels)}
