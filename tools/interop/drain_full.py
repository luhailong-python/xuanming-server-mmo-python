"""Exercise planned shutdown against the isolated native SceneManager stack."""

import asyncio
from dataclasses import replace
import os

from data_full import read_native_player
from scene_full import _read_push, redis_epoch, wallet
from smoke_original import RUN
from xuanming.runtime import Runtime
from xuanming.settings import NodeSettings


async def _target_entry(client, target, player_id):
    # Source teardown may finish before the Gate has consumed the new route.
    # First wait for the target entity, then ignore any earlier scene-change
    # notices still buffered by client RPCs performed before the drain.
    while player_id not in target.scene.locations:
        await asyncio.sleep(0.05)
    world = target.scene.locations[player_id]
    player = world.players[player_id]
    while True:
        _, notice = await _read_push(client, "SceneSceneClientPlayerNotifyEnterScene")
        if notice.scene_info.scene_id == world.info.scene_id:
            break
    assert player.snapshot.owner_epoch > 0 and player.admission is not None
    assert not player.snapshot.released and player.snapshot.owner == target.scene.info.node_uuid.encode()
    return player


async def verify_drain(stack, session, gate, source):
    player_id = session.player_id
    old_scene = source.scene
    old_world = old_scene.locations[player_id]
    old_epoch = old_world.players[player_id].snapshot.owner_epoch
    old_map = old_world.info.scene_config_id
    before = await wallet(session.client, 181)

    # Node 203 has its own topic partition, advertised RPC ports, etcd lease
    # and anomaly journal in the isolated harness. It is started only after
    # login so that the role is initially bound to the source scene.
    settings = replace(source.settings, scene=NodeSettings(203, advertise_host="127.0.0.1"),
                       anomaly_journal=RUN / "scene-drain-anomalies.json")
    target = await stack.enter_async_context(Runtime(settings, "scene"))
    assert target.scene.info.zone_id == old_scene.info.zone_id

    key = "SCENE_PLANNED_DRAIN_SELF_EVACUATE_AFTER_SECONDS"
    previous = os.environ.get(key)
    os.environ[key] = "0"
    try:
        source.request_shutdown()
        # This is the same context teardown used after SIGTERM; the node stays
        # registered and its network transports stay open through handoff.
        async with asyncio.timeout(70):
            await source._interop_lifecycle.aclose()
            player = await _target_entry(session.client, target, player_id)
    finally:
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous

    assert player_id not in old_scene.locations
    assert target.scene.locations[player_id].info.scene_config_id == old_map
    assert player.snapshot.owner_epoch > old_epoch
    assert await redis_epoch(player_id) == player.snapshot.owner_epoch
    bound = [item for item in gate.gate.sessions.values() if item.player_id == player_id]
    assert len(bound) == 1
    assert (bound[0].scene_node_id, bound[0].scene_instance_id) == (
        target.scene.info.node_id, target.scene.info.node_uuid)
    assert await wallet(session.client, 182) == before
    saved = await read_native_player(player_id)
    assert (saved.owner, saved.owner_epoch, saved.released) == (
        target.scene.info.node_uuid.encode(), player.snapshot.owner_epoch, False)
    result = dict(source_node=old_scene.info.node_id, target_node=target.scene.info.node_id,
                  source_epoch=old_epoch, target_epoch=player.snapshot.owner_epoch,
                  scene_config_id=old_map, wallet=before, mysql_save_seq=saved.save_seq,
                  source_shutdown=True, gate_routed=True, native_mysql_verified=True)
    print(f"PASS Scene planned shutdown: node {result['source_node']} -> {result['target_node']}, "
          f"epoch {old_epoch} -> {player.snapshot.owner_epoch}", flush=True)
    return target, player, result
