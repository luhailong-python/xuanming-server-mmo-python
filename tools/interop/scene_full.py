"""Two actual Go coordinators with Python Gate/Scene instances and native DB."""

import asyncio
import hashlib
import hmac
import os
import time
from contextlib import asynccontextmanager

from redis.asyncio import Redis

from smoke_original import ENV, GO, PORT, RUN, SOURCE, TICKET_SECRET, endpoint, wait_etcd
from xuanming.cluster import EtcdDirectory
from xuanming.data.asset_ledger import canonical
from xuanming.grpc_runtime import GrpcClient
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime, grpc_address
from xuanming.settings import NodeSettings, Settings
from xuanming.smoke import connect
from xuanming.transport import read_packet


PARTITIONS = 8


def _config(name, relative, replacements, *, extra=""):
    source = (SOURCE / relative).read_text(encoding="utf-8")
    pairs = (
        ("127.0.0.1:2379", endpoint("etcd")), ("127.0.0.1:6379", endpoint("redis")),
        ("127.0.0.1:9092", endpoint("kafka")), *replacements,
    )
    for old, new in pairs:
        source = source.replace(old, new)
    target = RUN / f"{name}.yaml"
    target.write_text(source + extra, encoding="utf-8")
    return target


async def start_control(processes):
    table_dir = f"\nTableDir: {(SOURCE / 'generated/tables').as_posix()}\n"
    locator = _config("locator", "go/player_locator/etc/player_locator.yaml", (
        ("127.0.0.1:53200", endpoint("locator")),
        ("Port: 9190", f"Port: {PORT['locator_metrics']}"),
        ("Key: scenemanagerservice.rpc", "Key: scenemanagerservice.rpc.zone1"),
    ), extra=table_dir)
    processes.start("locator", [GO / "player_locator.exe", "-f", locator])
    for zone, name in ((1, "manager"), (2, "manager2")):
        config = _config(name, "go/scene_manager/etc/scene_manager_service.yaml", (
            ("127.0.0.1:60300", endpoint(name)), ("\nZoneId: 1", f"\nZoneId: {zone}"),
            ("Key: scenemanagerservice.rpc", f"Key: scenemanagerservice.rpc.zone{zone}"),
            ('MetricsListenAddr: ":9150"', 'MetricsListenAddr: ""'),
            ("../../run/snowflake", (RUN / f"snowflake-{name}").as_posix()),
            ("local-dev-gate-token-secret-0123456789abcdef", TICKET_SECRET),
            ('"1": 16  # stress-test spawn map layers', '"1": 1'),
        ), extra=table_dir)
        processes.start(name, [GO / "scene_manager.exe", "-f", config])
    await asyncio.gather(*(processes.ready(name) for name in ("locator", "manager", "manager2")))
    p = Protocol()
    kinds = p.pool.FindEnumTypeByName("common.base.eNodeType").values_by_name
    directory = EtcdDirectory(p, endpoint("etcd"))
    try:
        kind = kinds["SceneManagerNodeService"].number
        for zone, name in ((1, "manager"), (2, "manager2")):
            peer = await wait_etcd(directory, kind, zone)
            assert peer.grpc_endpoint.port == PORT[name], (name, peer.grpc_endpoint.port)
    finally:
        await directory.close()


async def start_python(stack, *, landing=False, direct_gate=False):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption
    key = Ed25519PrivateKey.generate()
    ENV["MMORPG_INTERNAL_AUTH_SIGNING_KEY"] = key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()).hex()
    ENV["MMORPG_INTERNAL_AUTH_PUBLIC_KEYS"] = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex()
    # The runner is an isolated process. Only copy the synthetic credentials it created.
    probe_names = {"GLOBAL_MATCH_ENABLED", "MATCH_PROBE_SECRET"}
    for name in tuple(os.environ):
        if name.startswith("MMORPG_") or name in probe_names:
            del os.environ[name]
    os.environ.update({name: value for name, value in ENV.items() if name.startswith("MMORPG_") or name in probe_names})
    os.environ["GATE_CLIENT_RPC_ROUTER"] = "0" if direct_gate else "1"

    def settings(zone):
        return Settings(
            zone_id=zone, physical_region=1, kafka_partitions=PARTITIONS,
            etcd=endpoint("etcd"), kafka=endpoint("kafka"), router=endpoint("router"),
            scene_manager=endpoint("manager" if zone == 1 else "manager2"),
            player_store=endpoint("db"), data_service=endpoint("data"), player_inbox=endpoint("inbox"),
            match=endpoint("match"), redis=f"redis://{endpoint('redis')}/0",
            scene_control_redis=f"redis://{endpoint('redis')}/0" if landing else "",
            gate=NodeSettings(100 + zone, advertise_host="127.0.0.1", client_host="127.0.0.1", rpc_port=0),
            scene=NodeSettings(200 + zone, advertise_host="127.0.0.1"),
            battle=NodeSettings(301, advertise_host="127.0.0.1", client_host="127.0.0.1"),
        )

    battle = await stack.enter_async_context(Runtime(settings(1), "battle"))
    gates, scenes = {}, {}
    for zone in (1, 2):
        scenes[zone] = await stack.enter_async_context(Runtime(settings(zone), "scene"))
        gates[zone] = await stack.enter_async_context(Runtime(settings(zone), "gate"))
    return gates, scenes, battle


async def _read_push(client, *names, timeout=40):
    p = client.protocol
    ids = {p.message_ids[name]: name for name in names}
    async with asyncio.timeout(timeout):
        while True:
            for index, message in enumerate(client.pending):
                if not message.id and message.message_id in ids:
                    message = client.pending.pop(index)
                    return ids[message.message_id], p.parse(p.routes[message.message_id].request_type,
                                                            message.serialized_message)
            packet = await read_packet(client.reader)
            if packet is None:
                raise EOFError(f"client closed while awaiting {names}")
            if packet.type_name == "MessageContent":
                client.pending.append(p.parse("MessageContent", packet.payload))
                assert len(client.pending) < 128, f"too many unsolicited packets awaiting {names}"


async def entry(client, runtime, player_id):
    _, arrived = await _read_push(client, "SceneSceneClientPlayerNotifyEnterScene")
    scene = runtime.scene
    world = scene.locations[player_id]
    player = world.players[player_id]
    assert world.info.scene_id == arrived.scene_info.scene_id
    assert player.snapshot.owner_epoch > 0 and player.admission is not None
    assert not player.snapshot.released and player.snapshot.owner == scene.info.node_uuid.encode()
    return player


async def wallet(client, call_id):
    p = client.protocol
    value = await client.rpc(p.message_ids["SceneCurrencyClientPlayerGetCurrencyList"], call_id,
                             p.message("GetCurrencyListRequest"))
    return value.currency.values[0] if value.currency.values else 0


async def credit(runtime, player_id, seq, amount):
    p, scene = runtime.protocol, runtime.scene
    request = p.message("AssetOpRequest", player_id=player_id, stream=5, seq=seq, stream_epoch=1,
                        correlation_id=seq, tx_type=2, bundle={"currencies": [{"amount": amount}]})
    request.auth.caller = "mail"
    request.auth.timestamp_ms = time.time_ns() // 1_000_000
    request.auth.signature_hex = hmac.new(ENV["MMORPG_ASSET_OP_SECRET_MAIL"].encode(),
                                         canonical("credit", request), hashlib.sha256).hexdigest()
    peer = GrpcClient(grpc_address(scene.info.grpc_endpoint.ip, scene.info.grpc_endpoint.port))
    try:
        raw = await peer.call("/scene_node.SceneNodeGrpc/AssetCredit", request.SerializeToString())
    finally:
        await peer.close()
    reply = p.parse("AssetOpResponse", raw)
    assert reply.outcome == 1 and reply.durable and not reply.partial, str(reply)


@asynccontextmanager
async def travel(source_client, target_gate, target_scene, player_id, access_token, authenticate, enter_game, *,
                 request_id):
    p = source_client.protocol
    zone = target_scene.scene.info.zone_id
    reply = await source_client.rpc(p.message_ids["SceneSceneClientPlayerTravelToZone"], request_id,
                                    p.message("TravelToZoneRequest", target_zone_id=zone))
    assert not reply.error_message.id, str(reply)
    names = ("SceneClientPlayerCommonRedirectToGate", "SceneClientPlayerCommonSendTipToClient",
             "SceneClientPlayerCommonKickPlayer")
    kind, redirect = await _read_push(source_client, *names, timeout=50)
    assert kind == names[0], (kind, str(redirect))
    expected = target_gate.gate.info.client_endpoint
    assert (redirect.target_ip, redirect.target_port) == (expected.ip, expected.port)
    ticket = p.parse("GateTokenPayload", redirect.token_payload)
    assert ticket.player_id == player_id and ticket.zone_id == zone and ticket.target_zone_id == zone
    async with connect(p, expected) as client:
        await client.send(p.message("ClientTokenVerifyRequest", payload=redirect.token_payload,
                                    signature=redirect.token_signature))
        assert (await client.receive("ClientTokenVerifyResponse")).success
        await authenticate(client, access_token, call_id=1)
        await enter_game(client, player_id, call_id=2)
        player = await entry(client, target_scene, player_id)
        yield client, player


async def redis_epoch(player_id):
    redis = Redis.from_url(f"redis://{endpoint('redis')}/0")
    try:
        return int(await redis.get(f"player:{{{player_id}}}:owner_epoch"))
    finally:
        await redis.aclose()
