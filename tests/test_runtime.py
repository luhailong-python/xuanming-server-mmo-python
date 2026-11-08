import asyncio
import json
from dataclasses import replace

import grpc
import pytest

from xuanming.cluster import EtcdDirectory, command_route
from xuanming.grpc_runtime import GrpcClient
from xuanming.protocol import Protocol, RouteNode, RpcKind
from xuanming.runtime import Runtime, grpc_address
from xuanming.settings import Settings
from xuanming.smoke import smoke


def test_protocol_keeps_client_and_cluster_route_boundaries():
    protocol = Protocol()
    assert len(protocol.routes) >= 350
    assert protocol.routes[48].path == "/loginpb.ClientPlayerLogin/Login"
    assert protocol.routes[134].node_type == RouteNode.SCENE
    assert protocol.routes[134].kind == RpcKind.ONE_WAY
    assert protocol.routes[140].node_type == RouteNode.BATTLE
    assert protocol.routes[375].kind == RpcKind.SERVER_ONLY
    assert protocol.event_ids["ContractsKafkaBindSessionEvent"] == 32
    assert command_route("gate", 257) == ("gate-cmd_g1", 1)


def test_end_to_end():
    assert len(asyncio.run(smoke())) == 2


def test_battle_grpc_keeps_native_placement_guards(tmp_path):
    async def exercise():
        settings = Settings()
        settings = replace(
            settings, database=tmp_path / "players.sqlite3",
            gate=replace(settings.gate, port=0), scene=replace(settings.scene, port=0, grpc_port=0),
            battle=replace(settings.battle, port=0, grpc_port=0),
        )
        async with Runtime(settings, "local", dev_token="placement-test") as runtime:
            protocol, battle, info = runtime.protocol, runtime.battle, runtime.battle_info
            client = GrpcClient(grpc_address(info.grpc_endpoint.ip, info.grpc_endpoint.port))
            try:
                battle.admitting = False
                request = protocol.message("CreateBattleRequest", battle_id=10).SerializeToString()
                with pytest.raises(grpc.aio.AioRpcError) as refused:
                    await client.call("/BattleNode/CreateBattle", request)
                assert refused.value.code() == grpc.StatusCode.UNAVAILABLE
                assert refused.value.details() == "battle_not_allocatable"
                assert not battle.rooms and not battle.receipts

                battle.admitting = True
                invalid = await client.call("/BattleNode/CreateBattle", request)
                assert protocol.parse("CreateBattleResponse", invalid).error_message.id == 1005

                destroy = protocol.message("DestroyBattleRequest", battle_id=10, placement_operation_id="p1",
                                           target_instance_id=battle.instance_id)
                await client.call("/BattleNode/DestroyBattle", destroy.SerializeToString())
                await client.call("/BattleNode/DestroyBattle", destroy.SerializeToString())
                destroy.placement_operation_id = "p2"
                with pytest.raises(grpc.aio.AioRpcError) as mismatch:
                    await client.call("/BattleNode/DestroyBattle", destroy.SerializeToString())
                assert mismatch.value.code() == grpc.StatusCode.FAILED_PRECONDITION
                assert mismatch.value.details() == "placement_operation_mismatch"
                assert battle.receipts[10].operation_id == "p1"
            finally:
                await client.close()

    asyncio.run(exercise())


def test_etcd_registration_uses_protobuf_json_names_for_go_discovery():
    protocol = Protocol()
    info = protocol.message(
        "NodeInfo", node_id=17, node_type=4, zone_id=2, node_uuid="gate-instance",
        grpc_endpoint={"ip": "127.0.0.1", "port": 5500}, release_track="canary",
    )

    assert json.loads(EtcdDirectory._serialized(info)) == {
        "nodeId": 17, "nodeType": 4, "zoneId": 2, "nodeUuid": "gate-instance",
        "grpcEndpoint": {"ip": "127.0.0.1", "port": 5500}, "releaseTrack": "canary",
    }
