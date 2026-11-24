"""client_rpc_router 路由表、熔断、子集、元数据回显与 Gate RouterForwarder 端到端测试。"""

from __future__ import annotations

import asyncio
import base64
import random

import grpc
import pytest

from xuanming.client_rpc_router.breaker import EndpointCircuitBreaker
from xuanming.client_rpc_router.config import RouterConfig, parse_release_track
from xuanming.client_rpc_router.discovery import NodeEntry, TargetDirectory, aperture_subset_indices
from xuanming.client_rpc_router.routes import BATTLE_NODE_TYPE, build_route_table, watched_node_types
from xuanming.client_rpc_router.service import (
    ClientRpcRouterService,
    TIP_INVALID_PARAMETER,
    TIP_SERVICE_UNAVAILABLE,
)
from xuanming.grpc_runtime import GrpcClient, serve_grpc
from xuanming.kill_switch import KillSwitch
from xuanming.nodes.gate import GateNode, RouterForwarder, Session
from xuanming.nodes.gate_login_renewal import RENEW_ID
from xuanming.protocol import Protocol


def test_route_table_derives_enode_types_and_excludes_scene_monopoly():
    protocol = Protocol()
    routes = build_route_table(protocol)
    # Login、Match、Chat、Friend 等客户端协议全部映射到正确的 common.base.eNodeType
    login_id = protocol.message_ids["ClientPlayerLoginLogin"]
    assert routes[login_id].node_type == 5
    assert routes[login_id].node_type_name == "LoginNodeService"
    assert routes[login_id].client_protocol is True
    # Gate 合成的 SERVER_ONLY 登录 RPC（如 RenewSession 394）也在客户端服务上，必须允许路由
    assert routes[RENEW_ID].node_type == 5
    assert routes[RENEW_ID].client_protocol is True
    # 内部非客户端 RPC（如 ClientRpcRouter/Forward）标记 client_protocol=False
    forward_id = protocol.message_ids["ClientRpcRouterForward"]
    assert routes[forward_id].client_protocol is False
    # Python 本地 scene/monopoly.proto 不进入 router 路由表
    monopoly_id = protocol.message_ids["MonopolyClientPlayerRollDice"]
    assert monopoly_id not in routes
    # watched_node_types 不包含 BattleNodeService (28)
    watched_ids = {node_type for node_type, _ in watched_node_types(routes)}
    assert 5 in watched_ids
    assert BATTLE_NODE_TYPE not in watched_ids


def test_router_config_validation_and_release_track():
    protocol = Protocol()
    assert parse_release_track("") == "stable"
    assert parse_release_track("stable") == "stable"
    assert parse_release_track("canary") == "canary"
    assert parse_release_track("STABLE") is None
    assert parse_release_track(" canary") is None

    with pytest.raises(ValueError):
        RouterConfig(global_target_subset_size=1).validate(protocol)
    with pytest.raises(ValueError):
        RouterConfig(forward_timeout_ms=5000, server_timeout_ms=5000).validate(protocol)
    with pytest.raises(ValueError):
        RouterConfig(zone_scoped_node_types=("NoSuchNodeService",)).validate(protocol)


def test_circuit_breaker_and_aperture_subset():
    now = [0.0]
    breaker = EndpointCircuitBreaker(failure_threshold=2, open_duration_ms=5000, clock=lambda: now[0])
    assert breaker.allow("ep-1")
    breaker.record_failure("ep-1")
    assert breaker.allow("ep-1")
    breaker.record_failure("ep-1")
    assert not breaker.allow("ep-1")
    # 过熔断窗口后仅允许一个 HalfOpen 探测
    now[0] = 6.0
    assert breaker.allow("ep-1")
    assert not breaker.allow("ep-1")
    # 调用方取消仅释放探测槽，不重新打开熔断
    breaker.release_probe("ep-1")
    assert breaker.allow("ep-1")
    breaker.record_success("ep-1")
    assert breaker.allow("ep-1")

    # Aperture subset 返回的索引去重且合法
    indices = aperture_subset_indices(12, rank=1, peer_count=4, subset_size=4, rng=random.Random(7))
    assert len(indices) == len(set(indices))
    assert all(0 <= idx < 12 for idx in indices)


def test_router_forward_end_to_end_with_gate_forwarder_and_metadata_echo():
    async def exercise():
        protocol = Protocol()
        received_md: list[dict] = []
        received_bodies: list[bytes] = []

        async def handle_renew(request, context, _path):
            received_md.append(dict(context.invocation_metadata()))
            received_bodies.append(request.SerializeToString())
            return protocol.message("loginpb.RenewSessionResponse", renewed=True, renew_after_seconds=60)

        login_server, login_port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            {"loginpb.ClientPlayerLogin": {"RenewSession": handle_renew}},
            pass_context=True,
        )
        config = RouterConfig(node_id=106, advertise_host="127.0.0.1", global_target_subset_size=4)
        login_dir = TargetDirectory("LoginNodeService")
        login_dir.put(
            b"LoginNodeService.rpc/zone/2/node_type/5/node_id/1",
            NodeEntry(1, 5, "login-u1", 2, f"127.0.0.1:{login_port}", "stable"),
        )
        router_service = ClientRpcRouterService(protocol, config, {5: login_dir}, self_uuid="router-u1")
        router_server, router_port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            router_service.services(),
            pass_context=True,
        )
        client = GrpcClient(f"127.0.0.1:{router_port}", timeout=5)
        try:
            # 1) 使用真实 Gate RouterForwarder 调用 RenewSession（验证二进制头原样回显 + 签名头透传）
            info = protocol.message("NodeInfo", node_id=1, zone_id=2, node_uuid="gate-a", release_track="stable")
            session = Session((1 << 17) | 3, 7, None)
            forwarder = RouterForwarder(protocol, client, b"x" * 32)
            gate = GateNode(protocol, info, b"x" * 32, forwarder)
            raw_resp = await forwarder.renew_login(gate, session)
            parsed = protocol.parse("loginpb.RenewSessionResponse", raw_resp)
            assert parsed.renewed is True and parsed.renew_after_seconds == 60
            assert received_bodies == [b""]
            assert received_md[0]["x-caller-id"] == "gate"
            assert received_md[0]["x-release-track"] == "stable"
            assert "x-session-detail-bin" in received_md[0]
            await gate.close()

            # 2) 缺失 x-session-detail-bin 返回 gRPC UNAUTHENTICATED
            fwd_req = protocol.message(
                "client_rpc_router.ForwardRequest",
                zone_id=2,
                release_track="stable",
                request={"id": 1, "message_id": RENEW_ID},
            )
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await client.call("/client_rpc_router.ClientRpcRouter/Forward", fwd_req.SerializeToString())
            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

            # 3) 错误业务包（未知消息、非客户端消息、非法 release_track、无目标区）都必须回显 x-session-detail-bin
            session_bytes = base64.b64encode(b"session-payload")
            md = (("x-session-detail-bin", session_bytes), ("x-release-track", "spoofed"), ("authorization", "drop"))

            async def call_forward(req):
                call = client.request("/client_rpc_router.ClientRpcRouter/Forward", req.SerializeToString(), metadata=md)
                raw = await call
                echoed = dict(await call.initial_metadata()).get("x-session-detail-bin")
                assert echoed == session_bytes
                return protocol.parse("MessageContent", raw)

            # 未知消息号 -> 1005
            resp = await call_forward(protocol.message(
                "client_rpc_router.ForwardRequest", zone_id=2, request={"id": 9, "message_id": 999999},
            ))
            assert resp.id == 9 and resp.message_id == 999999 and resp.error_message.id == TIP_INVALID_PARAMETER

            # 非客户端 RPC（Forward 自己） -> 1005
            forward_id = protocol.message_ids["ClientRpcRouterForward"]
            resp = await call_forward(protocol.message(
                "client_rpc_router.ForwardRequest", zone_id=2, request={"id": 10, "message_id": forward_id},
            ))
            assert resp.error_message.id == TIP_INVALID_PARAMETER

            # 非法 release_track -> 1005
            resp = await call_forward(protocol.message(
                "client_rpc_router.ForwardRequest", zone_id=2, release_track="invalid",
                request={"id": 11, "message_id": RENEW_ID},
            ))
            assert resp.error_message.id == TIP_INVALID_PARAMETER

            # Login 为区服隔离，zone=99 不得回退到 zone=2 -> 1003
            resp = await call_forward(protocol.message(
                "client_rpc_router.ForwardRequest", zone_id=99, release_track="stable",
                request={"id": 12, "message_id": RENEW_ID},
            ))
            assert resp.error_message.id == TIP_SERVICE_UNAVAILABLE

            # BattleNodeService 拒绝路由 -> 1003
            battle_id = protocol.message_ids["BattleClientPlayerSubmitBattleAction"]
            resp = await call_forward(protocol.message(
                "client_rpc_router.ForwardRequest", zone_id=2, release_track="stable",
                request={"id": 13, "message_id": battle_id},
            ))
            assert resp.error_message.id == TIP_SERVICE_UNAVAILABLE
        finally:
            await client.close()
            await router_service.pool.close()
            await router_server.stop(0)
            await login_server.stop(0)

    asyncio.run(exercise())


def test_router_killswitch_protects_forward_and_disconnect_and_blocks_target():
    protocol = Protocol()
    info = protocol.message("NodeInfo", node_id=106, node_type=29, zone_id=1, node_uuid="router-ks")
    ks = KillSwitch(protocol, info, clock=lambda: 10.0)
    ks.replace(1, [(b"/mmorpg/killswitch/*", b"1")])
    # 通配符不得拦截受保护方法 Forward 和 Disconnect
    assert ks.check("client_rpc_router.ClientRpcRouter", "Forward") is None
    assert ks.check("loginpb.ClientPlayerLogin", "Disconnect") is None
    # 但会拦截非受保护方法
    assert ks.check("loginpb.ClientPlayerLogin", "Login") is not None
