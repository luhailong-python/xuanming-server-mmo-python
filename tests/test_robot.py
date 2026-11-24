"""纯 Python 机器人客户端、冒烟套件、并发压测与 CLI 单元测试。"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import replace
import hmac
import json
from pathlib import Path
import secrets
from tempfile import TemporaryDirectory
import time

from aiohttp import web
import pytest

from xuanming.grpc_runtime import GrpcClient
from xuanming.nodes.battle import BattleNode
from xuanming.nodes.battle_tcp import BattleClients, _PROBE_GRANT_DOMAIN
from xuanming.protocol import Protocol
from xuanming.robot import (
    BattleRobotClient,
    PortalHttpClient,
    RobotError,
    SMOKE_SCENARIOS,
    cli,
    decode_gate_ticket,
    run_local_smoke,
    run_local_stress,
)
from xuanming.runtime import Runtime, grpc_address
from xuanming.settings import Settings


def test_local_smoke_tcp():
    results = asyncio.run(run_local_smoke(use_websocket=False))
    assert [r.name for r in results] == list(SMOKE_SCENARIOS.keys())
    failed = [f"{r.name}: {r.error}" for r in results if not r.passed]
    assert not failed, f"TCP local smoke failed: {failed}"
    assert all(r.duration_ms >= 0 for r in results)


def test_local_smoke_websocket():
    results = asyncio.run(run_local_smoke(use_websocket=True))
    assert [r.name for r in results] == list(SMOKE_SCENARIOS.keys())
    failed = [f"{r.name}: {r.error}" for r in results if not r.passed]
    assert not failed, f"WebSocket local smoke failed: {failed}"
    assert all(r.duration_ms >= 0 for r in results)


def test_local_stress():
    report = asyncio.run(run_local_stress(concurrency=3, rounds=2))
    assert report.concurrency == 3
    assert report.rounds == 2
    assert report.errors == 0
    assert report.total_ops == 3 * 2 * 3
    assert report.throughput_ops_sec > 0
    assert 0 < report.p50_ms <= report.p95_ms <= report.p99_ms
    as_dict = report.to_dict()
    assert set(as_dict.keys()) == {
        "concurrency",
        "rounds",
        "total_ops",
        "errors",
        "throughput_ops_sec",
        "p50_ms",
        "p95_ms",
        "p99_ms",
    }


def test_portal_http_client():
    async def scenario():
        captured: list[tuple[str, dict | None]] = []

        async def handle_server_list(_request: web.Request) -> web.Response:
            captured.append(("server_list", None))
            return web.json_response({"zones": [{"zone_id": 1, "name": "Zone-1", "status": "OPEN"}]})

        async def handle_login(request: web.Request) -> web.Response:
            body = await request.json()
            captured.append(("login", body))
            return web.json_response({
                "code": 0,
                "access_token": "acc-1",
                "refresh_token": "ref-1",
                "players": [{"player_id": 1001, "zone_id": body["zone_id"]}],
            })

        async def handle_refresh(request: web.Request) -> web.Response:
            body = await request.json()
            captured.append(("refresh_token", body))
            return web.json_response({"code": 0, "access_token": "acc-2", "refresh_token": "ref-2"})

        async def handle_assign_gate(request: web.Request) -> web.Response:
            body = await request.json()
            captured.append(("assign_gate", body))
            return web.json_response({
                "code": 0,
                "gate_ip": "127.0.0.1",
                "gate_port": 8610,
                "token_payload": base64.b64encode(b"raw-payload").decode(),
                "token_signature": base64.b64encode(b"raw-sig").decode(),
                "token_deadline": 1_800_000_000,
            })

        async def handle_queue_status(request: web.Request) -> web.Response:
            body = await request.json()
            captured.append(("queue_status", body))
            return web.json_response({"code": 100, "queue_token": body["queue_token"], "queue_rank": 3})

        async def handle_real_name(request: web.Request) -> web.Response:
            body = await request.json()
            captured.append(("real_name", body))
            return web.json_response({"code": 0, "anti_addiction": {"real_name_required": False, "minor": False}})

        async def handle_delete(request: web.Request) -> web.Response:
            body = await request.json()
            captured.append(("delete", body))
            return web.json_response({"code": 0, "state": 1, "execute_after": 1_900_000_000})

        async def handle_cancel_delete(request: web.Request) -> web.Response:
            body = await request.json()
            captured.append(("cancel_delete", body))
            return web.json_response({"code": 0, "state": 0, "execute_after": 0})

        async def handle_error(_request: web.Request) -> web.Response:
            return web.Response(status=503, text="secret-body-must-not-leak")

        app = web.Application()
        app.router.add_get("/api/server-list", handle_server_list)
        app.router.add_post("/api/login", handle_login)
        app.router.add_post("/api/refresh-token", handle_refresh)
        app.router.add_post("/api/assign-gate", handle_assign_gate)
        app.router.add_post("/api/queue-status", handle_queue_status)
        app.router.add_post("/api/real-name", handle_real_name)
        app.router.add_post("/api/account/delete", handle_delete)
        app.router.add_post("/api/account/delete/cancel", handle_cancel_delete)
        app.router.add_get("/api/fail", handle_error)

        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        base_url = f"http://127.0.0.1:{port}"
        try:
            async with PortalHttpClient(base_url) as portal:
                zones = await portal.server_list()
                assert zones["zones"][0]["zone_id"] == 1

                login = await portal.login(zone_id=1, account="alice", password="pw")
                assert login["code"] == 0 and login["access_token"] == "acc-1"

                refreshed = await portal.refresh_token("ref-1")
                assert refreshed["access_token"] == "acc-2"

                assigned = await portal.assign_gate(zone_id=1, access_token="acc-2", device_id="dev-1")
                assert assigned["code"] == 0 and assigned["gate_port"] == 8610
                assert decode_gate_ticket(assigned) == (b"raw-payload", b"raw-sig")

                queued = await portal.queue_status(queue_token="qt-9", zone_id=1)
                assert queued["code"] == 100 and queued["queue_rank"] == 3

                real_name = await portal.submit_real_name(
                    zone_id=1, access_token="acc-2", name="TestUser", id_number="110101199001011234",
                )
                assert real_name["code"] == 0

                deleted = await portal.request_account_deletion(zone_id=1, account="alice", password="pw")
                assert deleted["code"] == 0 and deleted["state"] == 1

                cancelled = await portal.cancel_account_deletion(zone_id=1, access_token="acc-2")
                assert cancelled["code"] == 0 and cancelled["state"] == 0

                with pytest.raises(RobotError, match="HTTP 503") as exc_info:
                    await portal._request_json("GET", "/api/fail")
                assert "secret-body-must-not-leak" not in str(exc_info.value)

            assert [name for name, _ in captured] == [
                "server_list",
                "login",
                "refresh_token",
                "assign_gate",
                "queue_status",
                "real_name",
                "delete",
                "cancel_delete",
            ]
        finally:
            await runner.cleanup()

    asyncio.run(scenario())


def test_battle_robot_client_enter_and_probe():
    async def scenario():
        protocol = Protocol()
        # 1. 验证 BattleRobotClient 票据直连入房与 RPC
        with TemporaryDirectory(prefix="xuanming-robot-battle-") as folder:
            settings = Settings()
            settings = replace(
                settings,
                database=Path(folder) / "players.sqlite3",
                gate=replace(settings.gate, port=0),
                scene=replace(settings.scene, port=0, grpc_port=0),
                battle=replace(settings.battle, port=0, grpc_port=0),
            )
            async with Runtime(settings, "local", dev_token=secrets.token_hex(16)) as runtime:
                node = runtime.battle_info
                now = time.time_ns() // 1_000_000
                create = protocol.message(
                    "CreateBattleRequest",
                    battle_id=77,
                    match_mode=7,
                    seed=1234,
                    deadline_ms=now + 60_000,
                    admit_before_ms=now + 10_000,
                    placement_operation_id="robot-battle-test",
                    target_instance_id=node.node_uuid,
                    required_physical_region=node.physical_region,
                    outcome_protocol_version=1,
                )
                for pid, side in ((101, 0), (202, 1)):
                    p = create.players.add(player_id=pid, team_index=side, max_health=100, physical_attack=20)
                    p.base_attributes.health, p.base_attributes.speed = 100, 10
                    p.routing.gate_instance_id = runtime.gate.info.node_uuid
                    p.routing.scene_instance_id = runtime.scene.info.node_uuid
                grpc_client = GrpcClient(grpc_address(node.grpc_endpoint.ip, node.grpc_endpoint.port))
                try:
                    raw = await grpc_client.call("/BattleNode/CreateBattle", create.SerializeToString())
                    assert not protocol.parse("CreateBattleResponse", raw).error_message.id
                    raw = await grpc_client.call(
                        "/BattleNode/IssueBattleTicket",
                        protocol.message(
                            "IssueBattleTicketRequest",
                            battle_id=77,
                            player_id=101,
                            target_instance_id=node.node_uuid,
                        ).SerializeToString(),
                    )
                    assignment = protocol.parse("IssueBattleTicketResponse", raw).assignment
                finally:
                    await grpc_client.close()

                async with BattleRobotClient(protocol) as client:
                    await client.connect(node.endpoint.ip, node.endpoint.port)
                    verified = await client.enter_battle(assignment.token_payload, assignment.token_signature)
                    assert verified.success and verified.battle_id == 77
                    hb = await client.heartbeat(7)
                    assert hb.seq == 7
                    state = await client.rpc(
                        "BattleClientPlayer",
                        "GetBattleState",
                        {"battle_id": 77},
                        "BattleStateS2C",
                    )
                    assert len(state.actors) == 2 and state.round_index == 1

        # 2. 验证 BattleRobotClient 地域延迟探测握手
        probe_secret = secrets.token_bytes(32)
        battle = BattleNode(
            protocol,
            node_id=9,
            instance_id="battle-probe-1",
            host="127.0.0.1",
            port=0,
            physical_region=2,
            ticket_secret=secrets.token_bytes(32),
        )
        edge = BattleClients(protocol, battle, probe_secret=probe_secret)
        server = await asyncio.start_server(edge.serve, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            grant = protocol.message(
                "match.NetworkProbeGrant",
                player_id=101,
                plan_id="plan-1",
                session_binding="sess-1",
                physical_region=2,
                target_node_id=9,
                target_instance_id="battle-probe-1",
                expires_at_ms=time.time_ns() // 1_000_000 + 30_000,
            )
            payload = grant.SerializeToString(deterministic=True)
            signature = hmac.digest(probe_secret, _PROBE_GRANT_DOMAIN + payload, "sha256")
            signed_grant = protocol.message("match.SignedNetworkProbe", payload=payload, signature=signature)
            async with BattleRobotClient(protocol) as client:
                await client.connect("127.0.0.1", port)
                probe_res = await client.probe_latency(signed_grant)
                assert probe_res.sample_count == 3
                assert probe_res.rtt_ms >= 1
                assert probe_res.receipt.grant.plan_id == "plan-1"
        finally:
            await edge.close()
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_cli_local_smoke_json(capsys):
    code = cli.main(["local-smoke", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["ok"] is True
    assert data["failed"] == 0
    assert data["total"] == len(SMOKE_SCENARIOS)
    assert all(item["passed"] for item in data["scenarios"])


def test_cli_local_stress_json(capsys):
    code = cli.main(["stress", "--local", "--concurrency", "2", "--rounds", "2", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["errors"] == 0
    assert data["concurrency"] == 2
    assert data["rounds"] == 2
    assert data["total_ops"] == 2 * 2 * 3
