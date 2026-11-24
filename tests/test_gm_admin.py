"""gm_admin 纯 Python 移植版端到端与契约测试。

覆盖：
1. config/gm_admin_schema.sql 与 ops/schemamigrate.py 静态校验兼容性；
2. GmAdminConfig 配置解析与安全边界（禁 TOML 密钥、仅回环明文、URL 校验）；
3. 健康探针与 fail-closed 管理员鉴权、操作人校验、细粒度权限约束；
4. GET /gm/overview（真实回环网关区服代理）与 GET /gm/players/{playerId}（真实 gRPC DataService + PlayerLocator）；
5. 个人邮件 / 系统邮件 / 三段式活动邮件完整生命周期（草稿 -> 签名发送 -> 幂等重放 -> 分页/详情/审计 -> 失败与重试）；
6. 全部 7 项在线玩家 GM 命令目录、草稿、签名执行（含静态映射与 etcd 动态发现）、5 分钟过期与未知结果禁重发；
7. SqlGmJournal 事务与参数绑定测试。
"""

from __future__ import annotations

import asyncio
import base64
import functools
import hashlib
import hmac
import json
import time
import uuid
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Any

import aiohttp
import grpc
import pytest
from aiohttp import web

from xuanming.gm_admin import (
    READY_STATE_KEY,
    GameServicesClient,
    GmAdminConfig,
    MailCampaignManifest,
    MemoryGmJournal,
    SqlGmJournal,
    create_gm_app,
)
from xuanming.grpc_runtime import serve_grpc, verify_service_call
from xuanming.mail.model import manifest_hash as mail_manifest_hash
from xuanming.mail.model import shard_hash as mail_shard_hash
from xuanming.ops.schemamigrate import parse_schema_file
from xuanming.protocol import Protocol
from xuanming.settings import Settings

ADMIN_TOKEN = "gm-super-secret-token-for-unit-tests"
GATEWAY_KEY = "gateway-admin-key-for-unit-tests"
MAIL_SECRET = b"mail-admin-hmac-secret-at-least-32-bytes!"
SCENE_SECRET = b"scene-admin-hmac-secret-at-least-32-bytes"
REPO_ROOT = Path(__file__).resolve().parents[1]


class _FakeClock:
    def __init__(self, start_ms: int | None = None) -> None:
        self.now_ms = start_ms if start_ms is not None else time.time_ns() // 1_000_000

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, delta_ms: int) -> None:
        self.now_ms += delta_ms


def _verify_scene_signature(request, secret: bytes, path: str = "/scene_node.SceneNodeGrpc/GmExecutePlayerCommand") -> bool:
    """独立校验 Scene 端 gm-player-command-v1 HMAC 签名（对齐 C++ SceneNodeGrpc）。"""
    parts = request.auth.split("|")
    if len(parts) != 4:
        return False
    actor, ts_str, nonce, sig_hex = parts
    if actor != request.operator:
        return False
    business = type(request)()
    business.CopyFrom(request)
    business.ClearField("auth")
    digest = hashlib.sha256(business.SerializeToString(deterministic=True)).hexdigest()
    reason = f"gm-player-command-v1\n{digest}"
    canonical = f"{path}\n{request.target_node_id}\n{actor}\n{ts_str}\n{nonce}\n{reason}"
    expected = hmac.digest(secret, canonical.encode("utf-8"), "sha256").hex()
    return hmac.compare_digest(expected, sig_hex)


class _MockBackend:
    """真实回环 gRPC + HTTP 模拟集群：DataService、PlayerLocator、MailAdmin、SceneNodeGrpc、Gateway、etcd。"""

    def __init__(self, protocol: Protocol, clock: _FakeClock) -> None:
        self.protocol = protocol
        self.clock = clock
        self.players: dict[int, dict[str, Any]] = {
            10001: {
                "name": "剑修甲",
                "level": 45,
                "class_id": 2,
                "gender": 1,
                "appearance_id": "app_01",
                "home_zone_id": 7,
                "online": True,
                "session_id": 501,
                "login_generation": 9,
                "zone_id": 7,
                "node_id": "202",
                "scene_id": 3001,
                "awaiting_placement": False,
            },
            10002: {
                "name": "离线乙",
                "level": 12,
                "class_id": 1,
                "gender": 0,
                "appearance_id": "",
                "home_zone_id": None,
                "online": False,
            },
        }
        self.zones: list[dict[str, Any]] = [{"zone_id": 7, "name": "青云一区", "manual_status": 0}]
        self.gateway_fail = False
        self.gateway_seen_keys: list[str] = []

        self.mail_calls: list[tuple[str, Any]] = []
        self.mail_next_id = 9000
        self.mail_failure: tuple[str, grpc.StatusCode | int] | None = None
        self.mail_fail_once: tuple[str, grpc.StatusCode | int] | None = None

        self.scene_calls: list[Any] = []
        self.scene_failure: grpc.StatusCode | int | None = None

        self.etcd_kvs: list[dict[str, str]] = []
        self.etcd_calls = 0

    # -- DataService ----------------------------------------------------------------------------------

    async def batch_get_player_display(self, request, context, _path):
        resp = self.protocol.message("data_service.BatchGetPlayerDisplayResponse")
        for pid in request.player_ids:
            info = self.players.get(int(pid))
            if info is None:
                continue
            entry = resp.players[int(pid)]
            entry.name = info["name"]
            entry.level = info["level"]
            entry.class_id = info["class_id"]
            entry.gender = info["gender"]
            entry.appearance_id = info["appearance_id"]
        return resp

    async def get_player_home_zone(self, request, context, _path):
        info = self.players.get(int(request.player_id))
        if info is None or info.get("home_zone_id") is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "no home zone")
        return self.protocol.message("data_service.GetPlayerHomeZoneResponse", home_zone_id=info["home_zone_id"])

    # -- PlayerLocator --------------------------------------------------------------------------------

    async def get_session(self, request, context, _path):
        pid = int(request.player_id)
        info = self.players.get(pid)
        if info is None or not info.get("online"):
            return self.protocol.message("playerlocator.GetSessionResponse", found=False)
        session = self.protocol.message(
            "playerlocator.PlayerSession",
            player_id=pid,
            session_id=info["session_id"],
            login_generation=info["login_generation"],
            scene_node_id=info["node_id"],
            scene_id=info["scene_id"],
            gate_zone_id=info["zone_id"],
            state=1,  # SESSION_STATE_ONLINE
            last_active_ts=self.clock() // 1000,
        )
        resp = self.protocol.message("playerlocator.GetSessionResponse", found=True, session=session)
        if request.include_placement:
            resp.placement.found = True
            resp.placement.zone_id = info["zone_id"]
            resp.placement.node_id = info["node_id"]
            resp.placement.scene_id = info["scene_id"]
            resp.placement.session_id = info["session_id"]
            resp.placement.login_generation = info["login_generation"]
            resp.placement.awaiting_placement = info["awaiting_placement"]
        return resp

    # -- MailAdmin ------------------------------------------------------------------------------------

    async def _check_mail_auth(self, request, context, path: str) -> None:
        if not verify_service_call(path, request, "admin", self.clock(), MAIL_SECRET):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "bad mail admin signature")

    async def _maybe_fail_mail(self, op: str, context, resp_type: str):
        fault = None
        if self.mail_fail_once is not None and self.mail_fail_once[0] == op:
            fault = self.mail_fail_once[1]
            self.mail_fail_once = None
        elif self.mail_failure is not None and self.mail_failure[0] == op:
            fault = self.mail_failure[1]
        if fault is None:
            return None
        if isinstance(fault, grpc.StatusCode):
            await context.abort(fault, f"injected {fault.name}")
        resp = self.protocol.message(resp_type)
        resp.error_message.id = int(fault)
        return resp

    async def send_personal_mail(self, request, context, path):
        await self._check_mail_auth(request, context, path)
        self.mail_calls.append(("SendPersonalMail", request))
        err = await self._maybe_fail_mail("SendPersonalMail", context, "mailpb.SendMailResponse")
        if err is not None:
            return err
        self.mail_next_id += 1
        return self.protocol.message("mailpb.SendMailResponse", mail_id=self.mail_next_id, duplicated=False)

    async def send_system_mail(self, request, context, path):
        await self._check_mail_auth(request, context, path)
        self.mail_calls.append(("SendSystemMail", request))
        err = await self._maybe_fail_mail("SendSystemMail", context, "mailpb.SendMailResponse")
        if err is not None:
            return err
        self.mail_next_id += 1
        return self.protocol.message("mailpb.SendMailResponse", mail_id=self.mail_next_id, duplicated=False)

    async def import_mail_recipients(self, request, context, path):
        await self._check_mail_auth(request, context, path)
        self.mail_calls.append(("ImportMailRecipients", request))
        err = await self._maybe_fail_mail("ImportMailRecipients", context, "mailpb.ImportMailRecipientsResponse")
        if err is not None:
            return err
        return self.protocol.message(
            "mailpb.ImportMailRecipientsResponse",
            imported_count=len(request.player_ids),
            duplicated=False,
        )

    async def seal_mail_campaign(self, request, context, path):
        await self._check_mail_auth(request, context, path)
        self.mail_calls.append(("SealMailCampaign", request))
        err = await self._maybe_fail_mail("SealMailCampaign", context, "mailpb.SendMailResponse")
        if err is not None:
            return err
        self.mail_next_id += 1
        return self.protocol.message("mailpb.SendMailResponse", mail_id=self.mail_next_id, duplicated=False)

    # -- SceneNodeGrpc --------------------------------------------------------------------------------

    async def gm_execute_player_command(self, request, context, path):
        if not _verify_scene_signature(request, SCENE_SECRET, path):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "bad scene gm signature")
        self.scene_calls.append(request)
        if self.scene_failure is not None:
            fault = self.scene_failure
            if isinstance(fault, grpc.StatusCode):
                await context.abort(fault, f"injected scene {fault.name}")
            resp = self.protocol.message("scene_node.GmExecutePlayerCommandResponse", command_id=request.command_id)
            resp.error_message.id = int(fault)
            return resp
        return self.protocol.message(
            "scene_node.GmExecutePlayerCommandResponse",
            command_id=request.command_id,
            executed=True,
            balance_after=888,
            pet_id=777,
        )

    # -- Gateway & etcd HTTP --------------------------------------------------------------------------

    async def handle_admin_zones(self, request: web.Request) -> web.Response:
        self.gateway_seen_keys.append(request.headers.get("X-Admin-Key", ""))
        if self.gateway_fail:
            return web.Response(status=503, text="down")
        return web.json_response(self.zones)

    async def handle_etcd_range(self, _request: web.Request) -> web.Response:
        self.etcd_calls += 1
        return web.json_response({"kvs": self.etcd_kvs})


@asynccontextmanager
async def _start_harness(
    *,
    use_etcd: bool = False,
    mail_enabled: bool = True,
    admin_token: str = ADMIN_TOKEN,
    max_campaign_targets: int = 10_000,
):
    protocol = Protocol()
    clock = _FakeClock(start_ms=1_760_000_000_000)
    backend = _MockBackend(protocol, clock)

    async with AsyncExitStack() as stack:
        grpc_server, grpc_port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            {
                "data_service.DataService": {
                    "BatchGetPlayerDisplay": backend.batch_get_player_display,
                    "GetPlayerHomeZone": backend.get_player_home_zone,
                },
                "playerlocator.PlayerLocator": {
                    "GetSession": backend.get_session,
                },
                "mailpb.MailAdmin": {
                    "SendPersonalMail": backend.send_personal_mail,
                    "SendSystemMail": backend.send_system_mail,
                    "ImportMailRecipients": backend.import_mail_recipients,
                    "SealMailCampaign": backend.seal_mail_campaign,
                },
                "scene_node.SceneNodeGrpc": {
                    "GmExecutePlayerCommand": backend.gm_execute_player_command,
                },
            },
            pass_context=True,
        )
        stack.push_async_callback(grpc_server.stop, 0.2)
        grpc_addr = f"127.0.0.1:{grpc_port}"

        # Mock gateway & etcd HTTP server
        aux_app = web.Application()
        aux_app.router.add_get("/admin/zones", backend.handle_admin_zones)
        aux_app.router.add_post("/v3/kv/range", backend.handle_etcd_range)
        aux_runner = web.AppRunner(aux_app, access_log=None)
        await aux_runner.setup()
        stack.push_async_callback(aux_runner.cleanup)
        aux_site = web.TCPSite(aux_runner, "127.0.0.1", 0)
        await aux_site.start()
        aux_port = aux_site._server.sockets[0].getsockname()[1]
        aux_url = f"http://127.0.0.1:{aux_port}"

        if use_etcd:
            reg_val = json.dumps(
                {"nodeId": 202, "zoneId": 7, "grpcEndpoint": {"ip": "127.0.0.1", "port": grpc_port}}
            )
            backend.etcd_kvs = [
                {
                    "key": base64.b64encode(b"SceneNodeService.rpc/allocated/202").decode("ascii"),
                    "value": base64.b64encode(b'{"ignored":true}').decode("ascii"),
                },
                {
                    "key": base64.b64encode(b"SceneNodeService.rpc/202").decode("ascii"),
                    "value": base64.b64encode(reg_val.encode("utf-8")).decode("ascii"),
                },
            ]

        config = GmAdminConfig(
            host="127.0.0.1",
            port=0,
            admin_token=admin_token,
            gateway_url=aux_url,
            gateway_admin_key=GATEWAY_KEY,
            data_service=grpc_addr,
            player_locator=grpc_addr,
            mail_service=grpc_addr if mail_enabled else "",
            mail_admin_secret=MAIL_SECRET if mail_enabled else b"",
            scene_targets=() if use_etcd else (("7/202", grpc_addr),),
            scene_etcd_url=aux_url if use_etcd else "",
            scene_admin_secret=SCENE_SECRET,
            plaintext=True,
            max_campaign_targets=max_campaign_targets,
            use_memory_journal=True,
        )
        config.validate()

        journal = MemoryGmJournal(clock_ms=clock)
        services = GameServicesClient(protocol, config, clock_ms=clock)
        stack.push_async_callback(services.close)

        gm_app = create_gm_app(journal, services, config, clock_ms=clock)
        gm_runner = web.AppRunner(gm_app, access_log=None)
        await gm_runner.setup()
        stack.push_async_callback(gm_runner.cleanup)
        gm_site = web.TCPSite(gm_runner, "127.0.0.1", 0)
        await gm_site.start()
        gm_port = gm_site._server.sockets[0].getsockname()[1]
        base_url = f"http://127.0.0.1:{gm_port}"

        session = aiohttp.ClientSession()
        stack.push_async_callback(session.close)

        yield {
            "base_url": base_url,
            "session": session,
            "backend": backend,
            "clock": clock,
            "journal": journal,
            "services": services,
            "app": gm_app,
            "grpc_port": grpc_port,
        }


def _auth_headers(
    *,
    token: str = ADMIN_TOKEN,
    operator: str | None = None,
    permissions: str | None = None,
    header_name: str = "X-Admin-Key",
) -> dict[str, str]:
    headers = {header_name: token}
    if operator is not None:
        headers["X-Gm-Operator"] = operator
    if permissions is not None:
        headers["X-Gm-Permissions"] = permissions
    return headers


# -- 1. Schema 与配置测试 -----------------------------------------------------------------------------


def test_gm_admin_schema_parses_with_schemamigrate():
    plan = parse_schema_file(REPO_ROOT / "config" / "gm_admin_schema.sql")
    assert plan.db_name == "gm_admin"
    table_names = [table.table_name for table in plan.tables]
    assert table_names == ["gm_command", "gm_command_audit"]

    cmd_table = plan.tables[0]
    assert cmd_table.primary_key == ("id",)
    idx_map = {idx.name: (idx.unique, idx.columns) for idx in cmd_table.indexes}
    assert idx_map["uq_gm_command_request"] == (True, ("operator_name", "client_request_id"))
    assert idx_map["idx_gm_command_type_created"] == (False, ("command_type", "created_ms", "id"))
    assert idx_map["idx_gm_command_type_player_created"] == (False, ("command_type", "player_id", "created_ms", "id"))
    assert idx_map["idx_gm_command_type_status_created"] == (False, ("command_type", "status", "created_ms", "id"))

    audit_table = plan.tables[1]
    assert audit_table.primary_key == ("id",)
    assert any(idx.name == "idx_gm_command_audit_command" for idx in audit_table.indexes)


def test_campaign_manifest_matches_mail_service_hash_contract():
    raw_ids = ["300", "100", "200", "100"]
    plan = MailCampaignManifest.build(raw_ids)
    assert plan.recipient_count == 3
    assert plan.shards == (("100", "200", "300"),)

    expected_shard_digest, unique_ints = mail_shard_hash([300, 100, 200])
    assert unique_ints == (100, 200, 300)
    assert MailCampaignManifest.shard_hash_of(plan.shards[0]) == expected_shard_digest
    assert plan.manifest_hash == mail_manifest_hash([(0, expected_shard_digest, 3)])


def test_config_from_settings_rejects_toml_secrets_and_non_loopback_plaintext():
    settings = Settings(data_service="127.0.0.1:50001", player_locator="127.0.0.1:50002")
    cfg = GmAdminConfig.from_settings(
        settings,
        environ={
            "GM_ADMIN_TOKEN": "tok-123",
            "GM_MAIL_TARGET": "127.0.0.1:50900",
            "GM_MAIL_ADMIN_SECRET": MAIL_SECRET.decode(),
            "GM_SCENE_TARGETS_JSON": '{"1/2":"127.0.0.1:8612"}',
            "GM_SCENE_ADMIN_SECRET": SCENE_SECRET.decode(),
            "GM_GATEWAY_URL": "http://127.0.0.1:8081",
            "GM_DB_PASSWORD": "pw",
        },
    )
    assert cfg.port == 8082
    assert cfg.mail_enabled
    assert cfg.player_commands_enabled
    assert cfg.scene_target_map() == {"1/2": "127.0.0.1:8612"}

    # 明文非回环地址拒启
    with pytest.raises(ValueError, match="loopback"):
        GmAdminConfig.from_settings(
            settings,
            environ={"GM_MAIL_TARGET": "10.0.0.5:50900", "GM_GAME_PLAINTEXT": "true"},
        )

    # 非回环 HTTP 网关拒启
    with pytest.raises(ValueError, match="HTTPS or loopback HTTP"):
        GmAdminConfig.from_settings(
            settings,
            environ={"GM_GATEWAY_URL": "http://example.com:8081"},
        )

    # 模拟 TOML 传入密钥时拒启
    class _FakeSettings:
        gm_admin_node = {"admin_token": "leaked-in-toml"}

    with pytest.raises(ValueError, match="environment variables"):
        GmAdminConfig.from_settings(_FakeSettings(), environ={})  # type: ignore[arg-type]


# -- 2. 鉴权、健康探针、总览与玩家查询 ----------------------------------------------------------------


def _async_test(fn):
    @functools.wraps(fn)
    def _runner(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return _runner


@_async_test
async def test_health_and_fail_closed_auth():
    async with _start_harness(admin_token="") as env:
        session = env["session"]
        base = env["base_url"]
        # 健康探针免鉴权
        async with session.get(f"{base}/actuator/health") as resp:
            assert resp.status == 200
            assert (await resp.json())["status"] == "UP"
        # 未配置 admin_token 时必须 fail-closed 401
        async with session.get(f"{base}/gm/overview", headers={"X-Admin-Key": "anything"}) as resp:
            assert resp.status == 401

    async with _start_harness() as env:
        session = env["session"]
        base = env["base_url"]
        # 缺少密钥 -> 401
        async with session.get(f"{base}/gm/overview") as resp:
            assert resp.status == 401
        # 错误密钥 -> 401
        async with session.get(f"{base}/gm/overview", headers={"X-Admin-Key": "wrong"}) as resp:
            assert resp.status == 401
        # 同时传两种令牌头 -> 401
        async with session.get(
            f"{base}/gm/overview",
            headers={"X-Admin-Key": ADMIN_TOKEN, "X-Gm-Token": ADMIN_TOKEN},
        ) as resp:
            assert resp.status == 401
        # 非法操作人名 -> 401
        async with session.get(
            f"{base}/gm/overview",
            headers=_auth_headers(operator="bad|name"),
        ) as resp:
            assert resp.status == 401
        # 支持 X-Gm-Token 认证头
        async with session.get(
            f"{base}/gm/overview",
            headers=_auth_headers(header_name="X-Gm-Token"),
        ) as resp:
            assert resp.status == 200
            body = await resp.json()
            assert body["success"] is True
            assert body["result"]["mailEnabled"] is True
            assert body["result"]["zonesAvailable"] is True
            assert body["result"]["zones"][0]["name"] == "青云一区"
            assert env["backend"].gateway_seen_keys == [GATEWAY_KEY]
        # 权限裁剪 -> 403
        async with session.get(
            f"{base}/gm/players/10001",
            headers=_auth_headers(permissions="gm:zone:read"),
        ) as resp:
            assert resp.status == 403
        # readiness 随 ready 标记变化
        env["app"][READY_STATE_KEY]["ready"] = False
        async with session.get(f"{base}/actuator/health/readiness") as resp:
            assert resp.status == 503
        async with session.get(f"{base}/actuator/health/liveness") as resp:
            assert resp.status == 200


@_async_test
async def test_overview_degrades_when_gateway_fails_and_player_lookup():
    async with _start_harness() as env:
        session = env["session"]
        base = env["base_url"]
        env["backend"].gateway_fail = True
        async with session.get(f"{base}/gm/overview", headers=_auth_headers()) as resp:
            assert resp.status == 200
            result = (await resp.json())["result"]
            assert result["zonesAvailable"] is False
            assert result["zones"] == []
            assert len(result["currencies"]) >= 3

        # 在线玩家查询
        async with session.get(f"{base}/gm/players/10001", headers=_auth_headers()) as resp:
            assert resp.status == 200
            player = (await resp.json())["result"]
            assert player["found"] is True
            assert player["display"]["name"] == "剑修甲"
            assert player["homeZoneFound"] is True
            assert player["homeZoneId"] == 7
            assert player["sessionFound"] is True
            assert player["session"]["state"] == "SESSION_STATE_ONLINE"
            assert player["session"]["sceneNodeId"] == "202"
            assert player["session"]["sceneZoneId"] == 7

        # 无归属区离线玩家
        async with session.get(f"{base}/gm/players/10002", headers=_auth_headers()) as resp:
            assert resp.status == 200
            player = (await resp.json())["result"]
            assert player["found"] is True
            assert player["homeZoneFound"] is False
            assert player["sessionFound"] is False

        # 不存在的玩家
        async with session.get(f"{base}/gm/players/99999", headers=_auth_headers()) as resp:
            assert resp.status == 200
            assert (await resp.json())["result"] == {"playerId": "99999", "found": False}

        # 非法玩家 ID
        async with session.get(f"{base}/gm/players/0", headers=_auth_headers()) as resp:
            assert resp.status == 400


# -- 3. 个人邮件、系统邮件与活动邮件端到端流程 --------------------------------------------------------


@_async_test
async def test_personal_mail_draft_send_idempotent_replay_and_audit():
    async with _start_harness() as env:
        session = env["session"]
        base = env["base_url"]
        req_uuid = str(uuid.uuid4())
        payload = {
            "clientRequestId": req_uuid,
            "playerId": "10001",
            "title": "补偿邮件",
            "body": "停机维护补偿",
            "reason": "线上故障补偿",
            "validDays": 7,
            "currencies": [{"currencyType": 0, "amount": "500"}],
            "items": [{"configId": 1001, "count": 3}],
        }

        # 创建草稿
        async with session.post(f"{base}/gm/mail/drafts", json=payload, headers=_auth_headers(operator="gm_alice")) as resp:
            assert resp.status == 200
            draft = (await resp.json())["result"]
            assert draft["commandType"] == "MAIL"
            assert draft["status"] == "DRAFT"
            assert draft["operator"] == "gm_alice"
            cmd_id = draft["id"]

        # 相同幂等号重放返回原草稿
        async with session.post(f"{base}/gm/mail/drafts", json=payload, headers=_auth_headers(operator="gm_alice")) as resp:
            assert resp.status == 200
            assert (await resp.json())["result"]["id"] == cmd_id

        # 相同幂等号篡改内容返回 409
        tampered = dict(payload, title="篡改标题")
        async with session.post(f"{base}/gm/mail/drafts", json=tampered, headers=_auth_headers(operator="gm_alice")) as resp:
            assert resp.status == 409

        # 非创建者尝试发送 -> 403
        async with session.post(
            f"{base}/gm/mail/commands/{cmd_id}/send",
            headers=_auth_headers(operator="gm_bob"),
        ) as resp:
            assert resp.status == 403

        # 首次发送因网络故障变为 UNKNOWN，随后重试成功 -> DELIVERED
        env["backend"].mail_fail_once = ("SendPersonalMail", grpc.StatusCode.UNAVAILABLE)
        async with session.post(
            f"{base}/gm/mail/commands/{cmd_id}/send",
            headers=_auth_headers(operator="gm_alice"),
        ) as resp:
            assert resp.status == 200
            first_send = (await resp.json())["result"]
            assert first_send["status"] == "UNKNOWN"
            assert first_send["result"]["outcomeUnknown"] is True

        async with session.post(
            f"{base}/gm/mail/commands/{cmd_id}/send",
            headers=_auth_headers(operator="gm_alice"),
        ) as resp:
            assert resp.status == 200
            delivered = (await resp.json())["result"]
            assert delivered["status"] == "DELIVERED"
            assert int(delivered["result"]["mailId"]) > 0

        calls_after_delivery = len(env["backend"].mail_calls)

        # 再次调用 send 幂等返回 DELIVERED，不再触发下游 RPC
        async with session.post(
            f"{base}/gm/mail/commands/{cmd_id}/send",
            headers=_auth_headers(operator="gm_alice"),
        ) as resp:
            assert resp.status == 200
            assert (await resp.json())["result"]["status"] == "DELIVERED"
        assert len(env["backend"].mail_calls) == calls_after_delivery

        # 查询列表、详情、审计与跨类型隔离
        async with session.get(
            f"{base}/gm/mail/commands?page=1&pageSize=10&playerId=10001&status=DELIVERED",
            headers=_auth_headers(),
        ) as resp:
            assert resp.status == 200
            listing = (await resp.json())["result"]
            assert listing["total"] == 1
            assert listing["records"][0]["id"] == cmd_id

        async with session.get(f"{base}/gm/mail/commands/{cmd_id}", headers=_auth_headers()) as resp:
            assert resp.status == 200
            assert (await resp.json())["result"]["status"] == "DELIVERED"

        async with session.get(f"{base}/gm/mail/commands/{cmd_id}/audit", headers=_auth_headers()) as resp:
            assert resp.status == 200
            events = [row["event"] for row in (await resp.json())["result"]["records"]]
            assert events == ["DRAFT", "SENDING", "UNKNOWN", "SENDING", "DELIVERED"]

        # 用系统邮件入口读取个人邮件 ID -> 409
        async with session.get(f"{base}/gm/system-mail/commands/{cmd_id}", headers=_auth_headers()) as resp:
            assert resp.status == 409


@_async_test
async def test_system_mail_and_campaign_three_stage_flow():
    async with _start_harness() as env:
        session = env["session"]
        base = env["base_url"]

        # 1. 全服纯文本系统邮件
        sys_draft_body = {
            "clientRequestId": str(uuid.uuid4()),
            "title": "全服公告",
            "body": "本周五凌晨维护",
            "reason": "维护公告",
            "validDays": 3,
        }
        async with session.post(f"{base}/gm/system-mail/drafts", json=sys_draft_body, headers=_auth_headers()) as resp:
            assert resp.status == 200
            sys_id = (await resp.json())["result"]["id"]

        async with session.post(f"{base}/gm/system-mail/commands/{sys_id}/send", headers=_auth_headers()) as resp:
            assert resp.status == 200
            assert (await resp.json())["result"]["status"] == "DELIVERED"

        async with session.get(f"{base}/gm/system-mail/commands", headers=_auth_headers()) as resp:
            assert (await resp.json())["result"]["total"] == 1
        async with session.get(f"{base}/gm/system-mail/commands/{sys_id}", headers=_auth_headers()) as resp:
            assert (await resp.json())["result"]["commandType"] == "SYSTEM_MAIL"
        async with session.get(f"{base}/gm/system-mail/commands/{sys_id}/audit", headers=_auth_headers()) as resp:
            events = [r["event"] for r in (await resp.json())["result"]["records"]]
            assert events == ["DRAFT", "SENDING", "DELIVERED"]

        # 2. 三段式活动邮件（1005 名不重复玩家 -> 2 个分片，样本截断 20，中途导入故障后安全重试）
        recipients = [str(20000 + i) for i in range(1005)] + ["20000", "20001"]
        camp_body = {
            "clientRequestId": str(uuid.uuid4()),
            "title": "周年庆发奖",
            "body": "感谢陪伴",
            "reason": "周年庆活动发放",
            "validDays": 14,
            "currencies": [{"currencyType": 1, "amount": "100"}],
            "items": [{"configId": 5001, "count": 2}],
            "recipientPlayerIds": recipients,
        }
        async with session.post(f"{base}/gm/mail-campaigns/drafts", json=camp_body, headers=_auth_headers()) as resp:
            assert resp.status == 200
            camp_draft = (await resp.json())["result"]
            camp_id = camp_draft["id"]
            assert camp_draft["commandType"] == "MAIL_CAMPAIGN"
            assert camp_draft["request"]["totalRecipients"] == 1005
            assert camp_draft["request"]["shardCount"] == 2
            assert len(camp_draft["request"]["recipientSample"]) == 20

        # 阶段 2 导入分片失败 -> 标记 UNKNOWN，可安全重发完成三段式封存
        env["backend"].mail_fail_once = ("ImportMailRecipients", grpc.StatusCode.DEADLINE_EXCEEDED)
        async with session.post(f"{base}/gm/mail-campaigns/commands/{camp_id}/send", headers=_auth_headers()) as resp:
            assert resp.status == 200
            assert (await resp.json())["result"]["status"] == "UNKNOWN"

        async with session.post(f"{base}/gm/mail-campaigns/commands/{camp_id}/send", headers=_auth_headers()) as resp:
            assert resp.status == 200
            sealed = (await resp.json())["result"]
            assert sealed["status"] == "DELIVERED"
            assert sealed["result"]["sealed"] is True
            assert sealed["result"]["importedRecipients"] == 1005
            assert sealed["result"]["importedShards"] == 2

        async with session.get(f"{base}/gm/mail-campaigns/commands", headers=_auth_headers()) as resp:
            assert (await resp.json())["result"]["total"] == 1
        async with session.get(f"{base}/gm/mail-campaigns/commands/{camp_id}", headers=_auth_headers()) as resp:
            assert (await resp.json())["result"]["status"] == "DELIVERED"
        async with session.get(f"{base}/gm/mail-campaigns/commands/{camp_id}/audit", headers=_auth_headers()) as resp:
            events = [r["event"] for r in (await resp.json())["result"]["records"]]
            assert events == ["DRAFT", "SENDING", "UNKNOWN", "SENDING", "DELIVERED"]


# -- 4. 全部 7 项在线玩家 GM 命令与故障防护 -----------------------------------------------------------


@_async_test
async def test_player_commands_catalog_and_all_seven_operations():
    async with _start_harness(use_etcd=True) as env:
        session = env["session"]
        base = env["base_url"]

        # 目录查询
        async with session.get(f"{base}/gm/player-commands/catalog", headers=_auth_headers()) as resp:
            assert resp.status == 200
            catalog = (await resp.json())["result"]
            assert catalog["enabled"] is True
            assert catalog["limits"] == {"maxItemCount": 99, "maxLevel": 85}
            codes = [op["code"] for op in catalog["operations"]]
            assert codes == [
                "currency.add",
                "currency.deduct",
                "currency.block",
                "currency.unblock",
                "player.level",
                "pet.grant",
                "item.grant",
            ]

        cases = [
            ("currency.add", {"currencyType": 0, "amount": "1000"}),
            ("currency.deduct", {"currencyType": 1, "amount": "50"}),
            ("currency.block", {"currencyType": 2}),
            ("currency.unblock", {"currencyType": 2}),
            ("player.level", {"level": 60}),
            ("pet.grant", {"petTableId": 3001}),
            ("item.grant", {"configId": 4001, "count": 5}),
        ]
        created_ids: list[str] = []
        for op_code, args in cases:
            draft_payload = {
                "clientRequestId": str(uuid.uuid4()),
                "playerId": "10001",
                "operation": op_code,
                "arguments": args,
                "reason": f"测试 {op_code}",
            }
            async with session.post(f"{base}/gm/player-commands/drafts", json=draft_payload, headers=_auth_headers()) as resp:
                assert resp.status == 200, await resp.text()
                draft = (await resp.json())["result"]
                assert draft["operation"] == op_code
                cmd_id = draft["id"]
                created_ids.append(cmd_id)

            async with session.post(f"{base}/gm/player-commands/{cmd_id}/send", headers=_auth_headers()) as resp:
                assert resp.status == 200, await resp.text()
                executed = (await resp.json())["result"]
                assert executed["status"] == "EXECUTED"
                assert executed["result"]["executed"] is True

        assert len(env["backend"].scene_calls) == 7
        assert env["backend"].etcd_calls == 1  # 命中缓存，无需重复拉取 etcd

        # 列表、详情与审计
        async with session.get(f"{base}/gm/player-commands?playerId=10001&status=EXECUTED", headers=_auth_headers()) as resp:
            assert resp.status == 200
            assert (await resp.json())["result"]["total"] == 7

        last_id = created_ids[-1]
        async with session.get(f"{base}/gm/player-commands/{last_id}", headers=_auth_headers()) as resp:
            assert resp.status == 200
            assert (await resp.json())["result"]["operation"] == "item.grant"

        async with session.get(f"{base}/gm/player-commands/{last_id}/audit", headers=_auth_headers()) as resp:
            assert resp.status == 200
            events = [r["event"] for r in (await resp.json())["result"]["records"]]
            assert events == ["DRAFT", "SENDING", "EXECUTED"]


@_async_test
async def test_player_command_guards_offline_expired_unknown_no_resend_and_rejected():
    async with _start_harness() as env:
        session = env["session"]
        base = env["base_url"]

        # 离线玩家创建草稿被拒绝
        async with session.post(
            f"{base}/gm/player-commands/drafts",
            json={
                "clientRequestId": str(uuid.uuid4()),
                "playerId": "10002",
                "operation": "player.level",
                "arguments": {"level": 10},
                "reason": "离线校验",
            },
            headers=_auth_headers(),
        ) as resp:
            assert resp.status == 400

        # 超 5 分钟草稿发送返回 409
        async with session.post(
            f"{base}/gm/player-commands/drafts",
            json={
                "clientRequestId": str(uuid.uuid4()),
                "playerId": "10001",
                "operation": "player.level",
                "arguments": {"level": 50},
                "reason": "过期校验",
            },
            headers=_auth_headers(),
        ) as resp:
            expired_id = (await resp.json())["result"]["id"]
        env["clock"].advance(301_000)
        async with session.post(f"{base}/gm/player-commands/{expired_id}/send", headers=_auth_headers()) as resp:
            assert resp.status == 409

        # 场景返回非零 tip -> UNKNOWN 且禁止再次发送（409）
        async with session.post(
            f"{base}/gm/player-commands/drafts",
            json={
                "clientRequestId": str(uuid.uuid4()),
                "playerId": "10001",
                "operation": "currency.add",
                "arguments": {"currencyType": 0, "amount": "100"},
                "reason": "未知结果禁重发",
            },
            headers=_auth_headers(),
        ) as resp:
            unknown_id = (await resp.json())["result"]["id"]

        env["backend"].scene_failure = 4201
        async with session.post(f"{base}/gm/player-commands/{unknown_id}/send", headers=_auth_headers()) as resp:
            assert resp.status == 200
            body = (await resp.json())["result"]
            assert body["status"] == "UNKNOWN"
            assert body["result"]["code"] == "SERVER_TIP"
            assert body["result"]["tipId"] == 4201

        env["backend"].scene_failure = None
        async with session.post(f"{base}/gm/player-commands/{unknown_id}/send", headers=_auth_headers()) as resp:
            assert resp.status == 409

        # 场景明确拒绝（PERMISSION_DENIED）-> REJECTED
        async with session.post(
            f"{base}/gm/player-commands/drafts",
            json={
                "clientRequestId": str(uuid.uuid4()),
                "playerId": "10001",
                "operation": "item.grant",
                "arguments": {"configId": 1001, "count": 1},
                "reason": "拒绝状态测试",
            },
            headers=_auth_headers(),
        ) as resp:
            rejected_id = (await resp.json())["result"]["id"]

        env["backend"].scene_failure = grpc.StatusCode.PERMISSION_DENIED
        async with session.post(f"{base}/gm/player-commands/{rejected_id}/send", headers=_auth_headers()) as resp:
            assert resp.status == 200
            body = (await resp.json())["result"]
            assert body["status"] == "REJECTED"
            assert body["result"]["outcomeUnknown"] is False


# -- 5. SqlGmJournal 事务与 SQL 绑定单元测试 ----------------------------------------------------------


class _FakeCursor:
    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state
        self._rows: list[tuple[Any, ...]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def execute(self, sql: str, args=None) -> int:
        self._state["queries"].append((sql, args))
        normalized = " ".join(sql.split())
        if "FROM information_schema.tables" in normalized:
            self._rows = [("gm_command",), ("gm_command_audit",)]
            return 2
        if normalized.startswith("CREATE TABLE"):
            return 0
        if normalized.startswith("SELECT id,command_type,operator_name,client_request_id") and "WHERE operator_name=" in normalized:
            key = (bytes.fromhex(args[0]).decode("utf-8"), bytes.fromhex(args[1]).decode("utf-8"))
            cmd_id = self._state["by_key"].get(key)
            self._rows = [self._state["commands"][cmd_id]] if cmd_id else []
            return len(self._rows)
        if normalized.startswith("INSERT INTO gm_command "):
            cmd_id, kind, op_hex, req_hex, pid, reason, in_j, req_j, c_ms, u_ms = args
            row = (
                cmd_id, kind, bytes.fromhex(op_hex), bytes.fromhex(req_hex),
                pid, reason, in_j, req_j, None, "DRAFT", 0, c_ms, u_ms,
            )
            self._state["commands"][cmd_id] = row
            self._state["by_key"][(bytes.fromhex(op_hex).decode("utf-8"), bytes.fromhex(req_hex).decode("utf-8"))] = cmd_id
            return 1
        if normalized.startswith("INSERT INTO gm_command_audit"):
            cmd_id, op_hex, event, attempt, res_j, c_ms = args
            self._state["audit_seq"] += 1
            self._state["audits"].append(
                (self._state["audit_seq"], cmd_id, bytes.fromhex(op_hex), event, attempt, res_j, c_ms)
            )
            return 1
        if normalized.startswith("SELECT id,command_type,operator_name,client_request_id") and "WHERE id=%s" in normalized:
            row = self._state["commands"].get(args[0])
            self._rows = [row] if row else []
            return len(self._rows)
        if normalized.startswith("UPDATE gm_command SET status='SENDING'"):
            now_ms, cmd_id, attempt = args[0], args[1], args[2]
            row = self._state["commands"][cmd_id]
            if row[10] != attempt or row[9] not in ("DRAFT", "UNKNOWN"):
                return 0
            self._state["commands"][cmd_id] = (*row[:9], "SENDING", attempt + 1, row[11], now_ms)
            return 1
        if normalized.startswith("UPDATE gm_command SET status=%s,result_json=%s"):
            final_state, res_j, now_ms, cmd_id, attempt = args
            row = self._state["commands"][cmd_id]
            if row[9] != "SENDING" or row[10] != attempt:
                return 0
            self._state["commands"][cmd_id] = (*row[:8], res_j, final_state, attempt, row[11], now_ms)
            return 1
        if normalized.startswith("SELECT id,operator_name,event,attempt,result_json,created_ms FROM gm_command_audit"):
            self._rows = [
                (a[0], a[2], a[3], a[4], a[5], a[6])
                for a in self._state["audits"]
                if a[1] == args[0]
            ]
            return len(self._rows)
        raise AssertionError(f"Unhandled SQL in mock: {normalized}")

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._state)

    async def commit(self) -> None:
        self._state["commits"] += 1

    async def rollback(self) -> None:
        self._state["rollbacks"] += 1


class _FakeAcquire:
    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state

    async def __aenter__(self) -> _FakeConn:
        return _FakeConn(self._state)

    async def __aexit__(self, *_exc):
        return False


class _FakePool:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {
            "commands": {},
            "by_key": {},
            "audits": [],
            "audit_seq": 0,
            "queries": [],
            "commits": 0,
            "rollbacks": 0,
        }

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.state)


@_async_test
async def test_sql_gm_journal_bootstrap_prepare_send_and_audit():
    pool = _FakePool()
    clock = _FakeClock(start_ms=1_760_000_000_000)
    journal = SqlGmJournal(pool, clock_ms=clock, id_generator=lambda: 123456)  # type: ignore[arg-type]

    await journal.bootstrap()
    await journal.verify_schema()

    draft = await journal.prepare(
        "admin",
        str(uuid.uuid4()),
        "10001",
        "SQL 测试发放",
        {"senderKind": 1, "title": "标题", "body": "正文", "validDays": 5},
    )
    assert draft["id"] == "123456"
    assert draft["status"] == "DRAFT"

    async def _fake_sender(frozen: dict[str, Any]) -> dict[str, Any]:
        assert frozen["requestId"] == "123456"
        return {"mailId": "8888", "duplicated": False}

    sent = await journal.send("123456", "admin", _fake_sender)
    assert sent["status"] == "DELIVERED"
    assert sent["result"]["mailId"] == "8888"

    audit = await journal.audit_history("123456", "MAIL")
    assert [r["event"] for r in audit["records"]] == ["DRAFT", "SENDING", "DELIVERED"]
    assert pool.state["rollbacks"] == 0


@_async_test
async def test_mail_validation_edge_cases_and_disabled_mail_503():
    async with _start_harness(mail_enabled=False) as env:
        session = env["session"]
        base = env["base_url"]
        async with session.post(
            f"{base}/gm/mail/drafts",
            json={"clientRequestId": str(uuid.uuid4())},
            headers=_auth_headers(),
        ) as resp:
            assert resp.status == 503

    async with _start_harness() as env:
        session = env["session"]
        base = env["base_url"]

        # 重复 JSON 键被拒绝
        dup_raw = '{"clientRequestId":"' + str(uuid.uuid4()) + '","title":"a","title":"b"}'
        async with session.post(
            f"{base}/gm/system-mail/drafts",
            data=dup_raw.encode("utf-8"),
            headers={**_auth_headers(), "Content-Type": "application/json"},
        ) as resp:
            assert resp.status == 400

        # 个人邮件兼容旧式平铺附件格式
        flat_payload = {
            "clientRequestId": str(uuid.uuid4()),
            "playerId": "10001",
            "title": "旧式平铺附件",
            "body": "正文",
            "reason": "兼容测试",
            "validDays": 5,
            "currencyType": 0,
            "currencyAmount": "250",
            "itemConfigId": 2001,
            "itemCount": 2,
        }
        async with session.post(f"{base}/gm/mail/drafts", json=flat_payload, headers=_auth_headers()) as resp:
            assert resp.status == 200
            draft = (await resp.json())["result"]
            assert draft["request"]["attachments"]["currencies"] == [{"currencyType": 0, "amount": "250"}]
            assert draft["request"]["attachments"]["items"] == [{"configId": 2001, "count": 2}]

        # 混用数组与平铺字段 -> 400
        mixed = dict(flat_payload, clientRequestId=str(uuid.uuid4()), currencies=[{"currencyType": 0, "amount": "1"}])
        async with session.post(f"{base}/gm/mail/drafts", json=mixed, headers=_auth_headers()) as resp:
            assert resp.status == 400

        # 未知币种 -> 400
        bad_currency = {
            "clientRequestId": str(uuid.uuid4()),
            "playerId": "10001",
            "title": "坏币种",
            "body": "",
            "reason": "校验",
            "validDays": 1,
            "currencies": [{"currencyType": 999, "amount": "1"}],
        }
        async with session.post(f"{base}/gm/mail/drafts", json=bad_currency, headers=_auth_headers()) as resp:
            assert resp.status == 400

        # 缺少任何附件 -> 400
        no_attach = {
            "clientRequestId": str(uuid.uuid4()),
            "playerId": "10001",
            "title": "空附件",
            "body": "",
            "reason": "校验",
            "validDays": 1,
        }
        async with session.post(f"{base}/gm/mail/drafts", json=no_attach, headers=_auth_headers()) as resp:
            assert resp.status == 400


@_async_test
async def test_mail_lease_takeover_after_stuck_sending():
    clock = _FakeClock(start_ms=1_760_000_000_000)
    journal = MemoryGmJournal(clock_ms=clock)
    draft = await journal.prepare(
        "admin",
        str(uuid.uuid4()),
        "10001",
        "租约接管测试",
        {"senderKind": 1, "title": "t", "body": "b", "validDays": 3},
    )
    cmd_id = draft["id"]

    # 模拟首次发送占坑后挂起（先在内部停在 SENDING）
    started = asyncio.Event()
    release = asyncio.Event()

    async def _hanging_sender(_req):
        started.set()
        await release.wait()
        return {"mailId": "1"}

    first_task = asyncio.create_task(journal.send(cmd_id, "admin", _hanging_sender))
    await started.wait()

    # 租约期内并发重试必须被拒绝（409）
    with pytest.raises(Exception) as exc_info:
        await journal.send(cmd_id, "admin", lambda _req: asyncio.sleep(0, result={"mailId": "2"}))
    assert getattr(exc_info.value, "status", None) == 409

    # 超过 60s 租约后可安全接管并落盘 DELIVERED（attempt=2）
    clock.advance(61_000)

    async def _recovery_sender(_req):
        return {"mailId": "222", "duplicated": False}

    recovered = await journal.send(cmd_id, "admin", _recovery_sender)
    assert recovered["status"] == "DELIVERED"
    assert recovered["result"]["mailId"] == "222"

    # 旧的 attempt=1 迟到返回不得覆盖 attempt=2 的已送达结果
    release.set()
    stale_view = await first_task
    assert stale_view["status"] == "DELIVERED"
    assert stale_view["result"]["mailId"] == "222"

