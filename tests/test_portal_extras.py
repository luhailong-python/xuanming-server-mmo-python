"""门户新增接口测试：/api/real-name、/api/account/delete*、/admin/tables 与 /api/login 扩展字段。"""

import asyncio
import logging
from types import SimpleNamespace

import grpc
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from xuanming.grpc_runtime import GrpcClient
from xuanming.portal import create_app
from xuanming.portal_account import (
    MAX_ACCOUNT_CODE_POINTS,
    MAX_PASSWORD_CHARS,
    MAX_TOKEN_CHARS,
    register_routes as register_account_routes,
    validate_cancel_request,
    validate_delete_request,
)
from xuanming.portal_admin_tables import (
    OPERATOR_HEADER,
    EtcdTablePointerStore,
    TablePointerEntry,
    register_routes as register_table_routes,
)
from xuanming.portal_login import Limiter, cooldown_key, register_login_routes
from xuanming.protocol import Protocol

_SCHEMA = "0123456789abcdef"
_DIGEST_V1 = "1" * 64
_DIGEST_V2 = "2" * 64
_SHA_V1 = "a" * 64
_SHA_V2 = "b" * 64


def _portal_settings(**overrides):
    defaults = {
        "rate_limit_enabled": False,
        "trusted_proxies": [],
        "ip_rps": 2,
        "ip_burst": 10,
        "zone_rps": 500,
        "zone_burst": 1000,
        "account_cooldown_ms": 5000,
        "admin_api_key": "test-admin-secret-key",
        "cdn_base_url": "",
        "cdn_sign_ttl_seconds": 3600,
        "cdn_signing_secret": "test-cdn-signing-secret-0123456789",
        "hotfix_platforms": {},
        "home_zone_ids": [],
        "zone_policy_interval_seconds": 5,
        "role_capacity_interval_seconds": 10,
        "mall_admin_base_url": "",
        "mall_admin_secret": "",
        "mall_admin_subject": "mall-admin",
    }
    return SimpleNamespace(**(defaults | overrides))


class _FakeRpcError(grpc.aio.AioRpcError):
    def __init__(self, code: grpc.StatusCode, details: str = ""):
        super().__init__(code, grpc.aio.Metadata(), grpc.aio.Metadata(), details, details)


class _RecordingLimiter:
    def __init__(self, decision=None):
        self.decision = decision
        self.networks = ()
        self.calls = []

    async def check(self, zone, ip, account, scope):
        self.calls.append((zone, ip, account, scope))
        return self.decision


class _DummyDb:
    def acquire(self):
        raise AssertionError("db should not be called")


def test_login_carries_account_deletion_and_anti_addiction_fields():
    async def run():
        protocol = Protocol()

        class Rpc:
            async def call(self, zone, service, method, request, response, **kwargs):
                assert method == "Login"
                return protocol.message(
                    "loginpb.LoginResponse",
                    access_token="acc",
                    refresh_token="ref",
                    account_deletion_state=1,
                    account_deletion_execute_after=1_800_000_000,
                    previous_account_deleted_at=1_700_000_000,
                    anti_addiction={
                        "real_name_required": True,
                        "minor": False,
                        "play_deadline_ms": 1_800_003_600_000,
                    },
                )

        app = web.Application()
        deps = SimpleNamespace(
            protocol=protocol,
            rpc=Rpc(),
            db=_DummyDb(),
            limiter=Limiter(None, _portal_settings()),
        )
        register_login_routes(app, deps)
        async with TestClient(TestServer(app)) as client:
            resp = await client.post("/api/login", json={"zone_id": 1, "account": "alice", "password": "pw"})
            assert resp.status == 200
            data = await resp.json()
            assert data["code"] == 0
            assert data["account_deletion_state"] == 1
            assert data["account_deletion_execute_after"] == 1_800_000_000
            assert data["previous_account_deleted_at"] == 1_700_000_000
            assert data["anti_addiction"] == {
                "real_name_required": True,
                "minor": False,
                "play_deadline_ms": 1_800_003_600_000,
            }

    asyncio.run(run())


def test_real_name_outcomes_rate_limit_and_pii_redaction(caplog):
    async def run():
        protocol = Protocol()
        behavior = {"mode": "ok"}
        calls = []

        class Rpc:
            async def call(self, zone, service, method, request, response, **kwargs):
                calls.append((zone, service, method, request))
                mode = behavior["mode"]
                if mode == "ok":
                    return protocol.message(
                        "loginpb.SubmitRealNameResponse",
                        anti_addiction={"real_name_required": False, "minor": True, "play_deadline_ms": 999},
                    )
                if mode == "reject":
                    return protocol.message(
                        "loginpb.SubmitRealNameResponse",
                        error_message={"id": 2000},
                        anti_addiction={"real_name_required": True, "minor": False, "play_deadline_ms": 0},
                    )
                if mode == "fault":
                    return protocol.message(
                        "loginpb.SubmitRealNameResponse",
                        error_message={"id": 2047},
                    )
                if mode == "unauth":
                    raise _FakeRpcError(grpc.StatusCode.UNAUTHENTICATED)
                if mode == "unavailable":
                    raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE)
                raise RuntimeError("boom with secret-id-number")

        limiter = _RecordingLimiter()
        app = web.Application()
        deps = SimpleNamespace(protocol=protocol, rpc=Rpc(), db=_DummyDb(), limiter=limiter)
        register_login_routes(app, deps)

        payload = {
            "zone_id": 2,
            "access_token": "SYNTHETIC_ACCESS_TOKEN",
            "name": "张三",
            "id_number": "11010519491231002X",
        }
        with caplog.at_level(logging.WARNING):
            async with TestClient(TestServer(app)) as client:
                # 1. 成功返回 code=0 及最新 anti_addiction，省略 tip 与 message。
                resp = await client.post("/api/real-name", json=payload)
                assert resp.status == 200
                body = await resp.json()
                assert body == {
                    "code": 0,
                    "anti_addiction": {"real_name_required": False, "minor": True, "play_deadline_ms": 999},
                }
                assert calls[-1][:3] == (2, "LoginPreGate", "SubmitRealName")
                assert calls[-1][3].name == "张三" and calls[-1][3].id_number == "11010519491231002X"
                assert limiter.calls[-1][2] == cooldown_key("SYNTHETIC_ACCESS_TOKEN")
                assert limiter.calls[-1][3] == "real-name"

                # 2. 业务拒绝 -> code=422 + tip。
                behavior["mode"] = "reject"
                body = await (await client.post("/api/real-name", json=payload)).json()
                assert body["code"] == 422 and body["tip"] == 2000 and body["message"] == "upstream_err=2000"
                assert body["anti_addiction"]["real_name_required"] is True

                # 3. 故障 tip (2047 LoginRealNameServiceUnavailable) -> code=500 + tip。
                behavior["mode"] = "fault"
                body = await (await client.post("/api/real-name", json=payload)).json()
                assert body == {"code": 500, "message": "upstream_err=2047", "tip": 2047}

                # 4. gRPC UNAUTHENTICATED -> code=401。
                behavior["mode"] = "unauth"
                body = await (await client.post("/api/real-name", json=payload)).json()
                assert body == {"code": 401, "message": "UNAUTHENTICATED"}

                # 5. gRPC UNAVAILABLE -> code=500 login_unavailable。
                behavior["mode"] = "unavailable"
                body = await (await client.post("/api/real-name", json=payload)).json()
                assert body == {"code": 500, "message": "login_unavailable"}

                # 6. 意外异常不泄露消息细节。
                behavior["mode"] = "unexpected"
                body = await (await client.post("/api/real-name", json=payload)).json()
                assert body == {"code": 500, "message": "internal_error"}

                # 7. zone_id 不合法 -> code=500 invalid_zone。
                behavior["mode"] = "ok"
                body = await (await client.post("/api/real-name", json=payload | {"zone_id": 0})).json()
                assert body == {"code": 500, "message": "invalid_zone"}

                # 8. 限流拒绝与排队均回 429，且不调用 RPC。
                prev_calls = len(calls)
                limiter.decision = ("deny", "ACCOUNT_COOLDOWN")
                body = await (await client.post("/api/real-name", json=payload)).json()
                assert body == {"code": 429, "message": "ACCOUNT_COOLDOWN"}
                limiter.decision = ("queue", 1500)
                body = await (await client.post("/api/real-name", json=payload)).json()
                assert body == {"code": 429, "message": "rate_limited"}
                assert len(calls) == prev_calls

        # 确认姓名、身份证号与异常原文均未落盘日志。
        for record in caplog.records:
            msg = record.getMessage()
            assert "张三" not in msg
            assert "11010519491231002X" not in msg
            assert "secret-id-number" not in msg

    asyncio.run(run())


def test_account_deletion_validation_rules():
    assert validate_delete_request(0, "alice", "pw", "", "", "") == "invalid_zone_id"
    assert validate_delete_request(-1, "alice", "pw", "", "", "") == "invalid_zone_id"
    assert validate_delete_request(2**31, "alice", "pw", "", "", "") == "invalid_zone_id"
    assert validate_delete_request(2**31 - 1, "alice", "pw", "", "", "") is None

    # 账号按 Unicode 码点数计算：191 个表情仍合法。
    assert validate_delete_request(1, "😀" * MAX_ACCOUNT_CODE_POINTS, "pw", "", "", "") is None
    assert validate_delete_request(1, "a" * (MAX_ACCOUNT_CODE_POINTS + 1), "pw", "", "", "") == "field_too_long"
    assert validate_delete_request(1, "alice", "p" * (MAX_PASSWORD_CHARS + 1), "", "", "") == "field_too_long"
    assert validate_delete_request(1, "", "", "apple", "t" * MAX_TOKEN_CHARS, "c" * MAX_TOKEN_CHARS) is None
    assert validate_delete_request(1, "", "", "apple", "t", "c" * (MAX_TOKEN_CHARS + 1)) == "field_too_long"

    assert validate_cancel_request(0, "tok") == "invalid_zone_id"
    assert validate_cancel_request(1, "") == "empty_access_token"
    assert validate_cancel_request(1, "   ") == "empty_access_token"
    assert validate_cancel_request(1, "t" * (MAX_TOKEN_CHARS + 1)) == "field_too_long"
    assert validate_cancel_request(1, "tok") is None


def test_account_deletion_and_cancel_http_contract():
    async def run():
        protocol = Protocol()
        behavior = {"mode": "accepted"}
        calls = []

        class Rpc:
            async def call(self, zone, service, method, request, response, **kwargs):
                calls.append((zone, service, method, request))
                mode = behavior["mode"]
                if mode == "accepted":
                    return protocol.message(
                        "loginpb.AccountDeletionResponse",
                        state=1 if method == "RequestAccountDeletion" else 0,
                        execute_after=1_800_000_000 if method == "RequestAccountDeletion" else 0,
                    )
                if mode == "blocked":
                    return protocol.message(
                        "loginpb.AccountDeletionResponse",
                        error_message={"id": 2039},
                        state=0,
                        execute_after=0,
                        blockers=[
                            {"player_id": 101, "reason": "guild_leader"},
                            {"player_id": 102, "reason": "role_listed"},
                        ],
                    )
                if mode == "fault":
                    return protocol.message(
                        "loginpb.AccountDeletionResponse",
                        error_message={"id": 2035},
                    )
                if mode == "unauth":
                    raise _FakeRpcError(grpc.StatusCode.UNAUTHENTICATED)
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE)

        limiter = _RecordingLimiter()
        app = web.Application()
        deps = SimpleNamespace(protocol=protocol, rpc=Rpc(), db=_DummyDb(), limiter=limiter)
        register_account_routes(app, deps)

        async with TestClient(TestServer(app)) as client:
            # 1. 参数校验失败在限流与 RPC 之前回 401。
            body = await (await client.post("/api/account/delete", json={"zone_id": 0, "account": "alice"})).json()
            assert body == {"code": 401, "message": "invalid_zone_id", "tip": 0}
            body = await (await client.post("/api/account/delete/cancel", json={"zone_id": 3})).json()
            assert body == {"code": 401, "message": "empty_access_token", "tip": 0}
            assert not limiter.calls and not calls

            # 2. 口令账号申请注销成功，与 /api/login 共用 login scope 冷却键。
            body = await (await client.post(
                "/api/account/delete",
                json={"zone_id": 3, "account": "alice", "password": "pw"},
            )).json()
            assert body == {"code": 0, "tip": 0, "state": 1, "execute_after": 1_800_000_000}
            assert calls[-1][:3] == (3, "LoginPreGate", "RequestAccountDeletion")
            assert limiter.calls[-1][2] == cooldown_key("alice") and limiter.calls[-1][3] == "login"

            # 3. Apple 预检挂起清单透传，冷却键取 auth_token 摘要。
            behavior["mode"] = "blocked"
            body = await (await client.post(
                "/api/account/delete",
                json={
                    "zone_id": 3,
                    "auth_type": "apple",
                    "auth_token": "SYNTHETIC_ID_TOKEN",
                    "authorization_code": "apple-code",
                },
            )).json()
            assert body == {
                "code": 401,
                "message": "upstream_err=2039",
                "tip": 2039,
                "state": 0,
                "execute_after": 0,
                "blockers": [
                    {"player_id": 101, "reason": "guild_leader"},
                    {"player_id": 102, "reason": "role_listed"},
                ],
            }
            assert calls[-1][3].authorization_code == "apple-code"
            assert limiter.calls[-1][2] == cooldown_key("SYNTHETIC_ID_TOKEN")
            assert "SYNTHETIC_ID_TOKEN" not in limiter.calls[-1][2]

            # 4. 故障 tip -> code=500。
            behavior["mode"] = "fault"
            body = await (await client.post(
                "/api/account/delete",
                json={"zone_id": 3, "account": "alice", "password": "pw"},
            )).json()
            assert body["code"] == 500 and body["tip"] == 2035 and body["message"] == "upstream_err=2035"

            # 5. 撤销成功回 state=0、execute_after=0。
            behavior["mode"] = "accepted"
            body = await (await client.post(
                "/api/account/delete/cancel",
                json={"zone_id": 3, "access_token": "SYNTHETIC_ACCESS"},
            )).json()
            assert body == {"code": 0, "tip": 0, "state": 0, "execute_after": 0}
            assert calls[-1][:3] == (3, "LoginPreGate", "CancelAccountDeletion")
            assert calls[-1][3].access_token == "SYNTHETIC_ACCESS"
            assert limiter.calls[-1][2] == cooldown_key("SYNTHETIC_ACCESS") and limiter.calls[-1][3] == "login"

            # 6. gRPC UNAUTHENTICATED / UNAVAILABLE 不携带 state。
            behavior["mode"] = "unauth"
            body = await (await client.post(
                "/api/account/delete/cancel",
                json={"zone_id": 3, "access_token": "SYNTHETIC_ACCESS"},
            )).json()
            assert body == {"code": 401, "message": "UNAUTHENTICATED", "tip": 0}

            behavior["mode"] = "unavailable"
            body = await (await client.post(
                "/api/account/delete",
                json={"zone_id": 3, "account": "alice", "password": "pw"},
            )).json()
            assert body == {"code": 500, "message": "login_unavailable", "tip": 0}

            # 7. 限流拒绝与排队均回 429，且不调用 RPC。
            prev = len(calls)
            limiter.decision = ("deny", "ACCOUNT_COOLDOWN")
            body = await (await client.post(
                "/api/account/delete",
                json={"zone_id": 3, "account": "alice", "password": "pw"},
            )).json()
            assert body == {"code": 429, "message": "ACCOUNT_COOLDOWN", "tip": 0}

            limiter.decision = ("queue", 2500)
            body = await (await client.post(
                "/api/account/delete/cancel",
                json={"zone_id": 3, "access_token": "SYNTHETIC_ACCESS"},
            )).json()
            assert body == {"code": 429, "message": "QUEUEING", "tip": 0}
            assert len(calls) == prev

    asyncio.run(run())


class _MemoryPointerStore:
    def __init__(self):
        self.kv: dict[str, TablePointerEntry] = {}
        self.rev = 1
        self.force_conflict = False

    async def get(self, key: str):
        return self.kv.get(key)

    async def list(self, prefix: str):
        return dict(sorted((k, v) for k, v in self.kv.items() if k.startswith(prefix)))

    async def compare_and_put(self, key: str, expected_mod_revision: int, value: str) -> bool:
        if self.force_conflict:
            return False
        cur = self.kv.get(key)
        cur_rev = 0 if cur is None else cur.mod_revision
        if cur_rev != expected_mod_revision:
            return False
        self.rev += 1
        self.kv[key] = TablePointerEntry(value, self.rev)
        return True

    async def compare_and_delete(self, key: str, expected_mod_revision: int) -> bool:
        if self.force_conflict:
            return False
        cur = self.kv.get(key)
        if cur is None or cur.mod_revision != expected_mod_revision:
            return False
        del self.kv[key]
        return True


def test_admin_tables_publish_fold_cancel_status_and_delete():
    async def run():
        store = _MemoryPointerStore()
        now_ms = [1_800_000_000_000]
        app = web.Application()
        deps = SimpleNamespace(protocol=Protocol(), directory=None, settings=_portal_settings())
        register_table_routes(app, deps, store=store, clock=lambda: now_ms[0])
        headers = {"X-Admin-Key": "test-admin-secret-key", OPERATOR_HEADER: "ops.alice"}

        async with TestClient(TestServer(app)) as client:
            # 未鉴权返回 401。
            assert (await client.get("/admin/tables")).status == 401

            # 非 immediate 且提前量不足 60s -> 400。
            resp = await client.put("/admin/tables", headers=headers, json={
                "track": "stable",
                "schema_digest": _SCHEMA,
                "content_digest": _DIGEST_V1,
                "bundle_sha256": _SHA_V1,
                "version": 1,
                "activate_at_ms": now_ms[0] + 30_000,
                "immediate": False,
                "reason": "too soon",
            })
            assert resp.status == 400

            # 非法轨道 -> 400。
            resp = await client.put("/admin/tables", headers=headers, json={
                "track": "zone-1",
                "schema_digest": _SCHEMA,
                "content_digest": _DIGEST_V1,
                "bundle_sha256": _SHA_V1,
                "version": 1,
                "immediate": True,
            })
            assert resp.status == 400

            # 首次发布 v1（60 秒后生效）：current 与 active 均省略。
            resp = await client.put("/admin/tables", headers=headers, json={
                "track": "stable",
                "schema_digest": _SCHEMA,
                "content_digest": _DIGEST_V1,
                "bundle_sha256": _SHA_V1,
                "version": 1,
                "activate_at_ms": now_ms[0] + 60_000,
                "immediate": False,
                "reason": "release v1",
            })
            assert resp.status == 200
            v1 = await resp.json()
            assert "current" not in v1 and "active" not in v1
            assert v1["next"]["content_digest"] == _DIGEST_V1
            assert v1["updated_by"] == "ops.alice" and v1["reason"] == "release v1"

            # 到点前撤销 next 成功。
            resp = await client.delete(f"/admin/tables/next?track=stable&schema_digest={_SCHEMA}", headers=headers)
            assert resp.status == 200
            cancelled = await resp.json()
            assert "next" not in cancelled and cancelled["reason"] == "cancelled"

            # 重新发布 v1 并推进 61 秒，再发布 immediate v2 -> v1 折进 current。
            resp = await client.put("/admin/tables", headers=headers, json={
                "track": "stable",
                "schema_digest": _SCHEMA,
                "content_digest": _DIGEST_V1,
                "bundle_sha256": _SHA_V1,
                "version": 1,
                "activate_at_ms": now_ms[0] + 60_000,
                "immediate": False,
                "reason": "v1 again",
            })
            assert resp.status == 200
            now_ms[0] += 61_000

            # 到点后撤销 next -> 409。
            resp = await client.delete(f"/admin/tables/next?track=stable&schema_digest={_SCHEMA}", headers=headers)
            assert resp.status == 409

            # 发布 immediate v2 -> v1 折进 current 且去掉 activate_at_ms/immediate。
            resp = await client.put("/admin/tables", headers=headers, json={
                "track": "stable",
                "schema_digest": _SCHEMA,
                "content_digest": _DIGEST_V2,
                "bundle_sha256": _SHA_V2,
                "version": 2,
                "immediate": True,
                "reason": "hotfix v2",
            })
            assert resp.status == 200
            v2 = await resp.json()
            assert v2["current"] == {"content_digest": _DIGEST_V1, "bundle_sha256": _SHA_V1, "version": 1}
            assert v2["next"]["content_digest"] == _DIGEST_V2 and v2["next"]["immediate"] is True
            assert v2["active"]["content_digest"] == _DIGEST_V2

            # CAS 冲突 -> 409。
            store.force_conflict = True
            resp = await client.put("/admin/tables", headers=headers, json={
                "track": "stable",
                "schema_digest": _SCHEMA,
                "content_digest": _DIGEST_V1,
                "bundle_sha256": _SHA_V1,
                "version": 3,
                "immediate": True,
            })
            assert resp.status == 409
            store.force_conflict = False

            # 注入在线 Pod 状态回报与损坏指针线，验证 GET /admin/tables 聚合与删线守卫。
            store.kv["/mmorpg/tables/status/stable/scene-0"] = TablePointerEntry(
                f'{{"service":"scene","schema_digest":"{_SCHEMA}","loaded_digest":"{_DIGEST_V2}"}}', 10
            )
            store.kv["/mmorpg/tables/status/stable/battle-0"] = TablePointerEntry(
                f'{{"service":"battle","schema_digest":"{_SCHEMA}","loaded_digest":"{_DIGEST_V1}",'
                f'"pending_state":"rejected","pending_digest":"{_DIGEST_V2}","pending_reason":"invalid skill"}}', 11
            )
            store.kv["/mmorpg/tables/status/stable/match-0"] = TablePointerEntry(
                f'{{"service":"match","schema_digest":"{_SCHEMA}","loaded_digest":"{_DIGEST_V1}",'
                f'"pending_state":"needs_restart","pending_digest":"{_DIGEST_V2}","pending_reason":"ZoneGroup"}}', 12
            )
            store.kv["/mmorpg/tables/pointer/canary/fedcba9876543210"] = TablePointerEntry("not-json", 13)

            views = await (await client.get("/admin/tables", headers=headers)).json()
            assert len(views) == 2
            canary_view, stable_view = views[0], views[1]
            assert canary_view["track"] == "canary" and canary_view["valid"] is False
            assert stable_view["summary"] == {
                "total": 3,
                "loaded": 1,
                "prepared": 0,
                "rejected": 1,
                "needs_restart": 1,
            }
            assert stable_view["rejected_pods"] == [{
                "pod": "battle-0",
                "service": "battle",
                "state": "rejected",
                "digest": _DIGEST_V2,
                "reason": "invalid skill",
            }]
            assert stable_view["needs_restart_pods"][0]["pod"] == "match-0"

            # 存在在线回报 Pod 时拒绝删除 -> 409。
            resp = await client.delete(f"/admin/tables?track=stable&schema_digest={_SCHEMA}", headers=headers)
            assert resp.status == 409

            # 清空在线回报后删除成功。
            for pod_key in [k for k in store.kv if k.startswith("/mmorpg/tables/status/stable/")]:
                del store.kv[pod_key]
            resp = await client.delete(f"/admin/tables?track=stable&schema_digest={_SCHEMA}", headers=headers)
            assert resp.status == 200
            assert await resp.json() == {"track": "stable", "schema_digest": _SCHEMA, "deleted": True}
            assert await store.get(f"/mmorpg/tables/pointer/stable/{_SCHEMA}") is None

            # 再次删除不存在的线 -> 404。
            resp = await client.delete(f"/admin/tables?track=stable&schema_digest={_SCHEMA}", headers=headers)
            assert resp.status == 404

    asyncio.run(run())


def test_etcd_table_pointer_store_grpc_cas_and_portal_wiring():
    async def run():
        protocol = Protocol()
        kv_data: dict[bytes, tuple[bytes, int]] = {}
        rev = [0]

        async def handle_range(raw_req, _ctx):
            req = protocol.parse("etcdserverpb.RangeRequest", raw_req)
            kvs = []
            for k, (v, mod_rev) in sorted(kv_data.items()):
                if req.range_end:
                    if req.key <= k < req.range_end:
                        kvs.append({"key": k, "value": v, "mod_revision": mod_rev})
                elif k == req.key:
                    kvs.append({"key": k, "value": v, "mod_revision": mod_rev})
            return protocol.message("etcdserverpb.RangeResponse", kvs=kvs).SerializeToString()

        async def handle_txn(raw_req, _ctx):
            req = protocol.parse("etcdserverpb.TxnRequest", raw_req)
            matched = True
            for cmp_op in req.compare:
                cur_rev = kv_data[cmp_op.key][1] if cmp_op.key in kv_data else 0
                if cur_rev != cmp_op.mod_revision:
                    matched = False
                    break
            if matched:
                for op in req.success:
                    if op.HasField("request_put"):
                        rev[0] += 1
                        kv_data[op.request_put.key] = (op.request_put.value, rev[0])
                    elif op.HasField("request_delete_range"):
                        kv_data.pop(op.request_delete_range.key, None)
            return protocol.message("etcdserverpb.TxnResponse", succeeded=matched).SerializeToString()

        server = grpc.aio.server()
        server.add_generic_rpc_handlers((
            grpc.method_handlers_generic_handler("etcdserverpb.KV", {
                "Range": grpc.unary_unary_rpc_method_handler(handle_range),
                "Txn": grpc.unary_unary_rpc_method_handler(handle_txn),
            }),
        ))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        client = GrpcClient(f"127.0.0.1:{port}", timeout=3)
        try:
            directory = SimpleNamespace(client=client)
            store = EtcdTablePointerStore(protocol, directory)
            key = f"/mmorpg/tables/pointer/stable/{_SCHEMA}"
            assert await store.get(key) is None
            assert await store.compare_and_put(key, 0, '{"v":1}') is True
            assert await store.compare_and_put(key, 0, '{"v":2}') is False
            entry = await store.get(key)
            assert entry == TablePointerEntry('{"v":1}', 1)
            assert (await store.list("/mmorpg/tables/pointer/")) == {key: entry}
            assert await store.compare_and_put(key, 1, '{"v":2}') is True
            assert await store.compare_and_delete(key, 1) is False
            assert await store.compare_and_delete(key, 2) is True
            assert await store.get(key) is None

            # 验证 create_app 已正确挂载全部新路由，且 directory=None 时 /admin/tables 返回 503。
            deps = SimpleNamespace(
                settings=_portal_settings(),
                protocol=protocol,
                db=_DummyDb(),
                redis=None,
                directory=None,
                rpc=SimpleNamespace(),
                limiter=Limiter(None, _portal_settings()),
            )
            app = create_app(deps)
            async with TestClient(TestServer(app)) as http:
                resp = await http.get("/admin/tables", headers={"X-Admin-Key": "test-admin-secret-key"})
                assert resp.status == 503
                resp = await http.post("/api/account/delete/cancel", json={"zone_id": 0})
                assert resp.status == 200 and (await resp.json())["message"] == "invalid_zone_id"
                resp = await http.post("/api/real-name", json={"zone_id": 0})
                assert resp.status == 200 and (await resp.json())["message"] == "invalid_zone"
        finally:
            await client.close()
            await server.stop(0)

    asyncio.run(run())
