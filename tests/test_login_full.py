"""Python login 全量生产对齐测试：覆盖排队、排空迁移、区角色数、实名防沉迷、账号注销、运维面、生产口令、内容安全、合服重定向与 KillSwitch。"""

import asyncio
import datetime as dt
import sqlite3
import time

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from login_fakes import FakeRedis
from xuanming.cluster import LOGIN
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata
from xuanming.kill_switch import KillSwitch
from xuanming.login import (
    account_deletion,
    anti_addiction,
    gate_drain,
    keys,
    password_auth,
    queue,
    tips,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.login.config import LoginConfig
from xuanming.login.directory import DataService
from xuanming.login.logic import (
    assign_gate,
    create_player,
    enter_game,
    login,
)
from xuanming.login.service import LoginService, ServiceContext
from xuanming.login.sessions import PlayerLocator
from xuanming.login.tokens import AccessRejected, TokenManager
from xuanming.protocol import Protocol

DEV_SECRET = "dev-shared-secret"
ADMIN_TOKEN = "admin-super-secret-token-32bytes"


def _make_id_number(birth_yyyymmdd: str, seq: str = "001") -> str:
    """按 GB 11643 计算有效 18 位身份证号（仅供测试）。"""
    body = f"110101{birth_yyyymmdd}{seq}"
    weights = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
    codes = "10X98765432"
    check = codes[sum(int(c) * w for c, w in zip(body, weights)) % 11]
    return body + check


class _RpcError(Exception):
    def __init__(self, status_code: grpc.StatusCode, detail: str = ""):
        super().__init__(detail or status_code.name)
        self._code = status_code

    def code(self):
        return self._code


class FullCluster:
    """覆盖全部 data_service / player_locator / scene_manager / content_guard RPC 的测试替身。"""

    def __init__(self, protocol: Protocol):
        self.protocol = protocol
        self.roles: dict[str, list] = {}
        self.real_names: dict[str, str] = {}
        self.deletion_state: dict[str, tuple[int, int]] = {}  # account -> (state, execute_after_ms)
        self.deletion_blockers: dict[str, list[tuple[int, str]]] = {}
        self.deletion_sources: list[tuple[str, int]] = []
        self.ledgers: set[int] = set()
        self.routes: dict[int, int] = {}
        self.initial_commits: set[int] = set()
        self.remove_conflicts: set[int] = set()
        self.zone_role_counts: dict[int, int] = {1: 12345}
        self.count_error = False
        self.sessions: dict[int, object] = {}
        self.enter_calls: list[tuple] = []
        self.registered_zones: list[tuple[int, int, int]] = []
        self.next_id = 100

    async def data_service(self, path: str, data: bytes, timeout=None) -> bytes:
        p = self.protocol
        method = path.rsplit("/", 1)[1]
        if method == "AllocateIdSegment":
            lo = self.next_id
            self.next_id += 100
            return p.message("data_service.AllocateIdSegmentResponse", lo=lo, hi=self.next_id).SerializeToString()
        if method == "ReadAccountDirectory":
            req = p.parse("data_service.ReadAccountDirectoryRequest", data)
            if req.account not in self.roles:
                if not req.create_if_missing:
                    raise _RpcError(grpc.StatusCode.NOT_FOUND)
                self.roles[req.account] = []
            state, execute_ms = self.deletion_state.get(req.account, (0, 0))
            return p.message(
                "data_service.AccountDirectoryResponse",
                players={"players": self.roles[req.account]},
                real_name_pi=self.real_names.get(req.account, ""),
                lifecycle={"deletion_state": state, "deletion_execute_after_ms": execute_ms},
            ).SerializeToString()
        if method == "CreateAccountRole":
            req = p.parse("data_service.CreateAccountRoleRequest", data)
            roles = self.roles.setdefault(req.account, [])
            roles.append(req.role)
            return p.message("data_service.AccountDirectoryResponse", players={"players": roles}).SerializeToString()
        if method == "LookupAccountRole":
            req = p.parse("data_service.LookupAccountRoleRequest", data)
            for acc, roles in self.roles.items():
                for role in roles:
                    if role.player_id == req.player_id:
                        state, _ = self.deletion_state.get(acc, (0, 0))
                        return p.message(
                            "data_service.AccountRoleOwnership",
                            account=acc,
                            role=role,
                            owner_revision=7,
                            initial_player_commit=(req.player_id in self.initial_commits),
                            account_deletion_state=state,
                        ).SerializeToString()
            raise _RpcError(grpc.StatusCode.NOT_FOUND)
        if method == "RemoveAccountRoles":
            req = p.parse("data_service.RemoveAccountRolesRequest", data)
            pids = {r.player_id for r in req.roles}
            if pids & self.remove_conflicts:
                raise _RpcError(grpc.StatusCode.FAILED_PRECONDITION)
            roles = self.roles.get(req.account, [])
            before = len(roles)
            self.roles[req.account] = [r for r in roles if r.player_id not in pids]
            if len(self.roles[req.account]) == before:
                raise _RpcError(grpc.StatusCode.NOT_FOUND)
            return b""
        if method == "SetAccountRealName":
            req = p.parse("data_service.SetAccountRealNameRequest", data)
            stored = self.real_names.setdefault(req.account, req.real_name_pi)
            return p.message("data_service.SetAccountRealNameResponse", real_name_pi=stored).SerializeToString()
        if method == "ResetAccountRealName":
            req = p.parse("data_service.ResetAccountRealNameRequest", data)
            if req.account not in self.roles:
                raise _RpcError(grpc.StatusCode.NOT_FOUND)
            had = bool(self.real_names.pop(req.account, ""))
            return p.message("data_service.ResetAccountRealNameResponse", had_real_name=had).SerializeToString()
        if method == "RequestAccountDeletion":
            req = p.parse("data_service.RequestAccountDeletionRequest", data)
            self.deletion_sources.append((req.account, req.source))
            if req.account not in self.roles:
                return p.message("data_service.AccountDeletionResponse", outcome=2).SerializeToString()
            blockers = self.deletion_blockers.get(req.account, [])
            if blockers:
                resp = p.message("data_service.AccountDeletionResponse", outcome=3)
                for pid, reason in blockers:
                    resp.blockers.add(player_id=pid, reason=reason)
                return resp.SerializeToString()
            execute_ms = int((time.time() + 7 * 86400) * 1000)
            self.deletion_state[req.account] = (1, execute_ms)
            return p.message(
                "data_service.AccountDeletionResponse",
                outcome=1,
                lifecycle={"deletion_state": 1, "deletion_execute_after_ms": execute_ms},
            ).SerializeToString()
        if method == "CancelAccountDeletion":
            req = p.parse("data_service.CancelAccountDeletionRequest", data)
            if req.account not in self.roles:
                return p.message("data_service.AccountDeletionResponse", outcome=2).SerializeToString()
            self.deletion_state[req.account] = (0, 0)
            return p.message(
                "data_service.AccountDeletionResponse",
                outcome=1,
                lifecycle={"deletion_state": 0, "deletion_execute_after_ms": 0},
            ).SerializeToString()
        if method == "CountZoneRoles":
            if self.count_error:
                raise _RpcError(grpc.StatusCode.UNAVAILABLE, "db down")
            req = p.parse("data_service.CountZoneRolesRequest", data)
            return p.message("data_service.CountZoneRolesResponse",
                             roles=self.zone_role_counts.get(req.zone_id, 0)).SerializeToString()
        if method == "GetPlayerAssetOpLedger":
            req = p.parse("data_service.GetPlayerAssetOpLedgerRequest", data)
            return p.message("data_service.GetPlayerAssetOpLedgerResponse",
                             found=(req.player_id in self.ledgers)).SerializeToString()
        if method == "GetPlayerHomeZone":
            req = p.parse("data_service.GetPlayerHomeZoneRequest", data)
            if req.player_id not in self.routes:
                raise _RpcError(grpc.StatusCode.NOT_FOUND)
            return p.message("data_service.GetPlayerHomeZoneResponse",
                             home_zone_id=self.routes[req.player_id]).SerializeToString()
        if method == "RegisterPlayerZone":
            req = p.parse("data_service.RegisterPlayerZoneRequest", data)
            self.routes[req.player_id] = req.home_zone_id
            self.registered_zones.append((req.player_id, req.home_zone_id, req.storage_id))
            return b""
        if method == "BatchGetPlayerHomeZone":
            req = p.parse("data_service.BatchGetPlayerHomeZoneRequest", data)
            found = {pid: self.routes[pid] for pid in req.player_ids if pid in self.routes}
            return p.message("data_service.BatchGetPlayerHomeZoneResponse", player_zone_map=found).SerializeToString()
        if method == "ReservePlayerName":
            return p.message("data_service.ReservePlayerNameResponse", result=0).SerializeToString()
        if method == "ReleasePlayerName":
            return b""
        raise AssertionError(f"unexpected DataService method {method}")

    async def player_locator(self, path: str, data: bytes, timeout=None) -> bytes:
        p = self.protocol
        method = path.rsplit("/", 1)[1]
        if method == "GetSession":
            req = p.parse("playerlocator.GetSessionRequest", data)
            sess = self.sessions.get(req.player_id)
            return p.message("playerlocator.GetSessionResponse", session=sess,
                             found=(sess is not None)).SerializeToString()
        if method == "SetSession":
            sess = p.parse("playerlocator.SetSessionRequest", data).session
            self.sessions[sess.player_id] = sess
            return b""
        if method == "MarkOffline":
            req = p.parse("playerlocator.MarkOfflineRequest", data)
            self.sessions.pop(req.player_id, None)
            return b""
        return b""

    async def scene_manager(self, path: str, data: bytes, timeout=None) -> bytes:
        req = self.protocol.parse("scene_manager.EnterSceneRequest", data)
        self.enter_calls.append((req.player_id, req.zone_id, req.gate_zone_id))
        return self.protocol.message("scene_manager.EnterSceneResponse", error_code=0).SerializeToString()


class FakeDirectory:
    def __init__(self, nodes):
        self.nodes = list(nodes)

    async def candidates(self, node_type: int, zone_id: int):
        return [n for n in self.nodes if n.zone_id == zone_id]


def _gate_node(protocol: Protocol, node_id: int, ip: str, port: int, *, zone_id: int = 1,
               player_count: int = 0, max_connections: int = 100, node_uuid: str = ""):
    return protocol.message(
        "NodeInfo",
        node_id=node_id,
        zone_id=zone_id,
        node_uuid=node_uuid or f"gate-uuid-{node_id}",
        player_count=player_count,
        max_connections=max_connections,
        launch_time=1000,
        endpoint={"ip": ip, "port": port},
        client_endpoint={"ip": ip, "port": port},
    )


def _session_details(protocol: Protocol, **overrides):
    base = dict(session_id=1001, gate_node_id=11, gate_instance_id="gate-uuid-11", session_incarnation=1)
    return protocol.message("SessionDetails", **(base | overrides))


def _make_ctx(protocol: Protocol, cluster: FullCluster, *, config: LoginConfig | None = None,
              redis: FakeRedis | None = None, directory=None, published=None, **extra) -> ServiceContext:
    cfg = config or LoginConfig(
        zone_id=1,
        gate_token_secret=b"g" * 32,
        directory_secret=b"d" * 32,
        dev_password_secret=DEV_SECRET,
        admin_token=ADMIN_TOKEN,
    )
    rdb = redis if redis is not None else FakeRedis()
    published = published if published is not None else []

    async def publish(node_id, command, key):
        published.append((node_id, command, key))

    return ServiceContext(
        protocol=protocol,
        config=cfg,
        login_redis=rdb,
        player_redis=rdb,
        control_redis=rdb,
        tokens=TokenManager(rdb),
        locator=PlayerLocator(protocol, cluster.player_locator),
        data=DataService(protocol, cluster.data_service, cfg.directory_secret),
        scene_manager_call=cluster.scene_manager,
        publish=publish,
        directory=directory,
        **extra,
    )


# ── 1. Login Queue（入队、名次、放行、轮询、重取、满队列、重连免排队）───────────────────────────


def test_login_queue_enqueue_admit_poll_retake_and_full():
    async def exercise():
        protocol = Protocol()
        cluster = FullCluster(protocol)
        now_ms = [1_700_000_000_000]
        rdb = FakeRedis(clock_ms=lambda: now_ms[0])
        g1 = _gate_node(protocol, 11, "10.0.0.11", 7001, player_count=10, max_connections=100)
        directory = FakeDirectory([g1])
        cfg = LoginConfig(
            zone_id=1, gate_token_secret=b"g" * 32, directory_secret=b"d" * 32,
            dev_password_secret=DEV_SECRET, queue_enabled=True, queue_zone_capacity=11,
            queue_max_length=2, legacy_gate_login_enabled=False,
        )
        lq = queue.LoginQueue(rdb, cfg.gate_token_secret, entry_ttl_s=3600, admit_ttl_s=60,
                              max_length=2, handoff_grace_ms=20_000, clock_ms=lambda: now_ms[0])
        ctx = _make_ctx(protocol, cluster, config=cfg, redis=rdb, directory=directory, login_queue=lq)

        # 未带 access_token 且关闭匿名分配 -> UNAUTHENTICATED (4)
        anon = await assign_gate.run(ctx, protocol.message("loginpb.AssignGateRequest", zone_id=1), None)
        assert anon.status == queue.STATUS_UNAUTHENTICATED

        # 签发 4 个账号令牌
        tokens_by_acc = {}
        for acc in ("robot_a", "robot_b", "robot_c", "robot_d"):
            cluster.roles[acc] = []
            pair = await ctx.tokens.issue(acc, "password")
            tokens_by_acc[acc] = pair.access_token

        # 第 1 个请求命中快速通道（capacity=11, online=10 -> budget=1），拿到票据并占 1 个在途名额
        r1 = await assign_gate.run(ctx, protocol.message(
            "loginpb.AssignGateRequest", zone_id=1, access_token=tokens_by_acc["robot_a"],
        ), None)
        assert r1.status == queue.STATUS_ADMITTED and r1.ip == "10.0.0.11" and r1.port == 7001

        # 第 2、3 个请求无空余名额，依次入队（rank 0、rank 1）
        r2 = await assign_gate.run(ctx, protocol.message(
            "loginpb.AssignGateRequest", zone_id=1, access_token=tokens_by_acc["robot_b"],
        ), None)
        assert r2.status == queue.STATUS_QUEUEING and r2.queue_rank == 0 and r2.queue_total == 1 and r2.queue_token

        r3 = await assign_gate.run(ctx, protocol.message(
            "loginpb.AssignGateRequest", zone_id=1, access_token=tokens_by_acc["robot_c"],
        ), None)
        assert r3.status == queue.STATUS_QUEUEING and r3.queue_rank == 1 and r3.queue_total == 2

        # 同账号重新入队不掉队尾（复用原条目）
        r2_again = await assign_gate.run(ctx, protocol.message(
            "loginpb.AssignGateRequest", zone_id=1, access_token=tokens_by_acc["robot_b"],
        ), None)
        assert r2_again.status == queue.STATUS_QUEUEING and r2_again.queue_rank == 0 and r2_again.queue_total == 2

        # 第 4 个账号超出 max_length=2 -> STATUS_QUEUE_FULL (5)
        r4 = await assign_gate.run(ctx, protocol.message(
            "loginpb.AssignGateRequest", zone_id=1, access_token=tokens_by_acc["robot_d"],
        ), None)
        assert r4.status == queue.STATUS_QUEUE_FULL and 30_000 <= r4.retry_after_ms < 40_000

        # QueryQueueStatus 查询排队中 + AssignGate 携带 queue_token 重入 + 错区拒绝
        q_status = await assign_gate.query_queue_status(
            ctx, protocol.message("loginpb.QueryQueueStatusRequest", queue_token=r2.queue_token), None,
        )
        assert q_status.status == queue.STATUS_QUEUEING and q_status.queue_rank == 0 and q_status.queue_total == 2

        reentry = await assign_gate.run(ctx, protocol.message(
            "loginpb.AssignGateRequest", zone_id=1, queue_token=r3.queue_token,
        ), None)
        assert reentry.status == queue.STATUS_QUEUEING and reentry.queue_rank == 1

        wrong_zone = await assign_gate.run(ctx, protocol.message(
            "loginpb.AssignGateRequest", zone_id=9, access_token=tokens_by_acc["robot_d"],
        ), None)
        assert wrong_zone.status == queue.STATUS_ERROR

        # 时间推进越过快速通道 handoff_grace，dispatcher 放行队首 1 条
        now_ms[0] += 25_000
        admitted_n = await lq.pop_admit(
            1, 1, budget=1,
            pick_fn=lambda z, qid: queue.AdmitSlot(node_id=11, ip="10.0.0.11", port=7001, zone_id=z),
        )
        assert admitted_n == 1

        # 第一次轮询取走放行槽并现签票据
        polled = await assign_gate.query_queue_status(
            ctx, protocol.message("loginpb.QueryQueueStatusRequest", queue_token=r2.queue_token), None,
        )
        assert polled.status == queue.STATUS_ADMITTED and polled.token_payload and polled.token_signature

        # handoff_grace 内重复轮询允许重取（兜底回包丢失）
        retaken = await assign_gate.query_queue_status(
            ctx, protocol.message("loginpb.QueryQueueStatusRequest", queue_token=r2.queue_token), None,
        )
        assert retaken.status == queue.STATUS_ADMITTED and retaken.ip == "10.0.0.11"

        # 超过重取截止后再次轮询 -> EXPIRED (3)
        now_ms[0] += 25_000
        expired = await assign_gate.query_queue_status(
            ctx, protocol.message("loginpb.QueryQueueStatusRequest", queue_token=r2.queue_token), None,
        )
        assert expired.status == queue.STATUS_EXPIRED

        # 重连免排队：robot_d 已有本区在线会话，即使队列满也直接签票
        role = protocol.message("AccountSimplePlayer", player_id=888, zone_id=1, name="老角色")
        cluster.roles["robot_d"] = [role]
        cluster.sessions[888] = protocol.message(
            "playerlocator.PlayerSession", player_id=888, session_id=55, gate_id="11",
            gate_instance_id="gate-uuid-11", account="robot_d", state=1,
        )
        reconnect_resp = await assign_gate.run(ctx, protocol.message(
            "loginpb.AssignGateRequest", zone_id=1, access_token=tokens_by_acc["robot_d"],
        ), None)
        assert reconnect_resp.status == queue.STATUS_ADMITTED

        # 轮询间隔按名次缩放
        assert queue.retry_after_for_rank(2000, 0, 60_000) == 2000
        assert queue.retry_after_for_rank(2000, 3999, 60_000) == 4000
        assert queue.retry_after_for_rank(2000, 50_000, 60_000) == 30_000
        assert queue.retry_after_for_rank(2000, 50_000, 20_000) == 10_000

    asyncio.run(exercise())


# ── 2. Gate Drain 监控与 RequestGateMigration ───────────────────────────────────────────────────


def test_gate_drain_monitor_and_migration():
    async def exercise():
        protocol = Protocol()
        cluster = FullCluster(protocol)
        rdb = FakeRedis()
        g1 = _gate_node(protocol, 11, "10.0.0.11", 7001, player_count=20, max_connections=100)
        g2 = _gate_node(protocol, 12, "10.0.0.12", 7002, player_count=5, max_connections=100)
        directory = FakeDirectory([g1, g2])

        # 开关关闭 -> DISABLED (4)
        ctx_off = _make_ctx(protocol, cluster, redis=rdb, directory=directory)
        d1 = _session_details(protocol, gate_node_id=11, gate_instance_id="gate-uuid-11", player_id=500)
        res_off = await gate_drain.sign_gate_migration(ctx_off, d1)
        assert res_off.result == gate_drain.MIGRATION_DISABLED

        cfg_on = LoginConfig(
            zone_id=1, gate_token_secret=b"g" * 32, directory_secret=b"d" * 32,
            dev_password_secret=DEV_SECRET, gate_drain_migrate_enabled=True,
        )
        ctx = _make_ctx(protocol, cluster, config=cfg_on, redis=rdb, directory=directory)

        # 未标记 draining -> NOT_DRAINING (2)
        res_nd = await gate_drain.sign_gate_migration(ctx, d1)
        assert res_nd.result == gate_drain.MIGRATION_NOT_DRAINING

        # 标记 gate 11 draining 后，为已绑角色连接签出迁移票据（目标 gate 12，绑持票人 player_id=500，target_zone_id=0）
        await gate_drain.mark_gate_draining(rdb, 1, 11, now_unix=1000, ttl_seconds=600)
        res_ok = await gate_drain.sign_gate_migration(ctx, d1)
        assert res_ok.result == gate_drain.MIGRATION_OK
        assert (res_ok.target_gate_ip, res_ok.target_gate_port) == ("10.0.0.12", 7002)
        payload = protocol.parse("GateTokenPayload", res_ok.token_payload)
        assert (payload.gate_node_id, payload.zone_id, payload.player_id, payload.target_zone_id) == (12, 1, 500, 0)

        # 选角界面未登录连接（player_id=0 且无 login_session）拒绝签票 -> UNSPECIFIED
        d_unbound = _session_details(protocol, gate_node_id=11, gate_instance_id="gate-uuid-11", player_id=0)
        assert (await gate_drain.sign_gate_migration(ctx, d_unbound)).result == gate_drain.MIGRATION_UNSPECIFIED

        # 所有目标 gate 都排空时绝不回落 -> NO_TARGET (3)
        await gate_drain.mark_gate_draining(rdb, 1, 12, now_unix=1000, ttl_seconds=600)
        assert (await gate_drain.sign_gate_migration(ctx, d1)).result == gate_drain.MIGRATION_NO_TARGET

        # 排空监控判定与停写标记发布
        await gate_drain.clear_gate_draining(rdb, 1, 12)
        policy = gate_drain.GateDrainPolicy(drained_below_players=5, deadline_seconds=300)
        drained = await gate_drain.evaluate_draining_gates(
            rdb, 1,
            [gate_drain.GateOnline(11, player_count=3, instance_id="gate-uuid-11"),
             gate_drain.GateOnline(12, player_count=50, instance_id="gate-uuid-12")],
            policy,
            now_unix=1050,
        )
        assert len(drained) == 1 and drained[0].node_id == 11 and drained[0].reason == gate_drain.DRAIN_REASON_BELOW_THRESHOLD
        assert await rdb.get(keys.gate_drained_key(1, 11)) == b"below_threshold"

        await gate_drain.publish_stale_marks(rdb, 1, [12])
        assert await rdb.get(keys.gate_stale_key(1, 12)) == b"1"

        # 迁移许可计算
        due = await gate_drain.drain_migrations_due(
            rdb, 1,
            [gate_drain.GateOnline(11, player_count=20, instance_id="gate-uuid-11")],
            gate_drain.DrainMigratePolicy(enabled=True, rate_per_second=30, start_after_seconds=10, lease_ms=30_000),
            now_unix=1020,
        )
        assert len(due) == 1 and due[0].node_id == 11 and due[0].rate_per_second == 30

    asyncio.run(exercise())


# ── 3. GetZoneRoleCount + SubmitRealName + 防沉迷规则 ───────────────────────────────────────────


def test_zone_role_count_and_real_name_anti_addiction():
    async def exercise():
        protocol = Protocol()
        cluster = FullCluster(protocol)
        rdb = FakeRedis()
        key = Ed25519PrivateKey.generate()

        # 固定北京时间：周三 20:30（非可玩日）vs 周五 20:30（可玩时段）vs 额外节假日周三 20:30
        wed_2030 = dt.datetime(2026, 6, 3, 20, 30, tzinfo=anti_addiction.BEIJING)
        fri_2030 = dt.datetime(2026, 6, 5, 20, 30, tzinfo=anti_addiction.BEIJING)
        clock_now = [wed_2030]

        ctx = _make_ctx(protocol, cluster, redis=rdb)
        real_name_svc = anti_addiction.RealNameService(
            rdb, anti_addiction.SandboxNppaVerifier(), ctx.data, daily_limit=3, clock=lambda: clock_now[0],
        )
        aa = anti_addiction.AntiAddictionService(
            protocol, real_name_svc,
            calendar=anti_addiction.PlayCalendar(["2026-10-01"]),
            clock=lambda: clock_now[0],
        )
        ctx.anti_addiction = aa
        service = LoginService(ctx, CallerVerifier([key.public_key()]))
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}")
        try:
            # GetZoneRoleCount 正常、错区、下游异常
            cnt_raw = await client.call(
                "/loginpb.LoginPreGate/GetZoneRoleCount",
                protocol.message("loginpb.ZoneRoleCountRequest", zone_id=1).SerializeToString(),
            )
            assert protocol.parse("loginpb.ZoneRoleCountResponse", cnt_raw).roles == 12345

            with pytest.raises(grpc.aio.AioRpcError) as wrong_zone:
                await client.call(
                    "/loginpb.LoginPreGate/GetZoneRoleCount",
                    protocol.message("loginpb.ZoneRoleCountRequest", zone_id=9).SerializeToString(),
                )
            assert wrong_zone.value.code() == grpc.StatusCode.INVALID_ARGUMENT

            cluster.count_error = True
            with pytest.raises(grpc.aio.AioRpcError) as down_err:
                await client.call(
                    "/loginpb.LoginPreGate/GetZoneRoleCount",
                    protocol.message("loginpb.ZoneRoleCountRequest", zone_id=1).SerializeToString(),
                )
            assert down_err.value.code() == grpc.StatusCode.UNAVAILABLE
            cluster.count_error = False

            # 未实名账号 HTTP Login 返回 real_name_required=True；Gate Login 被拒绝（2042）
            http_raw = await client.call(
                "/loginpb.ClientPlayerLogin/Login",
                protocol.message("loginpb.LoginRequest", account="robot_minor", password=DEV_SECRET).SerializeToString(),
            )
            http_resp = protocol.parse("loginpb.LoginResponse", http_raw)
            assert http_resp.access_token and http_resp.anti_addiction.real_name_required

            sess_bytes = _session_details(protocol).SerializeToString()
            gate_path = "/loginpb.ClientPlayerLogin/Login"
            gate_raw = await client.call(
                gate_path,
                protocol.message("loginpb.LoginRequest", auth_type="access_token",
                                 auth_token=http_resp.access_token).SerializeToString(),
                metadata=session_metadata(sess_bytes, gate_path, key),
            )
            gate_resp = protocol.parse("loginpb.LoginResponse", gate_raw)
            assert gate_resp.error_message.id == tips.REAL_NAME_REQUIRED
            assert await rdb.get(keys.login_session_key("gate-uuid-11:1")) is None

            # SubmitRealName：非法 access_token -> UNAUTHENTICATED
            with pytest.raises(grpc.aio.AioRpcError) as bad_tok:
                await client.call(
                    "/loginpb.LoginPreGate/SubmitRealName",
                    protocol.message("loginpb.SubmitRealNameRequest", access_token="bad",
                                     name="张三", id_number="123").SerializeToString(),
                )
            assert bad_tok.value.code() == grpc.StatusCode.UNAUTHENTICATED

            # SubmitRealName：非法身份证号 -> REAL_NAME_INVALID (2043)
            bad_id_raw = await client.call(
                "/loginpb.LoginPreGate/SubmitRealName",
                protocol.message("loginpb.SubmitRealNameRequest", access_token=http_resp.access_token,
                                 name="小明", id_number="110101201206010010").SerializeToString(),
            )
            assert protocol.parse("loginpb.SubmitRealNameResponse", bad_id_raw).error_message.id == tips.REAL_NAME_INVALID

            # SubmitRealName：合法未成年身份证（2012-06-01，14 岁）绑定成功
            minor_id = _make_id_number("20120601")
            ok_raw = await client.call(
                "/loginpb.LoginPreGate/SubmitRealName",
                protocol.message("loginpb.SubmitRealNameRequest", access_token=http_resp.access_token,
                                 name="小明", id_number=minor_id).SerializeToString(),
            )
            ok_resp = protocol.parse("loginpb.SubmitRealNameResponse", ok_raw)
            assert not ok_resp.HasField("error_message") and ok_resp.anti_addiction.minor
            assert ok_resp.anti_addiction.play_deadline_ms == 0  # 周三不可玩

            # 周三 20:30 Gate Login 拦截 -> MINOR_NOT_IN_PLAY_TIME (2048)
            gate_wed = protocol.parse("loginpb.LoginResponse", await client.call(
                gate_path,
                protocol.message("loginpb.LoginRequest", auth_type="access_token",
                                 auth_token=http_resp.access_token).SerializeToString(),
                metadata=session_metadata(sess_bytes, gate_path, key),
            ))
            assert gate_wed.error_message.id == tips.MINOR_NOT_IN_PLAY_TIME

            # 切到周五 20:30 北京时间 -> 放行并下发 21:00 截止毫秒
            clock_now[0] = fri_2030
            gate_fri = protocol.parse("loginpb.LoginResponse", await client.call(
                gate_path,
                protocol.message("loginpb.LoginRequest", auth_type="access_token",
                                 auth_token=http_resp.access_token).SerializeToString(),
                metadata=session_metadata(sess_bytes, gate_path, key),
            ))
            assert not gate_fri.HasField("error_message") and gate_fri.anti_addiction.minor
            expected_deadline = int(dt.datetime(2026, 6, 5, 21, 0, tzinfo=anti_addiction.BEIJING).timestamp() * 1000)
            assert gate_fri.anti_addiction.play_deadline_ms == expected_deadline

            # 节假日额外可玩日（2026-10-01 周四 20:15）也放行；充值档位核对
            holiday_2015 = dt.datetime(2026, 10, 1, 20, 15, tzinfo=anti_addiction.BEIJING)
            assert aa.calendar.play_deadline(holiday_2015) is not None
            assert anti_addiction.spend_limit_at(7) == anti_addiction.SpendLimit()
            assert anti_addiction.spend_limit_at(14).monthly_fen == 200_00
            assert anti_addiction.spend_limit_at(17).monthly_fen == 400_00
            assert anti_addiction.spend_limit_at(18).unlimited is True
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(exercise())


# ── 4. RequestAccountDeletion / CancelAccountDeletion ──────────────────────────────────────────


def test_account_deletion_request_kick_revoke_and_cancel():
    async def exercise():
        protocol = Protocol()
        cluster = FullCluster(protocol)
        published = []
        rdb = FakeRedis()
        key = Ed25519PrivateKey.generate()
        ctx = _make_ctx(protocol, cluster, redis=rdb, published=published)

        service = LoginService(ctx, CallerVerifier([key.public_key()]))
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}")
        try:
            # 登录并准备一个在线角色
            http_resp = protocol.parse("loginpb.LoginResponse", await client.call(
                "/loginpb.ClientPlayerLogin/Login",
                protocol.message("loginpb.LoginRequest", account="robot_del", password=DEV_SECRET).SerializeToString(),
            ))
            role = protocol.message("AccountSimplePlayer", player_id=701, class_id=1, gender=1, zone_id=1, name="剑客")
            cluster.roles["robot_del"] = [role]
            cluster.sessions[701] = protocol.message(
                "playerlocator.PlayerSession", player_id=701, session_id=9001, gate_id="11",
                gate_instance_id="gate-uuid-11", account="robot_del", state=1, login_generation=3,
            )

            # 申请注销不接受 access_token -> ACCOUNT_NOT_FOUND (2000)
            rej = protocol.parse("loginpb.AccountDeletionResponse", await client.call(
                "/loginpb.LoginPreGate/RequestAccountDeletion",
                protocol.message("loginpb.AccountDeletionRequest", auth_type="access_token",
                                 auth_token=http_resp.access_token).SerializeToString(),
            ))
            assert rej.error_message.id == tips.ACCOUNT_NOT_FOUND

            # 有挂起阻断项（帮主）-> ACCOUNT_DELETION_BLOCKED (2039) + blockers
            cluster.deletion_blockers["robot_del"] = [(701, "guild_leader")]
            blocked = protocol.parse("loginpb.AccountDeletionResponse", await client.call(
                "/loginpb.LoginPreGate/RequestAccountDeletion",
                protocol.message("loginpb.AccountDeletionRequest", account="robot_del",
                                 password=DEV_SECRET).SerializeToString(),
            ))
            assert blocked.error_message.id == tips.ACCOUNT_DELETION_BLOCKED
            assert [(b.player_id, b.reason) for b in blocked.blockers] == [(701, "guild_leader")]

            # 清除阻断项后正式申请注销 -> 进入冷静期 (state=1)，旧令牌全部吊销并推送踢线事件
            cluster.deletion_blockers.pop("robot_del")
            req_ok = protocol.parse("loginpb.AccountDeletionResponse", await client.call(
                "/loginpb.LoginPreGate/RequestAccountDeletion",
                protocol.message("loginpb.AccountDeletionRequest", account="robot_del",
                                 password=DEV_SECRET).SerializeToString(),
            ))
            assert not req_ok.HasField("error_message") and req_ok.state == 1 and req_ok.execute_after > 0
            assert published and published[-1][1].event_id == protocol.event_ids["ContractsKafkaKickPlayerEvent"]
            with pytest.raises(AccessRejected):
                await ctx.tokens.validate_access(http_resp.access_token)

            # 冷静期内再次 HTTP Login 返回 account_deletion_state=1 与新令牌；此时建角/进场被拒 (2038)
            cooling_login = protocol.parse("loginpb.LoginResponse", await client.call(
                "/loginpb.ClientPlayerLogin/Login",
                protocol.message("loginpb.LoginRequest", account="robot_del", password=DEV_SECRET).SerializeToString(),
            ))
            assert cooling_login.account_deletion_state == 1 and cooling_login.access_token

            sess = _session_details(protocol)
            await rdb.set(keys.login_session_key("gate-uuid-11:1"), "robot_del")
            create_blocked = await create_player.run(
                ctx, protocol.message("loginpb.CreatePlayerRequest", name="新角色"), sess,
            )
            assert create_blocked.error_message.id == tips.ACCOUNT_DELETION_PENDING
            enter_blocked = await enter_game.run(
                ctx, protocol.message("loginpb.EnterGameRequest", player_id=701), sess,
            )
            assert enter_blocked.error_message.id == tips.ACCOUNT_DELETION_PENDING

            # 凭冷静期 access_token 撤销注销 -> state=0
            cancelled = protocol.parse("loginpb.AccountDeletionResponse", await client.call(
                "/loginpb.LoginPreGate/CancelAccountDeletion",
                protocol.message("loginpb.CancelAccountDeletionRequest",
                                 access_token=cooling_login.access_token).SerializeToString(),
            ))
            assert not cancelled.HasField("error_message") and cancelled.state == 0
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(exercise())


# ── 5. LoginAdmin（3 个 RPC + fail-closed 口令鉴权）─────────────────────────────────────────────


def test_login_admin_fail_closed_auth_and_all_three_rpcs():
    async def exercise():
        protocol = Protocol()
        cluster = FullCluster(protocol)
        rdb = FakeRedis()
        key = Ed25519PrivateKey.generate()

        # 5a. 未配置 admin_token -> 整个运维面停用（fail-closed: PERMISSION_DENIED）
        cfg_disabled = LoginConfig(zone_id=1, gate_token_secret=b"g" * 32, directory_secret=b"d" * 32, admin_token="")
        ctx_disabled = _make_ctx(protocol, cluster, config=cfg_disabled, redis=rdb)
        svc_disabled = LoginService(ctx_disabled, CallerVerifier([key.public_key()]))
        srv0, port0 = await serve_grpc(protocol, "127.0.0.1:0", svc_disabled.services(), pass_context=True)
        cli0 = GrpcClient(f"127.0.0.1:{port0}")
        try:
            for path, msg in (
                ("/loginpb.LoginAdmin/RemovePlayersFromAccounts",
                 protocol.message("loginpb.RemovePlayersFromAccountsRequest", operator="ops", reason="fix")),
                ("/loginpb.LoginAdmin/ScheduleAccountDeletion",
                 protocol.message("loginpb.ScheduleAccountDeletionRequest", player_id=1, operator="ops", reason="cs")),
                ("/loginpb.LoginAdmin/ResetAccountRealName",
                 protocol.message("loginpb.ResetAccountRealNameRequest", account="a", operator="ops", reason="cs")),
            ):
                with pytest.raises(grpc.aio.AioRpcError) as err:
                    await cli0.call(path, msg.SerializeToString())
                assert err.value.code() == grpc.StatusCode.PERMISSION_DENIED
        finally:
            await cli0.close()
            await srv0.stop(None)

        # 5b. 已配置 admin_token：错口令/缺口令拒收，正确口令执行全部 3 个 RPC
        ctx = _make_ctx(protocol, cluster, redis=rdb)
        svc = LoginService(ctx, CallerVerifier([key.public_key()]))
        server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}")
        good_md = (("x-admin-token", ADMIN_TOKEN),)
        try:
            # 错误口令 -> PERMISSION_DENIED
            with pytest.raises(grpc.aio.AioRpcError) as bad_auth:
                await client.call(
                    "/loginpb.LoginAdmin/ResetAccountRealName",
                    protocol.message("loginpb.ResetAccountRealNameRequest", account="robot_1",
                                     operator="ops", reason="typo").SerializeToString(),
                    metadata=(("x-admin-token", "wrong-token"),),
                )
            assert bad_auth.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 准备孤儿与非孤儿角色：
            # 101: 真孤儿（有归属、无 ledger、有 route、非 initial_commit）
            # 102: 有存档 ledger -> NOT_ORPHAN
            # 103: 无 route -> NOT_ORPHAN
            # 104: initial_player_commit=True -> NOT_ORPHAN
            # 105: 删除时冲突 -> CONFLICT
            cluster.roles["acc_admin"] = [
                protocol.message("AccountSimplePlayer", player_id=pid, zone_id=1, name=f"r{pid}")
                for pid in (101, 102, 103, 104, 105)
            ]
            cluster.ledgers.add(102)
            for pid in (101, 102, 104, 105):
                cluster.routes[pid] = 1
            cluster.initial_commits.add(104)
            cluster.remove_conflicts.add(105)

            # dry_run 不删目录
            dry = protocol.parse("loginpb.RemovePlayersFromAccountsResponse", await client.call(
                "/loginpb.LoginAdmin/RemovePlayersFromAccounts",
                protocol.message("loginpb.RemovePlayersFromAccountsRequest",
                                 player_ids=[101, 101, 102, 103, 104, 999],
                                 operator="ops", reason="audit", dry_run=True).SerializeToString(),
                metadata=good_md,
            ))
            assert (dry.removed_count, dry.not_orphan_count, dry.not_found_count, dry.failed_count) == (1, 3, 1, 0)
            assert any(r.player_id == 101 for r in cluster.roles["acc_admin"])

            # 正式摘除
            real = protocol.parse("loginpb.RemovePlayersFromAccountsResponse", await client.call(
                "/loginpb.LoginAdmin/RemovePlayersFromAccounts",
                protocol.message("loginpb.RemovePlayersFromAccountsRequest",
                                 player_ids=[101, 105], operator="ops", reason="cleanup", dry_run=False).SerializeToString(),
                metadata=good_md,
            ))
            assert (real.removed_count, real.failed_count) == (1, 1)
            assert all(r.player_id != 101 for r in cluster.roles["acc_admin"])

            # ResetAccountRealName（同时清理 pending Redis 键）
            cluster.real_names["acc_admin"] = anti_addiction.pi_birth_prefix(1995, 1, 1) + "a" * 32
            await rdb.set(keys.real_name_pending_key("acc_admin"), "pending-ai", ex=3600)
            reset_resp = protocol.parse("loginpb.ResetAccountRealNameResponse", await client.call(
                "/loginpb.LoginAdmin/ResetAccountRealName",
                protocol.message("loginpb.ResetAccountRealNameRequest",
                                 player_id=102, operator="cs", reason="wrong id").SerializeToString(),
                metadata=good_md,
            ))
            assert not reset_resp.HasField("error_message") and reset_resp.had_real_name is True
            assert await rdb.get(keys.real_name_pending_key("acc_admin")) is None

            # XOR 校验：同时填 player_id 和 account -> INVALID_ARGUMENT
            with pytest.raises(grpc.aio.AioRpcError) as bad_xor:
                await client.call(
                    "/loginpb.LoginAdmin/ResetAccountRealName",
                    protocol.message("loginpb.ResetAccountRealNameRequest",
                                     player_id=102, account="acc_admin", operator="cs", reason="bad").SerializeToString(),
                    metadata=good_md,
                )
            assert bad_xor.value.code() == grpc.StatusCode.INVALID_ARGUMENT

            # ScheduleAccountDeletion：按角色代办注销（source=2）
            sched = protocol.parse("loginpb.AccountDeletionResponse", await client.call(
                "/loginpb.LoginAdmin/ScheduleAccountDeletion",
                protocol.message("loginpb.ScheduleAccountDeletionRequest",
                                 player_id=102, operator="cs", reason="google play web request").SerializeToString(),
                metadata=good_md,
            ))
            assert not sched.HasField("error_message") and sched.state == 1
            assert ("acc_admin", account_deletion.SOURCE_ADMIN) in cluster.deletion_sources
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(exercise())


# ── 6. Production Password Auth + SQL Store + Redis Quota ───────────────────────────────────────


def test_production_password_auth_sql_store_and_quota():
    async def exercise():
        protocol = Protocol()
        cluster = FullCluster(protocol)
        now_ms = [1_700_000_000_000]
        rdb = FakeRedis(clock_ms=lambda: now_ms[0])

        # SQLite user_accounts 表存储 scrypt PHC 哈希
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        conn.execute("CREATE TABLE user_accounts (account TEXT PRIMARY KEY, password TEXT NOT NULL)")
        pw_hash = password_auth.hash_password("S3cr3t!Pass", ln=12)
        conn.execute("INSERT INTO user_accounts (account, password) VALUES (?, ?)", ("alice", pw_hash))
        conn.commit()
        try:
            store = password_auth.SQLPasswordHashStore(conn)
            quota = password_auth.RedisPasswordFailureQuota(rdb, limit=2, window_ms=60_000, lock_ms=120_000)
            provider = password_auth.ProductionPasswordProvider(store, quota, dummy_hash=password_auth.hash_password("dummy", ln=12))
            ctx = _make_ctx(protocol, cluster, redis=rdb, password_provider=provider)

            # 正确口令登录成功
            ok = await login.run(ctx, protocol.message("loginpb.LoginRequest", account="alice", password="S3cr3t!Pass"), None)
            assert not ok.HasField("error_message") and ok.access_token

            # 连续 2 次错误口令触发锁窗 -> 第 3 次即使口令正确也被配额拒绝（对外统一 2000 防枚举）
            for _ in range(2):
                bad = await login.run(ctx, protocol.message("loginpb.LoginRequest", account="alice", password="wrong"), None)
                assert bad.error_message.id == tips.ACCOUNT_NOT_FOUND

            with pytest.raises(password_auth.PasswordQuotaDeniedError):
                await provider.validate_password("alice", "S3cr3t!Pass")
            locked = await login.run(ctx, protocol.message("loginpb.LoginRequest", account="alice", password="S3cr3t!Pass"), None)
            assert locked.error_message.id == tips.ACCOUNT_NOT_FOUND

            # 锁窗到期后恢复可登录
            now_ms[0] += 125_000
            recovered = await login.run(ctx, protocol.message("loginpb.LoginRequest", account="alice", password="S3cr3t!Pass"), None)
            assert not recovered.HasField("error_message") and recovered.access_token

            # 配额 Redis 故障 -> fail-closed 拒绝认证
            rdb.fail.add("eval")
            with pytest.raises(password_auth.PasswordQuotaUnavailableError):
                await provider.validate_password("alice", "S3cr3t!Pass")
            rdb.fail.clear()
        finally:
            conn.close()

    asyncio.run(exercise())


# ── 7. CreatePlayer: ContentGuard + 合服归属区重定向 + EnterGame 重定向 ────────────────────────


def test_create_player_content_guard_and_home_zone_redirect():
    async def exercise():
        protocol = Protocol()
        cluster = FullCluster(protocol)
        rdb = FakeRedis()
        verdict_box = [1]  # 1=PASS, 3=REJECT, -1=raise error

        async def content_guard_call(path, data, timeout=None):
            assert path == "/content_guard.ContentGuard/CheckText"
            req = protocol.parse("content_guard.CheckTextRequest", data)
            assert req.scene == 1  # TEXT_SCENE_ROLE_NAME
            if verdict_box[0] < 0:
                raise ConnectionError("content_guard unreachable")
            return protocol.message("content_guard.CheckTextResponse", verdict=verdict_box[0]).SerializeToString()

        cfg = LoginConfig(
            zone_id=1, gate_token_secret=b"g" * 32, directory_secret=b"d" * 32,
            dev_password_secret=DEV_SECRET, redirect_on_enter_enabled=True,
        )
        ctx = _make_ctx(
            protocol, cluster, config=cfg, redis=rdb,
            content_guard_call=content_guard_call,
            merged_zones={1: 2, 2: 5},
        )
        cluster.roles["robot_cg"] = []
        sess = _session_details(protocol)
        await rdb.set(keys.login_session_key("gate-uuid-11:1"), "robot_cg")

        # 内容安全拒绝 -> ROLE_NAME_SENSITIVE (2034)
        verdict_box[0] = 3
        rej = await create_player.run(ctx, protocol.message("loginpb.CreatePlayerRequest", name="违规名"), sess)
        assert rej.error_message.id == tips.ROLE_NAME_SENSITIVE

        # 内容安全不可达 -> fail-closed ROLE_NAME_CHECK_UNAVAILABLE (2050)
        verdict_box[0] = -1
        unavail = await create_player.run(ctx, protocol.message("loginpb.CreatePlayerRequest", name="正常名"), sess)
        assert unavail.error_message.id == tips.ROLE_NAME_CHECK_UNAVAILABLE

        # 内容安全通过 -> 建角落到合服最终目标区 zone=5
        verdict_box[0] = 1
        created = await create_player.run(ctx, protocol.message("loginpb.CreatePlayerRequest", name="青云客"), sess)
        assert not created.HasField("error_message")
        player = created.players[0].player
        assert player.zone_id == 5
        assert cluster.registered_zones[-1][:2] == (player.player_id, 5)

        # EnterGame 开启 redirect_on_enter_enabled：首登按 player_route 归属区 5 路由 EnterScene
        entered = await enter_game.run(ctx, protocol.message("loginpb.EnterGameRequest", player_id=player.player_id), sess)
        assert not entered.HasField("error_message")
        await asyncio.gather(*ctx.tasks)
        assert cluster.enter_calls[-1] == (player.player_id, 5, 1)

    asyncio.run(exercise())


# ── 8. KillSwitch 集成 ────────────────────────────────────────────────────────────────────────


def test_login_service_kill_switch():
    async def exercise():
        protocol = Protocol()
        cluster = FullCluster(protocol)
        rdb = FakeRedis()
        key = Ed25519PrivateKey.generate()
        ctx = _make_ctx(protocol, cluster, redis=rdb)

        login_info = protocol.message("NodeInfo", node_type=LOGIN, node_uuid="login-uuid-1")
        ks = KillSwitch(protocol, login_info)
        ks.replace(1, [
            (b"/mmorpg/killswitch/ClientPlayerLogin/CreatePlayer", b'{"deny":true,"reason":"maintenance","code":14}'),
            (b"/mmorpg/killswitch/ClientPlayerLogin/*", b'{"deny":true,"reason":"freeze","code":14}'),
        ])

        service = LoginService(ctx, CallerVerifier([key.public_key()]), kill_switch=ks)
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}")
        try:
            # 通配/精确规则拦住 CreatePlayer -> UNAVAILABLE + reason
            with pytest.raises(grpc.aio.AioRpcError) as denied:
                await client.call(
                    "/loginpb.ClientPlayerLogin/CreatePlayer",
                    protocol.message("loginpb.CreatePlayerRequest", name="小明").SerializeToString(),
                )
            assert denied.value.code() == grpc.StatusCode.UNAVAILABLE
            assert "maintenance" in denied.value.details()

            # 受保护方法 Disconnect 不被通配规则拦住
            sess_bytes = _session_details(protocol).SerializeToString()
            disc_path = "/loginpb.ClientPlayerLogin/Disconnect"
            disc_raw = await client.call(
                disc_path,
                protocol.message("loginpb.LoginNodeDisconnectRequest").SerializeToString(),
                metadata=session_metadata(sess_bytes, disc_path, key),
            )
            assert protocol.parse("loginpb.LoginEmptyResponse", disc_raw) is not None

            # LoginPreGate 不受 ClientPlayerLogin 规则影响
            cnt_raw = await client.call(
                "/loginpb.LoginPreGate/GetZoneRoleCount",
                protocol.message("loginpb.ZoneRoleCountRequest", zone_id=1).SerializeToString(),
            )
            assert protocol.parse("loginpb.ZoneRoleCountResponse", cnt_raw).roles == 12345
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(exercise())


def test_all_login_proto_rpcs_wired_and_gate_migration_over_grpc():
    async def exercise():
        protocol = Protocol()
        cluster = FullCluster(protocol)
        rdb = FakeRedis()
        key = Ed25519PrivateKey.generate()
        g1 = _gate_node(protocol, 11, "10.0.0.11", 7001, player_count=20, max_connections=100)
        g2 = _gate_node(protocol, 12, "10.0.0.12", 7002, player_count=5, max_connections=100)
        cfg = LoginConfig(
            zone_id=1, gate_token_secret=b"g" * 32, directory_secret=b"d" * 32,
            dev_password_secret=DEV_SECRET, admin_token=ADMIN_TOKEN, gate_drain_migrate_enabled=True,
        )
        ctx = _make_ctx(protocol, cluster, config=cfg, redis=rdb, directory=FakeDirectory([g1, g2]))
        service = LoginService(ctx, CallerVerifier([key.public_key()]))
        wired = service.services()

        # 核对 proto/login/login.proto 全部 3 个服务共 17 个 RPC 一个不漏
        for svc_name in ("loginpb.ClientPlayerLogin", "loginpb.LoginPreGate", "loginpb.LoginAdmin"):
            proto_methods = {m.name for m in protocol.pool.FindServiceByName(svc_name).methods}
            assert set(wired[svc_name]) == proto_methods

        await gate_drain.mark_gate_draining(rdb, 1, 11, now_unix=1000, ttl_seconds=600)
        server, port = await serve_grpc(protocol, "127.0.0.1:0", wired, pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}")
        try:
            path = "/loginpb.ClientPlayerLogin/RequestGateMigration"
            sess_bytes = _session_details(protocol, gate_node_id=11, gate_instance_id="gate-uuid-11",
                                          player_id=600).SerializeToString()
            raw = await client.call(path, b"", metadata=session_metadata(sess_bytes, path, key))
            resp = protocol.parse("loginpb.GateMigrationResponse", raw)
            assert resp.result == gate_drain.MIGRATION_OK and resp.target_gate_ip == "10.0.0.12"
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(exercise())
