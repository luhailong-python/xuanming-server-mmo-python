"""CellRoute（4096 槽位单元路由）与 OwnerAuthority（线性一致归属权威）完整回归测试。

对照 Go 仓以下测试套件逐项对齐：
- ``go/shared/cellroute/cellroute_test.go``
- ``go/login/internal/config/cellroute_test.go``
- ``go/login/internal/svc/cellroute_test.go``
- ``go/login/internal/logic/loginpregate/cellroute_affinity_test.go``
- ``go/login/internal/logic/clientplayerlogin/cellroute_guard_test.go``
- ``go/shared/ownerauthority/ownerauthority_test.go``
- ``go/scene_manager/internal/svc/ownerauthority_test.go``
- ``go/scene_manager/internal/logic/owner_authority_bridge_test.go``
并补充 Login（AssignGate / EnterGame）与 SceneManagerService（EnterScene / LeaveScene）端到端集成测试。
"""

from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import asynccontextmanager

import pytest

from login_fakes import FakeRedis
from xuanming.login import keys as login_keys, login_session, session_fence, tips
from xuanming.login.cellroute import (
    LOGICAL_CELL_COUNT,
    AtomicTable,
    CellRouteConfig,
    CellRouteError,
    CellSpec,
    Entry,
    EtcdWatchEvent,
    InvalidConfigError,
    InvalidPlayerIdError,
    Router,
    StaticTable,
    apply_payload,
    build_balanced_entries,
    build_router,
    check_cell_route_ownership,
    marshal_table_json,
    parse_table_json,
    select_cell_affinity_candidates,
)
from xuanming.login.config import LoginConfig
from xuanming.login.directory import DataService
from xuanming.login.logic import assign_gate, enter_game
from xuanming.login.queue import GateCandidate
from xuanming.login.service import ServiceContext, init_cell_router
from xuanming.login.sessions import PlayerLocator
from xuanming.login.tokens import TokenManager
from xuanming.protocol import Protocol
from xuanming.scene_manager import (
    DEFAULT_RESUME_RETRY_AFTER_MS,
    ERR_OK,
    ERR_OWNER_EPOCH_CONFLICT,
    MAX_DETAIL_LEN,
    MAX_LEASE_SECONDS,
    SCENE_NODE_TYPE_MAIN_WORLD,
    AccountRoleInfo,
    AdmitRequest,
    BarrierNotOpenError,
    BeginRequest,
    EntryState,
    EpochConflictError,
    IdentityMismatchError,
    InvalidOperationError,
    LeaseRegressedError,
    MemoryOwnerAuthorityStore,
    MemoryPlayerPlacementStore,
    MemorySceneControlStore,
    OwnerAuthorityBridge,
    OwnerAuthorityConfig,
    OwnerRecord,
    OwnerTarget,
    OwnerType,
    Phase,
    ReleaseRequest,
    RenewLeaseRequest,
    ResumeInput,
    SceneManagerConfig,
    SceneManagerService,
    SourceRevisionStaleError,
    SqlOwnerAuthorityStore,
    WaitReason,
    keys as sm_keys,
    resolve_resume,
)


# ── 测试替身 ───────────────────────────────────────────────────────────────────

class FakeClock:
    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms


class FakeEtcdWatcher:
    """内存 etcd KV/Watch 替身，用于验证热更新与畸形载荷 fail-closed。"""

    def __init__(self, initial: dict[str, bytes] | None = None, revision: int = 1) -> None:
        self.kv: dict[str, bytes] = dict(initial or {})
        self.revision = revision
        self._queue: asyncio.Queue[EtcdWatchEvent] = asyncio.Queue()

    async def get(self, key: str) -> tuple[bytes | None, int]:
        return self.kv.get(key), self.revision

    async def watch(self, key: str, start_revision: int):
        while True:
            ev = await self._queue.get()
            yield ev

    async def put(self, key: str, value: bytes) -> None:
        self.revision += 1
        self.kv[key] = value
        await self._queue.put(EtcdWatchEvent(revision=self.revision, event_type="PUT", value=value))


class FakeSqlPool:
    """支持 SqlOwnerAuthorityStore 事务 SQL 语句的极简内存连接池，验证 SQL 路径语义。"""

    def __init__(self) -> None:
        self.records: dict[int, tuple] = {}
        self.leases: dict[str, tuple[str, int, int, int]] = {}
        self.logs: list[tuple] = []

    @asynccontextmanager
    async def acquire(self):
        yield _FakeSqlConn(self)


class _FakeSqlConn:
    def __init__(self, pool: FakeSqlPool) -> None:
        self._pool = pool
        self._snap_records = None
        self._snap_leases = None
        self._snap_logs = None

    async def begin(self) -> None:
        self._snap_records = dict(self._pool.records)
        self._snap_leases = dict(self._pool.leases)
        self._snap_logs = list(self._pool.logs)

    async def commit(self) -> None:
        self._snap_records = None

    async def rollback(self) -> None:
        if self._snap_records is not None:
            self._pool.records = self._snap_records
            self._pool.leases = self._snap_leases
            self._pool.logs = self._snap_logs
            self._snap_records = None

    @asynccontextmanager
    async def cursor(self):
        yield _FakeSqlCursor(self._pool)


class _FakeSqlCursor:
    def __init__(self, pool: FakeSqlPool) -> None:
        self._pool = pool
        self._row = None

    async def execute(self, sql: str, args: tuple) -> None:
        q = sql.strip()
        self._row = None
        if q.startswith("SELECT") and "FROM owner_record" in q:
            self._row = self._pool.records.get(int(args[0]))
        elif q.startswith("SELECT") and "FROM owner_instance_lease" in q:
            lease = self._pool.leases.get(str(args[0]))
            self._row = lease[:3] if lease is not None else None
        elif q.startswith("INSERT INTO owner_record"):
            self._pool.records[int(args[0])] = tuple(args)
        elif q.startswith("UPDATE owner_record SET phase ="):
            pid = int(args[2])
            rec = list(self._pool.records[pid])
            rec[3] = int(args[0])
            rec[12] = int(args[1])
            self._pool.records[pid] = tuple(rec)
        elif q.startswith("UPDATE owner_record SET owner_type ="):
            pid = int(args[3])
            rec = list(self._pool.records[pid])
            rec[2] = int(args[0])
            rec[3] = int(args[1])
            rec[4], rec[5], rec[6], rec[7], rec[8], rec[10] = "", "", 0, "", "", 0
            rec[12] = int(args[2])
            self._pool.records[pid] = tuple(rec)
        elif q.startswith("INSERT INTO owner_instance_lease"):
            self._pool.leases[str(args[0])] = (str(args[1]), int(args[2]), int(args[3]), int(args[4]))
        elif q.startswith("INSERT INTO owner_transition_log"):
            self._pool.logs.append(tuple(args))
        else:
            raise AssertionError(f"unexpected SQL: {q}")

    async def fetchone(self):
        return self._row


# ── 一、CellRoute 核心测试（对照 cellroute_test.go） ───────────────────────────

def test_build_balanced_and_deterministic_routing():
    cells = [
        CellSpec(region_id=1, cell_id=101),
        CellSpec(region_id=1, cell_id=102),
        CellSpec(region_id=2, cell_id=201),
    ]
    entries, regions = build_balanced_entries(cells)
    table = StaticTable(entries, regions)
    router = Router(table)

    with pytest.raises(InvalidPlayerIdError):
        router.route(0)

    counts: Counter[int] = Counter()
    for pid in range(1, LOGICAL_CELL_COUNT + 1):
        loc = router.route(pid)
        assert loc.region_id == regions[loc.cell_id]
        counts[loc.cell_id] += 1

    # 4096 / 3 = 1366, 1365, 1365
    assert counts[101] == 1366
    assert counts[102] == 1365
    assert counts[201] == 1365


def test_static_table_rejects_mismatch_and_invalid_json():
    entries, regions = build_balanced_entries([CellSpec(region_id=1, cell_id=10)])

    # 篡改 region 不一致必须拒绝
    tampered = list(entries)
    tampered[100] = Entry(region_id=99, cell_id=10)
    with pytest.raises(CellRouteError):
        StaticTable(tampered, regions)

    # 长度非 4096 必须拒绝
    with pytest.raises(CellRouteError):
        StaticTable(entries[:10], regions)

    # 同一 cell 声明在多个 region 必须拒绝
    with pytest.raises(CellRouteError):
        build_balanced_entries([CellSpec(region_id=1, cell_id=10), CellSpec(region_id=2, cell_id=10)])

    # 畸形 JSON 必须拒绝
    with pytest.raises(CellRouteError):
        parse_table_json(b"{broken")


def test_atomic_table_concurrent_swap_and_json_round_trip():
    async def exercise():
        e1, r1 = build_balanced_entries([CellSpec(region_id=1, cell_id=1)])
        t1 = StaticTable(e1, r1)
        e2, r2 = build_balanced_entries([CellSpec(region_id=2, cell_id=2)])
        t2 = StaticTable(e2, r2)

        raw = marshal_table_json(7, t2, r2)
        parsed, ver = parse_table_json(raw)
        assert ver == 7

        at = AtomicTable(t1)
        router = Router(at)

        async def reader(base_id: int) -> None:
            for i in range(200):
                loc = router.route(base_id + i + 1)
                assert loc.cell_id in (1, 2)
                if i % 50 == 0:
                    await asyncio.sleep(0)

        readers = [asyncio.create_task(reader(g * 1000)) for g in range(8)]

        # 畸形更新 fail-closed，保留旧路由表
        assert apply_payload("/test/cellroute", b"not-json", at) is False
        assert router.route(42).cell_id == 1

        at.swap(parsed)
        await asyncio.gather(*readers)

        loc_after = router.route(42)
        assert (loc_after.region_id, loc_after.cell_id) == (2, 2)

    asyncio.run(exercise())


def test_build_router_modes_and_etcd_watch():
    async def exercise():
        # 空模式停用
        router, stop = await build_router(CellRouteConfig(), None)
        assert router is None and stop is None

        # 静态模式
        router, stop = await build_router(
            CellRouteConfig(
                mode="static",
                cells=(CellSpec(region_id=1, cell_id=10), CellSpec(region_id=2, cell_id=20)),
            ),
            None,
        )
        assert router is not None and stop is None
        loc = router.route(12345)
        assert loc.cell_id in (10, 20)

        # 非法模式快速失败
        with pytest.raises(InvalidConfigError):
            await build_router(CellRouteConfig(mode="bogus"), None)

        # etcd 模式缺少 client 报错
        with pytest.raises(CellRouteError):
            await build_router(CellRouteConfig(mode="etcd", etcd_key="/k"), None)

        # etcd 模式：种子冷启动 -> 热更新 -> 畸形载荷 fail-closed -> 优雅停表
        etcd = FakeEtcdWatcher()
        router, stop = await build_router(
            CellRouteConfig(
                mode="etcd",
                etcd_key="/mmorpg/cellroute",
                cells=(CellSpec(region_id=1, cell_id=10),),
                resync_interval_s=10.0,
            ),
            etcd,
        )
        assert router is not None and stop is not None
        try:
            assert router.route(42).cell_id == 10

            # 推送畸形载荷：必须保持 cell 10
            await etcd.put("/mmorpg/cellroute", b"{invalid")
            await asyncio.sleep(0.02)
            assert router.route(42).cell_id == 10

            # 推送合法 v2 路由表：热切换到 cell 20
            e2, r2 = build_balanced_entries([CellSpec(region_id=2, cell_id=20)])
            raw_v2 = marshal_table_json(2, StaticTable(e2, r2), r2)
            await etcd.put("/mmorpg/cellroute", raw_v2)
            for _ in range(20):
                if router.route(42).cell_id == 20:
                    break
                await asyncio.sleep(0.01)
            assert router.route(42).cell_id == 20
        finally:
            await stop()

    asyncio.run(exercise())


# ── 二、Login Config / ServiceContext / Affinity / Guard ──────────────────────

def test_login_config_and_init_cell_router_modes():
    async def exercise():
        cfg_default = LoginConfig(zone_id=1, gate_token_secret=b"g" * 32)
        cfg_default.cell_route.validate()

        cfg_static = LoginConfig(
            zone_id=1,
            gate_token_secret=b"g" * 32,
            cell_route=CellRouteConfig(
                mode="static",
                cells=(CellSpec(region_id=1, cell_id=10), CellSpec(region_id=2, cell_id=20)),
            ),
        )
        cfg_static.cell_route.validate()

        cfg_invalid = LoginConfig(
            zone_id=1,
            gate_token_secret=b"g" * 32,
            cell_route=CellRouteConfig(mode="invalid"),
        )
        with pytest.raises(InvalidConfigError):
            cfg_invalid.cell_route.validate()

        r0, s0 = await init_cell_router(CellRouteConfig(), None)
        assert r0 is None and s0 is None

        r1, s1 = await init_cell_router(cfg_static.cell_route, None)
        assert r1 is not None and s1 is None
        assert r1.route(123456).cell_id in (10, 20)

        with pytest.raises(InvalidConfigError):
            await init_cell_router(CellRouteConfig(mode="bogus"), None)

    asyncio.run(exercise())


def test_select_cell_affinity_candidates():
    entries, regions = build_balanced_entries([
        CellSpec(region_id=1, cell_id=10),
        CellSpec(region_id=2, cell_id=20),
    ])
    router = Router(StaticTable(entries, regions))

    candidates = [
        GateCandidate(node_id=1, ip="10.0.0.1", port=7001, zone_id=10),
        GateCandidate(node_id=2, ip="10.0.0.2", port=7002, zone_id=20),
    ]

    # Player 1 -> logical_cell 1 -> Cell 10 -> NodeID 1
    got = select_cell_affinity_candidates(router, 1, candidates)
    assert len(got) == 1 and got[0].node_id == 1

    # Player 3000 -> logical_cell 3000 -> Cell 20 -> NodeID 2
    got = select_cell_affinity_candidates(router, 3000, candidates)
    assert len(got) == 1 and got[0].node_id == 2

    # 无匹配 cell 时回退原始候选列表
    unmatched = [GateCandidate(node_id=9, ip="10.0.0.9", port=7009, zone_id=99)]
    got = select_cell_affinity_candidates(router, 1, unmatched)
    assert len(got) == 1 and got[0].node_id == 9

    # router 为 None 时原样返回
    assert len(select_cell_affinity_candidates(None, 1, candidates)) == 2


def test_check_cell_route_ownership_and_enter_game_guard():
    async def exercise():
        entries, regions = build_balanced_entries([
            CellSpec(region_id=1, cell_id=10),
            CellSpec(region_id=2, cell_id=20),
        ])
        router = Router(StaticTable(entries, regions))

        # Nil router 始终放行
        check_cell_route_ownership(None, CellRouteConfig(self_region=1, self_cell=10, enforce_ownership=True), 42)
        # 本单元玩家放行
        check_cell_route_ownership(router, CellRouteConfig(self_region=1, self_cell=10, enforce_ownership=True), 1)
        # 异单元玩家：shadow 仅观测不阻断，enforce 拒入
        check_cell_route_ownership(router, CellRouteConfig(self_region=1, self_cell=10, enforce_ownership=False), 3000)
        with pytest.raises(CellRouteError):
            check_cell_route_ownership(router, CellRouteConfig(self_region=1, self_cell=10, enforce_ownership=True), 3000)

        # 端到端 EnterGame 守卫校验
        protocol = Protocol()
        redis = FakeRedis()
        session = protocol.message(
            "SessionDetails",
            session_id=9001,
            gate_node_id=7,
            gate_instance_id="gate-uuid",
            session_incarnation=1,
        )
        conn = session_fence.connection_of(session)
        assert conn is not None
        await redis.set(login_keys.login_session_key(conn), "acc_cell", px=60_000)

        async def _unused(*args, **kwargs):
            raise AssertionError("should not reach downstream call")

        cfg_enforce = LoginConfig(
            zone_id=1,
            gate_token_secret=b"g" * 32,
            directory_secret=b"d" * 32,
            cell_route=CellRouteConfig(
                mode="static",
                cells=(CellSpec(region_id=1, cell_id=10), CellSpec(region_id=2, cell_id=20)),
                self_region=1,
                self_cell=10,
                enforce_ownership=True,
            ),
        )
        ctx = ServiceContext(
            protocol=protocol,
            config=cfg_enforce,
            login_redis=redis,
            player_redis=redis,
            tokens=TokenManager(redis),
            locator=PlayerLocator(protocol, _unused),
            data=DataService(protocol, _unused, cfg_enforce.directory_secret),
            scene_manager_call=_unused,
            publish=_unused,
            directory=None,
        )
        assert ctx.cell_router is not None
        resp = await enter_game.run(ctx, protocol.message("loginpb.EnterGameRequest", player_id=3000), session)
        assert resp.error_message.id == tips.ENTER_GAME_GUID
        await ctx.close()

    asyncio.run(exercise())


# ── 三、OwnerAuthority 核心状态机与重连协议（对照 ownerauthority_test.go） ──────

def test_owner_authority_transition_barrier_lease_fence_and_release():
    async def exercise():
        clock = FakeClock(1_700_000_000_000)
        store = MemoryOwnerAuthorityStore(skew_margin_ms=2_000, clock_ms=clock)

        hub_a = OwnerTarget(
            pod_name="scene-0", instance_uid="uid-a", instance_epoch=1,
            assignment_id="assign-1", release_track="stable",
        )
        hub_b = OwnerTarget(
            pod_name="scene-1", instance_uid="uid-b", instance_epoch=1,
            assignment_id="assign-2", release_track="stable",
        )

        # 1. 租约截断到 MaxLeaseSeconds (30s) & 重建 Pod 同/低 epoch 回退拒收
        deadline = await store.renew_instance_lease(RenewLeaseRequest(
            pod_name=hub_a.pod_name, instance_uid=hub_a.instance_uid, instance_epoch=1, lease_seconds=120,
        ))
        assert deadline == clock.now_ms + MAX_LEASE_SECONDS * 1000

        with pytest.raises(LeaseRegressedError):
            await store.renew_instance_lease(RenewLeaseRequest(
                pod_name=hub_a.pod_name, instance_uid="uid-a-new", instance_epoch=1, lease_seconds=10,
            ))

        # 2. 首次 BeginTransition (epoch 0 -> 1) & detail 截断 512 字节
        rec1 = await store.begin_transition(BeginRequest(
            player_id=1001, expect_epoch=0, target_type=OwnerType.HUB, target=hub_a,
            source_revision=10, detail="x" * 800,
        ))
        assert rec1.owner_epoch == 1 and rec1.phase == Phase.PENDING
        assert len(store.logs) == 1 and len(store.logs[0].detail) == MAX_DETAIL_LEN

        # 相同 operation_id 幂等重放返回完全相同记录
        replay1 = await store.begin_transition(BeginRequest(
            player_id=1001, expect_epoch=999, target_type=OwnerType.HUB, target=hub_a,
            operation_id=rec1.operation_id,
        ))
        assert replay1.owner_epoch == 1 and replay1.operation_id == rec1.operation_id

        # 同 operation_id 不同目标 -> IdentityMismatchError
        with pytest.raises(IdentityMismatchError):
            await store.begin_transition(BeginRequest(
                player_id=1001, expect_epoch=1, target_type=OwnerType.INSTANCE, target=hub_b,
                operation_id=rec1.operation_id,
            ))

        # 非 UUIDv4 -> InvalidOperationError
        with pytest.raises(InvalidOperationError):
            await store.begin_transition(BeginRequest(
                player_id=1001, expect_epoch=1, target_type=OwnerType.INSTANCE, target=hub_b,
                operation_id="not-a-uuid",
            ))

        # 立即 Admit 成功（无旧承载者）
        admitted1 = await store.admit(AdmitRequest(
            player_id=1001, owner_epoch=1, operation_id=rec1.operation_id, target=hub_a,
        ))
        assert admitted1.phase == Phase.ADMITTED

        # Admit 幂等重放成功
        admitted1_again = await store.admit(AdmitRequest(
            player_id=1001, owner_epoch=1, operation_id=rec1.operation_id, target=hub_a,
        ))
        assert admitted1_again.phase == Phase.ADMITTED

        # 3. 过期 expect_epoch 被拒绝并携带当前权威记录
        with pytest.raises(EpochConflictError) as exc_info:
            await store.begin_transition(BeginRequest(
                player_id=1001, expect_epoch=0, target_type=OwnerType.INSTANCE, target=hub_b, source_revision=11,
            ))
        assert exc_info.value.current.owner_epoch == 1

        # 4. 过期 / 0 / 同修订跨目标的 source_revision 被拒绝
        for stale_rev in (0, 9, 10):
            with pytest.raises(SourceRevisionStaleError):
                await store.begin_transition(BeginRequest(
                    player_id=1001, expect_epoch=1, target_type=OwnerType.INSTANCE,
                    target=hub_b, source_revision=stale_rev,
                ))

        # 5. 跨物理实例交接：admit_not_before = old_lease + skew_margin (30s + 2s = 32s)
        rec2 = await store.begin_transition(BeginRequest(
            player_id=1001, expect_epoch=1, target_type=OwnerType.INSTANCE, target=hub_b, source_revision=11,
        ))
        assert rec2.owner_epoch == 2 and rec2.phase == Phase.PENDING
        want_barrier = deadline + 2_000
        assert rec2.admit_not_before_ms == want_barrier

        # 屏障打开前 Admit 返回 BarrierNotOpenError(retry_after_ms=32000)
        with pytest.raises(BarrierNotOpenError) as barrier_exc:
            await store.admit(AdmitRequest(
                player_id=1001, owner_epoch=2, operation_id=rec2.operation_id, target=hub_b,
            ))
        assert barrier_exc.value.retry_after_ms == 32_000

        # 推进时钟跨过屏障 -> Admit 成功
        clock.now_ms = want_barrier
        admitted2 = await store.admit(AdmitRequest(
            player_id=1001, owner_epoch=2, operation_id=rec2.operation_id, target=hub_b,
        ))
        assert admitted2.phase == Phase.ADMITTED

        # 6. epoch 1 的迟到 Release 为安全无操作，不干扰 epoch 2
        cur, released = await store.release(ReleaseRequest(
            player_id=1001, owner_epoch=1, operation_id=rec1.operation_id,
        ))
        assert not released and cur.owner_epoch == 2 and cur.phase == Phase.ADMITTED

        # 匹配 Release 清空归属同时保留单调 epoch 与 source_revision
        after_rel, released = await store.release(ReleaseRequest(
            player_id=1001, owner_epoch=2, operation_id=rec2.operation_id,
        ))
        assert released and after_rel.phase == Phase.NONE
        assert after_rel.owner_epoch == 2 and after_rel.source_revision == 11

    asyncio.run(exercise())


def test_resolve_resume_all_states():
    # 1. 权威不可用 -> WAIT(AUTHORITY_UNAVAILABLE)
    d = resolve_resume(ResumeInput(query_err=RuntimeError("db down"), has_role=True))
    assert d.state == EntryState.WAIT and d.wait_reason == WaitReason.AUTHORITY_UNAVAILABLE
    assert d.retry_after_ms == DEFAULT_RESUME_RETRY_AFTER_MS

    # 2. 未选角色 -> ROLE_REQUIRED
    d = resolve_resume(ResumeInput(has_role=False))
    assert d.state == EntryState.ROLE_REQUIRED and d.wait_reason == WaitReason.NONE

    # 3. 未分配承载者 -> WAIT(NO_OWNER_ASSIGNED)
    d = resolve_resume(ResumeInput(has_role=True, found=False))
    assert d.state == EntryState.WAIT and d.wait_reason == WaitReason.NO_OWNER_ASSIGNED

    # 4. Pending 且屏障未到 -> WAIT(BARRIER_PENDING, 精确剩余毫秒)
    d = resolve_resume(ResumeInput(
        has_role=True, found=True, now_ms=1000,
        record=OwnerRecord(owner_epoch=2, owner_type=OwnerType.HUB, phase=Phase.PENDING, admit_not_before_ms=3500),
    ))
    assert d.state == EntryState.WAIT and d.wait_reason == WaitReason.BARRIER_PENDING
    assert d.retry_after_ms == 2500

    # 5. Pending 且屏障已过 -> WAIT(TRANSITION_IN_PROGRESS)
    d = resolve_resume(ResumeInput(
        has_role=True, found=True, now_ms=4000,
        record=OwnerRecord(owner_epoch=2, owner_type=OwnerType.HUB, phase=Phase.PENDING, admit_not_before_ms=3500),
    ))
    assert d.state == EntryState.WAIT and d.wait_reason == WaitReason.TRANSITION_IN_PROGRESS

    # 6. Admitted -> TARGET
    d = resolve_resume(ResumeInput(
        has_role=True, found=True, now_ms=4000,
        record=OwnerRecord(owner_epoch=2, owner_type=OwnerType.INSTANCE, phase=Phase.ADMITTED),
    ))
    assert d.state == EntryState.TARGET and d.record.owner_epoch == 2


def test_sql_owner_authority_store_transaction_parity():
    async def exercise():
        clock = FakeClock(1_700_000_000_000)
        pool = FakeSqlPool()
        store = SqlOwnerAuthorityStore(pool, skew_margin_ms=2_000, clock_ms=clock)
        target_a = OwnerTarget("scene-0", "uid-a", 1, "z1:s10", "scene")
        target_b = OwnerTarget("scene-1", "uid-b", 1, "z1:s11", "scene")

        deadline = await store.renew_instance_lease(RenewLeaseRequest("scene-0", "uid-a", 1, 10))
        rec1 = await store.begin_transition(BeginRequest(2001, 0, OwnerType.INSTANCE, target_a, source_revision=1))
        await store.admit(AdmitRequest(2001, rec1.owner_epoch, rec1.operation_id, target_a))

        rec2 = await store.begin_transition(BeginRequest(2001, 1, OwnerType.INSTANCE, target_b, source_revision=2))
        assert rec2.admit_not_before_ms == deadline + 2_000
        with pytest.raises(BarrierNotOpenError):
            await store.admit(AdmitRequest(2001, rec2.owner_epoch, rec2.operation_id, target_b))

        clock.now_ms = rec2.admit_not_before_ms
        admitted = await store.admit(AdmitRequest(2001, rec2.owner_epoch, rec2.operation_id, target_b))
        assert admitted.phase == Phase.ADMITTED

        rel, ok = await store.release(ReleaseRequest(2001, rec2.owner_epoch, rec2.operation_id, detail="done"))
        assert ok and rel.phase == Phase.NONE and len(pool.logs) == 5

    asyncio.run(exercise())


# ── 四、OwnerAuthority 配置校验与 Bridge（对照 ownerauthority_test / bridge_test） ──

def test_owner_authority_config_validation_and_resolve_dsn():
    # 默认关闭
    cfg = OwnerAuthorityConfig()
    assert not cfg.enabled() and cfg.effective_mode() == "" and cfg.resolve_dsn() == ""

    # 非法 backend 拒收
    with pytest.raises(ValueError):
        OwnerAuthorityConfig(dsn="mysql://user:pass@127.0.0.1:3306/owner", backend="postgres").resolve_dsn()

    # 同时配置 dsn 与 dsn_env 拒收（单一来源守卫）
    with pytest.raises(ValueError):
        OwnerAuthorityConfig(
            dsn="mysql://user:pass@127.0.0.1:3306/owner",
            dsn_env="OWNER_DSN",
            backend="mysql",
        ).resolve_dsn(lambda _: "env_dsn")

    # 非法 mode 拒收
    with pytest.raises(ValueError):
        OwnerAuthorityConfig(
            mode="bogus", dsn="mysql://user:pass@127.0.0.1:3306/owner", backend="mysql",
        ).resolve_dsn()

    # 缺失 dsn_env 环境变量拒收
    with pytest.raises(ValueError):
        OwnerAuthorityConfig(dsn_env="OWNER_DSN", backend="mysql").resolve_dsn(lambda _: None)

    # 负 skew_margin_ms 拒收
    with pytest.raises(ValueError):
        OwnerAuthorityConfig(
            dsn="mysql://user:pass@127.0.0.1:3306/owner", skew_margin_ms=-1,
        ).resolve_dsn()

    # 合法 dsn_env + enforce
    valid = OwnerAuthorityConfig(mode="enforce", dsn_env="OWNER_DSN", backend="tidb", skew_margin_ms=2000)
    assert valid.enabled() and valid.effective_mode() == "enforce"
    assert valid.resolve_dsn(lambda name: "resolved_dsn" if name == "OWNER_DSN" else None) == "resolved_dsn"

    # SceneManagerConfig.validate 同步校验非法 owner_authority 配置
    with pytest.raises(ValueError):
        SceneManagerConfig(owner_authority=OwnerAuthorityConfig(dsn="dsn", mode="bad")).validate()


def test_owner_authority_bridge_shadow_and_enforce():
    async def exercise():
        # 1. Enforce 模式：进场提交 -> 过期修订拒收 -> 活跃租约屏障拦截 -> 错目标离场无操作 -> 匹配离场释放
        clock = FakeClock(1_700_000_000_000)
        enforce_store = MemoryOwnerAuthorityStore(skew_margin_ms=2_000, clock_ms=clock)
        observed: list[tuple[str, str, str]] = []
        enforce_bridge = OwnerAuthorityBridge(
            enforce_store, mode="enforce", metrics_hook=lambda m, op, res: observed.append((m, op, res)),
        )

        await enforce_bridge.sync_enter(
            player_id=7001, scene_id=100, node_id="scene-a", zone_id=1, redis_epoch=1, owner_revision=10, enforce=True,
        )
        with pytest.raises(SourceRevisionStaleError):
            await enforce_bridge.sync_enter(
                player_id=7001, scene_id=101, node_id="scene-b", zone_id=1, redis_epoch=2, owner_revision=9, enforce=True,
            )

        await enforce_store.renew_instance_lease(RenewLeaseRequest(
            pod_name="scene-a", instance_uid="scene-a", instance_epoch=1, lease_seconds=10,
        ))
        with pytest.raises(BarrierNotOpenError):
            await enforce_bridge.sync_enter(
                player_id=7001, scene_id=101, node_id="scene-b", zone_id=1, redis_epoch=2, owner_revision=11, enforce=True,
            )

        # 不匹配目标离场安全无操作；匹配新目标离场清空归属
        await enforce_bridge.sync_leave(player_id=7001, scene_id=100, node_id="scene-a", zone_id=1)
        await enforce_bridge.sync_leave(player_id=7001, scene_id=101, node_id="scene-b", zone_id=1)
        rec, found = await enforce_store.query(7001)
        assert found and rec.phase == Phase.NONE
        assert ("enforce", "enter", "ok") in observed
        assert ("enforce", "enter", "stale_rev") in observed
        assert ("enforce", "enter", "barrier") in observed
        assert ("enforce", "leave", "target_changed") in observed
        assert ("enforce", "leave", "ok") in observed

        # 2. Shadow 模式：即使 source_revision 过期也绝不阻断
        shadow_store = MemoryOwnerAuthorityStore(skew_margin_ms=2_000, clock_ms=clock)
        shadow_bridge = OwnerAuthorityBridge(shadow_store, mode="shadow")
        await shadow_bridge.sync_enter(
            player_id=7002, scene_id=200, node_id="scene-a", zone_id=1, redis_epoch=1, owner_revision=20, enforce=False,
        )
        await shadow_bridge.sync_enter(
            player_id=7002, scene_id=201, node_id="scene-b", zone_id=1, redis_epoch=2, owner_revision=10, enforce=False,
        )

    asyncio.run(exercise())


# ── 五、SceneManagerService EnterScene / LeaveScene 集成验证 ───────────────────

class _StubDataService:
    def __init__(self) -> None:
        self.roles: dict[int, AccountRoleInfo] = {}

    async def lookup_account_role(self, player_id: int) -> AccountRoleInfo:
        return self.roles[player_id]

    async def get_player_home_zone(self, player_id: int) -> int:
        return 1


class _StubNodeRegistry:
    def __init__(self, protocol: Protocol, node_ids: tuple[int, ...]) -> None:
        self._nodes = [
            protocol.message(
                "NodeInfo",
                node_id=nid,
                node_type=3,
                zone_id=1,
                scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD,
                node_uuid=f"uuid-{nid}",
            )
            for nid in node_ids
        ]

    async def list_scene_nodes(self, zone_id: int):
        return list(self._nodes)

    async def list_gate_nodes(self, zone_id: int):
        return []

    async def resolve_scene_grpc_endpoint(self, zone_id: int, node_id: str) -> str:
        return f"127.0.0.1:{50000 + int(node_id)}"

    async def get_unique_scene_instance_uuid(self, zone_id: int, node_id: str) -> str | None:
        return f"uuid-{node_id}"

    def is_node_registered(self, zone_id: int, node_id: str) -> bool:
        return any(str(n.node_id) == node_id for n in self._nodes)

    def is_node_identity_ambiguous(self, zone_id: int, node_id: str) -> bool:
        return False

    def is_zone_gone(self, zone_id: int) -> bool:
        return False

    def is_node_draining(self, zone_id: int, node_id: str) -> bool:
        return False

    def is_synced(self) -> bool:
        return True

    def get_gate_ws_url(self, zone_id: int, gate_node_id: int) -> str:
        return ""


class _StubSceneNodes:
    async def create_scene(self, *args, **kwargs) -> None:
        return None

    async def destroy_scene(self, *args, **kwargs) -> None:
        return None

    async def release_player(self, *args, **kwargs) -> None:
        return None


class _StubPublisher:
    def __init__(self) -> None:
        self.gate_cmds: list[tuple[int, int]] = []

    async def publish_gate_command(self, gate_node_id: int, player_id: int, command) -> None:
        self.gate_cmds.append((gate_node_id, player_id))

    async def publish_scene_command(self, scene_node_id: int, player_id: int, command) -> None:
        return None


def test_scene_manager_enter_leave_owner_authority_integration():
    async def exercise():
        protocol = Protocol()
        clock = FakeClock(1_750_000_000_000)
        control = MemorySceneControlStore()
        player = MemoryPlayerPlacementStore()
        data = _StubDataService()
        data.roles[8001] = AccountRoleInfo(player_id=8001, account="acc-8001", owner_revision=10)
        registry = _StubNodeRegistry(protocol, (101, 102))
        publisher = _StubPublisher()
        oa_store = MemoryOwnerAuthorityStore(skew_margin_ms=2_000, clock_ms=clock)

        cfg = SceneManagerConfig(
            zone_id=1,
            world_conf_ids=(1,),
            owner_authority=OwnerAuthorityConfig(mode="enforce", dsn="mem://oa"),
        )
        svc = SceneManagerService(
            protocol,
            cfg,
            control_store=control,
            player_store=player,
            data_service=data,
            scene_nodes=_StubSceneNodes(),
            node_registry=registry,
            publisher=publisher,
            clock_ms=clock,
            owner_authority=oa_store,
        )
        await svc.sync_nodes_to_control_store(1)
        await control.register_world_channel(zone_id=1, scene_id=501, node_id="101", conf_id=1, desired_channels=2)
        await control.register_world_channel(zone_id=1, scene_id=502, node_id="102", conf_id=1, desired_channels=2)

        # 1. 首次进场 501 (node 101)：OwnerAuthority 强门禁放行并提交 ADMITTED
        req1 = protocol.message(
            "scene_manager.EnterSceneRequest",
            player_id=8001,
            scene_id=501,
            session_id=1,
            login_generation=1,
            gate_id="7",
            gate_instance_id="gate-1",
            gate_zone_id=1,
            zone_id=1,
            owner_revision=10,
        )
        resp1 = await svc.enter_scene(req1)
        assert resp1.error_code == ERR_OK
        assert len(publisher.gate_cmds) == 1
        rec1, found1 = await oa_store.query(8001)
        assert found1 and rec1.phase == Phase.ADMITTED and rec1.target.pod_name == "101"

        # 2. 为 node 101 续约租约并写好 Redis handoff 标记，再跨节点切入 502 (node 102)
        #    OwnerAuthority 屏障未开 -> 在 Kafka 推路由前拦截并单调回滚 Redis 落点
        await oa_store.renew_instance_lease(RenewLeaseRequest("101", "101", 1, 10))
        epoch1 = await player.get_owner_epoch(8001)
        player.set_handoff_marker(8001, f"{epoch1}:{clock.now_ms // 1000}")
        data.roles[8001] = AccountRoleInfo(player_id=8001, account="acc-8001", owner_revision=11)

        req2 = protocol.message(
            "scene_manager.EnterSceneRequest",
            player_id=8001,
            scene_id=502,
            session_id=1,
            login_generation=1,
            gate_id="7",
            gate_instance_id="gate-1",
            gate_zone_id=1,
            zone_id=1,
            owner_revision=11,
        )
        resp2 = await svc.enter_scene(req2)
        assert resp2.error_code == ERR_OWNER_EPOCH_CONFLICT
        assert resp2.owner_epoch_after_rollback > 0
        # 确认未发第二次 Kafka 路由命令
        assert len(publisher.gate_cmds) == 1

        # 3. 推进时钟过屏障后再次切入 502 成功，随后 LeaveScene 释放 OwnerAuthority
        clock.now_ms += 15_000
        epoch_after_rb = await player.get_owner_epoch(8001)
        player.set_handoff_marker(8001, f"{epoch_after_rb}:{clock.now_ms // 1000}")
        data.roles[8001] = AccountRoleInfo(player_id=8001, account="acc-8001", owner_revision=12)
        req3 = protocol.message(
            "scene_manager.EnterSceneRequest",
            player_id=8001,
            scene_id=502,
            session_id=1,
            login_generation=1,
            gate_id="7",
            gate_instance_id="gate-1",
            gate_zone_id=1,
            zone_id=1,
            owner_revision=12,
        )
        resp3 = await svc.enter_scene(req3)
        assert resp3.error_code == ERR_OK
        assert len(publisher.gate_cmds) == 2

        epoch3 = await player.get_owner_epoch(8001)
        player.set_handoff_marker(8001, f"{epoch3}:{clock.now_ms // 1000}")
        leave_req = protocol.message(
            "scene_manager.LeaveSceneRequest",
            player_id=8001,
            scene_id=502,
            session_id=1,
            login_generation=1,
        )
        await svc.leave_scene(leave_req)
        rec_final, found_final = await oa_store.query(8001)
        assert found_final and rec_final.phase == Phase.NONE
        await svc.close()

    asyncio.run(exercise())
