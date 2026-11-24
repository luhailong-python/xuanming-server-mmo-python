"""Python ``db``（``playerstore.PlayerStore`` + ``db.Test``）端到端与单元测试。

使用真实的 ``DataServicePlayerStore`` 客户端直连真实的 ``serve_grpc(..., pass_context=True)`` 服务端，
覆盖认领、存盘、围栏拒绝、空行释放、审计发件箱、HMAC 鉴权、``/db/Test``、``GetPlayerRoute``、
``SqlPlayerStore`` SQL 构造与事务行为，以及可选真实 MySQL 集成测试（``XUANMING_TEST_MYSQL``）。
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace

import grpc
import pytest

from xuanming.data.player_rows import PlayerRowCodec, PlayerRowKind
from xuanming.data.player_store import DataServicePlayerStore, StoreRejected
from xuanming.db.config import DbConfig
from xuanming.db.model import (
    AUDIT_KIND_PLAYER_SNAPSHOT,
    AUDIT_KIND_TRANSACTION_LOG,
    AuditRecord,
    ClaimResult,
    OwnerRow,
    SaveLimits,
    SaveResult,
    StorageUnavailableError,
    StoreBackendError,
    decide_claim,
    decide_save,
    storage_db_name,
    validate_claim_request,
    validate_save_request,
)
from xuanming.db.service import DataServiceRouteResolver, DbService
from xuanming.db.sql_store import (
    SQL_CLAIM_OWNER,
    SQL_SELECT_OWNER,
    SQL_SELECT_ROWS,
    SqlPlayerStore,
    build_audit_outbox_sql,
    build_save_sql,
    open_storage_pool,
)
from xuanming.db.store import MemoryPlayerStore
from xuanming.grpc_runtime import GrpcClient, serve_grpc, sign_service_call, verify_service_call
from xuanming.protocol import Protocol

SCENE_SECRET = b"scene-player-store-secret-0123456789"
ROUTE_SECRET = b"db-role-fence-route-secret-0123456789"
STORAGE_ID = 1_000_000
INSTANCE_A = b"scene-instance-a"
INSTANCE_B = b"scene-instance-b"
PLAYER_STORE_PATH = "/playerstore.PlayerStore/"
MYSQL = os.environ.get("XUANMING_TEST_MYSQL")


class FakeRouteResolver:
    """可记录调用次数的内存落点解析器。"""

    def __init__(self, mapping: dict[int, int] | None = None, *, error: Exception | None = None) -> None:
        self.mapping = dict(mapping or {})
        self.error = error
        self.calls: list[int] = []

    async def resolve_storage(self, player_id: int) -> int:
        self.calls.append(player_id)
        if self.error is not None:
            raise self.error
        storage_id = self.mapping.get(player_id)
        if storage_id is None:
            raise StorageUnavailableError(f"player {player_id} route missing")
        return storage_id


@asynccontextmanager
async def running_db(
    *,
    known_gen: int = 1,
    routes: dict[int, int] | None = None,
    available_storages: list[int] | None = None,
    clock_ms=None,
    store=None,
    resolver=None,
):
    protocol = Protocol()
    clock = clock_ms or (lambda: time.time_ns() // 1_000_000)
    backend = store or MemoryPlayerStore(
        protocol,
        known_gen=known_gen,
        available_storages=available_storages,
        clock_ms=clock,
    )
    route_resolver = resolver or FakeRouteResolver(routes or {1001: STORAGE_ID})
    service = DbService(protocol, backend, route_resolver, SCENE_SECRET, clock_ms=clock)
    async with AsyncExitStack() as stack:
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        stack.push_async_callback(server.stop, 0)
        client = GrpcClient(f"127.0.0.1:{port}", timeout=5, receive_limit=8 << 20)
        stack.push_async_callback(client.close)
        player_store_client = DataServicePlayerStore(
            protocol, client.call, SCENE_SECRET, retry_budget=2.0
        )

        async def signed_call(method: str, request, *, caller: str = "scene", secret: bytes = SCENE_SECRET, ts_ms: int | None = None):
            path = f"{PLAYER_STORE_PATH}{method}"
            sign_service_call(path, request, caller, clock() if ts_ms is None else ts_ms, secret)
            raw = await client.call(path, request.SerializeToString(deterministic=True))
            return protocol.parse(f"playerstore.{method}Response", raw)

        yield SimpleNamespace(
            protocol=protocol,
            store=backend,
            resolver=route_resolver,
            client=client,
            ps=player_store_client,
            signed_call=signed_call,
        )


# ---------------------------------------------------------------------------
# 1. 纯围栏判定表（逐格对齐 Go decide_test.go）与入事务前校验
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("row", "epoch", "seq", "gen", "has_rows", "instance", "want"),
    [
        (None, 10, 1, 1, True, INSTANCE_A, SaveResult.NO_OWNER),
        (OwnerRow(10, 5, 1, INSTANCE_A), 9, 6, 1, True, INSTANCE_A, SaveResult.STALE_OWNER),
        (OwnerRow(10, 5, 1, INSTANCE_B), 9, 6, 1, True, INSTANCE_A, SaveResult.STALE_OWNER),
        (OwnerRow(10, 5, 1, INSTANCE_B), 10, 6, 1, True, INSTANCE_A, SaveResult.NOT_HOLDER),
        (OwnerRow(10, 5, 1, INSTANCE_B), 11, 6, 1, True, INSTANCE_A, SaveResult.UNCLAIMED),
        (OwnerRow(1, 0, 1, b""), 1, 1, 1, True, INSTANCE_A, SaveResult.NOT_HOLDER),
        (OwnerRow(10, 5, 1, INSTANCE_A), 11, 6, 1, True, INSTANCE_A, SaveResult.COMMITTED),
        (OwnerRow(10, 5, 1, INSTANCE_B), 10, 5, 1, True, INSTANCE_A, SaveResult.NOT_HOLDER),
        (OwnerRow(10, 5, 1, INSTANCE_A), 10, 5, 1, True, INSTANCE_A, SaveResult.EQUAL),
        (OwnerRow(10, 5, 1, INSTANCE_A), 11, 5, 1, True, INSTANCE_A, SaveResult.EQUAL),
        (OwnerRow(10, 5, 1, INSTANCE_A), 10, 4, 1, True, INSTANCE_A, SaveResult.OLDER_SEQ),
        (OwnerRow(10, 5, 3, INSTANCE_A), 10, 6, 2, True, INSTANCE_A, SaveResult.SCHEMA_REGRESSED),
        (OwnerRow(10, 5, 3, INSTANCE_A), 10, 6, 2, False, INSTANCE_A, SaveResult.COMMITTED),
        (OwnerRow(10, 5, 1, INSTANCE_A), 10, 6, 6, True, INSTANCE_A, SaveResult.SCHEMA_TOO_NEW),
        (OwnerRow(10, 5, 1, INSTANCE_A), 10, 6, 6, False, INSTANCE_A, SaveResult.SCHEMA_TOO_NEW),
        (OwnerRow(10, 5, 1, INSTANCE_A), 10, 6, 5, True, INSTANCE_A, SaveResult.COMMITTED),
        (OwnerRow(10, 5, 1, INSTANCE_A), 10, 6, 1, True, INSTANCE_A, SaveResult.COMMITTED),
    ],
)
def test_decide_save_matrix(row, epoch, seq, gen, has_rows, instance, want):
    assert decide_save(row, epoch, seq, gen, has_rows, instance, known_gen=5) == want


@pytest.mark.parametrize(
    ("row", "epoch", "instance", "binary_gen", "want"),
    [
        (None, 10, INSTANCE_A, 5, ClaimResult.NO_OWNER),
        (OwnerRow(10, 0, 1, INSTANCE_A), 9, INSTANCE_A, 5, ClaimResult.STALE_OWNER),
        (OwnerRow(10, 0, 1, INSTANCE_B), 10, INSTANCE_A, 5, ClaimResult.NOT_HOLDER),
        (OwnerRow(10, 0, 1, INSTANCE_A), 10, INSTANCE_A, 5, ClaimResult.CLAIMED),
        (OwnerRow(1, 0, 1, b""), 1, INSTANCE_A, 5, ClaimResult.CLAIMED),
        (OwnerRow(10, 0, 1, INSTANCE_B), 11, INSTANCE_A, 5, ClaimResult.CLAIMED),
        (OwnerRow(10, 0, 5, INSTANCE_B), 11, INSTANCE_A, 4, ClaimResult.CLAIMED),
        (OwnerRow(10, 0, 5, INSTANCE_A), 10, INSTANCE_A, 4, ClaimResult.CLAIMED),
        (OwnerRow(10, 0, 6, INSTANCE_B), 11, INSTANCE_A, 5, ClaimResult.SCHEMA_TOO_NEW),
        (OwnerRow(10, 0, 6, INSTANCE_A), 10, INSTANCE_A, 5, ClaimResult.SCHEMA_TOO_NEW),
        (OwnerRow(10, 0, 1, INSTANCE_B), 11, INSTANCE_A, 6, ClaimResult.SCHEMA_TOO_NEW),
        (OwnerRow(1, 0, 1, b""), 1, INSTANCE_A, 6, ClaimResult.SCHEMA_TOO_NEW),
    ],
)
def test_decide_claim_matrix(row, epoch, instance, binary_gen, want):
    assert decide_claim(row, epoch, instance, binary_gen, known_gen=5) == want


def test_validate_requests_and_limits():
    limits = SaveLimits(max_row_bytes=16, max_request_bytes=24)
    limits.validate()

    assert validate_claim_request(0, 1, INSTANCE_A) == ClaimResult.INVALID
    assert validate_claim_request(1, 0, INSTANCE_A) == ClaimResult.INVALID
    assert validate_claim_request(1, 1, b"") == ClaimResult.INVALID
    assert validate_claim_request(1, 1, b"x" * 65) == ClaimResult.INVALID
    assert validate_claim_request(1, 1, INSTANCE_A) == ClaimResult.UNSPECIFIED

    assert validate_save_request(0, 1, 1, INSTANCE_A, [], limits) == SaveResult.INVALID
    assert validate_save_request(1, 1, 1, INSTANCE_A, [(0, b"")], limits) == SaveResult.INVALID
    assert validate_save_request(1, 1, 1, INSTANCE_A, [(1, b"")], limits) == SaveResult.INVALID  # 退役号
    assert validate_save_request(1, 1, 1, INSTANCE_A, [(2, b"")], limits) == SaveResult.INVALID  # 退役号
    assert validate_save_request(1, 1, 1, INSTANCE_A, [(5, b"")], limits) == SaveResult.INVALID  # 退役号
    assert validate_save_request(1, 1, 1, INSTANCE_A, [(10, b"")], limits) == SaveResult.SCHEMA_TOO_NEW
    assert validate_save_request(1, 1, 1, INSTANCE_A, [(4, b"a"), (4, b"b")], limits) == SaveResult.INVALID
    assert validate_save_request(1, 1, 1, INSTANCE_A, [(4, b"x" * 17)], limits) == SaveResult.TOO_LARGE
    assert validate_save_request(1, 1, 1, INSTANCE_A, [(4, b"x" * 16), (6, b"y" * 9)], limits) == SaveResult.TOO_LARGE
    assert validate_save_request(1, 1, 1, INSTANCE_A, [(4, b"ok")], limits) == SaveResult.UNSPECIFIED

    with pytest.raises(ValueError):
        SaveLimits(max_row_bytes=0, max_request_bytes=10).validate()
    with pytest.raises(ValueError):
        SaveLimits(max_row_bytes=2 << 20, max_request_bytes=3 << 20).validate()
    with pytest.raises(ValueError):
        SaveLimits(max_row_bytes=1 << 20, max_request_bytes=9 << 20).validate()
    with pytest.raises(ValueError):
        storage_db_name(999_999)
    assert storage_db_name(1_000_000) == "player_store_1000000_db"


# ---------------------------------------------------------------------------
# 2. 真实 DataServicePlayerStore 客户端 + 真实 serve_grpc 服务端端到端测试
# ---------------------------------------------------------------------------

def test_load_claim_save_and_takeover_roundtrip():
    async def scenario():
        async with running_db() as env:
            env.store.seed_player(
                STORAGE_ID,
                1001,
                owner_epoch=1,
                save_seq=0,
                schema_gen=1,
                holder=b"",
                released=1,
                claim_seq=0,
                rows={PlayerRowKind.LOCATION: b"loc0", PlayerRowKind.PROFILE: b"prof0"},
            )

            # 首次认领：返回按 kind 升序排列的数据行并递增 claim_seq
            snap = await env.ps.load_and_claim(1001, INSTANCE_A, owner_epoch=1)
            assert snap.storage_id == STORAGE_ID
            assert snap.owner_epoch == 1
            assert snap.save_seq == 0
            assert snap.schema_gen == 1
            assert list(snap.rows.items()) == [(4, b"prof0"), (9, b"loc0")]
            owner_rec = env.store.get_owner(STORAGE_ID, 1001)
            assert owner_rec is not None
            assert owner_rec.claim_seq == 1
            assert owner_rec.released == 0
            assert owner_rec.holder == INSTANCE_A

            # 同实例同代次再次认领：放行且 claim_seq 继续递增（供 dead-holder 比较）
            snap_re = await env.ps.load_and_claim(1001, INSTANCE_A, owner_epoch=1)
            assert snap_re.owner_epoch == 1
            assert env.store.get_owner(STORAGE_ID, 1001).claim_seq == 2

            # 正常存盘：只提交脏行，不改动 claim_seq
            updated = await env.ps.commit(
                snap_re,
                {
                    PlayerRowKind.PROFILE: b"prof0",
                    PlayerRowKind.LOCATION: b"loc1",
                    PlayerRowKind.WALLET: b"wallet1",
                },
                force=True,
            )
            assert updated.save_seq > 0
            assert env.store.get_owner(STORAGE_ID, 1001).claim_seq == 2
            assert env.store.get_rows(STORAGE_ID, 1001) == {
                PlayerRowKind.PROFILE: b"prof0",
                PlayerRowKind.LOCATION: b"loc1",
                PlayerRowKind.WALLET: b"wallet1",
            }

            # 同代次另一实例认领被拒（NOT_HOLDER）
            client_b = DataServicePlayerStore(env.protocol, env.client.call, SCENE_SECRET, retry_budget=1.0)
            with pytest.raises(StoreRejected) as exc_info:
                await client_b.load_and_claim(1001, INSTANCE_B, owner_epoch=1)
            assert exc_info.value.result == "LOAD_AND_CLAIM_NOT_HOLDER"

            # 抬代次接管成功并围住旧实例
            snap_b = await client_b.load_and_claim(1001, INSTANCE_B, owner_epoch=2)
            assert snap_b.owner_epoch == 2
            assert snap_b.save_seq == updated.save_seq
            assert snap_b.rows[PlayerRowKind.WALLET] == b"wallet1"
            assert env.store.get_owner(STORAGE_ID, 1001).claim_seq == 3

            # 旧实例继续存盘被 STALE_OWNER 围栏拒绝
            with pytest.raises(StoreRejected) as stale_exc:
                await env.ps.commit(updated, {**updated.rows, PlayerRowKind.WALLET: b"stale"}, force=True)
            assert stale_exc.value.result == "SAVE_PLAYER_STALE_OWNER"

    asyncio.run(scenario())


def test_fencing_rejections_idempotent_replay_and_release_detached():
    async def scenario():
        async with running_db(known_gen=3) as env:
            env.store.seed_player(
                STORAGE_ID,
                1001,
                owner_epoch=5,
                save_seq=10,
                schema_gen=2,
                holder=INSTANCE_A,
                released=0,
                claim_seq=4,
                rows={PlayerRowKind.PROFILE: b"p"},
            )

            # 1) 幂等重放同 save_seq 返回 SAVE_PLAYER_EQUAL 且 audit_outbox_written=True
            replay = env.protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=1001,
                storage_id=STORAGE_ID,
                owner_epoch=5,
                save_seq=10,
                schema_gen=2,
                instance_id=INSTANCE_A,
            )
            resp = await env.signed_call("SavePlayer", replay)
            assert resp.result == SaveResult.EQUAL
            assert resp.audit_outbox_written is True
            assert resp.row_owner_epoch == 5
            assert resp.row_save_seq == 10

            # 2) 其他实例用更高代次未认领直接写 -> UNCLAIMED，audit_outbox_written=False
            unclaimed = env.protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=1001,
                storage_id=STORAGE_ID,
                owner_epoch=6,
                save_seq=11,
                schema_gen=2,
                instance_id=INSTANCE_B,
            )
            resp_unclaimed = await env.signed_call("SavePlayer", unclaimed)
            assert resp_unclaimed.result == SaveResult.UNCLAIMED
            assert resp_unclaimed.audit_outbox_written is False

            # 3) 更旧 save_seq -> OLDER_SEQ
            older = env.protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=1001,
                storage_id=STORAGE_ID,
                owner_epoch=5,
                save_seq=9,
                schema_gen=2,
                instance_id=INSTANCE_A,
            )
            assert (await env.signed_call("SavePlayer", older)).result == SaveResult.OLDER_SEQ

            # 4) 带行且 schema_gen 倒退 -> SCHEMA_REGRESSED
            regressed = env.protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=1001,
                storage_id=STORAGE_ID,
                owner_epoch=5,
                save_seq=11,
                schema_gen=1,
                instance_id=INSTANCE_A,
            )
            regressed.rows.add(kind=PlayerRowKind.PROFILE, payload=b"old")
            assert (await env.signed_call("SavePlayer", regressed)).result == SaveResult.SCHEMA_REGRESSED

            # 5) storage_id=0 但非 release 或带行 -> SAVE_PLAYER_INVALID，绝不查路由
            calls_before = len(env.resolver.calls)
            bad_zero_1 = env.protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=1001,
                storage_id=0,
                owner_epoch=5,
                save_seq=11,
                schema_gen=2,
                instance_id=INSTANCE_A,
                release=False,
            )
            bad_zero_2 = env.protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=1001,
                storage_id=0,
                owner_epoch=5,
                save_seq=11,
                schema_gen=2,
                instance_id=INSTANCE_A,
                release=True,
            )
            bad_zero_2.rows.add(kind=PlayerRowKind.PROFILE, payload=b"x")
            assert (await env.signed_call("SavePlayer", bad_zero_1)).result == SaveResult.INVALID
            assert (await env.signed_call("SavePlayer", bad_zero_2)).result == SaveResult.INVALID
            assert len(env.resolver.calls) == calls_before

            # 6) 通过真实 DataServicePlayerStore.release_detached 空行释放（schema_gen=1 < 库内 2）
            #    必须通过 RouteResolver 查落点、成功释放、且 GREATEST 保留 schema_gen=2！
            settled = await env.ps.release_detached(1001, INSTANCE_A, owner_epoch=5)
            assert settled is True
            assert env.resolver.calls[-1] == 1001
            owner = env.store.get_owner(STORAGE_ID, 1001)
            assert owner.released == 1
            assert owner.schema_gen == 2

    asyncio.run(scenario())


def test_audit_outbox_persisted_on_save():
    async def scenario():
        fixed_ms = 1_700_000_000_123
        async with running_db(clock_ms=lambda: fixed_ms) as env:
            codec = PlayerRowCodec(env.protocol)
            db_msg = env.protocol.message("player_database", player_id=1001)
            db_msg.profile_component.name = "Hero"
            db_msg.currency.values.extend([0, 999])
            projected = codec.project(db_msg)

            env.store.seed_player(
                STORAGE_ID,
                1001,
                owner_epoch=1,
                save_seq=0,
                schema_gen=1,
                holder=INSTANCE_A,
                released=0,
                rows={PlayerRowKind.PROFILE: projected[PlayerRowKind.PROFILE]},
            )

            req = env.protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=1001,
                storage_id=STORAGE_ID,
                owner_epoch=1,
                save_seq=42,
                schema_gen=1,
                instance_id=INSTANCE_A,
            )
            req.rows.add(kind=PlayerRowKind.WALLET, payload=projected[PlayerRowKind.WALLET])
            # 一条合法流水 + 一条 tx_id=0 的坏流水（后者跳过不阻断存盘）
            req.transaction_logs.add(tx_id=5001, from_player=1001, currency_delta=100, zone_id=1)
            req.transaction_logs.add(tx_id=0, from_player=1001, currency_delta=1)
            # 一条合法快照请求 + 一条 snapshot_id=0 的坏请求
            req.audit_snapshots.add(snapshot_id=7001, trigger=1, schema_version="v1", zone_id=1)
            req.audit_snapshots.add(snapshot_id=0, trigger=1, schema_version="v1", zone_id=1)

            resp = await env.signed_call("SavePlayer", req)
            assert resp.result == SaveResult.COMMITTED
            assert resp.audit_outbox_written is True

            outbox = env.store.get_audit_rows(STORAGE_ID, 1001)
            assert len(outbox) == 2
            tx_row, snap_row = outbox
            assert tx_row.shard == 1001 % 16
            assert tx_row.kind == AUDIT_KIND_TRANSACTION_LOG
            assert tx_row.record_id == 5001
            assert tx_row.created_ms == fixed_ms
            assert tx_row.save_seq == 42
            parsed_tx = env.protocol.parse("TransactionLogEntry", tx_row.payload)
            assert parsed_tx.tx_id == 5001
            assert parsed_tx.currency_delta == 100

            assert snap_row.kind == AUDIT_KIND_PLAYER_SNAPSHOT
            assert snap_row.record_id == 7001
            parsed_snap = env.protocol.parse("PlayerSnapshotEntry", snap_row.payload)
            assert parsed_snap.player_id == 1001
            assert parsed_snap.snapshot_time == fixed_ms // 1000
            assert parsed_snap.total_bytes > 0
            parsed_db = env.protocol.parse("player_database", parsed_snap.player_database_blob)
            assert parsed_db.player_id == 1001
            assert parsed_db.persist_schema_gen == 1
            assert parsed_db.profile_component.name == "Hero"
            assert list(parsed_db.currency.values) == [0, 999]

    asyncio.run(scenario())


def test_auth_failure_permission_denied_and_db_test_no_auth():
    async def scenario():
        async with running_db() as env:
            env.store.seed_player(STORAGE_ID, 1001, owner_epoch=1, holder=INSTANCE_A)

            # /db/Test 无需鉴权，直接返回空 TestResponse
            test_req = env.protocol.message("TestRequest")
            raw_test = await env.client.call("/db/Test", test_req.SerializeToString())
            test_resp = env.protocol.parse("TestResponse", raw_test)
            assert test_resp.SerializeToString() == b""

            # 未签名 LoadAndClaim -> PERMISSION_DENIED，不触碰路由与存储
            claim_req = env.protocol.message(
                "playerstore.LoadAndClaimRequest",
                player_id=1001,
                owner_epoch=1,
                instance_id=INSTANCE_A,
                binary_schema_gen=1,
            )
            with pytest.raises(grpc.aio.AioRpcError) as unsigned_err:
                await env.client.call(
                    f"{PLAYER_STORE_PATH}LoadAndClaim", claim_req.SerializeToString(deterministic=True)
                )
            assert unsigned_err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 错误 caller（非 "scene"）-> PERMISSION_DENIED
            with pytest.raises(grpc.aio.AioRpcError) as wrong_caller_err:
                await env.signed_call("LoadAndClaim", claim_req, caller="login")
            assert wrong_caller_err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 错误密钥 -> PERMISSION_DENIED，且 DataServicePlayerStore 立即快速失败
            bad_ps = DataServicePlayerStore(
                env.protocol, env.client.call, b"wrong-secret-0123456789abcdef0123", retry_budget=5.0
            )
            with pytest.raises(StoreRejected) as ps_err:
                await bad_ps.load_and_claim(1001, INSTANCE_A, owner_epoch=1)
            assert ps_err.value.result == "PERMISSION_DENIED"

            # 跨方法复用签名 -> PERMISSION_DENIED
            save_req = env.protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=1001,
                storage_id=STORAGE_ID,
                owner_epoch=1,
                save_seq=1,
                schema_gen=1,
                instance_id=INSTANCE_A,
            )
            sign_service_call(
                f"{PLAYER_STORE_PATH}LoadAndClaim", save_req, "scene", time.time_ns() // 1_000_000, SCENE_SECRET
            )
            with pytest.raises(grpc.aio.AioRpcError) as cross_method_err:
                await env.client.call(
                    f"{PLAYER_STORE_PATH}SavePlayer", save_req.SerializeToString(deterministic=True)
                )
            assert cross_method_err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 过期时间戳（> 5 分钟）-> PERMISSION_DENIED
            with pytest.raises(grpc.aio.AioRpcError) as expired_err:
                await env.signed_call(
                    "SavePlayer", save_req, ts_ms=(time.time_ns() // 1_000_000) - 400_000
                )
            assert expired_err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 上述全部被拒，从未触碰 RouteResolver
            assert env.resolver.calls == []

    asyncio.run(scenario())


def test_backend_error_maps_to_sanitized_unavailable():
    class ExplodingStore:
        async def load_and_claim(self, *args, **kwargs):
            raise StoreBackendError("Error 1146: secret internal mysql detail")

        async def save_player(self, *args, **kwargs):
            raise StoreBackendError("Error 1205: secret lock detail")

    async def scenario():
        async with running_db(store=ExplodingStore()) as env:
            claim_req = env.protocol.message(
                "playerstore.LoadAndClaimRequest",
                player_id=1001,
                owner_epoch=1,
                instance_id=INSTANCE_A,
                binary_schema_gen=1,
            )
            with pytest.raises(grpc.aio.AioRpcError) as claim_err:
                await env.signed_call("LoadAndClaim", claim_req)
            assert claim_err.value.code() == grpc.StatusCode.UNAVAILABLE
            assert "secret" not in (claim_err.value.details() or "")

            save_req = env.protocol.message(
                "playerstore.SavePlayerRequest",
                player_id=1001,
                storage_id=STORAGE_ID,
                owner_epoch=1,
                save_seq=1,
                schema_gen=1,
                instance_id=INSTANCE_A,
            )
            with pytest.raises(grpc.aio.AioRpcError) as save_err:
                await env.signed_call("SavePlayer", save_req)
            assert save_err.value.code() == grpc.StatusCode.UNAVAILABLE
            assert "secret" not in (save_err.value.details() or "")

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# 3. DataServiceRouteResolver（真实 gRPC 签名验签 + 错误边界）
# ---------------------------------------------------------------------------

def test_data_service_route_resolver_signs_db_caller():
    async def scenario():
        protocol = Protocol()
        replies: dict[int, dict] = {
            1001: {"found": True, "storage_id": STORAGE_ID, "home_zone": 1},
            1002: {"found": False, "storage_id": 0},
            1003: {"found": True, "storage_id": STORAGE_ID, "retry": True},
            1004: {"found": True, "storage_id": 0},
        }
        verified_callers: list[str] = []

        async def fake_get_player_route(request, context, path):
            assert verify_service_call(path, request, "db", time.time_ns() // 1_000_000, ROUTE_SECRET)
            verified_callers.append(request.auth.caller)
            return protocol.message("data_service.GetPlayerRouteResponse", **replies[request.player_id])

        async with AsyncExitStack() as stack:
            server, port = await serve_grpc(
                protocol,
                "127.0.0.1:0",
                {"data_service.DataService": {"GetPlayerRoute": fake_get_player_route}},
                pass_context=True,
            )
            stack.push_async_callback(server.stop, 0)
            client = GrpcClient(f"127.0.0.1:{port}", timeout=2.0)
            stack.push_async_callback(client.close)

            resolver = DataServiceRouteResolver(protocol, client.call, ROUTE_SECRET, timeout_sec=2.0)
            assert await resolver.resolve_storage(1001) == STORAGE_ID
            assert verified_callers == ["db"]

            for bad_pid in (1002, 1003, 1004):
                with pytest.raises(StorageUnavailableError):
                    await resolver.resolve_storage(bad_pid)

            with pytest.raises(ValueError):
                DataServiceRouteResolver(protocol, client.call, b"short")

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# 4. SqlPlayerStore SQL 构造、UNHEX 绑定、伪连接池事务与配置校验
# ---------------------------------------------------------------------------

def test_sql_builders_use_unhex_greatest_and_batching():
    query, args = build_save_sql(
        player_id=1001,
        owner_epoch=3,
        save_seq=99,
        schema_gen=2,
        instance_id=b"\x00\xffab",
        rows=[(PlayerRowKind.PROFILE, b"\x01\x02"), (PlayerRowKind.WALLET, b"")],
        release=True,
    )
    assert "GREATEST(schema_gen, VALUES(schema_gen))" in query
    assert "claim_seq" not in query
    assert query.count("UNHEX(%s)") == 6
    assert args == [
        1001, 0, 3, 99, 2, "00ff6162", 1, "",
        1001, 4, 3, 99, 2, "", 0, "0102",
        1001, 7, 3, 99, 2, "", 0, "",
    ]

    records = [AuditRecord(AUDIT_KIND_TRANSACTION_LOG, i + 1, bytes([i % 256])) for i in range(1001)]
    stmts = build_audit_outbox_sql(player_id=1001, save_seq=99, created_ms=123456, records=records)
    assert len(stmts) == 2
    q1, a1 = stmts[0]
    q2, a2 = stmts[1]
    assert "ON DUPLICATE KEY UPDATE record_id = record_id" in q1
    assert q1.count("UNHEX(%s)") == 1000
    assert len(a1) == 7000
    assert q2.count("UNHEX(%s)") == 1
    assert a2 == [1001 % 16, AUDIT_KIND_TRANSACTION_LOG, 123456, 1001, 1001, 99, "e8"]


def test_sql_player_store_transactions_with_fake_pool():
    class FakeCursor:
        def __init__(self, conn) -> None:
            self._conn = conn
            self._result = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def execute(self, sql, args=()):
            self._conn.executed.append((sql, tuple(args)))
            self._result = self._conn.handler(sql, tuple(args))

        async def fetchone(self):
            return self._result[0] if self._result else None

        async def fetchall(self):
            return list(self._result)

    class FakeConn:
        def __init__(self, handler) -> None:
            self.handler = handler
            self.executed: list[tuple[str, tuple]] = []
            self.began = 0
            self.committed = 0
            self.rolled_back = 0
            self.closed = False

        async def begin(self):
            self.began += 1

        async def commit(self):
            self.committed += 1

        async def rollback(self):
            self.rolled_back += 1

        def close(self):
            self.closed = True

        def cursor(self):
            return FakeCursor(self)

    class FakeAcquire:
        def __init__(self, conn) -> None:
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, *exc):
            return False

    class FakePool:
        def __init__(self, conn) -> None:
            self.conn = conn

        def acquire(self):
            return FakeAcquire(self.conn)

    async def scenario():
        protocol = Protocol()

        # 1) LoadAndClaim 成功 + bytes 强转 + 排序 + claim_seq+1
        def claim_handler(sql, args):
            if sql == SQL_SELECT_OWNER:
                return [(2, 10, 1, bytearray(INSTANCE_A))]
            if sql == SQL_SELECT_ROWS:
                return [(PlayerRowKind.LOCATION, bytearray(b"loc")), (PlayerRowKind.PROFILE, None)]
            return []

        conn = FakeConn(claim_handler)
        store = SqlPlayerStore({STORAGE_ID: FakePool(conn)}, protocol)
        outcome = await store.load_and_claim(STORAGE_ID, 1001, 2, INSTANCE_A, 1)
        assert outcome.result == ClaimResult.CLAIMED
        assert outcome.rows == ((4, b""), (9, b"loc"))
        assert conn.committed == 1 and conn.rolled_back == 0 and not conn.closed
        assert any(sql == SQL_CLAIM_OWNER and args == (2, INSTANCE_A.hex(), 1001) for sql, args in conn.executed)

        # 2) 业务拒绝（STALE_OWNER）走 rollback，连接保持健康不关闭
        conn_reject = FakeConn(lambda sql, args: [(5, 10, 1, INSTANCE_A)] if sql == SQL_SELECT_OWNER else [])
        store_reject = SqlPlayerStore({STORAGE_ID: FakePool(conn_reject)}, protocol)
        rej = await store_reject.save_player(STORAGE_ID, 1001, 4, 11, 1, INSTANCE_A, [], False)
        assert rej.result == SaveResult.STALE_OWNER
        assert conn_reject.rolled_back == 1 and conn_reject.committed == 0 and not conn_reject.closed

        # 3) SQL 异常关闭连接并转为 StoreBackendError
        def boom(sql, args):
            raise RuntimeError("Error 1205: lock wait timeout")

        conn_err = FakeConn(boom)
        store_err = SqlPlayerStore({STORAGE_ID: FakePool(conn_err)}, protocol)
        with pytest.raises(StoreBackendError):
            await store_err.save_player(STORAGE_ID, 1001, 5, 11, 1, INSTANCE_A, [], False)
        assert conn_err.closed is True

        # 4) SavePlayer 携带审计流水与快照时在同一事务内写入 player_audit_outbox
        def save_audit_handler(sql, args):
            if sql == SQL_SELECT_OWNER:
                return [(2, 10, 1, INSTANCE_A)]
            if sql == SQL_SELECT_ROWS:
                return [(PlayerRowKind.PROFILE, b"")]
            return []

        conn_audit = FakeConn(save_audit_handler)
        store_audit = SqlPlayerStore(
            {STORAGE_ID: FakePool(conn_audit)}, protocol, clock_ms=lambda: 1_700_000_000_500
        )
        tx_entry = protocol.message("TransactionLogEntry", tx_id=88, from_player=1001, currency_delta=5)
        snap_entry = protocol.message("PlayerSnapshotEntry", snapshot_id=99, trigger=1, zone_id=1)
        save_res = await store_audit.save_player(
            STORAGE_ID,
            1001,
            2,
            11,
            1,
            INSTANCE_A,
            [(PlayerRowKind.PROFILE, b"")],
            False,
            transaction_logs=[tx_entry],
            audit_snapshots=[snap_entry],
        )
        assert save_res.result == SaveResult.COMMITTED
        assert save_res.audit_outbox_written is True
        assert conn_audit.committed == 1 and not conn_audit.closed
        audit_inserts = [s for s, _ in conn_audit.executed if "INSERT INTO player_audit_outbox" in s]
        assert len(audit_inserts) == 1

        # 5) verify_schema 缺列时 fail-closed
        conn_schema = FakeConn(lambda sql, args: [("player_id",), ("kind",)])
        store_schema = SqlPlayerStore({STORAGE_ID: FakePool(conn_schema)}, protocol)
        with pytest.raises(RuntimeError, match="missing required columns"):
            await store_schema.verify_schema(STORAGE_ID)

    asyncio.run(scenario())


def test_db_config_validation():
    base_section = {
        "node_id": 107,
        "host": "0.0.0.0",
        "grpc_port": 6000,
        "advertise_host": "db-107",
        "storage_ids": [1_000_000],
    }
    valid_env = {
        "MMORPG_PLAYER_STORE_SECRET_SCENE": SCENE_SECRET.decode(),
        "MMORPG_ROLE_FENCE_SECRET_DB": ROUTE_SECRET.decode(),
        "DB_MYSQL_PASSWORD": "pw",
    }
    settings = SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="stable",
        etcd="etcd:2379",
        data_service="data-service:9000",
        db_node=base_section,
    )
    cfg = DbConfig.from_settings(settings, environ=valid_env)
    assert cfg.grpc_port == 6000
    assert cfg.lease_ttl_sec == 60
    assert cfg.storage_ids == (1_000_000,)
    assert cfg.scene_secret == SCENE_SECRET
    assert cfg.route_secret == ROUTE_SECRET

    # 缺少密钥 -> fail-closed
    with pytest.raises(ValueError, match="MMORPG_PLAYER_STORE_SECRET_SCENE"):
        DbConfig.from_settings(settings, environ={"MMORPG_ROLE_FENCE_SECRET_DB": ROUTE_SECRET.decode()})

    # sql_timeout_sec <= lock_wait_timeout_sec -> 拒绝
    bad_timeout = SimpleNamespace(**{**settings.__dict__, "db_node": {**base_section, "sql_timeout_sec": 10.0}})
    with pytest.raises(ValueError, match="sql_timeout_sec"):
        DbConfig.from_settings(bad_timeout, environ=valid_env)

    # 禁止把密钥写进 TOML
    leak_toml = SimpleNamespace(**{**settings.__dict__, "db_node": {**base_section, "scene_secret": "x" * 32}})
    with pytest.raises(ValueError, match="非法字段"):
        DbConfig.from_settings(leak_toml, environ=valid_env)


# ---------------------------------------------------------------------------
# 5. 可选真实 MySQL/TiDB 集成测试（设置 XUANMING_TEST_MYSQL=host:port:user:password 开启）
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not MYSQL, reason="XUANMING_TEST_MYSQL not set")
def test_sql_player_store_live_mysql():
    async def scenario():
        import aiomysql

        host, port, user, password = MYSQL.split(":", 3)
        db_name = f"ps_test_{uuid.uuid4().hex[:8]}"
        admin = await aiomysql.connect(host=host, port=int(port), user=user, password=password, autocommit=True)
        try:
            async with admin.cursor() as cur:
                await cur.execute(f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
            pool = await open_storage_pool(
                host=host,
                port=int(port),
                user=user,
                password=password,
                database=db_name,
                maxsize=4,
            )
            try:
                protocol = Protocol()
                store = SqlPlayerStore({STORAGE_ID: pool}, protocol)
                await store.bootstrap_schema(STORAGE_ID)
                await store.verify_schema(STORAGE_ID)

                # 种子写权行
                async with pool.acquire() as conn, conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO player_store "
                        "(player_id, kind, owner_epoch, save_seq, schema_gen, holder_instance, released, payload) "
                        "VALUES (%s, 0, 1, 0, 1, UNHEX(''), 1, UNHEX(''))",
                        (1001,),
                    )

                async with running_db(store=store) as env:
                    snap = await env.ps.load_and_claim(1001, INSTANCE_A, owner_epoch=1)
                    assert snap.storage_id == STORAGE_ID
                    saved = await env.ps.commit(snap, {PlayerRowKind.PROFILE: b"\x00\xffhello"}, force=True)
                    assert saved.save_seq > 0
                    reloaded = await env.ps.load_and_claim(1001, INSTANCE_A, owner_epoch=1)
                    assert reloaded.rows[PlayerRowKind.PROFILE] == b"\x00\xffhello"
                    assert await env.ps.release_detached(1001, INSTANCE_A, owner_epoch=1) is True
            finally:
                pool.close()
                await pool.wait_closed()
        finally:
            async with admin.cursor() as cur:
                await cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
            admin.close()

    asyncio.run(scenario())
