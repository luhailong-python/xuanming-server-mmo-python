"""data_service round trips: the login ``directory.py`` client is the golden client against the new gRPC server.

Every scenario runs on the in-memory store; with ``XUANMING_TEST_MYSQL=host:port:user:password`` (an account
that may CREATE/DROP databases, e.g. a throw-away container) the same scenarios also run on ``SqlStore``.
"""

import asyncio
import json
import os
import time
import uuid
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest

from data_service_fakes import MemoryStore
from xuanming.data_service import names
from xuanming.data_service.config import DataServiceConfig
from xuanming.data_service.model import MergedZones
from xuanming.data_service.service import DataService
from xuanming.grpc_runtime import GrpcClient, serve_grpc, sign_service_call
from xuanming.login.directory import (Conflict, DataService as Directory, DirectoryError, Full, Missing,
                                      NotCommitted, status_name)
from xuanming.protocol import Protocol
from xuanming.settings import Settings

SECRETS = {
    "login": b"login-directory-secret-0123456789",
    "payment": b"payment-directory-secret-01234567",
    "scene": b"scene-directory-secret-0123456789",
    "scene_manager": b"scene-manager-secret-0123456789ab",
    "trade": b"trade-directory-secret-0123456789",
    "db": b"db-role-fence-secret-0123456789ab",
    "player_inbox": b"inbox-role-fence-secret-012345678",
}
ADMIN_TOKEN = "test-data-service-admin-token-0123456789"
STORAGE = 1_000_000
PATH = "/data_service.DataService/"
MYSQL = os.environ.get("XUANMING_TEST_MYSQL")
KINDS = ["memory", pytest.param("sql", marks=pytest.mark.skipif(not MYSQL, reason="XUANMING_TEST_MYSQL not set"))]


async def sql_store(protocol, stack):
    import aiomysql

    from xuanming.data_service.sql_store import SqlStore, open_pool

    host, port, user, password = MYSQL.split(":", 3)
    credentials = dict(host=host, port=int(port), user=user, password=password)
    suffix = uuid.uuid4().hex[:8]
    names_ = {"global": f"ds_test_g_{suffix}", "store": f"ds_test_s_{suffix}"}
    admin = await aiomysql.connect(**credentials, autocommit=True)
    async with admin.cursor() as cursor:
        for database in names_.values():
            await cursor.execute(f"CREATE DATABASE {database} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
        # player_store is owned by go/db; this is only the shape upsertInitialPlayerStoreRows needs.
        await cursor.execute(
            f"CREATE TABLE {names_['store']}.player_store (player_id BIGINT UNSIGNED NOT NULL, kind INT NOT NULL,"
            "owner_epoch BIGINT UNSIGNED NOT NULL, save_seq BIGINT UNSIGNED NOT NULL, schema_gen INT UNSIGNED NOT NULL,"
            "holder_instance VARBINARY(64) NOT NULL, released INT UNSIGNED NOT NULL,"
            "claim_seq BIGINT UNSIGNED NOT NULL DEFAULT 0, payload MEDIUMBLOB NOT NULL,"
            "PRIMARY KEY(player_id,kind))")

    async def close_pool(item):
        item.close()
        await item.wait_closed()

    async def drop():
        async with admin.cursor() as cursor:
            for database in names_.values():
                await cursor.execute(f"DROP DATABASE IF EXISTS {database}")
        admin.close()

    stack.push_async_callback(drop)
    pool = await open_pool(**credentials, database=names_["global"])
    store_pool = await open_pool(**credentials, database=names_["store"], maxsize=2)
    for item in (pool, store_pool):
        stack.push_async_callback(close_pool, item)
    store = SqlStore(pool, protocol, storage_pools={STORAGE: store_pool})
    await store.bootstrap()
    return store


@asynccontextmanager
async def running(kind, *, admin_token=ADMIN_TOKEN, deletion_cooling_off_ms=60_000, clock_ms=None, recall_enabled=True):
    protocol = Protocol()
    clock = clock_ms or (lambda: time.time_ns() // 1_000_000)
    async with AsyncExitStack() as stack:
        store = await sql_store(protocol, stack) if kind == "sql" else MemoryStore(protocol, clock_ms=clock)
        service = DataService(
            protocol, store, SECRETS, zone_refresh=0.05, clock_ms=clock,
            admin_token=admin_token, deletion_cooling_off_ms=deletion_cooling_off_ms,
            recall_enabled=recall_enabled,
        )
        await service.start()
        stack.push_async_callback(service.close)
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        stack.push_async_callback(server.stop, 0)
        client = GrpcClient(f"127.0.0.1:{port}")
        stack.push_async_callback(client.close)
        env = SimpleNamespace(protocol=protocol, store=store, service=service, client=client, kind=kind,
                              directory=Directory(protocol, client.call, SECRETS["login"]))

        def role(pid, name="", **fields):
            values = dict(player_id=pid, class_id=1, gender=1, zone_id=1, name=name, appearance_id="01_hero")
            return protocol.message("AccountSimplePlayer", **(values | fields))

        async def signed(method, request, response, caller="scene", secret=None, metadata=()):
            sign_service_call(PATH + method, request, caller, clock(), secret or SECRETS[caller])
            raw = await client.call(PATH + method, request.SerializeToString(deterministic=True), metadata=metadata)
            if response == "Empty":
                return protocol.parse("google.protobuf.Empty", raw)
            return protocol.parse(f"data_service.{response}", raw)

        async def admin_call(method, request, response, token=ADMIN_TOKEN):
            md = (("x-admin-token", token),) if token is not None else ()
            raw = await client.call(PATH + method, request.SerializeToString(deterministic=True), metadata=md)
            if response == "Empty":
                return protocol.parse("google.protobuf.Empty", raw)
            return protocol.parse(f"data_service.{response}", raw)

        env.role, env.signed, env.admin_call = role, signed, admin_call
        yield env


def exercise(kind, scenario, **kwargs):
    async def main():
        async with running(kind, **kwargs) as env:
            await scenario(env)

    asyncio.run(main())


def code(error: grpc.aio.AioRpcError) -> str:
    return error.code().name


async def new_player(env, zone=1):
    """A player id with its home zone registered, like login's create flow."""
    pid = await env.directory.mint_player_id()
    await env.directory.register_player_zone(pid, zone, STORAGE)
    return pid


@pytest.mark.parametrize("kind", KINDS)
def test_login_create_flow_round_trip(kind):
    async def scenario(env):
        d = env.directory
        assert await d.read_account("acct", True) == []
        with pytest.raises(Missing):
            await d.read_account("nobody", False)
        pid = await new_player(env)
        assert await d.reserve_name(pid, "云中君") == (0, 0)
        players = await d.create_role("acct", env.role(pid, "云中君"))
        assert [p.player_id for p in players] == [pid]
        assert [p.player_id for p in await d.read_account("acct", False)] == [pid]
        owned = await d.lookup_role(pid)
        assert (owned.account, owned.owner_revision, owned.trade_listing_id) == ("acct", 1, 0)
        assert owned.initial_player_commit and owned.role.name == "云中君"
        assert await d.home_zones([pid, pid + 1]) == {pid: 1}
        assert await d.player_names([pid, pid + 1]) == {pid: "云中君"}
        listed = await d.role_list(await d.read_account("acct", False))
        assert listed[0].player.name == "云中君"
        # replay of the same create is idempotent; a different body for the same id conflicts
        assert [p.player_id for p in await d.create_role("acct", env.role(pid, "云中君"))] == [pid]
        with pytest.raises(Conflict):
            await d.create_role("acct", env.role(pid, "云中君", class_id=2))
        with pytest.raises(Conflict):
            await d.create_role("other", env.role(pid, "云中君"))

    exercise(kind, scenario)


@pytest.mark.parametrize("kind", KINDS)
def test_create_outcomes_not_committed_full_and_unknown(kind):
    async def scenario(env):
        d = env.directory
        pid = await env.directory.mint_player_id()  # no route registered: refused before the commit point
        with pytest.raises(NotCommitted):
            await d.create_role("acct", env.role(pid))
        for _ in range(5):
            await d.create_role("acct", env.role(await new_player(env)))
        with pytest.raises(Full):
            await d.create_role("acct", env.role(await new_player(env)))
        assert len(await d.read_account("acct", False)) == 5
        if env.kind == "memory":
            env.store.fail.add("read_account")
            with pytest.raises(DirectoryError) as error:  # unknown outcome is UNAVAILABLE, never ABORTED
                await d.read_account("acct", False)
            assert status_name(error.value.__cause__) == "UNAVAILABLE"

    exercise(kind, scenario)


@pytest.mark.parametrize("kind", KINDS)
def test_names_reserve_taken_invalid_release(kind):
    async def scenario(env):
        d = env.directory
        a, b, c = await new_player(env), await new_player(env), await new_player(env)
        assert await d.reserve_name(a, "Yun") == (0, 0)
        assert await d.reserve_name(a, "Yun") == (0, 0)                 # idempotent retry
        assert await d.reserve_name(b, "ＹＵＮ") == (1, a)              # NFKC + case fold hit the same key
        for bad in ("GM01", "官方客服", "a b", "", "x" * 33, "a_b"):
            assert await d.reserve_name(b, bad) == (2, 0), bad
        with pytest.raises(grpc.aio.AioRpcError) as error:               # one name per player id
            await d.reserve_name(a, "Other")
        assert code(error.value) == "FAILED_PRECONDITION" and "error_code=26" in error.value.details()
        # an uncommitted reservation is released, the id is sealed and the name is free again
        await d.release_name(a, "Yun")
        await d.release_name(a, "Yun")                                   # second compensation is a no-op
        assert await d.reserve_name(c, "Yun") == (0, 0)
        with pytest.raises(Conflict):
            await d.create_role("acct", env.role(a, "Yun"))
        # a committed role keeps its name: release is refused
        await d.create_role("acct2", env.role(c, "Yun"))
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await d.release_name(c, "Yun")
        assert code(error.value) == "FAILED_PRECONDITION" and "error_code=38" in error.value.details()
        assert await d.player_names([a, c]) == {c: "Yun"}

    exercise(kind, scenario)


@pytest.mark.parametrize("kind", KINDS)
def test_home_zone_register_conflict_invalid_and_merge(kind):
    async def scenario(env):
        d = env.directory
        pid = await new_player(env, zone=3)
        await d.register_player_zone(pid, 3, STORAGE)                    # same home again: ok
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await d.register_player_zone(pid, 4, STORAGE)
        assert code(error.value) == "ALREADY_EXISTS" and "error_code=21" in error.value.details()
        for args in ((0, 3, STORAGE), (pid, 0, STORAGE), (pid, 1_000_000, STORAGE), (pid, 3, 999_999)):
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await d.register_player_zone(*args)
            assert code(error.value) == "UNAVAILABLE" and "error_code=14" in error.value.details()
        if env.kind == "memory":                                         # merged zones resolve at the RPC exit
            env.store.merged[3] = 9
            for _ in range(100):
                if (await d.home_zones([pid])).get(pid) == 9:
                    break
                await asyncio.sleep(0.05)
            assert await d.home_zones([pid]) == {pid: 9}

    exercise(kind, scenario)


def test_merged_zone_chain_and_cycle_guard():
    zones = MergedZones({1: 2, 2: 3, 7: 8, 8: 7})
    assert (zones.final(1), zones.final(3), zones.final(5)) == (3, 3, 5)
    assert zones.final(7) in (7, 8)  # a corrupt cycle terminates


@pytest.mark.parametrize("kind", KINDS)
def test_caller_auth(kind):
    async def scenario(env):
        for secret in (b"x" * 32, SECRETS["scene"]):                    # unknown secret / right secret, wrong caller
            with pytest.raises(DirectoryError) as error:
                await Directory(env.protocol, env.client.call, secret).read_account("acct", True)
            assert status_name(error.value.__cause__) == "PERMISSION_DENIED"
        # scene_manager may only look roles up; unsigned names/zones/ids stay open like in Go
        request = env.protocol.message("data_service.LookupAccountRoleRequest", player_id=1)
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await env.signed("LookupAccountRole", request, "AccountRoleOwnership", "scene_manager")
        assert code(error.value) == "NOT_FOUND"                          # admitted, role is just missing
        request = env.protocol.message("data_service.ReleasePlayerNameRequest", player_id=1, name="Abc")
        with pytest.raises(grpc.aio.AioRpcError) as error:               # release is login-only
            await env.signed("ReleasePlayerName", request, "Empty", "scene")
        assert code(error.value) == "PERMISSION_DENIED"

    exercise(kind, scenario)


@pytest.mark.parametrize("kind", KINDS)
def test_role_admission_register_replay_close(kind):
    async def scenario(env):
        pid = await new_player(env)
        await env.directory.create_role("acct", env.role(pid))
        instance = str(uuid.uuid4())

        def identity(generation, **fields):
            values = dict(player_id=pid, scene_instance_id=instance, generation=generation, owner_revision=1,
                          owner_epoch=1, scene_node_id="scene-1", scene_id=7, zone_id=1, session_id=1,
                          login_generation=1) | fields
            return env.protocol.message("common.base.RoleAdmissionIdentity", **values)

        async def register(generation, **fields):
            request = env.protocol.message("data_service.RegisterRoleAdmissionRequest",
                                           identity=identity(generation, **fields))
            return (await env.signed("RegisterRoleAdmission", request, "RegisterRoleAdmissionResponse")
                    ).closed_through

        async def close(through, epoch=0, seq=0):
            request = env.protocol.message(
                "data_service.CloseRoleAdmissionsRequest", player_id=pid, scene_instance_id=instance,
                owner_revision=1, through_generation=through, required_owner_epoch=epoch, required_save_seq=seq)
            return (await env.signed("CloseRoleAdmissions", request, "CloseRoleAdmissionsResponse")).closed_through

        async def refused(awaitable, status):
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await awaitable
            assert code(error.value) == status

        assert await register(1) == 0
        assert await register(1) == 0                                    # same generation, same body: replay
        await refused(register(1, session_id=2), "FAILED_PRECONDITION")  # same generation, other body
        assert await register(3) == 0
        await refused(register(2), "FAILED_PRECONDITION")                # below the highest
        await refused(register(4, scene_node_id="scene-2"), "FAILED_PRECONDITION")  # node can't be reassigned
        await refused(register(4, owner_revision=2), "FAILED_PRECONDITION")
        await refused(register(0), "INVALID_ARGUMENT")
        assert await close(3, 5, 9) == 3
        assert await close(3, 5, 9) == 3                                 # same close digest: replay
        await refused(close(3, 6, 9), "FAILED_PRECONDITION")             # same generation, other floor
        await refused(close(4), "FAILED_PRECONDITION")                   # beyond the highest registered
        await refused(register(3), "FAILED_PRECONDITION")                # closed through 3
        assert await register(4) == 3
        await refused(close(1), "FAILED_PRECONDITION")                   # below the closed floor
        await refused(close(1, 1, 0), "INVALID_ARGUMENT")                # save floor needs both halves
        request = env.protocol.message("data_service.RegisterRoleAdmissionRequest", identity=identity(5))
        with pytest.raises(grpc.aio.AioRpcError) as error:               # only scene may register
            await env.signed("RegisterRoleAdmission", request, "RegisterRoleAdmissionResponse", "login")
        assert code(error.value) == "PERMISSION_DENIED"

    exercise(kind, scenario)


@pytest.mark.parametrize("kind", KINDS)
def test_id_segments_and_unimplemented(kind):
    async def scenario(env):
        async def allocate(tag, step=0):
            request = env.protocol.message("data_service.AllocateIdSegmentRequest", biz_tag=tag, step=step)
            raw = await env.client.call(PATH + "AllocateIdSegment", request.SerializeToString())
            return env.protocol.parse("data_service.AllocateIdSegmentResponse", raw)

        first, second, big = await allocate("item"), await allocate("txlog", 7), await allocate("item", 10_000_000)
        assert (first.error_code, first.hi - first.lo) == (0, 100)       # step 0 = default 100
        assert (second.error_code, second.lo, second.hi - second.lo) == (0, first.hi, 7)
        assert big.error_code == 0 and big.lo == second.hi and big.hi - big.lo == 10_000_000
        assert (await allocate("nope")).error_code == 20                 # unknown tag, still an OK status
        assert (await allocate("Bad Tag")).error_code == 14 and (await allocate("")).error_code == 14
        assert (await allocate("item", 10_000_001)).error_code == 14
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await env.client.call(PATH + "NonExistentDataServiceRpc", b"")
        assert code(error.value) == "UNIMPLEMENTED"

    exercise(kind, scenario)


def test_extended_route_fence_and_ledger_rpcs():
    async def scenario(env):
        await env.directory.register_player_zone(9001, 2, 1_000_000)
        await env.directory.create_role("acc-ext", env.role(9001, zone_id=2, name="Hero"))

        # GetPlayerHomeZone + GetMergedZones + CountZoneRoles
        raw_home = await env.client.call(
            PATH + "GetPlayerHomeZone",
            env.protocol.message("data_service.GetPlayerHomeZoneRequest", player_id=9001).SerializeToString(),
        )
        assert env.protocol.parse("data_service.GetPlayerHomeZoneResponse", raw_home).home_zone_id == 2
        raw_merged = await env.client.call(PATH + "GetMergedZones", b"")
        assert dict(env.protocol.parse("data_service.GetMergedZonesResponse", raw_merged).merged_into) == {}
        raw_cnt = await env.client.call(
            PATH + "CountZoneRoles",
            env.protocol.message("data_service.CountZoneRolesRequest", zone_id=2).SerializeToString(),
        )
        assert env.protocol.parse("data_service.CountZoneRolesResponse", raw_cnt).roles == 1

        # GetPlayerRoute (signed as db)
        req_route = env.protocol.message("data_service.GetPlayerRouteRequest", player_id=9001)
        sign_service_call(PATH + "GetPlayerRoute", req_route, "db", int(time.time() * 1000), SECRETS["db"])
        raw_route = await env.client.call(PATH + "GetPlayerRoute", req_route.SerializeToString(deterministic=True))
        route_resp = env.protocol.parse("data_service.GetPlayerRouteResponse", raw_route)
        assert (route_resp.found, route_resp.home_zone, route_resp.storage_id) == (True, 2, 1_000_000)

        # GetRoleFenceState (signed as player_inbox)
        req_fence = env.protocol.message("data_service.GetRoleFenceStateRequest", player_id=9001)
        sign_service_call(PATH + "GetRoleFenceState", req_fence, "player_inbox", int(time.time() * 1000), SECRETS["player_inbox"])
        raw_fence = await env.client.call(PATH + "GetRoleFenceState", req_fence.SerializeToString(deterministic=True))
        fence_resp = env.protocol.parse("data_service.GetRoleFenceStateResponse", raw_fence)
        assert fence_resp.found and fence_resp.state.account == "acc-ext" and fence_resp.state.owner_revision == 1

        # RegisterRoleAdmission -> VerifyRoleAdmission (signed as player_inbox)
        identity = env.protocol.message(
            "common.base.RoleAdmissionIdentity",
            player_id=9001,
            scene_instance_id="11111111-1111-1111-1111-111111111111",
            generation=1,
            owner_revision=1,
            owner_epoch=5,
            scene_node_id="scene-1",
            scene_id=100,
            zone_id=2,
            session_id=77,
            login_generation=1,
        )
        reg = env.protocol.message("data_service.RegisterRoleAdmissionRequest", identity=identity)
        sign_service_call(PATH + "RegisterRoleAdmission", reg, "scene", int(time.time() * 1000), SECRETS["scene"])
        await env.client.call(PATH + "RegisterRoleAdmission", reg.SerializeToString(deterministic=True))

        verify_req = env.protocol.message("data_service.VerifyRoleAdmissionRequest", admission=identity)
        sign_service_call(PATH + "VerifyRoleAdmission", verify_req, "player_inbox", int(time.time() * 1000), SECRETS["player_inbox"])
        raw_verify = await env.client.call(PATH + "VerifyRoleAdmission", verify_req.SerializeToString(deterministic=True))
        assert env.protocol.parse("data_service.VerifyRoleAdmissionResponse", raw_verify).valid is True

        # GetPlayerInboxLedger & GetPlayerItemGainBlock
        raw_ledger = await env.client.call(
            PATH + "GetPlayerInboxLedger",
            env.protocol.message("data_service.GetPlayerInboxLedgerRequest", player_id=9001).SerializeToString(),
        )
        assert env.protocol.parse("data_service.GetPlayerInboxLedgerResponse", raw_ledger).found is True
        raw_block = await env.client.call(
            PATH + "GetPlayerItemGainBlock",
            env.protocol.message("data_service.GetPlayerItemGainBlockRequest", player_id=9001).SerializeToString(),
        )
        assert env.protocol.parse("data_service.GetPlayerItemGainBlockResponse", raw_block).found is True

    exercise("memory", scenario)


def test_name_charset_vectors_shared_with_go():
    vectors = json.loads((Path(__file__).parent / "fixtures" / "player_name_charset_vectors.json").read_text("utf-8"))
    for item in vectors["allowed"]:
        assert names.allowed(chr(int(item["cp"], 16))), item["cp"]
    for item in vectors["rejected"]:
        assert not names.allowed(chr(int(item["cp"], 16))), item["cp"]


def test_name_normalize_contract():
    assert names.normalize("ＡＢ１２")[:2] == ("AB12", "ab12")          # NFKC then lower
    assert names.normalize("　云中君　")[:2] == ("云中君", "云中君")       # ideographic space trimmed
    assert names.normalize("   ")[2] == names.EMPTY
    assert names.normalize("GM3f7k")[2] == names.SENSITIVE and names.normalize("sigma")[2] == names.OK
    assert names.normalize("a" * 32)[2] == names.OK and names.normalize("a" * 33)[2] == names.INVALID
    assert names.normalize("a b")[2] == names.INVALID and names.normalize("😀")[2] == names.INVALID


def test_cluster_example_config_and_secret_rules():
    settings = Settings.load(Path(__file__).parents[1] / "config" / "cluster.example.toml")
    environ = {"MMORPG_ACCOUNT_DIRECTORY_SECRET_LOGIN": "a" * 32, "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE": "b" * 32}
    config = DataServiceConfig.from_settings(settings, environ)
    assert (config.node_id, config.grpc_port, config.logical_shards, sorted(config.secrets)) == (
        105, 9000, 1, ["login", "scene"])
    with pytest.raises(ValueError):  # secrets must be distinct
        DataServiceConfig.from_settings(settings, environ | {"MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE": "a" * 32})
    with pytest.raises(ValueError):  # and at least 32 bytes
        DataServiceConfig.from_settings(settings, environ | {"MMORPG_ACCOUNT_DIRECTORY_SECRET_LOGIN": "short"})
    sharded = Settings.load(Path(__file__).parents[1] / "config" / "cluster.example.toml")
    object.__setattr__(sharded, "data_service_node", sharded.data_service_node | {"logical_shards": 2})
    with pytest.raises(ValueError, match="logical_shards"):
        DataServiceConfig.from_settings(sharded, environ)
    with_payment = DataServiceConfig.from_settings(
        settings, environ | {"MMORPG_ACCOUNT_DIRECTORY_SECRET_PAYMENT": "p" * 32, "DATA_SERVICE_ADMIN_TOKEN": "tok"},
    )
    assert "payment" in with_payment.secrets and with_payment.admin_token == "tok"


def _make_pi(year: int, month: int, day: int, tag: str = "a") -> str:
    """按国家防沉迷 26 进制出生日期编码生成合法 38 位 pi。"""
    digits = "0123456789abcdefghijklmnop"
    value = year * 10000 + month * 100 + day
    prefix = []
    for _ in range(6):
        prefix.append(digits[value % 26])
        value //= 26
    return "".join(reversed(prefix)) + (tag * 32)[:32]


def test_all_proto_rpcs_registered():
    protocol = Protocol()
    proto_methods = {m.name for m in protocol.pool.FindServiceByName("data_service.DataService").methods}
    service = DataService(protocol, MemoryStore(protocol), SECRETS)
    registered = set(service.services()["data_service.DataService"].keys())
    assert proto_methods == registered
    assert len(registered) == 50


@pytest.mark.parametrize("kind", KINDS)
def test_real_name_lifecycle_and_auth(kind):
    async def scenario(env):
        pi_a = _make_pi(1995, 6, 15, "a")
        pi_b = _make_pi(2000, 1, 20, "b")

        # 账号不存在返回 NOT_FOUND
        set_missing = env.protocol.message("data_service.SetAccountRealNameRequest", account="missing", real_name_pi=pi_a)
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await env.signed("SetAccountRealName", set_missing, "SetAccountRealNameResponse", "login")
        assert code(err.value) == "NOT_FOUND"

        # 建账号与角色
        await env.directory.read_account("acct-rn", True)
        pid = await new_player(env)
        await env.directory.create_role("acct-rn", env.role(pid, "实名侠"))

        # 非法 pi 返回 INVALID_ARGUMENT
        bad_set = env.protocol.message("data_service.SetAccountRealNameRequest", account="acct-rn", real_name_pi="bad")
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await env.signed("SetAccountRealName", bad_set, "SetAccountRealNameResponse", "login")
        assert code(err.value) == "INVALID_ARGUMENT"

        # 首次写入、幂等重放、只写一次不覆盖
        r1 = await env.signed(
            "SetAccountRealName",
            env.protocol.message("data_service.SetAccountRealNameRequest", account="acct-rn", real_name_pi=pi_a),
            "SetAccountRealNameResponse", "login",
        )
        assert r1.real_name_pi == pi_a
        r2 = await env.signed(
            "SetAccountRealName",
            env.protocol.message("data_service.SetAccountRealNameRequest", account="acct-rn", real_name_pi=pi_b),
            "SetAccountRealNameResponse", "login",
        )
        assert r2.real_name_pi == pi_a  # 已有不同 pi 时不改，回库里现存值

        # ReadAccountDirectory 带回 real_name_pi
        dir_resp = await env.signed(
            "ReadAccountDirectory",
            env.protocol.message("data_service.ReadAccountDirectoryRequest", account="acct-rn"),
            "AccountDirectoryResponse", "login",
        )
        assert dir_resp.real_name_pi == pi_a

        # GetAccountRealName 只认 payment
        get_req = env.protocol.message("data_service.GetAccountRealNameRequest", player_id=pid)
        got = await env.signed("GetAccountRealName", get_req, "GetAccountRealNameResponse", "payment")
        assert (got.found, got.real_name_pi) == (True, pi_a)
        not_found = await env.signed(
            "GetAccountRealName",
            env.protocol.message("data_service.GetAccountRealNameRequest", player_id=999999),
            "GetAccountRealNameResponse", "payment",
        )
        assert not_found.found is False

        # ResetAccountRealName 幂等重置后允许重新绑定新 pi
        rst1 = await env.signed(
            "ResetAccountRealName",
            env.protocol.message("data_service.ResetAccountRealNameRequest", account="acct-rn"),
            "ResetAccountRealNameResponse", "login",
        )
        assert rst1.had_real_name is True
        rst2 = await env.signed(
            "ResetAccountRealName",
            env.protocol.message("data_service.ResetAccountRealNameRequest", account="acct-rn"),
            "ResetAccountRealNameResponse", "login",
        )
        assert rst2.had_real_name is False
        r3 = await env.signed(
            "SetAccountRealName",
            env.protocol.message("data_service.SetAccountRealNameRequest", account="acct-rn", real_name_pi=pi_b),
            "SetAccountRealNameResponse", "login",
        )
        assert r3.real_name_pi == pi_b

    exercise(kind, scenario)


def test_account_deletion_lifecycle_and_state_machine():
    now = [time.time_ns() // 1_000_000]

    async def scenario(env):
        await env.directory.read_account("acct-del", True)
        pid = await new_player(env)
        await env.directory.create_role("acct-del", env.role(pid, "待销角色"))

        # 挂起寄售角色会阻塞注销预检（BLOCKED）
        await env.signed(
            "BeginAccountRoleTrade",
            env.protocol.message(
                "data_service.BeginAccountRoleTradeRequest",
                listing_id=101, player_id=pid, seller_account="acct-del", expected_owner_revision=1,
            ),
            "AccountRoleTradeReceipt", "trade",
        )
        blocked = await env.signed(
            "RequestAccountDeletion",
            env.protocol.message("data_service.RequestAccountDeletionRequest", account="acct-del", source=1),
            "AccountDeletionResponse", "login",
        )
        assert blocked.outcome == 3 and [(b.player_id, b.reason) for b in blocked.blockers] == [(pid, "role_listed")]

        # 取消寄售后申请注销：0 -> 1
        await env.signed(
            "CancelAccountRoleTrade",
            env.protocol.message(
                "data_service.CancelAccountRoleTradeRequest",
                listing_id=101, player_id=pid, seller_account="acct-del", expected_owner_revision=1,
            ),
            "AccountRoleTradeReceipt", "trade",
        )
        req_ok = await env.signed(
            "RequestAccountDeletion",
            env.protocol.message("data_service.RequestAccountDeletionRequest", account="acct-del", source=1),
            "AccountDeletionResponse", "login",
        )
        assert req_ok.outcome == 1 and req_ok.lifecycle.deletion_state == 1
        assert req_ok.lifecycle.deletion_execute_after_ms == now[0] + 60_000

        # 冷静期禁止建新角，LookupAccountRole(include_account_state=True) 返回 PENDING(1)
        with pytest.raises(Full):
            await env.directory.create_role("acct-del", env.role(await new_player(env)))
        looked = await env.signed(
            "LookupAccountRole",
            env.protocol.message("data_service.LookupAccountRoleRequest", player_id=pid, include_account_state=True),
            "AccountRoleOwnership", "login",
        )
        assert looked.account_deletion_state == 1

        # 撤销注销：1 -> 0，再重新申请
        cancelled = await env.signed(
            "CancelAccountDeletion",
            env.protocol.message("data_service.CancelAccountDeletionRequest", account="acct-del"),
            "AccountDeletionResponse", "login",
        )
        assert cancelled.outcome == 1 and cancelled.lifecycle.deletion_state == 0
        await env.signed(
            "RequestAccountDeletion",
            env.protocol.message("data_service.RequestAccountDeletionRequest", account="acct-del", source=1),
            "AccountDeletionResponse", "login",
        )

        # 冷静期未满认领为空；冷静期满后认领：1 -> 2
        c0 = await env.signed(
            "ClaimAccountDeletions",
            env.protocol.message("data_service.ClaimAccountDeletionsRequest", limit=10),
            "ClaimAccountDeletionsResponse", "login",
        )
        assert list(c0.accounts) == []
        now[0] += 60_001
        c1 = await env.signed(
            "ClaimAccountDeletions",
            env.protocol.message("data_service.ClaimAccountDeletionsRequest", limit=10),
            "ClaimAccountDeletionsResponse", "login",
        )
        assert list(c1.accounts) == ["acct-del"]

        # 状态 2 不可撤销：TOO_LATE(5)
        too_late = await env.signed(
            "CancelAccountDeletion",
            env.protocol.message("data_service.CancelAccountDeletionRequest", account="acct-del"),
            "AccountDeletionResponse", "login",
        )
        assert too_late.outcome == 5 and too_late.lifecycle.deletion_state == 2

        # 还有存活角色时收尾返回 done=False, remaining_refs=1
        fin1 = await env.signed(
            "FinishAccountDeletion",
            env.protocol.message("data_service.FinishAccountDeletionRequest", account="acct-del"),
            "FinishAccountDeletionResponse", "login",
        )
        assert (fin1.done, fin1.remaining_refs) == (False, 1)

        # 删角后收尾成功写回执并删账号行，重放幂等
        await env.signed(
            "RemoveAccountRoles",
            env.protocol.message(
                "data_service.RemoveAccountRolesRequest",
                account="acct-del",
                roles=[env.protocol.message("data_service.AccountRoleVersion", player_id=pid, owner_revision=1)],
            ),
            "Empty", "login",
        )
        fin2 = await env.signed(
            "FinishAccountDeletion",
            env.protocol.message("data_service.FinishAccountDeletionRequest", account="acct-del"),
            "FinishAccountDeletionResponse", "login",
        )
        assert (fin2.done, fin2.remaining_refs) == (True, 0)
        fin3 = await env.signed(
            "FinishAccountDeletion",
            env.protocol.message("data_service.FinishAccountDeletionRequest", account="acct-del"),
            "FinishAccountDeletionResponse", "login",
        )
        assert fin3.done is True

        # 同账号重新登录建新行时返回 previous_deleted_ms > 0
        reborn = await env.signed(
            "ReadAccountDirectory",
            env.protocol.message("data_service.ReadAccountDirectoryRequest", account="acct-del", create_if_missing=True),
            "AccountDirectoryResponse", "login",
        )
        assert reborn.lifecycle.deletion_state == 0 and reborn.lifecycle.previous_deleted_ms == now[0]

    exercise("memory", scenario, deletion_cooling_off_ms=60_000, clock_ms=lambda: now[0])


@pytest.mark.parametrize("kind", KINDS)
def test_role_trade_prepare_freeze_cancel_transfer_slot_and_drain_ack(kind):
    async def scenario(env):
        pid = await new_player(env)
        await env.directory.create_role("seller", env.role(pid, "寄售侠"))
        await env.directory.read_account("buyer", True)
        instance = str(uuid.uuid4())

        # CheckAccountRoleSlot
        slot = await env.signed(
            "CheckAccountRoleSlot",
            env.protocol.message("data_service.CheckAccountRoleSlotRequest", account="buyer"),
            "CheckAccountRoleSlotResponse", "trade",
        )
        assert (slot.full, slot.retry) == (False, False)

        # 在线入场登记
        ident = env.protocol.message(
            "common.base.RoleAdmissionIdentity",
            player_id=pid, scene_instance_id=instance, generation=1, owner_revision=1,
            owner_epoch=5, scene_node_id="scene-1", scene_id=7, zone_id=1, session_id=1, login_generation=1,
        )
        await env.signed(
            "RegisterRoleAdmission",
            env.protocol.message("data_service.RegisterRoleAdmissionRequest", identity=ident),
            "RegisterRoleAdmissionResponse", "scene",
        )

        # BeginAccountRoleTrade -> FREEZING 且快照 holder
        begin_req = env.protocol.message(
            "data_service.BeginAccountRoleTradeRequest",
            listing_id=2001, player_id=pid, seller_account="seller", expected_owner_revision=1,
        )
        begun = await env.signed("BeginAccountRoleTrade", begin_req, "AccountRoleTradeReceipt", "trade")
        assert begun.state == 1 and len(begun.holders) == 1 and begun.holders[0].drained is False
        seal = begun.holders[0].seal

        # 未排空前 Confirm 拒绝；Cancel 进入 CANCEL_PENDING
        confirm_req = env.protocol.message(
            "data_service.ConfirmAccountRoleTradeFreezeRequest",
            listing_id=2001, player_id=pid, seller_account="seller", expected_owner_revision=1,
        )
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await env.signed("ConfirmAccountRoleTradeFreeze", confirm_req, "AccountRoleTradeReceipt", "trade")
        assert code(err.value) == "FAILED_PRECONDITION"

        # 排空 ACK（caller=scene，幂等重放）
        ack_req = env.protocol.message(
            "data_service.AckRoleTradeDrainRequest",
            seal=seal, required_owner_epoch=0, required_save_seq=0,
        )
        await env.signed("AckRoleTradeDrain", ack_req, "Empty", "scene")
        await env.signed("AckRoleTradeDrain", ack_req, "Empty", "scene")

        # 排空后更新存盘版本（模拟 scene 最终存盘），Confirm 进入 FROZEN
        if env.kind == "memory":
            env.store.store_holders[pid] = [5, b"", 1, 10]
        frozen = await env.signed("ConfirmAccountRoleTradeFreeze", confirm_req, "AccountRoleTradeReceipt", "trade")
        assert frozen.state == 2 and frozen.HasField("freeze_evidence") and len(frozen.freeze_evidence.persisted_digest) == 32

        # TransferAccountRole 过户到 buyer（幂等重放）
        tx_req = env.protocol.message(
            "data_service.TransferAccountRoleRequest",
            tx_id=3001, listing_id=2001, player_id=pid, seller_account="seller", buyer_account="buyer",
            expected_owner_revision=1,
        )
        receipt = await env.signed("TransferAccountRole", tx_req, "AccountRoleTransferReceipt", "trade")
        assert (receipt.tx_id, receipt.buyer_account, receipt.buyer_owner_revision) == (3001, "buyer", 2)
        replay = await env.signed("TransferAccountRole", tx_req, "AccountRoleTransferReceipt", "trade")
        assert replay.tx_id == 3001 and replay.buyer_owner_revision == 2
        assert [p.player_id for p in await env.directory.read_account("buyer", False)] == [pid]
        assert [p.player_id for p in await env.directory.read_account("seller", False)] == []

        # 取消先于 Begin 写墓碑，迟到 Begin 重放 CANCELLED
        cancel_pre = await env.signed(
            "CancelAccountRoleTrade",
            env.protocol.message(
                "data_service.CancelAccountRoleTradeRequest",
                listing_id=2002, player_id=pid, seller_account="buyer", expected_owner_revision=2,
            ),
            "AccountRoleTradeReceipt", "trade",
        )
        assert cancel_pre.state == 3
        late_begin = await env.signed(
            "BeginAccountRoleTrade",
            env.protocol.message(
                "data_service.BeginAccountRoleTradeRequest",
                listing_id=2002, player_id=pid, seller_account="buyer", expected_owner_revision=2,
            ),
            "AccountRoleTradeReceipt", "trade",
        )
        assert late_begin.state == 3

        # 正常挂起并取消解冻（含 CANCEL_PENDING -> ACK -> CANCELLED）
        ident2 = env.protocol.message(
            "common.base.RoleAdmissionIdentity",
            player_id=pid, scene_instance_id=str(uuid.uuid4()), generation=1, owner_revision=2,
            owner_epoch=6, scene_node_id="scene-1", scene_id=7, zone_id=1, session_id=2, login_generation=1,
        )
        await env.signed(
            "RegisterRoleAdmission",
            env.protocol.message("data_service.RegisterRoleAdmissionRequest", identity=ident2),
            "RegisterRoleAdmissionResponse", "scene",
        )
        b3 = await env.signed(
            "BeginAccountRoleTrade",
            env.protocol.message(
                "data_service.BeginAccountRoleTradeRequest",
                listing_id=2003, player_id=pid, seller_account="buyer", expected_owner_revision=2,
            ),
            "AccountRoleTradeReceipt", "trade",
        )
        c_pend = await env.signed(
            "CancelAccountRoleTrade",
            env.protocol.message(
                "data_service.CancelAccountRoleTradeRequest",
                listing_id=2003, player_id=pid, seller_account="buyer", expected_owner_revision=2,
            ),
            "AccountRoleTradeReceipt", "trade",
        )
        assert c_pend.state == 5  # CANCEL_PENDING
        await env.signed(
            "AckRoleTradeDrain",
            env.protocol.message("data_service.AckRoleTradeDrainRequest", seal=b3.holders[0].seal),
            "Empty", "scene",
        )
        c_done = await env.signed(
            "CancelAccountRoleTrade",
            env.protocol.message(
                "data_service.CancelAccountRoleTradeRequest",
                listing_id=2003, player_id=pid, seller_account="buyer", expected_owner_revision=2,
            ),
            "AccountRoleTradeReceipt", "trade",
        )
        assert c_done.state == 3  # CANCELLED

    exercise(kind, scenario)


@pytest.mark.parametrize("kind", KINDS)
def test_role_rename_appearance_repair_remove_and_channel_identity(kind):
    async def scenario(env):
        pid = await new_player(env)
        pid2 = await new_player(env)
        assert await env.directory.reserve_name(pid, "初名") == (0, 0)
        assert await env.directory.reserve_name(pid2, "别家名") == (0, 0)
        await env.directory.create_role("acct-ops", env.role(pid, "初名", appearance_id=""))
        await env.directory.create_role("acct-other", env.role(pid2, "别家名"))

        # RepairAccountRole 仅补缺失字段，已有不同字段报错
        repaired = await env.signed(
            "RepairAccountRole",
            env.protocol.message(
                "data_service.RepairAccountRoleRequest",
                account="acct-ops",
                role=env.role(pid, "初名", appearance_id="02_skin"),
                expected_owner_revision=1,
            ),
            "AccountDirectoryResponse", "login",
        )
        assert repaired.players.players[0].appearance_id == "02_skin"
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await env.signed(
                "RepairAccountRole",
                env.protocol.message(
                    "data_service.RepairAccountRoleRequest",
                    account="acct-ops",
                    role=env.role(pid, "初名", appearance_id="99_conflict"),
                    expected_owner_revision=1,
                ),
                "AccountDirectoryResponse", "login",
            )
        assert code(err.value) == "FAILED_PRECONDITION"

        # UpdateRoleAppearance 同步永久外观，不动 owner_revision
        upd1 = await env.signed(
            "UpdateRoleAppearance",
            env.protocol.message("data_service.UpdateRoleAppearanceRequest", player_id=pid, appearance_id="03_jade"),
            "UpdateRoleAppearanceResponse", "scene",
        )
        assert upd1.updated is True
        upd2 = await env.signed(
            "UpdateRoleAppearance",
            env.protocol.message("data_service.UpdateRoleAppearanceRequest", player_id=pid, appearance_id="03_jade"),
            "UpdateRoleAppearanceResponse", "scene",
        )
        assert upd2.updated is False
        owned = await env.directory.lookup_role(pid)
        assert owned.role.appearance_id == "03_jade" and owned.owner_revision == 1

        # UpsertAccountChannelIdentity -> GetPlayerChannelIdentity 返回持久化渠道与 open_id
        await env.signed(
            "UpsertAccountChannelIdentity",
            env.protocol.message(
                "data_service.UpsertAccountChannelIdentityRequest",
                account="acct-ops", channel="wechat_minigame", open_id="wx_openid_12345",
            ),
            "Empty", "login",
        )
        raw_ch = await env.client.call(
            PATH + "GetPlayerChannelIdentity",
            env.protocol.message("data_service.GetPlayerChannelIdentityRequest", player_id=pid).SerializeToString(),
        )
        ch = env.protocol.parse("data_service.GetPlayerChannelIdentityResponse", raw_ch)
        assert (ch.found, ch.account, ch.channel, ch.open_id) == (True, "acct-ops", "wechat_minigame", "wx_openid_12345")

        # RenameRole：成功、重放、STALE_SEQ、SAME_NAME、COOLDOWN、TAKEN、INVALID、NOT_FOUND
        rn1 = await env.signed(
            "RenameRole",
            env.protocol.message(
                "data_service.RenameRoleRequest",
                player_id=pid, seq=1, name="新剑仙", cooldown_seconds=3600, hold_seconds=600, min_chars=2, max_chars=8,
            ),
            "RenameRoleResponse", "scene",
        )
        assert (rn1.result, rn1.current_seq, rn1.name) == (1, 1, "新剑仙") and rn1.renamed_ms > 0
        assert await env.directory.player_names([pid]) == {pid: "新剑仙"}
        rn1_replay = await env.signed(
            "RenameRole",
            env.protocol.message(
                "data_service.RenameRoleRequest",
                player_id=pid, seq=1, name="新剑仙", cooldown_seconds=3600, hold_seconds=600, min_chars=2, max_chars=8,
            ),
            "RenameRoleResponse", "scene",
        )
        assert (rn1_replay.result, rn1_replay.name) == (1, "新剑仙")
        rn_stale = await env.signed(
            "RenameRole",
            env.protocol.message(
                "data_service.RenameRoleRequest",
                player_id=pid, seq=1, name="换个名", cooldown_seconds=3600, hold_seconds=600,
            ),
            "RenameRoleResponse", "scene",
        )
        assert (rn_stale.result, rn_stale.current_seq) == (6, 1)
        rn_same = await env.signed(
            "RenameRole",
            env.protocol.message(
                "data_service.RenameRoleRequest",
                player_id=pid, seq=2, name="新剑仙", cooldown_seconds=3600, hold_seconds=600,
            ),
            "RenameRoleResponse", "scene",
        )
        assert rn_same.result == 7
        rn_cd = await env.signed(
            "RenameRole",
            env.protocol.message(
                "data_service.RenameRoleRequest",
                player_id=pid, seq=3, name="冷却名", cooldown_seconds=3600, hold_seconds=600,
            ),
            "RenameRoleResponse", "scene",
        )
        assert rn_cd.result == 4 and rn_cd.next_allowed_ms > rn1.renamed_ms
        rn_taken = await env.signed(
            "RenameRole",
            env.protocol.message(
                "data_service.RenameRoleRequest",
                player_id=pid, seq=4, name="别家名", cooldown_seconds=0, hold_seconds=600,
            ),
            "RenameRoleResponse", "scene",
        )
        assert rn_taken.result == 2
        rn_inv = await env.signed(
            "RenameRole",
            env.protocol.message(
                "data_service.RenameRoleRequest",
                player_id=pid, seq=5, name="GM违规", cooldown_seconds=0, hold_seconds=600,
            ),
            "RenameRoleResponse", "scene",
        )
        assert rn_inv.result == 3
        rn_missing = await env.signed(
            "RenameRole",
            env.protocol.message("data_service.RenameRoleRequest", player_id=888888, seq=1, name="无主名"),
            "RenameRoleResponse", "scene",
        )
        assert rn_missing.result == 8

        # RemoveAccountRoles 删角并释放名字
        await env.signed(
            "RemoveAccountRoles",
            env.protocol.message(
                "data_service.RemoveAccountRolesRequest",
                account="acct-ops",
                roles=[env.protocol.message("data_service.AccountRoleVersion", player_id=pid, owner_revision=1)],
            ),
            "Empty", "login",
        )
        with pytest.raises(Missing):
            await env.directory.lookup_role(pid)
        assert await env.directory.reserve_name(await new_player(env), "新剑仙") == (0, 0)

    exercise(kind, scenario)


def test_dead_player_store_holder_read_and_release():
    async def scenario(env):
        pid = await new_player(env)
        await env.directory.create_role("acct-hold", env.role(pid, "持有者"))
        env.store.store_holders[pid] = [10, b"scene-dead-1", 3, 20]
        env.store.store_released[pid] = False

        # 未建角玩家返回 found=False
        miss = await env.signed(
            "ReadPlayerStoreHolder",
            env.protocol.message("data_service.ReadPlayerStoreHolderRequest", player_id=777777),
            "ReadPlayerStoreHolderResponse", "scene_manager",
        )
        assert miss.found is False

        # 读出三列
        got = await env.signed(
            "ReadPlayerStoreHolder",
            env.protocol.message("data_service.ReadPlayerStoreHolderRequest", player_id=pid),
            "ReadPlayerStoreHolderResponse", "scene_manager",
        )
        assert (got.found, got.holder.owner_epoch, got.holder.holder_instance, got.holder.claim_seq) == (
            True, 10, b"scene-dead-1", 3,
        )

        # seen.claim_seq 不一致（模拟读后重新认领）-> FAILED_PRECONDITION
        wrong_seen = env.protocol.message(
            "data_service.PlayerStoreHolder", owner_epoch=10, holder_instance=b"scene-dead-1", claim_seq=2,
        )
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await env.signed(
                "ReleaseDeadPlayerStoreHolder",
                env.protocol.message(
                    "data_service.ReleaseDeadPlayerStoreHolderRequest",
                    player_id=pid, owner_epoch=10, seen=wrong_seen,
                ),
                "Empty", "scene_manager",
            )
        assert code(err.value) == "FAILED_PRECONDITION"

        # 匹配 seen 成功释放：代次抬到 11、清空 holder_instance、置 released=True
        await env.signed(
            "ReleaseDeadPlayerStoreHolder",
            env.protocol.message(
                "data_service.ReleaseDeadPlayerStoreHolderRequest",
                player_id=pid, owner_epoch=10, seen=got.holder,
            ),
            "Empty", "scene_manager",
        )
        after = await env.signed(
            "ReadPlayerStoreHolder",
            env.protocol.message("data_service.ReadPlayerStoreHolderRequest", player_id=pid),
            "ReadPlayerStoreHolderResponse", "scene_manager",
        )
        assert (after.holder.owner_epoch, after.holder.holder_instance) == (11, b"")
        assert env.store.store_released[pid] is True

        # 重试同一 owner_epoch 幂等，不再抬代次
        await env.signed(
            "ReleaseDeadPlayerStoreHolder",
            env.protocol.message("data_service.ReleaseDeadPlayerStoreHolderRequest", player_id=pid, owner_epoch=10),
            "Empty", "scene_manager",
        )
        assert env.store.store_holders[pid][0] == 11

    exercise("memory", scenario)


def test_snapshots_txlog_and_batch_recall_admin_lifecycle():
    now = [time.time_ns() // 1_000_000]

    async def scenario(env):
        pid = await new_player(env)
        await env.directory.create_role("acct-rec", env.role(pid, "回收测试"))

        # 构造 player_snapshot（source=0 可见，source=1 不可见）
        env.store.snapshots.extend([
            {"snapshot_id": 10, "player_id": pid, "zone_id": 1, "type": 0, "created_at": 1000,
             "reason": "manual-1", "operator": "gm1", "data": b"abcd", "source": 0},
            {"snapshot_id": 11, "player_id": pid, "zone_id": 1, "type": 1, "created_at": 2000,
             "reason": "periodic", "operator": "cron", "data": b"123456", "source": 0},
            {"snapshot_id": 12, "player_id": pid, "zone_id": 1, "type": 0, "created_at": 3000,
             "reason": "scene-kafka", "operator": "scene-node", "data": b"xx", "source": 1},
        ])
        raw_snaps = await env.client.call(
            PATH + "ListPlayerSnapshots",
            env.protocol.message("data_service.ListPlayerSnapshotsRequest", player_id=pid, before_time=2500, limit=5).SerializeToString(),
        )
        snaps = env.protocol.parse("data_service.ListPlayerSnapshotsResponse", raw_snaps)
        assert snaps.error_code == 0 and [s.snapshot_id for s in snaps.snapshots] == [11, 10]
        assert snaps.snapshots[0].data_size_bytes == 6

        # 构造已提交资产账本（correlation_id=5001 在 TRADE_CREDIT 流 seq=1 上 APPLIED）
        db_msg = env.protocol.message("player_database")
        stream = db_msg.asset_op_ledger.streams.add()
        stream.stream = 4  # ASSET_OP_STREAM_TRADE_CREDIT
        stream.stream_epoch = 1
        stream.watermark = 0
        stream.max_seq = 1
        stream.seen_bits.extend([1] + [0] * 15)
        stream.applied_bits.extend([1] + [0] * 15)
        receipt = stream.receipts.add()
        receipt.seq = 1
        receipt.correlation_id = 5001
        env.store.store_rows[(pid, 7)] = db_msg.SerializeToString(deterministic=True)
        env.store.store_released[pid] = True

        # 构造流水：tx 101 有账本证实（进自动回收），tx 102 账本无记录（进 needs_review: NOT_IN_LEDGER=3）
        env.store.tx_logs.extend([
            {"tx_id": 101, "timestamp": 1500, "tx_type": 2, "from_player": 0, "to_player": pid,
             "item_uuid": 0, "item_config_id": 0, "item_quantity": 0, "currency_type": 0, "currency_delta": 500,
             "balance_before": 0, "balance_after": 500, "correlation_id": 5001, "extra": ""},
            {"tx_id": 102, "timestamp": 1600, "tx_type": 2, "from_player": 0, "to_player": pid,
             "item_uuid": 0, "item_config_id": 0, "item_quantity": 0, "currency_type": 0, "currency_delta": 300,
             "balance_before": 500, "balance_after": 800, "correlation_id": 5002, "extra": ""},
        ])

        # QueryTransactionLog
        raw_q = await env.client.call(
            PATH + "QueryTransactionLog",
            env.protocol.message("data_service.QueryTransactionLogRequest", player_id=pid, time_start=1000, time_end=2000).SerializeToString(),
        )
        q = env.protocol.parse("data_service.QueryTransactionLogResponse", raw_q)
        assert q.error_code == 0 and q.total_count == 2 and [r.tx_id for r in q.rows] == [102, 101]

        # BatchRecallItems 预览（dry_run=True）
        preview = await env.admin_call(
            "BatchRecallItems",
            env.protocol.message(
                "data_service.BatchRecallItemsRequest",
                player_ids=[pid], recall_kind=1, currency_type=0, time_start=1000, time_end=2000, dry_run=True,
            ),
            "BatchRecallItemsResponse",
        )
        assert preview.error_code == 0 and len(preview.results) == 1 and preview.results[0].amount == 500
        assert len(preview.needs_review) == 1 and preview.needs_review[0].tx_id == 102 and preview.needs_review[0].reason == 3

        # RegisterRecallReviewHandled 登记 tx 102 为人工已处置（含幂等重放）
        reg_req = env.protocol.message(
            "data_service.RegisterRecallReviewHandledRequest",
            recall_kind=1, currency_type=0, player_id=pid, correlation_id=5002,
            tx_ids=[102], review_reason=3, operator="gm_ops", reason="ticket-1001",
        )
        reg1 = await env.admin_call("RegisterRecallReviewHandled", reg_req, "RegisterRecallReviewHandledResponse")
        assert (reg1.error_code, reg1.registered, reg1.source_rows) == (0, True, 1)
        reg2 = await env.admin_call("RegisterRecallReviewHandled", reg_req, "RegisterRecallReviewHandledResponse")
        assert (reg2.error_code, reg2.registered) == (0, False)

        # 重新预览：tx 102 已剔除，摘要更新
        preview2 = await env.admin_call(
            "BatchRecallItems",
            env.protocol.message(
                "data_service.BatchRecallItemsRequest",
                player_ids=[pid], recall_kind=1, currency_type=0, time_start=1000, time_end=2000, dry_run=True,
            ),
            "BatchRecallItemsResponse",
        )
        assert len(preview2.needs_review) == 0 and preview2.plan_digest != preview.plan_digest

        # 带旧摘要执行被拒（32），带新摘要冻结成功（35 PENDING）
        bad_exec = await env.admin_call(
            "BatchRecallItems",
            env.protocol.message(
                "data_service.BatchRecallItemsRequest",
                player_ids=[pid], recall_kind=1, currency_type=0, time_start=1000, time_end=2000,
                dry_run=False, request_id=7001, expected_plan_digest=preview.plan_digest,
                operator="gm_ops", reason="bug-fix",
            ),
            "BatchRecallItemsResponse",
        )
        assert bad_exec.error_code == 32
        exec_ok = await env.admin_call(
            "BatchRecallItems",
            env.protocol.message(
                "data_service.BatchRecallItemsRequest",
                player_ids=[pid], recall_kind=1, currency_type=0, time_start=1000, time_end=2000,
                dry_run=False, request_id=7001, expected_plan_digest=preview2.plan_digest,
                operator="gm_ops", reason="bug-fix",
            ),
            "BatchRecallItemsResponse",
        )
        assert exec_ok.error_code == 35 and exec_ok.total_pending == 1 and len(exec_ok.results) == 1
        op_id = exec_ok.results[0].op_id

        # GetRecallBatch 查询现状
        got_batch = await env.admin_call(
            "GetRecallBatch",
            env.protocol.message("data_service.GetRecallBatchRequest", request_id=7001),
            "BatchRecallItemsResponse",
        )
        assert got_batch.error_code == 35 and got_batch.request_id == 7001

        # 模拟指令已派发为安静 PENDING 行并用 ResolveRecallOp 按 ABORTED(4) 终结
        env.store.recall_ops[op_id]["status"] = 1  # PENDING
        env.store.recall_ops[op_id]["stream_epoch"] = 1
        env.store.recall_ops[op_id]["seq"] = 1
        env.store.recall_ops[op_id]["last_send_ms"] = now[0] - 120_000
        env.store.recall_ops[op_id]["lease_until_ms"] = 0
        refused_res = await env.admin_call(
            "ResolveRecallOp",
            env.protocol.message(
                "data_service.ResolveRecallOpRequest",
                op_id=op_id, final_status=3, operator="gm_ops", reason="wrong-verdict",
            ),
            "ResolveRecallOpResponse",
        )
        assert refused_res.error_code == 39 and refused_res.resolved is False
        ok_res = await env.admin_call(
            "ResolveRecallOp",
            env.protocol.message(
                "data_service.ResolveRecallOpRequest",
                op_id=op_id, final_status=4, operator="gm_ops", reason="quiet-abort",
            ),
            "ResolveRecallOpResponse",
        )
        assert (ok_res.error_code, ok_res.resolved, ok_res.status) == (0, True, 4)

        # CancelRecallBatch 针对新批次取消 PLANNED 行并释放来源流水
        preview3 = await env.admin_call(
            "BatchRecallItems",
            env.protocol.message(
                "data_service.BatchRecallItemsRequest",
                player_ids=[pid], recall_kind=1, currency_type=0, time_start=1000, time_end=2000, dry_run=True,
            ),
            "BatchRecallItemsResponse",
        )
        assert len(preview3.results) == 1  # ABORTED 已把 tx 101 放回
        await env.admin_call(
            "BatchRecallItems",
            env.protocol.message(
                "data_service.BatchRecallItemsRequest",
                player_ids=[pid], recall_kind=1, currency_type=0, time_start=1000, time_end=2000,
                dry_run=False, request_id=7002, expected_plan_digest=preview3.plan_digest,
                operator="gm_ops", reason="second-try",
            ),
            "BatchRecallItemsResponse",
        )
        cancelled = await env.admin_call(
            "CancelRecallBatch",
            env.protocol.message(
                "data_service.CancelRecallBatchRequest",
                request_id=7002, operator="gm_ops", reason="abort-batch",
            ),
            "BatchRecallItemsResponse",
        )
        assert cancelled.error_code == 36 and cancelled.batch_state == 2 and cancelled.cancelled_ms > 0

    exercise("memory", scenario, clock_ms=lambda: now[0])


def test_fail_closed_caller_and_admin_auth():
    async def scenario_no_admin(env):
        # 未配置 admin_token 时，所有运维 RPC 与 ReleasePlayerName 运维路径一律 fail-closed
        for method, req_name in (
            ("BatchRecallItems", "BatchRecallItemsRequest"),
            ("GetRecallBatch", "GetRecallBatchRequest"),
            ("CancelRecallBatch", "CancelRecallBatchRequest"),
            ("ResolveRecallOp", "ResolveRecallOpRequest"),
            ("RegisterRecallReviewHandled", "RegisterRecallReviewHandledRequest"),
        ):
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await env.admin_call(method, env.protocol.message(f"data_service.{req_name}"), "BatchRecallItemsResponse", token="any")
            assert code(err.value) == "PERMISSION_DENIED" and "error_code=23" in err.value.details()

        # 严格调用方角色隔离（错 caller 一律 PERMISSION_DENIED）
        wrong_pairs = [
            ("GetAccountRealName", "GetAccountRealNameRequest", "GetAccountRealNameResponse", "login"),
            ("SetAccountRealName", "SetAccountRealNameRequest", "SetAccountRealNameResponse", "payment"),
            ("ResetAccountRealName", "ResetAccountRealNameRequest", "ResetAccountRealNameResponse", "payment"),
            ("RequestAccountDeletion", "RequestAccountDeletionRequest", "AccountDeletionResponse", "trade"),
            ("CancelAccountDeletion", "CancelAccountDeletionRequest", "AccountDeletionResponse", "scene"),
            ("ClaimAccountDeletions", "ClaimAccountDeletionsRequest", "ClaimAccountDeletionsResponse", "scene"),
            ("FinishAccountDeletion", "FinishAccountDeletionRequest", "FinishAccountDeletionResponse", "scene"),
            ("BeginAccountRoleTrade", "BeginAccountRoleTradeRequest", "AccountRoleTradeReceipt", "login"),
            ("ConfirmAccountRoleTradeFreeze", "ConfirmAccountRoleTradeFreezeRequest", "AccountRoleTradeReceipt", "login"),
            ("CancelAccountRoleTrade", "CancelAccountRoleTradeRequest", "AccountRoleTradeReceipt", "scene"),
            ("TransferAccountRole", "TransferAccountRoleRequest", "AccountRoleTransferReceipt", "login"),
            ("CheckAccountRoleSlot", "CheckAccountRoleSlotRequest", "CheckAccountRoleSlotResponse", "login"),
            ("AckRoleTradeDrain", "AckRoleTradeDrainRequest", "Empty", "trade"),
            ("RenameRole", "RenameRoleRequest", "RenameRoleResponse", "login"),
            ("UpdateRoleAppearance", "UpdateRoleAppearanceRequest", "UpdateRoleAppearanceResponse", "login"),
            ("UpsertAccountChannelIdentity", "UpsertAccountChannelIdentityRequest", "Empty", "scene"),
            ("RepairAccountRole", "RepairAccountRoleRequest", "AccountDirectoryResponse", "scene"),
            ("RemoveAccountRoles", "RemoveAccountRolesRequest", "Empty", "trade"),
            ("ReadPlayerStoreHolder", "ReadPlayerStoreHolderRequest", "ReadPlayerStoreHolderResponse", "scene"),
            ("ReleaseDeadPlayerStoreHolder", "ReleaseDeadPlayerStoreHolderRequest", "Empty", "login"),
        ]
        for method, req_name, resp_name, wrong_caller in wrong_pairs:
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await env.signed(method, env.protocol.message(f"data_service.{req_name}"), resp_name, wrong_caller)
            assert code(err.value) == "PERMISSION_DENIED", f"{method} accepted {wrong_caller}"

    exercise("memory", scenario_no_admin, admin_token="")

    async def scenario_admin_token(env):
        # 错 token 被拒；正确 token 可走 ReleasePlayerName 运维路径（不受时间窗限制）
        pid = await new_player(env)
        assert await env.directory.reserve_name(pid, "孤儿名") == (0, 0)
        env.store.names["孤儿名"]["created"] = 1  # 远早于释放时间窗
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await env.admin_call(
                "ReleasePlayerName",
                env.protocol.message("data_service.ReleasePlayerNameRequest", player_id=pid, name="孤儿名"),
                "Empty", token="wrong-token",
            )
        assert code(err.value) == "PERMISSION_DENIED"
        await env.admin_call(
            "ReleasePlayerName",
            env.protocol.message("data_service.ReleasePlayerNameRequest", player_id=pid, name="孤儿名"),
            "Empty", token=ADMIN_TOKEN,
        )
        assert "孤儿名" not in env.store.names

    exercise("memory", scenario_admin_token, admin_token=ADMIN_TOKEN)
