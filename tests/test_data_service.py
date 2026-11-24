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

SECRETS = {"login": b"login-directory-secret-0123456789", "scene": b"scene-directory-secret-0123456789",
           "scene_manager": b"scene-manager-secret-0123456789ab", "db": b"db-role-fence-secret-0123456789ab",
           "player_inbox": b"inbox-role-fence-secret-012345678"}
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
            "holder_instance VARBINARY(64) NOT NULL, released INT UNSIGNED NOT NULL, payload MEDIUMBLOB NOT NULL,"
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
async def running(kind):
    protocol = Protocol()
    async with AsyncExitStack() as stack:
        store = await sql_store(protocol, stack) if kind == "sql" else MemoryStore(protocol)
        service = DataService(protocol, store, SECRETS, zone_refresh=0.05)
        await service.start()
        stack.push_async_callback(service.close)
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        stack.push_async_callback(server.stop, 0)
        client = GrpcClient(f"127.0.0.1:{port}")
        stack.push_async_callback(client.close)
        env = SimpleNamespace(protocol=protocol, store=store, client=client, kind=kind,
                              directory=Directory(protocol, client.call, SECRETS["login"]))

        def role(pid, name="", **fields):
            values = dict(player_id=pid, class_id=1, gender=1, zone_id=1, name=name, appearance_id="01_hero")
            return protocol.message("AccountSimplePlayer", **(values | fields))

        async def signed(method, request, response, caller="scene", secret=None):
            sign_service_call(PATH + method, request, caller, time.time_ns() // 1_000_000,
                              secret or SECRETS[caller])
            raw = await client.call(PATH + method, request.SerializeToString(deterministic=True))
            return protocol.parse(f"data_service.{response}", raw)

        env.role, env.signed = role, signed
        yield env


def exercise(kind, scenario):
    async def main():
        async with running(kind) as env:
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
        for method in ("RepairAccountRole", "RenameRole"):
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await env.client.call(PATH + method, b"")
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
