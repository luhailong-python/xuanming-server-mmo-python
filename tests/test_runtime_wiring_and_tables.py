"""配置表同步、论剑配表加载、Login 运行时装配与 PlayerLocator 多区路由测试。"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from xuanming.cluster import LocalDirectory
from xuanming.data import ConfigTables
from xuanming.kill_switch import KillSwitch
from xuanming.login.account_deletion import AccountDeletionService
from xuanming.login.anti_addiction import AntiAddictionService
from xuanming.login.caller_auth import CallerVerifier
from xuanming.login.config import LoginConfig
from xuanming.login.password_auth import (
    CompositePasswordProvider,
    ProductionPasswordProvider,
    SqlPasswordAuthStore,
    hash_password,
    parse_mysql_dsn,
)
from xuanming.login.queue import LoginQueue
from xuanming.login.service import LoginService
from xuanming.match.ranked import (
    OFFICIAL_RANKED_PRESETS,
    RankedManager,
    RankedPresetSpec,
    RankedRuleConfig,
    load_ranked_presets,
)
from xuanming.nodes.battle_tables import battle_table_fingerprint
from xuanming.ops.schemamigrate import parse_schema_file
from xuanming.player_locator import (
    SCENE_MANAGER_NODE_TYPE,
    MemorySessionStore,
    PlayerLocatorConfig,
    RoutingSceneManagerClient,
    parse_scene_manager_endpoints,
)
from xuanming.player_locator.lease_monitor import notify_scene_manager_leave
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[1]


# -- 1. ConfigTables 同步与论剑排位配表加载 -----------------------------------------------------------


def test_config_tables_loads_synced_tables_with_json_pb_parity():
    tables_json = ConfigTables(fmt="json")
    tables_pb = ConfigTables(fmt="pb")

    assert tables_json.content_digest == tables_pb.content_digest
    expected_new = (
        "AfkPassRule",
        "AppearanceRule",
        "AppearanceSeries",
        "CharacterAppearance",
        "EquipArtifact",
        "EquipV1Affix",
        "PetEntry",
        "PetTier",
        "RankedPreset",
        "RankedRule",
        "SeasonRule",
        "SeasonSeal",
    )
    for name in expected_new:
        assert name in tables_json.tables, f"missing table {name}"
        assert len(tables_json[name]) > 0
        assert tables_json[name].rows() == tables_pb[name].rows()

    tip_text = json.loads((tables_json.directory / "tip_text.json").read_text(encoding="utf-8"))
    assert tip_text

    # 战斗/场景表指纹与最新配表基线严格一致（JSON/PB 双格式逐字节同值）。
    assert battle_table_fingerprint(tables_json) == "c51ba644116ac1ee8bdd8a2ed1fc055e"
    assert battle_table_fingerprint(tables_pb) == "c51ba644116ac1ee8bdd8a2ed1fc055e"


def test_ranked_rule_and_presets_load_from_config_tables_with_fallback():
    tables = ConfigTables()
    rule = RankedRuleConfig.from_config_tables(tables)
    assert rule == RankedRuleConfig()
    assert rule.rule_version == 2
    assert rule.battle_config_id == 5
    assert rule.season_duration_days == 56
    assert rule.tier_wushuang_centi == 185_000

    presets = load_ranked_presets(tables)
    assert presets == OFFICIAL_RANKED_PRESETS
    assert len(presets) == 6

    # 缺失行或缺失表时平滑回退内置默认值。
    assert RankedRuleConfig.from_config_tables(tables, rule_id=99999) == RankedRuleConfig()
    empty_tables = SimpleNamespace(tables={})
    assert RankedRuleConfig.from_config_tables(empty_tables) == RankedRuleConfig()
    assert RankedPresetSpec.load_from_config_tables(empty_tables) == OFFICIAL_RANKED_PRESETS

    manager = RankedManager(Protocol(), store=None, tables=tables)
    assert manager.rule == rule
    assert manager.presets == presets
    assert manager.is_valid_preset_pair(101, 203)
    assert not manager.is_valid_preset_pair(999, 203)


# -- 2. Login Schema SQL、SqlPasswordAuthStore 与 Runtime._login 装配 ---------------------------------


def test_login_schema_sql_valid_for_schemamigrate():
    plan = parse_schema_file(REPO_ROOT / "config" / "login_schema.sql")
    assert plan.db_name == "login"
    assert len(plan.tables) == 1
    table = plan.tables[0]
    assert table.table_name == "user_accounts"
    assert table.primary_key == ("account",)
    assert [col.name for col in table.columns] == ["account", "password"]
    assert table.idempotent_sql.upper().startswith("CREATE TABLE IF NOT EXISTS ")


def test_parse_mysql_dsn_and_sql_password_auth_store():
    url_params = parse_mysql_dsn("mysql://login_ro:p%40ss@10.0.0.5:3307/mmorpg")
    assert url_params == {
        "host": "10.0.0.5",
        "port": 3307,
        "user": "login_ro",
        "password": "p@ss",
        "database": "mmorpg",
    }
    go_params = parse_mysql_dsn("login_ro:secret@tcp(tidb.local:4000)/mmorpg?charset=utf8mb4")
    assert go_params == {
        "host": "tidb.local",
        "port": 4000,
        "user": "login_ro",
        "password": "secret",
        "database": "mmorpg",
    }
    with pytest.raises(ValueError):
        parse_mysql_dsn("postgres://localhost/db")

    class _FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def execute(self, sql, params):
            assert "user_accounts" in sql
            self._account = params[0]

        async def fetchall(self):
            return self._rows.get(self._account, [])

    class _FakeConn:
        def __init__(self, rows):
            self._rows = rows

        def cursor(self):
            return _FakeCursor(self._rows)

    class _FakeAcquire:
        def __init__(self, rows):
            self._rows = rows

        async def __aenter__(self):
            return _FakeConn(self._rows)

        async def __aexit__(self, *args):
            return None

    class _FakePool:
        def __init__(self, rows):
            self._rows = rows

        def acquire(self):
            return _FakeAcquire(self._rows)

    encoded = hash_password("s3cr3t-pass", ln=10)
    pool = _FakePool({"Alice": [("Alice", encoded)], "dup": [("dup", encoded), ("dup2", encoded)]})
    store = SqlPasswordAuthStore(pool)

    async def exercise():
        assert await store.lookup_password_hash("Alice") == ("Alice", encoded, True)
        assert await store.lookup_password_hash("missing") == ("", "", False)
        with pytest.raises(RuntimeError, match="duplicate"):
            await store.lookup_password_hash("dup")

    asyncio.run(exercise())


def test_login_runtime_wiring_context_merged_zones_and_kill_switch():
    protocol = Protocol()
    secret_bytes = b"k" * 32
    config = LoginConfig(
        zone_id=2,
        gate_token_secret=secret_bytes,
        directory_secret=secret_bytes,
        dev_password_secret="dev-pass",
        queue_enabled=True,
    )
    fake_redis = object()
    fake_control = object()
    fake_cg_call = object()
    fake_pool = object()

    merged_calls = {"count": 0, "fail": False}

    async def data_call(path: str, payload: bytes, timeout=None) -> bytes:
        assert path == "/data_service.DataService/GetMergedZones"
        merged_calls["count"] += 1
        if merged_calls["fail"]:
            raise RuntimeError("transient data_service error")
        return protocol.message(
            "data_service.GetMergedZonesResponse",
            merged_into={2: 1, 3: 2},
        ).SerializeToString()

    async def unused_call(*args, **kwargs):
        return b""

    async def publish(*args, **kwargs):
        return None

    context = Runtime._build_login_context(
        protocol,
        config,
        login_redis=fake_redis,
        player_redis=fake_redis,
        locator_call=unused_call,
        data_call=data_call,
        scene_manager_call=unused_call,
        publish=publish,
        directory=LocalDirectory(),
        control_redis=fake_control,
        content_guard_call=fake_cg_call,
        password_auth_pool=fake_pool,
        environ={
            "LOGIN_ANTI_ADDICTION_EXTRA_PLAY_DAYS": "2026-10-01,2026-10-02",
            "LOGIN_ANTI_ADDICTION_PLATFORM_MANAGED": "apple,wechat",
        },
    )
    assert context.control_redis is fake_control
    assert isinstance(context.login_queue, LoginQueue)
    assert context.login_queue.redis is fake_control
    assert isinstance(context.anti_addiction, AntiAddictionService)
    assert not context.anti_addiction.self_managed("apple")
    assert context.anti_addiction.self_managed("password")
    assert isinstance(context.account_deletion, AccountDeletionService)
    assert context.account_deletion_service() is context.account_deletion
    assert context.content_guard_call is fake_cg_call
    assert isinstance(context.password_provider, CompositePasswordProvider)

    # 无开发密码密钥时仅装配 ProductionPasswordProvider。
    prod_ctx = Runtime._build_login_context(
        protocol,
        replace(config, dev_password_secret="", queue_enabled=False),
        login_redis=fake_redis,
        player_redis=fake_redis,
        locator_call=unused_call,
        data_call=data_call,
        scene_manager_call=unused_call,
        publish=publish,
        directory=LocalDirectory(),
        password_auth_pool=fake_pool,
    )
    assert prod_ctx.login_queue is None
    assert isinstance(prod_ctx.password_provider, ProductionPasswordProvider)

    # ContentGuard 端点解析：环境变量优先，回退 content_guard_node 配置。
    settings = Settings(content_guard_node={"advertise_host": "10.0.1.8", "grpc_port": 50600})
    assert Runtime._resolve_login_content_guard_endpoint(settings, {}) == "10.0.1.8:50600"
    assert Runtime._resolve_login_content_guard_endpoint(
        settings, {"LOGIN_CONTENT_GUARD": "cg-override:50601"},
    ) == "cg-override:50601"

    # 合服映射刷新：正常更新 + 瞬态失败容忍保留旧映射。
    async def exercise_refresh():
        assert await Runtime._refresh_merged_zones_once(context) is True
        assert context.merged_zones == {2: 1, 3: 2}
        assert context.resolve_target_zone(3) == 1
        merged_calls["fail"] = True
        assert await Runtime._refresh_merged_zones_once(context) is False
        assert context.merged_zones == {2: 1, 3: 2}

    asyncio.run(exercise_refresh())

    # KillSwitch 挂载到 LoginService。
    node_info = protocol.message("NodeInfo", node_id=1, node_type=5, zone_id=2, node_uuid="login-test-uuid")
    ks = KillSwitch(protocol, node_info)
    svc = LoginService(context, CallerVerifier({}), kill_switch=ks)
    assert svc.kill_switch is ks


# -- 3. PlayerLocator 多区 SceneManager 路由 ---------------------------------------------------------


def test_player_locator_multi_zone_scene_manager_routing(monkeypatch):
    protocol = Protocol()
    monkeypatch.setenv("PLAYER_LOCATOR_SCENE_MANAGER_ENDPOINTS", "1=sm1:60000,2=sm2:60000")
    cfg = PlayerLocatorConfig.from_settings(Settings())
    assert cfg.scene_manager_endpoint_map() == {1: "sm1:60000", 2: "sm2:60000"}
    assert parse_scene_manager_endpoints({3: "sm3:60000"}) == ((3, "sm3:60000"),)
    with pytest.raises(ValueError):
        parse_scene_manager_endpoints("bad-entry")

    created: dict[str, _FakeGrpcClient] = {}

    class _FakeGrpcClient:
        def __init__(self, address: str, timeout: float) -> None:
            self.address = address
            self.timeout = timeout
            self.calls: list[tuple[str, bytes]] = []
            self.closed = False

        async def call(self, path: str, payload: bytes, *, timeout=None) -> bytes:
            self.calls.append((path, payload))
            if path.endswith("/EnterScene"):
                return protocol.message("scene_manager.EnterSceneResponse", error_code=0).SerializeToString()
            return b""

        async def close(self) -> None:
            self.closed = True

    def factory(address: str, timeout: float):
        client = _FakeGrpcClient(address, timeout)
        created[address] = client
        return client

    directory = LocalDirectory()
    sm3_info = protocol.message(
        "NodeInfo",
        node_id=301,
        node_type=SCENE_MANAGER_NODE_TYPE,
        zone_id=3,
        launch_time=200,
        grpc_endpoint={"ip": "10.0.3.10", "port": 60003},
    )
    directory.publish(sm3_info)

    router = RoutingSceneManagerClient(
        protocol,
        default_endpoint="sm-default:60000",
        zone_endpoints=cfg.scene_manager_endpoint_map(),
        directory=directory,
        timeout_sec=4.0,
        client_factory=factory,
    )

    async def exercise():
        assert await router.resolve_endpoint(1) == "sm1:60000"
        assert await router.resolve_endpoint(2) == "sm2:60000"
        assert await router.resolve_endpoint(3) == "10.0.3.10:60003"
        assert await router.resolve_endpoint(99) == "sm-default:60000"

        # EnterScene 按 gate_zone_id 路由到不同区 SceneManager，并复用同一地址缓存客户端。
        await router.enter_scene(
            player_id=1001, session_id=11, login_generation=1, owner_revision=1, gate_zone_id=1,
        )
        await router.enter_scene(
            player_id=1002, session_id=12, login_generation=1, owner_revision=1, gate_zone_id=2,
        )
        await router.enter_scene(
            player_id=1003, session_id=13, login_generation=1, owner_revision=1, gate_zone_id=3,
        )
        await router.enter_scene(
            player_id=1004, session_id=14, login_generation=1, owner_revision=1, gate_zone_id=99,
        )
        assert len(created["sm1:60000"].calls) == 1
        assert len(created["sm2:60000"].calls) == 1
        assert len(created["10.0.3.10:60003"].calls) == 1
        assert len(created["sm-default:60000"].calls) == 1

        # notify_scene_manager_leave 优先按权威 PlayerLocation.zone_id 路由（跨区游历场景）。
        store = MemorySessionStore()
        loc = protocol.message("storage.PlayerLocation", zone_id=2, scene_id=502, node_id="scene-2")
        store.set_location_bytes(2001, loc.SerializeToString())
        session = protocol.message(
            "playerlocator.PlayerSession",
            player_id=2001,
            session_id=21,
            login_generation=5,
            gate_zone_id=1,
            scene_id=502,
        )
        await notify_scene_manager_leave(protocol, store, router, session)
        assert len(created["sm2:60000"].calls) == 2
        path, body = created["sm2:60000"].calls[-1]
        assert path == "/scene_manager.SceneManager/LeaveScene"
        req = protocol.parse("scene_manager.LeaveSceneRequest", body)
        assert req.player_id == 2001 and req.scene_id == 502

        # 无 location 时回退 session.gate_zone_id 路由。
        session_fallback = protocol.message(
            "playerlocator.PlayerSession",
            player_id=2002,
            session_id=22,
            login_generation=6,
            gate_zone_id=1,
            scene_id=501,
        )
        await notify_scene_manager_leave(protocol, store, router, session_fallback)
        assert len(created["sm1:60000"].calls) == 2

        # close 回收全部懒缓存客户端。
        cached_clients = list(created.values())
        await router.close()
        assert all(client.closed for client in cached_clients)

    asyncio.run(exercise())
