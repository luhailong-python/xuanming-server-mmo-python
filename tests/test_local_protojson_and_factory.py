"""本地 ProtoJSON 存档（v2）与条件存储工厂测试。"""

import asyncio
import json
import os

import pytest

from xuanming.data import (
    DataServicePlayerStore,
    JsonPlayerStore,
    PlayerRowCodec,
    PlayerRowKind,
    StoreUnavailable,
    create_player_store,
)
from xuanming.protocol import Protocol
from xuanming.settings import Settings


def _build_sample_player(protocol: Protocol, player_id: int, *, name: str = "玄冥剑仙"):
    player = protocol.message("player_database", player_id=player_id, persist_schema_gen=1)
    player.level_component.level = 35
    player.currency.values.extend([1000, 250])
    player.client_request_ledger.next_seq = 12
    player.client_request_ledger.forge_revision = 4
    item = player.bag_component.items.add(item_uuid=9001, config_id=1001, stack_size=3, pos=2, bag_type=1)
    item.acquire_seq = 7
    player.bag_component.capacities.append(64)
    player.profile_component.name = name
    player.profile_component.gender = 1
    player.transform.location.x = 12.5
    player.transform.location.y = 3.25
    player.transform.location.z = -8.0
    player.attribute_component.active_scheme_id = 2
    player.uint64_pb_component.registration_timestamp = 1_700_000_000_000
    return player


def test_protojson_roundtrip_and_human_readable_disk_format(tmp_path):
    async def exercise():
        protocol = Protocol()
        codec = PlayerRowCodec(protocol)
        path = tmp_path / "data" / "players.json"
        store = JsonPlayerStore(path, protocol=protocol)

        player = _build_sample_player(protocol, 42, name="玄冥剑仙")
        rows = codec.project(player)

        snapshot = await store.load_and_claim(42, b"local-scene-1", 1)
        saved = await store.commit(snapshot, rows)
        assert saved.save_seq == 1
        assert saved.rows == rows

        raw_text = path.read_text(encoding="utf-8")
        assert "\n" in raw_text
        assert "玄冥剑仙" in raw_text
        document = json.loads(raw_text)
        assert document["format_version"] == 2
        record = document["players"]["42"]
        assert "rows_json" in record and "rows" not in record and "rows_raw" not in record
        rows_json = record["rows_json"]
        assert set(rows_json) == {kind.name for kind in PlayerRowKind}
        assert "currency" in rows_json["WALLET"]
        assert "client_request_ledger" in rows_json["WALLET"]
        assert "bag_component" in rows_json["BAG"]
        assert "level_component" in rows_json["CHARACTER"]
        assert "profile_component" in rows_json["PROFILE"]
        assert rows_json["PROFILE"]["profile_component"]["name"] == "玄冥剑仙"
        assert "attribute_component" in rows_json["GROWTH"]
        assert "transform" in rows_json["LOCATION"]

        reloaded_store = JsonPlayerStore(path, protocol=protocol, auto_create=False)
        reloaded = await reloaded_store.load_and_claim(42, b"local-scene-2")
        assert reloaded.owner_epoch == 2
        assert reloaded.save_seq == 1
        assert reloaded.rows == rows
        assert codec.assemble(reloaded) == player

    asyncio.run(exercise())


def test_v2_store_reads_legacy_v1_base64_and_upgrades_on_write(tmp_path):
    async def exercise():
        protocol = Protocol()
        codec = PlayerRowCodec(protocol)
        path = tmp_path / "players.json"

        legacy_store = JsonPlayerStore(path)
        player_a = _build_sample_player(protocol, 101, name="老档玩家甲")
        player_b = _build_sample_player(protocol, 102, name="老档玩家乙")
        rows_a = codec.project(player_a)
        rows_b = codec.project(player_b)

        snap_a = await legacy_store.load_and_claim(101, b"scene-old", 3)
        await legacy_store.commit(snap_a, rows_a)
        snap_b = await legacy_store.load_and_claim(102, b"scene-old", 5)
        await legacy_store.commit(snap_b, rows_b)

        legacy_doc = json.loads(path.read_text(encoding="utf-8"))
        assert legacy_doc["format_version"] == 1
        assert "rows" in legacy_doc["players"]["101"]

        v2_store = JsonPlayerStore(path, protocol=protocol, auto_create=False)
        claimed_a = await v2_store.load_and_claim(101, b"scene-v2")
        assert claimed_a.owner_epoch == 4
        assert claimed_a.rows == rows_a
        assert codec.assemble(claimed_a) == player_a

        upgraded_doc = json.loads(path.read_text(encoding="utf-8"))
        assert upgraded_doc["format_version"] == 2
        for pid in ("101", "102"):
            assert "rows_json" in upgraded_doc["players"][pid]
            assert "rows" not in upgraded_doc["players"][pid]

        claimed_b = await v2_store.load_and_claim(102, b"scene-v2")
        assert claimed_b.owner_epoch == 6
        assert claimed_b.rows == rows_b
        assert codec.assemble(claimed_b) == player_b

    asyncio.run(exercise())


def test_v2_store_rejects_corrupt_or_unknown_protojson_fields(tmp_path):
    async def exercise():
        protocol = Protocol()
        codec = PlayerRowCodec(protocol)
        path = tmp_path / "players.json"
        store = JsonPlayerStore(path, protocol=protocol)
        snapshot = await store.load_and_claim(7, b"scene-1", 1)
        await store.commit(snapshot, codec.project(_build_sample_player(protocol, 7)))

        # v1 存储（无 protocol）不得静默解析 v2 ProtoJSON 文档。
        v1_store = JsonPlayerStore(path, auto_create=False)
        with pytest.raises(StoreUnavailable):
            await v1_store.load_and_claim(7, b"scene-2")

        # 未知 proto 字段必须 fail-closed 抛出 StoreUnavailable。
        doc = json.loads(path.read_text(encoding="utf-8"))
        doc["players"]["7"]["rows_json"]["WALLET"]["nonexistent_proto_field"] = 123
        path.write_text(json.dumps(doc), encoding="utf-8")
        with pytest.raises(StoreUnavailable):
            await JsonPlayerStore(path, protocol=protocol, auto_create=False).load_and_claim(7, b"scene-3")

        # 非法行分组名必须 fail-closed 抛出 StoreUnavailable。
        doc = json.loads(path.read_text(encoding="utf-8"))
        del doc["players"]["7"]["rows_json"]["WALLET"]["nonexistent_proto_field"]
        doc["players"]["7"]["rows_json"]["UNKNOWN_ROW_KIND"] = {}
        path.write_text(json.dumps(doc), encoding="utf-8")
        with pytest.raises(StoreUnavailable):
            await JsonPlayerStore(path, protocol=protocol, auto_create=False).load_and_claim(7, b"scene-4")

        # 损坏 JSON 必须抛出 StoreUnavailable。
        path.write_text("{broken json", encoding="utf-8")
        with pytest.raises(StoreUnavailable):
            await JsonPlayerStore(path, protocol=protocol, auto_create=False).load_and_claim(7, b"scene-5")

    asyncio.run(exercise())


def test_json_store_retries_windows_permission_error_on_replace(tmp_path, monkeypatch):
    async def exercise():
        protocol = Protocol()
        codec = PlayerRowCodec(protocol)
        path = tmp_path / "players.json"
        store = JsonPlayerStore(path, protocol=protocol)
        monkeypatch.setattr(JsonPlayerStore, "_REPLACE_RETRY_BASE_SECONDS", 0.001)

        real_replace = os.replace
        failures_remaining = {"count": 2}

        def flaky_replace(src, dst):
            if failures_remaining["count"] > 0:
                failures_remaining["count"] -= 1
                raise PermissionError("simulated antivirus file lock")
            return real_replace(src, dst)

        monkeypatch.setattr(os, "replace", flaky_replace)

        snapshot = await store.load_and_claim(9, b"scene-1", 1)
        saved = await store.commit(snapshot, codec.project(_build_sample_player(protocol, 9)))
        assert saved.save_seq == 1
        assert failures_remaining["count"] == 0
        assert not list(tmp_path.glob("*.tmp"))

    asyncio.run(exercise())


def test_create_player_store_local_mode(tmp_path):
    async def exercise():
        protocol = Protocol()
        codec = PlayerRowCodec(protocol)

        # 未显式指定 local_player_store 时，默认与 database 同目录生成 players.json。
        default_settings = Settings(database=tmp_path / "run" / "players.sqlite3")
        store, closer = create_player_store(default_settings, protocol, local=True)
        assert isinstance(store, JsonPlayerStore)
        assert closer is None
        snap = await store.load_and_claim(11, b"scene-local", 1)
        await store.commit(snap, codec.project(_build_sample_player(protocol, 11)))
        expected_path = tmp_path / "run" / "players.json"
        assert expected_path.exists()
        assert json.loads(expected_path.read_text(encoding="utf-8"))["format_version"] == 2

        # 显式指定 local_player_store 时优先使用该路径。
        custom_path = tmp_path / "custom" / "my_players.json"
        custom_settings = Settings(
            database=tmp_path / "run" / "players.sqlite3",
            local_player_store=custom_path,
        )
        custom_store, custom_closer = create_player_store(custom_settings, protocol, local=True)
        assert isinstance(custom_store, JsonPlayerStore)
        assert custom_closer is None
        snap2 = await custom_store.load_and_claim(12, b"scene-local", 1)
        await custom_store.commit(snap2, codec.project(_build_sample_player(protocol, 12)))
        assert custom_path.exists()

    asyncio.run(exercise())


def test_create_player_store_cluster_mode_fail_closed_and_wired(tmp_path, monkeypatch):
    async def exercise():
        protocol = Protocol()
        monkeypatch.delenv("MMORPG_PLAYER_STORE_SECRET_SCENE", raising=False)
        created_clients = []

        class FakeGrpcClient:
            def __init__(self, endpoint, *, timeout=5, receive_limit=4 << 20):
                self.endpoint = endpoint
                self.timeout = timeout
                self.receive_limit = receive_limit
                self.closed = False
                created_clients.append(self)

            async def call(self, path, body):
                raise AssertionError(f"unexpected rpc call: {path}")

            async def close(self):
                self.closed = True

        # 1. 缺少 player_store 端点 -> fail-fast 拒启，严禁回退本地 JSON。
        with pytest.raises(ValueError, match="PlayerStore"):
            create_player_store(
                Settings(database=tmp_path / "players.sqlite3", player_store=""),
                protocol,
                local=False,
                grpc_client_factory=FakeGrpcClient,
                secret_resolver=lambda _name: b"0123456789abcdef0123456789abcdef",
            )
        assert not created_clients

        # 2. 缺少或过短密钥 -> fail-fast 拒启，不得泄漏 gRPC 连接。
        cluster_settings = Settings(
            database=tmp_path / "players.sqlite3",
            player_store="127.0.0.1:50051",
        )
        with pytest.raises(ValueError, match="MMORPG_PLAYER_STORE_SECRET_SCENE"):
            create_player_store(
                cluster_settings,
                protocol,
                local=False,
                grpc_client_factory=FakeGrpcClient,
            )
        with pytest.raises(ValueError, match="MMORPG_PLAYER_STORE_SECRET_SCENE"):
            create_player_store(
                cluster_settings,
                protocol,
                local=False,
                grpc_client_factory=FakeGrpcClient,
                secret_resolver=lambda _name: b"too-short",
            )
        with pytest.raises(ValueError, match="MMORPG_PLAYER_STORE_SECRET_SCENE"):
            create_player_store(
                cluster_settings,
                protocol,
                local=False,
                grpc_client_factory=FakeGrpcClient,
                secret_resolver=lambda _name: b" " * 40,
            )
        assert not created_clients

        # 3. 端点与 >=32 字节密钥齐全 -> 返回 DataServicePlayerStore 及关闭回调。
        store, closer = create_player_store(
            cluster_settings,
            protocol,
            local=False,
            grpc_client_factory=FakeGrpcClient,
            secret_resolver=lambda _name: "0123456789abcdef0123456789abcdef",
        )
        assert isinstance(store, DataServicePlayerStore)
        assert closer is not None
        assert len(created_clients) == 1
        assert created_clients[0].endpoint == "127.0.0.1:50051"
        assert created_clients[0].timeout == 5
        assert created_clients[0].receive_limit == (8 << 20)
        assert not created_clients[0].closed
        await closer()
        assert created_clients[0].closed

    asyncio.run(exercise())


def test_protojson_preserves_unknown_wire_fields_without_polluting_normal_rows(tmp_path):
    async def exercise():
        protocol = Protocol()
        codec = PlayerRowCodec(protocol)
        path = tmp_path / "players.json"
        store = JsonPlayerStore(path, protocol=protocol)

        player = _build_sample_player(protocol, 77, name="未知字段保真")
        scope = player.mission_component.scopes.add(scope=0, completed_mission_ids=[4])
        mission = scope.mission_list.missions[4]
        mission.id = 4
        mission.MergeFromString(b"\x98\x06\x01")
        rows = codec.project(player)

        snap = await store.load_and_claim(77, b"scene-unknown", 1)
        await store.commit(snap, rows)

        doc = json.loads(path.read_text(encoding="utf-8"))
        record = doc["players"]["77"]
        assert record["rows_json"]["PROFILE"]["profile_component"]["name"] == "未知字段保真"
        assert set(record["rows_raw"]) == {"GROWTH"}

        reloaded = await JsonPlayerStore(path, protocol=protocol, auto_create=False).load_and_claim(
            77, b"scene-unknown-2"
        )
        restored = codec.assemble(reloaded)
        restored_scope = next(s for s in restored.mission_component.scopes if not s.scope)
        assert b"\x98\x06\x01" in restored_scope.mission_list.missions[4].SerializeToString()

    asyncio.run(exercise())
