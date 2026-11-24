"""config_node 配置表查询、原子热更、鉴权与生命周期测试。"""

import asyncio
import hashlib
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp.test_utils import TestClient, TestServer

from xuanming.config_node import ConfigNodeConfig, ConfigNodeService, create_app, run
from xuanming.data.config_tables import _content_digest, _resource


def _copy_tables(dst: Path) -> Path:
    src = _resource("tables")
    shutil.copytree(src, dst)
    return dst


def _rewrite_activity_schedule(table_dir: Path, new_definition_key: str) -> None:
    manifest_path = table_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    target_entry = next(entry for entry in manifest["tables"] if entry["name"] == "ActivitySchedule")
    json_asset = next(asset for asset in target_entry["artifacts"] if asset["kind"] == "json")
    data_path = table_dir / json_asset["file"]
    data = json.loads(data_path.read_text("utf-8"))
    for row in data["data"]:
        if row.get("id") == 15:
            row["definitionKey"] = new_definition_key
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    data_path.write_bytes(raw)
    json_asset["size"] = len(raw)
    json_asset["sha256"] = hashlib.sha256(raw).hexdigest()
    manifest["version"] = int(manifest["version"]) + 1
    manifest["content_digest"] = _content_digest(manifest["tables"])
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")


def test_config_node_query_list_status_and_health():
    async def run_test():
        service = ConfigNodeService()
        app = create_app(service)
        async with TestClient(TestServer(app)) as client:
            # 未加载前健康检查返回 503。
            resp = await client.get("/actuator/health")
            assert resp.status == 503 and (await resp.json()) == {"status": "DOWN"}

            service.load_all()
            resp = await client.get("/actuator/health")
            assert resp.status == 200 and (await resp.json()) == {"status": "UP"}

            # 列出全部已加载表。
            listing = await (await client.get("/api/table")).json()
            assert listing["count"] > 0 and "ActivitySchedule" in listing["tables"]
            assert listing["loadVersion"] == 1

            # 查询状态（兼容 Java /api/table/status）。
            status = await (await client.get("/api/table/status")).json()
            assert status["loadVersion"] == 1
            assert status["lastLoadTimeMs"] >= 0
            assert status["tableCount"] == listing["count"]
            assert status["contentDigest"]

            # 按表名和主键查询单行（protobuf MessageToDict）。
            resp = await client.get("/api/table/ActivitySchedule/15")
            assert resp.status == 200
            row = await resp.json()
            assert row["id"] == 15
            assert row["definitionKey"] == "activity.mission.15.initial"

            # 不存在的表 / 不存在的行 -> 404；非整数主键 -> 400。
            assert (await client.get("/api/table/ActivitySchedule/99999999")).status == 404
            assert (await client.get("/api/table/NoSuchTable/1")).status == 404
            assert (await client.get("/api/table/ActivitySchedule/not-an-int")).status == 400

            # 多键表查询不抛 500。
            assert (await client.get("/api/table/TestMultiKey/99999999")).status in (200, 404)

    asyncio.run(run_test())


def test_config_node_atomic_reload_and_failure_preservation(tmp_path):
    table_dir = _copy_tables(tmp_path / "tables")

    async def run_test():
        service = ConfigNodeService(table_dir)
        service.load_all()
        app = create_app(service, reload_api_key="reload-secret-key")
        headers = {"X-Admin-Key": "reload-secret-key"}

        async with TestClient(TestServer(app)) as client:
            # 1. 鉴权失败返回 401。
            assert (await client.post("/api/reload")).status == 401
            assert (await client.post("/api/reload", headers={"X-Admin-Key": "wrong"})).status == 401

            # 2. 更新表内容与清单后原子热更成功，版本号递增且立即可见。
            _rewrite_activity_schedule(table_dir, "activity.mission.15.hotfixed")
            resp = await client.post("/api/reload", headers=headers)
            assert resp.status == 200
            body = await resp.json()
            assert body["status"] == "ok" and body["loadVersion"] == 2
            row = await (await client.get("/api/table/ActivitySchedule/15")).json()
            assert row["definitionKey"] == "activity.mission.15.hotfixed"

            # 3. 破坏 manifest.json 后热更失败返回 500，但旧快照与版本号保持完整。
            (table_dir / "manifest.json").write_text("{corrupt", encoding="utf-8")
            resp = await client.post("/api/reload", headers=headers)
            assert resp.status == 500
            assert (await resp.json())["status"] == "error"
            status = await (await client.get("/api/table/status")).json()
            assert status["loadVersion"] == 2
            row = await (await client.get("/api/table/ActivitySchedule/15")).json()
            assert row["definitionKey"] == "activity.mission.15.hotfixed"

    asyncio.run(run_test())


def test_config_node_reload_loopback_guard_when_no_api_key():
    async def run_test():
        service = ConfigNodeService()
        service.load_all()
        app = create_app(service, reload_api_key="")
        async with TestClient(TestServer(app)) as client:
            # 经代理转发头 X-Forwarded-For 拒绝热更 -> 403。
            resp = await client.post("/api/reload", headers={"X-Forwarded-For": "203.0.113.5"})
            assert resp.status == 403 and (await resp.json()) == {"status": "forbidden"}
            # 直连本机回环放行 -> 200。
            resp = await client.post("/api/reload")
            assert resp.status == 200 and (await resp.json())["status"] == "ok"

    asyncio.run(run_test())


def test_config_node_config_loading_and_validation():
    default_cfg = ConfigNodeConfig.from_settings(None, environ={})
    assert default_cfg.host == "127.0.0.1" and default_cfg.port == 8090
    assert default_cfg.table_dir is None and default_cfg.format == "json" and default_cfg.reload_api_key == ""

    settings = SimpleNamespace(config_node={
        "host": "0.0.0.0",
        "port": 8095,
        "table_dir": "custom/tables",
        "format": "pb",
        "reload_api_key": "from-toml",
    })
    cfg = ConfigNodeConfig.from_settings(settings, environ={})
    assert cfg.host == "0.0.0.0" and cfg.port == 8095
    assert cfg.table_dir == Path("custom/tables") and cfg.format == "pb" and cfg.reload_api_key == "from-toml"

    env_cfg = ConfigNodeConfig.from_settings(
        settings,
        environ={
            "XUANMING_CONFIG_NODE_HOST": "127.0.0.1",
            "XUANMING_CONFIG_NODE_PORT": "9090",
            "XUANMING_CONFIG_TABLE_DIR": "env/tables",
            "XUANMING_CONFIG_TABLE_FORMAT": "json",
            "XUANMING_CONFIG_RELOAD_API_KEY": "from-env",
        },
    )
    assert env_cfg.host == "127.0.0.1" and env_cfg.port == 9090
    assert env_cfg.table_dir == Path("env/tables") and env_cfg.format == "json"
    assert env_cfg.reload_api_key == "from-env"

    with pytest.raises(ValueError):
        ConfigNodeConfig.from_settings({"config_node": {"port": 70000}}, environ={})
    with pytest.raises(ValueError):
        ConfigNodeConfig.from_settings({"config_node": {"format": "xml"}}, environ={})


def test_config_node_runner_starts_serves_and_shuts_down_gracefully():
    async def run_test():
        ready = asyncio.Event()
        stop = asyncio.Event()
        bound = {}

        def on_ready(port, service):
            bound["port"] = port
            bound["service"] = service
            ready.set()

        task = asyncio.create_task(
            run({"config_node": {"host": "127.0.0.1", "port": 0}}, stop_event=stop, on_ready=on_ready)
        )
        try:
            await asyncio.wait_for(ready.wait(), timeout=5)
            base = f"http://127.0.0.1:{bound['port']}"
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{base}/api/table/status") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["loadVersion"] == 1 and data["tableCount"] > 0
                async with session.get(f"{base}/api/table/ActivitySchedule/15") as resp:
                    assert resp.status == 200
                    assert (await resp.json())["id"] == 15
        finally:
            stop.set()
            await asyncio.wait_for(task, timeout=5)

    asyncio.run(run_test())
