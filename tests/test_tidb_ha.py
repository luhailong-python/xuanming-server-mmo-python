import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from datetime import datetime, timezone

import pytest


def module():
    path = Path(__file__).resolve().parents[1] / "tools/tidb_ha/run.py"
    spec = importlib.util.spec_from_file_location("tidb_ha_runner", path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def test_ha_compose_topology_is_isolated_and_replicated():
    ha = module()
    compose = ha.compose_text("abc", "10.248.174.0/24")
    for name in ("pd1", "pd2", "pd3", "tikv1", "tikv2", "tikv3", "tidb1", "tidb2"):
        assert f"  {name}:\n" in compose
    assert compose.count('io.xuanming.interop.run=abc') == 8
    assert compose.count('127.0.0.1::2379') == 3
    assert compose.count('127.0.0.1::4000') == 2


def test_refuses_unowned_pd_and_container(monkeypatch):
    ha = module()
    with pytest.raises(RuntimeError, match="PD port is not owned"):
        ha.http_json(12345, "/pd/api/v1/members")
    info = {"Id": "container", "Config": {"Labels": {"com.docker.compose.project": "other",
            "io.xuanming.interop.run": "run", "com.docker.compose.service": "tidb1"}},
            "NetworkSettings": {"Networks": {"other_default": {}}}}
    monkeypatch.setattr(ha, "command", lambda *_: json.dumps([info]))
    with pytest.raises(RuntimeError, match="identity mismatch"):
        ha.owned_container("container", "project", "run", "tidb1")


def test_ledger_region_requires_exactly_one_healthy_three_peer_region(monkeypatch):
    ha = module()
    async def run():
        async def sql(*_): return ((276,),)
        monkeypatch.setattr(ha, "sql", sql)
        stores = {"stores": [{"store": {"id": index, "state_name": "Up", "labels": [{"key": "zone", "value": f"az{index}"}]}}
                             for index in (1, 2, 3)]}
        monkeypatch.setattr(ha, "http_json", lambda _port, path: stores if path.endswith("stores") else {
            "id": 276, "peers": [{"store_id": 1}, {"store_id": 2}, {"store_id": 3}], "leader": {"store_id": 3}})
        assert (await ha.table_region(1, 2))["leader"]["store_id"] == 3
        monkeypatch.setattr(ha, "http_json", lambda *_: {"id": 276, "peers": [{}, {}],
                                                        "leader": {"store_id": 3}})
        with pytest.raises(RuntimeError, match="three healthy"):
            await ha.table_region(1, 2)
        async def many(*_): return ((1,), (2,))
        monkeypatch.setattr(ha, "sql", many)
        with pytest.raises(RuntimeError, match="uniquely identify"):
            await ha.table_region(1, 2)
    asyncio.run(run())


def test_memory_guard_cancels_running_test(monkeypatch, tmp_path):
    ha = module()
    async def run():
        cancelled = asyncio.Event()
        async def blocked(*_):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        monkeypatch.setattr(ha, "test_cluster", blocked)
        values = iter((5 * 1024**3, 1))
        monkeypatch.setattr(ha, "available_memory", lambda: next(values))
        with pytest.raises(RuntimeError, match="free memory"):
            await ha.guarded_test(tmp_path)
        assert cancelled.is_set()
    asyncio.run(run())


def test_qualification_requires_same_instance_complete_results_and_untampered_logs(tmp_path, monkeypatch):
    ha = module()
    binding = {"project": "project", "run_id": "run", "container": "container", "sql_port": 1234}
    (tmp_path / "ready.json").write_text(json.dumps(binding))
    evidence = {p: {"primitive": p, "slots_tested": 64, "lock_wait_observed": True,
                    "writer_released_after_rollback": True, "physical_binding_sha256": "bind",
                    "authoritative_ddl_sha256": "ddl"} for p in ("SELECT_ONLY", "LOCK_TOUCH")}
    qual = {"format": "LOCAL_DEV_LOCK_QUALIFICATION_V1", "backend": "tidb", "cleanup": True,
            "physical_binding": binding, "run_id": "run", "physical_binding_sha256": "bind",
            "authoritative_ddl_sha256": "ddl", "isolation_levels": ["READ COMMITTED", "REPEATABLE READ"],
            "primitive_evidence": evidence}
    qual_data = json.dumps(qual).encode()
    (tmp_path / "qualification.json").write_bytes(qual_data)
    native_dir = tmp_path / "native"
    native_dir.mkdir()
    log_data = b"Go test passed"
    (native_dir / "native-epoch-test.log").write_bytes(log_data)
    native = {"format": "LOCAL_NATIVE_EPOCH_TEST_V1", "passed": True, "cleanup": True,
              "physical_binding": binding, "qualification_sha256": hashlib.sha256(qual_data).hexdigest(),
              "primitives": ["SELECT_ONLY", "LOCK_TOUCH"], "test_log_sha256": hashlib.sha256(log_data).hexdigest()}
    native_data = json.dumps(native).encode()
    (native_dir / "native-epoch-result.json").write_bytes(native_data)
    marker = {"qualification_path": "qualification.json", "qualification_sha256": hashlib.sha256(qual_data).hexdigest(),
              "native_epoch_path": "native/native-epoch-result.json", "native_epoch_sha256": hashlib.sha256(native_data).hexdigest(),
              "completed_at": datetime.now(timezone.utc).isoformat()}
    (tmp_path / "qualification.done").write_text(json.dumps(marker))
    assert ha.validate_qualification(tmp_path, binding)["qualification_sha256"] == marker["qualification_sha256"]
    (tmp_path / "qualification.done").unlink()
    script = Path(__file__).resolve().parents[1] / "tools/tidb_ha/mark_qualified.py"
    spec = importlib.util.spec_from_file_location("mark_qualified", script)
    marking = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(marking)
    monkeypatch.setattr(sys, "argv", [str(script), "--work-dir", str(tmp_path), "--qualification",
                                     str(tmp_path / "qualification.json"), "--native-result",
                                     str(native_dir / "native-epoch-result.json")])
    marking.main()
    assert ha.validate_qualification(tmp_path, binding)["qualification_sha256"] == marker["qualification_sha256"]
    (native_dir / "native-epoch-test.log").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="log SHA-256"):
        ha.validate_qualification(tmp_path, binding)
    (native_dir / "native-epoch-test.log").write_bytes(log_data)
    with pytest.raises(ValueError, match="this isolated TiDB instance"):
        ha.validate_qualification(tmp_path, binding | {"run_id": "different"})
    marker["qualification_path"] = "../outside.json"
    (tmp_path / "qualification.done").write_text(json.dumps(marker))
    with pytest.raises((ValueError, FileNotFoundError)):
        ha.validate_qualification(tmp_path, binding)
