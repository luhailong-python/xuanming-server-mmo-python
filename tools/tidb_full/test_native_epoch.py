import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location("native_epoch_unit", Path(__file__).with_name("native_epoch.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def identity():
    return {"project": "xuanming-ha-" + "1" * 16, "run_id": "2" * 32,
            "container": "3" * 64, "sql_port": 4000}


def test_owned_cluster_is_checked_before_sql(monkeypatch):
    data = identity()
    item = {"Id": data["container"], "State": {"Running": True}, "Config": {"Labels": {
        "com.docker.compose.project": data["project"], "io.xuanming.interop.run": data["run_id"],
        "com.docker.compose.service": "tidb1"}}, "NetworkSettings": {"Ports": {
        "4000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "4000"}]}}}
    monkeypatch.setattr(module.subprocess, "run", lambda *_a, **_k: SimpleNamespace(stdout=json.dumps([item]).encode()))
    module.verify_identity(data)
    item["NetworkSettings"]["Ports"]["4000/tcp"].append({"HostIp": "0.0.0.0", "HostPort": "4000"})
    with pytest.raises(RuntimeError):
        module.verify_identity(data)
    with pytest.raises(ValueError):
        module.verify_identity(data | {"run_id": "not-a-run"})


def test_proof_requires_matching_identity_and_both_primitive_digests():
    data = identity()
    binding = hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    records = {}
    for kind in ("SELECT_ONLY", "LOCK_TOUCH"):
        entry = {"primitive": kind, "slots_tested": 64, "isolation_levels": ["READ COMMITTED", "REPEATABLE READ"],
                 "authoritative_ddl_sha256": "a" * 64, "physical_binding_sha256": binding,
                 "lock_wait_observed": True, "writer_released_after_rollback": True}
        entry["sha256"] = hashlib.sha256(json.dumps(entry, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        records[kind] = entry
    proof = {"format": "LOCAL_DEV_LOCK_QUALIFICATION_V1", "cleanup": True, "update_denied_to_reader": True,
             "unrelated_update_denied_to_toucher": True, "lock_touch_rollback_preserved": True, "physical_binding": data,
             "physical_binding_sha256": binding, "authoritative_ddl_sha256": "a" * 64,
             "primitive_evidence": records}
    module.verify_proof(data, json.dumps(proof).encode())
    records["LOCK_TOUCH"]["slots_tested"] = 63
    with pytest.raises(ValueError):
        module.verify_proof(data, json.dumps(proof).encode())
    with pytest.raises(ValueError):
        module.verify_proof(data | {"sql_port": 4001}, json.dumps(proof).encode())
