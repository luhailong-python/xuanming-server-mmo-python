"""Run the native stale-epoch fence test against an owned disposable HA cluster."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys


def verify_identity(identity):
    if (not re.fullmatch(r"xuanming-ha-[a-f0-9]{16}", identity.get("project", ""))
            or not re.fullmatch(r"[a-f0-9]{32}", identity.get("run_id", ""))
            or not re.fullmatch(r"[a-f0-9]{64}", identity.get("container", ""))
            or type(identity.get("sql_port")) is not int or not 0 < identity["sql_port"] < 65536):
        raise ValueError("invalid disposable cluster identity")
    response = subprocess.run(["docker", "inspect", identity["container"]], check=True, capture_output=True,
                              timeout=10, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    item = json.loads(response.stdout)[0]
    labels = item["Config"]["Labels"] or {}
    bindings = item["NetworkSettings"]["Ports"]["4000/tcp"]
    if (item["Id"] != identity["container"] or not item["State"]["Running"]
            or labels.get("com.docker.compose.project") != identity["project"]
            or labels.get("io.xuanming.interop.run") != identity["run_id"]
            or labels.get("com.docker.compose.service") != "tidb1"
            or bindings != [{"HostIp": "127.0.0.1", "HostPort": str(identity["sql_port"])}]):
        raise RuntimeError("refusing unowned TiDB endpoint")


def verify_proof(identity, proof):
    qualification = json.loads(proof)
    binding = {key: identity[key] for key in ("sql_port", "run_id", "project", "container")}
    if (qualification.get("format") != "LOCAL_DEV_LOCK_QUALIFICATION_V1" or qualification.get("cleanup") is not True
            or qualification.get("update_denied_to_reader") is not True
            or qualification.get("unrelated_update_denied_to_toucher") is not True
            or qualification.get("lock_touch_rollback_preserved") is not True
            or qualification.get("physical_binding") != binding):
        raise ValueError("qualification proof belongs to a different isolated cluster")
    binding_sha = hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if qualification.get("physical_binding_sha256") != binding_sha:
        raise ValueError("qualification physical binding is invalid")
    for primitive in ("SELECT_ONLY", "LOCK_TOUCH"):
        evidence = qualification["primitive_evidence"][primitive]
        unsigned = {key: value for key, value in evidence.items() if key != "sha256"}
        expected = hashlib.sha256(json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if (evidence.get("sha256") != expected or evidence.get("primitive") != primitive
                or evidence.get("physical_binding_sha256") != binding_sha
                or evidence.get("authoritative_ddl_sha256") != qualification.get("authoritative_ddl_sha256")
                or evidence.get("slots_tested") != 64 or evidence.get("lock_wait_observed") is not True
                or evidence.get("writer_released_after_rollback") is not True
                or evidence.get("isolation_levels") != ["READ COMMITTED", "REPEATABLE READ"]):
            raise ValueError("qualification primitive evidence does not match")
    return qualification


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--identity", type=Path, required=True)
    parser.add_argument("--qualification", type=Path, required=True)
    parser.add_argument("--prepared", type=Path, required=True, help="prepared native snapshot with manifest")
    parser.add_argument("--output", type=Path, required=True, help="new output directory")
    args = parser.parse_args()
    identity = json.loads(args.identity.read_text(encoding="utf-8"))
    proof = args.qualification.read_bytes()
    verify_identity(identity)
    qualification = verify_proof(identity, proof)
    prepared = args.prepared.resolve(strict=True)
    source = prepared / "source"
    prepare_file = Path(__file__).resolve().parents[1] / "interop/prepare.py"
    prepare_spec = importlib.util.spec_from_file_location("interop_prepare_for_epoch", prepare_file)
    prepare = importlib.util.module_from_spec(prepare_spec)
    prepare_spec.loader.exec_module(prepare)
    manifest = json.loads((prepared / "manifest.json").read_text(encoding="utf-8"))
    if prepare.digest_tree(source) != manifest.get("source_sha256"):
        raise ValueError("native source no longer matches the prepared snapshot")
    catalog = (source / "go/shared/authoritysql/catalog.go").read_text(encoding="utf-8")
    ddl = re.findall(r"const catalogWriterSlotBootstrapDDL = `([^`]+)`", catalog)
    if len(ddl) != 1 or hashlib.sha256(ddl[0].encode()).hexdigest() != qualification.get("authoritative_ddl_sha256"):
        raise ValueError("native source DDL does not match qualification")
    output = args.output.resolve()
    if output.exists():
        raise ValueError("native test output directory must be new")
    output.mkdir(parents=True)
    fixture_dir = Path(__file__).resolve().parents[1] / "tidb_interop"
    sys.path.insert(0, str(fixture_dir))
    spec = importlib.util.spec_from_file_location("tidb_fixture", fixture_dir / "run.py")
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    fixture_id = secrets.token_hex(16)
    env = fixture.fixture_environment(source, f"127.0.0.1:{identity['sql_port']}", fixture_id)
    env.update(GOWORK="off", GOENV="off", GOTOOLCHAIN="local", MAIL_RANK_TEST_TIDB="1")
    binding = json.dumps(qualification["physical_binding"], sort_keys=True, separators=(",", ":")).encode()
    env["MMORPG_SQL_FIXTURE_PHYSICAL_AUTHORITY_SHA256"] = hashlib.sha256(binding).hexdigest()
    for primitive in ("SELECT_ONLY", "LOCK_TOUCH"):
        local_evidence = json.dumps({"primitive": primitive, "qualification_sha256": hashlib.sha256(proof).hexdigest(),
                                     "evidence": qualification["primitive_evidence"][primitive],
                                     "scope": "LOCAL_TEST_ONLY"}, sort_keys=True).encode()
        (output / f"qualification-{primitive}.json").write_bytes(local_evidence)
        env[f"MMORPG_SQL_FIXTURE_{primitive}_QUALIFICATION_SHA256"] = hashlib.sha256(local_evidence).hexdigest()
    result = subprocess.run(["go", "test", "-mod=readonly", "./authoritysql", "-run",
                             "^TestTiDBOldEpochWaitingSlotCannotSendBusinessSQL$", "-count=1", "-v"],
                            cwd=source / "go/shared", env=env, capture_output=True, timeout=180,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    (output / "native-epoch-test.log").write_bytes(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f"native epoch test failed; retained receipts: {env['MMORPG_SQL_FIXTURE_RECEIPT_DIR']}")
    for primitive in ("SELECT_ONLY", "LOCK_TOUCH"):
        if f"--- PASS: TestTiDBOldEpochWaitingSlotCannotSendBusinessSQL/{primitive}".encode() not in result.stdout:
            raise RuntimeError(f"native test did not confirm {primitive}; retained receipts: {env['MMORPG_SQL_FIXTURE_RECEIPT_DIR']}")
    verify_identity(identity)
    receipts = Path(env["MMORPG_SQL_FIXTURE_RECEIPT_DIR"])
    fixture.clean_receipts(receipts, fixture_id)
    if receipts.exists():
        raise RuntimeError("native test private receipts were not cleaned")
    (output / "native-epoch-result.json").write_text(json.dumps({
        "format": "LOCAL_NATIVE_EPOCH_TEST_V1", "passed": True, "cleanup": True,
        "physical_binding": qualification["physical_binding"],
        "qualification_sha256": hashlib.sha256(proof).hexdigest(),
        "test_log_sha256": hashlib.sha256(result.stdout + result.stderr).hexdigest(),
        "primitives": ["SELECT_ONLY", "LOCK_TOUCH"], "scope": "LOCAL_TEST_ONLY",
    }, sort_keys=True), encoding="utf-8")
    print("Native old-epoch tests passed for both primitives")


if __name__ == "__main__":
    main()
