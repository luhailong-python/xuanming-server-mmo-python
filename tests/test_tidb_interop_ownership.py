import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def _module():
    path = Path(__file__).resolve().parents[1] / "tools/tidb_interop/ownership.py"
    spec = importlib.util.spec_from_file_location("tidb_interop_ownership", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ownership_guard_only_accepts_exact_project_run_and_loopback(monkeypatch):
    module = _module()
    project, run_id, container = "xuanming-tidb-" + "a" * 16, "b" * 32, "c" * 64
    entry = {"Id": container, "State": {"Running": True},
             "Config": {"Labels": {"com.docker.compose.project": project,
                                   "com.docker.compose.service": "tidb", "io.xuanming.interop.run": run_id}},
             "NetworkSettings": {"Networks": {project + "_default": {}},
                                 "Ports": {"4000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "40001"}]}}}
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps([entry])))
    module.assert_owned_tidb(40001, project, container, run_id)
    for field, bad in (("Id", "d" * 64), ("State", {"Running": False})):
        previous = entry[field]
        entry[field] = bad
        with pytest.raises(RuntimeError):
            module.assert_owned_tidb(40001, project, container, run_id)
        entry[field] = previous
    entry["NetworkSettings"]["Ports"]["4000/tcp"][0]["HostIp"] = "0.0.0.0"
    with pytest.raises(RuntimeError):
        module.assert_owned_tidb(40001, project, container, run_id)
    entry["NetworkSettings"]["Ports"]["4000/tcp"][0]["HostIp"] = "127.0.0.1"
    entry["NetworkSettings"]["Networks"] = {"other_default": {}}
    with pytest.raises(RuntimeError):
        module.assert_owned_tidb(40001, project, container, run_id)


def test_ownership_guard_rejects_arbitrary_project_before_docker(monkeypatch):
    module = _module()
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: pytest.fail("must not inspect arbitrary IDs"))
    with pytest.raises(RuntimeError):
        module.assert_owned_tidb(4000, "my-existing-db", "c" * 64, "b" * 32)


def test_mall_failed_launch_cleans_private_files_and_keeps_secrets_out_of_argv(monkeypatch, tmp_path):
    folder = Path(__file__).resolve().parents[1] / "tools/tidb_interop"
    monkeypatch.syspath_prepend(str(folder))
    spec = importlib.util.spec_from_file_location("tidb_interop_mall", folder / "mall_check.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.setenv("MMORPG_SQL_SCHEMA_DSN", "do-not-inherit")
    monkeypatch.setenv("COMMERCE_ADMIN_SECRET", "do-not-inherit")
    assert "MMORPG_SQL_SCHEMA_DSN" not in module.local_environment()
    assert "COMMERCE_ADMIN_SECRET" not in module.local_environment()
    monkeypatch.setattr(module, "assert_owned_tidb", lambda *_: None)

    def setup(*_):
        root = tmp_path / ".mmorpg/sql-boot-local" / ("codex-mall-" + "a" * 32)
        (root / "output").mkdir(parents=True)
        grant = root / "output/boot-grant.json"
        grant.write_bytes(b"fixture")
        module.PRIVATE_ROOT = root
        return "mall_it", "fixture_user", "fixture_secret_1234567890123456", "test-deployment", "1" * 64, grant

    monkeypatch.setattr(module, "setup", setup)

    def command(argv, *, env=None, data=None):
        assert argv[:3] == ["docker", "run", "-d"]
        assert "fixture_secret_1234567890123456" not in " ".join(map(str, argv))
        assert env["MMORPG_SQL_PASSWORD"] == "fixture_secret_1234567890123456"
        raise RuntimeError("simulated launch failure")

    monkeypatch.setattr(module, "command", command)
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=1))
    args = SimpleNamespace(work=tmp_path, cpp_snapshot=tmp_path, mysql=tmp_path / "mysql", port=40000,
                           network="project_default", project="project", container="container", run_id="f" * 32,
                           java_image="java")
    with pytest.raises(RuntimeError, match="simulated launch failure"):
        module.main(args)
    assert not module.PRIVATE_ROOT.exists()


def test_receipt_cleanup_requires_the_run_specific_parent_and_name(monkeypatch, tmp_path):
    folder = Path(__file__).resolve().parents[1] / "tools/tidb_interop"
    monkeypatch.syspath_prepend(str(folder))
    spec = importlib.util.spec_from_file_location("tidb_interop_runner", folder / "run.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.Path, "home", classmethod(lambda cls: tmp_path))
    run_id = "a" * 32
    receipts = tmp_path / ".mmorpg/sql-boot-local" / ("codex-tidb-receipts-" + run_id)
    receipts.mkdir(parents=True)
    (receipts / "receipt").write_text("private", encoding="utf-8")
    module.clean_receipts(receipts, run_id)
    assert not receipts.exists()
    other = tmp_path / "outside" / ("codex-tidb-receipts-" + run_id)
    other.mkdir(parents=True)
    with pytest.raises(RuntimeError, match="refusing cleanup"):
        module.clean_receipts(other, run_id)
    assert other.exists()
