import importlib.util
import json
from pathlib import Path

import pytest


def test_rejects_modified_source_or_native_executable(tmp_path):
    spec = importlib.util.spec_from_file_location("tidb_full_preflight", Path(__file__).with_name("run.py"))
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    source, binaries = tmp_path / "source", tmp_path / "bin"
    source.mkdir()
    binaries.mkdir()
    (source / "source.go").write_text("package main", encoding="utf-8")
    for name, _, _ in runner.BUILDS:
        (binaries / f"{name}.exe").write_bytes(name.encode())
    manifest = {"revision": "test", "source_sha256": runner.digest_tree(source),
                "binaries": {f"{name}.exe": runner.digest(binaries / f"{name}.exe")
                             for name, _, _ in runner.BUILDS}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    runner.verify_prepared(tmp_path)
    (source / "source.go").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="source snapshot"):
        runner.verify_prepared(tmp_path)
    (source / "source.go").write_text("package main", encoding="utf-8")
    (binaries / "match.exe").write_bytes(b"changed")
    with pytest.raises(ValueError, match="native executable"):
        runner.verify_prepared(tmp_path)


def test_scene_drain_option_reaches_driver_and_smoke_chain():
    def load(name, path):
        spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(path))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    runner = load("tidb_full_drain_runner", "run.py")
    driver = load("tidb_full_drain_driver", "driver.py")
    for selected in (False, True):
        command = runner.driver_command(selected)
        args = driver.parse_args(command[2:])
        assert args.scene_drain is selected
        assert driver.smoke_options(args) == {
            "monopoly_board": True, "scene_landing": True, "scene_team": True, "scene_drain": selected,
        }


def test_scene_drain_evidence_requires_real_handoff():
    spec = importlib.util.spec_from_file_location("tidb_full_drain_validation", Path(__file__).with_name("run.py"))
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    evidence = {"source_node": 201, "target_node": 203, "source_epoch": 8, "target_epoch": 9,
                "scene_config_id": 1, "mysql_save_seq": 4, "source_shutdown": True, "gate_routed": True,
                "native_mysql_verified": True}
    assert runner.valid_scene_drain({"scene_planned_shutdown": evidence})
    for key, invalid in (("source_shutdown", False), ("gate_routed", False), ("native_mysql_verified", False),
                         ("target_node", 201), ("target_epoch", 8), ("scene_config_id", 0),
                         ("mysql_save_seq", 0), ("target_epoch", True)):
        assert not runner.valid_scene_drain({"scene_planned_shutdown": evidence | {key: invalid}})
    assert not runner.valid_scene_drain({})
    assert not runner.valid_scene_drain({"scene_planned_shutdown": None})
