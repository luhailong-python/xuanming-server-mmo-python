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
