import json
import io
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prepare import BUILDS, check_native_compatibility, digest, digest_tree, extract_snapshot
from run import verify_prepared


def test_scene_manager_redis_pool_conflict_is_rejected_before_start(tmp_path):
    mint = tmp_path / "go/scene_manager/internal/svc/owner_epoch_mint.go"
    redis = tmp_path / "go/shared/clusterredis/clusterredis.go"
    mint.parent.mkdir(parents=True)
    redis.parent.mkdir(parents=True)
    mint.write_text("const (\nownerEpochMintPoolSize = 130\n)\nconn.PoolSizePerNode = ownerEpochMintPoolSize\n", encoding="utf-8")
    redis.write_text("const (\nMaxPoolSizePerNode = 128\n)\nif c.PoolSizePerNode > MaxPoolSizePerNode {\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="ownerEpochMintPoolSize 130 exceeds clusterredis MaxPoolSizePerNode 128"):
        check_native_compatibility(tmp_path)
    redis.write_text(redis.read_text(encoding="utf-8").replace("= 128", "= 130"), encoding="utf-8")
    check_native_compatibility(tmp_path)


def test_snapshot_and_binaries_must_match_build_manifest(tmp_path):
    source, binaries = tmp_path / "source", tmp_path / "bin"
    source.mkdir()
    binaries.mkdir()
    (source / "source.go").write_text("package main", encoding="utf-8")
    for name, _, _ in BUILDS:
        (binaries / f"{name}.exe").write_bytes(name.encode())
    manifest = {"revision": "test", "source_sha256": digest_tree(source),
                "binaries": {f"{name}.exe": digest(binaries / f"{name}.exe") for name, _, _ in BUILDS}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    verify_prepared(tmp_path)
    (source / "source.go").write_text("modified", encoding="utf-8")
    with pytest.raises(RuntimeError, match="source snapshot changed"):
        verify_prepared(tmp_path)
    (source / "source.go").write_text("package main", encoding="utf-8")
    (binaries / "match.exe").write_bytes(b"modified")
    with pytest.raises(RuntimeError, match="executable changed"):
        verify_prepared(tmp_path)


@pytest.mark.parametrize("name,kind", [("../escaped", tarfile.REGTYPE), ("/absolute", tarfile.REGTYPE),
                                       ("safe/link", tarfile.SYMTYPE), ("C:/escape", tarfile.REGTYPE),
                                       ("safe/NUL.txt", tarfile.REGTYPE)])
def test_archive_rejects_traversal_links_and_windows_device_names(tmp_path, name, kind):
    archive, source = tmp_path / "snapshot.tar", tmp_path / "source"
    source.mkdir()
    with tarfile.open(archive, "w") as tar:
        item = tarfile.TarInfo(name)
        item.type = kind
        item.linkname = "elsewhere"
        if kind == tarfile.REGTYPE:
            item.size = 1
            tar.addfile(item, io.BytesIO(b"x"))
        else:
            tar.addfile(item)
    with pytest.raises(RuntimeError, match="unsafe snapshot entry"):
        extract_snapshot(archive, source)
    assert not (tmp_path / "escaped").exists()


def test_archive_extracts_regular_files_without_overwriting(tmp_path):
    archive, source = tmp_path / "snapshot.tar", tmp_path / "source"
    source.mkdir()
    with tarfile.open(archive, "w") as tar:
        item = tarfile.TarInfo("go/app/main.go")
        item.size = 3
        tar.addfile(item, io.BytesIO(b"abc"))
    extract_snapshot(archive, source)
    assert (source / "go/app/main.go").read_bytes() == b"abc"
    with pytest.raises(FileExistsError):
        extract_snapshot(archive, source)
