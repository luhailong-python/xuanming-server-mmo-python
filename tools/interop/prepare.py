"""Build the native test peers from one committed source revision (Windows)."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile


DEFAULT_REVISION = "098f18ade7a3a8692698add32dc64556a665c7dc"
BUILDS = (
    ("client_rpc_router", "client_rpc_router", "."), ("login", "login", "."),
    ("player_locator", "player_locator", "."), ("scene_manager", "scene_manager", "."),
    ("data_service", "data_service", "."), ("db", "db", "."),
    ("db_migrate", "db", "./cmd/migrate"), ("writer_authority", "data_service", "./cmd/writer_authority"),
    ("match", "match", "."), ("player_inbox", "player_inbox", "."),
)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def digest_tree(root):
    total = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        total.update(path.relative_to(root).as_posix().encode("utf-8"))
        total.update(b"\0")
        total.update(bytes.fromhex(digest(path)))
    return total.hexdigest()


def check_native_compatibility(source):
    """Fail early for the known native SceneManager Redis pool conflict."""
    mint = source / "go/scene_manager/internal/svc/owner_epoch_mint.go"
    redis = source / "go/shared/clusterredis/clusterredis.go"
    if not mint.is_file() or not redis.is_file():
        return
    mint_text, redis_text = mint.read_text(encoding="utf-8"), redis.read_text(encoding="utf-8")
    fixed = re.search(r"(?m)^\s*ownerEpochMintPoolSize\s*=\s*(\d+)\s*$", mint_text)
    maximum = re.search(r"(?m)^\s*MaxPoolSizePerNode\s*=\s*(\d+)\s*$", redis_text)
    if (fixed and maximum and "conn.PoolSizePerNode = ownerEpochMintPoolSize" in mint_text
            and "c.PoolSizePerNode > MaxPoolSizePerNode" in redis_text
            and int(fixed.group(1)) > int(maximum.group(1))):
        raise RuntimeError(
            "native SceneManager cannot start: ownerEpochMintPoolSize "
            f"{fixed.group(1)} exceeds clusterredis MaxPoolSizePerNode {maximum.group(1)}; "
            "select a corrected committed C++/Go revision"
        )


def extract_snapshot(archive, source):
    reserved = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
                *(f"LPT{i}" for i in range(1, 10))}
    with tarfile.open(archive) as tar:
        for item in tar:
            name = PurePosixPath(item.name)
            invalid_part = any(part.rstrip(" .") != part or part.split(".")[0].upper() in reserved
                               for part in name.parts)
            if (name.is_absolute() or ".." in name.parts or invalid_part or not (item.isdir() or item.isfile())
                    or any(char in item.name for char in '\\:<>"|?*')):
                raise RuntimeError(f"unsafe snapshot entry: {item.name}")
            target = source.joinpath(*name.parts).resolve()
            if not target.is_relative_to(source):
                raise RuntimeError(f"unsafe snapshot entry: {item.name}")
            if item.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(item) as src, target.open("xb") as dst:
                    shutil.copyfileobj(src, dst)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp-repo", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True, help="new disposable directory; must not exist")
    parser.add_argument("--go", type=Path, required=True, help="Go compiler executable")
    parser.add_argument("--revision", default=DEFAULT_REVISION, help="committed C++/Go repository revision")
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("this native middleware harness currently supports Windows only")
    repo = args.cpp_repo.resolve(strict=True)
    go = args.go.resolve(strict=True)
    work = args.work_dir.resolve()
    if work.exists():
        parser.error("work directory already exists; supply a new path")
    revision = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "--verify", f"{args.revision}^{{commit}}"], text=True).strip()
    if len(revision) != 40:
        raise RuntimeError("unexpected git commit ID")
    work.mkdir(parents=True)
    source, binaries = work / "source", work / "bin"
    source.mkdir()
    binaries.mkdir()
    archive = work / "snapshot.tar"
    subprocess.run(["git", "-C", str(repo), "archive", "--format=tar", f"--output={archive}", revision,
                    "go", "generated", "deploy/sql"], check=True)
    extract_snapshot(archive, source)
    archive.unlink()
    check_native_compatibility(source)
    source_hash = digest_tree(source)
    env = dict(os.environ, GOWORK="off", GOENV="off", GOTOOLCHAIN="local", CGO_ENABLED="0", GOTELEMETRY="off")
    for name, module, package in BUILDS:
        print(f"Building {name} from {revision}", flush=True)
        subprocess.run([str(go), "build", "-mod=readonly", "-buildvcs=false", "-tags=purego", "-o", str(binaries / f"{name}.exe"), package],
                       cwd=source / "go" / module, env=env, check=True)
    if digest_tree(source) != source_hash:
        raise RuntimeError("native build modified the committed source snapshot")
    manifest = {"revision": revision, "source_sha256": source_hash,
                "binaries": {f"{name}.exe": digest(binaries / f"{name}.exe")
                                                    for name, _, _ in BUILDS}}
    (work / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Prepared native peers: {work}")


if __name__ == "__main__":
    main()
