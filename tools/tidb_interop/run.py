"""Disposable, local-only TiDB integration for portal, Match and Mall (Windows)."""

import argparse
import csv
import hashlib
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time

from ownership import assert_owned_tidb

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
FLAGS = getattr(subprocess, "CREATE_NO_WINDOW", 0)
IMAGES = {
    "pd": "pingcap/pd@sha256:507b401ece53aba15db9628a90a1e350c3cd1b9b39c086c708e2e88541b0e7fd",
    "tikv": "pingcap/tikv@sha256:cdb681d570c490424d69ecc1ecda6865999f2b86ae3b2d2cb070989bf4d2701d",
    "tidb": "pingcap/tidb@sha256:3378c71dc8b3dd39742dc0ba546c80880a5d8b0da67a9f9d116cfc33505505c3",
}


def command(args, *, cwd=ROOT, env=None, timeout=120, stdout=None):
    result = subprocess.run([str(arg) for arg in args], cwd=cwd, env=env, stdout=stdout or subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout, creationflags=FLAGS)
    if result.returncode:
        # Never print arbitrary child output: SQL credentials are private.
        raise RuntimeError(f"{Path(str(args[0])).name} failed with exit status {result.returncode}")
    return result.stdout or b""


def archive(repo, commit, paths, destination, tarfile_path):
    command(["git", "-C", repo, "archive", "--format=tar", f"--output={tarfile_path}", commit, *paths], timeout=180)
    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    spec = importlib.util.spec_from_file_location("interop_prepare", HERE.parent / "interop/prepare.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.extract_snapshot(tarfile_path, root)


def fixture_environment(snapshot, endpoint, run_id):
    parent = Path.home() / ".mmorpg/sql-boot-local"
    parent.mkdir(parents=True, exist_ok=True)
    receipts = parent / ("codex-tidb-receipts-" + run_id)
    receipts.mkdir()
    sid_bytes = command(["whoami.exe", "/user", "/fo", "csv", "/nh"])
    sid = next(csv.reader([sid_bytes.decode().strip()]))[-1]
    if not sid.startswith("S-1-"):
        raise RuntimeError("cannot identify Windows user")
    command(["icacls.exe", receipts, "/inheritance:r", "/grant:r", f"*{sid}:(OI)(CI)F",
             "*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F", "/Q"])
    digest = lambda text: hashlib.sha256(text.encode()).hexdigest()
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("MMORPG_", "MATCH_IT_", "COMMERCE_"))}
    env.update(MATCH_IT_TIDB=endpoint, MATCH_IT_REQUIRE_TIDB="1", MMORPG_SQL_BACKEND="tidb",
               MMORPG_SQL_FIXTURE_ADMIN_DSN=f"root@tcp({endpoint})/",
               MMORPG_SQL_FIXTURE_PHYSICAL_AUTHORITY_SHA256=digest(f"LOCAL_TEST_PHYSICAL_V1:tidb:{endpoint}"),
               MMORPG_SQL_FIXTURE_LOCK_QUALIFICATION_SHA256=digest("LOCAL_TEST_LOCK_QUALIFICATION_V1:tidb"),
               MMORPG_SQL_FIXTURE_LOCK_PRIMITIVE="SELECT_ONLY", MMORPG_SQL_FIXTURE_RECEIPT_DIR=str(receipts))
    return env


def clean_receipts(receipts, run_id):
    if receipts is None or not receipts.exists():
        return
    resolved = receipts.resolve()
    allowed_parent = (Path.home() / ".mmorpg/sql-boot-local").resolve()
    if receipts.is_symlink() or resolved.parent != allowed_parent or resolved.name != "codex-tidb-receipts-" + run_id:
        raise RuntimeError("fixture output is outside the intended directory; refusing cleanup")
    shutil.rmtree(resolved)


def compose_yaml(run_id, subnet):
    label = f'    labels: ["io.xuanming.interop.run={run_id}"]\n'
    network = f'\nnetworks:\n  default:\n    ipam:\n      config:\n        - subnet: {subnet}\n' if subnet else ''
    return (
        'services:\n  pd:\n' + f'    image: {IMAGES["pd"]}\n' + label +
        '    mem_limit: 1g\n    command: ["--name=pd", "--data-dir=/data/pd", "--client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://pd:2379", "--peer-urls=http://0.0.0.0:2380", "--advertise-peer-urls=http://pd:2380", "--initial-cluster=pd=http://pd:2380", "--config=/etc/test/pd.toml"]\n'
        '    volumes: ["./pd.toml:/etc/test/pd.toml:ro"]\n'
        '  tikv:\n' + f'    image: {IMAGES["tikv"]}\n' + label +
        '    mem_limit: 3g\n    command: ["--pd=pd:2379", "--addr=0.0.0.0:20160", "--advertise-addr=tikv:20160", "--data-dir=/data/tikv", "--config=/etc/test/tikv.toml"]\n'
        '    volumes: ["./tikv.toml:/etc/test/tikv.toml:ro"]\n    depends_on: [pd]\n'
        '  tidb:\n' + f'    image: {IMAGES["tidb"]}\n' + label +
        '    mem_limit: 3g\n    command: ["--store=tikv", "--path=pd:2379", "--advertise-address=tidb", "--config=/etc/test/tidb.toml"]\n'
        '    volumes: ["./tidb.toml:/etc/test/tidb.toml:ro"]\n    ports: ["127.0.0.1::4000"]\n    depends_on: [tikv]\n' + network)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp-repo", type=Path, required=True, help="C++ repository with initialized protobuf submodule")
    parser.add_argument("--cpp-ref", required=True, help="Git commit including the Go Team fix")
    parser.add_argument("--mysql", type=Path, required=True, help="MySQL-compatible command-line client executable")
    parser.add_argument("--protoc", type=Path, required=True)
    parser.add_argument("--mvn", required=True, help="Maven executable")
    parser.add_argument("--java-image", default="eclipse-temurin:21-jre")
    parser.add_argument("--subnet", help="optional unused private Docker IPv4 subnet")
    parser.add_argument("--only-mall", action="store_true", help="run only the Mall integration, not full acceptance")
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("this runner currently supports Windows only")
    if args.subnet:
        network = ipaddress.ip_network(args.subnet)
        if network.version != 4 or not network.is_private:
            parser.error("subnet must be a private IPv4 network")
        args.subnet = str(network)
    for file in (args.mysql, args.protoc):
        if not file.is_file(): parser.error(f"required executable missing: {file}")
    cpp = args.cpp_repo.resolve()
    commit = command(["git", "-C", cpp, "rev-parse", f"{args.cpp_ref}^{{commit}}"]).decode().strip()
    subcommit = command(["git", "-C", cpp, "rev-parse", f"{commit}:third_party/protobuf"]).decode().strip()
    protobuf = cpp / "third_party/protobuf"
    command(["git", "-C", protobuf, "cat-file", "-e", f"{subcommit}^{{commit}}"])
    run_id = secrets.token_hex(16)
    project = "xuanming-tidb-" + secrets.token_hex(8)
    work = ROOT / "build" / ("tidb-interop-" + run_id)
    work.mkdir(parents=True)
    result = {"cpp_commit": commit, "run_id": run_id, "project": project, "ok": False,
              "scope": "mall-only" if args.only_mall else "portal-team-mall"}
    compose = work / "compose.yaml"
    compose.write_text(compose_yaml(run_id, args.subnet), encoding="utf-8")
    for name in ("pd.toml", "tikv.toml", "tidb.toml"):
        shutil.copyfile(HERE / name, work / name)
    stack = ["docker", "compose", "-p", project, "-f", compose]
    created = False
    receipts = None
    try:
        snapshot = work / "cpp"
        archive(cpp, commit, ["go", "java", "proto", "deploy/sql", "tools/scripts/sql_fixture_env.ps1"],
                snapshot, work / "cpp.tar")
        archive(protobuf, subcommit, ["src/google/protobuf"], snapshot / "third_party/protobuf", work / "protobuf.tar")
        created = True
        command(stack + ["up", "-d"], timeout=180)
        container = command(stack + ["ps", "-q", "tidb"]).decode().strip()
        binding = command(["docker", "port", container, "4000/tcp"]).decode().strip()
        host, _, port_text = binding.rpartition(":")
        if host != "127.0.0.1": raise RuntimeError("TiDB SQL is not bound to IPv4 loopback")
        port = int(port_text)
        assert_owned_tidb(port, project, container, run_id)
        (work / "connection.json").write_text(json.dumps({"port": port, "project": project,
                                                           "container": container, "run_id": run_id}), encoding="utf-8")
        print(f"Isolated TiDB startup: {work / 'connection.json'}", flush=True)
        deadline = time.monotonic() + 120
        while True:
            try:
                version = command([args.mysql, "--protocol=TCP", "--host=127.0.0.1", f"--port={port}",
                                   "--user=root", "--connect-timeout=2", "--batch", "--skip-column-names", "-e",
                                   "SELECT VERSION(), TIDB_VERSION()"], timeout=5).decode()
                if "tidb" not in version.lower() or "Store: tikv" not in version:
                    raise RuntimeError("database is not TiDB backed by TiKV")
                break
            except (RuntimeError, subprocess.TimeoutExpired):
                if time.monotonic() >= deadline: raise RuntimeError("TiDB did not become ready")
                time.sleep(2)
        result.update(tidb_version=version.strip(), backend="tikv", local_test_lock_declaration=True,
                      production_i3_qualification=False, multi_replica_failover_tested=False)
        identity = ["--port", str(port), "--project", project, "--container", container, "--run-id", run_id]
        if not args.only_mall:
            command([sys.executable, HERE / "portal_check.py", *identity, "--output", work / "portal-acceptance.json"])
            receipts = Path.home() / ".mmorpg/sql-boot-local" / ("codex-tidb-receipts-" + run_id)
            env = fixture_environment(snapshot, f"127.0.0.1:{port}", run_id)
            with (work / "go-team-test.log").open("wb") as log:
                command(["go", "test", "./internal/team", "-run", "^TestTeamTiDB", "-count=1", "-timeout=20m", "-v"],
                        cwd=snapshot / "go/match", env=env, timeout=1260, stdout=log)
            clean_receipts(receipts, run_id)
            result["go_private_artifacts_cleaned"] = True
        command(["go", "build", "-o", work / "writer_authority.exe", "./cmd/writer_authority"],
                cwd=snapshot / "go/data_service", timeout=300)
        protoc_uri = args.protoc.resolve().as_uri()
        with (work / "mall-build.log").open("wb") as log:
            command([args.mvn, "-q", "-pl", "mall_game", "-am", "-Dmaven.test.skip=true", f"-Dprotoc.spec={protoc_uri}", "package"],
                    cwd=snapshot / "java", timeout=600, stdout=log)
        command([sys.executable, HERE / "mall_check.py", *identity, "--work", work, "--cpp-snapshot", snapshot,
                 "--mysql", args.mysql.resolve(), "--network", project + "_default", "--java-image", args.java_image],
                timeout=300)
        result.update(portal=not args.only_mall, go_match_tidb=not args.only_mall, mall_prod_tidb=True, ok=True)
    finally:
        cleanup_error = None
        try:
            clean_receipts(receipts, run_id)
            if created:
                # Only this random Compose project is eligible for cleanup.
                identifiers = command(stack + ["ps", "-aq"]).decode().split()
                for identifier in identifiers:
                    obj = json.loads(command(["docker", "inspect", identifier]))[0]
                    labels = obj["Config"]["Labels"] or {}
                    if labels.get("io.xuanming.interop.run") != run_id or labels.get("com.docker.compose.project") != project:
                        raise RuntimeError("unexpected Docker resource; refusing cleanup")
                network_name = project + "_default"
                network = subprocess.run(["docker", "network", "inspect", network_name], capture_output=True,
                                         timeout=15, creationflags=FLAGS)
                if network.returncode == 0:
                    obj = json.loads(network.stdout)[0]
                    if (obj.get("Labels") or {}).get("com.docker.compose.project") != project:
                        raise RuntimeError("unexpected Docker network; refusing cleanup")
                command(stack + ["down", "--volumes"], timeout=120)
                remaining = command(stack + ["ps", "-aq"]).strip()
                network = subprocess.run(["docker", "network", "inspect", network_name], capture_output=True,
                                         timeout=15, creationflags=FLAGS)
                volumes = command(["docker", "volume", "ls", "-q", "--filter", f"label=com.docker.compose.project={project}"])
                result["cleanup"] = not remaining and network.returncode != 0 and not volumes.strip()
                if not result["cleanup"]:
                    raise RuntimeError("owned Docker resources remain")
        except Exception as exc:
            result["ok"] = False
            result["cleanup"] = False
            cleanup_error = exc
        finally:
            (work / "acceptance.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
            print(f"TiDB integration result: {work / 'acceptance.json'}", flush=True)
        if cleanup_error:
            raise cleanup_error


if __name__ == "__main__":
    main()
