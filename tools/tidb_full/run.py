"""Run the entire native/Python chain against one disposable TiDB/TiKV cluster."""

import argparse
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

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "tools/tidb_interop"), str(ROOT / "tools/interop")]
from safety import assert_exclusive_tidb
from prepare import BUILDS, digest, digest_tree
import windows_job

_stack_spec = importlib.util.spec_from_file_location("_xuanming_tidb_stack", ROOT / "tools/tidb_interop/run.py")
_stack_module = importlib.util.module_from_spec(_stack_spec)
_stack_spec.loader.exec_module(_stack_module)
compose_yaml = _stack_module.compose_yaml


def command(args, *, timeout=120):
    result = subprocess.run([str(arg) for arg in args], cwd=ROOT, capture_output=True,
                            timeout=timeout, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if result.returncode:
        raise RuntimeError(f"{Path(str(args[0])).name} failed with status {result.returncode}: "
                           + result.stderr.decode(errors="replace")[-1200:])
    return result.stdout


def verify_prepared(path):
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    source, binaries = path / "source", path / "bin"
    if digest_tree(source) != manifest.get("source_sha256"):
        raise ValueError("native source snapshot no longer matches its manifest")
    if set(manifest.get("binaries", {})) != {f"{name}.exe" for name, _, _ in BUILDS}:
        raise ValueError("native binary manifest is incomplete")
    for name, expected in manifest["binaries"].items():
        if digest(binaries / name) != expected:
            raise ValueError(f"native executable changed: {name}")
    return manifest, source, binaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("prepared", "work-dir", "etcd", "redis", "mysql", "kafka-java", "kafka-libs", "go", "private-helper"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--subnet", help="optional unused private IPv4 Docker subnet")
    args = parser.parse_args()
    if os.name != "nt" or not __debug__:
        parser.error("Windows and enabled Python assertions are required")
    subnet = None
    if args.subnet:
        subnet = ipaddress.ip_network(args.subnet)
        if subnet.version != 4 or not subnet.is_private:
            parser.error("subnet must be a private IPv4 network")
        subnet = str(subnet)
    prepared = args.prepared.resolve(strict=True)
    manifest, source, binaries = verify_prepared(prepared)
    work = args.work_dir.resolve()
    if work.exists():
        parser.error("work directory must be new")
    paths = {"SOURCE": source, "BIN": binaries, "WORK": work, "ETCD": args.etcd,
             "REDIS": args.redis, "MYSQL": args.mysql, "KAFKA_JAVA": args.kafka_java,
             "KAFKA_LIBS": args.kafka_libs, "GO": args.go, "PRIVATE_HELPER": args.private_helper}
    paths = {name: value if name == "WORK" else value.resolve(strict=True) for name, value in paths.items()}
    if not (paths["MYSQL"] / "bin/mysql.exe").is_file():
        parser.error("--mysql must contain bin/mysql.exe")
    for name in ("ETCD", "REDIS", "KAFKA_JAVA", "GO", "PRIVATE_HELPER"):
        if not paths[name].is_file(): parser.error(f"{name} must be an executable file")
    if not list(paths["KAFKA_LIBS"].glob("*.jar")):
        parser.error("Kafka lib directory contains no jars")
    work.mkdir(parents=True)
    run_id, project = secrets.token_hex(16), "xuanming-tidb-" + secrets.token_hex(8)
    compose = work / "compose.yaml"
    compose.write_text(compose_yaml(run_id, subnet), encoding="utf-8")
    for name in ("pd.toml", "tikv.toml", "tidb.toml"):
        shutil.copyfile(ROOT / "tools/tidb_interop" / name, work / name)
    stack = ["docker", "compose", "-p", project, "-f", compose]
    started = False
    result = {"ok": False, "cpp_commit": manifest["revision"], "run_id": run_id,
              "project": project, "mode": "isolated integration; not production I3 certification"}
    try:
        started = True  # Include cleanup when Compose only partially starts.
        command(stack + ["up", "-d"], timeout=240)
        container = command(stack + ["ps", "-q", "tidb"]).decode().strip()
        binding = command(["docker", "port", container, "4000/tcp"]).decode().strip()
        host, _, port_text = binding.rpartition(":")
        if host != "127.0.0.1":
            raise RuntimeError("TiDB SQL port is not bound exclusively to IPv4 loopback")
        port = int(port_text)
        assert_exclusive_tidb(port, project, container, run_id)
        deadline = time.monotonic() + 150
        while True:
            try:
                assert_exclusive_tidb(port, project, container, run_id)
                version = command([paths["MYSQL"] / "bin/mysql.exe", "--no-defaults", "--protocol=TCP", "--host=127.0.0.1",
                                   f"--port={port}", "--user=root", "--connect-timeout=2", "--batch",
                                   "--skip-column-names", "-e", "SELECT VERSION(), TIDB_VERSION()"], timeout=5)
                if b"tidb" not in version.lower() or b"Store: tikv" not in version:
                    raise RuntimeError("endpoint is not TiDB with TiKV storage")
                break
            except (RuntimeError, subprocess.TimeoutExpired):
                if time.monotonic() >= deadline:
                    raise RuntimeError("isolated TiDB did not become ready")
                time.sleep(2)
        env = dict(os.environ, **{f"XUANMING_IT_{key}": str(value) for key, value in paths.items()})
        env.update(XUANMING_TIDB_FULL_PORT=str(port), XUANMING_TIDB_FULL_PROJECT=project,
                   XUANMING_TIDB_FULL_CONTAINER=container, XUANMING_TIDB_FULL_RUN_ID=run_id,
                   PYTHONPATH=str(ROOT / "src"), PYTHONNOUSERSITE="1")
        env.pop("PYTHONOPTIMIZE", None)
        print(f"Testing complete chain with isolated TiDB; artifacts: {work}", flush=True)
        status = windows_job.run([sys.executable, str(Path(__file__).with_name("driver.py"))],
                                 cwd=ROOT, env=env, timeout=1200)
        if status:
            raise RuntimeError(f"TiDB full-chain driver failed with status {status}")
        runs = list(work.glob("run-*"))
        if len(runs) != 1 or not (runs[0] / "acceptance.json").is_file():
            raise RuntimeError("expected exactly one completed full-chain acceptance")
        report = json.loads((runs[0] / "acceptance.json").read_text(encoding="utf-8"))
        proof = json.loads((runs[0] / "tidb-lock-qualification.json").read_text(encoding="utf-8"))
        if (not report.get("ok") or report.get("stage") != "complete"
                or report.get("scene_team", {}).get("native_apply_join_tip") != 0
                or not report.get("scene_team", {}).get("native_invitation_accepted")
                or not report.get("cross_zone", {}).get("round_trip")
                or not report.get("battle", {}).get("native_go_db_battle_result")
                or proof.get("lockPrimitive") != "SELECT_ONLY" or not proof.get("select_only_blocked_writer")):
            raise RuntimeError("full-chain or TiDB locking evidence is incomplete")
        result.update(ok=True, tidb_version=version.decode(errors="replace").strip(),
                      backend="tidb/tikv", writer_lock="SELECT_ONLY", full_acceptance=str(runs[0] / "acceptance.json"),
                      qualification=str(runs[0] / "tidb-lock-qualification.json"))
    except Exception as exc:
        result.update(error=type(exc).__name__, reason=str(exc))
        raise
    finally:
        try:
            if started:
                for identifier in command(stack + ["ps", "-aq"]).decode().split():
                    info = json.loads(command(["docker", "inspect", identifier]))[0]
                    labels = info["Config"]["Labels"] or {}
                    if labels.get("io.xuanming.interop.run") != run_id or labels.get("com.docker.compose.project") != project:
                        raise RuntimeError("unowned Docker container; refusing cleanup")
                command(stack + ["down", "--volumes", "--remove-orphans"], timeout=120)
                result["cleanup"] = not bool(command(stack + ["ps", "-aq"]).strip())
                if not result["cleanup"]:
                    raise RuntimeError("test containers remain after cleanup")
        except BaseException as exc:
            result.update(ok=False, cleanup=False, cleanup_error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            (work / "tidb-full-acceptance.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"TiDB full-chain result: {work / 'tidb-full-acceptance.json'}", flush=True)


if __name__ == "__main__":
    main()
