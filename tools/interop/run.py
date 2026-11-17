"""Run the isolated native interoperability acceptance on Windows."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from prepare import BUILDS, digest, digest_tree


def verify_prepared(prepared):
    manifest = json.loads((prepared / "manifest.json").read_text(encoding="utf-8"))
    source, binaries = prepared / "source", prepared / "bin"
    if digest_tree(source) != manifest.get("source_sha256"):
        raise RuntimeError("prepared source snapshot changed after native compilation")
    if set(manifest.get("binaries", {})) != {f"{name}.exe" for name, _, _ in BUILDS}:
        raise RuntimeError("native build manifest is incomplete")
    for name, expected in manifest["binaries"].items():
        if digest(binaries / name) != expected:
            raise RuntimeError(f"native executable changed after compilation: {name}")
    return manifest, source, binaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("prepared", "etcd", "redis", "mysql", "kafka-java", "kafka-libs", "go", "private-helper"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--direct-gate", action="store_true", help="use direct Gate routing (Team requires Router)")
    parser.add_argument("--scene-drain", action="store_true", help="exercise planned shutdown with a second Scene")
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("the native middleware harness currently supports Windows only")
    prepared = args.prepared.resolve(strict=True)
    manifest, source, binaries = verify_prepared(prepared)
    paths = {"SOURCE": source, "BIN": binaries, "WORK": prepared,
             "ETCD": args.etcd, "REDIS": args.redis, "MYSQL": args.mysql,
             "KAFKA_JAVA": args.kafka_java, "KAFKA_LIBS": args.kafka_libs,
             "GO": args.go, "PRIVATE_HELPER": args.private_helper}
    paths = {key: path.resolve(strict=True) for key, path in paths.items()}
    for name in ("mysqld.exe", "mysql.exe", "mysqladmin.exe"):
        if not (paths["MYSQL"] / "bin" / name).is_file():
            raise RuntimeError(f"missing MySQL program: {name}")
    for name in ("ETCD", "REDIS", "KAFKA_JAVA", "GO", "PRIVATE_HELPER"):
        if not paths[name].is_file():
            raise RuntimeError(f"expected an executable file: {paths[name]}")
    if not list(paths["KAFKA_LIBS"].glob("*.jar")):
        raise RuntimeError("Kafka lib directory has no jars")
    env = dict(os.environ, **{f"XUANMING_IT_{key}": str(value) for key, value in paths.items()})
    # Assertions are acceptance checks, not optional diagnostics. Force the
    # project under test and prevent inherited optimization from removing them.
    env.pop("PYTHONOPTIMIZE", None)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[2] / "src")
    env["PYTHONNOUSERSITE"] = "1"
    print(f"Testing C++/Go commit {manifest['revision']} in {prepared}", flush=True)
    command = [sys.executable, str(Path(__file__).with_name("smoke_full.py")),
               "--ingress", "python", "--scene-landing", "--monopoly-board"]
    command += ["--direct-gate"] if args.direct_gate else ["--scene-team"]
    if args.scene_drain:
        command.append("--scene-drain")
    result = subprocess.run(command, env=env)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
