"""Run the 64-slot local qualification on an owned disposable HA cluster."""

import argparse
import asyncio
import importlib.util
import json
import os
from pathlib import Path

from native_epoch import verify_identity
from qualify import qualify


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--identity", type=Path, required=True, help="HA runner's ready.json")
    parser.add_argument("--prepared", type=Path, required=True, help="verified native snapshot")
    parser.add_argument("--output", type=Path, required=True, help="new proof file next to ready.json")
    args = parser.parse_args()
    identity_path = args.identity.resolve(strict=True)
    output = args.output.resolve()
    if output.exists() or output.parent != identity_path.parent:
        raise ValueError("proof must be a new file in the identity's directory")
    identity = json.loads(identity_path.read_text(encoding="utf-8"))
    verify_identity(identity)
    prepared = args.prepared.resolve(strict=True)
    source = prepared / "source"
    prepare_file = Path(__file__).resolve().parents[1] / "interop/prepare.py"
    spec = importlib.util.spec_from_file_location("interop_prepare_for_qualification", prepare_file)
    prepare = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(prepare)
    manifest = json.loads((prepared / "manifest.json").read_text(encoding="utf-8"))
    if prepare.digest_tree(source) != manifest.get("source_sha256"):
        raise ValueError("native source no longer matches the prepared snapshot")
    os.environ["XUANMING_IT_SOURCE"] = str(source)
    binding = {key: identity[key] for key in ("sql_port", "run_id", "project", "container")}
    asyncio.run(qualify(identity["sql_port"], identity["run_id"], output,
                        lambda: verify_identity(identity), identity=binding))
    if prepare.digest_tree(source) != manifest["source_sha256"]:
        raise RuntimeError("native source changed during qualification")
    print(f"Local qualification: {output}")


if __name__ == "__main__":
    main()
