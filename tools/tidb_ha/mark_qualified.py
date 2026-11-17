"""Verify same-run I3 and native evidence, then release the HA fault test."""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--qualification", type=Path, required=True)
    parser.add_argument("--native-result", type=Path, required=True)
    args = parser.parse_args()
    work = args.work_dir.resolve(strict=True)
    qualification, native = args.qualification.resolve(strict=True), args.native_result.resolve(strict=True)
    if not qualification.is_relative_to(work) or not native.is_relative_to(work):
        raise ValueError("both evidence files must belong to this run")
    marker = {"qualification_path": qualification.relative_to(work).as_posix(),
              "qualification_sha256": hashlib.sha256(qualification.read_bytes()).hexdigest(),
              "native_epoch_path": native.relative_to(work).as_posix(),
              "native_epoch_sha256": hashlib.sha256(native.read_bytes()).hexdigest(),
              "completed_at": datetime.now(timezone.utc).isoformat()}
    spec = importlib.util.spec_from_file_location("tidb_ha_runner", Path(__file__).with_name("run.py"))
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    identity = json.loads((work / "ready.json").read_text(encoding="utf-8"))
    runner.validate_qualification(work, identity, marker)
    marker_path = work / "qualification.done"
    # Never overwrite another qualification or leave a partly written marker.
    temporary = work / "qualification.done.tmp"
    with temporary.open("x", encoding="utf-8") as out:
        json.dump(marker, out, indent=2)
    temporary.rename(marker_path)
    print(f"Qualification verified: {marker_path}")


if __name__ == "__main__":
    main()
