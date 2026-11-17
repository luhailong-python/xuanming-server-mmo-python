"""Verify that the SQL endpoint belongs only to this disposable TiDB."""

import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tidb_interop"))
from ownership import assert_owned_tidb


def assert_exclusive_tidb(port, project, container, run_id):
    assert_owned_tidb(port, project, container, run_id)
    result = subprocess.run(["docker", "inspect", container], capture_output=True, text=True, check=True, timeout=15)
    entry = json.loads(result.stdout)[0]
    bindings = entry["NetworkSettings"]["Ports"].get("4000/tcp") or []
    if len(bindings) != 1 or bindings[0].get("HostIp") != "127.0.0.1" or bindings[0].get("HostPort") != str(port):
        raise RuntimeError("TiDB SQL listener is not exclusively bound to this loopback port")
