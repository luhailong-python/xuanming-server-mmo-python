"""Refuse SQL access unless endpoint belongs to this exact disposable run."""

import json
import re
import subprocess


def assert_owned_tidb(port, project, container, run_id):
    if (not re.fullmatch(r"xuanming-tidb-[a-f0-9]{16}", project)
            or not re.fullmatch(r"[a-f0-9]{32}", run_id)
            or not re.fullmatch(r"[a-f0-9]{64}", container)
            or not 1 <= port <= 65535):
        raise RuntimeError("invalid test identity")
    result = subprocess.run(["docker", "inspect", container], check=True, capture_output=True, text=True, timeout=15)
    entry = json.loads(result.stdout)[0]
    labels = entry["Config"]["Labels"] or {}
    ports = entry["NetworkSettings"]["Ports"].get("4000/tcp") or []
    if (entry["Id"] != container or not entry["State"]["Running"]
            or labels.get("com.docker.compose.project") != project
            or labels.get("com.docker.compose.service") != "tidb"
            or labels.get("io.xuanming.interop.run") != run_id
            or project + "_default" not in entry["NetworkSettings"]["Networks"]
            or not any(item.get("HostIp") == "127.0.0.1" and item.get("HostPort") == str(port) for item in ports)):
        raise RuntimeError("TiDB is not the isolated instance created by this run")
