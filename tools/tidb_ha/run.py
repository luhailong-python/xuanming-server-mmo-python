"""Exercise single-node failures on a newly created three-replica TiDB cluster."""

import argparse
import asyncio
import ctypes
import hashlib
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib.request import urlopen

import aiomysql

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "tidb_interop"))
_spec = importlib.util.spec_from_file_location("tidb_interop_runner", ROOT / "tools/tidb_interop/run.py")
_tidb_interop = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_tidb_interop)
IMAGES = _tidb_interop.IMAGES

FLAGS = getattr(subprocess, "CREATE_NO_WINDOW", 0)
SQL_OWNERS = {}
PD_OWNERS = {}
DIAGNOSTIC_DIR = None
MIN_FREE = 4 * 1024**3


def available_memory():
    class MemoryStatus(ctypes.Structure):
        _fields_ = [("length", ctypes.c_uint32), ("load", ctypes.c_uint32),
                    ("total_phys", ctypes.c_uint64), ("avail_phys", ctypes.c_uint64),
                    ("total_page", ctypes.c_uint64), ("avail_page", ctypes.c_uint64),
                    ("total_virtual", ctypes.c_uint64), ("avail_virtual", ctypes.c_uint64),
                    ("avail_extended", ctypes.c_uint64)]
    result = MemoryStatus()
    result.length = ctypes.sizeof(result)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(result)):
        raise OSError("cannot determine available host memory")
    return result.avail_phys


def command(args, timeout=60):
    result = subprocess.run([str(value) for value in args], capture_output=True, timeout=timeout,
                            creationflags=FLAGS)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed ({result.returncode}): {result.stderr.decode(errors='replace')[-1500:]}")
    return result.stdout.decode().strip()


def check_memory(work, phase):
    free = available_memory()
    with (work / "memory-samples.jsonl").open("a", encoding="utf-8") as out:
        out.write(json.dumps({"time": time.time(), "phase": phase, "available_bytes": free}) + "\n")
    if free < MIN_FREE:
        raise RuntimeError("host free memory dropped below 4 GiB; stopping isolated test")


def start_guarded(args, work, phase, timeout=120):
    check_memory(work, phase)
    process = subprocess.Popen([str(value) for value in args], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               creationflags=FLAGS)
    deadline = time.monotonic() + timeout
    try:
        while True:
            check_memory(work, phase)
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{phase} timed out")
            try:
                stdout, stderr = process.communicate(timeout=1)
                if process.returncode:
                    raise RuntimeError(f"{phase} failed ({process.returncode}): {stderr.decode(errors='replace')[-1500:]}")
                return stdout.decode().strip()
            except subprocess.TimeoutExpired:
                continue
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=15)


def compose_text(run_id, subnet):
    label = f'    labels: ["io.xuanming.interop.run={run_id}"]\n'
    peers = ",".join(f"pd{i}=http://pd{i}:2380" for i in range(1, 4))
    lines = ["services:\n"]
    for i in range(1, 4):
        lines += [f"  pd{i}:\n    image: {IMAGES['pd']}\n", label,
                  f'    mem_limit: 450m\n    environment: ["GOMEMLIMIT=300MiB"]\n    command: ["--name=pd{i}", "--data-dir=/data/pd", '
                  f'"--client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://pd{i}:2379", '
                  f'"--peer-urls=http://0.0.0.0:2380", "--advertise-peer-urls=http://pd{i}:2380", '
                  f'"--initial-cluster={peers}", "--config=/etc/test/pd.toml"]\n'
                  '    volumes: ["./pd.toml:/etc/test/pd.toml:ro"]\n'
                  '    ports: ["127.0.0.1::2379"]\n']
    for i in range(1, 4):
        lines += [f"  tikv{i}:\n    image: {IMAGES['tikv']}\n", label,
                  f'    mem_limit: 1150m\n    command: ["--pd=pd1:2379,pd2:2379,pd3:2379", '
                  f'"--addr=0.0.0.0:20160", "--advertise-addr=tikv{i}:20160", '
                  f'"--data-dir=/data/tikv", "--config=/etc/test/tikv{i}.toml"]\n'
                  f'    volumes: ["./tikv{i}.toml:/etc/test/tikv{i}.toml:ro"]\n'
                  '    depends_on: [pd1, pd2, pd3]\n']
    for i in range(1, 3):
        lines += [f"  tidb{i}:\n    image: {IMAGES['tidb']}\n", label,
                  '    mem_limit: 1150m\n    environment: ["GOMEMLIMIT=800MiB"]\n    command: ["--store=tikv", "--path=pd1:2379,pd2:2379,pd3:2379", '
                  f'"--advertise-address=tidb{i}", "--config=/etc/test/tidb.toml"]\n'
                  '    volumes: ["./tidb.toml:/etc/test/tidb.toml:ro"]\n'
                  '    ports: ["127.0.0.1::4000"]\n'
                  '    depends_on: [tikv1, tikv2, tikv3]\n']
    if subnet:
        lines.append(f"networks:\n  default:\n    ipam:\n      config:\n        - subnet: {subnet}\n")
    return "".join(lines)


def owned_container(container, project, run_id, expected_service=None):
    info = json.loads(command(["docker", "inspect", container]))[0]
    labels = info["Config"].get("Labels") or {}
    if (info["Id"] != container or labels.get("com.docker.compose.project") != project
            or labels.get("io.xuanming.interop.run") != run_id
            or expected_service and labels.get("com.docker.compose.service") != expected_service
            or project + "_default" not in info["NetworkSettings"]["Networks"]):
        raise RuntimeError("container identity mismatch; refusing operation")
    return info


def bound_port(container, inner):
    raw = command(["docker", "port", container, f"{inner}/tcp"]).splitlines()
    if len(raw) != 1 or not raw[0].startswith("127.0.0.1:"):
        raise RuntimeError("port must have exactly one IPv4 loopback binding")
    port = int(raw[0].rsplit(":", 1)[1])
    if not 0 < port < 65536:
        raise RuntimeError("invalid test port")
    return port


def http_json(port, path):
    identity = PD_OWNERS.get(port)
    if identity is None:
        raise RuntimeError("PD port is not owned by this test")
    info = owned_container(*identity)
    if not info["State"]["Running"] or bound_port(identity[0], 2379) != port:
        raise RuntimeError("test PD container is no longer bound to this port")
    with urlopen(f"http://127.0.0.1:{port}{path}", timeout=4) as response:
        return json.load(response)


async def sql(port, query, args=()):
    identity = SQL_OWNERS.get(port)
    if identity is None:
        raise RuntimeError("SQL port is not owned by this test")
    info = await asyncio.to_thread(owned_container, *identity)
    if not info["State"]["Running"] or await asyncio.to_thread(bound_port, identity[0], 4000) != port:
        raise RuntimeError("test SQL container is no longer bound to this port")
    async with asyncio.timeout(8):
        conn = await aiomysql.connect(host="127.0.0.1", port=port, user="root", autocommit=True,
                                      connect_timeout=3)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(query, args)
                return await cursor.fetchall()
        finally:
            conn.close()


async def eventually(fn, timeout, message, interval=1):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = await fn()
            if value:
                return value
        except Exception as exc:
            last = str(exc)
        await asyncio.sleep(interval)
    raise TimeoutError(f"{message}; last error: {last}")


async def ready(pd_ports, sql_ports):
    async def check():
        cluster_ids = set()
        for port in pd_ports:
            members = await asyncio.to_thread(http_json, port, "/pd/api/v1/members")
            if {member.get("name") for member in members.get("members", [])} != {"pd1", "pd2", "pd3"}:
                return False
            cluster_ids.add(members.get("header", {}).get("cluster_id"))
        if len(cluster_ids) != 1 or not next(iter(cluster_ids)):
            return False
        for port in sql_ports:
            rows = await sql(port, "SELECT VERSION(), TIDB_VERSION()")
            if not rows or "TiDB" not in rows[0][0] or "Store: tikv" not in rows[0][1]:
                return False
        return next(iter(cluster_ids))
    return await eventually(check, 180, "test cluster did not become ready")


async def replicated(pd_port):
    async def check():
        result = await asyncio.to_thread(http_json, pd_port, "/pd/api/v1/regions")
        regions = result.get("regions") or []
        return regions if regions and all(len(r.get("peers", [])) == 3 and not r.get("pending_peers")
                                          and not r.get("down_peers") for r in regions) else False
    return await eventually(check, 180, "three healthy TiKV replicas were not observed")


async def table_region(sql_port, pd_port):
    rows = await sql(sql_port, "SHOW TABLE ha_test.ledger REGIONS")
    if len(rows) != 1:
        raise RuntimeError("cannot uniquely identify the test ledger's Region")
    region_id = int(rows[0][0])
    region = await asyncio.to_thread(http_json, pd_port, f"/pd/api/v1/region/id/{region_id}")
    if (region.get("id") != region_id or len(region.get("peers", [])) != 3
            or region.get("down_peers") or region.get("pending_peers")
            or not region.get("leader", {}).get("store_id")):
        raise RuntimeError("test ledger Region does not have three healthy peers and a leader")
    stores = await asyncio.to_thread(http_json, pd_port, "/pd/api/v1/stores")
    if DIAGNOSTIC_DIR is not None:
        with (DIAGNOSTIC_DIR / "ledger-replica-observations.jsonl").open("a", encoding="utf-8") as output:
            output.write(json.dumps({"time": time.time(), "region": region, "stores": stores}) + "\n")
    zones = {}
    for entry in stores.get("stores", []):
        store = entry.get("store", {})
        labels = {item.get("key"): item.get("value") for item in store.get("labels", [])}
        if store.get("state_name") == "Up":
            zones[store.get("id")] = labels.get("zone")
    peer_zones = {zones.get(peer.get("store_id")) for peer in region["peers"]}
    if peer_zones != {"az1", "az2", "az3"}:
        raise RuntimeError(f"ledger Region replicas do not occupy three distinct configured zones: "
                           f"peer_zones={sorted(str(x) for x in peer_zones)}, stores={zones}, peers={region['peers']}")
    return region


def validate_qualification(work, ready, marker=None):
    if marker is None:
        marker = json.loads((work / "qualification.done").read_text(encoding="utf-8"))
    def checked_file(path_field, hash_field):
        name = marker.get(path_field)
        expected = marker.get(hash_field)
        if not isinstance(name, str) or not isinstance(expected, str) or len(expected) != 64:
            raise ValueError("qualification marker lacks evidence and SHA-256")
        path = (work / name).resolve(strict=True)
        if not path.is_relative_to(work.resolve()) or not path.is_file():
            raise ValueError("qualification evidence is outside this run")
        data = path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if digest != expected:
            raise ValueError("qualification evidence SHA-256 mismatch")
        return json.loads(data), path, digest

    qualification, qual_path, qual_sha = checked_file("qualification_path", "qualification_sha256")
    native, native_path, native_sha = checked_file("native_epoch_path", "native_epoch_sha256")
    binding = {name: ready[name] for name in ("project", "run_id", "container", "sql_port")}
    if (qualification.get("format") != "LOCAL_DEV_LOCK_QUALIFICATION_V1"
            or qualification.get("backend") != "tidb" or qualification.get("cleanup") is not True
            or qualification.get("physical_binding") != binding or qualification.get("run_id") != binding["run_id"]
            or set(qualification.get("isolation_levels", ())) != {"READ COMMITTED", "REPEATABLE READ"}):
        raise ValueError("qualification is not a completed test of this isolated TiDB instance")
    evidence = qualification.get("primitive_evidence", {})
    for primitive in ("SELECT_ONLY", "LOCK_TOUCH"):
        item = evidence.get(primitive, {})
        if (item.get("primitive") != primitive or item.get("slots_tested") != 64
                or item.get("lock_wait_observed") is not True or item.get("writer_released_after_rollback") is not True
                or item.get("physical_binding_sha256") != qualification.get("physical_binding_sha256")
                or item.get("authoritative_ddl_sha256") != qualification.get("authoritative_ddl_sha256")):
            raise ValueError(f"qualification lacks complete {primitive} slot evidence")
    if (native.get("format") != "LOCAL_NATIVE_EPOCH_TEST_V1" or native.get("passed") is not True
            or native.get("cleanup") is not True or native.get("physical_binding") != binding
            or native.get("qualification_sha256") != qual_sha
            or set(native.get("primitives", ())) != {"SELECT_ONLY", "LOCK_TOUCH"}):
        raise ValueError("native epoch test is not a completed test of this qualification")
    log = native_path.parent / "native-epoch-test.log"
    if not log.is_file() or hashlib.sha256(log.read_bytes()).hexdigest() != native.get("test_log_sha256"):
        raise ValueError("native epoch test log SHA-256 mismatch")
    timestamp = datetime.fromisoformat(marker["completed_at"].replace("Z", "+00:00")).timestamp()
    if timestamp < (work / "ready.json").stat().st_mtime - 1 or timestamp > time.time() + 5:
        raise ValueError("qualification marker time is outside this run")
    return {"qualification_path": str(qual_path.relative_to(work)), "qualification_sha256": qual_sha,
            "native_epoch_path": str(native_path.relative_to(work)), "native_epoch_sha256": native_sha,
            "completed_at": marker["completed_at"]}


async def test_cluster(work, stack, project, run_id, ids, ports, handoff):
    pd_ports = [ports[f"pd{i}"] for i in range(1, 4)]
    sql_ports = [ports[f"tidb{i}"] for i in range(1, 3)]
    cluster_id = await ready(pd_ports, sql_ports)
    await sql(sql_ports[0], "SET GLOBAL tidb_enable_check_constraint=ON")
    if (await sql(sql_ports[0], "SELECT @@GLOBAL.tidb_enable_check_constraint"))[0][0] not in (1, "1", "ON"):
        raise RuntimeError("isolated TiDB CHECK constraints were not enabled")
    await sql(sql_ports[0], "CREATE DATABASE ha_test")
    await sql(sql_ports[0], "CREATE TABLE ha_test.ledger (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)")
    await sql(sql_ports[0], "INSERT INTO ha_test.ledger VALUES (1, 101)")
    await replicated(pd_ports[0])
    initial_region = await eventually(lambda: table_region(sql_ports[0], pd_ports[0]), 180,
                                      "test ledger replicas did not become healthy across all zones")
    ready_path = work / "ready.json"
    ready_path.write_text(json.dumps({"project": project, "run_id": run_id, "container": ids["tidb1"],
                                      "sql_port": sql_ports[0], "pd_ports": pd_ports, "replicas": 3,
                                      "ledger_region": initial_region["id"]}, indent=2),
                          encoding="utf-8")
    print(f"Cluster ready: {ready_path}", flush=True)
    if handoff:
        deadline = time.monotonic() + 300
        while not (work / "qualification.done").is_file():
            if time.monotonic() > deadline:
                raise TimeoutError("qualification handoff did not finish within 300 seconds")
            await asyncio.sleep(1)
        qualification = validate_qualification(work, json.loads(ready_path.read_text(encoding="utf-8")))
    else:
        qualification = None
    acknowledged = [(1, 101)]
    scenarios = []

    def change(service, action):
        container = ids[service]
        owned_container(container, project, run_id, service)
        if action == "start":
            start_guarded(["docker", action, container], work, f"restart-{service}", timeout=45)
        else:
            command(["docker", action, container], timeout=45)
        if action == "start" and service.startswith(("pd", "tidb")):
            is_pd = service.startswith("pd")
            new_port = bound_port(container, 2379 if is_pd else 4000)
            old_port = ports[service]
            ports[service] = new_port
            owners = PD_OWNERS if is_pd else SQL_OWNERS
            owners.pop(old_port, None)
            owners[new_port] = (container, project, run_id, service)
            selected = pd_ports if is_pd else sql_ports
            selected[selected.index(old_port)] = new_port

    async def run_fault(label, service, port, row, *, region_id=None, old_store=None, inflight_port=None):
        pending = None
        commit_outcome = "not_tested"
        if inflight_port:
            identity = SQL_OWNERS[inflight_port]
            info = await asyncio.to_thread(owned_container, *identity)
            if not info["State"]["Running"] or await asyncio.to_thread(bound_port, identity[0], 4000) != inflight_port:
                raise RuntimeError("in-flight transaction endpoint is not owned by this test")
            pending = await aiomysql.connect(host="127.0.0.1", port=inflight_port, user="root", autocommit=False,
                                             connect_timeout=3)
            await pending.begin()
            async with pending.cursor() as cursor:
                await cursor.execute("INSERT INTO ha_test.ledger (id,value) VALUES (%s,%s)", row)
        try:
            change(service, "stop")
        except BaseException:
            if pending:
                pending.close()
            raise
        started = time.monotonic()
        try:
            if region_id:
                async def moved():
                    region = await asyncio.to_thread(http_json, pd_ports[0], f"/pd/api/v1/region/id/{region_id}")
                    new_store = region.get("leader", {}).get("store_id")
                    return new_store if new_store and new_store != old_store else False
                new_store = await eventually(moved, 120, "ledger Region leader did not transfer")
            else:
                new_store = None
            if pending:
                if label == "tidb_instance":
                    # The server died while holding this transaction. The
                    # client outcome is unknown until a safe replay resolves it.
                    pending.close()
                    commit_outcome = "server_lost_before_commit"
                else:
                    task = asyncio.create_task(pending.commit())
                    await asyncio.sleep(0)
                    if task.done():
                        try:
                            await task
                            commit_outcome = "confirmed"
                        except Exception:
                            commit_outcome = "unknown"
                    else:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        commit_outcome = "unknown_cancelled"
                    pending.close()
                pending = None
            async def commit():
                # The ID is the idempotency key. Do not overwrite an existing
                # different value when an earlier commit had an unknown result.
                await sql(port, "INSERT INTO ha_test.ledger (id,value) VALUES (%s,%s) "
                          "ON DUPLICATE KEY UPDATE value=value", row)
                return True
            await eventually(commit, 120, f"{label}: no committed write during fault")
            actual = await sql(port, "SELECT id,value FROM ha_test.ledger WHERE id=%s", (row[0],))
            if list(actual) != [row]:
                raise AssertionError(f"{label}: replay did not preserve the intended row")
            acknowledged.append(row)
            duration = time.monotonic() - started
        finally:
            if pending:
                pending.close()
            change(service, "start")
        if await ready(pd_ports, sql_ports) != cluster_id:
            raise RuntimeError("PD cluster identity changed during fault recovery")
        await replicated(pd_ports[0])
        await eventually(lambda: table_region(sql_ports[0], pd_ports[0]), 180,
                         "test ledger replicas did not recover across all zones")
        rows = await sql(sql_ports[0], "SELECT id,value FROM ha_test.ledger ORDER BY id")
        if list(rows) != acknowledged:
            raise AssertionError(f"{label}: acknowledged rows differ after recovery")
        scenarios.append({"fault": label, "service": service, "commit_after_seconds": round(duration, 3),
                          "acknowledged_rows": len(acknowledged), "rows_verified": True,
                          "pre_fault_transaction": bool(inflight_port), "original_commit_outcome": commit_outcome,
                          "region_id": region_id, "old_leader_store": old_store, "new_leader_store": new_store})
        print(f"Verified {label}: {service}", flush=True)

    leader = await asyncio.to_thread(http_json, pd_ports[0], "/pd/api/v1/leader")
    pd_service = leader.get("name")
    if pd_service not in ("pd1", "pd2", "pd3"):
        raise RuntimeError("unexpected PD leader identity")
    await run_fault("pd_leader", pd_service, sql_ports[0], (2, 102))

    stores = await asyncio.to_thread(http_json, pd_ports[0], "/pd/api/v1/stores")
    await replicated(pd_ports[0])
    region = await eventually(lambda: table_region(sql_ports[0], pd_ports[0]), 180,
                              "could not verify the test ledger Region before its leader fault")
    chosen = region["leader"]["store_id"]
    addresses = {entry["store"]["id"]: entry["store"]["address"] for entry in stores.get("stores", [])}
    tikv_service = addresses.get(chosen, "").split(":", 1)[0]
    if tikv_service not in ("tikv1", "tikv2", "tikv3"):
        raise RuntimeError("unexpected TiKV leader identity")
    await run_fault("tikv_region_leader", tikv_service, sql_ports[0], (3, 103), region_id=region["id"],
                    old_store=chosen, inflight_port=sql_ports[0])
    await run_fault("tidb_instance", "tidb1", sql_ports[1], (4, 104), inflight_port=sql_ports[0])
    return {"scenarios": scenarios, "replica_count": 3, "pd_count": 3, "tidb_count": 2, "pd_cluster_id": cluster_id,
            "qualification": qualification,
            "version": (await sql(sql_ports[0], "SELECT VERSION()"))[0][0]}


async def guarded_test(*args):
    work = args[0]
    task = asyncio.create_task(test_cluster(*args))
    try:
        while not task.done():
            check_memory(work, "exercise")
            await asyncio.wait((task,), timeout=2)
        return await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def main():
    global DIAGNOSTIC_DIR
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True, help="new disposable output directory")
    parser.add_argument("--subnet", help="unused private IPv4 Docker subnet")
    parser.add_argument("--wait-for-qualification", action="store_true", help="wait for qualification.done before faults")
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("this test currently supports Windows only")
    if available_memory() < 6 * 1024**3:
        parser.error("at least 6 GiB of free host memory is required before starting the reduced cluster")
    subnet = None
    if args.subnet:
        parsed = ipaddress.ip_network(args.subnet)
        if parsed.version != 4 or not parsed.is_private:
            parser.error("subnet must be private IPv4")
        subnet = str(parsed)
    work = args.work_dir.resolve()
    if work.exists():
        parser.error("work directory must not already exist")
    work.mkdir(parents=True)
    DIAGNOSTIC_DIR = work
    run_id, project = secrets.token_hex(16), "xuanming-ha-" + secrets.token_hex(8)
    (work / "compose.yaml").write_text(compose_text(run_id, subnet), encoding="utf-8")
    (work / "pd.toml").write_text('[replication]\nmax-replicas = 3\nlocation-labels = ["zone"]\n', encoding="utf-8")
    for i in range(1, 4):
        (work / f"tikv{i}.toml").write_text(
            'memory-usage-limit = "850MiB"\n[server]\ngrpc-memory-pool-quota = "64MiB"\n[server.labels]\n' + f'zone = "az{i}"\n' +
            '[storage.block-cache]\ncapacity = "64MiB"\n[raft-engine]\nmemory-limit = "32MiB"\n'
            '[rocksdb.defaultcf]\nwrite-buffer-size = "16MiB"\n[rocksdb.writecf]\nwrite-buffer-size = "16MiB"\n'
            '[rocksdb.lockcf]\nwrite-buffer-size = "8MiB"\n[rocksdb.raftcf]\nwrite-buffer-size = "16MiB"\n'
            , encoding="utf-8")
    (work / "tidb.toml").write_text('[performance]\ntxn-entry-size-limit = 33554432\n', encoding="utf-8")
    stack = ["docker", "compose", "-p", project, "-f", work / "compose.yaml"]
    result = {"ok": False, "project": project, "run_id": run_id,
              "scope": "isolated single-host simulation; not production AZ certification"}
    try:
        # Check host headroom between small startup stages; do not make changes
        # to any existing services when the host is under memory pressure.
        for group in (("pd1",), ("pd2",), ("pd3",), ("tikv1",), ("tikv2",), ("tikv3",), ("tidb1",), ("tidb2",)):
            start_guarded(stack + ["up", "-d", *group], work, f"start-{group[0]}", timeout=120)
        ids = {}
        for name in ("pd1", "pd2", "pd3", "tikv1", "tikv2", "tikv3", "tidb1", "tidb2"):
            container = command(stack + ["ps", "-q", name])
            owned_container(container, project, run_id, name)
            ids[name] = container
        ports = {name: bound_port(ids[name], 2379 if name.startswith("pd") else 4000)
                 for name in ("pd1", "pd2", "pd3", "tidb1", "tidb2")}
        for name in ("tidb1", "tidb2"):
            SQL_OWNERS[ports[name]] = (ids[name], project, run_id, name)
        for name in ("pd1", "pd2", "pd3"):
            PD_OWNERS[ports[name]] = (ids[name], project, run_id, name)
        result.update(asyncio.run(guarded_test(work, stack, project, run_id, ids, ports, args.wait_for_qualification)))
        result["ok"] = True
    except BaseException as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        try:
            for service in ("pd1", "pd2", "pd3", "tikv1", "tikv2", "tikv3", "tidb1", "tidb2"):
                container = command(stack + ["ps", "-aq", service])
                if not container:
                    continue
                info = owned_container(container, project, run_id, service)
                (work / f"{service}.state.json").write_text(json.dumps(info["State"], indent=2), encoding="utf-8")
                try:
                    (work / f"{service}.log").write_text(command(["docker", "logs", "--tail", "150", container]),
                                                          encoding="utf-8")
                except Exception:
                    pass
        except Exception:
            pass
        raise
    finally:
        try:
            for container in command(stack + ["ps", "-aq"]).splitlines():
                owned_container(container, project, run_id)
            network = project + "_default"
            probe = subprocess.run(["docker", "network", "inspect", network], capture_output=True,
                                   creationflags=FLAGS, timeout=15)
            if probe.returncode == 0:
                info = json.loads(probe.stdout)[0]
                if (info.get("Labels") or {}).get("com.docker.compose.project") != project:
                    raise RuntimeError("network identity mismatch; refusing cleanup")
            command(stack + ["down", "--volumes", "--remove-orphans"], timeout=120)
            result["cleanup"] = not bool(command(stack + ["ps", "-aq"]))
            if not result["cleanup"]:
                raise RuntimeError("test containers remain")
        except BaseException as exc:
            result.update(ok=False, cleanup=False, cleanup_error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            (work / "acceptance.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
            print(f"HA result: {work / 'acceptance.json'}", flush=True)


if __name__ == "__main__":
    main()
