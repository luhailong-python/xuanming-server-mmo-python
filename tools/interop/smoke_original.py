"""Isolated Windows middleware and native Go peer helpers for acceptance."""

import asyncio
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time
from contextlib import ExitStack
from urllib.request import Request, urlopen
import uuid

import grpc
from xuanming.cluster import EtcdDirectory


def required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing {name}; start this test through tools/interop/run.py")
    return Path(value).resolve(strict=True)


SCRATCH = required("XUANMING_IT_WORK")
SOURCE = required("XUANMING_IT_SOURCE")
GO = required("XUANMING_IT_BIN")
MYSQL = required("XUANMING_IT_MYSQL")
ETCD = required("XUANMING_IT_ETCD")
REDIS = required("XUANMING_IT_REDIS")
JAVA = required("XUANMING_IT_KAFKA_JAVA")
KAFKA_LIBS = required("XUANMING_IT_KAFKA_LIBS")
PYTHON_ROOT = Path(__file__).resolve().parents[2]
RUN = SCRATCH / f"run-{uuid.uuid4().hex}"
RUN.mkdir()
print(f"artifacts={RUN}", flush=True)
TEMP = RUN / "tmp"
TEMP.mkdir()
ENV = {key: value for key, value in os.environ.items()
       if not key.startswith(("MMORPG_", "LOGIN_", "GATEWAY_", "COMMERCE_"))
       and key not in {"POD_IP", "JDK_JAVA_OPTIONS", "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS"}}
ENV.update(TEMP=str(TEMP), TMP=str(TEMP))
TICKET_SECRET = secrets.token_hex(32)
ENV.update(MMORPG_ACCOUNT_DIRECTORY_SECRET_LOGIN=secrets.token_hex(32),
           MMORPG_RUN_MODE="dev", MMORPG_SQL_REQUIRE_TIDB="false")


def addresses(*names):
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in names]
        for item in sockets:
            item.bind(("127.0.0.1", 0))
        return dict(zip(names, (item.getsockname()[1] for item in sockets), strict=True))


PORT = addresses("etcd", "peer", "redis", "kafka", "controller", "mysql", "router", "login", "metrics", "gateway")
MYSQL_PROCESS = None


def endpoint(key):
    return f"127.0.0.1:{PORT[key]}"


class Processes:
    def __init__(self):
        self.processes = {}
        self.mysql_verified = False

    def start(self, name, command, *, env=None):
        directory = RUN / name
        directory.mkdir()
        log = directory / "process.log"
        with log.open("wb") as output:
            proc = subprocess.Popen([str(arg) for arg in command], cwd=directory, env=ENV if env is None else env,
                                    stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                                    creationflags=subprocess.CREATE_NO_WINDOW)
        self.processes[name] = (proc, log)
        return proc

    async def finish(self, name, timeout=90):
        proc, log = self.processes[name]
        code = await asyncio.to_thread(proc.wait, timeout)
        if code:
            raise RuntimeError(f"{name} exit={code}: {log}")

    async def ready(self, name, service=None, timeout=60):
        proc, log = self.processes[name]
        port = PORT[service or name]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"{name} exit={proc.returncode}: {log}")
            try:
                _, writer = await asyncio.open_connection("127.0.0.1", port)
            except OSError:
                await asyncio.sleep(0.2)
            else:
                writer.close()
                await writer.wait_closed()
                owner = await asyncio.to_thread(_listener_pid, port)
                child = owner is not None and _process_parents().get(owner) == proc.pid
                if owner is not None and owner != proc.pid and not child:
                    raise RuntimeError(f"refusing {name}: loopback port {port} belongs to another process")
                if owner == proc.pid or child:
                    return
                await asyncio.sleep(0.2)
        raise TimeoutError(f"{name} did not listen on {port}: {log}")

    def close(self):
        if self.mysql_verified and "mysql" in self.processes:
            proc, _ = self.processes["mysql"]
            try:
                owner = _listener_pid(PORT["mysql"])
                if proc.poll() is None and (owner == proc.pid or _process_parents().get(owner) == proc.pid):
                    subprocess.run([str(MYSQL / "bin/mysqladmin.exe"), "--no-defaults", "--protocol=TCP",
                                    "--host=127.0.0.1", f"--port={PORT['mysql']}", "--user=root", "shutdown"],
                                   cwd=RUN, env=ENV, stdin=subprocess.DEVNULL, capture_output=True,
                                   creationflags=subprocess.CREATE_NO_WINDOW, timeout=20)
            except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
                pass
        for proc, _ in reversed(tuple(self.processes.values())):
            if proc.poll() is None:
                # Windows MySQL may fork a child listener. Terminate direct
                # children first while their known parent is still alive.
                try:
                    for child, parent in _process_parents().items():
                        if parent == proc.pid:
                            try:
                                os.kill(child, signal.SIGTERM)
                            except OSError:
                                pass
                except OSError:
                    pass
                try:
                    proc.terminate()
                except OSError:
                    pass
        for proc, _ in reversed(tuple(self.processes.values())):
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(10)


def _listener_pid(port):
    result = subprocess.run(["netstat.exe", "-ano", "-p", "tcp"], capture_output=True, text=True,
                            timeout=10, creationflags=subprocess.CREATE_NO_WINDOW, check=True)
    for line in result.stdout.splitlines():
        fields = line.split()
        if (len(fields) >= 5 and fields[0].upper() == "TCP" and fields[3].upper() == "LISTENING"
                and fields[1].rsplit(":", 1)[0] in ("127.0.0.1", "0.0.0.0")
                and fields[1].rsplit(":", 1)[-1] == str(port)):
            return int(fields[4])
    return None


def _process_parents():
    class Entry(ctypes.Structure):
        _fields_ = [("size", wintypes.DWORD), ("usage", wintypes.DWORD), ("pid", wintypes.DWORD),
                    ("heap", ctypes.c_size_t), ("module", wintypes.DWORD), ("threads", wintypes.DWORD),
                    ("parent", wintypes.DWORD), ("priority", wintypes.LONG), ("flags", wintypes.DWORD),
                    ("name", wintypes.WCHAR * 260)]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel.Process32FirstW.argtypes = (wintypes.HANDLE, ctypes.POINTER(Entry))
    kernel.Process32NextW.argtypes = (wintypes.HANDLE, ctypes.POINTER(Entry))
    kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
    handle = kernel.CreateToolhelp32Snapshot(2, 0)
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        entry = Entry()
        entry.size = ctypes.sizeof(entry)
        result = {}
        ok = kernel.Process32FirstW(handle, ctypes.byref(entry))
        while ok:
            result[entry.pid] = entry.parent
            ok = kernel.Process32NextW(handle, ctypes.byref(entry))
        return result
    finally:
        kernel.CloseHandle(handle)


def run_mysql(sql):
    if MYSQL_PROCESS is None or MYSQL_PROCESS.poll() is not None:
        raise RuntimeError("refusing SQL access: isolated MySQL process is not running")
    result = subprocess.run([str(MYSQL / "bin/mysql.exe"), "--no-defaults", "--protocol=TCP",
                             "--host=127.0.0.1", f"--port={PORT['mysql']}", "--user=root"],
                            cwd=RUN, env=ENV, input=sql.encode(), capture_output=True,
                            creationflags=subprocess.CREATE_NO_WINDOW, timeout=20)
    if result.returncode:
        raise RuntimeError(result.stderr.decode(errors="replace"))
    return result.stdout


async def mysql(processes):
    global MYSQL_PROCESS
    database = RUN / "mysql-data"
    database.mkdir()
    flags = [MYSQL / "bin/mysqld.exe", "--no-defaults", f"--basedir={MYSQL}", f"--datadir={database}"]
    processes.start("mysql-init", [*flags, "--initialize-insecure", "--console"])
    await processes.finish("mysql-init")
    MYSQL_PROCESS = processes.start("mysql", [*flags, "--bind-address=127.0.0.1", f"--port={PORT['mysql']}", "--mysqlx=0", "--console"])
    await processes.ready("mysql")
    actual = (await asyncio.to_thread(run_mysql, "SELECT @@datadir;")).decode().splitlines()[-1].strip()
    if Path(actual).resolve() != database.resolve():
        raise RuntimeError("refusing SQL access: server data directory is not this isolated run")
    processes.mysql_verified = True
    sql = (PYTHON_ROOT / "config/portal_schema.sql").read_text(encoding="utf-8")
    await asyncio.to_thread(run_mysql, f"CREATE DATABASE interop; USE interop;\n{sql}\n"
                            "INSERT INTO zone_config(zone_id,name,recommended) VALUES(1,'zone-1',1);")


async def kafka(processes):
    config = RUN / "kafka.properties"
    config.write_text(f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@{endpoint('controller')}
listeners=PLAINTEXT://{endpoint('kafka')},CONTROLLER://{endpoint('controller')}
advertised.listeners=PLAINTEXT://{endpoint('kafka')}
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={(RUN / 'kafka-data').as_posix()}
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
""", encoding="utf-8")
    argv = [JAVA, "-Xmx512m", "-cp", str(KAFKA_LIBS / "*")]
    cluster_id = secrets.token_urlsafe(16)
    while cluster_id.startswith("-"):
        cluster_id = secrets.token_urlsafe(16)
    processes.start("kafka-format", [*argv, "kafka.tools.StorageTool", "format", "-t", cluster_id, "-c", config])
    await processes.finish("kafka-format", 30)
    processes.start("kafka", [*argv, "kafka.Kafka", config])
    await processes.ready("kafka")


async def wait_etcd(directory, node_type, zone, node_id=0):
    for _ in range(60):
        try:
            if node_id:
                info = await directory.lookup(node_type, zone, node_id)
                if info:
                    return info
            else:
                found = await directory.candidates(node_type, zone)
                if found:
                    return found[0]
        except grpc.aio.AioRpcError:
            pass
        await asyncio.sleep(0.2)
    raise TimeoutError(f"Missing registration type={node_type} zone={zone} node={node_id}")


def original_configs():
    router = (SOURCE / "go/client_rpc_router/etc/client_rpc_router.yaml").read_text(encoding="utf-8")
    router_path = RUN / "router.yaml"
    router_path.write_text(router.replace("127.0.0.1:2379", endpoint("etcd"))
                          .replace("127.0.0.1:50600", endpoint("router")), encoding="utf-8")
    login = (SOURCE / "go/login/etc/login.yaml").read_text(encoding="utf-8")
    for before, after in (("127.0.0.1:2379", endpoint("etcd")), ("127.0.0.1:53000", endpoint("login")),
                          ("127.0.0.1:6379", endpoint("redis")), ("127.0.0.1:9092", endpoint("kafka")),
                          ("Port: 9101", f"Port: {PORT['metrics']}"), ("../../run/snowflake", (RUN / "snowflake").as_posix()),
                          ("local-dev-gate-token-secret-0123456789abcdef", TICKET_SECRET),
                          ("local-dev-queue-token-secret-0123456789abcdef", secrets.token_hex(32)),
                          ("Mode: dev", "Mode: pro"), ("DevPasswordAuth:\n  Enabled: true", "DevPasswordAuth:\n  Enabled: false")):
        login = login.replace(before, after)
    login += f"\nTableDir: {(SOURCE / 'generated/tables').as_posix()}\n"
    login_path = RUN / "login.yaml"
    login_path.write_text(login, encoding="utf-8")
    return router_path, login_path


def http(route, body=None):
    request = Request(f"http://{endpoint('gateway')}{route}", data=None if body is None else json.dumps(body).encode(),
                      headers={"Content-Type": "application/json", "traceparent": "00-11111111111111111111111111111111-2222222222222222-01"})
    with urlopen(request, timeout=8) as response:
        return json.load(response)
