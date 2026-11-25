"""玄冥 Python 服务端并行多进程监督器（纯标准库实现）。

设计目标（参考单机与多节点运维实践）：
1. 仅依赖 Python 3.11+ 标准库，在创建 `.venv` 之前或之后均可直接运行；
2. 进程表（`config/processes.<profile>.json`）作为唯一事实源，启动前校验进程名唯一、
   监听端口无冲突、命令合法，并按需探测外部基础设施（MySQL/Redis/etcd/Kafka）；
3. 并行拉起全部受管进程，避免串行冷启动累积耗时，每个进程独立输出到 `run/logs/<name>.log`；
4. 以真实 TCP 端口连通性（`socket.create_connection`）作为就绪判据，而非仅判断父进程存活；
5. 守护循环采用指数退避重启（`RESTART_BACKOFF`），稳定运行 `HEALTHY_AFTER` 秒后清零失败计数；
6. 优雅停机：Windows 发送 `CTRL_BREAK_EVENT`，POSIX 向进程组发送 `SIGTERM`，
   等待 `GRACE_SECONDS` 后再兜底强制清理整棵进程树。
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

# 就绪探测参数：每 REPROBE_MS 毫秒试连一次，最多等待 READY_TIMEOUT 秒。
READY_TIMEOUT = 20.0
REPROBE_MS = 40
# 重启退避序列（秒）：连续失败越多等待越长，最后一档封顶。
RESTART_BACKOFF = (0.5, 1.0, 2.0, 5.0, 15.0, 30.0)
# 进程就绪并稳定运行超过该秒数后，认为恢复健康并重置失败计数。
HEALTHY_AFTER = 30.0
# 优雅停机等待上限（秒），超时后强制终止进程树。
GRACE_SECONDS = 30.0

IS_WIN = os.name == "nt"


def _now() -> float:
    return time.monotonic()


def _log(msg: str, *, json_mode: bool = False) -> None:
    """输出监督器运行日志（JSON 模式下写 stderr，保证 stdout 仅输出纯 JSON）。"""
    stream = sys.stderr if json_mode else sys.stdout
    stream.write("[sup] " + msg + "\n")
    stream.flush()


def _probe_tcp(host: str, port: int, timeout: float = 0.25) -> bool:
    """通过真实 TCP 握手探测端口是否已监听。"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def pick_python(root: Path = ROOT) -> str:
    """按 `.venv -> 当前解释器` 顺序定位 Python 可执行文件。"""
    venv_python = (
        root / ".venv" / "Scripts" / "python.exe"
        if IS_WIN
        else root / ".venv" / "bin" / "python"
    )
    if venv_python.exists():
        return str(venv_python)
    return sys.executable or "python"


def _is_valid_port(value: Any) -> bool:
    """判断端口号是否为合法的 1..65535 整数（排除 bool）。"""
    return type(value) is int and 1 <= value <= 65535


def _resolve_command(spec: dict[str, Any], python: str) -> list[str]:
    """将进程表中的 command / script / args 规格归一化为可执行参数列表。"""
    if "command" in spec and spec["command"] is not None:
        raw = spec["command"]
        if not isinstance(raw, list) or not raw or not all(isinstance(x, str) and x.strip() for x in raw):
            raise ValueError(f"process {spec.get('name')!r}: 'command' must be a non-empty list of strings")
        cmd = [python if part == "{python}" else part for part in raw]
        first = cmd[0]
        if first.startswith("-") or first.endswith(".py"):
            return [python, *cmd]
        return cmd

    if "script" in spec and spec["script"] is not None:
        script = spec["script"]
        if not isinstance(script, str) or not script.strip():
            raise ValueError(f"process {spec.get('name')!r}: 'script' must be a non-empty string")
        args = spec.get("args") or []
        if not isinstance(args, list) or not all(isinstance(x, str) for x in args):
            raise ValueError(f"process {spec.get('name')!r}: 'args' must be a list of strings")
        return [python, script, *args]

    if "args" in spec and spec["args"] is not None:
        args = spec["args"]
        if not isinstance(args, list) or not args or not all(isinstance(x, str) and x.strip() for x in args):
            raise ValueError(f"process {spec.get('name')!r}: 'args' must be a non-empty list of strings")
        return [python, *args]

    raise ValueError(f"process {spec.get('name')!r}: must specify 'command', 'script', or 'args'")


@dataclass(frozen=True)
class RequireSpec:
    """外部依赖端点规格（仅做启动前可达性探测，不负责拉起）。"""

    name: str
    host: str
    port: int


@dataclass(frozen=True)
class ProcessSpec:
    """校验后的单进程规格。"""

    name: str
    command: list[str]
    host: str
    port: int | None
    extra_ports: tuple[int, ...] = ()
    grace_ms: int = 300
    optional: bool = False
    env: dict[str, str] = field(default_factory=dict)
    env_defaults: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None

    def all_ports(self) -> list[int]:
        """返回该进程声明的全部监听端口（主端口 + 附加端口）。"""
        ports: list[int] = []
        if self.port is not None:
            ports.append(self.port)
        ports.extend(self.extra_ports)
        return ports


def validate_profile(profile: dict[str, Any], python: str) -> tuple[list[RequireSpec], list[ProcessSpec]]:
    """校验进程表结构：进程名唯一、监听端口无重叠、命令与依赖项合法。"""
    if not isinstance(profile, dict):
        raise ValueError("profile JSON root must be an object")

    raw_requires = profile.get("requires")
    if raw_requires is None:
        raw_requires = []
    if not isinstance(raw_requires, list):
        raise ValueError("'requires' must be a list")

    requires: list[RequireSpec] = []
    for idx, dep in enumerate(raw_requires):
        if not isinstance(dep, dict):
            raise ValueError(f"requires[{idx}] must be an object")
        dep_name = dep.get("name")
        if not isinstance(dep_name, str) or not dep_name.strip():
            raise ValueError(f"requires[{idx}]: 'name' must be a non-empty string")
        dep_host = dep.get("host", "127.0.0.1")
        if not isinstance(dep_host, str) or not dep_host.strip():
            raise ValueError(f"requires[{idx}] ({dep_name}): 'host' must be a non-empty string")
        dep_port = dep.get("port")
        if not _is_valid_port(dep_port):
            raise ValueError(f"requires[{idx}] ({dep_name}): invalid port {dep_port!r}")
        requires.append(RequireSpec(name=dep_name.strip(), host=dep_host.strip(), port=dep_port))

    raw_processes = profile.get("processes")
    if not isinstance(raw_processes, list) or not raw_processes:
        raise ValueError("'processes' must be a non-empty list")

    seen_names: set[str] = set()
    port_owners: dict[int, str] = {}
    processes: list[ProcessSpec] = []

    for idx, item in enumerate(raw_processes):
        if not isinstance(item, dict):
            raise ValueError(f"processes[{idx}] must be an object")
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"processes[{idx}]: 'name' must be a non-empty string")
        name = name.strip()
        if name in seen_names:
            raise ValueError(f"duplicate process name: {name!r}")
        seen_names.add(name)

        command = _resolve_command(item, python)

        host = item.get("host", "127.0.0.1")
        if not isinstance(host, str) or not host.strip():
            raise ValueError(f"process {name!r}: 'host' must be a non-empty string")
        host = host.strip()

        port = item.get("port")
        if port is not None and not _is_valid_port(port):
            raise ValueError(f"process {name!r}: invalid port {port!r}")

        raw_extra = item.get("extra_ports") or []
        if not isinstance(raw_extra, list) or any(not _is_valid_port(p) for p in raw_extra):
            raise ValueError(f"process {name!r}: 'extra_ports' must be a list of valid port integers")
        extra_ports = tuple(raw_extra)

        # 校验进程内与跨进程监听端口不重叠。
        for listen_port in ([port] if port is not None else []) + list(extra_ports):
            existing_owner = port_owners.get(listen_port)
            if existing_owner is not None:
                raise ValueError(
                    f"duplicate listen port {listen_port}: used by both {existing_owner!r} and {name!r}"
                )
            port_owners[listen_port] = name

        grace_ms = item.get("grace_ms", 300)
        if type(grace_ms) is not int or grace_ms < 0:
            raise ValueError(f"process {name!r}: 'grace_ms' must be a non-negative integer")

        optional = item.get("optional", False)
        if not isinstance(optional, bool):
            raise ValueError(f"process {name!r}: 'optional' must be a boolean")

        env = _parse_env_map(item.get("env"), name, "env")
        env_defaults = _parse_env_map(item.get("env_defaults"), name, "env_defaults")

        cwd = item.get("cwd")
        if cwd is not None and (not isinstance(cwd, str) or not cwd.strip()):
            raise ValueError(f"process {name!r}: 'cwd' must be a non-empty string when provided")

        processes.append(
            ProcessSpec(
                name=name,
                command=command,
                host=host,
                port=port,
                extra_ports=extra_ports,
                grace_ms=grace_ms,
                optional=optional,
                env=env,
                env_defaults=env_defaults,
                cwd=cwd.strip() if cwd else None,
            )
        )

    return requires, processes


def _parse_env_map(raw: Any, proc_name: str, field_name: str) -> dict[str, str]:
    """校验进程表中的环境变量字典。"""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"process {proc_name!r}: '{field_name}' must be an object")
    out: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not k.strip() or not isinstance(v, str):
            raise ValueError(f"process {proc_name!r}: '{field_name}' entries must be string -> string")
        out[k] = v
    return out


def load_profile(
    profile_name: str,
    config_path: Path | None = None,
    *,
    root: Path = ROOT,
) -> tuple[dict[str, Any], Path]:
    """加载指定配置路径或 `config/processes.<profile>.json`。"""
    if config_path is not None:
        path = config_path if config_path.is_absolute() else (Path.cwd() / config_path)
        if not path.exists() and not config_path.is_absolute():
            alt = root / config_path
            if alt.exists():
                path = alt
        if not path.exists():
            raise FileNotFoundError(f"process config not found: {path}")
    else:
        path = root / "config" / f"processes.{profile_name}.json"
        if not path.exists():
            available = sorted(p.name for p in (root / "config").glob("processes.*.json"))
            hint = ", ".join(available) if available else "(none)"
            raise FileNotFoundError(f"process profile not found: {path} (available: {hint})")

    data = json.loads(path.read_text(encoding="utf-8"))
    return data, path.resolve()


def probe_requires(requires: list[RequireSpec], timeout: float = 0.8) -> list[dict[str, Any]]:
    """探测全部外部基础设施依赖，返回逐项结果列表。"""
    results: list[dict[str, Any]] = []
    for dep in requires:
        reachable = _probe_tcp(dep.host, dep.port, timeout=timeout)
        results.append(
            {
                "name": dep.name,
                "host": dep.host,
                "port": dep.port,
                "reachable": reachable,
            }
        )
    return results


def _signal_stop(proc: subprocess.Popen[Any]) -> bool:
    """发送可捕获的优雅停机信号：Windows 发 CTRL_BREAK_EVENT，POSIX 向进程组发 SIGTERM。"""
    try:
        if IS_WIN:
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        return True
    except (OSError, ValueError, AttributeError):
        return False


def _kill_tree(proc: subprocess.Popen[Any], grace: float = GRACE_SECONDS, *, json_mode: bool = False) -> None:
    """优雅停机并兜底清理整棵进程树（防止 trampoline 子进程残留占端口）。"""
    if proc.poll() is not None:
        return
    if _signal_stop(proc):
        try:
            proc.wait(timeout=grace)
            return
        except subprocess.TimeoutExpired:
            _log(f"Graceful shutdown timed out after {grace:.1f}s; forcing kill pid={proc.pid}", json_mode=json_mode)

    if IS_WIN:
        subprocess.run(
            ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except OSError:
            proc.kill()

    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()


class ManagedProcess:
    """单个受管进程的运行时状态与生命周期控制器。"""

    def __init__(self, spec: ProcessSpec, log_dir: Path, *, root: Path = ROOT, json_mode: bool = False) -> None:
        self.spec = spec
        self.root = root
        self.json_mode = json_mode
        self.log_path = log_dir / f"{spec.name}.log"
        self.proc: subprocess.Popen[Any] | None = None
        self.handle: Any = None
        self.fails = 0
        self.started_at = 0.0
        self.ready_at = 0.0
        self.next_try = 0.0
        self.pending_restart = False

    def build_env(self, base_env: dict[str, str] | None = None) -> dict[str, str]:
        """构造子进程环境变量：先补齐 UTF-8 与未设置默认值，再应用显式覆盖。"""
        env = dict(os.environ if base_env is None else base_env)
        env.setdefault("PYTHONUTF8", "1")
        env.setdefault("PYTHONIOENCODING", "utf-8")
        for key, value in self.spec.env_defaults.items():
            if not env.get(key):
                env[key] = value
        env.update(self.spec.env)
        return env

    def _resolve_cwd(self) -> str:
        if not self.spec.cwd:
            return str(self.root)
        path = Path(self.spec.cwd)
        return str(path if path.is_absolute() else (self.root / path))

    def spawn(self) -> None:
        """启动子进程并将 stdout/stderr 合并重定向至独立日志文件。"""
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = open(self.log_path, "a", encoding="utf-8", errors="replace")
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        self.handle.write(f"\n===== [{stamp}] spawn {self.spec.name} =====\n")
        self.handle.flush()

        kwargs: dict[str, Any] = {}
        if IS_WIN:
            # 独立进程组：确保父进程能精准投递 CTRL_BREAK_EVENT，且 Ctrl+C 由监督器统一编排。
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True

        self.proc = subprocess.Popen(
            self.spec.command,
            cwd=self._resolve_cwd(),
            stdout=self.handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=self.build_env(),
            **kwargs,
        )
        self.started_at = _now()
        self.ready_at = 0.0
        self.pending_restart = False

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def wait_ready(self, deadline: float) -> bool:
        """等待进程就绪：有监听端口则真实 TCP 探测，无端口则等待 grace_ms 确认未早退。"""
        if self.spec.port is None:
            grace_sec = self.spec.grace_ms / 1000.0
            while _now() - self.started_at < grace_sec:
                if not self.alive():
                    return False
                time.sleep(REPROBE_MS / 1000.0)
            if not self.alive():
                return False
            self.ready_at = _now()
            return True

        while _now() < deadline:
            if not self.alive():
                return False
            if _probe_tcp(self.spec.host, self.spec.port):
                self.ready_at = _now()
                return True
            time.sleep(REPROBE_MS / 1000.0)
        return False

    def stop(self, grace: float = GRACE_SECONDS) -> None:
        """停止子进程并释放日志文件句柄。"""
        if self.proc is not None:
            _kill_tree(self.proc, grace=grace, json_mode=self.json_mode)
            self.proc = None
        if self.handle is not None:
            try:
                self.handle.close()
            except OSError:
                pass
            self.handle = None

    def boot_ms(self) -> int:
        if not self.ready_at:
            return -1
        return int((self.ready_at - self.started_at) * 1000)

    def tail(self, lines: int = 8) -> str:
        try:
            content = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return ""
        if not content:
            return ""
        return "\n         " + "\n         ".join(content[-lines:])


def boot_all(
    items: list[ManagedProcess],
    timeout: float = READY_TIMEOUT,
    *,
    json_mode: bool = False,
) -> tuple[bool, int, dict[str, bool]]:
    """并行拉起全部受管进程并并发等待所有端口就绪。"""
    t0 = _now()
    for item in items:
        item.spawn()
    _log(f"Spawned {len(items)} process(es) in parallel, waiting for readiness...", json_mode=json_mode)

    deadline = t0 + timeout
    results: dict[str, bool] = {}
    threads: list[threading.Thread] = []

    for item in items:
        def _wait(target: ManagedProcess = item) -> None:
            results[target.spec.name] = target.wait_ready(deadline)

        th = threading.Thread(target=_wait, daemon=True)
        th.start()
        threads.append(th)

    for th in threads:
        th.join()

    total_ms = int((_now() - t0) * 1000)
    all_ok = True
    for item in items:
        where = f"{item.spec.host}:{item.spec.port}" if item.spec.port is not None else "(no-port)"
        if results.get(item.spec.name, False):
            _log(f"  [OK]   {item.spec.name:<18s} {item.boot_ms():5d} ms -> {where}", json_mode=json_mode)
        elif item.spec.optional:
            _log(f"  [SKIP] {item.spec.name:<18s} not ready (optional)", json_mode=json_mode)
        else:
            all_ok = False
            _log(
                f"  [FAIL] {item.spec.name:<18s} not ready, inspect {item.log_path}{item.tail()}",
                json_mode=json_mode,
            )
    status = "all ready" if all_ok else "with failures"
    _log(f"Boot finished in {total_ms} ms ({status})", json_mode=json_mode)
    return all_ok, total_ms, results


def watch_loop(
    items: list[ManagedProcess],
    *,
    restart: bool = True,
    ready_timeout: float = READY_TIMEOUT,
    grace_seconds: float = GRACE_SECONDS,
    stop_event: threading.Event | None = None,
    json_mode: bool = False,
) -> None:
    """守护循环：检测进程异常退出并按指数退避自动拉起，稳定运行后重置失败计数。"""
    _log("Entering watchdog loop. Press Ctrl+C to stop all processes.", json_mode=json_mode)
    reported_no_restart: set[str] = set()
    event = stop_event or threading.Event()

    while not event.is_set():
        event.wait(0.4)
        if event.is_set():
            break
        now = _now()
        for item in items:
            if item.alive():
                if item.fails and item.ready_at and (now - item.ready_at) > HEALTHY_AFTER:
                    _log(f"Process {item.spec.name} healthy for >{HEALTHY_AFTER:.0f}s; resetting failure counter", json_mode=json_mode)
                    item.fails = 0
                continue

            if not restart:
                if item.spec.name not in reported_no_restart:
                    reported_no_restart.add(item.spec.name)
                    code = item.proc.returncode if item.proc else "?"
                    _log(f"Process {item.spec.name} exited (code={code}); restart disabled", json_mode=json_mode)
                continue

            if not item.pending_restart:
                code = item.proc.returncode if item.proc else "?"
                item.fails += 1
                backoff = RESTART_BACKOFF[min(item.fails - 1, len(RESTART_BACKOFF) - 1)]
                item.next_try = now + backoff
                item.pending_restart = True
                _log(
                    f"Process {item.spec.name} exited (code={code}); retry #{item.fails} scheduled in {backoff:.1f}s",
                    json_mode=json_mode,
                )
                continue

            if now < item.next_try:
                continue

            item.stop(grace=grace_seconds)
            item.spawn()
            ok = item.wait_ready(_now() + ready_timeout)
            if ok:
                _log(f"Process {item.spec.name} restarted and ready in {item.boot_ms()} ms", json_mode=json_mode)
            else:
                _log(f"Process {item.spec.name} failed readiness check after restart", json_mode=json_mode)


def _build_plan_payload(
    profile_name: str,
    config_path: Path,
    storage: str,
    python: str,
    log_dir: Path,
    requires: list[RequireSpec],
    processes: list[ProcessSpec],
) -> dict[str, Any]:
    return {
        "mode": "dry-run",
        "profile": profile_name,
        "config_path": str(config_path),
        "storage": storage,
        "python": python,
        "log_dir": str(log_dir),
        "requires": [{"name": d.name, "host": d.host, "port": d.port} for d in requires],
        "processes": [
            {
                "name": p.name,
                "command": p.command,
                "host": p.host,
                "port": p.port,
                "extra_ports": list(p.extra_ports),
                "grace_ms": p.grace_ms,
                "optional": p.optional,
                "env": p.env,
                "env_defaults": p.env_defaults,
                "log_path": str(log_dir / f"{p.name}.log"),
            }
            for p in processes
        ],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Xuanming pure-stdlib parallel multi-process supervisor",
    )
    parser.add_argument(
        "--profile",
        default="local",
        help="Process profile name matching config/processes.<profile>.json (e.g. local, cluster)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Explicit path to a process profile JSON file (overrides --profile)",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=None,
        help="Directory for per-process log files (default: run/logs)",
    )
    parser.add_argument(
        "--skip-requires",
        action="store_true",
        help="Skip preflight TCP connectivity checks for external infrastructure dependencies",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate profile and print execution plan without spawning processes",
    )
    parser.add_argument(
        "--check-requires",
        action="store_true",
        help="Validate profile and probe external infrastructure endpoints, then exit",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Spawn all processes in parallel, wait for readiness, print summary, gracefully stop, and exit",
    )
    parser.add_argument(
        "--no-restart",
        action="store_true",
        help="Do not restart crashed processes in watchdog mode",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit structured JSON output to stdout (human logs are sent to stderr)",
    )
    parser.add_argument(
        "--ready-timeout",
        type=float,
        default=READY_TIMEOUT,
        help=f"Per-boot readiness timeout in seconds (default: {READY_TIMEOUT})",
    )
    parser.add_argument(
        "--grace-seconds",
        type=float,
        default=GRACE_SECONDS,
        help=f"Graceful shutdown timeout in seconds before forced kill (default: {GRACE_SECONDS})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    json_mode = bool(args.json)

    try:
        profile_data, config_path = load_profile(args.profile, args.config)
        python = pick_python()
        requires, processes = validate_profile(profile_data, python)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        _log(f"ERROR: {exc}", json_mode=json_mode)
        return 2

    profile_label = str(profile_data.get("profile") or args.profile)
    storage_label = str(profile_data.get("storage") or "unknown")
    log_dir = args.log_dir.resolve() if args.log_dir is not None else (ROOT / "run" / "logs")

    if args.dry_run:
        plan = _build_plan_payload(profile_label, config_path, storage_label, python, log_dir, requires, processes)
        if json_mode:
            sys.stdout.write(json.dumps(plan, ensure_ascii=False, indent=2) + "\n")
            sys.stdout.flush()
        else:
            _log(f"Dry-run validated profile={profile_label} storage={storage_label} processes={len(processes)}")
            _log(f"Python executable: {python}")
            _log(f"Log directory: {log_dir}")
            for proc_spec in processes:
                port_text = f"{proc_spec.host}:{proc_spec.port}" if proc_spec.port is not None else "(no-port)"
                _log(f"  - {proc_spec.name:<18s} {port_text:<18s} {' '.join(proc_spec.command)}")
        return 0

    if args.check_requires:
        dep_results = probe_requires(requires)
        all_reachable = all(entry["reachable"] for entry in dep_results)
        payload = {
            "mode": "check-requires",
            "profile": profile_label,
            "config_path": str(config_path),
            "ok": all_reachable,
            "requires": dep_results,
        }
        if json_mode:
            sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
            sys.stdout.flush()
        else:
            if not dep_results:
                _log(f"Profile={profile_label} declares no external infrastructure dependencies.")
            for entry in dep_results:
                tag = "OK" if entry["reachable"] else "UNREACHABLE"
                _log(f"  [{tag}] {entry['name']} @ {entry['host']}:{entry['port']}")
        return 0 if all_reachable else 1

    _log(f"profile={profile_label} storage={storage_label} processes={len(processes)}", json_mode=json_mode)
    _log(f"python={python}", json_mode=json_mode)
    _log(f"log_dir={log_dir}", json_mode=json_mode)

    if requires and not args.skip_requires:
        dep_results = probe_requires(requires)
        unreachable = [d for d in dep_results if not d["reachable"]]
        if unreachable:
            details = "\n".join(f"  - {d['name']} @ {d['host']}:{d['port']}" for d in unreachable)
            _log(
                "ERROR: Required external infrastructure is unreachable:\n"
                f"{details}\n"
                "Start the required services (MySQL/Redis/etcd/Kafka) first, pass --skip-requires to bypass, "
                "or use the local profile (--profile local).",
                json_mode=json_mode,
            )
            if json_mode:
                sys.stdout.write(
                    json.dumps(
                        {
                            "mode": "once" if args.once else "run",
                            "profile": profile_label,
                            "ok": False,
                            "error": "required infrastructure unreachable",
                            "requires": dep_results,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n"
                )
                sys.stdout.flush()
            return 1

    log_dir.mkdir(parents=True, exist_ok=True)
    items = [ManagedProcess(spec, log_dir, json_mode=json_mode) for spec in processes]
    stop_event = threading.Event()
    stopping = threading.Event()

    def shutdown() -> None:
        if stopping.is_set():
            return
        stopping.set()
        stop_event.set()
        _log("Shutting down all managed processes...", json_mode=json_mode)
        for item in reversed(items):
            item.stop(grace=args.grace_seconds)
        _log("All managed processes stopped.", json_mode=json_mode)

    def on_signal(signum: int, _frame: Any) -> None:
        _log(f"Received signal {signum}, initiating graceful shutdown...", json_mode=json_mode)
        stop_event.set()

    signal.signal(signal.SIGINT, on_signal)
    for sig_name in ("SIGTERM", "SIGBREAK"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                signal.signal(sig, on_signal)
            except (OSError, ValueError):
                pass

    try:
        ok, total_ms, ready_map = boot_all(items, timeout=args.ready_timeout, json_mode=json_mode)
        if args.once:
            summary = {
                "mode": "once",
                "profile": profile_label,
                "config_path": str(config_path),
                "ok": ok,
                "total_ms": total_ms,
                "processes": [
                    {
                        "name": item.spec.name,
                        "host": item.spec.host,
                        "port": item.spec.port,
                        "ready": bool(ready_map.get(item.spec.name, False)),
                        "optional": item.spec.optional,
                        "boot_ms": item.boot_ms(),
                        "pid": item.proc.pid if item.proc is not None else None,
                        "log_path": str(item.log_path),
                    }
                    for item in items
                ],
            }
            shutdown()
            if json_mode:
                sys.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
                sys.stdout.flush()
            return 0 if ok else 1

        if not ok:
            shutdown()
            return 1

        watch_loop(
            items,
            restart=not args.no_restart,
            ready_timeout=args.ready_timeout,
            grace_seconds=args.grace_seconds,
            stop_event=stop_event,
            json_mode=json_mode,
        )
    except KeyboardInterrupt:
        pass
    finally:
        shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
