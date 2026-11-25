"""验证纯标准库多进程监督器、进程表配置与 Windows 一键启动脚本。"""

from __future__ import annotations

import ast
import importlib.util
import json
import os
import re
import socket
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SUPERVISOR_PATH = ROOT / "tools" / "supervisor.py"
MAIN_PATH = ROOT / "src" / "xuanming" / "__main__.py"
LOCAL_PROFILE_PATH = ROOT / "config" / "processes.local.json"
CLUSTER_PROFILE_PATH = ROOT / "config" / "processes.cluster.json"
LOCAL_TOML_PATH = ROOT / "config" / "local.toml"
CLUSTER_TOML_PATH = ROOT / "config" / "cluster.example.toml"
PORTAL_TOML_PATH = ROOT / "config" / "portal.example.toml"


def _load_supervisor_module():
    spec = importlib.util.spec_from_file_location("xuanming_supervisor_test", SUPERVISOR_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _free_tcp_ports(count: int) -> list[int]:
    """一次申请多个互不重复的本机空闲 TCP 端口，用于隔离启动真实节点。"""
    sockets: list[socket.socket] = []
    ports: list[int] = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            ports.append(sock.getsockname()[1])
            sockets.append(sock)
    finally:
        for sock in sockets:
            sock.close()
    assert len(set(ports)) == count
    return ports


def _extract_main_subcommands() -> tuple[set[str], set[str]]:
    """从 src/xuanming/__main__.py 提取全部已注册子命令与 run 角色。"""
    source = MAIN_PATH.read_text(encoding="utf-8")
    subcommands = set(re.findall(r'commands\.add_parser\(\s*"([^"]+)"', source))
    role_match = re.search(r'server\.add_argument\(\s*"role"\s*,\s*choices=\(([^)]+)\)', source)
    assert role_match is not None, "failed to locate run role choices in __main__.py"
    run_roles = set(re.findall(r'"([^"]+)"', role_match.group(1)))
    return subcommands, run_roles


def test_supervisor_uses_stdlib_only() -> None:
    """监督器必须在未安装第三方依赖前即可运行，严格仅允许导入 Python 标准库。"""
    tree = ast.parse(SUPERVISOR_PATH.read_text(encoding="utf-8"), filename=str(SUPERVISOR_PATH))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    non_stdlib = sorted(name for name in imported if name not in sys.stdlib_module_names)
    assert non_stdlib == [], f"tools/supervisor.py must use stdlib only, found: {non_stdlib}"


def test_local_profile_matches_local_toml_and_main() -> None:
    """校验 config/processes.local.json 的命令、端口、环境变量默认值及无冲突。"""
    sup = _load_supervisor_module()
    subcommands, run_roles = _extract_main_subcommands()
    local_toml = tomllib.loads(LOCAL_TOML_PATH.read_text(encoding="utf-8"))
    profile = json.loads(LOCAL_PROFILE_PATH.read_text(encoding="utf-8"))

    assert profile["profile"] == "local"
    assert profile["storage"] == "json"
    assert profile["requires"] == []

    requires, processes = sup.validate_profile(profile, sys.executable)
    assert requires == []
    assert len(processes) == 1

    proc = processes[0]
    assert proc.name == "xuanming-local"
    assert proc.host == "127.0.0.1"
    assert proc.port == local_toml["gate"]["port"] == 8610
    assert set(proc.extra_ports) == {
        local_toml["scene"]["port"],
        local_toml["scene"]["grpc_port"],
        local_toml["battle"]["port"],
        local_toml["battle"]["grpc_port"],
    }
    assert proc.command[1:] == ["-m", "xuanming", "run", "local", "--config", "config/local.toml"]
    assert "run" in subcommands
    assert "local" in run_roles

    # 未设置 XUANMING_DEV_LOGIN_TOKEN 时使用默认值；外部已设置时保留外部值。
    managed = sup.ManagedProcess(proc, ROOT / "run" / "logs")
    default_env = managed.build_env({})
    assert default_env["XUANMING_DEV_LOGIN_TOKEN"] == "dev-local-token"
    override_env = managed.build_env({"XUANMING_DEV_LOGIN_TOKEN": "custom-token"})
    assert override_env["XUANMING_DEV_LOGIN_TOKEN"] == "custom-token"


def test_cluster_profile_ports_and_subcommands_match_configs() -> None:
    """校验 config/processes.cluster.json 的全部 24 个进程、端口无碰撞且与配置对齐。"""
    sup = _load_supervisor_module()
    subcommands, run_roles = _extract_main_subcommands()
    cluster_toml = tomllib.loads(CLUSTER_TOML_PATH.read_text(encoding="utf-8"))
    portal_toml = tomllib.loads(PORTAL_TOML_PATH.read_text(encoding="utf-8"))
    profile = json.loads(CLUSTER_PROFILE_PATH.read_text(encoding="utf-8"))

    assert profile["profile"] == "cluster"
    assert profile["storage"] == "mysql"

    requires, processes = sup.validate_profile(profile, sys.executable)
    assert {(d.name, d.host, d.port) for d in requires} == {
        ("mysql", "127.0.0.1", 3306),
        ("redis", "127.0.0.1", 6379),
        ("etcd", "127.0.0.1", 2379),
        ("kafka", "127.0.0.1", 9092),
    }

    expected_ports = {
        "data-service": cluster_toml["data_service_node"]["grpc_port"],
        "db": cluster_toml["db_node"]["grpc_port"],
        "player-locator": cluster_toml["player_locator_node"]["grpc_port"],
        "player-inbox": cluster_toml["player_inbox_node"]["grpc_port"],
        "router": cluster_toml["router_node"]["grpc_port"],
        "scene-manager": cluster_toml["scene_manager_node"]["grpc_port"],
        "match": cluster_toml["match_node"]["grpc_port"],
        "content-guard": cluster_toml["content_guard_node"]["grpc_port"],
        "chat": cluster_toml["chat_node"]["grpc_port"],
        "mail": cluster_toml["mail_node"]["grpc_port"],
        "rank": cluster_toml["rank_node"]["grpc_port"],
        "friend": cluster_toml["friend_node"]["grpc_port"],
        "payment": cluster_toml["payment_node"]["grpc_port"],
        "trade": cluster_toml["trade_node"]["grpc_port"],
        "guild": cluster_toml["guild_node"]["grpc_port"],
        "mall-game": cluster_toml["mall_game_node"]["port"],
        "gm-admin": cluster_toml["gm_admin_node"]["port"],
        "config-node": cluster_toml["config_node"]["port"],
        "satoken-auth": cluster_toml["satoken_auth_node"]["port"],
        "portal": portal_toml["portal"]["port"],
        "login": cluster_toml["login"]["grpc_port"],
        "scene": cluster_toml["scene"]["port"],
        "battle": cluster_toml["battle"]["port"],
        "gate": cluster_toml["gate"]["port"],
    }

    assert {p.name for p in processes} == set(expected_ports)
    all_ports: list[int] = []
    for proc in processes:
        assert proc.port == expected_ports[proc.name], f"port mismatch for {proc.name}"
        all_ports.extend(proc.all_ports())
        # 校验命令行子命令均在 __main__.py 中真实存在。
        assert proc.command[1:3] == ["-m", "xuanming"]
        cmd_name = proc.command[3]
        assert cmd_name in subcommands, f"unknown subcommand {cmd_name!r} for {proc.name}"
        if cmd_name == "run":
            assert proc.command[4] in run_roles, f"unknown run role {proc.command[4]!r} for {proc.name}"
    assert len(all_ports) == len(set(all_ports)), "duplicate listen ports detected in cluster profile"


def test_validate_profile_rejects_duplicates_and_bad_commands() -> None:
    """校验进程名重复、端口冲突、非法命令均会快速失败。"""
    sup = _load_supervisor_module()

    with pytest.raises(ValueError, match="duplicate process name"):
        sup.validate_profile(
            {
                "processes": [
                    {"name": "dup", "command": ["-m", "xuanming", "run", "local"], "port": 9001},
                    {"name": "dup", "command": ["-m", "xuanming", "run", "local"], "port": 9002},
                ]
            },
            sys.executable,
        )

    with pytest.raises(ValueError, match="duplicate listen port"):
        sup.validate_profile(
            {
                "processes": [
                    {"name": "a", "command": ["-m", "xuanming", "run", "gate"], "port": 8610, "extra_ports": [8615]},
                    {"name": "b", "command": ["-m", "xuanming", "run", "scene"], "port": 8615},
                ]
            },
            sys.executable,
        )

    with pytest.raises(ValueError, match="must specify"):
        sup.validate_profile({"processes": [{"name": "broken", "port": 9001}]}, sys.executable)


def test_windows_batch_launchers_exist_and_contain_expected_entrypoints() -> None:
    """校验 3 个 Windows 一键启动 .bat 文件存在并包含预期入口与参数。"""
    single = ROOT / "启动玄冥服务器.bat"
    local_sup = ROOT / "启动-本地存档.bat"
    cluster_sup = ROOT / "启动-多节点集群.bat"

    for path in (single, local_sup, cluster_sup):
        assert path.exists(), f"missing launcher: {path}"

    single_text = single.read_text(encoding="utf-8")
    assert 'pushd "%~dp0"' in single_text
    assert ".venv\\Scripts\\python.exe" in single_text
    assert "where py" in single_text
    assert "where python" in single_text
    assert "sys.version_info >= (3, 11)" in single_text
    assert "XUANMING_DEV_LOGIN_TOKEN=dev-local-token" in single_text
    assert "run\\logs\\server-console.log" in single_text
    assert "Get-Content -Wait -Tail 50" in single_text
    assert "-m xuanming run local --config config\\local.toml" in single_text
    assert "Get-Content -Tail 40" in single_text
    assert "pause" in single_text

    local_text = local_sup.read_text(encoding="utf-8")
    assert ".venv\\Scripts\\python.exe" in local_text
    assert "tools\\supervisor.py" in local_text
    assert "--profile local" in local_text
    assert "pause" in local_text

    cluster_text = cluster_sup.read_text(encoding="utf-8")
    assert ".venv\\Scripts\\python.exe" in cluster_text
    assert "tools\\supervisor.py" in cluster_text
    assert "--profile cluster" in cluster_text
    assert "pause" in cluster_text


@pytest.mark.parametrize(
    ("profile_name", "expected_count", "expected_requires"),
    [
        ("local", 1, 0),
        ("cluster", 24, 4),
    ],
)
def test_supervisor_dry_run_json(profile_name: str, expected_count: int, expected_requires: int) -> None:
    """验证 supervisor.py --dry-run --json 对 local 与 cluster 均返回合法计划并退出 0。"""
    completed = subprocess.run(
        [sys.executable, str(SUPERVISOR_PATH), "--profile", profile_name, "--dry-run", "--json"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["mode"] == "dry-run"
    assert payload["profile"] == profile_name
    assert len(payload["processes"]) == expected_count
    assert len(payload["requires"]) == expected_requires


def test_supervisor_check_requires_fail_and_pass(tmp_path: Path) -> None:
    """验证 --check-requires 能精准区分不可达端口与真实监听端口。"""
    closed_port = _free_tcp_ports(1)[0]
    cfg_fail = tmp_path / "req-fail.json"
    cfg_fail.write_text(
        json.dumps(
            {
                "profile": "req-test",
                "storage": "mysql",
                "requires": [{"name": "closed-dep", "host": "127.0.0.1", "port": closed_port}],
                "processes": [{"name": "dummy", "command": ["-c", "pass"], "port": None}],
            }
        ),
        encoding="utf-8",
    )
    fail_res = subprocess.run(
        [sys.executable, str(SUPERVISOR_PATH), "--config", str(cfg_fail), "--check-requires", "--json"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        check=False,
    )
    assert fail_res.returncode == 1
    fail_payload = json.loads(fail_res.stdout)
    assert fail_payload["ok"] is False
    assert fail_payload["requires"][0]["reachable"] is False

    # 真实绑定监听端口后再测 --check-requires，应返回 0 且 reachable=True。
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(4)
        open_port = listener.getsockname()[1]
        cfg_ok = tmp_path / "req-ok.json"
        cfg_ok.write_text(
            json.dumps(
                {
                    "profile": "req-test",
                    "storage": "mysql",
                    "requires": [{"name": "live-dep", "host": "127.0.0.1", "port": open_port}],
                    "processes": [{"name": "dummy", "command": ["-c", "pass"], "port": None}],
                }
            ),
            encoding="utf-8",
        )
        ok_res = subprocess.run(
            [sys.executable, str(SUPERVISOR_PATH), "--config", str(cfg_ok), "--check-requires", "--json"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=15,
            check=False,
        )
    assert ok_res.returncode == 0, ok_res.stderr
    ok_payload = json.loads(ok_res.stdout)
    assert ok_payload["ok"] is True
    assert ok_payload["requires"][0]["reachable"] is True


def test_supervisor_real_parallel_spawn_ready_and_graceful_stop(tmp_path: Path) -> None:
    """真实端到端测试：并行拉起真实 xuanming run local 与辅助 TCP 进程，探测真实端口就绪并优雅停机。"""
    gate_port, scene_port, scene_grpc, battle_port, battle_grpc, sidecar_port = _free_tcp_ports(6)
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    log_dir = tmp_path / "logs"

    temp_toml = tmp_path / "local.toml"
    temp_toml.write_text(
        "\n".join(
            [
                "zone_id = 1",
                f'database = "{(run_dir / "players.sqlite3").as_posix()}"',
                f'local_player_store = "{(run_dir / "players.json").as_posix()}"',
                f'anomaly_journal = "{(run_dir / "asset-anomaly.json").as_posix()}"',
                "",
                "[gate]",
                'host = "127.0.0.1"',
                f"port = {gate_port}",
                "",
                "[scene]",
                'host = "127.0.0.1"',
                f"port = {scene_port}",
                f"grpc_port = {scene_grpc}",
                "",
                "[battle]",
                'host = "127.0.0.1"',
                f"port = {battle_port}",
                f"grpc_port = {battle_grpc}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    # 轻量标准库 TCP 伴生进程，验证多进程并行 spawn、并行 TCP 探测与优雅退出。
    sidecar_script = tmp_path / "sidecar.py"
    sidecar_script.write_text(
        "\n".join(
            [
                "import signal, socket, sys, time",
                "stop = False",
                "def _stop(*_a):",
                "    global stop",
                "    stop = True",
                "signal.signal(signal.SIGINT, _stop)",
                "for name in ('SIGTERM', 'SIGBREAK'):",
                "    sig = getattr(signal, name, None)",
                "    if sig is not None:",
                "        signal.signal(sig, _stop)",
                "srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)",
                "srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)",
                f"srv.bind(('127.0.0.1', {sidecar_port}))",
                "srv.listen(8)",
                "srv.settimeout(0.1)",
                "print('sidecar ready', flush=True)",
                "try:",
                "    while not stop:",
                "        try:",
                "            conn, _ = srv.accept()",
                "            conn.close()",
                "        except TimeoutError:",
                "            pass",
                "finally:",
                "    srv.close()",
            ]
        ),
        encoding="utf-8",
    )

    temp_profile = tmp_path / "processes.temp.json"
    temp_profile.write_text(
        json.dumps(
            {
                "profile": "local-e2e",
                "storage": "json",
                "requires": [],
                "processes": [
                    {
                        "name": "xuanming-local",
                        "command": ["-m", "xuanming", "run", "local", "--config", str(temp_toml)],
                        "host": "127.0.0.1",
                        "port": gate_port,
                        "extra_ports": [scene_port, scene_grpc, battle_port, battle_grpc],
                        "env_defaults": {"XUANMING_DEV_LOGIN_TOKEN": "dev-local-token"},
                    },
                    {
                        "name": "tcp-sidecar",
                        "script": str(sidecar_script),
                        "host": "127.0.0.1",
                        "port": sidecar_port,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    # 显式移除 XUANMING_DEV_LOGIN_TOKEN，验证 processes env_defaults 自动补齐。
    env = dict(os.environ)
    env.pop("XUANMING_DEV_LOGIN_TOKEN", None)

    completed = subprocess.run(
        [
            sys.executable,
            str(SUPERVISOR_PATH),
            "--config",
            str(temp_profile),
            "--log-dir",
            str(log_dir),
            "--once",
            "--json",
            "--grace-seconds",
            "5",
        ],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, f"stdout={completed.stdout}\nstderr={completed.stderr}"
    payload = json.loads(completed.stdout)
    assert payload["mode"] == "once"
    assert payload["ok"] is True
    assert payload["total_ms"] >= 0

    by_name = {item["name"]: item for item in payload["processes"]}
    assert set(by_name) == {"xuanming-local", "tcp-sidecar"}
    for name in ("xuanming-local", "tcp-sidecar"):
        assert by_name[name]["ready"] is True
        assert by_name[name]["boot_ms"] >= 0
        assert Path(by_name[name]["log_path"]).exists()

    # 验证停机后 Gate 端口与伴生端口均已彻底释放（无残留进程）。
    for port in (gate_port, sidecar_port):
        with pytest.raises(OSError):
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                pass


def test_supervisor_skip_requires_and_portless_grace(tmp_path: Path) -> None:
    """验证 --skip-requires 跳过外部探测，且 port=null 进程按 grace_ms 判定就绪。"""
    closed_port = _free_tcp_ports(1)[0]
    cfg = tmp_path / "portless.json"
    cfg.write_text(
        json.dumps(
            {
                "profile": "portless-test",
                "storage": "json",
                "requires": [{"name": "offline-db", "host": "127.0.0.1", "port": closed_port}],
                "processes": [
                    {
                        "name": "worker",
                        "command": ["-c", "import time; time.sleep(10)"],
                        "port": None,
                        "grace_ms": 150,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    # 不加 --skip-requires 时因依赖不可达而快速失败。
    blocked = subprocess.run(
        [sys.executable, str(SUPERVISOR_PATH), "--config", str(cfg), "--log-dir", str(tmp_path / "logs"), "--once", "--json"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        check=False,
    )
    assert blocked.returncode == 1
    assert json.loads(blocked.stdout)["ok"] is False

    # 加 --skip-requires 后跳过探测，无端口进程存活满 grace_ms 后判定就绪并优雅停机。
    passed = subprocess.run(
        [
            sys.executable,
            str(SUPERVISOR_PATH),
            "--config",
            str(cfg),
            "--log-dir",
            str(tmp_path / "logs"),
            "--skip-requires",
            "--once",
            "--json",
            "--grace-seconds",
            "3",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        check=False,
    )
    assert passed.returncode == 0, passed.stderr
    payload = json.loads(passed.stdout)
    assert payload["ok"] is True
    assert payload["processes"][0]["ready"] is True

