"""纯 Python 部署栈（deploy/）契约测试与 3 个新增运维 CLI 子命令测试。

覆盖范围：
1. ``deploy/Dockerfile``、``deploy/docker-compose.yml``、``deploy/k8s/*.yaml`` 与 ``deploy/README.md``：
   - 零 Go/Java 镜像与工具链；
   - 非 root UID/GID 10001 与 K8s 容器安全加固项；
   - 全部 23 个纯 Python 业务服务与 ``__main__.py`` 子命令、`cluster.example.toml` / `portal.example.toml` 端口逐一对齐；
   - 密钥仅通过环境变量占位 / ``secretKeyRef`` 注入，无明文生产密钥。
2. ``kafka-dlq``：JSONL 死信信封校验（拒绝畸形/缺字段、无静默丢弃）、``inspect`` 过滤与重复位点识别、``replay-plan`` 去重及 ``unprocessable`` 默认拦截。
3. ``devpay-notify``：``--dry-run`` 签名校验不泄密 + 真实回环 ``PaymentNotifyServer``（含 ``DevPayChannel``）端到端 HTTP 投递与验签。
4. ``battle-script-review``：真实回环 ``MatchGrpcService``（``match.MatchInternal``）端到端 ``list`` / ``reject`` / ``approve`` 与 ``STALE`` / ``DENIED`` 校验。
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import tomllib
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.grpc_runtime import serve_grpc
from xuanming.login.caller_auth import CallerVerifier
from xuanming.match import (
    BattleNodeEntry,
    BattleScriptManager,
    GatherPipeline,
    GrpcEndpointPool,
    MatchConfig,
    MatchGrpcService,
    MemoryMatchStore,
    MemoryPresenceProvider,
    RankedManager,
    SpectateManager,
    StaticBattleNodeResolver,
)
from xuanming.match.model import CallerIdentity
from xuanming.ops.admin_cli import (
    DEVPAY_SECRET_ENV,
    build_devpay_notify_request,
    inspect_kafka_dlq,
    main as admin_cli_main,
    plan_kafka_dlq_replay,
)
from xuanming.payment.channels import DEVPAY_SIGNATURE_HEADER, DevPayChannel, devpay_sign
from xuanming.payment.model import ChannelNotification
from xuanming.payment.notify_http import PaymentNotifyServer
from xuanming.protocol import Protocol

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_DIR = REPO_ROOT / "deploy"
K8S_DIR = DEPLOY_DIR / "k8s"
CONFIG_DIR = REPO_ROOT / "config"
MAIN_PY = REPO_ROOT / "src" / "xuanming" / "__main__.py"

ALL_23_SERVICES: tuple[str, ...] = (
    "portal",
    "data-service",
    "db",
    "player-locator",
    "player-inbox",
    "router",
    "scene-manager",
    "match",
    "content-guard",
    "chat",
    "mail",
    "rank",
    "friend",
    "payment",
    "trade",
    "guild",
    "mall-game",
    "gm-admin",
    "config-node",
    "login",
    "gate",
    "scene",
    "battle",
)

HTTP_SERVICES: dict[str, tuple[str, str]] = {
    "portal": ("/actuator/health/liveness", "/actuator/health/readiness"),
    "mall-game": ("/actuator/health/liveness", "/actuator/health/readiness"),
    "gm-admin": ("/actuator/health/liveness", "/actuator/health/readiness"),
    "config-node": ("/actuator/health", "/actuator/health"),
}

FORBIDDEN_RUNTIME_RE = re.compile(
    r"\b(golang|openjdk|temurin|eclipse-temurin|maven|gradle|jre|jdk|go\s+build)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# 辅助函数：从配置权威源解析预期 CLI 与端口，并拆分 Compose / K8s YAML 块
# ---------------------------------------------------------------------------


def _expected_cli_and_ports() -> dict[str, tuple[list[str], set[int]]]:
    cluster = tomllib.loads((CONFIG_DIR / "cluster.example.toml").read_text(encoding="utf-8"))
    portal = tomllib.loads((CONFIG_DIR / "portal.example.toml").read_text(encoding="utf-8"))["portal"]

    return {
        "portal": (["portal", "--config", "/app/config/portal.example.toml"], {portal["port"], portal["management_port"]}),
        "data-service": (["data-service", "--config", "/app/config/cluster.example.toml"], {cluster["data_service_node"]["grpc_port"]}),
        "db": (["db", "--config", "/app/config/cluster.example.toml"], {cluster["db_node"]["grpc_port"]}),
        "player-locator": (["player-locator", "--config", "/app/config/cluster.example.toml"], {cluster["player_locator_node"]["grpc_port"]}),
        "player-inbox": (["player-inbox", "--config", "/app/config/cluster.example.toml"], {cluster["player_inbox_node"]["grpc_port"]}),
        "router": (["router", "--config", "/app/config/cluster.example.toml"], {cluster["router_node"]["grpc_port"]}),
        "scene-manager": (["scene-manager", "--config", "/app/config/cluster.example.toml"], {cluster["scene_manager_node"]["grpc_port"]}),
        "match": (["match", "--config", "/app/config/cluster.example.toml"], {cluster["match_node"]["grpc_port"]}),
        "content-guard": (["content-guard", "--config", "/app/config/cluster.example.toml"], {cluster["content_guard_node"]["grpc_port"]}),
        "chat": (["chat", "--config", "/app/config/cluster.example.toml"], {cluster["chat_node"]["grpc_port"]}),
        "mail": (["mail", "--config", "/app/config/cluster.example.toml"], {cluster["mail_node"]["grpc_port"]}),
        "rank": (["rank", "--config", "/app/config/cluster.example.toml"], {cluster["rank_node"]["grpc_port"]}),
        "friend": (["friend", "--config", "/app/config/cluster.example.toml"], {cluster["friend_node"]["grpc_port"]}),
        "payment": (
            ["payment", "--config", "/app/config/cluster.example.toml"],
            {cluster["payment_node"]["grpc_port"], cluster["payment_node"]["notify_port"]},
        ),
        "trade": (["trade", "--config", "/app/config/cluster.example.toml"], {cluster["trade_node"]["grpc_port"]}),
        "guild": (["guild", "--config", "/app/config/cluster.example.toml"], {cluster["guild_node"]["grpc_port"]}),
        "mall-game": (["mall-game", "--config", "/app/config/cluster.example.toml"], {cluster["mall_game_node"]["port"]}),
        "gm-admin": (["gm-admin", "--config", "/app/config/cluster.example.toml"], {cluster["gm_admin_node"]["port"]}),
        "config-node": (["config-node", "--config", "/app/config/cluster.example.toml"], {cluster["config_node"]["port"]}),
        "login": (["run", "login", "--config", "/app/config/cluster.example.toml"], {cluster["login"]["grpc_port"]}),
        "gate": (
            ["run", "gate", "--config", "/app/config/cluster.example.toml"],
            {cluster["gate"]["port"], cluster["gate"]["rpc_port"], cluster["gate"]["websocket_port"]},
        ),
        "scene": (
            ["run", "scene", "--config", "/app/config/cluster.example.toml"],
            {cluster["scene"]["port"], cluster["scene"]["grpc_port"]},
        ),
        "battle": (
            ["run", "battle", "--config", "/app/config/cluster.example.toml"],
            {cluster["battle"]["port"], cluster["battle"]["grpc_port"], cluster["battle"]["websocket_port"]},
        ),
    }


def _parse_compose_services(compose_text: str) -> dict[str, str]:
    """按 2 空格顶层服务块提取 docker-compose.yml 的 services 映射（零第三方依赖）。"""
    lines = compose_text.splitlines()
    in_services = False
    blocks: dict[str, list[str]] = {}
    current: str | None = None
    for line in lines:
        if re.match(r"^services:\s*$", line):
            in_services = True
            continue
        if in_services and line and not line[0].isspace() and not line.startswith("#"):
            break
        if not in_services:
            continue
        svc_match = re.match(r"^  ([a-z0-9-]+):\s*$", line)
        if svc_match:
            current = svc_match.group(1)
            blocks[current] = []
            continue
        if current is not None:
            blocks[current].append(line)
    return {name: "\n".join(body) for name, body in blocks.items()}


def _split_yaml_documents(text: str) -> list[str]:
    docs: list[str] = []
    cur: list[str] = []
    for line in text.splitlines():
        if line.strip() == "---":
            chunk = "\n".join(cur).strip()
            if chunk and any(not ln.lstrip().startswith("#") and ln.strip() for ln in cur):
                docs.append(chunk)
            cur = []
            continue
        cur.append(line)
    chunk = "\n".join(cur).strip()
    if chunk and any(not ln.lstrip().startswith("#") and ln.strip() for ln in cur):
        docs.append(chunk)
    return docs


def _extract_main_commands() -> tuple[set[str], set[str]]:
    src = MAIN_PY.read_text(encoding="utf-8")
    subcommands = set(re.findall(r'commands\.add_parser\("([^"]+)"', src))
    run_roles_match = re.search(r'server\.add_argument\("role",\s*choices=\(([^)]+)\)', src)
    assert run_roles_match is not None
    run_roles = set(re.findall(r'"([^"]+)"', run_roles_match.group(1)))
    return subcommands, run_roles


def _start_loop_in_thread(coro_factory):
    """在独立后台线程运行异步服务，返回 (state, stop_fn)，供同步 CLI 端到端测试调用。"""
    ready = threading.Event()
    stop_signal = threading.Event()
    box: dict[str, object] = {}

    def runner() -> None:
        async def main_loop() -> None:
            try:
                state, cleanup = await coro_factory()
                box["state"] = state
                ready.set()
                while not stop_signal.is_set():
                    await asyncio.sleep(0.02)
                await cleanup()
            except BaseException as exc:
                box["error"] = exc
                ready.set()

        asyncio.run(main_loop())

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    assert ready.wait(timeout=10.0), "background test server failed to start"
    if "error" in box:
        raise box["error"]  # type: ignore[misc]

    def shutdown() -> None:
        stop_signal.set()
        thread.join(timeout=10.0)

    return box["state"], shutdown


# ---------------------------------------------------------------------------
# 1. 部署产物契约测试（Dockerfile / docker-compose.yml / k8s / README）
# ---------------------------------------------------------------------------


def test_dockerfile_pure_python_multistage_nonroot() -> None:
    dockerfile = (DEPLOY_DIR / "Dockerfile").read_text(encoding="utf-8")
    from_lines = [ln.strip() for ln in dockerfile.splitlines() if ln.strip().upper().startswith("FROM ")]
    assert len(from_lines) >= 2, "Dockerfile must be multi-stage"
    assert all("python:3.13-slim" in ln for ln in from_lines)
    assert not FORBIDDEN_RUNTIME_RE.search(dockerfile), "Dockerfile must not contain Go/Java toolchains"
    assert '.[cluster]' in dockerfile
    assert "10001" in dockerfile
    assert "xuanming" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert "WORKDIR /app" in dockerfile
    assert 'ENTRYPOINT ["python", "-m", "xuanming"]' in dockerfile
    assert 'CMD ["smoke"]' in dockerfile


def test_docker_compose_infra_schemamigrate_and_all_23_services() -> None:
    compose_text = (DEPLOY_DIR / "docker-compose.yml").read_text(encoding="utf-8")
    assert not FORBIDDEN_RUNTIME_RE.search(compose_text), "docker-compose.yml must not reference Go/Java images"

    services = _parse_compose_services(compose_text)
    for infra in ("mysql", "redis", "etcd", "kafka", "schemamigrate"):
        assert infra in services, f"missing infrastructure/init service: {infra}"

    assert "image: mysql:8.0" in services["mysql"]
    assert "healthcheck:" in services["mysql"]
    assert "image: redis:7" in services["redis"]
    assert "etcd:v3.5" in services["etcd"]
    assert "KAFKA_PROCESS_ROLES" in services["kafka"]
    assert "schemamigrate" in services["schemamigrate"]
    assert "--host" in services["schemamigrate"] and "mysql" in services["schemamigrate"]
    assert "3306" in services["schemamigrate"]
    assert 'restart: "no"' in services["schemamigrate"]

    expected = _expected_cli_and_ports()
    main_cmds, run_roles = _extract_main_commands()
    assert set(expected) == set(ALL_23_SERVICES)

    for svc_name in ALL_23_SERVICES:
        assert svc_name in services, f"missing business service in docker-compose.yml: {svc_name}"
        block = services[svc_name]
        exp_cmd, exp_ports = expected[svc_name]
        # 校验共享统一纯 Python 镜像与非 root 锚点
        assert "<<: *xuanming-python-service" in block or "image: xuanming-python:latest" in block
        # 校验 CLI 子命令严格匹配 __main__.py
        cmd_match = re.search(r"command:\s*\[([^\]]+)\]", block)
        assert cmd_match is not None, f"{svc_name}: missing command list"
        parsed_cmd = re.findall(r'"([^"]+)"', cmd_match.group(1))
        assert parsed_cmd == exp_cmd, f"{svc_name}: expected command {exp_cmd}, got {parsed_cmd}"
        assert parsed_cmd[0] in main_cmds
        if parsed_cmd[0] == "run":
            assert parsed_cmd[1] in run_roles

        # 校验所有配置端口均暴露或映射
        declared_ports: set[int] = set()
        for pm in re.finditer(r'"(?:\d+:)?(\d+)"', block):
            declared_ports.add(int(pm.group(1)))
        assert exp_ports.issubset(declared_ports), (
            f"{svc_name}: expected ports {exp_ports} not covered by {declared_ports}"
        )

        # 校验环境变量密钥全部走 ${VAR...} 占位，严禁明文硬编码
        for env_line in re.findall(r"^\s+([A-Z0-9_]+):\s*(.+)$", block, flags=re.MULTILINE):
            key, val = env_line
            if any(tok in key for tok in ("PASSWORD", "SECRET", "KEY", "TOKEN")):
                assert val.strip().startswith("${"), f"{svc_name}: {key} must use env interpolation, got {val!r}"


def test_k8s_manifests_security_probes_and_all_23_services() -> None:
    ns_text = (K8S_DIR / "namespace.yaml").read_text(encoding="utf-8")
    assert "kind: Namespace" in ns_text and "name: xuanming" in ns_text

    kust_text = (K8S_DIR / "kustomization.yaml").read_text(encoding="utf-8")
    for ref in ("namespace.yaml", "schemamigrate-job.yaml", "services.yaml"):
        assert ref in kust_text

    job_text = (K8S_DIR / "schemamigrate-job.yaml").read_text(encoding="utf-8")
    assert "apiVersion: batch/v1" in job_text
    assert "kind: Job" in job_text
    assert "schemamigrate" in job_text
    assert "runAsNonRoot: true" in job_text
    assert "runAsUser: 10001" in job_text
    assert "allowPrivilegeEscalation: false" in job_text
    assert "readOnlyRootFilesystem: true" in job_text
    assert "secretKeyRef:" in job_text
    assert not FORBIDDEN_RUNTIME_RE.search(job_text)

    svc_yaml = (K8S_DIR / "services.yaml").read_text(encoding="utf-8")
    assert not FORBIDDEN_RUNTIME_RE.search(svc_yaml), "k8s services.yaml must not reference Go/Java images"

    docs = _split_yaml_documents(svc_yaml)
    deployments: dict[str, str] = {}
    k8s_services: dict[str, str] = {}
    for doc in docs:
        kind_m = re.search(r"^kind:\s*(\S+)", doc, flags=re.MULTILINE)
        name_m = re.search(r"^metadata:\s*\n\s+name:\s*(\S+)", doc, flags=re.MULTILINE)
        assert kind_m and name_m, f"malformed k8s document:\n{doc[:120]}"
        kind, name = kind_m.group(1), name_m.group(1)
        if kind == "Deployment":
            deployments[name] = doc
        elif kind == "Service":
            k8s_services[name] = doc

    expected = _expected_cli_and_ports()
    assert set(deployments) == set(ALL_23_SERVICES)
    assert set(k8s_services) == set(ALL_23_SERVICES)

    for svc_name in ALL_23_SERVICES:
        dep = deployments[svc_name]
        svc = k8s_services[svc_name]
        exp_cmd, exp_ports = expected[svc_name]

        assert "image: xuanming-python:latest" in dep
        assert "runAsNonRoot: true" in dep
        assert "runAsUser: 10001" in dep
        assert "allowPrivilegeEscalation: false" in dep
        assert "readOnlyRootFilesystem: true" in dep
        assert "mountPath: /tmp" in dep
        assert "emptyDir: {}" in dep
        assert "requests:" in dep and "limits:" in dep
        assert "livenessProbe:" in dep and "readinessProbe:" in dep
        assert "secretKeyRef:" in dep
        assert "type: ClusterIP" in svc

        args_m = re.search(r"args:\s*\[([^\]]+)\]", dep)
        assert args_m is not None, f"k8s Deployment {svc_name}: missing args"
        parsed_args = re.findall(r'"([^"]+)"', args_m.group(1))
        assert parsed_args == exp_cmd, f"k8s {svc_name}: expected args {exp_cmd}, got {parsed_args}"

        dep_ports = {int(p) for p in re.findall(r"containerPort:\s*(\d+)", dep)}
        svc_ports = {int(p) for p in re.findall(r"^\s+port:\s*(\d+)", svc, flags=re.MULTILINE)}
        assert dep_ports == exp_ports, f"k8s Deployment {svc_name}: expected ports {exp_ports}, got {dep_ports}"
        assert svc_ports == exp_ports, f"k8s Service {svc_name}: expected ports {exp_ports}, got {svc_ports}"

        if svc_name in HTTP_SERVICES:
            live_path, ready_path = HTTP_SERVICES[svc_name]
            assert "httpGet:" in dep
            assert f"path: {live_path}" in dep
            assert f"path: {ready_path}" in dep
        else:
            assert "tcpSocket:" in dep

    readme_text = (DEPLOY_DIR / "README.md").read_text(encoding="utf-8")
    for token in ("Docker Compose", "Kubernetes", "schemamigrate", "kafka-dlq", "devpay-notify", "battle-script-review"):
        assert token in readme_text


# ---------------------------------------------------------------------------
# 2. kafka-dlq CLI 测试（JSONL 校验、inspect、replay-plan）
# ---------------------------------------------------------------------------


def _write_dlq_jsonl(path: Path, entries: list[dict[str, object]]) -> Path:
    path.write_text("\n".join(json.dumps(item) for item in entries) + "\n", encoding="utf-8")
    return path


def test_kafka_dlq_inspect_and_replay_plan_end_to_end(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    dlq_file = _write_dlq_jsonl(
        tmp_path / "match-dlq.jsonl",
        [
            {
                "dlq_offset": 10,
                "topic": "match-results",
                "partition": 3,
                "offset": 100,
                "group": "match-settlement",
                "cause": "transient",
                "reason": "mysql timeout",
                "dead_lettered_at": "2026-11-24T01:00:00Z",
                "attempts": 5,
                "key": "battle-1",
                "value": '{"battle_id":1}',
            },
            {
                "dlq_offset": 11,
                "topic": "match-results",
                "partition": 3,
                "offset": 100,  # 与 dlq_offset=10 相同原位点
                "group": "match-settlement",
                "cause": "transient",
                "reason": "rebalance retry",
                "dead_lettered_at": "2026-11-24T01:01:00Z",
                "attempts": 5,
                "key": "battle-1",
                "value": '{"battle_id":1}',
            },
            {
                "dlq_offset": 12,
                "topic": "rank-events",
                "partition": 1,
                "offset": 42,
                "group": "rank-consumer",
                "cause": "unprocessable",
                "reason": "protobuf decode error",
                "dead_lettered_at": "2026-11-24T01:02:00Z",
                "attempts": 1,
                "key": "player-9",
                "value_bytes": 128,
            },
        ],
    )

    # 1) inspect 全量 + limit 截断计数不静默隐藏
    rc = admin_cli_main(["kafka-dlq", "inspect", "--file", str(dlq_file), "--limit", "2", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["total_records"] == 3
    assert out["matched_records"] == 3
    assert out["returned_records"] == 2
    assert out["truncated"] == 1
    assert out["duplicate_count"] == 1
    assert out["by_topic"] == {"match-results": 2, "rank-events": 1}
    assert out["records"][1]["duplicate_of_dlq_offset"] == 10

    # 2) inspect 按 topic 过滤
    filtered = inspect_kafka_dlq(dlq_file, topic="match-results")
    assert filtered["matched_records"] == 2
    assert filtered["duplicate_count"] == 1

    # 3) replay-plan 对 match-results 自动去重并显式报告跳过项
    rc = admin_cli_main(["kafka-dlq", "replay-plan", "--file", str(dlq_file), "--topic", "match-results", "--json"])
    assert rc == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["selected_count"] == 1
    assert plan["skipped_duplicate_count"] == 1
    assert plan["skipped_duplicates"][0]["dlq_offset"] == 11
    assert plan["skipped_duplicates"][0]["duplicate_of_dlq_offset"] == 10
    assert len(plan["commands"]) == 1
    assert "--topic match-results --partition 3 --dlq-offset 10" in plan["commands"][0]

    # 4) replay-plan 默认拒绝 cause=unprocessable
    rc_blocked = admin_cli_main(["kafka-dlq", "replay-plan", "--file", str(dlq_file), "--json"])
    assert rc_blocked == 1
    assert "unprocessable" in capsys.readouterr().err

    # 5) 显式 --allow-unprocessable 后允许生成完整重放计划
    plan_all = plan_kafka_dlq_replay(dlq_file, allow_unprocessable=True)
    assert plan_all["selected_count"] == 2
    assert any("--allow-unprocessable" in cmd for cmd in plan_all["commands"])


def test_kafka_dlq_rejects_malformed_or_missing_envelope_fields(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bad_envelope = tmp_path / "bad.jsonl"
    bad_envelope.write_text(
        json.dumps({"dlq_offset": 1, "topic": "match-results"}) + "\n",
        encoding="utf-8",
    )
    rc = admin_cli_main(["kafka-dlq", "inspect", "--file", str(bad_envelope)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "missing required DLQ envelope field" in err

    bad_json = tmp_path / "corrupt.jsonl"
    bad_json.write_text("{not-json\n", encoding="utf-8")
    rc2 = admin_cli_main(["kafka-dlq", "inspect", "--file", str(bad_json)])
    assert rc2 == 1
    assert "invalid JSON line" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# 3. devpay-notify CLI 测试（--dry-run + 真实回环 PaymentNotifyServer 验签投递）
# ---------------------------------------------------------------------------


class _StubPaymentCore:
    """最小 PaymentCore 替身，承载真实 DevPayChannel 并记录已验签的通知。"""

    def __init__(self, channel: DevPayChannel) -> None:
        self._channel = channel
        self.received: list[ChannelNotification] = []

    def channel_by_name(self, name: str):
        return self._channel if name == self._channel.name else None

    async def handle_notification(self, _channel, note: ChannelNotification) -> None:
        self.received.append(note)


def test_devpay_notify_dry_run_and_live_payment_notify_server(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    secret_str = "devpay-test-hmac-secret-at-least-32-bytes!"
    order_uuid = "11111111-2222-3333-4444-555555555555"
    expected_trade_no = order_uuid.replace("-", "")
    monkeypatch.setenv(DEVPAY_SECRET_ENV, secret_str)

    # 1) --dry-run 校验签名且不泄露明文密钥
    rc_dry = admin_cli_main([
        "devpay-notify",
        "--url", "http://127.0.0.1:18130/notify/devpay",
        "--order-id", order_uuid,
        "--amount-minor", "600",
        "--currency", "CNY",
        "--dry-run",
        "--json",
    ])
    assert rc_dry == 0
    captured = capsys.readouterr()
    assert secret_str not in captured.out
    assert secret_str not in captured.err
    dry_payload = json.loads(captured.out)
    assert dry_payload["dry_run"] is True
    assert dry_payload["delivered"] is False
    assert dry_payload["out_trade_no"] == expected_trade_no
    assert dry_payload["transaction_id"] == f"devpay-{expected_trade_no}"
    assert dry_payload["signature_header"] == DEVPAY_SIGNATURE_HEADER
    built = build_devpay_notify_request(
        url="http://127.0.0.1:18130/notify/devpay",
        order_id=order_uuid,
        amount_minor=600,
        currency="CNY",
        secret=secret_str,
    )
    assert dry_payload["signature_hex"] == devpay_sign(secret_str, built.body).hex()

    # 2) 启动真实回环 PaymentNotifyServer（挂载真实 DevPayChannel）
    channel = DevPayChannel(secret_str)
    core = _StubPaymentCore(channel)

    async def start_notify_server():
        srv = PaymentNotifyServer(core, host="127.0.0.1", port=0)
        port = await srv.start()
        return port, srv.close

    port, shutdown = _start_loop_in_thread(start_notify_server)
    try:
        notify_url = f"http://127.0.0.1:{port}/notify/devpay"

        # 2a) 正常支付回调 + 通过 --transaction-id 推断 seq=2
        tx_id = f"devpay-{expected_trade_no}-2"
        rc_ok = admin_cli_main([
            "devpay-notify",
            "--url", notify_url,
            "--order-id", order_uuid,
            "--transaction-id", tx_id,
            "--amount-minor", "1280",
            "--currency", "CNY",
            "--json",
        ])
        assert rc_ok == 0
        res_ok = json.loads(capsys.readouterr().out)
        assert res_ok["delivered"] is True
        assert res_ok["status_code"] == 200
        assert res_ok["seq"] == 2
        assert len(core.received) == 1
        payment = core.received[0].payment
        assert payment is not None
        assert payment.out_trade_no == expected_trade_no
        assert payment.transaction_id == tx_id
        assert payment.amount_minor == 1280
        assert payment.currency == "CNY"

        # 2b) 渠道撤销回调
        rc_rev = admin_cli_main([
            "devpay-notify",
            "--url", notify_url,
            "--order-id", order_uuid,
            "--transaction-id", tx_id,
            "--revoke",
            "--json",
        ])
        assert rc_rev == 0
        res_rev = json.loads(capsys.readouterr().out)
        assert res_rev["delivered"] is True
        assert res_rev["revoke"] is True
        assert len(core.received) == 2
        assert core.received[1].revocations[0].transaction_id == tx_id

        # 2c) 错误密钥被 PaymentNotifyServer 拒绝（HTTP 401 -> CLI 退出码 1）
        monkeypatch.setenv("CUSTOM_BAD_DEVPAY_SECRET", "wrong-secret-value-at-least-32-bytes!!")
        rc_bad = admin_cli_main([
            "devpay-notify",
            "--url", notify_url,
            "--order-id", order_uuid,
            "--amount-minor", "1280",
            "--secret-env", "CUSTOM_BAD_DEVPAY_SECRET",
            "--json",
        ])
        assert rc_bad == 1
        bad_out = json.loads(capsys.readouterr().out)
        assert bad_out["delivered"] is False
        assert bad_out["status_code"] == 401
        assert len(core.received) == 2

        # 2d) 缺失环境变量密钥 fail-closed
        monkeypatch.delenv("UNSET_DEVPAY_SECRET", raising=False)
        rc_missing = admin_cli_main([
            "devpay-notify",
            "--url", notify_url,
            "--order-id", order_uuid,
            "--amount-minor", "100",
            "--secret-env", "UNSET_DEVPAY_SECRET",
        ])
        assert rc_missing == 1
        assert "UNSET_DEVPAY_SECRET" in capsys.readouterr().err
    finally:
        shutdown()


# ---------------------------------------------------------------------------
# 4. battle-script-review CLI 测试（真实回环 MatchGrpcService）
# ---------------------------------------------------------------------------


def _sample_player_battle_script(protocol: Protocol, *, name: str):
    script = protocol.message("BattleScript", schema_version=1, name=name, description="auto focus")
    role = script.roles.add(role_id=1, order=10, name="DPS", actor_type=1)
    rule = role.rules.add(priority=10, action=1)
    rule.target.side = 2
    rule.target.order = 1
    return script


def test_battle_script_review_cli_against_real_match_grpc(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ops_secret = "match-ops-review-secret-32-bytes-ok!"
    monkeypatch.setenv("TEST_MATCH_OPS_SECRET", ops_secret)

    async def start_match_server():
        protocol = Protocol()
        gate_key = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([gate_key.public_key()], allowed_callers=("gate",))
        cfg = MatchConfig(
            node_id=101,
            team_node_id=102,
            grpc_port=0,
            battle_outcome_secret=b"battle-outcome-secret-32-bytes-!!",
            scene_secret=b"scene-directory-secret-32-bytes-!",
            coordinator_secret=b"coordinator-fence-secret-32-byte!",
            ops_secret=ops_secret.encode("utf-8"),
        )
        store = MemoryMatchStore(protocol)
        presence = MemoryPresenceProvider()
        battle_nodes = StaticBattleNodeResolver(
            [BattleNodeEntry(node_id=30, node_uuid="b-1", grpc_endpoint="127.0.0.1:19998", physical_region=1)]
        )
        rpc_pool = GrpcEndpointPool(default_timeout=5.0)
        gather = GatherPipeline(protocol, store, cfg, rpc_pool, presence, battle_nodes)
        spectate = SpectateManager(protocol, store, cfg, rpc_pool, presence)
        battle_scripts = BattleScriptManager(protocol, store)
        ranked = RankedManager(protocol, store)
        match_svc = MatchGrpcService(
            protocol,
            store,
            cfg,
            gate_verifier=verifier,
            presence=presence,
            battle_nodes=battle_nodes,
            gather=gather,
            spectate=spectate,
            battle_scripts=battle_scripts,
            ranked=ranked,
            inline_gather=True,
        )

        # 玩家提交一份待审战斗剧本（rev=1）
        caller = CallerIdentity(
            player_id=1001,
            account="acc-1001",
            owner_revision=1,
            session_id=1001,
            gate_node_id=1,
            gate_instance_id="gate-1",
            session_incarnation=1,
        )
        sub_req = protocol.message("match.SubmitBattleScriptRequest")
        sub_req.script.CopyFrom(_sample_player_battle_script(protocol, name="FocusV1"))
        sub_resp = await battle_scripts.submit_battle_script(caller, sub_req)
        assert sub_resp.error_message.id == 0 and int(sub_resp.script.script_id) > 0
        sid = int(sub_resp.script.script_id)

        grpc_srv, port = await serve_grpc(protocol, "127.0.0.1:0", match_svc.services(), pass_context=True)

        async def submit_rev2() -> int:
            req2 = protocol.message("match.SubmitBattleScriptRequest", script_id=sid, expected_revision=1)
            req2.script.CopyFrom(_sample_player_battle_script(protocol, name="FocusV2"))
            resp2 = await battle_scripts.submit_battle_script(caller, req2)
            assert resp2.error_message.id == 0
            return int(resp2.script.revision)

        loop = asyncio.get_running_loop()

        async def cleanup() -> None:
            await grpc_srv.stop(1.0)
            await rpc_pool.close()

        return {"port": port, "script_id": sid, "loop": loop, "submit_rev2": submit_rev2}, cleanup

    state, shutdown = _start_loop_in_thread(start_match_server)
    try:
        endpoint = f"127.0.0.1:{state['port']}"
        script_id = state["script_id"]

        # 1) 错误密钥被 match.MatchInternal 拒绝 -> DENIED & 退出码 1
        monkeypatch.setenv("BAD_MATCH_OPS_SECRET", "wrong-match-ops-secret-32-bytes!!")
        rc_denied = admin_cli_main([
            "battle-script-review",
            "list",
            "--endpoint", endpoint,
            "--admin-token-env", "BAD_MATCH_OPS_SECRET",
            "--json",
        ])
        assert rc_denied == 1
        denied_out = json.loads(capsys.readouterr().out)
        assert denied_out["ok"] is False
        assert denied_out["reject_name"] == "DENIED"

        # 2) 合法 ops 拉取待审列表
        rc_list = admin_cli_main([
            "battle-script-review",
            "list",
            "--endpoint", endpoint,
            "--admin-token-env", "TEST_MATCH_OPS_SECRET",
            "--json",
        ])
        assert rc_list == 0
        list_out = json.loads(capsys.readouterr().out)
        assert list_out["ok"] is True
        assert list_out["pending_count"] == 1
        assert list_out["scripts"][0]["script_id"] == script_id
        assert list_out["scripts"][0]["revision"] == 1
        assert list_out["scripts"][0]["content"]["name"] == "FocusV1"

        # 3) 驳回 rev=1（携带 --note）
        rc_rej = admin_cli_main([
            "battle-script-review",
            "reject",
            "--endpoint", endpoint,
            "--script-id", str(script_id),
            "--revision", "1",
            "--reviewer", "ops-alice",
            "--note", "needs clearer name",
            "--admin-token-env", "TEST_MATCH_OPS_SECRET",
            "--json",
        ])
        assert rc_rej == 0
        rej_out = json.loads(capsys.readouterr().out)
        assert rej_out["ok"] is True
        assert rej_out["approve"] is False
        assert rej_out["reject_name"] == "NONE"

        # 4) 重复审核已处理版本 -> STALE & 退出码 1
        rc_stale = admin_cli_main([
            "battle-script-review",
            "approve",
            "--endpoint", endpoint,
            "--script-id", str(script_id),
            "--revision", "1",
            "--reviewer", "ops-bob",
            "--note", "late ok",
            "--admin-token-env", "TEST_MATCH_OPS_SECRET",
            "--json",
        ])
        assert rc_stale == 1
        stale_out = json.loads(capsys.readouterr().out)
        assert stale_out["ok"] is False
        assert stale_out["reject_name"] == "STALE"

        # 5) 玩家提交 rev=2 -> 通过审核 -> 再次 list 为空
        rev2 = asyncio.run_coroutine_threadsafe(state["submit_rev2"](), state["loop"]).result(timeout=5.0)
        assert rev2 == 2

        rc_app = admin_cli_main([
            "battle-script-review",
            "approve",
            "--endpoint", endpoint,
            "--script-id", str(script_id),
            "--revision", "2",
            "--reviewer", "ops-alice",
            "--note", "lgtm",
            "--admin-token-env", "TEST_MATCH_OPS_SECRET",
            "--json",
        ])
        assert rc_app == 0
        app_out = json.loads(capsys.readouterr().out)
        assert app_out["ok"] is True
        assert app_out["approve"] is True
        assert app_out["reject_name"] == "NONE"

        rc_empty = admin_cli_main([
            "battle-script-review",
            "list",
            "--endpoint", endpoint,
            "--admin-token-env", "TEST_MATCH_OPS_SECRET",
            "--json",
        ])
        assert rc_empty == 0
        empty_out = json.loads(capsys.readouterr().out)
        assert empty_out["pending_count"] == 0
    finally:
        shutdown()
