"""运行时弹性原语、gRPC 保活、只读 dbcheck 与运维 admin_cli 单元测试。"""

from __future__ import annotations

import asyncio
import email.utils
import io
import json
import logging
import re
from pathlib import Path

import pytest

from xuanming.grpc_runtime import (
    DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS,
    DEFAULT_GRPC_KEEPALIVE_TIME_MS,
    DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS,
    DEFAULT_GRPC_MAX_CONNECTION_AGE_GRACE_MS,
    DEFAULT_GRPC_MAX_CONNECTION_AGE_MS,
    GrpcClient,
    gate_signing_key,
    grpc_client_options,
    grpc_server_options,
    serve_grpc,
)
from xuanming.login.password_auth import hash_password
from xuanming.ops import (
    CLASS_BOUNDED,
    CLASS_OUTBOX,
    CLASS_SWEPT,
    PUBLIC_KEYS_ENV,
    SIGNING_KEY_ENV,
    ClockCheck,
    ClockSkewError,
    DbGuard,
    FieldBudget,
    LiveViewGate,
    LoopLiveness,
    PayloadLimitError,
    RequiredIndexSpec,
    TableBudget,
    TableRegistryEntry,
    ZoneMergeError,
    admin_cli_main,
    apply_zone_merge,
    audit_live_database,
    audit_offline_schemas,
    check_lexicon_text,
    compare_snapshots,
    dbcheck_main,
    generate_identity_keypair,
    index_columns_match_prefix,
    lexicon_stats,
    password_admin_disable,
    password_admin_set,
    password_admin_verify_account,
    plan_zone_merge,
    read_snapshot,
    run_data_consistency_check,
    validate_lexicon_file,
    verify_identity_seed,
    verify_password,
    write_snapshot,
)
from xuanming.protocol import Protocol

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "config"

_WRITE_SQL_RE = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|REPLACE|TRUNCATE|DROP|ALTER|CREATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# 通用内存异步连接池替身
# ---------------------------------------------------------------------------


class _FakeAsyncCursor:
    def __init__(self, handler, log: list[tuple[str, object]]) -> None:
        self._handler = handler
        self._log = log
        self._rows: list[tuple] = []
        self._idx = 0
        self.rowcount = 0

    async def __aenter__(self) -> _FakeAsyncCursor:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def execute(self, sql: str, args=None) -> int:
        self._log.append((sql, args))
        result = self._handler(sql, args)
        if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], int):
            self.rowcount, rows = result
            self._rows = list(rows)
        elif isinstance(result, list):
            self._rows = list(result)
            self.rowcount = len(self._rows)
        elif isinstance(result, int):
            self._rows = []
            self.rowcount = result
        else:
            self._rows = []
            self.rowcount = 0
        self._idx = 0
        return self.rowcount

    async def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    async def fetchall(self):
        rest = self._rows[self._idx :]
        self._idx = len(self._rows)
        return list(rest)


class _FakeAsyncConn:
    def __init__(self, pool: _FakeAsyncPool) -> None:
        self._pool = pool

    def cursor(self) -> _FakeAsyncCursor:
        return _FakeAsyncCursor(self._pool.handler, self._pool.executed)

    async def begin(self) -> None:
        self._pool.begins += 1

    async def commit(self) -> None:
        self._pool.commits += 1

    async def rollback(self) -> None:
        self._pool.rollbacks += 1


class _FakeAsyncAcquire:
    def __init__(self, pool: _FakeAsyncPool) -> None:
        self._pool = pool

    async def __aenter__(self) -> _FakeAsyncConn:
        return _FakeAsyncConn(self._pool)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeAsyncPool:
    def __init__(self, handler) -> None:
        self.handler = handler
        self.executed: list[tuple[str, object]] = []
        self.begins = 0
        self.commits = 0
        self.rollbacks = 0

    def acquire(self) -> _FakeAsyncAcquire:
        return _FakeAsyncAcquire(self)


def _assert_zero_writes(executed: list[tuple[str, object]]) -> None:
    assert executed, "expected at least one read-only SQL query"
    for sql, _args in executed:
        assert not _WRITE_SQL_RE.match(sql), f"write SQL emitted in read-only path: {sql!r}"
        assert sql.strip().upper().startswith("SELECT "), f"non-SELECT SQL emitted: {sql!r}"


# ---------------------------------------------------------------------------
# 1. ClockCheck 测试
# ---------------------------------------------------------------------------


def test_clock_check_modes_and_http_date() -> None:
    # strict 模式下偏差 <= 30s 放行，> 30s 抛 ClockSkewError。
    strict = ClockCheck(max_skew_sec=30.0, mode="strict")
    ok_res = strict.check_clock_skew(1_700_000_010.0, 1_700_000_000.0)
    assert ok_res.ok is True
    assert ok_res.warn_only is False
    assert ok_res.skew_sec == pytest.approx(10.0)

    with pytest.raises(ClockSkewError) as exc_info:
        strict.check_clock_skew(1_700_000_045.0, 1_700_000_000.0)
    assert exc_info.value.skew_sec == pytest.approx(45.0)
    assert exc_info.value.max_skew_sec == pytest.approx(30.0)

    # warn 模式超限不阻断，返回 ok=True 且 warn_only=True。
    warn = ClockCheck(max_skew_sec=30.0, environ={"MMORPG_CLOCK_CHECK_MODE": "warn"})
    warn_res = warn.check_clock_skew(1_700_000_000.0, 1_700_000_120.0)
    assert warn_res.ok is True
    assert warn_res.warn_only is True
    assert warn_res.skew_sec == pytest.approx(-120.0)

    # off 模式直接跳过（即使 HTTP Date 畸形也不解析）。
    off = ClockCheck(mode="off")
    off_res = off.check_http_date_skew("not-a-date", sent_ts=0.0, received_ts=0.0)
    assert off_res.ok is True and off_res.mode == "off"

    # HTTP Date 头按收发中点与整秒中点(+0.5s)核对。
    ref_ts = 1_700_000_000.0
    date_header = email.utils.formatdate(ref_ts, usegmt=True)
    http_ok = strict.check_http_date_skew(date_header, sent_ts=ref_ts, received_ts=ref_ts + 1.0)
    assert http_ok.ok is True
    assert abs(http_ok.skew_sec) < 0.01

    with pytest.raises(ClockSkewError):
        strict.check_http_date_skew(date_header, sent_ts=ref_ts + 60.0, received_ts=ref_ts + 61.0)

    with pytest.raises(ValueError, match="unusable HTTP Date"):
        strict.check_http_date_skew("invalid-date", sent_ts=ref_ts, received_ts=ref_ts)

    with pytest.raises(ValueError, match="invalid clock check mode"):
        ClockCheck(mode="bogus")


# ---------------------------------------------------------------------------
# 2. DbGuard 测试
# ---------------------------------------------------------------------------


def test_db_guard_payload_thresholds_and_cutoff(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)

    # < 80% 静默放行。
    normal = DbGuard.check_payload("player_inbox_item", "payload", 799, 1000)
    assert normal.ok is True and normal.warned is False

    # >= 80% 且 < 100% 放行并记录 WARN。
    warned = DbGuard.check_payload("player_inbox_item", "payload", 800, 1000)
    assert warned.ok is True and warned.warned is True
    assert any("db_payload_approaching_limit" in rec.message for rec in caplog.records)

    # 100% fail-closed 拒写。
    with pytest.raises(PayloadLimitError) as err:
        DbGuard.check_payload("player_inbox_item", "payload", 1000, 1000)
    assert err.value.size_bytes == 1000
    assert err.value.limit_bytes == 1000

    # allow_equal=True 允许恰好等于上限，超过才拒写。
    eq_ok = DbGuard.check_payload("player_inbox_item", "payload", 1000, 1000, allow_equal=True)
    assert eq_ok.ok is True and eq_ok.warned is True
    with pytest.raises(PayloadLimitError):
        DbGuard.check_payload("player_inbox_item", "payload", 1001, 1000, allow_equal=True)

    # sweep_cutoff_ms：正常返回正截止点；负值或零值下溢返回 None 跳过本轮。
    assert DbGuard.sweep_cutoff_ms(1_700_000_000_000, 86_400_000) == 1_699_913_600_000
    assert DbGuard.sweep_cutoff_ms(10_000, 86_400_000) is None
    assert DbGuard.sweep_cutoff_ms(86_400_000, 86_400_000) is None
    with pytest.raises(ValueError, match="retention_ms"):
        DbGuard.sweep_cutoff_ms(1_700_000_000_000, 0)
    with pytest.raises(ValueError, match="retention_ms"):
        DbGuard.sweep_cutoff_ms(1_700_000_000_000, 40_000 * 86_400_000)


def test_db_guard_check_table_capacity_readonly() -> None:
    def handler(sql: str, args):
        upper = sql.upper()
        if upper.startswith("SELECT DATABASE()"):
            return [("mmorpg_chat",)]
        if "INFORMATION_SCHEMA.TABLES" in upper:
            assert args == ("mmorpg_chat",)
            return [
                ("chat_send_authority", 5000, 10_000_000, 2000),
                ("chat_policy", 10, 4096, 409),
            ]
        raise AssertionError(f"unexpected SQL: {sql}")

    pool = _FakeAsyncPool(handler)
    report = asyncio.run(
        DbGuard.check_table_capacity(
            pool,
            [
                TableBudget(
                    table="chat_send_authority",
                    max_rows=1000,
                    max_bytes=5_000_000,
                    max_avg_row_bytes=1024,
                    note="chat send log",
                ),
                TableBudget(table="chat_policy", max_rows=100),
            ],
        )
    )
    _assert_zero_writes(pool.executed)
    assert report.db_name == "mmorpg_chat"
    assert report.checked == 2
    assert report.ok is False
    assert {v.kind for v in report.violations} == {"rows", "bytes", "avg_row_bytes"}


# ---------------------------------------------------------------------------
# 3. LiveViewGate 测试
# ---------------------------------------------------------------------------


def test_live_view_gate_empty_drop_expiry_recovery_and_override() -> None:
    gate = LiveViewGate(hold_sec=30.0)

    # 1. 初次快照直接接受。
    d0 = gate.observe({"g1", "g2", "g3", "g4"}, now=100.0)
    assert d0.verdict == "accept" and not d0.held
    assert d0.active == frozenset({"g1", "g2", "g3", "g4"})

    # 2. 空快照永远压住旧视图（即使超过 hold_sec 也不自动放行）。
    d1 = gate.observe(set(), now=105.0)
    assert d1.verdict == "held_empty" and d1.held
    assert d1.active == frozenset({"g1", "g2", "g3", "g4"})
    d2 = gate.observe(set(), now=200.0)
    assert d2.verdict == "held_empty" and d2.held
    assert d2.active == frozenset({"g1", "g2", "g3", "g4"})

    # 3. 回到 4 台完整视图 -> 复位放行。
    d3 = gate.observe({"g1", "g2", "g3", "g4"}, now=205.0)
    assert d3.verdict == "accept" and not d3.held and not gate.is_holding

    # 4. 骤降：prev=4, curr=1 (1*2 < 4) -> 压住旧快照。
    d4 = gate.observe({"g1"}, now=300.0)
    assert d4.verdict == "held_drop" and d4.held
    assert d4.active == frozenset({"g1", "g2", "g3", "g4"})

    # 压住期间视图未满 hold_sec 继续压住。
    d5 = gate.observe({"g1"}, now=329.0)
    assert d5.verdict == "held_drop" and d5.held

    # 稳定满 30s 后自动放行并替换基线。
    d6 = gate.observe({"g1"}, now=330.0)
    assert d6.verdict == "released" and not d6.held
    assert d6.active == frozenset({"g1"})
    assert gate.snapshot == frozenset({"g1"})

    # 5. 小集群（len(prev) < 4）降幅不触发骤降压住。
    gate.reset({"n1", "n2", "n3"})
    d_small = gate.observe({"n1"}, now=400.0)
    assert d_small.verdict == "accept" and not d_small.held
    assert d_small.active == frozenset({"n1"})

    # 6. override=True 可强制接受空快照。
    d_over = gate.observe(set(), now=410.0, override=True)
    assert d_over.verdict == "accept" and not d_over.held
    assert d_over.active == frozenset()


# ---------------------------------------------------------------------------
# 4. LoopLiveness 测试
# ---------------------------------------------------------------------------


def test_loop_liveness_lifecycle_and_stale_detection() -> None:
    now_box = [1000.0]
    live = LoopLiveness(clock=lambda: now_box[0])

    live.register("inbox_relay")
    live.register("rank_flush", now=950.0)
    assert live.snapshot() == {"inbox_relay": 1000.0, "rank_flush": 950.0}
    assert live.stale_loops(now=1005.0, threshold_sec=30.0) == ["rank_flush"]

    # mark_ok 推进后不再 stale。
    live.mark_ok("rank_flush", now=1004.0)
    assert live.stale_loops(now=1005.0, threshold_sec=30.0) == []

    # run_round 异常不推进时间戳，正常完成才推进。
    now_box[0] = 1050.0
    ok_fail, val_fail = live.run_round("inbox_relay", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    assert ok_fail is False and val_fail is None
    assert live.snapshot()["inbox_relay"] == 1000.0
    assert live.stale_loops(now=1050.0, threshold_sec=30.0) == ["inbox_relay", "rank_flush"]

    ok_pass, val_pass = live.run_round("inbox_relay", lambda: 42)
    assert ok_pass is True and val_pass == 42
    assert live.snapshot()["inbox_relay"] == 1050.0

    # 有意停机注销后不再出现在快照或 stale 列表。
    assert live.unregister("rank_flush") is True
    assert live.unregister("rank_flush") is False
    assert live.snapshot() == {"inbox_relay": 1050.0}


# ---------------------------------------------------------------------------
# 5. grpc_runtime MaxConnectionAge + Keepalive 测试
# ---------------------------------------------------------------------------


def test_grpc_runtime_default_and_env_keepalive_options() -> None:
    server_opts = dict(grpc_server_options(environ={}))
    assert server_opts["grpc.max_connection_age_ms"] == DEFAULT_GRPC_MAX_CONNECTION_AGE_MS == 15 * 60 * 1000
    assert server_opts["grpc.max_connection_age_grace_ms"] == DEFAULT_GRPC_MAX_CONNECTION_AGE_GRACE_MS == 30 * 1000
    assert server_opts["grpc.keepalive_time_ms"] == DEFAULT_GRPC_KEEPALIVE_TIME_MS == 5 * 60 * 1000
    assert server_opts["grpc.keepalive_timeout_ms"] == DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS == 20 * 1000

    client_opts = dict(grpc_client_options(environ={}))
    assert client_opts["grpc.keepalive_time_ms"] == DEFAULT_GRPC_KEEPALIVE_TIME_MS == 5 * 60 * 1000
    assert client_opts["grpc.keepalive_timeout_ms"] == DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS == 20 * 1000
    assert client_opts["grpc.keepalive_permit_without_calls"] == DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS == 0

    # 支持环境变量覆盖与显式入参优先。
    env = {
        "MMORPG_GRPC_MAX_CONNECTION_AGE_MS": "600000",
        "MMORPG_GRPC_MAX_CONNECTION_AGE_GRACE_MS": "15000",
        "MMORPG_GRPC_KEEPALIVE_TIME_MS": "120000",
        "MMORPG_GRPC_KEEPALIVE_TIMEOUT_MS": "10000",
        "MMORPG_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS": "1",
    }
    env_server = dict(grpc_server_options(environ=env))
    assert env_server["grpc.max_connection_age_ms"] == 600_000
    assert env_server["grpc.max_connection_age_grace_ms"] == 15_000
    assert env_server["grpc.keepalive_time_ms"] == 120_000

    explicit_client = dict(grpc_client_options(keepalive_time_ms=90_000, environ=env))
    assert explicit_client["grpc.keepalive_time_ms"] == 90_000
    assert explicit_client["grpc.keepalive_permit_without_calls"] == 1

    # 端到端环回调用，确认带默认保活参数的服务端与客户端完全兼容。
    async def exercise():
        protocol = Protocol()
        server, port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            {
                "BattleNode": {
                    "CreateBattle": lambda req: protocol.message("CreateBattleResponse"),
                }
            },
        )
        client = GrpcClient(f"127.0.0.1:{port}")
        try:
            assert dict(client.options)["grpc.keepalive_time_ms"] == DEFAULT_GRPC_KEEPALIVE_TIME_MS
            req = protocol.message("CreateBattleRequest", battle_id=1).SerializeToString()
            raw = await client.call("/BattleNode/CreateBattle", req)
            assert protocol.parse("CreateBattleResponse", raw) is not None
        finally:
            await client.close()
            await server.stop(0)

    asyncio.run(exercise())


# ---------------------------------------------------------------------------
# 6. dbcheck 测试（离线 + 在线只读 + 快照/对比 + 大字段 + pending）
# ---------------------------------------------------------------------------


def test_dbcheck_offline_registry_and_cleanup_index_prefix(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # 离线模式扫描真实 config/ 全部 15 份 *_schema.sql。
    report = audit_offline_schemas(CONFIG_DIR, size_check=True)
    assert report.passed is True
    assert report.checked_tables >= 100
    assert report.violations == ()

    # 校验前导列严格前缀匹配语义。
    assert index_columns_match_prefix(("status", "next_attempt_ms", "op_id"), ("status", "next_attempt_ms")) is True
    assert index_columns_match_prefix(("next_attempt_ms", "status"), ("status", "next_attempt_ms")) is False

    # 刻意构造错误的清理索引前导列 -> 离线审计拦截。
    bad_registry = {
        "mmorpg_data_service": {
            "gm_recall_op": TableRegistryEntry(
                table_class=CLASS_SWEPT,
                required_indexes=(
                    RequiredIndexSpec("idx_gm_recall_op_due", ("next_attempt_ms", "status")),
                ),
            )
        }
    }
    bad_report = audit_offline_schemas(CONFIG_DIR, registry=bad_registry)
    assert bad_report.passed is False
    assert any("idx_gm_recall_op_due" in v for v in bad_report.violations)

    # CLI --offline --json + --snapshot + --compare。
    snap_path = tmp_path / "before.json"
    rc = dbcheck_main(
        ["--offline", "--schema-dir", str(CONFIG_DIR), "--snapshot", str(snap_path), "--json"]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert snap_path.is_file()


def test_dbcheck_live_readonly_bigfield_pending_and_snapshot_compare(tmp_path: Path) -> None:
    def handler(sql: str, args):
        upper = sql.upper()
        if "INFORMATION_SCHEMA.TABLES" in upper:
            return [
                ("mmorpg_chat", "chat_send_authority", 1200, 1024),
                ("mmorpg_chat", "chat_outbox", 50, 512),
                ("mmorpg_chat", "mystery_table", 10, 128),
            ]
        if "INFORMATION_SCHEMA.STATISTICS" in upper:
            # 故意返回错误前导列顺序以验证索引门禁。
            return [("request_id",), ("accepted_at",)]
        if "MAX(LENGTH(`REQUEST_PAYLOAD`))" in upper:
            return [(1200, 20000, 4096.0, 2)]
        if "CAST(`REQUEST_ID` AS CHAR)" in upper:
            return [("req-9001", 20000), ("req-9002", 18000)]
        if "FROM `MMORPG_CHAT`.`CHAT_OUTBOX`" in upper:
            return [(250,)]
        if "FROM `MMORPG_CHAT`.`CHAT_SEND_AUTHORITY` WHERE ACCEPTED_AT < NOW()" in upper:
            return [(42,)]
        raise AssertionError(f"unexpected SQL in dbcheck test: {sql!r} args={args!r}")

    pool = _FakeAsyncPool(handler)
    registry = {
        "mmorpg_chat": {
            "chat_send_authority": TableRegistryEntry(
                table_class=CLASS_SWEPT,
                required_indexes=(RequiredIndexSpec("idx_cleanup", ("accepted_at",)),),
                pending_where="accepted_at < NOW()",
            ),
            "chat_outbox": TableRegistryEntry(table_class=CLASS_OUTBOX),
        }
    }
    big_fields = (
        FieldBudget(
            db="mmorpg_chat",
            table="chat_send_authority",
            column="request_payload",
            pk="request_id",
            max_bytes=16 * 1024,
        ),
    )

    before_path = tmp_path / "before.json"
    after_path = tmp_path / "after.json"
    write_snapshot(before_path, {"mmorpg_chat.chat_send_authority": 1000, "mmorpg_chat.chat_outbox": 10})

    report = asyncio.run(
        audit_live_database(
            pool,
            registry=registry,
            big_fields=big_fields,
            strict_registry=True,
            outbox_max=200,
            size_check=True,
            top_rows=2,
            pending=True,
            snapshot_path=after_path,
            compare_path=before_path,
        )
    )
    # 断言全程零写语句。
    _assert_zero_writes(pool.executed)

    assert report.passed is False
    assert any("unregistered table: mmorpg_chat.mystery_table" in v for v in report.violations)
    assert any("outbox backlog: mmorpg_chat.chat_outbox rows=250 max=200" in v for v in report.violations)
    assert any("missing/mismatched cleanup index mmorpg_chat.chat_send_authority.idx_cleanup" in v for v in report.violations)
    assert any("big-field budget exceeded" in v for v in report.violations)

    assert len(report.big_fields) == 1
    assert report.big_fields[0].status == "FAIL"
    assert [(r.pk, r.size_bytes) for r in report.big_fields[0].top_rows] == [
        ("req-9001", 20000),
        ("req-9002", 18000),
    ]
    assert len(report.pending) == 1
    assert report.pending[0].pending_rows == 42

    saved = read_snapshot(after_path)
    assert saved["mmorpg_chat.chat_outbox"] == 250
    deltas = {d.key: d.delta for d in compare_snapshots(read_snapshot(before_path), saved)}
    assert deltas["mmorpg_chat.chat_send_authority"] == 200
    assert deltas["mmorpg_chat.chat_outbox"] == 240


# ---------------------------------------------------------------------------
# 7. admin_cli 全部 5 个子命令测试
# ---------------------------------------------------------------------------


def test_admin_cli_identitykeygen(capsys: pytest.CaptureFixture[str]) -> None:
    pair = generate_identity_keypair()
    assert len(pair.signing_key_hex) == 64
    assert len(pair.public_key_hex) == 64
    assert verify_identity_seed(pair.signing_key_hex) == pair.public_key_hex

    # 生成的密钥对可直接被 gate_signing_key 加载验签。
    loaded = gate_signing_key(
        {
            SIGNING_KEY_ENV: pair.signing_key_hex,
            PUBLIC_KEYS_ENV: pair.public_key_hex,
        }
    )
    assert loaded is not None

    # CLI 生成与 --verify 测试。
    assert admin_cli_main(["identitykeygen", "--json"]) == 0
    generated = json.loads(capsys.readouterr().out)
    assert len(generated[SIGNING_KEY_ENV]) == 64
    assert len(generated[PUBLIC_KEYS_ENV]) == 64

    assert admin_cli_main(["identitykeygen", "--verify", generated[SIGNING_KEY_ENV], "--json"]) == 0
    verified = json.loads(capsys.readouterr().out)
    assert verified["verified"] is True
    assert verified[PUBLIC_KEYS_ENV] == generated[PUBLIC_KEYS_ENV]

    # 畸形种子拒绝。
    assert admin_cli_main(["identitykeygen", "--verify", "deadbeef"]) == 1
    capsys.readouterr()


def test_admin_cli_password_admin_hash_verify_set_disable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    secret = "S3cur3-P@ssw0rd-NeverLogMe!"

    # CLI hash & verify（经 stdin 读取，绝不记录明文）。
    monkeypatch.setattr("sys.stdin", io.StringIO(secret + "\n"))
    assert admin_cli_main(["password-admin", "hash", "--password-stdin"]) == 0
    encoded = capsys.readouterr().out.strip()
    assert encoded.startswith("$scrypt$")
    assert verify_password(secret, encoded) is True
    assert verify_password("wrong-pass", encoded) is False

    monkeypatch.setattr("sys.stdin", io.StringIO(secret + "\n"))
    assert admin_cli_main(["password-admin", "verify", "--hash", encoded, "--password-stdin"]) == 0
    capsys.readouterr()

    monkeypatch.setattr("sys.stdin", io.StringIO("wrong\n"))
    assert admin_cli_main(["password-admin", "verify", "--hash", encoded, "--password-stdin"]) == 1
    capsys.readouterr()

    # 内存替身库测试 set (--create / 拒绝覆盖 / 停用后重设) 与 disable。
    accounts: dict[str, str | None] = {}

    def handler(sql: str, args):
        upper = sql.upper()
        if upper.startswith("SELECT ACCOUNT, PASSWORD FROM USER_ACCOUNTS"):
            acc = args[0]
            if acc not in accounts:
                return []
            return [(acc, accounts[acc])]
        if upper.startswith("INSERT INTO USER_ACCOUNTS"):
            acc, pw_hash = args
            accounts[acc] = pw_hash
            return 1
        if upper.startswith("UPDATE USER_ACCOUNTS SET PASSWORD = NULL"):
            acc = args[0]
            if acc not in accounts:
                return 0
            accounts[acc] = None
            return 1
        if upper.startswith("UPDATE USER_ACCOUNTS SET PASSWORD = %S"):
            pw_hash, acc = args
            if acc not in accounts:
                return 0
            accounts[acc] = pw_hash
            return 1
        raise AssertionError(f"unexpected SQL: {sql}")

    pool = _FakeAsyncPool(handler)
    assert asyncio.run(password_admin_set(pool, "alice", secret, create=True)) == "inserted"
    assert asyncio.run(password_admin_verify_account(pool, "alice", secret)) is True
    assert asyncio.run(password_admin_verify_account(pool, "alice", "other")) is False

    # 已有哈希时默认拒绝覆盖。
    with pytest.raises(ValueError, match="already has a password hash"):
        asyncio.run(password_admin_set(pool, "alice", "NewPass123!"))

    # disable 置 NULL 后验证失败，随后可重新写入。
    assert asyncio.run(password_admin_disable(pool, "alice")) is True
    assert accounts["alice"] is None
    assert asyncio.run(password_admin_verify_account(pool, "alice", secret)) is False
    assert asyncio.run(password_admin_set(pool, "alice", "ResetPass456!")) == "updated"
    assert asyncio.run(password_admin_verify_account(pool, "alice", "ResetPass456!")) is True

    # 明文口令不得出现在任何日志中。
    assert all(secret not in rec.getMessage() for rec in caplog.records)


def test_admin_cli_lexicon_validate_check_stats(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lex_file = tmp_path / "lexicon.txt"
    lex_file.write_text(
        "#content_guard_lexicon 1\n"
        "D\tabuse\tsub\t坏词\n"
        "D\tpolitics\tsub\t禁词\n"
        "A\t好人坏词白名单\n",
        encoding="utf-8",
    )

    val = validate_lexicon_file(lex_file)
    assert val["valid"] is True
    assert val["deny_entries"] == 2
    assert val["allow_entries"] == 1

    # check: chat 中 abuse 打码、白名单放行、politics 拒绝。
    masked_res = check_lexicon_text(lex_file, "你好坏词世界", scene="chat")
    assert masked_res["verdict"] == "MASKED"
    assert "*" in str(masked_res["masked"])
    assert masked_res["category"] == "abuse"

    pass_res = check_lexicon_text(lex_file, "好人坏词白名单", scene="chat")
    assert pass_res["verdict"] == "PASS"

    reject_res = check_lexicon_text(lex_file, "包含禁词内容", scene="chat")
    assert reject_res["verdict"] == "REJECT"
    assert reject_res["category"] == "politics"

    stats = lexicon_stats(lex_file)
    assert stats["by_category"]["abuse"] == 1
    assert stats["by_category"]["politics"] == 1

    # CLI 调用验证。
    assert admin_cli_main(["lexicon", "validate", str(lex_file), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["valid"] is True

    assert admin_cli_main(["lexicon", "stats", str(lex_file), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["deny_entries"] == 2

    assert admin_cli_main(["lexicon", "check", str(lex_file), "--text", "包含禁词", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["verdict"] == "REJECT"

    # 格式错误的词库文件被拒绝。
    bad_file = tmp_path / "bad_lexicon.txt"
    bad_file.write_text("broken header\n", encoding="utf-8")
    assert admin_cli_main(["lexicon", "validate", str(bad_file)]) == 1
    capsys.readouterr()


def test_admin_cli_merge_zone_plan_validate_apply(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # 1. 合法计划与幂等计划。
    plan = plan_zone_merge({}, 101, 102)
    assert plan.action == "insert" and plan.already_merged is False and plan.final_zone == 102

    idem = plan_zone_merge({101: 102}, 101, 102)
    assert idem.action == "noop" and idem.already_merged is True

    # 2. 拒绝 source == target、目录冻结、重复改目标、成环与链式合服。
    with pytest.raises(ZoneMergeError, match="must differ"):
        plan_zone_merge({}, 101, 101)
    with pytest.raises(ZoneMergeError, match="frozen"):
        plan_zone_merge({}, 101, 102, writable=False)
    with pytest.raises(ZoneMergeError, match="already merged"):
        plan_zone_merge({101: 102}, 101, 103)
    with pytest.raises(ZoneMergeError, match="cycle"):
        plan_zone_merge({102: 101}, 101, 102)
    with pytest.raises(ZoneMergeError, match="chained merge"):
        plan_zone_merge({102: 103}, 101, 102)

    # 3. 异步 apply：先 --dry-run（零写 SQL），再真写（INSERT INTO zone_merged_into）。
    merges_db: dict[int, int] = {}

    def handler(sql: str, args):
        upper = sql.upper()
        if "FROM ROUTE_META" in upper:
            return [(1,)]
        if "SELECT DST_ZONE FROM ZONE_MERGED_INTO" in upper:
            z = args[0]
            return [(merges_db[z],)] if z in merges_db else []
        if "SELECT SRC_ZONE, DST_ZONE FROM ZONE_MERGED_INTO" in upper:
            return sorted(merges_db.items())
        if upper.startswith("INSERT INTO ZONE_MERGED_INTO"):
            src, dst, _run_id, _ms = args
            merges_db[int(src)] = int(dst)
            return 1
        raise AssertionError(f"unexpected SQL: {sql}")

    dry_pool = _FakeAsyncPool(handler)
    dry_res = asyncio.run(apply_zone_merge(dry_pool, 101, 102, dry_run=True))
    assert dry_res.dry_run is True and dry_res.applied is False
    _assert_zero_writes(dry_pool.executed)
    assert merges_db == {}

    write_pool = _FakeAsyncPool(handler)
    apply_res = asyncio.run(apply_zone_merge(write_pool, 101, 102, dry_run=False))
    assert apply_res.dry_run is False and apply_res.applied is True
    assert merges_db == {101: 102}
    assert write_pool.commits == 1

    # CLI plan & validate（基于 --existing-json）。
    existing_json = tmp_path / "merges.json"
    existing_json.write_text(json.dumps({"101": 102}), encoding="utf-8")
    assert (
        admin_cli_main(
            [
                "merge-zone",
                "plan",
                "--source-zone",
                "103",
                "--target-zone",
                "102",
                "--existing-json",
                str(existing_json),
                "--json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["action"] == "insert"

    # 链式合服通过 CLI 返回退出码 1。
    assert (
        admin_cli_main(
            [
                "merge-zone",
                "validate",
                "--source-zone",
                "103",
                "--target-zone",
                "101",
                "--existing-json",
                str(existing_json),
            ]
        )
        == 1
    )
    capsys.readouterr()


def test_admin_cli_data_consistency_check_readonly_and_dry_run(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # 1. CLI --dry-run --json 不连库返回 0。
    assert admin_cli_main(["data-consistency-check", "--dry-run", "--live-zones", "1,2", "--json"]) == 0
    dry_payload = json.loads(capsys.readouterr().out)
    assert dry_payload["dry_run"] is True and dry_payload["passed"] is True
    assert dry_payload["live_zones"] == [1, 2]

    # 2. 干净数据库通过，且严格零写 SQL。
    clean_zones = [1, 2]

    def clean_handler(sql: str, args):
        upper = sql.upper()
        if "SELECT HOME_ZONE FROM PLAYER_ROUTE" in upper:
            nxt = [z for z in clean_zones if z >= args[0]]
            return [(nxt[0],)] if nxt else []
        if "FROM PLAYER_ROUTE R LEFT JOIN ACCOUNT_ROLE A" in upper:
            return [(100, 0)]
        if "FROM (SELECT DISTINCT MAIL_ID FROM MAIL_STATE) D WHERE NOT EXISTS" in upper:
            return [(0,)]
        if "FROM (SELECT DISTINCT MAIL_ID FROM MAIL_STATE) D" in upper:
            return [(10,)]
        if "SELECT DISTINCT ZONE_ID FROM `GUILD`" in upper:
            return [(1,), (2,)]
        if "SELECT COUNT(*) FROM `GUILD`" in upper:
            return [(5,)]
        raise AssertionError(f"unexpected SQL: {sql}")

    clean_pool = _FakeAsyncPool(clean_handler)
    clean_report = asyncio.run(
        run_data_consistency_check(
            pool=clean_pool,
            rank_keys_provider=lambda: ["guild_rank:zone:{gr:1}", "guild_rank:zone:{gr:2}"],
        )
    )
    _assert_zero_writes(clean_pool.executed)
    assert clean_report.passed is True
    assert clean_report.live_zones == (1, 2)
    assert all(item.severity == "info" for item in clean_report.checks)

    # 3. 异常数据场景：孤儿 account_role、孤儿 mail_state、死区 guild.zone_id、孤儿 guild_rank。
    def dirty_handler(sql: str, args):
        upper = sql.upper()
        if "SELECT HOME_ZONE FROM PLAYER_ROUTE" in upper:
            nxt = [z for z in [1, 2] if z >= args[0]]
            return [(nxt[0],)] if nxt else []
        if "FROM PLAYER_ROUTE R LEFT JOIN ACCOUNT_ROLE A" in upper:
            return [(100, 3)]
        if "SELECT D.MAIL_ID FROM" in upper:
            return [(7001,), (7002,)]
        if "WHERE NOT EXISTS" in upper:
            return [(2,)]
        if "FROM (SELECT DISTINCT MAIL_ID FROM MAIL_STATE) D" in upper:
            return [(10,)]
        if "SELECT DISTINCT ZONE_ID FROM `GUILD`" in upper:
            return [(1,), (999,)]
        if "WHERE ZONE_ID = %S" in upper:
            return [(4,)]
        if "SELECT COUNT(*) FROM `GUILD`" in upper:
            return [(12,)]
        raise AssertionError(f"unexpected SQL: {sql}")

    dirty_pool = _FakeAsyncPool(dirty_handler)
    dirty_report = asyncio.run(
        run_data_consistency_check(
            pool=dirty_pool,
            rank_keys_provider=lambda: [
                "guild_rank:zone:{gr:1}",
                "guild_rank:zone:{gr:888}",
                "guild_rank:zone:legacy_3",
            ],
        )
    )
    _assert_zero_writes(dirty_pool.executed)
    assert dirty_report.passed is False
    by_name = {c.name: c for c in dirty_report.checks}
    assert by_name["player->account linkage"].severity == "block"
    assert by_name["player->account linkage"].bad_count == 3
    assert by_name["mail_state.mail_id"].severity == "warn"
    assert by_name["mail_state.mail_id"].bad_count == 2
    assert by_name["guild.zone_id"].severity == "block"
    assert by_name["guild.zone_id"].bad_count == 4
    assert by_name["guild_rank:zone:*"].severity == "block"
    assert by_name["guild_rank:zone:*"].bad_count == 1
