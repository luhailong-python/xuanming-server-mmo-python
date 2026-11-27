"""运维工具 ``xuanming.ops.schemamigrate`` 与 ``xuanming.ops.table_sync`` 单元测试。"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import io
import json
import re
import tarfile
from pathlib import Path

import pytest

from xuanming.data.config_tables import ConfigTables
from xuanming.ops import (
    BUNDLE_MANIFEST_FILENAME,
    DESCRIPTOR_FILENAME,
    KNOWN_SCHEMA_FILES,
    POINTER_FILENAME,
    SchemaValidationError,
    TablePointer,
    TableSyncError,
    apply_plan,
    is_lower_hex,
    object_key,
    object_sha256_key,
    pack_bundle,
    parse_schema_sql,
    plan_migrations,
    read_pointer,
    verify_and_unpack,
    verify_bundle,
    write_pointer,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "config"
TABLES_DIR = REPO_ROOT / "src" / "xuanming" / "data" / "tables"
DESCRIPTORS_FILE = REPO_ROOT / "src" / "xuanming" / "data" / "_table_descriptors.pb"

# 16 份真实 *_schema.sql 的表数量基线。
EXPECTED_DB_TABLE_COUNTS = {
    "chat": 6,
    "data_service": 24,
    "db": 2,
    "friend": 5,
    "gm_admin": 2,
    "guild": 17,
    "login": 1,
    "mail": 9,
    "mall_game": 28,
    "match": 33,
    "payment": 4,
    "player_inbox": 4,
    "portal": 3,
    "rank": 9,
    "satoken_auth": 2,
    "trade": 7,
}

_FAKE_CREATE_RE = re.compile(
    r"^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?",
    re.IGNORECASE,
)


class _FakeCursor:
    """模拟 ``aiomysql`` 异步游标，支持查询已有表与执行幂等建表语句。"""

    def __init__(self, pool: _FakePool) -> None:
        self._pool = pool
        self._rows: list[tuple[str]] = []

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def execute(self, sql: str, args=None) -> int:
        self._pool.executed_sql.append(sql)
        normalized = " ".join(sql.split()).upper()
        if normalized.startswith("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"):
            self._rows = [(name,) for name in sorted(self._pool.existing_tables)]
            return len(self._rows)
        match = _FAKE_CREATE_RE.match(sql)
        if not match:
            raise AssertionError(f"unexpected DDL statement in fake pool: {sql!r}")
        table_name = match.group(1)
        self._pool.ddl_sql.append(sql)
        self._pool.existing_tables.add(table_name)
        self._rows = []
        return 0

    async def fetchall(self) -> list[tuple[str]]:
        return list(self._rows)


class _FakeConnection:
    def __init__(self, pool: _FakePool) -> None:
        self._pool = pool

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._pool)

    async def commit(self) -> None:
        self._pool.commit_count += 1


class _FakeAcquire:
    def __init__(self, pool: _FakePool) -> None:
        self._pool = pool

    async def __aenter__(self) -> _FakeConnection:
        return _FakeConnection(self._pool)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakePool:
    """与 ``aiomysql.Pool`` 协议兼容的内存替身。"""

    def __init__(self, existing_tables: set[str] | None = None) -> None:
        self.existing_tables: set[str] = set(existing_tables or ())
        self.executed_sql: list[str] = []
        self.ddl_sql: list[str] = []
        self.commit_count: int = 0

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self)


def _write_custom_tar_gz(path: Path, members: list[tuple[tarfile.TarInfo, bytes | None]]) -> None:
    with path.open("wb") as raw_fh:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw_fh, compresslevel=9, mtime=0) as gz_fh:
            with tarfile.open(fileobj=gz_fh, mode="w", format=tarfile.GNU_FORMAT) as tar:
                for info, payload in members:
                    if payload is None:
                        tar.addfile(info)
                    else:
                        info.size = len(payload)
                        tar.addfile(info, io.BytesIO(payload))


# ---------------------------------------------------------------------------
# schemamigrate 测试
# ---------------------------------------------------------------------------


def test_plan_migrations_all_16_real_schemas() -> None:
    plan = plan_migrations(CONFIG_DIR)
    assert len(plan.databases) == len(KNOWN_SCHEMA_FILES) == 16
    assert {db.source_file for db in plan.databases} == set(KNOWN_SCHEMA_FILES)

    counts = {db.db_name: len(db.tables) for db in plan.databases}
    assert counts == EXPECTED_DB_TABLE_COUNTS
    assert plan.table_count == sum(EXPECTED_DB_TABLE_COUNTS.values())
    assert is_lower_hex(plan.digest, 64)

    # 校验每张表的列、主键、索引与幂等 SQL 完整性，且单库内无重名表。
    for db in plan.databases:
        assert is_lower_hex(db.digest, 64)
        table_names = [t.table_name for t in db.tables]
        assert len(table_names) == len(set(table_names))
        for table in db.tables:
            col_names = {c.name for c in table.columns}
            assert col_names, f"{db.db_name}.{table.table_name} has no columns"
            assert table.primary_key, f"{db.db_name}.{table.table_name} missing PK"
            assert set(table.primary_key).issubset(col_names)
            for idx in table.indexes:
                assert idx.columns
                assert set(idx.columns).issubset(col_names)
            assert table.idempotent_sql.upper().startswith("CREATE TABLE IF NOT EXISTS ")

    # 抽检不同风格 DDL（内联主键、单行无反引号、CHECK 约束、TiDB TTL 注释）。
    portal_db = plan.by_db("portal")
    zone_cfg = {t.table_name: t for t in portal_db.tables}["zone_config"]
    assert zone_cfg.primary_key == ("zone_id",)
    zone_wl = {t.table_name: t for t in portal_db.tables}["zone_whitelist"]
    assert any(idx.name == "uk_zone_account" and idx.unique for idx in zone_wl.indexes)

    ds_db = plan.by_db("data_service")
    ds_tables = {t.table_name: t for t in ds_db.tables}
    assert ds_tables["account_directory"].primary_key == ("account",)
    assert ds_tables["player_route"].primary_key == ("route_shard", "player_id")

    # 摘要必须确定性可复现。
    plan_again = plan_migrations(CONFIG_DIR)
    assert plan_again.digest == plan.digest


def test_plan_migrations_db_filter_and_unknown_db() -> None:
    single = plan_migrations(CONFIG_DIR, db_filter="chat")
    assert [db.db_name for db in single.databases] == ["chat"]
    assert single.table_count == 6

    with_suffix = plan_migrations(CONFIG_DIR, db_filter="chat_schema.sql")
    assert with_suffix.digest == single.digest

    multi = plan_migrations(CONFIG_DIR, db_filter=["portal", "db"])
    assert [db.db_name for db in multi.databases] == ["db", "portal"]
    assert multi.table_count == 5

    with pytest.raises(SchemaValidationError, match="unknown database"):
        plan_migrations(CONFIG_DIR, db_filter="does_not_exist")


@pytest.mark.parametrize(
    ("bad_sql", "error_pattern"),
    [
        (
            "CREATE TABLE IF NOT EXISTS dup_t (id BIGINT PRIMARY KEY); "
            "CREATE TABLE IF NOT EXISTS dup_t (id BIGINT PRIMARY KEY);",
            "duplicate table",
        ),
        (
            "CREATE TABLE IF NOT EXISTS dup_col (id BIGINT PRIMARY KEY, id INT);",
            "duplicate column",
        ),
        (
            "CREATE TABLE IF NOT EXISTS no_pk (id BIGINT NOT NULL);",
            "missing PRIMARY KEY",
        ),
        (
            "CREATE TABLE IF NOT EXISTS bad_pk (id BIGINT NOT NULL, PRIMARY KEY (ghost));",
            "unknown column",
        ),
        (
            "CREATE TABLE IF NOT EXISTS bad_idx (id BIGINT PRIMARY KEY, INDEX idx_x (ghost));",
            "unknown column",
        ),
        (
            "DROP TABLE IF EXISTS evil;",
            "expected CREATE TABLE",
        ),
        (
            "CREATE TABLE IF NOT EXISTS broken (id BIGINT PRIMARY KEY",
            "unbalanced parentheses",
        ),
        (
            "CREATE TABLE IF NOT EXISTS trailing_comma (id BIGINT PRIMARY KEY,);",
            "trailing comma",
        ),
        (
            "CREATE TABLE IF NOT EXISTS a (id BIGINT PRIMARY KEY) "
            "CREATE TABLE IF NOT EXISTS b (id BIGINT PRIMARY KEY);",
            "missing semicolon",
        ),
    ],
)
def test_parse_schema_sql_rejects_malformed_or_duplicate(bad_sql: str, error_pattern: str) -> None:
    with pytest.raises(SchemaValidationError, match=error_pattern):
        parse_schema_sql(bad_sql, db_name="test_db", source_file="test_schema.sql")


def test_apply_plan_dry_run_vs_apply_and_idempotency() -> None:
    plan = plan_migrations(CONFIG_DIR, db_filter="chat")
    pool = _FakePool(existing_tables={"chat_policy"})

    # 1. dry_run=True：不执行任何建表 DDL，清晰区分 pending 与 skipped。
    dry_report = asyncio.run(apply_plan(pool, plan, dry_run=True))
    assert dry_report.dry_run is True
    assert dry_report.applied == ()
    assert dry_report.skipped == ("chat_policy",)
    assert len(dry_report.pending) == 5
    assert "chat_policy" not in dry_report.pending
    assert len(dry_report.statements) == 5
    assert pool.ddl_sql == []
    assert pool.commit_count == 0

    # 2. dry_run=False：仅对缺失的 5 张表执行 CREATE TABLE IF NOT EXISTS。
    apply_report = asyncio.run(apply_plan(pool, plan, dry_run=False))
    assert apply_report.dry_run is False
    assert apply_report.skipped == ("chat_policy",)
    assert apply_report.pending == ()
    assert len(apply_report.applied) == 5
    assert len(pool.ddl_sql) == 5
    assert all(stmt.upper().startswith("CREATE TABLE IF NOT EXISTS ") for stmt in pool.ddl_sql)
    assert pool.commit_count == 1

    # 3. 二次执行完全幂等：全部跳过，零 DDL。
    pool.ddl_sql.clear()
    second_report = asyncio.run(apply_plan(pool, plan, dry_run=False))
    assert second_report.applied == ()
    assert second_report.pending == ()
    assert len(second_report.skipped) == 6
    assert pool.ddl_sql == []

    # 4. 离线 dry_run 允许 pool=None；非 dry_run 缺少 pool 必须拒绝。
    offline_report = asyncio.run(apply_plan(None, plan, dry_run=True))
    assert len(offline_report.pending) == 6
    with pytest.raises(ValueError, match="pool is required"):
        asyncio.run(apply_plan(None, plan, dry_run=False))


def test_apply_plan_enforces_if_not_exists_for_plain_create_table() -> None:
    db_plan = parse_schema_sql(
        "CREATE TABLE plain_t (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;",
        db_name="plain",
    )
    pool = _FakePool()
    report = asyncio.run(apply_plan(pool, db_plan, dry_run=False))
    assert report.applied == ("plain_t",)
    assert len(pool.ddl_sql) == 1
    assert pool.ddl_sql[0].startswith("CREATE TABLE IF NOT EXISTS plain_t ")


# ---------------------------------------------------------------------------
# table_sync 测试
# ---------------------------------------------------------------------------


def test_pack_verify_and_unpack_real_tables_deterministic(tmp_path: Path) -> None:
    bundle_a = tmp_path / "out_a" / "tables.tar.gz"
    bundle_b = tmp_path / "out_b" / "tables.tar.gz"

    res_a = pack_bundle(TABLES_DIR, bundle_a, descriptor_path=DESCRIPTORS_FILE)
    res_b = pack_bundle(TABLES_DIR, bundle_b, descriptor_path=DESCRIPTORS_FILE)

    # 两次打包必须字节级完全确定。
    assert bundle_a.read_bytes() == bundle_b.read_bytes()
    assert res_a.manifest == res_b.manifest
    assert is_lower_hex(res_a.manifest.bundle_sha256, 64)
    assert is_lower_hex(res_a.manifest.content_digest, 64)
    assert res_a.manifest.version == "106"

    # 168 个 tables/ 文件（83 json + 83 pb + manifest.json + tip_text.json）+ 1 个 _table_descriptors.pb = 169。
    manifest_paths = {entry.path for entry in res_a.manifest.files}
    assert len(manifest_paths) == 169
    assert DESCRIPTOR_FILENAME in manifest_paths
    assert "manifest.json" in manifest_paths
    assert "item.json" in manifest_paths
    assert "item.pb" in manifest_paths

    # 校验侧车 .sha256 与 .manifest.json。
    assert res_a.sha256_path.read_text(encoding="utf-8") == (
        f"{res_a.manifest.bundle_sha256}  {bundle_a.name}\n"
    )
    sidecar_manifest = json.loads(res_a.manifest_path.read_text(encoding="utf-8"))
    assert sidecar_manifest["version"] == "106"
    assert sidecar_manifest["bundle_sha256"] == res_a.manifest.bundle_sha256
    assert len(sidecar_manifest["files"]) == 169

    # 只读校验通过。
    verified = verify_bundle(bundle_a, expected_sha256=res_a.manifest.bundle_sha256)
    assert verified == res_a.manifest

    # 原子解包到临时目录并验证全部文件内容一致，且可被 ConfigTables 正常加载。
    dest_dir = tmp_path / "unpacked"
    unpacked_manifest = verify_and_unpack(
        bundle_a, dest_dir, expected_sha256=res_a.manifest.bundle_sha256
    )
    assert unpacked_manifest == res_a.manifest
    assert (dest_dir / "item.json").read_bytes() == (TABLES_DIR / "item.json").read_bytes()
    assert (dest_dir / DESCRIPTOR_FILENAME).read_bytes() == DESCRIPTORS_FILE.read_bytes()

    loaded_tables = ConfigTables(dest_dir, fmt="pb", descriptor_set=dest_dir / DESCRIPTOR_FILENAME)
    assert loaded_tables.version == 106
    assert len(loaded_tables.tables) == 83


def test_unpack_rejects_tampered_bundle_and_preserves_existing_dest(tmp_path: Path) -> None:
    bundle_path = tmp_path / "tables.tar.gz"
    res = pack_bundle(TABLES_DIR, bundle_path)
    dest_dir = tmp_path / "live_tables"
    verify_and_unpack(bundle_path, dest_dir, expected_sha256=res.manifest.bundle_sha256)
    sentinel_file = dest_dir / "item.json"
    original_bytes = sentinel_file.read_bytes()

    # 1. 外部期望 sha256 不匹配 -> 拒绝且不破坏既有目标目录。
    wrong_sha = "0" * 64
    with pytest.raises(TableSyncError, match="archive sha256 mismatch"):
        verify_and_unpack(bundle_path, dest_dir, expected_sha256=wrong_sha)
    assert sentinel_file.read_bytes() == original_bytes
    assert not any(p.name.startswith(".extract-") for p in tmp_path.iterdir())

    # 2. 篡改归档内单个文件字节（即使不传 expected_sha256）-> 内置清单逐文件校验拦截。
    tampered_archive = tmp_path / "tampered_inner.tar.gz"
    raw_members: list[tuple[tarfile.TarInfo, bytes | None]] = []
    with tarfile.open(bundle_path, mode="r:gz") as tar:
        for member in tar:
            data = tar.extractfile(member).read()
            if member.name == "item.json":
                data = data + b" "
            info = tarfile.TarInfo(name=member.name)
            info.mode = 0o644
            info.mtime = 0
            raw_members.append((info, data))
    _write_custom_tar_gz(tampered_archive, raw_members)

    with pytest.raises(TableSyncError, match="size mismatch|sha256 mismatch"):
        verify_and_unpack(tampered_archive, dest_dir)
    assert sentinel_file.read_bytes() == original_bytes


@pytest.mark.parametrize(
    ("evil_name", "member_type", "error_pattern"),
    [
        ("../evil.json", tarfile.REGTYPE, "forbidden path or traversal"),
        ("sub/../../evil.json", tarfile.REGTYPE, "forbidden path or traversal"),
        ("/etc/passwd", tarfile.REGTYPE, "forbidden path or traversal"),
        ("..\\evil.json", tarfile.REGTYPE, "forbidden path or traversal"),
        ("C:evil.json", tarfile.REGTYPE, "forbidden path or traversal"),
        ("link.json", tarfile.SYMTYPE, "forbidden type"),
        ("hardlink.json", tarfile.LNKTYPE, "forbidden type"),
    ],
)
def test_unpack_rejects_path_traversal_and_symlinks(
    tmp_path: Path,
    evil_name: str,
    member_type: bytes,
    error_pattern: str,
) -> None:
    malicious = tmp_path / "evil.tar.gz"
    info = tarfile.TarInfo(name=evil_name)
    info.type = member_type
    info.mode = 0o644
    if member_type in (tarfile.SYMTYPE, tarfile.LNKTYPE):
        info.linkname = "target.json"
        members = [(info, None)]
    else:
        members = [(info, b'{"pwned": true}')]

    # 同时附带一个最小清单，确保报错来自路径/类型守卫而非缺清单。
    manifest_info = tarfile.TarInfo(name=BUNDLE_MANIFEST_FILENAME)
    manifest_info.mode = 0o644
    members.append((manifest_info, b'{"version":"1","files":[]}'))
    _write_custom_tar_gz(malicious, members)

    dest_dir = tmp_path / "safe_dest"
    with pytest.raises(TableSyncError, match=error_pattern):
        verify_and_unpack(malicious, dest_dir)

    assert not dest_dir.exists()
    assert not (tmp_path / "evil.json").exists()
    assert not any(p.name.startswith(".extract-") for p in tmp_path.iterdir())


def test_pointer_read_write_round_trip_and_validation(tmp_path: Path) -> None:
    pointer_file = tmp_path / POINTER_FILENAME
    digest_a = hashlib.sha256(b"bundle-v34").hexdigest()
    digest_b = hashlib.sha256(b"bundle-v35").hexdigest()

    ptr_v1 = TablePointer(
        current_version="34",
        bundle_key=object_key("0123456789abcdef", digest_a),
        bundle_sha256=digest_a,
        updated_ms=1763950000123,
    )
    assert object_sha256_key("0123456789abcdef", digest_a) == f"{ptr_v1.bundle_key}.sha256"

    write_pointer(pointer_file, ptr_v1)
    assert read_pointer(pointer_file) == ptr_v1

    # 覆盖写保持原子且更新全部字段。
    ptr_v2 = TablePointer(
        current_version="35",
        bundle_key=object_key("0123456789abcdef", digest_b),
        bundle_sha256=digest_b,
        updated_ms=1763950005999,
    )
    write_pointer(pointer_file, ptr_v2)
    assert read_pointer(pointer_file) == ptr_v2

    # 校验异常指针与损坏文件（fail-closed）。
    with pytest.raises(TableSyncError, match="64 lower-hex"):
        TablePointer(
            current_version="35",
            bundle_key="tables/a/b.tar.gz",
            bundle_sha256="NOT_HEX",
            updated_ms=1,
        )
    with pytest.raises(TableSyncError, match="non-negative integer"):
        TablePointer(
            current_version="35",
            bundle_key="tables/a/b.tar.gz",
            bundle_sha256=digest_b,
            updated_ms=-1,
        )

    bad_pointer = tmp_path / "bad_pointer.json"
    bad_pointer.write_text("{broken json", encoding="utf-8")
    with pytest.raises(TableSyncError, match="failed to parse pointer"):
        read_pointer(bad_pointer)

    with pytest.raises(TableSyncError, match="pointer file not found"):
        read_pointer(tmp_path / "missing_pointer.json")
