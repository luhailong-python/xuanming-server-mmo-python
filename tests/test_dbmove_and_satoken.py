"""在线物理库搬迁工具（``ops.dbmove``）与 Sa-Token 认证服务（``satoken_auth``）单元测试。"""

from __future__ import annotations

import asyncio
import json
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from xuanming.ops.dbmove import (
    ChunkChecksum,
    CutoverRefusedError,
    DbMoveRefusedError,
    DbMover,
    EXIT_OK,
    EXIT_REFUSED,
    MemoryMoveJournal,
    MovePhase,
    MovePlan,
    ReplicationLagGuard,
    ReplicationStatus,
    SideConfig,
    SqlMoveJournal,
    TableMoveSpec,
    compute_rows_checksum,
    flip_marker_token,
    main as dbmove_main,
)
from xuanming.ops.schemamigrate import parse_schema_file
from xuanming.satoken_auth import (
    DuplicateUserError,
    MemoryAuthStore,
    MemoryTokenSessionStore,
    OAuthProfile,
    OAuthProviderConfig,
    RedisTokenSessionStore,
    SaTokenAuthConfig,
    SqlAuthStore,
    create_auth_app,
    session_redis_key,
    token_redis_key,
    verify_password_hash,
)
from xuanming.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[1]
SATOKEN_SCHEMA_PATH = REPO_ROOT / "config" / "satoken_auth_schema.sql"


# ============================================================================
# 假异步数据库池与假异步 Redis（用于纯内存单测，不依赖外部容器）
# ============================================================================


class FakeSqlTable:
    """支持主键有序扫描、区间扫描与幂等 UPSERT 的内存 SQL 表。"""

    def __init__(self, columns: tuple[str, ...], pk_columns: tuple[str, ...]) -> None:
        self.columns = columns
        self.pk_columns = pk_columns
        self.rows: dict[tuple[Any, ...], dict[str, Any]] = {}

    def put(self, row: dict[str, Any]) -> None:
        pk = tuple(row[c] for c in self.pk_columns)
        self.rows[pk] = dict(row)

    def delete(self, pk: tuple[Any, ...]) -> None:
        self.rows.pop(pk, None)

    def query(
        self,
        *,
        after_pk: tuple[Any, ...] | None = None,
        upper_pk: tuple[Any, ...] | None = None,
        limit: int | None = None,
    ) -> list[tuple[Any, ...]]:
        ordered_pks = sorted(self.rows.keys())
        result: list[tuple[Any, ...]] = []
        for pk in ordered_pks:
            if after_pk is not None and not (pk > after_pk):
                continue
            if upper_pk is not None and not (pk <= upper_pk):
                continue
            row = self.rows[pk]
            result.append(tuple(row[c] for c in self.columns))
            if limit is not None and len(result) >= limit:
                break
        return result


class FakeAsyncCursor:
    """模拟 ``aiomysql.Cursor``，支持 ``dbmove``、``SqlMoveJournal`` 与 ``SqlAuthStore`` 的 SQL 语句。"""

    def __init__(self, db: FakeAsyncPool) -> None:
        self._db = db
        self.description: tuple[tuple[str, ...], ...] | None = None
        self.lastrowid: int = 0
        self._rows: list[tuple[Any, ...]] = []

    async def __aenter__(self) -> FakeAsyncCursor:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        normalized = " ".join(sql.strip().split())
        upper = normalized.upper()

        if upper.startswith("CREATE TABLE"):
            return 0

        # -- SqlMoveJournal: db_move_journal & db_move_sentinel
        if "`db_move_journal`" in normalized:
            if upper.startswith("INSERT INTO"):
                run_id, db_name, phase, sentinel, state_json, updated_ms = params
                self._db.journal_rows[run_id] = {
                    "run_id": run_id,
                    "database_name": db_name,
                    "phase": phase,
                    "sentinel_token": sentinel,
                    "state_json": state_json,
                    "updated_ms": updated_ms,
                }
                return 1
            if upper.startswith("SELECT `STATE_JSON`"):
                row = self._db.journal_rows.get(params[0])
                self._rows = [(row["state_json"],)] if row else []
                return len(self._rows)

        if "`db_move_sentinel`" in normalized:
            if upper.startswith("INSERT INTO"):
                run_id, side, token, epoch, written_ms = params
                self._db.sentinel_rows[(run_id, side, token)] = (epoch, written_ms)
                return 1
            if upper.startswith("SELECT COUNT(*)"):
                run_id, side, token = params
                cnt = 1 if (run_id, side, token) in self._db.sentinel_rows else 0
                self._rows = [(cnt,)]
                return 1

        # -- SqlAuthStore: sys_user & sys_user_oauth
        if "`sys_user`" in normalized:
            if upper.startswith("INSERT INTO `SYS_USER`"):
                username, pw_hash, nickname, avatar, is_guest, created_ms = params
                if any(u["username"] == username for u in self._db.sys_users.values()):
                    raise RuntimeError(1062, f"Duplicate entry '{username}'")
                uid = self._db.next_user_id
                self._db.next_user_id += 1
                self._db.sys_users[uid] = {
                    "id": uid,
                    "username": username,
                    "password_hash": pw_hash,
                    "nickname": nickname,
                    "avatar": avatar,
                    "is_guest": is_guest,
                    "status": 1,
                    "created_ms": created_ms,
                    "last_login_ms": 0,
                }
                self.lastrowid = uid
                return 1
            if upper.startswith("SELECT") and "WHERE `USERNAME` = %S" in upper:
                row = next((u for u in self._db.sys_users.values() if u["username"] == params[0]), None)
                self._rows = [self._user_tuple(row)] if row else []
                return len(self._rows)
            if upper.startswith("SELECT") and "WHERE `ID` = %S" in upper:
                row = self._db.sys_users.get(int(params[0]))
                self._rows = [self._user_tuple(row)] if row else []
                return len(self._rows)
            if upper.startswith("UPDATE `SYS_USER` SET `LAST_LOGIN_MS` = %S"):
                last_ms, uid = params
                if int(uid) in self._db.sys_users:
                    self._db.sys_users[int(uid)]["last_login_ms"] = last_ms
                return 1

        if "`sys_user_oauth`" in normalized:
            if upper.startswith("INSERT INTO `SYS_USER_OAUTH`"):
                uid, provider, open_id, union_id, created_ms = params
                if any(o["provider"] == provider and o["open_id"] == open_id for o in self._db.sys_oauths.values()):
                    raise RuntimeError(1062, "Duplicate entry")
                oid = self._db.next_oauth_id
                self._db.next_oauth_id += 1
                self._db.sys_oauths[oid] = {
                    "id": oid,
                    "user_id": uid,
                    "provider": provider,
                    "open_id": open_id,
                    "union_id": union_id,
                    "created_ms": created_ms,
                }
                self.lastrowid = oid
                return 1
            if upper.startswith("SELECT") and "WHERE `PROVIDER` = %S AND `OPEN_ID` = %S" in upper:
                provider, open_id = params
                row = next(
                    (o for o in self._db.sys_oauths.values() if o["provider"] == provider and o["open_id"] == open_id),
                    None,
                )
                self._rows = [self._oauth_tuple(row)] if row else []
                return len(self._rows)
            if upper.startswith("SELECT") and "WHERE `USER_ID` = %S" in upper:
                uid = int(params[0])
                matched = [self._oauth_tuple(o) for o in self._db.sys_oauths.values() if o["user_id"] == uid]
                self._rows = matched
                return len(matched)

        # -- 业务表 SELECT / INSERT ON DUPLICATE KEY UPDATE（用于 dbmove 回填与校验）
        select_m = re.match(r"^SELECT\s+.+?\s+FROM\s+`([^`]+)`(?:\s+WHERE\s+(.+?))?\s+ORDER BY\s+(.+?)(?:\s+LIMIT\s+%s)?$", normalized, re.IGNORECASE)
        if select_m:
            table_name = select_m.group(1)
            where_part = select_m.group(2) or ""
            has_limit = upper.endswith("LIMIT %S")
            table = self._db.tables[table_name]
            pk_len = len(table.pk_columns)
            idx = 0
            after_pk: tuple[Any, ...] | None = None
            upper_pk: tuple[Any, ...] | None = None
            if ">" in where_part:
                after_pk = tuple(params[idx : idx + pk_len])
                idx += pk_len
            if "<=" in where_part:
                upper_pk = tuple(params[idx : idx + pk_len])
                idx += pk_len
            limit = int(params[idx]) if has_limit else None
            self.description = tuple((c,) for c in table.columns)
            self._rows = table.query(after_pk=after_pk, upper_pk=upper_pk, limit=limit)
            return len(self._rows)

        insert_m = re.match(
            r"^INSERT INTO\s+`([^`]+)`\s*\(([^)]+)\)\s*VALUES\s*\([^)]+\)\s*ON DUPLICATE KEY UPDATE",
            normalized,
            re.IGNORECASE,
        )
        if insert_m:
            table_name = insert_m.group(1)
            cols = [c.strip().strip("`") for c in insert_m.group(2).split(",")]
            table = self._db.tables[table_name]
            table.put({cols[i]: params[i] for i in range(len(cols))})
            return 1

        raise AssertionError(f"Unexpected SQL in FakeAsyncCursor: {normalized}")

    @staticmethod
    def _user_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            row["id"],
            row["username"],
            row["password_hash"],
            row["nickname"],
            row["avatar"],
            row["is_guest"],
            row["status"],
            row["created_ms"],
            row["last_login_ms"],
        )

    @staticmethod
    def _oauth_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            row["id"],
            row["user_id"],
            row["provider"],
            row["open_id"],
            row["union_id"],
            row["created_ms"],
        )

    async def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class FakeAsyncConnection:
    def __init__(self, db: FakeAsyncPool) -> None:
        self._db = db

    def cursor(self) -> FakeAsyncCursor:
        return FakeAsyncCursor(self._db)


class FakeAsyncPool:
    """最小异步连接池假体，兼容 ``async with pool.acquire() as conn``。"""

    def __init__(self, tables: dict[str, FakeSqlTable] | None = None) -> None:
        self.tables: dict[str, FakeSqlTable] = dict(tables or {})
        self.journal_rows: dict[str, dict[str, Any]] = {}
        self.sentinel_rows: dict[tuple[str, str, str], tuple[int, int]] = {}
        self.sys_users: dict[int, dict[str, Any]] = {}
        self.sys_oauths: dict[int, dict[str, Any]] = {}
        self.next_user_id: int = 1
        self.next_oauth_id: int = 1

    @asynccontextmanager
    async def acquire(self):
        yield FakeAsyncConnection(self)

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


class FakeAsyncRedis:
    """纯内存异步 Redis 客户端假体，用于验证 ``RedisTokenSessionStore`` 确切键格式与 TTL。"""

    def __init__(self) -> None:
        self.data: dict[str, tuple[str, int]] = {}

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.data[key] = (str(value), int(ex) if ex is not None else -1)
        return True

    async def get(self, key: str) -> str | None:
        entry = self.data.get(key)
        return entry[0] if entry is not None else None

    async def ttl(self, key: str) -> int:
        entry = self.data.get(key)
        return entry[1] if entry is not None else -2

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            if self.data.pop(key, None) is not None:
                removed += 1
        return removed


# ============================================================================
# Part 1: dbmove 测试套件
# ============================================================================


def _make_plan(
    *,
    run_id: str = "a" * 32,
    chunk_size: int = 2,
    tables: tuple[TableMoveSpec, ...] | None = None,
    sentinel_token: str = "sentinel-token-001",
    max_lag_ms: int = 3000,
) -> MovePlan:
    return MovePlan(
        run_id=run_id,
        source=SideConfig(role="source", host="10.0.0.1", port=3306, user="ops", password="secret-password", database="game_db"),
        target=SideConfig(role="target", host="10.0.0.2", port=3306, user="ops", password="target-password", database="game_db"),
        tables=tables
        or (
            TableMoveSpec(table="players", pk_columns=("id",), columns=("id", "name", "gold")),
            TableMoveSpec(table="orders", pk_columns=("zone_id", "order_id"), columns=("zone_id", "order_id", "amount")),
        ),
        chunk_size=chunk_size,
        sentinel_token=sentinel_token,
        freeze_gate=True,
        max_lag_ms=max_lag_ms,
    )


def _seed_pools() -> tuple[FakeAsyncPool, FakeAsyncPool]:
    src_players = FakeSqlTable(("id", "name", "gold"), ("id",))
    for i in range(1, 6):
        src_players.put({"id": i, "name": f"hero_{i}", "gold": i * 100})

    src_orders = FakeSqlTable(("zone_id", "order_id", "amount"), ("zone_id", "order_id"))
    for zone_id, order_id, amount in [(1, 10, 50), (1, 20, 75), (2, 1, 120)]:
        src_orders.put({"zone_id": zone_id, "order_id": order_id, "amount": amount})

    tgt_players = FakeSqlTable(("id", "name", "gold"), ("id",))
    tgt_orders = FakeSqlTable(("zone_id", "order_id", "amount"), ("zone_id", "order_id"))

    return (
        FakeAsyncPool({"players": src_players, "orders": src_orders}),
        FakeAsyncPool({"players": tgt_players, "orders": tgt_orders}),
    )


def test_dbmove_config_and_plan_validation_redacts_secrets() -> None:
    side = SideConfig.from_dsn("mysql://ops:super-secret@10.0.0.1:3306/game_db", role="source")
    assert "super-secret" not in repr(side)
    assert "password" not in side.to_safe_dict()

    with pytest.raises(DbMoveRefusedError, match="no primary key"):
        TableMoveSpec(table="bad_table", pk_columns=())

    same_source = SideConfig(role="source", host="10.0.0.1", port=3306, database="game_db")
    same_target = SideConfig(role="target", host="10.0.0.1", port=3306, database="game_db")
    with pytest.raises(DbMoveRefusedError, match="same database endpoint"):
        MovePlan(source=same_source, target=same_target, tables=(TableMoveSpec("t1", ("id",)),))


def test_compute_rows_checksum_deterministic_and_collision_free() -> None:
    rows_a = [{"id": 1, "a": "x,y", "b": "z"}, {"id": 2, "a": None, "b": ""}]
    rows_b = [{"id": 1, "a": "x", "b": "y,z"}, {"id": 2, "a": "", "b": None}]
    c1 = compute_rows_checksum(rows_a, columns=("id", "a", "b"))
    c2 = compute_rows_checksum(rows_a, columns=("id", "a", "b"))
    c3 = compute_rows_checksum(rows_b, columns=("id", "a", "b"))
    assert c1 == c2
    assert c1 != c3
    assert compute_rows_checksum([(None,)], None) != compute_rows_checksum([("",)], None)
    assert compute_rows_checksum([(0,)], None) != compute_rows_checksum([(False,)], None)


def test_dbmove_plan_chunked_resumable_backfill_verify_and_cutover() -> None:
    async def _run() -> None:
        src_pool, tgt_pool = _seed_pools()
        ctrl_pool = FakeAsyncPool()
        journal = SqlMoveJournal(ctrl_pool)
        await journal.ensure_tables()

        frozen_flag = {"frozen": False, "epoch": 0}

        async def freeze_hook(_plan: MovePlan) -> int:
            frozen_flag["frozen"] = True
            frozen_flag["epoch"] += 1
            return frozen_flag["epoch"]

        async def unfreeze_hook(_plan: MovePlan) -> None:
            frozen_flag["frozen"] = False

        # 模拟复制链路：源写入哨兵后，自动同步到目标。
        async def sentinel_writer(run_id: str, side: str, token: str, epoch: int) -> None:
            if side == "source":
                await journal.write_sentinel(run_id, "target", token, epoch=epoch)

        plan = _make_plan(chunk_size=2)
        mover = DbMover(
            source_pool=src_pool,
            target_pool=tgt_pool,
            journal=journal,
            freeze_hook=freeze_hook,
            unfreeze_hook=unfreeze_hook,
            sentinel_writer=sentinel_writer,
        )

        # 1. dry-run plan 不写控制表
        dry_plan = await mover.plan(plan, dry_run=True)
        assert dry_plan.dry_run is True
        assert await journal.load(plan.run_id) is None

        # 2. 真实 plan 初始化 INIT
        plan_rep = await mover.plan(plan)
        assert plan_rep.phase == MovePhase.INIT.value

        # 3. 中断并断点续传回填（先每表只拉 1 块，再继续拉完）
        bf1 = await mover.backfill(plan, max_chunks_per_table=1)
        assert bf1.phase == MovePhase.BACKFILL.value
        assert bf1.total_rows_copied == 4  # players 2 行 + orders 2 行
        mid_state = await journal.load(plan.run_id)
        assert mid_state is not None
        assert mid_state.table_cursors["players"].last_pk == (2,)
        assert mid_state.table_cursors["orders"].last_pk == (1, 20)

        bf2 = await mover.backfill(plan)
        assert bf2.phase == MovePhase.REPLICATE.value
        assert bf2.total_rows_copied == 4  # players 剩余 3 行 + orders 剩余 1 行
        assert len(tgt_pool.tables["players"].rows) == 5
        assert len(tgt_pool.tables["orders"].rows) == 3

        # 4. 校验全表一致
        ver = await mover.verify(plan)
        assert ver.matched is True
        assert ver.mismatches == []
        assert ver.tables["players"].chunks_checked == 3
        assert ver.tables["orders"].chunks_checked == 2

        # 5. 切流成功，源库保持冻结且目标写入翻转标记
        cut = await mover.cutover(plan)
        assert cut.phase == MovePhase.DONE.value
        assert cut.source_frozen is True
        assert frozen_flag["frozen"] is True
        assert await journal.has_sentinel(plan.run_id, "target", flip_marker_token(plan.run_id))

        # 6. 幂等重跑 cutover 返回 already_done=True
        cut_again = await mover.cutover(plan)
        assert cut_again.already_done is True

        # 7. 已切流完成后拒绝 abort（防脑裂）
        with pytest.raises(DbMoveRefusedError, match="already completed cutover"):
            await mover.abort(plan)

    asyncio.run(_run())


def test_dbmove_checksum_mismatch_detection_reports_exact_pk_ranges() -> None:
    async def _run() -> None:
        src_pool, tgt_pool = _seed_pools()
        mover = DbMover(source_pool=src_pool, target_pool=tgt_pool, journal=MemoryMoveJournal())
        plan = _make_plan(chunk_size=2)
        await mover.plan(plan)
        await mover.backfill(plan)

        # 篡改目标 players 第 2 块（id=3）并追加多余尾部行（id=99）
        tgt_pool.tables["players"].put({"id": 3, "name": "tampered", "gold": 999999})
        tgt_pool.tables["players"].put({"id": 99, "name": "ghost", "gold": 0})

        res = await ChunkChecksum.verify_table_checksum(
            src_pool, tgt_pool, "players", ("id",), columns=("id", "name", "gold"), chunk_size=2
        )
        assert res.matched is False
        assert len(res.mismatches) == 2
        assert res.mismatches[0].pk_start == (3,)
        assert res.mismatches[0].pk_end == (4,)
        assert res.mismatches[1].pk_start == (99,)
        assert res.mismatches[1].source_rows == 0
        assert res.mismatches[1].target_rows == 1

        ver = await mover.verify(plan)
        assert ver.matched is False
        assert ver.verified_at_ms == 0

    asyncio.run(_run())


def test_dbmove_cutover_safety_refusals_and_abort_rollback() -> None:
    async def _run() -> None:
        # 场景 A：复制延迟超阈值，切流前直接拒绝且不冻结源库
        src_pool, tgt_pool = _seed_pools()
        journal = MemoryMoveJournal()
        frozen_calls = {"freeze": 0, "unfreeze": 0}
        repl_state = {"status": ReplicationStatus(running=True, lag_ms=5000, drained=False)}

        async def freeze_hook(_plan: MovePlan) -> int:
            frozen_calls["freeze"] += 1
            return frozen_calls["freeze"]

        async def unfreeze_hook(_plan: MovePlan) -> None:
            frozen_calls["unfreeze"] += 1

        mover = DbMover(
            source_pool=src_pool,
            target_pool=tgt_pool,
            journal=journal,
            lag_guard=ReplicationLagGuard(max_lag_ms=1000),
            replication_probe=lambda: asyncio.sleep(0, result=repl_state["status"]),
            freeze_hook=freeze_hook,
            unfreeze_hook=unfreeze_hook,
        )
        plan = _make_plan(max_lag_ms=1000)
        await mover.plan(plan)
        await mover.backfill(plan)
        assert (await mover.verify(plan)).matched is True

        with pytest.raises(CutoverRefusedError, match="exceeds max_lag_ms"):
            await mover.cutover(plan)
        assert frozen_calls["freeze"] == 0

        # 场景 B：延迟恢复正常，但目标缺少哨兵令牌 -> 窗口回滚并自动解冻源库，同时清除 verify 记录
        repl_state["status"] = ReplicationStatus(running=True, lag_ms=10, drained=True)
        with pytest.raises(CutoverRefusedError, match="sentinel token has not replicated"):
            await mover.cutover(plan)
        assert frozen_calls["freeze"] == 1
        assert frozen_calls["unfreeze"] == 1
        st = await journal.load(plan.run_id)
        assert st is not None
        assert st.frozen is False
        assert st.verified_at_ms == 0

        # 清空 verify 后直接 cutover 必须被拒绝
        with pytest.raises(CutoverRefusedError, match="no successful verify"):
            await mover.cutover(plan)

        # 场景 C：重新 verify 后目标数据被意外改写 -> 静默窗口最终校验失败，自动解冻回滚
        assert (await mover.verify(plan)).matched is True
        await journal.write_sentinel(plan.run_id, "target", plan.sentinel_token)
        tgt_pool.tables["orders"].put({"zone_id": 2, "order_id": 1, "amount": 9999})
        with pytest.raises(CutoverRefusedError, match="final quiet-window checksum mismatch"):
            await mover.cutover(plan)
        assert frozen_calls["freeze"] == 2
        assert frozen_calls["unfreeze"] == 2
        st2 = await journal.load(plan.run_id)
        assert st2 is not None
        assert st2.frozen is False
        assert st2.verified_at_ms == 0

        # 场景 D：若目标已存在翻转标记，即使状态未切到 DONE，abort 也必须拒绝（防 etcd 回档脑裂）
        await journal.write_sentinel(plan.run_id, "target", flip_marker_token(plan.run_id))
        with pytest.raises(DbMoveRefusedError, match="flip marker"):
            await mover.abort(plan)

        # 场景 E：正常中止（无翻转标记，源库处于冻结态时自动解冻）
        plan_abort = _make_plan(run_id="b" * 32)
        await mover.plan(plan_abort)
        await journal.transition(plan_abort.run_id, MovePhase.VERIFY, frozen=True)
        abort_rep = await mover.abort(plan_abort, reason="maintenance cancelled")
        assert abort_rep.phase == MovePhase.ABORTED.value
        assert abort_rep.source_unfrozen is True
        assert frozen_calls["unfreeze"] == 3

    asyncio.run(_run())


def test_dbmove_cli_plan_dry_run_json(capsys: pytest.CaptureFixture[str]) -> None:
    code = dbmove_main(
        [
            "plan",
            "--dry-run",
            "--json",
            "--source-dsn",
            "mysql://ops:super-secret-src@10.1.0.1:3306/game_db",
            "--target-dsn",
            "mysql://ops:super-secret-tgt@10.2.0.2:3306/game_db",
            "--tables",
            "players:id,orders:zone_id+order_id",
            "--chunk-size",
            "500",
        ]
    )
    out = capsys.readouterr()
    assert code == EXIT_OK
    assert "super-secret-src" not in out.out
    assert "super-secret-tgt" not in out.out
    payload = json.loads(out.out)
    assert payload["ok"] is True
    assert payload["dry_run"] is True
    assert payload["database"] == "game_db"
    assert payload["tables"] == ["players", "orders"]

    # 缺参数返回 EXIT_REFUSED
    bad_code = dbmove_main(["plan", "--json"])
    assert bad_code == EXIT_REFUSED


# ============================================================================
# Part 2: satoken_auth 测试套件
# ============================================================================


def test_satoken_auth_schema_parses_with_schemamigrate() -> None:
    plan = parse_schema_file(SATOKEN_SCHEMA_PATH)
    assert plan.db_name == "satoken_auth"
    table_names = [t.table_name for t in plan.tables]
    assert table_names == ["sys_user", "sys_user_oauth"]

    sys_user = plan.tables[0]
    assert sys_user.primary_key == ("id",)
    assert any(idx.unique and idx.columns == ("username",) for idx in sys_user.indexes)
    assert "CREATE TABLE IF NOT EXISTS" in sys_user.idempotent_sql.upper()

    sys_oauth = plan.tables[1]
    assert sys_oauth.primary_key == ("id",)
    assert any(idx.unique and idx.columns == ("provider", "open_id") for idx in sys_oauth.indexes)


def test_satoken_config_fail_closed_in_prod() -> None:
    # 即使显式传入 dev_login_enabled=True，prod 下必须强制关闭
    prod_cfg = SaTokenAuthConfig(profile="prod", dev_login_enabled=True)
    assert prod_cfg.allow_dev_login is False
    assert prod_cfg.dev_login_enabled is False

    # from_settings 在 prod 下同样强制关闭
    from_env_prod = SaTokenAuthConfig.from_settings(
        Settings(),
        environ={"SATOKEN_AUTH_PROFILE": "prod", "SATOKEN_DEV_LOGIN_ENABLED": "true"},
    )
    assert from_env_prod.profile == "prod"
    assert from_env_prod.allow_dev_login is False

    # dev/test 且显式开启才允许
    from_env_dev = SaTokenAuthConfig.from_settings(
        Settings(),
        environ={"SATOKEN_AUTH_PROFILE": "dev", "SATOKEN_DEV_LOGIN_ENABLED": "true"},
    )
    assert from_env_dev.profile == "dev"
    assert from_env_dev.allow_dev_login is True

    # dev 但未开启 dev_login_enabled 依然关闭
    from_env_dev_off = SaTokenAuthConfig.from_settings(
        Settings(),
        environ={"SATOKEN_AUTH_PROFILE": "dev"},
    )
    assert from_env_dev_off.allow_dev_login is False


def test_sql_auth_store_and_redis_token_session_store() -> None:
    async def _run() -> None:
        pool = FakeAsyncPool()
        sql_store = SqlAuthStore(pool)
        user = await sql_store.create_user("alice", "$scrypt$dummy", nickname="Alice")
        assert user.id == 1
        assert (await sql_store.get_user_by_username("alice")) is not None

        with pytest.raises(DuplicateUserError):
            await sql_store.create_user("alice", "$scrypt$dummy")

        oauth_u1, created1 = await sql_store.find_or_create_oauth_user(
            "wechat", "union_abc", nickname="WXAlice", union_id="union_abc"
        )
        oauth_u2, created2 = await sql_store.find_or_create_oauth_user(
            "wechat", "union_abc", nickname="WXAlice", union_id="union_abc"
        )
        assert created1 is True
        assert created2 is False
        assert oauth_u1.id == oauth_u2.id
        assert len(await sql_store.list_user_oauths(oauth_u1.id)) == 1

        fake_redis = FakeAsyncRedis()
        redis_store = RedisTokenSessionStore(fake_redis)
        session = await redis_store.create_session(str(user.id), token_name="satoken", ttl_sec=3600)
        expected_tkey = token_redis_key("satoken", session.token_value)
        expected_skey = session_redis_key("satoken", str(user.id))
        assert expected_tkey == f"satoken:login:token:{session.token_value}"
        assert expected_skey == f"satoken:login:session:{user.id}"
        assert fake_redis.data[expected_tkey] == (str(user.id), 3600)
        assert expected_skey in fake_redis.data

        loaded = await redis_store.get_session_by_token(session.token_value)
        assert loaded is not None
        assert loaded.login_id == str(user.id)
        assert await redis_store.revoke_token(session.token_value) is True
        assert await redis_store.get_session_by_token(session.token_value) is None
        assert expected_tkey not in fake_redis.data

    asyncio.run(_run())


@asynccontextmanager
async def _start_loopback_server(app: web.Application):
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets  # type: ignore[union-attr]
    port = sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


def test_satoken_http_full_lifecycle_oauth_and_profile_gate() -> None:
    async def _run() -> None:
        user_store = MemoryAuthStore()
        token_store = MemoryTokenSessionStore()
        prod_config = SaTokenAuthConfig(
            host="127.0.0.1",
            port=0,
            profile="prod",
            dev_login_enabled=True,  # prod 下必须被 fail-closed 拦截
            token_name="satoken",
            token_ttl_sec=7200,
            oauth_providers={
                "wechat": OAuthProviderConfig(
                    provider="wechat",
                    client_id="wx-client-id",
                    client_secret="wx-client-secret",
                    redirect_uri="http://127.0.0.1:8085/auth/oauth/wechat/callback",
                    union_id=True,
                )
            },
        )

        async def fake_exchange(provider: str, code: str, _cfg: OAuthProviderConfig) -> OAuthProfile:
            assert provider == "wechat"
            return OAuthProfile(
                open_id=f"open_{code}",
                union_id="union_stable_42",
                nickname="WeChatHero",
                avatar="https://cdn.example.invalid/a.png",
            )

        app = create_auth_app(user_store, token_store, prod_config, oauth_exchange=fake_exchange)

        async with _start_loopback_server(app) as base_url:
            async with aiohttp.ClientSession() as client:
                # 1. 健康探针
                async with client.get(f"{base_url}/actuator/health") as resp:
                    assert resp.status == 200
                    health = await resp.json()
                    assert health["status"] == "UP"
                    assert health["profile"] == "prod"
                    assert health["devLoginEnabled"] is False

                # 2. 注册账号并验证密码哈希是 scrypt PHC 且不对外泄露
                async with client.post(
                    f"{base_url}/auth/register",
                    json={"username": "player01", "password": "StrongPassword!1", "nickname": "SwordMaster"},
                ) as resp:
                    assert resp.status == 200
                    reg = await resp.json()
                    assert reg["code"] == 200
                    assert reg["data"]["username"] == "player01"
                    assert "password_hash" not in reg["data"]
                    user_id = reg["data"]["userId"]

                stored_user = await user_store.get_user_by_username("player01")
                assert stored_user is not None
                assert stored_user.password_hash.startswith("$scrypt$")
                assert verify_password_hash("StrongPassword!1", stored_user.password_hash) is True

                # 重复注册返回 409
                async with client.post(
                    f"{base_url}/auth/register",
                    json={"username": "player01", "password": "StrongPassword!1"},
                ) as resp:
                    assert resp.status == 409

                # 3. 错误密码被拒
                async with client.post(
                    f"{base_url}/auth/login",
                    json={"username": "player01", "password": "WrongPassword!"},
                ) as resp:
                    assert resp.status == 401

                # 4. 正确登录签发 Sa-Token，并验证 Redis 键格式
                async with client.post(
                    f"{base_url}/auth/login",
                    json={"username": "player01", "password": "StrongPassword!1"},
                ) as resp:
                    assert resp.status == 200
                    login_body = await resp.json()
                    assert login_body["code"] == 200
                    assert login_body["msg"] == "ok"
                    token_value = login_body["data"]["tokenValue"]
                    assert login_body["data"]["tokenName"] == "satoken"
                    assert login_body["data"]["loginId"] == str(user_id)

                snap = token_store.keys_snapshot()
                assert snap[f"satoken:login:token:{token_value}"] == str(user_id)
                assert f"satoken:login:session:{user_id}" in snap

                # 5. isLogin / tokenInfo / userInfo（测试 satoken 头与 Bearer 头）
                async with client.get(f"{base_url}/auth/isLogin") as resp:
                    assert (await resp.json())["data"]["isLogin"] is False

                async with client.get(f"{base_url}/auth/isLogin", headers={"satoken": token_value}) as resp:
                    body = await resp.json()
                    assert body["data"] == {"isLogin": True, "loginId": str(user_id)}

                async with client.get(
                    f"{base_url}/auth/tokenInfo",
                    headers={"Authorization": f"Bearer {token_value}"},
                ) as resp:
                    assert resp.status == 200
                    ti = await resp.json()
                    assert ti["data"]["tokenValue"] == token_value
                    assert ti["data"]["loginId"] == str(user_id)

                async with client.get(f"{base_url}/auth/userInfo", headers={"satoken": token_value}) as resp:
                    assert resp.status == 200
                    ui = await resp.json()
                    assert ui["data"]["username"] == "player01"
                    assert ui["data"]["nickname"] == "SwordMaster"
                    assert "password_hash" not in ui["data"]

                # 6. logout 吊销令牌
                async with client.post(f"{base_url}/auth/logout", headers={"satoken": token_value}) as resp:
                    assert resp.status == 200
                    assert (await resp.json())["data"]["removed"] is True

                async with client.get(f"{base_url}/auth/isLogin", headers={"satoken": token_value}) as resp:
                    assert (await resp.json())["data"]["isLogin"] is False

                async with client.get(f"{base_url}/auth/tokenInfo", headers={"satoken": token_value}) as resp:
                    assert resp.status == 401

                # 7. OAuth 授权地址与回调（首次建号绑定，二次复用同一 userId；重放 state 拒绝）
                async with client.get(f"{base_url}/auth/oauth/wechat/url") as resp:
                    assert resp.status == 200
                    url_data = (await resp.json())["data"]
                    state1 = url_data["state"]
                    assert "wx-client-id" in url_data["url"]

                async with client.post(
                    f"{base_url}/auth/oauth/wechat/callback",
                    json={"code": "code_1", "state": state1},
                ) as resp:
                    assert resp.status == 200
                    cb1 = (await resp.json())["data"]
                    assert cb1["isNewUser"] is True
                    oauth_uid = cb1["loginId"]
                    assert cb1["user"]["nickname"] == "WeChatHero"

                # 重放同一 state 被拒绝
                async with client.post(
                    f"{base_url}/auth/oauth/wechat/callback",
                    json={"code": "code_1", "state": state1},
                ) as resp:
                    assert resp.status == 400

                # 第二次使用不同 code 但同一 union_id 回调 -> 绑定到同一内部 userId
                async with client.get(f"{base_url}/auth/oauth/wechat/url") as resp:
                    state2 = (await resp.json())["data"]["state"]
                async with client.post(
                    f"{base_url}/auth/oauth/wechat/callback",
                    json={"code": "code_2", "state": state2},
                ) as resp:
                    assert resp.status == 200
                    cb2 = (await resp.json())["data"]
                    assert cb2["isNewUser"] is False
                    assert cb2["loginId"] == oauth_uid

                # 8. prod 下 /auth/dev/login 严格拒绝（403）
                async with client.post(
                    f"{base_url}/auth/dev/login",
                    json={"account": "admin_bypass"},
                ) as resp:
                    assert resp.status == 403

        # 9. dev profile 且 dev_login_enabled=True 时允许 /auth/dev/login
        dev_config = SaTokenAuthConfig(
            host="127.0.0.1",
            port=0,
            profile="dev",
            dev_login_enabled=True,
        )
        dev_app = create_auth_app(MemoryAuthStore(), MemoryTokenSessionStore(), dev_config)
        async with _start_loopback_server(dev_app) as dev_url:
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    f"{dev_url}/auth/dev/login",
                    json={"account": "dev_tester"},
                ) as resp:
                    assert resp.status == 200
                    dev_data = (await resp.json())["data"]
                    assert dev_data["loginId"] == "dev_tester"
                    dev_token = dev_data["tokenValue"]

                async with client.get(f"{dev_url}/auth/isLogin", headers={"satoken": dev_token}) as resp:
                    assert (await resp.json())["data"] == {"isLogin": True, "loginId": "dev_tester"}

                async with client.get(f"{dev_url}/auth/userInfo", headers={"satoken": dev_token}) as resp:
                    assert resp.status == 200
                    assert (await resp.json())["data"]["loginId"] == "dev_tester"

    asyncio.run(_run())
