"""玄冥服务端运维管理命令行工具集（对应 Go ``identitykeygen``、``password_admin``、``lexicon``、``merge_zone`` 与 ``data_consistency_check``）。

子命令一览：
1. ``identitykeygen``：基于 ``cryptography`` 生成/校验 Gate 内部认证 Ed25519 密钥对；
2. ``password-admin``：``hash`` / ``verify`` / ``set`` / ``disable`` 管理 ``login`` 库的 ``user_accounts``（全程不记录明文口令）；
3. ``lexicon``：``validate`` / ``check`` / ``stats`` 校验与测试 ``content_guard`` 词库文件；
4. ``merge-zone``：``plan`` / ``validate`` / ``apply`` 执行不停服合服映射校验与受控写入（支持 ``--dry-run``）；
5. ``data-consistency-check``：严格只读跨表一致性审计（校验 ``player_route`` / ``account_role`` / ``mail`` / ``guild`` 与帮会榜）。
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import inspect
import json
import logging
import os
import re
import secrets
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from xuanming.content_guard.engine import Engine, SceneClass, Verdict
from xuanming.content_guard.wordlist import (
    Category,
    LexiconFormatError,
    LexiconWordError,
    category_name,
    compile_matcher,
    parse_lexicon,
)
from xuanming.login.password_auth import (
    SqlPasswordAuthStore,
    hash_password,
    open_password_auth_pool,
    parse_mysql_dsn,
    validate_password_account,
    verify_password_hash,
)

from .dbcheck import _assert_identifier, _assert_read_only_sql, _decode_str

logger = logging.getLogger(__name__)

# 供调用方与测试统一引用，与 xuanming.login.password_auth.verify_password_hash 保持一致。
verify_password = verify_password_hash

# ---------------------------------------------------------------------------
# 1. identitykeygen（对应 Go go/shared/cmd/identitykeygen）
# ---------------------------------------------------------------------------

SIGNING_KEY_ENV = "MMORPG_INTERNAL_AUTH_SIGNING_KEY"
PUBLIC_KEYS_ENV = "MMORPG_INTERNAL_AUTH_PUBLIC_KEYS"
_HEX_64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_VERIFY_PROBE_MSG = b"mmorpg-identitykeygen-verify/v1"


@dataclass(frozen=True, slots=True)
class IdentityKeypair:
    """Gate 内部认证 Ed25519 密钥对（均为 64 位小写十六进制）。"""

    signing_key_hex: str
    public_key_hex: str

    def to_env_lines(self) -> str:
        return (
            f"{SIGNING_KEY_ENV}={self.signing_key_hex}\n"
            f"{PUBLIC_KEYS_ENV}={self.public_key_hex}\n"
        )

    def to_dict(self) -> dict[str, str]:
        return {
            SIGNING_KEY_ENV: self.signing_key_hex,
            PUBLIC_KEYS_ENV: self.public_key_hex,
        }


def generate_identity_keypair() -> IdentityKeypair:
    """生成一对随机 32 字节 Ed25519 签名种子与对应公钥。"""
    seed = secrets.token_bytes(32)
    private_key = Ed25519PrivateKey.from_private_bytes(seed)
    public_raw = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return IdentityKeypair(
        signing_key_hex=seed.hex(),
        public_key_hex=public_raw.hex(),
    )


def verify_identity_seed(seed_hex: str) -> str:
    """校验 64 位十六进制 Ed25519 私钥种子，完成签名/验签自检并返回配对的 64 位公钥。"""
    cleaned = seed_hex.strip()
    if not _HEX_64_RE.match(cleaned):
        raise ValueError(f"{SIGNING_KEY_ENV} must be exactly 64 hex characters")
    seed = bytes.fromhex(cleaned)
    private_key = Ed25519PrivateKey.from_private_bytes(seed)
    public_key = private_key.public_key()
    sig = private_key.sign(_VERIFY_PROBE_MSG)
    try:
        public_key.verify(sig, _VERIFY_PROBE_MSG)
    except InvalidSignature as exc:  # pragma: no cover - Ed25519 内部自洽性兜底
        raise ValueError("Ed25519 self-verification failed") from exc
    return public_key.public_bytes(Encoding.Raw, PublicFormat.Raw).hex()


# ---------------------------------------------------------------------------
# 2. password-admin（对应 Go go/login/cmd/password_admin）
# ---------------------------------------------------------------------------


def _read_password_input(
    *,
    from_stdin: bool,
    confirm: bool,
    stream: TextIO | None = None,
) -> str:
    """安全读取明文口令（仅从 stdin 或 TTY 读入，严禁在日志或错误消息中回显口令）。"""
    if from_stdin:
        reader = sys.stdin if stream is None else stream
        raw_lines = [line.rstrip("\r\n") for line in reader.read().splitlines()]
        if confirm:
            if len(raw_lines) != 2:
                raise ValueError("password-stdin requires exactly two confirmation lines")
            if raw_lines[0] != raw_lines[1]:
                raise ValueError("password confirmation does not match")
            if not raw_lines[0]:
                raise ValueError("password must not be empty")
            return raw_lines[0]
        if len(raw_lines) not in (1, 2):
            raise ValueError("password-stdin requires one password line")
        if len(raw_lines) == 2 and raw_lines[0] != raw_lines[1]:
            raise ValueError("password confirmation does not match")
        if not raw_lines[0]:
            raise ValueError("password must not be empty")
        return raw_lines[0]

    first = getpass.getpass("Password: ")
    if not first:
        raise ValueError("password must not be empty")
    if confirm:
        second = getpass.getpass("Confirm password: ")
        if first != second:
            raise ValueError("password confirmation does not match")
    return first


async def password_admin_verify_account(pool, account: str, password: str) -> bool:
    """使用 ``SqlPasswordAuthStore`` 从 ``user_accounts`` 查询哈希并校验口令。"""
    if not validate_password_account(account):
        raise ValueError("invalid account format")
    store = SqlPasswordAuthStore(pool)
    _canonical, encoded, found = await store.lookup_password_hash(account)
    if not found or not encoded:
        return False
    return verify_password(password, encoded)


async def password_admin_set(
    pool,
    account: str,
    password: str,
    *,
    create: bool = False,
    force: bool = False,
) -> str:
    """在 ``user_accounts`` 中受控写入口令哈希，返回 ``"inserted"`` 或 ``"updated"``。

    默认遵守 Go ``password_admin`` 安全口径：不覆盖已有非空口令哈希（需先 ``disable`` 或显式 ``force=True``）。
    """
    if pool is None:
        raise ValueError("pool is required for password-admin set")
    if not validate_password_account(account):
        raise ValueError("invalid account format")
    encoded = hash_password(password)

    async with pool.acquire() as conn:
        if hasattr(conn, "begin"):
            await conn.begin()
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT account, password FROM user_accounts WHERE account = %s FOR UPDATE",
                    (account,),
                )
                rows = await cursor.fetchall()
                if len(rows) > 1:
                    raise RuntimeError(f"account matched {len(rows)} rows; primary key integrity violated")
                if not rows:
                    if not create:
                        raise ValueError(
                            "account does not exist; pass --create to provision a new password account"
                        )
                    await cursor.execute(
                        "INSERT INTO user_accounts (account, password) VALUES (%s, %s)",
                        (account, encoded),
                    )
                    action = "inserted"
                else:
                    if create:
                        raise ValueError(
                            "account already exists; run without --create to set password for existing account"
                        )
                    existing_hash = _decode_str(rows[0][1])
                    if existing_hash and not force:
                        raise ValueError(
                            "account already has a password hash; run disable first or pass --force"
                        )
                    await cursor.execute(
                        "UPDATE user_accounts SET password = %s WHERE account = %s",
                        (encoded, account),
                    )
                    action = "updated"
            if hasattr(conn, "commit"):
                await conn.commit()
        except Exception:
            if hasattr(conn, "rollback"):
                await conn.rollback()
            raise
    logger.info("password_admin set succeeded: account=%s action=%s", account, action)
    return action


async def password_admin_disable(pool, account: str) -> bool:
    """停用指定账号的口令登录（将 ``user_accounts.password`` 置为 ``NULL``）。"""
    if pool is None:
        raise ValueError("pool is required for password-admin disable")
    if not validate_password_account(account):
        raise ValueError("invalid account format")
    async with pool.acquire() as conn:
        if hasattr(conn, "begin"):
            await conn.begin()
        try:
            async with conn.cursor() as cursor:
                affected = await cursor.execute(
                    "UPDATE user_accounts SET password = NULL WHERE account = %s",
                    (account,),
                )
                rowcount = getattr(cursor, "rowcount", None)
                updated = int(rowcount) if isinstance(rowcount, int) and rowcount >= 0 else int(affected or 0)
            if hasattr(conn, "commit"):
                await conn.commit()
        except Exception:
            if hasattr(conn, "rollback"):
                await conn.rollback()
            raise
    if updated <= 0:
        logger.warning("password_admin disable found no matching account: account=%s", account)
        return False
    logger.info("password_admin disabled password login: account=%s", account)
    return True


# ---------------------------------------------------------------------------
# 3. lexicon（对应 Go go/content_guard/cmd/lexicon）
# ---------------------------------------------------------------------------

_SCENE_MAP: dict[str, SceneClass] = {
    "name": SceneClass.NAME,
    "notice": SceneClass.NOTICE,
    "chat": SceneClass.CHAT,
    "private_label": SceneClass.PRIVATE_LABEL,
    "private-label": SceneClass.PRIVATE_LABEL,
}


def parse_scene_class(name: str) -> SceneClass:
    """解析内容安全场景名。"""
    key = name.strip().lower()
    if key not in _SCENE_MAP:
        raise ValueError(
            f"unknown scene {name!r}; expected one of: name, notice, chat, private_label"
        )
    return _SCENE_MAP[key]


def validate_lexicon_file(path: Path | str) -> dict[str, object]:
    """解析并试编译词库文件，校验全部条目合法性并返回统计信息。"""
    file_path = Path(path)
    raw = file_path.read_bytes()
    lexicon = parse_lexicon(raw)
    _matcher, report = compile_matcher(lexicon)
    return {
        "valid": True,
        "file": str(file_path),
        "bytes": len(raw),
        "deny_entries": len(lexicon.deny),
        "allow_entries": len(lexicon.allow),
        "compiled_deny_words": report.deny_words,
        "compiled_allow_words": report.allow_words,
        "dropped": report.dropped,
        "duplicates": report.duplicates,
    }


def check_lexicon_text(
    path: Path | str | None,
    text: str,
    *,
    scene: str = "chat",
) -> dict[str, object]:
    """使用 ``content_guard.Engine`` 在指定词库与场景下判定输入文本。"""
    scene_class = parse_scene_class(scene)
    engine = Engine()
    if path is not None:
        lexicon = parse_lexicon(Path(path).read_bytes())
        engine.swap(1, lexicon)
    res = engine.check(scene_class, text)
    return {
        "scene": scene_class.name.lower(),
        "verdict": Verdict(res.verdict).name,
        "category": category_name(res.category),
        "masked": res.masked,
        "version": res.version,
    }


def lexicon_stats(path: Path | str) -> dict[str, object]:
    """统计词库文件条目分布、分类明细与编译态词数。"""
    file_path = Path(path)
    raw = file_path.read_bytes()
    lexicon = parse_lexicon(raw)
    _matcher, report = compile_matcher(lexicon)
    cat_counts: Counter[str] = Counter()
    for entry in lexicon.deny:
        cat_counts[category_name(entry.category)] += 1
    by_category = {
        category_name(cat): cat_counts.get(category_name(cat), 0)
        for cat in (
            Category.POLITICS,
            Category.PORN,
            Category.ABUSE,
            Category.AD,
            Category.IMPERSONATION,
            Category.OTHER,
        )
    }
    return {
        "file": str(file_path),
        "bytes": len(raw),
        "deny_entries": len(lexicon.deny),
        "allow_entries": len(lexicon.allow),
        "compiled_deny_words": report.deny_words,
        "compiled_allow_words": report.allow_words,
        "dropped": report.dropped,
        "duplicates": report.duplicates,
        "by_category": by_category,
    }


# ---------------------------------------------------------------------------
# 4. merge-zone（对应 Go tools/merge_zone）
# ---------------------------------------------------------------------------

ROUTE_ZONE_MAX = 999_999
MAX_MERGED_ZONE_HOPS = 16


class ZoneMergeError(ValueError):
    """合服映射参数不合法、目录被冻结、成环或出现链式合服。"""


@dataclass(frozen=True, slots=True)
class ZoneMergePlan:
    """单次合服操作的校验与执行计划。"""

    source_zone: int
    target_zone: int
    action: str  # "insert" | "noop"
    already_merged: bool
    final_zone: int
    run_id: str
    dry_run: bool = False
    applied: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "source_zone": self.source_zone,
            "target_zone": self.target_zone,
            "action": self.action,
            "already_merged": self.already_merged,
            "final_zone": self.final_zone,
            "run_id": self.run_id,
            "dry_run": self.dry_run,
            "applied": self.applied,
        }


def _validate_zone_id(zone: int, label: str) -> int:
    if not isinstance(zone, int) or isinstance(zone, bool) or not (1 <= zone <= ROUTE_ZONE_MAX):
        raise ZoneMergeError(f"{label} must be in [1, {ROUTE_ZONE_MAX}], got {zone!r}")
    return zone


def resolve_merged_zone(merges: Mapping[int, int], zone: int) -> int:
    """沿 ``zone_merged_into`` 解析最终区；检测到成环、目标为 0 或超过 16 跳时 fail-closed 报错。"""
    cur = zone
    visited: set[int] = set()
    for hops in range(MAX_MERGED_ZONE_HOPS + 1):
        if cur not in merges:
            return cur
        if cur in visited:
            raise ZoneMergeError(f"merged zone cycle detected involving zone {cur}")
        visited.add(cur)
        nxt = int(merges[cur])
        if nxt <= 0 or nxt > ROUTE_ZONE_MAX:
            raise ZoneMergeError(f"zone {cur} maps to out-of-range zone {nxt}")
        if hops == MAX_MERGED_ZONE_HOPS:
            raise ZoneMergeError(
                f"merged zone chain for zone {zone} exceeds hop limit {MAX_MERGED_ZONE_HOPS}"
            )
        cur = nxt
    raise ZoneMergeError(f"merged zone chain for zone {zone} did not terminate")


def validate_merge_table(merges: Mapping[int, int]) -> dict[int, int]:
    """校验整张 ``zone_merged_into`` 映射表并返回每个源区对应的最终区映射。"""
    normalized: dict[int, int] = {}
    for raw_src, raw_dst in merges.items():
        src = _validate_zone_id(int(raw_src), "src_zone")
        dst = _validate_zone_id(int(raw_dst), "dst_zone")
        if src == dst:
            raise ZoneMergeError(f"self-loop detected in zone_merged_into: zone {src} -> {dst}")
        normalized[src] = dst

    finals: dict[int, int] = {}
    for src in sorted(normalized):
        finals[src] = resolve_merged_zone(normalized, src)
    return finals


def new_merge_run_id(source_zone: int, target_zone: int, *, now: datetime | None = None) -> str:
    ts = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    return f"{source_zone}-{target_zone}-{ts.strftime('%Y%m%dT%H%M%SZ')}"


def plan_zone_merge(
    existing_merges: Mapping[int, int],
    source_zone: int,
    target_zone: int,
    *,
    writable: bool = True,
    now: datetime | None = None,
) -> ZoneMergePlan:
    """纯函数校验 ``source_zone -> target_zone`` 合服计划（对齐 Go ``decideMerge``）。

    拦截规则：
    - ``route_meta.writable=0`` 时拒绝；
    - ``source_zone == target_zone`` 或越界时拒绝；
    - 相同映射已存在时幂等返回 ``action="noop"``；
    - 源区已合入其他目标区时拒绝；
    - 目标区自身已被合走时拒绝（若最终区就是源区则报成环，否则报链式合服并提示合入最终区）；
    - 写入后整张映射表必须通过无环与跳数校验。
    """
    src = _validate_zone_id(source_zone, "source_zone")
    dst = _validate_zone_id(target_zone, "target_zone")
    if src == dst:
        raise ZoneMergeError(f"source_zone and target_zone must differ (both {src})")
    if not writable:
        raise ZoneMergeError("route_meta.writable=0: route catalog is frozen for maintenance")

    merges = {int(k): int(v) for k, v in existing_merges.items()}
    if src in merges:
        cur_dst = merges[src]
        if cur_dst == dst:
            final_zone = resolve_merged_zone(merges, src)
            return ZoneMergePlan(
                source_zone=src,
                target_zone=dst,
                action="noop",
                already_merged=True,
                final_zone=final_zone,
                run_id="",
            )
        raise ZoneMergeError(
            f"zone {src} is already merged into zone {cur_dst}: unmerge first before changing target"
        )

    if dst in merges:
        next_dst = merges[dst]
        final = resolve_merged_zone(merges, dst)
        if final == src:
            raise ZoneMergeError(
                f"zone {src} -> zone {dst} would form a cycle: zone {dst} already resolves to zone {src}"
            )
        raise ZoneMergeError(
            f"chained merge rejected: target zone {dst} was already merged into zone {next_dst}; "
            f"merge into its final zone {final} instead"
        )

    candidate = dict(merges)
    candidate[src] = dst
    finals = validate_merge_table(candidate)
    return ZoneMergePlan(
        source_zone=src,
        target_zone=dst,
        action="insert",
        already_merged=False,
        final_zone=finals[src],
        run_id=new_merge_run_id(src, dst, now=now),
    )


async def fetch_zone_merges(pool) -> tuple[bool, dict[int, int]]:
    """只读拉取 ``route_meta.writable`` 与全部 ``zone_merged_into`` 映射。"""
    if pool is None:
        raise ValueError("pool is required to read zone_merged_into")
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT writable FROM route_meta WHERE id = 1")
            meta_row = await cursor.fetchone()
            if meta_row is None:
                raise ZoneMergeError("route_meta has no row id=1: database is not initialized")
            writable = bool(int(meta_row[0]))

            await cursor.execute(
                "SELECT src_zone, dst_zone FROM zone_merged_into ORDER BY src_zone"
            )
            rows = await cursor.fetchall()
            merges = {int(r[0]): int(r[1]) for r in rows}
    return writable, merges


async def apply_zone_merge(
    pool,
    source_zone: int,
    target_zone: int,
    *,
    dry_run: bool = False,
    now: datetime | None = None,
) -> ZoneMergePlan:
    """校验并执行合服映射写入；``dry_run=True`` 仅做只读快照校验，不执行任何写 SQL。"""
    if pool is None:
        raise ValueError("pool is required for merge-zone apply")

    if dry_run:
        writable, merges = await fetch_zone_merges(pool)
        plan = plan_zone_merge(merges, source_zone, target_zone, writable=writable, now=now)
        return ZoneMergePlan(
            source_zone=plan.source_zone,
            target_zone=plan.target_zone,
            action=plan.action,
            already_merged=plan.already_merged,
            final_zone=plan.final_zone,
            run_id=plan.run_id,
            dry_run=True,
            applied=False,
        )

    now_ms = int((time.time() if now is None else now.timestamp()) * 1000)
    async with pool.acquire() as conn:
        if hasattr(conn, "begin"):
            await conn.begin()
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT writable FROM route_meta WHERE id = 1 FOR UPDATE")
                meta_row = await cursor.fetchone()
                if meta_row is None:
                    raise ZoneMergeError("route_meta has no row id=1: database is not initialized")
                writable = bool(int(meta_row[0]))

                for zone in sorted({int(source_zone), int(target_zone)}):
                    await cursor.execute(
                        "SELECT dst_zone FROM zone_merged_into WHERE src_zone = %s FOR UPDATE",
                        (zone,),
                    )
                    await cursor.fetchone()

                await cursor.execute(
                    "SELECT src_zone, dst_zone FROM zone_merged_into ORDER BY src_zone"
                )
                rows = await cursor.fetchall()
                merges = {int(r[0]): int(r[1]) for r in rows}

                plan = plan_zone_merge(merges, source_zone, target_zone, writable=writable, now=now)
                if not plan.already_merged:
                    await cursor.execute(
                        "INSERT INTO zone_merged_into (src_zone, dst_zone, run_id, merged_at_ms) "
                        "VALUES (%s, %s, %s, %s)",
                        (plan.source_zone, plan.target_zone, plan.run_id, now_ms),
                    )
            if hasattr(conn, "commit"):
                await conn.commit()
        except Exception:
            if hasattr(conn, "rollback"):
                await conn.rollback()
            raise

    logger.info(
        "merge_zone apply finished: src=%d dst=%d action=%s run_id=%s",
        plan.source_zone,
        plan.target_zone,
        plan.action,
        plan.run_id or "-",
    )
    return ZoneMergePlan(
        source_zone=plan.source_zone,
        target_zone=plan.target_zone,
        action=plan.action,
        already_merged=plan.already_merged,
        final_zone=plan.final_zone,
        run_id=plan.run_id,
        dry_run=False,
        applied=not plan.already_merged,
    )


# ---------------------------------------------------------------------------
# 5. data-consistency-check（对应 Go tools/data_consistency_check，严格只读）
# ---------------------------------------------------------------------------

ZONE_RANK_KEY_PREFIX = "guild_rank:zone:"
_GUILD_RANK_KEY_RE = re.compile(r"^guild_rank:zone:\{gr:([1-9][0-9]*)\}$")
_ACCOUNT_LINKAGE_SQL = (
    "SELECT COUNT(*), "
    "COALESCE(SUM(CASE WHEN a.player_id IS NULL OR LENGTH(a.account) = 0 THEN 1 ELSE 0 END), 0) "
    "FROM player_route r LEFT JOIN account_role a ON a.player_id = r.player_id "
    "WHERE r.home_zone IS NOT NULL"
)


@dataclass(frozen=True, slots=True)
class ConsistencyCheckItem:
    """单条跨表一致性检查结论（``severity`` 取值 ``info`` / ``warn`` / ``block`` / ``fail``）。"""

    name: str
    severity: str
    bad_count: int
    total_count: int
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "severity": self.severity,
            "bad_count": self.bad_count,
            "total_count": self.total_count,
            "notes": self.notes,
        }


@dataclass(frozen=True, slots=True)
class ConsistencyReport:
    """只读数据一致性体检汇总报告。"""

    dry_run: bool
    passed: bool
    live_zones: tuple[int, ...]
    checks: tuple[ConsistencyCheckItem, ...]

    @property
    def blocking_count(self) -> int:
        return sum(1 for item in self.checks if item.severity in ("block", "fail"))

    def to_dict(self) -> dict[str, object]:
        return {
            "dry_run": self.dry_run,
            "passed": self.passed,
            "blocking_count": self.blocking_count,
            "live_zones": list(self.live_zones),
            "checks": [item.to_dict() for item in self.checks],
        }


def parse_guild_rank_zone_key(key: str) -> int | None:
    """解析 ``guild_rank:zone:{gr:<zone>}`` 榜键并返回区号；不符合规范返回 ``None``。"""
    match = _GUILD_RANK_KEY_RE.match(key)
    if not match:
        return None
    zone = int(match.group(1))
    if not (1 <= zone <= ROUTE_ZONE_MAX):
        return None
    return zone


async def _ro_query_one(pool, sql: str, args=None):
    _assert_read_only_sql(sql)
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            if args is None:
                await cursor.execute(sql)
            else:
                await cursor.execute(sql, args)
            return await cursor.fetchone()


async def _ro_query_all(pool, sql: str, args=None):
    _assert_read_only_sql(sql)
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            if args is None:
                await cursor.execute(sql)
            else:
                await cursor.execute(sql, args)
            return await cursor.fetchall()


async def _infer_live_zones(route_pool) -> tuple[set[int], ConsistencyCheckItem]:
    """在 ``player_route.home_zone`` 上按索引跳读全部不同区号，同时核对值范围形状。"""
    live: set[int] = set()
    malformed_samples: list[str] = []
    scanned = 0
    malformed = 0
    cursor_from = 0

    try:
        while True:
            row = await _ro_query_one(
                route_pool,
                "SELECT home_zone FROM player_route WHERE home_zone >= %s ORDER BY home_zone LIMIT 1",
                (cursor_from,),
            )
            if row is None or row[0] is None:
                break
            zone = int(row[0])
            if zone < cursor_from:
                raise RuntimeError(f"next home_zone from {cursor_from} returned non-increasing {zone}")
            scanned += 1
            if zone == 0 or zone > ROUTE_ZONE_MAX:
                malformed += 1
                if len(malformed_samples) < 10:
                    malformed_samples.append(f"home_zone={zone}")
            else:
                live.add(zone)
            cursor_from = zone + 1
    except Exception as exc:
        return set(), ConsistencyCheckItem(
            name="route home_zone values",
            severity="fail",
            bad_count=0,
            total_count=0,
            notes=f"CHECK FAILED: player_route home_zone scan failed: {exc}",
        )

    if malformed > 0:
        return live, ConsistencyCheckItem(
            name="route home_zone values",
            severity="block",
            bad_count=malformed,
            total_count=scanned,
            notes=(
                f"{malformed}/{scanned} distinct player_route.home_zone values malformed "
                f"(excluded from live zones), samples: {', '.join(malformed_samples)}"
            ),
        )
    return live, ConsistencyCheckItem(
        name="route home_zone values",
        severity="info",
        bad_count=0,
        total_count=scanned,
        notes=f"{scanned} distinct player_route.home_zone values scanned, none malformed",
    )


async def _check_player_account_linkage(route_pool) -> ConsistencyCheckItem:
    if route_pool is None:
        return ConsistencyCheckItem(
            name="player->account linkage",
            severity="warn",
            bad_count=0,
            total_count=0,
            notes="NOT CHECKED: no route database handle",
        )
    try:
        row = await _ro_query_one(route_pool, _ACCOUNT_LINKAGE_SQL)
        total = int(row[0] or 0) if row else 0
        bad = int(row[1] or 0) if row else 0
    except Exception as exc:
        return ConsistencyCheckItem(
            name="player->account linkage",
            severity="fail",
            bad_count=0,
            total_count=0,
            notes=f"CHECK FAILED: account_role linkage query failed: {exc}",
        )
    if bad > 0:
        return ConsistencyCheckItem(
            name="player->account linkage",
            severity="block",
            bad_count=bad,
            total_count=total,
            notes=f"{bad}/{total} player_route rows lack a valid account_role entry",
        )
    return ConsistencyCheckItem(
        name="player->account linkage",
        severity="info",
        bad_count=0,
        total_count=total,
        notes="all routed players have valid account_role rows",
    )


async def _check_mail_state_orphans(mail_pool) -> ConsistencyCheckItem:
    if mail_pool is None:
        return ConsistencyCheckItem(
            name="mail_state.mail_id",
            severity="warn",
            bad_count=0,
            total_count=0,
            notes="NOT CHECKED: no mail database handle",
        )
    distinct_ids = "(SELECT DISTINCT mail_id FROM mail_state) d"
    orphan_filter = " WHERE NOT EXISTS (SELECT 1 FROM mail_message m WHERE m.mail_id = d.mail_id)"
    try:
        total_row = await _ro_query_one(mail_pool, f"SELECT COUNT(*) FROM {distinct_ids}")
        total = int(total_row[0] or 0) if total_row else 0
    except Exception as exc:
        return ConsistencyCheckItem(
            name="mail_state.mail_id",
            severity="warn",
            bad_count=0,
            total_count=0,
            notes=f"NOT CHECKED: mail_state unreadable: {exc}",
        )
    try:
        bad_row = await _ro_query_one(
            mail_pool, f"SELECT COUNT(*) FROM {distinct_ids}{orphan_filter}"
        )
        bad = int(bad_row[0] or 0) if bad_row else 0
    except Exception as exc:
        return ConsistencyCheckItem(
            name="mail_state.mail_id",
            severity="warn",
            bad_count=0,
            total_count=total,
            notes=f"NOT CHECKED: orphan mail_id query failed: {exc}",
        )
    if bad == 0:
        return ConsistencyCheckItem(
            name="mail_state.mail_id",
            severity="info",
            bad_count=0,
            total_count=total,
            notes=f"0 orphan mail_id among {total} distinct ids referenced by mail_state",
        )
    sample_str = ""
    try:
        sample_rows = await _ro_query_all(
            mail_pool,
            f"SELECT d.mail_id FROM {distinct_ids}{orphan_filter} ORDER BY d.mail_id LIMIT 5",
        )
        sample_str = ",".join(str(int(r[0])) for r in sample_rows)
    except Exception as exc:
        sample_str = f"samples unavailable: {exc}"
    return ConsistencyCheckItem(
        name="mail_state.mail_id",
        severity="warn",
        bad_count=bad,
        total_count=total,
        notes=f"{bad} mail_id referenced by mail_state missing from mail_message (e.g. [{sample_str}])",
    )


async def _check_guild_zone_fk(
    guild_pool,
    known_zones: set[int],
    *,
    guild_schema: str = "",
) -> ConsistencyCheckItem:
    if guild_pool is None:
        return ConsistencyCheckItem(
            name="guild.zone_id",
            severity="warn",
            bad_count=0,
            total_count=0,
            notes="NOT CHECKED: no guild database handle",
        )
    if guild_schema:
        _assert_identifier(guild_schema, "guild_schema")
        guild_table = f"`{guild_schema}`.`guild`"
    else:
        guild_table = "`guild`"
    try:
        total_row = await _ro_query_one(guild_pool, f"SELECT COUNT(*) FROM {guild_table}")
        total = int(total_row[0] or 0) if total_row else 0
    except Exception as exc:
        return ConsistencyCheckItem(
            name="guild.zone_id",
            severity="warn",
            bad_count=0,
            total_count=0,
            notes=f"NOT CHECKED: {guild_table} unreadable: {exc}",
        )
    try:
        zone_rows = await _ro_query_all(guild_pool, f"SELECT DISTINCT zone_id FROM {guild_table}")
    except Exception as exc:
        return ConsistencyCheckItem(
            name="guild.zone_id",
            severity="warn",
            bad_count=0,
            total_count=total,
            notes=f"NOT CHECKED: distinct zone_id query failed: {exc}",
        )

    dead = sorted(int(r[0]) for r in zone_rows if int(r[0]) not in known_zones)
    if not dead:
        return ConsistencyCheckItem(
            name="guild.zone_id",
            severity="info",
            bad_count=0,
            total_count=total,
            notes="all guild rows reference live zones",
        )
    bad = 0
    for zone in dead:
        cnt_row = await _ro_query_one(
            guild_pool, f"SELECT COUNT(*) FROM {guild_table} WHERE zone_id = %s", (zone,)
        )
        bad += int(cnt_row[0] or 0) if cnt_row else 0
    return ConsistencyCheckItem(
        name="guild.zone_id",
        severity="block",
        bad_count=bad,
        total_count=total,
        notes=f"{bad} guilds reference zones {dead} outside known live zones",
    )


async def _check_guild_rank_orphans(
    known_zones: set[int],
    rank_keys_provider: Callable[[], Iterable[str] | Awaitable[Iterable[str]]] | None = None,
) -> ConsistencyCheckItem:
    if rank_keys_provider is None:
        return ConsistencyCheckItem(
            name="guild_rank:zone:*",
            severity="warn",
            bad_count=0,
            total_count=0,
            notes="NOT CHECKED: no guild rank key source provided",
        )
    try:
        raw_keys = rank_keys_provider()
        if inspect.isawaitable(raw_keys):
            raw_keys = await raw_keys
        seen = sorted(set(str(k) for k in raw_keys))
    except Exception as exc:
        return ConsistencyCheckItem(
            name="guild_rank:zone:*",
            severity="fail",
            bad_count=0,
            total_count=0,
            notes=f"NOT CHECKED: guild rank key scan failed: {exc}",
        )

    orphans: set[int] = set()
    unrecognized: list[str] = []
    for key in seen:
        zone = parse_guild_rank_zone_key(key)
        if zone is None:
            unrecognized.append(key)
            continue
        if zone not in known_zones:
            orphans.add(zone)

    notes: list[str] = []
    if orphans:
        severity = "block"
        notes.append(f"orphan guild rank ZSETs for zones {sorted(orphans)} outside live zones")
    elif unrecognized:
        severity = "warn"
    else:
        severity = "info"
        notes.append("no orphan guild rank ZSETs")
    if unrecognized:
        samples = ", ".join(repr(k) for k in unrecognized[:5])
        notes.append(f"{len(unrecognized)}/{len(seen)} keys have unrecognized shape, e.g. [{samples}]")
    return ConsistencyCheckItem(
        name="guild_rank:zone:*",
        severity=severity,
        bad_count=len(orphans),
        total_count=len(seen),
        notes="; ".join(notes),
    )


async def run_data_consistency_check(
    *,
    pool=None,
    route_pool=None,
    guild_pool=None,
    mail_pool=None,
    rank_keys_provider: Callable[[], Iterable[str] | Awaitable[Iterable[str]]] | None = None,
    live_zones: Sequence[int] | None = None,
    guild_schema: str = "",
    dry_run: bool = False,
) -> ConsistencyReport:
    """执行严格只读跨表一致性审计（支持注入单库或分库异步连接池以及 ``dry_run``）。"""
    if dry_run:
        planned_zones = tuple(sorted(set(int(z) for z in (live_zones or ()))))
        checks = (
            ConsistencyCheckItem(
                name="route home_zone values",
                severity="info",
                bad_count=0,
                total_count=0,
                notes="DRY-RUN: read-only scan of player_route.home_zone skipped",
            ),
            ConsistencyCheckItem(
                name="player->account linkage",
                severity="info",
                bad_count=0,
                total_count=0,
                notes="DRY-RUN: read-only LEFT JOIN player_route/account_role skipped",
            ),
            ConsistencyCheckItem(
                name="mail_state.mail_id",
                severity="info",
                bad_count=0,
                total_count=0,
                notes="DRY-RUN: read-only anti-join mail_state/mail_message skipped",
            ),
            ConsistencyCheckItem(
                name="guild.zone_id",
                severity="info",
                bad_count=0,
                total_count=0,
                notes="DRY-RUN: read-only guild.zone_id check skipped",
            ),
            ConsistencyCheckItem(
                name="guild_rank:zone:*",
                severity="info",
                bad_count=0,
                total_count=0,
                notes="DRY-RUN: read-only guild rank key check skipped",
            ),
        )
        return ConsistencyReport(dry_run=True, passed=True, live_zones=planned_zones, checks=checks)

    eff_route = route_pool if route_pool is not None else pool
    eff_guild = guild_pool if guild_pool is not None else pool
    eff_mail = mail_pool if mail_pool is not None else pool

    if live_zones is not None:
        known_zones = {int(z) for z in live_zones}
        for z in known_zones:
            _validate_zone_id(z, "live_zone")
        shape_item = ConsistencyCheckItem(
            name="route home_zone values",
            severity="warn",
            bad_count=0,
            total_count=0,
            notes="NOT CHECKED: --live-zones given explicitly, route values not scanned",
        )
    else:
        if eff_route is None:
            raise ValueError("route_pool or explicit live_zones is required for data-consistency-check")
        known_zones, shape_item = await _infer_live_zones(eff_route)

    checks = (
        shape_item,
        await _check_player_account_linkage(eff_route),
        await _check_mail_state_orphans(eff_mail),
        await _check_guild_zone_fk(eff_guild, known_zones, guild_schema=guild_schema),
        await _check_guild_rank_orphans(known_zones, rank_keys_provider=rank_keys_provider),
    )
    blocking = sum(1 for item in checks if item.severity in ("block", "fail"))
    logger.info(
        "data_consistency_check finished: live_zones=%d checks=%d blocking=%d",
        len(known_zones),
        len(checks),
        blocking,
    )
    return ConsistencyReport(
        dry_run=False,
        passed=blocking == 0,
        live_zones=tuple(sorted(known_zones)),
        checks=checks,
    )


# ---------------------------------------------------------------------------
# 统一 CLI 入口（admin_cli.main）
# ---------------------------------------------------------------------------


def _emit_output(payload: object, *, as_json: bool, text: str) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(text)


async def _open_mysql_pool(dsn: str, *, read_only: bool = False):
    import aiomysql

    params = parse_mysql_dsn(dsn)
    init_cmd = (
        "SET SESSION transaction_read_only = 1"
        if read_only
        else "SET SESSION transaction_isolation='READ-COMMITTED'"
    )
    return await aiomysql.create_pool(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        db=params["database"],
        minsize=1,
        maxsize=2,
        connect_timeout=3,
        autocommit=read_only,
        charset="utf8mb4",
        init_command=init_cmd,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="admin-cli",
        description="Xuanming operational admin CLI tools.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # 1. identitykeygen
    p_id = sub.add_parser("identitykeygen", help="Generate or verify Ed25519 internal auth keys.")
    p_id.add_argument("--verify", default="", metavar="SEED_HEX", help="Verify a 64-hex Ed25519 seed.")
    p_id.add_argument(
        "--derive",
        action="store_true",
        help=f"Derive public key from {SIGNING_KEY_ENV} environment variable.",
    )
    p_id.add_argument("--json", action="store_true", dest="as_json", help="Emit JSON output.")

    # 2. password-admin
    p_pw = sub.add_parser("password-admin", help="Manage login user_accounts password credentials.")
    pw_sub = p_pw.add_subparsers(dest="pw_action", required=True)

    p_pw_hash = pw_sub.add_parser("hash", help="Generate a PHC scrypt password hash.")
    p_pw_hash.add_argument("--password-stdin", action="store_true", help="Read password from stdin.")
    p_pw_hash.add_argument("--json", action="store_true", dest="as_json")

    p_pw_verify = pw_sub.add_parser("verify", help="Verify a password against a hash or database account.")
    p_pw_verify.add_argument("--hash", dest="encoded_hash", default="", help="PHC password hash string.")
    p_pw_verify.add_argument("--account", default="", help="Account name to verify against database.")
    p_pw_verify.add_argument("--dsn", default="", help="Login credential database DSN.")
    p_pw_verify.add_argument("--password-stdin", action="store_true", help="Read password from stdin.")
    p_pw_verify.add_argument("--json", action="store_true", dest="as_json")

    p_pw_set = pw_sub.add_parser("set", help="Set password hash for a user_accounts row.")
    p_pw_set.add_argument("--account", required=True, help="Target account name.")
    p_pw_set.add_argument("--dsn", required=True, help="Login credential database DSN.")
    p_pw_set.add_argument("--create", action="store_true", help="Insert a new account row.")
    p_pw_set.add_argument("--force", action="store_true", help="Allow overwriting an existing hash.")
    p_pw_set.add_argument("--password-stdin", action="store_true", help="Read two matching password lines from stdin.")
    p_pw_set.add_argument("--json", action="store_true", dest="as_json")

    p_pw_dis = pw_sub.add_parser("disable", help="Disable password login (set password=NULL).")
    p_pw_dis.add_argument("--account", required=True, help="Target account name.")
    p_pw_dis.add_argument("--dsn", required=True, help="Login credential database DSN.")
    p_pw_dis.add_argument("--json", action="store_true", dest="as_json")

    # 3. lexicon
    p_lex = sub.add_parser("lexicon", help="Validate, test, and inspect content_guard lexicon files.")
    lex_sub = p_lex.add_subparsers(dest="lex_action", required=True)

    p_lex_val = lex_sub.add_parser("validate", help="Validate lexicon file syntax and compilation.")
    p_lex_val.add_argument("file", type=Path, help="Path to #content_guard_lexicon 1 file.")
    p_lex_val.add_argument("--json", action="store_true", dest="as_json")

    p_lex_chk = lex_sub.add_parser("check", help="Evaluate text against a lexicon file.")
    p_lex_chk.add_argument("file", type=Path, nargs="?", default=None, help="Optional lexicon file path.")
    p_lex_chk.add_argument("--text", required=True, help="Text to inspect.")
    p_lex_chk.add_argument("--scene", default="chat", help="Scene class: name|notice|chat|private_label.")
    p_lex_chk.add_argument("--json", action="store_true", dest="as_json")

    p_lex_stats = lex_sub.add_parser("stats", help="Print lexicon entry statistics.")
    p_lex_stats.add_argument("file", type=Path, help="Path to #content_guard_lexicon 1 file.")
    p_lex_stats.add_argument("--json", action="store_true", dest="as_json")

    # 4. merge-zone
    p_mz = sub.add_parser("merge-zone", help="Plan, validate, or apply zone_merged_into mappings.")
    mz_sub = p_mz.add_subparsers(dest="mz_action", required=True)

    p_mz_plan = mz_sub.add_parser("plan", help="Validate and plan a zone merge without writing.")
    p_mz_plan.add_argument("--source-zone", type=int, required=True)
    p_mz_plan.add_argument("--target-zone", type=int, required=True)
    p_mz_plan.add_argument("--existing-json", type=Path, default=None, help="JSON map of existing src->dst merges.")
    p_mz_plan.add_argument("--dsn", default="", help="Route directory shard 0 DSN.")
    p_mz_plan.add_argument("--json", action="store_true", dest="as_json")

    p_mz_val = mz_sub.add_parser("validate", help="Validate an existing zone_merged_into mapping table.")
    p_mz_val.add_argument("--existing-json", type=Path, default=None, help="JSON map of existing src->dst merges.")
    p_mz_val.add_argument("--dsn", default="", help="Route directory shard 0 DSN.")
    p_mz_val.add_argument("--source-zone", type=int, default=0)
    p_mz_val.add_argument("--target-zone", type=int, default=0)
    p_mz_val.add_argument("--json", action="store_true", dest="as_json")

    p_mz_apply = mz_sub.add_parser("apply", help="Apply a zone merge mapping (supports --dry-run).")
    p_mz_apply.add_argument("--source-zone", type=int, required=True)
    p_mz_apply.add_argument("--target-zone", type=int, required=True)
    p_mz_apply.add_argument("--dsn", required=True, help="Route directory shard 0 DSN.")
    p_mz_apply.add_argument("--dry-run", action="store_true", help="Validate against DB without writing.")
    p_mz_apply.add_argument("--json", action="store_true", dest="as_json")

    # 5. data-consistency-check
    p_dcc = sub.add_parser(
        "data-consistency-check",
        help="Read-only cross-table auditor for player_route, account_role, mail, and guild.",
    )
    p_dcc.add_argument("--route-dsn", default="", help="Route/account database DSN.")
    p_dcc.add_argument("--guild-dsn", default="", help="Guild database DSN (defaults to --route-dsn).")
    p_dcc.add_argument("--mail-dsn", default="", help="Mail database DSN (defaults to --route-dsn).")
    p_dcc.add_argument("--guild-schema", default="", help="Optional schema name qualifying the guild table.")
    p_dcc.add_argument("--live-zones", default="", help="Comma-separated live zone IDs.")
    p_dcc.add_argument("--dry-run", action="store_true", help="Print planned read-only checks without connecting.")
    p_dcc.add_argument("--json", action="store_true", dest="as_json")

    return parser


def _load_existing_merges_json(path: Path) -> dict[int, int]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ZoneMergeError(f"existing merge JSON must be an object: {path}")
    return {int(k): int(v) for k, v in raw.items()}


async def _run_identitykeygen(args: argparse.Namespace) -> int:
    if args.verify:
        pub_hex = verify_identity_seed(args.verify)
        payload = {"verified": True, PUBLIC_KEYS_ENV: pub_hex}
        _emit_output(payload, as_json=args.as_json, text=f"{PUBLIC_KEYS_ENV}={pub_hex}")
        return 0
    if args.derive:
        pub_hex = verify_identity_seed(os.environ.get(SIGNING_KEY_ENV, ""))
        payload = {PUBLIC_KEYS_ENV: pub_hex}
        _emit_output(payload, as_json=args.as_json, text=pub_hex)
        return 0
    pair = generate_identity_keypair()
    print(
        "Store the signing key in Secret management only; never commit it or paste it into logs.",
        file=sys.stderr,
    )
    _emit_output(pair.to_dict(), as_json=args.as_json, text=pair.to_env_lines().rstrip("\n"))
    return 0


async def _run_password_admin(args: argparse.Namespace) -> int:
    if args.pw_action == "hash":
        password = _read_password_input(from_stdin=args.password_stdin, confirm=False)
        encoded = hash_password(password)
        _emit_output({"hash": encoded}, as_json=args.as_json, text=encoded)
        return 0

    if args.pw_action == "verify":
        password = _read_password_input(from_stdin=args.password_stdin, confirm=False)
        if args.encoded_hash:
            matched = verify_password(password, args.encoded_hash)
        elif args.account and args.dsn:
            pool = await open_password_auth_pool(args.dsn, minsize=1, maxsize=2)
            try:
                matched = await password_admin_verify_account(pool, args.account, password)
            finally:
                pool.close()
                await pool.wait_closed()
        else:
            raise ValueError("verify requires either --hash or both --account and --dsn")
        _emit_output(
            {"matched": matched},
            as_json=args.as_json,
            text="OK: password matched" if matched else "MISMATCH: password did not match",
        )
        return 0 if matched else 1

    if args.pw_action == "set":
        password = _read_password_input(from_stdin=args.password_stdin, confirm=True)
        pool = await _open_mysql_pool(args.dsn, read_only=False)
        try:
            action = await password_admin_set(
                pool, args.account, password, create=args.create, force=args.force
            )
        finally:
            pool.close()
            await pool.wait_closed()
        _emit_output(
            {"account": args.account, "action": action},
            as_json=args.as_json,
            text=f"password hash {action} for account {args.account}",
        )
        return 0

    if args.pw_action == "disable":
        pool = await _open_mysql_pool(args.dsn, read_only=False)
        try:
            disabled = await password_admin_disable(pool, args.account)
        finally:
            pool.close()
            await pool.wait_closed()
        if not disabled:
            _emit_output(
                {"account": args.account, "disabled": False},
                as_json=args.as_json,
                text=f"account not found: {args.account}",
            )
            return 1
        _emit_output(
            {"account": args.account, "disabled": True},
            as_json=args.as_json,
            text=f"password login disabled for account {args.account}",
        )
        return 0

    raise ValueError(f"unsupported password-admin action: {args.pw_action}")


async def _run_lexicon(args: argparse.Namespace) -> int:
    if args.lex_action == "validate":
        res = validate_lexicon_file(args.file)
        _emit_output(
            res,
            as_json=args.as_json,
            text=(
                f"OK: valid lexicon {res['file']} "
                f"(deny={res['deny_entries']} allow={res['allow_entries']} "
                f"compiled_deny={res['compiled_deny_words']})"
            ),
        )
        return 0
    if args.lex_action == "check":
        res = check_lexicon_text(args.file, args.text, scene=args.scene)
        _emit_output(
            res,
            as_json=args.as_json,
            text=f"verdict={res['verdict']} category={res['category']} masked={res['masked']!r}",
        )
        return 0
    if args.lex_action == "stats":
        res = lexicon_stats(args.file)
        _emit_output(
            res,
            as_json=args.as_json,
            text=(
                f"lexicon stats: deny={res['deny_entries']} allow={res['allow_entries']} "
                f"compiled_deny={res['compiled_deny_words']} compiled_allow={res['compiled_allow_words']} "
                f"dropped={res['dropped']} duplicates={res['duplicates']} bytes={res['bytes']}"
            ),
        )
        return 0
    raise ValueError(f"unsupported lexicon action: {args.lex_action}")


async def _run_merge_zone(args: argparse.Namespace) -> int:
    if args.mz_action == "plan":
        writable = True
        merges: dict[int, int] = {}
        if args.existing_json is not None:
            merges = _load_existing_merges_json(args.existing_json)
        elif args.dsn:
            pool = await _open_mysql_pool(args.dsn, read_only=True)
            try:
                writable, merges = await fetch_zone_merges(pool)
            finally:
                pool.close()
                await pool.wait_closed()
        plan = plan_zone_merge(merges, args.source_zone, args.target_zone, writable=writable)
        _emit_output(
            plan.to_dict(),
            as_json=args.as_json,
            text=(
                f"[PLAN] zone {plan.source_zone} -> zone {plan.target_zone} "
                f"action={plan.action} final_zone={plan.final_zone}"
            ),
        )
        return 0

    if args.mz_action == "validate":
        writable = True
        merges = {}
        if args.existing_json is not None:
            merges = _load_existing_merges_json(args.existing_json)
        elif args.dsn:
            pool = await _open_mysql_pool(args.dsn, read_only=True)
            try:
                writable, merges = await fetch_zone_merges(pool)
            finally:
                pool.close()
                await pool.wait_closed()
        finals = validate_merge_table(merges)
        if args.source_zone or args.target_zone:
            plan = plan_zone_merge(merges, args.source_zone, args.target_zone, writable=writable)
            payload = {"valid": True, "writable": writable, "finals": finals, "plan": plan.to_dict()}
        else:
            payload = {"valid": True, "writable": writable, "finals": finals}
        _emit_output(
            payload,
            as_json=args.as_json,
            text=f"OK: validated {len(merges)} merge mapping(s), writable={writable}",
        )
        return 0

    if args.mz_action == "apply":
        pool = await _open_mysql_pool(args.dsn, read_only=args.dry_run)
        try:
            plan = await apply_zone_merge(
                pool, args.source_zone, args.target_zone, dry_run=args.dry_run
            )
        finally:
            pool.close()
            await pool.wait_closed()
        prefix = "[DRY-RUN]" if plan.dry_run else "[APPLIED]"
        _emit_output(
            plan.to_dict(),
            as_json=args.as_json,
            text=(
                f"{prefix} zone {plan.source_zone} -> zone {plan.target_zone} "
                f"action={plan.action} applied={plan.applied}"
            ),
        )
        return 0

    raise ValueError(f"unsupported merge-zone action: {args.mz_action}")


async def _run_data_consistency_check(args: argparse.Namespace) -> int:
    live_zones: list[int] | None = None
    if args.live_zones.strip():
        live_zones = [int(tok.strip()) for tok in args.live_zones.split(",") if tok.strip()]

    if args.dry_run:
        report = await run_data_consistency_check(
            live_zones=live_zones,
            guild_schema=args.guild_schema,
            dry_run=True,
        )
    else:
        pools = []
        route_pool = None
        guild_pool = None
        mail_pool = None
        try:
            if args.route_dsn:
                route_pool = await _open_mysql_pool(args.route_dsn, read_only=True)
                pools.append(route_pool)
            if args.guild_dsn:
                guild_pool = await _open_mysql_pool(args.guild_dsn, read_only=True)
                pools.append(guild_pool)
            if args.mail_dsn:
                mail_pool = await _open_mysql_pool(args.mail_dsn, read_only=True)
                pools.append(mail_pool)
            report = await run_data_consistency_check(
                pool=route_pool,
                route_pool=route_pool,
                guild_pool=guild_pool if guild_pool is not None else route_pool,
                mail_pool=mail_pool if mail_pool is not None else route_pool,
                live_zones=live_zones,
                guild_schema=args.guild_schema,
                dry_run=False,
            )
        finally:
            for p in pools:
                p.close()
                await p.wait_closed()

    lines = ["=== Data consistency check ==="]
    for item in report.checks:
        lines.append(
            f"[{item.severity.upper():<5s}] {item.name:<28s} bad={item.bad_count} "
            f"total={item.total_count}  {item.notes}"
        )
    lines.append("Result: PASS" if report.passed else f"Result: FAIL ({report.blocking_count} blocking)")
    _emit_output(report.to_dict(), as_json=args.as_json, text="\n".join(lines))
    return 0 if report.passed else 1


async def _dispatch_async(args: argparse.Namespace) -> int:
    if args.command == "identitykeygen":
        return await _run_identitykeygen(args)
    if args.command == "password-admin":
        return await _run_password_admin(args)
    if args.command == "lexicon":
        return await _run_lexicon(args)
    if args.command == "merge-zone":
        return await _run_merge_zone(args)
    if args.command == "data-consistency-check":
        return await _run_data_consistency_check(args)
    raise ValueError(f"unknown admin-cli command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    """运维管理 CLI 统一入口，返回退出码（0=成功，1=校验/业务拒绝，2=致命异常）。"""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_dispatch_async(args))
    except (
        ZoneMergeError,
        LexiconFormatError,
        LexiconWordError,
        ValueError,
    ) as exc:
        logger.error("admin_cli rejected command=%s: %s", args.command, exc)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except (OSError, RuntimeError, json.JSONDecodeError) as exc:
        logger.error("admin_cli fatal error command=%s: %s", args.command, exc)
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
