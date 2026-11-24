"""玄冥服务端运维管理命令行工具集（对应 Go ``identitykeygen``、``password_admin``、``lexicon``、``merge_zone``、``data_consistency_check``、``kafka_dlq``、``devpaynotify`` 与 ``battlescriptreview``）。

子命令一览：
1. ``identitykeygen``：基于 ``cryptography`` 生成/校验 Gate 内部认证 Ed25519 密钥对；
2. ``password-admin``：``hash`` / ``verify`` / ``set`` / ``disable`` 管理 ``login`` 库的 ``user_accounts``（全程不记录明文口令）；
3. ``lexicon``：``validate`` / ``check`` / ``stats`` 校验与测试 ``content_guard`` 词库文件；
4. ``merge-zone``：``plan`` / ``validate`` / ``apply`` 执行不停服合服映射校验与受控写入（支持 ``--dry-run``）；
5. ``data-consistency-check``：严格只读跨表一致性审计（校验 ``player_route`` / ``account_role`` / ``mail`` / ``guild`` 与帮会榜）；
6. ``kafka-dlq``：``inspect`` / ``replay-plan`` 校验 JSONL 死信信封并生成无静默丢弃的重放计划；
7. ``devpay-notify``：按 ``payment/channels.py`` 协议计算 HMAC-SHA256 签名并向 ``PaymentNotifyServer`` 投递开发联调回调；
8. ``battle-script-review``：``list`` / ``approve`` / ``reject`` 通过真实 gRPC 调用 ``match.MatchInternal`` 审核玩家战斗剧本。
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
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO
from urllib.parse import urlsplit

import grpc
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from google.protobuf import json_format

from xuanming.content_guard.engine import Engine, SceneClass, Verdict
from xuanming.content_guard.wordlist import (
    Category,
    LexiconFormatError,
    LexiconWordError,
    category_name,
    compile_matcher,
    parse_lexicon,
)
from xuanming.grpc_runtime import GrpcClient, sign_service_call
from xuanming.login.password_auth import (
    SqlPasswordAuthStore,
    hash_password,
    open_password_auth_pool,
    parse_mysql_dsn,
    validate_password_account,
    verify_password_hash,
)
from xuanming.match.battle_scripts import (
    MAX_PENDING_BATTLE_SCRIPTS_PAGE,
    MAX_REVIEW_REASON_BYTES,
    MAX_REVIEWER_BYTES,
    REVIEW_REJECT_DENIED,
    REVIEW_REJECT_INTERNAL,
    REVIEW_REJECT_INVALID,
    REVIEW_REJECT_NONE,
    REVIEW_REJECT_STALE,
)
from xuanming.match.config import MIN_HMAC_SECRET_BYTES, OPS_CALLER_SECRET_ENV
from xuanming.match.model import valid_operator_text
from xuanming.payment.channels import DEVPAY_SIGNATURE_HEADER, devpay_sign
from xuanming.payment.config import DEVPAY_SECRET_ENV, MIN_SECRET_BYTES
from xuanming.payment.errors import InvalidKeyError
from xuanming.payment.model import (
    order_id_from_out_trade_no,
    out_trade_no,
    valid_transaction_id,
)
from xuanming.protocol import Protocol

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
# 6. kafka-dlq（对应 Go go/shared/cmd/kafka_dlq，面向 JSONL 死信信封）
# ---------------------------------------------------------------------------

KAFKA_DLQ_REQUIRED_FIELDS: tuple[str, ...] = (
    "dlq_offset",
    "topic",
    "partition",
    "offset",
    "group",
    "cause",
    "reason",
    "dead_lettered_at",
)
KAFKA_DLQ_CAUSE_UNPROCESSABLE = "unprocessable"


@dataclass(frozen=True, slots=True)
class KafkaDlqRecord:
    """单条经信封校验的 Kafka 死信 JSONL 记录。"""

    line_no: int
    dlq_offset: int
    topic: str
    partition: int
    offset: int
    group: str
    cause: str
    reason: str
    dead_lettered_at: str
    attempts: int = 0
    key: str = ""
    value_bytes: int = 0
    headers: tuple[tuple[str, str], ...] = ()

    @property
    def position(self) -> str:
        return f"{self.topic}[{self.partition}]@{self.offset}"

    def to_dict(self, *, duplicate_of_dlq_offset: int | None = None) -> dict[str, object]:
        data: dict[str, object] = {
            "line_no": self.line_no,
            "dlq_offset": self.dlq_offset,
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "original": self.position,
            "group": self.group,
            "cause": self.cause,
            "reason": self.reason,
            "dead_lettered_at": self.dead_lettered_at,
            "attempts": self.attempts,
            "key": self.key,
            "value_bytes": self.value_bytes,
            "headers": dict(self.headers),
        }
        if duplicate_of_dlq_offset is not None:
            data["duplicate_of_dlq_offset"] = duplicate_of_dlq_offset
        return data


def _require_non_negative_int(raw: object, field_name: str, where: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise ValueError(f"{where}: field {field_name!r} must be a non-negative integer")
    return raw


def _require_non_empty_str(raw: object, field_name: str, where: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{where}: field {field_name!r} must be a non-empty string")
    return raw.strip()


def load_kafka_dlq_jsonl(path: Path | str) -> list[KafkaDlqRecord]:
    """逐行解析并强校验 JSONL 死信文件；任何畸形行或缺失必填信封字段均 fail-closed 报错，绝不静默丢弃。"""
    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8")
    records: list[KafkaDlqRecord] = []
    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        where = f"{file_path}:{line_no}"
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{where}: invalid JSON line: {exc.msg}") from exc
        if not isinstance(obj, dict):
            raise ValueError(f"{where}: DLQ entry must be a JSON object")

        missing = [field for field in KAFKA_DLQ_REQUIRED_FIELDS if field not in obj]
        if missing:
            raise ValueError(
                f"{where}: missing required DLQ envelope field(s): {', '.join(missing)}"
            )

        dlq_offset = _require_non_negative_int(obj["dlq_offset"], "dlq_offset", where)
        topic = _require_non_empty_str(obj["topic"], "topic", where)
        partition = _require_non_negative_int(obj["partition"], "partition", where)
        offset = _require_non_negative_int(obj["offset"], "offset", where)
        group = _require_non_empty_str(obj["group"], "group", where)
        cause = _require_non_empty_str(obj["cause"], "cause", where)
        if not isinstance(obj["reason"], str):
            raise ValueError(f"{where}: field 'reason' must be a string")
        reason = obj["reason"]
        dead_lettered_at = _require_non_empty_str(obj["dead_lettered_at"], "dead_lettered_at", where)

        attempts = _require_non_negative_int(obj.get("attempts", 0), "attempts", where)
        raw_key = obj.get("key", "")
        if not isinstance(raw_key, str):
            raise ValueError(f"{where}: field 'key' must be a string")

        if "value_bytes" in obj:
            value_bytes = _require_non_negative_int(obj["value_bytes"], "value_bytes", where)
        elif "value" in obj:
            if not isinstance(obj["value"], str):
                raise ValueError(f"{where}: field 'value' must be a string")
            value_bytes = len(obj["value"].encode("utf-8"))
        elif "value_b64" in obj:
            if not isinstance(obj["value_b64"], str):
                raise ValueError(f"{where}: field 'value_b64' must be a string")
            value_bytes = len(obj["value_b64"].encode("ascii"))
        else:
            value_bytes = 0

        raw_headers = obj.get("headers", {})
        if not isinstance(raw_headers, dict):
            raise ValueError(f"{where}: field 'headers' must be a JSON object")
        headers = tuple(sorted((str(k), str(v)) for k, v in raw_headers.items()))

        records.append(
            KafkaDlqRecord(
                line_no=line_no,
                dlq_offset=dlq_offset,
                topic=topic,
                partition=partition,
                offset=offset,
                group=group,
                cause=cause,
                reason=reason,
                dead_lettered_at=dead_lettered_at,
                attempts=attempts,
                key=raw_key,
                value_bytes=value_bytes,
                headers=headers,
            )
        )
    return records


def inspect_kafka_dlq(
    path: Path | str,
    *,
    topic: str = "",
    limit: int = 0,
) -> dict[str, object]:
    """巡检 JSONL 死信文件：先全量校验信封完整性，再按 topic 过滤、标记重复原位点并输出汇总。"""
    if limit < 0:
        raise ValueError("limit must be >= 0")
    records = load_kafka_dlq_jsonl(path)
    topic_filter = topic.strip()
    matched = [rec for rec in records if not topic_filter or rec.topic == topic_filter]

    seen_pos: dict[tuple[str, int, int], int] = {}
    by_topic: Counter[str] = Counter()
    by_group: Counter[str] = Counter()
    by_cause: Counter[str] = Counter()
    duplicate_count = 0
    rendered: list[dict[str, object]] = []

    for rec in matched:
        by_topic[rec.topic] += 1
        by_group[rec.group] += 1
        by_cause[rec.cause] += 1
        pos_key = (rec.topic, rec.partition, rec.offset)
        dup_of: int | None = None
        if pos_key in seen_pos:
            dup_of = seen_pos[pos_key]
            duplicate_count += 1
        else:
            seen_pos[pos_key] = rec.dlq_offset
        if limit == 0 or len(rendered) < limit:
            rendered.append(rec.to_dict(duplicate_of_dlq_offset=dup_of))

    truncated = max(0, len(matched) - len(rendered))
    logger.info(
        "kafka_dlq inspect finished: file=%s total=%d matched=%d returned=%d duplicates=%d",
        path,
        len(records),
        len(matched),
        len(rendered),
        duplicate_count,
    )
    return {
        "file": str(path),
        "topic_filter": topic_filter,
        "total_records": len(records),
        "matched_records": len(matched),
        "returned_records": len(rendered),
        "truncated": truncated,
        "duplicate_count": duplicate_count,
        "by_topic": dict(sorted(by_topic.items())),
        "by_group": dict(sorted(by_group.items())),
        "by_cause": dict(sorted(by_cause.items())),
        "records": rendered,
    }


def plan_kafka_dlq_replay(
    path: Path | str,
    *,
    topic: str = "",
    limit: int = 0,
    allow_unprocessable: bool = False,
) -> dict[str, object]:
    """生成死信重放计划：拒绝未放行的 ``unprocessable``，按原位点去重并显式列出每条跳过记录与重放命令。"""
    if limit < 0:
        raise ValueError("limit must be >= 0")
    records = load_kafka_dlq_jsonl(path)
    topic_filter = topic.strip()
    matched = [rec for rec in records if not topic_filter or rec.topic == topic_filter]

    seen_pos: dict[tuple[str, int, int], int] = {}
    unique_selected: list[KafkaDlqRecord] = []
    skipped_duplicates: list[dict[str, object]] = []

    for rec in matched:
        if rec.cause == KAFKA_DLQ_CAUSE_UNPROCESSABLE and not allow_unprocessable:
            raise ValueError(
                f"dlq_offset={rec.dlq_offset} ({rec.position}) was dead-lettered as "
                f"{KAFKA_DLQ_CAUSE_UNPROCESSABLE}; fix the decoder or producer first, "
                f"then rerun with --allow-unprocessable"
            )
        pos_key = (rec.topic, rec.partition, rec.offset)
        if pos_key in seen_pos:
            skipped_duplicates.append(
                {
                    "dlq_offset": rec.dlq_offset,
                    "original": rec.position,
                    "duplicate_of_dlq_offset": seen_pos[pos_key],
                    "reason": "same original (topic, partition, offset) already selected",
                }
            )
            continue
        seen_pos[pos_key] = rec.dlq_offset
        unique_selected.append(rec)

    selected = unique_selected[:limit] if limit > 0 else unique_selected
    deferred_count = max(0, len(unique_selected) - len(selected))

    commands: list[str] = []
    selected_dicts: list[dict[str, object]] = []
    for rec in selected:
        allow_flag = " --allow-unprocessable" if rec.cause == KAFKA_DLQ_CAUSE_UNPROCESSABLE else ""
        cmd = (
            f"kafka-dlq replay --group {rec.group} --topic {rec.topic} "
            f"--partition {rec.partition} --dlq-offset {rec.dlq_offset} "
            f"--original-offset {rec.offset}{allow_flag}"
        )
        commands.append(cmd)
        item = rec.to_dict()
        item["replay_command"] = cmd
        selected_dicts.append(item)

    logger.info(
        "kafka_dlq replay_plan finished: file=%s matched=%d selected=%d skipped_duplicates=%d deferred=%d",
        path,
        len(matched),
        len(selected_dicts),
        len(skipped_duplicates),
        deferred_count,
    )
    return {
        "file": str(path),
        "topic_filter": topic_filter,
        "allow_unprocessable": allow_unprocessable,
        "total_records": len(records),
        "matched_records": len(matched),
        "selected_count": len(selected_dicts),
        "skipped_duplicate_count": len(skipped_duplicates),
        "deferred_count": deferred_count,
        "selected": selected_dicts,
        "skipped_duplicates": skipped_duplicates,
        "commands": commands,
    }


# ---------------------------------------------------------------------------
# 7. devpay-notify（对应 Go go/payment/cmd/devpaynotify 与 payment/notify_http.py）
# ---------------------------------------------------------------------------

DEVPAY_DEFAULT_URL = "http://127.0.0.1:18130/notify/devpay"


@dataclass(frozen=True, slots=True)
class DevPayNotifyRequest:
    """已签名的 DevPay 回调 HTTP 请求（与 ``DevPayChannel.parse_notification`` 逐字节对齐）。"""

    url: str
    out_trade_no: str
    transaction_id: str
    requested_transaction_id: str
    amount_minor: int
    currency: str
    seq: int
    revoke: bool
    body: bytes
    signature_hex: str
    headers: dict[str, str]


def _normalize_devpay_trade_no(order_id: str) -> str:
    cleaned = order_id.strip().lower()
    try:
        return out_trade_no(cleaned)
    except InvalidKeyError:
        pass
    try:
        order_id_from_out_trade_no(cleaned)
        return cleaned
    except InvalidKeyError as exc:
        raise ValueError(
            f"order_id must be a lowercase UUID or 32-hex out_trade_no, got {order_id!r}"
        ) from exc


def _resolve_devpay_seq(
    trade_no: str,
    *,
    transaction_id: str = "",
    seq: int | None = None,
) -> tuple[int, str]:
    tx_clean = transaction_id.strip()
    inferred_seq: int | None = None
    if tx_clean:
        if not valid_transaction_id(tx_clean):
            raise ValueError(f"invalid transaction_id format: {tx_clean!r}")
        prefix = f"devpay-{trade_no}"
        if tx_clean == prefix:
            inferred_seq = 0
        elif tx_clean.startswith(prefix + "-"):
            suffix = tx_clean[len(prefix) + 1 :]
            if suffix.isdigit():
                inferred_seq = int(suffix)

    if seq is not None and inferred_seq is not None and seq != inferred_seq:
        raise ValueError(
            f"conflicting --seq ({seq}) and --transaction-id ({tx_clean}, inferred seq={inferred_seq})"
        )
    effective_seq = seq if seq is not None else (inferred_seq if inferred_seq is not None else 0)
    if isinstance(effective_seq, bool) or not isinstance(effective_seq, int) or not (0 <= effective_seq <= 0xFFFFFFFF):
        raise ValueError(f"seq must be a uint32 (0..{0xFFFFFFFF}), got {effective_seq!r}")

    derived_tx_id = f"devpay-{trade_no}" if effective_seq == 0 else f"devpay-{trade_no}-{effective_seq}"
    return effective_seq, derived_tx_id


def _load_secret_from_env(env_name: str, *, min_bytes: int, environ: Mapping[str, str] | None = None) -> bytes:
    cleaned_name = env_name.strip()
    if not cleaned_name:
        raise ValueError("secret environment variable name must not be empty")
    env = os.environ if environ is None else environ
    raw = env.get(cleaned_name, "")
    if not raw:
        raise ValueError(f"environment variable {cleaned_name} is unset or empty")
    if raw != raw.strip():
        raise ValueError(f"environment variable {cleaned_name} has leading or trailing whitespace")
    encoded = raw.encode("utf-8")
    if len(encoded) < min_bytes:
        raise ValueError(
            f"environment variable {cleaned_name} must be at least {min_bytes} bytes"
        )
    return encoded


def build_devpay_notify_request(
    *,
    url: str,
    order_id: str,
    amount_minor: int,
    currency: str = "CNY",
    transaction_id: str = "",
    seq: int | None = None,
    revoke: bool = False,
    secret: bytes | str,
) -> DevPayNotifyRequest:
    """构造并签名 DevPay 回调请求；JSON 字段严格限定为 ``DevPayChannel`` 白名单 5 项。"""
    parsed = urlsplit(url.strip())
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError(f"devpay notify URL must be http(s) with a hostname, got {url!r}")

    secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
    if len(secret_bytes) < MIN_SECRET_BYTES:
        raise ValueError(f"devpay secret must be at least {MIN_SECRET_BYTES} bytes")

    trade_no = _normalize_devpay_trade_no(order_id)
    effective_seq, derived_tx_id = _resolve_devpay_seq(
        trade_no, transaction_id=transaction_id, seq=seq
    )

    if isinstance(amount_minor, bool) or not isinstance(amount_minor, int) or amount_minor < 0:
        raise ValueError("amount_minor must be a non-negative integer")
    currency_clean = currency.strip()
    if not revoke and (amount_minor <= 0 or not currency_clean):
        raise ValueError("amount_minor > 0 and non-empty currency are required unless --revoke is set")

    # 字段集必须与 payment/channels.py 的 _DEVPAY_ALLOWED_FIELDS 完全一致，禁止塞入未知键。
    payload = {
        "out_trade_no": trade_no,
        "amount_minor": amount_minor,
        "currency": currency_clean,
        "seq": effective_seq,
        "revoke": bool(revoke),
    }
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sig_hex = devpay_sign(secret_bytes, body).hex()
    headers = {
        "Content-Type": "application/json",
        DEVPAY_SIGNATURE_HEADER: sig_hex,
    }
    return DevPayNotifyRequest(
        url=url.strip(),
        out_trade_no=trade_no,
        transaction_id=derived_tx_id,
        requested_transaction_id=transaction_id.strip(),
        amount_minor=amount_minor,
        currency=currency_clean,
        seq=effective_seq,
        revoke=bool(revoke),
        body=body,
        signature_hex=sig_hex,
        headers=headers,
    )


def _post_devpay_sync(req: DevPayNotifyRequest, timeout: float) -> tuple[int, str]:
    http_req = urllib.request.Request(
        req.url,
        data=req.body,
        headers=req.headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(http_req, timeout=timeout) as resp:
            status = int(resp.status)
            raw = resp.read(4096)
            return status, raw.decode("utf-8", errors="replace").strip()
    except urllib.error.HTTPError as exc:
        raw = exc.read(4096) if exc.fp is not None else b""
        return int(exc.code), raw.decode("utf-8", errors="replace").strip()


async def send_devpay_notify(
    *,
    url: str,
    order_id: str,
    amount_minor: int,
    currency: str = "CNY",
    transaction_id: str = "",
    seq: int | None = None,
    revoke: bool = False,
    secret: bytes | str,
    dry_run: bool = False,
    timeout: float = 10.0,
) -> dict[str, object]:
    """构造签名请求并（非 dry-run 时）向 ``PaymentNotifyServer`` 发送 POST 回调。"""
    if timeout <= 0:
        raise ValueError("timeout must be > 0")
    req = build_devpay_notify_request(
        url=url,
        order_id=order_id,
        amount_minor=amount_minor,
        currency=currency,
        transaction_id=transaction_id,
        seq=seq,
        revoke=revoke,
        secret=secret,
    )
    base_result: dict[str, object] = {
        "url": req.url,
        "out_trade_no": req.out_trade_no,
        "transaction_id": req.transaction_id,
        "requested_transaction_id": req.requested_transaction_id,
        "amount_minor": req.amount_minor,
        "currency": req.currency,
        "seq": req.seq,
        "revoke": req.revoke,
        "signature_header": DEVPAY_SIGNATURE_HEADER,
        "signature_hex": req.signature_hex,
        "dry_run": dry_run,
    }
    if dry_run:
        logger.info(
            "devpay_notify dry-run prepared: url=%s out_trade_no=%s seq=%d revoke=%s",
            req.url,
            req.out_trade_no,
            req.seq,
            req.revoke,
        )
        base_result["delivered"] = False
        base_result["status_code"] = 0
        base_result["response_body"] = ""
        return base_result

    status_code, response_body = await asyncio.to_thread(_post_devpay_sync, req, timeout)
    delivered = status_code == 200
    logger.info(
        "devpay_notify finished: url=%s out_trade_no=%s status=%d delivered=%s",
        req.url,
        req.out_trade_no,
        status_code,
        delivered,
    )
    base_result["delivered"] = delivered
    base_result["status_code"] = status_code
    base_result["response_body"] = response_body
    return base_result


# ---------------------------------------------------------------------------
# 8. battle-script-review（对应 Go go/match/cmd/battlescriptreview）
# ---------------------------------------------------------------------------

MATCH_OPS_CALLER = "ops"
LIST_PENDING_BATTLE_SCRIPTS_PATH = "/match.MatchInternal/ListPendingBattleScripts"
REVIEW_BATTLE_SCRIPT_PATH = "/match.MatchInternal/ReviewBattleScript"

_REVIEW_REJECT_NAMES: dict[int, str] = {
    REVIEW_REJECT_NONE: "NONE",
    REVIEW_REJECT_DENIED: "DENIED",
    REVIEW_REJECT_INVALID: "INVALID",
    REVIEW_REJECT_STALE: "STALE",
    REVIEW_REJECT_INTERNAL: "INTERNAL",
}


def _reject_name(code: int) -> str:
    return _REVIEW_REJECT_NAMES.get(int(code), f"UNKNOWN({code})")


async def list_pending_battle_scripts_rpc(
    endpoint: str,
    *,
    limit: int = 20,
    secret: bytes | str,
    timeout: float = 5.0,
    clock_ms: Callable[[], int] | None = None,
) -> dict[str, object]:
    """以 ``ops`` 内部 HMAC 签名调用 ``/match.MatchInternal/ListPendingBattleScripts``。"""
    target = endpoint.strip()
    if not target:
        raise ValueError("endpoint is required")
    if isinstance(limit, bool) or not isinstance(limit, int) or not (1 <= limit <= MAX_PENDING_BATTLE_SCRIPTS_PAGE):
        raise ValueError(f"limit must be in 1..{MAX_PENDING_BATTLE_SCRIPTS_PAGE}")
    if timeout <= 0:
        raise ValueError("timeout must be > 0")
    secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
    if len(secret_bytes) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"admin token must be at least {MIN_HMAC_SECRET_BYTES} bytes")

    protocol = Protocol()
    req = protocol.message("match.ListPendingBattleScriptsRequest", limit=limit)
    now_ms = (time.time_ns() // 1_000_000) if clock_ms is None else int(clock_ms())
    sign_service_call(LIST_PENDING_BATTLE_SCRIPTS_PATH, req, MATCH_OPS_CALLER, now_ms, secret_bytes)

    client = GrpcClient(target, timeout=timeout, enable_retries=False)
    try:
        raw_reply = await client.call(
            LIST_PENDING_BATTLE_SCRIPTS_PATH,
            req.SerializeToString(),
            timeout=timeout,
        )
    finally:
        await client.close()

    reply = protocol.parse("match.ListPendingBattleScriptsResponse", raw_reply)
    reject = int(reply.reject)
    scripts: list[dict[str, object]] = []
    for pending in reply.scripts:
        scripts.append(
            {
                "script_id": int(pending.script_id),
                "owner_player_id": int(pending.owner_player_id),
                "revision": int(pending.revision),
                "submitted_ms": int(pending.submitted_ms),
                "approved_revision": int(pending.approved_revision),
                "content": json_format.MessageToDict(
                    pending.content, preserving_proto_field_name=True
                ),
            }
        )
    logger.info(
        "battle_script_review list finished: endpoint=%s reject=%s pending=%d",
        target,
        _reject_name(reject),
        len(scripts),
    )
    return {
        "ok": reject == REVIEW_REJECT_NONE,
        "endpoint": target,
        "reject": reject,
        "reject_name": _reject_name(reject),
        "pending_count": len(scripts),
        "scripts": scripts,
    }


async def review_battle_script_rpc(
    endpoint: str,
    *,
    script_id: int,
    revision: int = 1,
    approve: bool,
    reviewer: str,
    note: str = "",
    secret: bytes | str,
    timeout: float = 5.0,
    clock_ms: Callable[[], int] | None = None,
) -> dict[str, object]:
    """以 ``ops`` 内部 HMAC 签名调用 ``/match.MatchInternal/ReviewBattleScript``。"""
    target = endpoint.strip()
    if not target:
        raise ValueError("endpoint is required")
    if isinstance(script_id, bool) or not isinstance(script_id, int) or script_id <= 0:
        raise ValueError("script_id must be a positive integer")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision <= 0:
        raise ValueError("revision must be a positive integer")
    if not valid_operator_text(reviewer, MAX_REVIEWER_BYTES, required=True):
        raise ValueError(f"reviewer is required and must be <= {MAX_REVIEWER_BYTES} UTF-8 bytes without control chars")
    if not valid_operator_text(note, MAX_REVIEW_REASON_BYTES, required=not approve):
        if not approve and not note:
            raise ValueError("rejecting a battle script requires --note (shown to the author)")
        raise ValueError(f"note must be <= {MAX_REVIEW_REASON_BYTES} UTF-8 bytes without control chars")
    if timeout <= 0:
        raise ValueError("timeout must be > 0")
    secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
    if len(secret_bytes) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"admin token must be at least {MIN_HMAC_SECRET_BYTES} bytes")

    protocol = Protocol()
    req = protocol.message(
        "match.ReviewBattleScriptRequest",
        script_id=script_id,
        revision=revision,
        approve=bool(approve),
        reviewer=reviewer,
        reason=note,
    )
    now_ms = (time.time_ns() // 1_000_000) if clock_ms is None else int(clock_ms())
    sign_service_call(REVIEW_BATTLE_SCRIPT_PATH, req, MATCH_OPS_CALLER, now_ms, secret_bytes)

    client = GrpcClient(target, timeout=timeout, enable_retries=False)
    try:
        raw_reply = await client.call(
            REVIEW_BATTLE_SCRIPT_PATH,
            req.SerializeToString(),
            timeout=timeout,
        )
    finally:
        await client.close()

    reply = protocol.parse("match.ReviewBattleScriptResponse", raw_reply)
    reject = int(reply.reject)
    logger.info(
        "battle_script_review review finished: endpoint=%s script_id=%d revision=%d approve=%s reject=%s",
        target,
        script_id,
        revision,
        approve,
        _reject_name(reject),
    )
    return {
        "ok": reject == REVIEW_REJECT_NONE,
        "endpoint": target,
        "script_id": script_id,
        "revision": revision,
        "approve": bool(approve),
        "reviewer": reviewer,
        "note": note,
        "reject": reject,
        "reject_name": _reject_name(reject),
    }


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

    # 6. kafka-dlq
    p_dlq = sub.add_parser(
        "kafka-dlq",
        help="Inspect JSONL dead-letter files and generate deduplicated replay plans.",
    )
    dlq_sub = p_dlq.add_subparsers(dest="dlq_action", required=True)

    p_dlq_inspect = dlq_sub.add_parser("inspect", help="Validate and summarize a JSONL DLQ file.")
    p_dlq_inspect.add_argument("--file", type=Path, required=True, help="Path to JSONL DLQ file.")
    p_dlq_inspect.add_argument("--topic", default="", help="Filter records by original Kafka topic.")
    p_dlq_inspect.add_argument("--limit", type=int, default=0, help="Maximum records to display (0 = unlimited).")
    p_dlq_inspect.add_argument("--json", action="store_true", dest="as_json")

    p_dlq_replay = dlq_sub.add_parser(
        "replay-plan",
        help="Generate deduplicated replay commands for a JSONL DLQ file without writing to Kafka.",
    )
    p_dlq_replay.add_argument("--file", type=Path, required=True, help="Path to JSONL DLQ file.")
    p_dlq_replay.add_argument("--topic", default="", help="Filter records by original Kafka topic.")
    p_dlq_replay.add_argument("--limit", type=int, default=0, help="Maximum unique records to include (0 = unlimited).")
    p_dlq_replay.add_argument(
        "--allow-unprocessable",
        action="store_true",
        help="Include records dead-lettered with cause=unprocessable (only after fixing decoder/producer).",
    )
    p_dlq_replay.add_argument("--json", action="store_true", dest="as_json")

    # 7. devpay-notify
    p_devpay = sub.add_parser(
        "devpay-notify",
        help="Sign and POST a devpay callback to PaymentNotifyServer.",
    )
    p_devpay.add_argument("--url", default=DEVPAY_DEFAULT_URL, help="Payment notify endpoint URL.")
    p_devpay.add_argument("--order-id", required=True, help="Mall order UUID or 32-hex out_trade_no.")
    p_devpay.add_argument("--transaction-id", default="", help="Optional channel transaction ID (e.g. devpay-<trade>-1).")
    p_devpay.add_argument("--seq", type=int, default=None, help="Optional uint32 sequence number for duplicate payments.")
    p_devpay.add_argument("--amount-minor", type=int, default=0, help="Paid amount in minor units.")
    p_devpay.add_argument("--currency", default="CNY", help="Currency code (default: CNY).")
    p_devpay.add_argument("--revoke", action="store_true", help="Send a channel revocation notice instead of payment.")
    p_devpay.add_argument(
        "--secret-env",
        default=DEVPAY_SECRET_ENV,
        help=f"Environment variable holding the devpay HMAC secret (default: {DEVPAY_SECRET_ENV}).",
    )
    p_devpay.add_argument("--timeout", type=float, default=10.0, help="HTTP request timeout in seconds.")
    p_devpay.add_argument("--dry-run", action="store_true", help="Sign and print payload without sending HTTP POST.")
    p_devpay.add_argument("--json", action="store_true", dest="as_json")

    # 8. battle-script-review
    p_bsr = sub.add_parser(
        "battle-script-review",
        help="List, approve, or reject player battle scripts via match.MatchInternal gRPC.",
    )
    bsr_sub = p_bsr.add_subparsers(dest="bsr_action", required=True)

    p_bsr_list = bsr_sub.add_parser("list", help="List pending player battle scripts.")
    p_bsr_list.add_argument("--endpoint", required=True, help="Match gRPC endpoint (host:port).")
    p_bsr_list.add_argument("--limit", type=int, default=20, help="Max pending scripts to fetch (1..50).")
    p_bsr_list.add_argument(
        "--admin-token-env",
        default=OPS_CALLER_SECRET_ENV,
        help=f"Environment variable holding the match ops secret (default: {OPS_CALLER_SECRET_ENV}).",
    )
    p_bsr_list.add_argument("--timeout", type=float, default=5.0, help="gRPC call timeout in seconds.")
    p_bsr_list.add_argument("--json", action="store_true", dest="as_json")

    for action_name, help_text, note_required in (
        ("approve", "Approve a pending battle script revision.", False),
        ("reject", "Reject a pending battle script revision (requires --note).", True),
    ):
        p_act = bsr_sub.add_parser(action_name, help=help_text)
        p_act.add_argument("--endpoint", required=True, help="Match gRPC endpoint (host:port).")
        p_act.add_argument("--script-id", type=int, required=True, help="Target battle script ID.")
        p_act.add_argument("--revision", type=int, default=1, help="Script revision for CAS review (default: 1).")
        p_act.add_argument("--reviewer", required=True, help="Reviewer name recorded in audit log.")
        p_act.add_argument(
            "--note",
            "--reason",
            dest="note",
            required=note_required,
            default="",
            help="Review note or rejection reason shown to the author.",
        )
        p_act.add_argument(
            "--admin-token-env",
            default=OPS_CALLER_SECRET_ENV,
            help=f"Environment variable holding the match ops secret (default: {OPS_CALLER_SECRET_ENV}).",
        )
        p_act.add_argument("--timeout", type=float, default=5.0, help="gRPC call timeout in seconds.")
        p_act.add_argument("--json", action="store_true", dest="as_json")

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


async def _run_kafka_dlq(args: argparse.Namespace) -> int:
    if args.dlq_action == "inspect":
        report = inspect_kafka_dlq(args.file, topic=args.topic, limit=args.limit)
        lines: list[str] = []
        for rec in report["records"]:
            dup_suffix = (
                f" duplicate_of_dlq_offset={rec['duplicate_of_dlq_offset']}"
                if "duplicate_of_dlq_offset" in rec
                else ""
            )
            lines.append(
                f"dlq_offset={rec['dlq_offset']} original={rec['original']} group={rec['group']} "
                f"cause={rec['cause']} attempts={rec['attempts']} dead_lettered_at={rec['dead_lettered_at']} "
                f"value_bytes={rec['value_bytes']} reason={rec['reason']!r}{dup_suffix}"
            )
        lines.append(
            f"summary: total={report['total_records']} matched={report['matched_records']} "
            f"returned={report['returned_records']} truncated={report['truncated']} "
            f"duplicates={report['duplicate_count']}"
        )
        _emit_output(report, as_json=args.as_json, text="\n".join(lines))
        return 0

    if args.dlq_action == "replay-plan":
        plan = plan_kafka_dlq_replay(
            args.file,
            topic=args.topic,
            limit=args.limit,
            allow_unprocessable=args.allow_unprocessable,
        )
        lines = []
        for skip in plan["skipped_duplicates"]:
            lines.append(
                f"skipped dlq_offset={skip['dlq_offset']} original={skip['original']} "
                f"duplicate_of_dlq_offset={skip['duplicate_of_dlq_offset']}"
            )
        for cmd in plan["commands"]:
            lines.append(f"plan: {cmd}")
        lines.append(
            f"replay-plan: selected={plan['selected_count']} "
            f"skipped_duplicates={plan['skipped_duplicate_count']} "
            f"deferred={plan['deferred_count']} matched={plan['matched_records']}"
        )
        _emit_output(plan, as_json=args.as_json, text="\n".join(lines))
        return 0

    raise ValueError(f"unsupported kafka-dlq action: {args.dlq_action}")


async def _run_devpay_notify(args: argparse.Namespace) -> int:
    secret_bytes = _load_secret_from_env(args.secret_env, min_bytes=MIN_SECRET_BYTES)
    res = await send_devpay_notify(
        url=args.url,
        order_id=args.order_id,
        amount_minor=args.amount_minor,
        currency=args.currency,
        transaction_id=args.transaction_id,
        seq=args.seq,
        revoke=args.revoke,
        secret=secret_bytes,
        dry_run=args.dry_run,
        timeout=args.timeout,
    )
    if res["dry_run"]:
        text = (
            f"[DRY-RUN] POST {res['url']} out_trade_no={res['out_trade_no']} "
            f"tx={res['transaction_id']} seq={res['seq']} revoke={res['revoke']} "
            f"{res['signature_header']}={res['signature_hex']}"
        )
        _emit_output(res, as_json=args.as_json, text=text)
        return 0

    if res["delivered"]:
        text = (
            f"accepted: out_trade_no={res['out_trade_no']} tx={res['transaction_id']} "
            f"seq={res['seq']} revoke={res['revoke']} status={res['status_code']}"
        )
        _emit_output(res, as_json=args.as_json, text=text)
        return 0

    text = (
        f"rejected: out_trade_no={res['out_trade_no']} status={res['status_code']} "
        f"body={res['response_body']!r}"
    )
    _emit_output(res, as_json=args.as_json, text=text)
    print(f"ERROR: payment notify rejected with HTTP {res['status_code']}", file=sys.stderr)
    return 1


async def _run_battle_script_review(args: argparse.Namespace) -> int:
    secret_bytes = _load_secret_from_env(args.admin_token_env, min_bytes=MIN_HMAC_SECRET_BYTES)
    if args.bsr_action == "list":
        res = await list_pending_battle_scripts_rpc(
            args.endpoint,
            limit=args.limit,
            secret=secret_bytes,
            timeout=args.timeout,
        )
        lines: list[str] = []
        if not res["ok"]:
            lines.append(f"reject={res['reject_name']}")
            print(
                f"ERROR: ListPendingBattleScripts rejected ({res['reject_name']}); "
                f"check {args.admin_token_env} and match ops_secret",
                file=sys.stderr,
            )
        else:
            for item in res["scripts"]:
                lines.append(
                    f"script={item['script_id']} revision={item['revision']} "
                    f"owner={item['owner_player_id']} submitted_ms={item['submitted_ms']} "
                    f"approved_revision={item['approved_revision']}"
                )
            lines.append(f"pending={res['pending_count']}")
        _emit_output(res, as_json=args.as_json, text="\n".join(lines))
        return 0 if res["ok"] else 1

    if args.bsr_action in ("approve", "reject"):
        approve = args.bsr_action == "approve"
        res = await review_battle_script_rpc(
            args.endpoint,
            script_id=args.script_id,
            revision=args.revision,
            approve=approve,
            reviewer=args.reviewer,
            note=args.note,
            secret=secret_bytes,
            timeout=args.timeout,
        )
        text = (
            f"script={res['script_id']} revision={res['revision']} "
            f"approve={res['approve']} reject={res['reject_name']}"
        )
        _emit_output(res, as_json=args.as_json, text=text)
        if not res["ok"]:
            print(
                f"ERROR: ReviewBattleScript rejected ({res['reject_name']})",
                file=sys.stderr,
            )
            return 1
        return 0

    raise ValueError(f"unsupported battle-script-review action: {args.bsr_action}")


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
    if args.command == "kafka-dlq":
        return await _run_kafka_dlq(args)
    if args.command == "devpay-notify":
        return await _run_devpay_notify(args)
    if args.command == "battle-script-review":
        return await _run_battle_script_review(args)
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
        InvalidKeyError,
        ValueError,
    ) as exc:
        logger.error("admin_cli rejected command=%s: %s", args.command, exc)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except (OSError, RuntimeError, json.JSONDecodeError, urllib.error.URLError, grpc.aio.AioRpcError) as exc:
        logger.error("admin_cli fatal error command=%s: %s", args.command, exc)
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
