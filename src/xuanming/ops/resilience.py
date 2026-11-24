"""生产级运行时弹性原语（对应 Go 侧 ``clockcheck``、``dbguard``、``liveview`` 与 ``looplive``）。

职责划分：
1. ``ClockCheck``：启动期或周期性核对本机墙钟与参照时钟（或 HTTP ``Date`` 响应头）的偏移，
   支持 ``strict`` / ``warn`` / ``off`` 三档模式；
2. ``DbGuard``：写入侧大字段三档字节守卫（80% 告警、100% fail-closed 拒写）、保留期清理截止点
   防负值/下溢计算，以及基于 ``information_schema.TABLES`` 的只读表容量巡检；
3. ``LiveViewGate``：服务发现存活集理性闸门，空集或骤降（基线 >= 4 且新视图不足一半）时
   保持旧快照，防止 etcd 抖动/回档引发雪崩；
4. ``LoopLiveness``：周期后台循环活性登记表，记录各命名循环最近一次正常完结的 Unix 时间戳，
   用于发现卡死或持续异常退出的循环。
"""

from __future__ import annotations

import email.utils
import logging
import os
import threading
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TypeVar

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# ---------------------------------------------------------------------------
# ClockCheck（对应 Go shared/clockcheck）
# ---------------------------------------------------------------------------

DEFAULT_CLOCK_MAX_SKEW_SEC: float = 30.0
CLOCK_CHECK_MODE_ENV: str = "MMORPG_CLOCK_CHECK_MODE"
_VALID_CLOCK_MODES: frozenset[str] = frozenset({"strict", "warn", "off"})


class ClockSkewError(RuntimeError):
    """本机墙钟与参照时钟偏差超出允许阈值（strict 模式下阻断启动）。"""

    def __init__(self, message: str, *, skew_sec: float, max_skew_sec: float) -> None:
        super().__init__(message)
        self.skew_sec = skew_sec
        self.max_skew_sec = max_skew_sec


@dataclass(frozen=True, slots=True)
class ClockCheckResult:
    """单次时钟偏移校验结论。"""

    ok: bool
    skew_sec: float
    mode: str
    warn_only: bool = False
    message: str = ""


class ClockCheck:
    """墙钟偏差校验器（支持 ``strict`` / ``warn`` / ``off`` 模式，默认上限 30 秒）。"""

    def __init__(
        self,
        *,
        max_skew_sec: float = DEFAULT_CLOCK_MAX_SKEW_SEC,
        mode: str | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        if max_skew_sec <= 0:
            raise ValueError(f"max_skew_sec must be positive, got {max_skew_sec}")
        env = os.environ if environ is None else environ
        raw_mode = env.get(CLOCK_CHECK_MODE_ENV, "") if mode is None else mode
        normalized = raw_mode.strip().lower() or "strict"
        if normalized not in _VALID_CLOCK_MODES:
            raise ValueError(
                f"invalid clock check mode {raw_mode!r}; expected one of strict, warn, off"
            )
        self.max_skew_sec = float(max_skew_sec)
        self.mode = normalized

    def check_clock_skew(self, local_ts: float, reference_ts: float) -> ClockCheckResult:
        """核对本机时间戳与参照时间戳（Unix 秒）的偏移量。"""
        if self.mode == "off":
            return ClockCheckResult(
                ok=True,
                skew_sec=0.0,
                mode="off",
                warn_only=False,
                message="clock check disabled (mode=off)",
            )
        skew = float(local_ts) - float(reference_ts)
        if abs(skew) <= self.max_skew_sec:
            msg = (
                f"clock check passed: local clock skew is {skew:+.3f}s "
                f"(limit {self.max_skew_sec:.1f}s)"
            )
            logger.info(msg)
            return ClockCheckResult(
                ok=True,
                skew_sec=skew,
                mode=self.mode,
                warn_only=False,
                message=msg,
            )

        msg = (
            f"local clock skew is {skew:+.3f}s, exceeding allowed {self.max_skew_sec:.1f}s; "
            f"fix node NTP sync or set {CLOCK_CHECK_MODE_ENV}=warn to bypass"
        )
        if self.mode == "warn":
            logger.warning("clock check exceeded limit in warn mode: %s", msg)
            return ClockCheckResult(
                ok=True,
                skew_sec=skew,
                mode="warn",
                warn_only=True,
                message=msg,
            )
        logger.error("clock check failed in strict mode: %s", msg)
        raise ClockSkewError(msg, skew_sec=skew, max_skew_sec=self.max_skew_sec)

    def check_http_date_skew(
        self,
        date_header: str,
        *,
        sent_ts: float | None = None,
        received_ts: float | None = None,
    ) -> ClockCheckResult:
        """按 HTTP 响应头 ``Date``（RFC 7231，精确到秒）核对本机时钟。

        与 Go ``clockcheck.measure`` 一致：本机取请求收发中点，参照取 ``Date`` 所在整秒中点（+0.5s）。
        """
        if self.mode == "off":
            return ClockCheckResult(
                ok=True,
                skew_sec=0.0,
                mode="off",
                warn_only=False,
                message="clock check disabled (mode=off)",
            )
        if not date_header or not date_header.strip():
            raise ValueError("HTTP Date header must not be empty")
        try:
            ref_dt = email.utils.parsedate_to_datetime(date_header.strip())
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unusable HTTP Date header: {date_header!r}") from exc
        if ref_dt is None or ref_dt.tzinfo is None:
            raise ValueError(f"HTTP Date header missing timezone: {date_header!r}")

        now_fallback = time.time()
        sent = now_fallback if sent_ts is None else float(sent_ts)
        received = sent if received_ts is None else float(received_ts)
        local_mid = sent + (received - sent) / 2.0
        ref_mid = ref_dt.timestamp() + 0.5
        return self.check_clock_skew(local_mid, ref_mid)

    async def check_http_endpoint(
        self,
        url: str,
        *,
        timeout_sec: float = 3.0,
        ssl_context=None,
    ) -> ClockCheckResult:
        """向指定 HTTP(S) 端点发 GET 请求并按其 ``Date`` 头校验时钟（使用 ``aiohttp``）。"""
        if self.mode == "off":
            return ClockCheckResult(
                ok=True,
                skew_sec=0.0,
                mode="off",
                warn_only=False,
                message="clock check disabled (mode=off)",
            )
        import aiohttp

        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            sent = time.time()
            async with session.get(url, ssl=ssl_context) as resp:
                received = time.time()
                date_header = resp.headers.get("Date", "")
        return self.check_http_date_skew(date_header, sent_ts=sent, received_ts=received)


def check_clock_skew(
    local_ts: float,
    reference_ts: float,
    *,
    max_skew_sec: float = DEFAULT_CLOCK_MAX_SKEW_SEC,
    mode: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> ClockCheckResult:
    """便捷函数：按指定模式与阈值核对本机与参照时间戳偏差。"""
    return ClockCheck(max_skew_sec=max_skew_sec, mode=mode, environ=environ).check_clock_skew(
        local_ts, reference_ts
    )


def check_http_date_skew(
    date_header: str,
    *,
    sent_ts: float | None = None,
    received_ts: float | None = None,
    max_skew_sec: float = DEFAULT_CLOCK_MAX_SKEW_SEC,
    mode: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> ClockCheckResult:
    """便捷函数：按 HTTP ``Date`` 响应头核对本机墙钟偏差。"""
    return ClockCheck(max_skew_sec=max_skew_sec, mode=mode, environ=environ).check_http_date_skew(
        date_header, sent_ts=sent_ts, received_ts=received_ts
    )


# ---------------------------------------------------------------------------
# DbGuard（对应 Go shared/dbguard）
# ---------------------------------------------------------------------------

PAYLOAD_WARN_RATIO: float = 0.8
MAX_RETENTION_DAYS: int = 36_500
MILLIS_PER_DAY: int = 24 * 60 * 60 * 1000
MAX_RETENTION_MS: int = MAX_RETENTION_DAYS * MILLIS_PER_DAY


class PayloadLimitError(ValueError):
    """写入载荷字节数达到或超过上限（fail-closed 拒写）。"""

    def __init__(
        self,
        table: str,
        column: str,
        size_bytes: int,
        limit_bytes: int,
        *,
        hint: str = "",
    ) -> None:
        detail = f" ({hint})" if hint else ""
        super().__init__(
            f"dbguard: payload too large for {table}.{column} "
            f"({size_bytes} bytes, limit {limit_bytes}){detail}"
        )
        self.table = table
        self.column = column
        self.size_bytes = size_bytes
        self.limit_bytes = limit_bytes
        self.hint = hint


@dataclass(frozen=True, slots=True)
class PayloadCheckResult:
    """单次载荷字节守卫结果。"""

    ok: bool
    warned: bool
    size_bytes: int
    limit_bytes: int
    ratio: float


@dataclass(frozen=True, slots=True)
class TableBudget:
    """单张表的容量预算；字段为 0 表示跳过对应维度检查。"""

    table: str
    max_rows: int = 0
    max_bytes: int = 0
    max_avg_row_bytes: int = 0
    note: str = ""


@dataclass(frozen=True, slots=True)
class CapacityViolation:
    """单条表容量超预算记录。"""

    table: str
    kind: str  # "rows" | "bytes" | "avg_row_bytes"
    actual: int
    budget: int
    note: str = ""


@dataclass(frozen=True, slots=True)
class TableCapacityReport:
    """一轮 ``information_schema.TABLES`` 只读容量巡检汇总。"""

    db_name: str
    checked: int
    violations: tuple[CapacityViolation, ...]

    @property
    def ok(self) -> bool:
        return not self.violations


class DbGuard:
    """数据库写入载荷守卫、清理截止点防下溢与只读表容量巡检。"""

    WARN_RATIO: float = PAYLOAD_WARN_RATIO

    def __init__(self, *, warn_ratio: float = PAYLOAD_WARN_RATIO) -> None:
        if not 0.0 < warn_ratio <= 1.0:
            raise ValueError(f"warn_ratio must be in (0, 1], got {warn_ratio}")
        self.warn_ratio = float(warn_ratio)

    @staticmethod
    def check_payload(
        table: str,
        column: str,
        size_bytes: int,
        limit_bytes: int,
        *,
        allow_equal: bool = False,
        warn_ratio: float = PAYLOAD_WARN_RATIO,
        db_name: str = "",
        hint: str = "",
    ) -> PayloadCheckResult:
        """写入侧三档字节守卫：达 100% 上限 fail-closed 抛 ``PayloadLimitError``，达 80% 放行并打 WARN。"""
        if size_bytes < 0:
            raise ValueError(f"size_bytes must be non-negative, got {size_bytes}")
        if not 0.0 < warn_ratio <= 1.0:
            raise ValueError(f"warn_ratio must be in (0, 1], got {warn_ratio}")
        if limit_bytes <= 0:
            return PayloadCheckResult(
                ok=True,
                warned=False,
                size_bytes=size_bytes,
                limit_bytes=limit_bytes,
                ratio=0.0,
            )

        ratio = float(size_bytes) / float(limit_bytes)
        exceeded = (size_bytes > limit_bytes) if allow_equal else (size_bytes >= limit_bytes)
        if exceeded:
            logger.error(
                "db_payload_too_large_rejected db=%s table=%s column=%s size=%d limit=%d hint=%s",
                db_name or "-",
                table,
                column,
                size_bytes,
                limit_bytes,
                hint or "-",
            )
            raise PayloadLimitError(table, column, size_bytes, limit_bytes, hint=hint)

        if float(size_bytes) >= float(limit_bytes) * warn_ratio:
            logger.warning(
                "db_payload_approaching_limit db=%s table=%s column=%s size=%d limit=%d ratio=%.3f hint=%s",
                db_name or "-",
                table,
                column,
                size_bytes,
                limit_bytes,
                ratio,
                hint or "-",
            )
            return PayloadCheckResult(
                ok=True,
                warned=True,
                size_bytes=size_bytes,
                limit_bytes=limit_bytes,
                ratio=ratio,
            )
        return PayloadCheckResult(
            ok=True,
            warned=False,
            size_bytes=size_bytes,
            limit_bytes=limit_bytes,
            ratio=ratio,
        )

    @staticmethod
    def sweep_cutoff_ms(
        now_ms: int,
        retention_ms: int,
        *,
        max_retention_ms: int = MAX_RETENTION_MS,
    ) -> int | None:
        """计算毫秒级保留期清理截止点；防负值/零值下溢，时钟异常时返回 ``None`` 跳过本轮。"""
        if retention_ms <= 0 or retention_ms > max_retention_ms:
            raise ValueError(
                f"dbguard: retention_ms must be within [1, {max_retention_ms}], got {retention_ms}"
            )
        cutoff = int(now_ms) - int(retention_ms)
        if cutoff <= 0:
            logger.warning(
                "dbguard sweep skipped due to non-positive cutoff: now_ms=%d retention_ms=%d cutoff=%d",
                now_ms,
                retention_ms,
                cutoff,
            )
            return None
        return cutoff

    @staticmethod
    async def check_table_capacity(
        pool,
        budgets: Sequence[TableBudget],
        *,
        db_name: str = "",
    ) -> TableCapacityReport:
        """基于 ``information_schema.TABLES`` 执行毫秒级只读表容量巡检，超预算只记录不阻断业务。"""
        if not budgets:
            return TableCapacityReport(db_name=db_name, checked=0, violations=())
        if pool is None:
            raise ValueError("pool is required for check_table_capacity")

        resolved_db = db_name
        stats: dict[str, tuple[int, int, int]] = {}
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                if not resolved_db:
                    await cursor.execute("SELECT DATABASE()")
                    row = await cursor.fetchone()
                    if row and row[0]:
                        resolved_db = (
                            row[0].decode("utf-8")
                            if isinstance(row[0], (bytes, bytearray))
                            else str(row[0])
                        )
                    if not resolved_db:
                        logger.error("dbguard failed to resolve active database name")
                        return TableCapacityReport(db_name="", checked=0, violations=())

                await cursor.execute(
                    "SELECT TABLE_NAME, COALESCE(TABLE_ROWS, 0), COALESCE(DATA_LENGTH, 0), "
                    "COALESCE(AVG_ROW_LENGTH, 0) "
                    "FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'",
                    (resolved_db,),
                )
                rows = await cursor.fetchall()
                for row in rows:
                    raw_name = row[0]
                    name = (
                        raw_name.decode("utf-8")
                        if isinstance(raw_name, (bytes, bytearray))
                        else str(raw_name)
                    )
                    stats[name] = (int(row[1] or 0), int(row[2] or 0), int(row[3] or 0))

        checked = 0
        violations: list[CapacityViolation] = []
        for budget in budgets:
            entry = stats.get(budget.table)
            if entry is None:
                continue
            checked += 1
            table_rows, data_bytes, avg_row_bytes = entry
            for kind, actual, limit in (
                ("rows", table_rows, budget.max_rows),
                ("bytes", data_bytes, budget.max_bytes),
                ("avg_row_bytes", avg_row_bytes, budget.max_avg_row_bytes),
            ):
                if limit > 0 and actual > limit:
                    violations.append(
                        CapacityViolation(
                            table=budget.table,
                            kind=kind,
                            actual=actual,
                            budget=limit,
                            note=budget.note,
                        )
                    )
                    logger.error(
                        "db_capacity_budget_exceeded db=%s table=%s kind=%s actual=%d budget=%d note=%s",
                        resolved_db,
                        budget.table,
                        kind,
                        actual,
                        limit,
                        budget.note or "-",
                    )
        return TableCapacityReport(
            db_name=resolved_db,
            checked=checked,
            violations=tuple(violations),
        )


# ---------------------------------------------------------------------------
# LiveViewGate（对应 Go shared/liveview）
# ---------------------------------------------------------------------------

LIVE_VIEW_MIN_KNOWN: int = 4
_VALID_RECOVERIES: frozenset[str] = frozenset({"below_threshold", "when_clear"})


@dataclass(frozen=True, slots=True)
class LiveViewDecision:
    """单次存活集观察的闸门判定结果。"""

    verdict: str  # "accept" | "held_empty" | "held_drop" | "released"
    held: bool
    active: frozenset[str]
    previous_count: int
    current_count: int
    reason: str = ""


class LiveViewGate:
    """存活集合理性闸门状态机。

    规则（对齐 Go ``shared/liveview``）：
    - 新快照为空且旧快照非空：始终压住旧快照（``held_empty``），空集不自动超时放行，除非显式 ``override=True``；
    - 旧快照基线 ``>= min_known``（默认 4）且新快照不足一半（``len(curr) * 2 < len(prev)``）：
      进入压住（``held_drop``），维持旧快照；
    - 压住期间缺席数（或当前规模）一旦变动即重新计时；连续稳定满 ``hold_sec`` 后自动放行（``released``）并替换快照；
    - 显式传入 ``override=True`` 时强制接受新快照并复位状态机。
    """

    def __init__(
        self,
        *,
        hold_sec: float = 60.0,
        min_known: int = LIVE_VIEW_MIN_KNOWN,
        recovery: str = "below_threshold",
        initial: Iterable[str] | None = None,
    ) -> None:
        if hold_sec < 0:
            raise ValueError(f"hold_sec must be non-negative, got {hold_sec}")
        if min_known < 1:
            raise ValueError(f"min_known must be >= 1, got {min_known}")
        if recovery not in _VALID_RECOVERIES:
            raise ValueError(
                f"invalid recovery policy {recovery!r}; expected below_threshold or when_clear"
            )
        self.hold_sec = float(hold_sec)
        self.min_known = int(min_known)
        self.recovery = recovery
        self._previous: frozenset[str] = frozenset(initial) if initial is not None else frozenset()
        self._holding: bool = False
        self._last_signature: tuple[int, int] | None = None
        self._calm_since: float | None = None

    @property
    def snapshot(self) -> frozenset[str]:
        """当前生效的存活成员集合快照。"""
        return self._previous

    @property
    def is_holding(self) -> bool:
        """当前是否正处于压住旧快照状态。"""
        return self._holding

    def reset(self, snapshot: Iterable[str] | None = None) -> None:
        """复位闸门状态，可选替换为指定基线。"""
        self._previous = frozenset(snapshot) if snapshot is not None else frozenset()
        self._holding = False
        self._last_signature = None
        self._calm_since = None

    def observe(
        self,
        current: Iterable[str],
        *,
        now: float | None = None,
        override: bool = False,
    ) -> LiveViewDecision:
        """观察最新存活集并返回本次应采用的视图快照。"""
        curr = frozenset(current)
        prev = self._previous
        ts = time.monotonic() if now is None else float(now)

        if override:
            self._clear_hold()
            self._previous = curr
            logger.warning(
                "liveview gate overridden: previous=%d current=%d",
                len(prev),
                len(curr),
            )
            return LiveViewDecision(
                verdict="accept",
                held=False,
                active=curr,
                previous_count=len(prev),
                current_count=len(curr),
                reason="override",
            )

        if not prev:
            self._clear_hold()
            self._previous = curr
            return LiveViewDecision(
                verdict="accept",
                held=False,
                active=curr,
                previous_count=0,
                current_count=len(curr),
                reason="initial",
            )

        if not curr:
            # 空快照永不自动放行，每次观察刷新计时起点。
            self._holding = True
            self._last_signature = (len(prev), 0)
            self._calm_since = ts
            logger.warning(
                "liveview gate held empty snapshot: previous=%d current=0",
                len(prev),
            )
            return LiveViewDecision(
                verdict="held_empty",
                held=True,
                active=prev,
                previous_count=len(prev),
                current_count=0,
                reason="empty_snapshot",
            )

        missing = len(prev - curr)
        dropped = len(prev) >= self.min_known and (len(curr) * 2 < len(prev))

        if not self._holding:
            if not dropped:
                self._previous = curr
                return LiveViewDecision(
                    verdict="accept",
                    held=False,
                    active=curr,
                    previous_count=len(prev),
                    current_count=len(curr),
                    reason="ok",
                )
            self._holding = True
            self._last_signature = (missing, len(curr))
            self._calm_since = ts
            logger.warning(
                "liveview gate held sharp drop: previous=%d current=%d missing=%d",
                len(prev),
                len(curr),
                missing,
            )
            return LiveViewDecision(
                verdict="held_drop",
                held=True,
                active=prev,
                previous_count=len(prev),
                current_count=len(curr),
                reason="sharp_drop",
            )

        # 已处于压住状态：检查是否恢复。
        if missing == 0 or (not dropped and self.recovery == "below_threshold"):
            self._clear_hold()
            self._previous = curr
            logger.info(
                "liveview gate recovered: previous=%d current=%d missing=%d",
                len(prev),
                len(curr),
                missing,
            )
            return LiveViewDecision(
                verdict="accept",
                held=False,
                active=curr,
                previous_count=len(prev),
                current_count=len(curr),
                reason="recovered",
            )

        # 视图仍在变化时重新开始稳定计时。
        sig = (missing, len(curr))
        if sig != self._last_signature or self._calm_since is None:
            self._last_signature = sig
            self._calm_since = ts
            return LiveViewDecision(
                verdict="held_drop",
                held=True,
                active=prev,
                previous_count=len(prev),
                current_count=len(curr),
                reason="view_changing",
            )

        # 缺席形态连续稳定满 hold_sec 后放行。
        if (ts - self._calm_since) >= self.hold_sec:
            self._clear_hold()
            self._previous = curr
            logger.warning(
                "liveview gate released after hold expiry: previous=%d current=%d hold_sec=%.1f",
                len(prev),
                len(curr),
                self.hold_sec,
            )
            return LiveViewDecision(
                verdict="released",
                held=False,
                active=curr,
                previous_count=len(prev),
                current_count=len(curr),
                reason="hold_expired",
            )

        return LiveViewDecision(
            verdict="held_drop",
            held=True,
            active=prev,
            previous_count=len(prev),
            current_count=len(curr),
            reason="holding",
        )

    def _clear_hold(self) -> None:
        self._holding = False
        self._last_signature = None
        self._calm_since = None


# ---------------------------------------------------------------------------
# LoopLiveness（对应 Go shared/looplive）
# ---------------------------------------------------------------------------


class LoopLiveness:
    """周期后台循环活性注册表，按循环名记录 ``last_round_unix``（线程安全）。"""

    def __init__(self, *, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._loops: dict[str, float] = {}

    def register(self, name: str, *, now: float | None = None) -> float:
        """登记后台循环并写入启动时刻（避免首轮随机退避期间误报停摆）。"""
        cleaned = self._validate_name(name)
        ts = float(self._clock() if now is None else now)
        with self._lock:
            self._loops[cleaned] = ts
        return ts

    def mark_ok(self, name: str, *, now: float | None = None) -> float:
        """记录命名循环完成一轮的 Unix 秒时间戳。"""
        cleaned = self._validate_name(name)
        ts = float(self._clock() if now is None else now)
        with self._lock:
            self._loops[cleaned] = ts
        return ts

    def unregister(self, name: str) -> bool:
        """循环有意停机时移除活性记录；意外退出/异常不移除以便告警发现停摆。"""
        cleaned = self._validate_name(name)
        with self._lock:
            return self._loops.pop(cleaned, None) is not None

    def stale_loops(self, now: float | None = None, threshold_sec: float = 60.0) -> list[str]:
        """返回超过 ``threshold_sec`` 秒未推进 ``last_round_unix`` 的循环名称列表（升序）。"""
        if threshold_sec < 0:
            raise ValueError(f"threshold_sec must be non-negative, got {threshold_sec}")
        current = float(self._clock() if now is None else now)
        with self._lock:
            return sorted(
                name for name, last_ts in self._loops.items() if (current - last_ts) > threshold_sec
            )

    def snapshot(self) -> dict[str, float]:
        """返回全部已登记循环的 ``{loop_name: last_round_unix}`` 只读副本。"""
        with self._lock:
            return {name: self._loops[name] for name in sorted(self._loops)}

    def run_round(self, name: str, fn: Callable[[], _T]) -> tuple[bool, _T | None]:
        """执行一轮同步循环：正常返回推进活性时刻；抛出未捕获异常时记录日志且不推进时刻。"""
        cleaned = self._validate_name(name)
        try:
            result = fn()
        except Exception:
            logger.exception("looplive round failed with unhandled exception: loop=%s", cleaned)
            return False, None
        self.mark_ok(cleaned)
        return True, result

    async def run_round_async(
        self,
        name: str,
        fn: Callable[[], Awaitable[_T]],
    ) -> tuple[bool, _T | None]:
        """执行一轮异步循环：正常返回推进活性时刻；抛出未捕获异常时记录日志且不推进时刻。"""
        cleaned = self._validate_name(name)
        try:
            result = await fn()
        except Exception:
            logger.exception("looplive async round failed with unhandled exception: loop=%s", cleaned)
            return False, None
        self.mark_ok(cleaned)
        return True, result

    @staticmethod
    def _validate_name(name: str) -> str:
        cleaned = name.strip()
        if not cleaned:
            raise ValueError("loop name must not be empty")
        return cleaned
