"""确定性 4096 槽位 Region->Cell 单元路由（对照 Go ``go/shared/cellroute``）。

提供路由表校验、JSON 编解码、无锁原子热切换、etcd 监听重同步，以及登录侧
AssignGate 亲和筛选与 EnterGame 归属守卫。
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol as TypingProtocol, TypeVar

import grpc

from ..grpc_runtime import GrpcClient

logger = logging.getLogger(__name__)

# 逻辑路由槽位总数（固定 4096）与区域协调分片上限（固定 64）
LOGICAL_CELL_COUNT = 4096
LOGICAL_REGION_COUNT = 64

DEFAULT_RESYNC_INTERVAL_S = 60.0
WATCH_RECONNECT_BACKOFF_S = 2.0

_T = TypeVar("_T")


class CellRouteError(ValueError):
    """CellRoute 领域基础异常。"""


class InvalidPlayerIdError(CellRouteError):
    """player_id 为 0 或非法时抛出的路由异常。"""


class InvalidConfigError(CellRouteError):
    """CellRoute 配置校验失败异常。"""


@dataclass(frozen=True, slots=True)
class Entry:
    """单个逻辑槽位映射到的物理 (region_id, cell_id)。"""

    region_id: int
    cell_id: int


@dataclass(frozen=True, slots=True)
class Location:
    """玩家确定性路由结果。"""

    region_id: int
    cell_id: int
    logical_cell: int


@dataclass(frozen=True, slots=True)
class CellSpec:
    """声明归属于指定 region 的物理 cell。"""

    region_id: int
    cell_id: int


@dataclass(frozen=True, slots=True)
class CellRouteConfig:
    """无状态入口服务（如 login）的可选单元路由配置。

    - ``mode == ""``：停用路由（零开销，完全向后兼容）。
    - ``mode == "static"``：按 ``cells`` 均衡构造 4096 槽位静态表。
    - ``mode == "etcd"``：从 ``etcd_key`` 加载并监听热更新，可用 ``cells`` 作为冷启动种子。
    """

    mode: str = ""
    cells: tuple[CellSpec, ...] = ()
    etcd_key: str = ""
    resync_interval_s: float = 0.0
    self_region: int = 0
    self_cell: int = 0
    enforce_ownership: bool = False

    def validate(self) -> None:
        """启动期快速失败校验配置不变量。"""
        if self.mode == "":
            return
        if self.mode == "static":
            if not self.cells:
                raise InvalidConfigError("cellroute: invalid configuration: static mode requires non-empty cells")
            build_balanced_entries(self.cells)
            return
        if self.mode == "etcd":
            if not self.etcd_key:
                raise InvalidConfigError("cellroute: invalid configuration: etcd mode requires non-empty etcd_key")
            if self.cells:
                build_balanced_entries(self.cells)
            return
        raise InvalidConfigError(
            f"cellroute: invalid configuration: unsupported mode {self.mode!r} (want empty, static, or etcd)"
        )


def logical_cell_of(player_id: int) -> int:
    """计算 player_id 对应的确定性逻辑槽位号（0..4095）。"""
    return int(player_id) % LOGICAL_CELL_COUNT


def build_balanced_entries(cells: Sequence[CellSpec]) -> tuple[list[Entry], dict[int, int]]:
    """将 4096 个连续逻辑槽位均匀分配到给定物理 cell 列表。"""
    if not cells:
        raise CellRouteError("cellroute: cells must not be empty")
    region_of_cell: dict[int, int] = {}
    for spec in cells:
        prev = region_of_cell.get(spec.cell_id)
        if prev is not None and prev != spec.region_id:
            raise CellRouteError(
                f"cellroute: cell {spec.cell_id} declared in multiple regions ({prev} and {spec.region_id})"
            )
        region_of_cell[spec.cell_id] = spec.region_id

    entries: list[Entry] = []
    n = len(cells)
    base, rem = divmod(LOGICAL_CELL_COUNT, n)
    for idx, spec in enumerate(cells):
        width = base + (1 if idx < rem else 0)
        entry = Entry(region_id=spec.region_id, cell_id=spec.cell_id)
        entries.extend([entry] * width)
    return entries, region_of_cell


class Table(TypingProtocol):
    """只读 4096 槽位路由表协议。"""

    def lookup(self, logical_cell: int) -> Entry | None: ...
    def __len__(self) -> int: ...


class StaticTable:
    """不可变 4096 槽位路由表，构造时强制校验长度与 Region-Cell 拓扑一致性。"""

    __slots__ = ("_entries",)

    def __init__(self, entries: Sequence[Entry], region_of_cell: Mapping[int, int]) -> None:
        if len(entries) != LOGICAL_CELL_COUNT:
            raise CellRouteError(f"cellroute: entries len {len(entries)} != LogicalCellCount {LOGICAL_CELL_COUNT}")
        validated: list[Entry] = []
        for lc, entry in enumerate(entries):
            reg = region_of_cell.get(entry.cell_id)
            if reg is None:
                raise CellRouteError(
                    f"cellroute: logical_cell {lc} -> cell {entry.cell_id} not declared in region_of_cell"
                )
            if reg != entry.region_id:
                raise CellRouteError(
                    f"cellroute: logical_cell {lc} -> cell {entry.cell_id} region mismatch: "
                    f"entry={entry.region_id} topology={reg}"
                )
            validated.append(entry)
        self._entries: tuple[Entry, ...] = tuple(validated)

    def lookup(self, logical_cell: int) -> Entry | None:
        if logical_cell < 0 or logical_cell >= len(self._entries):
            return None
        return self._entries[logical_cell]

    def __len__(self) -> int:
        return len(self._entries)

    def entries_copy(self) -> list[Entry]:
        """返回 4096 条目防御性副本。"""
        return list(self._entries)


class AtomicTable:
    """支持热切换的不可变表指针封装，校验通过后以单次引用替换生效。"""

    __slots__ = ("_table",)

    def __init__(self, initial: StaticTable) -> None:
        if not isinstance(initial, StaticTable) or len(initial) != LOGICAL_CELL_COUNT:
            raise CellRouteError("cellroute: initial StaticTable is nil or wrong length")
        self._table: StaticTable = initial

    def lookup(self, logical_cell: int) -> Entry | None:
        table = self._table
        return table.lookup(logical_cell)

    def __len__(self) -> int:
        return len(self._table)

    def swap(self, next_table: StaticTable) -> None:
        """原子替换已校验的 StaticTable。"""
        if not isinstance(next_table, StaticTable) or len(next_table) != LOGICAL_CELL_COUNT:
            raise CellRouteError("cellroute: cannot swap nil or invalid StaticTable")
        self._table = next_table


class Router:
    """纯内存确定性路由器：无网络 I/O 将 player_id 映射到物理 (region_id, cell_id)。"""

    __slots__ = ("_table",)

    def __init__(self, table: Table) -> None:
        if table is None:
            raise CellRouteError("cellroute: nil table")
        if len(table) != LOGICAL_CELL_COUNT:
            raise CellRouteError(f"cellroute: table len {len(table)} != LogicalCellCount {LOGICAL_CELL_COUNT}")
        self._table = table

    def route(self, player_id: int) -> Location:
        """解析 player_id 对应的物理位置（拒绝 player_id <= 0）。"""
        if player_id <= 0:
            raise InvalidPlayerIdError("cellroute: player_id must be non-zero")
        lc = logical_cell_of(player_id)
        entry = self._table.lookup(lc)
        if entry is None:
            raise CellRouteError(f"cellroute: logical_cell {lc} not mapped (player_id={player_id})")
        return Location(region_id=entry.region_id, cell_id=entry.cell_id, logical_cell=lc)


# ── JSON 编解码（与 Go TablePayload wire schema 逐字段对齐） ──────────────────

def _parse_uint(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CellRouteError(f"cellroute: invalid unsigned integer for {field_name}: {value!r}")
    return value


def marshal_table_json(version: int, table: StaticTable, region_of_cell: Mapping[int, int]) -> bytes:
    """将已校验 StaticTable 与 cell->region 映射序列化为与 Go 兼容的 JSON 字节。"""
    if table is None:
        raise CellRouteError("cellroute: cannot marshal nil table")
    payload = {
        "version": int(version),
        "region_of_cell": {str(int(cell_id)): int(region_id) for cell_id, region_id in region_of_cell.items()},
        "entries": [{"region_id": e.region_id, "cell_id": e.cell_id} for e in table.entries_copy()],
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def parse_table_json(raw: bytes | str) -> tuple[StaticTable, int]:
    """解析并严格校验 etcd 中的 TablePayload JSON，返回 (StaticTable, version)。"""
    try:
        data = json.loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw)
    except Exception as exc:
        raise CellRouteError(f"cellroute: invalid table json: {exc}") from exc
    if not isinstance(data, dict):
        raise CellRouteError("cellroute: invalid table json: root must be object")

    version = _parse_uint(data.get("version", 0), "version")
    raw_regions = data.get("region_of_cell")
    if not isinstance(raw_regions, dict):
        raise CellRouteError("cellroute: invalid table json: region_of_cell must be object")
    region_of_cell: dict[int, int] = {}
    for k, v in raw_regions.items():
        try:
            cell_id = int(k)
        except (TypeError, ValueError) as exc:
            raise CellRouteError(f"cellroute: invalid cell_id key {k!r}") from exc
        if cell_id < 0:
            raise CellRouteError(f"cellroute: invalid negative cell_id {cell_id}")
        region_of_cell[cell_id] = _parse_uint(v, f"region_of_cell[{k}]")

    raw_entries = data.get("entries")
    if not isinstance(raw_entries, list):
        raise CellRouteError("cellroute: invalid table json: entries must be list")
    entries: list[Entry] = []
    for idx, item in enumerate(raw_entries):
        if not isinstance(item, dict):
            raise CellRouteError(f"cellroute: invalid entry at index {idx}")
        entries.append(
            Entry(
                region_id=_parse_uint(item.get("region_id", 0), f"entries[{idx}].region_id"),
                cell_id=_parse_uint(item.get("cell_id", 0), f"entries[{idx}].cell_id"),
            )
        )

    table = StaticTable(entries, region_of_cell)
    return table, version


# ── etcd 热监听与路由器装配 ────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class EtcdWatchEvent:
    """单条 etcd watch 变更事件。"""

    revision: int
    event_type: str  # "PUT" | "DELETE"
    value: bytes = b""


class EtcdKVWatcher(TypingProtocol):
    """CellRoute 使用的最小 etcd KV + Watch 抽象，便于生产 gRPC 与单测替身复用。"""

    async def get(self, key: str) -> tuple[bytes | None, int]: ...
    def watch(self, key: str, start_revision: int) -> AsyncIterator[EtcdWatchEvent]: ...


class GrpcEtcdKVWatcher:
    """基于仓库原生 GrpcClient + Protocol 的 etcd v3 KV/Watch 适配器（零新增依赖）。"""

    def __init__(self, protocol, endpoint: str, *, timeout: float = 5.0) -> None:
        self._protocol = protocol
        self._client = GrpcClient(endpoint, timeout=timeout)

    async def get(self, key: str) -> tuple[bytes | None, int]:
        req = self._protocol.message("etcdserverpb.RangeRequest", key=key.encode("utf-8"))
        raw = await self._client.call("/etcdserverpb.KV/Range", req.SerializeToString())
        resp = self._protocol.parse("etcdserverpb.RangeResponse", raw)
        rev = int(resp.header.revision)
        if not resp.kvs:
            return None, rev
        return bytes(resp.kvs[0].value), rev

    async def watch(self, key: str, start_revision: int) -> AsyncIterator[EtcdWatchEvent]:
        call = self._client.channel.stream_stream("/etcdserverpb.Watch/Watch")()
        try:
            create_req: dict[str, object] = {"key": key.encode("utf-8")}
            if start_revision > 0:
                create_req["start_revision"] = int(start_revision)
            req = self._protocol.message("etcdserverpb.WatchRequest", create_request=create_req)
            await call.write(req.SerializeToString())
            while True:
                raw = await call.read()
                if raw is grpc.aio.EOF:
                    return
                resp = self._protocol.parse("etcdserverpb.WatchResponse", raw)
                if resp.canceled:
                    return
                header_rev = int(resp.header.revision)
                for ev in resp.events:
                    ev_type = "DELETE" if int(ev.type) == 1 else "PUT"
                    yield EtcdWatchEvent(revision=header_rev, event_type=ev_type, value=bytes(ev.kv.value))
        finally:
            call.cancel()

    async def close(self) -> None:
        await self._client.close()


def apply_payload(key: str, raw: bytes | str, target: AtomicTable) -> bool:
    """解析并应用路由表更新；畸形载荷 fail-closed 拒收，保留上一版合法路由表。"""
    try:
        table, version = parse_table_json(raw)
    except Exception as exc:
        logger.error(
            "[cellroute] rejected invalid routing table update on key=%s (keeping previous table): %s",
            key, exc,
        )
        return False
    try:
        target.swap(table)
    except Exception as exc:
        logger.error("[cellroute] failed to swap routing table on key=%s: %s", key, exc)
        return False
    logger.info("[cellroute] reloaded routing table on key=%s version=%d", key, version)
    return True


def build_static_router(cfg: CellRouteConfig) -> Router | None:
    """同步构建静态路由器；``mode == ""`` 返回 ``None``，``mode == "etcd"`` 请使用 ``build_router``。"""
    cfg.validate()
    if cfg.mode == "":
        return None
    if cfg.mode == "static":
        entries, regions = build_balanced_entries(cfg.cells)
        return Router(StaticTable(entries, regions))
    raise InvalidConfigError(f"cellroute: mode {cfg.mode!r} requires async build_router")


async def _load_initial_from_etcd(cli: EtcdKVWatcher, cfg: CellRouteConfig) -> tuple[StaticTable, int]:
    value: bytes | None = None
    rev = 0
    get_err: Exception | None = None
    try:
        value, rev = await cli.get(cfg.etcd_key)
    except Exception as exc:
        get_err = exc

    if get_err is None and value is not None:
        try:
            table, _ = parse_table_json(value)
        except Exception as exc:
            raise CellRouteError(f"cellroute: initial etcd key {cfg.etcd_key} invalid: {exc}") from exc
        return table, rev

    if cfg.cells:
        entries, regions = build_balanced_entries(cfg.cells)
        return StaticTable(entries, regions), rev

    if get_err is not None:
        raise CellRouteError(f"cellroute: failed to load initial etcd key {cfg.etcd_key}: {get_err}") from get_err
    raise CellRouteError(f"cellroute: initial etcd key {cfg.etcd_key} not found and no seed cells configured")


class CellRouteWatcher:
    """持续监听 etcd 路由表键并执行周期全量重同步（断连退避重连，畸形更新 fail-closed）。"""

    def __init__(
        self,
        cli: EtcdKVWatcher,
        key: str,
        target: AtomicTable,
        *,
        start_revision: int = 0,
        resync_interval_s: float = 0.0,
        reconnect_backoff_s: float = WATCH_RECONNECT_BACKOFF_S,
    ) -> None:
        self._cli = cli
        self._key = key
        self._target = target
        self._rev = int(start_revision)
        self._resync_interval_s = resync_interval_s if resync_interval_s > 0 else DEFAULT_RESYNC_INTERVAL_S
        self._reconnect_backoff_s = reconnect_backoff_s
        self._closed = False
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="cellroute-etcd-watch")

    async def _consume_watch(self) -> None:
        start_rev = self._rev + 1 if self._rev > 0 else 0
        async for ev in self._cli.watch(self._key, start_rev):
            if self._closed:
                return
            if ev.revision > self._rev:
                self._rev = ev.revision
            if ev.event_type != "PUT":
                continue
            apply_payload(self._key, ev.value, self._target)

    async def _run(self) -> None:
        while not self._closed:
            try:
                async with asyncio.timeout(self._resync_interval_s):
                    await self._consume_watch()
            except asyncio.CancelledError:
                return
            except TimeoutError:
                pass
            except Exception as exc:
                logger.error("[cellroute] watch canceled on key=%s: %s", self._key, exc)

            if self._closed:
                return

            # 周期全量重同步或断连恢复
            try:
                async with asyncio.timeout(5.0):
                    value, rev = await self._cli.get(self._key)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("[cellroute] resync get failed on key=%s: %s", self._key, exc)
                try:
                    await asyncio.sleep(self._reconnect_backoff_s)
                except asyncio.CancelledError:
                    return
                continue

            if rev > self._rev:
                self._rev = rev
            if value is not None:
                apply_payload(self._key, value, self._target)

    async def stop(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None


async def build_router(
    cfg: CellRouteConfig,
    etcd_client: EtcdKVWatcher | None = None,
) -> tuple[Router | None, Callable[[], Awaitable[None]] | None]:
    """按配置构造可选 Router；``cfg.mode == ""`` 返回 ``(None, None)``。"""
    cfg.validate()
    if cfg.mode == "":
        return None, None
    if cfg.mode == "static":
        entries, regions = build_balanced_entries(cfg.cells)
        return Router(StaticTable(entries, regions)), None
    if cfg.mode == "etcd":
        if etcd_client is None:
            raise CellRouteError("cellroute: etcd mode requires non-nil etcd client")
        initial, rev = await _load_initial_from_etcd(etcd_client, cfg)
        atomic_table = AtomicTable(initial)
        router = Router(atomic_table)
        watcher = CellRouteWatcher(
            etcd_client,
            cfg.etcd_key,
            atomic_table,
            start_revision=rev,
            resync_interval_s=cfg.resync_interval_s,
        )
        watcher.start()
        return router, watcher.stop
    raise InvalidConfigError(f"cellroute: invalid configuration: unsupported mode {cfg.mode!r}")


# ── 登录侧亲和选路与归属守卫（对照 assigngatelogic.go / entergamelogic.go） ───

def select_cell_affinity_candidates(
    router: Router | None,
    player_id: int,
    candidates: Sequence[_T],
) -> list[_T]:
    """启用 CellRouter 时优先筛选 zone_id 命中玩家 cell_id 的 gate；无匹配则回退全量候选。"""
    items = list(candidates)
    if router is None or player_id <= 0 or not items:
        return items
    try:
        loc = router.route(player_id)
    except Exception:
        return items
    matched = [cand for cand in items if int(getattr(cand, "zone_id", 0)) == loc.cell_id]
    return matched if matched else items


def check_cell_route_ownership(router: Router | None, cfg: CellRouteConfig, player_id: int) -> None:
    """校验 EnterGame 玩家是否归属本单元；shadow 模式仅打日志不阻断，enforce 模式抛错拒入。"""
    if router is None:
        return
    try:
        loc = router.route(player_id)
    except Exception as exc:
        logger.error("[cellroute] EnterGame route lookup failed player_id=%d err=%s", player_id, exc)
        if cfg.enforce_ownership:
            raise
        return
    if (cfg.self_region != 0 or cfg.self_cell != 0) and (
        loc.region_id != cfg.self_region or loc.cell_id != cfg.self_cell
    ):
        logger.error(
            "[cellroute] foreign cell EnterGame detected player_id=%d self=(%d,%d) owner=(%d,%d) "
            "logical_cell=%d enforce=%s",
            player_id, cfg.self_region, cfg.self_cell, loc.region_id, loc.cell_id,
            loc.logical_cell, cfg.enforce_ownership,
        )
        if cfg.enforce_ownership:
            raise CellRouteError(
                f"player {player_id} belongs to cell ({loc.region_id},{loc.cell_id}), "
                f"not local cell ({cfg.self_region},{cfg.self_cell})"
            )
