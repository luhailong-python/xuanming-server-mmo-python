"""配置表查询与原子热更 HTTP 服务（对标 Java config_node TableService/TableController/ReloadController）。"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import logging
import re
import time
from pathlib import Path

from aiohttp import web
from google.protobuf.json_format import MessageToDict

from ..data import ConfigTableError, ConfigTables

_LOG = logging.getLogger(__name__)
_INT_PATTERN = re.compile(r"-?[0-9]+\Z")


def _is_loopback(addr: str | None) -> bool:
    if not addr:
        return False
    try:
        return ipaddress.ip_address(addr).is_loopback
    except ValueError:
        return False


def _key_matches(configured: str, supplied: list[str]) -> bool:
    if len(supplied) != 1:
        return False
    expected = hashlib.sha256(configured.encode("utf-8")).digest()
    actual = hashlib.sha256(supplied[0].encode("utf-8")).digest()
    return hmac.compare_digest(expected, actual)


class ConfigNodeService:
    """管理配置表快照的初始加载、原子热更与按表/主键查询。"""

    def __init__(
        self,
        table_dir: str | Path | None = None,
        *,
        fmt: str = "json",
        descriptor_set: str | Path | None = None,
        tables: ConfigTables | None = None,
    ) -> None:
        self._table_dir = Path(table_dir) if table_dir is not None else None
        self._format = fmt
        self._descriptor_set = Path(descriptor_set) if descriptor_set is not None else None
        self._reload_lock = asyncio.Lock()
        self._snapshot: ConfigTables | None = tables
        self._load_version: int = 1 if tables is not None else 0
        self._last_load_time_ms: int = 0

    @property
    def snapshot(self) -> ConfigTables | None:
        return self._snapshot

    @property
    def load_version(self) -> int:
        return self._load_version

    @property
    def last_load_time_ms(self) -> int:
        return self._last_load_time_ms

    def _build_snapshot(self, directory: Path | None) -> ConfigTables:
        return ConfigTables(directory, self._format, descriptor_set=self._descriptor_set)

    def load_all(self) -> ConfigTables:
        """同步执行全量配置表加载，供节点启动期调用。"""
        started = time.perf_counter()
        candidate = self._build_snapshot(self._table_dir)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        self._snapshot = candidate
        self._load_version += 1
        self._last_load_time_ms = elapsed_ms
        _LOG.info(
            "config_node tables loaded version=%d tables=%d took_ms=%d",
            self._load_version,
            len(candidate.tables),
            elapsed_ms,
        )
        return candidate

    async def reload(self, directory: str | Path | None = None) -> ConfigTables:
        """原子重载全量配置表；校验失败时保留旧快照与版本号不变。"""
        target = self._table_dir if directory is None else Path(directory)
        async with self._reload_lock:
            started = time.perf_counter()
            try:
                candidate = await asyncio.to_thread(self._build_snapshot, target)
            except ConfigTableError:
                _LOG.error("config_node table reload failed", exc_info=True)
                raise
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            self._snapshot = candidate
            self._load_version += 1
            self._last_load_time_ms = elapsed_ms
            _LOG.info(
                "config_node tables reloaded version=%d tables=%d took_ms=%d",
                self._load_version,
                len(candidate.tables),
                elapsed_ms,
            )
            return candidate

    def list_tables(self) -> list[str]:
        """返回当前快照已加载的全部配置表名（按字典序）。"""
        snapshot = self._snapshot
        return sorted(snapshot.tables.keys()) if snapshot is not None else []

    def query_by_id(self, table_name: str, row_id: int) -> dict | None:
        """按表名和主键值查询单行并转为 protobuf JSON 字典；表或行不存在返回 None。"""
        snapshot = self._snapshot
        if snapshot is None:
            return None
        table = snapshot.tables.get(table_name)
        if table is None:
            return None
        try:
            row = table.get(row_id)
        except KeyError:
            matches = table.find_all(row_id)
            row = matches[0] if matches else None
        if row is None:
            return None
        return MessageToDict(row, preserving_proto_field_name=False)

    def status(self) -> dict:
        """返回当前加载版本、耗时与已加载表清单，兼容 Java TableController /api/table/status。"""
        snapshot = self._snapshot
        return {
            "loadVersion": self._load_version,
            "lastLoadTimeMs": self._last_load_time_ms,
            "tableCount": len(snapshot.tables) if snapshot is not None else 0,
            "tables": sorted(snapshot.tables.keys()) if snapshot is not None else [],
            "tableVersion": snapshot.version if snapshot is not None else 0,
            "contentDigest": snapshot.content_digest if snapshot is not None else "",
        }


def create_app(service: ConfigNodeService, *, reload_api_key: str = "") -> web.Application:
    """构建 config_node aiohttp 应用，挂载查表、状态、健康检查与热更端点。"""
    configured_key = reload_api_key.strip() if isinstance(reload_api_key, str) else ""
    app = web.Application(client_max_size=64 * 1024)

    async def list_tables(_request: web.Request) -> web.Response:
        names = service.list_tables()
        return web.json_response({
            "tables": names,
            "count": len(names),
            "loadVersion": service.load_version,
            "lastLoadTimeMs": service.last_load_time_ms,
        })

    async def table_status(_request: web.Request) -> web.Response:
        return web.json_response(service.status())

    async def get_by_id(request: web.Request) -> web.Response:
        table_name = request.match_info["tableName"]
        raw_id = request.match_info["id"]
        if not _INT_PATTERN.fullmatch(raw_id):
            return web.json_response({"error": "invalid id"}, status=400)
        row_id = int(raw_id)
        if not -(1 << 63) <= row_id < (1 << 63):
            return web.json_response({"error": "invalid id"}, status=400)
        row = service.query_by_id(table_name, row_id)
        if row is None:
            return web.Response(status=404)
        return web.json_response(row)

    async def reload_tables(request: web.Request) -> web.Response:
        # 鉴权策略与 Java ReloadController 一致：配了密钥则强制 X-Admin-Key 恒定时间比对；
        # 未配密钥时仅允许直连本机回环（拒绝 X-Forwarded-For 或远端来源）。
        if configured_key:
            supplied = request.headers.getall("X-Admin-Key", [])
            if not _key_matches(configured_key, supplied):
                return web.json_response({"status": "unauthorized"}, status=401)
        else:
            forwarded = request.headers.get("X-Forwarded-For", "")
            if forwarded.strip() or not _is_loopback(request.remote):
                return web.json_response({"status": "forbidden"}, status=403)
        try:
            await service.reload()
        except ConfigTableError:
            return web.json_response({"status": "error", "error": "table reload failed"}, status=500)
        return web.json_response({
            "status": "ok",
            "loadVersion": service.load_version,
            "lastLoadTimeMs": service.last_load_time_ms,
        })

    async def health(_request: web.Request) -> web.Response:
        loaded = service.snapshot is not None
        return web.json_response(
            {"status": "UP" if loaded else "DOWN"},
            status=200 if loaded else 503,
        )

    app.router.add_get("/api/table", list_tables)
    app.router.add_get("/api/table/status", table_status)
    app.router.add_get("/api/table/{tableName}/{id}", get_by_id)
    app.router.add_post("/api/reload", reload_tables)
    app.router.add_get("/actuator/health", health)
    return app
