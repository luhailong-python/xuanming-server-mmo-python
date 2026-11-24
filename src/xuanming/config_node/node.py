"""config_node 节点启动器：加载配置表 -> 启动 aiohttp 服务 -> 监听 SIGTERM 优雅停机。"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from aiohttp import web

from .app import ConfigNodeService, create_app

_LOG = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8090


@dataclass(frozen=True, slots=True)
class ConfigNodeConfig:
    """config_node 运行配置，支持 [config_node] 配置段与环境变量覆盖。"""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    table_dir: Path | None = None
    format: str = "json"
    reload_api_key: str = ""

    @classmethod
    def from_settings(cls, settings=None, *, environ: Mapping[str, str] | None = None) -> "ConfigNodeConfig":
        env = os.environ if environ is None else environ
        raw = _extract_section(settings)

        host = str(env.get("XUANMING_CONFIG_NODE_HOST") or raw.get("host", DEFAULT_HOST)).strip()
        raw_port = env.get("XUANMING_CONFIG_NODE_PORT")
        if raw_port is not None and raw_port.strip() != "":
            try:
                port = int(raw_port)
            except ValueError as exc:
                raise ValueError("invalid XUANMING_CONFIG_NODE_PORT") from exc
        else:
            port = raw.get("port", DEFAULT_PORT)

        raw_dir = env.get("XUANMING_CONFIG_TABLE_DIR")
        if raw_dir is not None and raw_dir.strip() != "":
            table_dir: Path | None = Path(raw_dir.strip())
        else:
            cfg_dir = raw.get("table_dir")
            table_dir = Path(cfg_dir) if cfg_dir else None

        fmt = str(env.get("XUANMING_CONFIG_TABLE_FORMAT") or raw.get("format", "json")).strip().lower()
        reload_api_key = str(
            env.get("XUANMING_CONFIG_RELOAD_API_KEY") or raw.get("reload_api_key", "")
        ).strip()

        if not host or isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("invalid config_node host or port")
        if fmt not in ("json", "pb"):
            raise ValueError("config_node format must be 'json' or 'pb'")

        return cls(
            host=host,
            port=port,
            table_dir=table_dir,
            format=fmt,
            reload_api_key=reload_api_key,
        )


def _extract_section(settings) -> dict:
    if settings is None:
        return {}
    if isinstance(settings, Mapping):
        section = settings.get("config_node")
        return dict(section) if isinstance(section, Mapping) else dict(settings)
    section = getattr(settings, "config_node", None)
    if section is None:
        return {}
    if isinstance(section, Mapping):
        return dict(section)
    return {
        field: getattr(section, field)
        for field in ("host", "port", "table_dir", "format", "reload_api_key")
        if hasattr(section, field)
    }


async def run(
    settings=None,
    *,
    stop_event: asyncio.Event | None = None,
    on_ready: Callable[[int, ConfigNodeService], None] | None = None,
) -> None:
    """运行 config_node HTTP 服务，直至收到 SIGTERM 或外部 stop_event 触发优雅下线。"""
    config = ConfigNodeConfig.from_settings(settings)
    service = ConfigNodeService(config.table_dir, fmt=config.format)
    service.load_all()
    app = create_app(service, reload_api_key=config.reload_api_key)

    runner = web.AppRunner(app, access_log=None)
    stop = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        await runner.setup()
        site = web.TCPSite(runner, config.host, config.port)
        await site.start()
        sockets = site._server.sockets if site._server is not None else ()
        bound_port = sockets[0].getsockname()[1] if sockets else config.port
        _LOG.info(
            "config_node ready host=%s port=%d tables=%d version=%d",
            config.host,
            bound_port,
            len(service.list_tables()),
            service.load_version,
        )
        if on_ready is not None:
            on_ready(bound_port, service)
        await stop.wait()
    finally:
        signal.signal(signal.SIGTERM, previous)
        await runner.cleanup()
        _LOG.info("config_node stopped")
