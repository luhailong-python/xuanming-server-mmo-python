"""Independent HTTP portal, replacing the native Gateway's HTTP entry."""

import asyncio
import os
import tomllib
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import aiomysql
from aiohttp import web
from redis.asyncio import Redis

from .cluster import EtcdDirectory
from .portal_login import Limiter, LoginRpc, register_login_routes
from .portal_observability import Metrics, create_management_app
from .protocol import Protocol


def load_settings(path: Path):
    with path.open("rb") as file:
        values = tomllib.load(file).get("portal", {})
    defaults = {
        "host": "127.0.0.1", "port": 8080, "management_host": "127.0.0.1", "management_port": 8082,
        "mysql_host": "127.0.0.1", "mysql_port": 3306,
        "mysql_user": "", "mysql_database": "", "redis_url": "redis://127.0.0.1:6379/0",
        "etcd": "", "login_endpoints": {}, "rate_limit_enabled": False, "trusted_proxies": [],
        "ip_rps": 2, "ip_burst": 10, "zone_rps": 500, "zone_burst": 1000,
        "account_cooldown_ms": 5000, "cdn_base_url": "", "cdn_sign_ttl_seconds": 3600,
        "hotfix_platforms": {},
        "home_zone_ids": [], "zone_policy_interval_seconds": 5, "role_capacity_interval_seconds": 10,
        "zone_overrides": {}, "wave_enabled": False, "wave_start_epoch_seconds": 0, "wave_steps": [],
        "mall_admin_base_url": "", "mall_admin_subject": "mall-admin",
    }
    extra = set(values) - defaults.keys()
    if extra:
        raise ValueError(f"unknown portal settings: {sorted(extra)}")
    settings = SimpleNamespace(**(defaults | values))
    settings.mysql_password = os.getenv("XUANMING_PORTAL_MYSQL_PASSWORD", "")
    settings.cdn_signing_secret = os.getenv("XUANMING_PORTAL_CDN_SIGNING_SECRET", "")
    settings.admin_api_key = os.getenv("XUANMING_PORTAL_ADMIN_API_KEY", "")
    settings.mall_admin_secret = os.getenv("COMMERCE_ADMIN_SECRET", "")
    if (not settings.mysql_host or not settings.mysql_user or not settings.mysql_database
            or not 0 <= settings.port <= 65535 or not 0 <= settings.management_port <= 65535
            or not settings.management_host or not 0 < settings.mysql_port <= 65535
            or not all(isinstance(x, int) and x > 0 for x in
                       (settings.ip_rps, settings.ip_burst, settings.zone_rps, settings.zone_burst))
            or not isinstance(settings.login_endpoints, dict)):
        raise ValueError("invalid portal settings")
    if (not isinstance(settings.home_zone_ids, list)
            or any(type(zone) is not int or not 0 < zone < 2**32 for zone in settings.home_zone_ids)
            or len(set(settings.home_zone_ids)) != len(settings.home_zone_ids)
            or settings.home_zone_ids and not settings.etcd
            or settings.zone_policy_interval_seconds <= 0 or settings.role_capacity_interval_seconds <= 0):
        raise ValueError("invalid home zone configuration")
    if (not isinstance(settings.zone_overrides, dict) or not isinstance(settings.wave_steps, list)
            or type(settings.wave_start_epoch_seconds) is not int
            or any(not isinstance(spec, dict) or any(type(spec.get(k, 0)) is not int for k in ("rps", "burst"))
                   for spec in settings.zone_overrides.values())
            or any(not isinstance(step, dict) or type(step.get("offset_seconds")) is not int
                   or not isinstance(step.get("allow_zones"), list)
                   or any(type(zone) is not int for zone in step["allow_zones"]) for step in settings.wave_steps)):
        raise ValueError("invalid portal rate-limit settings")
    return settings


@asynccontextmanager
async def open_deps(settings):
    protocol = Protocol()
    pool = await aiomysql.create_pool(host=settings.mysql_host, port=settings.mysql_port,
                                      user=settings.mysql_user, password=settings.mysql_password,
                                      db=settings.mysql_database, minsize=1, maxsize=10,
                                      connect_timeout=3, autocommit=False)
    async def close_pool():
        pool.close()
        await pool.wait_closed()

    async with AsyncExitStack() as stack:
        stack.push_async_callback(close_pool)
        redis = Redis.from_url(settings.redis_url, socket_timeout=1, socket_connect_timeout=1)
        stack.push_async_callback(redis.aclose)
        directory = EtcdDirectory(protocol, settings.etcd) if settings.etcd else None
        if directory:
            stack.push_async_callback(directory.close)
        rpc = LoginRpc(protocol, settings.login_endpoints, directory)
        stack.push_async_callback(rpc.close)
        metrics = Metrics()
        deps = SimpleNamespace(settings=settings, protocol=protocol, db=pool, redis=redis,
                               directory=directory, rpc=rpc, metrics=metrics,
                               limiter=Limiter(redis, settings, metrics=metrics))
        yield deps


def create_app(deps, extra_routes=None):
    from .portal_admin_announcements import register_routes as register_admin_announcements
    from .portal_admin_killswitch import register_routes as register_admin_killswitch
    from .portal_admin_zones import register_routes as register_admin_zones
    from .portal_catalog import register_routes as register_catalog
    from .portal_server_list import register_routes as register_server_list
    from .portal_health import register_background as register_zone_health
    from .portal_mall_admin import register_routes as register_mall_admin
    from .portal_zone_policy import register_background as register_zone_policy

    if not hasattr(deps, "metrics"):
        deps.metrics = Metrics()
    app = web.Application(client_max_size=64 * 1024, middlewares=[deps.metrics.instrument])
    register_login_routes(app, deps)
    register_catalog(app, deps)
    register_server_list(app, deps)
    register_admin_announcements(app, deps)
    register_admin_killswitch(app, deps)
    register_admin_zones(app, deps)
    register_mall_admin(app, deps)
    register_zone_policy(app, deps)
    register_zone_health(app, deps)
    if extra_routes:
        extra_routes(app, deps)
    return app


async def run_portal(settings):
    async with open_deps(settings) as deps:
        runner = web.AppRunner(create_app(deps), access_log=None)
        management = web.AppRunner(create_management_app(deps), access_log=None)
        try:
            await runner.setup()
            await management.setup()
            await web.TCPSite(management, settings.management_host, settings.management_port).start()
            await web.TCPSite(runner, settings.host, settings.port).start()
            deps.metrics.ready = True
            await asyncio.Event().wait()
        finally:
            deps.metrics.ready = False
            await runner.cleanup()
            await management.cleanup()
