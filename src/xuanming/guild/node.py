"""guild 节点生命周期：配置校验 -> 建池验表 -> 装配 gRPC/排行榜 -> 注册 etcd(ttl=60) -> 优雅下线。"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from contextlib import AsyncExitStack
from pathlib import Path

from ..cluster import EtcdDirectory
from ..data import ConfigTables
from ..grpc_runtime import GrpcClient, serve_grpc
from ..kill_switch import EtcdKillSwitch, KillSwitch
from ..login.caller_auth import CallerVerifier
from ..protocol import Protocol
from ..settings import Settings
from .config import GuildConfig
from .model import GUILD_NODE_TYPE, GuildTableCatalog
from .rank_store import GuildRankStore, MemoryGuildRankStore, RedisGuildRankStore
from .service import GuildService
from .sql_store import SqlGuildStore, open_pool
from .store import (
    ContentGuardChecker,
    GrpcContentGuardChecker,
    GrpcHomeZoneDirectory,
    GrpcMergedZoneDirectory,
    GrpcPlayerNameResolver,
    GrpcRoleFenceDirectory,
    IdSegmentAllocator,
    NullContentGuardChecker,
)

logger = logging.getLogger(__name__)


async def close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


def _load_catalog(tables_dir: str) -> GuildTableCatalog:
    tables = ConfigTables(Path(tables_dir)) if tables_dir else ConfigTables()
    return GuildTableCatalog.from_config_tables(tables)


async def open_service(
    config: GuildConfig,
    protocol: Protocol,
    stack: AsyncExitStack,
    *,
    environ=None,
) -> tuple[GuildService, SqlGuildStore]:
    """打开独占库连接池、验表、加载合服映射并装配 GuildService。"""
    catalog = _load_catalog(config.tables_dir)
    pool = await open_pool(
        host=config.mysql_host,
        port=config.mysql_port,
        user=config.mysql_user,
        password=config.mysql_password,
        database=config.mysql_database,
        maxsize=config.max_open_conn,
        minsize=config.min_open_conn,
    )
    stack.push_async_callback(close_pool, pool)

    ds_client = GrpcClient(config.data_service, timeout=config.request_budget_sec)
    stack.push_async_callback(ds_client.close)

    merged_dir = GrpcMergedZoneDirectory(protocol, ds_client.call)
    merged_zones = await merged_dir.get_merged_zones()

    role_directory = GrpcRoleFenceDirectory(protocol, ds_client.call, config.role_fence_secret)
    home_zones = GrpcHomeZoneDirectory(protocol, ds_client.call)
    player_names = GrpcPlayerNameResolver(protocol, ds_client.call)
    guild_ids = IdSegmentAllocator(protocol, ds_client.call, "guild")
    op_ids = IdSegmentAllocator(protocol, ds_client.call, "guild_asset_op")

    content_guard: ContentGuardChecker = NullContentGuardChecker()
    if config.content_guard:
        cg_client = GrpcClient(config.content_guard, timeout=1.5, enable_retries=False)
        stack.push_async_callback(cg_client.close)
        content_guard = GrpcContentGuardChecker(protocol, cg_client.call)

    store = SqlGuildStore(
        pool,
        protocol,
        catalog=catalog,
        guild_ids=guild_ids,
        op_ids=op_ids,
        role_directory=role_directory,
        merged_zones=merged_zones,
    )
    if config.bootstrap_schema:
        await store.bootstrap()
    await store.verify_schema()

    rank_store: GuildRankStore
    if config.redis:
        from redis.asyncio import Redis

        redis_client = Redis.from_url(config.redis, decode_responses=False)
        stack.push_async_callback(redis_client.aclose)
        rank_store = RedisGuildRankStore(redis_client, merged_zones=merged_zones)
    else:
        rank_store = MemoryGuildRankStore(merged_zones=merged_zones)

    if config.runs_background_work:
        try:
            rank_rows = await store.list_rank_rows()
            await rank_store.rebuild_ranks(rank_rows)
            logger.info("guild rank reconciled rows=%d", len(rank_rows))
        except Exception:
            logger.error("guild rank initial rebuild failed", exc_info=True)

    caller_verifier = CallerVerifier.from_env(environ, allowed_callers=("gate",))
    service = GuildService(
        protocol,
        store,
        rank_store=rank_store,
        caller_verifier=caller_verifier,
        coordinator_secret=config.coordinator_secret,
        home_zones=home_zones,
        player_names=player_names,
        content_guard=content_guard,
        asset_op_enabled=config.asset_op_enabled,
        asset_op_lease_ms=config.asset_op_lease_ms,
        terminal_retention_days=config.terminal_retention_days,
        trials_enabled=config.trials_enabled,
        application_quota_per_minute=config.application_quota_per_minute,
        request_budget_sec=config.request_budget_sec,
    )
    return service, store


async def run(settings: Settings) -> None:
    """运行 guild 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    protocol = Protocol()
    config = GuildConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=GUILD_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=int(time.time()),
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )

            service, _ = await open_service(config, protocol, stack)

            kill_switch = KillSwitch(protocol, info)
            etcd_ks = EtcdKillSwitch(protocol, settings.etcd, kill_switch)
            etcd_ks.start()
            stack.push_async_callback(etcd_ks.close)

            server, port = await serve_grpc(
                protocol,
                f"{config.host}:{config.grpc_port}",
                service.services(),
                kill_switch=kill_switch,
                pass_context=True,
            )
            grace_sec = max(3.0, config.request_budget_sec)
            stack.push_async_callback(server.stop, grace_sec)

            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="guild-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "guild ready node_id=%d zone=%d grpc=%s:%d ttl=%d track=%s",
                config.node_id,
                config.zone_id,
                config.advertise_host,
                port,
                config.lease_ttl_sec,
                config.release_track,
            )
            done, _ = await asyncio.wait(
                {keepalive, asyncio.ensure_future(stop.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if keepalive in done:
                keepalive.result()
    finally:
        signal.signal(signal.SIGTERM, previous)
