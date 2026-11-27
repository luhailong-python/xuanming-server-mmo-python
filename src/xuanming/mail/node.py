"""mail 节点生命周期：配置校验 -> 建池验表 -> 装配 gRPC/后台循环 -> 注册 etcd(ttl=60) -> 优雅下线。"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from contextlib import AsyncExitStack
from pathlib import Path

from ..cluster import EtcdDirectory, KafkaCommands
from ..data import ConfigTables
from ..grpc_runtime import GrpcClient, serve_grpc
from ..kill_switch import EtcdKillSwitch, KillSwitch
from ..login.caller_auth import CallerVerifier
from ..protocol import Protocol
from ..settings import Settings
from .asset_loop import (
    AssetDeliveryLoop,
    AssetOpCaller,
    GrpcDataServiceLedgerReader,
    RedisEtcdSceneResolver,
)
from .config import MailConfig
from .model import MAIL_NODE_TYPE
from .notify import KafkaMailNotifier
from .service import MailService
from .sql_store import SqlMailStore, open_pool
from .store import (
    GrpcGuildDirectory,
    GrpcRoleFenceDirectory,
    IdSegmentAllocator,
    MailNotifier,
    NullGuildDirectory,
)
from .sweep import MailExpirySweeper, SweepStore

logger = logging.getLogger(__name__)


async def close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


def _load_item_exists(tables_dir: str):
    tables = ConfigTables(Path(tables_dir)) if tables_dir else ConfigTables()
    item_table = tables["Item"]
    return lambda config_id: item_table.get(config_id) is not None


def build_sweeper(store: SweepStore, config: MailConfig) -> MailExpirySweeper | None:
    """按发布轨道构建过期邮件清理器；canary 副本仅承接 RPC，停用清理循环。"""
    if not config.runs_background_work:
        return None
    return MailExpirySweeper(
        store,
        mode=config.sweep_mode,
        retention_days=config.sweep_retention_days,
        batch_limit=config.sweep_batch_limit,
        interval_sec=config.sweep_interval_sec,
    )


async def open_service(
    config: MailConfig,
    protocol: Protocol,
    stack: AsyncExitStack,
    *,
    etcd_directory: EtcdDirectory | None = None,
    environ=None,
) -> tuple[MailService, SqlMailStore, AssetDeliveryLoop | None, MailExpirySweeper | None]:
    """打开独占库连接池、验表并装配 MailService、KafkaMailNotifier、AssetDeliveryLoop 与 MailExpirySweeper。"""
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

    role_directory = GrpcRoleFenceDirectory(protocol, ds_client.call, config.role_fence_secret)
    mail_ids = IdSegmentAllocator(protocol, ds_client.call, "mail")
    op_ids = IdSegmentAllocator(protocol, ds_client.call, "mail_asset_op")

    guilds = NullGuildDirectory()
    if config.guild_service:
        guild_client = GrpcClient(config.guild_service, timeout=0.5, enable_retries=False)
        stack.push_async_callback(guild_client.close)
        guilds = GrpcGuildDirectory(protocol, guild_client.call)

    store = SqlMailStore(
        pool,
        protocol,
        mail_ids=mail_ids,
        op_ids=op_ids,
        item_exists=_load_item_exists(config.tables_dir),
        guilds=guilds,
        role_directory=role_directory,
        limits=config.limits,
    )
    if config.bootstrap_schema:
        await store.bootstrap()
    await store.verify_schema()

    player_redis = None
    player_redis_url = config.effective_player_redis
    if player_redis_url:
        from redis.asyncio import Redis

        player_redis = Redis.from_url(player_redis_url, decode_responses=False)
        stack.push_async_callback(player_redis.aclose)

    notifier: MailNotifier | None = None
    if config.kafka and player_redis is not None:
        kafka = KafkaCommands(
            config.kafka,
            partitions=config.kafka_partitions,
            generation=config.kafka_generation,
        )
        await kafka.start()
        stack.push_async_callback(kafka.close)
        notifier = KafkaMailNotifier(
            protocol,
            player_redis,
            kafka,
            timeout_sec=config.notify_timeout_sec,
        )
    else:
        logger.info("mail kafka or player_redis unconfigured: post-commit NotifyMailChanged disabled")

    caller_verifier = CallerVerifier.from_env(environ, allowed_callers=("gate",))
    service = MailService(
        protocol,
        store,
        caller_verifier=caller_verifier,
        producer_secrets=config.producer_secret_map(),
        coordinator_secret=config.coordinator_secret,
        notifier=notifier,
        request_budget_sec=config.request_budget_sec,
        approval_required_callers=config.approval_required_callers,
        allow_unapproved=config.admin_allow_unapproved,
    )

    asset_loop: AssetDeliveryLoop | None = None
    if config.runs_background_work and player_redis is not None and etcd_directory is not None:
        resolver = RedisEtcdSceneResolver(protocol, player_redis, etcd_directory)
        stack.push_async_callback(resolver.close)
        caller = AssetOpCaller(protocol, resolver, config.asset_op_secret)
        ledger_reader = GrpcDataServiceLedgerReader(protocol, ds_client.call)
        asset_loop = AssetDeliveryLoop(
            protocol,
            store,
            caller,
            ledger_reader,
            interval_sec=config.asset_loop_interval_sec,
            batch=config.asset_loop_batch,
            workers=config.asset_loop_workers,
        )

    sweeper = build_sweeper(store, config)
    return service, store, asset_loop, sweeper


async def run(settings: Settings) -> None:
    """运行 mail 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    protocol = Protocol()
    config = MailConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=MAIL_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=int(time.time()),
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )
            scene_directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            stack.push_async_callback(scene_directory.close)

            service, store, asset_loop, sweeper = await open_service(
                config, protocol, stack, etcd_directory=scene_directory
            )

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

            bg_tasks: list[asyncio.Task] = []
            if config.runs_background_work:
                if asset_loop is not None:
                    t_asset = asyncio.create_task(asset_loop.run(stop_event=stop), name="mail-asset-loop")
                    bg_tasks.append(t_asset)
                    stack.callback(t_asset.cancel)
                if sweeper is not None:
                    t_sweep = asyncio.create_task(sweeper.run(stop_event=stop), name="mail-sweep-loop")
                    bg_tasks.append(t_sweep)
                    stack.callback(t_sweep.cancel)
            else:
                logger.info(
                    "mail release_track=%s: asset_loop and sweep disabled (canary serves RPCs only)",
                    config.release_track,
                )

            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="mail-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "mail ready node_id=%d zone=%d grpc=%s:%d ttl=%d track=%s",
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
