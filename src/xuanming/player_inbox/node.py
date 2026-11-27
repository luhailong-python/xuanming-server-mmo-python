"""player_inbox 节点生命周期：配置校验 -> 建池验表 -> 启动 gRPC -> 注册 etcd(ttl=60) -> 优雅下线。"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from contextlib import AsyncExitStack

from ..cluster import EtcdDirectory, KafkaCommands
from ..grpc_runtime import GrpcClient, serve_grpc
from ..kill_switch import EtcdKillSwitch, KillSwitch
from ..protocol import Protocol
from ..settings import Settings
from .config import PlayerInboxConfig
from .model import PLAYER_INBOX_NODE_TYPE
from .notify import KafkaInboxNotifier, NullInboxNotifier, RedisLocationReader
from .registry import InboxRegistry
from .service import PlayerInboxService
from .sql_store import SqlInboxStore, open_pool
from .store import GrpcDataServiceDirectory

logger = logging.getLogger(__name__)


async def close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def open_service(
    config: PlayerInboxConfig,
    protocol: Protocol,
    stack: AsyncExitStack,
    *,
    etcd_directory: EtcdDirectory | None = None,
) -> PlayerInboxService:
    """打开独占库连接池、验表核片并装配 ``PlayerInboxService``（尚未监听端口）。"""
    registry = InboxRegistry(protocol)
    allowed_types = registry.parse_enqueue_types(config.enqueue_types)

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
    directory = GrpcDataServiceDirectory(protocol, ds_client.call, config.role_fence_secret)

    notifier = NullInboxNotifier()
    if config.kafka and config.redis and etcd_directory is not None:
        from redis.asyncio import Redis

        redis_client = Redis.from_url(config.redis, decode_responses=False)
        stack.push_async_callback(redis_client.aclose)
        kafka = KafkaCommands(
            config.kafka,
            partitions=config.kafka_partitions,
            generation=config.kafka_generation,
        )
        await kafka.start()
        stack.push_async_callback(kafka.close)
        notifier = KafkaInboxNotifier(
            protocol,
            RedisLocationReader(redis_client),
            etcd_directory,
            kafka,
        )

    store = SqlInboxStore(
        pool,
        protocol,
        registry,
        directory,
        allowed_types=allowed_types,
        notifier=notifier,
    )
    if config.bootstrap_schema:
        await store.bootstrap()
    await store.verify_schema()
    await store.check_authority_indexes()
    await store.check_row_shards()

    return PlayerInboxService(
        protocol,
        store,
        scene_secret=config.scene_secret,
        coordinator_secret=config.coordinator_secret,
        match_secret=config.match_secret,
        admin_token=config.admin_token,
        request_budget_sec=config.request_budget_sec,
        allow_unapproved=config.admin_allow_unapproved,
    )


async def run(settings: Settings) -> None:
    """运行 player_inbox 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    protocol = Protocol()
    config = PlayerInboxConfig.from_settings(settings, protocol)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=PLAYER_INBOX_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=int(time.time()),
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )
            scene_directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            stack.push_async_callback(scene_directory.close)

            service = await open_service(config, protocol, stack, etcd_directory=scene_directory)

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
            # AsyncExitStack 按 LIFO 执行：先注销 etcd，再排空在途 gRPC，最后关连接池与依赖。
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="player-inbox-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "player_inbox ready node_id=%d zone=%d grpc=%s:%d ttl=%d admin=%s battle_producer=%s",
                config.node_id,
                config.zone_id,
                config.advertise_host,
                port,
                config.lease_ttl_sec,
                bool(config.admin_token),
                bool(config.match_secret),
            )
            done, _ = await asyncio.wait(
                {keepalive, asyncio.ensure_future(stop.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if keepalive in done:
                keepalive.result()
    finally:
        signal.signal(signal.SIGTERM, previous)
