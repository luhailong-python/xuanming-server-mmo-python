"""friend 节点生命周期：配置校验 -> 建池验表 -> 装配依赖 -> 启动 gRPC -> 注册 etcd(ttl=60) -> 优雅下线。"""

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
from ..login.caller_auth import CallerVerifier
from ..protocol import Protocol
from ..settings import Settings
from .config import FriendConfig
from .model import FRIEND_NODE_TYPE
from .notify import FriendPusher, KafkaFriendPusher, NullFriendPusher
from .redis_client import (
    RedisCacheInvalidator,
    RedisQuotaLimiter,
    RedisSessionStore,
    RedisSyncDigestBumper,
)
from .service import FriendService
from .sql_store import SqlFriendStore, open_pool
from .store import GrpcFriendDataServiceClient

logger = logging.getLogger(__name__)


async def close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def open_service(
    config: FriendConfig,
    protocol: Protocol,
    stack: AsyncExitStack,
    *,
    verifier: CallerVerifier | None = None,
) -> FriendService:
    """打开独占库连接池、Redis/Kafka 句柄并装配 ``FriendService``。"""
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

    ds_client = GrpcClient(config.data_service, timeout=max(3.0, config.request_budget_sec))
    stack.push_async_callback(ds_client.close)
    data_service = GrpcFriendDataServiceClient(protocol, ds_client.call, config.role_fence_secret)

    control_redis = None
    player_redis = None
    if config.control_redis:
        from redis.asyncio import Redis

        control_redis = Redis.from_url(config.control_redis, decode_responses=False)
        stack.push_async_callback(control_redis.aclose)
    effective_player_url = config.effective_player_redis
    if effective_player_url:
        if effective_player_url == config.control_redis and control_redis is not None:
            player_redis = control_redis
        else:
            from redis.asyncio import Redis

            player_redis = Redis.from_url(effective_player_url, decode_responses=False)
            stack.push_async_callback(player_redis.aclose)

    sessions = (
        RedisSessionStore(player_redis, protocol, batch_size=config.list_read_hard_limit)
        if player_redis is not None
        else None
    )
    quota = RedisQuotaLimiter(control_redis) if control_redis is not None else None
    sync_digest = RedisSyncDigestBumper(player_redis) if player_redis is not None else None
    cache_invalidator = RedisCacheInvalidator(control_redis) if control_redis is not None else None

    pusher: FriendPusher = NullFriendPusher()
    if config.kafka and sessions is not None:
        kafka = KafkaCommands(
            config.kafka,
            partitions=config.kafka_partitions,
            generation=config.kafka_generation,
        )
        await kafka.start()
        stack.push_async_callback(kafka.close)
        pusher = KafkaFriendPusher(protocol, sessions, kafka)
    else:
        logger.info("friend kafka or player_redis unconfigured: S2C push disabled, clients pull lists")

    store = SqlFriendStore(
        pool,
        protocol,
        data_service,
        config.to_rules(),
        cache_invalidator=cache_invalidator,
    )
    if config.bootstrap_schema:
        await store.bootstrap()
    await store.verify_schema()

    gate_verifier = verifier if verifier is not None else CallerVerifier.from_env(allowed_callers=("gate",))
    return FriendService(
        protocol,
        store,
        gate_verifier,
        coordinator_secret=config.coordinator_secret,
        sessions=sessions,
        quota=quota,
        sync_digest=sync_digest,
        pusher=pusher,
        request_budget_sec=config.request_budget_sec,
    )


async def run(settings: Settings) -> None:
    """运行 friend 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    protocol = Protocol()
    config = FriendConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=FRIEND_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=int(time.time()),
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )

            service = await open_service(config, protocol, stack)

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
            grace_sec = max(3.0, config.rpc_timeout_sec)
            stack.push_async_callback(server.stop, grace_sec)

            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            # AsyncExitStack 按 LIFO 执行：先注销 etcd，再排空在途 gRPC，最后关闭连接池与外部客户端。
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="friend-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "friend ready node_id=%d zone=%d grpc=%s:%d ttl=%d kafka=%s",
                config.node_id,
                config.zone_id,
                config.advertise_host,
                port,
                config.lease_ttl_sec,
                bool(config.kafka),
            )
            done, _ = await asyncio.wait(
                {keepalive, asyncio.ensure_future(stop.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if keepalive in done:
                keepalive.result()
    finally:
        signal.signal(signal.SIGTERM, previous)
