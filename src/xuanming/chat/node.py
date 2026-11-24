"""chat 节点生命周期：配置校验 -> 建池验表核策略 -> 启动 gRPC -> 注册 etcd(ttl=60) -> 优雅下线。"""

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
from .config import ChatConfig
from .model import CHAT_NODE_TYPE
from .service import ChatService
from .sql_store import SqlChatStore, open_pool
from .store import (
    ContentReviewer,
    GrpcActorVerifier,
    GrpcContentReviewer,
    GrpcProfileReader,
    KafkaWorldPusher,
    PassthroughContentReviewer,
    WorldPusher,
)

logger = logging.getLogger(__name__)


async def close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def open_service(
    config: ChatConfig,
    protocol: Protocol,
    stack: AsyncExitStack,
    *,
    verifier: CallerVerifier | None = None,
) -> ChatService:
    """打开独占库连接池、验表核策略并装配 ``ChatService``（无 Redis 依赖）。"""
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
    actor_verifier = GrpcActorVerifier(
        protocol,
        ds_client.call,
        config.role_fence_secret,
        ttl_sec=float(config.actor_verify_ttl_seconds),
    )
    profile_reader = GrpcProfileReader(
        protocol,
        ds_client.call,
        ttl_sec=float(config.actor_verify_ttl_seconds),
    )

    reviewer: ContentReviewer
    if config.content_guard:
        cg_client = GrpcClient(config.content_guard, timeout=max(3.0, config.content_guard_timeout_ms / 1000.0))
        stack.push_async_callback(cg_client.close)
        if config.content_guard_optional:
            logger.error(
                "chat running with content_guard_optional=true: text passes unchecked when content_guard is unreachable"
            )
        reviewer = GrpcContentReviewer(
            protocol,
            cg_client.call,
            timeout_sec=config.content_guard_timeout_ms / 1000.0,
            optional=config.content_guard_optional,
        )
    else:
        logger.error("chat has no content_guard target and content_guard_optional=true: local dev passthrough enabled")
        reviewer = PassthroughContentReviewer()

    world_pusher: WorldPusher | None = None
    if config.kafka:
        kafka = KafkaCommands(
            config.kafka,
            partitions=config.kafka_partitions,
            generation=config.kafka_generation,
        )
        await kafka.start()
        stack.push_async_callback(kafka.close)
        world_pusher = KafkaWorldPusher(protocol, kafka.producer, generation=config.kafka_generation)
    else:
        logger.info("chat kafka brokers empty: world channel push disabled, clients pull history")

    store = SqlChatStore(
        pool,
        protocol,
        actor_verifier,
        config.to_rules(),
        reviewer=reviewer,
        profile_reader=profile_reader,
    )
    stack.push_async_callback(store.close)
    if config.bootstrap_schema:
        await store.bootstrap()
    await store.verify_schema()
    await store.ensure_policy()

    gate_verifier = verifier if verifier is not None else CallerVerifier.from_env(allowed_callers=("gate",))
    service = ChatService(
        protocol,
        store,
        gate_verifier,
        world_pusher=world_pusher,
        platform_reviewer=reviewer,
    )
    stack.push_async_callback(service.close)
    return service


async def run(settings: Settings) -> None:
    """运行 chat 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    protocol = Protocol()
    config = ChatConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=CHAT_NODE_TYPE,
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
            grace_sec = max(3.0, config.request_budget_sec)
            stack.push_async_callback(server.stop, grace_sec)

            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            # AsyncExitStack 按 LIFO 执行：先注销 etcd，再排空在途 gRPC，最后关闭连接池与客户端。
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="chat-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "chat ready node_id=%d zone=%d grpc=%s:%d ttl=%d kafka=%s content_guard=%s",
                config.node_id,
                config.zone_id,
                config.advertise_host,
                port,
                config.lease_ttl_sec,
                bool(config.kafka),
                bool(config.content_guard),
            )
            done, _ = await asyncio.wait(
                {keepalive, asyncio.ensure_future(stop.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if keepalive in done:
                keepalive.result()
    finally:
        signal.signal(signal.SIGTERM, previous)
