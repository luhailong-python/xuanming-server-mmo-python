"""scene_manager 节点生命周期：配置校验 -> 两域 Redis/DataService/Kafka/etcd 连接 -> 世界初始化 -> gRPC 服务 -> 优雅下线。"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from contextlib import AsyncExitStack

from ..cluster import EtcdDirectory
from ..grpc_runtime import GrpcClient, serve_grpc
from ..kill_switch import EtcdKillSwitch, KillSwitch
from ..protocol import Protocol
from ..settings import Settings, secret as require_secret
from .channel_directory import WorldChannelDirectoryPublisher
from .config import (
    ROLE_DIRECTORY_SECRET_ENV,
    SCENE_MANAGER_NODE_TYPE,
    SceneManagerConfig,
)
from .dead_holder import DeadHolderReleaser
from .service import (
    EtcdNodeRegistry,
    GrpcDataServiceClient,
    GrpcSceneNodeClient,
    KafkaCommandPublisher,
    SceneManagerService,
)
from .store import (
    RedisPlayerPlacementStore,
    RedisSceneControlStore,
    open_redis,
)
from .sweepers import DeadNodeSweeper, IdleInstanceSweeper, TeardownJobSweeper

logger = logging.getLogger(__name__)


async def _close_redis(client) -> None:
    close = getattr(client, "aclose", None) or getattr(client, "close", None)
    if close is not None:
        result = close()
        if asyncio.iscoroutine(result):
            await result


async def _cancel_task(task: asyncio.Task) -> None:
    if not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def _start_periodic_task(stack: AsyncExitStack, coro, *, name: str) -> asyncio.Task:
    task = asyncio.create_task(coro, name=name)
    stack.push_async_callback(_cancel_task, task)
    return task


async def _node_sync_loop(
    registry: EtcdNodeRegistry,
    service: SceneManagerService,
    interval_sec: float,
    stop: asyncio.Event,
) -> None:
    """周期刷新本 zone Scene 节点列表并同步到控制域负载集，新节点上线时补建缺失大世界频道。"""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_sec)
            return
        except asyncio.TimeoutError:
            pass
        try:
            await registry.sync_zone()
            await service.sync_nodes_to_control_store()
        except Exception as exc:
            logger.warning("scene_manager periodic node sync failed zone=%d error=%s", service.config.zone_id, exc)


async def run(settings: Settings) -> None:
    cfg = SceneManagerConfig.from_settings(settings)
    if not settings.etcd:
        raise ValueError("scene_manager 需要配置 etcd 地址")
    if not settings.redis:
        raise ValueError("scene_manager 需要配置玩家域 Redis 地址")
    control_redis_url = settings.scene_control_redis or settings.redis
    if settings.redis_cluster and not settings.scene_control_redis:
        raise ValueError("Redis Cluster 模式下 scene_manager 必须分别配置 scene_control_redis 与 redis（玩家域）")
    if not settings.data_service:
        raise ValueError("scene_manager 需要配置 DataService 地址")
    role_secret = require_secret(ROLE_DIRECTORY_SECRET_ENV)

    protocol = Protocol()
    node_uuid = str(uuid.uuid4())
    info = protocol.message(
        "NodeInfo",
        node_id=cfg.node_id,
        node_type=SCENE_MANAGER_NODE_TYPE,
        zone_id=cfg.zone_id,
        launch_time=int(time.time()),
        node_uuid=node_uuid,
        physical_region=cfg.physical_region,
        release_track=cfg.release_track,
        protocol_type=1,
    )
    kill_switch = KillSwitch(protocol, info)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            # 严格两域分离：控制域与玩家域各自持有独立 Redis 客户端
            control_redis = open_redis(control_redis_url, cluster=settings.redis_cluster)
            stack.push_async_callback(_close_redis, control_redis)
            await control_redis.ping()
            control_store = RedisSceneControlStore(control_redis)

            player_redis = open_redis(settings.redis, cluster=settings.redis_cluster)
            stack.push_async_callback(_close_redis, player_redis)
            await player_redis.ping()
            player_store = RedisPlayerPlacementStore(player_redis)

            data_grpc = GrpcClient(settings.data_service, timeout=max(cfg.role_directory_timeout_sec, cfg.home_zone_lookup_timeout_sec))
            stack.push_async_callback(data_grpc.close)
            data_service = GrpcDataServiceClient(
                protocol,
                data_grpc,
                role_secret,
                role_timeout_sec=cfg.role_directory_timeout_sec,
                home_zone_timeout_sec=cfg.home_zone_lookup_timeout_sec,
            )

            publisher = None
            if settings.kafka:
                publisher = KafkaCommandPublisher(
                    settings.kafka,
                    partitions=settings.kafka_partitions,
                    generation=settings.kafka_generation,
                    write_timeout_sec=cfg.kafka_write_timeout_sec,
                )
                await publisher.start()
                stack.push_async_callback(publisher.close)

            etcd_ks = EtcdKillSwitch(protocol, settings.etcd, kill_switch)
            etcd_ks.start()
            stack.push_async_callback(etcd_ks.close)

            directory = EtcdDirectory(protocol, settings.etcd, ttl=cfg.etcd_lease_ttl_sec)
            stack.push_async_callback(directory.close)
            registry = EtcdNodeRegistry(directory, own_zone_id=cfg.zone_id)

            scene_node_client = GrpcSceneNodeClient(
                protocol,
                registry,
                own_zone_id=cfg.zone_id,
                timeout_sec=cfg.scene_node_rpc_timeout_sec,
            )
            stack.push_async_callback(scene_node_client.close)

            dead_holder_releaser = (
                DeadHolderReleaser(data_service, registry, zone_id=cfg.zone_id)
                if cfg.enable_dead_holder_release
                else None
            )

            service = SceneManagerService(
                protocol,
                cfg,
                control_store=control_store,
                player_store=player_store,
                data_service=data_service,
                scene_nodes=scene_node_client,
                node_registry=registry,
                publisher=publisher,
                dead_holder_releaser=dead_holder_releaser,
            )
            stack.push_async_callback(service.close)

            # 启动期同步节点并按 World.json 自动初始化大世界频道
            await registry.sync_zone()
            await service.sync_nodes_to_control_store()
            await service.init_world_scenes(cfg.zone_id, wait_for_lock=False)

            _start_periodic_task(
                stack,
                _node_sync_loop(registry, service, cfg.node_sync_interval_sec, stop),
                name="scene-manager-node-sync",
            )
            if cfg.effective_channel_directory_refresh_sec() > 0:
                channel_publisher = WorldChannelDirectoryPublisher(
                    protocol, cfg, control_store=control_store, node_registry=registry,
                )
                _start_periodic_task(stack, channel_publisher.run(stop), name="scene-manager-channel-directory")
            if cfg.instance_idle_timeout_sec > 0 or cfg.effective_mirror_idle_timeout_sec() > 0:
                idle_sweeper = IdleInstanceSweeper(service)
                _start_periodic_task(stack, idle_sweeper.run(stop), name="scene-manager-idle-sweeper")
            if cfg.teardown_sweep_interval_sec > 0:
                teardown_sweeper = TeardownJobSweeper(service)
                _start_periodic_task(stack, teardown_sweeper.run(stop), name="scene-manager-teardown-sweeper")
            if cfg.dead_node_sweep_interval_sec > 0:
                dead_node_sweeper = DeadNodeSweeper(service)
                _start_periodic_task(stack, dead_node_sweeper.run(stop), name="scene-manager-dead-node-sweeper")

            server, port = await serve_grpc(
                protocol,
                f"{cfg.host}:{cfg.grpc_port}",
                service.services(),
                kill_switch=kill_switch,
                pass_context=True,
            )
            stack.push_async_callback(server.stop, max(3.0, cfg.scene_node_rpc_timeout_sec))

            info.grpc_endpoint.ip = info.endpoint.ip = cfg.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="scene-manager-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "scene_manager ready node_id=%d zone=%d track=%s grpc=%s:%d",
                cfg.node_id, cfg.zone_id, cfg.release_track, cfg.advertise_host, port,
            )
            done, _ = await asyncio.wait(
                {keepalive, asyncio.ensure_future(stop.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if keepalive in done:
                keepalive.result()
    finally:
        signal.signal(signal.SIGTERM, previous)
