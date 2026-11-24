"""player_locator 节点生命周期：配置校验 -> Redis/Kafka/SceneManager 连接 -> gRPC 启动 -> etcd 注册 -> 优雅下线。"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from contextlib import AsyncExitStack

from ..cluster import EtcdDirectory, KafkaCommands
from ..grpc_runtime import serve_grpc
from ..kill_switch import EtcdKillSwitch, KillSwitch
from ..protocol import Protocol
from ..settings import Settings
from .config import PLAYER_LOCATOR_NODE_TYPE, PlayerLocatorConfig
from .gate_sweep import DirectoryLiveGateView, GateSweeper
from .lease_monitor import KafkaGatePublisher, LeaseMonitor, RoutingSceneManagerClient
from .service import PlayerLocatorService
from .store import RedisSessionStore, open_player_redis

logger = logging.getLogger(__name__)


async def _close_redis(client) -> None:
    close = getattr(client, "aclose", None) or getattr(client, "close", None)
    if close is not None:
        result = close()
        if asyncio.iscoroutine(result):
            await result


async def run(settings: Settings) -> None:
    config = PlayerLocatorConfig.from_settings(settings)
    if not settings.etcd:
        raise ValueError("player_locator 需要配置 etcd 地址")
    if not settings.redis:
        raise ValueError("player_locator 需要配置玩家域 Redis 地址")

    protocol = Protocol()
    node_uuid = str(uuid.uuid4())
    info = protocol.message(
        "NodeInfo",
        node_id=config.node_id,
        node_type=PLAYER_LOCATOR_NODE_TYPE,
        zone_id=config.zone_id,
        launch_time=int(time.time()),
        node_uuid=node_uuid,
        physical_region=config.physical_region,
        release_track=config.release_track,
        protocol_type=1,
    )
    kill_switch = KillSwitch(protocol, info)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            redis_client = open_player_redis(settings.redis, cluster=settings.redis_cluster)
            stack.push_async_callback(_close_redis, redis_client)
            await redis_client.ping()
            store = RedisSessionStore(redis_client)

            scene_manager = RoutingSceneManagerClient(
                protocol,
                default_endpoint=settings.scene_manager,
                zone_endpoints=config.scene_manager_endpoint_map(),
                timeout_sec=config.scene_manager_timeout_sec,
            )
            stack.push_async_callback(scene_manager.close)

            gate_publisher = None
            if settings.kafka:
                kafka = KafkaCommands(
                    settings.kafka,
                    partitions=settings.kafka_partitions,
                    generation=settings.kafka_generation,
                )
                await kafka.start()
                stack.push_async_callback(kafka.close)
                gate_publisher = KafkaGatePublisher(kafka)

            etcd_ks = EtcdKillSwitch(protocol, settings.etcd, kill_switch)
            etcd_ks.start()
            stack.push_async_callback(etcd_ks.close)

            monitor = LeaseMonitor(
                protocol,
                store,
                config,
                scene_manager=scene_manager,
                gate_publisher=gate_publisher,
            )
            service = PlayerLocatorService(
                protocol,
                store,
                config,
                scene_manager=scene_manager,
                wake_lease_monitor=monitor.wake,
            )

            monitor_task = asyncio.create_task(monitor.run(stop), name="player-locator-lease-monitor")
            stack.callback(monitor_task.cancel)

            server, port = await serve_grpc(
                protocol,
                f"{config.host}:{config.grpc_port}",
                service.services(),
                kill_switch=kill_switch,
                pass_context=True,
            )
            stack.push_async_callback(server.stop, max(3.0, config.scene_manager_timeout_sec))

            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.etcd_lease_ttl_sec)
            scene_manager.directory = directory
            # 先注销 etcd 再排空在途 RPC 与租约监控（AsyncExitStack 按 LIFO 逆序回收）
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="player-locator-etcd-lease")
            stack.callback(keepalive.cancel)

            if config.reconcile_interval_sec > 0:
                sweeper = GateSweeper(protocol, store, DirectoryLiveGateView(directory), config)
                sweeper_task = asyncio.create_task(sweeper.run(stop), name="player-locator-gate-sweeper")
                stack.callback(sweeper_task.cancel)

            logger.info(
                "player_locator ready node_id=%d zone=%d track=%s grpc=%s:%d",
                config.node_id, config.zone_id, config.release_track, config.advertise_host, port,
            )
            done, _ = await asyncio.wait(
                {keepalive, asyncio.ensure_future(stop.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if keepalive in done:
                keepalive.result()
    finally:
        signal.signal(signal.SIGTERM, previous)
