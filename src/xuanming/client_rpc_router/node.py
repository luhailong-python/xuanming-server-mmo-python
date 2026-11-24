"""client_rpc_router 节点生命周期：配置校验 -> 监听发现 -> gRPC 启动 -> etcd 注册 -> 优雅下线。"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from contextlib import AsyncExitStack

from ..cluster import EtcdDirectory
from ..grpc_runtime import serve_grpc
from ..kill_switch import EtcdKillSwitch, KillSwitch
from ..protocol import Protocol
from ..settings import Settings
from .breaker import EndpointCircuitBreaker
from .config import ROUTER_NODE_TYPE, RouterConfig
from .discovery import ChannelPool, EtcdTargetWatcher, TargetDirectory
from .routes import build_route_table, watched_node_types
from .service import ClientRpcRouterService

logger = logging.getLogger(__name__)


async def _idle_reaper(pool: ChannelPool, sweep_sec: float) -> None:
    while True:
        await asyncio.sleep(sweep_sec)
        try:
            await pool.sweep_idle()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("router idle channel sweep error", exc_info=True)


async def run(settings: Settings) -> None:
    protocol = Protocol()
    config = RouterConfig.from_settings(settings, protocol)
    if not settings.etcd:
        raise ValueError("client_rpc_router 需要配置 etcd 地址")

    node_uuid = str(uuid.uuid4())
    routes = build_route_table(protocol)
    targets = watched_node_types(routes)
    pool = ChannelPool(forward_timeout_ms=config.forward_timeout_ms, idle_timeout_sec=config.idle_conn_timeout_sec)
    breaker = EndpointCircuitBreaker(
        failure_threshold=config.breaker_failure_threshold,
        open_duration_ms=config.breaker_open_duration_ms,
    )

    def on_evict(endpoint: str) -> None:
        pool.evict(endpoint)
        breaker.remove(endpoint)

    directories: dict[int, TargetDirectory] = {
        node_type: TargetDirectory(name, on_evict=on_evict) for node_type, name in targets
    }
    peers_dir = TargetDirectory("ClientRpcRouterNodeService")

    info = protocol.message(
        "NodeInfo",
        node_id=config.node_id,
        node_type=ROUTER_NODE_TYPE,
        zone_id=config.zone_id,
        launch_time=int(time.time()),
        node_uuid=node_uuid,
        physical_region=config.physical_region,
        release_track=config.release_track,
        protocol_type=1,
    )
    kill_switch = KillSwitch(protocol, info)
    service = ClientRpcRouterService(
        protocol,
        config,
        directories,
        peers=peers_dir,
        pool=pool,
        breaker=breaker,
        kill_switch=kill_switch,
        routes=routes,
        self_uuid=node_uuid,
    )

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            watchers = [EtcdTargetWatcher(protocol, settings.etcd, directory) for directory in directories.values()]
            watchers.append(EtcdTargetWatcher(protocol, settings.etcd, peers_dir))
            for watcher in watchers:
                watcher.start()
                stack.push_async_callback(watcher.close)

            etcd_ks = EtcdKillSwitch(protocol, settings.etcd, kill_switch)
            etcd_ks.start()
            stack.push_async_callback(etcd_ks.close)

            reaper = asyncio.create_task(_idle_reaper(pool, config.idle_conn_sweep_sec), name="router-idle-reaper")
            stack.callback(reaper.cancel)
            stack.push_async_callback(pool.close)

            grace_sec = max(3.0, config.forward_timeout_ms / 1000.0)
            server, port = await serve_grpc(
                protocol,
                f"{config.host}:{config.grpc_port}",
                service.services(),
                kill_switch=kill_switch,
                pass_context=True,
            )
            stack.push_async_callback(server.stop, grace_sec)

            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            # 先注销 etcd 再排空在途请求（AsyncExitStack 按 LIFO 执行，故先注册后释放）
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="router-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "client_rpc_router ready node_id=%d zone=%d track=%s grpc=%s:%d targets=%d",
                config.node_id, config.zone_id, config.release_track, config.advertise_host, port, len(targets),
            )
            done, _ = await asyncio.wait(
                {keepalive, asyncio.ensure_future(stop.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if keepalive in done:
                keepalive.result()
    finally:
        signal.signal(signal.SIGTERM, previous)
