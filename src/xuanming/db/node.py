"""db（player_store）节点生命周期：配置校验 -> 建池验表 -> 启动 gRPC -> 注册 etcd -> 优雅下线。"""

from __future__ import annotations

import asyncio
import inspect
import logging
import signal
import time
import uuid
from contextlib import AsyncExitStack

from ..cluster import EtcdDirectory
from ..grpc_runtime import GrpcClient, serve_grpc
from ..protocol import Protocol
from ..settings import Settings
from .config import DbConfig
from .model import MAX_SAVE_REQUEST_BYTES, PLAYER_STORE_NODE_TYPE
from .service import DataServiceRouteResolver, DbService
from .sql_store import SqlPlayerStore, open_storage_pool

logger = logging.getLogger(__name__)


async def close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def open_service(config: DbConfig, protocol: Protocol, stack: AsyncExitStack) -> DbService:
    """打开全部地域存档库连接池、验表并装配 ``DbService``（尚未监听端口）。"""
    pools: dict[int, object] = {}
    for storage_id in config.storage_ids:
        pool = await open_storage_pool(
            host=config.mysql_host,
            port=config.mysql_port,
            user=config.mysql_user,
            password=config.mysql_password,
            storage_id=storage_id,
            maxsize=config.max_open_conn,
            minsize=config.min_open_conn,
            lock_wait_timeout_sec=config.lock_wait_timeout_sec,
        )
        stack.push_async_callback(close_pool, pool)
        pools[storage_id] = pool

    store = SqlPlayerStore(
        pools,
        protocol,
        known_gen=config.known_gen,
        limits=config.limits,
        sql_timeout_sec=config.sql_timeout_sec,
    )
    if config.bootstrap_schema:
        await store.bootstrap_all()
    await store.verify_all_schemas()

    route_client = GrpcClient(config.data_service, timeout=config.route_timeout_sec)
    stack.push_async_callback(route_client.close)
    resolver = DataServiceRouteResolver(
        protocol,
        route_client.call,
        config.route_secret,
        timeout_sec=config.route_timeout_sec,
    )
    return DbService(protocol, store, resolver, config.scene_secret)


async def run(settings: Settings) -> None:
    """运行 db 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    config = DbConfig.from_settings(settings)
    protocol = Protocol()
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            service = await open_service(config, protocol, stack)
            server, port = await serve_grpc(
                protocol,
                f"{config.host}:{config.grpc_port}",
                service.services(),
                pass_context=True,
                max_receive_message_length=MAX_SAVE_REQUEST_BYTES,
            )
            grace_sec = max(3.0, config.sql_timeout_sec)
            stack.push_async_callback(server.stop, grace_sec)

            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=PLAYER_STORE_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=int(time.time()),
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )
            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port

            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            # AsyncExitStack 为 LIFO：先注销 etcd，再停止 gRPC 排空请求，最后关闭数据库连接池。
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="db-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "db ready node_id=%d zone=%d grpc=%s:%d storages=%s ttl=%d",
                config.node_id,
                config.zone_id,
                config.advertise_host,
                port,
                config.storage_ids,
                config.lease_ttl_sec,
            )
            done, _ = await asyncio.wait(
                {keepalive, asyncio.ensure_future(stop.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if keepalive in done:
                keepalive.result()
    finally:
        signal.signal(signal.SIGTERM, previous)
