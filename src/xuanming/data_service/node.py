"""data_service node: SQL store + gRPC server + etcd registration (Go ``data_service.go`` startup order)."""

import asyncio
import logging
import signal
import time
import uuid
from contextlib import AsyncExitStack

from ..cluster import EtcdDirectory
from ..grpc_runtime import serve_grpc
from ..protocol import Protocol
from ..settings import Settings
from .config import DataServiceConfig
from .service import DataService
from .sql_store import SqlStore, open_pool

logger = logging.getLogger(__name__)

DATA_SERVICE_NODE = 26  # base.ENodeType DataServiceNodeService


async def close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def open_service(config: DataServiceConfig, protocol: Protocol, stack: AsyncExitStack) -> DataService:
    """Pools -> DDL -> merged-zone first read (fatal) -> handlers. Nothing is listening yet."""
    credentials = dict(host=config.mysql_host, port=config.mysql_port, user=config.mysql_user,
                       password=config.mysql_password)
    pool = await open_pool(**credentials, database=config.mysql_database, maxsize=config.max_open_conn)
    stores = {storage: await open_pool(**credentials, database=f"player_store_{storage}_db", maxsize=2)
              for storage in config.storage_ids}  # placement.StoreDBName; also the storage allow-list
    for item in (pool, *stores.values()):
        stack.push_async_callback(close_pool, item)
    store = SqlStore(pool, protocol, max_roles=config.max_players_per_account, storage_pools=stores)
    await store.bootstrap()
    service = DataService(protocol, store, config.secrets, id_tags=config.id_tags,
                          zone_refresh=config.merged_zone_refresh_sec)
    await service.start()
    stack.push_async_callback(service.close)
    return service


async def run(settings: Settings) -> None:
    config = DataServiceConfig.from_settings(settings)
    protocol = Protocol()
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            service = await open_service(config, protocol, stack)
            server, port = await serve_grpc(protocol, f"{config.host}:{config.grpc_port}", service.services(),
                                            pass_context=True)
            stack.push_async_callback(server.stop, 3)
            # Registration comes last and is fatal, like Go: no etcd entry means scene never finds the service.
            info = protocol.message(
                "NodeInfo", node_id=config.node_id, node_type=DATA_SERVICE_NODE, zone_id=settings.zone_id,
                launch_time=int(time.time()), node_uuid=str(uuid.uuid4()), physical_region=settings.physical_region,
                release_track=settings.release_track, protocol_type=1)
            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            directory = EtcdDirectory(protocol, settings.etcd)
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="etcd-lease")
            stack.callback(keepalive.cancel)
            logger.info("data_service 就绪 node_id=%d zone=%d grpc=%s:%d", config.node_id, settings.zone_id,
                        config.advertise_host, port)
            done, _ = await asyncio.wait({keepalive, asyncio.ensure_future(stop.wait())},
                                         return_when=asyncio.FIRST_COMPLETED)
            if keepalive in done:
                keepalive.result()  # lease lost: exit non-zero so the supervisor restarts the node
    finally:
        signal.signal(signal.SIGTERM, previous)
