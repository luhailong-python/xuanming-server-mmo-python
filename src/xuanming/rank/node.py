"""rank 节点生命周期：配置校验 -> 建池验表 -> 启动 gRPC 与奖励工作器 -> 注册 etcd(ttl=60) -> 优雅下线。"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from collections.abc import Callable
from contextlib import AsyncExitStack

from ..cluster import EtcdDirectory
from ..data.config_tables import ConfigTables
from ..data.id_segments import IdSegments
from ..grpc_runtime import GrpcClient, serve_grpc
from ..kill_switch import EtcdKillSwitch, KillSwitch
from ..login.caller_auth import CallerVerifier
from ..protocol import Protocol
from ..settings import Settings
from .config import RankConfig
from .model import RANK_NODE_TYPE
from .reward_worker import GrpcMailAdminClient, RewardWorker
from .service import GrpcNameReader, RankService
from .sql_store import SqlRankStore, open_pool

logger = logging.getLogger(__name__)


async def close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


def _default_item_exists() -> Callable[[int], bool]:
    item_table = ConfigTables()["Item"]
    return lambda item_id: item_table.get(item_id) is not None


async def open_service(
    config: RankConfig,
    protocol: Protocol,
    stack: AsyncExitStack,
    *,
    caller_verifier: CallerVerifier | None = None,
    item_exists: Callable[[int], bool] | None = None,
) -> tuple[RankService, RewardWorker]:
    """打开独占库连接池、验表并装配 ``RankService`` 与 ``RewardWorker``（尚未监听端口）。"""
    verifier = caller_verifier or CallerVerifier.from_env(allowed_callers=("gate",))
    check_item = item_exists or _default_item_exists()

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

    ds_client = GrpcClient(config.data_service, timeout=2.0)
    stack.push_async_callback(ds_client.close)

    mail_client = GrpcClient(config.mail_service, timeout=1.8)
    stack.push_async_callback(mail_client.close)

    store = SqlRankStore(
        pool,
        protocol,
        next_board_id=IdSegments.remote(protocol, ds_client.call, "rank_board"),
        next_settlement_id=IdSegments.remote(protocol, ds_client.call, "rank_settlement"),
        next_reward_id=IdSegments.remote(protocol, ds_client.call, "rank_reward"),
        item_exists=check_item,
    )
    store.configure_reward_mail_window(config.reward_mail_window_ms())
    if config.bootstrap_schema:
        await store.bootstrap()
    await store.verify_schema()

    service = RankService(
        protocol,
        store,
        producer_secrets=config.producer_secrets_dict(),
        caller_verifier=verifier,
        name_reader=GrpcNameReader(protocol, ds_client),
        request_budget_sec=config.request_budget_sec,
    )
    worker = RewardWorker(
        protocol,
        store,
        GrpcMailAdminClient(protocol, mail_client),
        mail_secret=config.mail_secret,
    )
    return service, worker


async def _stop_worker(task: asyncio.Task, stop_event: asyncio.Event) -> None:
    stop_event.set()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def run(settings: Settings) -> None:
    """运行 rank 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    protocol = Protocol()
    config = RankConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=RANK_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=int(time.time()),
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )

            service, worker = await open_service(config, protocol, stack)

            # canary 副本仅服务客户端 RPC，不运行全局奖励投递后台（与 Go releasetrack 一致）
            worker_stop = asyncio.Event()
            if config.release_track == "stable":
                worker_task = asyncio.create_task(worker.run(worker_stop), name="rank-reward-worker")
                stack.push_async_callback(_stop_worker, worker_task, worker_stop)
            else:
                logger.info("rank reward worker disabled release_track=%s", config.release_track)

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
            # AsyncExitStack 按 LIFO 执行：先注销 etcd，再排空在途 gRPC，再停奖励工作器，最后关连接池与依赖。
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="rank-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "rank ready node_id=%d zone=%d grpc=%s:%d ttl=%d release_track=%s",
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
