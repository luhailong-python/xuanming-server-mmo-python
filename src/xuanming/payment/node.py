"""payment 节点生命周期：配置校验 -> 建池验表 -> 装配 gRPC/回调 HTTP/后台工作器 -> 注册 etcd(ttl=60) -> 优雅下线。"""

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
from ..login.caller_auth import CallerVerifier
from ..protocol import Protocol
from ..settings import Settings
from .channels import build_channels
from .config import DEFAULT_MALL_TIMEOUT_MS, PaymentConfig
from .mall_client import HttpMallClient
from .model import PAYMENT_NODE_TYPE, PaymentOptions
from .notify_http import PaymentNotifyServer
from .service import PaymentCore, PaymentGrpcService
from .sql_store import SqlPaymentStore, open_pool
from .workers import PaymentWorkers

logger = logging.getLogger(__name__)


async def _close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def open_service(
    config: PaymentConfig,
    protocol: Protocol,
    stack: AsyncExitStack,
    *,
    environ=None,
) -> tuple[PaymentGrpcService, PaymentCore, PaymentNotifyServer, PaymentWorkers]:
    """打开独占库连接池、验表并装配支付核心、gRPC 适配、回调 HTTP 与后台工作器。"""
    pool = await open_pool(
        host=config.mysql_host,
        port=config.mysql_port,
        user=config.mysql_user,
        password=config.mysql_password,
        database=config.mysql_database,
        maxsize=config.max_open_conn,
        minsize=config.min_open_conn,
    )
    stack.push_async_callback(_close_pool, pool)

    store = SqlPaymentStore(pool)
    if config.bootstrap_schema:
        await store.bootstrap()
    await store.verify_schema()

    mall_timeout_sec = (config.mall_timeout_ms or DEFAULT_MALL_TIMEOUT_MS) / 1000.0
    mall_client = HttpMallClient(
        config.mall_base_url,
        config.mall_payment_subject,
        config.mall_payment_secret,
        timeout_sec=mall_timeout_sec,
    )
    stack.push_async_callback(mall_client.close)

    channels = build_channels(config)
    options = PaymentOptions(
        products=config.products,
        test_policies=config.test_policies,
    )
    core = PaymentCore(
        store,
        mall_client,
        channels,
        config.platform_channel_ids(),
        options=options,
    )
    caller_verifier = CallerVerifier.from_env(environ, allowed_callers=("gate",))
    grpc_service = PaymentGrpcService(protocol, core, caller_verifier=caller_verifier)
    notify_server = PaymentNotifyServer(core, host=config.notify_host, port=config.notify_port)
    workers = PaymentWorkers(core)
    return grpc_service, core, notify_server, workers


async def run(settings: Settings) -> None:
    """运行 payment 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    protocol = Protocol()
    config = PaymentConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=PAYMENT_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=int(time.time()),
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )

            grpc_service, _core, notify_server, workers = await open_service(config, protocol, stack)

            kill_switch = KillSwitch(protocol, info)
            etcd_ks = EtcdKillSwitch(protocol, settings.etcd, kill_switch)
            etcd_ks.start()
            stack.push_async_callback(etcd_ks.close)

            notify_port = await notify_server.start()
            stack.push_async_callback(notify_server.close)

            worker_task: asyncio.Task | None = None
            if config.runs_background_work:
                worker_task = asyncio.create_task(workers.run(stop), name="payment-workers")
                stack.callback(worker_task.cancel)
            else:
                logger.info(
                    "payment release_track=%s: background workers disabled (canary serves RPCs and webhooks only)",
                    config.release_track,
                )

            server, grpc_port = await serve_grpc(
                protocol,
                f"{config.host}:{config.grpc_port}",
                grpc_service.services(),
                kill_switch=kill_switch,
                pass_context=True,
            )
            grace_sec = max(3.0, config.request_budget_sec)
            stack.push_async_callback(server.stop, grace_sec)

            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = grpc_port
            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="payment-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "payment ready node_id=%d zone=%d grpc=%s:%d notify=%s:%d ttl=%d track=%s",
                config.node_id,
                config.zone_id,
                config.advertise_host,
                grpc_port,
                config.notify_host,
                notify_port,
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
