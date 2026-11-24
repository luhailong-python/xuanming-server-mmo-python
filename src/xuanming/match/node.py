"""match（含 team）节点生命周期：建池验表 -> 装配 5 组 gRPC 服务 -> 双节点 etcd 注册 -> 优雅下线。

支持 release_track = stable | canary：
- stable：运行全部全局后台循环（按池撮合器、终局发件箱）；
- canary：禁用全局后台循环，仅响应客户端 RPC 与数据面补签。
"""

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
from .config import MatchConfig
from .gather import (
    EtcdBattleNodeResolver,
    GatherPipeline,
    GrpcEndpointPool,
    Matchmaker,
    MemoryPresenceProvider,
    StaticBattleNodeResolver,
)
from .match_service import MatchGrpcService, MatchServer
from .model import MATCH_NODE_TYPE, TEAM_NODE_TYPE
from .outbox import OutcomeOutboxWorker
from .spectate import SpectateManager
from .sql_store import SqlMatchStore, open_pool
from .store import MatchStore, MemoryMatchStore
from .team_service import KafkaTeamNotifier, NullTeamNotifier, TeamGrpcService

logger = logging.getLogger(__name__)


async def _close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def open_service(
    config: MatchConfig,
    protocol: Protocol,
    stack: AsyncExitStack,
    *,
    gate_verifier: CallerVerifier | None = None,
    store: MatchStore | None = None,
    etcd_directory: EtcdDirectory | None = None,
) -> MatchServer:
    """装配存储、发现、Kafka 通知、发件箱与全部 5 组 gRPC 服务（尚未监听端口）。"""
    if store is None:
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
        sql_store = SqlMatchStore(pool, protocol, node_id=config.node_id)
        if config.bootstrap_schema:
            await sql_store.bootstrap()
        await sql_store.verify_schema()
        store = sql_store

    rpc_pool = GrpcEndpointPool(default_timeout=config.request_budget_sec)
    stack.push_async_callback(rpc_pool.close)

    battle_nodes = EtcdBattleNodeResolver(etcd_directory) if etcd_directory is not None else StaticBattleNodeResolver()
    presence = MemoryPresenceProvider()

    kafka_commands: KafkaCommands | None = None
    publish_result = None
    team_notifier = NullTeamNotifier()
    if config.kafka:
        kafka_commands = KafkaCommands(
            config.kafka,
            partitions=config.kafka_partitions,
            generation=config.kafka_generation,
        )
        await kafka_commands.start()
        stack.push_async_callback(kafka_commands.close)
        team_notifier = KafkaTeamNotifier(protocol, presence, kafka_commands.send)

        async def _pub_result(battle_id: int, raw_payload: bytes) -> None:
            await kafka_commands.producer.send_and_wait(
                config.result_topic,
                value=raw_payload,
                key=str(battle_id).encode("utf-8"),
            )

        publish_result = _pub_result

    inbox_call = None
    if config.player_inbox:
        inbox_client = GrpcClient(config.player_inbox, timeout=3.0, enable_retries=False)
        stack.push_async_callback(inbox_client.close)
        inbox_call = inbox_client.call

    gather = GatherPipeline(protocol, store, config, rpc_pool, presence, battle_nodes)
    matchmaker = Matchmaker(protocol, store, config, gather, battle_nodes)
    outbox = OutcomeOutboxWorker(
        protocol,
        store,
        inbox_call=inbox_call,
        inbox_secret=config.player_inbox_secret,
        publish_result=publish_result,
        interval_sec=config.outbox_interval_sec,
    )
    spectate = SpectateManager(protocol, store, config, rpc_pool, presence)

    verifier = gate_verifier if gate_verifier is not None else CallerVerifier.from_env()
    team_svc = TeamGrpcService(
        protocol,
        store,
        config,
        gate_verifier=verifier,
        presence=presence,
        battle_nodes=battle_nodes,
        gather=gather,
        notifier=team_notifier,
    )
    match_svc = MatchGrpcService(
        protocol,
        store,
        config,
        gate_verifier=verifier,
        presence=presence,
        battle_nodes=battle_nodes,
        gather=gather,
        spectate=spectate,
    )
    server = MatchServer(
        protocol=protocol,
        store=store,
        config=config,
        match_service=match_svc,
        team_service=team_svc,
        gather=gather,
        matchmaker=matchmaker,
        outbox=outbox,
        spectate=spectate,
    )
    stack.push_async_callback(server.close)
    return server


async def run(settings: Settings) -> None:
    """运行 match（含 team）节点直至收到 SIGTERM 或任一 etcd 租约失效。"""
    protocol = Protocol()
    config = MatchConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            launch_ts = int(time.time())
            match_info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=MATCH_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=launch_ts,
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )
            team_info = protocol.message(
                "NodeInfo",
                node_id=config.team_node_id,
                node_type=TEAM_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=launch_ts,
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )

            discovery = EtcdDirectory(protocol, config.etcd, ttl=config.lease_ttl_sec) if config.etcd else None
            if discovery is not None:
                stack.push_async_callback(discovery.close)

            match_server = await open_service(config, protocol, stack, etcd_directory=discovery)

            kill_switch = KillSwitch(protocol, match_info)
            if config.etcd:
                etcd_ks = EtcdKillSwitch(protocol, config.etcd, kill_switch)
                etcd_ks.start()
                stack.push_async_callback(etcd_ks.close)

            grpc_server, port = await serve_grpc(
                protocol,
                f"{config.host}:{config.grpc_port}",
                match_server.services(),
                kill_switch=kill_switch,
                pass_context=True,
            )
            stack.push_async_callback(grpc_server.stop, max(3.0, config.request_budget_sec))

            bg_tasks: set[asyncio.Task] = set()
            if config.runs_global_background:
                matcher_task = asyncio.create_task(match_server.matchmaker.run_forever(stop), name="match-matcher-loop")
                outbox_task = asyncio.create_task(match_server.outbox.run_forever(stop), name="match-outbox-loop")
                stack.callback(matcher_task.cancel)
                stack.callback(outbox_task.cancel)
                bg_tasks.update((matcher_task, outbox_task))
            else:
                logger.info(
                    "match release_track=%s: global background loops disabled (canary serves RPCs only)",
                    config.release_track,
                )

            wait_set: set[asyncio.Task] = {asyncio.create_task(stop.wait(), name="match-stop-wait")}
            if config.etcd:
                # 双注册：先注册 TeamNodeService(10)，再注册 MatchNodeService(16)；退出时 LIFO 先注销 Match 再注销 Team
                for role_name, info in (("team", team_info), ("match", match_info)):
                    info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
                    info.grpc_endpoint.port = info.endpoint.port = port
                    reg_dir = EtcdDirectory(protocol, config.etcd, ttl=config.lease_ttl_sec)
                    stack.push_async_callback(reg_dir.close)
                    await reg_dir.register(info)
                    ka = asyncio.create_task(reg_dir.maintain(info), name=f"match-{role_name}-etcd-lease")
                    stack.callback(ka.cancel)
                    wait_set.add(ka)

            logger.info(
                "match service ready match_node=%d team_node=%d zone=%d track=%s grpc=%s:%d background=%s",
                config.node_id,
                config.team_node_id,
                config.zone_id,
                config.release_track,
                config.advertise_host,
                port,
                config.runs_global_background,
            )
            done, pending = await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)
            stop.set()
            for t in pending:
                t.cancel()
            for t in done:
                if not t.cancelled():
                    exc = t.exception()
                    if exc is not None:
                        raise exc
    finally:
        signal.signal(signal.SIGTERM, previous)
