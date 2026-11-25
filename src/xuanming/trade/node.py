"""trade 节点生命周期：配置校验 -> 建池验表 -> 装配 gRPC/资产与合同后台循环 -> 注册 etcd(ttl=60) -> 优雅下线。"""

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
from ..login.caller_auth import CallerVerifier
from ..protocol import Protocol
from ..settings import Settings
from .asset_loop import (
    AssetOpCaller,
    AssetOpLoop,
    GrpcDataServiceLedgerReader,
    RedisEtcdSceneResolver,
)
from .commerce_engine import CommerceEngine
from .config import TradeConfig
from .economy_credit import (
    DirectoryOwnershipLookup,
    PlayerTradeGuard,
    SellerCreditService,
    StoreDailyPlayerTradeTracker,
    StoreOwnershipLookup,
    StoreReferencePriceReader,
)
from .mall_adapter import MallAdapter, MallPoller
from .model import MergedZones, TRADE_NODE_TYPE
from .rmb import RmbConsignments
from .role_trade import GrpcContentGuardChecker, GrpcRoleSealer, GrpcRoleTradeDirectory, RmbIdentity
from .service import TradeService
from .sql_store import SqlTradeStore, open_pool
from .store import (
    GrpcHomeZoneDirectory,
    GrpcMergedZonesDirectory,
    GrpcRoleFenceDirectory,
    IdSegmentAllocator,
)

logger = logging.getLogger(__name__)


async def close_pool(pool) -> None:
    pool.close()
    await pool.wait_closed()


async def _run_commerce_loop(engine: CommerceEngine, interval_sec: float, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await engine.tick()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error("trade commerce engine tick failed", exc_info=True)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
            return
        except asyncio.TimeoutError:
            pass


async def _run_mall_loop(
    poller: MallPoller | None,
    rmb: RmbConsignments | None,
    interval_sec: float,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        if poller is not None:
            try:
                await poller.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("trade mall poller tick failed", exc_info=True)
        if rmb is not None:
            try:
                await rmb.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("trade rmb poller tick failed", exc_info=True)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
            return
        except asyncio.TimeoutError:
            pass


async def _run_merged_zones_loop(
    directory: GrpcMergedZonesDirectory,
    holder: list[MergedZones],
    interval_sec: float,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
            return
        except asyncio.TimeoutError:
            pass
        try:
            holder[0] = await directory.get_merged_zones()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("trade merged zones refresh failed", exc_info=True)


async def open_service(
    config: TradeConfig,
    protocol: Protocol,
    stack: AsyncExitStack,
    *,
    etcd_directory: EtcdDirectory | None = None,
    environ=None,
) -> tuple[TradeService, SqlTradeStore, AssetOpLoop | None, CommerceEngine | None, GrpcMergedZonesDirectory, list[MergedZones]]:
    """打开独占库连接池、验表并装配 TradeService、CommerceEngine 与 AssetOpLoop（canary 停用后台循环）。"""
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

    role_directory = GrpcRoleFenceDirectory(protocol, ds_client.call, config.role_fence_secret)
    home_zones = GrpcHomeZoneDirectory(protocol, ds_client.call)
    merged_zones_dir = GrpcMergedZonesDirectory(protocol, ds_client.call)
    listing_ids = IdSegmentAllocator(protocol, ds_client.call, "trade_listing")
    op_ids = IdSegmentAllocator(protocol, ds_client.call, "trade_asset_op")

    store = SqlTradeStore(pool, protocol, role_directory=role_directory, id_allocator=listing_ids)
    if config.bootstrap_schema:
        await store.bootstrap()
    await store.verify_schema()

    try:
        initial_zones = await merged_zones_dir.get_merged_zones()
    except Exception:
        logger.warning("trade initial merged zones fetch failed, starting with identity map", exc_info=True)
        initial_zones = MergedZones()
    zones_holder = [initial_zones]

    asset_loop: AssetOpLoop | None = None
    asset_caller: AssetOpCaller | None = None
    ledger_reader: GrpcDataServiceLedgerReader | None = None
    if config.asset_op_enabled and config.redis and etcd_directory is not None:
        from redis.asyncio import Redis

        redis_client = Redis.from_url(config.redis, decode_responses=False)
        stack.push_async_callback(redis_client.aclose)
        resolver = RedisEtcdSceneResolver(protocol, redis_client, etcd_directory)
        stack.push_async_callback(resolver.close)
        asset_caller = AssetOpCaller(protocol, resolver, config.asset_op_secret)
        ledger_reader = GrpcDataServiceLedgerReader(protocol, ds_client.call)
        if config.runs_background_work:
            asset_loop = AssetOpLoop(
                protocol,
                store,
                asset_caller,
                ledger_reader,
                interval_sec=config.asset_loop_interval_sec,
                batch=config.asset_loop_batch,
                workers=config.asset_loop_workers,
            )

    commerce_engine: CommerceEngine | None = None
    mall_adapter: MallAdapter | None = None
    mall_poller: MallPoller | None = None
    rmb_consignments: RmbConsignments | None = None
    rmb_identity: RmbIdentity | None = None
    content_guard_checker: GrpcContentGuardChecker | None = None
    reference_prices = StoreReferencePriceReader(store)
    daily_outflow = StoreDailyPlayerTradeTracker(store)
    seller_credits: SellerCreditService | None = SellerCreditService(
        store=store,
        ownership=StoreOwnershipLookup(store),
        policy=config.seller_credit,
    )

    if config.content_guard:
        cg_client = GrpcClient(config.content_guard, timeout=config.request_budget_sec)
        stack.push_async_callback(cg_client.close)
        content_guard_checker = GrpcContentGuardChecker(protocol, cg_client.call)

    if config.asset_op_enabled:
        player_trade_guard = PlayerTradeGuard(
            policy=config.player_trade,
            prices=reference_prices,
            daily_out=daily_outflow,
        )
        commerce_engine = CommerceEngine(
            protocol,
            store,
            commerce_ids=listing_ids,
            op_ids=op_ids,
            policy=config.policy,
            asset_caller=asset_caller,
            ledger_reader=ledger_reader,
            credits=seller_credits,
            player_trade_guard=player_trade_guard,
        )
        if config.mall_url:
            mall_adapter = MallAdapter(
                protocol,
                config.mall_url,
                config.mall_gateway_secret,
                config.mall_delivery_secret,
            )
            stack.push_async_callback(mall_adapter.close)
            mall_poller = MallPoller(mall_adapter, commerce_engine)
        if len(config.account_directory_secret) >= 32:
            role_trade_dir = GrpcRoleTradeDirectory(protocol, ds_client.call, config.account_directory_secret)
            rmb_identity = RmbIdentity(role_trade_dir)
            seller_credits = SellerCreditService(
                store=store,
                ownership=DirectoryOwnershipLookup(role_trade_dir),
                policy=config.seller_credit,
            )
            commerce_engine.credits = seller_credits
            if config.rmb_enabled and mall_adapter is not None:
                sealer = GrpcRoleSealer(protocol, etcd_directory, config.account_directory_secret)
                stack.push_async_callback(sealer.close)
                rmb_consignments = RmbConsignments(
                    protocol,
                    commerce_engine,
                    mall_adapter,
                    role_trade_dir,
                    sealer,
                    credits=seller_credits,
                    currency_types=config.rmb_currencies,
                )
                commerce_engine.rmb = rmb_consignments

    caller_verifier = CallerVerifier.from_env(environ, allowed_callers=("gate",))
    service = TradeService(
        protocol,
        store,
        caller_verifier=caller_verifier,
        coordinator_secret=config.coordinator_secret,
        home_zones=home_zones,
        listing_ids=listing_ids,
        commerce=commerce_engine,
        market=config.market,
        zones_provider=lambda: zones_holder[0],
        mode=config.mode,
        mall_url=config.mall_url,
        rmb_enabled=config.rmb_enabled,
        mall=mall_adapter,
        rmb=rmb_consignments,
        rmb_identity=rmb_identity,
        content_guard=content_guard_checker,
        credits=seller_credits,
        reference_prices=reference_prices,
        player_trade_policy=config.player_trade,
        daily_outflow=daily_outflow,
        manual_resolution_secret=config.manual_resolution_secret,
        manual_resolution_environment=config.manual_resolution_environment,
        request_budget_sec=config.request_budget_sec,
    )
    service.mall_poller = mall_poller
    return service, store, asset_loop, commerce_engine, merged_zones_dir, zones_holder


async def run(settings: Settings) -> None:
    """运行 trade 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    protocol = Protocol()
    config = TradeConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=TRADE_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=int(time.time()),
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )
            scene_directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            stack.push_async_callback(scene_directory.close)

            service, _store, asset_loop, commerce_engine, merged_zones_dir, zones_holder = await open_service(
                config, protocol, stack, etcd_directory=scene_directory
            )

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

            if config.runs_background_work:
                if asset_loop is not None:
                    t_asset = asyncio.create_task(asset_loop.run(stop_event=stop), name="trade-asset-loop")
                    stack.callback(t_asset.cancel)
                if commerce_engine is not None:
                    t_comm = asyncio.create_task(
                        _run_commerce_loop(commerce_engine, config.commerce_tick_interval_sec, stop),
                        name="trade-commerce-loop",
                    )
                    stack.callback(t_comm.cancel)
                    mall_poller = getattr(service, "mall_poller", None)
                    if mall_poller is not None or commerce_engine.rmb is not None:
                        t_mall = asyncio.create_task(
                            _run_mall_loop(mall_poller, commerce_engine.rmb, config.mall_poll_interval_sec, stop),
                            name="trade-mall-loop",
                        )
                        stack.callback(t_mall.cancel)
                t_zones = asyncio.create_task(
                    _run_merged_zones_loop(merged_zones_dir, zones_holder, config.merged_zones_refresh_sec, stop),
                    name="trade-merged-zones-loop",
                )
                stack.callback(t_zones.cancel)
            else:
                logger.info(
                    "trade release_track=%s: background loops disabled (canary serves RPCs only)",
                    config.release_track,
                )

            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(directory.maintain(info), name="trade-etcd-lease")
            stack.callback(keepalive.cancel)

            logger.info(
                "trade ready node_id=%d zone=%d grpc=%s:%d ttl=%d track=%s",
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
