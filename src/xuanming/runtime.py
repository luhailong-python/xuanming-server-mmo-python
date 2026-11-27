import asyncio
import logging
import os
import secrets
import time
import uuid
from contextlib import AsyncExitStack

from .cluster import BATTLE, GATE, LOGIN, SCENE, EtcdDirectory, KafkaCommands, LocalDirectory
from .data import ConfigTables, PlayerRowCodec, StoreRejected, StoreUnavailable, create_player_store
from .data.id_segments import IdSegments
from .data.navigation import load_navigation
from .grpc_runtime import GrpcClient, GrpcUnavailable, gate_signing_key, serve_grpc, sign_service_call
from .kill_switch import EtcdKillSwitch, KillSwitch
from .match_client import MatchClient
from .node_link import GateLinks, SceneLinks
from .nodes.battle import BattleNode
from .nodes.battle_agones import BattleAgones
from .nodes.battle_tcp import BattleClients
from .nodes.battle_settlement import BattleSettlement
from .nodes.gate import GateNode, RouterForwarder
from .nodes.gate_spectate import GateSpectate
from .nodes.monopoly import Monopoly
from .nodes.player_inbox import BATTLE as INBOX_BATTLE, InboxPeer, LocalInbox, SceneInbox
from .nodes.scene import SceneNode
from .nodes.scene_agones import SceneAgones
from .nodes.scene_admin import SceneAdmin
from .nodes.scene_activity import Activities
from .nodes.scene_afk_pass import SceneAfkPass
from .nodes.scene_appearance import SceneAppearance
from .nodes.scene_assets import SceneAssets
from .nodes.scene_attributes import SceneAttributes, initialize_attribute_schemes
from .nodes.scene_battle import SceneBattle
from .nodes.scene_connection_hold import afk_pass_key
from .nodes.scene_equip_craft import SceneEquipCraft
from .nodes.scene_equip_loadout import SceneEquipLoadout
from .nodes.scene_festival_beast import SceneFestivalBeast
from .nodes.scene_gm_commands import SceneGmCommands
from .nodes.scene_jianghu import SceneJianghu
from .nodes.scene_landing import SceneLanding
from .nodes.scene_mail_outbox import MailSender, SceneMailOutbox
from .nodes.scene_merchant import SceneMerchant, redis_key
from .nodes.scene_mission import Missions
from .nodes.scene_pets import ScenePets
from .nodes.scene_planned_drain import budget_from_env, planned_drain
from .nodes.scene_rename import SceneRename
from .nodes.scene_rewarded_ad import SceneRewardedAd
from .nodes.scene_season import SceneSeason
from .nodes.scene_skill_craft import SceneSkillCraft
from .nodes.scene_skills import SceneSkills, initialize_skill_list
from .nodes.scene_spirit_shop import SceneSpiritShop
from .nodes.scene_travel_pass import SceneTravelPass
from .nodes.scene_stats import initialize_base
from .nodes.scene_team import SceneTeam
from .nodes.scene_travel import create_scene_travel
from .nodes.scene_trial_journal import SceneTrialJournal
from .protocol import Protocol
from .settings import Settings, secret
from .transport.client_endpoint import ClientEndpointSettings

logger = logging.getLogger(__name__)
_ADMISSION_OPEN = 1
_ADMISSION_CLOSED = 2
_SYNC_STAGE1_TRADABLE_PATH = "/data_service.DataService/SyncAccountStage1Tradable"


def grpc_address(host, port) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


class Runtime:
    def __init__(self, settings: Settings, role: str, *, dev_token: str = "") -> None:
        self.settings = settings
        self.role = role
        self.local = role == "local"
        self.dev_token = dev_token
        self.protocol = Protocol()
        self._stack = AsyncExitStack()
        self._background: list[asyncio.Task] = []
        self._servers: list[asyncio.Server] = []
        self._websocket_servers = []
        self._shutdown_requested = asyncio.Event()
        self.gate = None
        self.scene = None
        self.battle = None
        self.local_services = None
        self._battle_control = None
        self._battle_clients = None
        self._battle_agones = None
        self._scene_agones = None
        self.battle_info = None
        self.login_info = None
        self._local_outcomes: dict[int, asyncio.Task] = {}
        self._local_notices: set[asyncio.Task] = set()
        self._kill_switches: dict[str, KillSwitch] = {}
        self._tables: ConfigTables | None = None

    def _config_tables(self) -> ConfigTables:
        if self._tables is None:
            self._tables = ConfigTables()
        return self._tables

    async def __aenter__(self):
        self.settings.validate(self.role)
        if self.role == "scene":
            self._scene_drain_budget = budget_from_env()
        try:
            if self.local:
                from .nodes.local_services import LocalServiceHub

                self.directory = LocalDirectory()
                self.local_services = LocalServiceHub(
                    self.protocol, tables=self._config_tables(), zone_id=self.settings.zone_id,
                )
                self._stack.push_async_callback(self.local_services.close)
                await self._scene()
                await self._gate()
                await self._battle()
                self.local_services.bind_runtime(
                    local_login=self.local_login, gate=self.gate, scene=self.scene, battle=self.battle,
                )
            else:
                self.directory = EtcdDirectory(self.protocol, self.settings.etcd)
                self._stack.push_async_callback(self.directory.close)
                self.kafka = KafkaCommands(
                    self.settings.kafka, partitions=self.settings.kafka_partitions,
                    generation=self.settings.kafka_generation,
                )
                self._stack.push_async_callback(self.kafka.close)
                node_settings = getattr(self.settings, self.role)
                consumes = self.role in ("gate", "scene")
                await self.kafka.start(self.role if consumes else None, node_settings.node_id)
                await {"gate": self._gate, "scene": self._scene, "battle": self._battle,
                       "login": self._login}[self.role]()
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        try:
            if (exc_type is None and self.role == "scene" and self.scene is not None
                    and self._shutdown_requested.is_set() and getattr(self.directory, "lease", 0)):
                await planned_drain(self.scene, self.directory, self._scene_drain_budget)
        finally:
            try:
                if self.battle is not None:
                    self.battle.begin_shutdown()
                for server in self._servers:
                    server.close()
                for server in self._websocket_servers:
                    server.close()
                # Preserve the lease and transports through the save barrier.
                if self.scene is not None:
                    await self.scene.close()
                if self._scene_agones is not None:
                    await self._scene_agones.close()
            finally:
                for task in self._background:
                    task.cancel()
                await asyncio.gather(*self._background, return_exceptions=True)
                try:
                    if self._battle_agones is not None:
                        await self._battle_agones.close()
                    closers = [edge.close() for edge in (self._battle_control, self._battle_clients) if edge is not None]
                    if self.battle_info is not None and not self.local and self.directory.lease:
                        closers.append(self._close_battle_registration())
                    if closers:
                        await asyncio.gather(*closers)
                finally:
                    try:
                        await self._stack.aclose()
                    finally:
                        await asyncio.gather(*(server.wait_closed() for server in self._servers))
                        await asyncio.gather(*(server.wait_closed() for server in self._websocket_servers))

    async def _close_battle_registration(self) -> None:
        self.battle_info.admission = _ADMISSION_CLOSED
        try:
            await self.directory.update(self.battle_info)
        except Exception:
            logger.warning("停机时更新 Battle 准入登记失败", exc_info=True)

    async def wait(self) -> None:
        shutdown = asyncio.create_task(self._shutdown_requested.wait(), name="runtime-shutdown")
        try:
            done, _ = await asyncio.wait([shutdown, *self._background], return_when=asyncio.FIRST_COMPLETED)
            for task in done - {shutdown}:
                task.result()
            if shutdown not in done:
                raise RuntimeError("节点后台任务意外退出")
        finally:
            shutdown.cancel()
            await asyncio.gather(shutdown, return_exceptions=True)

    def request_shutdown(self) -> None:
        self._shutdown_requested.set()

    def _info(self, role, kind):
        options = getattr(self.settings, role)
        return self.protocol.message(
            "NodeInfo", node_id=options.node_id, node_type=kind, zone_id=self.settings.zone_id,
            launch_time=int(time.time()), node_uuid=str(uuid.uuid4()),
            physical_region=self.settings.physical_region, release_track=self.settings.release_track,
            protocol_type=int(kind == BATTLE),
            scene_node_type=self.settings.scene_node_type if kind == SCENE else 0,
        )

    async def _listen_tcp(self, role, info, handler) -> None:
        options = getattr(self.settings, role)
        server = await asyncio.start_server(handler, options.host, options.port)
        self._servers.append(server)
        host = options.advertise_host or options.host
        port = server.sockets[0].getsockname()[1]
        info.endpoint.ip, info.endpoint.port = host, port
        if options.client_host or options.client_port:
            info.client_endpoint.ip = options.client_host or host
            info.client_endpoint.port = options.client_port or port

    async def _listen_websocket(self, role, handler) -> None:
        options = getattr(self.settings, role)
        if options.websocket_port is None:
            return
        from websockets.asyncio.server import serve
        from .transport import DEFAULT_GATE_FRAME_SIZE

        server = await serve(
            handler, options.host, options.websocket_port,
            max_size=DEFAULT_GATE_FRAME_SIZE + 4, max_queue=128, compression=None,
            ping_interval=20, ping_timeout=30,
        )
        self._websocket_servers.append(server)

    async def _listen_grpc(self, role, info, services, **options) -> None:
        node = getattr(self.settings, role)
        address = grpc_address(node.host, node.grpc_port)
        server, port = await serve_grpc(self.protocol, address, services,
                                        kill_switch=self._kill_switches.get(info.node_uuid), **options)
        self._stack.push_async_callback(server.stop, 3)
        info.grpc_endpoint.ip = node.advertise_host or node.host
        info.grpc_endpoint.port = port

    async def _publish(self, info, load=None) -> None:
        if self.local:
            self.directory.publish(info)
        else:
            await self.directory.register(info)
            self._spawn(self.directory.maintain(info, load=load), "etcd-lease")

    def _spawn(self, coroutine, name) -> None:
        self._background.append(asyncio.create_task(coroutine, name=name))

    def _configure_kill_switch(self, info):
        switch = KillSwitch(self.protocol, info)
        self._kill_switches[info.node_uuid] = switch
        if not self.local:
            sync = EtcdKillSwitch(self.protocol, self.settings.etcd, switch)
            sync.start()
            self._stack.push_async_callback(sync.close)
        return switch

    async def _gate(self) -> None:
        info = self._info("gate", GATE)
        switch = self._configure_kill_switch(info)
        endpoint = ClientEndpointSettings() if self.local else ClientEndpointSettings.from_env(os.environ)
        if endpoint.source == "agones":
            raise ValueError("Gate 不支持 CLIENT_ENDPOINT_SOURCE=agones")
        info.max_connections = self.settings.max_gate_connections
        if self.local:
            from .nodes.local_login import LocalLogin
            from .nodes.local_services import LocalGatewayForwarder

            token = self.dev_token or os.getenv("XUANMING_DEV_LOGIN_TOKEN", "")
            if not token:
                raise ValueError("本地模式需要设置 XUANMING_DEV_LOGIN_TOKEN")
            accounts_path = (self.settings.local_player_store or self.settings.database).with_name("local_accounts.json")
            self.local_login = LocalLogin(
                self.protocol, self.scene.info, access_token=token, store_path=accounts_path,
            )
            forward = LocalGatewayForwarder(self.local_login, self.local_services)
        elif self.settings.gate_uses_router():
            client = GrpcClient(self.settings.router)
            self._stack.push_async_callback(client.close)
            forward = RouterForwarder(self.protocol, client, gate_signing_key())
        else:
            from .nodes.gate_direct import DirectForwarder

            forward = DirectForwarder(self.protocol, self.directory, gate_signing_key())
            self._stack.push_async_callback(forward.close)
        gate_secret = secret("MMORPG_GATE_TOKEN_SECRET", fallback=secrets.token_bytes(32) if self.local else b"")
        gate = GateNode(self.protocol, info, gate_secret, forward, max_connections=info.max_connections)
        gate.kill_switch = switch
        self.gate = gate
        links = GateLinks(self.protocol, info, self.directory, gate.push, source_aware=True, on_broadcast=gate.broadcast)
        links.kill_switch = switch
        self._stack.push_async_callback(links.close)
        gate.scene = links
        self._stack.push_async_callback(gate.close)
        await self._listen_tcp("gate", info, gate.handle_client)
        await self._listen_websocket("gate", gate.handle_websocket)
        if self.settings.gate.rpc_port is not None:
            from .nodes.gate_rpc import GateRpcService

            service = GateRpcService(gate, self.directory, self.request_shutdown)
            self._stack.push_async_callback(service.close)
            if not info.HasField("client_endpoint"):
                info.client_endpoint.CopyFrom(info.endpoint)
            server = await asyncio.start_server(service.serve, self.settings.gate.host, self.settings.gate.rpc_port)
            self._servers.append(server)
            info.endpoint.port = server.sockets[0].getsockname()[1]
        self._resolve_client_endpoint(info, endpoint)
        if not self.local:
            self._spawn(self.kafka.consume(gate.command), "gate-commands")
            await self.kafka.start_world_chat()
            self._spawn(self.kafka.consume_world_chat(gate.world_chat), "gate-world-chat")
            spectate = os.getenv("MMORPG_SPECTATE_TOKEN_SECRET", "").encode()
            if spectate:
                battle_key = os.getenv("MMORPG_BATTLE_TOKEN_SECRET", "").encode().strip()
                if len(spectate.strip()) < 32 or spectate.strip() in (gate_secret.strip(), battle_key):
                    raise ValueError("观战票据密钥至少 32 字节，且必须与 Gate/Battle 密钥分域")
                gate.spectate = GateSpectate(gate, spectate)
                await self.kafka.start_spectate(info.node_id)
                self._spawn(self.kafka.consume_spectate(gate.spectate.frame), "gate-spectate")
                self._spawn(gate.spectate.sweep(), "gate-spectate-sweep")
        await self._publish(info, load=lambda: len(gate.sessions))

    @staticmethod
    def _resolve_client_endpoint(info, settings, *, agones=None) -> None:
        configured = (info.client_endpoint.ip, info.client_endpoint.port) if info.HasField("client_endpoint") else None
        address = settings.resolve(configured=configured, agones=agones)
        if address is None:
            info.ClearField("client_endpoint")
        else:
            info.client_endpoint.ip, info.client_endpoint.port = address

    @staticmethod
    def _resolve_login_content_guard_endpoint(settings: Settings, environ=None) -> str:
        """从环境变量或 ``content_guard_node`` 配置解析可选内容安全 gRPC 地址。"""
        env = os.environ if environ is None else environ
        direct = env.get("LOGIN_CONTENT_GUARD", "").strip()
        if direct:
            return direct
        raw = dict(getattr(settings, "content_guard_node", {}) or {})
        endpoint = str(raw.get("endpoint", "")).strip()
        if endpoint:
            return endpoint
        host = str(raw.get("advertise_host") or raw.get("host") or "").strip()
        port = raw.get("grpc_port")
        if host and port:
            if host in ("0.0.0.0", "::"):
                host = "127.0.0.1"
            return grpc_address(host, int(port))
        return ""

    @staticmethod
    def _build_login_context(
        protocol: Protocol,
        config,
        *,
        login_redis,
        player_redis,
        locator_call,
        data_call,
        scene_manager_call,
        publish,
        directory,
        control_redis=None,
        content_guard_call=None,
        password_auth_pool=None,
        environ=None,
    ):
        """装配生产完整版 Login ``ServiceContext``（排队、防沉迷、注销、内容安全与口令认证）。"""
        from .login.account_deletion import AccountDeletionService
        from .login.anti_addiction import AntiAddictionService, PlayCalendar, RealNameService, SandboxNppaVerifier
        from .login.directory import DataService
        from .login.password_auth import (
            CompositePasswordProvider,
            DevPasswordProvider,
            ProductionPasswordProvider,
            RedisPasswordFailureQuota,
            SqlPasswordAuthStore,
        )
        from .login.queue import LoginQueue
        from .login.service import ServiceContext
        from .login.sessions import PlayerLocator
        from .login.tokens import TokenManager

        env = os.environ if environ is None else environ
        tokens = TokenManager(login_redis)
        locator = PlayerLocator(protocol, locator_call)
        data = DataService(protocol, data_call, config.directory_secret)
        queue_redis = control_redis if control_redis is not None else login_redis
        login_queue = (
            LoginQueue(
                queue_redis,
                config.gate_token_secret,
                entry_ttl_s=config.queue_entry_ttl_s,
                admit_ttl_s=config.queue_admit_ttl_s,
                max_length=config.queue_max_length,
            )
            if config.queue_enabled
            else None
        )
        extra_days_raw = env.get("LOGIN_ANTI_ADDICTION_EXTRA_PLAY_DAYS", "").strip()
        extra_days = tuple(item.strip() for item in extra_days_raw.split(",") if item.strip())
        platform_raw = env.get("LOGIN_ANTI_ADDICTION_PLATFORM_MANAGED", "").strip()
        platform_managed = frozenset(item.strip() for item in platform_raw.split(",") if item.strip())
        real_name = RealNameService(login_redis, SandboxNppaVerifier(), data)
        anti_addiction = AntiAddictionService(
            protocol,
            real_name,
            calendar=PlayCalendar(extra_days),
            platform_managed=platform_managed,
        )
        account_deletion = AccountDeletionService(protocol, data, tokens, locator, publish)
        password_provider = None
        if password_auth_pool is not None:
            quota = RedisPasswordFailureQuota(queue_redis)
            production = ProductionPasswordProvider(SqlPasswordAuthStore(password_auth_pool), quota)
            if config.dev_password_secret:
                dev = DevPasswordProvider(config.dev_password_secret, config.dev_password_prefixes)
                password_provider = CompositePasswordProvider(production, dev)
            else:
                password_provider = production
        return ServiceContext(
            protocol=protocol,
            config=config,
            login_redis=login_redis,
            player_redis=player_redis,
            tokens=tokens,
            locator=locator,
            data=data,
            scene_manager_call=scene_manager_call,
            publish=publish,
            directory=directory,
            control_redis=control_redis,
            login_queue=login_queue,
            password_provider=password_provider,
            anti_addiction=anti_addiction,
            account_deletion=account_deletion,
            content_guard_call=content_guard_call,
        )

    @staticmethod
    async def _refresh_merged_zones_once(context) -> bool:
        """向 ``data_service`` 拉取一次合服映射并原子替换 ``context.merged_zones``；容忍瞬态故障。"""
        try:
            mapping = await context.data.get_merged_zones()
        except Exception as error:
            logger.warning("Login merged zone refresh failed: %s", error)
            return False
        context.merged_zones = {int(src): int(dst) for src, dst in mapping.items() if int(src) > 0 and int(dst) > 0}
        return True

    async def _refresh_merged_zones_loop(self, context, *, interval_s: float = 30.0) -> None:
        """后台定时刷新合服归属区映射，单次拉取失败仅记日志不退出任务。"""
        while True:
            await self._refresh_merged_zones_once(context)
            await asyncio.sleep(interval_s)

    async def _login(self) -> None:
        from redis.asyncio import Redis, RedisCluster

        from .login.caller_auth import CallerVerifier
        from .login.config import LoginConfig
        from .login.password_auth import open_password_auth_pool
        from .login.service import LoginService

        info = self._info("login", LOGIN)
        info.protocol_type = 1  # GRPC: gates discover Login by grpc_endpoint
        switch = self._configure_kill_switch(info)
        config = LoginConfig.from_env(self.settings.zone_id)
        redis_type = RedisCluster if self.settings.redis_cluster else Redis
        redis = {}
        for name, url in (("player", self.settings.redis), ("login", self.settings.login_redis or self.settings.redis)):
            redis[name] = redis_type.from_url(url, decode_responses=False, socket_connect_timeout=3, socket_timeout=3)
            self._stack.push_async_callback(redis[name].aclose)
            await redis[name].ping()

        control_redis = None
        control_url = os.getenv("LOGIN_CONTROL_REDIS", "").strip() or self.settings.scene_control_redis
        if control_url:
            control_redis = redis_type.from_url(
                control_url, decode_responses=False, socket_connect_timeout=3, socket_timeout=3,
            )
            self._stack.push_async_callback(control_redis.aclose)
            await control_redis.ping()

        clients = {}
        for name, timeout in (("player_locator", 5), ("scene_manager", 13.5), ("data_service", 3)):
            clients[name] = GrpcClient(getattr(self.settings, name), timeout=timeout)
            self._stack.push_async_callback(clients[name].close)

        content_guard_call = None
        content_guard_addr = self._resolve_login_content_guard_endpoint(self.settings)
        if content_guard_addr:
            cg_client = GrpcClient(content_guard_addr, timeout=2.0)
            self._stack.push_async_callback(cg_client.close)
            content_guard_call = cg_client.call

        password_pool = None
        password_dsn = (
            os.getenv("LOGIN_PASSWORD_AUTH_DSN", "").strip()
            or os.getenv("MMORPG_LOGIN_PASSWORD_DSN", "").strip()
        )
        if password_dsn:
            password_pool = await open_password_auth_pool(password_dsn)

            async def _close_password_pool(pool=password_pool) -> None:
                pool.close()
                await pool.wait_closed()

            self._stack.push_async_callback(_close_password_pool)

        async def publish(gate_node_id, command, key):
            await self.kafka.publish("gate", gate_node_id, command, key=key)

        context = self._build_login_context(
            self.protocol,
            config,
            login_redis=redis["login"],
            player_redis=redis["player"],
            locator_call=clients["player_locator"].call,
            data_call=clients["data_service"].call,
            scene_manager_call=clients["scene_manager"].call,
            publish=publish,
            directory=self.directory,
            control_redis=control_redis,
            content_guard_call=content_guard_call,
            password_auth_pool=password_pool,
        )
        self._stack.push_async_callback(context.close)
        await self._refresh_merged_zones_once(context)
        self._spawn(self._refresh_merged_zones_loop(context), "login-merged-zones")
        service = LoginService(context, CallerVerifier.from_env(), kill_switch=switch)
        await self._listen_grpc("login", info, service.services(), pass_context=True)
        info.endpoint.CopyFrom(info.grpc_endpoint)
        self.login_info = info
        await self._publish(info)

    @staticmethod
    def _bind_equip_craft_account_quota_sync(scene, data_service_call, account_secret, *, stack=None):
        """集群模式下为 SceneEquipCraft 接入账号级阶段一可交易配额异步同步传输。"""
        secret_bytes = account_secret.encode("utf-8") if isinstance(account_secret, str) else bytes(account_secret)
        pending: dict[int, asyncio.Task] = {}

        async def _sync_once(player_id: int, desired_char_total: int, now_ms: int) -> None:
            stamp = int(now_ms) if now_ms else (time.time_ns() // 1_000_000)
            req = scene.protocol.message(
                "data_service.SyncAccountStage1TradableRequest",
                player_id=int(player_id),
                desired_char_total=int(desired_char_total),
            )
            sign_service_call(_SYNC_STAGE1_TRADABLE_PATH, req, "scene", stamp, secret_bytes)
            try:
                raw = await data_service_call(_SYNC_STAGE1_TRADABLE_PATH, req.SerializeToString())
                reply = scene.protocol.parse("data_service.SyncAccountStage1TradableResponse", raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "EquipCraft account stage1 quota sync RPC failed player_id=%d desired=%d: %s",
                    player_id,
                    desired_char_total,
                    exc,
                )
                return

            async with scene._lock(player_id):
                world = scene.locations.get(player_id)
                player = None if world is None else world.players.get(player_id)
                if player is None:
                    return
                data = scene.rows.assemble(player.snapshot)
                changed = scene.equip_craft.apply_account_quota_sync_reply(
                    player_id,
                    data,
                    desired_char_total=int(desired_char_total),
                    found=bool(reply.found),
                    account_used_total=int(reply.account_used_total),
                    confirmed_char_total=int(reply.confirmed_char_total),
                )
                if changed and scene.mutable(player):
                    try:
                        await scene.commit_player_data(player, data)
                    except (StoreRejected, StoreUnavailable, OSError):
                        logger.warning(
                            "EquipCraft account stage1 quota sync save failed player_id=%d",
                            player_id,
                            exc_info=True,
                        )

        def _dispatch(player_id: int, desired_char_total: int, now_ms: int):
            existing = pending.get(player_id)
            if existing is not None and not existing.done():
                return existing
            task = asyncio.create_task(
                _sync_once(player_id, desired_char_total, now_ms),
                name=f"equip-quota-sync-{player_id}",
            )
            pending[player_id] = task

            def _cleanup(finished: asyncio.Task) -> None:
                if pending.get(player_id) is finished:
                    pending.pop(player_id, None)

            task.add_done_callback(_cleanup)
            return task

        if stack is not None:
            async def _close_pending() -> None:
                tasks = list(pending.values())
                pending.clear()
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            stack.push_async_callback(_close_pending)

        scene.equip_craft.set_account_quota_sync_transport(_dispatch)
        return _dispatch

    async def _scene(self) -> None:
        info = self._info("scene", SCENE)
        switch = self._configure_kill_switch(info)
        tables = self._config_tables()
        audit = anomaly = allocate_tx_id = data_service = None
        store, store_closer = create_player_store(
            self.settings,
            self.protocol,
            local=self.local,
            grpc_client_factory=lambda target: GrpcClient(target, timeout=5, receive_limit=8 << 20),
            secret_resolver=secret,
        )
        if store_closer is not None:
            self._stack.push_async_callback(store_closer)
        if self.local:
            self.settings.database.parent.mkdir(parents=True, exist_ok=True)
            allocate_item_id = IdSegments.local(self.settings.database, "item")
        else:
            data_service = GrpcClient(self.settings.data_service)
            self._stack.push_async_callback(data_service.close)
            allocate_item_id = IdSegments.remote(self.protocol, data_service.call, "item")
            allocate_tx_id = IdSegments.remote(self.protocol, data_service.call, "txlog")

            async def audit(entry):
                await self.kafka.publish_audit(entry, self.settings.audit_generation)

            anomaly = self.kafka.publish_anomaly
        links = SceneLinks(self.protocol, info, self.directory)
        self._stack.push_async_callback(links.close)
        scene = SceneNode(self.protocol, info, store, PlayerRowCodec(self.protocol), links.emit,
                          emit_many=links.emit_many, tick_interval=self.settings.scene_tick_seconds)
        scene.kill_switch = switch
        scene.tables = tables
        scene.navigation = load_navigation(tables)
        self.scene = scene
        links.bind(scene, admin=SceneAdmin(scene, self.request_shutdown))
        methods = {"CreateScene": scene.create, "DestroyScene": scene.destroy, "ReleasePlayer": scene.release}
        match_call = redis = None
        account_secret = b""
        if self.local:
            peer = self.local_inbox = LocalInbox(self.protocol, self.settings.database)
            scene.create(self.protocol.message("CreateSceneRequest", config_id=1, scene_id=1))
            scene.register("SceneSceneClientPlayer", "EnterScene", scene.local_scene_entry)
            scene.register("SceneSceneClientPlayer", "TravelToZone", scene.local_zone_travel)
            if self.local_services is not None:
                match_call = self.local_services.call_raw
                account_secret = self.local_services.internal_secret
        else:
            account_secret = secret("MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE")
            scene.travel = await create_scene_travel(scene, tables, self.settings, self.directory)
            methods["SealAccountRoleTrade"] = scene.travel.seal
            self._stack.push_async_callback(scene.travel.close)
            redis = scene.travel.redis

            async def read_afk_pass(player_id: int):
                return await redis.get(afk_pass_key(player_id))

            scene.connection_holds.read_pass = read_afk_pass
            match = MatchClient(self.directory, self.settings.match, timeout=6)
            self._stack.push_async_callback(match.close)
            match_call = match.call
            inbox_client = GrpcClient(self.settings.player_inbox)
            self._stack.push_async_callback(inbox_client.close)
            peer = InboxPeer(self.protocol, inbox_client.call, info.node_id, account_secret)
        scene.team = SceneTeam(scene, scene.travel)
        scene.team.register()
        self._stack.push_async_callback(scene.team.close)
        scene.battle = SceneBattle(scene, tables, redis=redis, match_call=match_call, terminal_secret=account_secret)
        methods.update(PrepareBattle=scene.battle.prepare, CancelBattlePrepare=scene.battle.cancel)
        scene.assets = SceneAssets(
            scene, tables, allow_gm=self.local and os.getenv("SCENE_RUN_MODE", "") in ("dev", "test"),
            allocate_item_id=allocate_item_id,
            emit_audit=audit, allocate_tx_id=allocate_tx_id, emit_anomaly=anomaly,
            anomaly_journal_path=(self.settings.anomaly_journal
                                  or self.settings.database.with_name("asset-anomaly.json")),
        )
        await scene.assets.anomaly.start()
        self._stack.push_async_callback(scene.assets.close)
        scene.assets.register_client_handlers()
        Monopoly(scene).register()
        methods.update(scene.assets.grpc_methods())
        merchant_reader = control_redis = None
        if self.settings.scene_control_redis:
            from redis.asyncio import Redis

            control_redis = Redis.from_url(
                self.settings.scene_control_redis, decode_responses=False,
                socket_connect_timeout=3, socket_timeout=3,
            )
            self._stack.push_async_callback(control_redis.aclose)

            async def merchant_reader():
                return await control_redis.hget(redis_key(info.zone_id), "state")

        scene.channels.control_redis = control_redis
        scene.landing = SceneLanding(scene, player_redis=redis, control_redis=control_redis)
        scene.landing.register()
        self._stack.push_async_callback(scene.landing.close)
        merchant = SceneMerchant(scene, tables, scene.assets, read_state=merchant_reader)
        merchant.register()
        if merchant_reader:
            self._spawn(merchant.refresh_loop(), "mystery-merchant")
        missions = Missions(scene, tables, scene.assets)
        missions.register()
        scene.attributes = SceneAttributes(scene, tables, scene.assets, missions=missions, allow_gm=scene.assets.allow_gm)
        scene.attributes.register()
        scene.pets = ScenePets(scene, tables)
        scene.pets.register()
        scene.afk_pass = SceneAfkPass(scene, tables, scene.assets, player_redis=redis)
        scene.afk_pass.register()
        scene.appearance = SceneAppearance(scene, tables, scene.assets)
        scene.appearance.register()
        scene.season = SceneSeason(scene, tables, scene.assets)
        scene.season.register()
        scene.trial_journal = SceneTrialJournal(scene, tables, scene.assets)
        scene.trial_journal.register()
        scene.equip_craft = SceneEquipCraft(scene, tables, scene.assets)
        scene.equip_craft.register()
        if not self.local and data_service is not None:
            self._bind_equip_craft_account_quota_sync(
                scene, data_service.call, account_secret, stack=self._stack,
            )
        scene.skill_craft = SceneSkillCraft(scene, tables, scene.assets)
        scene.skill_craft.register()
        scene.festival_beast = SceneFestivalBeast(scene, tables, scene.assets)
        scene.festival_beast.register()
        scene.spirit_shop = SceneSpiritShop(scene, tables, scene.assets)
        scene.spirit_shop.register()
        scene.travel_pass = SceneTravelPass(scene, tables, scene.assets)
        scene.travel_pass.register()
        scene.rewarded_ad = SceneRewardedAd(scene, tables, scene.assets)
        scene.rewarded_ad.register()
        scene.equip_loadout = SceneEquipLoadout(scene, tables, scene.assets)
        scene.equip_loadout.register()
        scene.jianghu = SceneJianghu(scene, tables, scene.assets)
        scene.jianghu.register()
        methods.update(scene.jianghu.grpc_methods())
        scene_cg_call = self.local_services.call_raw if (self.local and self.local_services is not None) else None
        if not self.local:
            cg_endpoint = os.getenv("SCENE_CONTENT_GUARD", "").strip()
            if not cg_endpoint and isinstance(self.settings.content_guard_node, dict):
                cg_host = self.settings.content_guard_node.get("advertise_host") or self.settings.content_guard_node.get("host")
                cg_port = self.settings.content_guard_node.get("grpc_port")
                if cg_host and cg_port:
                    cg_endpoint = f"{cg_host}:{cg_port}"
            if cg_endpoint:
                cg_client = GrpcClient(cg_endpoint, timeout=3)
                self._stack.push_async_callback(cg_client.close)
                scene_cg_call = cg_client.call
        scene.rename = SceneRename(
            scene,
            tables,
            scene.assets,
            data_service_call=None if data_service is None else data_service.call,
            directory_secret=account_secret,
            content_guard_call=scene_cg_call,
        )
        scene.rename.register()
        self._stack.push_async_callback(scene.rename.close)
        scene.gm_commands = SceneGmCommands(scene)
        methods.update(scene.gm_commands.grpc_methods())
        Activities(missions).register()
        settlement = BattleSettlement(scene, tables, scene.assets, missions)
        settlement.register()
        mail_ids = IdSegments.local(self.settings.database, "txlog") if self.local else allocate_tx_id
        mail_sender = (
            self.local_services.mail_sender
            if (self.local and self.local_services is not None)
            else MailSender(self.protocol, self.directory)
        )
        scene.mail_outbox = SceneMailOutbox(scene, mail_ids, mail_sender)
        scene.inbox = SceneInbox(scene, peer, settlement, scene.assets, poll_seconds=5 if self.local else 60)
        skills = SceneSkills(scene, tables)
        skills.register()
        scene.skill_casts = skills.casts
        scene.buffs = skills.casts.buffs
        scene.buffs.visibility_changed = scene.refresh_actor_visibility

        def initialize(data):
            if self.local:
                self.local_login.initialize_role(data)
            data.level_component.SetInParent()
            initialize_attribute_schemes(data)
            scene.pets.initialize_on_load(data)
            initialize_skill_list(tables, data)
            initialize_base(self.protocol, tables, data)

        scene.initialize_player = initialize
        await self._listen_tcp("scene", info, links.serve)
        await self._listen_grpc("scene", info, {"scene_node.SceneNodeGrpc": methods})
        if not self.local:
            self._spawn(self.kafka.consume(self._scene_command), "scene-commands")
        self._spawn(self._scene_loop(), "scene-tick")
        await self._publish(info)
        if not self.local:
            self._scene_agones = SceneAgones.from_env()
            if self._scene_agones is not None:
                self._scene_agones.start()
                self._spawn(self._scene_agones.watch_drain(self.request_shutdown), "scene-agones-drain")

    async def _scene_loop(self) -> None:
        previous = time.monotonic()
        while True:
            await asyncio.sleep(self.settings.scene_tick_seconds)
            now = time.monotonic()
            await self.scene.tick(min(now - previous, 0.25))
            if getattr(self.scene, "appearance", None) is not None and self.scene.appearance.has_active_trials:
                await self.scene.appearance.expire_trials()
            if self._scene_agones is not None:
                self._scene_agones.touch_heartbeat()
            previous = now

    async def _scene_command(self, data: bytes) -> None:
        try:
            command = self.protocol.parse("contracts.kafka.SceneCommand", data)
            if (command.target_scene_id != self.scene.info.node_id
                    or command.target_instance_id != self.scene.info.node_uuid or not command.payload):
                return
            if command.event_id == self.protocol.event_ids["ContractsKafkaPlayerLeaseExpiredEvent"]:
                event = self.protocol.parse("contracts.kafka.PlayerLeaseExpiredEvent", command.payload)
                await self.scene.lease_expired(
                    event.player_id, event.session_id, event.gate_instance_id, event.scene_id,
                )
            elif command.event_id == self.protocol.event_ids["BattleConfirmedEvent"]:
                event = self.protocol.parse("BattleConfirmedEvent", command.payload)
                if event.player_id == command.player_id:
                    await self.scene.battle.confirm(event)
            elif command.event_id == self.protocol.event_ids["PlayerInboxNotifyEvent"]:
                event = self.protocol.parse("PlayerInboxNotifyEvent", command.payload)
                if event.player_id == command.player_id:
                    self.scene.inbox.wake(event.player_id, event.domain)
            elif command.event_id == self.protocol.event_ids["PlayerTeamRefreshEvent"]:
                event = self.protocol.parse("PlayerTeamRefreshEvent", command.payload)
                if event.player_id == command.player_id:
                    self.scene.team.hint(event.player_id)
            elif command.event_id == self.protocol.event_ids.get("PlayerHostedPlacementEvent"):
                event = self.protocol.parse("PlayerHostedPlacementEvent", command.payload)
                if (event.player_id == command.player_id and event.player_id and event.scene_id
                        and event.owner_epoch and event.session_id and event.login_generation and event.owner_revision):
                    request = self.protocol.message(
                        "PlayerEnterGameNodeRequest", player_id=event.player_id, scene_id=event.scene_id,
                        home_zone_id=event.home_zone_id, owner_epoch=event.owner_epoch, session_id=event.session_id,
                        login_generation=event.login_generation, owner_revision=event.owner_revision,
                    )
                    if self.scene.travel:
                        await self.scene.travel.hosted_placement(request)
                    else:
                        await self.scene.enter(request, "", hosted=True)
        except Exception:
            logger.warning("Scene 命令执行失败", exc_info=True)

    async def _battle(self) -> None:
        info = self._info("battle", BATTLE)
        switch = self._configure_kill_switch(info)
        info.admission = _ADMISSION_CLOSED
        options = self.settings.battle
        endpoint = ClientEndpointSettings() if self.local else ClientEndpointSettings.from_env(os.environ)
        lifecycle = self._battle_agones = BattleAgones.from_env() if not self.local else None
        if endpoint.source == "agones" and lifecycle is None:
            raise ValueError("CLIENT_ENDPOINT_SOURCE=agones 需要 Agones SDK")
        probe_secret = b""
        if not self.local and (os.getenv("GLOBAL_MATCH_ENABLED") == "true" or os.getenv("MATCH_PROBE_SECRET")):
            probe_secret = secret("MATCH_PROBE_SECRET")
            if not info.physical_region:
                raise ValueError("全球匹配需要非零 physical_region")
        control = None
        if not self.local:
            from .nodes.battle_control import BattleControl

            async def publish(role, node_id, player_id, message):
                if role in ("relay", "broadcast"):
                    await self.kafka.publish_spectate(role, node_id, player_id, message)
                else:
                    await self.kafka.publish(role, node_id, message, key=str(player_id).encode())

            control = BattleControl(
                self.protocol, match_endpoint=self.settings.match,
                outcome_secret=secret("MMORPG_MATCH_OUTCOME_SECRET_BATTLE"), publish=publish,
                match_client=MatchClient(self.directory, self.settings.match),
            )
            self._battle_control = control
        battle_secret = secret(
            "MMORPG_BATTLE_TOKEN_SECRET", fallback=secrets.token_bytes(32) if self.local else b"",
        )

        def local_commit(battle_id, outcome):
            if battle_id not in self._local_outcomes:
                self._local_outcomes[battle_id] = asyncio.create_task(self._local_commit(battle_id))

        def local_confirm(routing, player_id, battle_id, deadline):
            scene = self.scene
            if (routing.scene_node_id, routing.scene_instance_id) != (scene.info.node_id, scene.info.node_uuid):
                return
            event = self.protocol.message(
                "BattleConfirmedEvent", player_id=player_id, battle_id=battle_id, deadline_ms=deadline,
            )
            task = asyncio.create_task(scene.battle.confirm(event))
            self._local_notices.add(task)
            task.add_done_callback(self._local_notices.discard)

        def local_lobby(routing, player_id, message_id, message):
            if (routing.gate_node_id, routing.gate_instance_id) != (self.gate.info.node_id, self.gate.info.node_uuid):
                return
            request = self.protocol.message(
                "NodeRouteMessageRequest", header={"session_id": routing.session_id, "target_player_id": player_id},
                message_content=self.protocol.content(message_id, body=message.SerializeToString()),
            )
            self.gate.push(request)

        self.battle = battle = BattleNode(
            self.protocol, node_id=info.node_id, instance_id=info.node_uuid,
            host=options.client_host or options.advertise_host or options.host, port=options.port,
            physical_region=info.physical_region, ticket_secret=battle_secret,
            max_connections=self.settings.max_battle_connections,
            on_outcome=local_commit if self.local else control.outcome,
            on_lobby_push=local_lobby if self.local else control.lobby,
            on_confirm=local_confirm if self.local else control.confirm,
            on_spectate_frame=None if self.local else control.spectate,
            on_rooms_changed=lifecycle.rooms_changed if lifecycle is not None else None,
            tables=self._config_tables(),
        )
        battle.kill_switch = switch
        if control is not None:
            control.bind(battle)
        battle.admitting = False

        async def create_battle(request):
            if not battle.admitting:
                raise GrpcUnavailable("battle_not_allocatable")
            if lifecycle is None:
                return battle.create(request)
            async with lifecycle.permit() as granted:
                if not granted or not battle.admitting:
                    raise GrpcUnavailable("battle_not_allocatable")
                return battle.create(request)

        def query_creation(request):
            response = battle.query_creation(request)
            if response.target_instance_id:
                response.rejects_new_battles = response.rejects_new_battles or info.admission != _ADMISSION_OPEN
            return response

        def destroy_battle(request):
            if not battle.accepts_placement_operation(request):
                raise ValueError("placement_operation_mismatch")
            return battle.destroy(request)

        clients = BattleClients(self.protocol, battle, probe_secret=probe_secret)
        self._battle_clients = clients
        self._stack.callback(battle.close)
        if self.local:
            self._stack.push_async_callback(self._finish_local_outcomes)
        await self._listen_tcp("battle", info, clients.serve)
        await self._listen_websocket("battle", clients.serve_websocket)
        agones_endpoint = await lifecycle.client_endpoint() if endpoint.source == "agones" else None
        self._resolve_client_endpoint(info, endpoint, agones=agones_endpoint)
        battle.host = info.client_endpoint.ip or info.endpoint.ip
        battle.port = info.client_endpoint.port or info.endpoint.port
        await self._listen_grpc("battle", info, {"BattleNode": {
            "CreateBattle": create_battle, "QueryBattleCreation": query_creation,
            "DestroyBattle": destroy_battle, "AddObserver": battle.add_observer,
            "RemoveObserver": battle.remove_observer, "QueryObserver": battle.query_observer,
            "IssueBattleTicket": battle.issue_ticket,
            "AttachRelayGate": battle.attach_relay_gate,
            "PromoteSpectateBroadcast": battle.promote_spectate_broadcast,
        }})
        self.battle_info = info
        await self._publish(info)
        battle.admitting = True
        if lifecycle is not None:
            lifecycle.start()
            self._spawn(self._battle_admission_loop(info, lifecycle), "battle-admission")
        else:
            info.admission = _ADMISSION_OPEN
            if not self.local:
                await self.directory.update(info)
        self._spawn(self._battle_loop(), "battle-tick")

    async def _battle_loop(self) -> None:
        while True:
            await asyncio.sleep(0.05)
            self.battle.tick()
            if self._battle_agones is not None:
                self._battle_agones.touch_heartbeat()

    async def _battle_admission_loop(self, info, lifecycle) -> None:
        while True:
            desired = _ADMISSION_OPEN if lifecycle.admitting and self.battle.admitting else _ADMISSION_CLOSED
            if info.admission != desired:
                info.admission = desired
                await self.directory.update(info)
            await asyncio.sleep(1)

    async def _local_commit(self, battle_id) -> None:
        try:
            outcome = self.battle.outcome_request(battle_id)
            if outcome is None:
                return
            result = self.protocol.parse("contracts.kafka.BattleResultEvent", outcome.raw_result_payload)
            if result.settlement_format_version != 1 or not result.settlements:
                raise ValueError("本地战斗终局缺少个人原件")
            await self.local_inbox.enqueue(result.settlements)
            self.battle.decide_outcome(battle_id, committed=True)
            for settlement in result.settlements:
                self.scene.inbox.wake(settlement.player_id, INBOX_BATTLE)
        except Exception:
            logger.exception("本地战斗终局保存失败，将保留原件重试 battle_id=%s", battle_id)
        finally:
            self._local_outcomes.pop(battle_id, None)

    async def _finish_local_outcomes(self) -> None:
        await asyncio.gather(*tuple(self._local_outcomes.values()), *tuple(self._local_notices), return_exceptions=True)
