import asyncio
import logging
import os
import secrets
import time
import uuid
from contextlib import AsyncExitStack

from .cluster import BATTLE, GATE, SCENE, EtcdDirectory, KafkaCommands, LocalDirectory
from .data import ConfigTables, DataServicePlayerStore, PlayerRowCodec, SqlitePlayerStore
from .data.id_segments import IdSegments
from .data.navigation import load_navigation
from .grpc_runtime import GrpcClient, GrpcUnavailable, serve_grpc
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
from .nodes.scene_activity import Activities
from .nodes.scene_assets import SceneAssets
from .nodes.scene_attributes import SceneAttributes, initialize_attribute_schemes
from .nodes.scene_battle import SceneBattle
from .nodes.scene_connection_hold import afk_pass_key
from .nodes.scene_landing import SceneLanding
from .nodes.scene_merchant import SceneMerchant, redis_key
from .nodes.scene_mission import Missions
from .nodes.scene_pets import ScenePets
from .nodes.scene_skills import SceneSkills, initialize_skill_list
from .nodes.scene_stats import initialize_base
from .nodes.scene_team import SceneTeam
from .nodes.scene_travel import create_scene_travel
from .protocol import Protocol
from .settings import Settings, secret
from .transport.client_endpoint import ClientEndpointSettings

logger = logging.getLogger(__name__)
_ADMISSION_OPEN = 1
_ADMISSION_CLOSED = 2


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
        self._shutdown_requested = asyncio.Event()
        self.gate = None
        self.scene = None
        self.battle = None
        self._battle_control = None
        self._battle_clients = None
        self._battle_agones = None
        self.battle_info = None
        self._local_outcomes: dict[int, asyncio.Task] = {}
        self._local_notices: set[asyncio.Task] = set()

    async def __aenter__(self):
        self.settings.validate(self.role)
        try:
            if self.local:
                self.directory = LocalDirectory()
                await self._scene()
                await self._gate()
                await self._battle()
            else:
                self.directory = EtcdDirectory(self.protocol, self.settings.etcd)
                self._stack.push_async_callback(self.directory.close)
                self.kafka = KafkaCommands(
                    self.settings.kafka, partitions=self.settings.kafka_partitions,
                    generation=self.settings.kafka_generation,
                )
                self._stack.push_async_callback(self.kafka.close)
                node_settings = getattr(self.settings, self.role)
                await self.kafka.start(self.role if self.role != "battle" else None, node_settings.node_id)
                await {"gate": self._gate, "scene": self._scene, "battle": self._battle}[self.role]()
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.battle is not None:
            self.battle.begin_shutdown()
        for server in self._servers:
            server.close()
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

    async def _listen_grpc(self, role, info, services) -> None:
        options = getattr(self.settings, role)
        address = grpc_address(options.host, options.grpc_port)
        server, port = await serve_grpc(self.protocol, address, services)
        self._stack.push_async_callback(server.stop, 3)
        info.grpc_endpoint.ip = options.advertise_host or options.host
        info.grpc_endpoint.port = port

    async def _publish(self, info, load=None) -> None:
        if self.local:
            self.directory.publish(info)
        else:
            await self.directory.register(info)
            self._spawn(self.directory.maintain(info, load=load), "etcd-lease")

    def _spawn(self, coroutine, name) -> None:
        self._background.append(asyncio.create_task(coroutine, name=name))

    async def _gate(self) -> None:
        info = self._info("gate", GATE)
        endpoint = ClientEndpointSettings() if self.local else ClientEndpointSettings.from_env(os.environ)
        if endpoint.source == "agones":
            raise ValueError("Gate 不支持 CLIENT_ENDPOINT_SOURCE=agones")
        info.max_connections = self.settings.max_gate_connections
        if self.local:
            from .nodes.local_login import LocalLogin

            token = self.dev_token or os.getenv("XUANMING_DEV_LOGIN_TOKEN", "")
            if not token:
                raise ValueError("本地模式需要设置 XUANMING_DEV_LOGIN_TOKEN")
            forward = self.local_login = LocalLogin(self.protocol, self.scene.info, access_token=token)
        elif self.settings.gate_uses_router():
            client = GrpcClient(self.settings.router)
            self._stack.push_async_callback(client.close)
            forward = RouterForwarder(self.protocol, client, secret("MMORPG_INTERNAL_AUTH_SECRET"))
        else:
            from .nodes.gate_direct import DirectForwarder

            forward = DirectForwarder(self.protocol, self.directory, secret("MMORPG_INTERNAL_AUTH_SECRET"))
            self._stack.push_async_callback(forward.close)
        gate_secret = secret("MMORPG_GATE_TOKEN_SECRET", fallback=secrets.token_bytes(32) if self.local else b"")
        gate = GateNode(self.protocol, info, gate_secret, forward, max_connections=info.max_connections)
        self.gate = gate
        links = GateLinks(self.protocol, info, self.directory, gate.push, source_aware=True, on_broadcast=gate.broadcast)
        self._stack.push_async_callback(links.close)
        gate.scene = links
        self._stack.push_async_callback(gate.close)
        await self._listen_tcp("gate", info, gate.handle_client)
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

    async def _scene(self) -> None:
        info = self._info("scene", SCENE)
        tables = ConfigTables()
        audit = anomaly = allocate_tx_id = None
        if self.local:
            self.settings.database.parent.mkdir(parents=True, exist_ok=True)
            store = SqlitePlayerStore(self.settings.database)
            allocate_item_id = IdSegments.local(self.settings.database, "item")
        else:
            client = GrpcClient(self.settings.player_store, timeout=5, receive_limit=8 << 20)
            self._stack.push_async_callback(client.close)
            store = DataServicePlayerStore(self.protocol, client.call, secret("MMORPG_PLAYER_STORE_SECRET_SCENE"))
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
                          emit_many=links.emit_many)
        scene.tables = tables
        scene.navigation = load_navigation(tables)
        self.scene = scene
        links.bind(scene)
        methods = {"CreateScene": scene.create, "DestroyScene": scene.destroy, "ReleasePlayer": scene.release}
        match_call = redis = None
        account_secret = b""
        if self.local:
            peer = self.local_inbox = LocalInbox(self.protocol, self.settings.database)
            scene.create(self.protocol.message("CreateSceneRequest", config_id=1, scene_id=1))
            scene.register("SceneSceneClientPlayer", "EnterScene", scene.local_scene_entry)
        else:
            account_secret = secret("MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE")
            scene.travel = await create_scene_travel(scene, tables, self.settings)
            methods["SealAccountRoleTrade"] = scene.travel.seal
            self._stack.push_async_callback(scene.travel.close)
            redis = scene.travel.redis

            async def read_afk_pass(player_id: int):
                return await redis.get(afk_pass_key(player_id))

            scene.connection_holds.read_pass = read_afk_pass
            match = GrpcClient(self.settings.match, timeout=6)
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
        )
        self._stack.push_async_callback(scene.assets.close)
        self._stack.push_async_callback(scene.close)
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
        Activities(missions).register()
        settlement = BattleSettlement(scene, tables, scene.assets, missions)
        settlement.register()
        scene.inbox = SceneInbox(scene, peer, settlement, scene.assets, poll_seconds=5 if self.local else 60)
        skills = SceneSkills(scene, tables)
        skills.register()
        scene.skill_casts = skills.casts
        scene.buffs = skills.casts.buffs
        scene.buffs.visibility_changed = scene.refresh_actor_visibility
        self._stack.push_async_callback(skills.close)

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

    async def _scene_loop(self) -> None:
        previous = time.monotonic()
        while True:
            await asyncio.sleep(self.settings.scene_tick_seconds)
            now = time.monotonic()
            await self.scene.tick(min(now - previous, 0.25))
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
        except Exception:
            logger.warning("Scene 命令执行失败", exc_info=True)

    async def _battle(self) -> None:
        info = self._info("battle", BATTLE)
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
            )
            self._battle_control = control
            self._stack.push_async_callback(control.close)
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
            tables=ConfigTables(),
        )
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
        self._stack.push_async_callback(clients.close)
        await self._listen_tcp("battle", info, clients.serve)
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
