"""纯 Python 全微服务端到端联调测试：零 Go 进程，经真实 grpc.aio 环回贯通全部移植服务。"""

from __future__ import annotations

import asyncio
import time
from contextlib import AsyncExitStack
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from data_service_fakes import MemoryStore as DataServiceMemoryStore
from test_scene_manager import FakeNodeRegistry, FakePublisher, FakeSceneNodes
from xuanming.chat.config import ChatConfig
from xuanming.chat.service import ChatService
from xuanming.chat.store import GrpcActorVerifier, GrpcContentReviewer, MemoryChatStore
from xuanming.client_rpc_router.config import RouterConfig
from xuanming.client_rpc_router.discovery import NodeEntry, TargetDirectory
from xuanming.client_rpc_router.service import ClientRpcRouterService
from xuanming.content_guard.config import ContentGuardConfig
from xuanming.content_guard.engine import Engine as ContentGuardEngine
from xuanming.content_guard.service import ContentGuardService
from xuanming.data.player_store import DataServicePlayerStore
from xuanming.data_service.service import DataService
from xuanming.db.service import DataServiceRouteResolver, DbService
from xuanming.db.store import MemoryPlayerStore
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata, sign_service_call
from xuanming.login.caller_auth import CallerVerifier
from xuanming.login.directory import DataService as LoginDirectoryClient
from xuanming.login.sessions import PlayerLocator as LoginLocatorClient
from xuanming.player_inbox.notify import NullInboxNotifier
from xuanming.player_inbox.registry import InboxRegistry
from xuanming.player_inbox.service import PlayerInboxService
from xuanming.player_inbox.store import GrpcDataServiceDirectory, MemoryInboxStore
from xuanming.player_locator.config import PlayerLocatorConfig
from xuanming.player_locator.service import PlayerLocatorService
from xuanming.player_locator.store import MemorySessionStore
from xuanming.protocol import Protocol
from xuanming.scene_manager.config import SceneManagerConfig
from xuanming.scene_manager.service import GrpcDataServiceClient, SceneManagerService
from xuanming.scene_manager.store import MemoryPlayerPlacementStore, MemorySceneControlStore
from xuanming.settings import Settings


def test_cluster_example_toml_loads_all_python_service_sections():
    """校验 config/cluster.example.toml 可一次性加载全部 19 个纯 Python 微服务配置段且端口互不冲突。"""
    settings = Settings.load(Path(__file__).parents[1] / "config" / "cluster.example.toml")
    grpc_sections = (
        "data_service_node", "router_node", "db_node", "player_locator_node",
        "player_inbox_node", "scene_manager_node", "match_node", "content_guard_node",
        "chat_node", "mail_node", "rank_node", "friend_node", "payment_node",
        "trade_node", "guild_node",
    )
    http_sections = ("mall_game_node", "gm_admin_node", "config_node", "satoken_auth_node")
    ports = []
    for name in grpc_sections:
        section = getattr(settings, name)
        assert isinstance(section, dict) and section, f"missing [{name}] in cluster.example.toml"
        ports.append((name, int(section["grpc_port"])))
    for name in http_sections:
        section = getattr(settings, name)
        assert isinstance(section, dict) and section, f"missing [{name}] in cluster.example.toml"
        ports.append((name, int(section["port"])))
    port_values = [port for _, port in ports]
    assert len(port_values) == len(set(port_values)), f"duplicate ports: {ports}"


def test_pure_python_cluster_end_to_end_grpc_chain():
    """真实 grpc.aio 多服务环回：DataService + DB + PlayerLocator + SceneManager + PlayerInbox + ContentGuard + Chat + Router。"""
    async def exercise():
        protocol = Protocol()
        gate_key = Ed25519PrivateKey.generate()
        verifier = CallerVerifier((gate_key.public_key(),), allowed_callers=("gate",))

        secrets = {
            "login": b"login-e2e-secret-0123456789abcde",
            "scene": b"scene-e2e-secret-0123456789abcde",
            "scene_manager": b"sm-e2e-secret-0123456789abcdefgh",
            "db": b"db-e2e-secret-0123456789abcdefgh",
            "player_inbox": b"inbox-e2e-secret-0123456789abcde",
            "chat": b"chat-e2e-secret-0123456789abcdef",
            "match": b"match-e2e-secret-0123456789abcde",
            "coordinator": b"coord-e2e-secret-0123456789abcde",
            "player_store_scene": b"pstore-scene-secret-0123456789ab",
            "gate_token": b"gate-token-secret-0123456789abcd",
        }

        async with AsyncExitStack() as stack:
            # 1) 启动 DataService
            ds_store = DataServiceMemoryStore(protocol)
            ds_svc = DataService(protocol, ds_store, secrets, zone_refresh=60.0)
            await ds_svc.start()
            stack.push_async_callback(ds_svc.close)
            ds_server, ds_port = await serve_grpc(protocol, "127.0.0.1:0", ds_svc.services(), pass_context=True)
            stack.push_async_callback(ds_server.stop, 0)
            ds_client = GrpcClient(f"127.0.0.1:{ds_port}", timeout=5)
            stack.push_async_callback(ds_client.close)

            # 2) 启动 DB (PlayerStore) 并对接真实 DataService.GetPlayerRoute
            db_store = MemoryPlayerStore(protocol)
            db_resolver = DataServiceRouteResolver(protocol, ds_client.call, secrets["db"])
            db_svc = DbService(protocol, db_store, db_resolver, secrets["player_store_scene"])
            db_server, db_port = await serve_grpc(protocol, "127.0.0.1:0", db_svc.services(), pass_context=True)
            stack.push_async_callback(db_server.stop, 0)
            db_client = GrpcClient(f"127.0.0.1:{db_port}", timeout=5)
            stack.push_async_callback(db_client.close)

            # 3) 启动 PlayerLocator
            loc_store = MemorySessionStore()
            loc_svc = PlayerLocatorService(protocol, loc_store, PlayerLocatorConfig(advertise_host="127.0.0.1"))
            loc_server, loc_port = await serve_grpc(protocol, "127.0.0.1:0", loc_svc.services(), pass_context=True)
            stack.push_async_callback(loc_server.stop, 0)
            loc_client = GrpcClient(f"127.0.0.1:{loc_port}", timeout=5)
            stack.push_async_callback(loc_client.close)

            # 4) 启动 SceneManager 并对接真实 DataService.LookupAccountRole / GetPlayerHomeZone
            sm_ctl = MemorySceneControlStore()
            sm_player = MemoryPlayerPlacementStore()
            sm_nodes = FakeSceneNodes()
            sm_registry = FakeNodeRegistry(protocol)
            sm_registry.add_scene_node(zone_id=1, node_id=201, node_uuid="scene-u1", grpc_port=18612)
            sm_publisher = FakePublisher()
            sm_ds = GrpcDataServiceClient(protocol, ds_client, secrets["scene_manager"])
            sm_svc = SceneManagerService(
                protocol,
                SceneManagerConfig(zone_id=1, advertise_host="127.0.0.1", world_conf_ids=(1,), gate_token_secret=secrets["gate_token"].decode()),
                control_store=sm_ctl,
                player_store=sm_player,
                data_service=sm_ds,
                scene_nodes=sm_nodes,
                node_registry=sm_registry,
                publisher=sm_publisher,
            )
            await sm_svc.sync_nodes_to_control_store(1)
            await sm_svc.init_world_scenes(1)
            sm_server, sm_port = await serve_grpc(protocol, "127.0.0.1:0", sm_svc.services(), pass_context=True)
            stack.push_async_callback(sm_server.stop, 0)
            sm_client = GrpcClient(f"127.0.0.1:{sm_port}", timeout=5)
            stack.push_async_callback(sm_client.close)

            # 5) 启动 PlayerInbox 并对接真实 DataService (VerifyRoleAdmission / GetRoleFenceState / GetPlayerHomeZone)
            inbox_dir = GrpcDataServiceDirectory(protocol, ds_client.call, secrets["player_inbox"])
            inbox_registry = InboxRegistry(protocol)
            inbox_store = MemoryInboxStore(
                protocol, inbox_registry, inbox_dir, allowed_types=frozenset({4}), notifier=NullInboxNotifier(),
            )
            inbox_svc = PlayerInboxService(
                protocol,
                inbox_store,
                scene_secret=secrets["scene"],
                coordinator_secret=secrets["coordinator"],
                match_secret=secrets["match"],
            )
            inbox_server, inbox_port = await serve_grpc(protocol, "127.0.0.1:0", inbox_svc.services(), pass_context=True)
            stack.push_async_callback(inbox_server.stop, 0)
            inbox_client = GrpcClient(f"127.0.0.1:{inbox_port}", timeout=5)
            stack.push_async_callback(inbox_client.close)

            # 6) 启动 ContentGuard + Chat + ClientRpcRouter
            cg_svc = ContentGuardService(protocol, ContentGuardEngine())
            cg_server, cg_port = await serve_grpc(protocol, "127.0.0.1:0", cg_svc.services(), pass_context=True)
            stack.push_async_callback(cg_server.stop, 0)
            cg_client = GrpcClient(f"127.0.0.1:{cg_port}", timeout=5)
            stack.push_async_callback(cg_client.close)

            chat_cfg = ChatConfig(
                advertise_host="127.0.0.1",
                content_guard=f"127.0.0.1:{cg_port}",
                content_guard_optional=False,
                role_fence_secret=secrets["chat"],
            )
            chat_actor = GrpcActorVerifier(protocol, ds_client.call, secrets["chat"])
            chat_reviewer = GrpcContentReviewer(protocol, cg_client.call)
            chat_store = MemoryChatStore(protocol, chat_actor, reviewer=chat_reviewer)
            chat_svc = ChatService(protocol, chat_store, verifier)
            chat_server, chat_port = await serve_grpc(protocol, "127.0.0.1:0", chat_svc.services(), pass_context=True)
            stack.push_async_callback(chat_server.stop, 0)

            chat_dir = TargetDirectory("ChatNodeService")
            chat_dir.put(
                b"ChatNodeService.rpc/zone/1/node_type/9/node_id/112",
                NodeEntry(112, 9, "chat-u1", 1, f"127.0.0.1:{chat_port}", "stable"),
            )
            router_svc = ClientRpcRouterService(
                protocol, RouterConfig(advertise_host="127.0.0.1"), {9: chat_dir}, self_uuid="router-u1",
            )
            stack.push_async_callback(router_svc.pool.close)
            router_server, router_port = await serve_grpc(
                protocol, "127.0.0.1:0", router_svc.services(), pass_context=True,
            )
            stack.push_async_callback(router_server.stop, 0)
            router_client = GrpcClient(f"127.0.0.1:{router_port}", timeout=5)
            stack.push_async_callback(router_client.close)

            # === 全链路交互验证 ===
            # A) Login -> DataService 建角（发号 + 注册区服路由 + 占名 + 建角提交）
            login_dir = LoginDirectoryClient(protocol, ds_client.call, secrets["login"])
            pid = await login_dir.mint_player_id()
            await login_dir.register_player_zone(pid, 1, 1_000_000)
            assert await login_dir.reserve_name(pid, "玄冥侠客") == (0, 0)
            role = protocol.message(
                "AccountSimplePlayer", player_id=pid, class_id=1, gender=1, zone_id=1,
                name="玄冥侠客", appearance_id="01_hero",
            )
            roles = await login_dir.create_role("acc_e2e", role)
            assert len(roles) == 1 and roles[0].player_id == pid

            # B) Login -> PlayerLocator 写入在线态
            locator = LoginLocatorClient(protocol, loc_client.call)
            session_id = (1 << 17) | 1
            sess = protocol.message(
                "playerlocator.PlayerSession",
                player_id=pid,
                account="acc_e2e",
                session_id=session_id,
                gate_id="1",
                gate_instance_id="gate-u1",
                session_incarnation=1,
                state=1,
                session_version=1,
                login_generation=1,
                gate_zone_id=1,
            )
            await locator.set(sess)
            loaded_sess = await locator.get(pid)
            assert loaded_sess is not None and loaded_sess.session_id == session_id

            # C) Login -> SceneManager EnterScene（走真实 DataService 校验归属与归属区，下发 Kafka RoutePlayerEvent）
            enter_req = protocol.message(
                "scene_manager.EnterSceneRequest",
                player_id=pid,
                session_id=session_id,
                request_id="req-enter-1",
                gate_id="1",
                gate_instance_id="gate-u1",
                gate_zone_id=1,
                zone_id=1,
                login_generation=1,
                owner_revision=1,
                correlation_id=101,
            )
            enter_raw = await sm_client.call("/scene_manager.SceneManager/EnterScene", enter_req.SerializeToString())
            enter_resp = protocol.parse("scene_manager.EnterSceneResponse", enter_raw)
            assert enter_resp.error_code == 0 and enter_resp.player_id == pid and enter_resp.correlation_id == 101
            assert sm_publisher.gate_commands and sm_publisher.gate_commands[-1][:2] == (1, pid)
            route_event = protocol.parse("contracts.kafka.RoutePlayerEvent", sm_publisher.gate_commands[-1][2].payload)
            assert route_event.scene_id > 0 and route_event.owner_epoch > 0

            # D) Scene -> DB (PlayerStore) 认领并存盘（DB 内部向真实 DataService 查 GetPlayerRoute）
            db_store.seed_player(1_000_000, pid, rows={4: b"init"})
            scene_store = DataServicePlayerStore(protocol, db_client.call, secrets["player_store_scene"])
            snap = await scene_store.load_and_claim(pid, b"scene-u1", owner_epoch=route_event.owner_epoch)
            assert snap.owner_epoch == route_event.owner_epoch and snap.rows[4] == b"init"
            saved_snap = await scene_store.commit(snap, {4: b"init", 6: b"char-v2"}, force=True)
            assert saved_snap.save_seq > 0 and saved_snap.rows[6] == b"char-v2"

            # E) Scene -> DataService 登记入场 -> Match -> PlayerInbox 投递战斗结算 -> Scene 拉取并确认
            identity = protocol.message(
                "common.base.RoleAdmissionIdentity",
                player_id=pid,
                scene_instance_id="11111111-1111-1111-1111-111111111111",
                generation=1,
                owner_revision=1,
                owner_epoch=route_event.owner_epoch,
                scene_node_id="scene-201",
                scene_id=route_event.scene_id,
                zone_id=1,
                session_id=session_id,
                login_generation=1,
            )
            reg_req = protocol.message("data_service.RegisterRoleAdmissionRequest", identity=identity)
            sign_service_call(
                "/data_service.DataService/RegisterRoleAdmission",
                reg_req, "scene", time.time_ns() // 1_000_000, secrets["scene"],
            )
            await ds_client.call(
                "/data_service.DataService/RegisterRoleAdmission", reg_req.SerializeToString(deterministic=True),
            )

            now_ms = time.time_ns() // 1_000_000
            settlement = protocol.message(
                "BattleSettlementData", battle_id=777, player_id=pid, outcome=1,
                player_team_index=0, finished_at_ms=now_ms, match_mode=4, battle_config_id=1,
            )
            enq_req = protocol.message(
                "player_inbox.BattleSettlementInboxRequest",
                player_id=pid,
                battle_id=777,
                issued_at_ms=now_ms,
                expected_owner_account="acc_e2e",
                expected_owner_revision=1,
                settlement_payload=settlement.SerializeToString(deterministic=True),
            )
            sign_service_call(
                "/player_inbox.PlayerInbox/EnqueueBattleSettlement", enq_req, "match", now_ms, secrets["match"],
            )
            enq_raw = await inbox_client.call(
                "/player_inbox.PlayerInbox/EnqueueBattleSettlement", enq_req.SerializeToString(deterministic=True),
            )
            enq_resp = protocol.parse("player_inbox.BattleSettlementInboxResponse", enq_raw)
            assert enq_resp.seq > 0 and enq_resp.state == 1

            fetch_req = protocol.message(
                "player_inbox.FetchPlayerInboxRequest",
                player_id=pid, domain=2, owner_epoch=route_event.owner_epoch,
                correlation_id=1, scene_node_id=201, max_items=1, admission=identity,
            )
            sign_service_call("/player_inbox.PlayerInbox/FetchItems", fetch_req, "scene", now_ms, secrets["scene"])
            fetch_raw = await inbox_client.call(
                "/player_inbox.PlayerInbox/FetchItems", fetch_req.SerializeToString(deterministic=True),
            )
            fetch_resp = protocol.parse("player_inbox.FetchPlayerInboxResponse", fetch_raw)
            assert len(fetch_resp.items) == 1 and fetch_resp.items[0].seq == enq_resp.seq

            # F) Gate -> ClientRpcRouter -> Chat -> ContentGuard + DataService（真实签名转发与敏感词过滤）
            details = protocol.message(
                "SessionDetails",
                session_id=session_id,
                player_id=pid,
                gate_instance_id="gate-u1",
                session_incarnation=1,
                authenticated_account="acc_e2e",
                owner_revision=1,
            )
            send_chat_id = protocol.message_ids["ClientPlayerChatSendChat"]
            chat_req = protocol.message(
                "chatpb.SendChatRequest",
                request_id="chat-e2e-1",
                message={"channel": 1, "content": "大家好，纯 Python 集群启动成功！"},
            )
            fwd_req = protocol.message(
                "client_rpc_router.ForwardRequest",
                zone_id=1,
                release_track="stable",
                request={"id": 42, "message_id": send_chat_id, "body": chat_req.SerializeToString()},
            )
            md = session_metadata(details.SerializeToString(), "/chatpb.ClientPlayerChat/SendChat", gate_key)
            call = router_client.request(
                "/client_rpc_router.ClientRpcRouter/Forward", fwd_req.SerializeToString(), metadata=md,
            )
            fwd_raw = await call
            assert dict(await call.initial_metadata()).get("x-session-detail-bin") == md[0][1]
            envelope = protocol.parse("MessageContent", fwd_raw)
            assert envelope.id == 42 and envelope.error_message.id == 0
            chat_resp = protocol.parse("chatpb.SendChatResponse", envelope.serialized_message)
            assert chat_resp.error_message.id == 0

    asyncio.run(exercise())
