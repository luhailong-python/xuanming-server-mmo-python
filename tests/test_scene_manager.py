"""scene_manager 全链路与状态机回归测试（真实 serve_grpc + 两域内存孪生 + 协议契约校验）。"""

from __future__ import annotations

import asyncio
import hmac
from dataclasses import dataclass

import grpc
import pytest

from xuanming.data_service.service import DataService
from xuanming.grpc_runtime import GrpcClient, serve_grpc, verify_service_call
from xuanming.nodes.scene_channels import SceneChannels
from xuanming.protocol import Protocol
from xuanming.scene_manager import (
    AccountRoleInfo,
    CrossDomainRedisKeyError,
    DeadHolderReleaseBlockedError,
    DeadHolderReleaseOutcome,
    DeadHolderReleaser,
    DeadNodeSweeper,
    ERR_CHANNEL_FULL,
    ERR_CHANNEL_SWITCH_COOLDOWN,
    ERR_CHANNEL_UNAVAILABLE,
    ERR_ENTER_SCENE_IDEMPOTENCY_CONFLICT,
    ERR_HANDOFF_PENDING,
    ERR_HOME_ZONE_UNAVAILABLE,
    ERR_KAFKA_ROUTE,
    ERR_NO_AVAILABLE_NODE,
    ERR_OK,
    ERR_PLACEMENT_SESSION_SUPERSEDED,
    ERR_SCENE_REENTRY_BARRIER,
    ERR_SOURCE_SCENE_GONE,
    EVENT_ID_PLAYER_HOSTED_PLACEMENT,
    EVENT_ID_REDIRECT_TO_GATE,
    EVENT_ID_ROUTE_PLAYER,
    GATE_NODE_TYPE,
    GrpcDataServiceClient,
    GrpcSceneNodeClient,
    HomeZoneNotFoundError,
    IdleInstanceSweeper,
    KafkaCommandPublisher,
    MemoryPlayerPlacementStore,
    MemorySceneControlStore,
    PlayerStoreHolderInfo,
    SCENE_CHANNEL_STATE_BUSY,
    SCENE_CHANNEL_STATE_CLOSING,
    SCENE_CHANNEL_STATE_FULL,
    SCENE_CHANNEL_STATE_SMOOTH,
    SCENE_CHANNEL_STATE_UNAVAILABLE,
    SCENE_NODE_TYPE,
    SCENE_NODE_TYPE_INSTANCE,
    SCENE_NODE_TYPE_MAIN_WORLD,
    SCENE_TYPE_INSTANCE,
    SCENE_TYPE_MAIN_WORLD,
    SceneIdGenerator,
    SceneManagerConfig,
    SceneManagerService,
    TeardownJobSweeper,
    WorldChannelDirectoryPublisher,
    keys,
    load_world_conf_ids,
)
from tests.data_service_fakes import MemoryStore as DataMemoryStore


# ── 测试夹具与替身 ─────────────────────────────────────────────────────────────

class FakeClock:
    def __init__(self, start_ms: int = 1_750_000_000_000) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance_sec(self, seconds: int) -> None:
        self.now_ms += seconds * 1000


class FakeDataService:
    def __init__(self) -> None:
        self.roles: dict[int, AccountRoleInfo] = {}
        self.home_zones: dict[int, int] = {}
        self.lookup_calls: list[int] = []
        self.home_zone_calls: list[int] = []

    def register_player(self, player_id: int, *, revision: int = 1, home_zone: int | None = 1, account: str = "acc-1") -> None:
        self.roles[player_id] = AccountRoleInfo(player_id=player_id, account=account, owner_revision=revision)
        if home_zone is not None:
            self.home_zones[player_id] = home_zone

    async def lookup_account_role(self, player_id: int) -> AccountRoleInfo:
        self.lookup_calls.append(player_id)
        if player_id not in self.roles:
            raise LookupError(f"role {player_id} missing")
        return self.roles[player_id]

    async def get_player_home_zone(self, player_id: int) -> int:
        self.home_zone_calls.append(player_id)
        if player_id not in self.home_zones:
            raise HomeZoneNotFoundError(f"no home zone mapping for {player_id}")
        return self.home_zones[player_id]


@dataclass(frozen=True)
class ReleaseCall:
    zone_id: int
    node_id: str
    player_id: int
    target_scene_id: int
    target_node_id: str
    expected_owner_epoch: int
    session_ended: bool
    covered_session_id: int
    covered_login_generation: int


class FakeSceneNodes:
    def __init__(self) -> None:
        self.created: list[tuple[int, str, int, int, int, tuple[int, ...]]] = []
        self.destroyed: list[tuple[int, str, int]] = []
        self.releases: list[ReleaseCall] = []
        self.fail_create_nodes: set[str] = set()
        self.fail_destroy_nodes: set[str] = set()

    async def create_scene(
        self, zone_id: int, node_id: str, *, config_id: int, scene_id: int,
        mirror_config_id: int = 0, creator_ids=(), placement_generation: int = 0,
    ) -> None:
        if node_id in self.fail_create_nodes:
            raise RuntimeError(f"simulated CreateScene failure on node {node_id}")
        self.created.append((zone_id, node_id, config_id, scene_id, mirror_config_id, tuple(creator_ids)))

    async def destroy_scene(self, zone_id: int, node_id: str, *, scene_id: int, placement_generation: int = 0) -> None:
        if node_id in self.fail_destroy_nodes:
            raise RuntimeError(f"simulated DestroyScene failure on node {node_id}")
        self.destroyed.append((zone_id, node_id, scene_id))

    async def release_player(
        self, zone_id: int, node_id: str, *, player_id: int,
        target_scene_id: int = 0, target_node_id: str = "", expected_owner_epoch: int = 0,
        session_ended: bool = False, covered_session_id: int = 0,
        covered_login_generation: int = 0, keep_held: bool = False,
    ) -> None:
        self.releases.append(
            ReleaseCall(
                zone_id=zone_id,
                node_id=node_id,
                player_id=player_id,
                target_scene_id=target_scene_id,
                target_node_id=target_node_id,
                expected_owner_epoch=expected_owner_epoch,
                session_ended=session_ended,
                covered_session_id=covered_session_id,
                covered_login_generation=covered_login_generation,
            )
        )


class FakeNodeRegistry:
    def __init__(self, protocol: Protocol) -> None:
        self.protocol = protocol
        self.scene_nodes: dict[int, list] = {}
        self.gate_nodes: dict[int, list] = {}
        self.gate_ws_urls: dict[tuple[int, int], str] = {}
        self.synced = True

    def add_scene_node(
        self, *, zone_id: int, node_id: int, scene_node_type: int = SCENE_NODE_TYPE_MAIN_WORLD,
        node_uuid: str = "", grpc_port: int = 9001, tcp_port: int = 8001, draining: bool = False,
    ):
        info = self.protocol.message(
            "NodeInfo",
            node_id=node_id,
            node_type=SCENE_NODE_TYPE,
            zone_id=zone_id,
            scene_node_type=scene_node_type,
            node_uuid=node_uuid or f"scene-uuid-{zone_id}-{node_id}",
            admission=2 if draining else 1,
        )
        info.endpoint.ip = "127.0.0.1"
        info.endpoint.port = tcp_port
        if grpc_port > 0:
            info.grpc_endpoint.ip = "127.0.0.1"
            info.grpc_endpoint.port = grpc_port
        self.scene_nodes.setdefault(zone_id, []).append(info)
        return info

    def remove_scene_node(self, zone_id: int, node_id: int) -> None:
        self.scene_nodes[zone_id] = [n for n in self.scene_nodes.get(zone_id, []) if n.node_id != node_id]

    def add_gate_node(
        self, *, zone_id: int, node_id: int, ip: str = "10.0.0.1", port: int = 7001,
        client_ip: str = "", client_port: int = 0, player_count: int = 0, launch_time: int = 100,
        ws_url: str = "",
    ):
        info = self.protocol.message(
            "NodeInfo",
            node_id=node_id,
            node_type=GATE_NODE_TYPE,
            zone_id=zone_id,
            node_uuid=f"gate-uuid-{zone_id}-{node_id}",
            player_count=player_count,
            launch_time=launch_time,
        )
        info.endpoint.ip = ip
        info.endpoint.port = port
        if client_ip and client_port:
            info.client_endpoint.ip = client_ip
            info.client_endpoint.port = client_port
        self.gate_nodes.setdefault(zone_id, []).append(info)
        if ws_url:
            self.gate_ws_urls[(zone_id, node_id)] = ws_url
        return info

    async def list_scene_nodes(self, zone_id: int) -> list:
        return list(self.scene_nodes.get(zone_id, ()))

    async def list_gate_nodes(self, zone_id: int) -> list:
        return list(self.gate_nodes.get(zone_id, ()))

    async def resolve_scene_grpc_endpoint(self, zone_id: int, node_id: str) -> str:
        matches = [n for n in self.scene_nodes.get(zone_id, ()) if str(n.node_id) == node_id]
        if len(matches) != 1:
            raise LookupError(f"scene node {node_id} not uniquely registered in zone {zone_id}")
        info = matches[0]
        if not info.HasField("grpc_endpoint") or not info.grpc_endpoint.ip or info.grpc_endpoint.port <= 0:
            raise RuntimeError(f"scene node {node_id} has no grpc_endpoint")
        return f"{info.grpc_endpoint.ip}:{info.grpc_endpoint.port}"

    async def get_unique_scene_instance_uuid(self, zone_id: int, node_id: str) -> str | None:
        matches = [n for n in self.scene_nodes.get(zone_id, ()) if str(n.node_id) == node_id]
        return matches[0].node_uuid if len(matches) == 1 and matches[0].node_uuid else None

    def is_node_registered(self, zone_id: int, node_id: str) -> bool:
        return any(str(n.node_id) == node_id for n in self.scene_nodes.get(zone_id, ()))

    def is_node_identity_ambiguous(self, zone_id: int, node_id: str) -> bool:
        return sum(1 for n in self.scene_nodes.get(zone_id, ()) if str(n.node_id) == node_id) > 1

    def is_zone_gone(self, zone_id: int) -> bool:
        return self.synced and zone_id in self.scene_nodes and not self.scene_nodes[zone_id]

    def is_node_draining(self, zone_id: int, node_id: str) -> bool:
        return any(str(n.node_id) == node_id and n.admission == 2 for n in self.scene_nodes.get(zone_id, ()))

    def is_synced(self) -> bool:
        return self.synced

    def get_gate_ws_url(self, zone_id: int, gate_node_id: int) -> str:
        return self.gate_ws_urls.get((zone_id, gate_node_id), "")


class FakePublisher:
    def __init__(self) -> None:
        self.gate_commands: list[tuple[int, int, object]] = []
        self.scene_commands: list[tuple[int, int, object]] = []
        self.fail_next: Exception | None = None

    async def publish_gate_command(self, gate_node_id: int, player_id: int, command) -> None:
        if self.fail_next is not None:
            err, self.fail_next = self.fail_next, None
            raise err
        self.gate_commands.append((gate_node_id, player_id, command))

    async def publish_scene_command(self, scene_node_id: int, player_id: int, command) -> None:
        if self.fail_next is not None:
            err, self.fail_next = self.fail_next, None
            raise err
        self.scene_commands.append((scene_node_id, player_id, command))


class Harness:
    """运行真实 serve_grpc(pass_context=True) 与 GrpcClient 的集成测试夹具。"""

    def __init__(
        self,
        *,
        cfg: SceneManagerConfig | None = None,
        data_service=None,
        dead_holder_releaser: DeadHolderReleaser | None = None,
        clock: FakeClock | None = None,
        protocol: Protocol | None = None,
    ) -> None:
        self.protocol = protocol or Protocol()
        self.clock = clock or FakeClock()
        self.control = MemorySceneControlStore(clock_ms=self.clock)
        self.players = MemoryPlayerPlacementStore(clock_ms=self.clock)
        self.data = data_service if data_service is not None else FakeDataService()
        self.scene_nodes = FakeSceneNodes()
        self.registry = FakeNodeRegistry(self.protocol)
        self.publisher = FakePublisher()
        self.config = cfg or SceneManagerConfig(
            zone_id=1,
            gate_token_secret=b"S" * 32,
            world_conf_ids=(1, 2),
            strict_node_type_separation=True,
            reentry_barrier_sec=20,
        )
        self.service = SceneManagerService(
            self.protocol,
            self.config,
            control_store=self.control,
            player_store=self.players,
            data_service=self.data,
            scene_nodes=self.scene_nodes,
            node_registry=self.registry,
            publisher=self.publisher,
            id_gen=SceneIdGenerator(1, clock_ms=self.clock),
            clock_ms=self.clock,
            dead_holder_releaser=dead_holder_releaser,
        )
        self.server = None
        self.client: GrpcClient | None = None

    async def __aenter__(self) -> "Harness":
        self.server, port = await serve_grpc(
            self.protocol,
            "127.0.0.1:0",
            self.service.services(),
            pass_context=True,
        )
        self.client = GrpcClient(f"127.0.0.1:{port}", timeout=5)
        return self

    async def __aexit__(self, *exc) -> None:
        await self.service.close()
        if self.client is not None:
            await self.client.close()
        if self.server is not None:
            await self.server.stop(1)

    async def create_scene(self, **fields):
        req = self.protocol.message("scene_manager.CreateSceneRequest", **fields)
        raw = await self.client.call("/scene_manager.SceneManager/CreateScene", req.SerializeToString())
        return self.protocol.parse("scene_manager.CreateSceneResponse", raw)

    async def destroy_scene(self, **fields):
        req = self.protocol.message("scene_manager.DestroySceneRequest", **fields)
        raw = await self.client.call("/scene_manager.SceneManager/DestroyScene", req.SerializeToString())
        return self.protocol.parse("Empty", raw)

    async def enter_scene(self, **fields):
        req = self.protocol.message("scene_manager.EnterSceneRequest", **fields)
        raw = await self.client.call("/scene_manager.SceneManager/EnterScene", req.SerializeToString())
        return self.protocol.parse("scene_manager.EnterSceneResponse", raw)

    async def leave_scene(self, **fields):
        req = self.protocol.message("scene_manager.LeaveSceneRequest", **fields)
        raw = await self.client.call("/scene_manager.SceneManager/LeaveScene", req.SerializeToString())
        return self.protocol.parse("Empty", raw)

    async def read_location(self, player_id: int):
        raw, _ = await self.players.read_location_snapshot(player_id)
        if not raw:
            return None
        return self.protocol.parse("storage.PlayerLocation", raw)


# ==============================================================================
# 1. 两域键格式、隔离守卫与 World.json 加载测试
# ==============================================================================

def test_two_domain_keys_and_world_table():
    assert keys.scene_node_key(1, 500) == "scene:{ctl:1}:500:node"
    assert keys.instance_members_key(1, 500) == "instance:{ctl:1}:500:members"
    assert keys.location_key(1001) == "player:{1001}:location"
    assert keys.enter_scene_dedup_key(1001, "req-1") == "enter_scene:dedup:{1001}:req-1"

    keys.assert_control_domain_keys([keys.scene_node_key(1, 500), keys.instance_members_key(1, 500)])
    keys.assert_player_domain_keys([keys.location_key(1001), keys.owner_epoch_key(1001), keys.handoff_key(1001)])

    with pytest.raises(CrossDomainRedisKeyError):
        keys.assert_control_domain_keys([keys.scene_node_key(1, 500), keys.location_key(1001)])
    with pytest.raises(CrossDomainRedisKeyError):
        keys.assert_player_domain_keys([keys.location_key(1001), keys.scene_node_key(1, 500)])
    with pytest.raises(CrossDomainRedisKeyError):
        keys.assert_control_domain_keys([keys.scene_node_key(1, 500), keys.scene_node_key(2, 501)])

    world_ids = load_world_conf_ids()
    assert len(world_ids) >= 1 and world_ids[0] == 1


# ==============================================================================
# 2. 世界频道自动初始化 + CreateScene / DestroyScene（含镜像共置、级联与回滚）
# ==============================================================================

def test_world_init_create_destroy_mirror_and_rollback():
    async def scenario():
        cfg = SceneManagerConfig(
            zone_id=1,
            gate_token_secret=b"S" * 32,
            world_channel_count=2,
            strict_node_type_separation=True,
        )
        async with Harness(cfg=cfg) as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            h.registry.add_scene_node(zone_id=1, node_id=11, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            h.registry.add_scene_node(zone_id=1, node_id=20, scene_node_type=SCENE_NODE_TYPE_INSTANCE)
            await h.service.sync_nodes_to_control_store(1)

            created = await h.service.init_world_scenes(1)
            world_ids = load_world_conf_ids()
            assert created == len(world_ids) * 2

            # 主世界 CreateScene 返回最空频道
            wresp = await h.create_scene(scene_conf_id=1, scene_type=SCENE_TYPE_MAIN_WORLD, zone_id=1)
            assert wresp.error_code == ERR_OK
            assert wresp.scene_id > 0
            assert wresp.node_id in ("10", "11")

            # DestroyScene 拒绝销毁大世界频道
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await h.destroy_scene(scene_id=wresp.scene_id, zone_id=1)
            assert exc_info.value.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert await h.control.scene_node_exists(1, wresp.scene_id)

            # 创建普通副本 -> 落在 INSTANCE 节点 20
            inst = await h.create_scene(scene_conf_id=9001, scene_type=SCENE_TYPE_INSTANCE, zone_id=1, creator_ids=[1001])
            assert inst.error_code == ERR_OK
            assert inst.node_id == "20"
            assert list(inst.creator_ids) == [1001]

            # 源克隆镜像：源不存在时拒绝 ERR_SOURCE_SCENE_GONE
            missing_src = await h.create_scene(
                scene_conf_id=1, scene_type=SCENE_TYPE_INSTANCE, zone_id=1, source_scene_id=999999,
            )
            assert missing_src.error_code == ERR_SOURCE_SCENE_GONE

            # 镜像共置：源在大世界节点 wresp.node_id，镜像跨类型共置到同一节点并回显 creator_ids
            mirror = await h.create_scene(
                scene_conf_id=1,
                scene_type=SCENE_TYPE_INSTANCE,
                zone_id=1,
                source_scene_id=wresp.scene_id,
                mirror_config_id=77,
                creator_ids=[1001, 1002],
            )
            assert mirror.error_code == ERR_OK
            assert mirror.node_id == wresp.node_id
            assert list(mirror.creator_ids) == [1001, 1002]
            assert str(mirror.scene_id) in await h.control.get_scene_mirrors(1, wresp.scene_id)

            # 给副本挂一个子镜像，验证 DestroyScene 级联销毁子镜像并通知 SceneNodeGrpc
            sub_mirror = await h.create_scene(
                scene_conf_id=9001,
                scene_type=SCENE_TYPE_INSTANCE,
                zone_id=1,
                source_scene_id=inst.scene_id,
                mirror_config_id=88,
            )
            assert sub_mirror.error_code == ERR_OK
            assert sub_mirror.node_id == "20"
            await h.destroy_scene(scene_id=inst.scene_id, zone_id=1)
            assert not await h.control.scene_node_exists(1, inst.scene_id)
            assert not await h.control.scene_node_exists(1, sub_mirror.scene_id)
            destroyed_ids = {sid for _, _, sid in h.scene_nodes.destroyed}
            assert {inst.scene_id, sub_mirror.scene_id}.issubset(destroyed_ids)

            # SceneNodeGrpc.CreateScene 失败 -> 原子回滚控制域键、节点计数并补发 DestroyScene
            count_before = await h.control.get_node_scene_count(1, "20")
            h.scene_nodes.fail_create_nodes.add("20")
            failed = await h.create_scene(scene_conf_id=9002, scene_type=SCENE_TYPE_INSTANCE, zone_id=1)
            assert failed.error_code == ERR_NO_AVAILABLE_NODE
            assert await h.control.get_node_scene_count(1, "20") == count_before

    asyncio.run(scenario())


# ==============================================================================
# 3. EnterScene 首登、同落点重连、去重回显与修订校验
# ==============================================================================

def test_enter_scene_first_login_reconnect_dedupe_and_identity():
    async def scenario():
        async with Harness() as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)
            h.data.register_player(1001, revision=5, home_zone=1)

            # owner_revision == 0 -> INVALID_ARGUMENT
            with pytest.raises(grpc.aio.AioRpcError) as bad_rev:
                await h.enter_scene(player_id=1001, gate_id="4", gate_instance_id="g-1", gate_zone_id=1)
            assert bad_rev.value.code() == grpc.StatusCode.INVALID_ARGUMENT

            # 首登进场景：铸造 epoch、推 RoutePlayerEvent (event_id=11)、回显 player_id & correlation_id
            resp1 = await h.enter_scene(
                player_id=1001,
                scene_conf_id=1,
                session_id=101,
                request_id="req-login-1",
                gate_id="4",
                gate_instance_id="gate-uuid-4",
                gate_zone_id=1,
                correlation_id=7001,
                login_generation=10,
                owner_revision=5,
            )
            assert resp1.error_code == ERR_OK
            assert resp1.player_id == 1001
            assert resp1.correlation_id == 7001
            assert len(h.publisher.gate_commands) == 1
            gate_id, pid, cmd = h.publisher.gate_commands[0]
            assert (gate_id, pid, cmd.event_id, cmd.target_instance_id) == (4, 1001, EVENT_ID_ROUTE_PLAYER, "gate-uuid-4")
            route_ev = h.protocol.parse("contracts.kafka.RoutePlayerEvent", cmd.payload)
            loc1 = await h.read_location(1001)
            assert loc1 is not None and loc1.node_id == "10"
            assert route_ev.owner_epoch == loc1.owner_epoch == h.clock.now_ms
            assert route_ev.home_zone_id == 1 and route_ev.owner_revision == 5

            # 去重重放：同一 request_id + 不同 correlation_id/login_generation -> 重放成功，回显新 correlation_id，不重复推 Kafka
            replay = await h.enter_scene(
                player_id=1001,
                scene_conf_id=1,
                session_id=101,
                request_id="req-login-1",
                gate_id="4",
                gate_instance_id="gate-uuid-4",
                gate_zone_id=1,
                correlation_id=7002,
                login_generation=11,
                owner_revision=5,
            )
            assert replay.error_code == ERR_OK
            assert replay.player_id == 1001
            assert replay.correlation_id == 7002
            assert len(h.publisher.gate_commands) == 1

            # 同一 request_id 但业务参数不同 -> 16 幂等冲突（仍回显 player_id & correlation_id）
            conflict = await h.enter_scene(
                player_id=1001,
                scene_conf_id=2,
                session_id=101,
                request_id="req-login-1",
                gate_id="4",
                gate_instance_id="gate-uuid-4",
                gate_zone_id=1,
                correlation_id=7003,
                login_generation=11,
                owner_revision=5,
            )
            assert conflict.error_code == ERR_ENTER_SCENE_IDEMPOTENCY_CONFLICT
            assert conflict.player_id == 1001 and conflict.correlation_id == 7003

            # 同落点重连（更高 login_generation）：不铸造新 epoch，更新会话身份；即使 Kafka 失败也不回滚身份
            epoch_before = await h.players.get_owner_epoch(1001)
            h.publisher.fail_next = RuntimeError("kafka down")
            reconn_fail = await h.enter_scene(
                player_id=1001,
                scene_id=loc1.scene_id,
                session_id=202,
                gate_id="4",
                gate_instance_id="gate-uuid-4",
                gate_zone_id=1,
                correlation_id=7004,
                login_generation=20,
                owner_revision=5,
            )
            assert reconn_fail.error_code == ERR_KAFKA_ROUTE
            assert reconn_fail.player_id == 1001 and reconn_fail.correlation_id == 7004
            assert await h.players.get_owner_epoch(1001) == epoch_before
            loc_after_fail = await h.read_location(1001)
            assert (loc_after_fail.session_id, loc_after_fail.login_generation) == (202, 20)

            # 旧会话陈旧请求被 25 拦截
            stale = await h.enter_scene(
                player_id=1001,
                scene_id=loc1.scene_id,
                session_id=101,
                gate_id="4",
                gate_instance_id="gate-uuid-4",
                gate_zone_id=1,
                correlation_id=7005,
                login_generation=10,
                owner_revision=5,
            )
            assert stale.error_code == ERR_PLACEMENT_SESSION_SUPERSEDED
            assert stale.player_id == 1001 and stale.correlation_id == 7005

    asyncio.run(scenario())


# ==============================================================================
# 4. 跨节点两阶段交接（18 -> handoff -> route + 异步 ReleasePlayer）与 Kafka 失败单调回滚
# ==============================================================================

def test_cross_node_two_step_handoff_and_monotonic_rollback():
    async def scenario():
        async with Harness() as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            h.registry.add_scene_node(zone_id=1, node_id=20, scene_node_type=SCENE_NODE_TYPE_INSTANCE)
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)
            h.data.register_player(2001, revision=3, home_zone=1)

            inst = await h.create_scene(scene_conf_id=9001, scene_type=SCENE_TYPE_INSTANCE, zone_id=1)
            assert inst.error_code == ERR_OK and inst.node_id == "20"

            # 先落到大世界节点 10
            first = await h.enter_scene(
                player_id=2001,
                scene_conf_id=1,
                session_id=301,
                gate_id="4",
                gate_instance_id="gate-4",
                gate_zone_id=1,
                correlation_id=1,
                login_generation=1,
                owner_revision=3,
            )
            assert first.error_code == ERR_OK
            loc_a = await h.read_location(2001)
            epoch_a = loc_a.owner_epoch
            assert loc_a.node_id == "10"

            # 第一跳：无 handoff 标记 -> 返回 18 (ErrHandoffPending)，不改 location/epoch，不发 ReleasePlayer
            step1 = await h.enter_scene(
                player_id=2001,
                scene_id=inst.scene_id,
                session_id=301,
                gate_id="4",
                gate_instance_id="gate-4",
                gate_zone_id=1,
                correlation_id=2,
                login_generation=1,
                owner_revision=3,
            )
            assert step1.error_code == ERR_HANDOFF_PENDING
            assert step1.player_id == 2001 and step1.correlation_id == 2
            assert await h.players.get_owner_epoch(2001) == epoch_a
            assert await h.control.get_scene_member_count(1, inst.scene_id) == 0

            # 源节点存盘后写 handoff 标记；先模拟 Kafka 失败验证单调 bump 回滚 + rollback_receipt + 转写 handoff EX 300
            saved_at_ms = h.clock.now_ms
            marker = f"{epoch_a}:{saved_at_ms}"
            h.players.set_handoff_marker(2001, marker)
            h.clock.advance_sec(1)
            h.publisher.fail_next = RuntimeError("broker timeout")

            failed_route = await h.enter_scene(
                player_id=2001,
                scene_id=inst.scene_id,
                session_id=301,
                gate_id="4",
                gate_instance_id="gate-4",
                gate_zone_id=1,
                correlation_id=3,
                login_generation=1,
                owner_revision=3,
            )
            assert failed_route.error_code == ERR_KAFKA_ROUTE
            assert failed_route.owner_epoch_after_rollback >= epoch_a + 2
            epoch_after_rb = await h.players.get_owner_epoch(2001)
            assert epoch_after_rb == failed_route.owner_epoch_after_rollback
            loc_rb = await h.read_location(2001)
            assert loc_rb.scene_id == loc_a.scene_id and loc_rb.node_id == "10"
            assert loc_rb.owner_epoch == epoch_after_rb
            assert loc_rb.rollback_receipt == marker
            # 转写后的 handoff 标记与 EX 300
            forwarded = await h.players.get_handoff_marker(2001)
            assert forwarded == f"{epoch_after_rb}:{saved_at_ms}"
            assert 295 <= (h.players.get_handoff_ttl_remaining_sec(2001) or 0) <= 300
            # Kafka 失败不发 ReleasePlayer
            await h.service.drain_background_tasks()
            assert len(h.scene_nodes.releases) == 0

            # 第二跳（凭转写后的 handoff 标记重试成功）：推 RoutePlayerEvent + Kafka ACK 后异步通知旧节点 ReleasePlayer
            h.clock.advance_sec(1)
            step2 = await h.enter_scene(
                player_id=2001,
                scene_id=inst.scene_id,
                session_id=301,
                gate_id="4",
                gate_instance_id="gate-4",
                gate_zone_id=1,
                correlation_id=4,
                login_generation=1,
                owner_revision=3,
            )
            assert step2.error_code == ERR_OK
            assert step2.player_id == 2001 and step2.correlation_id == 4
            loc_b = await h.read_location(2001)
            assert loc_b.scene_id == inst.scene_id and loc_b.node_id == "20"
            assert loc_b.owner_epoch > epoch_after_rb
            await h.service.drain_background_tasks()
            assert len(h.scene_nodes.releases) == 1
            rel = h.scene_nodes.releases[0]
            assert (rel.node_id, rel.player_id, rel.expected_owner_epoch, rel.session_ended) == (
                "10", 2001, epoch_after_rb, False,
            )

    asyncio.run(scenario())


# ==============================================================================
# 5. 跨区 Gate 重定向（GateTokenPayload HMAC 签名 + ws_url + awaiting placement + RedirectToGateEvent=37）
# ==============================================================================

def test_cross_zone_gate_redirect_and_hmac_signature():
    async def scenario():
        secret = b"G" * 32
        cfg = SceneManagerConfig(zone_id=1, gate_token_secret=secret, world_conf_ids=(1, 2))
        async with Harness(cfg=cfg) as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            h.registry.add_scene_node(zone_id=2, node_id=21, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            h.registry.add_gate_node(
                zone_id=2,
                node_id=42,
                ip="10.2.0.42",
                port=8610,
                client_ip="203.0.113.42",
                client_port=18610,
                ws_url="wss://gate42.example.com/ws",
            )
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)
            h.data.register_player(3001, revision=2, home_zone=1)

            # 先在 zone 1 落点
            enter1 = await h.enter_scene(
                player_id=3001,
                scene_conf_id=1,
                session_id=401,
                gate_id="4",
                gate_instance_id="gate-1",
                gate_zone_id=1,
                correlation_id=10,
                login_generation=1,
                owner_revision=2,
            )
            assert enter1.error_code == ERR_OK
            loc1 = await h.read_location(3001)

            # 跨 zone 传送（未写 handoff 标记 -> 18）
            pending = await h.enter_scene(
                player_id=3001,
                zone_id=2,
                scene_conf_id=2,
                session_id=401,
                gate_id="4",
                gate_instance_id="gate-1",
                gate_zone_id=1,
                correlation_id=11,
                login_generation=1,
                owner_revision=2,
            )
            assert pending.error_code == ERR_HANDOFF_PENDING

            # 写 handoff 标记后再次跨 zone 传送 -> 签发 GateTokenPayload + 写 awaiting placement + 推 event_id=37
            h.players.set_handoff_marker(3001, f"{loc1.owner_epoch}:{h.clock.now_ms}")
            h.clock.advance_sec(1)
            redir = await h.enter_scene(
                player_id=3001,
                zone_id=2,
                scene_conf_id=2,
                session_id=401,
                gate_id="4",
                gate_instance_id="gate-1",
                gate_zone_id=1,
                correlation_id=12,
                login_generation=1,
                owner_revision=2,
            )
            assert redir.error_code == ERR_OK
            assert redir.redirect.target_gate_ip == "203.0.113.42"
            assert redir.redirect.target_gate_port == 18610
            assert redir.redirect.target_gate_ws_url == "wss://gate42.example.com/ws"

            # 验证 HMAC-SHA256 签名与 nodes/gate.py issue_gate_ticket 完全一致
            expected_sig = hmac.new(secret, redir.redirect.token_payload, "sha256").hexdigest().encode("ascii")
            assert hmac.compare_digest(expected_sig, redir.redirect.token_signature)
            token = h.protocol.parse("GateTokenPayload", redir.redirect.token_payload)
            assert (token.gate_node_id, token.zone_id, token.player_id, token.target_zone_id) == (42, 2, 3001, 2)

            # 验证 awaiting placement 已写入
            awaiting = await h.read_location(3001)
            assert awaiting.zone_id == 2 and awaiting.node_id == "" and awaiting.scene_id == 0
            assert awaiting.pending_scene_conf_id == 2 and awaiting.owner_epoch > loc1.owner_epoch

            # 验证 Kafka 推了 RedirectToGateEvent (event_id=37)
            gate_id, pid, cmd = h.publisher.gate_commands[-1]
            assert (gate_id, pid, cmd.event_id) == (4, 3001, EVENT_ID_REDIRECT_TO_GATE)

    asyncio.run(scenario())


# ==============================================================================
# 6. hosted_placement (event_id=53)、client_channel_pick (22/23/24) 与 evacuation
# ==============================================================================

def test_hosted_placement_channel_pick_and_evacuation():
    async def scenario():
        cfg = SceneManagerConfig(
            zone_id=1,
            gate_token_secret=b"H" * 32,
            world_conf_ids=(1,),
            world_channel_count=2,
            channel_max_players=1,
            channel_switch_cooldown_sec=30,
        )
        async with Harness(cfg=cfg) as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD, node_uuid="uuid-10")
            h.registry.add_scene_node(zone_id=1, node_id=11, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD, node_uuid="uuid-11")
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)
            channels = [int(s) for s in await h.control.get_world_channels(1, 1)]
            assert len(channels) == 2
            h.data.register_player(4001, revision=1, home_zone=1)
            h.data.register_player(4002, revision=1, home_zone=1)

            # hosted_placement：直接向目标 scene 节点发 SceneCommand(PlayerHostedPlacementEvent, event_id=53)
            hosted = await h.enter_scene(
                player_id=4001,
                scene_id=channels[0],
                session_id=501,
                gate_zone_id=1,
                correlation_id=21,
                login_generation=5,
                owner_revision=1,
                hosted_placement=True,
            )
            assert hosted.error_code == ERR_OK
            assert len(h.publisher.scene_commands) == 1
            snode, pid, scmd = h.publisher.scene_commands[0]
            assert (pid, scmd.event_id) == (4001, EVENT_ID_PLAYER_HOSTED_PLACEMENT)
            expected_owner = await h.control.get_scene_node(1, channels[0])
            assert snode == int(expected_owner)
            assert scmd.target_instance_id == f"uuid-{expected_owner}"
            hev = h.protocol.parse("PlayerHostedPlacementEvent", scmd.payload)
            assert (hev.player_id, hev.scene_id, hev.home_zone_id, hev.owner_revision) == (4001, channels[0], 1, 1)

            # client_channel_pick：目标频道已满 (1/1) -> 23
            full_resp = await h.enter_scene(
                player_id=4002,
                scene_id=channels[0],
                scene_conf_id=1,
                session_id=502,
                gate_id="4",
                gate_instance_id="g-4",
                gate_zone_id=1,
                login_generation=1,
                owner_revision=1,
                client_channel_pick=True,
            )
            assert full_resp.error_code == ERR_CHANNEL_FULL

            # client_channel_pick：目标频道正常切入并触发冷却，再切回被 24 拒绝
            # 先让 4002 正常进入 channels[1]
            ok_enter = await h.enter_scene(
                player_id=4002,
                scene_id=channels[1],
                scene_conf_id=1,
                session_id=502,
                gate_id="4",
                gate_instance_id="g-4",
                gate_zone_id=1,
                login_generation=1,
                owner_revision=1,
            )
            assert ok_enter.error_code == ERR_OK
            # 标 draining -> 选线返回 22
            h.control.mark_channel_draining(1, 1, channels[0])
            drain_resp = await h.enter_scene(
                player_id=4002,
                scene_id=channels[0],
                scene_conf_id=1,
                session_id=502,
                gate_id="4",
                gate_instance_id="g-4",
                gate_zone_id=1,
                login_generation=1,
                owner_revision=1,
                client_channel_pick=True,
            )
            assert drain_resp.error_code == ERR_CHANNEL_UNAVAILABLE

            # 冷却键存在 -> 返回 24
            await h.players.set_channel_switch_cooldown(4002, 30)
            h.control.sets.setdefault(keys.world_channels_key(1, 1), set()).add(str(channels[0]))
            h.control.sets.pop(keys.world_draining_set_key(1, 1), None)
            await h.control.remove_scene_member_keys(1, channels[0], expected_owner, 4001)
            cd_resp = await h.enter_scene(
                player_id=4002,
                scene_id=channels[0],
                scene_conf_id=1,
                session_id=502,
                gate_id="4",
                gate_instance_id="g-4",
                gate_zone_id=1,
                login_generation=1,
                owner_revision=1,
                client_channel_pick=True,
            )
            assert cd_resp.error_code == ERR_CHANNEL_SWITCH_COOLDOWN

            # evacuation：源节点排空中时，疏散将玩家迁移到另一条非排空频道
            loc_4002 = await h.read_location(4002)
            src_node_id = int(loc_4002.node_id)
            for node in h.registry.scene_nodes[1]:
                if node.node_id == src_node_id:
                    node.admission = 2  # CLOSED (draining)
            h.players.set_handoff_marker(4002, f"{loc_4002.owner_epoch}:{h.clock.now_ms}")
            # 仅当存在位于非排空节点上的频道时可成功疏散
            other_channel = channels[0]
            other_owner = await h.control.get_scene_node(1, other_channel)
            if other_owner == loc_4002.node_id:
                other_node = "11" if loc_4002.node_id == "10" else "10"
                h.control.strings[keys.scene_node_key(1, other_channel)] = other_node.encode("utf-8")
            h.clock.advance_sec(1)
            evac = await h.enter_scene(
                player_id=4002,
                scene_id=loc_4002.scene_id,
                scene_conf_id=1,
                session_id=502,
                gate_id="4",
                gate_instance_id="g-4",
                gate_zone_id=1,
                login_generation=1,
                owner_revision=1,
                evacuation=True,
            )
            assert evac.error_code == ERR_OK
            loc_after_evac = await h.read_location(4002)
            assert loc_after_evac.node_id != loc_4002.node_id

    asyncio.run(scenario())


# ==============================================================================
# 7. LeaveScene：pending (FAILED_PRECONDITION + async ReleasePlayer(session_ended=True)) vs released
# ==============================================================================

def test_leave_scene_pending_then_released_and_identity_guard():
    async def scenario():
        async with Harness() as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)
            h.data.register_player(5001, revision=4, home_zone=1)

            enter = await h.enter_scene(
                player_id=5001,
                scene_conf_id=1,
                session_id=601,
                gate_id="4",
                gate_instance_id="g-4",
                gate_zone_id=1,
                login_generation=10,
                owner_revision=4,
            )
            assert enter.error_code == ERR_OK
            loc = await h.read_location(5001)
            assert await h.control.get_scene_member_count(1, loc.scene_id) == 1

            # 陈旧会话（login_generation=5 < 10）的 LeaveScene 不删 location、不扣人数、回 OK
            await h.leave_scene(player_id=5001, scene_id=loc.scene_id, session_id=600, login_generation=5, zone_id=1)
            assert await h.read_location(5001) is not None
            assert await h.control.get_scene_member_count(1, loc.scene_id) == 1

            # 匹配会话但持有节点未写 handoff 标记 -> FAILED_PRECONDITION("holder_release_pending: ...") + 异步 session_ended 释放
            with pytest.raises(grpc.aio.AioRpcError) as pending_exc:
                await h.leave_scene(player_id=5001, scene_id=loc.scene_id, session_id=601, login_generation=10, zone_id=1)
            assert pending_exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert "holder_release_pending:" in (pending_exc.value.details() or "")
            assert await h.read_location(5001) is not None
            await h.service.drain_background_tasks()
            assert len(h.scene_nodes.releases) == 1
            rel = h.scene_nodes.releases[0]
            assert (rel.node_id, rel.player_id, rel.expected_owner_epoch, rel.session_ended, rel.covered_session_id, rel.covered_login_generation) == (
                "10", 5001, loc.owner_epoch, True, 601, 10,
            )

            # 持有节点落盘并写出当前代 handoff 标记 -> LeaveScene 原子删除 location 并移除成员
            h.players.set_handoff_marker(5001, f"{loc.owner_epoch}:{h.clock.now_ms}")
            await h.leave_scene(player_id=5001, scene_id=loc.scene_id, session_id=601, login_generation=10, zone_id=1)
            assert await h.read_location(5001) is None
            assert await h.control.get_scene_member_count(1, loc.scene_id) == 0

    asyncio.run(scenario())


# ==============================================================================
# 8. 真实出站 gRPC 适配器校验（DataService 签名 & SceneNodeGrpc 仅拨号 grpc_endpoint）
# ==============================================================================

def test_outbound_grpc_adapters_signing_and_grpc_endpoint_only():
    async def scenario():
        protocol = Protocol()
        secret = b"K" * 32
        clock = FakeClock()

        # 1. 启动真实 DataService gRPC 服务端，校验 LookupAccountRole 签名与 GetPlayerHomeZone 无签名
        seen_paths: list[str] = []

        async def handle_lookup(req, ctx, path):
            seen_paths.append(path)
            if not verify_service_call(path, req, "scene_manager", clock(), secret):
                await ctx.abort(grpc.StatusCode.PERMISSION_DENIED, "bad signature")
            resp = protocol.message("data_service.AccountRoleOwnership", account="acc-z", owner_revision=9)
            resp.role.player_id = req.player_id
            return resp

        async def handle_home_zone(req, ctx, path):
            seen_paths.append(path)
            return protocol.message("data_service.GetPlayerHomeZoneResponse", home_zone_id=3)

        ds_server, ds_port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            {"data_service.DataService": {"LookupAccountRole": handle_lookup, "GetPlayerHomeZone": handle_home_zone}},
            pass_context=True,
        )
        ds_grpc = GrpcClient(f"127.0.0.1:{ds_port}", timeout=5)
        try:
            ds_client = GrpcDataServiceClient(protocol, ds_grpc, secret, clock_ms=clock)
            role = await ds_client.lookup_account_role(8888)
            assert (role.player_id, role.account, role.owner_revision) == (8888, "acc-z", 9)
            hz = await ds_client.get_player_home_zone(8888)
            assert hz == 3
            assert seen_paths == [
                "/data_service.DataService/LookupAccountRole",
                "/data_service.DataService/GetPlayerHomeZone",
            ]
        finally:
            await ds_grpc.close()
            await ds_server.stop(1)

        # 2. 启动真实 SceneNodeGrpc 服务端，验证只拨号 grpc_endpoint 且无 grpc_endpoint 时拒绝回退 TCP
        async def handle_scene_create(req, ctx, path):
            info = protocol.message("SceneInfoComp", scene_id=req.scene_id, scene_config_id=req.config_id)
            return protocol.message("CreateSceneResponse", scene_info=info)

        async def handle_scene_empty(req, ctx, path):
            return protocol.message("Empty")

        sn_server, sn_port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            {
                "scene_node.SceneNodeGrpc": {
                    "CreateScene": handle_scene_create,
                    "DestroyScene": handle_scene_empty,
                    "ReleasePlayer": handle_scene_empty,
                }
            },
            pass_context=True,
        )
        registry = FakeNodeRegistry(protocol)
        registry.add_scene_node(zone_id=1, node_id=10, grpc_port=sn_port, tcp_port=19999)
        registry.add_scene_node(zone_id=1, node_id=11, grpc_port=0, tcp_port=19998)
        sn_client = GrpcSceneNodeClient(protocol, registry, own_zone_id=1)
        try:
            await sn_client.create_scene(1, "10", config_id=1, scene_id=12345)
            await sn_client.destroy_scene(1, "10", scene_id=12345)
            await sn_client.release_player(1, "10", player_id=8888, expected_owner_epoch=7)
            with pytest.raises(RuntimeError, match="no grpc_endpoint"):
                await sn_client.create_scene(1, "11", config_id=1, scene_id=12346)
        finally:
            await sn_client.close()
            await sn_server.stop(1)

    asyncio.run(scenario())


# ==============================================================================
# 9. LeaveScene 死节点 / no_holder 分支、跨区二跳消费 pending_scene_conf_id 与 Kafka 分区路由
# ==============================================================================

def test_leave_dead_owner_no_holder_and_kafka_partitioning():
    async def scenario():
        # 验证 KafkaCommandPublisher 显式分区 node_id % partitions 与 topic 命名
        sent: list[tuple[str, bytes, bytes, int]] = []

        class _FakeProducer:
            async def send_and_wait(self, topic, value, *, key, partition):
                sent.append((topic, value, key, partition))

        protocol = Protocol()
        pub = KafkaCommandPublisher("127.0.0.1:9092", partitions=64, generation=3, producer=_FakeProducer())
        gcmd = protocol.message("contracts.kafka.GateCommand", player_id=77, target_gate_id=70, event_id=EVENT_ID_ROUTE_PLAYER)
        scmd = protocol.message("contracts.kafka.SceneCommand", player_id=77, target_scene_id=130, event_id=EVENT_ID_PLAYER_HOSTED_PLACEMENT)
        await pub.publish_gate_command(70, 77, gcmd)
        await pub.publish_scene_command(130, 77, scmd)
        assert sent[0][0] == "gate-cmd_g3" and sent[0][2] == b"77" and sent[0][3] == 70 % 64
        assert sent[1][0] == "scene-cmd_g3" and sent[1][2] == b"77" and sent[1][3] == 130 % 64

        # 验证 LeaveScene 死节点（过再入屏障后）与 no_holder 直接清理 location
        cfg = SceneManagerConfig(zone_id=1, gate_token_secret=b"D" * 32, world_conf_ids=(1, 2), reentry_barrier_sec=20)
        async with Harness(cfg=cfg) as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)
            h.data.register_player(6001, revision=1, home_zone=1)

            enter = await h.enter_scene(
                player_id=6001, scene_conf_id=1, session_id=701,
                gate_id="4", gate_instance_id="g-4", gate_zone_id=1, login_generation=1, owner_revision=1,
            )
            assert enter.error_code == ERR_OK
            loc = await h.read_location(6001)

            # 节点 10 下线，再入屏障未过时 LeaveScene 仍返回 FAILED_PRECONDITION
            h.registry.remove_scene_node(1, 10)
            await h.service.sync_nodes_to_control_store(1)
            with pytest.raises(grpc.aio.AioRpcError) as barrier_exc:
                await h.leave_scene(player_id=6001, scene_id=loc.scene_id, session_id=701, login_generation=1, zone_id=1)
            assert barrier_exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            # 推进时钟越过 20s 再入屏障 -> 判 owner_dead 直接删除 location
            h.clock.advance_sec(25)
            await h.leave_scene(player_id=6001, scene_id=loc.scene_id, session_id=701, login_generation=1, zone_id=1)
            assert await h.read_location(6001) is None

            # 跨区二跳消费 awaiting placement（node_id=""）：未映射 home_zone 拒绝返回 20，映射后落到 pending_scene_conf_id=2
            h.registry.add_scene_node(zone_id=1, node_id=12, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)
            h.data.register_player(6002, revision=2, home_zone=None)
            await h.players.observe_location_revision(6002, 2)
            awaiting_loc = h.protocol.message(
                "storage.PlayerLocation",
                scene_id=0, node_id="", zone_id=1, owner_epoch=100,
                pending_scene_conf_id=2, update_time=h.clock.now_ms // 1000,
                session_id=801, login_generation=2,
            )
            raw_awaiting = awaiting_loc.SerializeToString()
            await h.players.mint_epoch_and_set_location(
                player_id=6002, observed_epoch=0, location_bytes=raw_awaiting,
                required_marker="", expect_mode="missing", expect_raw=b"",
                owner_revision=2, new_tag=keys.location_revision_tag(2, raw_awaiting),
                observed_tag="", target_epoch=100,
            )
            unmapped = await h.enter_scene(
                player_id=6002, session_id=801, gate_id="4", gate_instance_id="g-4",
                gate_zone_id=1, login_generation=2, owner_revision=2,
            )
            assert unmapped.error_code == ERR_HOME_ZONE_UNAVAILABLE

            h.data.home_zones[6002] = 9
            h.clock.advance_sec(1)
            leg2 = await h.enter_scene(
                player_id=6002, session_id=801, gate_id="4", gate_instance_id="g-4",
                gate_zone_id=1, login_generation=2, owner_revision=2,
            )
            assert leg2.error_code == ERR_OK
            loc2 = await h.read_location(6002)
            assert loc2.node_id == "12"
            assert str(loc2.scene_id) in await h.control.get_world_channels(1, 2)

            # no_holder 的 LeaveScene 直接清理
            awaiting2 = h.protocol.message(
                "storage.PlayerLocation",
                scene_id=0, node_id="", zone_id=1, owner_epoch=loc2.owner_epoch + 1,
                update_time=h.clock.now_ms // 1000, session_id=801, login_generation=2,
            )
            raw2 = awaiting2.SerializeToString()
            h.players.strings[keys.location_key(6002)] = raw2
            h.players.strings[keys.location_revision_key(6002)] = keys.location_revision_tag(2, raw2).encode("ascii")
            await h.leave_scene(player_id=6002, scene_id=0, session_id=801, login_generation=2, zone_id=1)
            assert await h.read_location(6002) is None

    asyncio.run(scenario())


# ==============================================================================
# 10. WorldChannelDirectoryPublisher：稳定 1-based 线号、状态分类、TTL 与 nodes/scene_channels.py 联读
# ==============================================================================

def test_world_channel_directory_publisher_and_scene_channels_consumer():
    async def scenario():
        cfg = SceneManagerConfig(
            zone_id=1,
            gate_token_secret=b"C" * 32,
            world_conf_ids=(1,),
            world_channel_count=4,
            channel_max_players=10,
            channel_busy_percent=60,
            channel_switch_cooldown_sec=15,
            channel_directory_refresh_sec=2.0,
        )
        async with Harness(cfg=cfg) as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            h.registry.add_scene_node(zone_id=1, node_id=11, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)

            channels = sorted(int(s) for s in await h.control.get_world_channels(1, 1))
            assert len(channels) == 4
            c1, c2, c3, c4 = channels

            # c1: 2 人 -> SMOOTH; c2: 6 人 (60%) -> BUSY; c3: 10 人 (100%) -> FULL; c4: draining -> CLOSING
            for idx in range(2):
                await h.control.add_scene_member(1, c1, await h.control.get_scene_node(1, c1), 100 + idx)
            for idx in range(6):
                await h.control.add_scene_member(1, c2, await h.control.get_scene_node(1, c2), 200 + idx)
            for idx in range(10):
                await h.control.add_scene_member(1, c3, await h.control.get_scene_node(1, c3), 300 + idx)
            h.control.mark_channel_draining(1, 1, c4)

            publisher = WorldChannelDirectoryPublisher(
                h.protocol, cfg, control_store=h.control, node_registry=h.registry, clock_ms=h.clock,
            )
            assert await publisher.publish_zone(1) == 1

            # 校验键名、TTL (~8s) 与确定性 1-based 线号
            dir_key = keys.world_channel_directory_key(1, 1)
            assert dir_key == "world_channel_directory:{ctl:1}:1"
            assert h.control.get_ttl_remaining_sec(dir_key) == 8

            raw = await h.control.get_world_channel_directory(1, 1)
            assert raw is not None
            parsed = h.protocol.parse("SceneChannelDirectory", raw)
            assert (parsed.zone_id, parsed.scene_config_id, parsed.max_players_per_channel, parsed.switch_cooldown_seconds, parsed.switch_enabled) == (
                1, 1, 10, 15, True,
            )
            assert [(c.scene_id, c.channel_no, c.player_count, c.state) for c in parsed.channels] == [
                (c1, 1, 2, SCENE_CHANNEL_STATE_SMOOTH),
                (c2, 2, 6, SCENE_CHANNEL_STATE_BUSY),
                (c3, 3, 10, SCENE_CHANNEL_STATE_FULL),
                (c4, 4, 0, SCENE_CHANNEL_STATE_CLOSING),
            ]

            # 真实 nodes/scene_channels.py SceneChannels 消费直读校验
            pushed: list[tuple[object, str, object]] = []

            class _FakePlayer:
                player_id = 9001

            class _FakeWorld:
                def __init__(self, info, player):
                    self.info = info
                    self.players = {player.player_id: player}

            class _FakeScene:
                def __init__(self, protocol, world, player):
                    self.protocol = protocol
                    self.info = protocol.message("NodeInfo", zone_id=1)
                    self.locations = {player.player_id: world}

                def _push(self, player, name, msg):
                    pushed.append((player, name, msg))

            player = _FakePlayer()
            scene_info = h.protocol.message("SceneInfoComp", scene_id=c1, scene_config_id=1)
            world = _FakeWorld(scene_info, player)
            fake_scene = _FakeScene(h.protocol, world, player)
            consumer = SceneChannels(fake_scene, control_redis=h.control)
            req = h.protocol.message("SceneInfoRequest", with_channel_directory=True)
            consumer.query(world, player, req)
            await asyncio.gather(*tuple(consumer._pending))
            await consumer.close()

            assert len(pushed) == 1
            _, msg_name, s2c = pushed[0]
            assert msg_name == "SceneSceneClientPlayerNotifySceneInfo"
            assert s2c.HasField("channel_directory")
            assert s2c.channel_directory.scene_config_id == 1
            assert [c.channel_no for c in s2c.channel_directory.channels] == [1, 2, 3, 4]
            assert [c.state for c in s2c.channel_directory.channels] == [
                SCENE_CHANNEL_STATE_SMOOTH,
                SCENE_CHANNEL_STATE_BUSY,
                SCENE_CHANNEL_STATE_FULL,
                SCENE_CHANNEL_STATE_CLOSING,
            ]

            # c2 下线销毁后新线 c5 复用空出的 2 号线，且属主节点下线后 c1 展示为 UNAVAILABLE
            h.control.sets[keys.world_channels_key(1, 1)].discard(str(c2))
            c5 = c4 + 100
            owner_c1 = await h.control.get_scene_node(1, c1)
            other_node = "11" if owner_c1 == "10" else "10"
            h.control.strings[keys.scene_node_key(1, c5)] = other_node.encode("utf-8")
            h.control.sets[keys.world_channels_key(1, 1)].add(str(c5))
            await h.control.remove_scene_node(1, owner_c1, death_at_unix_sec=h.clock.now_ms // 1000)
            await publisher.publish_zone(1)
            parsed2 = h.protocol.parse("SceneChannelDirectory", await h.control.get_world_channel_directory(1, 1))
            by_sid = {c.scene_id: (c.channel_no, c.state) for c in parsed2.channels}
            assert by_sid[c5][0] == 2
            assert by_sid[c1] == (1, SCENE_CHANNEL_STATE_UNAVAILABLE)

            # 推进时钟越过 8s TTL -> 目录自然过期
            h.clock.advance_sec(9)
            assert await h.control.get_world_channel_directory(1, 1) is None

    asyncio.run(scenario())


# ==============================================================================
# 11. IdleInstanceSweeper：空副本 300s vs 镜像 30s、有人保活与并发进入 CAS 保护
# ==============================================================================

def test_idle_instance_sweeper_timeouts_busy_and_mirror_cascade():
    async def scenario():
        cfg = SceneManagerConfig(
            zone_id=1,
            gate_token_secret=b"I" * 32,
            world_conf_ids=(1,),
            instance_idle_timeout_sec=300,
            mirror_idle_timeout_sec=30,
        )
        async with Harness(cfg=cfg) as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD)
            h.registry.add_scene_node(zone_id=1, node_id=20, scene_node_type=SCENE_NODE_TYPE_INSTANCE)
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)
            world_ch = int((await h.control.get_world_channels(1, 1))[0])

            inst_empty = await h.create_scene(scene_conf_id=9001, scene_type=SCENE_TYPE_INSTANCE, zone_id=1)
            inst_busy = await h.create_scene(scene_conf_id=9002, scene_type=SCENE_TYPE_INSTANCE, zone_id=1)
            inst_parent = await h.create_scene(scene_conf_id=9003, scene_type=SCENE_TYPE_INSTANCE, zone_id=1)
            mirror_world = await h.create_scene(
                scene_conf_id=1, scene_type=SCENE_TYPE_INSTANCE, zone_id=1,
                source_scene_id=world_ch, mirror_config_id=71,
            )
            # 给 inst_busy 放一名在线玩家
            await h.control.add_scene_member(1, inst_busy.scene_id, "20", 7001)

            sweeper = IdleInstanceSweeper(h.service, clock_ms=h.clock)

            # 经过 31s：空镜像（30s）被销毁并摘出源镜像集；普通空副本（300s）与有人副本均保留
            h.clock.advance_sec(31)
            assert await sweeper.sweep_once(1) == 1
            assert not await h.control.scene_node_exists(1, mirror_world.scene_id)
            assert str(mirror_world.scene_id) not in await h.control.get_scene_mirrors(1, world_ch)
            assert await h.control.scene_node_exists(1, inst_empty.scene_id)
            assert await h.control.scene_node_exists(1, inst_busy.scene_id)
            # inst_busy 的活跃时间戳已刷新到当前时刻
            scores = dict(await h.control.list_active_instances_with_scores(1))
            assert int(scores[str(inst_busy.scene_id)]) == h.clock.now_ms // 1000

            # 在 290s 时给 inst_parent 挂一个子镜像（20s 后自身不到 30s，但随父级联销毁）
            h.clock.advance_sec(259)
            sub_mirror = await h.create_scene(
                scene_conf_id=9003, scene_type=SCENE_TYPE_INSTANCE, zone_id=1,
                source_scene_id=inst_parent.scene_id, mirror_config_id=72,
            )

            # 再过 15s（总计 305s）：inst_empty 与 inst_parent 超 300s 被销毁（级联销毁 sub_mirror），inst_busy 仍存活
            h.clock.advance_sec(15)
            assert await sweeper.sweep_once(1) == 2
            assert not await h.control.scene_node_exists(1, inst_empty.scene_id)
            assert not await h.control.scene_node_exists(1, inst_parent.scene_id)
            assert not await h.control.scene_node_exists(1, sub_mirror.scene_id)
            assert await h.control.scene_node_exists(1, inst_busy.scene_id)
            destroyed_ids = {sid for _, _, sid in h.scene_nodes.destroyed}
            assert {mirror_world.scene_id, inst_empty.scene_id, inst_parent.scene_id, sub_mirror.scene_id}.issubset(destroyed_ids)
            assert inst_busy.scene_id not in destroyed_ids

            # 直接验证 Lua 空闲 CAS 防竞态：有玩家时 atomic_destroy_instance 返回 busy 且不删键
            claim = await h.control.atomic_destroy_instance(
                zone_id=1, scene_id=inst_busy.scene_id, expected_owner="20", now_unix_sec=h.clock.now_ms // 1000,
            )
            assert claim.busy is True and claim.claimed is False
            assert await h.control.scene_node_exists(1, inst_busy.scene_id)

    asyncio.run(scenario())


# ==============================================================================
# 12. TeardownJobSweeper：拆除失败留待办 -> 租期后重试 DestroyScene -> finish_scene_teardown
# ==============================================================================

def test_teardown_job_sweeper_retry_completion_and_dangling_cleanup():
    async def scenario():
        cfg = SceneManagerConfig(
            zone_id=1,
            gate_token_secret=b"T" * 32,
            world_conf_ids=(1,),
            teardown_job_lease_sec=90,
        )
        async with Harness(cfg=cfg) as h:
            h.registry.add_scene_node(zone_id=1, node_id=20, scene_node_type=SCENE_NODE_TYPE_INSTANCE)
            await h.service.sync_nodes_to_control_store(1)

            inst = await h.create_scene(scene_conf_id=9101, scene_type=SCENE_TYPE_INSTANCE, zone_id=1)
            assert inst.error_code == ERR_OK

            # 注入节点 20 DestroyScene 失败 -> 认领已删映射但拆除任务留在 pending
            h.scene_nodes.fail_destroy_nodes.add("20")
            await h.destroy_scene(scene_id=inst.scene_id, zone_id=1)
            assert not await h.control.scene_node_exists(1, inst.scene_id)
            job = await h.control.load_scene_teardown_job(1, inst.scene_id)
            assert job is not None and job.kind == "instance" and job.claim.node_id == "20"

            sweeper = TeardownJobSweeper(h.service, clock_ms=h.clock)

            # 租期内（+30s）不接手
            h.clock.advance_sec(30)
            assert await sweeper.sweep_once(1) == 0

            # 超租期（+65s，总 95s）但节点仍报错 -> 重试失败，任务继续保留
            h.clock.advance_sec(65)
            assert await sweeper.sweep_once(1) == 0
            assert await h.control.load_scene_teardown_job(1, inst.scene_id) is not None

            # 节点恢复 -> 下次扫描补发 DestroyScene 并 finish_scene_teardown 清空待办
            h.scene_nodes.fail_destroy_nodes.clear()
            assert await sweeper.sweep_once(1) == 1
            assert await h.control.load_scene_teardown_job(1, inst.scene_id) is None
            assert await h.control.list_due_teardown_jobs(1, h.clock.now_ms // 1000 + 1000, 10) == []
            assert (1, "20", inst.scene_id) in h.scene_nodes.destroyed

            # 悬空 pending 条目（无 Hash）自动摘除
            dangling_sid = 999888
            h.control._zadd(keys.scene_teardown_pending_key(1), str(dangling_sid), float(h.clock.now_ms // 1000 - 200))
            assert await sweeper.sweep_once(1) == 0
            assert await h.control.list_due_teardown_jobs(1, h.clock.now_ms // 1000 + 1000, 10) == []

    asyncio.run(scenario())


# ==============================================================================
# 13. DeadNodeSweeper：20s 再入屏障、死节点孤儿副本清理、保大世界频道与同实例回归撤销
# ==============================================================================

def test_dead_node_sweeper_barrier_orphans_world_preservation_and_returning_instance():
    async def scenario():
        cfg = SceneManagerConfig(
            zone_id=1,
            gate_token_secret=b"N" * 32,
            world_conf_ids=(1,),
            world_channel_count=1,
            reentry_barrier_sec=20,
        )
        async with Harness(cfg=cfg) as h:
            h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD, node_uuid="uuid-10")
            h.registry.add_scene_node(zone_id=1, node_id=20, scene_node_type=SCENE_NODE_TYPE_INSTANCE, node_uuid="uuid-20")
            h.registry.add_scene_node(zone_id=1, node_id=21, scene_node_type=SCENE_NODE_TYPE_INSTANCE, node_uuid="uuid-21")
            await h.service.sync_nodes_to_control_store(1)
            await h.service.init_world_scenes(1)
            world_ch = int((await h.control.get_world_channels(1, 1))[0])

            # 在节点 20 建两个副本，在节点 10 建一个大世界共置镜像
            inst_orphan = await h.create_scene(scene_conf_id=9201, scene_type=SCENE_TYPE_INSTANCE, zone_id=1, target_node_id="20")
            inst_moved = await h.create_scene(scene_conf_id=9202, scene_type=SCENE_TYPE_INSTANCE, zone_id=1, target_node_id="20")
            mirror_on_10 = await h.create_scene(
                scene_conf_id=1, scene_type=SCENE_TYPE_INSTANCE, zone_id=1,
                source_scene_id=world_ch, mirror_config_id=73,
            )
            assert mirror_on_10.node_id == "10"

            # 节点 20 死亡 -> sync 写 death_at 并入队死节点收尾任务
            h.registry.remove_scene_node(1, 20)
            await h.service.sync_nodes_to_control_store(1)
            tasks, _ = await h.control.list_dead_node_reconcile_tasks(1)
            assert [t.node_id for t in tasks] == ["20"]

            # 模拟 inst_moved 在收尾前已被改派到存活节点 21
            assert await h.control.reassign_scene_node(
                zone_id=1, scene_id=inst_moved.scene_id, old_node_id="20", new_node_id="21", placement_generation=2,
            )

            sweeper = DeadNodeSweeper(h.service, clock_ms=h.clock)

            # 屏障内（+10s < 20s）：不清理任何孤儿
            h.clock.advance_sec(10)
            assert await sweeper.sweep_once(1) == 0
            assert await h.control.scene_node_exists(1, inst_orphan.scene_id)

            # 过屏障（+15s，总 25s）：销毁 inst_orphan，保留已改派的 inst_moved，清空死节点 20 的 scene_count
            h.clock.advance_sec(15)
            assert await sweeper.sweep_once(1) == 1
            assert not await h.control.scene_node_exists(1, inst_orphan.scene_id)
            assert not await h.control.is_active_instance(1, inst_orphan.scene_id)
            assert await h.control.get_scene_node(1, inst_moved.scene_id) == "21"
            assert await h.control.get_node_scene_count(1, "20") is None

            # 节点 10（承载大世界频道 + 镜像）死亡 -> 过屏障后仅拆镜像，绝不毁大世界频道
            h.registry.remove_scene_node(1, 10)
            await h.service.sync_nodes_to_control_store(1)
            h.clock.advance_sec(25)
            assert await sweeper.sweep_once(1) == 1
            assert not await h.control.scene_node_exists(1, mirror_on_10.scene_id)
            assert await h.control.get_scene_node(1, world_ch) == "10"

            # 判死同一代进程回归（node_uuid 相同）：撤销收尾任务并保住名下副本
            inst_live = await h.create_scene(scene_conf_id=9203, scene_type=SCENE_TYPE_INSTANCE, zone_id=1, target_node_id="21")
            h.registry.remove_scene_node(1, 21)
            await h.service.sync_nodes_to_control_store(1)
            # 同一进程 uuid-21 在过屏障前重新注册回 etcd（尚未经 sync 撤任务，由 sweeper 主动撤）
            h.registry.add_scene_node(zone_id=1, node_id=21, scene_node_type=SCENE_NODE_TYPE_INSTANCE, node_uuid="uuid-21")
            h.clock.advance_sec(25)
            assert await sweeper.sweep_once(1) == 1
            assert await h.control.get_scene_node(1, inst_live.scene_id) == "21"
            assert await h.control.get_node_death_at(1, "21") is None

    asyncio.run(scenario())


# ==============================================================================
# 14. DeadHolderReleaser：对接真实 DataService 的 ReadPlayerStoreHolder / ReleaseDeadPlayerStoreHolder 全链路
# ==============================================================================

def test_dead_holder_releaser_round_trip_against_real_data_service():
    async def scenario():
        protocol = Protocol()
        clock = FakeClock()
        secret = b"M" * 32

        # 启动真实 DataService（含真实 HMAC 验签与 ReadPlayerStoreHolder / ReleaseDeadPlayerStoreHolder）
        ds_store = DataMemoryStore(protocol, clock_ms=clock)
        ds_service = DataService(protocol, ds_store, {"scene_manager": secret}, clock_ms=clock)
        await ds_service.start()
        ds_server, ds_port = await serve_grpc(
            protocol, "127.0.0.1:0", ds_service.services(), pass_context=True,
        )
        ds_grpc = GrpcClient(f"127.0.0.1:{ds_port}", timeout=5)
        try:
            ds_client = GrpcDataServiceClient(protocol, ds_grpc, secret, clock_ms=clock)

            # 1. 预置角色与路由到真实 DataService
            for pid, name in ((8001, "死持有离场"), (8002, "死持有接管")):
                await ds_store.register_zone(pid, 1, 1_000_000)
                role_msg = protocol.message("AccountSimplePlayer", player_id=pid, name=name, zone_id=1)
                await ds_store.create_role(f"acc-{pid}", role_msg)

            # 2. 直连验证 ReadPlayerStoreHolder / ReleaseDeadPlayerStoreHolder 签名往返
            assert await ds_client.read_player_store_holder(999999) is None
            ds_store.store_holders[8001] = [100, b"uuid-dead-10", 5, 1]
            ds_store.store_released[8001] = False
            seen = await ds_client.read_player_store_holder(8001)
            assert seen == PlayerStoreHolderInfo(owner_epoch=100, holder_instance=b"uuid-dead-10", claim_seq=5)

            # seen.claim_seq 不匹配 -> 真实 DataService 拒绝 FAILED_PRECONDITION
            with pytest.raises(grpc.aio.AioRpcError) as mismatch:
                await ds_client.release_dead_player_store_holder(
                    8001, 100, seen=PlayerStoreHolderInfo(100, b"uuid-dead-10", 4),
                )
            assert mismatch.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            # 3. 完整接线 SceneManagerService + DeadHolderReleaser + 真实 DataService
            cfg = SceneManagerConfig(
                zone_id=1,
                gate_token_secret=b"P" * 32,
                world_conf_ids=(1,),
                reentry_barrier_sec=20,
            )
            async with Harness(cfg=cfg, data_service=ds_client, clock=clock, protocol=protocol) as h:
                h.service.dead_holder_releaser = DeadHolderReleaser(ds_client, h.registry, zone_id=1)
                h.registry.add_scene_node(zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD, node_uuid="uuid-10")
                h.registry.add_scene_node(zone_id=1, node_id=11, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD, node_uuid="uuid-11")
                await h.service.sync_nodes_to_control_store(1)
                await h.service.init_world_scenes(1)

                # 让 8001 和 8002 落到节点 10
                ch_on_10 = None
                for sid_str in await h.control.get_world_channels(1, 1):
                    if await h.control.get_scene_node(1, int(sid_str)) == "10":
                        ch_on_10 = int(sid_str)
                        break
                if ch_on_10 is None:
                    ch_on_10 = int((await h.control.get_world_channels(1, 1))[0])
                    h.control.strings[keys.scene_node_key(1, ch_on_10)] = b"10"

                for pid, sess in ((8001, 1001), (8002, 1002)):
                    ent = await h.enter_scene(
                        player_id=pid, scene_id=ch_on_10, session_id=sess,
                        gate_id="4", gate_instance_id="g-4", gate_zone_id=1,
                        login_generation=1, owner_revision=1,
                    )
                    assert ent.error_code == ERR_OK

                loc_8001 = await h.read_location(8001)
                loc_8002 = await h.read_location(8002)
                assert loc_8001.node_id == "10" and loc_8002.node_id == "10"

                # 节点 10 掉线且过 20s 屏障；但先把 8001/8002 写权行指向仍在册的 uuid-11（模拟交接源仍在册/镜像滞后）
                ds_store.store_holders[8001] = [loc_8001.owner_epoch, b"uuid-11", 7, 1]
                ds_store.store_released[8001] = False
                ds_store.store_holders[8002] = [loc_8002.owner_epoch, b"uuid-11", 8, 1]
                ds_store.store_released[8002] = False

                h.registry.remove_scene_node(1, 10)
                await h.service.sync_nodes_to_control_store(1)
                clock.advance_sec(25)

                # 持有实例 uuid-11 仍在 etcd 注册表 -> LeaveScene 拒绝删位置、不释放写权行
                with pytest.raises(grpc.aio.AioRpcError) as kept_leave:
                    await h.leave_scene(
                        player_id=8001, scene_id=loc_8001.scene_id, session_id=1001, login_generation=1, zone_id=1,
                    )
                assert kept_leave.value.code() == grpc.StatusCode.FAILED_PRECONDITION
                assert "holder_release_pending" in (kept_leave.value.details() or "")
                assert await h.read_location(8001) is not None
                assert ds_store.store_released[8001] is False

                # 持有实例 uuid-11 仍在册 -> EnterScene 死节点接管也被拦回 ERR_HANDOFF_PENDING，不释放写权行
                blocked_enter = await h.enter_scene(
                    player_id=8002, scene_conf_id=1, session_id=1002,
                    gate_id="4", gate_instance_id="g-4", gate_zone_id=1,
                    login_generation=2, owner_revision=1,
                )
                assert blocked_enter.error_code == ERR_HANDOFF_PENDING
                assert ds_store.store_released[8002] is False
                assert (await h.read_location(8002)).node_id == "10"

                # 将写权行改回已不在册的死实例 uuid-10 -> LeaveScene 经真实 DataService 释放写权行并删 location
                ds_store.store_holders[8001] = [loc_8001.owner_epoch, b"uuid-10", 7, 1]
                ds_store.store_released[8001] = False
                await h.leave_scene(
                    player_id=8001, scene_id=loc_8001.scene_id, session_id=1001, login_generation=1, zone_id=1,
                )
                assert await h.read_location(8001) is None
                after_8001 = await ds_client.read_player_store_holder(8001)
                assert after_8001 is not None
                assert after_8001.owner_epoch == loc_8001.owner_epoch + 1
                assert after_8001.holder_instance == b""
                assert ds_store.store_released[8001] is True

                # 8002 写权行同样指向死实例 uuid-10 -> EnterScene 先释放死持有者，再接管到活节点 11
                ds_store.store_holders[8002] = [loc_8002.owner_epoch, b"uuid-10", 8, 1]
                ds_store.store_released[8002] = False
                # 在节点 11 上准备可用大世界频道（触发自愈改派）
                clock.advance_sec(1)
                await h.service.init_world_scenes(1)
                takeover = await h.enter_scene(
                    player_id=8002, scene_conf_id=1, session_id=1002,
                    gate_id="4", gate_instance_id="g-4", gate_zone_id=1,
                    login_generation=2, owner_revision=1,
                )
                assert takeover.error_code == ERR_OK
                loc_8002_new = await h.read_location(8002)
                assert loc_8002_new.node_id == "11" and loc_8002_new.owner_epoch > loc_8002.owner_epoch
                after_8002 = await ds_client.read_player_store_holder(8002)
                assert after_8002.owner_epoch == loc_8002.owner_epoch + 1
                assert after_8002.holder_instance == b""
                assert ds_store.store_released[8002] is True

                # 额外验证 NO_ROW 与 NEWER_OWNER 均放行
                releaser = h.service.dead_holder_releaser
                assert await releaser.release_dead_holder(999999, 50) == DeadHolderReleaseOutcome.NO_ROW
                assert await releaser.release_dead_holder(8001, loc_8001.owner_epoch) == DeadHolderReleaseOutcome.NEWER_OWNER
        finally:
            await ds_grpc.close()
            await ds_server.stop(1)
            await ds_service.close()

    asyncio.run(scenario())
