"""player_locator 端到端与状态机回归测试（真实 gRPC + 既有 login.sessions.PlayerLocator 客户端 + LeaseMonitor）。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import grpc
import pytest

from xuanming.grpc_runtime import GrpcClient, serve_grpc
from xuanming.login.keys import crc16
from xuanming.login.sessions import PlayerLocator as LoginPlayerLocatorClient, SessionChanged
from xuanming.player_locator import (
    DEFAULT_GRPC_PORT,
    PLAYER_LOCATOR_NODE_TYPE,
    CommitLeaseResult,
    HolderReleasePendingError,
    LeaseClaim,
    LeaseMonitor,
    MemorySessionStore,
    PlayerLocatorConfig,
    PlayerLocatorService,
    RearmLeaseResult,
    RedisSessionStore,
    ReplaceSessionResult,
)
from xuanming.player_locator import keys, lua
from xuanming.protocol import Protocol


class FakeRpcError(RuntimeError):
    """模拟带 gRPC StatusCode 的下游 RPC 错误。"""

    def __init__(self, code: grpc.StatusCode, detail: str) -> None:
        super().__init__(detail)
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


@dataclass(frozen=True)
class LeaveCall:
    player_id: int
    scene_id: int
    session_id: int
    login_generation: int


class FakeSceneManager:
    def __init__(self) -> None:
        self.calls: list[LeaveCall] = []
        self.next_error: Exception | None = None

    async def leave_scene(
        self, *, player_id: int, scene_id: int, session_id: int, login_generation: int,
    ) -> None:
        self.calls.append(LeaveCall(player_id, scene_id, session_id, login_generation))
        if self.next_error is not None:
            raise self.next_error


@dataclass(frozen=True)
class PublishedGateCommand:
    gate_node_id: int
    player_id: int
    command: object


class FakeGatePublisher:
    def __init__(self) -> None:
        self.messages: list[PublishedGateCommand] = []
        self.next_error: Exception | None = None

    async def publish_gate_command(self, gate_node_id: int, player_id: int, command) -> None:
        if self.next_error is not None:
            raise self.next_error
        self.messages.append(PublishedGateCommand(gate_node_id, player_id, command))


def _make_session(
    protocol: Protocol,
    player_id: int,
    *,
    session_id: int = 101,
    gate_id: str = "7",
    gate_instance_id: str = "gate-uuid-1",
    scene_node_id: str = "scene-1",
    scene_id: int = 500,
    version: int = 1,
    state: int = keys.SESSION_STATE_ONLINE,
    request_id: str = "req-1",
    account: str = "acc-1",
    login_generation: int = 100,
    session_incarnation: int = 1,
    gate_zone_id: int = 2,
):
    return protocol.message(
        "playerlocator.PlayerSession",
        player_id=player_id,
        session_id=session_id,
        gate_id=gate_id,
        gate_instance_id=gate_instance_id,
        scene_node_id=scene_node_id,
        scene_id=scene_id,
        session_version=version,
        state=state,
        last_active_ts=1_700_000_000_000,
        request_id=request_id,
        account=account,
        login_generation=login_generation,
        session_incarnation=session_incarnation,
        gate_zone_id=gate_zone_id,
    )


def _rpc_code(exc: BaseException) -> grpc.StatusCode | None:
    fn = getattr(exc, "code", None)
    return fn() if callable(fn) else None


def test_keys_match_go_format_and_same_slot_contract():
    assert crc16("123456789") == 0x31C3
    for pid in (1, 42, 10001, 9007199254740995):
        slot = keys.player_slot(pid)
        tag = keys.slot_index_tag(slot)
        assert keys.session_key(pid) == f"player:session:{{{pid}}}"
        assert keys.location_key(pid) == f"player:{{{pid}}}:location"
        assert keys.afk_pass_key(pid) == f"player:afk_pass:{{{pid}}}"

        lease = keys.lease_keys_for_player(pid)
        assert lease.ready == f"player:leases:{tag}"
        assert lease.processing == f"player:leases:{tag}:processing"
        assert lease.claim_tokens == f"player:leases:{tag}:claim_tokens"
        assert lease.claim_payloads == f"player:leases:{tag}:claim_payloads"
        assert keys.gate_registry_key(slot) == f"player:gate_index:{tag}:gates"
        assert keys.gate_session_index_key(slot, "g-1") == f"player:gate_index:{tag}:sessions:g-1"

        # 同一段 Lua 涉及的全部 KEYS 必须落在完全相同的 Redis Cluster slot
        for key_tuple in (
            keys.replace_session_keys(pid, "", "gate-a"),
            keys.replace_session_keys(pid, "gate-a", "gate-b"),
            keys.replace_session_keys(pid, "gate-a", ""),
            keys.set_disconnecting_keys(pid, "gate-a"),
            keys.set_disconnecting_keys(pid, ""),
            keys.delete_session_keys(pid, "gate-a"),
            keys.pin_lease_claim_payload_keys(pid),
            keys.ack_lease_claim_keys(pid),
            keys.enqueue_offline_cleanup_keys(pid),
            keys.session_lifecycle_keys(pid),
            (keys.session_key(pid), keys.location_key(pid), keys.afk_pass_key(pid)),
        ):
            assert {keys.hash_slot(k) for k in key_tuple} == {slot}

    # 10 段核心 Lua 脚本全部存在
    scripts = (
        lua.REPLACE_SESSION_AND_CANCEL_LEASE_LUA,
        lua.SET_DISCONNECTING_LUA,
        lua.DELETE_SESSION_IF_UNCHANGED_LUA,
        lua.CLAIM_EXPIRED_LEASES_LUA,
        lua.PIN_LEASE_CLAIM_PAYLOAD_LUA,
        lua.COMMIT_LEASE_EXPIRY_LUA,
        lua.ACK_LEASE_CLAIM_LUA,
        lua.RENEW_LEASE_CLAIMS_LUA,
        lua.REARM_LEASE_CLAIM_LUA,
        lua.ENQUEUE_OFFLINE_CLEANUP_LUA,
    )
    assert len(scripts) == 10
    assert all("redis.call" in script for script in scripts)


def test_config_defaults_and_validation():
    cfg = PlayerLocatorConfig()
    assert cfg.grpc_port == DEFAULT_GRPC_PORT == 50300
    assert PLAYER_LOCATOR_NODE_TYPE == 24
    cfg.validate()

    with pytest.raises(ValueError):
        PlayerLocatorConfig(zone_id=0).validate()
    with pytest.raises(ValueError):
        PlayerLocatorConfig(release_track="STABLE").validate()
    with pytest.raises(ValueError):
        PlayerLocatorConfig(grpc_port=70000).validate()


def test_grpc_all_seven_rpcs_and_include_placement():
    async def exercise():
        protocol = Protocol()
        store = MemorySessionStore()
        scene_manager = FakeSceneManager()
        now = [1_700_000_000.0]
        service = PlayerLocatorService(
            protocol, store, PlayerLocatorConfig(), scene_manager=scene_manager, clock=lambda: now[0],
        )
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        grpc_client = GrpcClient(f"127.0.0.1:{port}", timeout=3)
        locator = LoginPlayerLocatorClient(protocol, grpc_client.call)
        try:
            pid = 9007199254740995
            assert await locator.get(pid) is None

            # SetLocation 与 GetLocation 必须 fail-closed 返回错误
            with pytest.raises(grpc.aio.AioRpcError) as set_loc_err:
                req = protocol.message("playerlocator.PlayerLocation", uid=pid, server_id="s-1")
                await grpc_client.call("/playerlocator.PlayerLocator/SetLocation", req.SerializeToString())
            assert set_loc_err.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            with pytest.raises(grpc.aio.AioRpcError) as get_loc_err:
                req = protocol.message("playerlocator.PlayerId", uid=pid)
                await grpc_client.call("/playerlocator.PlayerLocator/GetLocation", req.SerializeToString())
            assert get_loc_err.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            # 首次登录写入
            first = _make_session(protocol, pid, scene_id=0, scene_node_id="", gate_zone_id=3, session_incarnation=9)
            await locator.set(first)
            got = await locator.get(pid)
            assert got is not None
            assert got.player_id == pid
            assert got.gate_zone_id == 3
            assert got.session_incarnation == 9
            assert got.login_generation == 100
            # Gate 索引不变量 I1 / I2 已维护
            slot = keys.player_slot(pid)
            assert str(pid) in store.sets[keys.gate_session_index_key(slot, first.gate_instance_id)]
            assert first.gate_instance_id in store.sets[keys.gate_registry_key(slot)]

            # GetSession 默认不附带 placement
            plain_raw = await grpc_client.call(
                "/playerlocator.PlayerLocator/GetSession",
                protocol.message("playerlocator.GetSessionRequest", player_id=pid).SerializeToString(),
            )
            plain = protocol.parse("playerlocator.GetSessionResponse", plain_raw)
            assert plain.found is True
            assert not plain.HasField("placement")

            # include_placement=True 无 location 键时返回 placement.found=False
            no_loc_raw = await grpc_client.call(
                "/playerlocator.PlayerLocator/GetSession",
                protocol.message(
                    "playerlocator.GetSessionRequest", player_id=pid, include_placement=True,
                ).SerializeToString(),
            )
            no_loc = protocol.parse("playerlocator.GetSessionResponse", no_loc_raw)
            assert no_loc.found is True
            assert no_loc.HasField("placement")
            assert no_loc.placement.found is False

            # 写入 SceneManager 权威 location 后，include_placement 原子返回完整投影
            store.set_location_bytes(
                pid,
                protocol.message(
                    "storage.PlayerLocation",
                    scene_id=8888,
                    node_id="7",
                    zone_id=2,
                    owner_epoch=15,
                    session_id=first.session_id,
                    login_generation=100,
                ).SerializeToString(),
            )
            with_loc_raw = await grpc_client.call(
                "/playerlocator.PlayerLocator/GetSession",
                protocol.message(
                    "playerlocator.GetSessionRequest", player_id=pid, include_placement=True,
                ).SerializeToString(),
            )
            with_loc = protocol.parse("playerlocator.GetSessionResponse", with_loc_raw)
            assert with_loc.placement.found is True
            assert with_loc.placement.zone_id == 2
            assert with_loc.placement.node_id == "7"
            assert with_loc.placement.scene_id == 8888
            assert with_loc.placement.owner_epoch == 15
            assert with_loc.placement.awaiting_placement is False
            assert with_loc.placement.session_id == first.session_id
            assert with_loc.placement.login_generation == 100

            # 跨区交接等待落点（node_id 为空且 owner_epoch 非 0）
            store.set_location_bytes(
                pid,
                protocol.message("storage.PlayerLocation", zone_id=4, owner_epoch=22).SerializeToString(),
            )
            handoff_raw = await grpc_client.call(
                "/playerlocator.PlayerLocator/GetSession",
                protocol.message(
                    "playerlocator.GetSessionRequest", player_id=pid, include_placement=True,
                ).SerializeToString(),
            )
            handoff = protocol.parse("playerlocator.GetSessionResponse", handoff_raw)
            assert handoff.placement.found is True
            assert handoff.placement.awaiting_placement is True
            assert handoff.placement.zone_id == 4
            assert handoff.placement.node_id == ""
        finally:
            await grpc_client.close()
            await server.stop(0)

    asyncio.run(exercise())


def test_grpc_set_session_inheritance_replay_generation_fence_and_aborted_cas():
    async def exercise():
        protocol = Protocol()
        store = MemorySessionStore()
        now = [1_700_000_000.0]
        service = PlayerLocatorService(protocol, store, PlayerLocatorConfig(), clock=lambda: now[0])
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        grpc_client = GrpcClient(f"127.0.0.1:{port}", timeout=3)
        locator = LoginPlayerLocatorClient(protocol, grpc_client.call)
        try:
            pid = 16101
            first = _make_session(
                protocol, pid,
                session_id=10,
                gate_id="7",
                gate_instance_id="gate-A",
                scene_id=777,
                scene_node_id="scene-9",
                version=1,
                login_generation=100,
                session_incarnation=5,
                gate_zone_id=2,
            )
            await locator.set(first)

            # 1. 同请求幂等重放排在代次检查之前：即使 login_generation 非 0 且相等也必须放行
            replay = _make_session(
                protocol, pid,
                session_id=10,
                gate_id="7",
                gate_instance_id="gate-A",
                scene_id=0,  # 继承后与 current 完全相等
                scene_node_id="",
                version=1,
                login_generation=100,
                session_incarnation=5,
                gate_zone_id=2,
            )
            await locator.set(replay)

            # 2. 代次不前进（版本对得上但 generation <= current）必须回 FAILED_PRECONDITION，绝不回 ABORTED
            regressed = _make_session(
                protocol, pid, session_id=11, version=2, login_generation=100,
            )
            with pytest.raises(grpc.aio.AioRpcError) as reg_err:
                await locator.set(regressed)
            assert reg_err.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            # 3. 合法顶号换 gate：仅继承 scene_id/scene_node_id，不继承 incarnation/gate_zone_id/login_generation
            replace = _make_session(
                protocol, pid,
                session_id=20,
                gate_id="8",
                gate_instance_id="gate-B",
                scene_id=0,
                scene_node_id="",
                version=2,
                login_generation=0,  # 旧版 login 传 0 如实写 0，不继承 100
                session_incarnation=0,
                gate_zone_id=0,
            )
            await locator.set(replace)
            stored = await locator.get(pid)
            assert stored.scene_id == 777
            assert stored.scene_node_id == "scene-9"
            assert stored.login_generation == 0
            assert stored.session_incarnation == 0
            assert stored.gate_zone_id == 0
            slot = keys.player_slot(pid)
            assert keys.gate_session_index_key(slot, "gate-A") not in store.sets
            assert "gate-A" not in store.sets.get(keys.gate_registry_key(slot), set())
            assert str(pid) in store.sets[keys.gate_session_index_key(slot, "gate-B")]

            # 4. 同一 gate 上顶号保留索引成员
            same_gate_replace = _make_session(
                protocol, pid,
                session_id=21,
                gate_id="8",
                gate_instance_id="gate-B",
                scene_id=777,
                scene_node_id="scene-9",
                version=3,
                login_generation=200,
                session_incarnation=2,
                gate_zone_id=3,
            )
            await locator.set(same_gate_replace)
            assert str(pid) in store.sets[keys.gate_session_index_key(slot, "gate-B")]

            # 5. 版本失配（如断线推进了版本）必须回 ABORTED -> login 客户端映射为 SessionChanged
            stale_takeover = _make_session(protocol, pid, session_id=22, version=4, login_generation=201)
            await locator.set_disconnecting(pid, 21, "gate-B", 2)
            with pytest.raises(SessionChanged) as ver_err:
                await locator.set(stale_takeover)
            assert _rpc_code(ver_err.value.__cause__) == grpc.StatusCode.ABORTED

            # 6. GET 与 CAS Lua 之间并发篡改导致 Lua 返回 0，同样回 ABORTED
            fresh = await locator.get(pid)
            next_write = _make_session(
                protocol, pid, session_id=23, version=fresh.session_version + 1, login_generation=202,
            )
            triggered = [False]

            def tamper_once(op: str, target_pid: int) -> None:
                if op == "replace" and target_pid == pid and not triggered[0]:
                    triggered[0] = True
                    mutated = _make_session(
                        protocol, pid, session_id=999, version=fresh.session_version + 1, login_generation=202,
                    )
                    store.strings[keys.session_key(pid)] = mutated.SerializeToString()

            store.on_before_cas = tamper_once
            with pytest.raises(SessionChanged) as cas_err:
                await locator.set(next_write)
            assert _rpc_code(cas_err.value.__cause__) == grpc.StatusCode.ABORTED
            store.on_before_cas = None

            # 7. cleanup pending（missing 模式检测到 processing claim 返回 2）必须回 FAILED_PRECONDITION，绝不回 ABORTED
            pending_pid = 16199
            lease = keys.lease_keys_for_player(pending_pid)
            store.zsets[lease.processing] = {str(pending_pid): now[0] + 30}
            with pytest.raises(grpc.aio.AioRpcError) as pending_err:
                await locator.set(_make_session(protocol, pending_pid, version=1))
            assert pending_err.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        finally:
            await grpc_client.close()
            await server.stop(0)

    asyncio.run(exercise())


def test_grpc_set_disconnecting_fences_and_reconnect_rules():
    async def exercise():
        protocol = Protocol()
        store = MemorySessionStore()
        now = [1_700_000_000.0]
        service = PlayerLocatorService(protocol, store, PlayerLocatorConfig(), clock=lambda: now[0])
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        grpc_client = GrpcClient(f"127.0.0.1:{port}", timeout=3)
        locator = LoginPlayerLocatorClient(protocol, grpc_client.call)
        try:
            pid = 18001
            # 空会话 SetDisconnecting 为 no-op
            await locator.set_disconnecting(pid, 7, "gate-I2", 1)

            first = _make_session(
                protocol, pid,
                session_id=7,
                gate_id="7",
                gate_instance_id="gate-I2",
                version=1,
                login_generation=100,
                session_incarnation=2,
                gate_zone_id=2,
                account="hero",
            )
            await locator.set(first)
            before_bytes = store.strings[keys.session_key(pid)]

            # 错误 session_id / 错误 gate_instance_id / 错误 session_incarnation 全部忽略
            await locator.set_disconnecting(pid, 999, "gate-I2", 2)
            assert store.strings[keys.session_key(pid)] == before_bytes

            await locator.set_disconnecting(pid, 7, "gate-I1", 2)
            assert store.strings[keys.session_key(pid)] == before_bytes
            assert service.stale_gate_instance_counts["set_disconnecting"] == 1

            await locator.set_disconnecting(pid, 7, "gate-I2", 1)
            assert store.strings[keys.session_key(pid)] == before_bytes

            # 正确断线：ONLINE -> DISCONNECTING + ZADD ready + 摘 gate 索引
            await grpc_client.call(
                "/playerlocator.PlayerLocator/SetDisconnecting",
                protocol.message(
                    "playerlocator.SetDisconnectingRequest",
                    player_id=pid,
                    session_id=7,
                    lease_ttl_seconds=10,
                    gate_instance_id="gate-I2",
                    session_incarnation=2,
                ).SerializeToString(),
            )
            disc_bytes = store.strings[keys.session_key(pid)]
            disc_session = await locator.get(pid)
            assert disc_session.state == keys.SESSION_STATE_DISCONNECTING
            assert disc_session.session_version == 2
            lease = keys.lease_keys_for_player(pid)
            first_deadline = store.zsets[lease.ready][str(pid)]
            assert first_deadline == int(now[0] + 10)
            slot = keys.player_slot(pid)
            assert keys.gate_session_index_key(slot, "gate-I2") not in store.sets

            # 已 DISCONNECTING 重放（即使给 3600s TTL）绝不延长截止时间、不改记录
            now[0] += 5
            await grpc_client.call(
                "/playerlocator.PlayerLocator/SetDisconnecting",
                protocol.message(
                    "playerlocator.SetDisconnectingRequest",
                    player_id=pid,
                    session_id=7,
                    lease_ttl_seconds=3600,
                    gate_instance_id="gate-I2",
                    session_incarnation=2,
                ).SerializeToString(),
            )
            assert store.strings[keys.session_key(pid)] == disc_bytes
            assert store.zsets[lease.ready][str(pid)] == first_deadline

            # Reconnect 账号不匹配 / 代次不前进均失败
            bad_acc_raw = await grpc_client.call(
                "/playerlocator.PlayerLocator/Reconnect",
                protocol.message(
                    "playerlocator.ReconnectRequest",
                    player_id=pid, new_session_id=8, gate_id="8", gate_instance_id="gate-I3",
                    account="other", request_id="r1", login_generation=101,
                ).SerializeToString(),
            )
            assert protocol.parse("playerlocator.ReconnectResponse", bad_acc_raw).success is False

            stale_gen_raw = await grpc_client.call(
                "/playerlocator.PlayerLocator/Reconnect",
                protocol.message(
                    "playerlocator.ReconnectRequest",
                    player_id=pid, new_session_id=8, gate_id="8", gate_instance_id="gate-I3",
                    account="hero", request_id="r1", login_generation=100,
                ).SerializeToString(),
            )
            assert protocol.parse("playerlocator.ReconnectResponse", stale_gen_raw).success is False

            # 合法重连：取消租约、重建 gate 索引、原样写 incarnation/gate_zone_id/login_generation
            reconn_raw = await grpc_client.call(
                "/playerlocator.PlayerLocator/Reconnect",
                protocol.message(
                    "playerlocator.ReconnectRequest",
                    player_id=pid,
                    new_session_id=8,
                    gate_id="8",
                    gate_instance_id="gate-I3",
                    account="hero",
                    request_id="r1",
                    login_generation=101,
                    session_incarnation=4,
                    gate_zone_id=5,
                ).SerializeToString(),
            )
            reconn = protocol.parse("playerlocator.ReconnectResponse", reconn_raw)
            assert reconn.success is True
            assert reconn.session.state == keys.SESSION_STATE_ONLINE
            assert reconn.session.session_version == 3
            assert reconn.session.session_incarnation == 4
            assert reconn.session.gate_zone_id == 5
            assert reconn.session.login_generation == 101
            assert lease.ready not in store.zsets
            assert str(pid) in store.sets[keys.gate_session_index_key(slot, "gate-I3")]
            committed_bytes = store.strings[keys.session_key(pid)]

            # 重连幂等重放：相同 (session_id, request_id, login_generation) 成功且不改字节
            same_replay_raw = await grpc_client.call(
                "/playerlocator.PlayerLocator/Reconnect",
                protocol.message(
                    "playerlocator.ReconnectRequest",
                    player_id=pid, new_session_id=8, gate_id="8", gate_instance_id="gate-I3",
                    account="hero", request_id="r1", login_generation=101,
                ).SerializeToString(),
            )
            assert protocol.parse("playerlocator.ReconnectResponse", same_replay_raw).success is True
            assert store.strings[keys.session_key(pid)] == committed_bytes

            # 同一幂等键配不同 login_generation（更大或更小）必须拒绝且不改字节
            for bad_gen in (102, 100):
                diff_replay_raw = await grpc_client.call(
                    "/playerlocator.PlayerLocator/Reconnect",
                    protocol.message(
                        "playerlocator.ReconnectRequest",
                        player_id=pid, new_session_id=8, gate_id="8", gate_instance_id="gate-I3",
                        account="hero", request_id="r1", login_generation=bad_gen,
                    ).SerializeToString(),
                )
                resp = protocol.parse("playerlocator.ReconnectResponse", diff_replay_raw)
                assert resp.success is False
                assert "replay generation mismatch" in resp.error_message
                assert store.strings[keys.session_key(pid)] == committed_bytes
        finally:
            await grpc_client.close()
            await server.stop(0)

    asyncio.run(exercise())


def test_grpc_mark_offline_fences_fallback_location_and_cleanup_retry_without_gate_kick():
    async def exercise():
        protocol = Protocol()
        store = MemorySessionStore()
        scene_manager = FakeSceneManager()
        gate_publisher = FakeGatePublisher()
        now = [1_700_000_000.0]
        wakes: list[tuple[int, float]] = []

        config = PlayerLocatorConfig(claim_ttl_sec=5.0)
        monitor = LeaseMonitor(
            protocol, store, config,
            scene_manager=scene_manager,
            gate_publisher=gate_publisher,
            clock=lambda: now[0],
        )

        def record_wake(slot: int, ready_at: float) -> None:
            wakes.append((slot, ready_at))
            monitor.wake(slot, ready_at)

        service = PlayerLocatorService(
            protocol, store, config,
            scene_manager=scene_manager,
            wake_lease_monitor=record_wake,
            clock=lambda: now[0],
        )
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        grpc_client = GrpcClient(f"127.0.0.1:{port}", timeout=3)
        locator = LoginPlayerLocatorClient(protocol, grpc_client.call)
        try:
            pid = 17102
            # 参数校验失败
            with pytest.raises(grpc.aio.AioRpcError) as bad_req:
                await grpc_client.call(
                    "/playerlocator.PlayerLocator/MarkOffline",
                    protocol.message("playerlocator.MarkOfflineRequest", player_id=pid).SerializeToString(),
                )
            assert bad_req.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            # 会话 scene_id=0，依赖 player:{pid}:location 回退；同时种一个活跃 AFK pass 证明正常登出不受月卡保护
            session = _make_session(
                protocol, pid,
                session_id=33,
                gate_id="7",
                gate_instance_id="gate-I2",
                scene_id=0,
                scene_node_id="",
                version=1,
                login_generation=555,
            )
            await locator.set(session)
            store.set_location_bytes(
                pid,
                protocol.message("storage.PlayerLocation", scene_id=9001, node_id="scene-1").SerializeToString(),
            )
            store.set_afk_pass(pid, int(now[0] + 3600))

            # 旧 gate 实例的迟到 MarkOffline 必须忽略且不调用 LeaveScene
            await locator.delete(pid, 33, "gate-I1")
            assert await locator.get(pid) is not None
            assert len(scene_manager.calls) == 0
            assert service.stale_gate_instance_counts["mark_offline"] == 1

            # 正确实例 MarkOffline，但 SceneManager 返回 FAILED_PRECONDITION（持有节点未释放）
            scene_manager.next_error = FakeRpcError(grpc.StatusCode.FAILED_PRECONDITION, "holder_release_pending")
            await locator.delete(pid, 33, "gate-I2")
            assert await locator.get(pid) is None
            assert scene_manager.calls == [LeaveCall(pid, 9001, 33, 555)]

            # 清理条目入队并延后 2s，且唤醒 LeaseMonitor；载荷中 gate_id/gate_instance_id 已清空
            slot = keys.player_slot(pid)
            lease = keys.lease_keys_for_player(pid)
            assert store.zsets[lease.ready][str(pid)] == int(now[0] + 2)
            assert len(wakes) == 1 and wakes[0][0] == slot and wakes[0][1] == now[0] + 2.0

            queued = protocol.parse("playerlocator.PlayerSession", store.hashes[lease.claim_payloads][str(pid)])
            assert queued.state == keys.SESSION_STATE_DISCONNECTING
            assert queued.gate_id == ""
            assert queued.gate_instance_id == ""

            # 1s 后尚未到首拍时刻，不会认领
            now[0] += 1.0
            await monitor.process_slots([slot])
            assert len(scene_manager.calls) == 1

            # 2s 后认领并重试 LeaveScene，仍返回 FAILED_PRECONDITION -> 保留在 processing，阻断首次登录
            now[0] += 1.0
            await monitor.process_slots([slot])
            assert len(scene_manager.calls) == 2
            assert str(pid) in store.zsets[lease.processing]
            assert len(gate_publisher.messages) == 0  # 正常登出重试绝不发 Gate 踢线事件

            with pytest.raises(grpc.aio.AioRpcError) as blocked_login:
                await locator.set(_make_session(protocol, pid, version=1))
            assert blocked_login.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            # 过 claim TTL 且 SceneManager 放行后，重试成功并 ack，且不发任何 GateCommand
            scene_manager.next_error = None
            now[0] += 10.0
            await monitor.process_slots([slot])
            assert len(scene_manager.calls) == 3
            assert lease.processing not in store.zsets
            assert lease.claim_tokens not in store.hashes
            assert lease.claim_payloads not in store.hashes
            assert len(gate_publisher.messages) == 0

            # 清理完成后新登录放行
            await locator.set(_make_session(protocol, pid, version=1))
            assert await locator.get(pid) is not None

            # 非 FAILED_PRECONDITION 错误（如 UNAVAILABLE）立即以 now 入队（不延后 2s）
            pid2 = 17103
            s2 = _make_session(protocol, pid2, session_id=44, gate_id="7", gate_instance_id="gate-I2", scene_id=9002)
            await locator.set(s2)
            scene_manager.next_error = FakeRpcError(grpc.StatusCode.UNAVAILABLE, "scene manager down")
            await locator.delete(pid2, 44, "gate-I2")
            lease2 = keys.lease_keys_for_player(pid2)
            assert store.zsets[lease2.ready][str(pid2)] == int(now[0])
        finally:
            await grpc_client.close()
            await server.stop(0)

    asyncio.run(exercise())


def test_lease_monitor_expiry_gate_event_34_afk_rearm_and_retry():
    async def exercise():
        protocol = Protocol()
        store = MemorySessionStore()
        scene_manager = FakeSceneManager()
        gate_publisher = FakeGatePublisher()
        now = [1_700_000_000.0]
        config = PlayerLocatorConfig(claim_ttl_sec=10.0)
        monitor = LeaseMonitor(
            protocol, store, config,
            scene_manager=scene_manager,
            gate_publisher=gate_publisher,
            clock=lambda: now[0],
        )
        service = PlayerLocatorService(protocol, store, config, scene_manager=scene_manager, clock=lambda: now[0])

        # --- 场景 A：挂机月卡有效 -> 续租 300s；探测异常 -> fail-closed 续租 10s ---
        pid_afk = 20001
        s_afk = _make_session(protocol, pid_afk, session_id=50, gate_id="7", gate_instance_id="gate-7", scene_id=600)
        await service.set_session(protocol.message("playerlocator.SetSessionRequest", session=s_afk), None)
        await service.set_disconnecting(
            protocol.message(
                "playerlocator.SetDisconnectingRequest",
                player_id=pid_afk, session_id=50, lease_ttl_seconds=30, gate_instance_id="gate-7",
            ),
            None,
        )
        slot_afk = keys.player_slot(pid_afk)
        lease_afk = keys.lease_keys_for_player(pid_afk)

        # 月卡有效（剩余 1000s）
        store.set_afk_pass(pid_afk, int(now[0] + 1000))
        now[0] += 30.0
        await monitor.process_slots([slot_afk])
        assert keys.session_key(pid_afk) in store.strings
        assert store.zsets[lease_afk.ready][str(pid_afk)] == int(now[0] + 300)
        assert len(gate_publisher.messages) == 0
        assert len(scene_manager.calls) == 0

        # 月卡探测失败（Redis 抖动）-> fail-closed 顺延 10s
        store.fail_afk_probe = True
        now[0] += 300.0
        await monitor.process_slots([slot_afk])
        assert keys.session_key(pid_afk) in store.strings
        assert store.zsets[lease_afk.ready][str(pid_afk)] == int(now[0] + 10)
        assert len(gate_publisher.messages) == 0
        store.fail_afk_probe = False

        # 月卡已过期 -> 正式清理，但模拟 Gate 投递首拍失败 -> 保留 processing claim 重试
        store.set_afk_pass(pid_afk, int(now[0] - 1))
        gate_publisher.next_error = RuntimeError("kafka broker down")
        now[0] += 10.0
        await monitor.process_slots([slot_afk])
        assert keys.session_key(pid_afk) not in store.strings  # commit 已删会话
        assert str(pid_afk) in store.zsets[lease_afk.processing]  # 未 ack，保留重试
        assert len(scene_manager.calls) == 1  # Gate 失败不短路 LeaveScene

        # Gate 恢复，claim 超时后重试：发送 event_id=34 的 GateCommand + LeaveScene，随后 ack
        gate_publisher.next_error = None
        now[0] += 15.0
        await monitor.process_slots([slot_afk])
        assert lease_afk.processing not in store.zsets
        assert lease_afk.claim_tokens not in store.hashes
        assert lease_afk.claim_payloads not in store.hashes
        assert len(gate_publisher.messages) == 1
        published = gate_publisher.messages[0]
        assert published.gate_node_id == 7
        assert published.player_id == pid_afk
        assert published.command.event_id == 34
        assert published.command.target_gate_id == 7
        assert published.command.target_instance_id == "gate-7"
        assert published.command.player_id == pid_afk
        assert published.command.session_id == 50
        inner = protocol.parse("contracts.kafka.PlayerLeaseExpiredEvent", published.command.payload)
        assert inner.player_id == pid_afk
        assert inner.session_id == 50
        assert inner.scene_id == 600
        assert inner.scene_node_id == "scene-1"
        assert inner.gate_id == "7"
        assert inner.gate_instance_id == "gate-7"

    asyncio.run(exercise())


def test_redis_session_store_registers_all_ten_scripts_and_passes_exact_keys():
    async def exercise():
        registered: list[str] = []
        invocations: list[tuple[str, list[str], list]] = []

        class StubPipeline:
            def __init__(self, transaction):
                assert transaction is False
                self.ops: list[tuple[str, str]] = []

            def zcount(self, key: str, _min, _max):
                self.ops.append(("zcount", key))

            async def execute(self, raise_on_error: bool = False):
                assert raise_on_error is False
                return [1, 0]

        class StubRedis:
            def register_script(self, script: str):
                registered.append(script)

                async def run(*, keys, args):
                    invocations.append((script, list(keys), list(args)))
                    if script is lua.CLAIM_EXPIRED_LEASES_LUA:
                        return [b"42", b"tok:42"]
                    if script is lua.PIN_LEASE_CLAIM_PAYLOAD_LUA:
                        return b"payload-bytes"
                    return 1

                return run

            async def get(self, key: str):
                return b"raw-" + key.encode()

            async def mget(self, *req_keys: str):
                return [b"s-bytes", b"l-bytes"]

            def pipeline(self, *, transaction: bool = True):
                return StubPipeline(transaction)

        store = RedisSessionStore(StubRedis())
        assert len(registered) == 10

        s_bytes, l_bytes = await store.get_session_and_location_bytes(42)
        assert (s_bytes, l_bytes) == (b"s-bytes", b"l-bytes")

        res = await store.replace_session_and_cancel_lease(42, None, b"new", 0, "", "gate-1")
        assert res == ReplaceSessionResult.APPLIED
        assert invocations[-1][1] == list(keys.replace_session_keys(42, "", "gate-1"))
        assert invocations[-1][2] == ["missing", b"", b"new", 0, "42", "", "gate-1"]

        slot = keys.player_slot(42)
        due = await store.probe_due_lease_slots([slot], 1000)
        assert due == [slot]

        claims = await store.claim_expired_leases(slot, 1000, 1030, 10, batch_token="tok")
        assert claims == [LeaseClaim(42, "tok:42", b"payload-bytes")]
        assert await store.commit_lease_expiry(claims[0]) == CommitLeaseResult.COMMITTED
        assert await store.rearm_lease_claim(claims[0], 1300) == RearmLeaseResult.REARMED
        assert await store.ack_lease_claim(claims[0]) is True
        assert await store.renew_lease_claims(claims, 1060) == 1
        assert await store.enqueue_offline_cleanup(42, b"retry", 1002) is True

    asyncio.run(exercise())

