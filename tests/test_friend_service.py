"""friend 服务端到端 gRPC、Gate Ed25519/HMAC 鉴权、权威事务、在线目录、角色护栏与 SQL 绑定测试。"""

from __future__ import annotations

import asyncio
import functools
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.data.id_segments import mix_guid
from xuanming.friend import (
    COORDINATOR_SECRET_ENV,
    DATABASE_NAME,
    DEFAULT_GRPC_PORT,
    DEFAULT_LEASE_TTL_SEC,
    ERR_ALREADY_FRIENDS,
    ERR_BLOCKED,
    ERR_BLOCK_LIST_FULL,
    ERR_CANNOT_ADD_SELF,
    ERR_FRIEND_LIST_FULL,
    ERR_INVALID_PARAMETER,
    ERR_NO_PENDING_REQUEST,
    ERR_RATE_LIMITED,
    ERR_REQUEST_ALREADY_SENT,
    ERR_ROLE_NOT_VERIFIED,
    ERR_STORAGE,
    ERR_TARGET_FRIEND_LIST_FULL,
    ERR_TARGET_INBOX_FULL,
    ERR_TOO_MANY_PENDING,
    FRIEND_EVENT_REASON_REQUEST_ACCEPTED,
    FRIEND_EVENT_REASON_REQUEST_RECEIVED,
    FRIEND_NODE_TYPE,
    FRIEND_SERVICE,
    FriendConfig,
    FriendRules,
    FriendService,
    GrpcFriendDataServiceClient,
    KafkaFriendPusher,
    MemoryFriendStore,
    NOTIFY_FRIEND_EVENT_MSG_ID,
    PUSH_TO_PLAYER_EVENT_ID,
    PlayerBrief,
    REQUEST_STATUS_ACCEPTED,
    REQUEST_STATUS_PENDING,
    REQUEST_STATUS_REJECTED,
    ROLE_FENCE_SECRET_ENV,
    ROLE_FENCE_SERVICE,
    RedisQuotaLimiter,
    RedisSessionStore,
    RedisSyncDigestBumper,
    SCHEMA_FILE,
    SYNC_DIGEST_BUMP_SCRIPT,
    SYNC_DOMAIN_FRIEND_BLOCK,
    SYNC_DOMAIN_FRIEND_LIST,
    SYNC_DOMAIN_FRIEND_REQUEST,
    SqlFriendStore,
    player_session_key,
    sync_digest_key,
)
from xuanming.friend.errors import RoleFenceUnavailableError, StoreUnavailableError
from xuanming.friend.model import ActorIdentity, AddFriendLimits, DIRECTORY_QUOTA_PER_MINUTE, SYNC_DIGEST_TTL_SEC
from xuanming.friend.store import _MemRequestRow
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata, sign_service_call, verify_service_call
from xuanming.login.caller_auth import CallerVerifier, SESSION_DETAIL
from xuanming.protocol import Protocol
from xuanming.settings import Settings

FRIEND_PREFIX = f"/{FRIEND_SERVICE}/"
FENCE_PREFIX = f"/{ROLE_FENCE_SERVICE}/"
COORDINATOR_SECRET = b"friend-role-fence-coordinator-01234"
FRIEND_ROLE_FENCE_SECRET = b"friend-role-fence-caller-secret-012"


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


# ---------------------------------------------------------------------------
# 测试替身：时钟、DataService、Redis、Kafka、FakePool
# ---------------------------------------------------------------------------


class FakeClock:
    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self.ms = start_ms

    def now_ms(self) -> int:
        return self.ms

    def advance(self, delta_ms: int) -> None:
        self.ms += delta_ms


class FakeDataService:
    """内存 DataService 替身：提供角色护栏、friend_request 发号（含 mix_guid）与展示资料查询。"""

    def __init__(self, protocol: Protocol) -> None:
        self.protocol = protocol
        self.fences: dict[int, object] = {}
        self.displays: dict[int, PlayerBrief] = {}
        self.names: dict[int, str] = {}
        self.home_zones: dict[int, int] = {}
        self.next_seq = 1
        self.allocated_ids: list[int] = []
        self.fence_unavailable = False
        self.display_error = False
        self.home_zone_error = False

    def register_role(
        self,
        player_id: int,
        account: str,
        *,
        owner_revision: int = 1,
        version: int = 1,
        home_zone: int = 1,
        freeze_token: int = 0,
        freeze_final: bool = False,
        name: str = "",
        level: int = 10,
        class_id: int = 1,
        gender: int = 1,
        appearance_id: str = "look_1",
    ) -> None:
        self.fences[player_id] = self.protocol.message(
            "rolefence.RoleFenceState",
            player_id=player_id,
            account=account,
            owner_revision=owner_revision,
            freeze_token=freeze_token,
            freeze_final=freeze_final,
            version=version,
            home_zone=home_zone,
        )
        self.home_zones[player_id] = home_zone
        if name:
            self.displays[player_id] = PlayerBrief(
                name=name, level=level, class_id=class_id, gender=gender, appearance_id=appearance_id
            )
            self.names[player_id] = name

    async def get_role_fence_state(self, player_id: int):
        if self.fence_unavailable:
            raise RoleFenceUnavailableError("injected fence outage")
        return self.fences.get(player_id)

    async def allocate_request_id(self) -> int:
        val = mix_guid(self.next_seq)
        self.next_seq += 1
        self.allocated_ids.append(val)
        return val

    async def batch_get_player_display(self, player_ids):
        if self.display_error:
            raise RuntimeError("display RPC unavailable")
        return {pid: self.displays[pid] for pid in player_ids if pid in self.displays}

    async def batch_get_player_name(self, player_ids):
        return {pid: self.names[pid] for pid in player_ids if pid in self.names}

    async def batch_get_player_home_zone(self, player_ids):
        if self.home_zone_error:
            raise RuntimeError("home_zone RPC unavailable")
        return {pid: self.home_zones[pid] for pid in player_ids if pid in self.home_zones}


class FakeRedis:
    """支持 get/mget/scan/eval 的轻量内存 Redis 替身，可注入故障与分页批次。"""

    def __init__(self) -> None:
        self.kv: dict[str, bytes] = {}
        self.counters: dict[str, int] = {}
        self.digest_evals: list[tuple[str, tuple[str, ...]]] = []
        self.quota_evals: list[tuple[str, tuple[str, ...]]] = []
        self.scan_page_size = 64
        self.fail_mget = False
        self.fail_eval = False
        self.fail_scan = False
        self.mget_short = False

    def set_session(
        self,
        protocol: Protocol,
        player_id: int,
        *,
        state: int = 1,
        last_active_ts: int = 1_700_000_000_123,
        session_id: int = 500,
        gate_id: str = "1",
        gate_instance_id: str = "gate-inst-1",
        override_player_id: int | None = None,
    ) -> None:
        msg = protocol.message(
            "playerlocator.PlayerSession",
            player_id=player_id if override_player_id is None else override_player_id,
            session_id=session_id,
            gate_id=gate_id,
            gate_instance_id=gate_instance_id,
            state=state,
            last_active_ts=last_active_ts,
        )
        self.kv[player_session_key(player_id)] = msg.SerializeToString()

    async def get(self, key: str):
        if self.fail_mget:
            raise RuntimeError("redis get down")
        return self.kv.get(key)

    async def mget(self, keys: list[str]):
        if self.fail_mget:
            raise RuntimeError("redis mget down")
        if self.mget_short and keys:
            return []
        return [self.kv.get(k) for k in keys]

    async def scan(self, *, cursor: int = 0, match: str = "*", count: int = 64):
        if self.fail_scan:
            raise RuntimeError("redis scan down")
        matched = sorted(k for k in self.kv if k.startswith("player:session:{") and k.endswith("}"))
        step = min(self.scan_page_size, count)
        batch = matched[cursor : cursor + step]
        next_cur = cursor + step if cursor + step < len(matched) else 0
        return next_cur, [k.encode("utf-8") for k in batch]

    async def eval(self, script: str, numkeys: int, *keys_and_args):
        if self.fail_eval:
            raise RuntimeError("redis eval down")
        keys = keys_and_args[:numkeys]
        args = tuple(str(a) for a in keys_and_args[numkeys:])
        if script == SYNC_DIGEST_BUMP_SCRIPT:
            self.digest_evals.append((str(keys[0]), args))
            return 1
        key = str(keys[0])
        self.quota_evals.append((key, args))
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]


class FakeKafkaPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, int, object, bytes | None]] = []

    async def publish(self, role: str, node_id: int, message, *, key: bytes | None = None) -> None:
        self.published.append((role, node_id, message, key))


class Harness:
    """启动真实 gRPC 服务（serve_grpc(..., pass_context=True)）与客户端的测试装配器。"""

    def __init__(
        self,
        *,
        rules: FriendRules | None = None,
        with_sessions: bool = True,
        with_quota: bool = True,
        with_kafka: bool = True,
    ) -> None:
        self.protocol = Protocol()
        self.clock = FakeClock()
        self.gate_key = Ed25519PrivateKey.generate()
        self.verifier = CallerVerifier((self.gate_key.public_key(),), allowed_callers=("gate",))
        self.ds = FakeDataService(self.protocol)
        self.player_redis = FakeRedis()
        self.control_redis = FakeRedis()
        self.kafka = FakeKafkaPublisher()
        self.store = MemoryFriendStore(
            self.protocol,
            self.ds,
            rules if rules is not None else FriendRules(),
            clock_ms=self.clock.now_ms,
        )
        self.sessions = RedisSessionStore(self.player_redis, self.protocol) if with_sessions else None
        self.quota = RedisQuotaLimiter(self.control_redis) if with_quota else None
        self.sync_digest = RedisSyncDigestBumper(self.player_redis)
        self.pusher = (
            KafkaFriendPusher(self.protocol, self.sessions, self.kafka, clock_ms=self.clock.now_ms)
            if (with_kafka and self.sessions is not None)
            else None
        )
        self.service = FriendService(
            self.protocol,
            self.store,
            self.verifier,
            coordinator_secret=COORDINATOR_SECRET,
            sessions=self.sessions,
            quota=self.quota,
            sync_digest=self.sync_digest,
            pusher=self.pusher,
            clock_ms=self.clock.now_ms,
        )
        self.client: GrpcClient | None = None

    def register_player(self, player_id: int, account: str, **kwargs) -> None:
        self.ds.register_role(player_id, account, **kwargs)

    def session_md(
        self,
        method: str,
        player_id: int,
        account: str,
        *,
        owner_revision: int = 1,
        signer: Ed25519PrivateKey | None = None,
        session_id: int = 100,
        gate_instance_id: str = "gate-1",
        session_incarnation: int = 1,
    ) -> tuple:
        details = self.protocol.message(
            "SessionDetails",
            session_id=session_id,
            player_id=player_id,
            gate_instance_id=gate_instance_id,
            session_incarnation=session_incarnation,
            authenticated_account=account,
            owner_revision=owner_revision,
        )
        return session_metadata(
            details.SerializeToString(),
            method,
            self.gate_key if signer is None else signer,
        )

    async def call_friend(
        self,
        rpc: str,
        req_name: str,
        resp_name: str,
        player_id: int,
        account: str,
        *,
        owner_revision: int = 1,
        metadata: tuple | None = None,
        **fields,
    ):
        assert self.client is not None
        path = FRIEND_PREFIX + rpc
        req = self.protocol.message(req_name, **fields)
        md = self.session_md(path, player_id, account, owner_revision=owner_revision) if metadata is None else metadata
        raw = await self.client.call(path, req.SerializeToString(), metadata=md)
        return self.protocol.parse(resp_name, raw)

    async def call_fence(
        self,
        rpc: str,
        req_name: str,
        resp_name: str,
        *,
        secret: bytes | None = COORDINATOR_SECRET,
        caller: str = "coordinator",
        metadata: tuple = (),
        **fields,
    ):
        assert self.client is not None
        path = FENCE_PREFIX + rpc
        req = self.protocol.message(req_name, **fields)
        if secret is not None:
            sign_service_call(path, req, caller, self.clock.now_ms(), secret)
        raw = await self.client.call(path, req.SerializeToString(), metadata=metadata)
        return self.protocol.parse(resp_name, raw)

    @asynccontextmanager
    async def running(self):
        async with AsyncExitStack() as stack:
            server, port = await serve_grpc(
                self.protocol, "127.0.0.1:0", self.service.services(), pass_context=True
            )
            stack.push_async_callback(server.stop, 0.5)
            client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)
            stack.push_async_callback(client.close)
            self.client = client
            yield self


# ---------------------------------------------------------------------------
# 1. Gate Ed25519 / HMAC / 非 C2S 鉴权测试
# ---------------------------------------------------------------------------


@_async_test
async def test_gate_auth_and_non_c2s_rejection():
    h = Harness()
    h.register_player(1001, "acc_a")
    h.register_player(1002, "acc_b")
    async with h.running():
        assert h.client is not None
        # 缺会话头 -> PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await h.call_friend(
                "GetFriendList",
                "friendpb.GetFriendListRequest",
                "friendpb.GetFriendListResponse",
                1001,
                "acc_a",
                metadata=(),
            )
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 错误私钥签名 -> UNAUTHENTICATED
        bad_key = Ed25519PrivateKey.generate()
        bad_md = h.session_md(FRIEND_PREFIX + "GetFriendList", 1001, "acc_a", signer=bad_key)
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await h.call_friend(
                "GetFriendList",
                "friendpb.GetFriendListRequest",
                "friendpb.GetFriendListResponse",
                1001,
                "acc_a",
                metadata=bad_md,
            )
        assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # 不完整原身份（player_id=0 / owner_revision=0 / 空 account）-> PERMISSION_DENIED
        for pid, acc, rev in ((0, "acc_a", 1), (1001, "acc_a", 0), (1001, "", 1)):
            md = h.session_md(FRIEND_PREFIX + "GetFriendList", pid, acc, owner_revision=rev)
            with pytest.raises(grpc.aio.AioRpcError) as exc:
                await h.call_friend(
                    "GetFriendList",
                    "friendpb.GetFriendListRequest",
                    "friendpb.GetFriendListResponse",
                    1001,
                    "acc_a",
                    metadata=md,
                )
            assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # NotifyFriendEvent：带客户端会话 -> PERMISSION_DENIED；无会话 -> UNIMPLEMENTED
        push_path = FRIEND_PREFIX + "NotifyFriendEvent"
        push_req = h.protocol.message("friendpb.FriendEventS2C", reason=1, by_player_id=1001).SerializeToString()
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await h.client.call(push_path, push_req, metadata=h.session_md(push_path, 1001, "acc_a"))
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await h.client.call(push_path, push_req)
        assert exc.value.code() == grpc.StatusCode.UNIMPLEMENTED

        # RoleFenceParticipant：带客户端会话 -> PERMISSION_DENIED；错 HMAC/错 caller -> PERMISSION_DENIED
        state = h.ds.fences[1001]
        fence_md = h.session_md(FENCE_PREFIX + "ApplyRoleFence", 1001, "acc_a")
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await h.call_fence(
                "ApplyRoleFence",
                "rolefence.ApplyRoleFenceRequest",
                "rolefence.ApplyRoleFenceResponse",
                state=state,
                metadata=fence_md,
            )
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await h.call_fence(
                "ApplyRoleFence",
                "rolefence.ApplyRoleFenceRequest",
                "rolefence.ApplyRoleFenceResponse",
                state=state,
                secret=b"wrong-coordinator-secret-0123456789",
            )
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await h.call_fence(
                "ApplyRoleFence",
                "rolefence.ApplyRoleFenceRequest",
                "rolefence.ApplyRoleFenceResponse",
                state=state,
                caller="friend",
            )
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED


# ---------------------------------------------------------------------------
# 2. 好友申请 / 幂等重放 / 反向收敛 / 拒绝 / 删好友 / 推送与同步摘要
# ---------------------------------------------------------------------------


@_async_test
async def test_add_accept_reject_remove_lifecycle_and_notifications():
    h = Harness()
    h.register_player(101, "acc_101", name="Alice", level=25, class_id=2, home_zone=3)
    h.register_player(102, "acc_102", name="Bob", level=30, class_id=3, home_zone=3)
    h.player_redis.set_session(h.protocol, 101, state=1, last_active_ts=5555, session_id=11, gate_id="2", gate_instance_id="g-2")
    h.player_redis.set_session(h.protocol, 102, state=1, last_active_ts=6666, session_id=22, gate_id="3", gate_instance_id="g-3")

    async with h.running():
        # 参数边界：target=0 -> 1005，target=me -> 15000
        r0 = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 101, "acc_101", target_player_id=0)
        assert r0.error_message.id == ERR_INVALID_PARAMETER
        rself = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 101, "acc_101", target_player_id=101)
        assert rself.error_message.id == ERR_CANNOT_ADD_SELF

        # 发起申请：使用 mix_guid(1) 发号，触发 target 的 FRIEND_REQUEST 摘要与 Kafka 推送(event=39, msg=235)
        add1 = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 101, "acc_101", target_player_id=102)
        assert add1.error_message.id == 0
        req_id_1 = int(add1.request_id)
        assert req_id_1 == mix_guid(1)
        assert len(h.ds.allocated_ids) == 1
        assert h.player_redis.digest_evals == [(sync_digest_key(102), (str(SYNC_DIGEST_TTL_SEC), str(SYNC_DOMAIN_FRIEND_REQUEST)))]
        assert len(h.kafka.published) == 1
        role, gate_node_id, gate_cmd, key = h.kafka.published[0]
        assert role == "gate" and gate_node_id == 3 and key == b"102"
        assert int(gate_cmd.event_id) == PUSH_TO_PLAYER_EVENT_ID
        assert int(gate_cmd.target_gate_id) == 3 and gate_cmd.target_instance_id == "g-3"
        push_ev = h.protocol.parse("contracts.kafka.PushToPlayerEvent", gate_cmd.payload)
        assert int(push_ev.message_content.message_id) == NOTIFY_FRIEND_EVENT_MSG_ID
        friend_ev = h.protocol.parse("friendpb.FriendEventS2C", push_ev.message_content.serialized_message)
        assert int(friend_ev.reason) == FRIEND_EVENT_REASON_REQUEST_RECEIVED
        assert int(friend_ev.by_player_id) == 101

        # 重复 pending 重放：返回同一 request_id，不领新号、不刷新时间、不重复推送/抬摘要
        t0 = h.clock.now_ms()
        h.clock.advance(5000)
        add_replay = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 101, "acc_101", target_player_id=102)
        assert add_replay.error_message.id == 0
        assert int(add_replay.request_id) == req_id_1
        assert len(h.ds.allocated_ids) == 1
        assert len(h.kafka.published) == 1
        assert len(h.player_redis.digest_evals) == 1

        pending = await h.call_friend("GetPendingRequests", "friendpb.GetPendingRequestsRequest", "friendpb.GetPendingRequestsResponse", 102, "acc_102")
        assert len(pending.requests) == 1
        assert int(pending.requests[0].request_id) == req_id_1
        assert int(pending.requests[0].request_time_ms) == t0

        # 反向申请 102 -> 101，验收 AcceptFriend 一并收敛反向 pending 且忽略伪造的 from_player_id
        add_rev = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 102, "acc_102", target_player_id=101)
        assert add_rev.error_message.id == 0
        assert int(add_rev.request_id) == mix_guid(2)

        h.player_redis.digest_evals.clear()
        h.kafka.published.clear()
        acc_resp = await h.call_friend(
            "AcceptFriend",
            "friendpb.AcceptFriendRequest",
            "friendpb.AcceptFriendResponse",
            102,
            "acc_102",
            from_player_id=999_999,  # 刻意伪造，服务端必须忽略并按 request_id 定位
            request_id=req_id_1,
        )
        assert acc_resp.error_message.id == 0
        # 双方均收到 FRIEND_LIST + FRIEND_REQUEST 摘要 bump，且推 REQUEST_ACCEPTED 给原申请人 101
        assert [k for k, _ in h.player_redis.digest_evals] == [sync_digest_key(102), sync_digest_key(101)]
        assert h.player_redis.digest_evals[0][1] == (
            str(SYNC_DIGEST_TTL_SEC),
            str(SYNC_DOMAIN_FRIEND_LIST),
            str(SYNC_DOMAIN_FRIEND_REQUEST),
        )
        assert len(h.kafka.published) == 1
        _, g_id, cmd2, k2 = h.kafka.published[0]
        assert g_id == 2 and k2 == b"101"
        ev2 = h.protocol.parse(
            "friendpb.FriendEventS2C",
            h.protocol.parse("contracts.kafka.PushToPlayerEvent", cmd2.payload).message_content.serialized_message,
        )
        assert int(ev2.reason) == FRIEND_EVENT_REASON_REQUEST_ACCEPTED and int(ev2.by_player_id) == 102

        # 反向 pending 已被收敛，101 收件箱为空
        pending_101 = await h.call_friend("GetPendingRequests", "friendpb.GetPendingRequestsRequest", "friendpb.GetPendingRequestsResponse", 101, "acc_101")
        assert len(pending_101.requests) == 0

        # 同代次 AcceptFriend 幂等重放不重复写/推/抬摘要
        h.player_redis.digest_evals.clear()
        h.kafka.published.clear()
        acc_replay = await h.call_friend(
            "AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 102, "acc_102", request_id=req_id_1
        )
        assert acc_replay.error_message.id == 0
        assert not h.player_redis.digest_evals and not h.kafka.published

        # 已是好友再 AddFriend -> 15001
        dup_friend = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 101, "acc_101", target_player_id=102)
        assert dup_friend.error_message.id == ERR_ALREADY_FRIENDS

        # GetFriendList 返回在线状态 + 档案补全
        fl = await h.call_friend("GetFriendList", "friendpb.GetFriendListRequest", "friendpb.GetFriendListResponse", 101, "acc_101")
        assert fl.error_message.id == 0 and len(fl.friends) == 1
        f0 = fl.friends[0]
        assert int(f0.friend_player_id) == 102 and f0.is_online is True and int(f0.last_active_ms) == 6666
        assert f0.name == "Bob" and int(f0.level) == 30 and int(f0.class_id) == 3 and int(f0.zone_id) == 3

        # RemoveFriend 删非好友：不创建目标 capacity 行、不抬摘要、不推送
        h.register_player(199, "acc_199")
        h.player_redis.digest_evals.clear()
        rm_noop = await h.call_friend("RemoveFriend", "friendpb.RemoveFriendRequest", "friendpb.RemoveFriendResponse", 101, "acc_101", target_player_id=199)
        assert rm_noop.error_message.id == 0
        assert 199 not in h.store.mem_state.capacities
        assert not h.player_redis.digest_evals and not h.kafka.published

        # RemoveFriend 删真好友：删双边、减计数、仅抬双方 FRIEND_LIST，不推送
        rm_ok = await h.call_friend("RemoveFriend", "friendpb.RemoveFriendRequest", "friendpb.RemoveFriendResponse", 101, "acc_101", target_player_id=102)
        assert rm_ok.error_message.id == 0
        assert h.store.mem_state.capacities[101].friend_count == 0
        assert h.store.mem_state.capacities[102].friend_count == 0
        assert [k for k, _ in h.player_redis.digest_evals] == [sync_digest_key(101), sync_digest_key(102)]
        assert not h.kafka.published

        # 终态后重新发起申请必须换新代次；RejectFriend 忽略伪造 from_player_id 且不推送
        add_new = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 101, "acc_101", target_player_id=102)
        assert add_new.error_message.id == 0 and int(add_new.request_id) != req_id_1
        h.player_redis.digest_evals.clear()
        h.kafka.published.clear()
        rej = await h.call_friend(
            "RejectFriend",
            "friendpb.RejectFriendRequest",
            "friendpb.RejectFriendResponse",
            102,
            "acc_102",
            from_player_id=777_777,
            request_id=int(add_new.request_id),
        )
        assert rej.error_message.id == 0
        assert h.player_redis.digest_evals == [(sync_digest_key(102), (str(SYNC_DIGEST_TTL_SEC), str(SYNC_DOMAIN_FRIEND_REQUEST)))]
        assert not h.kafka.published
        # 旧代次不可再接受
        stale_acc = await h.call_friend("AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 102, "acc_102", request_id=req_id_1)
        assert stale_acc.error_message.id == ERR_NO_PENDING_REQUEST


# ---------------------------------------------------------------------------
# 3. 非对称好友满员映射 + 出入站申请上限 + 限流 + 历史 request_id=0 回填
# ---------------------------------------------------------------------------


@_async_test
async def test_asymmetric_limits_pending_caps_quota_and_legacy_backfill():
    rules = FriendRules(
        max_friends=1,
        max_pending_requests=1,
        max_incoming_requests=1,
        max_blocks=1,
        request_quota_per_minute=20,
    )
    h = Harness(rules=rules)
    for pid in (1, 2, 3, 4, 5):
        h.register_player(pid, f"acc_{pid}")

    async with h.running():
        # 先让 1 和 2 成为好友（双方达到 max_friends=1）
        r12 = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 1, "acc_1", target_player_id=2)
        assert r12.error_message.id == 0
        # 1 出站已达 1 -> 再向 3 发申请 -> ERR_TOO_MANY_PENDING (15006)
        r13 = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 1, "acc_1", target_player_id=3)
        assert r13.error_message.id == ERR_TOO_MANY_PENDING
        # 3 向 2 发申请，2 入站已满 -> ERR_TARGET_INBOX_FULL (15009)
        r32 = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 3, "acc_3", target_player_id=2)
        assert r32.error_message.id == ERR_TARGET_INBOX_FULL

        # 同时让 4 向 3 发一条申请待处理，再让 1 和 2 结为好友
        r43 = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 4, "acc_4", target_player_id=3)
        assert r43.error_message.id == 0
        acc12 = await h.call_friend("AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 2, "acc_2", request_id=int(r12.request_id))
        assert acc12.error_message.id == 0

        # AddFriend 非对称满员：
        # 1（已满）加 5（未满）-> SenderFull -> 15002 (ERR_FRIEND_LIST_FULL)
        full_self = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 1, "acc_1", target_player_id=5)
        assert full_self.error_message.id == ERR_FRIEND_LIST_FULL
        # 5（未满）加 1（已满）-> AcceptorFull -> 15004 (ERR_TARGET_FRIEND_LIST_FULL)
        full_target = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 5, "acc_5", target_player_id=1)
        assert full_target.error_message.id == ERR_TARGET_FRIEND_LIST_FULL

        # AcceptFriend 非对称满员（与 AddFriend 相反）：
        # 先把 3 和 5 结为好友（3 满，4 未满），此时 3 接受 4->3 -> AcceptorFull -> 15002
        # 再让 4 和 5 的申请改为 4 满、3 未满 -> SenderFull -> 15004
        r35 = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 3, "acc_3", target_player_id=5)
        assert r35.error_message.id == 0
        assert (await h.call_friend("AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 5, "acc_5", request_id=int(r35.request_id))).error_message.id == 0
        acc_self_full = await h.call_friend("AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 3, "acc_3", request_id=int(r43.request_id))
        assert acc_self_full.error_message.id == ERR_FRIEND_LIST_FULL

        # 移除 3-5 后让 4 与 5 成为好友，再由 3 接受 4->3 -> SenderFull -> 15004
        assert (await h.call_friend("RemoveFriend", "friendpb.RemoveFriendRequest", "friendpb.RemoveFriendResponse", 3, "acc_3", target_player_id=5)).error_message.id == 0
        r54 = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 5, "acc_5", target_player_id=4)
        assert r54.error_message.id == 0
        assert (await h.call_friend("AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 4, "acc_4", request_id=int(r54.request_id))).error_message.id == 0
        acc_sender_full = await h.call_friend("AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 3, "acc_3", request_id=int(r43.request_id))
        assert acc_sender_full.error_message.id == ERR_TARGET_FRIEND_LIST_FULL

        # 历史 request_id=0 待处理申请在 GetPendingRequests 时按需补号并保留原始 request_time_ms
        h.store.mem_state.requests[(2, 5)] = _MemRequestRow(
            from_player_id=2,
            to_player_id=5,
            request_time_ms=123_456,
            status=REQUEST_STATUS_PENDING,
            updated_ms=123_456,
            request_id=0,
        )
        p5 = await h.call_friend("GetPendingRequests", "friendpb.GetPendingRequestsRequest", "friendpb.GetPendingRequestsResponse", 5, "acc_5")
        assert len(p5.requests) == 1
        assert int(p5.requests[0].request_id) != 0
        assert int(p5.requests[0].request_time_ms) == 123_456


@_async_test
async def test_request_quota_rate_limit_and_fail_open():
    h = Harness(rules=FriendRules(request_quota_per_minute=2))
    for pid in (10, 11, 12, 13, 14):
        h.register_player(pid, f"acc_{pid}")
    async with h.running():
        assert (await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 10, "acc_10", target_player_id=11)).error_message.id == 0
        assert (await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 10, "acc_10", target_player_id=12)).error_message.id == 0
        rl = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 10, "acc_10", target_player_id=13)
        assert rl.error_message.id == ERR_RATE_LIMITED

        # ControlRedis 故障时 AddFriend 频控必须 fail-open 放行
        h.control_redis.fail_eval = True
        ok = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 10, "acc_10", target_player_id=14)
        assert ok.error_message.id == 0


# ---------------------------------------------------------------------------
# 4. 拉黑 / 解除拉黑 / 中性 15007 / 清理边与双向 pending
# ---------------------------------------------------------------------------


@_async_test
async def test_block_unblock_neutral_error_and_side_effects():
    h = Harness(rules=FriendRules(max_blocks=1))
    for pid in (201, 202, 203):
        h.register_player(pid, f"acc_{pid}")
    async with h.running():
        # Block 自己或 0 -> 1005（绝不能是 15000）
        assert (await h.call_friend("Block", "friendpb.BlockRequest", "friendpb.BlockResponse", 201, "acc_201", target_player_id=0)).error_message.id == ERR_INVALID_PARAMETER
        assert (await h.call_friend("Block", "friendpb.BlockRequest", "friendpb.BlockResponse", 201, "acc_201", target_player_id=201)).error_message.id == ERR_INVALID_PARAMETER
        assert (await h.call_friend("Unblock", "friendpb.UnblockRequest", "friendpb.UnblockResponse", 201, "acc_201", target_player_id=201)).error_message.id == ERR_INVALID_PARAMETER

        # 成为好友后拉黑：应删边、减计数、抬双方 FRIEND_LIST（不推送）
        add_r = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 201, "acc_201", target_player_id=202)
        assert (await h.call_friend("AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 202, "acc_202", request_id=int(add_r.request_id))).error_message.id == 0
        h.player_redis.digest_evals.clear()
        h.kafka.published.clear()

        blk = await h.call_friend("Block", "friendpb.BlockRequest", "friendpb.BlockResponse", 201, "acc_201", target_player_id=202)
        assert blk.error_message.id == 0
        assert (201, 202) not in h.store.mem_state.friends and (202, 201) not in h.store.mem_state.friends
        assert h.store.mem_state.capacities[201].friend_count == 0
        assert h.store.mem_state.capacities[202].friend_count == 0
        assert not h.kafka.published
        # 被拉黑方收到 FRIEND_LIST（因为边被删），但不收 FRIEND_BLOCK / FRIEND_REQUEST
        peer_bumps = [args for k, args in h.player_redis.digest_evals if k == sync_digest_key(202)]
        assert peer_bumps == [(str(SYNC_DIGEST_TTL_SEC), str(SYNC_DOMAIN_FRIEND_LIST))]

        # 两个方向 AddFriend 均返回中性码 15007
        assert (await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 201, "acc_201", target_player_id=202)).error_message.id == ERR_BLOCKED
        assert (await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 202, "acc_202", target_player_id=201)).error_message.id == ERR_BLOCKED

        # 黑名单已满：拉黑新人 -> 15008 且不创建 203 的 capacity 行；幂等重拉黑 202 仍成功
        full_blk = await h.call_friend("Block", "friendpb.BlockRequest", "friendpb.BlockResponse", 201, "acc_201", target_player_id=203)
        assert full_blk.error_message.id == ERR_BLOCK_LIST_FULL
        assert 203 not in h.store.mem_state.capacities
        re_blk = await h.call_friend("Block", "friendpb.BlockRequest", "friendpb.BlockResponse", 201, "acc_201", target_player_id=202)
        assert re_blk.error_message.id == 0

        blocks = await h.call_friend("ListBlocks", "friendpb.ListBlocksRequest", "friendpb.ListBlocksResponse", 201, "acc_201")
        assert [int(b.blocked_player_id) for b in blocks.blocks] == [202]

        # Unblock 解除拉黑，不恢复好友关系
        unblk = await h.call_friend("Unblock", "friendpb.UnblockRequest", "friendpb.UnblockResponse", 201, "acc_201", target_player_id=202)
        assert unblk.error_message.id == 0
        fl = await h.call_friend("GetFriendList", "friendpb.GetFriendListRequest", "friendpb.GetFriendListResponse", 201, "acc_201")
        assert len(fl.friends) == 0

        # 双向 pending 在拉黑时一并置为 REJECTED，且对端收到 FRIEND_REQUEST 摘要 bump
        r_a = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 201, "acc_201", target_player_id=202)
        r_b = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 202, "acc_202", target_player_id=201)
        assert r_a.error_message.id == 0 and r_b.error_message.id == 0
        h.player_redis.digest_evals.clear()
        assert (await h.call_friend("Block", "friendpb.BlockRequest", "friendpb.BlockResponse", 201, "acc_201", target_player_id=202)).error_message.id == 0
        assert h.store.mem_state.requests[(201, 202)].status == REQUEST_STATUS_REJECTED
        assert h.store.mem_state.requests[(202, 201)].status == REQUEST_STATUS_REJECTED
        peer_bumps = [args for k, args in h.player_redis.digest_evals if k == sync_digest_key(202)]
        assert peer_bumps == [(str(SYNC_DIGEST_TTL_SEC), str(SYNC_DOMAIN_FRIEND_REQUEST))]


# ---------------------------------------------------------------------------
# 5. GetFriendList 强闭环在线状态（fail-closed）与尽力而为档案补全
# ---------------------------------------------------------------------------


@_async_test
async def test_get_friend_list_fail_closed_online_and_best_effort_profiles():
    h = Harness()
    h.register_player(301, "acc_301", name="P1", home_zone=2)
    h.register_player(302, "acc_302", home_zone=2)
    # 302 在 display 缺失 name，但 BatchGetPlayerName 有值，检验回源兜底
    h.ds.displays[302] = PlayerBrief(name="", level=18, class_id=4, gender=2, appearance_id="app2")
    h.ds.names[302] = "FallbackName"
    h.player_redis.set_session(h.protocol, 302, state=2, last_active_ts=9999)  # DISCONNECTING = 离线

    async with h.running():
        r = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 301, "acc_301", target_player_id=302)
        assert (await h.call_friend("AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 302, "acc_302", request_id=int(r.request_id))).error_message.id == 0

        # DISCONNECTING 视为离线；姓名经 BatchGetPlayerName 补全
        fl = await h.call_friend("GetFriendList", "friendpb.GetFriendListRequest", "friendpb.GetFriendListResponse", 301, "acc_301")
        assert fl.error_message.id == 0 and len(fl.friends) == 1
        assert fl.friends[0].is_online is False and fl.friends[0].name == "FallbackName" and int(fl.friends[0].zone_id) == 2

        # 展示资料 RPC 失败：尽力而为不隐藏真实好友关系与在线状态
        h.player_redis.set_session(h.protocol, 302, state=1, last_active_ts=8888)
        h.ds.display_error = True
        fl_degraded = await h.call_friend("GetFriendList", "friendpb.GetFriendListRequest", "friendpb.GetFriendListResponse", 301, "acc_301")
        assert fl_degraded.error_message.id == 0 and len(fl_degraded.friends) == 1
        assert fl_degraded.friends[0].is_online is True and int(fl_degraded.friends[0].last_active_ms) == 8888
        h.ds.display_error = False

        # PlayerRedis mget 故障 -> 必须 fail-closed 回 1003
        h.player_redis.fail_mget = True
        fl_err = await h.call_friend("GetFriendList", "friendpb.GetFriendListRequest", "friendpb.GetFriendListResponse", 301, "acc_301")
        assert fl_err.error_message.id == ERR_STORAGE
        h.player_redis.fail_mget = False

        # 坏 protobuf -> fail-closed 回 1003
        h.player_redis.kv[player_session_key(302)] = b"\xff\xff\xff\xff"
        fl_corrupt = await h.call_friend("GetFriendList", "friendpb.GetFriendListRequest", "friendpb.GetFriendListResponse", 301, "acc_301")
        assert fl_corrupt.error_message.id == ERR_STORAGE

        # PlayerSession.player_id 身份不符 -> fail-closed 回 1003
        h.player_redis.set_session(h.protocol, 302, state=1, override_player_id=999)
        fl_mismatch = await h.call_friend("GetFriendList", "friendpb.GetFriendListRequest", "friendpb.GetFriendListResponse", 301, "acc_301")
        assert fl_mismatch.error_message.id == ERR_STORAGE


# ---------------------------------------------------------------------------
# 6. RecommendFriends（普通推荐 + 在线目录分页/搜索/同区/配额）
# ---------------------------------------------------------------------------


@_async_test
async def test_recommend_friends_normal_and_online_directory():
    h = Harness(rules=FriendRules(recommend_default_limit=5, recommend_max_limit=10, recommend_max_exclude=4))
    for pid in range(401, 412):
        zone = 1 if pid != 409 else 2
        h.register_player(pid, f"acc_{pid}", name=f"Hero_{pid}", level=0 if pid == 405 else 20, class_id=1, home_zone=zone)

    # 构造图：401 的好友是 402、403；404 与 402/403 均是好友（mutual=2）；405 只与 402 是好友（mutual=1）；406-407 是孤立好友对（random 兜底）
    for a, b in ((401, 402), (401, 403), (402, 404), (403, 404), (402, 405), (406, 407)):
        h.store.mem_state.friends[(a, b)] = 1000
        h.store.mem_state.friends[(b, a)] = 1000

    # 设置在线会话：404、405（level=0 允许）、406、409（异区）、410（无 class_id）
    h.player_redis.set_session(h.protocol, 404, state=1, last_active_ts=4040)
    h.player_redis.set_session(h.protocol, 405, state=1, last_active_ts=4050)
    h.player_redis.set_session(h.protocol, 406, state=1, last_active_ts=4060)
    h.player_redis.set_session(h.protocol, 409, state=1, last_active_ts=4090)
    h.player_redis.set_session(h.protocol, 410, state=1, last_active_ts=4100)
    h.ds.displays[410] = PlayerBrief(name="NoClass", level=5, class_id=0, gender=1, appearance_id="")

    async with h.running():
        # exclude 越界 -> 1005
        bad_ex = await h.call_friend(
            "RecommendFriends",
            "friendpb.RecommendFriendsRequest",
            "friendpb.RecommendFriendsResponse",
            401,
            "acc_401",
            exclude_player_ids=[1, 2, 3, 4, 5],
        )
        assert bad_ex.error_message.id == ERR_INVALID_PARAMETER

        # 普通推荐：先 mutual（404 mutual=2, 405 mutual=1），再 random 兜底（406/407）
        rec = await h.call_friend(
            "RecommendFriends",
            "friendpb.RecommendFriendsRequest",
            "friendpb.RecommendFriendsResponse",
            401,
            "acc_401",
            limit=5,
        )
        assert rec.error_message.id == 0 and rec.online_directory is False
        cids = [int(c.candidate_player_id) for c in rec.candidates]
        assert cids[:2] == [404, 405]
        assert int(rec.candidates[0].mutual_friends) == 2 and rec.candidates[0].is_online is True
        assert int(rec.candidates[1].mutual_friends) == 1
        assert set(cids).isdisjoint({401, 402, 403})

        # 在线目录：同区过滤（排除 409）、跳过 class_id=0（排除 410）、允许 level=0（保留 405）、支持分页游标
        h.player_redis.scan_page_size = 2
        page1 = await h.call_friend(
            "RecommendFriends",
            "friendpb.RecommendFriendsRequest",
            "friendpb.RecommendFriendsResponse",
            401,
            "acc_401",
            online_only=True,
            limit=2,
        )
        assert page1.error_message.id == 0 and page1.online_directory is True
        assert [int(c.candidate_player_id) for c in page1.candidates] == [404, 405]
        assert int(page1.candidates[1].level) == 0 and int(page1.candidates[1].zone_id) == 1
        assert page1.next_cursor.startswith("v2:00000000.0.")

        page2 = await h.call_friend(
            "RecommendFriends",
            "friendpb.RecommendFriendsRequest",
            "friendpb.RecommendFriendsResponse",
            401,
            "acc_401",
            online_only=True,
            limit=2,
            cursor=page1.next_cursor,
        )
        assert page2.error_message.id == 0 and page2.online_directory is True
        assert [int(c.candidate_player_id) for c in page2.candidates] == [406]
        assert page2.next_cursor == ""

        # 在线目录按昵称 / ID 搜索
        q_resp = await h.call_friend(
            "RecommendFriends",
            "friendpb.RecommendFriendsRequest",
            "friendpb.RecommendFriendsResponse",
            401,
            "acc_401",
            online_only=True,
            query="405",
        )
        assert q_resp.error_message.id == 0 and [int(c.candidate_player_id) for c in q_resp.candidates] == [405]

        # 非法 cursor / 超长 query -> 1005 且 online_directory=True
        bad_cur = await h.call_friend(
            "RecommendFriends",
            "friendpb.RecommendFriendsRequest",
            "friendpb.RecommendFriendsResponse",
            401,
            "acc_401",
            online_only=True,
            cursor="bad-cursor",
        )
        assert bad_cur.error_message.id == ERR_INVALID_PARAMETER and bad_cur.online_directory is True

        bad_q = await h.call_friend(
            "RecommendFriends",
            "friendpb.RecommendFriendsRequest",
            "friendpb.RecommendFriendsResponse",
            401,
            "acc_401",
            online_only=True,
            query="x" * 65,
        )
        assert bad_q.error_message.id == ERR_INVALID_PARAMETER and bad_q.online_directory is True

        # 在线目录限流：Redis 故障 fail-closed -> 1003；超 60 次/分钟 -> 1008
        h.control_redis.fail_eval = True
        dir_err = await h.call_friend(
            "RecommendFriends",
            "friendpb.RecommendFriendsRequest",
            "friendpb.RecommendFriendsResponse",
            401,
            "acc_401",
            online_only=True,
        )
        assert dir_err.error_message.id == ERR_STORAGE and dir_err.online_directory is True
        h.control_redis.fail_eval = False

        h.control_redis.counters["friend:{directory:401}"] = DIRECTORY_QUOTA_PER_MINUTE
        dir_rl = await h.call_friend(
            "RecommendFriends",
            "friendpb.RecommendFriendsRequest",
            "friendpb.RecommendFriendsResponse",
            401,
            "acc_401",
            online_only=True,
        )
        assert dir_rl.error_message.id == ERR_RATE_LIMITED and dir_rl.online_directory is True


# ---------------------------------------------------------------------------
# 7. RoleFenceParticipant（NoPending + RolePurge）与护栏拦截（1006）
# ---------------------------------------------------------------------------


@_async_test
async def test_role_fence_participant_freeze_unfreeze_and_purge():
    h = Harness()
    h.register_player(501, "acc_501")
    h.register_player(502, "acc_502")
    h.register_player(503, "acc_503")

    async with h.running():
        # 建立 501-502 好友关系、503->501 申请、501 拉黑 503 的测试数据
        r = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 501, "acc_501", target_player_id=502)
        assert (await h.call_friend("AcceptFriend", "friendpb.AcceptFriendRequest", "friendpb.AcceptFriendResponse", 502, "acc_502", request_id=int(r.request_id))).error_message.id == 0
        r503 = await h.call_friend("AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 503, "acc_503", target_player_id=501)
        assert r503.error_message.id == 0
        assert (await h.call_friend("Block", "friendpb.BlockRequest", "friendpb.BlockResponse", 501, "acc_501", target_player_id=503)).error_message.id == 0
        assert h.store.mem_state.capacities[502].friend_count == 1

        # dry_run PrepareRoleFreeze -> clean=True（NoPending）
        freeze_state = h.protocol.message(
            "rolefence.RoleFenceState",
            player_id=501,
            account="acc_501",
            owner_revision=1,
            freeze_token=888,
            freeze_final=False,
            version=2,
            home_zone=1,
        )
        dry = await h.call_fence(
            "PrepareRoleFreeze",
            "rolefence.PrepareRoleFreezeRequest",
            "rolefence.PrepareRoleFreezeResponse",
            state=freeze_state,
            dry_run=True,
        )
        assert dry.clean is True and not dry.stale and not dry.retry

        # 真实冻结并同步权威目录 -> 501 写操作回 1006
        h.ds.fences[501] = freeze_state
        prep = await h.call_fence(
            "PrepareRoleFreeze",
            "rolefence.PrepareRoleFreezeRequest",
            "rolefence.PrepareRoleFreezeResponse",
            state=freeze_state,
            dry_run=False,
        )
        assert prep.clean is True

        frozen_try = await h.call_friend("ListBlocks", "friendpb.ListBlocksRequest", "friendpb.ListBlocksResponse", 501, "acc_501")
        assert frozen_try.error_message.id == ERR_ROLE_NOT_VERIFIED

        # 过时版本的 PrepareRoleFreeze -> stale=True
        stale_state = h.protocol.message(
            "rolefence.RoleFenceState",
            player_id=501,
            account="acc_501",
            owner_revision=1,
            freeze_token=777,
            freeze_final=False,
            version=1,
            home_zone=1,
        )
        stale_resp = await h.call_fence(
            "PrepareRoleFreeze",
            "rolefence.PrepareRoleFreezeRequest",
            "rolefence.PrepareRoleFreezeResponse",
            state=stale_state,
        )
        assert stale_resp.stale is True and not stale_resp.clean

        # 护栏目录不可达 -> 1006
        h.ds.fence_unavailable = True
        unavail = await h.call_friend("ListBlocks", "friendpb.ListBlocksRequest", "friendpb.ListBlocksResponse", 501, "acc_501")
        assert unavail.error_message.id == ERR_ROLE_NOT_VERIFIED
        h.ds.fence_unavailable = False

        # 非墓碑调 PurgeRemovedRole -> INVALID_ARGUMENT
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await h.call_fence(
                "PurgeRemovedRole",
                "rolefence.PurgeRemovedRoleRequest",
                "rolefence.PurgeRemovedRoleResponse",
                state=freeze_state,
            )
        assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT

        # 墓碑态 PurgeRemovedRole：清理 501 全部好友边/申请/拉黑/容量行，且 502 的 friend_count 归零
        tombstone = h.protocol.message(
            "rolefence.RoleFenceState",
            player_id=501,
            account="",
            owner_revision=1,
            freeze_token=0,
            freeze_final=False,
            version=3,
            home_zone=1,
        )
        h.ds.fences[501] = tombstone
        purge_resp = await h.call_fence(
            "PurgeRemovedRole",
            "rolefence.PurgeRemovedRoleRequest",
            "rolefence.PurgeRemovedRoleResponse",
            state=tombstone,
        )
        assert purge_resp.retry is False
        assert not any(501 in pair for pair in h.store.mem_state.friends)
        assert not any(501 in pair for pair in h.store.mem_state.requests)
        assert not any(501 in pair for pair in h.store.mem_state.blocks)
        assert 501 not in h.store.mem_state.capacities
        assert h.store.mem_state.capacities[502].friend_count == 0


@_async_test
async def test_role_fence_unfreeze_absent_peer_owner_transfer_and_accept_blocked():
    h = Harness()
    h.register_player(601, "acc_601")
    h.register_player(602, "acc_602")

    async with h.running():
        # 1) 对端 699 在目录中不存在：按 Go beginGuardedTx 跳过缺席对端，AddFriend 仍可创建申请
        add_absent_peer = await h.call_friend(
            "AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 601, "acc_601", target_player_id=699
        )
        assert add_absent_peer.error_message.id == 0

        # 2) 调用者 698 在目录中不存在 -> 1006
        add_absent_actor = await h.call_friend(
            "AddFriend", "friendpb.AddFriendRequest", "friendpb.AddFriendResponse", 698, "acc_698", target_player_id=601
        )
        assert add_absent_actor.error_message.id == ERR_ROLE_NOT_VERIFIED

        # 3) 冻结后通过 ApplyRoleFence 解冻（更高版本）-> 业务恢复
        frozen = h.protocol.message(
            "rolefence.RoleFenceState",
            player_id=601,
            account="acc_601",
            owner_revision=1,
            freeze_token=99,
            freeze_final=False,
            version=2,
            home_zone=1,
        )
        h.ds.fences[601] = frozen
        assert (
            await h.call_fence(
                "PrepareRoleFreeze",
                "rolefence.PrepareRoleFreezeRequest",
                "rolefence.PrepareRoleFreezeResponse",
                state=frozen,
            )
        ).clean is True
        assert (
            await h.call_friend("GetFriendList", "friendpb.GetFriendListRequest", "friendpb.GetFriendListResponse", 601, "acc_601")
        ).error_message.id == ERR_ROLE_NOT_VERIFIED

        unfrozen = h.protocol.message(
            "rolefence.RoleFenceState",
            player_id=601,
            account="acc_601",
            owner_revision=1,
            freeze_token=0,
            freeze_final=False,
            version=3,
            home_zone=1,
        )
        h.ds.fences[601] = unfrozen
        assert (
            await h.call_fence(
                "ApplyRoleFence",
                "rolefence.ApplyRoleFenceRequest",
                "rolefence.ApplyRoleFenceResponse",
                state=unfrozen,
            )
        ).retry is False
        assert (
            await h.call_friend("GetFriendList", "friendpb.GetFriendListRequest", "friendpb.GetFriendListResponse", 601, "acc_601")
        ).error_message.id == 0

        # 4) 角色过户（owner_revision 升至 2）-> 旧会话 rev=1 返回 1006
        transferred = h.protocol.message(
            "rolefence.RoleFenceState",
            player_id=601,
            account="acc_new_owner",
            owner_revision=2,
            freeze_token=0,
            freeze_final=False,
            version=4,
            home_zone=1,
        )
        h.ds.fences[601] = transferred
        await h.call_fence(
            "ApplyRoleFence",
            "rolefence.ApplyRoleFenceRequest",
            "rolefence.ApplyRoleFenceResponse",
            state=transferred,
        )
        assert (
            await h.call_friend(
                "GetFriendList",
                "friendpb.GetFriendListRequest",
                "friendpb.GetFriendListResponse",
                601,
                "acc_601",
                owner_revision=1,
            )
        ).error_message.id == ERR_ROLE_NOT_VERIFIED
        # 新主会话 rev=2 放行
        assert (
            await h.call_friend(
                "GetFriendList",
                "friendpb.GetFriendListRequest",
                "friendpb.GetFriendListResponse",
                601,
                "acc_new_owner",
                owner_revision=2,
            )
        ).error_message.id == 0

        # 5) AcceptFriend 命中任意方向拉黑 -> 15007
        r = await h.call_friend(
            "AddFriend",
            "friendpb.AddFriendRequest",
            "friendpb.AddFriendResponse",
            601,
            "acc_new_owner",
            owner_revision=2,
            target_player_id=602,
        )
        assert r.error_message.id == 0
        h.store.mem_state.blocks[(602, 601)] = 1000
        acc_blk = await h.call_friend(
            "AcceptFriend",
            "friendpb.AcceptFriendRequest",
            "friendpb.AcceptFriendResponse",
            602,
            "acc_602",
            request_id=int(r.request_id),
        )
        assert acc_blk.error_message.id == ERR_BLOCKED

        # 6) 目标离线/断线/gate_instance_id 为空时推送不抛错且不发 Kafka
        h.kafka.published.clear()
        h.player_redis.set_session(h.protocol, 602, state=2, gate_id="1", gate_instance_id="g1")
        h.store.mem_state.blocks.clear()
        assert (
            await h.call_friend(
                "AcceptFriend",
                "friendpb.AcceptFriendRequest",
                "friendpb.AcceptFriendResponse",
                602,
                "acc_602",
                request_id=int(r.request_id),
            )
        ).error_message.id == 0
        assert not h.kafka.published


# ---------------------------------------------------------------------------
# 8. 出站 GrpcFriendDataServiceClient（HMAC "friend" 验签 + AllocateIdSegment）
# ---------------------------------------------------------------------------


@_async_test
async def test_outbound_grpc_data_service_client_signing_and_id_allocation():
    protocol = Protocol()
    clock = FakeClock()
    seen_auth_ok: list[bool] = []
    seen_tags: list[str] = []

    async def handle_get_role_fence(request, context, path):
        ok = verify_service_call(path, request, "friend", clock.now_ms(), FRIEND_ROLE_FENCE_SECRET)
        seen_auth_ok.append(ok)
        if not ok:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "bad sig")
        state = protocol.message(
            "rolefence.RoleFenceState",
            player_id=request.player_id,
            account="acc_out",
            owner_revision=2,
            freeze_token=0,
            freeze_final=False,
            version=5,
            home_zone=1,
        )
        return protocol.message("data_service.GetRoleFenceStateResponse", found=True, state=state)

    async def handle_allocate(request, context, path):
        seen_tags.append(request.biz_tag)
        return protocol.message("data_service.AllocateIdSegmentResponse", lo=10, hi=12)

    async with AsyncExitStack() as stack:
        server, port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            {
                "data_service.DataService": {
                    "GetRoleFenceState": handle_get_role_fence,
                    "AllocateIdSegment": handle_allocate,
                }
            },
            pass_context=True,
        )
        stack.push_async_callback(server.stop, 0.5)
        client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)
        stack.push_async_callback(client.close)

        ds_client = GrpcFriendDataServiceClient(
            protocol,
            client.call,
            FRIEND_ROLE_FENCE_SECRET,
            clock_ms=clock.now_ms,
            id_segment_size=2,
        )
        state = await ds_client.get_role_fence_state(777)
        assert state is not None and state.account == "acc_out" and seen_auth_ok == [True]
        id1 = await ds_client.allocate_request_id()
        id2 = await ds_client.allocate_request_id()
        assert (id1, id2) == (mix_guid(10), mix_guid(11))
        assert seen_tags == ["friend_request"]


# ---------------------------------------------------------------------------
# 9. SqlFriendStore DDL、UNHEX 绑定、结构校验与锁序验证 + FriendConfig 校验
# ---------------------------------------------------------------------------


class _FakeSqlCursor:
    def __init__(self, log: list[tuple[str, tuple]], responses: dict[str, list]) -> None:
        self._log = log
        self._responses = responses
        self._rows: list = []
        self.rowcount = 1

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def execute(self, sql: str, params=()):
        norm = " ".join(sql.split())
        self._log.append((norm, tuple(params) if isinstance(params, (tuple, list)) else (params,)))
        self._rows = []
        self.rowcount = 1
        for key, queue in self._responses.items():
            if key in norm and queue:
                item = queue.pop(0)
                if isinstance(item, tuple) and len(item) == 2 and item[0] == "__rowcount__":
                    self.rowcount = item[1]
                else:
                    self._rows = list(item)
                break

    async def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    async def fetchall(self):
        rows, self._rows = self._rows, []
        return rows


class _FakeSqlConn:
    def __init__(self, log: list[tuple[str, tuple]], responses: dict[str, list]) -> None:
        self._log = log
        self._responses = responses
        self.commits = 0
        self.rollbacks = 0

    async def begin(self) -> None:
        self._log.append(("BEGIN", ()))

    async def commit(self) -> None:
        self.commits += 1
        self._log.append(("COMMIT", ()))

    async def rollback(self) -> None:
        self.rollbacks += 1
        self._log.append(("ROLLBACK", ()))

    def cursor(self):
        return _FakeSqlCursor(self._log, self._responses)


class _FakeSqlPool:
    def __init__(self, responses: dict[str, list] | None = None) -> None:
        self.log: list[tuple[str, tuple]] = []
        self.responses: dict[str, list] = responses if responses is not None else {}

    def acquire(self):
        pool = self

        @asynccontextmanager
        async def _cm():
            yield _FakeSqlConn(pool.log, pool.responses)

        return _cm()


@_async_test
async def test_sql_friend_store_ddl_verify_unhex_and_lock_order():
    ddl = SCHEMA_FILE.read_text("utf-8")
    for table in ("`friend`", "`friend_request`", "`friend_capacity`", "`friend_block`", "role_fence"):
        assert table in ddl
    assert "idx_friend_request_2" in ddl and "VARBINARY(191)" in ddl

    protocol = Protocol()
    ds = FakeDataService(protocol)
    ds.register_role(20, "acc_20")
    ds.register_role(10, "acc_10")

    # 1) bootstrap + verify_schema
    pool = _FakeSqlPool(
        responses={
            "information_schema.statistics WHERE table_schema=DATABASE() AND table_name='friend_request'": [[(1,)]],
            "information_schema.COLUMNS": [[("varbinary", 191)]],
            "INDEX_NAME='PRIMARY'": [[("player_id",)]],
        }
    )
    sql_store = SqlFriendStore(pool, protocol, ds, FriendRules())
    await sql_store.bootstrap()
    create_stmts = [sql for sql, _ in pool.log if sql.startswith("CREATE TABLE")]
    assert len(create_stmts) == 5
    await sql_store.verify_schema()

    # 2) apply_role_fence 使用 UNHEX(%s) 绑定 UTF-8 账号字节
    pool.log.clear()
    await sql_store.apply_role_fence(ds.fences[10])
    apply_sql, apply_params = pool.log[-1]
    assert "UNHEX(%s)" in apply_sql
    assert apply_params[1] == b"acc_10".hex()

    # 3) CreateRequest 严格遵守锁序：role_fence(10->20) -> friend_capacity(10->20) -> 单行主键点锁
    fence_10 = (b"acc_10", 1, 0, 0, 1, 1, 100)
    fence_20 = (b"acc_20", 1, 0, 0, 1, 1, 100)
    pool2 = _FakeSqlPool(
        responses={
            # ensure_bound_capacity + 主写事务第 1 轮（无号触发领号）+ ensure + 主写事务第 2 轮
            "FROM role_fence WHERE player_id=%s FOR UPDATE": [
                [fence_10], [fence_20],  # ensure 1
                [fence_10], [fence_20],  # main 1 (needs request_id)
                [fence_10], [fence_20],  # ensure 2
                [fence_10], [fence_20],  # main 2 (commits)
            ],
            "SELECT COUNT(*) FROM friend WHERE player_id = %s": [[(0,)], [(0,)], [(0,)], [(0,)]],
            "SELECT friend_count FROM friend_capacity WHERE player_id = %s FOR UPDATE": [
                [(0,)], [(0,)],  # main 1
                [(0,)], [(0,)],  # main 2
            ],
            "SELECT COUNT(*) FROM friend_request WHERE from_player_id": [[(0,)], [(0,)]],
            "SELECT COUNT(*) FROM friend_request WHERE to_player_id": [[(0,)], [(0,)]],
        }
    )
    sql_store2 = SqlFriendStore(pool2, protocol, ds, FriendRules())
    actor = ActorIdentity(player_id=20, account="acc_20", owner_revision=1)
    change = await sql_store2.create_request(actor, 10, AddFriendLimits(200, 50, 200))
    assert change.changed is True and change.request_id == mix_guid(1)

    # 提取最后一次已提交事务中的 SQL 序列，验证 role_fence(10, 20) 先于 friend_capacity(10, 20) 先于点锁
    begins = [i for i, (s, _) in enumerate(pool2.log) if s == "BEGIN"]
    last_tx = pool2.log[begins[-1] :]
    lock_seq = [(s, p) for s, p in last_tx if "FOR UPDATE" in s]
    assert lock_seq[0][0].startswith("SELECT account") and lock_seq[0][1] == (10,)
    assert lock_seq[1][0].startswith("SELECT account") and lock_seq[1][1] == (20,)
    assert "FROM friend_capacity" in lock_seq[2][0] and lock_seq[2][1] == (10,)
    assert "FROM friend_capacity" in lock_seq[3][0] and lock_seq[3][1] == (20,)
    assert "FROM friend_block" in lock_seq[4][0] and lock_seq[4][1] == (20, 10)
    assert "FROM friend_block" in lock_seq[5][0] and lock_seq[5][1] == (10, 20)
    assert "FROM friend WHERE" in lock_seq[6][0] and lock_seq[6][1] == (20, 10)
    assert "FROM friend WHERE" in lock_seq[7][0] and lock_seq[7][1] == (10, 20)
    assert "FROM friend_request" in lock_seq[8][0] and lock_seq[8][1] == (20, 10)


def test_friend_config_defaults_and_fail_closed_validation():
    assert FRIEND_NODE_TYPE == 27
    assert DEFAULT_GRPC_PORT == 50400
    assert DEFAULT_LEASE_TTL_SEC == 60
    assert DATABASE_NAME == "mmorpg_friend"

    settings = SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="stable",
        data_service="127.0.0.1:50500",
        redis="redis://127.0.0.1:6379/0",
        kafka="",
        kafka_generation=1,
        kafka_partitions=256,
        etcd="127.0.0.1:2379",
        friend_node={"node_id": 110, "advertise_host": "127.0.0.1"},
    )
    env = {
        COORDINATOR_SECRET_ENV: COORDINATOR_SECRET.decode("utf-8"),
        ROLE_FENCE_SECRET_ENV: FRIEND_ROLE_FENCE_SECRET.decode("utf-8"),
    }
    cfg = FriendConfig.from_settings(settings, environ=env)
    assert cfg.grpc_port == 50400 and cfg.lease_ttl_sec == 60 and cfg.request_budget_sec == 3.5

    # 禁止在 TOML 中明文配置密钥
    bad_settings = SimpleNamespace(
        **{
            **settings.__dict__,
            "friend_node": {"node_id": 110, "advertise_host": "127.0.0.1", "coordinator_secret": "leak"},
        }
    )
    with pytest.raises(ValueError):
        FriendConfig.from_settings(bad_settings, environ=env)

    # 缺少环境变量密钥 -> fail-closed 拒启
    with pytest.raises(ValueError):
        FriendConfig.from_settings(settings, environ={})
