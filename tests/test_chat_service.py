"""chat 服务端到端 gRPC、Gate Ed25519 鉴权、权威存储、世界近窗单飞、内容安全与 SQL 绑定测试。"""

from __future__ import annotations

import asyncio
import base64
import functools
import hashlib
import struct
from contextlib import AsyncExitStack
from pathlib import Path

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper

from xuanming.chat import (
    ActorIdentity,
    CHAT_NODE_TYPE,
    CHAT_SERVICE,
    ChatConfig,
    ChatRules,
    ChatService,
    DEFAULT_GRPC_PORT,
    DEFAULT_LEASE_TTL_SEC,
    ERR_CHANNEL_UNAVAILABLE,
    ERR_CONTACT_REJECTED,
    ERR_CONTENT_CHECK_UNAVAILABLE,
    ERR_CONTENT_REJECTED,
    ERR_DUPLICATE_SPAM,
    ERR_INVALID_PARAMETER,
    ERR_LEVEL_TOO_LOW,
    ERR_MESSAGE_NOT_FOUND,
    ERR_MESSAGE_TOO_LONG,
    ERR_PRIVATE_TARGET_LIMIT,
    ERR_RATE_LIMITED,
    GrpcActorVerifier,
    GrpcContentReviewer,
    KafkaWorldPusher,
    MemoryChatStore,
    OriginalRoleError,
    PassthroughContentReviewer,
    PlayerProfile,
    PolicyMismatchError,
    REVIEW_MASKED,
    REVIEW_PASS,
    REVIEW_REJECT,
    REVIEW_UNAVAILABLE,
    ROLE_FENCE_SECRET_ENV,
    ReviewResult,
    SqlChatStore,
    WORLD_BROADCAST_MSG_ID,
    WORLD_RETRACT_MSG_ID,
    WORLD_STREAM_SHARDS,
    WorldSnapshot,
    WorldWindow,
    policy_bytes,
    policy_hash,
    stream_key,
    world_stream_key,
)
from xuanming.chat.model import (
    CHANNEL_PRIVATE,
    CHANNEL_SYSTEM,
    CHANNEL_TEAM,
    CHANNEL_UNSPECIFIED,
    CHANNEL_WORLD,
    HistoryRow,
    ORIGINAL_RETENTION_DAYS,
    TEXT_CATEGORY_AD,
    TEXT_SCENE_CHAT_PRIVATE,
    TEXT_SCENE_CHAT_WORLD,
    TEXT_VERDICT_MASKED,
    TEXT_VERDICT_PASS,
    TEXT_VERDICT_REJECT,
    normalize_spam_hash,
)
from xuanming.chat.sql_store import SCHEMA_FILE, has_retention_attributes
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata, verify_service_call
from xuanming.login.caller_auth import CallerVerifier, SESSION_DETAIL
from xuanming.protocol import Protocol
from xuanming.settings import Settings

CHAT_SVC_PREFIX = f"/{CHAT_SERVICE}/"
ROLE_FENCE_SECRET = b"chat-role-fence-secret-0123456789"


# ---------------------------------------------------------------------------
# 测试辅助替身
# ---------------------------------------------------------------------------


class FakeClock:
    """可手动推进的确定性毫秒与单调秒时钟。"""

    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self.ms = start_ms
        self.mono = 100.0

    def now_ms(self) -> int:
        return self.ms

    def now_mono(self) -> float:
        return self.mono

    def advance_ms(self, delta_ms: int) -> None:
        self.ms += delta_ms
        self.mono += delta_ms / 1000.0


class FakeActorVerifier:
    """可配置的内存角色归属校验器。"""

    def __init__(self, allowed: dict[int, tuple[str, int]] | None = None) -> None:
        self.allowed: dict[int, tuple[str, int]] = dict(allowed or {})
        self.calls: list[ActorIdentity] = []

    async def verify_actor(self, identity: ActorIdentity) -> None:
        self.calls.append(identity)
        if not self.allowed:
            return
        entry = self.allowed.get(identity.player_id)
        if entry is None or entry != (identity.account, identity.owner_revision):
            raise OriginalRoleError("actor mismatch")


class FakeProfileReader:
    """可配置的玩家档案读取器。"""

    def __init__(self, profiles: dict[int, PlayerProfile] | None = None) -> None:
        self.profiles = dict(profiles or {})

    async def get_player_profile(self, player_id: int) -> PlayerProfile:
        return self.profiles.get(player_id, PlayerProfile())


class FakeReviewer:
    """可编程的内容安全审核器，支持同步判定与异步平台复检。"""

    def __init__(self) -> None:
        self.sync_handler = None
        self.platform_handler = None
        self.sync_calls: list[tuple] = []
        self.platform_calls: list[tuple] = []
        self.platform_done = asyncio.Event()

    async def check_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
        async_platform: bool = False,
    ) -> ReviewResult:
        self.sync_calls.append((scene, player_id, text, account, async_platform))
        if self.sync_handler is not None:
            return self.sync_handler(scene, player_id, text, account, async_platform)
        return ReviewResult(verdict=REVIEW_PASS)

    async def check_platform_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
    ) -> ReviewResult:
        self.platform_calls.append((scene, player_id, text, account))
        try:
            if self.platform_handler is not None:
                return self.platform_handler(scene, player_id, text, account)
            return ReviewResult(verdict=REVIEW_PASS)
        finally:
            self.platform_done.set()


class RecordingKafkaProducer:
    """记录 send_and_wait 调用的假 Kafka 生产者。"""

    def __init__(self) -> None:
        self.records: list[tuple[str, bytes, bytes]] = []

    async def send_and_wait(self, topic: str, value: bytes, *, key: bytes = b"") -> None:
        self.records.append((topic, key, value))


def make_session(
    protocol: Protocol,
    player_id: int,
    *,
    account: str | None = None,
    owner_revision: int = 1,
    session_id: int = 1001,
    gate_instance_id: str = "gate-1",
    session_incarnation: int = 1,
) -> bytes:
    details = protocol.message(
        "SessionDetails",
        session_id=session_id,
        player_id=player_id,
        gate_node_id=1,
        gate_instance_id=gate_instance_id,
        session_incarnation=session_incarnation,
        owner_revision=owner_revision,
        authenticated_account=f"acc_{player_id}" if account is None else account,
    )
    return details.SerializeToString()


async def start_chat_fixture(
    *,
    rules: ChatRules | None = None,
    reviewer=None,
    profile_reader=None,
    actor_verifier=None,
    clock: FakeClock | None = None,
):
    """启动真实 gRPC ChatService 并返回 (stack, protocol, client, store, signer, producer, clock)。"""
    protocol = Protocol()
    clock = clock or FakeClock()
    signer = Ed25519PrivateKey.generate()
    verifier = CallerVerifier([signer.public_key()], allowed_callers=("gate",))
    actor_verifier = actor_verifier or FakeActorVerifier()
    store = MemoryChatStore(
        protocol,
        actor_verifier,
        rules or ChatRules(world_channel_cooldown_ms=0, world_history_cache_ms=500),
        reviewer=reviewer,
        profile_reader=profile_reader,
        clock_ms=clock.now_ms,
        monotonic_clock=clock.now_mono,
    )
    producer = RecordingKafkaProducer()
    pusher = KafkaWorldPusher(protocol, producer, generation=2)
    service = ChatService(protocol, store, verifier, world_pusher=pusher, platform_reviewer=reviewer)

    stack = AsyncExitStack()
    server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
    stack.push_async_callback(server.stop, 1.0)
    stack.push_async_callback(service.close)
    stack.push_async_callback(store.close)
    client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)
    stack.push_async_callback(client.close)
    return stack, protocol, client, store, signer, producer, clock


# ---------------------------------------------------------------------------
# 1. Gate Ed25519 鉴权、服务端内部方法隔离与 Push 未实现
# ---------------------------------------------------------------------------


@_async_test
async def test_gate_auth_and_method_boundaries():
    stack, protocol, client, _store, signer, _producer, _clock = await start_chat_fixture()
    async with stack:
        req = protocol.message(
            "chatpb.SendChatRequest",
            request_id="req-auth-1",
            message=protocol.message("chatpb.ChatMessage", channel=CHANNEL_WORLD, content="hello"),
        )
        raw_req = req.SerializeToString()

        # 缺会话头 -> PERMISSION_DENIED
        for method, payload in (
            ("SendChat", raw_req),
            ("PullChatHistory", protocol.message("chatpb.PullChatHistoryRequest", channel=CHANNEL_WORLD).SerializeToString()),
            (
                "ReportChat",
                protocol.message(
                    "chatpb.ReportChatRequest",
                    channel=CHANNEL_WORLD,
                    target_sender_player_id=10,
                    send_time_ms=1000,
                    reason=1,
                ).SerializeToString(),
            ),
        ):
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await client.call(CHAT_SVC_PREFIX + method, payload)
            assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 错误签名私钥 -> UNAUTHENTICATED
        wrong_signer = Ed25519PrivateKey.generate()
        bad_md = session_metadata(make_session(protocol, 101), CHAT_SVC_PREFIX + "SendChat", wrong_signer)
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await client.call(CHAT_SVC_PREFIX + "SendChat", raw_req, metadata=bad_md)
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # 身份不完整（player_id=0 / owner_revision=0 / account=""）-> PERMISSION_DENIED
        for bad_session in (
            make_session(protocol, 0, account="acc_0", owner_revision=1),
            make_session(protocol, 101, account="acc_101", owner_revision=0),
            make_session(protocol, 101, account="", owner_revision=1),
        ):
            md = session_metadata(bad_session, CHAT_SVC_PREFIX + "SendChat", signer)
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await client.call(CHAT_SVC_PREFIX + "SendChat", raw_req, metadata=md)
            assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # RetractWorldChat 为服务端内部方法：携带 x-session-detail-bin 一律 PERMISSION_DENIED
        retract_raw = protocol.message(
            "chatpb.RetractWorldChatRequest", sender_player_id=101, send_time_ms=1000, reason="mod"
        ).SerializeToString()
        signed_retract_md = session_metadata(
            make_session(protocol, 101), CHAT_SVC_PREFIX + "RetractWorldChat", signer
        )
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await client.call(CHAT_SVC_PREFIX + "RetractWorldChat", retract_raw, metadata=signed_retract_md)
        assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 裸带 x-session-detail-bin 同样拒绝
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await client.call(
                CHAT_SVC_PREFIX + "RetractWorldChat",
                retract_raw,
                metadata=((SESSION_DETAIL, base64.b64encode(make_session(protocol, 101))),),
            )
        assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # Push 声明（NotifyWorldChat / NotifyWorldChatRetracted）必须保持 UNIMPLEMENTED
        for push_method in ("NotifyWorldChat", "NotifyWorldChatRetracted"):
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await client.call(CHAT_SVC_PREFIX + push_method, b"")
            assert exc_info.value.code() == grpc.StatusCode.UNIMPLEMENTED

        # 合法 Gate 签名请求成功，且初始元数据回显 x-session-detail-bin
        good_session = make_session(protocol, 101)
        good_md = session_metadata(good_session, CHAT_SVC_PREFIX + "SendChat", signer)
        call = client.request(CHAT_SVC_PREFIX + "SendChat", raw_req, metadata=good_md)
        resp_bytes = await call
        resp = protocol.parse("chatpb.SendChatResponse", resp_bytes)
        assert resp.error_message.id == 0
        init_md = dict(await call.initial_metadata())
        assert init_md[SESSION_DETAIL] == base64.b64encode(good_session)


# ---------------------------------------------------------------------------
# 2. SendChat：世界/私聊、防伪造发言人、幂等重放先于拒绝、冲突、限流与冷却
# ---------------------------------------------------------------------------


@_async_test
async def test_send_world_private_spoofing_idempotency_conflict_ratelimit_cooldown():
    rules = ChatRules(rate_limit_per_second=2, world_channel_cooldown_ms=5000, world_history_cache_ms=500)
    stack, protocol, client, _store, signer, producer, clock = await start_chat_fixture(rules=rules)
    async with stack:
        async def send(player_id: int, channel: int, content: str, request_id: str, target: int = 0, spoof: int = 0):
            req = protocol.message(
                "chatpb.SendChatRequest",
                request_id=request_id,
                message=protocol.message(
                    "chatpb.ChatMessage",
                    sender_player_id=spoof,
                    target_player_id=target,
                    channel=channel,
                    content=content,
                ),
            )
            md = session_metadata(make_session(protocol, player_id), CHAT_SVC_PREFIX + "SendChat", signer)
            raw = await client.call(CHAT_SVC_PREFIX + "SendChat", req.SerializeToString(), metadata=md)
            return protocol.parse("chatpb.SendChatResponse", raw)

        async def pull(player_id: int, channel: int, peer: int = 0, limit: int = 20):
            req = protocol.message(
                "chatpb.PullChatHistoryRequest", channel=channel, peer_player_id=peer, limit=limit
            )
            md = session_metadata(make_session(protocol, player_id), CHAT_SVC_PREFIX + "PullChatHistory", signer)
            raw = await client.call(CHAT_SVC_PREFIX + "PullChatHistory", req.SerializeToString(), metadata=md)
            return protocol.parse("chatpb.PullChatHistoryResponse", raw)

        # 世界频道发送：伪造 sender_player_id=999999 必须被已验签的 101 覆盖
        r1 = await send(101, CHANNEL_WORLD, "  hello world  ", "req-w1", spoof=999999)
        assert r1.error_message.id == 0
        assert len(producer.records) == 1
        topic, key, payload = producer.records[0]
        assert topic == "chat-world-bcast_g2"
        assert key == b"101"
        bcast = protocol.parse("contracts.kafka.BroadcastToAllEvent", payload)
        assert bcast.message_content.message_id == WORLD_BROADCAST_MSG_ID
        bcast_msg = protocol.parse("chatpb.ChatMessage", bcast.message_content.serialized_message)
        assert bcast_msg.sender_player_id == 101
        assert bcast_msg.content == "hello world"
        assert bcast_msg.send_time_ms == clock.now_ms()

        # 紧接着第二条世界消息（仍在 5000ms 冷却内）-> 限流码 1008
        clock.advance_ms(100)
        r_cool = await send(101, CHANNEL_WORLD, "too soon", "req-w2")
        assert r_cool.error_message.id == ERR_RATE_LIMITED
        assert len(producer.records) == 1

        # 冷却期间重放原 request_id="req-w1"（先于拒绝判重）-> 成功且不二次广播
        r_replay = await send(101, CHANNEL_WORLD, "  hello world  ", "req-w1", spoof=999999)
        assert r_replay.error_message.id == 0
        assert len(producer.records) == 1

        # 用同一 request_id="req-w1" 改正文 -> 摘要冲突返回 1005
        r_conflict = await send(101, CHANNEL_WORLD, "tampered", "req-w1", spoof=999999)
        assert r_conflict.error_message.id == ERR_INVALID_PARAMETER

        # 同一秒内发私聊不受世界冷却约束，但受每秒 2 条总限流约束（本秒已有 1 条世界消息）
        r_p1 = await send(101, CHANNEL_PRIVATE, "hi 202", "req-p1", target=202, spoof=888)
        assert r_p1.error_message.id == 0
        assert len(producer.records) == 1  # 私聊不发世界广播

        # 本秒第 3 条 -> 每秒限流 1008
        r_rl = await send(101, CHANNEL_PRIVATE, "burst", "req-p2", target=202)
        assert r_rl.error_message.id == ERR_RATE_LIMITED

        # 限流满额状态下重放 req-p1 -> 仍成功（replay-before-rejection）
        assert (await send(101, CHANNEL_PRIVATE, "hi 202", "req-p1", target=202, spoof=888)).error_message.id == 0

        # 推进到 5100ms 后世界冷却解除且进入新秒窗口
        clock.advance_ms(5000)
        assert (await send(101, CHANNEL_WORLD, "after cooldown", "req-w3")).error_message.id == 0
        assert len(producer.records) == 2

        # 验证私聊历史双方对称可见、第三人不可见、发言人已覆盖
        hist_101 = await pull(101, CHANNEL_PRIVATE, peer=202)
        hist_202 = await pull(202, CHANNEL_PRIVATE, peer=101)
        hist_303 = await pull(303, CHANNEL_PRIVATE, peer=101)
        assert len(hist_101.messages) == 1
        assert hist_101.messages[0].sender_player_id == 101
        assert hist_101.messages[0].target_player_id == 202
        assert hist_101.messages[0].content == "hi 202"
        assert len(hist_202.messages) == 1
        assert len(hist_303.messages) == 0

        # 非法通道 / 空正文 / 超长正文 / 给自己私聊
        assert (await send(101, CHANNEL_TEAM, "team", "req-t")).error_message.id == ERR_CHANNEL_UNAVAILABLE
        assert (await send(101, CHANNEL_SYSTEM, "sys", "req-s")).error_message.id == ERR_CHANNEL_UNAVAILABLE
        assert (await send(101, CHANNEL_UNSPECIFIED, "u", "req-u")).error_message.id == ERR_CHANNEL_UNAVAILABLE
        assert (await send(101, CHANNEL_PRIVATE, "self", "req-self", target=101)).error_message.id == ERR_INVALID_PARAMETER
        assert (await send(101, CHANNEL_WORLD, "   ", "req-empty")).error_message.id == ERR_MESSAGE_TOO_LONG
        assert (await send(101, CHANNEL_WORLD, "a" * 513, "req-long")).error_message.id == ERR_MESSAGE_TOO_LONG


# ---------------------------------------------------------------------------
# 3. 8 分段世界历史归并、worldWindow 单飞缓存与闲置 TTL 截断
# ---------------------------------------------------------------------------


@_async_test
async def test_world_8shard_merge_and_idle_ttl_truncation():
    rules = ChatRules(
        world_channel_cooldown_ms=0,
        world_history_cache_ms=500,
        history_ttl_seconds=60,
        history_default_limit=20,
        history_max_limit=50,
    )
    stack, protocol, client, store, signer, _producer, clock = await start_chat_fixture(rules=rules)
    async with stack:
        # 16 名不同玩家发世界消息，覆盖全部 8 个 world-v3:0..7 分段
        shards_seen = set()
        for idx in range(1, 17):
            clock.advance_ms(1000)
            shards_seen.add(world_stream_key(idx))
            req = protocol.message(
                "chatpb.SendChatRequest",
                request_id=f"w-{idx}",
                message=protocol.message("chatpb.ChatMessage", channel=CHANNEL_WORLD, content=f"msg-{idx}"),
            )
            md = session_metadata(make_session(protocol, idx), CHAT_SVC_PREFIX + "SendChat", signer)
            resp = protocol.parse(
                "chatpb.SendChatResponse",
                await client.call(CHAT_SVC_PREFIX + "SendChat", req.SerializeToString(), metadata=md),
            )
            assert resp.error_message.id == 0
        assert len(shards_seen) == WORLD_STREAM_SHARDS

        async def pull_world(limit: int = 20):
            req = protocol.message("chatpb.PullChatHistoryRequest", channel=CHANNEL_WORLD, limit=limit)
            md = session_metadata(make_session(protocol, 1), CHAT_SVC_PREFIX + "PullChatHistory", signer)
            return protocol.parse(
                "chatpb.PullChatHistoryResponse",
                await client.call(CHAT_SVC_PREFIX + "PullChatHistory", req.SerializeToString(), metadata=md),
            )

        res = await pull_world(limit=10)
        assert res.error_message.id == 0
        assert [m.content for m in res.messages] == [f"msg-{i}" for i in range(16, 6, -1)]

        # 制造一段 65s 闲置间隔再发新消息：更早的 16 条必须被闲置 TTL 截断
        clock.advance_ms(65_000)
        req_new = protocol.message(
            "chatpb.SendChatRequest",
            request_id="w-after-gap",
            message=protocol.message("chatpb.ChatMessage", channel=CHANNEL_WORLD, content="fresh-msg"),
        )
        md_new = session_metadata(make_session(protocol, 1), CHAT_SVC_PREFIX + "SendChat", signer)
        assert (
            protocol.parse(
                "chatpb.SendChatResponse",
                await client.call(CHAT_SVC_PREFIX + "SendChat", req_new.SerializeToString(), metadata=md_new),
            ).error_message.id
            == 0
        )

        after_gap = await pull_world(limit=20)
        assert [m.content for m in after_gap.messages] == ["fresh-msg"]

        # 再推进 60s（达到 HistoryTTLSeconds）后整窗不可见
        clock.advance_ms(60_000)
        expired = await pull_world(limit=20)
        assert list(expired.messages) == []


@_async_test
async def test_world_window_single_flight_cancel_isolation_and_epoch_guard():
    clock = FakeClock()
    load_started = asyncio.Event()
    release_load = asyncio.Event()
    load_count = 0

    async def loader() -> WorldSnapshot:
        nonlocal load_count
        load_count += 1
        current = load_count
        load_started.set()
        await release_load.wait()
        row = HistoryRow(sender=current, request_id=f"r{current}".encode(), payload=b"p", accepted_ms=clock.now_ms())
        return WorldSnapshot(rows=(row,), db_now_ms=clock.now_ms())

    win = WorldWindow(0.5, loader, monotonic_clock=clock.now_mono)
    try:
        # 两个并发等待方触发单飞重读
        t1 = asyncio.create_task(win.get())
        await load_started.wait()
        t2 = asyncio.create_task(win.get())
        await asyncio.sleep(0)

        # 取消发起方 t1，不影响独立后台任务，也不影响等待方 t2
        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1

        release_load.set()
        snap = await t2
        assert load_count == 1
        assert snap.rows[0].sender == 1

        # 500ms 内再次读取命中缓存，不再触发 loader
        clock.advance_ms(200)
        snap2 = await win.get()
        assert snap2 is snap
        assert load_count == 1

        # 过期后发起新重读，但在途期间调用 invalidate()：旧结果返回给已有等待方但不回填缓存（epoch 护栏）
        clock.advance_ms(400)
        load_started.clear()
        release_load.clear()
        t3 = asyncio.create_task(win.get())
        await load_started.wait()
        win.invalidate()
        release_load.set()
        stale = await t3
        assert stale.rows[0].sender == 2

        # 紧接着下一次 get 必须发起第 3 次重读（证明旧结果未写入缓存）
        fresh = await win.get()
        assert load_count == 3
        assert fresh.rows[0].sender == 3
    finally:
        await win.close()


# ---------------------------------------------------------------------------
# 4. ReportChat、RetractWorldChat 与异步平台复检自动撤回
# ---------------------------------------------------------------------------


@_async_test
async def test_report_retract_and_async_platform_recheck():
    reviewer = FakeReviewer()
    stack, protocol, client, store, signer, producer, clock = await start_chat_fixture(reviewer=reviewer)
    async with stack:
        # 1) 正常世界消息 -> 举报 -> 内部撤回
        req = protocol.message(
            "chatpb.SendChatRequest",
            request_id="w-rep-1",
            message=protocol.message("chatpb.ChatMessage", channel=CHANNEL_WORLD, content="hello report"),
        )
        md_101 = session_metadata(make_session(protocol, 101), CHAT_SVC_PREFIX + "SendChat", signer)
        await client.call(CHAT_SVC_PREFIX + "SendChat", req.SerializeToString(), metadata=md_101)
        sent_ms = clock.now_ms()

        # 202 举报 101 的消息
        rep_req = protocol.message(
            "chatpb.ReportChatRequest",
            channel=CHANNEL_WORLD,
            target_sender_player_id=101,
            send_time_ms=sent_ms,
            reason=2,
            detail=" abusive text ",
        )
        md_202 = session_metadata(make_session(protocol, 202), CHAT_SVC_PREFIX + "ReportChat", signer)
        rep_resp = protocol.parse(
            "chatpb.ReportChatResponse",
            await client.call(CHAT_SVC_PREFIX + "ReportChat", rep_req.SerializeToString(), metadata=md_202),
        )
        assert rep_resp.error_message.id == 0
        assert (202, CHANNEL_WORLD, 101, sent_ms) in store.reports

        # 举报不存在的消息 -> 18006
        rep_missing = protocol.message(
            "chatpb.ReportChatRequest",
            channel=CHANNEL_WORLD,
            target_sender_player_id=101,
            send_time_ms=sent_ms + 999,
            reason=1,
        )
        md_202_2 = session_metadata(make_session(protocol, 202), CHAT_SVC_PREFIX + "ReportChat", signer)
        assert (
            protocol.parse(
                "chatpb.ReportChatResponse",
                await client.call(CHAT_SVC_PREFIX + "ReportChat", rep_missing.SerializeToString(), metadata=md_202_2),
            ).error_message.id
            == ERR_MESSAGE_NOT_FOUND
        )

        # 服务端调用 RetractWorldChat（不带 session 头）
        retract_req = protocol.message(
            "chatpb.RetractWorldChatRequest", sender_player_id=101, send_time_ms=sent_ms, reason=" gm_remove "
        )
        ret_resp = protocol.parse(
            "chatpb.RetractWorldChatResponse",
            await client.call(CHAT_SVC_PREFIX + "RetractWorldChat", retract_req.SerializeToString()),
        )
        assert ret_resp.error_message.id == 0
        assert ret_resp.retracted is True

        # 验证已推送撤回通知（msg_id=435）且世界历史已过滤该条
        assert len(producer.records) == 2
        retract_bcast = protocol.parse("contracts.kafka.BroadcastToAllEvent", producer.records[1][2])
        assert retract_bcast.message_content.message_id == WORLD_RETRACT_MSG_ID
        retract_notify = protocol.parse("chatpb.ChatRetractedNotify", retract_bcast.message_content.serialized_message)
        assert retract_notify.sender_player_id == 101
        assert retract_notify.send_time_ms == sent_ms
        assert retract_notify.reason == "gm_remove"

        pull_req = protocol.message("chatpb.PullChatHistoryRequest", channel=CHANNEL_WORLD)
        md_pull = session_metadata(make_session(protocol, 202), CHAT_SVC_PREFIX + "PullChatHistory", signer)
        pulled = protocol.parse(
            "chatpb.PullChatHistoryResponse",
            await client.call(CHAT_SVC_PREFIX + "PullChatHistory", pull_req.SerializeToString(), metadata=md_pull),
        )
        assert list(pulled.messages) == []

        # 二次撤回同一消息返回 retracted=False 且不重复广播
        ret_resp_2 = protocol.parse(
            "chatpb.RetractWorldChatResponse",
            await client.call(CHAT_SVC_PREFIX + "RetractWorldChat", retract_req.SerializeToString()),
        )
        assert ret_resp_2.error_message.id == 0
        assert ret_resp_2.retracted is False
        assert len(producer.records) == 2

        # 撤回不存在消息 -> 18006
        ret_missing = protocol.message(
            "chatpb.RetractWorldChatRequest", sender_player_id=101, send_time_ms=sent_ms + 777, reason="x"
        )
        assert (
            protocol.parse(
                "chatpb.RetractWorldChatResponse",
                await client.call(CHAT_SVC_PREFIX + "RetractWorldChat", ret_missing.SerializeToString()),
            ).error_message.id
            == ERR_MESSAGE_NOT_FOUND
        )

        # 2) 世界频道 pending_platform_review=True -> 后台 CheckPlatformText 返回 REJECT -> 自动撤回并广播 435
        clock.advance_ms(1000)
        reviewer.sync_handler = lambda *_: ReviewResult(verdict=REVIEW_PASS, pending_platform_review=True)
        reviewer.platform_handler = lambda *_: ReviewResult(verdict=REVIEW_REJECT)
        req_async = protocol.message(
            "chatpb.SendChatRequest",
            request_id="w-async-1",
            message=protocol.message("chatpb.ChatMessage", channel=CHANNEL_WORLD, content="async bad text"),
        )
        md_async = session_metadata(make_session(protocol, 102), CHAT_SVC_PREFIX + "SendChat", signer)
        resp_async = protocol.parse(
            "chatpb.SendChatResponse",
            await client.call(CHAT_SVC_PREFIX + "SendChat", req_async.SerializeToString(), metadata=md_async),
        )
        assert resp_async.error_message.id == 0
        await asyncio.wait_for(reviewer.platform_done.wait(), timeout=2.0)
        # 等待后台任务完成撤回与广播
        await asyncio.sleep(0.05)

        assert (102, clock.now_ms()) in store.retracted
        last_bcast = protocol.parse("contracts.kafka.BroadcastToAllEvent", producer.records[-1][2])
        assert last_bcast.message_content.message_id == WORLD_RETRACT_MSG_ID
        last_notify = protocol.parse("chatpb.ChatRetractedNotify", last_bcast.message_content.serialized_message)
        assert last_notify.sender_player_id == 102
        assert last_notify.reason == "platform_async_reject"


# ---------------------------------------------------------------------------
# 5. ContentGuard 审查（打码/拒绝/广告引流拒绝/不可用/直通）、防刷屏与新号私聊限额
# ---------------------------------------------------------------------------


@_async_test
async def test_content_guard_verdicts_spam_level_and_new_account_limits():
    reviewer = FakeReviewer()
    profiles = FakeProfileReader({
        10: PlayerProfile(level=5, account_created_ms=1_700_000_000_000),  # 低级新号
        20: PlayerProfile(level=30, account_created_ms=1_600_000_000_000),  # 高级老号
        21: PlayerProfile(level=30, account_created_ms=1_600_000_000_000),
    })
    rules = ChatRules(
        world_channel_cooldown_ms=0,
        world_min_level=10,
        duplicate_spam_window_ms=60_000,
        duplicate_spam_max_senders=1,
        duplicate_spam_max_per_sender=2,
        new_account_age_hours=24,
        new_account_daily_private_limit=1,
    )
    stack, protocol, client, _store, signer, _producer, clock = await start_chat_fixture(
        rules=rules, reviewer=reviewer, profile_reader=profiles
    )
    async with stack:
        async def send(player_id: int, channel: int, content: str, request_id: str, target: int = 0):
            req = protocol.message(
                "chatpb.SendChatRequest",
                request_id=request_id,
                message=protocol.message("chatpb.ChatMessage", channel=channel, target_player_id=target, content=content),
            )
            md = session_metadata(make_session(protocol, player_id), CHAT_SVC_PREFIX + "SendChat", signer)
            return protocol.parse(
                "chatpb.SendChatResponse",
                await client.call(CHAT_SVC_PREFIX + "SendChat", req.SerializeToString(), metadata=md),
            )

        # 等级不足 -> 18002
        assert (await send(10, CHANNEL_WORLD, "low level", "lvl-1")).error_message.id == ERR_LEVEL_TOO_LOW

        # 打码通过 -> 历史中存储打码后文本
        reviewer.sync_handler = lambda *_: ReviewResult(verdict=REVIEW_MASKED, masked="hello ***")
        assert (await send(20, CHANNEL_WORLD, "hello bad", "cg-mask")).error_message.id == 0
        md_pull = session_metadata(make_session(protocol, 20), CHAT_SVC_PREFIX + "PullChatHistory", signer)
        hist = protocol.parse(
            "chatpb.PullChatHistoryResponse",
            await client.call(
                CHAT_SVC_PREFIX + "PullChatHistory",
                protocol.message("chatpb.PullChatHistoryRequest", channel=CHANNEL_WORLD).SerializeToString(),
                metadata=md_pull,
            ),
        )
        assert hist.messages[0].content == "hello ***"

        # 普通违规拒绝 -> 18000；广告引流拒绝 -> 18005；服务不可用 -> 18001
        reviewer.sync_handler = lambda *_: ReviewResult(verdict=REVIEW_REJECT, category=1)
        assert (await send(20, CHANNEL_WORLD, "bad politics", "cg-rej")).error_message.id == ERR_CONTENT_REJECTED
        reviewer.sync_handler = lambda *_: ReviewResult(verdict=REVIEW_REJECT, category=TEXT_CATEGORY_AD)
        assert (await send(20, CHANNEL_WORLD, "add qq 12345", "cg-ad")).error_message.id == ERR_CONTACT_REJECTED
        reviewer.sync_handler = lambda *_: ReviewResult(verdict=REVIEW_UNAVAILABLE)
        assert (
            await send(20, CHANNEL_WORLD, "cannot check", "cg-unavail")
        ).error_message.id == ERR_CONTENT_CHECK_UNAVAILABLE

        # 之前已成功的 cg-mask 在内容安全不可用时重放仍成功（replay-before-rejection）
        assert (await send(20, CHANNEL_WORLD, "hello bad", "cg-mask")).error_message.id == 0

        # 恢复正常放行，验证跨账号重复刷屏（全角/标点归一化一致）
        reviewer.sync_handler = None
        assert (await send(20, CHANNEL_WORLD, "Buy Gold Now!", "spam-1")).error_message.id == 0
        assert (await send(21, CHANNEL_WORLD, "Ｂｕｙ，ｇｏｌｄ　ｎｏｗ！", "spam-2")).error_message.id == ERR_DUPLICATE_SPAM
        assert normalize_spam_hash("Buy Gold Now!") == normalize_spam_hash("Ｂｕｙ，ｇｏｌｄ　ｎｏｗ！")
        assert normalize_spam_hash("hi!") is None  # 少于 4 字符不触发刷屏检测

        # 新号每日私聊不同目标上限（1 人）：给 20 发多次允许，再给 21 发拒绝 18004
        assert (await send(10, CHANNEL_PRIVATE, "hey 20", "np-1", target=20)).error_message.id == 0
        assert (await send(10, CHANNEL_PRIVATE, "again 20", "np-2", target=20)).error_message.id == 0
        assert (await send(10, CHANNEL_PRIVATE, "hey 21", "np-3", target=21)).error_message.id == ERR_PRIVATE_TARGET_LIMIT


@_async_test
async def test_grpc_actor_verifier_and_grpc_content_reviewer():
    protocol = Protocol()
    clock = FakeClock()

    # 验证 GrpcActorVerifier 使用 caller="chat" + HMAC 签名与 TTL 缓存
    ds_calls: list[int] = []
    frozen = False

    async def fake_ds_call(path: str, data: bytes, *, timeout=None) -> bytes:
        assert path == "/data_service.DataService/GetRoleFenceState"
        req = protocol.parse("data_service.GetRoleFenceStateRequest", data)
        assert verify_service_call(path, req, "chat", clock.now_ms(), ROLE_FENCE_SECRET)
        ds_calls.append(int(req.player_id))
        return protocol.message(
            "data_service.GetRoleFenceStateResponse",
            found=True,
            state=protocol.message(
                "rolefence.RoleFenceState",
                player_id=req.player_id,
                account="acc_7",
                owner_revision=3,
                freeze_token=1 if frozen else 0,
                freeze_final=frozen,
                version=1,
                home_zone=1,
            ),
        ).SerializeToString()

    verifier = GrpcActorVerifier(
        protocol,
        fake_ds_call,
        ROLE_FENCE_SECRET,
        ttl_sec=30.0,
        clock_ms=clock.now_ms,
        monotonic_clock=clock.now_mono,
    )
    ident = ActorIdentity(player_id=7, account="acc_7", owner_revision=3)
    await verifier.verify_actor(ident)
    await verifier.verify_actor(ident)  # 命中 30s TTL 缓存
    assert len(ds_calls) == 1

    # 推进 31s 缓存过期，且角色被最终冻结 -> OriginalRoleError
    clock.advance_ms(31_000)
    frozen = True
    with pytest.raises(OriginalRoleError):
        await verifier.verify_actor(ident)
    assert len(ds_calls) == 2

    # 验证 GrpcContentReviewer 严格模式与 optional 直通模式
    async def fake_cg_call(path: str, data: bytes, *, timeout=None) -> bytes:
        req = protocol.parse("content_guard.CheckTextRequest", data)
        if req.text == "boom":
            raise RuntimeError("cg down")
        if req.text == "mask":
            return protocol.message(
                "content_guard.CheckTextResponse", verdict=TEXT_VERDICT_MASKED, masked_text="****"
            ).SerializeToString()
        if req.text == "ad":
            return protocol.message(
                "content_guard.CheckTextResponse", verdict=TEXT_VERDICT_REJECT, category=TEXT_CATEGORY_AD
            ).SerializeToString()
        return protocol.message(
            "content_guard.CheckTextResponse", verdict=TEXT_VERDICT_PASS, pending_platform_review=req.async_platform
        ).SerializeToString()

    strict_cg = GrpcContentReviewer(protocol, fake_cg_call, optional=False)
    opt_cg = GrpcContentReviewer(protocol, fake_cg_call, optional=True)
    assert (await strict_cg.check_text(TEXT_SCENE_CHAT_WORLD, 7, "ok", async_platform=True)).pending_platform_review
    assert (await strict_cg.check_text(TEXT_SCENE_CHAT_PRIVATE, 7, "mask")).masked == "****"
    assert (await strict_cg.check_text(TEXT_SCENE_CHAT_PRIVATE, 7, "ad")).category == TEXT_CATEGORY_AD
    assert (await strict_cg.check_text(TEXT_SCENE_CHAT_PRIVATE, 7, "boom")).verdict == REVIEW_UNAVAILABLE
    assert (await opt_cg.check_text(TEXT_SCENE_CHAT_PRIVATE, 7, "boom")).verdict == REVIEW_PASS
    assert (await PassthroughContentReviewer().check_text(TEXT_SCENE_CHAT_WORLD, 7, "any")).verdict == REVIEW_PASS


# ---------------------------------------------------------------------------
# 6. SqlChatStore：policy_hash 对齐、UNHEX(%s) 二进制绑定与 DDL 校验
# ---------------------------------------------------------------------------


class _FakeSqlCursor:
    def __init__(self, pool: "_FakeSqlPool") -> None:
        self._pool = pool
        self.rowcount = 1
        self._rows: list[tuple] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def execute(self, sql: str, params=None) -> None:
        self._pool.executed.append((sql, params))
        self.rowcount = 1
        self._rows = []
        upper = sql.strip().upper()
        if upper.startswith("SELECT POLICY_HASH FROM CHAT_POLICY"):
            if self._pool.policy_hash is not None:
                self._rows = [(self._pool.policy_hash,)]
        elif upper.startswith("INSERT INTO CHAT_POLICY"):
            if self._pool.policy_hash is None and params:
                self._pool.policy_hash = bytes.fromhex(params[0])
        elif upper.startswith("SELECT CAST(UNIX_TIMESTAMP"):
            self._rows = [(self._pool.db_now_ms,)]
        elif upper.startswith("SELECT COUNT(*) FROM CHAT_SEND_AUTHORITY"):
            self._rows = [(0,)]
        elif upper.startswith("SELECT 1 FROM CHAT_SEND_AUTHORITY"):
            if self._pool.message_exists_flag:
                self._rows = [(1,)]
        elif upper.startswith("SELECT PLAYER_ID,REQUEST_ID,MESSAGE_PAYLOAD,ACCEPTED_MS"):
            self._rows = []

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return list(self._rows)


class _FakeSqlConn:
    def __init__(self, pool: "_FakeSqlPool") -> None:
        self._pool = pool

    async def begin(self) -> None:
        pass

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass

    def cursor(self) -> _FakeSqlCursor:
        return _FakeSqlCursor(self._pool)


class _FakeAcquire:
    def __init__(self, pool: "_FakeSqlPool") -> None:
        self._pool = pool

    async def __aenter__(self) -> _FakeSqlConn:
        return _FakeSqlConn(self._pool)

    async def __aexit__(self, *_):
        return False


class _FakeSqlPool:
    def __init__(self, *, policy_digest: bytes | None = None) -> None:
        self.policy_hash = policy_digest
        self.db_now_ms = 1_700_000_010_000
        self.message_exists_flag = False
        self.executed: list[tuple[str, tuple | None]] = []

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self)


@_async_test
async def test_sql_store_policy_hash_unhex_bindings_and_schema_sql():
    # 1) 验证 policy_bytes 与 Go authority/store.go policyBytes 逐字节一致
    rules = ChatRules(
        max_content_bytes=512,
        rate_limit_per_second=5,
        history_max_entries=200,
        history_ttl_seconds=604800,
        history_default_limit=20,
        history_max_limit=50,
        world_channel_cooldown_ms=5000,
        world_history_cache_ms=0,
    )
    expected_bytes = b"chat-authority-v2" + struct.pack(
        ">9Q", 512, 5, 200, 604800, 20, 50, WORLD_STREAM_SHARDS, ORIGINAL_RETENTION_DAYS, 0
    )
    assert policy_bytes(rules) == expected_bytes
    assert policy_hash(rules) == hashlib.sha256(expected_bytes).digest()

    # 2) ensure_policy 首次写入 UNHEX(%s)、重启一致通过、规则漂移抛 PolicyMismatchError
    protocol = Protocol()
    pool = _FakeSqlPool()
    sql_store = SqlChatStore(protocol=protocol, pool=pool, actor_verifier=FakeActorVerifier(), rules=rules)
    await sql_store.ensure_policy()
    assert pool.policy_hash == policy_hash(rules)
    insert_policy_calls = [item for item in pool.executed if "INSERT INTO chat_policy" in item[0]]
    assert len(insert_policy_calls) == 1
    assert "UNHEX(%s)" in insert_policy_calls[0][0]
    assert insert_policy_calls[0][1] == (policy_hash(rules).hex(),)

    # 再次核对一致通过
    await sql_store.ensure_policy()

    # 规则漂移 -> PolicyMismatchError
    drifted = SqlChatStore(
        protocol=protocol,
        pool=pool,
        actor_verifier=FakeActorVerifier(),
        rules=ChatRules(max_content_bytes=256),
    )
    with pytest.raises(PolicyMismatchError):
        await drifted.ensure_policy()

    # 3) 验证 Send / Retract / Report 全部二进制列均以 UNHEX(%s) + hex str 绑定，无裸 bytes
    pool.executed.clear()
    ident = ActorIdentity(player_id=42, account="acc_42", owner_revision=1)
    send_req = protocol.message(
        "chatpb.SendChatRequest",
        request_id="sql-req-1",
        message=protocol.message("chatpb.ChatMessage", channel=CHANNEL_WORLD, content="hello sql"),
    )
    res = await sql_store.send(ident, send_req)
    assert res.tip == 0 and res.message is not None

    insert_send = [item for item in pool.executed if "INSERT INTO chat_send_authority" in item[0]]
    assert len(insert_send) == 1
    sql_text, params = insert_send[0]
    assert sql_text.count("UNHEX(%s)") == 6
    assert all(not isinstance(p, (bytes, bytearray, memoryview)) for p in params)
    assert bytes.fromhex(params[1]) == b"sql-req-1"
    assert bytes.fromhex(params[2]) == b"acc_42"
    assert bytes.fromhex(params[7]) == world_stream_key(42).encode("utf-8")

    pool.message_exists_flag = True
    retracted, tip = await sql_store.retract_world(42, pool.db_now_ms, "mod_reason")
    assert retracted is True and tip == 0
    insert_retract = [item for item in pool.executed if "INSERT IGNORE INTO chat_world_retraction" in item[0]]
    assert len(insert_retract) == 1
    assert "UNHEX(%s)" in insert_retract[0][0]
    assert bytes.fromhex(insert_retract[0][1][3]) == b"mod_reason"

    rep_req = protocol.message(
        "chatpb.ReportChatRequest",
        channel=CHANNEL_WORLD,
        target_sender_player_id=42,
        send_time_ms=pool.db_now_ms,
        reason=1,
        detail="spam link",
    )
    assert await sql_store.report_message(ActorIdentity(99, "acc_99", 1), rep_req) == 0
    insert_rep = [item for item in pool.executed if "INSERT IGNORE INTO chat_report" in item[0]]
    assert len(insert_rep) == 1
    assert "UNHEX(%s)" in insert_rep[0][0]
    assert bytes.fromhex(insert_rep[0][1][5]) == b"spam link"

    # 4) 验证 config/chat_schema.sql 包含全部 6 张表且 TTL 属性可识别
    schema_text = SCHEMA_FILE.read_text("utf-8")
    for table in (
        "chat_send_authority",
        "chat_policy",
        "chat_world_retraction",
        "chat_report",
        "chat_repeat_event",
        "chat_private_daily_target",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in schema_text
    assert has_retention_attributes(schema_text)
    assert stream_key(20, 10) == "private-v2:10:20"
    assert CHAT_NODE_TYPE == 9
    assert DEFAULT_GRPC_PORT == 50700
    assert DEFAULT_LEASE_TTL_SEC == 60


def test_chat_config_fail_closed_validation():
    base_cfg = ChatConfig(
        advertise_host="127.0.0.1",
        data_service="127.0.0.1:9000",
        content_guard="127.0.0.1:50900",
        role_fence_secret=ROLE_FENCE_SECRET,
    )
    base_cfg.validate(settings=Settings(etcd="127.0.0.1:2379"))

    # 缺 content_guard 且非 optional 必须拒启
    with pytest.raises(ValueError, match="content_guard"):
        ChatConfig(
            advertise_host="127.0.0.1",
            data_service="127.0.0.1:9000",
            content_guard="",
            content_guard_optional=False,
            role_fence_secret=ROLE_FENCE_SECRET,
        ).validate()

    # 显式 content_guard_optional=True 放行本地开发模式
    ChatConfig(
        advertise_host="127.0.0.1",
        data_service="127.0.0.1:9000",
        content_guard="",
        content_guard_optional=True,
        role_fence_secret=ROLE_FENCE_SECRET,
    ).validate()

    # 密钥不足 32 字节拒绝
    with pytest.raises(ValueError, match=ROLE_FENCE_SECRET_ENV):
        ChatConfig(
            advertise_host="127.0.0.1",
            data_service="127.0.0.1:9000",
            content_guard="127.0.0.1:50900",
            role_fence_secret=b"short",
        ).validate()

    # request_budget_sec > 4.0 拒绝
    with pytest.raises(ValueError, match="request_budget_sec"):
        ChatConfig(
            advertise_host="127.0.0.1",
            data_service="127.0.0.1:9000",
            content_guard="127.0.0.1:50900",
            request_budget_sec=5.0,
            role_fence_secret=ROLE_FENCE_SECRET,
        ).validate()


@_async_test
async def test_kafka_best_effort_failure_and_actor_rejection_and_limit_clamp():
    class FailingProducer:
        async def send_and_wait(self, topic: str, value: bytes, *, key: bytes = b"") -> None:
            raise RuntimeError("kafka broker unreachable")

    protocol = Protocol()
    clock = FakeClock()
    signer = Ed25519PrivateKey.generate()
    verifier = CallerVerifier([signer.public_key()], allowed_callers=("gate",))
    actor_verifier = FakeActorVerifier({101: ("acc_101", 1)})
    rules = ChatRules(
        world_channel_cooldown_ms=0,
        world_history_cache_ms=0,
        history_default_limit=2,
        history_max_limit=3,
    )
    store = MemoryChatStore(
        protocol,
        actor_verifier,
        rules,
        clock_ms=clock.now_ms,
        monotonic_clock=clock.now_mono,
    )
    pusher = KafkaWorldPusher(protocol, FailingProducer(), generation=1)
    service = ChatService(protocol, store, verifier, world_pusher=pusher)

    async with AsyncExitStack() as stack:
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        stack.push_async_callback(server.stop, 1.0)
        stack.push_async_callback(service.close)
        stack.push_async_callback(store.close)
        client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)
        stack.push_async_callback(client.close)

        # Kafka 推送异常不影响已提交的 SendChat，连续写入 4 条
        for idx in range(1, 5):
            clock.advance_ms(1000)
            req = protocol.message(
                "chatpb.SendChatRequest",
                request_id=f"kb-{idx}",
                message=protocol.message("chatpb.ChatMessage", channel=CHANNEL_WORLD, content=f"ok-{idx}"),
            )
            md = session_metadata(make_session(protocol, 101), CHAT_SVC_PREFIX + "SendChat", signer)
            resp = protocol.parse(
                "chatpb.SendChatResponse",
                await client.call(CHAT_SVC_PREFIX + "SendChat", req.SerializeToString(), metadata=md),
            )
            assert resp.error_message.id == 0

        # limit=0 使用 history_default_limit=2；limit=99 截断为 history_max_limit=3
        md_def = session_metadata(make_session(protocol, 101), CHAT_SVC_PREFIX + "PullChatHistory", signer)
        r_def = protocol.parse(
            "chatpb.PullChatHistoryResponse",
            await client.call(
                CHAT_SVC_PREFIX + "PullChatHistory",
                protocol.message("chatpb.PullChatHistoryRequest", channel=CHANNEL_WORLD, limit=0).SerializeToString(),
                metadata=md_def,
            ),
        )
        assert [m.content for m in r_def.messages] == ["ok-4", "ok-3"]

        md_max = session_metadata(make_session(protocol, 101), CHAT_SVC_PREFIX + "PullChatHistory", signer)
        r_max = protocol.parse(
            "chatpb.PullChatHistoryResponse",
            await client.call(
                CHAT_SVC_PREFIX + "PullChatHistory",
                protocol.message("chatpb.PullChatHistoryRequest", channel=CHANNEL_WORLD, limit=99).SerializeToString(),
                metadata=md_max,
            ),
        )
        assert [m.content for m in r_max.messages] == ["ok-4", "ok-3", "ok-2"]

        # Kafka 异常不影响 RetractWorldChat 提交
        ret_resp = protocol.parse(
            "chatpb.RetractWorldChatResponse",
            await client.call(
                CHAT_SVC_PREFIX + "RetractWorldChat",
                protocol.message(
                    "chatpb.RetractWorldChatRequest", sender_player_id=101, send_time_ms=clock.now_ms(), reason="kfail"
                ).SerializeToString(),
            ),
        )
        assert ret_resp.error_message.id == 0
        assert ret_resp.retracted is True

        # 角色过户（owner_revision 变化）-> in-band Tip 1005
        actor_verifier.allowed[101] = ("acc_new_owner", 2)
        md_stale = session_metadata(make_session(protocol, 101, owner_revision=1), CHAT_SVC_PREFIX + "SendChat", signer)
        stale_resp = protocol.parse(
            "chatpb.SendChatResponse",
            await client.call(
                CHAT_SVC_PREFIX + "SendChat",
                protocol.message(
                    "chatpb.SendChatRequest",
                    request_id="stale-1",
                    message=protocol.message("chatpb.ChatMessage", channel=CHANNEL_WORLD, content="stale"),
                ).SerializeToString(),
                metadata=md_stale,
            ),
        )
        assert stale_resp.error_message.id == ERR_INVALID_PARAMETER


@_async_test
async def test_sql_store_bootstrap_and_verify_schema():
    from xuanming.chat.sql_store import _EXPECTED_COLUMNS, _EXPECTED_INDEXES

    class _SchemaCursor:
        def __init__(self, *, missing_index: bool = False, nullable_count: int = 0) -> None:
            self._missing_index = missing_index
            self._nullable_count = nullable_count
            self._rows: list[tuple] = []
            self.ddls: list[str] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        async def execute(self, sql: str, params=None) -> None:
            self._rows = []
            if sql.upper().startswith("CREATE TABLE"):
                self.ddls.append(sql)
            elif "FROM information_schema.statistics" in sql:
                for key, cols_csv in _EXPECTED_INDEXES.items():
                    if self._missing_index and key == "chat_send_authority/idx_chat_rate":
                        continue
                    table, name = key.split("/")
                    non_unique = 0 if name == "PRIMARY" else 1
                    for col in cols_csv.split(","):
                        self._rows.append((table, name, col, non_unique, None))
            elif "is_nullable<>'NO'" in sql:
                self._rows = [(self._nullable_count,)]
            elif "FROM information_schema.columns" in sql:
                for key, shape in _EXPECTED_COLUMNS.items():
                    table, col = key.split("/")
                    if "/" in shape:
                        base_kind, max_len_str = shape.split("/")
                        self._rows.append((table, col, base_kind, f"{base_kind}({max_len_str})", int(max_len_str)))
                    elif shape.startswith("datetime"):
                        self._rows.append((table, col, "datetime", shape, None))
                    elif shape.endswith(" unsigned"):
                        base_kind = shape.removesuffix(" unsigned")
                        self._rows.append((table, col, base_kind, f"{base_kind} unsigned", None))
                    else:
                        self._rows.append((table, col, shape, shape, None))

        async def fetchone(self):
            return self._rows[0] if self._rows else None

        async def fetchall(self):
            return list(self._rows)

    class _SchemaPool:
        def __init__(self, cursor: _SchemaCursor) -> None:
            self._cursor = cursor

        def acquire(self):
            pool = self

            class _Acq:
                async def __aenter__(self):
                    class _Conn:
                        def close(self):
                            pass

                        def cursor(self):
                            return pool._cursor

                    return _Conn()

                async def __aexit__(self, *_):
                    return False

            return _Acq()

    protocol = Protocol()
    cur_ok = _SchemaCursor()
    store_ok = SqlChatStore(_SchemaPool(cur_ok), protocol, FakeActorVerifier(), ChatRules(world_history_cache_ms=0))
    await store_ok.bootstrap()
    assert len(cur_ok.ddls) == 6
    await store_ok.verify_schema()

    store_bad_idx = SqlChatStore(
        _SchemaPool(_SchemaCursor(missing_index=True)), protocol, FakeActorVerifier(), ChatRules(world_history_cache_ms=0)
    )
    with pytest.raises(RuntimeError, match="index mismatch"):
        await store_bad_idx.verify_schema()

    store_nullable = SqlChatStore(
        _SchemaPool(_SchemaCursor(nullable_count=1)), protocol, FakeActorVerifier(), ChatRules(world_history_cache_ms=0)
    )
    with pytest.raises(RuntimeError, match="NOT NULL"):
        await store_nullable.verify_schema()

