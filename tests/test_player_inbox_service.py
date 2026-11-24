"""player_inbox 端到端 gRPC 服务、客户端（InboxPeer）、鉴权、领域不变量与对账测试。"""

from __future__ import annotations

import asyncio
import time
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest

from xuanming.grpc_runtime import (
    GrpcClient,
    serve_grpc,
    service_digest,
    sign_service_call,
    verify_service_call,
)
from xuanming.nodes.player_inbox import APPLIED, BATTLE, GM, InboxPeer, REJECTED, SKIPPED, UNKNOWN
from xuanming.player_inbox import (
    InboxRegistry,
    KafkaInboxNotifier,
    MemoryInboxStore,
    NotifyResult,
    PlayerInboxConfig,
    PlayerInboxService,
    SqlInboxStore,
)
from xuanming.player_inbox.config import (
    COORDINATOR_SECRET_ENV,
    DEFAULT_ADMIN_TOKEN_ENV,
    MATCH_SECRET_ENV,
    ROLE_FENCE_SECRET_ENV,
    SCENE_SECRET_ENV,
)
from xuanming.player_inbox.errors import (
    ERR_DISABLED,
    ERR_EPOCH_REQUIRED,
    ERR_FULL,
    ERR_IDEMPOTENCY_CONFLICT,
    ERR_INVALID_PARAMETER,
    ERR_ISSUED_OUT_OF_WINDOW,
    ERR_ITEM_NOT_FOUND,
    ERR_ITEM_STATE_CONFLICT,
    ERR_PLAYER_NOT_FOUND,
    ERR_STALE_OWNER,
    ERR_STORE_UNAVAILABLE,
)
from xuanming.player_inbox.model import (
    ACK_ALREADY_SAME,
    ACK_CONFLICT,
    ACK_FINALIZED,
    ACK_MISMATCH,
    ACK_NOT_DELIVERED,
    ACK_SKIP_NOT_REQUESTED,
    ACK_UNKNOWN_SEQ,
    AUDIT_FORCE_VOID,
    AUDIT_SANCTION,
    AUDIT_VOID,
    DIRECTIVE_VOID,
    DOMAIN_BATTLE_SETTLEMENT,
    DOMAIN_GM_SANCTION,
    FENCE_ADVANCE,
    FENCE_EPOCH_REQUIRED,
    FENCE_FIRST,
    FENCE_MATCH,
    FENCE_NO_STREAM,
    FENCE_STALE,
    FINALIZER_ADMIN,
    FORCE_VOID_LEDGER_ABSENT,
    FORCE_VOID_LEDGER_INVALID,
    FORCE_VOID_NOT_DECIDED,
    FORCE_VOID_NOT_DELIVERED,
    FORCE_VOID_PROVEN,
    FORCE_VOID_REQUEST_IN_LEDGER,
    FORCE_VOID_SEQ_NOT_IN_RING,
    ISSUED_AT_MAX_FUTURE_MS,
    ISSUED_AT_MAX_PAST_MS,
    MSG_BATTLE_SETTLEMENT,
    MSG_GM_BLOCK_CURRENCY_GAIN,
    MSG_GM_BLOCK_ITEM_GAIN,
    MSG_GM_UNBLOCK_CURRENCY_GAIN,
    MSG_GM_UNBLOCK_ITEM_GAIN,
    PLAYER_INBOX_NOTIFY_EVENT_ID,
    PRODUCER_GM,
    PRODUCER_MATCH,
    RECONCILE_FINAL_NOT_IN_LEDGER,
    RECONCILE_PENDING_BUT_DECIDED,
    ROLE_BLOCKER_ITEM,
    STATE_APPLIED,
    STATE_PENDING,
    STATE_REJECTED,
    STATE_SETTLED_UNKNOWN,
    STATE_VOIDED,
    VOID_CANCELLED,
    VOID_MODE_FORCE_MISMATCH,
    VOID_QUARANTINED,
    content_sha256,
    mix64,
    next_seq,
    row_shard,
)
from xuanming.player_inbox.store import GrpcDataServiceDirectory
from xuanming.protocol import Protocol
from xuanming.settings import Settings

SCENE_SECRET = b"scene-directory-secret-0123456789"
MATCH_SECRET = b"match-player-inbox-secret-0123456"
COORDINATOR_SECRET = b"role-fence-coordinator-secret-012"
ROLE_FENCE_SECRET = b"role-fence-player-inbox-secret-01"
ADMIN_TOKEN = "super-secret-admin-token-0123456"

INBOX_SVC = "/player_inbox.PlayerInbox/"
ADMIN_SVC = "/player_inbox.PlayerInboxAdmin/"
FENCE_SVC = "/rolefence.RoleFenceParticipant/"
SCENE_UUID = "11111111-2222-3333-4444-555555555555"

ALL_ALLOWED = frozenset((
    MSG_GM_BLOCK_CURRENCY_GAIN,
    MSG_GM_UNBLOCK_CURRENCY_GAIN,
    MSG_GM_BLOCK_ITEM_GAIN,
    MSG_GM_UNBLOCK_ITEM_GAIN,
    MSG_BATTLE_SETTLEMENT,
))


class FakeDirectory:
    """可编程的 data_service 外部目录假实现。"""

    def __init__(self, protocol: Protocol) -> None:
        self.protocol = protocol
        self.players: set[int] = set()
        self.fences: dict[int, object] = {}
        self.valid_admissions: set[tuple[int, int, int]] = set()
        self.ledgers: dict[int, object] = {}
        self.item_blocks: dict[int, list[int]] = {}
        self.fail_ledger = False

    def seed_player(
        self,
        player_id: int,
        account: str = "acct_a",
        *,
        owner_revision: int = 1,
        freeze_token: int = 0,
        version: int = 1,
        home_zone: int = 1,
    ) -> None:
        self.players.add(player_id)
        self.fences[player_id] = self.protocol.message(
            "rolefence.RoleFenceState",
            player_id=player_id,
            account=account,
            owner_revision=owner_revision,
            freeze_token=freeze_token,
            version=version,
            home_zone=home_zone,
        )

    def admit(self, player_id: int, owner_revision: int, owner_epoch: int):
        self.valid_admissions.add((player_id, owner_revision, owner_epoch))
        return self.protocol.message(
            "common.base.RoleAdmissionIdentity",
            player_id=player_id,
            scene_instance_id=SCENE_UUID,
            generation=1,
            owner_revision=owner_revision,
            owner_epoch=owner_epoch,
            scene_node_id="2",
            scene_id=100,
            zone_id=1,
            session_id=7,
            login_generation=1,
        )

    async def player_exists(self, player_id: int) -> bool:
        return player_id in self.players

    async def get_role_fence_state(self, player_id: int):
        return self.fences.get(player_id)

    async def verify_role_admission(self, admission) -> bool:
        return (int(admission.player_id), int(admission.owner_revision), int(admission.owner_epoch)) in self.valid_admissions

    async def read_inbox_ledger(self, player_id: int):
        if self.fail_ledger:
            raise RuntimeError("ledger rpc failed")
        if player_id not in self.ledgers:
            return None, False
        return self.ledgers[player_id], True

    async def read_item_gain_block(self, player_id: int):
        if player_id not in self.item_blocks:
            return None, False
        return self.protocol.message("PlayerItemGainBlockComp", item_config_ids=self.item_blocks[player_id]), True


class RecordingNotifier:
    """记录提交后提醒的可注入 Notifier。"""

    def __init__(self, result: NotifyResult = NotifyResult.SENT) -> None:
        self.result = result
        self.events: list[tuple[int, int]] = []

    async def notify(self, player_id: int, domain: int) -> NotifyResult:
        self.events.append((player_id, domain))
        return self.result


class MutableClock:
    def __init__(self, now_ms: int | None = None) -> None:
        self.now_ms = (time.time_ns() // 1_000_000) if now_ms is None else now_ms

    def __call__(self) -> int:
        return self.now_ms


@asynccontextmanager
async def running_inbox(
    *,
    allowed_types: frozenset[int] = ALL_ALLOWED,
    match_secret: bytes = MATCH_SECRET,
    admin_token: str = ADMIN_TOKEN,
    clock: MutableClock | None = None,
):
    protocol = Protocol()
    clock = clock or MutableClock()
    directory = FakeDirectory(protocol)
    notifier = RecordingNotifier()
    registry = InboxRegistry(protocol)
    store = MemoryInboxStore(
        protocol,
        registry,
        directory,
        allowed_types=allowed_types,
        notifier=notifier,
        clock_ms=clock,
    )
    service = PlayerInboxService(
        protocol,
        store,
        scene_secret=SCENE_SECRET,
        coordinator_secret=COORDINATOR_SECRET,
        match_secret=match_secret,
        admin_token=admin_token,
        clock_ms=clock,
    )
    async with AsyncExitStack() as stack:
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        stack.push_async_callback(server.stop, 0)
        client = GrpcClient(f"127.0.0.1:{port}")
        stack.push_async_callback(client.close)
        peer = InboxPeer(protocol, client.call, node_id=2, secret=SCENE_SECRET)

        def make_player(player_id: int, owner_revision: int = 1, owner_epoch: int = 1):
            identity = directory.admit(player_id, owner_revision, owner_epoch)
            return SimpleNamespace(
                player_id=player_id,
                role_identity=identity,
                snapshot=SimpleNamespace(owner_epoch=owner_epoch),
            )

        async def admin_call(method: str, req, resp_type: str, *, token: str | None = ADMIN_TOKEN):
            md = (("x-admin-token", token),) if token is not None else ()
            raw = await client.call(ADMIN_SVC + method, req.SerializeToString(deterministic=True), metadata=md)
            return protocol.parse(resp_type, raw)

        async def match_call(method: str, req, *, secret: bytes = MATCH_SECRET, metadata: tuple = ()):
            path = INBOX_SVC + method
            sign_service_call(path, req, "match", clock(), secret)
            raw = await client.call(path, req.SerializeToString(deterministic=True), metadata=metadata)
            return protocol.parse("player_inbox.BattleSettlementInboxResponse", raw)

        async def coord_call(method: str, req, resp_type: str, *, secret: bytes = COORDINATOR_SECRET):
            path = FENCE_SVC + method
            sign_service_call(path, req, "coordinator", clock(), secret)
            raw = await client.call(path, req.SerializeToString(deterministic=True))
            return protocol.parse(resp_type, raw)

        yield SimpleNamespace(
            protocol=protocol,
            clock=clock,
            directory=directory,
            notifier=notifier,
            store=store,
            service=service,
            client=client,
            peer=peer,
            make_player=make_player,
            admin_call=admin_call,
            match_call=match_call,
            coord_call=coord_call,
        )


def _battle_payload(protocol: Protocol, player_id: int, battle_id: int, *, outcome: int = 1, **extra) -> bytes:
    return protocol.message(
        "BattleSettlementData",
        battle_id=battle_id,
        player_id=player_id,
        outcome=outcome,
        **extra,
    ).SerializeToString(deterministic=True)


# ── 1. 分片、摘要与序号算法与 Go 金标对齐 ─────────────────────────────────────────────────────────────


def test_shard_digest_and_seq_parity() -> None:
    gamma = 0x9E3779B97F4A7C15
    assert mix64(gamma) == 0xE220A8397B1DCDAF
    assert mix64((2 * gamma) & ((1 << 64) - 1)) == 0x6E789E6AA1B965F4
    assert mix64(1) == 0x5692161D100B05E5

    expected_shards = {
        0: 0,
        1: 37,
        2: 10,
        63: 21,
        64: 19,
        65: 43,
        12345: 17,
        (1 << 63) + 5: 26,
        (1 << 64) - 1: 59,
    }
    for pid, want in expected_shards.items():
        assert row_shard(pid) == want

    assert content_sha256(MSG_BATTLE_SETTLEMENT, 0x0102030405060708, b"\x08\x01").hex() == (
        "f719478459441aff1901145cc52f3d68fb76c81781022620b7e9f988e45ec43b"
    )
    assert content_sha256(MSG_GM_BLOCK_CURRENCY_GAIN, 1000, b"").hex() == (
        "071fb8d284782f437b2e09b7a933ce5fd614252c2e0c16abfd5be3ad6454e193"
    )
    assert content_sha256(1, 1000, None) == content_sha256(1, 1000, b"")

    now_ms = 1_793_000_000_000
    s1 = next_seq(1, now_ms)
    assert s1 == (now_ms << 12)
    assert next_seq(s1 + 1, now_ms) == s1 + 1
    assert next_seq(s1 + 5, now_ms - 10_000) == s1 + 5  # 时钟回拨时仍单调递增


# ── 2. 配置校验与 v1 禁用类型检查 ─────────────────────────────────────────────────────────────────────


def _fake_settings(node_dict: dict) -> SimpleNamespace:
    return SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="stable",
        etcd="127.0.0.1:2379",
        data_service="127.0.0.1:9000",
        redis="",
        kafka="",
        kafka_generation=1,
        kafka_partitions=256,
        player_inbox_node=node_dict,
    )


def test_config_validation_blocks_debt_and_weak_credentials() -> None:
    protocol = Protocol()
    base_env = {
        SCENE_SECRET_ENV: SCENE_SECRET.decode(),
        MATCH_SECRET_ENV: MATCH_SECRET.decode(),
        COORDINATOR_SECRET_ENV: COORDINATOR_SECRET.decode(),
        ROLE_FENCE_SECRET_ENV: ROLE_FENCE_SECRET.decode(),
        DEFAULT_ADMIN_TOKEN_ENV: ADMIN_TOKEN,
    }
    settings = _fake_settings({
        "node_id": 109,
        "advertise_host": "127.0.0.1",
        "enqueue_types": [
            "PLAYER_INBOX_MSG_GM_BLOCK_CURRENCY_GAIN",
            "PLAYER_INBOX_MSG_BATTLE_SETTLEMENT",
        ],
    })
    cfg = PlayerInboxConfig.from_settings(settings, protocol, environ=base_env)
    assert cfg.grpc_port == 50950
    assert cfg.lease_ttl_sec == 60
    assert cfg.admin_token == ADMIN_TOKEN

    # v1 禁止在 enqueue_types 中开启 GM_ATTACH_CURRENCY_DEBT
    bad_settings = _fake_settings({
        "node_id": 109,
        "advertise_host": "127.0.0.1",
        "enqueue_types": ["PLAYER_INBOX_MSG_GM_ATTACH_CURRENCY_DEBT"],
    })
    with pytest.raises(ValueError, match="GM_ATTACH_CURRENCY_DEBT"):
        PlayerInboxConfig.from_settings(bad_settings, protocol, environ=base_env)

    # 短 admin_token 拒启
    short_env = dict(base_env, **{DEFAULT_ADMIN_TOKEN_ENV: "too-short"})
    with pytest.raises(ValueError, match="16"):
        PlayerInboxConfig.from_settings(settings, protocol, environ=short_env)

    # 空 admin_token 允许启动（运维面停用）
    empty_admin_env = dict(base_env, **{DEFAULT_ADMIN_TOKEN_ENV: ""})
    cfg_no_admin = PlayerInboxConfig.from_settings(settings, protocol, environ=empty_admin_env)
    assert cfg_no_admin.admin_token == ""

    # 缺失 match_secret 允许启动（仅 Battle 生产面回 UNAVAILABLE）
    no_match_env = {k: v for k, v in base_env.items() if k != MATCH_SECRET_ENV}
    cfg_no_match = PlayerInboxConfig.from_settings(settings, protocol, environ=no_match_env)
    assert cfg_no_match.match_secret == b""


# ── 3. Fetch / Ack 全生命周期（使用真实 InboxPeer 客户端） ────────────────────────────────────────────


def test_fetch_ack_lifecycle_with_real_inbox_peer() -> None:
    async def scenario() -> None:
        async with running_inbox() as env:
            pid = 10001
            env.directory.seed_player(pid, "acct_alpha", owner_revision=1)
            player = env.make_player(pid, owner_revision=1, owner_epoch=10)

            # 无流时 Fetch 返回 NO_STREAM
            empty = await env.peer.fetch(player, GM, correlation=1, cursor=0)
            assert empty.fence == FENCE_NO_STREAM
            assert empty.error_message.id == 0
            assert len(empty.items) == 0

            # GM 下发货币封锁
            sanction_req = env.protocol.message(
                "player_inbox.GmSanctionPlayerRequest",
                player_id=pid,
                request_id=9001,
                issued_at_ms=env.clock(),
                block_gain=env.protocol.message("player_inbox.GmCurrencyGainPayload", currency_type=1),
                reason="risk_control",
                operator="gm_alice",
            )
            sanction_resp = await env.admin_call("GmSanctionPlayer", sanction_req, "player_inbox.GmSanctionPlayerResponse")
            assert sanction_resp.error_message.id == 0
            assert sanction_resp.created is True
            assert sanction_resp.state == STATE_PENDING
            seq1 = sanction_resp.seq
            assert seq1 >= (env.clock() << 12)
            assert env.notifier.events == [(pid, DOMAIN_GM_SANCTION)]
            # 提醒已落定，notify_due_ms 被 CAS 清零
            summary = await env.admin_call(
                "GetSummary",
                env.protocol.message("player_inbox.GetPlayerInboxSummaryRequest", player_id=pid),
                "player_inbox.GetPlayerInboxSummaryResponse",
            )
            assert summary.streams[0].notify_due_ms == 0
            assert summary.streams[0].open_count == 1

            # 同键同摘要幂等重放：不重复创建，也不再发提醒
            dup_resp = await env.admin_call("GmSanctionPlayer", sanction_req, "player_inbox.GmSanctionPlayerResponse")
            assert dup_resp.error_message.id == 0
            assert dup_resp.created is False
            assert dup_resp.seq == seq1
            assert len(env.notifier.events) == 1

            # 同键不同摘要（刷新 issued_at_ms）：幂等冲突
            conflict_req = env.protocol.message("player_inbox.GmSanctionPlayerRequest")
            conflict_req.CopyFrom(sanction_req)
            conflict_req.issued_at_ms = env.clock() + 1
            conflict_resp = await env.admin_call("GmSanctionPlayer", conflict_req, "player_inbox.GmSanctionPlayerResponse")
            assert conflict_resp.error_message.id == ERR_IDEMPOTENCY_CONFLICT

            # InboxPeer Fetch（首次推进围栏 -> FIRST）
            fetched = await env.peer.fetch(player, GM, correlation=2, cursor=0)
            assert fetched.error_message.id == 0
            assert fetched.fence == FENCE_FIRST
            assert fetched.correlation_id == 2
            assert len(fetched.items) == 1
            assert fetched.items[0].seq == seq1
            assert fetched.items[0].delivery_count == 1
            assert fetched.items[0].request_id == 9001

            # 再次 Fetch -> MATCH，delivery_count=2
            fetched2 = await env.peer.fetch(player, GM, correlation=3, cursor=0)
            assert fetched2.fence == FENCE_MATCH
            assert fetched2.items[0].delivery_count == 2

            # 旧 epoch Fetch -> STALE + ERR_STALE_OWNER
            stale_player = env.make_player(pid, owner_revision=1, owner_epoch=9)
            stale_resp = await env.peer.fetch(stale_player, GM, correlation=4, cursor=0)
            assert stale_resp.fence == FENCE_STALE
            assert stale_resp.error_message.id == ERR_STALE_OWNER
            assert len(stale_resp.items) == 0

            # Ack 异常分支：request_id 不匹配 -> MISMATCH，SKIPPED 无指令 -> SKIP_NOT_REQUESTED
            bad_ack = await env.peer.ack(
                player,
                GM,
                correlation=5,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport",
                        seq=seq1,
                        outcome=APPLIED,
                        request_id=9999,
                    )
                ],
            )
            assert bad_ack.results[0].result == ACK_MISMATCH
            assert bad_ack.results[0].state == STATE_PENDING

            skip_ack = await env.peer.ack(
                player,
                GM,
                correlation=6,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport",
                        seq=seq1,
                        outcome=SKIPPED,
                        reason=VOID_CANCELLED,
                        request_id=9001,
                    )
                ],
            )
            assert skip_ack.results[0].result == ACK_SKIP_NOT_REQUESTED

            # 正常 Ack APPLIED -> FINALIZED
            ok_ack = await env.peer.ack(
                player,
                GM,
                correlation=7,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport",
                        seq=seq1,
                        outcome=APPLIED,
                        request_id=9001,
                    )
                ],
            )
            assert ok_ack.fence == FENCE_MATCH
            assert ok_ack.results[0].result == ACK_FINALIZED
            assert ok_ack.results[0].state == STATE_APPLIED

            # 重复同结局 Ack -> ALREADY_SAME；不同结局 Ack -> CONFLICT（永不翻转）
            same_ack = await env.peer.ack(
                player,
                GM,
                correlation=8,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport",
                        seq=seq1,
                        outcome=APPLIED,
                        request_id=9001,
                    )
                ],
            )
            assert same_ack.results[0].result == ACK_ALREADY_SAME

            flip_ack = await env.peer.ack(
                player,
                GM,
                correlation=9,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport",
                        seq=seq1,
                        outcome=REJECTED,
                        reason=29003,
                        request_id=9001,
                    )
                ],
            )
            assert flip_ack.results[0].result == ACK_CONFLICT
            assert flip_ack.results[0].state == STATE_APPLIED

            # 新 epoch + CursorAhead -> ADVANCE 并抬高 next_seq
            higher_player = env.make_player(pid, owner_revision=1, owner_epoch=11)
            ahead_cursor = seq1 + 500
            ahead_fetch = await env.peer.fetch(higher_player, GM, correlation=10, cursor=ahead_cursor)
            assert ahead_fetch.fence == FENCE_ADVANCE
            summary2 = await env.admin_call(
                "GetSummary",
                env.protocol.message("player_inbox.GetPlayerInboxSummaryRequest", player_id=pid),
                "player_inbox.GetPlayerInboxSummaryResponse",
            )
            assert summary2.max_fence_epoch == 11
            assert summary2.streams[0].next_seq == ahead_cursor + 1
            assert summary2.streams[0].open_count == 0

    asyncio.run(scenario())


# ── 4. Battle 结算入箱、回执签名、VOID 白名单、不可作废与仅 APPLIED 终结 ─────────────────────────────


def test_battle_settlement_lifecycle_and_invariants() -> None:
    async def scenario() -> None:
        async with running_inbox() as env:
            pid = 20001
            battle_id = 77001
            env.directory.seed_player(pid, "acct_battle", owner_revision=3)
            player = env.make_player(pid, owner_revision=3, owner_epoch=5)

            # 绕过 7 天过去窗口（30 天前签发的原件仍可恢复入箱）
            old_issued = env.clock() - 30 * 24 * 60 * 60 * 1000
            payload = _battle_payload(env.protocol, pid, battle_id, outcome=1, gold_gain=500)
            req = env.protocol.message(
                "player_inbox.BattleSettlementInboxRequest",
                player_id=pid,
                battle_id=battle_id,
                expected_owner_account="acct_battle",
                expected_owner_revision=3,
                issued_at_ms=old_issued,
                settlement_payload=payload,
            )
            enq_resp = await env.match_call("EnqueueBattleSettlement", req)
            assert enq_resp.error_message.id == 0
            assert enq_resp.state == STATE_PENDING
            assert enq_resp.seq > 0
            assert enq_resp.content_sha256 == content_sha256(MSG_BATTLE_SETTLEMENT, old_issued, payload)
            assert enq_resp.request_digest == service_digest(req)
            assert verify_service_call(
                INBOX_SVC + "EnqueueBattleSettlement/reply",
                enq_resp,
                "inbox",
                env.clock(),
                MATCH_SECRET,
            )

            # LookupBattleSettlement 返回永久 SQL 收据并带回执签名
            look_req = env.protocol.message("player_inbox.BattleSettlementInboxRequest")
            look_req.CopyFrom(req)
            look_resp = await env.match_call("LookupBattleSettlement", look_req)
            assert look_resp.seq == enq_resp.seq
            assert look_resp.state == STATE_PENDING
            assert look_resp.request_digest == service_digest(look_req)
            assert verify_service_call(
                INBOX_SVC + "LookupBattleSettlement/reply",
                look_resp,
                "inbox",
                env.clock(),
                MATCH_SECRET,
            )

            # Battle 域容量上限 max_pending=1：新对局满箱，旧对局重放幂等成功
            req2 = env.protocol.message(
                "player_inbox.BattleSettlementInboxRequest",
                player_id=pid,
                battle_id=battle_id + 1,
                expected_owner_account="acct_battle",
                expected_owner_revision=3,
                issued_at_ms=env.clock(),
                settlement_payload=_battle_payload(env.protocol, pid, battle_id + 1, outcome=1),
            )
            full_resp = await env.match_call("EnqueueBattleSettlement", req2)
            assert full_resp.error_message.id == ERR_FULL
            dup_battle = await env.match_call("EnqueueBattleSettlement", req)
            assert dup_battle.error_message.id == 0
            assert dup_battle.seq == enq_resp.seq

            # Battle 域永不可作废
            void_battle = await env.admin_call(
                "VoidItem",
                env.protocol.message(
                    "player_inbox.VoidPlayerInboxItemRequest",
                    player_id=pid,
                    domain=DOMAIN_BATTLE_SETTLEMENT,
                    seq=enq_resp.seq,
                    reason=VOID_CANCELLED,
                    operator="gm_ops",
                ),
                "player_inbox.VoidPlayerInboxItemResponse",
            )
            assert void_battle.error_message.id == ERR_ITEM_STATE_CONFLICT

            # InboxPeer 拉取 Battle 结算并测试仅 APPLIED 可终结
            fetched = await env.peer.fetch(player, BATTLE, correlation=1, cursor=0)
            assert len(fetched.items) == 1
            assert fetched.items[0].seq == enq_resp.seq

            for bad_outcome in (REJECTED, SKIPPED, UNKNOWN):
                bad = await env.peer.ack(
                    player,
                    BATTLE,
                    correlation=2,
                    reports=[
                        env.protocol.message(
                            "player_inbox.PlayerInboxDecisionReport",
                            seq=enq_resp.seq,
                            outcome=bad_outcome,
                            reason=1,
                            request_id=battle_id,
                        )
                    ],
                )
                assert bad.results[0].result == ACK_CONFLICT
                assert bad.results[0].state == STATE_PENDING

            # 仅 APPLIED 终结
            applied = await env.peer.ack(
                player,
                BATTLE,
                correlation=3,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport",
                        seq=enq_resp.seq,
                        outcome=APPLIED,
                        request_id=battle_id,
                    )
                ],
            )
            assert applied.results[0].result == ACK_FINALIZED
            assert applied.results[0].state == STATE_APPLIED

            # Lookup 反映已终结为 APPLIED
            look_after = await env.match_call("LookupBattleSettlement", look_req)
            assert look_after.state == STATE_APPLIED

            # BATTLE_OUTCOME_VOID 白名单校验：无效果字段通过，带 gold_gain 被拒 (FAILED_PRECONDITION)
            void_ok_payload = _battle_payload(
                env.protocol, pid, 77002, outcome=4, player_team_index=1, finished_at_ms=env.clock()
            )
            void_ok_req = env.protocol.message(
                "player_inbox.BattleSettlementInboxRequest",
                player_id=pid,
                battle_id=77002,
                expected_owner_account="acct_battle",
                expected_owner_revision=3,
                issued_at_ms=env.clock(),
                settlement_payload=void_ok_payload,
            )
            void_ok_resp = await env.match_call("EnqueueBattleSettlement", void_ok_req)
            assert void_ok_resp.error_message.id == 0
            assert void_ok_resp.state == STATE_PENDING

            void_bad_payload = _battle_payload(env.protocol, pid, 77003, outcome=4, gold_gain=10)
            void_bad_req = env.protocol.message(
                "player_inbox.BattleSettlementInboxRequest",
                player_id=pid,
                battle_id=77003,
                expected_owner_account="acct_battle",
                expected_owner_revision=3,
                issued_at_ms=env.clock(),
                settlement_payload=void_bad_payload,
            )
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await env.match_call("EnqueueBattleSettlement", void_bad_req)
            assert exc_info.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            # 同 battle_id 不同正文冲突 -> FAILED_PRECONDITION
            conflict_battle = env.protocol.message("player_inbox.BattleSettlementInboxRequest")
            conflict_battle.CopyFrom(req)
            conflict_battle.issued_at_ms = old_issued + 1
            with pytest.raises(grpc.aio.AioRpcError) as exc_info2:
                await env.match_call("EnqueueBattleSettlement", conflict_battle)
            assert exc_info2.value.code() == grpc.StatusCode.FAILED_PRECONDITION

    asyncio.run(scenario())


# ── 5. GM 作废（普通 + FORCE_MISMATCH）、载荷剥离、对账、物品封禁查询与 v1 欠款禁用 ────────────────────


def test_gm_sanction_void_force_void_reconcile_and_item_blocks() -> None:
    async def scenario() -> None:
        async with running_inbox() as env:
            pid = 30001
            env.directory.seed_player(pid, "acct_gm", owner_revision=1)
            player = env.make_player(pid, owner_revision=1, owner_epoch=1)

            # v1 挂欠款禁用 -> ERR_DISABLED
            debt_req = env.protocol.message(
                "player_inbox.GmSanctionPlayerRequest",
                player_id=pid,
                request_id=5000,
                issued_at_ms=env.clock(),
                attach_debt=env.protocol.message(
                    "player_inbox.GmCurrencyDebtPayload", currency_type=0, owe_amount=100, reason="bug"
                ),
                reason="chargeback",
                operator="gm_bob",
            )
            debt_resp = await env.admin_call("GmSanctionPlayer", debt_req, "player_inbox.GmSanctionPlayerResponse")
            assert debt_resp.error_message.id == ERR_DISABLED

            # 1) 未投递条目普通作废 -> 直接 VOIDED + ADMIN + 审计
            r1 = await env.admin_call(
                "GmSanctionPlayer",
                env.protocol.message(
                    "player_inbox.GmSanctionPlayerRequest",
                    player_id=pid,
                    request_id=5001,
                    issued_at_ms=env.clock(),
                    block_item_gain=env.protocol.message("player_inbox.GmItemGainPayload", item_config_id=101),
                    reason="block_item_101",
                    operator="gm_bob",
                ),
                "player_inbox.GmSanctionPlayerResponse",
            )
            assert r1.created is True
            v1 = await env.admin_call(
                "VoidItem",
                env.protocol.message(
                    "player_inbox.VoidPlayerInboxItemRequest",
                    player_id=pid,
                    domain=DOMAIN_GM_SANCTION,
                    seq=r1.seq,
                    reason=VOID_CANCELLED,
                    operator="gm_bob",
                    note="mistake",
                ),
                "player_inbox.VoidPlayerInboxItemResponse",
            )
            assert v1.error_message.id == 0
            assert v1.state == STATE_VOIDED
            assert v1.void_requested is False
            lookup1 = await env.admin_call(
                "LookupByRequest",
                env.protocol.message(
                    "player_inbox.LookupPlayerInboxRequest",
                    player_id=pid,
                    producer_kind=PRODUCER_GM,
                    request_id=5001,
                ),
                "player_inbox.LookupPlayerInboxResponse",
            )
            assert lookup1.item.finalized_by == FINALIZER_ADMIN

            # 2) 已投递条目普通作废 -> 只挂 VOID 指令，下次 Fetch 剥离 payload，Ack(SKIPPED) 终结
            r2 = await env.admin_call(
                "GmSanctionPlayer",
                env.protocol.message(
                    "player_inbox.GmSanctionPlayerRequest",
                    player_id=pid,
                    request_id=5002,
                    issued_at_ms=env.clock(),
                    block_item_gain=env.protocol.message("player_inbox.GmItemGainPayload", item_config_id=202),
                    reason="block_item_202",
                    operator="gm_bob",
                ),
                "player_inbox.GmSanctionPlayerResponse",
            )
            # GetItemGainBlocks 应同时返回已落盘与未生效封禁单
            env.directory.item_blocks[pid] = [99]
            blocks_resp = await env.admin_call(
                "GetItemGainBlocks",
                env.protocol.message("player_inbox.GetPlayerItemGainBlocksRequest", player_id=pid),
                "player_inbox.GetPlayerItemGainBlocksResponse",
            )
            assert blocks_resp.found is True
            assert list(blocks_resp.blocked_item_config_ids) == [99]
            assert len(blocks_resp.pending) == 1
            assert blocks_resp.pending[0].item_config_id == 202

            f2 = await env.peer.fetch(player, GM, correlation=1, cursor=0)
            assert len(f2.items) == 1
            assert len(f2.items[0].payload) > 0

            v2 = await env.admin_call(
                "VoidItem",
                env.protocol.message(
                    "player_inbox.VoidPlayerInboxItemRequest",
                    player_id=pid,
                    domain=DOMAIN_GM_SANCTION,
                    seq=r2.seq,
                    reason=VOID_QUARANTINED,
                    operator="gm_bob",
                    note="poison",
                ),
                "player_inbox.VoidPlayerInboxItemResponse",
            )
            assert v2.error_message.id == 0
            assert v2.state == STATE_PENDING
            assert v2.void_requested is True

            # 带指令后再次 Fetch：载荷必须被剥离为空
            f2_void = await env.peer.fetch(player, GM, correlation=2, cursor=0)
            assert f2_void.items[0].directive == DIRECTIVE_VOID
            assert f2_void.items[0].void_reason == VOID_QUARANTINED
            assert f2_void.items[0].payload == b""

            # Ack(SKIPPED) 终结
            ack2 = await env.peer.ack(
                player,
                GM,
                correlation=3,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport",
                        seq=r2.seq,
                        outcome=SKIPPED,
                        reason=VOID_QUARANTINED,
                        request_id=5002,
                    )
                ],
            )
            assert ack2.results[0].result == ACK_FINALIZED
            assert ack2.results[0].state == STATE_VOIDED

            # 3) FORCE_MISMATCH 账本证明分支 + 成功强制作废
            r3 = await env.admin_call(
                "GmSanctionPlayer",
                env.protocol.message(
                    "player_inbox.GmSanctionPlayerRequest",
                    player_id=pid,
                    request_id=5003,
                    issued_at_ms=env.clock(),
                    unblock_gain=env.protocol.message("player_inbox.GmCurrencyGainPayload", currency_type=0),
                    reason="unblock_gold",
                    operator="gm_bob",
                ),
                "player_inbox.GmSanctionPlayerResponse",
            )
            force_req = env.protocol.message(
                "player_inbox.VoidPlayerInboxItemRequest",
                player_id=pid,
                domain=DOMAIN_GM_SANCTION,
                seq=r3.seq,
                reason=VOID_QUARANTINED,
                operator="gm_bob",
                note="ticket-123",
                mode=VOID_MODE_FORCE_MISMATCH,
            )
            # 未投递 -> NOT_DELIVERED
            fv_not_del = await env.admin_call("VoidItem", force_req, "player_inbox.VoidPlayerInboxItemResponse")
            assert fv_not_del.error_message.id == ERR_ITEM_STATE_CONFLICT
            assert fv_not_del.force_verdict == FORCE_VOID_NOT_DELIVERED

            # 投递一次后，账本缺失 -> LEDGER_ABSENT
            await env.peer.fetch(player, GM, correlation=4, cursor=0)
            fv_absent = await env.admin_call("VoidItem", force_req, "player_inbox.VoidPlayerInboxItemResponse")
            assert fv_absent.force_verdict == FORCE_VOID_LEDGER_ABSENT

            # 账本损坏（重复域）-> LEDGER_INVALID
            bad_ledger = env.protocol.message("PlayerInboxLedgerComp")
            bad_ledger.cursors.add(domain=DOMAIN_GM_SANCTION, decided_through_seq=r3.seq)
            bad_ledger.cursors.add(domain=DOMAIN_GM_SANCTION, decided_through_seq=r3.seq)
            env.directory.ledgers[pid] = bad_ledger
            fv_invalid = await env.admin_call("VoidItem", force_req, "player_inbox.VoidPlayerInboxItemResponse")
            assert fv_invalid.force_verdict == FORCE_VOID_LEDGER_INVALID

            # 账本水位未到 -> NOT_DECIDED
            low_ledger = env.protocol.message("PlayerInboxLedgerComp")
            low_ledger.cursors.add(domain=DOMAIN_GM_SANCTION, decided_through_seq=r3.seq - 1)
            env.directory.ledgers[pid] = low_ledger
            fv_low = await env.admin_call("VoidItem", force_req, "player_inbox.VoidPlayerInboxItemResponse")
            assert fv_low.force_verdict == FORCE_VOID_NOT_DECIDED

            # 水位已过但环里无该 seq -> SEQ_NOT_IN_RING
            no_ring_ledger = env.protocol.message("PlayerInboxLedgerComp")
            no_ring_ledger.cursors.add(domain=DOMAIN_GM_SANCTION, decided_through_seq=r3.seq)
            env.directory.ledgers[pid] = no_ring_ledger
            fv_noring = await env.admin_call("VoidItem", force_req, "player_inbox.VoidPlayerInboxItemResponse")
            assert fv_noring.force_verdict == FORCE_VOID_SEQ_NOT_IN_RING

            # 环里出现本行 request_id -> REQUEST_IN_LEDGER
            same_req_ledger = env.protocol.message("PlayerInboxLedgerComp")
            c = same_req_ledger.cursors.add(domain=DOMAIN_GM_SANCTION, decided_through_seq=r3.seq)
            c.recent.add(seq=r3.seq, outcome=APPLIED, request_id=5003, msg_type=MSG_GM_UNBLOCK_CURRENCY_GAIN)
            env.directory.ledgers[pid] = same_req_ledger
            fv_same = await env.admin_call("VoidItem", force_req, "player_inbox.VoidPlayerInboxItemResponse")
            assert fv_same.force_verdict == FORCE_VOID_REQUEST_IN_LEDGER

            # 环里同 seq 记的是另一个 request_id -> PROVEN，强制作废成功并写 FORCE_VOID 审计！
            proven_ledger = env.protocol.message("PlayerInboxLedgerComp")
            c2 = proven_ledger.cursors.add(domain=DOMAIN_GM_SANCTION, decided_through_seq=r3.seq)
            c2.recent.add(seq=r3.seq, outcome=APPLIED, request_id=888888, msg_type=MSG_GM_BLOCK_CURRENCY_GAIN)
            env.directory.ledgers[pid] = proven_ledger
            fv_ok = await env.admin_call("VoidItem", force_req, "player_inbox.VoidPlayerInboxItemResponse")
            assert fv_ok.error_message.id == 0
            assert fv_ok.state == STATE_VOIDED
            assert fv_ok.force_verdict == FORCE_VOID_PROVEN
            assert (row_shard(pid), pid, DOMAIN_GM_SANCTION, r3.seq, AUDIT_FORCE_VOID) in env.store.mem_state.audits

            # 4) ReconcilePlayer 检测 FINAL_NOT_IN_LEDGER 与 PENDING_BUT_DECIDED
            rec = await env.admin_call(
                "ReconcilePlayer",
                env.protocol.message("player_inbox.ReconcilePlayerInboxRequest", player_id=pid),
                "player_inbox.ReconcilePlayerInboxResponse",
            )
            assert rec.error_message.id == 0
            assert rec.ledger_found is True
            assert any(d.seq == r2.seq and d.kind == RECONCILE_FINAL_NOT_IN_LEDGER for d in rec.diffs)

            # ListItems 翻页检查
            listed = await env.admin_call(
                "ListItems",
                env.protocol.message(
                    "player_inbox.ListPlayerInboxItemsRequest",
                    player_id=pid,
                    domain=DOMAIN_GM_SANCTION,
                    limit=2,
                ),
                "player_inbox.ListPlayerInboxItemsResponse",
            )
            assert len(listed.items) == 2
            assert listed.has_more is True

            # 7 天后对已存在的 GM 请求重放：判重先于时间窗，返回 duplicate
            env.clock.now_ms += ISSUED_AT_MAX_PAST_MS + 60_000
            r1_issued = lookup1.item.issued_at_ms
            old_retry = await env.admin_call(
                "GmSanctionPlayer",
                env.protocol.message(
                    "player_inbox.GmSanctionPlayerRequest",
                    player_id=pid,
                    request_id=5001,
                    issued_at_ms=r1_issued,
                    block_item_gain=env.protocol.message("player_inbox.GmItemGainPayload", item_config_id=101),
                    reason="block_item_101",
                    operator="gm_bob",
                ),
                "player_inbox.GmSanctionPlayerResponse",
            )
            assert old_retry.error_message.id == 0
            assert old_retry.created is False
            assert old_retry.seq == r1.seq
            # 新请求携带超窗时间戳被拒
            new_expired = await env.admin_call(
                "GmSanctionPlayer",
                env.protocol.message(
                    "player_inbox.GmSanctionPlayerRequest",
                    player_id=pid,
                    request_id=5999,
                    issued_at_ms=r1_issued,
                    block_item_gain=env.protocol.message("player_inbox.GmItemGainPayload", item_config_id=101),
                    reason="too_old",
                    operator="gm_bob",
                ),
                "player_inbox.GmSanctionPlayerResponse",
            )
            assert new_expired.error_message.id == ERR_ISSUED_OUT_OF_WINDOW

    asyncio.run(scenario())


# ── 6. RoleFenceParticipant: PrepareRoleFreeze / ApplyRoleFence / PurgeRemovedRole ───────────────────


def test_role_fence_freeze_apply_and_purge() -> None:
    async def scenario() -> None:
        async with running_inbox() as env:
            pid = 40001
            env.directory.seed_player(pid, "acct_rf", owner_revision=1, version=1)
            player = env.make_player(pid, owner_revision=1, owner_epoch=1)

            # 写入一条 PENDING 条目
            enq = await env.admin_call(
                "GmSanctionPlayer",
                env.protocol.message(
                    "player_inbox.GmSanctionPlayerRequest",
                    player_id=pid,
                    request_id=6001,
                    issued_at_ms=env.clock(),
                    block_gain=env.protocol.message("player_inbox.GmCurrencyGainPayload", currency_type=0),
                    reason="check_freeze",
                    operator="gm_ops",
                ),
                "player_inbox.GmSanctionPlayerResponse",
            )
            assert enq.created is True

            freeze_state = env.protocol.message(
                "rolefence.RoleFenceState",
                player_id=pid,
                account="acct_rf",
                owner_revision=1,
                freeze_token=888,
                version=2,
                home_zone=1,
            )
            # 有 PENDING 条目时 PrepareRoleFreeze 报 blocker 且回滚（不冻结副本！）
            prep_blocked = await env.coord_call(
                "PrepareRoleFreeze",
                env.protocol.message("rolefence.PrepareRoleFreezeRequest", state=freeze_state),
                "rolefence.PrepareRoleFreezeResponse",
            )
            assert prep_blocked.clean is False
            assert prep_blocked.blocker == ROLE_BLOCKER_ITEM
            assert env.store.mem_state.role_fences[pid].freeze_token == 0  # 回滚验证

            # 消费并确认完毕后，dry_run 返回 clean=True 且不写副本
            await env.peer.fetch(player, GM, correlation=1, cursor=0)
            await env.peer.ack(
                player,
                GM,
                correlation=2,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport",
                        seq=enq.seq,
                        outcome=APPLIED,
                        request_id=6001,
                    )
                ],
            )
            prep_dry = await env.coord_call(
                "PrepareRoleFreeze",
                env.protocol.message("rolefence.PrepareRoleFreezeRequest", state=freeze_state, dry_run=True),
                "rolefence.PrepareRoleFreezeResponse",
            )
            assert prep_dry.clean is True
            assert env.store.mem_state.role_fences[pid].freeze_token == 0

            # 正式 PrepareRoleFreeze -> clean=True 并冻结副本；同步更新目录模拟真实冻结
            prep_ok = await env.coord_call(
                "PrepareRoleFreeze",
                env.protocol.message("rolefence.PrepareRoleFreezeRequest", state=freeze_state),
                "rolefence.PrepareRoleFreezeResponse",
            )
            assert prep_ok.clean is True
            env.directory.fences[pid] = freeze_state

            # 冻结状态下 GM 入箱被拒（ERR_STALE_OWNER，非故障）
            blocked_sanction = await env.admin_call(
                "GmSanctionPlayer",
                env.protocol.message(
                    "player_inbox.GmSanctionPlayerRequest",
                    player_id=pid,
                    request_id=6002,
                    issued_at_ms=env.clock(),
                    block_gain=env.protocol.message("player_inbox.GmCurrencyGainPayload", currency_type=0),
                    reason="during_freeze",
                    operator="gm_ops",
                ),
                "player_inbox.GmSanctionPlayerResponse",
            )
            assert blocked_sanction.error_message.id == ERR_STALE_OWNER

            # ApplyRoleFence 版本更大的解冻生效；旧版本被忽略
            unfreeze = env.protocol.message(
                "rolefence.RoleFenceState",
                player_id=pid,
                account="acct_rf",
                owner_revision=1,
                freeze_token=0,
                version=3,
                home_zone=1,
            )
            await env.coord_call(
                "ApplyRoleFence",
                env.protocol.message("rolefence.ApplyRoleFenceRequest", state=unfreeze),
                "rolefence.ApplyRoleFenceResponse",
            )
            assert env.store.mem_state.role_fences[pid].version == 3
            assert env.store.mem_state.role_fences[pid].freeze_token == 0

            # 过期 Prepare（version=2 < 3）返回 stale=True
            prep_stale = await env.coord_call(
                "PrepareRoleFreeze",
                env.protocol.message("rolefence.PrepareRoleFreezeRequest", state=freeze_state),
                "rolefence.PrepareRoleFreezeResponse",
            )
            assert prep_stale.stale is True

            # PurgeRemovedRole：墓碑清 stream 与 item，保留 audit！
            tombstone = env.protocol.message(
                "rolefence.RoleFenceState",
                player_id=pid,
                account="",
                owner_revision=1,
                freeze_token=0,
                version=4,
                home_zone=1,
            )
            purge_resp = await env.coord_call(
                "PurgeRemovedRole",
                env.protocol.message("rolefence.PurgeRemovedRoleRequest", state=tombstone),
                "rolefence.PurgeRemovedRoleResponse",
            )
            assert purge_resp.retry is False
            assert not any(k[1] == pid for k in env.store.mem_state.streams)
            assert not any(k[1] == pid for k in env.store.mem_state.items)
            assert any(k[1] == pid and k[4] == AUDIT_SANCTION for k in env.store.mem_state.audits)

    asyncio.run(scenario())


# ── 7. 五组凭据 fail-closed 鉴权与出站目录签名测试 ────────────────────────────────────────────────────


def test_five_credential_fail_closed_auth() -> None:
    async def scenario() -> None:
        # 7.1 运维口令为空 & Match 密钥未配置
        async with running_inbox(match_secret=b"", admin_token="") as env:
            with pytest.raises(grpc.aio.AioRpcError) as admin_err:
                await env.admin_call(
                    "GetSummary",
                    env.protocol.message("player_inbox.GetPlayerInboxSummaryRequest", player_id=1),
                    "player_inbox.GetPlayerInboxSummaryResponse",
                    token="any-token-long-enough-012345",
                )
            assert admin_err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            battle_req = env.protocol.message(
                "player_inbox.BattleSettlementInboxRequest",
                player_id=1,
                battle_id=1,
                expected_owner_account="a",
                expected_owner_revision=1,
                issued_at_ms=env.clock(),
                settlement_payload=_battle_payload(env.protocol, 1, 1),
            )
            with pytest.raises(grpc.aio.AioRpcError) as match_err:
                await env.match_call("EnqueueBattleSettlement", battle_req)
            assert match_err.value.code() == grpc.StatusCode.UNAVAILABLE

        # 7.2 错误签名、x-session-detail-bin 拦截、错误运维口令与错误协调者密钥
        async with running_inbox() as env:
            pid = 50001
            env.directory.seed_player(pid)
            player = env.make_player(pid, owner_revision=1, owner_epoch=1)

            # Scene 错签名
            bad_peer = InboxPeer(env.protocol, env.client.call, node_id=2, secret=b"wrong-scene-secret-0123456789012")
            with pytest.raises(grpc.aio.AioRpcError) as scene_err:
                await bad_peer.fetch(player, GM, correlation=1, cursor=0)
            assert scene_err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # Scene 携带 x-session-detail-bin 被拒
            fetch_req = env.protocol.message(
                "player_inbox.FetchPlayerInboxRequest",
                player_id=pid,
                domain=GM,
                owner_epoch=1,
                correlation_id=1,
                admission=player.role_identity,
            )
            sign_service_call(INBOX_SVC + "FetchItems", fetch_req, "scene", env.clock(), SCENE_SECRET)
            with pytest.raises(grpc.aio.AioRpcError) as sess_err:
                await env.client.call(
                    INBOX_SVC + "FetchItems",
                    fetch_req.SerializeToString(deterministic=True),
                    metadata=(("x-session-detail-bin", b"gate-session"),),
                )
            assert sess_err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # Match 错签名与 x-session-detail-bin 被拒
            battle_req = env.protocol.message(
                "player_inbox.BattleSettlementInboxRequest",
                player_id=pid,
                battle_id=1,
                expected_owner_account="acct_a",
                expected_owner_revision=1,
                issued_at_ms=env.clock(),
                settlement_payload=_battle_payload(env.protocol, pid, 1),
            )
            with pytest.raises(grpc.aio.AioRpcError) as match_bad:
                await env.match_call("LookupBattleSettlement", battle_req, secret=b"wrong-match-secret-0123456789012")
            assert match_bad.value.code() == grpc.StatusCode.PERMISSION_DENIED

            with pytest.raises(grpc.aio.AioRpcError) as match_sess:
                await env.match_call(
                    "LookupBattleSettlement",
                    battle_req,
                    metadata=(("x-session-detail-bin", b"gate-session"),),
                )
            assert match_sess.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # Admin 错口令与缺口令
            for tok in (None, "wrong-admin-token-01234567890123"):
                with pytest.raises(grpc.aio.AioRpcError) as adm_bad:
                    await env.admin_call(
                        "GetSummary",
                        env.protocol.message("player_inbox.GetPlayerInboxSummaryRequest", player_id=pid),
                        "player_inbox.GetPlayerInboxSummaryResponse",
                        token=tok,
                    )
                assert adm_bad.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # Coordinator 错签名
            st = env.protocol.message(
                "rolefence.RoleFenceState",
                player_id=pid,
                account="acct_a",
                owner_revision=1,
                freeze_token=1,
                version=2,
                home_zone=1,
            )
            with pytest.raises(grpc.aio.AioRpcError) as coord_bad:
                await env.coord_call(
                    "ApplyRoleFence",
                    env.protocol.message("rolefence.ApplyRoleFenceRequest", state=st),
                    "rolefence.ApplyRoleFenceResponse",
                    secret=b"wrong-coord-secret-0123456789012",
                )
            assert coord_bad.value.code() == grpc.StatusCode.PERMISSION_DENIED

    asyncio.run(scenario())


def test_outbound_grpc_directory_signs_as_player_inbox() -> None:
    async def scenario() -> None:
        protocol = Protocol()
        clock = MutableClock()
        seen: list[tuple[str, str, bool]] = []

        async def fake_call(path: str, data: bytes, *, timeout=None) -> bytes:
            if path.endswith("/GetRoleFenceState"):
                req = protocol.parse("data_service.GetRoleFenceStateRequest", data)
                ok = verify_service_call(path, req, "player_inbox", clock(), ROLE_FENCE_SECRET)
                seen.append((path, req.auth.caller, ok))
                state = protocol.message(
                    "rolefence.RoleFenceState",
                    player_id=req.player_id,
                    account="acct_out",
                    owner_revision=2,
                    version=1,
                    home_zone=1,
                )
                return protocol.message("data_service.GetRoleFenceStateResponse", found=True, state=state).SerializeToString()
            if path.endswith("/VerifyRoleAdmission"):
                req = protocol.parse("data_service.VerifyRoleAdmissionRequest", data)
                ok = verify_service_call(path, req, "player_inbox", clock(), ROLE_FENCE_SECRET)
                seen.append((path, req.auth.caller, ok))
                return protocol.message("data_service.VerifyRoleAdmissionResponse", valid=True).SerializeToString()
            raise AssertionError(f"unexpected path {path}")

        directory = GrpcDataServiceDirectory(protocol, fake_call, ROLE_FENCE_SECRET, clock_ms=clock)
        state = await directory.get_role_fence_state(77)
        assert state is not None and state.account == "acct_out"
        admission = protocol.message("common.base.RoleAdmissionIdentity", player_id=77)
        assert await directory.verify_role_admission(admission) is True
        assert seen == [
            ("/data_service.DataService/GetRoleFenceState", "player_inbox", True),
            ("/data_service.DataService/VerifyRoleAdmission", "player_inbox", True),
        ]

    asyncio.run(scenario())


# ── 8. KafkaInboxNotifier 与 SqlInboxStore UNHEX(%s) / DDL 检查 ──────────────────────────────────────


def test_kafka_notifier_emits_scene_command_event_49() -> None:
    async def scenario() -> None:
        protocol = Protocol()
        published: list[tuple[str, int, object, bytes | None]] = []

        class FakeLoc:
            def __init__(self, raw: bytes | None) -> None:
                self.raw = raw

            async def get_location(self, _player_id: int) -> bytes | None:
                return self.raw

        class FakeDir:
            async def lookup(self, _node_type: int, _zone: int, node_id: int):
                return protocol.message("NodeInfo", node_id=node_id, node_uuid=SCENE_UUID)

        class FakePub:
            async def publish(self, role: str, node_id: int, message, *, key: bytes | None = None) -> None:
                published.append((role, node_id, message, key))

        loc_bytes = protocol.message(
            "storage.PlayerLocation", node_id="12", zone_id=1, owner_epoch=3
        ).SerializeToString()
        notifier = KafkaInboxNotifier(protocol, FakeLoc(loc_bytes), FakeDir(), FakePub())
        res = await notifier.notify(888, DOMAIN_GM_SANCTION)
        assert res == NotifyResult.SENT
        assert len(published) == 1
        role, node_id, cmd, key = published[0]
        assert role == "scene" and node_id == 12 and key == b"888"
        assert cmd.event_id == PLAYER_INBOX_NOTIFY_EVENT_ID
        assert cmd.target_instance_id == SCENE_UUID
        ev = protocol.parse("PlayerInboxNotifyEvent", cmd.payload)
        assert ev.player_id == 888 and ev.domain == DOMAIN_GM_SANCTION

        # 离线返回 OFFLINE 且 settled=True
        offline = await KafkaInboxNotifier(protocol, FakeLoc(None), FakeDir(), FakePub()).notify(888, DOMAIN_GM_SANCTION)
        assert offline == NotifyResult.OFFLINE and offline.settled

    asyncio.run(scenario())


def test_edge_cases_epoch_required_ack_table_reconcile_and_admission_mismatch() -> None:
    async def scenario() -> None:
        async with running_inbox() as env:
            pid = 60001
            env.directory.seed_player(pid, "acct_edge", owner_revision=2)
            player = env.make_player(pid, owner_revision=2, owner_epoch=1)

            # owner_epoch == 0 -> EPOCH_REQUIRED + ERR_EPOCH_REQUIRED
            zero_req = env.protocol.message(
                "player_inbox.FetchPlayerInboxRequest",
                player_id=pid,
                domain=GM,
                owner_epoch=0,
                correlation_id=1,
            )
            sign_service_call(INBOX_SVC + "FetchItems", zero_req, "scene", env.clock(), SCENE_SECRET)
            zero_raw = await env.client.call(INBOX_SVC + "FetchItems", zero_req.SerializeToString(deterministic=True))
            zero_resp = env.protocol.parse("player_inbox.FetchPlayerInboxResponse", zero_raw)
            assert zero_resp.fence == FENCE_EPOCH_REQUIRED
            assert zero_resp.error_message.id == ERR_EPOCH_REQUIRED

            # Ack on NO_STREAM -> UNKNOWN_SEQ
            no_stream_ack = await env.peer.ack(
                player,
                GM,
                correlation=2,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport", seq=1, outcome=APPLIED, request_id=1
                    )
                ],
            )
            assert no_stream_ack.fence == FENCE_NO_STREAM
            assert no_stream_ack.results[0].result == ACK_UNKNOWN_SEQ

            # 入箱但未投递 -> Ack NOT_DELIVERED
            enq = await env.admin_call(
                "GmSanctionPlayer",
                env.protocol.message(
                    "player_inbox.GmSanctionPlayerRequest",
                    player_id=pid,
                    request_id=7001,
                    issued_at_ms=env.clock(),
                    block_gain=env.protocol.message("player_inbox.GmCurrencyGainPayload", currency_type=2),
                    reason="not_delivered_case",
                    operator="gm_ops",
                ),
                "player_inbox.GmSanctionPlayerResponse",
            )
            not_del_ack = await env.peer.ack(
                player,
                GM,
                correlation=3,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport", seq=enq.seq, outcome=APPLIED, request_id=7001
                    )
                ],
            )
            assert not_del_ack.results[0].result == ACK_NOT_DELIVERED

            # ReconcilePlayer: ledger 缺失 -> ledger_found=False
            rec_absent = await env.admin_call(
                "ReconcilePlayer",
                env.protocol.message("player_inbox.ReconcilePlayerInboxRequest", player_id=pid),
                "player_inbox.ReconcilePlayerInboxResponse",
            )
            assert rec_absent.error_message.id == 0
            assert rec_absent.ledger_found is False

            # ReconcilePlayer: 账本水位已过但收件箱仍 PENDING -> PENDING_BUT_DECIDED
            ledger = env.protocol.message("PlayerInboxLedgerComp")
            ledger.cursors.add(domain=DOMAIN_GM_SANCTION, decided_through_seq=enq.seq)
            env.directory.ledgers[pid] = ledger
            rec_pending = await env.admin_call(
                "ReconcilePlayer",
                env.protocol.message("player_inbox.ReconcilePlayerInboxRequest", player_id=pid),
                "player_inbox.ReconcilePlayerInboxResponse",
            )
            assert rec_pending.ledger_found is True
            assert len(rec_pending.diffs) == 1
            assert rec_pending.diffs[0].kind == RECONCILE_PENDING_BUT_DECIDED

            # ReconcilePlayer: 账本结构损坏 -> ERR_STORE_UNAVAILABLE（fail-closed）
            bad_ledger = env.protocol.message("PlayerInboxLedgerComp")
            bad_ledger.cursors.add(domain=0, decided_through_seq=1)
            env.directory.ledgers[pid] = bad_ledger
            rec_invalid = await env.admin_call(
                "ReconcilePlayer",
                env.protocol.message("player_inbox.ReconcilePlayerInboxRequest", player_id=pid),
                "player_inbox.ReconcilePlayerInboxResponse",
            )
            assert rec_invalid.error_message.id == ERR_STORE_UNAVAILABLE

            # 投递后以 UNKNOWN 确认 -> SETTLED_UNKNOWN
            await env.peer.fetch(player, GM, correlation=4, cursor=0)
            unk_ack = await env.peer.ack(
                player,
                GM,
                correlation=5,
                reports=[
                    env.protocol.message(
                        "player_inbox.PlayerInboxDecisionReport", seq=enq.seq, outcome=UNKNOWN, request_id=7001
                    )
                ],
            )
            assert unk_ack.results[0].result == ACK_FINALIZED
            assert unk_ack.results[0].state == STATE_SETTLED_UNKNOWN

            # 过期的 owner_revision（角色已换主）-> ERR_STALE_OWNER
            stale_rev_player = env.make_player(pid, owner_revision=1, owner_epoch=2)
            stale_rev_resp = await env.peer.fetch(stale_rev_player, GM, correlation=6, cursor=0)
            assert stale_rev_resp.error_message.id == ERR_STALE_OWNER

    asyncio.run(scenario())


def test_schema_sql_and_unhex_bindings() -> None:
    schema_path = Path(__file__).resolve().parents[1] / "config" / "player_inbox_schema.sql"
    ddl = schema_path.read_text("utf-8")
    for table in ("player_inbox_stream", "player_inbox_item", "player_inbox_audit", "role_fence"):
        assert table in ddl
    assert "uk_player_inbox_item" in ddl
    assert "idx_player_inbox_item_1" in ddl
    assert "VARBINARY(191)" in ddl

    # 运行期验证 _SqlTx 对 role_fence / item / audit / mismatched_pending 均使用 UNHEX(%s)
    from xuanming.player_inbox.model import AuditRow, ItemRow
    from xuanming.player_inbox.sql_store import _SqlTx

    executed: list[tuple[str, tuple]] = []

    class FakeCursor:
        rowcount = 1

        async def execute(self, sql: str, args=()):
            executed.append((sql, tuple(args)))

        async def fetchone(self):
            return None

    async def exercise_sql() -> None:
        protocol = Protocol()
        tx = _SqlTx(FakeCursor())
        st = protocol.message(
            "rolefence.RoleFenceState",
            player_id=1,
            account="acct_bin",
            owner_revision=1,
            freeze_token=0,
            version=1,
            home_zone=1,
        )
        await tx.apply_role_fence(st, 1000)
        await tx.has_mismatched_pending_original(row_shard(1), 1, "acct_bin", 1)
        item = ItemRow(
            shard=row_shard(1),
            player_id=1,
            domain=DOMAIN_GM_SANCTION,
            seq=10,
            msg_type=MSG_GM_BLOCK_CURRENCY_GAIN,
            producer_kind=PRODUCER_GM,
            request_id=100,
            content_sha256=b"\xab" * 32,
            payload=b"\x08\x01",
            state=STATE_PENDING,
            directive=0,
            void_reason=0,
            delivery_count=0,
            last_delivered_epoch=0,
            last_delivered_ms=0,
            outcome_reason=0,
            finalized_by=0,
            decided_epoch=0,
            issued_at_ms=1000,
            created_ms=1000,
            updated_ms=1000,
            operator_name="gm",
            note="n",
            owner_account="acct_bin",
            owner_revision=1,
        )
        await tx.insert_item(item)
        await tx.insert_audit(
            AuditRow(
                shard=row_shard(1),
                player_id=1,
                domain=DOMAIN_GM_SANCTION,
                seq=10,
                action=AUDIT_SANCTION,
                operator="gm",
                peer_addr="127.0.0.1",
                reason="r",
                created_ms=1000,
                msg_type=MSG_GM_BLOCK_CURRENCY_GAIN,
                request_id=100,
                issued_at_ms=1000,
                content_sha256=b"\xab" * 32,
                payload=b"\x08\x01",
                void_reason=0,
            )
        )

    asyncio.run(exercise_sql())
    assert len(executed) == 4
    for sql, args in executed:
        assert "UNHEX(%s)" in sql
        assert all(not isinstance(a, (bytes, bytearray)) for a in args)
    assert SqlInboxStore is not None
