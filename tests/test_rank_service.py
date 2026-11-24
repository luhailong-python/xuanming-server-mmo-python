"""rank 服务端到端 gRPC、领域语义、奖励工作器状态机、SQL 事务重试与 fail-closed 鉴权测试。"""

from __future__ import annotations

import asyncio
import base64
import functools
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper

import grpc
import pymysql
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from xuanming.grpc_runtime import (
    GrpcClient,
    serve_grpc,
    service_digest,
    session_metadata,
    sign_service_call,
    verify_service_call,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.protocol import Protocol
from xuanming.rank import (
    ATTEMPT_DELIVERED,
    ATTEMPT_RECIPIENT_GONE,
    ATTEMPT_REJECTED,
    ATTEMPT_UNKNOWN,
    ERR_BOARD_NOT_FOUND,
    ERR_GENERATION_CLOSED,
    ERR_INVALID_PARAMETER,
    ERR_MAIL_RECIPIENT_DELETED,
    ERR_REQUEST_CONFLICT,
    ERR_SERVICE_UNAVAILABLE,
    ERR_SETTLEMENT_PENDING,
    EXPIRED_PROBE_DELAY_MS,
    MAX_ATTEMPTS,
    MIN_CLAIM_MS,
    MemoryRankStore,
    RANK_GEN_OPEN,
    RANK_GEN_SEALED,
    RANK_SCOPE_GLOBAL,
    RANK_SCOPE_GUILD,
    REWARD_ABANDONED,
    REWARD_DELIVERED,
    REWARD_PENDING,
    REWARD_SENDER_KIND,
    RankConfig,
    RankService,
    RewardRow,
    RewardWorker,
    SCHEMA_FILE,
    SEND_PERSONAL_MAIL_PATH,
    SUBMIT_INCREMENT,
    SUBMIT_SET,
    SUBMIT_SET_IF_HIGHER,
    SqlRankStore,
    checked_increment_int64,
    classify_mail_reply,
    expired_probe_ms,
    next_step,
    order_key,
    page_window_for,
    reward_mail_window_ms,
    send_cutoff_ms,
    send_deferred_to,
)
from xuanming.rank.model import DAY_MS, INT64_MAX, INT64_MIN
from xuanming.rank.store import default_monotonic_id_generator

MATCH_SECRET = b"match-rank-secret-0123456789abcdef"
ADMIN_SECRET = b"admin-rank-secret-0123456789abcdef"
MAIL_SECRET = b"rank-mail-secret-0123456789abcdef0"
GATE_SEED = bytes(range(32))
GATE_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(GATE_SEED)
GATE_PUBLIC_KEY = GATE_PRIVATE_KEY.public_key()
GATE_PUBLIC_HEX = GATE_PUBLIC_KEY.public_bytes(Encoding.Raw, PublicFormat.Raw).hex()

MINUTE_MS = 60 * 1000
BASE_NOW_MS = 1_700_000_000_000


class ManualClock:
    def __init__(self, ms: int = BASE_NOW_MS) -> None:
        self.ms = ms

    def __call__(self) -> int:
        return self.ms

    def sec(self) -> float:
        return self.ms / 1000.0


class FakeNameReader:
    def __init__(self, names: dict[int, str] | None = None, *, fail: Exception | None = None, delay_sec: float = 0.0):
        self.names = dict(names or {})
        self.fail = fail
        self.delay_sec = delay_sec
        self.calls: list[list[int]] = []

    async def batch_get_player_name(self, player_ids: list[int], *, timeout: float = 0.2) -> dict[int, str]:
        self.calls.append(list(player_ids))
        if self.delay_sec > 0:
            await asyncio.sleep(self.delay_sec)
        if self.fail is not None:
            raise self.fail
        return {pid: self.names[pid] for pid in player_ids if pid in self.names}


class ScriptedMailClient:
    def __init__(self, clock: ManualClock, handler) -> None:
        self.clock = clock
        self.handler = handler
        self.requests: list = []
        self.sent_at: list[int] = []

    async def send_personal_mail(self, request, *, timeout: float = 1.5):
        cloned = type(request)()
        cloned.CopyFrom(request)
        self.requests.append(cloned)
        self.sent_at.append(self.clock.ms)
        result = self.handler(len(self.requests), cloned)
        if isinstance(result, Exception):
            raise result
        return result


def make_session_bytes(
    protocol: Protocol,
    *,
    player_id: int = 1001,
    owner_revision: int = 1,
    account: str = "acc_1001",
    session_id: int = 10,
    gate_instance_id: str = "gate-1",
    session_incarnation: int = 1,
) -> bytes:
    details = protocol.message(
        "SessionDetails",
        session_id=session_id,
        player_id=player_id,
        gate_instance_id=gate_instance_id,
        session_incarnation=session_incarnation,
        authenticated_account=account,
        owner_revision=owner_revision,
    )
    return details.SerializeToString()


def make_gate_md(
    protocol: Protocol,
    method: str,
    *,
    player_id: int = 1001,
    owner_revision: int = 1,
    account: str = "acc_1001",
    signer: Ed25519PrivateKey = GATE_PRIVATE_KEY,
) -> tuple:
    subject = make_session_bytes(
        protocol,
        player_id=player_id,
        owner_revision=owner_revision,
        account=account,
    )
    return session_metadata(subject, method, signer)


@asynccontextmanager
async def running_rank_service(
    *,
    clock: ManualClock | None = None,
    names: FakeNameReader | None = None,
    item_exists=None,
    rank_limit: int = 10_000,
    store: MemoryRankStore | None = None,
):
    protocol = Protocol()
    clock = clock or ManualClock()
    names = FakeNameReader({i: f"P{i}" for i in range(1, 200)}) if names is None else names
    if store is None:
        store = MemoryRankStore(
            protocol,
            item_exists=(lambda item_id: item_id in {1, 2, 100}) if item_exists is None else item_exists,
            clock_ms=clock,
            rank_limit=rank_limit,
        )
    verifier = CallerVerifier([GATE_PUBLIC_KEY], allowed_callers=("gate",))
    service = RankService(
        protocol,
        store,
        producer_secrets={"match": MATCH_SECRET, "admin": ADMIN_SECRET},
        caller_verifier=verifier,
        name_reader=names,
        clock_ms=clock,
    )
    server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
    client = GrpcClient(f"127.0.0.1:{port}", timeout=3.0)
    try:
        yield protocol, store, client, clock, names
    finally:
        await client.close()
        await server.stop(1.0)


async def call_admin(protocol: Protocol, client: GrpcClient, method: str, req, caller: str, secret: bytes, now_ms: int):
    path = f"/rankpb.RankAdmin/{method}"
    sign_service_call(path, req, caller, now_ms, secret)
    raw = await client.call(path, req.SerializeToString())
    resp_type = {
        "OpenRankGeneration": "rankpb.OpenRankGenerationResponse",
        "SubmitRankScores": "rankpb.SubmitRankScoresResponse",
        "FreezeRankGeneration": "rankpb.FreezeRankGenerationResponse",
        "RemoveRankEntry": "rankpb.RankAdminResponse",
        "DeleteRankGeneration": "rankpb.RankAdminResponse",
    }[method]
    return protocol.parse(resp_type, raw)


async def call_client(protocol: Protocol, client: GrpcClient, method: str, req, metadata: tuple):
    path = f"/rankpb.ClientPlayerRank/{method}"
    raw = await client.call(path, req.SerializeToString(), metadata=metadata)
    resp_type = {
        "GetRankPage": "rankpb.GetRankPageResponse",
        "GetRankAround": "rankpb.GetRankAroundResponse",
    }[method]
    return protocol.parse(resp_type, raw)


# -- 1. Open / Submit（多分片、无缺口游标、重放/冲突、INCREMENT int64 溢出、SET_IF_HIGHER、score_version、升降序） --


@_async_test
async def test_open_submit_multishard_modes_versions_and_ordering():
    async with running_rank_service() as (protocol, store, client, clock, names):
        # 1. Open descending board (board_type=1, MATCH_1V1)
        open_req = protocol.message(
            "rankpb.OpenRankGenerationRequest",
            producer="match",
            source_epoch=1,
            ascending=False,
            request_id=101,
        )
        open_req.board.board_type = 1
        open_req.board.scope = RANK_SCOPE_GLOBAL
        open_req.board.scope_id = 0
        open_req.board.period = "2026-S1"
        open_req.board.generation = 1

        open_resp = await call_admin(protocol, client, "OpenRankGeneration", open_req, "match", MATCH_SECRET, clock.ms)
        assert open_resp.error_message.id == 0
        assert open_resp.board_id == 1
        assert not open_resp.duplicated

        # Idempotent replay
        replay_resp = await call_admin(protocol, client, "OpenRankGeneration", open_req, "match", MATCH_SECRET, clock.ms)
        assert replay_resp.error_message.id == 0
        assert replay_resp.board_id == 1
        assert replay_resp.duplicated

        # Conflict on different request_id / hash
        conflict_open = protocol.message("rankpb.OpenRankGenerationRequest")
        conflict_open.CopyFrom(open_req)
        conflict_open.request_id = 102
        conflict_resp = await call_admin(protocol, client, "OpenRankGeneration", conflict_open, "match", MATCH_SECRET, clock.ms)
        assert conflict_resp.error_message.id == ERR_REQUEST_CONFLICT

        # 2. Gapless cursor check: jumping 0 -> 2 on shard 0 rejected (21003)
        gap_req = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=1,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=2,
            request_id=201,
            mode=SUBMIT_SET,
        )
        e = gap_req.entries.add()
        e.entity_id, e.score, e.achieved_ms = 10, 500, 1000
        gap_resp = await call_admin(protocol, client, "SubmitRankScores", gap_req, "match", MATCH_SECRET, clock.ms)
        assert gap_resp.error_message.id == ERR_SETTLEMENT_PENDING

        # Invalid source_shard >= 1024 rejected (1005)
        bad_shard = protocol.message("rankpb.SubmitRankScoresRequest")
        bad_shard.CopyFrom(gap_req)
        bad_shard.source_shard = 1024
        bad_shard.source_cursor = 1
        bad_shard_resp = await call_admin(protocol, client, "SubmitRankScores", bad_shard, "match", MATCH_SECRET, clock.ms)
        assert bad_shard_resp.error_message.id == ERR_INVALID_PARAMETER

        # 3. Multi-shard independent cursors: shard 0 cursor 1 + shard 7 cursor 1
        s0_c1 = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=1,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=1,
            request_id=202,
            mode=SUBMIT_SET,
        )
        for eid, score, ts, ver in (
            (11, 100, 2000, 5),
            (12, 100, 1500, 1),  # same score as 11, earlier achieved_ms -> ahead of 11
            (13, 100, 1500, 1),  # tie score & achieved_ms with 12, larger entity_id -> behind 12
        ):
            item = s0_c1.entries.add()
            item.entity_id, item.score, item.achieved_ms, item.score_version = eid, score, ts, ver
        r1 = await call_admin(protocol, client, "SubmitRankScores", s0_c1, "match", MATCH_SECRET, clock.ms)
        assert r1.error_message.id == 0
        assert r1.committed_cursor == 1 and not r1.duplicated

        # Replay shard 0 cursor 1 -> duplicated=True
        r1_dup = await call_admin(protocol, client, "SubmitRankScores", s0_c1, "match", MATCH_SECRET, clock.ms)
        assert r1_dup.error_message.id == 0
        assert r1_dup.committed_cursor == 1 and r1_dup.duplicated

        # Replay shard 0 cursor 1 with altered payload -> conflict 21002
        s0_c1_bad = protocol.message("rankpb.SubmitRankScoresRequest")
        s0_c1_bad.CopyFrom(s0_c1)
        s0_c1_bad.entries[0].score = 999
        r1_conflict = await call_admin(protocol, client, "SubmitRankScores", s0_c1_bad, "match", MATCH_SECRET, clock.ms)
        assert r1_conflict.error_message.id == ERR_REQUEST_CONFLICT

        # Shard 7 cursor 1: score_version guard (stale version 3 <= 5 for entity 11 skipped; newer version 6 updates!)
        s7_c1 = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=1,
            generation=1,
            source_epoch=1,
            source_shard=7,
            source_cursor=1,
            request_id=203,
            mode=SUBMIT_SET,
        )
        e_stale = s7_c1.entries.add()
        e_stale.entity_id, e_stale.score, e_stale.achieved_ms, e_stale.score_version = 11, 9999, 3000, 3
        e_new = s7_c1.entries.add()
        e_new.entity_id, e_new.score, e_new.achieved_ms, e_new.score_version = 14, INT64_MAX, 1000, 1
        e_min = s7_c1.entries.add()
        e_min.entity_id, e_min.score, e_min.achieved_ms, e_min.score_version = 15, INT64_MIN, 1000, 1
        r2 = await call_admin(protocol, client, "SubmitRankScores", s7_c1, "match", MATCH_SECRET, clock.ms)
        assert r2.error_message.id == 0 and r2.committed_cursor == 1

        # Verify entity 11 stayed at 100 (version 5), and ordering is 14(Max) > 12(100,1500) > 13(100,1500) > 11(100,2000) > 15(Min)
        page_req = protocol.message("rankpb.GetRankPageRequest", page=1, page_size=20)
        page_req.board.board_type = 1
        page_req.board.scope = RANK_SCOPE_GLOBAL
        page_resp = await call_client(
            protocol,
            client,
            "GetRankPage",
            page_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", player_id=11),
        )
        assert page_resp.error_message.id == 0
        assert [(e.rank, e.entity_id, e.score, e.name) for e in page_resp.entries] == [
            (1, 14, INT64_MAX, "P14"),
            (2, 12, 100, "P12"),
            (3, 13, 100, "P13"),
            (4, 11, 100, "P11"),
            (5, 15, INT64_MIN, "P15"),
        ]
        assert page_resp.self.rank == 4 and page_resp.self.name == "P11"

        # 4. INCREMENT overflow check: entity 14 is at INT64_MAX, +1 -> 1005; entity 15 is at INT64_MIN, -1 -> 1005
        inc_over = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=1,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=2,
            request_id=204,
            mode=SUBMIT_INCREMENT,
        )
        e = inc_over.entries.add()
        e.entity_id, e.score, e.achieved_ms = 14, 1, 2000
        resp_over = await call_admin(protocol, client, "SubmitRankScores", inc_over, "match", MATCH_SECRET, clock.ms)
        assert resp_over.error_message.id == ERR_INVALID_PARAMETER

        inc_under = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=1,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=2,
            request_id=205,
            mode=SUBMIT_INCREMENT,
        )
        e = inc_under.entries.add()
        e.entity_id, e.score, e.achieved_ms = 15, -1, 2000
        resp_under = await call_admin(protocol, client, "SubmitRankScores", inc_under, "match", MATCH_SECRET, clock.ms)
        assert resp_under.error_message.id == ERR_INVALID_PARAMETER

        # Valid INCREMENT on entity 11 (+50 -> 150)
        inc_ok = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=1,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=2,
            request_id=206,
            mode=SUBMIT_INCREMENT,
        )
        e = inc_ok.entries.add()
        e.entity_id, e.score, e.achieved_ms, e.score_version = 11, 50, 4000, 6
        resp_inc = await call_admin(protocol, client, "SubmitRankScores", inc_ok, "match", MATCH_SECRET, clock.ms)
        assert resp_inc.error_message.id == 0 and resp_inc.committed_cursor == 2

        # 5. SET_IF_HIGHER on descending board: lower score ignored, higher score accepted
        sih = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=1,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=3,
            request_id=207,
            mode=SUBMIT_SET_IF_HIGHER,
        )
        e1 = sih.entries.add()
        e1.entity_id, e1.score, e1.achieved_ms = 11, 120, 5000  # <= 150, ignored
        e2 = sih.entries.add()
        e2.entity_id, e2.score, e2.achieved_ms = 12, 200, 5000  # > 100, accepted
        resp_sih = await call_admin(protocol, client, "SubmitRankScores", sih, "match", MATCH_SECRET, clock.ms)
        assert resp_sih.error_message.id == 0

        page2 = await call_client(
            protocol,
            client,
            "GetRankPage",
            page_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", player_id=12),
        )
        scores_by_id = {e.entity_id: e.score for e in page2.entries}
        assert scores_by_id[11] == 150
        assert scores_by_id[12] == 200

        # 6. Ascending board: SET_IF_HIGHER accepts STRICTLY LOWER scores (faster time wins!)
        asc_open = protocol.message(
            "rankpb.OpenRankGenerationRequest",
            producer="match",
            source_epoch=1,
            ascending=True,
            request_id=301,
        )
        asc_open.board.board_type = 2
        asc_open.board.scope = RANK_SCOPE_GLOBAL
        asc_open.board.period = "speedrun"
        asc_open.board.generation = 1
        asc_resp = await call_admin(protocol, client, "OpenRankGeneration", asc_open, "match", MATCH_SECRET, clock.ms)
        assert asc_resp.error_message.id == 0
        asc_id = asc_resp.board_id

        asc_s1 = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=asc_id,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=1,
            request_id=302,
            mode=SUBMIT_SET,
        )
        for eid, score, ts in ((21, 500, 1000), (22, 300, 1000), (23, INT64_MIN, 1000), (24, INT64_MAX, 1000)):
            item = asc_s1.entries.add()
            item.entity_id, item.score, item.achieved_ms = eid, score, ts
        assert (await call_admin(protocol, client, "SubmitRankScores", asc_s1, "match", MATCH_SECRET, clock.ms)).error_message.id == 0

        asc_s2 = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=asc_id,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=2,
            request_id=303,
            mode=SUBMIT_SET_IF_HIGHER,
        )
        e_worse = asc_s2.entries.add()
        e_worse.entity_id, e_worse.score, e_worse.achieved_ms = 22, 400, 2000  # worse than 300 -> skip
        e_better = asc_s2.entries.add()
        e_better.entity_id, e_better.score, e_better.achieved_ms = 21, 200, 2000  # better than 500 -> accept
        assert (await call_admin(protocol, client, "SubmitRankScores", asc_s2, "match", MATCH_SECRET, clock.ms)).error_message.id == 0

        asc_page_req = protocol.message("rankpb.GetRankPageRequest", page=1, page_size=10)
        asc_page_req.board.board_type = 2
        asc_page_req.board.scope = RANK_SCOPE_GLOBAL
        asc_page = await call_client(
            protocol,
            client,
            "GetRankPage",
            asc_page_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", player_id=21),
        )
        assert [(e.rank, e.entity_id, e.score) for e in asc_page.entries] == [
            (1, 23, INT64_MIN),
            (2, 21, 200),
            (3, 22, 300),
            (4, 24, INT64_MAX),
        ]


# -- 2. Remove 墓碑 + Delete 隐藏整代 ------------------------------------------------------------------


@_async_test
async def test_remove_tombstone_and_delete_generation():
    async with running_rank_service() as (protocol, store, client, clock, _names):
        open_req = protocol.message(
            "rankpb.OpenRankGenerationRequest",
            producer="match",
            source_epoch=1,
            ascending=False,
            request_id=1,
        )
        open_req.board.board_type = 1
        open_req.board.scope = RANK_SCOPE_GLOBAL
        open_req.board.period = "p1"
        open_req.board.generation = 1
        board_id = (await call_admin(protocol, client, "OpenRankGeneration", open_req, "match", MATCH_SECRET, clock.ms)).board_id

        s1 = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=board_id,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=1,
            request_id=2,
            mode=SUBMIT_SET,
        )
        for eid, sc in ((1, 100), (2, 200)):
            it = s1.entries.add()
            it.entity_id, it.score, it.achieved_ms = eid, sc, 1000
        assert (await call_admin(protocol, client, "SubmitRankScores", s1, "match", MATCH_SECRET, clock.ms)).error_message.id == 0

        # Remove entity 2
        rem = protocol.message(
            "rankpb.RemoveRankEntryRequest",
            board_id=board_id,
            generation=1,
            entity_id=2,
            request_id=10,
        )
        assert (await call_admin(protocol, client, "RemoveRankEntry", rem, "match", MATCH_SECRET, clock.ms)).error_message.id == 0
        # Idempotent replay
        assert (await call_admin(protocol, client, "RemoveRankEntry", rem, "match", MATCH_SECRET, clock.ms)).error_message.id == 0
        # Reusing same request_id for DeleteRankGeneration must conflict (21002)
        del_same_id = protocol.message(
            "rankpb.RemoveRankEntryRequest",
            board_id=board_id,
            generation=1,
            entity_id=0,
            request_id=10,
        )
        assert (
            await call_admin(protocol, client, "DeleteRankGeneration", del_same_id, "match", MATCH_SECRET, clock.ms)
        ).error_message.id == ERR_REQUEST_CONFLICT

        # Late submit containing removed entity 2 + new entity 3: cursor advances, 2 stays removed, 3 inserted
        s2 = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=board_id,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=2,
            request_id=3,
            mode=SUBMIT_SET,
        )
        for eid, sc in ((2, 9999), (3, 300)):
            it = s2.entries.add()
            it.entity_id, it.score, it.achieved_ms = eid, sc, 2000
        r2 = await call_admin(protocol, client, "SubmitRankScores", s2, "match", MATCH_SECRET, clock.ms)
        assert r2.error_message.id == 0 and r2.committed_cursor == 2

        page_req = protocol.message("rankpb.GetRankPageRequest", page=1, page_size=10)
        page_req.board.board_type = 1
        page_req.board.scope = RANK_SCOPE_GLOBAL
        page = await call_client(
            protocol,
            client,
            "GetRankPage",
            page_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", player_id=2),
        )
        assert page.total_count == 2
        assert [e.entity_id for e in page.entries] == [3, 1]
        assert not page.HasField("self")

        # Freeze first, then DeleteRankGeneration: board hidden from public reads, rewards & snapshots preserved
        freeze = protocol.message(
            "rankpb.FreezeRankGenerationRequest",
            board_id=board_id,
            generation=1,
            source_epoch=1,
            expected_source_cursor=2,
            request_id=20,
            top_n=10,
        )
        tier = freeze.rewards.add()
        tier.first_rank, tier.last_rank = 1, 2
        c = tier.attachments.currencies.add()
        c.currency_type, c.amount = 1, 50
        f_resp = await call_admin(protocol, client, "FreezeRankGeneration", freeze, "match", MATCH_SECRET, clock.ms)
        assert f_resp.error_message.id == 0 and f_resp.winner_count == 2
        assert len(store.rewards) == 2

        # Remove on sealed board rejected (21001)
        rem_sealed = protocol.message(
            "rankpb.RemoveRankEntryRequest",
            board_id=board_id,
            generation=1,
            entity_id=1,
            request_id=21,
        )
        assert (
            await call_admin(protocol, client, "RemoveRankEntry", rem_sealed, "match", MATCH_SECRET, clock.ms)
        ).error_message.id == ERR_GENERATION_CLOSED

        # Delete hides generation; rewards & snapshots untouched
        del_req = protocol.message(
            "rankpb.RemoveRankEntryRequest",
            board_id=board_id,
            generation=1,
            entity_id=0,
            request_id=22,
        )
        assert (await call_admin(protocol, client, "DeleteRankGeneration", del_req, "match", MATCH_SECRET, clock.ms)).error_message.id == 0
        assert len(store.rewards) == 2
        assert len(store.snapshots) == 2

        hidden_page = await call_client(
            protocol,
            client,
            "GetRankPage",
            page_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", player_id=1),
        )
        assert hidden_page.error_message.id == ERR_BOARD_NOT_FOUND


# -- 3. Page / Around 分页、beyond_rank_limit 与 BatchGetPlayerName -----------------------------------


@_async_test
async def test_page_around_beyond_rank_limit_and_name_enrichment():
    names = FakeNameReader({i: f"Hero-{i}" for i in range(1, 30)})
    async with running_rank_service(names=names, rank_limit=5) as (protocol, store, client, clock, _):
        open_req = protocol.message(
            "rankpb.OpenRankGenerationRequest",
            producer="match",
            source_epoch=1,
            ascending=False,
            request_id=1,
        )
        open_req.board.board_type = 1
        open_req.board.scope = RANK_SCOPE_GLOBAL
        open_req.board.period = "p1"
        open_req.board.generation = 1
        board_id = (await call_admin(protocol, client, "OpenRankGeneration", open_req, "match", MATCH_SECRET, clock.ms)).board_id

        # Insert 12 players: entity 1 has highest score 1200, entity 12 has lowest score 100
        sub = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=board_id,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=1,
            request_id=2,
            mode=SUBMIT_SET,
        )
        for idx in range(1, 13):
            it = sub.entries.add()
            it.entity_id, it.score, it.achieved_ms = idx, (13 - idx) * 100, 1000
        assert (await call_admin(protocol, client, "SubmitRankScores", sub, "match", MATCH_SECRET, clock.ms)).error_message.id == 0

        # Page with rank_limit=5, page_size=3 -> 2 pages (ranks 1..3, 4..5); total_count=12; deep page clamped to 2
        page_req = protocol.message("rankpb.GetRankPageRequest", page=99, page_size=3)
        page_req.board.board_type = 1
        page_req.board.scope = RANK_SCOPE_GLOBAL
        resp = await call_client(
            protocol,
            client,
            "GetRankPage",
            page_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", player_id=8),
        )
        assert resp.error_message.id == 0
        assert resp.total_count == 12
        assert resp.rank_limit == 5
        assert resp.page == 2 and resp.page_count == 2
        assert [(e.rank, e.entity_id, e.name, e.beyond_rank_limit) for e in resp.entries] == [
            (4, 4, "Hero-4", False),
            (5, 5, "Hero-5", False),
        ]
        # Player 8 is rank 8 > rank_limit 5 -> beyond_rank_limit=True, rank=0, name still enriched!
        assert resp.self.entity_id == 8
        assert resp.self.rank == 0
        assert resp.self.beyond_rank_limit is True
        assert resp.self.name == "Hero-8"

        # Around for player 5 (at rank_limit boundary) with radius=2 -> ranks 3,4,5 exact; 6,7 beyond_rank_limit
        around_req = protocol.message("rankpb.GetRankAroundRequest", radius=2)
        around_req.board.board_type = 1
        around_req.board.scope = RANK_SCOPE_GLOBAL
        around_5 = await call_client(
            protocol,
            client,
            "GetRankAround",
            around_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankAround", player_id=5),
        )
        assert around_5.error_message.id == 0
        assert around_5.rank_limit == 5
        assert [(e.rank, e.entity_id, e.beyond_rank_limit, e.name) for e in around_5.entries] == [
            (3, 3, False, "Hero-3"),
            (4, 4, False, "Hero-4"),
            (5, 5, False, "Hero-5"),
            (0, 6, True, "Hero-6"),
            (0, 7, True, "Hero-7"),
        ]
        assert around_5.self.rank == 5 and not around_5.self.beyond_rank_limit

        # Around for player 10 (ahead=9 >= count_cap=5+2=7 -> capped): self and all neighbors marked beyond_rank_limit
        around_10 = await call_client(
            protocol,
            client,
            "GetRankAround",
            around_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankAround", player_id=10),
        )
        assert [(e.rank, e.entity_id, e.beyond_rank_limit) for e in around_10.entries] == [
            (0, 8, True),
            (0, 9, True),
            (0, 10, True),
            (0, 11, True),
            (0, 12, True),
        ]
        assert around_10.self.rank == 0 and around_10.self.beyond_rank_limit

        # Around for player not on board -> empty entries, no self
        around_missing = await call_client(
            protocol,
            client,
            "GetRankAround",
            around_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankAround", player_id=999),
        )
        assert around_missing.error_message.id == 0
        assert len(around_missing.entries) == 0
        assert not around_missing.HasField("self")

        # Name reader failure -> fail-closed 1003
        names.fail = RuntimeError("data_service down")
        resp_err = await call_client(
            protocol,
            client,
            "GetRankPage",
            page_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", player_id=1),
        )
        assert resp_err.error_message.id == ERR_SERVICE_UNAVAILABLE

        # Name reader exceeds 200ms timeout -> fail-closed 1003
        names.fail = None
        names.delay_sec = 0.35
        resp_timeout = await call_client(
            protocol,
            client,
            "GetRankPage",
            page_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", player_id=1),
        )
        assert resp_timeout.error_message.id == ERR_SERVICE_UNAVAILABLE


# -- 4. Freeze 校验 + RewardWorker 状态机 -------------------------------------------------------------


@_async_test
async def test_freeze_validation_and_reward_worker_state_machine():
    clock = ManualClock(BASE_NOW_MS)
    async with running_rank_service(clock=clock) as (protocol, store, client, _, _):
        open_req = protocol.message(
            "rankpb.OpenRankGenerationRequest",
            producer="match",
            source_epoch=1,
            ascending=False,
            request_id=1,
        )
        open_req.board.board_type = 1
        open_req.board.scope = RANK_SCOPE_GLOBAL
        open_req.board.period = "season-1"
        open_req.board.generation = 1
        board_id = (await call_admin(protocol, client, "OpenRankGeneration", open_req, "match", MATCH_SECRET, clock.ms)).board_id

        # Submit across 2 shards: shard 0 (cursor 1,2) + shard 3 (cursor 1) -> SUM = 3
        for shard, cur, req_id, eid, sc in (
            (0, 1, 11, 101, 500),
            (0, 2, 12, 102, 400),
            (3, 1, 13, 103, 300),
        ):
            sub = protocol.message(
                "rankpb.SubmitRankScoresRequest",
                board_id=board_id,
                generation=1,
                source_epoch=1,
                source_shard=shard,
                source_cursor=cur,
                request_id=req_id,
                mode=SUBMIT_SET,
            )
            it = sub.entries.add()
            it.entity_id, it.score, it.achieved_ms = eid, sc, 1000
            assert (await call_admin(protocol, client, "SubmitRankScores", sub, "match", MATCH_SECRET, clock.ms)).error_message.id == 0

        # Wrong expected_source_cursor -> 21003
        bad_cursor = protocol.message(
            "rankpb.FreezeRankGenerationRequest",
            board_id=board_id,
            generation=1,
            source_epoch=1,
            expected_source_cursor=2,
            request_id=50,
            top_n=10,
        )
        assert (
            await call_admin(protocol, client, "FreezeRankGeneration", bad_cursor, "match", MATCH_SECRET, clock.ms)
        ).error_message.id == ERR_SETTLEMENT_PENDING

        # Overlapping reward tiers -> 1005
        overlap = protocol.message(
            "rankpb.FreezeRankGenerationRequest",
            board_id=board_id,
            generation=1,
            source_epoch=1,
            expected_source_cursor=3,
            request_id=51,
            top_n=10,
        )
        for lo, hi in ((1, 2), (2, 3)):
            t = overlap.rewards.add()
            t.first_rank, t.last_rank = lo, hi
            c = t.attachments.currencies.add()
            c.currency_type, c.amount = 1, 10
        assert (
            await call_admin(protocol, client, "FreezeRankGeneration", overlap, "match", MATCH_SECRET, clock.ms)
        ).error_message.id == ERR_INVALID_PARAMETER

        # Unknown item config_id -> 1005
        bad_item = protocol.message(
            "rankpb.FreezeRankGenerationRequest",
            board_id=board_id,
            generation=1,
            source_epoch=1,
            expected_source_cursor=3,
            request_id=52,
            top_n=10,
        )
        t = bad_item.rewards.add()
        t.first_rank, t.last_rank = 1, 1
        it = t.attachments.items.add()
        it.config_id, it.count = 999999, 1
        assert (
            await call_admin(protocol, client, "FreezeRankGeneration", bad_item, "match", MATCH_SECRET, clock.ms)
        ).error_message.id == ERR_INVALID_PARAMETER

        # Valid freeze with 2 non-overlapping tiers (ranks 1..1 and 2..3)
        freeze = protocol.message(
            "rankpb.FreezeRankGenerationRequest",
            board_id=board_id,
            generation=1,
            source_epoch=1,
            expected_source_cursor=3,
            request_id=53,
            top_n=10,
        )
        t1 = freeze.rewards.add()
        t1.first_rank, t1.last_rank = 1, 1
        c1 = t1.attachments.currencies.add()
        c1.currency_type, c1.amount = 1, 100
        i1 = t1.attachments.items.add()
        i1.config_id, i1.count = 100, 2

        t2 = freeze.rewards.add()
        t2.first_rank, t2.last_rank = 2, 3
        c2 = t2.attachments.currencies.add()
        c2.currency_type, c2.amount = 0, 50

        f_resp = await call_admin(protocol, client, "FreezeRankGeneration", freeze, "match", MATCH_SECRET, clock.ms)
        assert f_resp.error_message.id == 0
        assert f_resp.winner_count == 3 and not f_resp.duplicated

        # Freeze replay -> duplicated=True
        f_dup = await call_admin(protocol, client, "FreezeRankGeneration", freeze, "match", MATCH_SECRET, clock.ms)
        assert f_dup.error_message.id == 0 and f_dup.settlement_id == f_resp.settlement_id and f_dup.duplicated

        # Verify 3 reward rows with frozen expire_ms = sealed_ms + 30 days
        assert len(store.rewards) == 3
        expected_expire = BASE_NOW_MS + 30 * DAY_MS
        for row in store.rewards.values():
            assert row.status == REWARD_PENDING
            assert row.expire_ms == expected_expire

        # Drive RewardWorker across all 3 rows with distinct outcomes:
        # reward 1 (player 101): delivered cleanly + verify HMAC signature
        # reward 2 (player 102): 17011 recipient deleted -> immediate Abandoned
        # reward 3 (player 103): unknown error -> stays Pending with backoff, then delivered on retry
        r3_attempts = {"n": 0}

        def _mail_handler(_call_num, req):
            assert verify_service_call(SEND_PERSONAL_MAIL_PATH, req, "rank", clock.ms, MAIL_SECRET)
            assert req.sender_kind == REWARD_SENDER_KIND
            if req.recipient_player_id == 101:
                return protocol.message("mailpb.SendMailResponse", mail_id=7001)
            if req.recipient_player_id == 102:
                resp = protocol.message("mailpb.SendMailResponse")
                resp.error_message.id = ERR_MAIL_RECIPIENT_DELETED
                return resp
            r3_attempts["n"] += 1
            if r3_attempts["n"] == 1:
                return RuntimeError("transport timeout")
            if r3_attempts["n"] == 2:
                resp = protocol.message("mailpb.SendMailResponse")
                resp.error_message.id = ERR_SERVICE_UNAVAILABLE
                return resp
            return protocol.message("mailpb.SendMailResponse", mail_id=7003, duplicated=True)

        mail = ScriptedMailClient(clock, _mail_handler)
        worker = RewardWorker(protocol, store, mail, mail_secret=MAIL_SECRET, clock_ms=clock)

        await worker.tick()
        by_player = {row.player_id: row for row in store.rewards.values()}
        assert by_player[101].status == REWARD_DELIVERED and by_player[101].mail_id == 7001
        assert by_player[102].status == REWARD_ABANDONED and by_player[102].mail_id == 0
        assert by_player[103].status == REWARD_PENDING and by_player[103].next_attempt_ms == BASE_NOW_MS + 1000

        # Retry 2 (fault 1003 -> unknown -> backoff 2s)
        clock.ms = by_player[103].next_attempt_ms
        await worker.tick()
        assert by_player[103].status == REWARD_PENDING and by_player[103].next_attempt_ms == clock.ms + 2000

        # Retry 3 -> delivered via duplicate replay
        clock.ms = by_player[103].next_attempt_ms
        await worker.tick()
        assert by_player[103].status == REWARD_DELIVERED and by_player[103].mail_id == 7003


@_async_test
async def test_reward_worker_cutoff_defer_and_post_expiry_probe():
    protocol = Protocol()
    clock = ManualClock(BASE_NOW_MS)
    store = MemoryRankStore(protocol, clock_ms=clock)
    atts = protocol.message("mailpb.MailAttachments")
    c = atts.currencies.add()
    c.currency_type, c.amount = 1, 20
    payload = atts.SerializeToString(deterministic=True)

    cutoff = BASE_NOW_MS + 60 * MINUTE_MS
    expire = cutoff + MIN_CLAIM_MS
    probe = expired_probe_ms(expire)

    # Row 1: discovered past cutoff -> deferred to probe (0 attempts added), probe biz-reject -> Abandoned
    store.rewards[1] = RewardRow(
        reward_id=1,
        settlement_id=10,
        player_id=501,
        rank=1,
        attachments=payload,
        status=REWARD_PENDING,
        next_attempt_ms=BASE_NOW_MS,
        attempts=3,
        created_ms=BASE_NOW_MS,
        updated_ms=BASE_NOW_MS,
        expire_ms=expire,
    )
    # Row 2: unknown outcome past expiry & MAX_ATTEMPTS -> stays pending forever until mail replies original mail_id
    store.rewards[2] = RewardRow(
        reward_id=2,
        settlement_id=10,
        player_id=502,
        rank=2,
        attachments=payload,
        status=REWARD_PENDING,
        next_attempt_ms=probe,
        attempts=MAX_ATTEMPTS + 10,
        created_ms=BASE_NOW_MS,
        updated_ms=BASE_NOW_MS,
        expire_ms=expire,
    )
    # Row 3: corrupt expire_ms == 0 -> abandoned without sending
    store.rewards[3] = RewardRow(
        reward_id=3,
        settlement_id=10,
        player_id=503,
        rank=3,
        attachments=payload,
        status=REWARD_PENDING,
        next_attempt_ms=BASE_NOW_MS,
        created_ms=BASE_NOW_MS,
        updated_ms=BASE_NOW_MS,
        expire_ms=0,
    )

    row2_calls = {"n": 0}

    def _reply(_n, req):
        if req.request_id == 1:
            resp = protocol.message("mailpb.SendMailResponse")
            resp.error_message.id = ERR_INVALID_PARAMETER  # expired, confirmed not sent
            return resp
        row2_calls["n"] += 1
        if row2_calls["n"] == 1:
            resp = protocol.message("mailpb.SendMailResponse")
            resp.error_message.id = 999999  # unknown code -> retry forever
            return resp
        return protocol.message("mailpb.SendMailResponse", mail_id=8802, duplicated=True)

    mail = ScriptedMailClient(clock, _reply)
    worker = RewardWorker(protocol, store, mail, mail_secret=MAIL_SECRET, clock_ms=clock)

    # Move clock to cutoff + 1ms: row 1 deferred without sending, row 3 abandoned without sending
    clock.ms = cutoff + 1
    await worker.tick()
    assert len(mail.requests) == 0
    assert store.rewards[1].status == REWARD_PENDING
    assert store.rewards[1].next_attempt_ms == probe
    assert store.rewards[1].attempts == 3
    assert store.rewards[3].status == REWARD_ABANDONED

    # At expire (still < probe): nothing sent
    clock.ms = expire
    await worker.tick()
    assert len(mail.requests) == 0

    # At probe: row 1 probed -> biz reject -> Abandoned; row 2 unknown -> stays Pending
    clock.ms = probe
    await worker.tick()
    assert len(mail.requests) == 2
    assert store.rewards[1].status == REWARD_ABANDONED and store.rewards[1].attempts == 4
    assert store.rewards[2].status == REWARD_PENDING

    # Next retry for row 2 -> mail replies original mail_id -> Delivered
    clock.ms = store.rewards[2].next_attempt_ms
    await worker.tick()
    assert store.rewards[2].status == REWARD_DELIVERED and store.rewards[2].mail_id == 8802


# -- 5. Fail-closed Gate Ed25519 与 HMAC 鉴权 ----------------------------------------------------------


@_async_test
async def test_fail_closed_gate_and_hmac_auth():
    async with running_rank_service() as (protocol, _store, client, clock, _names):
        page_req = protocol.message("rankpb.GetRankPageRequest", page=1, page_size=10)
        page_req.board.board_type = 1
        page_req.board.scope = RANK_SCOPE_GLOBAL

        # ClientPlayerRank without session -> PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await call_client(protocol, client, "GetRankPage", page_req, ())
        assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # ClientPlayerRank with invalid signature (wrong key) -> UNAUTHENTICATED
        wrong_key = Ed25519PrivateKey.generate()
        bad_md = make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", signer=wrong_key)
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await call_client(protocol, client, "GetRankPage", page_req, bad_md)
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # ClientPlayerRank with signature bound to different method -> UNAUTHENTICATED
        wrong_method_md = make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankAround")
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await call_client(protocol, client, "GetRankPage", page_req, wrong_method_md)
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # ClientPlayerRank with missing player_id / owner_revision / account -> PERMISSION_DENIED
        for kwargs in (
            {"player_id": 0, "owner_revision": 1, "account": "acc"},
            {"player_id": 1001, "owner_revision": 0, "account": "acc"},
            {"player_id": 1001, "owner_revision": 1, "account": ""},
        ):
            incomplete_md = make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage", **kwargs)
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await call_client(protocol, client, "GetRankPage", page_req, incomplete_md)
            assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # ClientPlayerRank non-GLOBAL scope -> gRPC OK with in-band tip 1005
        guild_req = protocol.message("rankpb.GetRankPageRequest", page=1, page_size=10)
        guild_req.board.board_type = 1
        guild_req.board.scope = RANK_SCOPE_GUILD
        guild_resp = await call_client(
            protocol,
            client,
            "GetRankPage",
            guild_req,
            make_gate_md(protocol, "/rankpb.ClientPlayerRank/GetRankPage"),
        )
        assert guild_resp.error_message.id == ERR_INVALID_PARAMETER

        # RankAdmin with x-session-detail-bin present -> PERMISSION_DENIED
        open_req = protocol.message(
            "rankpb.OpenRankGenerationRequest",
            producer="match",
            source_epoch=1,
            request_id=1,
        )
        open_req.board.board_type = 1
        open_req.board.scope = RANK_SCOPE_GLOBAL
        open_req.board.period = "p1"
        open_req.board.generation = 1
        sign_service_call("/rankpb.RankAdmin/OpenRankGeneration", open_req, "match", clock.ms, MATCH_SECRET)
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await client.call(
                "/rankpb.RankAdmin/OpenRankGeneration",
                open_req.SerializeToString(),
                metadata=(("x-session-detail-bin", base64.b64encode(b"anything")),),
            )
        assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # RankAdmin with bad HMAC -> UNAUTHENTICATED
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await call_admin(protocol, client, "OpenRankGeneration", open_req, "match", ADMIN_SECRET, clock.ms)
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # RankAdmin with unknown caller -> UNAUTHENTICATED
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await call_admin(protocol, client, "OpenRankGeneration", open_req, "rogue", MATCH_SECRET, clock.ms)
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # Valid Open by match, then Submit by admin (valid HMAC, wrong board producer) -> gRPC OK + tip 1005
        opened = await call_admin(protocol, client, "OpenRankGeneration", open_req, "match", MATCH_SECRET, clock.ms)
        assert opened.error_message.id == 0
        sub = protocol.message(
            "rankpb.SubmitRankScoresRequest",
            board_id=opened.board_id,
            generation=1,
            source_epoch=1,
            source_shard=0,
            source_cursor=1,
            request_id=2,
            mode=SUBMIT_SET,
        )
        it = sub.entries.add()
        it.entity_id, it.score, it.achieved_ms = 1, 10, 1000
        denied = await call_admin(protocol, client, "SubmitRankScores", sub, "admin", ADMIN_SECRET, clock.ms)
        assert denied.error_message.id == ERR_INVALID_PARAMETER


# -- 6. 纯函数、配置校验、DDL 核对与 SqlRankStore 事务重试验证 -------------------------------------------


def test_pure_domain_helpers_and_policy():
    # order_key bijection across full int64
    assert order_key(False, INT64_MIN) == INT64_MAX
    assert order_key(False, INT64_MAX) == INT64_MIN
    assert order_key(True, INT64_MIN) == INT64_MIN
    assert checked_increment_int64(INT64_MAX - 5, 5) == INT64_MAX
    with pytest.raises(Exception):
        checked_increment_int64(INT64_MAX, 1)

    # page_window_for
    w = page_window_for(1_000_000, 50_000, 30, 100)
    assert (w.page, w.pages, w.offset, w.rows) == (4, 4, 90, 10)

    # reward_mail_window_ms bounds
    assert reward_mail_window_ms(2) == 2 * DAY_MS
    assert reward_mail_window_ms(30) == 30 * DAY_MS
    assert reward_mail_window_ms(365) == 30 * DAY_MS
    for bad in (0, 1, 366):
        with pytest.raises(ValueError):
            reward_mail_window_ms(bad)

    # classify_mail_reply & next_step
    protocol = Protocol()
    assert classify_mail_reply(None, RuntimeError("net")) == ATTEMPT_UNKNOWN
    assert classify_mail_reply(protocol.message("mailpb.SendMailResponse"), None) == ATTEMPT_UNKNOWN
    assert classify_mail_reply(protocol.message("mailpb.SendMailResponse", mail_id=5), None) == ATTEMPT_DELIVERED
    box_full = protocol.message("mailpb.SendMailResponse")
    box_full.error_message.id = 17003
    assert classify_mail_reply(box_full, None) == ATTEMPT_REJECTED
    gone = protocol.message("mailpb.SendMailResponse")
    gone.error_message.id = ERR_MAIL_RECIPIENT_DELETED
    assert classify_mail_reply(gone, None) == ATTEMPT_RECIPIENT_GONE

    expire = BASE_NOW_MS + 30 * DAY_MS
    cutoff, probe = send_cutoff_ms(expire), expired_probe_ms(expire)
    assert send_deferred_to(cutoff, expire) == (probe, False)
    assert send_deferred_to(cutoff + 1, expire) == (probe, True)
    assert send_deferred_to(probe, expire) == (probe, False)
    assert next_step(ATTEMPT_REJECTED, MAX_ATTEMPTS - 1, BASE_NOW_MS, expire) == (REWARD_ABANDONED, BASE_NOW_MS)


def test_schema_sql_and_rank_config_validation():
    sql_text = SCHEMA_FILE.read_text("utf-8")
    for table in (
        "rank_board",
        "rank_board_shard",
        "rank_score",
        "rank_input_receipt",
        "rank_settlement",
        "rank_snapshot",
        "rank_mail_reward",
        "rank_mutation",
        "rank_removed_entry",
    ):
        assert f"CREATE TABLE IF NOT EXISTS `{table}`" in sql_text
    assert "`rank` bigint unsigned NOT NULL DEFAULT 0" in sql_text
    assert "`period` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin" in sql_text
    assert "INDEX `idx_rank_score_0`" in sql_text
    assert "INDEX `idx_rank_mail_reward_0`" in sql_text
    assert "UNIQUE KEY `uk_rank_board`" in sql_text

    env = {
        "RANK_PRODUCER_SECRET_MATCH": MATCH_SECRET.decode(),
        "RANK_PRODUCER_SECRET_ADMIN": ADMIN_SECRET.decode(),
        "MAIL_PRODUCER_SECRET_RANK": MAIL_SECRET.decode(),
    }
    settings = SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="stable",
        data_service="127.0.0.1:9000",
        etcd="127.0.0.1:2379",
        rank_node={
            "node_id": 110,
            "advertise_host": "127.0.0.1",
            "mail_service": "127.0.0.1:50900",
        },
    )
    cfg = RankConfig.from_settings(settings, env)
    assert cfg.grpc_port == 51100 and cfg.lease_ttl_sec == 60

    # Forbidden secret field in TOML rejected
    bad_settings = SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="stable",
        data_service="127.0.0.1:9000",
        etcd="127.0.0.1:2379",
        rank_node={"node_id": 110, "advertise_host": "127.0.0.1", "mail_service": "127.0.0.1:50900", "mail_secret": "x"},
    )
    with pytest.raises(ValueError):
        RankConfig.from_settings(bad_settings, env)


@_async_test
async def test_sql_store_tx_isolation_retry_and_unhex_bindings():
    """用轻量假连接池验证 SqlRankStore 写事务 1213/1062 重试、读事务 REPEATABLE READ 及 UNHEX/反引号 SQL。"""
    protocol = Protocol()
    executed: list[str] = []

    class FakeCursor:
        def __init__(self, state):
            self._state = state
            self._rows = []
            self.rowcount = 1

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, sql: str, args=None):
            executed.append(sql)
            # 模拟第一次写事务遇到 1213 死锁
            if self._state["fail_1213"] and "FOR UPDATE" in sql:
                self._state["fail_1213"] = False
                raise pymysql.OperationalError(1213, "Deadlock found")
            if "FROM `rank_board`" in sql:
                # 16 列
                self._rows = [(1, 1, 1, 0, "p1", 1, "match", 1, 0, RANK_GEN_OPEN, 0, 1, "h", 100, 0, 0)]
            elif "FROM `rank_board_shard`" in sql and "ORDER BY" in sql:
                self._rows = [(0, 1, 1)]
            elif "FROM `rank_score`" in sql and "ORDER BY" in sql:
                self._rows = [(101, 500, 1000)]
            else:
                self._rows = []

        async def fetchone(self):
            return self._rows[0] if self._rows else None

        async def fetchall(self):
            return list(self._rows)

    class FakeConn:
        def __init__(self, state):
            self._state = state

        def cursor(self):
            return FakeCursor(self._state)

        async def begin(self):
            executed.append("BEGIN")

        async def commit(self):
            executed.append("COMMIT")

        async def rollback(self):
            executed.append("ROLLBACK")

        def close(self):
            pass

    class FakePool:
        def __init__(self, state):
            self._state = state

        @asynccontextmanager
        async def acquire(self):
            yield FakeConn(self._state)

    state = {"fail_1213": True}
    sql_store = SqlRankStore(
        FakePool(state),
        protocol,
        next_board_id=default_monotonic_id_generator(1),
        next_settlement_id=default_monotonic_id_generator(10),
        next_reward_id=default_monotonic_id_generator(100),
        item_exists=lambda _id: True,
        clock_ms=lambda: BASE_NOW_MS,
    )

    freeze = protocol.message(
        "rankpb.FreezeRankGenerationRequest",
        board_id=1,
        generation=1,
        source_epoch=1,
        expected_source_cursor=1,
        request_id=77,
        top_n=10,
    )
    tier = freeze.rewards.add()
    tier.first_rank, tier.last_rank = 1, 1
    c = tier.attachments.currencies.add()
    c.currency_type, c.amount = 1, 10

    resp = await sql_store.freeze_generation("match", freeze)
    assert resp.settlement_id == 10 and resp.winner_count == 1
    assert executed.count("ROLLBACK") == 1
    assert any("SET TRANSACTION ISOLATION LEVEL READ COMMITTED" in s for s in executed)
    assert any("INSERT INTO `rank_snapshot` (`settlement_id`,`rank`," in s for s in executed)
    assert any("INSERT INTO `rank_mail_reward`" in s and "UNHEX(%s)" in s and "`rank`" in s for s in executed)

    # Read path uses REPEATABLE READ
    executed.clear()
    page_req = protocol.message("rankpb.GetRankPageRequest", page=1, page_size=10)
    page_req.board.board_type = 1
    page_req.board.scope = RANK_SCOPE_GLOBAL
    await sql_store.get_page(101, page_req)
    assert any("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" in s for s in executed)


@_async_test
async def test_sql_store_fresh_insert_1062_race_retries():
    """验证并发分片首次插入同一实体撞 1062 时，SqlRankStore 整笔回滚重试并成功提交。"""
    protocol = Protocol()
    events: list[str] = []
    race_state = {"triggered": False}

    class RaceCursor:
        def __init__(self):
            self._rows = []
            self.rowcount = 1

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, sql: str, args=None):
            events.append(sql)
            if "FROM `rank_board_shard`" in sql:
                self._rows = [(0, 0)]
            elif "FROM `rank_board`" in sql:
                self._rows = [(1, 1, 1, 0, "p1", 1, "match", 1, 0, RANK_GEN_OPEN, 0, 1, "h", 100, 0, 0)]
            elif "FROM `rank_removed_entry`" in sql:
                self._rows = []
            elif "FROM `rank_score`" in sql:
                # 首次尝试未查到（新实体），重试时已被并发片插入
                self._rows = [(10, 50, 900, 0)] if race_state["triggered"] else []
            elif "INSERT INTO `rank_score`" in sql and "ON DUPLICATE KEY" not in sql:
                race_state["triggered"] = True
                raise pymysql.IntegrityError(1062, "Duplicate entry for key PRIMARY")
            else:
                self._rows = []

        async def fetchone(self):
            return self._rows[0] if self._rows else None

        async def fetchall(self):
            return list(self._rows)

    class RaceConn:
        def cursor(self):
            return RaceCursor()

        async def begin(self):
            events.append("BEGIN")

        async def commit(self):
            events.append("COMMIT")

        async def rollback(self):
            events.append("ROLLBACK")

        def close(self):
            pass

    class RacePool:
        @asynccontextmanager
        async def acquire(self):
            yield RaceConn()

    sql_store = SqlRankStore(
        RacePool(),
        protocol,
        next_board_id=default_monotonic_id_generator(1),
        next_settlement_id=default_monotonic_id_generator(10),
        next_reward_id=default_monotonic_id_generator(100),
        clock_ms=lambda: BASE_NOW_MS,
    )
    sub = protocol.message(
        "rankpb.SubmitRankScoresRequest",
        board_id=1,
        generation=1,
        source_epoch=1,
        source_shard=2,
        source_cursor=1,
        request_id=900,
        mode=SUBMIT_INCREMENT,
    )
    e = sub.entries.add()
    e.entity_id, e.score, e.achieved_ms = 10, 25, 1000
    resp = await sql_store.submit_scores("match", sub)
    assert resp.committed_cursor == 1 and not resp.duplicated
    assert events.count("ROLLBACK") == 1
    assert any("ON DUPLICATE KEY UPDATE" in s for s in events)
