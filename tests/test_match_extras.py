"""match 扩展能力端到端测试：BattleScripts CRUD + 运营审核、Elo 评分 + RankOutboxPublisher 对接真实 RankService、
以及论剑排位 V1（GetRankedProfile / SaveRankedPreset / UpdateRankedCosmetic / GetRankedBattleResult / ConfirmRankedPreset）。
"""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.grpc_runtime import (
    GrpcClient,
    serve_grpc,
    session_metadata,
    sign_service_call,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.match import (
    BattleNodeEntry,
    BattleScriptManager,
    GatherPipeline,
    GrpcEndpointPool,
    MatchConfig,
    MatchGrpcService,
    MatchServer,
    Matchmaker,
    MemoryMatchStore,
    MemoryPresenceProvider,
    OutcomeOutboxWorker,
    PlayerPresence,
    RankOutboxPublisher,
    RankedManager,
    RankedSeasonRow,
    SpectateManager,
    StaticBattleNodeResolver,
    TeamGrpcService,
    apply_battle_result,
)
from xuanming.match.battle_scripts import (
    MAX_BATTLE_SCRIPTS_PER_PLAYER,
    MAX_PLAYER_BATTLE_SCRIPT_BYTES,
    REVIEW_REJECT_DENIED,
    REVIEW_REJECT_INVALID,
    REVIEW_REJECT_STALE,
    REVIEW_STATE_APPROVED,
    REVIEW_STATE_PENDING,
    REVIEW_STATE_REJECTED,
    SCRIPT_ORIGIN_OFFICIAL,
    SCRIPT_ORIGIN_PLAYER,
    SCRIPT_ORIGIN_UNSPECIFIED,
)
from xuanming.match.config import OPS_CALLER_SECRET_ENV, RANK_PRODUCER_SECRET_ENV
from xuanming.match.errors import (
    BattleOutcomeOriginalError,
    ERR_INVALID_PARAMETER,
    ERR_MATCH_BATTLE_REPORT_NOT_FOUND,
    ERR_MATCH_BATTLE_SCRIPT_INVALID,
    ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID,
    ERR_MATCH_BATTLE_SCRIPT_NOT_APPROVED,
    ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND,
    ERR_MATCH_BATTLE_SCRIPT_REVISION_CONFLICT,
    ERR_MATCH_RANKED_PRESET_INVALID,
    ERR_MATCH_RANKED_RULE_VERSION_MISMATCH,
    ERR_MATCH_RANKED_SEASON_SETTLING,
    ERR_TEAM_NO_TEAM,
    ERR_TEAM_RANKED_PRESET_INVALID,
)
from xuanming.match.model import MODE_1V1, MODE_PVE_SOLO, MODE_PVE_TEAM, MODE_PVP_TEAM
from xuanming.match.ranked import (
    DEFAULT_CHARACTER_PRESET_ID,
    DEFAULT_PET_PRESET_ID,
    RANKED_MATCH_RATING_CONFIRMED,
    RANKED_REWARD_AVATAR_PATTERN,
    RANKED_SEASON_CLOSED,
    RANKED_SEASON_SETTLING,
    RANKED_TIER_QINGFENG,
    RANKED_TIER_UNSPECIFIED,
    RANKED_TIER_YUNZHANG,
)
from xuanming.match.rating import (
    DEFAULT_RATING_CENTI,
    RANK_BOARD_TYPES,
    RANK_SCOPE_GLOBAL,
    RATING_OUTCOME_APPLIED,
    RATING_OUTCOME_DUPLICATE,
    RATING_OUTCOME_IGNORED,
    rank_source_shard,
)
from xuanming.match.sql_store import REQUIRED_TABLES
from xuanming.protocol import Protocol
from xuanming.rank import MemoryRankStore, RankService
from xuanming.settings import Settings

BATTLE_SECRET = b"battle-outcome-secret-32-bytes-!!"
SCENE_SECRET = b"scene-directory-secret-32-bytes-!"
COORD_SECRET = b"coordinator-fence-secret-32-byte!"
OPS_SECRET = b"match-ops-caller-secret-32-bytes!"
RANK_SECRET = b"match-rank-producer-secret-32byt!"


class _FakeClock:
    def __init__(self, start_ms: int | None = None) -> None:
        self.now = start_ms or (time.time_ns() // 1_000_000)

    def __call__(self) -> int:
        return self.now

    def advance(self, delta_ms: int) -> None:
        self.now += delta_ms


class _FakeRankNames:
    def __init__(self, names: dict[int, str]) -> None:
        self.names = dict(names)

    async def batch_get_player_name(self, player_ids: list[int], *, timeout: float = 0.2) -> dict[int, str]:
        return {pid: self.names.get(pid, f"Hero{pid}") for pid in player_ids}


async def _call(client: GrpcClient, protocol: Protocol, path: str, req, resp_type: str, *, metadata: tuple = ()):
    raw = await client.call(path, req.SerializeToString(deterministic=True), metadata=metadata)
    return protocol.parse(resp_type, raw)


@asynccontextmanager
async def _extras_harness(*, ops_secret: bytes = OPS_SECRET, rank_secret: bytes = RANK_SECRET):
    protocol = Protocol()
    clock = _FakeClock()
    gate_key = Ed25519PrivateKey.generate()
    verifier = CallerVerifier([gate_key.public_key()], allowed_callers=("gate",))

    # 1. 启动真实 Python RankService gRPC 回环服务
    rank_store = MemoryRankStore(protocol, item_exists=lambda _: True, clock_ms=clock)
    rank_svc = RankService(
        protocol,
        rank_store,
        producer_secrets={"match": RANK_SECRET},
        caller_verifier=verifier,
        name_reader=_FakeRankNames({1001: "Alpha", 1002: "Beta", 1003: "Gamma", 1004: "Delta"}),
        clock_ms=clock,
    )
    rank_srv, rank_port = await serve_grpc(protocol, "127.0.0.1:0", rank_svc.services(), pass_context=True)
    rank_client = GrpcClient(f"127.0.0.1:{rank_port}", timeout=5.0)

    # 2. 启动真实 Match + Team 统一 gRPC 服务（pass_context=True）
    cfg = MatchConfig(
        node_id=101,
        team_node_id=102,
        grpc_port=0,
        battle_outcome_secret=BATTLE_SECRET,
        scene_secret=SCENE_SECRET,
        coordinator_secret=COORD_SECRET,
        ops_secret=ops_secret,
        rank_secret=rank_secret,
        rank_endpoint=f"127.0.0.1:{rank_port}",
    )
    store = MemoryMatchStore(protocol, clock_ms=clock)
    presence = MemoryPresenceProvider()
    for pid in (1001, 1002, 1003, 1004):
        presence.set_presence(
            PlayerPresence(
                player_id=pid,
                online=True,
                zone_id=1,
                scene_node_id=2,
                scene_instance_id="scene-uuid-1",
                scene_grpc_endpoint="127.0.0.1:19999",
                session_id=(1 << 17) | pid,
                gate_node_id=1,
                gate_instance_id="gate-uuid-1",
                name=f"Hero{pid}",
                level=30,
                class_id=1,
            )
        )
    battle_resolver = StaticBattleNodeResolver(
        [BattleNodeEntry(node_id=30, node_uuid="battle-inst-1", grpc_endpoint="127.0.0.1:19998", physical_region=1)]
    )
    rpc_pool = GrpcEndpointPool(default_timeout=5.0)
    gather = GatherPipeline(protocol, store, cfg, rpc_pool, presence, battle_resolver)
    matchmaker = Matchmaker(protocol, store, cfg, gather, battle_resolver)
    outbox = OutcomeOutboxWorker(
        protocol, store, inbox_call=None, inbox_secret=b"", publish_result=None, clock_ms=clock
    )
    spectate = SpectateManager(protocol, store, cfg, rpc_pool, presence)
    battle_scripts = BattleScriptManager(protocol, store)
    ranked = RankedManager(protocol, store, clock_ms=clock)
    rank_publisher = RankOutboxPublisher(
        protocol,
        store,
        rank_call=rank_client.call,
        rank_secret=rank_secret,
        clock_ms=clock,
    )
    team_svc = TeamGrpcService(
        protocol,
        store,
        cfg,
        gate_verifier=verifier,
        presence=presence,
        battle_nodes=battle_resolver,
        gather=gather,
        ranked=ranked,
        inline_gather=True,
        clock_ms=clock,
    )
    match_svc = MatchGrpcService(
        protocol,
        store,
        cfg,
        gate_verifier=verifier,
        presence=presence,
        battle_nodes=battle_resolver,
        gather=gather,
        spectate=spectate,
        battle_scripts=battle_scripts,
        ranked=ranked,
        inline_gather=True,
        clock_ms=clock,
    )
    match_server = MatchServer(
        protocol=protocol,
        store=store,
        config=cfg,
        match_service=match_svc,
        team_service=team_svc,
        gather=gather,
        matchmaker=matchmaker,
        outbox=outbox,
        spectate=spectate,
        rank_publisher=rank_publisher,
    )
    grpc_srv, match_port = await serve_grpc(
        protocol, "127.0.0.1:0", match_server.services(), pass_context=True
    )
    client = GrpcClient(f"127.0.0.1:{match_port}", timeout=5.0)

    def gate_md(player_id: int, method: str, *, account: str | None = None, owner_revision: int = 1) -> tuple:
        details = protocol.message(
            "SessionDetails",
            session_id=(1 << 17) | player_id,
            player_id=player_id,
            gate_node_id=1,
            gate_instance_id="gate-uuid-1",
            session_incarnation=100 + player_id,
            owner_revision=owner_revision,
            authenticated_account=f"acct:{player_id}" if account is None else account,
        )
        return session_metadata(details.SerializeToString(deterministic=True), method, gate_key)

    try:
        yield {
            "protocol": protocol,
            "clock": clock,
            "client": client,
            "rank_client": rank_client,
            "gate_md": gate_md,
            "store": store,
            "rank_store": rank_store,
            "rank_publisher": rank_publisher,
            "ranked": ranked,
            "config": cfg,
        }
    finally:
        await client.close()
        await grpc_srv.stop(1.0)
        await match_server.close()
        await rpc_pool.close()
        await rank_client.close()
        await rank_srv.stop(1.0)


def _sample_player_script(protocol: Protocol, *, name: str = "破军集火", reverse_order: bool = False):
    script = protocol.message("BattleScript", schema_version=1, script_id=999, name=name, description="test script")
    orders = [(2, 20), (1, 10)] if reverse_order else [(1, 10), (2, 20)]
    for rid, order in orders:
        role = script.roles.add(role_id=rid, order=order, name=f"role-{rid}", actor_type=1, class_id=1)
        if rid == 1:
            r2 = role.rules.add(priority=20, action=1)  # ATTACK
            r2.target.side = 4  # ENGINE_PICK
            r1 = role.rules.add(priority=10, action=2, skill_id=1101)  # SKILL 1101 (class 1 enemy)
            r1.target.side = 2  # ENEMY
            r1.target.order = 1  # HP_LOWEST
        else:
            r = role.rules.add(priority=10, action=3)  # DEFEND
            r.target.side = 1  # SELF
    return script


# -- 1. BattleScripts CRUD + Review（通过/驳回）+ Ops Auth Fail-Closed --------------------------------


def test_battle_scripts_crud_review_and_ops_auth():
    async def run():
        async with _extras_harness() as h:
            p, c, md, clock = h["protocol"], h["client"], h["gate_md"], h["clock"]

            list_path = "/match.MatchService/ListBattleScripts"
            submit_path = "/match.MatchService/SubmitBattleScript"
            delete_path = "/match.MatchService/DeleteBattleScript"
            loadout_path = "/match.MatchService/SetBattleScriptLoadout"
            pending_path = "/match.MatchInternal/ListPendingBattleScripts"
            review_path = "/match.MatchInternal/ReviewBattleScript"

            # 1) 初始列表：包含内置官方剧本（id=1,2）与容量上限
            r_list = await _call(
                c, p, list_path, p.message("match.ListBattleScriptsRequest"),
                "match.ListBattleScriptsResponse", metadata=md(1001, list_path),
            )
            assert r_list.error_message.id == 0
            assert [s.script_id for s in r_list.official_scripts] == [1, 2]
            assert len(r_list.my_scripts) == 0
            assert r_list.max_scripts == MAX_BATTLE_SCRIPTS_PER_PLAYER
            assert r_list.max_script_bytes == MAX_PLAYER_BATTLE_SCRIPT_BYTES

            # 2) 非法剧本（未知技能）提交被拒并返回英文位置详情
            bad_script = _sample_player_script(p)
            bad_script.roles[0].rules[1].skill_id = 999999
            r_bad = await _call(
                c, p, submit_path, p.message("match.SubmitBattleScriptRequest", script=bad_script),
                "match.SubmitBattleScriptResponse", metadata=md(1001, submit_path),
            )
            assert r_bad.error_message.id == ERR_MATCH_BATTLE_SCRIPT_INVALID
            assert "unknown skill 999999" in r_bad.invalid_detail

            # 3) 合法剧本提交：script_id 归零，roles 按 order 排序，rules 按 priority 排序，进入 PENDING
            valid_script = _sample_player_script(p, reverse_order=True)
            r_sub1 = await _call(
                c, p, submit_path, p.message("match.SubmitBattleScriptRequest", script=valid_script),
                "match.SubmitBattleScriptResponse", metadata=md(1001, submit_path),
            )
            assert r_sub1.error_message.id == 0
            sid = r_sub1.script.script_id
            assert sid > 0 and r_sub1.script.revision == 1
            assert r_sub1.script.review_state == REVIEW_STATE_PENDING
            assert r_sub1.script.approved_revision == 0
            assert r_sub1.script.content.script_id == 0
            assert [r.order for r in r_sub1.script.content.roles] == [10, 20]
            assert [ru.priority for ru in r_sub1.script.content.roles[0].rules] == [10, 20]

            # 4) 未过审剧本禁止选用
            req_lo = p.message("match.SetBattleScriptLoadoutRequest")
            req_lo.entry.match_mode = MODE_PVE_TEAM
            req_lo.entry.battle_config_id = 1
            req_lo.entry.origin = SCRIPT_ORIGIN_PLAYER
            req_lo.entry.player_script_id = sid
            r_unapproved = await _call(
                c, p, loadout_path, req_lo, "match.SetBattleScriptLoadoutResponse", metadata=md(1001, loadout_path),
            )
            assert r_unapproved.error_message.id == ERR_MATCH_BATTLE_SCRIPT_NOT_APPROVED

            # 5) Ops 鉴权 fail-closed：带客户端会话被 PERMISSION_DENIED 拒绝，错密钥回 DENIED
            lp_req = p.message("match.ListPendingBattleScriptsRequest", limit=10)
            sign_service_call(pending_path, lp_req, "ops", clock(), OPS_SECRET)
            with pytest.raises(grpc.aio.AioRpcError) as exc_sess:
                await _call(
                    c, p, pending_path, lp_req, "match.ListPendingBattleScriptsResponse",
                    metadata=md(1001, pending_path),
                )
            assert exc_sess.value.code() == grpc.StatusCode.PERMISSION_DENIED

            lp_wrong = p.message("match.ListPendingBattleScriptsRequest", limit=10)
            sign_service_call(pending_path, lp_wrong, "ops", clock(), b"wrong-ops-secret-32-bytes-000000")
            r_denied = await _call(c, p, pending_path, lp_wrong, "match.ListPendingBattleScriptsResponse")
            assert r_denied.reject == REVIEW_REJECT_DENIED

            # 6) 合法 ops 拉取待审列表
            lp_ok = p.message("match.ListPendingBattleScriptsRequest", limit=10)
            sign_service_call(pending_path, lp_ok, "ops", clock(), OPS_SECRET)
            r_pending = await _call(c, p, pending_path, lp_ok, "match.ListPendingBattleScriptsResponse")
            assert r_pending.reject == 0 and len(r_pending.scripts) == 1
            assert r_pending.scripts[0].script_id == sid and r_pending.scripts[0].owner_player_id == 1001

            # 7) 驳回缺少理由 -> INVALID；正常驳回 -> REJECTED；重复审同版 -> STALE
            rev_no_reason = p.message(
                "match.ReviewBattleScriptRequest",
                script_id=sid, revision=1, approve=False, reviewer="ops-alice", reason="",
            )
            sign_service_call(review_path, rev_no_reason, "ops", clock(), OPS_SECRET)
            r_inv = await _call(c, p, review_path, rev_no_reason, "match.ReviewBattleScriptResponse")
            assert r_inv.reject == REVIEW_REJECT_INVALID

            rev_reject = p.message(
                "match.ReviewBattleScriptRequest",
                script_id=sid, revision=1, approve=False, reviewer="ops-alice", reason="name needs clarity",
            )
            sign_service_call(review_path, rev_reject, "ops", clock(), OPS_SECRET)
            r_rej = await _call(c, p, review_path, rev_reject, "match.ReviewBattleScriptResponse")
            assert r_rej.reject == 0

            rev_stale = p.message(
                "match.ReviewBattleScriptRequest",
                script_id=sid, revision=1, approve=True, reviewer="ops-bob", reason="ok",
            )
            sign_service_call(review_path, rev_stale, "ops", clock(), OPS_SECRET)
            r_st = await _call(c, p, review_path, rev_stale, "match.ReviewBattleScriptResponse")
            assert r_st.reject == REVIEW_REJECT_STALE

            # 8) 玩家改写（错 expected_revision 冲突 -> 正确 revision=1 升到 rev=2）-> Ops 审核通过
            r_conf = await _call(
                c, p, submit_path,
                p.message("match.SubmitBattleScriptRequest", script_id=sid, expected_revision=99, script=_sample_player_script(p, name="v2")),
                "match.SubmitBattleScriptResponse", metadata=md(1001, submit_path),
            )
            assert r_conf.error_message.id == ERR_MATCH_BATTLE_SCRIPT_REVISION_CONFLICT

            r_sub2 = await _call(
                c, p, submit_path,
                p.message("match.SubmitBattleScriptRequest", script_id=sid, expected_revision=1, script=_sample_player_script(p, name="v2")),
                "match.SubmitBattleScriptResponse", metadata=md(1001, submit_path),
            )
            assert r_sub2.error_message.id == 0 and r_sub2.script.revision == 2 and r_sub2.script.review_state == REVIEW_STATE_PENDING

            rev_ok = p.message(
                "match.ReviewBattleScriptRequest",
                script_id=sid, revision=2, approve=True, reviewer="ops-alice", reason="lgtm",
            )
            sign_service_call(review_path, rev_ok, "ops", clock(), OPS_SECRET)
            r_app = await _call(c, p, review_path, rev_ok, "match.ReviewBattleScriptResponse")
            assert r_app.reject == 0

            # 改写为 rev=3 待审时，approved_revision 仍保持 2
            r_sub3 = await _call(
                c, p, submit_path,
                p.message("match.SubmitBattleScriptRequest", script_id=sid, expected_revision=2, script=_sample_player_script(p, name="v3")),
                "match.SubmitBattleScriptResponse", metadata=md(1001, submit_path),
            )
            assert r_sub3.error_message.id == 0 and r_sub3.script.revision == 3
            assert r_sub3.script.review_state == REVIEW_STATE_PENDING and r_sub3.script.approved_revision == 2

            # 9) 选用校验：通过版可用于 PVE，不可用于显式计分模式（1V1），不可用怪物方官方剧本，不可选他人剧本
            r_lo_ok = await _call(c, p, loadout_path, req_lo, "match.SetBattleScriptLoadoutResponse", metadata=md(1001, loadout_path))
            assert r_lo_ok.error_message.id == 0 and len(r_lo_ok.loadout) == 1 and r_lo_ok.loadout[0].player_script_id == sid

            req_rated = p.message("match.SetBattleScriptLoadoutRequest")
            req_rated.entry.match_mode = MODE_1V1
            req_rated.entry.origin = SCRIPT_ORIGIN_PLAYER
            req_rated.entry.player_script_id = sid
            r_lo_rated = await _call(c, p, loadout_path, req_rated, "match.SetBattleScriptLoadoutResponse", metadata=md(1001, loadout_path))
            assert r_lo_rated.error_message.id == ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID

            req_monster_off = p.message("match.SetBattleScriptLoadoutRequest")
            req_monster_off.entry.match_mode = MODE_PVE_TEAM
            req_monster_off.entry.origin = SCRIPT_ORIGIN_OFFICIAL
            req_monster_off.entry.official_script_id = 2  # monster side
            r_lo_mon = await _call(c, p, loadout_path, req_monster_off, "match.SetBattleScriptLoadoutResponse", metadata=md(1001, loadout_path))
            assert r_lo_mon.error_message.id == ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID

            req_off_ok = p.message("match.SetBattleScriptLoadoutRequest")
            req_off_ok.entry.match_mode = MODE_1V1
            req_off_ok.entry.origin = SCRIPT_ORIGIN_OFFICIAL
            req_off_ok.entry.official_script_id = 1
            r_lo_off = await _call(c, p, loadout_path, req_off_ok, "match.SetBattleScriptLoadoutResponse", metadata=md(1001, loadout_path))
            assert r_lo_off.error_message.id == 0 and len(r_lo_off.loadout) == 2

            # 他人（1002）不能选用或删除 1001 的剧本
            r_other_del = await _call(
                c, p, delete_path, p.message("match.DeleteBattleScriptRequest", script_id=sid, expected_revision=3),
                "match.DeleteBattleScriptResponse", metadata=md(1002, delete_path),
            )
            assert r_other_del.error_message.id == ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND

            # 10) 删除剧本同事务清掉指向它的选用，但保留官方剧本选用
            r_del = await _call(
                c, p, delete_path, p.message("match.DeleteBattleScriptRequest", script_id=sid, expected_revision=3),
                "match.DeleteBattleScriptResponse", metadata=md(1001, delete_path),
            )
            assert r_del.error_message.id == 0

            r_list_after = await _call(
                c, p, list_path, p.message("match.ListBattleScriptsRequest"),
                "match.ListBattleScriptsResponse", metadata=md(1001, list_path),
            )
            assert len(r_list_after.my_scripts) == 0
            assert len(r_list_after.loadout) == 1 and r_list_after.loadout[0].official_script_id == 1

            # 清除官方选用
            req_clear = p.message("match.SetBattleScriptLoadoutRequest")
            req_clear.entry.match_mode = MODE_1V1
            req_clear.entry.origin = SCRIPT_ORIGIN_UNSPECIFIED
            r_clr = await _call(c, p, loadout_path, req_clear, "match.SetBattleScriptLoadoutResponse", metadata=md(1001, loadout_path))
            assert r_clr.error_message.id == 0 and len(r_clr.loadout) == 0

        # ops_secret 留空时 fail-closed 拒绝
        async with _extras_harness(ops_secret=b"") as h2:
            p2, c2, clock2 = h2["protocol"], h2["client"], h2["clock"]
            req = p2.message("match.ListPendingBattleScriptsRequest", limit=5)
            sign_service_call("/match.MatchInternal/ListPendingBattleScripts", req, "ops", clock2(), OPS_SECRET)
            resp = await _call(c2, p2, "/match.MatchInternal/ListPendingBattleScripts", req, "match.ListPendingBattleScriptsResponse")
            assert resp.reject == REVIEW_REJECT_DENIED

    asyncio.run(run())


# -- 2. Elo 评分幂等 + RankOutboxPublisher -> 真实 Python RankService.SubmitRankScores 端到端 ---------


def _build_battle_result_event(
    protocol: Protocol,
    *,
    battle_id: int,
    mode: int,
    outcome: int,
    team_a: list[int],
    team_b: list[int],
    total_rounds: int = 5,
    side_a_alive: bool = True,
    side_b_alive: bool = False,
    finished_at_ms: int = 1_700_000_000_000,
):
    ev = protocol.message(
        "contracts.kafka.BattleResultEvent",
        battle_id=battle_id,
        match_mode=mode,
        battle_config_id=5 if mode == MODE_PVP_TEAM else 1,
        outcome=outcome,
        total_rounds=total_rounds,
        finished_at_ms=finished_at_ms,
        settlement_format_version=1,
    )
    ev.teams.add(team_index=0, player_ids=team_a)
    ev.teams.add(team_index=1, player_ids=team_b)
    for pid in team_a:
        ev.settlements.add(
            player_id=pid, battle_id=battle_id, outcome=outcome,
            player_team_index=0, total_rounds=total_rounds, is_dead=not side_a_alive,
        )
    for pid in team_b:
        ev.settlements.add(
            player_id=pid, battle_id=battle_id, outcome=outcome,
            player_team_index=1, total_rounds=total_rounds, is_dead=not side_b_alive,
        )
    return ev


def test_elo_rating_idempotency_and_rank_outbox_publisher_e2e():
    async def run():
        async with _extras_harness() as h:
            p, store, clock = h["protocol"], h["store"], h["clock"]
            publisher: RankOutboxPublisher = h["rank_publisher"]
            rank_client: GrpcClient = h["rank_client"]
            md = h["gate_md"]

            # 1) 非计分模式（PVE_SOLO）忽略
            pve_ev = _build_battle_result_event(p, battle_id=5001, mode=MODE_PVE_SOLO, outcome=1, team_a=[1001], team_b=[1002])
            res_pve = await apply_battle_result(store, p, pve_ev)
            assert res_pve.outcome == RATING_OUTCOME_IGNORED

            # 2) 首场 1v1 计分局：1001 胜 1002 -> 等分 150000 变化 +1600 / -1600
            ev1 = _build_battle_result_event(
                p, battle_id=6401, mode=MODE_1V1, outcome=1, team_a=[1001], team_b=[1002], finished_at_ms=clock()
            )
            r1 = await apply_battle_result(store, p, ev1)
            assert r1.outcome == RATING_OUTCOME_APPLIED and r1.source_cursor == 1
            assert (await store.get_rating(1001, MODE_1V1))[0] == DEFAULT_RATING_CENTI + 1600
            assert (await store.get_rating(1002, MODE_1V1))[0] == DEFAULT_RATING_CENTI - 1600
            assert (await store.get_rating(1001, MODE_1V1))[1] == 1  # games = score_version

            # 同 battle_id 同载荷幂等重放 -> duplicate，不重复加分也不追加 outbox
            r1_dup = await apply_battle_result(store, p, ev1)
            assert r1_dup.outcome == RATING_OUTCOME_DUPLICATE
            assert (await store.get_rating(1001, MODE_1V1))[0] == DEFAULT_RATING_CENTI + 1600

            # 同 battle_id 篡改载荷 -> 抛 BattleOutcomeOriginalError
            ev1_tampered = _build_battle_result_event(
                p, battle_id=6401, mode=MODE_1V1, outcome=2, team_a=[1001], team_b=[1002],
                side_a_alive=False, side_b_alive=True, finished_at_ms=clock(),
            )
            with pytest.raises(BattleOutcomeOriginalError):
                await apply_battle_result(store, p, ev1_tampered)

            # 3) RankOutboxPublisher 投递至真实 Python RankService（OpenRankGeneration + SubmitRankScores）
            st1 = await publisher.run_once()
            assert st1.boards_opened == 1 and st1.batches_submitted == 1 and st1.errors == 0

            # 通过真实 ClientPlayerRank.GetRankPage 查询榜单确认分数与名次
            page_path = "/rankpb.ClientPlayerRank/GetRankPage"
            page_req = p.message("rankpb.GetRankPageRequest", page=1, page_size=10)
            page_req.board.board_type = RANK_BOARD_TYPES[MODE_1V1]
            page_req.board.scope = RANK_SCOPE_GLOBAL
            page_req.board.period = "all"
            page_req.board.generation = 1
            page_resp = await _call(
                rank_client, p, page_path, page_req, "rankpb.GetRankPageResponse", metadata=md(1001, page_path)
            )
            assert page_resp.error_message.id == 0 and len(page_resp.entries) == 2
            assert page_resp.entries[0].entity_id == 1001 and page_resp.entries[0].score == 151_600 and page_resp.entries[0].rank == 1
            assert page_resp.entries[1].entity_id == 1002 and page_resp.entries[1].score == 148_400 and page_resp.entries[1].rank == 2

            # 4) 同一分片（6401 + 64 = 6465）第二局：回合打满（SIDE_B_WIN 但 0 队存活 -> 按平局结算）
            clock.advance(5000)
            ev2 = _build_battle_result_event(
                p, battle_id=6465, mode=MODE_1V1, outcome=2, team_a=[1001], team_b=[1002],
                side_a_alive=True, side_b_alive=True, finished_at_ms=clock(),
            )
            r2 = await apply_battle_result(store, p, ev2)
            assert r2.outcome == RATING_OUTCOME_APPLIED
            assert r2.shard == rank_source_shard(6401) and r2.source_cursor == 2
            # 1001 原分更高，平局小幅扣分，1002 小幅涨分
            score_1001_after = (await store.get_rating(1001, MODE_1V1))[0]
            score_1002_after = (await store.get_rating(1002, MODE_1V1))[0]
            assert score_1001_after < 151_600 and score_1002_after > 148_400

            st2 = await publisher.run_once()
            assert st2.boards_opened == 0 and st2.batches_submitted == 1 and st2.errors == 0

            # 5) 模拟本地游标回退后幂等重投：RankService 返回 duplicated=True 且 committed_cursor=2，本地游标安全追平
            shard_key = (1, MODE_1V1, rank_source_shard(6401))
            src = store._state.rank_sources[shard_key]
            store._state.rank_sources[shard_key] = type(src)(
                source_epoch=src.source_epoch, mode=src.mode, shard=src.shard,
                produced_cursor=src.produced_cursor, committed_cursor=0,
                board_id=src.board_id, stopped=src.stopped,
            )
            st_replay = await publisher.run_once()
            assert st_replay.batches_duplicated == 1 and st_replay.errors == 0
            assert store._state.rank_sources[shard_key].committed_cursor == 2

            page_resp2 = await _call(
                rank_client, p, page_path, page_req, "rankpb.GetRankPageResponse", metadata=md(1001, page_path)
            )
            assert page_resp2.entries[0].entity_id == 1001 and page_resp2.entries[0].score == score_1001_after

    asyncio.run(run())


# -- 3. Ranked Play V1 + ClientPlayerTeam.ConfirmRankedPreset ----------------------------------------


def test_ranked_v1_profile_presets_cosmetics_battle_result_and_team_confirm():
    async def run():
        async with _extras_harness() as h:
            p, c, md, clock = h["protocol"], h["client"], h["gate_md"], h["clock"]
            ranked: RankedManager = h["ranked"]
            store: MemoryMatchStore = h["store"]

            prof_path = "/match.MatchService/GetRankedProfile"
            save_path = "/match.MatchService/SaveRankedPreset"
            cos_path = "/match.MatchService/UpdateRankedCosmetic"
            res_path = "/match.MatchService/GetRankedBattleResult"

            # 1) GetRankedProfile：默认 S1 赛季 + 6 套正式预设 + 默认 101/203 + 未亮段
            prof = await _call(
                c, p, prof_path, p.message("match.GetRankedProfileRequest"),
                "match.GetRankedProfileResponse", metadata=md(1001, prof_path),
            )
            assert prof.error_message.id == 0
            assert prof.current_season.season_id == 1 and prof.current_season.season_key == "s1"
            assert prof.current_season.rule_version == 2 and prof.current_season.battle_config_id == 5
            assert prof.current_season.wait_prompt_seconds == 120
            assert (
                prof.current_season.spread_tier1_points,
                prof.current_season.spread_tier2_points,
                prof.current_season.spread_tier3_points,
            ) == (100, 200, 300)
            assert [pr.preset_id for pr in prof.presets] == [101, 102, 103, 201, 202, 203]
            assert prof.selected_presets.character_preset_id == DEFAULT_CHARACTER_PRESET_ID
            assert prof.selected_presets.pet_preset_id == DEFAULT_PET_PRESET_ID
            assert not prof.current_progress.qualified and prof.current_progress.current_tier == RANKED_TIER_UNSPECIFIED

            # 2) SaveRankedPreset 边界校验与成功落库
            r_bad_preset = await _call(
                c, p, save_path,
                p.message("match.SaveRankedPresetRequest", character_preset_id=999, pet_preset_id=201, rule_version=2),
                "match.SaveRankedPresetResponse", metadata=md(1001, save_path),
            )
            assert r_bad_preset.error_message.id == ERR_MATCH_RANKED_PRESET_INVALID

            r_bad_ver = await _call(
                c, p, save_path,
                p.message("match.SaveRankedPresetRequest", character_preset_id=102, pet_preset_id=201, rule_version=99),
                "match.SaveRankedPresetResponse", metadata=md(1001, save_path),
            )
            assert r_bad_ver.error_message.id == ERR_MATCH_RANKED_RULE_VERSION_MISMATCH

            r_saved = await _call(
                c, p, save_path,
                p.message("match.SaveRankedPresetRequest", character_preset_id=102, pet_preset_id=201, rule_version=2),
                "match.SaveRankedPresetResponse", metadata=md(1001, save_path),
            )
            assert r_saved.error_message.id == 0
            assert r_saved.selected_presets.character_preset_id == 102 and r_saved.selected_presets.pet_preset_id == 201

            # 3) 未拥有的展示权益拒绝佩戴
            bad_cos = p.message("match.UpdateRankedCosmeticRequest")
            bad_cos.loadout.equipped_avatar_pattern_code = "s1_avatar_pattern"
            r_cos_bad = await _call(
                c, p, cos_path, bad_cos, "match.UpdateRankedCosmeticResponse", metadata=md(1001, cos_path),
            )
            assert r_cos_bad.error_message.id == ERR_INVALID_PARAMETER

            # 无战绩时 GetRankedBattleResult 回 NOT_FOUND
            r_no_res = await _call(
                c, p, res_path, p.message("match.GetRankedBattleResultRequest", battle_id=0),
                "match.GetRankedBattleResultResponse", metadata=md(1001, res_path),
            )
            assert r_no_res.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND

            # 4) 落账 4 场有效排位：首场亮段、跨 160000 进云章、第 4 场解锁头像纹样且保持最高段位云章
            matches = [
                (8001, 150_000, 151_600, 1),
                (8002, 151_600, 156_000, 1),
                (8003, 156_000, 161_000, 1),  # 进云章 (V2 >= 160000)
                (8004, 161_000, 154_000, 2),  # 回青锋 (V2 >= 152000)，但 peak_tier 保留云章 + 解锁 4 场头像纹样
            ]
            for bid, before, after, outcome in matches:
                clock.advance(1000)
                await ranked.record_battle_settlement(
                    battle_id=bid, player_id=1001, season_id=1, team_size=1,
                    status=RANKED_MATCH_RATING_CONFIRMED, outcome=outcome, player_team_index=0,
                    total_rounds=6, participated=True, fled=False,
                    rating_before_centi=before, rating_after_centi=after,
                    character_preset_id=102, pet_preset_id=201,
                )
            # 幂等重放第 4 场：不重复累加场次
            await ranked.record_battle_settlement(
                battle_id=8004, player_id=1001, season_id=1, team_size=1,
                status=RANKED_MATCH_RATING_CONFIRMED, outcome=2, player_team_index=0,
                total_rounds=6, participated=True, fled=False,
                rating_before_centi=155_600, rating_after_centi=154_000,
            )

            # GetRankedBattleResult（指定 battle_id=8001 与最新 battle_id=0）
            r_b1 = await _call(
                c, p, res_path, p.message("match.GetRankedBattleResultRequest", battle_id=8001),
                "match.GetRankedBattleResultResponse", metadata=md(1001, res_path),
            )
            assert r_b1.error_message.id == 0 and r_b1.result.newly_qualified is True and r_b1.result.rating_delta_centi == 1600

            r_latest = await _call(
                c, p, res_path, p.message("match.GetRankedBattleResultRequest", battle_id=0),
                "match.GetRankedBattleResultResponse", metadata=md(1001, res_path),
            )
            assert r_latest.error_message.id == 0 and r_latest.result.battle_id == 8004
            assert r_latest.result.season_valid_matches_after == 4
            assert len(r_latest.result.newly_unlocked_rewards) == 1
            assert r_latest.result.newly_unlocked_rewards[0].item_code == "s1_avatar_pattern"

            # 佩戴已解锁的头像纹样成功
            ok_cos = p.message("match.UpdateRankedCosmeticRequest")
            ok_cos.loadout.equipped_avatar_pattern_code = "s1_avatar_pattern"
            r_cos_ok = await _call(
                c, p, cos_path, ok_cos, "match.UpdateRankedCosmeticResponse", metadata=md(1001, cos_path),
            )
            assert r_cos_ok.error_message.id == 0
            assert r_cos_ok.loadout.equipped_avatar_pattern_code == "s1_avatar_pattern"

            # 再查 GetRankedProfile（含往季记录）验证进度、最高段位、权益与佩戴均持久
            await store.upsert_ranked_season(
                RankedSeasonRow(
                    season_id=0, season_key="s0", season_name="季前测试", rank_generation=1,
                    source_epoch=1, rule_version=1, state=RANKED_SEASON_CLOSED, rollover_stage=8,
                    start_ms=1000, end_ms=2000,
                )
            )
            prof2 = await _call(
                c, p, prof_path, p.message("match.GetRankedProfileRequest", include_history=True),
                "match.GetRankedProfileResponse", metadata=md(1001, prof_path),
            )
            assert prof2.current_progress.qualified is True
            assert prof2.current_progress.valid_matches == 4 and prof2.current_progress.wins == 3 and prof2.current_progress.losses == 1
            assert prof2.current_progress.current_tier == RANKED_TIER_QINGFENG
            assert prof2.current_progress.peak_tier == RANKED_TIER_YUNZHANG
            assert prof2.cosmetic_loadout.equipped_avatar_pattern_code == "s1_avatar_pattern"
            assert any(e.kind == RANKED_REWARD_AVATAR_PATTERN and e.item_code == "s1_avatar_pattern" for e in prof2.entitlements)
            assert len(prof2.past_seasons) == 1 and prof2.past_seasons[0].rank_board_sealed is True

            # 5) ClientPlayerTeam.ConfirmRankedPreset 全链路校验
            create_path = "/teampb.ClientPlayerTeam/CreateTeam"
            invite_path = "/teampb.ClientPlayerTeam/InviteToTeam"
            respond_path = "/teampb.ClientPlayerTeam/RespondInvite"
            confirm_path = "/teampb.ClientPlayerTeam/ConfirmRankedPreset"
            kick_path = "/teampb.ClientPlayerTeam/KickMember"

            # 无队成员确认被拒
            r_noteam = await _call(
                c, p, confirm_path,
                p.message("teampb.ConfirmRankedPresetRequest", expected_team_id=1, character_preset_id=101, pet_preset_id=203, rule_version=2, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1001, confirm_path),
            )
            assert r_noteam.error_message.id == ERR_TEAM_NO_TEAM

            # 1001 建队并拉入 1002
            r_ct = await _call(c, p, create_path, p.message("teampb.CreateTeamRequest", request_id=str(uuid.uuid4())), "teampb.TeamResponse", metadata=md(1001, create_path))
            tid = r_ct.team.team_id
            await _call(c, p, invite_path, p.message("teampb.InviteToTeamRequest", target_player_id=1002, expected_team_id=tid, request_id=str(uuid.uuid4())), "teampb.TeamResponse", metadata=md(1001, invite_path))
            await _call(c, p, respond_path, p.message("teampb.RespondInviteRequest", team_id=tid, accept=True, request_id=str(uuid.uuid4())), "teampb.TeamResponse", metadata=md(1002, respond_path))

            # 非法预设 / 规则版本不符拒绝
            r_bad_tp = await _call(
                c, p, confirm_path,
                p.message("teampb.ConfirmRankedPresetRequest", expected_team_id=tid, character_preset_id=101, pet_preset_id=999, rule_version=2, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1001, confirm_path),
            )
            assert r_bad_tp.error_message.id == ERR_TEAM_RANKED_PRESET_INVALID

            r_bad_tv = await _call(
                c, p, confirm_path,
                p.message("teampb.ConfirmRankedPresetRequest", expected_team_id=tid, character_preset_id=101, pet_preset_id=203, rule_version=1, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1001, confirm_path),
            )
            assert r_bad_tv.error_message.id == ERR_TEAM_RANKED_PRESET_INVALID

            # 1001 与 1002 各自确认 -> ranked_presets_all_confirmed 变为 True
            c1_req_id = str(uuid.uuid4())
            r_c1 = await _call(
                c, p, confirm_path,
                p.message("teampb.ConfirmRankedPresetRequest", expected_team_id=tid, character_preset_id=102, pet_preset_id=201, rule_version=2, request_id=c1_req_id),
                "teampb.TeamResponse", metadata=md(1001, confirm_path),
            )
            assert r_c1.error_message.id == 0 and r_c1.team.ranked_presets_all_confirmed is False
            assert r_c1.team.ranked_season_id == 1 and r_c1.team.ranked_rule_version == 2

            r_c2 = await _call(
                c, p, confirm_path,
                p.message("teampb.ConfirmRankedPresetRequest", expected_team_id=tid, character_preset_id=103, pet_preset_id=203, rule_version=2, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1002, confirm_path),
            )
            assert r_c2.error_message.id == 0 and r_c2.team.ranked_presets_all_confirmed is True
            by_pid = {m.player_id: m for m in r_c2.team.members}
            assert by_pid[1001].ranked_character_preset_id == 102 and by_pid[1001].ranked_preset_confirmed is True
            assert by_pid[1002].ranked_character_preset_id == 103 and by_pid[1002].ranked_preset_confirmed is True

            # 名单变动（踢出 1002）自动清空留存成员 1001 的旧确认态
            r_kicked = await _call(
                c, p, kick_path,
                p.message("teampb.KickMemberRequest", target_player_id=1002, expected_team_id=tid, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1001, kick_path),
            )
            assert r_kicked.error_message.id == 0 and r_kicked.team.ranked_presets_all_confirmed is False
            assert r_kicked.team.members[0].ranked_preset_confirmed is False

            # 赛季进入 SETTLING 态时，SaveRankedPreset 与 ConfirmRankedPreset 均返回 SETTLING
            s1 = await store.get_active_ranked_season()
            await store.upsert_ranked_season(
                RankedSeasonRow(
                    season_id=s1.season_id, season_key=s1.season_key, season_name=s1.season_name,
                    rank_generation=s1.rank_generation, source_epoch=s1.source_epoch,
                    rule_version=s1.rule_version, state=RANKED_SEASON_SETTLING, rollover_stage=1,
                    start_ms=s1.start_ms, end_ms=s1.end_ms,
                )
            )
            r_settle = await _call(
                c, p, save_path,
                p.message("match.SaveRankedPresetRequest", character_preset_id=101, pet_preset_id=203, rule_version=1),
                "match.SaveRankedPresetResponse", metadata=md(1001, save_path),
            )
            assert r_settle.error_message.id == ERR_MATCH_RANKED_SEASON_SETTLING

    asyncio.run(run())


# -- 4. 配置环境变量 fail-closed 与 SQL Schema DDL 完整性校验 ----------------------------------------


def test_config_secrets_and_new_schema_ddl():
    base_env = {
        "MMORPG_MATCH_OUTCOME_SECRET_BATTLE": "a" * 32,
        "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE": "b" * 32,
        "MMORPG_ROLE_FENCE_SECRET_COORDINATOR": "c" * 32,
        OPS_CALLER_SECRET_ENV: "d" * 32,
        RANK_PRODUCER_SECRET_ENV: "e" * 32,
    }
    cfg = MatchConfig.from_settings(Settings(), environ=base_env)
    assert cfg.ops_secret == b"d" * 32
    assert cfg.rank_secret == b"e" * 32

    # 过短密钥必须 fail-closed 拒绝启动
    with pytest.raises(ValueError, match=OPS_CALLER_SECRET_ENV):
        MatchConfig.from_settings(Settings(), environ={**base_env, OPS_CALLER_SECRET_ENV: "too-short"})
    with pytest.raises(ValueError, match=RANK_PRODUCER_SECRET_ENV):
        MatchConfig.from_settings(Settings(), environ={**base_env, RANK_PRODUCER_SECRET_ENV: "too-short"})

    # 全部新增 13 张表必须同时在 REQUIRED_TABLES 与 config/match_schema.sql 中存在
    new_tables = (
        "match_battle_script",
        "match_battle_script_review",
        "match_battle_script_loadout",
        "match_rating",
        "match_rating_applied",
        "match_rank_source",
        "match_rank_outbox",
        "match_rank_removal",
        "match_ranked_season",
        "match_ranked_player_prefs",
        "match_ranked_player_season",
        "match_ranked_reward",
        "match_ranked_battle",
    )
    ddl = (Path(__file__).resolve().parents[1] / "config" / "match_schema.sql").read_text(encoding="utf-8")
    for tbl in new_tables:
        assert tbl in REQUIRED_TABLES, f"{tbl} missing from SqlMatchStore.REQUIRED_TABLES"
        assert re.search(rf"CREATE TABLE IF NOT EXISTS `{tbl}`", ddl), f"{tbl} missing from config/match_schema.sql"
