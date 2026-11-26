"""《群仙问道赛》V2/V3（506..508, 511..514）、CustomDuelRules 与 TournamentBattleContext 全链路测试。"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from test_monopoly import TOKEN, _enter, _settings
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata
from xuanming.login.caller_auth import CallerVerifier
from xuanming.match import (
    BattleNodeEntry,
    GatherPipeline,
    GrpcEndpointPool,
    MatchConfig,
    MatchGrpcService,
    Matchmaker,
    MemoryMatchStore,
    MemoryPresenceProvider,
    PlayerPresence,
    SpectateCheerHub,
    SpectateManager,
    StaticBattleNodeResolver,
    TournamentCatalog,
    TournamentManager,
)
from xuanming.match.errors import (
    ERR_INVALID_PARAMETER,
    ERR_MATCH_BATTLE_REPORT_NOT_FOUND,
    ERR_MATCH_RANKED_PRESET_INVALID,
    ERR_MATCH_TOURNAMENT_ALREADY_SALUTED_TODAY,
    ERR_MATCH_TOURNAMENT_EMBLEM_INVALID,
    ERR_MATCH_TOURNAMENT_NO_CHAMPION_TO_SALUTE,
    ERR_MATCH_TOURNAMENT_NOT_CAPTAIN,
    ERR_MATCH_TOURNAMENT_PREDICTION_CLOSED,
    ERR_MATCH_TOURNAMENT_PREDICTION_TARGET_INVALID,
    ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_COOLDOWN,
    ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_INVALID,
    ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE,
)
from xuanming.match.model import (
    MODE_PVE_SOLO,
    MODE_SEASON_TOURNAMENT,
    SPECTATE_VISIBILITY_PRIVATE,
    SPECTATE_VISIBILITY_PUBLIC,
)
from xuanming.match.tournament import (
    LEYLINE_FIRE,
    LEYLINE_WATER,
    MATCH_FINISHED,
    MATCH_IN_BATTLE,
    MATCH_INTERMISSION,
    MATCH_WAITING_OPPONENT,
    MILLIS_PER_DAY,
    OUTCOME_SIDE_A_WIN,
    OUTCOME_SIDE_B_WIN,
    PHASE_WARMUP,
    PREDICTION_HIT,
    PREDICTION_MISS,
    REWARD_TIER_PREDICTION,
    REWARD_TIER_THIRD_PLACE,
    REWARD_TIER_TOP4,
    STAGE_KNOCKOUT,
    STAGE_QUALIFIER,
    STAGE_SETTLED,
    best_of_for_knockout,
    compute_match_mvp,
    decode_leyline_sequence,
    draw_leylines,
    encode_leyline_sequence,
    is_valid_emblem_id,
    leyline_for_game,
    normalize_emblem_id,
    prediction_weight_for_round,
    utc8_day_key,
)
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime


# ── 1. V2/V3 纯规则核与 Go rules_test.go 逐条对齐 ─────────────────────────────────────────────────────


def test_tournament_v2_v3_pure_rules_and_cheer_hub():
    # 16 强：R1/R2 为 BO1；R3(半决赛)/R4(决赛) 为 BO3；2 强决赛为 BO3
    assert best_of_for_knockout(16, 1, True) == 1
    assert best_of_for_knockout(16, 2, True) == 1
    assert best_of_for_knockout(16, 3, True) == 3
    assert best_of_for_knockout(16, 4, True) == 3
    assert best_of_for_knockout(16, 4, False) == 1
    assert best_of_for_knockout(2, 1, True) == 3

    # 五行灵脉抽签确定性且 3 局互不重复
    seq1 = draw_leylines(1, 900001, 3)
    seq2 = draw_leylines(1, 900001, 3)
    assert len(seq1) == 3 and seq1 == seq2
    assert len(set(seq1)) == 3 and all(1 <= el <= 5 for el in seq1)
    assert decode_leyline_sequence(encode_leyline_sequence(seq1)) == seq1
    assert leyline_for_game(seq1, 1) == seq1[0]
    assert leyline_for_game(seq1, 3) == seq1[2]
    assert leyline_for_game(seq1, 4) == 0

    # UTC+8 自然日键跨日检验（2026-11-25 15:59:59 UTC == 23:59:59 UTC+8）
    assert utc8_day_key(1_795_622_399_000) == 20261125
    assert utc8_day_key(1_795_622_401_000) == 20261126

    # 队徽白名单 1..8，0 归一化为 1
    assert normalize_emblem_id(0) == (1, True)
    assert normalize_emblem_id(8) == (8, True)
    assert normalize_emblem_id(9) == (0, False)
    assert is_valid_emblem_id(1) and not is_valid_emblem_id(0) and not is_valid_emblem_id(9)

    # 神机预测轮次权重：十六强=1, 八强=2, 半决赛=3, 季军战=3, 总决赛=5
    assert prediction_weight_for_round(16, 1, False) == 1
    assert prediction_weight_for_round(16, 2, False) == 2
    assert prediction_weight_for_round(16, 3, False) == 3
    assert prediction_weight_for_round(16, 4, False) == 5
    assert prediction_weight_for_round(16, 4, True) == 3

    # 问鼎之星 MVP 评分与确定性平局决胜
    proto = Protocol()
    s1 = proto.message(
        "BattleSettlementData",
        player_id=101,
        player_team_index=0,
        total_damage_dealt=12_000,
        total_healing_done=4_000,
        total_controls_landed=2,
        is_dead=False,
        fled=False,
    )
    s2 = proto.message(
        "BattleSettlementData",
        player_id=102,
        player_team_index=1,
        total_damage_dealt=12_000,
        total_healing_done=4_000,
        total_controls_landed=2,
        is_dead=False,
        fled=False,
    )
    mvp = compute_match_mvp([s2, s1], winner_team_index=0)
    assert mvp is not None
    # 12000//100 + 4000//80 + 2*60 + 120(alive) + 180(win) = 120 + 50 + 120 + 120 + 180 = 590
    assert mvp.player_id == 101 and mvp.score == 590

    # SpectateCheerHub 白名单、5 秒冷却、增量拉取
    hub = SpectateCheerHub()
    _, _, invalid = hub.send(777001, 1001, "青云子", 0, 0, 10_000)
    assert invalid
    _, _, invalid = hub.send(777001, 1001, "青云子", 7, 0, 10_000)
    assert invalid
    _, _, invalid = hub.send(777001, 1001, "青云子", 3, 2, 10_000)
    assert invalid

    snap, cd, invalid = hub.send(777001, 1001, "青云子", 3, 0, 10_000)
    assert not cd and not invalid and snap.count_a == 1 and snap.count_b == 0
    assert snap.cooldown_until_ms == 15_000 and len(snap.recent_cheers) == 1 and snap.recent_cheers[0].seq == 1

    snap_cd, cd, invalid = hub.send(777001, 1001, "青云子", 6, 0, 12_000)
    assert cd and not invalid and snap_cd.count_a == 1

    snap2, cd, invalid = hub.send(777001, 1002, "   ", 6, 1, 13_000)
    assert not cd and not invalid and snap2.count_a == 1 and snap2.count_b == 1
    assert snap2.recent_cheers[0].sender_name == "观战道友"

    delta = hub.snapshot(777001, 1001, 1, 15_000)
    assert delta.cooldown_until_ms == 0 and len(delta.recent_cheers) == 1 and delta.recent_cheers[0].seq == 2


# ── 2. 四战队完整 V2/V3 锦标赛全链路（队徽/热身/BO3/局间自动开局/季军战/MVP/竞猜/观道/观战喝彩/公开回放）──


@asynccontextmanager
async def _v3_tournament_harness(clock_ref: list[int]):
    protocol = Protocol()
    key = Ed25519PrivateKey.generate()
    verifier = CallerVerifier([key.public_key()], allowed_callers=("gate",))
    store = MemoryMatchStore(protocol, clock_ms=lambda: clock_ref[0])
    config = MatchConfig(require_network_probe=False)
    presence = MemoryPresenceProvider()
    nodes = StaticBattleNodeResolver()
    rpc_pool = GrpcEndpointPool()
    gather = GatherPipeline(protocol, store, config, rpc_pool, presence, nodes)
    spectate = SpectateManager(protocol, store, config, rpc_pool, presence)
    tournament = TournamentManager(
        protocol,
        catalog=TournamentCatalog.default_fallback(),
        clock_ms=lambda: clock_ref[0],
    )
    match_svc = MatchGrpcService(
        protocol,
        store,
        config,
        gate_verifier=verifier,
        presence=presence,
        battle_nodes=nodes,
        gather=gather,
        spectate=spectate,
        tournament=tournament,
        clock_ms=lambda: clock_ref[0],
    )
    server, port = await serve_grpc(protocol, "127.0.0.1:0", match_svc.services(), pass_context=True)
    client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)
    try:
        async def call(player_id: int, method: str, **fields):
            path = f"/match.MatchService/{method}"
            req = protocol.message(f"match.{method}Request", **fields)
            details = protocol.message(
                "SessionDetails",
                session_id=(1 << 17) | player_id,
                player_id=player_id,
                gate_node_id=1,
                gate_instance_id="gate-v3",
                session_incarnation=100 + player_id,
                owner_revision=1,
                authenticated_account=f"Immortal{player_id}",
            )
            meta = session_metadata(details.SerializeToString(deterministic=True), path, key)
            raw = await client.call(path, req.SerializeToString(deterministic=True), metadata=meta)
            return protocol.parse(f"match.{method}Response", raw)

        yield protocol, store, tournament, call
    finally:
        await client.close()
        await match_svc.close()
        await server.stop(0)
        await rpc_pool.close()


def test_tournament_v3_full_four_squad_lifecycle_grpc():
    async def exercise():
        base_ms = 1_795_622_399_000  # 2026-11-25 23:59:59 UTC+8
        clock = [base_ms]
        async with _v3_tournament_harness(clock) as (protocol, store, tm, call):
            # 1. 尚无冠军时观道致敬被拒
            salute0 = await call(901, "SaluteTournamentChampion")
            assert salute0.error_message.id == ERR_MATCH_TOURNAMENT_NO_CHAMPION_TO_SALUTE

            # 2. 建 4 支 3 人战队 + 校验队徽白名单与更换（511）
            bad_emblem = await call(101, "CreateTournamentSquad", squad_name="越界队徽", emblem_id=9)
            assert bad_emblem.error_message.id == ERR_MATCH_TOURNAMENT_EMBLEM_INVALID

            squads: list[tuple[int, int]] = []  # (captain_id, squad_id)
            names_and_emblems = [
                (101, "青云剑宗", 0),   # 0 归一化为 1
                (201, "太极玄门", 2),
                (301, "赤炎丹阁", 3),
                (401, "紫霄雷宫", 6),
            ]
            for cap, name, emblem in names_and_emblems:
                res = await call(cap, "CreateTournamentSquad", squad_name=name, emblem_id=emblem)
                assert not res.error_message.id
                assert res.squad.emblem_id == (1 if emblem == 0 else emblem)
                sid = res.squad.squad_id
                for offset in (1, 2):
                    pid = cap + offset
                    assert not (await call(cap, "InviteTournamentMember", target_player_id=pid)).error_message.id
                    assert not (await call(pid, "RespondTournamentInvite", squad_id=sid, accept=True)).error_message.id
                squads.append((cap, sid))

            # 队长改队徽成功，非队长与非法 ID 拒绝
            assert (await call(102, "UpdateTournamentSquadEmblem", emblem_id=5)).error_message.id == ERR_MATCH_TOURNAMENT_NOT_CAPTAIN
            assert (await call(101, "UpdateTournamentSquadEmblem", emblem_id=0)).error_message.id == ERR_MATCH_TOURNAMENT_EMBLEM_INVALID
            upd = await call(101, "UpdateTournamentSquadEmblem", emblem_id=7)
            assert not upd.error_message.id and upd.squad.emblem_id == 7

            # 3. 模块 D：报名期零积分热身约战（512）定向撮合 + 自选灵脉 + 零污染验证
            bad_self_wu = await call(101, "StartTournamentWarmup", target_squad_id=squads[0][1])
            assert bad_self_wu.error_message.id == ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE
            wu1 = await call(101, "StartTournamentWarmup", target_squad_id=squads[1][1], leyline_element=0)
            assert not wu1.error_message.id and wu1.battle_id == 0 and wu1.match.phase == PHASE_WARMUP
            wu2 = await call(201, "StartTournamentWarmup", target_squad_id=squads[0][1], leyline_element=LEYLINE_FIRE)
            assert not wu2.error_message.id and wu2.battle_id > 0 and wu2.match.current_leyline_element == LEYLINE_FIRE
            wu_ctx = tm.battle_context_for(wu2.battle_id)
            assert wu_ctx is not None and wu_ctx.is_warmup and wu_ctx.leyline_element == LEYLINE_FIRE
            assert tm.apply_battle_result(wu2.battle_id, OUTCOME_SIDE_A_WIN)
            assert not tm.apply_battle_result(wu2.battle_id, OUTCOME_SIDE_A_WIN)  # 幂等去重
            # 热身绝不污染海选积分与场次
            ov_a = await call(101, "GetTournamentOverview")
            assert ov_a.my_squad.qualifier_played == 0 and ov_a.my_squad.qualifier_points == 0

            # 4. 锁定 4 支战队报名 -> 海选阶段 -> 淘汰赛阶段（4 强 = 2 场半决赛 BO3 + 决赛 BO3 + 季军战 BO3）
            for cap, _ in squads:
                assert not (await call(cap, "RegisterTournamentSquad")).error_message.id

            clock[0] = base_ms + 42 * MILLIS_PER_DAY
            assert tm.advance_season() == STAGE_QUALIFIER
            # 打一轮海选区分种子：101>201, 301>401
            assert (await call(101, "StartTournamentMatch")).battle_id == 0
            q1 = await call(201, "StartTournamentMatch")
            assert q1.battle_id > 0 and tm.apply_battle_result(q1.battle_id, OUTCOME_SIDE_A_WIN)
            assert (await call(301, "StartTournamentMatch")).battle_id == 0
            q2 = await call(401, "StartTournamentMatch")
            assert q2.battle_id > 0 and tm.apply_battle_result(q2.battle_id, OUTCOME_SIDE_A_WIN)

            clock[0] = base_ms + 49 * MILLIS_PER_DAY
            assert tm.advance_season() == STAGE_KNOCKOUT
            # 淘汰赛期热身约战不可用
            assert (await call(101, "StartTournamentWarmup")).error_message.id == ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE

            st_ko = await call(901, "ListTournamentStandings")
            bracket = list(st_ko.knockout_bracket)
            assert len(bracket) == 4  # 2 半决赛 + 1 总决赛 + 1 季军战
            sf1, sf2, final_m, third_m = bracket[0], bracket[1], bracket[2], bracket[3]
            assert sf1.best_of == 3 and len(sf1.leyline_sequence) == 3
            assert not final_m.is_third_place and final_m.slot_index == 0
            assert third_m.is_third_place and third_m.slot_index == 1 and third_m.best_of == 3

            # 5. 模块 E：观众 901/902 在开战前免费预测（513），开战后封盘拒绝
            bad_target = await call(901, "PredictTournamentMatch", match_id=sf1.match_id, predicted_squad_id=999999)
            assert bad_target.error_message.id == ERR_MATCH_TOURNAMENT_PREDICTION_TARGET_INVALID
            p1 = await call(901, "PredictTournamentMatch", match_id=sf1.match_id, predicted_squad_id=sf1.squad_a_id)
            assert not p1.error_message.id and p1.predict_count_a == 1 and p1.predict_count_b == 0
            p2 = await call(902, "PredictTournamentMatch", match_id=sf1.match_id, predicted_squad_id=sf1.squad_b_id)
            assert not p2.error_message.id and p2.predict_count_a == 1 and p2.predict_count_b == 1
            # 901 再预测半决赛 2
            assert not (await call(901, "PredictTournamentMatch", match_id=sf2.match_id, predicted_squad_id=sf2.squad_a_id)).error_message.id

            # 6. 半决赛 1 开打（BO3 + 观战喝彩 507/508 + 局间换阵自动开局 + MVP + 公开回放）
            cap_of = {sid: cap for cap, sid in squads}
            assert (await call(cap_of[sf1.squad_a_id], "StartTournamentMatch", match_id=sf1.match_id)).battle_id == 0
            sf1_g1 = await call(cap_of[sf1.squad_b_id], "StartTournamentMatch", match_id=sf1.match_id)
            assert not sf1_g1.error_message.id and sf1_g1.battle_id > 0

            # 开战后修改预测被封盘拒绝
            closed_pred = await call(901, "PredictTournamentMatch", match_id=sf1.match_id, predicted_squad_id=sf1.squad_b_id)
            assert closed_pred.error_message.id == ERR_MATCH_TOURNAMENT_PREDICTION_CLOSED

            # 观战预设喝彩 507 + 冷却 + 508 增量拉取 + 伪造 battle_id 拒绝
            assert (await call(901, "SendTournamentSpectateCheer", battle_id=555555, phrase_id=1, side=0)).error_message.id == ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_INVALID
            ch1 = await call(901, "SendTournamentSpectateCheer", battle_id=sf1_g1.battle_id, phrase_id=3, side=0)
            assert not ch1.error_message.id and ch1.count_a == 1 and len(ch1.recent_cheers) == 1
            ch_cd = await call(901, "SendTournamentSpectateCheer", battle_id=sf1_g1.battle_id, phrase_id=4, side=0)
            assert ch_cd.error_message.id == ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_COOLDOWN and ch_cd.count_a == 1
            ch2 = await call(902, "SendTournamentSpectateCheer", battle_id=sf1_g1.battle_id, phrase_id=6, side=1)
            assert not ch2.error_message.id and ch2.count_a == 1 and ch2.count_b == 1
            pulled = await call(901, "GetTournamentSpectateCheer", battle_id=sf1_g1.battle_id, after_seq=1)
            assert not pulled.error_message.id and len(pulled.recent_cheers) == 1 and pulled.recent_cheers[0].phrase_id == 6

            # 公开淘汰赛战报回放校验（非参赛观众 901 可读淘汰赛回放，不可读非淘汰赛私密回放）
            from xuanming.match.store import _MemReportRow

            env = protocol.message("BattleReplayEnvelope", format_version=1, raw_bytes=6, payload=b"replay")
            store._state.reports[sf1_g1.battle_id] = _MemReportRow(
                battle_id=sf1_g1.battle_id,
                created_ms=clock[0],
                match_mode=MODE_SEASON_TOURNAMENT,
                battle_config_id=5,
                envelope=env.SerializeToString(deterministic=True),
                envelope_digest=b"d",
                ref_count=1,
            )
            store._state.reports[888888] = _MemReportRow(
                battle_id=888888,
                created_ms=clock[0],
                match_mode=1,
                battle_config_id=1,
                envelope=env.SerializeToString(deterministic=True),
                envelope_digest=b"d",
                ref_count=1,
            )
            pub_rep = await call(901, "GetBattleReport", battle_id=sf1_g1.battle_id)
            assert not pub_rep.error_message.id
            priv_rep = await call(901, "GetBattleReport", battle_id=888888)
            assert priv_rep.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND

            # 半决赛 1 第 1 局 A 胜 -> 切入 60 秒局间换阵 INTERMISSION 并推进至第 2 局灵脉
            assert tm.apply_battle_result(sf1_g1.battle_id, OUTCOME_SIDE_A_WIN)
            st_inter = await call(901, "ListTournamentStandings")
            sf1_view = next(m for m in st_inter.knockout_bracket if m.match_id == sf1.match_id)
            assert sf1_view.state == MATCH_INTERMISSION and sf1_view.wins_a == 1 and sf1_view.current_game_index == 2
            assert sf1_view.current_leyline_element == sf1.leyline_sequence[1]

            # 推进 61 秒触发局间到期自动开第 2 局
            clock[0] += 61_000
            auto_launched = tm.take_due_intermission_launches()
            assert len(auto_launched) == 1
            _, sf1_g2_bid, sf1_g2_ctx = auto_launched[0]
            assert sf1_g2_ctx.game_index == 2 and sf1_g2_ctx.wins_a == 1 and sf1_g2_ctx.leyline_element == sf1.leyline_sequence[1]
            assert tm.apply_battle_result(sf1_g2_bid, OUTCOME_SIDE_A_WIN)

            # 7. 半决赛 2：A 连胜 2 局（双方手动确认第 2 局）
            for _ in range(2):
                assert (await call(cap_of[sf2.squad_a_id], "StartTournamentMatch", match_id=sf2.match_id)).battle_id == 0
                g = await call(cap_of[sf2.squad_b_id], "StartTournamentMatch", match_id=sf2.match_id)
                assert g.battle_id > 0 and tm.apply_battle_result(g.battle_id, OUTCOME_SIDE_A_WIN)

            # 校验半决赛败者已自动落入季军战，胜者已落入总决赛
            st_finals = await call(901, "ListTournamentStandings")
            final_v = next(m for m in st_finals.knockout_bracket if m.match_id == final_m.match_id)
            third_v = next(m for m in st_finals.knockout_bracket if m.match_id == third_m.match_id)
            assert final_v.squad_a_id == sf1.squad_a_id and final_v.squad_b_id == sf2.squad_a_id
            assert third_v.squad_a_id == sf1.squad_b_id and third_v.squad_b_id == sf2.squad_b_id

            # 901 追加预测总决赛与季军战
            assert not (await call(901, "PredictTournamentMatch", match_id=final_m.match_id, predicted_squad_id=final_v.squad_a_id)).error_message.id
            assert not (await call(901, "PredictTournamentMatch", match_id=third_m.match_id, predicted_squad_id=third_v.squad_b_id)).error_message.id

            # 8. 打完总决赛（附真实结算统计评出 MVP）
            mvp_cap = cap_of[final_v.squad_a_id]
            tm.sync_member_profiles([(mvp_cap, "青云剑尊", 2, 1, "app_sword_01")])
            final_settlement = protocol.message(
                "BattleSettlementData",
                player_id=mvp_cap,
                player_team_index=0,
                total_damage_dealt=45_000,
                total_healing_done=8_000,
                total_controls_landed=3,
            )
            for _ in range(2):
                assert (await call(cap_of[final_v.squad_a_id], "StartTournamentMatch", match_id=final_m.match_id)).battle_id == 0
                fg = await call(cap_of[final_v.squad_b_id], "StartTournamentMatch", match_id=final_m.match_id)
                assert fg.battle_id > 0 and tm.apply_battle_result(fg.battle_id, OUTCOME_SIDE_A_WIN, settlements=[final_settlement])

            # 季军战未打完时禁止提前封榜结算！
            assert tm.advance_season() == STAGE_KNOCKOUT

            # 9. 打完季军战（B 队胜 -> 季军第 3 名，A 队第 4 名）
            for _ in range(2):
                assert (await call(cap_of[third_v.squad_a_id], "StartTournamentMatch", match_id=third_m.match_id)).battle_id == 0
                tg = await call(cap_of[third_v.squad_b_id], "StartTournamentMatch", match_id=third_m.match_id)
                assert tg.battle_id > 0 and tm.apply_battle_result(tg.battle_id, OUTCOME_SIDE_B_WIN)

            # 决赛与季军战均完赛 -> 封榜结算！
            assert tm.advance_season() == STAGE_SETTLED

            # 10. 校验冠军/季军/MVP/真实外观快照、神机榜（514）与神机竞猜领奖、季军独立领奖
            ov_done = await call(901, "GetTournamentOverview", include_history=True)
            cs = ov_done.current_season
            assert cs.champion_squad_id == final_v.squad_a_id
            assert cs.champion_emblem_id == 7  # 之前队长 101 换过的队徽
            assert cs.third_place_squad_id == third_v.squad_b_id
            assert cs.final_mvp_player_id == mvp_cap and cs.final_mvp_player_name == "青云剑尊" and cs.final_mvp_score > 0
            assert any(m.player_id == mvp_cap and m.appearance_id == "app_sword_01" for m in cs.champion_members)

            board = await call(901, "GetTournamentPredictionBoard")
            assert not board.error_message.id
            assert board.my_hit_count == 4  # 4 场全中（2 半决赛 3+3 + 季军战 3 + 总决赛 5 = 14 分）
            assert board.my_total_points == 14 and board.my_rank == 1
            assert len(board.top_predictors) == 2 and board.top_predictors[0].player_id == 901
            assert all(e.result_state == PREDICTION_HIT for e in board.my_predictions)

            board902 = await call(902, "GetTournamentPredictionBoard")
            assert board902.my_hit_count == 0 and board902.my_predictions[0].result_state == PREDICTION_MISS

            # 观众 901 领取神机慧眼礼（tier 9）
            c_oracle = await call(901, "ClaimTournamentReward")
            assert not c_oracle.error_message.id and len(c_oracle.claimed_rewards) == 1
            assert c_oracle.claimed_rewards[0].tier_kind == REWARD_TIER_PREDICTION and c_oracle.claimed_rewards[0].gold_amount == 12000

            # 季军队长领第 3 名独立季军奖（tier 8, 65000），第 4 名领四强奖（tier 3, 50000）
            c_third = await call(cap_of[third_v.squad_b_id], "ClaimTournamentReward")
            assert c_third.claimed_rewards[0].tier_kind == REWARD_TIER_THIRD_PLACE and c_third.claimed_rewards[0].gold_amount == 65000
            c_fourth = await call(cap_of[third_v.squad_a_id], "ClaimTournamentReward")
            assert c_fourth.claimed_rewards[0].tier_kind == REWARD_TIER_TOP4 and c_fourth.claimed_rewards[0].gold_amount == 50000

            # 11. 模块 B：每日免费观道致敬（506）+ 同日防刷 + UTC+8 跨日刷新
            sal1 = await call(901, "SaluteTournamentChampion")
            assert not sal1.error_message.id and sal1.champion_salute_count == 1 and sal1.saluted_today and sal1.gold_amount == 5000
            assert len(sal1.items) == 1 and sal1.items[0].item_id == 20 and sal1.items[0].count == 3
            sal_dup = await call(901, "SaluteTournamentChampion")
            assert sal_dup.error_message.id == ERR_MATCH_TOURNAMENT_ALREADY_SALUTED_TODAY

            clock[0] += MILLIS_PER_DAY  # 次日 UTC+8 解冻
            sal2 = await call(901, "SaluteTournamentChampion")
            assert not sal2.error_message.id and sal2.champion_salute_count == 2

    asyncio.run(exercise())


# ── 3. CustomDuelRules / TournamentBattleContext / is_wuxing_tower 经 Gather 透传验证 ──────────────────


class _RecordingRpc:
    """记录 GatherPipeline 发出的 PrepareBattle / CreateBattle 请求原件并回显成功。"""

    def __init__(self, protocol: Protocol) -> None:
        self.protocol = protocol
        self.prepare_reqs: list = []
        self.create_reqs: list = []

    async def call(self, endpoint: str, path: str, data: bytes, *, timeout: float = 5.0) -> bytes:
        if path == "/scene_node.SceneNodeGrpc/PrepareBattle":
            req = self.protocol.parse("PrepareBattleRequest", data)
            self.prepare_reqs.append(req)
            resp = self.protocol.message("PrepareBattleResponse", table_fingerprint="fp-v3")
            resp.snapshot.player_id = int(req.player_id)
            resp.snapshot.player_name = f"Daoist{req.player_id}"
            resp.snapshot.class_id = 3
            resp.snapshot.gender = 2
            resp.snapshot.appearance_id = f"skin_{req.player_id}"
            return resp.SerializeToString(deterministic=True)
        if path == "/BattleNode/QueryBattleCreation":
            req = self.protocol.parse("QueryBattleCreationRequest", data)
            resp = self.protocol.message(
                "QueryBattleCreationResponse",
                target_instance_id=req.target_instance_id,
                supported_outcome_protocol_version=1,
            )
            return resp.SerializeToString(deterministic=True)
        if path == "/BattleNode/CreateBattle":
            req = self.protocol.parse("CreateBattleRequest", data)
            self.create_reqs.append(req)
            resp = self.protocol.message("CreateBattleResponse", adopted_outcome_protocol_version=1, room_count=1)
            return resp.SerializeToString(deterministic=True)
        return self.protocol.message("Empty").SerializeToString(deterministic=True)

    async def close(self) -> None:
        return None


def test_custom_duel_rules_tournament_context_and_wuxing_tower_forwarding():
    async def exercise():
        protocol = Protocol()
        key = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([key.public_key()], allowed_callers=("gate",))
        store = MemoryMatchStore(protocol)
        config = MatchConfig(require_network_probe=False)
        presence = MemoryPresenceProvider()
        for pid in (1001, 1002, 1003):
            presence.set_presence(PlayerPresence(player_id=pid, online=True, scene_grpc_endpoint="scene-1:9001"))
        nodes = StaticBattleNodeResolver(
            [BattleNodeEntry(node_id=10, node_uuid="battle-uuid-1", grpc_endpoint="battle-1:9100", physical_region=1)]
        )
        rpc = _RecordingRpc(protocol)
        gather = GatherPipeline(protocol, store, config, rpc, presence, nodes)
        spectate = SpectateManager(protocol, store, config, rpc, presence)
        tm = TournamentManager(protocol, catalog=TournamentCatalog.default_fallback())
        svc = MatchGrpcService(
            protocol,
            store,
            config,
            gate_verifier=verifier,
            presence=presence,
            battle_nodes=nodes,
            gather=gather,
            spectate=spectate,
            tournament=tm,
            inline_gather=True,
        )
        server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)
        try:
            async def call(player_id: int, method: str, req_msg):
                path = f"/match.MatchService/{method}"
                details = protocol.message(
                    "SessionDetails",
                    session_id=(1 << 17) | player_id,
                    player_id=player_id,
                    gate_node_id=1,
                    gate_instance_id="gate-1",
                    session_incarnation=100 + player_id,
                    owner_revision=1,
                    authenticated_account=f"P{player_id}",
                )
                meta = session_metadata(details.SerializeToString(deterministic=True), path, key)
                raw = await client.call(path, req_msg.SerializeToString(deterministic=True), metadata=meta)
                return protocol.parse(f"match.{method}Response", raw)

            # 1. 非法回合上限（7 不在 {0,8,12,20}）与非法预设被拒绝
            bad_req = protocol.message("match.ChallengePlayerRequest", target_player_id=1002, battle_config_id=1)
            bad_req.custom_rules.round_limit_override = 7
            bad_resp = await call(1001, "ChallengePlayer", bad_req)
            assert bad_resp.error_message.id == ERR_INVALID_PARAMETER

            bad_preset_req = protocol.message("match.ChallengePlayerRequest", target_player_id=1002, battle_config_id=1)
            bad_preset_req.custom_rules.use_free_preset = True
            bad_preset_req.custom_rules.challenger_character_preset_id = 999
            bad_preset_resp = await call(1001, "ChallengePlayer", bad_preset_req)
            assert bad_preset_resp.error_message.id == ERR_MATCH_RANKED_PRESET_INVALID

            # 2. 合法自定义切磋（公平模板 + 公开观战 + 禁控 + 物法易势 + 8 回合）经 Gather 冻结至 Prepare & Create
            ok_req = protocol.message("match.ChallengePlayerRequest", target_player_id=1002, battle_config_id=5)
            ok_req.custom_rules.use_free_preset = True
            ok_req.custom_rules.enable_polarity_rotation = True
            ok_req.custom_rules.round_limit_override = 8
            ok_req.custom_rules.ban_control_skills = True
            ok_req.custom_rules.allow_public_spectate = True
            ok_req.custom_rules.challenger_character_preset_id = 102
            ok_req.custom_rules.challenger_pet_preset_id = 201
            ch = await call(1001, "ChallengePlayer", ok_req)
            assert not ch.error_message.id and ch.challenge_id > 0

            rc_req = protocol.message(
                "match.RespondChallengeRequest",
                challenge_id=ch.challenge_id,
                accept=True,
                responder_character_preset_id=103,
                responder_pet_preset_id=202,
            )
            rc = await call(1002, "RespondChallenge", rc_req)
            assert not rc.error_message.id
            assert len(rpc.create_reqs) == 1
            creq = rpc.create_reqs[0]
            assert creq.spectate_visibility == SPECTATE_VISIBILITY_PUBLIC
            assert creq.custom_duel_rules.use_free_preset and creq.custom_duel_rules.ban_control_skills
            assert creq.custom_duel_rules.round_limit_override == 8 and creq.custom_duel_rules.enable_polarity_rotation
            assert creq.ranked_context.season_id == 0 and creq.ranked_context.use_free_preset
            presets_map = {
                int(m.player_id): (int(m.presets.character_preset_id), int(m.presets.pet_preset_id))
                for m in creq.ranked_context.members
            }
            assert presets_map == {1001: (102, 201), 1002: (103, 202)}
            assert all(pr.custom_duel_rules.round_limit_override == 8 for pr in rpc.prepare_reqs)

            # 3. TournamentBattleContext 冻结透传 + 成员真实外观自动回填
            sq_res = await call(1001, "CreateTournamentSquad", protocol.message("match.CreateTournamentSquadRequest", squad_name="灵脉问道", emblem_id=4))
            assert not sq_res.error_message.id
            tctx = protocol.message(
                "TournamentBattleContext",
                season_id=1,
                match_id=777,
                best_of=3,
                game_index=2,
                wins_a=1,
                wins_b=0,
                leyline_element=LEYLINE_WATER,
                squad_a_id=sq_res.squad.squad_id,
                squad_a_name="灵脉问道",
                squad_a_emblem_id=4,
                is_third_place=True,
            )
            tk_a = protocol.message("match.MatchAuthorityTicketRecord", player_id=1001, ticket="tk-t-1", state=1, expires_ms=9_999_999_999_999)
            store._state.tickets[1001] = tk_a
            f_tourney = await store.claim_formation(
                mode=MODE_SEASON_TOURNAMENT,
                config_id=5,
                members=[(1001, "tk-t-1", 0, True)],
                physical_region=1,
                target_node_id=10,
                target_instance_id="battle-uuid-1",
                target_endpoint="battle-1:9100",
                release_track="stable",
                battle_deadline_ms=60_000,
                requeue_on_failure=False,
                tournament_context=tctx.SerializeToString(deterministic=True),
            )
            assert await gather.run_formation(f_tourney)
            assert rpc.create_reqs[-1].tournament_context.leyline_element == LEYLINE_WATER
            assert rpc.create_reqs[-1].tournament_context.is_third_place
            # Gather 已顺手回填成员 1001 真实外观
            ov_after = await call(1001, "GetTournamentOverview", protocol.message("match.GetTournamentOverviewRequest"))
            assert ov_after.my_squad.members[0].appearance_id == "skin_1001" and ov_after.my_squad.members[0].class_id == 3

            # 4. JoinQueue(is_wuxing_tower=True) 透传至 PrepareBattleRequest
            jq_req = protocol.message(
                "match.JoinQueueRequest",
                mode=MODE_PVE_SOLO,
                battle_config_id=101,
                request_id=str(uuid.uuid4()),
                is_wuxing_tower=True,
            )
            jq = await call(1003, "JoinQueue", jq_req)
            assert jq.error_code == 0
            mm = Matchmaker(protocol, store, config, gather, nodes)
            formed = await mm.run_once()
            assert len(formed) == 1
            assert rpc.prepare_reqs[-1].player_id == 1003 and rpc.prepare_reqs[-1].is_wuxing_tower is True
            # 普通单人 PVE 切磋保持私密
            assert rpc.create_reqs[-1].spectate_visibility == SPECTATE_VISIBILITY_PRIVATE
        finally:
            await client.close()
            await svc.close()
            await server.stop(0)

    asyncio.run(exercise())


# ── 4. 真实 Gate TCP 帧链路冒烟：通过 Runtime("local") 实调全部新增 Tournament V3 RPC（506..508, 511..514）──


async def _gate_call(client, call_id: int, message_name: str, **fields):
    p = client.protocol
    mid = p.message_ids[message_name]
    req = p.message(p.routes[mid].request_type, **fields)
    return await client.rpc(mid, call_id, req)


def test_real_gate_tcp_routes_all_new_tournament_v3_rpcs(tmp_path):
    async def exercise():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime, account="v3alpha") as (client_a, _player_a):
                async with _enter(runtime, account="v3beta") as (client_b, _player_b):
                    # 建队（带 V3 emblem_id=4）
                    sq = await _gate_call(
                        client_a, 101, "MatchServiceCreateTournamentSquad", squad_name="玄冥问道", emblem_id=4
                    )
                    assert not sq.error_message.id and sq.squad.emblem_id == 4

                    # 511: UpdateTournamentSquadEmblem
                    upd = await _gate_call(client_a, 102, "MatchServiceUpdateTournamentSquadEmblem", emblem_id=8)
                    assert not upd.error_message.id and upd.squad.emblem_id == 8

                    # 512: StartTournamentWarmup（1 人不足 3 人首发 -> 返回业务码，证明真实路由通畅）
                    wu = await _gate_call(
                        client_a, 103, "MatchServiceStartTournamentWarmup", target_squad_id=0, leyline_element=LEYLINE_FIRE
                    )
                    assert wu.error_message.id != 0

                    # 513: PredictTournamentMatch（报名期未进淘汰赛 -> 返回预测封盘码 16061）
                    pred = await _gate_call(
                        client_b, 104, "MatchServicePredictTournamentMatch", match_id=1, predicted_squad_id=sq.squad.squad_id
                    )
                    assert pred.error_message.id == ERR_MATCH_TOURNAMENT_PREDICTION_CLOSED

                    # 514: GetTournamentPredictionBoard
                    board = await _gate_call(client_b, 105, "MatchServiceGetTournamentPredictionBoard", season_id=0)
                    assert not board.error_message.id and board.season_id == 1 and board.min_hits_for_reward == 2

                    # 506: SaluteTournamentChampion（首季尚未产生冠军 -> 16057）
                    sal = await _gate_call(client_b, 106, "MatchServiceSaluteTournamentChampion", season_id=0)
                    assert sal.error_message.id == ERR_MATCH_TOURNAMENT_NO_CHAMPION_TO_SALUTE

                    # 507 & 508: SendTournamentSpectateCheer / GetTournamentSpectateCheer
                    send_bad = await _gate_call(
                        client_b, 107, "MatchServiceSendTournamentSpectateCheer", battle_id=123456, phrase_id=1, side=0
                    )
                    assert send_bad.error_message.id == ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_INVALID
                    get_ch = await _gate_call(
                        client_b, 108, "MatchServiceGetTournamentSpectateCheer", battle_id=123456, after_seq=0
                    )
                    assert not get_ch.error_message.id and get_ch.battle_id == 123456

    asyncio.run(exercise())
