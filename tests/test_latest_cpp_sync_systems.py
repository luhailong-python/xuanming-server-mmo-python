"""最新 C++ 同步特性全链路测试：7 张新配置表、群仙问道赛 11 条 RPC、技能制书与代打 6 条 RPC、瑞兽迎春 3 条 RPC。"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from itertools import count
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata
from xuanming.login.caller_auth import CallerVerifier
from xuanming.match import (
    GatherPipeline,
    GrpcEndpointPool,
    MatchConfig,
    MatchGrpcService,
    MemoryMatchStore,
    MemoryPresenceProvider,
    SpectateManager,
    StaticBattleNodeResolver,
    TournamentCatalog,
    TournamentManager,
)
from xuanming.match.errors import (
    ERR_MATCH_TOURNAMENT_ALREADY_CHEERED,
    ERR_MATCH_TOURNAMENT_ALREADY_IN_SQUAD,
    ERR_MATCH_TOURNAMENT_INVITE_NOT_FOUND,
    ERR_MATCH_TOURNAMENT_LINEUP_INVALID,
    ERR_MATCH_TOURNAMENT_MEMBER_NOT_ENOUGH,
    ERR_MATCH_TOURNAMENT_NOT_CAPTAIN,
    ERR_MATCH_TOURNAMENT_REWARD_UNAVAILABLE,
    ERR_MATCH_TOURNAMENT_SQUAD_LOCKED,
    ERR_MATCH_TOURNAMENT_SQUAD_NAME_INVALID,
    ERR_MATCH_TOURNAMENT_STAGE_CLOSED,
)
from xuanming.match.tournament import (
    DELIVERY_CLAIMED,
    MATCH_IN_BATTLE,
    MATCH_WAITING_OPPONENT,
    MILLIS_PER_DAY,
    OUTCOME_SIDE_A_WIN,
    SQUAD_CHAMPION,
    SQUAD_REGISTERED,
    STAGE_KNOCKOUT,
    STAGE_QUALIFIER,
    STAGE_REGISTRATION,
    STAGE_SETTLED,
)
from xuanming.nodes.battle import BattleError, TurnEngine
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_equip_craft import (
    CRAFTING_MATERIAL_ID,
    POLISH_SAND_ID,
    SceneEquipCraft,
)
from xuanming.nodes.scene_mail_outbox import SceneMailOutbox
from xuanming.nodes.scene_player import _fill_item
from xuanming.nodes.scene_festival_beast import (
    BLOCK_ALREADY_CLAIMED,
    BLOCK_NONE,
    BLOCK_PET_DEAD,
    BLOCK_PET_REQUIRED,
    CLAIM_ALREADY_CLAIMED,
    CLAIM_APPLIED,
    DAY_MS,
    FESTIVAL_BEAST_CONFIG_REVISION_MISMATCH,
    FESTIVAL_BEAST_LAUNCH_IN_FLIGHT,
    FESTIVAL_BEAST_NOT_ELIGIBLE,
    FESTIVAL_BEAST_PET_DEAD,
    FESTIVAL_BEAST_PET_REQUIRED,
    FESTIVAL_BEAST_REQUEST_CONFLICT,
    FestivalBeastActivitySpec,
    LAUNCH_ADMITTED,
    PHASE_HISTORY,
    PHASE_OPEN,
    PHASE_UPCOMING,
    SceneFestivalBeast,
)
from xuanming.nodes.scene_skill_craft import (
    CRAFT_MODE_RANDOM_REGULAR,
    CRAFT_MODE_TARGETED_RARE,
    CRAFT_MODE_TARGETED_REGULAR,
    MASTERY_MASTER,
    RARE_SCROLL_INK_ITEM_ID,
    RARE_SCROLL_PAGE_ITEM_ID,
    SCROLL_FRAGMENT_ITEM_ID,
    SKILL_ALREADY_BOOK_UNLOCKED,
    SKILL_BOOK_NOT_DISMANTLABLE,
    SKILL_CRAFT_MATERIAL_NOT_ENOUGH,
    SKILL_CRAFT_REVISION_CHANGED,
    SKILL_NOT_IN_CLASS,
    SKILL_PROXY_FEE_INVALID,
    SKILL_PROXY_STALL_REQUIRES_MASTER,
    SceneSkillCraft,
    proxy_net_gold,
)
from xuanming.protocol import Protocol


# ── 1. 7 张新增配置表（json / pb 双格式一致性与 68 表基线） ──────────────────────────────────────────


def test_seven_new_config_tables_json_and_pb_parity():
    json_tables = ConfigTables(fmt="json")
    pb_tables = ConfigTables(fmt="pb")

    assert len(json_tables.tables) == 83
    assert len(pb_tables.tables) == 83
    assert json_tables.content_digest == pb_tables.content_digest

    expected_counts = {
        "EquipAffixQuality": 5,
        "FestivalBeastActivity": 1,
        "FestivalBeastTier": 8,
        "SkillScrollCraft": 7,
        "TournamentReward": 9,
        "TournamentRule": 1,
        "TrialJournalEntry": 42,
        "TrialJournalMilestone": 31,
        "TrialJournalSeal": 96,
        "JianghuRule": 1,
    }
    for name, expected in expected_counts.items():
        jrows = json_tables[name].rows()
        prows = pb_tables[name].rows()
        assert len(jrows) == expected
        assert len(prows) == expected
        assert [r.SerializeToString(deterministic=True) for r in jrows] == [
            r.SerializeToString(deterministic=True) for r in prows
        ]

    # 主键索引与核心字段抽检。
    rule = json_tables["TournamentRule"].get(1)
    assert rule is not None and rule.battle_team_size == 3 and rule.roster_max_size == 4
    assert json_tables["EquipAffixQuality"].get(5).ratio_pct == 100
    assert json_tables["SkillScrollCraft"].get(2).rarity_tier == 1
    assert json_tables["TrialJournalEntry"].get(101).variant_key == "rejuvenation"
    assert json_tables["TrialJournalMilestone"].get(901).required_seals == 18
    assert json_tables["TrialJournalSeal"].get(1011).entry_id == 101


# ── 2. 全服赛季战队锦标赛《群仙问道赛》11 条 RPC 端到端测试 ────────────────────────────────────────────


@asynccontextmanager
async def _tournament_harness(clock_ref: list[int]):
    protocol = Protocol()
    tables = ConfigTables()
    key = Ed25519PrivateKey.generate()
    verifier = CallerVerifier([key.public_key()], allowed_callers=("gate",))
    store = MemoryMatchStore(protocol)
    config = MatchConfig(require_network_probe=False)
    presence = MemoryPresenceProvider()
    nodes = StaticBattleNodeResolver()
    rpc_pool = GrpcEndpointPool()
    gather = GatherPipeline(protocol, store, config, rpc_pool, presence, nodes)
    spectate = SpectateManager(protocol, store, config, rpc_pool, presence)
    tournament = TournamentManager(
        protocol,
        catalog=TournamentCatalog.from_tables(tables),
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
            req_name = f"match.{method}Request"
            res_name = f"match.{method}Response"
            req = protocol.message(req_name, **fields)
            details = protocol.message(
                "SessionDetails",
                session_id=(1 << 17) | player_id,
                player_id=player_id,
                gate_node_id=1,
                gate_instance_id="gate-uuid-1",
                session_incarnation=100 + player_id,
                owner_revision=1,
                authenticated_account=f"Hero{player_id}",
            )
            meta = session_metadata(details.SerializeToString(deterministic=True), path, key)
            raw = await client.call(path, req.SerializeToString(deterministic=True), metadata=meta)
            return protocol.parse(res_name, raw)

        yield tournament, call
    finally:
        await client.close()
        await match_svc.close()
        await server.stop(0)
        await rpc_pool.close()


def test_tournament_all_eleven_rpcs_full_season_lifecycle():
    async def exercise():
        base_ms = 1_760_000_000_000
        clock = [base_ms]
        async with _tournament_harness(clock) as (tm, call):
            # 1. GetTournamentOverview：初始报名期 + 7 档奖励展示。
            ov0 = await call(1, "GetTournamentOverview")
            assert not ov0.error_message.id
            assert ov0.current_season.season_id == 1
            assert ov0.current_season.stage == STAGE_REGISTRATION
            assert ov0.current_season.battle_team_size == 3
            assert len(ov0.reward_tiers) == 9

            # 2. CreateTournamentSquad：非法名拒绝、正常建队、重名/双队拒绝。
            bad_name = await call(1, "CreateTournamentSquad", squad_name="A")
            assert bad_name.error_message.id == ERR_MATCH_TOURNAMENT_SQUAD_NAME_INVALID
            sq_a_res = await call(1, "CreateTournamentSquad", squad_name="青云问道")
            assert not sq_a_res.error_message.id and sq_a_res.squad.member_count == 1
            sq_a_id = sq_a_res.squad.squad_id

            dup_name = await call(2, "CreateTournamentSquad", squad_name=" 青云 问道 ")
            assert dup_name.error_message.id == ERR_MATCH_TOURNAMENT_SQUAD_NAME_INVALID
            double_sq = await call(1, "CreateTournamentSquad", squad_name="另一战队")
            assert double_sq.error_message.id == ERR_MATCH_TOURNAMENT_ALREADY_IN_SQUAD

            # 3. InviteTournamentMember & RespondTournamentInvite：拉满 4 人（3 首发 + 1 替补），校验拒绝/过期/非队长权限。
            assert (await call(1, "InviteTournamentMember", target_player_id=1)).error_message.id == ERR_MATCH_TOURNAMENT_LINEUP_INVALID
            inv_2 = await call(1, "InviteTournamentMember", target_player_id=2)
            assert not inv_2.error_message.id and inv_2.invite.squad_id == sq_a_id
            # 玩家 2 查总览可见待处理邀请，先拒绝再重新邀请接受。
            ov2 = await call(2, "GetTournamentOverview")
            assert len(ov2.pending_invites) == 1 and ov2.pending_invites[0].squad_name == "青云问道"
            dec2 = await call(2, "RespondTournamentInvite", squad_id=sq_a_id, accept=False)
            assert not dec2.error_message.id
            assert (await call(2, "RespondTournamentInvite", squad_id=sq_a_id, accept=True)).error_message.id == ERR_MATCH_TOURNAMENT_INVITE_NOT_FOUND

            for pid in (2, 3, 4):
                assert not (await call(1, "InviteTournamentMember", target_player_id=pid)).error_message.id
                acc = await call(pid, "RespondTournamentInvite", squad_id=sq_a_id, accept=True)
                assert not acc.error_message.id
            # 非队长邀请被拒；满员再邀也被拒。
            assert (await call(2, "InviteTournamentMember", target_player_id=5)).error_message.id == ERR_MATCH_TOURNAMENT_NOT_CAPTAIN
            assert (await call(1, "InviteTournamentMember", target_player_id=5)).error_message.id != 0

            # 4. UpdateTournamentLineup：队长把 4 号替补换上首发，非队长与非法名单拒绝。
            assert (await call(2, "UpdateTournamentLineup", starter_player_ids=[1, 2, 4])).error_message.id == ERR_MATCH_TOURNAMENT_NOT_CAPTAIN
            assert (await call(1, "UpdateTournamentLineup", starter_player_ids=[1, 2])).error_message.id == ERR_MATCH_TOURNAMENT_LINEUP_INVALID
            lu = await call(1, "UpdateTournamentLineup", starter_player_ids=[1, 4, 2])
            assert not lu.error_message.id
            slots = {m.player_id: m.starter_slot for m in lu.squad.members}
            assert slots == {1: 1, 4: 2, 2: 3, 3: 0}

            # 5. LeaveTournamentSquad：建临时队测试踢人、退队移交队长与最后一人解散释放队名。
            tmp_sq = await call(20, "CreateTournamentSquad", squad_name="临时演武")
            assert not tmp_sq.error_message.id
            assert not (await call(20, "InviteTournamentMember", target_player_id=21)).error_message.id
            assert not (await call(21, "RespondTournamentInvite", squad_id=tmp_sq.squad.squad_id, accept=True)).error_message.id
            kick_err = await call(21, "LeaveTournamentSquad", target_player_id=20)
            assert kick_err.error_message.id == ERR_MATCH_TOURNAMENT_NOT_CAPTAIN
            # 队长 20 主动退队 -> 队长移交给 21；随后 21 退队 -> 战队解散并释放队名。
            leave_cap = await call(20, "LeaveTournamentSquad", target_player_id=0)
            assert not leave_cap.error_message.id and not leave_cap.squad_disbanded
            leave_last = await call(21, "LeaveTournamentSquad", target_player_id=0)
            assert not leave_last.error_message.id and leave_last.squad_disbanded
            # 队名已释放，可供新队使用。
            sq_b_res = await call(11, "CreateTournamentSquad", squad_name="临时演武")
            assert not sq_b_res.error_message.id
            sq_b_id = sq_b_res.squad.squad_id

            # 6. RegisterTournamentSquad：人数不足拒绝，凑齐 3 人后锁定报名，锁定后禁止邀人/退队，重调幂等。
            assert (await call(11, "RegisterTournamentSquad")).error_message.id == ERR_MATCH_TOURNAMENT_MEMBER_NOT_ENOUGH
            for pid in (12, 13):
                assert not (await call(11, "InviteTournamentMember", target_player_id=pid)).error_message.id
                assert not (await call(pid, "RespondTournamentInvite", squad_id=sq_b_id, accept=True)).error_message.id
            reg_a = await call(1, "RegisterTournamentSquad")
            assert not reg_a.error_message.id and reg_a.squad.state == SQUAD_REGISTERED
            reg_a_again = await call(1, "RegisterTournamentSquad")
            assert not reg_a_again.error_message.id and reg_a_again.squad.state == SQUAD_REGISTERED
            assert (await call(1, "LeaveTournamentSquad", target_player_id=3)).error_message.id == ERR_MATCH_TOURNAMENT_SQUAD_LOCKED
            reg_b = await call(11, "RegisterTournamentSquad")
            assert not reg_b.error_message.id and reg_b.squad.state == SQUAD_REGISTERED

            # 7. CheerTournamentSquad：观众 99 免费助威青云问道，重复助威拒绝。
            cheer = await call(99, "CheerTournamentSquad", squad_id=sq_a_id)
            assert not cheer.error_message.id and cheer.squad_cheer_count == 1
            assert (await call(99, "CheerTournamentSquad", squad_id=sq_b_id)).error_message.id == ERR_MATCH_TOURNAMENT_ALREADY_CHEERED

            # 报名期不允许开打海选。
            assert (await call(1, "StartTournamentMatch")).error_message.id == ERR_MATCH_TOURNAMENT_STAGE_CLOSED

            # 8. 推进至海选阶段并打两场海选：A 胜 B -> A 积 6 分排第 1，B 积 0 分排第 2。
            clock[0] = base_ms + 42 * MILLIS_PER_DAY
            assert tm.advance_season() == STAGE_QUALIFIER

            for _ in range(2):
                m1 = await call(1, "StartTournamentMatch")
                assert not m1.error_message.id and m1.battle_id == 0 and m1.match.state == MATCH_WAITING_OPPONENT
                m2 = await call(11, "StartTournamentMatch")
                assert not m2.error_message.id and m2.battle_id > 0 and m2.match.state == MATCH_IN_BATTLE
                assert tm.apply_battle_result(m2.battle_id, OUTCOME_SIDE_A_WIN)
                assert not tm.apply_battle_result(m2.battle_id, OUTCOME_SIDE_A_WIN)  # 幂等去重

            # 9. ListTournamentStandings：按积分排序，A 在前 B 在后。
            st = await call(1, "ListTournamentStandings", page=1, page_size=10)
            assert not st.error_message.id and st.total_squads == 2
            assert [s.squad_id for s in st.standings] == [sq_a_id, sq_b_id]
            assert st.standings[0].qualifier_points == 6 and st.standings[1].qualifier_points == 0
            assert st.my_squad.squad_id == sq_a_id

            # 10. 推进至淘汰赛阶段（2 强决赛）：双方就绪开打并产生冠军 A，再结算。
            clock[0] = base_ms + 49 * MILLIS_PER_DAY
            assert tm.advance_season() == STAGE_KNOCKOUT
            st_ko = await call(1, "ListTournamentStandings")
            assert len(st_ko.knockout_bracket) == 1
            final_match_id = st_ko.knockout_bracket[0].match_id

            ko1 = await call(1, "StartTournamentMatch", match_id=final_match_id, lineup_player_ids=[1, 4, 2])
            assert not ko1.error_message.id and ko1.battle_id == 0 and ko1.match.ready_a
            ko2 = await call(11, "StartTournamentMatch", match_id=final_match_id)
            assert not ko2.error_message.id and ko2.battle_id > 0 and ko2.match.featured_spectate
            # 观战精选可见该局。
            ov_live = await call(99, "GetTournamentOverview")
            assert len(ov_live.featured_matches) == 1 and ov_live.featured_matches[0].battle_id == ko2.battle_id

            assert tm.apply_battle_result(ko2.battle_id, OUTCOME_SIDE_A_WIN)
            # 现行配表启用 4 强 BO3（2 胜决出冠军）：局间 60s 冷却后自动推进至第 2 局并完成夺冠。
            if tm.advance_season() != STAGE_SETTLED:
                clock[0] += 61_000
                launched = tm.take_due_intermission_launches()
                assert len(launched) == 1
                assert tm.apply_battle_result(launched[0][1], OUTCOME_SIDE_A_WIN)
            assert tm.advance_season() == STAGE_SETTLED

            # 11. ClaimTournamentReward：冠军队员 1 领冠军奖、观众 99 领助威命中奖、重复领取报错，冠军殿堂可见。
            c1 = await call(1, "ClaimTournamentReward")
            assert not c1.error_message.id and len(c1.claimed_rewards) == 1
            assert c1.claimed_rewards[0].final_rank == 1 and c1.claimed_rewards[0].gold_amount == 120000
            assert c1.claimed_rewards[0].delivery_state == DELIVERY_CLAIMED
            assert (await call(1, "ClaimTournamentReward")).error_message.id == ERR_MATCH_TOURNAMENT_REWARD_UNAVAILABLE

            c99 = await call(99, "ClaimTournamentReward")
            assert not c99.error_message.id and len(c99.claimed_rewards) == 1
            assert c99.claimed_rewards[0].gold_amount == 5000

            ov_final = await call(1, "GetTournamentOverview", include_history=True)
            assert ov_final.my_squad.state == SQUAD_CHAMPION
            assert len(ov_final.hall_of_fame) == 1 and ov_final.hall_of_fame[0].champion_squad_id == sq_a_id

    asyncio.run(exercise())


# ── 3. 技能制书与主城宗师代打摊位 6 条 RPC 测试 ────────────────────────────────────────────────────────


async def _make_multi_player_scene(tmp_path, players_spec: list[dict], *, clock_ref=None, fb_activities=None, roll_fn=None):
    protocol = Protocol()
    tables = ConfigTables()
    store = SqlitePlayerStore(tmp_path / "scene_sync.sqlite3")
    codec = PlayerRowCodec(protocol)
    guids = count(50_000)

    async def alloc_guid():
        return next(guids)

    for spec in players_spec:
        pid = spec["player_id"]
        seed = await store.load_and_claim(pid, b"seed")
        data = protocol.message("player_database", player_id=pid)
        data.level_component.level = spec.get("level", 30)
        data.currency.values.extend([spec.get("gold", 0), 0, 0])
        data.profile_component.name = spec.get("name", f"Player{pid}")
        data.craft_component.forge_mastery_exp = spec.get("forge_exp", 0)
        data.craft_component.scroll_mastery_exp = spec.get("scroll_exp", 0)
        data.festival_beast_component.SetInParent()
        data.pet_component.SetInParent()
        for pet_spec in spec.get("pets", ()):
            p = data.pet_component.pets.add(
                pet_id=pet_spec["pet_id"],
                pet_table_id=pet_spec.get("pet_table_id", 1),
                name=pet_spec.get("name", "灵狐"),
                level=pet_spec.get("level", 20),
                health=pet_spec.get("health", 500),
                mana=pet_spec.get("mana", 200),
            )
            if pet_spec.get("active"):
                data.pet_component.active_pet_id = p.pet_id
        for idx, (cid, qty, bound) in enumerate(spec.get("items", ())):
            item = protocol.message(
                "ItemComp",
                item_id=next(guids),
                config_id=cid,
                size=qty,
                bound=bound,
                acquire_seq=idx + 1,
            )
            entry = data.bag_component.items.add(pos=idx, bag_type=0)
            entry.instance.CopyFrom(item)
        await store.commit(seed, codec.project(data), release=True)

    node = protocol.message("NodeInfo", node_id=8, node_uuid="sync-scene", zone_id=1)
    scene = SceneNode(protocol, node, store, codec, lambda *_: None)
    scene.tables = tables
    assets = SceneAssets(scene, tables, {}, allow_gm=True, allocate_item_id=alloc_guid)
    scene.assets = assets
    clock = (lambda: clock_ref[0]) if clock_ref is not None else (lambda: 1_760_000_000_000)

    sent_mails = []
    outbox_clock = [10.0]

    class _FakeMailSender:
        def secret(self):
            return b"x" * 32

        async def __call__(self, player_id, entry):
            sent_mails.append((
                player_id,
                int(entry.recipient_player_id),
                int(entry.source),
                [(int(c.currency_type), int(c.amount)) for c in entry.currencies],
            ))
            return SimpleNamespace(error_message=SimpleNamespace(id=0), mail_id=next(guids))

        async def close(self):
            pass

    scene.mail_outbox = SceneMailOutbox(scene, alloc_guid, _FakeMailSender(), clock=lambda: outbox_clock[0])
    scene._test_sent_mails = sent_mails
    scene._test_outbox_clock = outbox_clock

    equip_craft = SceneEquipCraft(scene, tables, assets, clock=clock)
    scene.equip_craft = equip_craft
    skill_craft = SceneSkillCraft(scene, tables, assets, clock=clock, roll_fn=roll_fn)
    festival_beast = SceneFestivalBeast(scene, tables, assets, clock=clock, activities=fb_activities)
    assets.register_client_handlers()
    equip_craft.register()
    skill_craft.register()
    festival_beast.register()

    scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))
    for spec in players_spec:
        pid = spec["player_id"]
        await scene.enter(
            protocol.message(
                "PlayerEnterGameNodeRequest",
                player_id=pid,
                session_id=100 + pid,
                scene_id=1,
                owner_revision=1,
                login_generation=1,
            ),
            "gate",
        )

    async def rpc(player_id: int, service: str, method: str, **fields):
        msg_id = protocol.message_ids[f"{service}{method}"]
        route = protocol.routes[msg_id]
        body = protocol.message(route.request_type, **fields).SerializeToString()
        req = protocol.message(
            "ProcessClientPlayerMessageRequest",
            session_id=100 + player_id,
            player_id=player_id,
            message_content=protocol.content(msg_id, 1, body=body),
        )
        res = await scene.process(req, "gate")
        assert not res.message_content.error_message.id
        return protocol.parse(route.response_type, res.message_content.serialized_message)

    return scene, skill_craft, festival_beast, rpc


def test_skill_craft_six_rpcs_and_proxy_stall(tmp_path):
    async def exercise():
        # roll_fn 恒返 0：随机选中池首技能且巧匠/宗师必触发 2 张绑定返页。
        scene, skill_craft, _, rpc = await _make_multi_player_scene(
            tmp_path,
            [
                {
                    "player_id": 1,
                    "name": "墨竹",
                    "gold": 1000,
                    "items": [
                        (SCROLL_FRAGMENT_ITEM_ID, 60, False),
                        (RARE_SCROLL_PAGE_ITEM_ID, 1, False),
                        (RARE_SCROLL_INK_ITEM_ID, 1, True),
                    ],
                    "pets": [{"pet_id": 7001, "pet_table_id": 1, "health": 500, "active": True}],
                },
                {
                    "player_id": 2,
                    "name": "陆大师",
                    "gold": 500,
                    "forge_exp": 60,
                    "scroll_exp": 60,
                },
            ],
            roll_fn=lambda bound: 0,
        )
        try:
            assert skill_craft.validate_tables()

            # 1. GetSkillCraftingState
            st0 = await rpc(1, "SceneSkillClientPlayer", "GetSkillCraftingState")
            assert not st0.error_message.id
            assert len(st0.state.scrolls) == 7
            assert st0.state.gold_balance == 1000 and st0.state.fragment_owned == 60

            # 2. SetCraftProxyStall & ListCraftProxyStalls
            assert (
                await rpc(
                    1, "SceneSkillClientPlayer", "SetCraftProxyStall",
                    open=True, proxy_forge_fee_gold=60, proxy_scroll_fee_gold=60,
                )
            ).error_message.id == SKILL_PROXY_STALL_REQUIRES_MASTER
            assert (
                await rpc(
                    2, "SceneSkillClientPlayer", "SetCraftProxyStall",
                    open=True, proxy_forge_fee_gold=30, proxy_scroll_fee_gold=60,
                )
            ).error_message.id == SKILL_PROXY_FEE_INVALID
            open_stall = await rpc(
                2, "SceneSkillClientPlayer", "SetCraftProxyStall",
                open=True, proxy_forge_fee_gold=80, proxy_scroll_fee_gold=80,
            )
            assert not open_stall.error_message.id and open_stall.state.proxy_stall_open

            stalls = await rpc(1, "SceneSkillClientPlayer", "ListCraftProxyStalls")
            assert not stalls.error_message.id and len(stalls.stalls) == 1
            assert stalls.stalls[0].master_player_id == 2 and stalls.stalls[0].scroll_mastery_tier == MASTERY_MASTER

            # 3. CraftSkillBook：走宗师 2 代制随机常规卷宗 1（扣 20 残页 + 50 基底 + 80 手工费 = 130 银两，获 2 张绑定返页 + 宗师署名）。
            c1 = await rpc(
                1, "SceneSkillClientPlayer", "CraftSkillBook",
                mode=CRAFT_MODE_RANDOM_REGULAR, scroll_id=1, target_skill_table_id=0,
                expected_revision=0, client_request_id=801, proxy_master_player_id=2,
            )
            assert not c1.error_message.id and not c1.replayed
            assert c1.crafted_book.book_id == 1 and c1.crafted_book.crafter_player_id == 2
            assert c1.crafted_book.crafter_name == "陆大师" and not c1.crafted_book.bound
            assert c1.bonus_fragments_granted == 2
            assert c1.state.gold_balance == 870 and c1.state.fragment_owned == 42  # 60 - 20 + 2
            # 验证宗师 2 不直接改本地钱包，而是由买家同事务邮件出箱异步派发 95% 净银两（80 - 4 = 76）。
            st_master = await rpc(2, "SceneSkillClientPlayer", "GetSkillCraftingState")
            assert st_master.state.gold_balance == 500
            buyer_saved = scene.rows.assemble(scene.locations[1].players[1].snapshot)
            assert len(buyer_saved.mail_outbox.entries) == 1
            out_entry = buyer_saved.mail_outbox.entries[0]
            assert out_entry.recipient_player_id == 2 and out_entry.source == 2
            assert [(c.currency_type, c.amount) for c in out_entry.currencies] == [(0, proxy_net_gold(80))]
            scene._test_outbox_clock[0] += 2.0
            scene.mail_outbox.tick()
            await asyncio.gather(*tuple(scene.mail_outbox._jobs.values()))
            assert scene._test_sent_mails == [(1, 2, 2, [(0, 76)])]
            assert not scene.rows.assemble(scene.locations[1].players[1].snapshot).mail_outbox.entries

            # 同键幂等重放不重复扣料/扣费。
            c1_rep = await rpc(
                1, "SceneSkillClientPlayer", "CraftSkillBook",
                mode=CRAFT_MODE_RANDOM_REGULAR, scroll_id=1, target_skill_table_id=0,
                expected_revision=0, client_request_id=801, proxy_master_player_id=2,
            )
            assert not c1_rep.error_message.id and c1_rep.replayed and c1_rep.crafted_book.book_id == 1

            # 旧水位拒绝。
            assert (
                await rpc(
                    1, "SceneSkillClientPlayer", "CraftSkillBook",
                    mode=CRAFT_MODE_TARGETED_REGULAR, scroll_id=3, target_skill_table_id=12020,
                    expected_revision=0, client_request_id=802,
                )
            ).error_message.id == SKILL_CRAFT_REVISION_CHANGED

            # 4. 定向制书常规卷宗 3（消耗 40 残页 + 100 银两）+ 稀世卷宗 2（消耗 1 残卷 + 1 秘墨 + 100 银两 -> 强制绑定）。
            c2 = await rpc(
                1, "SceneSkillClientPlayer", "CraftSkillBook",
                mode=CRAFT_MODE_TARGETED_REGULAR, scroll_id=3, target_skill_table_id=12020,
                expected_revision=1, client_request_id=803,
            )
            assert not c2.error_message.id and c2.crafted_book.skill_table_id == 12020 and c2.state.scroll_mastery.exp == 1
            c3 = await rpc(
                1, "SceneSkillClientPlayer", "CraftSkillBook",
                mode=CRAFT_MODE_TARGETED_RARE, scroll_id=2, target_skill_table_id=10026,
                expected_revision=2, client_request_id=804,
            )
            assert not c3.error_message.id and c3.crafted_book.bound is True and c3.crafted_book.rarity_tier == 1

            # 残页不足被拒。
            assert (
                await rpc(
                    1, "SceneSkillClientPlayer", "CraftSkillBook",
                    mode=CRAFT_MODE_RANDOM_REGULAR, scroll_id=1, target_skill_table_id=0,
                    expected_revision=3, client_request_id=805,
                )
            ).error_message.id == SKILL_CRAFT_MATERIAL_NOT_ENOUGH

            # 5. DismantleSkillBook：稀世书不可拆；常规书拆解固定返还 16 残页并幂等重放。
            assert (
                await rpc(
                    1, "SceneSkillClientPlayer", "DismantleSkillBook",
                    book_id=c3.crafted_book.book_id, expected_revision=3, client_request_id=810,
                )
            ).error_message.id == SKILL_BOOK_NOT_DISMANTLABLE

            d1 = await rpc(
                1, "SceneSkillClientPlayer", "DismantleSkillBook",
                book_id=c1.crafted_book.book_id, expected_revision=3, client_request_id=811,
            )
            assert not d1.error_message.id and d1.refunded_fragments == 16 and d1.state.fragment_owned == 18
            d1_rep = await rpc(
                1, "SceneSkillClientPlayer", "DismantleSkillBook",
                book_id=c1.crafted_book.book_id, expected_revision=3, client_request_id=811,
            )
            assert not d1_rep.error_message.id and d1_rep.replayed and d1_rep.refunded_fragments == 16

            # 6. LearnSkillBook：人物专用技 12020 不可给宝宝学；人物研习成功且幂等；稀世通用技 10026 可给宝宝学。
            assert (
                await rpc(
                    1, "SceneSkillClientPlayer", "LearnSkillBook",
                    book_id=c2.crafted_book.book_id, pet_id=7001, expected_revision=4, client_request_id=820,
                )
            ).error_message.id == SKILL_NOT_IN_CLASS
            l1 = await rpc(
                1, "SceneSkillClientPlayer", "LearnSkillBook",
                book_id=c2.crafted_book.book_id, pet_id=0, expected_revision=4, client_request_id=821,
            )
            assert not l1.error_message.id and l1.learned_skill_table_id == 12020
            assert 12020 in l1.state.book_unlocked_skill_ids
            l1_rep = await rpc(
                1, "SceneSkillClientPlayer", "LearnSkillBook",
                book_id=c2.crafted_book.book_id, pet_id=0, expected_revision=4, client_request_id=821,
            )
            assert not l1_rep.error_message.id and l1_rep.replayed and l1_rep.learned_skill_table_id == 12020

            l2 = await rpc(
                1, "SceneSkillClientPlayer", "LearnSkillBook",
                book_id=c3.crafted_book.book_id, pet_id=7001, expected_revision=5, client_request_id=822,
            )
            assert not l2.error_message.id and l2.learned_skill_table_id == 10026
            saved = scene.rows.assemble(scene.locations[1].players[1].snapshot)
            assert any(s.skill_table_id == 12020 and s.rank == 1 for s in saved.skill_list.skill_list)
            assert 10026 in saved.pet_component.pets[0].skill_table_ids

            # 关闭摊位后列表清空。
            close_stall = await rpc(2, "SceneSkillClientPlayer", "SetCraftProxyStall", open=False)
            assert not close_stall.error_message.id and not close_stall.state.proxy_stall_open
            assert len((await rpc(1, "SceneSkillClientPlayer", "ListCraftProxyStalls")).stalls) == 0
        finally:
            await scene.close()

    asyncio.run(exercise())


# ── 4. 瑞兽迎春限时活动 3 条 RPC 测试 ──────────────────────────────────────────────────────────────────


def test_festival_beast_three_rpcs_and_permanent_reward_claim(tmp_path):
    async def exercise():
        start_ms = 1_760_000_000_000
        end_ms = start_ms + 14 * DAY_MS
        clock = [start_ms - 1000]
        spec = FestivalBeastActivitySpec(
            id=202601,
            config_revision=2,
            enabled=True,
            effective_start_at_ms=start_ms,
            effective_end_at_ms=end_ms,
            required_wins=3,
            reward_identity="festival_beast_chunxishou_v1",
            reward_definition_version=1,
            reward_pet_table_id=101,
        )
        scene, _, fb, rpc = await _make_multi_player_scene(
            tmp_path,
            [
                {
                    "player_id": 1,
                    "level": 30,
                    "pets": [{"pet_id": 9001, "pet_table_id": 1, "health": 0, "active": False}],
                },
            ],
            clock_ref=clock,
            fb_activities=(spec,),
        )
        try:
            # 1. 预告期：GetFestivalBeastDetail 返回 UPCOMING 且不可开打。
            d_up = await rpc(1, "SceneActivityClientPlayer", "GetFestivalBeastDetail")
            assert not d_up.error_message.id and d_up.phase == PHASE_UPCOMING
            assert not d_up.can_start_challenge and d_up.matched_tier_id == 3  # 30 级匹配 20..34 档

            # 2. 进入开放期：无出战宝宝 -> BLOCK_PET_REQUIRED；出战 0 血宝宝 -> BLOCK_PET_DEAD；复活后可打。
            clock[0] = start_ms + 1000
            d_no_pet = await rpc(1, "SceneActivityClientPlayer", "GetFestivalBeastDetail")
            assert d_no_pet.phase == PHASE_OPEN and d_no_pet.start_block_code == BLOCK_PET_REQUIRED

            req1 = str(uuid.uuid4())
            assert (
                await rpc(
                    1, "SceneActivityClientPlayer", "StartFestivalBeastChallenge",
                    client_request_id=req1, activity_instance_id=202601, config_revision=2,
                )
            ).error_message.id == FESTIVAL_BEAST_PET_REQUIRED

            player = scene.locations[1].players[1]
            data = scene.rows.assemble(player.snapshot)
            data.pet_component.active_pet_id = 9001
            await scene.commit_player_data(player, data)
            assert (await rpc(1, "SceneActivityClientPlayer", "GetFestivalBeastDetail")).start_block_code == BLOCK_PET_DEAD
            assert (
                await rpc(
                    1, "SceneActivityClientPlayer", "StartFestivalBeastChallenge",
                    client_request_id=req1, activity_instance_id=202601, config_revision=2,
                )
            ).error_message.id == FESTIVAL_BEAST_PET_DEAD

            data = scene.rows.assemble(player.snapshot)
            data.pet_component.pets[0].health = 500
            await scene.commit_player_data(player, data)

            # 版本号不匹配拒绝。
            assert (
                await rpc(
                    1, "SceneActivityClientPlayer", "StartFestivalBeastChallenge",
                    client_request_id=req1, activity_instance_id=202601, config_revision=1,
                )
            ).error_message.id == FESTIVAL_BEAST_CONFIG_REVISION_MISMATCH

            # 3. 发起挑战成功 + 同 UUID 重放 + 参数冲突拒绝 + 异 UUID 并发在途拒绝。
            s1 = await rpc(
                1, "SceneActivityClientPlayer", "StartFestivalBeastChallenge",
                client_request_id=req1, activity_instance_id=202601, config_revision=2,
            )
            assert not s1.error_message.id and not s1.replayed
            assert s1.launch_state == LAUNCH_ADMITTED and s1.battle_id > 0 and s1.locked_pet_id == 9001

            s1_rep = await rpc(
                1, "SceneActivityClientPlayer", "StartFestivalBeastChallenge",
                client_request_id=req1, activity_instance_id=202601, config_revision=2,
            )
            assert not s1_rep.error_message.id and s1_rep.replayed and s1_rep.battle_id == s1.battle_id

            assert (
                await rpc(
                    1, "SceneActivityClientPlayer", "StartFestivalBeastChallenge",
                    client_request_id=req1, activity_instance_id=202601, config_revision=99,
                )
            ).error_message.id == FESTIVAL_BEAST_REQUEST_CONFLICT
            assert (
                await rpc(
                    1, "SceneActivityClientPlayer", "StartFestivalBeastChallenge",
                    client_request_id=str(uuid.uuid4()), activity_instance_id=202601, config_revision=2,
                )
            ).error_message.id == FESTIVAL_BEAST_LAUNCH_IN_FLIGHT

            # 未达标前领奖被拒。
            assert (
                await rpc(
                    1, "SceneActivityClientPlayer", "ClaimFestivalBeastReward",
                    client_request_id=str(uuid.uuid4()), reward_identity=spec.reward_identity,
                )
            ).error_message.id == FESTIVAL_BEAST_NOT_ELIGIBLE

            # 4. 结算 1 败 + 3 胜 -> 清理在途开局、累加胜场至 3/3 并生成永久资格。
            data = scene.rows.assemble(player.snapshot)
            r_lose = fb.record_battle_result(data, activity_instance_id=202601, won=False, battle_id=s1.battle_id)
            assert r_lose["wins_after"] == 0 and not r_lose["newly_eligible"]
            assert not data.festival_beast_component.HasField("pending_launch")
            for idx in range(1, 4):
                r_win = fb.record_battle_result(data, activity_instance_id=202601, won=True, battle_id=910_000 + idx)
                assert r_win["wins_after"] == idx
                assert r_win["newly_eligible"] is (idx == 3)
            await scene.commit_player_data(player, data)

            # 5. 推进到活动结束并超过 7 天展示期进入 HISTORY：永久资格依然永远可领！
            clock[0] = end_ms + 10 * DAY_MS
            d_hist = await rpc(1, "SceneActivityClientPlayer", "GetFestivalBeastDetail")
            assert d_hist.phase == PHASE_HISTORY and d_hist.has_permanent_eligibility
            assert d_hist.can_claim_reward and d_hist.claim_block_code == BLOCK_NONE

            claim_uuid_1 = str(uuid.uuid4())
            cl1 = await rpc(
                1, "SceneActivityClientPlayer", "ClaimFestivalBeastReward",
                client_request_id=claim_uuid_1, reward_identity=spec.reward_identity,
            )
            assert not cl1.error_message.id and cl1.outcome == CLAIM_APPLIED and not cl1.replayed
            assert cl1.durable and cl1.pet_id > 0 and cl1.reward_pet_table_id == 101

            # 同 UUID 重放返回 replayed=True；同 UUID 换 reward_identity 报错；异 UUID 收敛至同一 pet_id。
            cl1_rep = await rpc(
                1, "SceneActivityClientPlayer", "ClaimFestivalBeastReward",
                client_request_id=claim_uuid_1, reward_identity=spec.reward_identity,
            )
            assert not cl1_rep.error_message.id and cl1_rep.replayed and cl1_rep.pet_id == cl1.pet_id
            assert (
                await rpc(
                    1, "SceneActivityClientPlayer", "ClaimFestivalBeastReward",
                    client_request_id=claim_uuid_1, reward_identity="other_identity",
                )
            ).error_message.id == FESTIVAL_BEAST_REQUEST_CONFLICT

            cl2 = await rpc(
                1, "SceneActivityClientPlayer", "ClaimFestivalBeastReward",
                client_request_id=str(uuid.uuid4()), reward_identity=spec.reward_identity,
            )
            assert not cl2.error_message.id and cl2.outcome == CLAIM_ALREADY_CLAIMED
            assert cl2.pet_id == cl1.pet_id and cl2.client_request_id == claim_uuid_1

            # 核验详情已标记已领取且宠物确实落盘（且只铸了一只 101 神兽）。
            d_done = await rpc(1, "SceneActivityClientPlayer", "GetFestivalBeastDetail")
            assert d_done.reward_claimed and d_done.claimed_pet_id == cl1.pet_id
            assert d_done.claim_block_code == BLOCK_ALREADY_CLAIMED
            saved = scene.rows.assemble(player.snapshot)
            granted = [p for p in saved.pet_component.pets if p.pet_table_id == 101]
            assert len(granted) == 1 and granted[0].pet_id == cl1.pet_id
        finally:
            await scene.close()

    asyncio.run(exercise())


# ── 5. 跨节点/离线名匠代打 + 控辅强化公平受益测试 ───────────────────────────────────────────────────────


def test_offline_proxy_craft_equip_and_skill_outbox_payout(tmp_path):
    async def exercise():
        scene, _, _, rpc = await _make_multi_player_scene(
            tmp_path,
            [
                {
                    "player_id": 1,
                    "name": "墨竹",
                    "gold": 2000,
                    "items": [
                        (CRAFTING_MATERIAL_ID, 40, False),
                        (POLISH_SAND_ID, 10, False),
                        (SCROLL_FRAGMENT_ITEM_ID, 40, False),
                    ],
                },
                {
                    "player_id": 2,
                    "name": "陆大师",
                    "gold": 500,
                    "forge_exp": 60,
                    "scroll_exp": 60,
                },
            ],
            roll_fn=lambda bound: 0,
        )
        try:
            # 1. 宗师 2 开摊（锻造手工费 100 银两、制书手工费 80 银两），随后让宗师正式下线离场（释放归属锁）。
            opened = await rpc(
                2, "SceneSkillClientPlayer", "SetCraftProxyStall",
                open=True, proxy_forge_fee_gold=100, proxy_scroll_fee_gold=80,
            )
            assert not opened.error_message.id and opened.state.proxy_stall_open
            master_world = scene.locations[2]
            master_player = master_world.players[2]
            await scene._remove(master_world, master_player, release_credential=True)
            assert 2 not in scene.locations

            # 2. 宗师已不在本节点，但共享摊位目录仍能查到并 24 小时接单。
            stalls = await rpc(1, "SceneSkillClientPlayer", "ListCraftProxyStalls")
            assert not stalls.error_message.id and len(stalls.stalls) == 1
            assert stalls.stalls[0].master_player_id == 2 and stalls.stalls[0].proxy_forge_fee_gold == 100

            # 3. 买家离线走宗师 2 代打打造 V1 武器（3101）+ 安装固定词条 103（固定出 5 档极品以触发宗师署名）。
            scene.equip_craft._roll_fn = lambda _tier: 5
            rules = await rpc(1, "SceneBagClientPlayer", "GetEquipmentCraftRules")
            assert not rules.error_message.id
            created = await rpc(
                1, "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=scene.protocol.message("EquipmentCraftChoice", operation=1, config_id=3101),
                rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=0,
                client_request_id=901,
            )
            assert not created.error_message.id and created.outcome == 2
            weapon_id = created.item_id

            installed = await rpc(
                1, "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=scene.protocol.message(
                    "EquipmentCraftChoice",
                    operation=2,
                    item_id=weapon_id,
                    affix_slot=1,
                    affix_id=103,
                    proxy_master_player_id=2,
                ),
                rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=1,
                client_request_id=902,
            )
            assert not installed.error_message.id and installed.outcome == 2
            weapon_info = next(it for it in installed.bag.items if it.item_id == weapon_id)
            assert weapon_info.crafter_player_id == 2 and weapon_info.crafter_name == "陆大师"

            # 4. 买家同时离线走宗师 2 代制随机常规技能书。
            c_book = await rpc(
                1, "SceneSkillClientPlayer", "CraftSkillBook",
                mode=CRAFT_MODE_RANDOM_REGULAR,
                scroll_id=1,
                target_skill_table_id=0,
                expected_revision=0,
                client_request_id=903,
                proxy_master_player_id=2,
            )
            assert not c_book.error_message.id and c_book.crafted_book.crafter_player_id == 2
            assert c_book.crafted_book.crafter_name == "陆大师"

            # 5. 两笔代打的 95% 净银两（95、76）全部落入买家持久化出箱，派发后依次通过 MailAdmin 发出。
            outbox_entries = scene.rows.assemble(scene.locations[1].players[1].snapshot).mail_outbox.entries
            assert len(outbox_entries) == 2
            assert [(e.recipient_player_id, [(c.currency_type, c.amount) for c in e.currencies]) for e in outbox_entries] == [
                (2, [(0, 95)]),
                (2, [(0, 76)]),
            ]
            for _ in range(2):
                scene._test_outbox_clock[0] += 2.0
                scene.mail_outbox.tick()
                await asyncio.gather(*tuple(scene.mail_outbox._jobs.values()))
            assert scene._test_sent_mails == [
                (1, 2, 2, [(0, 95)]),
                (1, 2, 2, [(0, 76)]),
            ]
            assert not scene.rows.assemble(scene.locations[1].players[1].snapshot).mail_outbox.entries

            # 6. 节点重启清空内存目录后，宗师重新登录载入存档会自动把持久化营业摊位同步回共享目录。
            scene.craft_proxy_directory._stalls.clear()
            assert len((await rpc(1, "SceneSkillClientPlayer", "ListCraftProxyStalls")).stalls) == 0
            await scene.enter(
                scene.protocol.message(
                    "PlayerEnterGameNodeRequest",
                    player_id=2, session_id=102, scene_id=1, owner_revision=2, login_generation=2,
                ),
                "gate",
            )
            reloaded_stalls = await rpc(1, "SceneSkillClientPlayer", "ListCraftProxyStalls")
            assert len(reloaded_stalls.stalls) == 1 and reloaded_stalls.stalls[0].master_player_id == 2
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_support_control_fair_enhance_benefit_and_speed_never_scaled(tmp_path):
    async def exercise():
        scene, _, _, rpc = await _make_multi_player_scene(
            tmp_path,
            [{"player_id": 1, "name": "奶妈", "level": 30, "gold": 5000}],
        )
        try:
            tables = scene.tables
            protocol = scene.protocol
            player = scene.locations[1].players[1]
            equip_craft = scene.equip_craft

            # 1. 校验 V1 鞋子（3104，含防御 attr=8 与速度 attr=11）在 +11 (+24%) 时：防御放大，速度永不随强化放大。
            shoe_item = protocol.message("ItemComp", item_id=70001, config_id=3104, size=1, acquire_seq=1)
            shoe_item.equip.enhance_level = 11
            shoe_info = protocol.message("BagItemInfo", item_id=70001, config_id=3104, count=1)
            _fill_item(tables, shoe_info, shoe_item.equip)
            assert shoe_info.enhance_level == 11 and shoe_info.enhance_bonus_permyriad == 2400
            by_attr = {a.attr_id: a for a in shoe_info.base_attrs}
            assert by_attr[8].value == 14 and by_attr[8].enhance_bonus == 2  # floor(12 * 12400 / 10000) = 14
            assert by_attr[11].value == 12 and by_attr[11].enhance_bonus == 0  # 速度严禁放大

            # 2. 把 4 件 V1 满品相基础装穿在装备栏（bag_type=2）：
            #    全部带治疗 103 (effect=5, 800bps) 与封灵命中 105 (effect=11, 400bps)，其中 3101 额外校验物攻 101 (effect=1) 不放大。
            def _build_equipped_data(enhance_level: int, slot1_affix_id: int):
                data = scene.rows.assemble(player.snapshot)
                data.bag_component.ClearField("items")
                for pos, cfg_id in enumerate((3101, 3102, 3103, 3104)):
                    it = protocol.message("ItemComp", item_id=80000 + pos, config_id=cfg_id, size=1, acquire_seq=pos + 1)
                    it.equip.enhance_level = enhance_level
                    it.equip.fixed_affixes.add(slot=1, affix_id=slot1_affix_id, quality_tier=5)
                    it.equip.fixed_affixes.add(slot=2, affix_id=201, quality_tier=5)
                    slot_entry = data.bag_component.items.add(pos=pos, bag_type=2)
                    slot_entry.instance.CopyFrom(it)
                return data

            # +0 基线：4 件满品相治疗 = 3200 bps，单件展示 = 800 bps，cap = 3200。
            d0 = _build_equipped_data(0, 103)
            eff0 = protocol.message("EquipmentEffects")
            equip_craft.collect_effects(d0, eff0)
            assert eff0.outgoing_heal_bps == 3200

            # +10 (+20%)：奶盾享受 50% 折算 (+10%) => 单件 880 bps，4 件 3520 bps；控场 105 享受 25% 折算 (+5%) => 单件 420 bps，4 件 1680 bps。
            d10_heal = _build_equipped_data(10, 103)
            eff10_heal = protocol.message("EquipmentEffects")
            equip_craft.collect_effects(d10_heal, eff10_heal)
            assert eff10_heal.outgoing_heal_bps == 3520
            disp10 = protocol.message("BagItemInfo", item_id=80000, config_id=3101, count=1)
            equip_craft.fill_v1_display(d10_heal.bag_component.items[0].instance, disp10)
            assert disp10.fixed_affix_slots[0].selected.value_bps == 880
            assert disp10.fixed_affix_slots[0].selected.source_cap_bps == 3520

            d10_ctrl = _build_equipped_data(10, 105)
            eff10_ctrl = protocol.message("EquipmentEffects")
            equip_craft.collect_effects(d10_ctrl, eff10_ctrl)
            assert eff10_ctrl.ailment_hit_bps[1] == 1680

            # +11 (+24%)：奶盾 +12% => 单件 896 bps，4 件 3584 bps；控场 +6% => 单件 424 bps，4 件 1696 bps；物攻 101 不二次放大仍为 3200 bps。
            d11_heal = _build_equipped_data(11, 103)
            eff11_heal = protocol.message("EquipmentEffects")
            equip_craft.collect_effects(d11_heal, eff11_heal)
            assert eff11_heal.outgoing_heal_bps == 3584
            assert eff11_heal.support_sources[0].source_cap_bps == 3584
            assert eff11_heal.support_sources[0].effective_bonus_bps == 896

            d11_ctrl = _build_equipped_data(11, 105)
            eff11_ctrl = protocol.message("EquipmentEffects")
            equip_craft.collect_effects(d11_ctrl, eff11_ctrl)
            assert eff11_ctrl.ailment_hit_bps[1] == 1696

            d11_atk = _build_equipped_data(11, 101)
            eff11_atk = protocol.message("EquipmentEffects")
            equip_craft.collect_effects(d11_atk, eff11_atk)
            assert eff11_atk.physical_attack_bps == 3200

            # 3. 写入 +11 穿戴鞋到存档并调用 GetBag(bag_type=2)，验证满级预览下速度增强加值依然为 0。
            await scene.commit_player_data(player, d11_heal)
            bag_res = await rpc(1, "SceneBagClientPlayer", "GetBag", bag_type=2)
            assert not bag_res.error_message.id
            shoe_bag = next(it for it in bag_res.bag.items if it.config_id == 3104)
            shoe_bag_attrs = {a.attr_id: a for a in shoe_bag.base_attrs}
            assert shoe_bag_attrs[8].value == 14 and shoe_bag_attrs[8].enhance_bonus == 2
            assert shoe_bag_attrs[11].value == 12 and shoe_bag_attrs[11].enhance_bonus == 0

            # 4. 战斗入口上限护栏：接受 +11 满配 3584 / 1696，拒绝越界 3585 / 1697 / 速度 201。
            def _make_battle_req(eq_msg):
                req = protocol.message(
                    "CreateBattleRequest",
                    battle_id=99001,
                    battle_config_id=101,
                    seed=7,
                    match_mode=1,
                    created_at_ms=10_000,
                    deadline_ms=1_000_000,
                    admit_before_ms=50_000,
                    required_physical_region=1,
                    placement_operation_id="op-cap-check",
                    target_instance_id="battle-1",
                    outcome_protocol_version=1,
                )
                for pid, team, eq in ((1, 0, eq_msg), (2, 1, None)):
                    snap = req.players.add(
                        player_id=pid,
                        player_name=f"P{pid}",
                        level=30,
                        max_health=1000,
                        max_mana=200,
                        physical_attack=100,
                        magic_attack=100,
                        defense=100,
                        team_index=team,
                        class_id=1,
                        base_attributes=protocol.message(
                            "BaseAttributesComp", health=1000, mana=200, strength=10, armor=20, speed=100,
                        ),
                        routing=protocol.message("BattleRouting", gate_instance_id="g1", scene_instance_id="s1"),
                    )
                    if eq is not None:
                        snap.equipment_effects.CopyFrom(eq)
                return req

            ok_eq = protocol.message(
                "EquipmentEffects",
                outgoing_heal_bps=3584,
                shield_capacity_bps=3584,
                speed_bps=200,
            )
            ok_eq.ailment_hit_bps[1] = 1696
            TurnEngine(protocol, _make_battle_req(ok_eq), skills={}, items={}, max_rounds=10)

            for bad_eq in (
                protocol.message("EquipmentEffects", outgoing_heal_bps=3585),
                protocol.message("EquipmentEffects", shield_capacity_bps=3585),
                protocol.message("EquipmentEffects", speed_bps=201),
            ):
                with pytest.raises(BattleError):
                    TurnEngine(protocol, _make_battle_req(bad_eq), skills={}, items={}, max_rounds=10)
            bad_ctrl = protocol.message("EquipmentEffects")
            bad_ctrl.ailment_hit_bps[1] = 1697
            with pytest.raises(BattleError):
                TurnEngine(protocol, _make_battle_req(bad_ctrl), skills={}, items={}, max_rounds=10)
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_offline_proxy_reputation_title_unlock_digest_ack_and_hot_rank_top3(tmp_path):
    """验证离线/跨线名匠代打口碑异步回账、百炼宗师雅号解锁、落盘失败保留待回账、573 幂等确认与西市 Top 3 热榜。"""

    async def exercise():
        scene, _, _, rpc = await _make_multi_player_scene(
            tmp_path,
            [
                {"player_id": 1, "name": "采购侠士", "level": 60, "gold": 50_000},
                {"player_id": 2, "name": "离火名匠", "level": 60, "gold": 5_000},
                {"player_id": 3, "name": "玄冰名匠", "level": 60, "gold": 5_000},
                {"player_id": 4, "name": "青木名匠", "level": 60, "gold": 5_000},
                {"player_id": 5, "name": "闲云名匠", "level": 60, "gold": 5_000},
            ],
        )
        try:
            # 1. 四位双料宗师开摊；离火名匠(2)已有 4 件极品（再出 1 件极品解锁 2 号雅号“百炼宗师”）。
            for mid in (2, 3, 4, 5):
                mp = scene.locations[mid].players[mid]
                md = scene.rows.assemble(mp.snapshot)
                md.craft_component.forge_mastery_exp = 60
                md.craft_component.scroll_mastery_exp = 60
                if mid == 2:
                    md.craft_component.supreme_forged_count = 4
                await scene.commit_player_data(mp, md)
                opened = await rpc(
                    mid, "SceneSkillClientPlayer", "SetCraftProxyStall",
                    open=True, proxy_forge_fee_gold=100, proxy_scroll_fee_gold=80,
                )
                assert not opened.error_message.id

            # 2. 离火名匠(2)下线离场，共享摊位继续 24 小时营业。
            master2_world = scene.locations[2]
            master2_player = master2_world.players[2]
            await scene._remove(master2_world, master2_player, release_credential=True)
            assert 2 not in scene.locations

            # 3. 给采购侠士(1)发放打造与稀世制书材料并造 3 件白板装。
            rules = await rpc(1, "SceneBagClientPlayer", "GetEquipmentCraftRules")
            recipe = next(r for r in rules.recipes if r.product.equip_profile == 1)
            affix = next(a for a in rules.affixes if a.pool == 1 and not a.polish_exempt)
            state0 = await rpc(1, "SceneSkillClientPlayer", "GetSkillCraftingState")
            rare_scroll = next(s for s in state0.state.scrolls if s.rarity_tier == 1)
            rare_skill_id = rare_scroll.skill_table_ids[0]

            buyer = scene.locations[1].players[1]
            bd = scene.rows.assemble(buyer.snapshot)
            add_err = await scene.assets.add_items(
                bd,
                {
                    recipe.material_config_id: 200,
                    affix.material_config_id: 200,
                    rare_scroll.rare_scroll_item_id: 10,
                    rare_scroll.rare_ink_item_id: 10,
                },
            )
            assert add_err == 0
            await scene.commit_player_data(buyer, bd)

            equip_ids = []
            forge_rev = rules.forge_revision
            for idx in range(3):
                created = await rpc(
                    1, "SceneBagClientPlayer", "CommitEquipmentCraft",
                    client_request_id=9101 + idx,
                    rules_fingerprint=rules.rules_fingerprint,
                    expected_forge_revision=forge_rev,
                    choice=scene.protocol.message("EquipmentCraftChoice", operation=1, config_id=recipe.product.config_id),
                )
                assert not created.error_message.id
                equip_ids.append(created.item_id)
                forge_rev += 1

            # 4. 离火名匠(2) 离线接单：1 笔极品装条(95 净银两) + 1 笔稀世制书(76 净银两) -> score=1*3+2=5。
            scene.equip_craft._roll_fn = lambda _tier: 5
            r1 = await rpc(
                1, "SceneBagClientPlayer", "CommitEquipmentCraft",
                client_request_id=9111,
                rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=forge_rev,
                choice=scene.protocol.message(
                    "EquipmentCraftChoice",
                    operation=2,
                    item_id=equip_ids[0],
                    affix_slot=1,
                    affix_id=affix.affix_id,
                    proxy_master_player_id=2,
                ),
            )
            assert not r1.error_message.id and r1.result_quality_tier == 5
            forge_rev += 1

            r_rare = await rpc(
                1, "SceneSkillClientPlayer", "CraftSkillBook",
                mode=CRAFT_MODE_TARGETED_RARE,
                scroll_id=rare_scroll.scroll_id,
                target_skill_table_id=rare_skill_id,
                expected_revision=0,
                client_request_id=9112,
                proxy_master_player_id=2,
            )
            assert not r_rare.error_message.id

            # 5. 玄冰名匠(3) 在线极品单(score=4)；青木名匠(4) 在线凡品单(score=1)；闲云名匠(5) 无单(score=0)。
            scene.equip_craft._roll_fn = lambda _tier: 5
            r2 = await rpc(
                1, "SceneBagClientPlayer", "CommitEquipmentCraft",
                client_request_id=9113,
                rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=forge_rev,
                choice=scene.protocol.message(
                    "EquipmentCraftChoice",
                    operation=2,
                    item_id=equip_ids[1],
                    affix_slot=1,
                    affix_id=affix.affix_id,
                    proxy_master_player_id=3,
                ),
            )
            assert not r2.error_message.id and r2.result_quality_tier == 5
            forge_rev += 1

            scene.equip_craft._roll_fn = lambda _tier: 1
            r3 = await rpc(
                1, "SceneBagClientPlayer", "CommitEquipmentCraft",
                client_request_id=9114,
                rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=forge_rev,
                choice=scene.protocol.message(
                    "EquipmentCraftChoice",
                    operation=2,
                    item_id=equip_ids[2],
                    affix_slot=1,
                    affix_id=affix.affix_id,
                    proxy_master_player_id=4,
                ),
            )
            assert not r3.error_message.id

            # 6. 查询西市名匠摊位：离线名匠(2) 实时叠加待回账口碑与 Top 1 热榜，3=Top 2，4=Top 3，5=0。
            stalls = await rpc(1, "SceneSkillClientPlayer", "ListCraftProxyStalls")
            by_id = {s.master_player_id: s for s in stalls.stalls}
            assert by_id[2].proxy_orders_completed == 2
            assert by_id[2].supreme_forged_count == 5
            assert by_id[2].rare_books_crafted_count == 1
            assert by_id[2].recent_24h_orders == 2 and by_id[2].recent_24h_supreme == 1
            assert by_id[2].hot_rank == 1
            assert by_id[3].hot_rank == 2
            assert by_id[4].hot_rank == 3
            assert by_id[5].hot_rank == 0

            # 7. 离火名匠(2) 重新上线：自动回账待结算口碑，解锁百炼宗师(2)，产出未读战报。
            await scene.enter(
                scene.protocol.message(
                    "PlayerEnterGameNodeRequest",
                    player_id=2, session_id=102, scene_id=1, owner_revision=2, login_generation=2,
                ),
                "gate",
            )
            st_back = await rpc(2, "SceneSkillClientPlayer", "GetSkillCraftingState")
            assert not st_back.error_message.id
            snap = st_back.state
            assert snap.proxy_orders_completed == 2
            assert snap.supreme_forged_count == 5
            assert snap.rare_books_crafted_count == 1
            assert 2 in snap.unlocked_craft_title_ids
            assert snap.HasField("unread_proxy_digest")
            assert snap.unread_proxy_digest.orders_completed == 2
            assert snap.unread_proxy_digest.supreme_forged_count == 1
            assert snap.unread_proxy_digest.rare_books_crafted_count == 1
            assert snap.unread_proxy_digest.net_gold_earned == 171
            assert snap.unread_proxy_digest.last_buyer_name == "采购侠士"
            assert snap.unread_proxy_digest.newly_unlocked_title_mask & (1 << 1)

            # 8. 573 AckCraftProxyDigest 幂等清空战报且保留口碑，佩戴百炼宗师(2)生效。
            ack1 = await rpc(2, "SceneSkillClientPlayer", "AckCraftProxyDigest")
            assert not ack1.error_message.id and not ack1.state.HasField("unread_proxy_digest")
            assert ack1.state.proxy_orders_completed == 2
            ack2 = await rpc(2, "SceneSkillClientPlayer", "AckCraftProxyDigest")
            assert not ack2.error_message.id and not ack2.state.HasField("unread_proxy_digest")

            equipped = await rpc(
                2, "SceneSkillClientPlayer", "SetCraftProxyStall",
                open=True, proxy_forge_fee_gold=100, proxy_scroll_fee_gold=80,
                master_showcase_tag=1, equipped_craft_title_id=2,
            )
            assert not equipped.error_message.id and equipped.state.equipped_craft_title_id == 2
        finally:
            await scene.close()

    asyncio.run(exercise())
